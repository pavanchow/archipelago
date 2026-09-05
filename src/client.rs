//! The file system API layered over the cluster.
//!
//! Each call drives messages through the deterministic network and blocks in
//! logical time until it reaches quorum or the operation deadline. Because the
//! whole cluster is in process, "blocking" means pumping the event loop until
//! the awaited replies arrive.

use crate::chunk::{chunk_bytes, reassemble};
use crate::cluster::Cluster;
use crate::error::{Error, Result};
use crate::hash::{sha256, Hash};
use crate::message::{
    meta_err, DirEntry, Message, MetaOp, MetaResult, NodeId, Query, QueryResult, StatInfo,
};
use crate::metadata::normalize;
use crate::placement::place;
use std::collections::{BTreeMap, BTreeSet};

impl Cluster {
    /// Write `bytes` to `path`, creating or overwriting the file. Chunks are
    /// content-addressed and each is replicated to `write_quorum` nodes before
    /// the file manifest is committed to metadata.
    pub fn write_file(&mut self, path: &str, bytes: &[u8]) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        let (chunks, manifest) = chunk_bytes(bytes, self.opts.chunk_size);

        let live = self.live_storage();
        let wq = self.opts.write_quorum;
        if live.len() < wq {
            return Err(Error::WriteQuorumFailed {
                needed: wq,
                got: live.len(),
            });
        }

        // Dedupe identical chunks. Each distinct chunk is stored once.
        let mut unique: BTreeMap<Hash, Vec<u8>> = BTreeMap::new();
        for c in chunks {
            unique.entry(c.id).or_insert(c.data);
        }

        for (id, data) in &unique {
            for n in place(id, &live, self.opts.replication_factor) {
                self.send(
                    NodeId::Client,
                    NodeId::Storage(n),
                    &Message::StoreChunk {
                        id: *id,
                        data: data.clone(),
                    },
                );
            }
        }

        let deadline = self.clock() + self.opts.op_deadline;
        let mut acks: BTreeMap<Hash, BTreeSet<u32>> = BTreeMap::new();
        let satisfied = |acks: &BTreeMap<Hash, BTreeSet<u32>>| {
            unique
                .keys()
                .all(|id| acks.get(id).map(|s| s.len()).unwrap_or(0) >= wq)
        };
        loop {
            for (from, msg) in self.take_inbox() {
                if let Message::StoreAck { id } = msg {
                    if let NodeId::Storage(i) = from {
                        acks.entry(id).or_default().insert(i);
                    }
                }
            }
            if satisfied(&acks) {
                break;
            }
            if self.clock() >= deadline {
                let got = unique
                    .keys()
                    .map(|id| acks.get(id).map(|s| s.len()).unwrap_or(0))
                    .min()
                    .unwrap_or(0);
                return Err(Error::WriteQuorumFailed { needed: wq, got });
            }
            if !self.pump_step() {
                if satisfied(&acks) {
                    break;
                }
                let got = unique
                    .keys()
                    .map(|id| acks.get(id).map(|s| s.len()).unwrap_or(0))
                    .min()
                    .unwrap_or(0);
                return Err(Error::WriteQuorumFailed { needed: wq, got });
            }
        }

        self.meta_apply(MetaOp::Put {
            path: path.clone(),
            manifest,
        }, &path)
    }

    /// Read the whole file at `path`, verifying its content hash end to end.
    pub fn read_file(&mut self, path: &str) -> Result<Vec<u8>> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        let manifest = match self.meta_query(Query::Get { path: path.clone() }, &path)? {
            QueryResult::Manifest(Some(m)) => m,
            QueryResult::Manifest(None) => return Err(Error::NotFound(path)),
            QueryResult::Err(c) => return Err(meta_err(c, &path)),
            _ => return Err(Error::MetadataUnavailable),
        };

        let live = self.live_storage();
        let distinct: BTreeSet<Hash> = manifest.chunks.iter().copied().collect();

        // read_quorum is one: query every live node and take the first copy that
        // verifies against its content hash. A None answer marks a repair target.
        for id in &distinct {
            for &n in &live {
                self.send(NodeId::Client, NodeId::Storage(n), &Message::FetchChunk { id: *id });
            }
        }

        let deadline = self.clock() + self.opts.op_deadline;
        let mut have: BTreeMap<Hash, Vec<u8>> = BTreeMap::new();
        let mut missing_at: BTreeMap<Hash, BTreeSet<u32>> = BTreeMap::new();
        loop {
            for (from, msg) in self.take_inbox() {
                if let Message::ChunkData { id, data } = msg {
                    match data {
                        Some(d) if sha256(&d) == id => {
                            have.entry(id).or_insert(d);
                        }
                        _ => {
                            if let NodeId::Storage(i) = from {
                                missing_at.entry(id).or_default().insert(i);
                            }
                        }
                    }
                }
            }
            if distinct.iter().all(|id| have.contains_key(id)) {
                break;
            }
            if self.clock() >= deadline {
                break;
            }
            if !self.pump_step() {
                break;
            }
        }

        for id in &distinct {
            if !have.contains_key(id) {
                return Err(Error::ChunkUnavailable(id.short()));
            }
        }

        // Light read-repair: hand any live desired node that lacked a chunk a
        // fresh copy. Fire and forget, the acks are drained by later ops.
        for id in &distinct {
            let data = &have[id];
            for n in place(id, &live, self.opts.replication_factor) {
                if missing_at.get(id).map(|s| s.contains(&n)).unwrap_or(false) {
                    self.send(
                        NodeId::Client,
                        NodeId::Storage(n),
                        &Message::StoreChunk { id: *id, data: data.clone() },
                    );
                }
            }
        }

        let parts: Vec<Vec<u8>> = manifest.chunks.iter().map(|id| have[id].clone()).collect();
        let bytes = reassemble(&parts);
        if sha256(&bytes) != manifest.content_hash {
            return Err(Error::IntegrityError);
        }
        Ok(bytes)
    }

    /// Delete a file or empty directory.
    pub fn delete(&mut self, path: &str) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        self.meta_apply(MetaOp::Delete { path: path.clone() }, &path)
    }

    /// Create a directory. The parent must already exist.
    pub fn mkdir(&mut self, path: &str) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        self.meta_apply(MetaOp::Mkdir { path: path.clone() }, &path)
    }

    /// Rename a file or directory subtree.
    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let from = normalize(from).ok_or_else(|| Error::InvalidPath(from.into()))?;
        let to = normalize(to).ok_or_else(|| Error::InvalidPath(to.into()))?;
        self.meta_apply(MetaOp::Rename { from: from.clone(), to: to.clone() }, &to)
    }

    /// List the immediate children of a directory.
    pub fn list(&mut self, path: &str) -> Result<Vec<DirEntry>> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        match self.meta_query(Query::List { path: path.clone() }, &path)? {
            QueryResult::Listing(entries) => Ok(entries),
            QueryResult::Err(c) => Err(meta_err(c, &path)),
            _ => Err(Error::MetadataUnavailable),
        }
    }

    /// Stat a single path.
    pub fn stat(&mut self, path: &str) -> Result<StatInfo> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        match self.meta_query(Query::Stat { path: path.clone() }, &path)? {
            QueryResult::Stat(Some(s)) => Ok(s),
            QueryResult::Stat(None) => Err(Error::NotFound(path)),
            QueryResult::Err(c) => Err(meta_err(c, &path)),
            _ => Err(Error::MetadataUnavailable),
        }
    }

    fn meta_apply(&mut self, op: MetaOp, err_path: &str) -> Result<()> {
        let primary = self.primary_meta().ok_or(Error::MetadataUnavailable)?;
        let req = self.alloc_req();
        self.send(
            NodeId::Client,
            NodeId::Meta(primary),
            &Message::MetaOpMsg { req_id: req, op },
        );
        let deadline = self.clock() + self.opts.op_deadline;
        loop {
            for (_from, msg) in self.take_inbox() {
                if let Message::MetaCommitted { req_id, result } = msg {
                    if req_id == req {
                        return match result {
                            MetaResult::Ok => Ok(()),
                            MetaResult::Err(c) => Err(meta_err(c, err_path)),
                        };
                    }
                }
            }
            if self.clock() >= deadline {
                return Err(Error::MetadataUnavailable);
            }
            if !self.pump_step() {
                return Err(Error::MetadataUnavailable);
            }
        }
    }

    fn meta_query(&mut self, q: Query, _err_path: &str) -> Result<QueryResult> {
        let primary = self.primary_meta().ok_or(Error::MetadataUnavailable)?;
        let req = self.alloc_req();
        self.send(
            NodeId::Client,
            NodeId::Meta(primary),
            &Message::MetaQueryMsg { req_id: req, q },
        );
        let deadline = self.clock() + self.opts.op_deadline;
        loop {
            for (_from, msg) in self.take_inbox() {
                if let Message::MetaQueryResp { req_id, result } = msg {
                    if req_id == req {
                        return Ok(result);
                    }
                }
            }
            if self.clock() >= deadline {
                return Err(Error::MetadataUnavailable);
            }
            if !self.pump_step() {
                return Err(Error::MetadataUnavailable);
            }
        }
    }
}
