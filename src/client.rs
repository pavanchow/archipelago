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
    /// the file manifest is committed to metadata. With erasure coding
    /// configured, chunks are instead encoded into `k + m` shards spread over
    /// distinct nodes.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path,
/// [`Error::WriteQuorumFailed`] when chunk acknowledgements fall short of
/// the quorum within the deadline, and [`Error::MetadataUnavailable`] when
/// the manifest cannot be committed.
    pub fn write_file(&mut self, path: &str, bytes: &[u8]) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        match self.opts.erasure {
            Some(er) => self.write_file_erasure(&path, bytes, er),
            None => self.write_file_replicated(&path, bytes),
        }
    }

    /// The replicated write path.
    fn write_file_replicated(&mut self, path: &str, bytes: &[u8]) -> Result<()> {
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
                .all(|id| acks.get(id).map_or(0, BTreeSet::len) >= wq)
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
                    .map(|id| acks.get(id).map_or(0, BTreeSet::len))
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
                    .map(|id| acks.get(id).map_or(0, BTreeSet::len))
                    .min()
                    .unwrap_or(0);
                return Err(Error::WriteQuorumFailed { needed: wq, got });
            }
        }

        self.meta_apply(
            MetaOp::Put {
                path: path.to_string(),
                manifest,
            },
            path,
        )
    }

    /// The erasure-coded write path. Every chunk becomes `k + m` shards; the
    /// shards of one chunk go to distinct storage nodes, so losing any `m`
    /// nodes still leaves `k` shards and the chunk stays readable. The write
    /// commits once every chunk group has `max(write_quorum, k)` durable
    /// shard positions, because fewer than k shards cannot reconstruct.
    fn write_file_erasure(&mut self, path: &str, bytes: &[u8], er: crate::erasure::Erasure) -> Result<()> {
        let cs = self.opts.chunk_size.max(1);
        let live = self.live_storage();
        if live.len() < er.total() {
            return Err(Error::WriteQuorumFailed {
                needed: er.total(),
                got: live.len(),
            });
        }

        // Encode every chunk into its shard group and collect the flat shard
        // id list the manifest will carry.
        let mut groups: Vec<Vec<(Hash, Vec<u8>)>> = Vec::new();
        for window in bytes.chunks(cs) {
            let shards = er.encode(window);
            groups.push(
                shards
                    .into_iter()
                    .map(|s| {
                        let id = sha256(&s);
                        (id, s)
                    })
                    .collect(),
            );
        }
        let manifest = crate::chunk::Manifest {
            size: bytes.len() as u64,
            content_hash: sha256(bytes),
            chunks: groups
                .iter()
                .flat_map(|g| g.iter().map(|(id, _)| *id))
                .collect(),
            erasure: Some((er.k as u8, er.m as u8)),
        };
        let eff_wq = self.opts.write_quorum.max(er.k);

        // Choose one distinct primary node per shard position within a group.
        // Identical shard bytes at several positions are stored once; every
        // position sharing that content address is satisfied by the same blob.
        let mut sends: Vec<(u32, Hash, Vec<u8>)> = Vec::new();
        let mut assigned: BTreeMap<Hash, u32> = BTreeMap::new();
        for g in &groups {
            let mut used: BTreeSet<u32> = BTreeSet::new();
            for (id, data) in g {
                if assigned.contains_key(id) {
                    continue;
                }
                let ranking = place(id, &live, live.len());
                let node = ranking
                    .iter()
                    .copied()
                    .find(|n| !used.contains(n))
                    .unwrap_or(ranking[0]);
                used.insert(node);
                assigned.insert(*id, node);
                sends.push((node, *id, data.clone()));
            }
        }
        for (n, id, data) in &sends {
            self.send(
                NodeId::Client,
                NodeId::Storage(*n),
                &Message::StoreChunk { id: *id, data: data.clone() },
            );
        }

        let deadline = self.clock() + self.opts.op_deadline;
        let mut acks: BTreeMap<Hash, BTreeSet<u32>> = BTreeMap::new();
        let group_ok = |g: &Vec<(Hash, Vec<u8>)>, acks: &BTreeMap<Hash, BTreeSet<u32>>| {
            g.iter()
                .filter(|(id, _)| acks.contains_key(id))
                .count()
                >= eff_wq
        };
        loop {
            for (from, msg) in self.take_inbox() {
                if let Message::StoreAck { id } = msg {
                    if let NodeId::Storage(_) = from {
                        acks.entry(id).or_default();
                    }
                }
            }
            if groups.iter().all(|g| group_ok(g, &acks)) {
                break;
            }
            if self.clock() >= deadline || !self.pump_step() {
                if groups.iter().all(|g| group_ok(g, &acks)) {
                    break;
                }
                let got = groups
                    .iter()
                    .map(|g| g.iter().filter(|(id, _)| acks.contains_key(id)).count())
                    .min()
                    .unwrap_or(0);
                return Err(Error::WriteQuorumFailed { needed: eff_wq, got });
            }
        }

        self.meta_apply(
            MetaOp::Put {
                path: path.to_string(),
                manifest,
            },
            path,
        )
    }

    /// Read the whole file at `path`, verifying its content hash end to end.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path,
/// [`Error::NotFound`] when the file does not exist,
/// [`Error::MetadataUnavailable`] when the metadata service cannot answer,
/// [`Error::ChunkUnavailable`] when no live copy of a needed chunk (or
/// enough erasure shards) survives, and [`Error::IntegrityError`] when the
/// reassembled bytes fail the content hash.
    pub fn read_file(&mut self, path: &str) -> Result<Vec<u8>> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        let manifest = match self.meta_query(Query::Get { path: path.clone() }, &path)? {
            QueryResult::Manifest(Some(m)) => m,
            QueryResult::Manifest(None) => return Err(Error::NotFound(path)),
            QueryResult::Err(c) => return Err(meta_err(c, &path)),
            _ => return Err(Error::MetadataUnavailable),
        };
        match manifest.erasure {
            Some((k, m)) => {
                let er = crate::erasure::Erasure::new(k as usize, m as usize)
                    .map_err(|_| Error::IntegrityError)?;
                self.read_file_erasure(&path, &manifest, er)
            }
            None => self.read_file_replicated(&path, &manifest),
        }
    }

    /// The erasure-coded read path. Every shard position is fetched from the
    /// live nodes and verified against its content address, so a corrupt or
    /// misplaced shard degrades to a missing one. Any k verified positions
    /// reconstruct a chunk; after reconstruction, lost shard positions are
    /// re-encoded and re-stored as fire-and-forget repair.
    fn read_file_erasure(
        &mut self,
        path: &str,
        manifest: &crate::chunk::Manifest,
        er: crate::erasure::Erasure,
    ) -> Result<Vec<u8>> {
        let live = self.live_storage();
        let total = er.total();
        let cs = self.opts.chunk_size.max(1);
        let distinct: BTreeSet<Hash> = manifest.chunks.iter().copied().collect();

        for id in &distinct {
            for &n in &live {
                self.send(NodeId::Client, NodeId::Storage(n), &Message::FetchChunk { id: *id });
            }
        }

        let deadline = self.clock() + self.opts.op_deadline;
        let mut have: BTreeMap<Hash, Vec<u8>> = BTreeMap::new();
        loop {
            for (_from, msg) in self.take_inbox() {
                if let Message::ChunkData { id, data } = msg {
                    if let Some(d) = data.filter(|d| sha256(d) == id) {
                        have.entry(id).or_insert(d);
                    }
                }
            }
            if distinct.iter().all(|id| have.contains_key(id)) {
                break;
            }
            if self.clock() >= deadline || !self.pump_step() {
                break;
            }
        }

        let size = manifest.size as usize;
        let n_groups = manifest.chunks.len() / total;
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(n_groups);
        for g in 0..n_groups {
            let group_ids = &manifest.chunks[g * total..(g + 1) * total];
            let chunk_len = (size - g * cs).min(cs);
            let shard_len = er.shard_len(chunk_len).max(1);
            let slots: Vec<Option<&[u8]>> = group_ids
                .iter()
                .map(|id| {
                    have.get(id)
                        .filter(|d| d.len() == shard_len)
                        .map(Vec::as_slice)
                })
                .collect();
            let chunk = er.decode(&slots, chunk_len).map_err(|_| {
                Error::ChunkUnavailable(format!("{path} chunk group {g}"))
            })?;

            // Repair: rebuild every missing position from the reconstructed
            // chunk and hand it to the best live node for that shard.
            let full = er.encode(&chunk);
            for (pos, id) in group_ids.iter().enumerate() {
                if slots[pos].is_none() && sha256(&full[pos]) == *id {
                    if let Some(&n) = place(id, &live, 1).first() {
                        self.send(
                            NodeId::Client,
                            NodeId::Storage(n),
                            &Message::StoreChunk {
                                id: *id,
                                data: full[pos].clone(),
                            },
                        );
                    }
                }
            }
            out.push(chunk);
        }

        let bytes = reassemble(&out);
        if sha256(&bytes) != manifest.content_hash {
            return Err(Error::IntegrityError);
        }
        Ok(bytes)
    }

    /// The replicated read path.
    fn read_file_replicated(&mut self, _path: &str, manifest: &crate::chunk::Manifest) -> Result<Vec<u8>> {
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
                if missing_at.get(id).is_some_and(|s| s.contains(&n)) {
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
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path,
/// [`Error::DirectoryNotEmpty`] when a directory still has children, and
/// [`Error::NotFound`] when the path does not exist.
    pub fn delete(&mut self, path: &str) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        self.meta_apply(MetaOp::Delete { path: path.clone() }, &path)
    }

    /// Create a directory. The parent must already exist.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path,
/// [`Error::AlreadyExists`] when the path exists, and
/// [`Error::NotFound`] or [`Error::NotADirectory`] when the parent cannot
/// hold a new directory.
    pub fn mkdir(&mut self, path: &str) -> Result<()> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        self.meta_apply(MetaOp::Mkdir { path: path.clone() }, &path)
    }

    /// Rename a file or directory subtree.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for malformed paths or a rename into the
/// source's own subtree, [`Error::NotFound`] when the source is missing, and
/// [`Error::AlreadyExists`] when the destination exists.
    pub fn rename(&mut self, from: &str, to: &str) -> Result<()> {
        let from = normalize(from).ok_or_else(|| Error::InvalidPath(from.into()))?;
        let to = normalize(to).ok_or_else(|| Error::InvalidPath(to.into()))?;
        self.meta_apply(MetaOp::Rename { from: from.clone(), to: to.clone() }, &to)
    }

    /// List the immediate children of a directory.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path,
/// [`Error::NotFound`] when the directory does not exist, and
/// [`Error::NotADirectory`] when the path is a file.
    pub fn list(&mut self, path: &str) -> Result<Vec<DirEntry>> {
        let path = normalize(path).ok_or_else(|| Error::InvalidPath(path.into()))?;
        match self.meta_query(Query::List { path: path.clone() }, &path)? {
            QueryResult::Listing(entries) => Ok(entries),
            QueryResult::Err(c) => Err(meta_err(c, &path)),
            _ => Err(Error::MetadataUnavailable),
        }
    }

    /// Stat a single path.
/// # Errors
/// 
/// Returns [`Error::InvalidPath`] for a malformed path and
/// [`Error::NotFound`] when the path does not exist.
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
