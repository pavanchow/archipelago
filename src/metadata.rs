//! The metadata service: the namespace, the replicated op-log, and the
//! placement bookkeeping that drives self-healing.
//!
//! One metadata node is the primary. Clients send mutating ops to it. The
//! primary assigns each op a sequence number and replicates it to the backups,
//! committing only when a quorum of metadata nodes have logged it. This is
//! primary-backup replication with a replicated op-log. Reads are served from
//! the primary. Placement of chunk replicas is tracked here so the primary can
//! order re-replication of under-replicated chunks.

use crate::chunk::Manifest;
use crate::hash::Hash;
use crate::message::{code, DirEntry, Message, MetaOp, MetaResult, NodeId, Query, QueryResult, StatInfo};
use crate::placement::place;
use std::collections::{BTreeMap, BTreeSet};

/// Normalize an absolute path. Returns `None` for anything malformed.
pub fn normalize(path: &str) -> Option<String> {
    if !path.starts_with('/') {
        return None;
    }
    if path == "/" {
        return Some("/".into());
    }
    let mut comps = Vec::new();
    for (i, part) in path.split('/').enumerate() {
        if i == 0 {
            continue;
        }
        if part.is_empty() || part == "." || part == ".." {
            return None;
        }
        comps.push(part);
    }
    if comps.is_empty() {
        return None;
    }
    Some(format!("/{}", comps.join("/")))
}

fn parent(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/".into()),
        Some(i) => Some(path[..i].to_string()),
        None => None,
    }
}

fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

fn is_under(path: &str, ancestor: &str) -> bool {
    if ancestor == "/" {
        return path != "/";
    }
    path.starts_with(&format!("{ancestor}/"))
}

/// A committed-state snapshot used when promoting a new primary and when
/// catching backups up after role changes.
#[derive(Clone)]
pub struct MetaSnapshot {
    files: BTreeMap<String, Manifest>,
    dirs: BTreeSet<String>,
    log: Vec<(u64, MetaOp)>,
    next_seq: u64,
    applied_seq: u64,
}

struct Pending {
    req_id: u64,
    client: NodeId,
    op: MetaOp,
    acks: BTreeSet<u32>,
}

/// A metadata node. Holds a copy of the namespace and log.
pub struct MetaNode {
    /// Node index.
    pub idx: u32,
    files: BTreeMap<String, Manifest>,
    dirs: BTreeSet<String>,
    log: Vec<(u64, MetaOp)>,
    next_seq: u64,
    applied_seq: u64,
    is_primary: bool,
    meta_quorum: usize,
    peers: Vec<u32>,
    pending: BTreeMap<u64, Pending>,
    buffer: BTreeMap<u64, MetaOp>,
    // Placement bookkeeping, meaningful on the primary.
    r: usize,
    live_storage: Vec<u32>,
    holders: BTreeMap<Hash, BTreeSet<u32>>,
}

impl MetaNode {
    /// Create a metadata node.
    pub fn new(idx: u32, meta_quorum: usize, peers: Vec<u32>, r: usize) -> Self {
        let mut dirs = BTreeSet::new();
        dirs.insert("/".to_string());
        MetaNode {
            idx,
            files: BTreeMap::new(),
            dirs,
            log: Vec::new(),
            next_seq: 0,
            applied_seq: 0,
            is_primary: false,
            meta_quorum,
            peers,
            pending: BTreeMap::new(),
            buffer: BTreeMap::new(),
            r,
            live_storage: Vec::new(),
            holders: BTreeMap::new(),
        }
    }

    /// Highest applied sequence number, used to pick the most current node on
    /// promotion.
    pub fn applied_seq(&self) -> u64 {
        self.applied_seq
    }

    /// Whether this node currently believes it is the primary.
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    /// A file manifest if present.
    pub fn manifest(&self, path: &str) -> Option<&Manifest> {
        self.files.get(path)
    }

    /// All chunk ids referenced by any file, in sorted order.
    pub fn referenced_chunks(&self) -> BTreeSet<Hash> {
        let mut set = BTreeSet::new();
        for m in self.files.values() {
            for c in &m.chunks {
                set.insert(*c);
            }
        }
        set
    }

    /// Snapshot of the namespace for state hashing and status.
    pub fn namespace(&self) -> (&BTreeMap<String, Manifest>, &BTreeSet<String>) {
        (&self.files, &self.dirs)
    }

    /// Configure this node as primary or backup and refresh its peer list.
    pub fn set_role(&mut self, is_primary: bool, peers: Vec<u32>) {
        self.is_primary = is_primary;
        self.peers = peers;
    }

    /// Update the live storage set and replication factor for healing decisions.
    pub fn update_membership(&mut self, live: Vec<u32>, r: usize) {
        self.live_storage = live;
        self.r = r;
    }

    /// Export committed state for promotion reconciliation.
    pub fn snapshot(&self) -> MetaSnapshot {
        MetaSnapshot {
            files: self.files.clone(),
            dirs: self.dirs.clone(),
            log: self.log.clone(),
            next_seq: self.next_seq,
            applied_seq: self.applied_seq,
        }
    }

    /// Install a snapshot taken from the most current live node when promoting.
    pub fn restore(&mut self, s: MetaSnapshot) {
        self.next_seq = s.next_seq.max(self.next_seq);
        self.applied_seq = s.applied_seq;
        self.files = s.files;
        self.dirs = s.dirs;
        self.log = s.log;
        self.buffer.clear();
        self.pending.clear();
    }

    fn exists_dir(&self, p: &str) -> bool {
        p == "/" || self.dirs.contains(p)
    }

    fn exists(&self, p: &str) -> bool {
        self.exists_dir(p) || self.files.contains_key(p)
    }

    /// Validate `op` against the current state without mutating it.
    fn validate(&self, op: &MetaOp) -> Result<(), u8> {
        match op {
            MetaOp::Put { path, .. } => {
                if self.dirs.contains(path) {
                    return Err(code::IS_A_DIRECTORY);
                }
                match parent(path) {
                    None => Err(code::INVALID_PATH),
                    Some(par) => {
                        if self.exists_dir(&par) {
                            Ok(())
                        } else if self.files.contains_key(&par) {
                            Err(code::NOT_A_DIRECTORY)
                        } else {
                            Err(code::NOT_FOUND)
                        }
                    }
                }
            }
            MetaOp::Mkdir { path } => {
                if self.exists(path) {
                    return Err(code::ALREADY_EXISTS);
                }
                match parent(path) {
                    None => Err(code::INVALID_PATH),
                    Some(par) => {
                        if self.exists_dir(&par) {
                            Ok(())
                        } else if self.files.contains_key(&par) {
                            Err(code::NOT_A_DIRECTORY)
                        } else {
                            Err(code::NOT_FOUND)
                        }
                    }
                }
            }
            MetaOp::Delete { path } => {
                if path == "/" {
                    return Err(code::INVALID_PATH);
                }
                if self.files.contains_key(path) {
                    Ok(())
                } else if self.dirs.contains(path) {
                    let has_child = self.children(path).next().is_some();
                    if has_child {
                        Err(code::NOT_EMPTY)
                    } else {
                        Ok(())
                    }
                } else {
                    Err(code::NOT_FOUND)
                }
            }
            MetaOp::Rename { from, to } => {
                if from == "/" || to == "/" {
                    return Err(code::INVALID_PATH);
                }
                if !self.exists(from) {
                    return Err(code::NOT_FOUND);
                }
                if self.exists(to) {
                    return Err(code::ALREADY_EXISTS);
                }
                if is_under(to, from) {
                    return Err(code::INVALID_PATH);
                }
                match parent(to) {
                    None => Err(code::INVALID_PATH),
                    Some(par) => {
                        if self.exists_dir(&par) {
                            Ok(())
                        } else if self.files.contains_key(&par) {
                            Err(code::NOT_A_DIRECTORY)
                        } else {
                            Err(code::NOT_FOUND)
                        }
                    }
                }
            }
        }
    }

    /// Apply `op`, assuming it has already validated.
    fn apply(&mut self, op: &MetaOp) {
        match op {
            MetaOp::Put { path, manifest } => {
                self.files.insert(path.clone(), manifest.clone());
            }
            MetaOp::Mkdir { path } => {
                self.dirs.insert(path.clone());
            }
            MetaOp::Delete { path } => {
                self.files.remove(path);
                self.dirs.remove(path);
            }
            MetaOp::Rename { from, to } => {
                if let Some(m) = self.files.remove(from) {
                    self.files.insert(to.clone(), m);
                } else if self.dirs.remove(from) {
                    self.dirs.insert(to.clone());
                    // Move any subtree entries.
                    let moved_dirs: Vec<String> =
                        self.dirs.iter().filter(|d| is_under(d, from)).cloned().collect();
                    for d in moved_dirs {
                        self.dirs.remove(&d);
                        let rest = &d[from.len()..];
                        self.dirs.insert(format!("{to}{rest}"));
                    }
                    let moved_files: Vec<String> =
                        self.files.keys().filter(|f| is_under(f, from)).cloned().collect();
                    for f in moved_files {
                        let m = self.files.remove(&f).unwrap();
                        let rest = &f[from.len()..];
                        self.files.insert(format!("{to}{rest}"), m);
                    }
                }
            }
        }
    }

    fn children<'a>(&'a self, dir: &'a str) -> impl Iterator<Item = (String, bool, u64)> + 'a {
        let dir_owned = dir.to_string();
        let files = self.files.iter().filter_map(move |(p, m)| {
            if parent(p).as_deref() == Some(&dir_owned) {
                Some((basename(p).to_string(), false, m.size))
            } else {
                None
            }
        });
        let dir_owned2 = dir.to_string();
        let subdirs = self.dirs.iter().filter_map(move |d| {
            if d != "/" && parent(d).as_deref() == Some(&dir_owned2) {
                Some((basename(d).to_string(), true, 0u64))
            } else {
                None
            }
        });
        files.chain(subdirs)
    }

    /// Answer a read-only query from the current committed state.
    fn answer(&self, q: &Query) -> QueryResult {
        match q {
            Query::Get { path } => {
                if self.dirs.contains(path) {
                    QueryResult::Err(code::IS_A_DIRECTORY)
                } else {
                    QueryResult::Manifest(self.files.get(path).cloned())
                }
            }
            Query::List { path } => {
                if self.exists_dir(path) {
                    let mut entries: Vec<DirEntry> = self
                        .children(path)
                        .map(|(name, is_dir, size)| DirEntry { name, is_dir, size })
                        .collect();
                    entries.sort_by(|a, b| a.name.cmp(&b.name));
                    QueryResult::Listing(entries)
                } else if self.files.contains_key(path) {
                    QueryResult::Err(code::NOT_A_DIRECTORY)
                } else {
                    QueryResult::Err(code::NOT_FOUND)
                }
            }
            Query::Stat { path } => {
                if let Some(m) = self.files.get(path) {
                    QueryResult::Stat(Some(StatInfo {
                        is_dir: false,
                        size: m.size,
                        content_hash: m.content_hash,
                    }))
                } else if self.exists_dir(path) {
                    QueryResult::Stat(Some(StatInfo {
                        is_dir: true,
                        size: 0,
                        content_hash: Hash([0u8; 32]),
                    }))
                } else {
                    QueryResult::Stat(None)
                }
            }
        }
    }

    /// Handle one message. Returns (from, to, message) tuples to send.
    /// # Panics
    ///
    /// Panics when a metadata quorum of one is configured and the node is
    /// asked to replicate to itself, which the cluster never does.
    /// The protocol dispatcher reads best as one match over message kinds.
    #[allow(clippy::too_many_lines)]
    pub fn handle(&mut self, from: NodeId, msg: Message) -> Vec<(NodeId, NodeId, Message)> {
        let me = NodeId::Meta(self.idx);
        match msg {
            Message::MetaQueryMsg { req_id, q } => {
                let result = self.answer(&q);
                vec![(me, from, Message::MetaQueryResp { req_id, result })]
            }
            Message::MetaOpMsg { req_id, op } => {
                if let Err(c) = self.validate(&op) {
                    return vec![(
                        me,
                        from,
                        Message::MetaCommitted {
                            req_id,
                            result: MetaResult::Err(c),
                        },
                    )];
                }
                let seq = self.next_seq;
                self.next_seq += 1;
                let mut acks = BTreeSet::new();
                acks.insert(self.idx);
                if acks.len() >= self.meta_quorum {
                    // Quorum of one: commit immediately.
                    self.apply(&op);
                    self.log.push((seq, op));
                    self.applied_seq = seq + 1;
                    return vec![(
                        me,
                        from,
                        Message::MetaCommitted {
                            req_id,
                            result: MetaResult::Ok,
                        },
                    )];
                }
                let mut out = Vec::new();
                for &p in &self.peers {
                    if p != self.idx {
                        out.push((
                            me,
                            NodeId::Meta(p),
                            Message::MetaReplicate { seq, op: op.clone() },
                        ));
                    }
                }
                self.pending.insert(
                    seq,
                    Pending {
                        req_id,
                        client: from,
                        op,
                        acks,
                    },
                );
                out
            }
            Message::MetaReplicate { seq, op } => {
                self.buffer.insert(seq, op);
                // Apply as far through the log as the buffer allows, then
                // acknowledge only the sequence numbers that are now durably
                // applied here. Acknowledging an op that is merely buffered
                // (because an earlier op is missing) would let a commit
                // quorum form without a second node holding the op, so one
                // primary crash could lose a committed write.
                let mut applied = Vec::new();
                while let Some(op) = self.buffer.remove(&self.applied_seq) {
                    let s = self.applied_seq;
                    self.apply(&op);
                    self.log.push((s, op));
                    self.applied_seq += 1;
                    applied.push(s);
                }
                applied
                    .into_iter()
                    .map(|s| (me, from, Message::MetaReplicateAck { seq: s }))
                    .collect()
            }
            Message::MetaReplicateAck { seq } => {
                let ready = if let Some(p) = self.pending.get_mut(&seq) {
                    if let NodeId::Meta(i) = from {
                        p.acks.insert(i);
                    }
                    p.acks.len() >= self.meta_quorum
                } else {
                    false
                };
                if ready {
                    let p = self.pending.remove(&seq).unwrap();
                    self.apply(&p.op);
                    self.log.push((seq, p.op));
                    self.applied_seq = self.applied_seq.max(seq + 1);
                    vec![(
                        me,
                        p.client,
                        Message::MetaCommitted {
                            req_id: p.req_id,
                            result: MetaResult::Ok,
                        },
                    )]
                } else {
                    Vec::new()
                }
            }
            Message::Heartbeat { node, chunks } => {
                // Refresh this node's holdings.
                for set in self.holders.values_mut() {
                    set.remove(&node);
                }
                for c in &chunks {
                    self.holders.entry(*c).or_default().insert(node);
                }
                self.plan_replication(me)
            }
            _ => Vec::new(),
        }
    }

    /// For every under-replicated referenced chunk, order a live holder to copy
    /// it to a desired node that lacks it.
    fn plan_replication(&self, me: NodeId) -> Vec<(NodeId, NodeId, Message)> {
        if !self.is_primary {
            return Vec::new();
        }
        let live: BTreeSet<u32> = self.live_storage.iter().copied().collect();
        let mut out = Vec::new();
        for chunk in self.referenced_chunks() {
            let empty = BTreeSet::new();
            let holders = self.holders.get(&chunk).unwrap_or(&empty);
            let live_holders: Vec<u32> = holders.iter().copied().filter(|h| live.contains(h)).collect();
            if live_holders.is_empty() {
                continue;
            }
            let desired = place(&chunk, &self.live_storage, self.r);
            let source = live_holders[0];
            for dest in desired {
                if !holders.contains(&dest) {
                    out.push((
                        me,
                        NodeId::Storage(source),
                        Message::ReplicateOrder { id: chunk, dest },
                    ));
                }
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    fn m() -> Manifest {
        Manifest {
            size: 3,
            content_hash: sha256(b"abc"),
            chunks: vec![sha256(b"abc")],
            erasure: None,
        }
    }

    fn node() -> MetaNode {
        let mut n = MetaNode::new(0, 1, vec![0], 3);
        n.set_role(true, vec![0]);
        n
    }

    #[test]
    fn normalize_rules() {
        assert_eq!(normalize("/"), Some("/".into()));
        assert_eq!(normalize("/a/b"), Some("/a/b".into()));
        assert_eq!(normalize("a/b"), None);
        assert_eq!(normalize("/a/"), None);
        assert_eq!(normalize("/a//b"), None);
        assert_eq!(normalize("/a/../b"), None);
    }

    #[test]
    fn mkdir_list_delete() {
        let mut n = node();
        assert!(matches!(
            n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 1, op: MetaOp::Mkdir { path: "/d".into() } })[0].2,
            Message::MetaCommitted { result: MetaResult::Ok, .. }
        ));
        // Mkdir with missing parent fails.
        let r = n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 2, op: MetaOp::Mkdir { path: "/x/y".into() } });
        assert!(matches!(r[0].2, Message::MetaCommitted { result: MetaResult::Err(code::NOT_FOUND), .. }));
        // Put a file under /d.
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 3, op: MetaOp::Put { path: "/d/f".into(), manifest: m() } });
        let resp = &n.handle(NodeId::Client, Message::MetaQueryMsg { req_id: 4, q: Query::List { path: "/d".into() } })[0].2;
        if let Message::MetaQueryResp { result: QueryResult::Listing(entries), .. } = resp {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].name, "f");
        } else {
            panic!("expected listing");
        }
    }

    #[test]
    fn delete_non_empty_dir_rejected() {
        let mut n = node();
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 1, op: MetaOp::Mkdir { path: "/d".into() } });
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 2, op: MetaOp::Put { path: "/d/f".into(), manifest: m() } });
        let r = n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 3, op: MetaOp::Delete { path: "/d".into() } });
        assert!(matches!(r[0].2, Message::MetaCommitted { result: MetaResult::Err(code::NOT_EMPTY), .. }));
    }

    #[test]
    fn rename_subtree() {
        let mut n = node();
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 1, op: MetaOp::Mkdir { path: "/a".into() } });
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 2, op: MetaOp::Put { path: "/a/f".into(), manifest: m() } });
        let r = n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 3, op: MetaOp::Rename { from: "/a".into(), to: "/b".into() } });
        assert!(matches!(r[0].2, Message::MetaCommitted { result: MetaResult::Ok, .. }));
        assert!(n.manifest("/b/f").is_some());
        assert!(n.manifest("/a/f").is_none());
    }

    #[test]
    fn rename_into_descendant_rejected() {
        let mut n = node();
        n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 1, op: MetaOp::Mkdir { path: "/a".into() } });
        let r = n.handle(NodeId::Client, Message::MetaOpMsg { req_id: 2, op: MetaOp::Rename { from: "/a".into(), to: "/a/b".into() } });
        assert!(matches!(r[0].2, Message::MetaCommitted { result: MetaResult::Err(code::INVALID_PATH), .. }));
    }
}
