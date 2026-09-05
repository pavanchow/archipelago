//! The cluster: storage nodes, metadata nodes, and the network sim behind one
//! handle. Owns the deterministic event loop and the fault controls.

use crate::encode::Encoder;
use crate::hash::{sha256, Hash};
use crate::message::{Message, NodeId};
use crate::metadata::MetaNode;
use crate::net::Network;
use crate::options::Options;
use crate::placement::place;
use crate::storagenode::StorageNode;
use std::collections::BTreeMap;

/// Per-node status line.
#[derive(Clone, Debug)]
pub struct NodeStatus {
    /// Storage node index.
    pub idx: u32,
    /// Whether the node is online.
    pub live: bool,
    /// Distinct chunks held.
    pub chunks: usize,
}

/// Replica health for one file.
#[derive(Clone, Debug)]
pub struct FileHealth {
    /// File path.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Number of chunk positions.
    pub chunk_count: usize,
    /// The fewest live replicas held by any of the file's chunks.
    pub min_live_replicas: usize,
}

/// A snapshot of cluster health.
#[derive(Clone, Debug)]
pub struct Status {
    /// Logical clock at the time of the snapshot.
    pub clock: u64,
    /// Per storage node status.
    pub nodes: Vec<NodeStatus>,
    /// Per file replica health.
    pub files: Vec<FileHealth>,
}

/// The whole simulated cluster.
pub struct Cluster {
    pub(crate) opts: Options,
    pub(crate) net: Network,
    pub(crate) storages: BTreeMap<u32, StorageNode>,
    pub(crate) metas: BTreeMap<u32, MetaNode>,
    pub(crate) client_inbox: Vec<(NodeId, Message)>,
    pub(crate) next_req: u64,
}

impl Cluster {
    /// Build a cluster from `opts` seeded by `seed`. The network runs in
    /// reliable mode (no random drops) when `opts.link.drop_prob` is zero.
    pub fn new(opts: Options, seed: u64) -> Self {
        let reliable = opts.link.drop_prob == 0.0;
        let net = Network::new(seed, opts.link, reliable);
        let mut storages = BTreeMap::new();
        for i in 0..opts.node_count {
            storages.insert(i, StorageNode::new(i));
        }
        let all_meta: Vec<u32> = (0..opts.meta_count).collect();
        let mut metas = BTreeMap::new();
        for i in 0..opts.meta_count {
            metas.insert(
                i,
                MetaNode::new(i, opts.meta_quorum, all_meta.clone(), opts.replication_factor),
            );
        }
        let mut c = Cluster {
            opts,
            net,
            storages,
            metas,
            client_inbox: Vec::new(),
            next_req: 0,
        };
        c.refresh_meta_roles();
        c
    }

    /// Current logical clock.
    pub fn clock(&self) -> u64 {
        self.net.clock()
    }

    pub(crate) fn alloc_req(&mut self) -> u64 {
        let r = self.next_req;
        self.next_req += 1;
        r
    }

    pub(crate) fn send(&mut self, from: NodeId, to: NodeId, msg: &Message) {
        self.net.send(from, to, msg);
    }

    pub(crate) fn take_inbox(&mut self) -> Vec<(NodeId, Message)> {
        std::mem::take(&mut self.client_inbox)
    }

    /// Live storage node indices in ascending order.
    pub(crate) fn live_storage(&self) -> Vec<u32> {
        (0..self.opts.node_count)
            .filter(|&i| !self.net.is_down(NodeId::Storage(i)))
            .collect()
    }

    fn live_meta(&self) -> Vec<u32> {
        (0..self.opts.meta_count)
            .filter(|&i| !self.net.is_down(NodeId::Meta(i)))
            .collect()
    }

    /// The current primary metadata node, if any is live.
    pub(crate) fn primary_meta(&self) -> Option<u32> {
        self.live_meta().into_iter().next()
    }

    /// Deliver the next due message and dispatch it. Returns false when the
    /// network is idle.
    pub(crate) fn pump_step(&mut self) -> bool {
        let Some((from, to, msg)) = self.net.step() else {
            return false;
        };
        let out = match to {
            NodeId::Client => {
                self.client_inbox.push((from, msg));
                Vec::new()
            }
            NodeId::Storage(i) => match self.storages.get_mut(&i) {
                Some(n) => n.handle(from, msg),
                None => Vec::new(),
            },
            NodeId::Meta(i) => match self.metas.get_mut(&i) {
                Some(n) => n.handle(from, msg),
                None => Vec::new(),
            },
        };
        for (f, t, m) in out {
            self.net.send(f, t, &m);
        }
        true
    }

    /// Choose the primary, reconcile its log from the most current live node,
    /// and refresh every node's role and the primary's membership view.
    fn refresh_meta_roles(&mut self) {
        let live = self.live_meta();
        let Some(&primary) = live.first() else {
            return;
        };
        // Pick the live node with the most applied ops to seed the primary.
        let best = live
            .iter()
            .copied()
            .max_by(|&a, &b| {
                self.metas[&a]
                    .applied_seq()
                    .cmp(&self.metas[&b].applied_seq())
                    .then(b.cmp(&a))
            })
            .unwrap();
        if best != primary {
            let snap = self.metas[&best].snapshot();
            self.metas.get_mut(&primary).unwrap().restore(snap);
        }
        // Catch every backup up to the primary's committed state. A backup that
        // missed replicated ops while offline holds a permanent gap in its log
        // otherwise, can never apply anything again, and its stale buffer is
        // dead weight. Modeled as an instant state transfer, consistent with
        // the promotion path above.
        let catchup = self.metas[&primary].snapshot();
        let all_meta: Vec<u32> = (0..self.opts.meta_count).collect();
        for (&idx, node) in self.metas.iter_mut() {
            let is_primary = idx == primary;
            node.set_role(is_primary, all_meta.clone());
            if !is_primary {
                node.restore(catchup.clone());
            }
        }
        let live_storage = self.live_storage();
        self.metas
            .get_mut(&primary)
            .unwrap()
            .update_membership(live_storage, self.opts.replication_factor);
    }

    /// Take a storage node offline. Its stored bytes are retained.
    pub fn crash_node(&mut self, idx: u32) {
        self.net.crash(NodeId::Storage(idx));
    }

    /// Bring a storage node back online with its store intact.
    pub fn recover_node(&mut self, idx: u32) {
        self.net.recover(NodeId::Storage(idx));
    }

    /// Take a metadata node offline and re-elect the primary.
    pub fn crash_meta(&mut self, idx: u32) {
        self.net.crash(NodeId::Meta(idx));
        self.refresh_meta_roles();
    }

    /// Bring a metadata node back online and refresh roles.
    pub fn recover_meta(&mut self, idx: u32) {
        self.net.recover(NodeId::Meta(idx));
        self.refresh_meta_roles();
    }

    /// Partition the given storage nodes away from the rest of the cluster and
    /// the client. Cross-partition messages are dropped until [`Cluster::heal`].
    pub fn partition(&mut self, storage_idxs: &[u32]) {
        let mut all = vec![NodeId::Client];
        for i in 0..self.opts.node_count {
            all.push(NodeId::Storage(i));
        }
        for i in 0..self.opts.meta_count {
            all.push(NodeId::Meta(i));
        }
        let group_b: Vec<NodeId> = storage_idxs.iter().map(|&i| NodeId::Storage(i)).collect();
        self.net.partition(&group_b, &all);
    }

    /// Remove any active partition.
    pub fn heal(&mut self) {
        self.net.heal();
    }

    /// Run heartbeats and re-replication until every referenced chunk has the
    /// target number of live replicas or a bounded number of rounds elapses.
    /// Returns whether full replication was achieved.
    pub fn stabilize(&mut self) -> bool {
        self.refresh_meta_roles();
        let Some(primary) = self.primary_meta() else {
            return false;
        };
        let rounds = self.opts.node_count as usize * 2 + 8;
        for _ in 0..rounds {
            let live = self.live_storage();
            for i in &live {
                let chunks = self.storages[i].chunk_ids();
                self.net.send(
                    NodeId::Storage(*i),
                    NodeId::Meta(primary),
                    &Message::Heartbeat {
                        node: *i,
                        chunks,
                    },
                );
            }
            while self.pump_step() {}
            if self.fully_replicated() {
                return true;
            }
        }
        self.fully_replicated()
    }

    fn target_replicas(&self) -> usize {
        self.opts.replication_factor.min(self.live_storage().len())
    }

    fn live_replica_count(&self, chunk: &Hash) -> usize {
        self.live_storage()
            .iter()
            .filter(|&&i| self.storages[&i].has(chunk))
            .count()
    }

    fn fully_replicated(&self) -> bool {
        let target = self.target_replicas();
        let Some(primary) = self.primary_meta() else {
            return false;
        };
        self.metas[&primary]
            .referenced_chunks()
            .iter()
            .all(|c| self.live_replica_count(c) >= target)
    }

    /// A health snapshot.
    pub fn status(&self) -> Status {
        let nodes = (0..self.opts.node_count)
            .map(|i| NodeStatus {
                idx: i,
                live: !self.net.is_down(NodeId::Storage(i)),
                chunks: self.storages[&i].count(),
            })
            .collect();
        let mut files = Vec::new();
        if let Some(primary) = self.primary_meta() {
            let (fmap, _dirs) = self.metas[&primary].namespace();
            for (path, m) in fmap {
                let min = m
                    .chunks
                    .iter()
                    .map(|c| self.live_replica_count(c))
                    .min()
                    .unwrap_or(self.target_replicas());
                files.push(FileHealth {
                    path: path.clone(),
                    size: m.size,
                    chunk_count: m.chunks.len(),
                    min_live_replicas: min,
                });
            }
        }
        Status {
            clock: self.clock(),
            nodes,
            files,
        }
    }

    /// Placement of a chunk over the current live storage set.
    pub fn placement_of(&self, chunk: &Hash) -> Vec<u32> {
        place(chunk, &self.live_storage(), self.opts.replication_factor)
    }

    /// A hash of the durable cluster state: every storage node's chunk set plus
    /// the primary metadata namespace. Two runs with the same seed and script
    /// must produce the same digest.
    pub fn state_hash(&self) -> Hash {
        let mut e = Encoder::new();
        for (&idx, node) in &self.storages {
            e.put_uvarint(u64::from(idx));
            e.put_u8(u8::from(self.net.is_down(NodeId::Storage(idx))));
            let ids = node.chunk_ids();
            e.put_uvarint(ids.len() as u64);
            for id in ids {
                e.put_hash(&id);
            }
        }
        if let Some(primary) = self.primary_meta() {
            let (files, dirs) = self.metas[&primary].namespace();
            e.put_uvarint(files.len() as u64);
            for (path, m) in files {
                e.put_str(path);
                e.put_uvarint(m.size);
                e.put_hash(&m.content_hash);
                e.put_uvarint(m.chunks.len() as u64);
                for c in &m.chunks {
                    e.put_hash(c);
                }
            }
            e.put_uvarint(dirs.len() as u64);
            for d in dirs {
                e.put_str(d);
            }
        }
        sha256(&e.finish())
    }

    /// A hash of the full network delivery order, for the determinism gate.
    pub fn delivery_digest(&self) -> Hash {
        let mut e = Encoder::new();
        for d in &self.net.delivery_log {
            e.put_uvarint(d.time);
            e.put_u8(d.tag);
            match d.from {
                NodeId::Client => e.put_u8(0),
                NodeId::Storage(i) => {
                    e.put_u8(1);
                    e.put_uvarint(u64::from(i));
                }
                NodeId::Meta(i) => {
                    e.put_u8(2);
                    e.put_uvarint(u64::from(i));
                }
            }
            match d.to {
                NodeId::Client => e.put_u8(0),
                NodeId::Storage(i) => {
                    e.put_u8(1);
                    e.put_uvarint(u64::from(i));
                }
                NodeId::Meta(i) => {
                    e.put_u8(2);
                    e.put_uvarint(u64::from(i));
                }
            }
        }
        sha256(&e.finish())
    }
}
