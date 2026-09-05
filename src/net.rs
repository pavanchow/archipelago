//! The deterministic network simulator.
//!
//! The whole cluster runs in one process. The "network" is a seeded priority
//! queue of envelopes ordered by delivery time. A pure-std PRNG drives per-link
//! latency, message drops, and reordering. Nodes can be crashed and healed, and
//! the node set can be split into partitions. Given the same seed and the same
//! sequence of sends and control operations, deliveries happen in the identical
//! order every run, which is what makes the distributed system testable.

use crate::message::{decode_envelope, encode_envelope, Message, NodeId};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

/// A seeded PRNG. xorshift64 with a splitmix64 seed mixer. Pure std.
#[derive(Clone)]
pub struct Prng {
    state: u64,
}

impl Prng {
    /// Seed the generator. Any seed value is accepted.
    pub fn new(seed: u64) -> Self {
        // splitmix64 to spread a possibly small seed across all bits.
        let mut z = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        let state = (z ^ (z >> 31)) | 1;
        Prng { state }
    }

    /// Next 64 bit value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// A uniform float in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// True with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        self.next_f64() < p
    }
}

/// Tunable link behaviour for the simulator.
#[derive(Clone, Copy, Debug)]
pub struct LinkParams {
    /// Minimum one-way latency in logical ticks.
    pub base_latency: u64,
    /// Extra latency drawn uniformly from `0..=jitter`. Drives reordering.
    pub jitter: u64,
    /// Probability a message is dropped on send (ignored in reliable mode).
    pub drop_prob: f64,
}

impl Default for LinkParams {
    fn default() -> Self {
        LinkParams {
            base_latency: 1,
            jitter: 4,
            drop_prob: 0.0,
        }
    }
}

#[derive(Clone)]
struct Envelope {
    time: u64,
    seq: u64,
    bytes: Vec<u8>,
}

impl PartialEq for Envelope {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.seq == other.seq
    }
}
impl Eq for Envelope {}
impl Ord for Envelope {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so the BinaryHeap (a max-heap) yields the earliest first.
        other
            .time
            .cmp(&self.time)
            .then(other.seq.cmp(&self.seq))
    }
}
impl PartialOrd for Envelope {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// One recorded delivery, used by the determinism gate.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Delivery {
    /// Logical time of delivery.
    pub time: u64,
    /// Sender.
    pub from: NodeId,
    /// Receiver.
    pub to: NodeId,
    /// Message kind tag.
    pub tag: u8,
}

/// The simulated network.
pub struct Network {
    clock: u64,
    seq: u64,
    rng: Prng,
    params: LinkParams,
    reliable: bool,
    queue: BinaryHeap<Envelope>,
    down: BTreeSet<NodeId>,
    partition: BTreeMap<NodeId, u8>,
    /// Ordered record of every delivered envelope.
    pub delivery_log: Vec<Delivery>,
}

impl Network {
    /// Create a network. In `reliable` mode no messages are ever dropped, which
    /// the differential gate relies on.
    pub fn new(seed: u64, params: LinkParams, reliable: bool) -> Self {
        Network {
            clock: 0,
            seq: 0,
            rng: Prng::new(seed),
            params,
            reliable,
            queue: BinaryHeap::new(),
            down: BTreeSet::new(),
            partition: BTreeMap::new(),
            delivery_log: Vec::new(),
        }
    }

    /// Current logical clock.
    pub fn clock(&self) -> u64 {
        self.clock
    }

    /// Whether `a` and `b` can currently exchange messages.
    fn connected(&self, a: NodeId, b: NodeId) -> bool {
        if self.down.contains(&a) || self.down.contains(&b) {
            return false;
        }
        match (self.partition.get(&a), self.partition.get(&b)) {
            (Some(ga), Some(gb)) => ga == gb,
            _ => true,
        }
    }

    /// Enqueue a message. Latency and (outside reliable mode) drops are decided
    /// here so the PRNG is consumed in send order.
    pub fn send(&mut self, from: NodeId, to: NodeId, msg: &Message) {
        let latency = self.params.base_latency
            + if self.params.jitter > 0 {
                self.rng.next_u64() % (self.params.jitter + 1)
            } else {
                0
            };
        let dropped = if self.reliable {
            false
        } else {
            self.rng.chance(self.params.drop_prob)
        };
        // A send from or to a down node, across a partition, or randomly dropped
        // simply never lands. We still consumed the RNG above to keep the stream
        // stable regardless of topology.
        if dropped || !self.connected(from, to) {
            return;
        }
        let bytes = encode_envelope(from, to, msg);
        self.queue.push(Envelope {
            time: self.clock + latency,
            seq: self.seq,
            bytes,
        });
        self.seq += 1;
    }

    /// Deliver the next due message. Messages whose endpoints became
    /// disconnected while in flight are dropped. Returns the decoded delivery.
    pub fn step(&mut self) -> Option<(NodeId, NodeId, Message)> {
        while let Some(env) = self.queue.pop() {
            self.clock = self.clock.max(env.time);
            let (from, to, msg) = decode_envelope(&env.bytes).expect("valid envelope");
            if !self.connected(from, to) {
                continue;
            }
            self.delivery_log.push(Delivery {
                time: self.clock,
                from,
                to,
                tag: msg.tag(),
            });
            return Some((from, to, msg));
        }
        None
    }

    /// Whether any messages remain queued.
    pub fn has_pending(&self) -> bool {
        !self.queue.is_empty()
    }

    /// Take a node offline. Queued messages to or from it will be dropped when
    /// they come due.
    pub fn crash(&mut self, node: NodeId) {
        self.down.insert(node);
    }

    /// Bring a node back online.
    pub fn recover(&mut self, node: NodeId) {
        self.down.remove(&node);
    }

    /// Whether a node is currently offline.
    pub fn is_down(&self, node: NodeId) -> bool {
        self.down.contains(&node)
    }

    /// Split the cluster: every node in `group_b` goes to partition 1, all other
    /// referenced nodes to partition 0. Cross-partition messages are dropped.
    pub fn partition(&mut self, group_b: &[NodeId], all: &[NodeId]) {
        self.partition.clear();
        let b: BTreeSet<NodeId> = group_b.iter().copied().collect();
        for &n in all {
            self.partition.insert(n, u8::from(b.contains(&n)));
        }
    }

    /// Remove any active partition.
    pub fn heal(&mut self) {
        self.partition.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    fn msg() -> Message {
        Message::StoreAck { id: sha256(b"x") }
    }

    #[test]
    fn prng_deterministic() {
        let mut a = Prng::new(42);
        let mut b = Prng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn prng_spreads() {
        let mut p = Prng::new(1);
        let mut ones = 0u32;
        for _ in 0..64 {
            ones += (p.next_u64() & 1) as u32;
        }
        assert!(ones > 15 && ones < 49);
    }

    #[test]
    fn partition_blocks_then_heals() {
        let mut net = Network::new(1, LinkParams::default(), true);
        let all = [NodeId::Storage(0), NodeId::Storage(1)];
        net.partition(&[NodeId::Storage(1)], &all);
        net.send(NodeId::Storage(0), NodeId::Storage(1), &msg());
        assert!(!net.has_pending(), "cross-partition send should be dropped");

        net.heal();
        net.send(NodeId::Storage(0), NodeId::Storage(1), &msg());
        assert!(net.step().is_some());
    }

    #[test]
    fn crash_drops_in_flight() {
        let mut net = Network::new(1, LinkParams::default(), true);
        net.send(NodeId::Storage(0), NodeId::Storage(1), &msg());
        net.crash(NodeId::Storage(1));
        assert!(net.step().is_none());
        net.recover(NodeId::Storage(1));
        net.send(NodeId::Storage(0), NodeId::Storage(1), &msg());
        assert!(net.step().is_some());
    }

    #[test]
    fn reliable_never_drops() {
        let params = LinkParams {
            base_latency: 1,
            jitter: 3,
            drop_prob: 1.0,
        };
        let mut net = Network::new(7, params, true);
        for _ in 0..50 {
            net.send(NodeId::Storage(0), NodeId::Meta(0), &msg());
        }
        let mut delivered = 0;
        while net.step().is_some() {
            delivered += 1;
        }
        assert_eq!(delivered, 50);
    }

    #[test]
    fn same_seed_same_delivery_order() {
        let run = || {
            let params = LinkParams {
                base_latency: 1,
                jitter: 6,
                drop_prob: 0.2,
            };
            let mut net = Network::new(99, params, false);
            for i in 0..200u32 {
                net.send(NodeId::Storage(i % 5), NodeId::Meta(0), &msg());
            }
            let mut order = Vec::new();
            while let Some((f, t, _)) = net.step() {
                order.push((f, t, net.clock()));
            }
            order
        };
        assert_eq!(run(), run());
    }
}
