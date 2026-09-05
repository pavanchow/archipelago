//! Replica placement by rendezvous hashing (highest random weight).
//!
//! Given a chunk id, a set of live storage nodes, and a replication factor, we
//! deterministically choose the nodes that should hold the chunk. Rendezvous
//! hashing has the property that adding or removing one node only moves the
//! chunks that were placed on (or would move onto) that one node, which keeps
//! rebalancing minimal.

use crate::hash::sha256;

/// Combine a node index and a chunk id into a 64 bit weight.
fn weight(node: u32, chunk: &crate::hash::Hash) -> u64 {
    let mut buf = Vec::with_capacity(4 + 32);
    buf.extend_from_slice(&node.to_le_bytes());
    buf.extend_from_slice(chunk.as_bytes());
    let d = sha256(&buf);
    let mut w = [0u8; 8];
    w.copy_from_slice(&d.as_bytes()[..8]);
    u64::from_be_bytes(w)
}

/// Choose up to `r` nodes to hold `chunk` from the live set `nodes`.
///
/// The result is ordered by descending weight so the first entry is the primary
/// replica. When fewer than `r` nodes are live, every live node is returned.
pub fn place(chunk: &crate::hash::Hash, nodes: &[u32], r: usize) -> Vec<u32> {
    let mut scored: Vec<(u64, u32)> = nodes.iter().map(|&n| (weight(n, chunk), n)).collect();
    // Sort by weight descending, node id ascending for a stable deterministic tie-break.
    scored.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.into_iter().take(r).map(|(_, n)| n).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;
    use std::collections::BTreeMap;

    fn chunk(i: u64) -> crate::hash::Hash {
        sha256(&i.to_le_bytes())
    }

    #[test]
    fn deterministic() {
        let nodes: Vec<u32> = (0..8).collect();
        let c = chunk(42);
        assert_eq!(place(&c, &nodes, 3), place(&c, &nodes, 3));
    }

    #[test]
    fn returns_distinct_nodes() {
        let nodes: Vec<u32> = (0..8).collect();
        for i in 0..200u64 {
            let sel = place(&chunk(i), &nodes, 3);
            assert_eq!(sel.len(), 3);
            let mut seen = std::collections::BTreeSet::new();
            for n in sel {
                assert!(seen.insert(n), "duplicate node in placement");
            }
        }
    }

    #[test]
    fn fewer_nodes_than_r() {
        let nodes = vec![0u32, 1];
        let sel = place(&chunk(1), &nodes, 3);
        assert_eq!(sel.len(), 2);
    }

    #[test]
    fn roughly_balanced_primary() {
        let nodes: Vec<u32> = (0..10).collect();
        let mut counts: BTreeMap<u32, usize> = BTreeMap::new();
        let n = 10_000u64;
        for i in 0..n {
            let sel = place(&chunk(i), &nodes, 1);
            *counts.entry(sel[0]).or_default() += 1;
        }
        let expected = n as usize / nodes.len();
        for (_node, c) in counts {
            let diff = (c as i64 - expected as i64).unsigned_abs();
            assert!(diff < expected as u64 / 2, "imbalance too high: {c}");
        }
    }

    #[test]
    fn minimal_movement_on_removal() {
        let all: Vec<u32> = (0..10).collect();
        let removed = 4u32;
        let live: Vec<u32> = all.iter().copied().filter(|&n| n != removed).collect();
        let r = 3;
        let mut moved = 0u64;
        let mut unaffected = 0u64;
        let total = 20_000u64;
        for i in 0..total {
            let c = chunk(i);
            let before = place(&c, &all, r);
            let after = place(&c, &live, r);
            if before.contains(&removed) {
                // Only chunks that lived on the removed node should change.
                moved += 1;
            } else {
                assert_eq!(before, after, "unaffected chunk was moved");
                unaffected += 1;
            }
        }
        // Roughly r/n of chunks touched the removed node.
        assert!(moved > 0 && unaffected > 0);
        let frac = moved as f64 / total as f64;
        assert!(frac < 0.45, "moved fraction too high: {frac}");
    }

    #[test]
    fn adding_node_only_pulls_onto_it() {
        let before_nodes: Vec<u32> = (0..9).collect();
        let after_nodes: Vec<u32> = (0..10).collect();
        let added = 9u32;
        let r = 3;
        for i in 0..20_000u64 {
            let c = chunk(i);
            let before = place(&c, &before_nodes, r);
            let after = place(&c, &after_nodes, r);
            if !after.contains(&added) {
                assert_eq!(before, after, "placement changed without touching new node");
            }
        }
    }
}
