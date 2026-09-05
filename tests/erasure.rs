//! Gate 8: cluster-level erasure coding.
//!
//! The erasure module is unit tested for its field arithmetic and its
//! encode/decode contract. These tests cover the integration: files written
//! with `Options.erasure` are stored as k+m shards over distinct nodes, read
//! back byte for byte, survive the loss of any m shard holders, fail cleanly
//! beyond m, and self-heal when a read regenerates lost shards.

// Gate tests intentionally use terse helpers and magic seed constants, and
// their local helper functions do not carry rustdoc error sections. Casts
// come from bounded modulo draws and short loop names are conventional here.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::must_use_candidate,
    clippy::match_same_arms,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::single_match_else,
    clippy::too_many_lines
)]

use archipelago::{sha256, Cluster, Error, Options};

mod common;
use common::{Oracle, Rng};

fn erasure_cluster(seed: u64, k: usize, m: usize, nodes: u32) -> Cluster {
    let opts = Options {
        node_count: nodes,
        ..Options::small_erasure(k, m)
    };
    Cluster::new(opts, seed)
}

#[test]
fn erasure_round_trip_across_sizes() {
    let mut c = erasure_cluster(11, 2, 2, 5);
    c.mkdir("/e").unwrap();

    let sizes = [0usize, 1, 1023, 1024, 1025, 4096, 5000];
    for (i, &len) in sizes.iter().enumerate() {
        let data: Vec<u8> = (0..len).map(|b| (b % 253) as u8).collect();
        let path = format!("/e/f{i}");
        c.write_file(&path, &data).unwrap();
        assert_eq!(c.read_file(&path).unwrap(), data, "len {len}");
        let s = c.stat(&path).unwrap();
        assert_eq!(s.size as usize, len);
        assert_eq!(s.content_hash, sha256(&data));
    }

    // Overwrite shrinks and grows the shard layout correctly.
    let path = "/e/over";
    for &len in &sizes {
        let data: Vec<u8> = (0..len).map(|b| (b.wrapping_mul(31) % 249) as u8).collect();
        c.write_file(path, &data).unwrap();
        assert_eq!(c.read_file(path).unwrap(), data, "len {len}");
    }
    c.delete(path).unwrap();
    assert!(c.read_file(path).is_err());
}

#[test]
fn erasure_differential_against_oracle() {
    let ops: usize = std::env::var("ARCH_FUZZ_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    for seed in [3u64, 9, 27] {
        let mut c = erasure_cluster(seed, 2, 2, 5);
        let mut o = Oracle::new();
        let mut r = Rng::new(seed);
        let paths = ["/f0", "/f1", "/d0", "/d0/f0", "/d0/f1", "/d0/sub"];
        for step in 0..ops {
            let p = paths[r.below(paths.len() as u64) as usize];
            match r.below(5) {
                0 => {
                    let data = r.bytes(3000);
                    let sys = c.write_file(p, &data).is_ok();
                    let ora = o.write(p, &data).is_ok();
                    assert_eq!(sys, ora, "seed {seed} step {step}: write {p}");
                }
                1 => {
                    let sys = c.read_file(p);
                    let ora = o.read(p).map(<[u8]>::to_vec);
                    assert_eq!(sys.is_ok(), ora.is_some(), "seed {seed} step {step}: read {p}");
                    if let (Ok(got), Some(want)) = (sys, ora) {
                        assert_eq!(&got, &want, "seed {seed} step {step}: bytes {p}");
                    }
                }
                2 => {
                    let sys = c.delete(p).is_ok();
                    let ora = o.delete(p).is_ok();
                    assert_eq!(sys, ora, "seed {seed} step {step}: delete {p}");
                }
                3 => {
                    let sys = c.mkdir(p).is_ok();
                    let ora = o.mkdir(p).is_ok();
                    assert_eq!(sys, ora, "seed {seed} step {step}: mkdir {p}");
                }
                _ => {
                    let q = paths[r.below(paths.len() as u64) as usize];
                    let sys = c.rename(p, q).is_ok();
                    let ora = o.rename(p, q).is_ok();
                    assert_eq!(sys, ora, "seed {seed} step {step}: rename {p} {q}");
                }
            }
        }
        // Everything the oracle holds must read back exactly.
        for path in o.file_paths() {
            let want = o.read(&path).expect("oracle file");
            assert_eq!(
                &c.read_file(&path).unwrap(),
                &want,
                "seed {seed}: final bytes at {path}"
            );
        }
    }
}

#[test]
fn erasure_tolerates_m_node_losses() {
    // k=2, m=2: every chunk's four shards live on four distinct of the five
    // nodes. Losing any two holders leaves k shards and the file still reads.
    let mut c = erasure_cluster(42, 2, 2, 5);
    let data = b"sharded across distinct nodes for two-loss tolerance".to_vec();
    c.mkdir("/r").unwrap();
    c.write_file("/r/f", &data).unwrap();

    let holders: Vec<u32> = c
        .status()
        .nodes
        .iter()
        .filter(|n| n.chunks > 0)
        .map(|n| n.idx)
        .collect();
    assert_eq!(holders.len(), 4, "shards must be on four distinct nodes");

    c.crash_node(holders[0]);
    assert_eq!(c.read_file("/r/f").unwrap(), data, "one holder down");
    c.crash_node(holders[1]);
    assert_eq!(c.read_file("/r/f").unwrap(), data, "two holders down");

    for h in &holders[..2] {
        c.recover_node(*h);
    }
    c.stabilize();
    assert_eq!(c.read_file("/r/f").unwrap(), data);
}

#[test]
fn erasure_fails_cleanly_beyond_m_losses() {
    // A fresh cluster with no reads in between, because a read repairs the
    // shards it reconstructs and would add redundancy. Crash m+1 of the k+m
    // holders and the read must be a clean unavailability, never wrong bytes.
    let mut c = erasure_cluster(42, 2, 2, 5);
    let data = b"sharded across distinct nodes for two-loss tolerance".to_vec();
    c.mkdir("/r").unwrap();
    c.write_file("/r/f", &data).unwrap();

    let holders: Vec<u32> = c
        .status()
        .nodes
        .iter()
        .filter(|n| n.chunks > 0)
        .map(|n| n.idx)
        .collect();
    assert_eq!(holders.len(), 4);

    for h in &holders[..3] {
        c.crash_node(*h);
    }
    match c.read_file("/r/f") {
        Err(Error::ChunkUnavailable(_)) => {}
        Err(other) => panic!("expected ChunkUnavailable, got {other:?}"),
        Ok(bytes) => panic!("got {} bytes beyond m losses", bytes.len()),
    }

    // Recovery restores the durable shards and full access.
    for h in &holders[..3] {
        c.recover_node(*h);
    }
    assert!(c.stabilize(), "cluster must converge after recovery");
    assert_eq!(c.read_file("/r/f").unwrap(), data);
}

#[test]
fn erasure_read_repair_regenerates_lost_shards() {
    // With one node down the file still reads. The read reconstructs the
    // chunks and re-encodes the shards that lived on the crashed node onto
    // live nodes, so the cluster converges again even before the node
    // recovers.
    let mut c = erasure_cluster(77, 2, 2, 5);
    c.mkdir("/p").unwrap();
    // A multi-chunk file exercises repair across several groups.
    let data: Vec<u8> = (0..3500u32).map(|i| (i % 251) as u8).collect();
    c.write_file("/p/f", &data).unwrap();
    assert!(c.stabilize(), "fresh cluster must be fully replicated");

    c.crash_node(2);
    // Before any read the lost shards are gone: not fully replicated.
    assert!(!c.stabilize(), "lost shards must be reported as missing");

    assert_eq!(c.read_file("/p/f").unwrap(), data, "read under one loss");
    assert!(
        c.stabilize(),
        "read repair must regenerate the lost shards"
    );

    // And the recovered node rejoins without losing anything.
    c.recover_node(2);
    c.stabilize();
    assert_eq!(c.read_file("/p/f").unwrap(), data);
}

#[test]
fn erasure_needs_enough_nodes() {
    // k+m = 5 shards cannot fit on 3 nodes with distinct placement.
    let mut c = erasure_cluster(5, 3, 2, 3);
    match c.write_file("/f", b"too few nodes") {
        Err(Error::WriteQuorumFailed { needed, got }) => {
            assert_eq!(needed, 5);
            assert_eq!(got, 3);
        }
        Err(other) => panic!("expected WriteQuorumFailed, got {other:?}"),
        Ok(()) => panic!("write must fail without room for a whole shard group"),
    }
}
