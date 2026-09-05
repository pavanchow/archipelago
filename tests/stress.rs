//! Gate 7: stress across the Options space and determinism under faults.
//!
//! Two jobs:
//!
//! 1. An options matrix: the randomized differential gate only ever runs the
//!    default tuning. Here the same differential style is applied across a
//!    sampled grid of chunk sizes, replication factors, quorums, node counts,
//!    metadata counts, and network jitter, so a tuning-dependent defect
//!    cannot hide. Defaults are modest; ARCH_STRESS_OPS scales the op count
//!    for stress runs.
//!
//! 2. Determinism under faults: the same seed and the same script that
//!    includes crashes, recoveries, partitions, and metadata churn must
//!    produce the identical state hash and the identical delivery order,
//!    twice, at every sampled tuning.

use archipelago::net::LinkParams;
use archipelago::{Cluster, Hash, Options};
use std::collections::BTreeSet;

mod common;
use common::{Oracle, Rng};

fn sampled_options(r: &mut Rng) -> (Options, String) {
    let chunk_sizes = [64usize, 256, 1024, 8192, 65536];
    let node_counts = [3u32, 5, 9];
    let meta_counts = [1u32, 3, 5];

    let chunk_size = chunk_sizes[r.below(chunk_sizes.len() as u64) as usize];
    let node_count = node_counts[r.below(node_counts.len() as u64) as usize];
    let meta_count = meta_counts[r.below(meta_counts.len() as u64) as usize];

    // Replication at most the node count, write quorum at most R, meta
    // quorum at most the meta count. Only writable tunings are sampled.
    let max_r = (node_count as usize).min(5);
    let replication_factor = 1 + r.below(max_r as u64) as usize;
    let write_quorum = 1 + r.below(replication_factor as u64) as usize;
    let meta_quorum = 1 + r.below(meta_count as u64) as usize;
    let jitter = [0u64, 2, 8][r.below(3) as usize];

    let opts = Options {
        chunk_size,
        replication_factor,
        write_quorum,
        read_quorum: 1,
        node_count,
        meta_count,
        meta_quorum,
        op_deadline: 10_000,
        link: LinkParams {
            base_latency: 1,
            jitter,
            drop_prob: 0.0,
        },
    };
    let label = format!(
        "cs={chunk_size} r={replication_factor} wq={write_quorum} nodes={node_count} \
         metas={meta_count} mq={meta_quorum} jitter={jitter}"
    );
    (opts, label)
}

const PATHS: &[&str] = &["/f0", "/f1", "/d0", "/d0/f0", "/d0/f1", "/d0/sub", "/d0/sub/f0"];

fn mini_differential(opts: Options, seed: u64, ops: usize, label: &str) {
    let mut c = Cluster::new(opts, seed);
    let mut o = Oracle::new();
    let mut r = Rng::new(seed);

    for step in 0..ops {
        let op = r.below(6);
        let p = PATHS[r.below(PATHS.len() as u64) as usize];
        match op {
            0 => {
                let data = r.bytes(3000);
                let sys = c.write_file(p, &data).is_ok();
                let ora = o.write(p, &data).is_ok();
                assert_eq!(
                    sys, ora,
                    "{label} seed {seed} step {step}: write disagreement at {p}"
                );
            }
            1 => {
                let sys = c.read_file(p);
                let ora = o.read(p).map(|b| b.to_vec());
                assert_eq!(
                    sys.is_ok(),
                    ora.is_some(),
                    "{label} seed {seed} step {step}: read agreement broke at {p}"
                );
                if let (Ok(bytes), Some(want)) = (sys, ora) {
                    assert_eq!(
                        &bytes, &want,
                        "{label} seed {seed} step {step}: bytes differ at {p}"
                    );
                }
            }
            2 => {
                let sys = c.delete(p).is_ok();
                let ora = o.delete(p).is_ok();
                assert_eq!(
                    sys, ora,
                    "{label} seed {seed} step {step}: delete disagreement at {p}"
                );
            }
            3 => {
                let sys = c.mkdir(p).is_ok();
                let ora = o.mkdir(p).is_ok();
                assert_eq!(
                    sys, ora,
                    "{label} seed {seed} step {step}: mkdir disagreement at {p}"
                );
            }
            4 => {
                let q = PATHS[r.below(PATHS.len() as u64) as usize];
                let sys = c.rename(p, q).is_ok();
                let ora = o.rename(p, q).is_ok();
                assert_eq!(
                    sys, ora,
                    "{label} seed {seed} step {step}: rename disagreement {p} -> {q}"
                );
            }
            _ => {
                let sys = c.stat(p).is_ok();
                let ora = o.exists(p);
                assert_eq!(
                    sys, ora,
                    "{label} seed {seed} step {step}: stat disagreement at {p}"
                );
            }
        }
    }

    // Final full byte and listing agreement.
    for path in o.file_paths() {
        let want = o.read(&path).expect("oracle file");
        let got = c
            .read_file(&path)
            .unwrap_or_else(|e| panic!("{label} seed {seed}: {path} unreadable: {e:?}"));
        assert_eq!(&got, want, "{label} seed {seed}: bytes differ at {path}");
    }
    let mut dirs = o.dir_paths();
    dirs.push("/".into());
    dirs.sort();
    dirs.dedup();
    for dir in dirs {
        let want = o.listing(&dir).unwrap();
        let got: BTreeSet<(String, bool)> = c
            .list(&dir)
            .unwrap_or_else(|e| panic!("{label} seed {seed}: list {dir}: {e:?}"))
            .into_iter()
            .map(|e| (e.name, e.is_dir))
            .collect();
        assert_eq!(got, want, "{label} seed {seed}: listing differs at {dir}");
    }
}

#[test]
fn options_matrix_differential() {
    let ops: usize = std::env::var("ARCH_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    let mut r = Rng::new(2026_09_05);
    for cfg in 0..20 {
        let (opts, label) = sampled_options(&mut r);
        for seed in [7u64, 8] {
            mini_differential(opts, seed, ops, &format!("[cfg {cfg} {label}]"));
        }
    }
}

/// A scripted run with faults and namespace churn, entirely driven by the
/// seed. Used for the determinism gate at stress scale.
fn fault_script(opts: Options, seed: u64, rounds: usize) -> (Hash, Hash) {
    let nodes = opts.node_count;
    let metas = opts.meta_count;
    let mut c = Cluster::new(opts, seed);
    let mut r = Rng::new(seed);
    c.mkdir("/s").unwrap();

    for i in 0..rounds {
        let data = r.bytes(3000);
        let _ = c.write_file(&format!("/s/f{}", i % 10), &data);
        let _ = c.read_file(&format!("/s/f{}", r.below(10)));

        match r.below(10) {
            0 | 1 => c.crash_node(r.below(nodes as u64) as u32),
            2 => c.recover_node(r.below(nodes as u64) as u32),
            3 if metas > 1 => c.crash_meta(r.below(metas as u64) as u32),
            4 if metas > 1 => c.recover_meta(r.below(metas as u64) as u32),
            5 => {
                let n = 1 + r.below((nodes / 2).max(1) as u64) as u32;
                let group: Vec<u32> = (0..n).map(|k| k % nodes).collect();
                c.partition(&group);
            }
            6 => c.heal(),
            7 => {
                c.stabilize();
            }
            _ => {}
        }
    }

    // Converge fully: every node and meta back, no partitions.
    for i in 0..nodes {
        c.recover_node(i);
    }
    for i in 0..metas {
        c.recover_meta(i);
    }
    c.heal();
    c.stabilize();
    (c.state_hash(), c.delivery_digest())
}

#[test]
fn determinism_under_faults_across_tunings() {
    let rounds: usize = std::env::var("ARCH_STRESS_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);
    let mut r = Rng::new(777_001);
    for _cfg in 0..12 {
        let (opts, label) = sampled_options(&mut r);
        let a = fault_script(opts, 555, rounds);
        let b = fault_script(opts, 555, rounds);
        assert_eq!(
            a.0, b.0,
            "{label}: state hash diverged between identical runs"
        );
        assert_eq!(
            a.1, b.1,
            "{label}: delivery order diverged between identical runs"
        );
    }
}

#[test]
fn different_seeds_produce_different_schedules() {
    let (opts, label) = {
        let mut r = Rng::new(424_242);
        sampled_options(&mut r)
    };
    let a = fault_script(opts, 1, 30);
    let b = fault_script(opts, 2, 30);
    assert_ne!(a.1, b.1, "{label}: seeds produced identical delivery order");
}
