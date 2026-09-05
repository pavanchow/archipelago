//! Gate 5: differential testing under injected faults.
//!
//! The classic differential gate runs in reliable-network mode with no faults.
//! This gate runs the same kind of random operation stream while crash,
//! recovery, and partition events are interleaved, with the oracle checked as
//! far as the fault model allows:
//!
//! - A read that succeeds must return exactly the oracle bytes, unless the
//!   path was touched by a mutating op that failed client side and may have
//!   committed anyway (tainted paths). Wrong bytes on an untainted path are a
//!   hard failure.
//! - While faults are active, ops may fail with unavailability errors, but
//!   never with wrong results.
//! - In every quiescent window (all nodes recovered, partition healed,
//!   re-replication converged) the system must be back in strict agreement
//!   with the oracle: every file reads back byte for byte and listings match.
//! - A committed file that vanishes untainted, or comes back with different
//!   bytes untainted, is a hard failure. Within the configured fault bounds
//!   (fewer than R storage nodes down, fewer than a metadata quorum down)
//!   that must never happen.
//!
//! Op count is controllable with ARCH_FUZZ_OPS.

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

use archipelago::{Cluster, Error, Options};
use std::collections::BTreeSet;

mod common;
use common::{is_under, Oracle, Rng};

const NODES: u32 = 5;
const METAS: u32 = 3;

const PATHS: &[&str] = &[
    "/f0", "/f1", "/d0", "/d0/f0", "/d0/f1", "/d0/sub", "/d0/sub/f0", "/d1", "/d1/f0", "/d1/deep",
    "/d1/deep/f0",
];

fn is_unavailable(e: &Error) -> bool {
    matches!(
        e,
        Error::ChunkUnavailable(_) | Error::MetadataUnavailable | Error::WriteQuorumFailed { .. }
    )
}

fn ancestors(path: &str) -> Vec<String> {
    let mut v = Vec::new();
    let mut cur = path.to_string();
    while let Some(p) = common::parent(&cur) {
        v.push(p.clone());
        cur = p;
    }
    v
}

fn taint(tainted: &mut BTreeSet<String>, path: &str) {
    tainted.insert(path.to_string());
    for a in ancestors(path) {
        tainted.insert(a);
    }
}

fn taint_subtree(tainted: &mut BTreeSet<String>, from: &str, o: &Oracle) {
    taint(tainted, from);
    for f in o.file_paths() {
        if is_under(&f, from) {
            tainted.insert(f);
        }
    }
}

/// Recursively list every file path in the cluster.
fn walk_files(c: &mut Cluster, dir: &str) -> Vec<String> {
    let mut out = Vec::new();
    for e in c.list(dir).expect("directory must list during walk") {
        let child = if dir == "/" {
            format!("/{}", e.name)
        } else {
            format!("{dir}/{}", e.name)
        };
        if e.is_dir {
            out.extend(walk_files(c, &child));
        } else {
            out.push(child);
        }
    }
    out
}

/// Drive the cluster back to a quiescent state and re-derive the oracle.
///
/// Tainted paths may legitimately differ from the oracle because a mutating
/// op that failed client side can still commit. Untainted divergence is a
/// hard failure.
fn quiesce(c: &mut Cluster, o: &mut Oracle, tainted: &mut BTreeSet<String>) {
    for i in 0..NODES {
        c.recover_node(i);
    }
    for i in 0..METAS {
        c.recover_meta(i);
    }
    c.heal();
    assert!(c.stabilize(), "stabilize must converge after recovery");

    // Files the oracle knows that the cluster lost: legitimate only if a
    // failed-but-committed delete or rename could have removed them.
    for path in o.file_paths() {
        if c.stat(&path).is_err() {
            assert!(
                tainted.contains(&path),
                "untainted committed file {path} vanished after recovery"
            );
            o.delete(&path).ok();
        }
    }

    // Files the cluster holds: verify untainted bytes, adopt unknown files
    // (failed-but-committed writes) with their verified bytes.
    for path in walk_files(c, "/") {
        if !o.exists(&path) {
            for anc in ancestors(&path) {
                if !o.is_dir(&anc) {
                    assert!(
                        tainted.contains(&anc) || !o.exists(&anc),
                        "untainted file {anc} became a directory"
                    );
                    o.adopt_dir(&anc);
                }
            }
            let data = c.read_file(&path).expect("just listed file must read");
            o.adopt_file(&path, &data);
        } else if o.is_file(&path) && tainted.contains(&path) {
            if let Ok(data) = c.read_file(&path) {
                o.adopt_file(&path, &data);
            }
        } else if o.is_dir(&path) {
            panic!("path {path} is both a file in the cluster and a directory in the oracle");
        }
    }
    tainted.clear();
}

/// Full strict agreement between cluster and oracle. Only valid in a
/// quiescent window right after `quiesce`.
fn strict_consistency(c: &mut Cluster, o: &Oracle, seed: u64, step: usize) {
    for path in o.file_paths() {
        let want = o.read(&path).expect("oracle file");
        let got = c
            .read_file(&path)
            .unwrap_or_else(|e| panic!("seed {seed} step {step}: oracle file {path} unreadable: {e:?}"));
        assert_eq!(&got, want, "seed {seed} step {step}: byte mismatch at {path}");
    }
    let mut dirs = o.dir_paths();
    if !dirs.contains(&"/".to_string()) {
        dirs.push("/".into());
    }
    for dir in dirs {
        let want = o.listing(&dir).unwrap();
        let got: BTreeSet<(String, bool)> = c
            .list(&dir)
            .unwrap_or_else(|e| panic!("seed {seed} step {step}: listing {dir} failed: {e:?}"))
            .into_iter()
            .map(|e| (e.name, e.is_dir))
            .collect();
        assert_eq!(
            got, want,
            "seed {seed} step {step}: listing mismatch at {dir}"
        );
    }
}

fn run_seed(seed: u64, ops: usize) {
    let mut c = Cluster::new(Options::small(), seed);
    let mut o = Oracle::new();
    let mut r = Rng::new(seed);
    let mut tainted: BTreeSet<String> = BTreeSet::new();

    // Fault state.
    let mut down_storage: BTreeSet<u32> = BTreeSet::new();
    let mut down_meta: BTreeSet<u32> = BTreeSet::new();
    let mut partitioned = false;

    // A quiescent, strictly checked window every FAULT_WINDOW ops.
    const FAULT_WINDOW: usize = 12;

    for step in 0..ops {
        if step % FAULT_WINDOW == FAULT_WINDOW - 1 {
            quiesce(&mut c, &mut o, &mut tainted);
            down_storage.clear();
            down_meta.clear();
            partitioned = false;
            strict_consistency(&mut c, &o, seed, step);
            continue;
        }

        // Inject a fault roughly every third op.
        if r.below(3) == 0 {
            match r.below(6) {
                0 | 1 if down_storage.len() < 2 && !partitioned => {
                    let live: Vec<u32> = (0..NODES).filter(|i| !down_storage.contains(i)).collect();
                    if !live.is_empty() {
                        let n = live[r.below(live.len() as u64) as usize];
                        c.crash_node(n);
                        down_storage.insert(n);
                    }
                }
                2 if !down_storage.is_empty() => {
                    let n = *down_storage.iter().next().unwrap();
                    c.recover_node(n);
                    down_storage.remove(&n);
                }
                3 if down_meta.is_empty() && !partitioned => {
                    // The meta quorum is 2 of 3: exactly one meta may be down.
                    let live: Vec<u32> = (0..METAS).filter(|i| !down_meta.contains(i)).collect();
                    let n = live[r.below(live.len() as u64) as usize];
                    c.crash_meta(n);
                    down_meta.insert(n);
                }
                4 if !down_meta.is_empty() => {
                    let n = *down_meta.iter().next().unwrap();
                    c.recover_meta(n);
                    down_meta.remove(&n);
                }
                5 if down_storage.is_empty() && down_meta.is_empty() && !partitioned => {
                    // Partition one or two storage nodes away: fewer than R.
                    let count = 1 + r.below(2) as usize;
                    let group: Vec<u32> = (0..NODES)
                        .filter(|_| r.below(NODES as u64) < count as u64)
                        .take(2)
                        .collect();
                    if !group.is_empty() {
                        c.partition(&group);
                        partitioned = true;
                    }
                }
                _ => {}
            }
        }

        let faults_active = !down_storage.is_empty() || !down_meta.is_empty() || partitioned;

        let op = r.below(7);
        let p = PATHS[r.below(PATHS.len() as u64) as usize];
        match op {
            0 => {
                let data = r.bytes(2500);
                match c.write_file(p, &data) {
                    Ok(()) => {
                        assert!(
                            !o.is_dir(p),
                            "seed {seed} step {step}: wrote over a directory {p}"
                        );
                        if !o.exists(p) {
                            // The cluster accepted, so the parent chain exists
                            // there. Any missing oracle ancestor is a
                            // failed-but-committed mkdir.
                            for anc in ancestors(p) {
                                assert!(
                                    !o.is_file(&anc) || tainted.contains(&anc),
                                    "seed {seed} step {step}: wrote under untainted file {anc}"
                                );
                                if !o.is_dir(&anc) {
                                    o.adopt_dir(&anc);
                                }
                            }
                        }
                        assert!(
                            o.can_write(p),
                            "seed {seed} step {step}: cluster accepted write the oracle rejects: {p}"
                        );
                        o.write(p, &data).ok();
                    }
                    Err(e) => {
                        assert!(
                            (faults_active && is_unavailable(&e)) || !o.can_write(p),
                            "seed {seed} step {step}: unexpected write error: {e:?}"
                        );
                        if is_unavailable(&e) {
                            taint(&mut tainted, p);
                        }
                    }
                }
            }
            1 => {
                // Reads may fail under faults, which is allowed.
                if let Ok(bytes) = c.read_file(p) {
                    match o.read(p) {
                        Some(want) => {
                            if tainted.contains(p) {
                                o.adopt_file(p, &bytes);
                            } else {
                                assert_eq!(
                                    &bytes, want,
                                    "seed {seed} step {step}: WRONG BYTES at {p}"
                                );
                            }
                        }
                        None => {
                            assert!(
                                !tainted.is_empty(),
                                "seed {seed} step {step}: read of unknown untainted file {p}"
                            );
                            o.adopt_file(p, &bytes);
                        }
                    }
                }
            }
            2 => {
                match c.delete(p) {
                    Ok(()) => {
                        assert!(
                            o.can_delete(p),
                            "seed {seed} step {step}: cluster deleted what oracle protects: {p}"
                        );
                        o.delete(p).ok();
                    }
                    Err(e) => {
                        assert!(
                            (faults_active && is_unavailable(&e)) || !o.can_delete(p),
                            "seed {seed} step {step}: unexpected delete error: {e:?}"
                        );
                        if is_unavailable(&e) {
                            taint(&mut tainted, p);
                        }
                    }
                }
            }
            3 => {
                match c.mkdir(p) {
                    Ok(()) => {
                        assert!(
                            o.can_mkdir(p),
                            "seed {seed} step {step}: cluster mkdir the oracle rejects: {p}"
                        );
                        o.mkdir(p).ok();
                    }
                    Err(e) => {
                        assert!(
                            (faults_active && is_unavailable(&e)) || !o.can_mkdir(p),
                            "seed {seed} step {step}: unexpected mkdir error: {e:?}"
                        );
                        if is_unavailable(&e) {
                            taint(&mut tainted, p);
                        }
                    }
                }
            }
            4 => {
                let q = PATHS[r.below(PATHS.len() as u64) as usize];
                match c.rename(p, q) {
                    Ok(()) => {
                        assert!(
                            o.can_rename(p, q),
                            "seed {seed} step {step}: cluster renamed what oracle rejects: {p} -> {q}"
                        );
                        o.rename(p, q).ok();
                    }
                    Err(e) => {
                        assert!(
                            (faults_active && is_unavailable(&e)) || !o.can_rename(p, q),
                            "seed {seed} step {step}: unexpected rename error: {e:?}"
                        );
                        if is_unavailable(&e) {
                            taint_subtree(&mut tainted, p, &o);
                        }
                    }
                }
            }
            5 => {
                let res = c.list(p);
                let want = o.listing(p);
                match (res, want) {
                    (Ok(entries), Ok(want)) => {
                        if !tainted.contains(p) {
                            let got: BTreeSet<(String, bool)> =
                                entries.into_iter().map(|e| (e.name, e.is_dir)).collect();
                            assert_eq!(got, want, "seed {seed} step {step}: listing drift at {p}");
                        }
                    }
                    (Err(e), Ok(_)) => {
                        assert!(
                            faults_active && is_unavailable(&e),
                            "seed {seed} step {step}: unexpected list error: {e:?}"
                        );
                    }
                    (Ok(_), Err(())) => panic!(
                        "seed {seed} step {step}: listed non-directory {p} the oracle rejects"
                    ),
                    (Err(_), Err(())) => {}
                }
            }
            _ => {
                match (c.stat(p), o.exists(p)) {
                    (Ok(_), true) => {}
                    (Ok(_), false) => panic!(
                        "seed {seed} step {step}: stat found {p} the oracle does not know"
                    ),
                    (Err(e), true) => {
                        assert!(
                            faults_active && is_unavailable(&e),
                            "seed {seed} step {step}: stat of existing {p} failed: {e:?}"
                        );
                    }
                    (Err(_), false) => {}
                }
            }
        }
    }

    // Final quiescent state must be strictly consistent.
    quiesce(&mut c, &mut o, &mut tainted);
    strict_consistency(&mut c, &o, seed, ops);
}

#[test]
fn fault_differential_never_loses_or_corrupts() {
    let ops: usize = std::env::var("ARCH_FUZZ_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(120);
    for seed in [1u64, 2, 3, 17, 250, 99, 12345] {
        run_seed(seed, ops);
    }
}
