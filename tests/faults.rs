//! Gate 2: durability and fault tolerance.
//!
//! With replication factor R, losing fewer than R replicas must never lose
//! data. Losing exactly R replicas of a chunk must be reported as
//! unavailability, never as wrong bytes. After healing, re-replication must
//! restore R live replicas of every chunk.

use archipelago::{sha256, Cluster, Error, Options};

fn write_corpus(c: &mut Cluster, seed: u64) -> Vec<(String, Vec<u8>)> {
    c.mkdir("/c").unwrap();
    let mut out = Vec::new();
    for i in 0..12u64 {
        let len = ((seed.wrapping_mul(31).wrapping_add(i * 997)) % 9000) as usize + 1;
        let data: Vec<u8> = (0..len).map(|j| (j as u64 ^ (i + seed)) as u8).collect();
        let path = format!("/c/f{i}");
        c.write_file(&path, &data).unwrap();
        out.push((path, data));
    }
    out
}

fn verify_all(c: &mut Cluster, corpus: &[(String, Vec<u8>)]) {
    for (path, data) in corpus {
        let got = c.read_file(path).expect("file must survive < R failures");
        assert_eq!(sha256(&got), sha256(data), "content hash mismatch on {path}");
    }
}

#[test]
fn no_data_loss_below_r_failures() {
    for seed in [1u64, 5, 13, 99, 2024] {
        let mut c = Cluster::new(Options::default(), seed);
        let corpus = write_corpus(&mut c, seed);

        // Crash R-1 = 2 nodes at adversarial points, verifying between each.
        c.crash_node(0);
        verify_all(&mut c, &corpus);
        c.crash_node(3);
        verify_all(&mut c, &corpus);

        // Re-replicate onto the remaining live nodes while the two stay down.
        assert!(c.stabilize(), "re-replication did not converge (seed {seed})");
        for f in c.status().files {
            assert_eq!(
                f.min_live_replicas, 3,
                "chunk of {} under-replicated after heal (seed {seed})",
                f.path
            );
        }

        // Recover the crashed nodes and confirm everything still reads.
        c.recover_node(0);
        c.recover_node(3);
        c.stabilize();
        verify_all(&mut c, &corpus);
    }
}

#[test]
fn partition_below_r_keeps_data() {
    let mut c = Cluster::new(Options::default(), 77);
    let corpus = write_corpus(&mut c, 77);

    // Isolate two nodes from the client and the rest. Fewer than R, so data
    // must still be reachable through the majority side.
    c.partition(&[1, 4]);
    verify_all(&mut c, &corpus);
    c.heal();
    verify_all(&mut c, &corpus);
}

#[test]
fn exactly_r_failures_reports_unavailable_not_corrupt() {
    let opts = Options::default();
    let mut c = Cluster::new(opts, 3);
    let data = b"a single chunk file whose every replica we will kill".to_vec();
    c.write_file("/victim", &data).unwrap();
    assert_eq!(c.read_file("/victim").unwrap(), data);

    // The file is one chunk. Crash every node that holds it: exactly R failures
    // affecting this chunk.
    let chunk_id = sha256(&data);
    let holders = c.placement_of(&chunk_id);
    assert_eq!(holders.len(), opts.replication_factor);
    for n in holders {
        c.crash_node(n);
    }

    match c.read_file("/victim") {
        Err(Error::ChunkUnavailable(_)) => {}
        Err(other) => panic!("expected ChunkUnavailable, got {other:?}"),
        Ok(bytes) => panic!("returned {} bytes instead of failing loudly", bytes.len()),
    }
}

#[test]
fn committed_write_survives_meta_failure_sequence() {
    // A write acknowledged by a metadata quorum must survive the loss of fewer
    // than a quorum of metadata nodes at any single point in time. This
    // exercises a subtler window than metadata_survives_primary_crash: a
    // backup that holds a gap in its log must not let its acknowledgement
    // count toward a commit quorum, because its acknowledgement does not yet
    // mean the op is durable there.
    let opts = Options {
        meta_count: 5,
        meta_quorum: 3,
        ..Options::default()
    };
    let mut c = Cluster::new(opts, 11);

    // Two backups are down while the first write commits, so they miss seq 0
    // and come back with a gap in their replicated log.
    c.crash_meta(3);
    c.crash_meta(4);
    c.write_file("/keep", b"committed before the gap").unwrap();
    c.recover_meta(3);
    c.recover_meta(4);

    // The two backups that hold seq 0 crash. The live set is exactly the
    // primary plus the two gap-holding backups.
    c.crash_meta(1);
    c.crash_meta(2);

    let victim = b"a committed write that must survive one meta failure".to_vec();
    let committed = c.write_file("/victim", &victim).is_ok();

    // Every other metadata node recovers and then the primary itself crashes.
    // Exactly one metadata node is down at the end, which is within the
    // tolerance of a three node quorum.
    c.recover_meta(1);
    c.recover_meta(2);
    c.crash_meta(0);

    // A write the client saw as Ok must still be readable.
    if committed {
        let got = c
            .read_file("/victim")
            .expect("a committed write must survive one concurrent meta failure");
        assert_eq!(got, victim);
    }
    // The first write committed on a healthy full quorum and must always survive.
    assert_eq!(
        c.read_file("/keep").unwrap(),
        b"committed before the gap".to_vec()
    );
}

#[test]
fn metadata_survives_primary_crash() {
    let mut c = Cluster::new(Options::default(), 8);
    c.mkdir("/m").unwrap();
    c.write_file("/m/keep", b"metadata durability check").unwrap();

    // Crash the metadata primary. A backup must be promoted with the committed
    // namespace intact.
    c.crash_meta(0);
    assert_eq!(c.read_file("/m/keep").unwrap(), b"metadata durability check");
    let listing = c.list("/m").unwrap();
    assert!(listing.iter().any(|e| e.name == "keep"));
}
