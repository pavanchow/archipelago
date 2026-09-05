//! Gate 4: determinism.
//!
//! The same seed and the same operation script must produce the identical
//! network delivery order and the identical final cluster state. This is what
//! licenses the simulator as a testing tool: a failing run is reproducible
//! exactly from its seed.

use archipelago::{Cluster, Hash, Options};

fn scripted_run(seed: u64) -> (Hash, Hash) {
    let opts = Options {
        chunk_size: 300,
        // Jitter drives reordering. Determinism must hold regardless.
        ..Options::default()
    };
    let mut c = Cluster::new(opts, seed);

    c.mkdir("/a").unwrap();
    c.mkdir("/a/b").unwrap();
    for i in 0..8 {
        let data: Vec<u8> = (0..(i * 250 + 40)).map(|j| (j * 7 + i) as u8).collect();
        c.write_file(&format!("/a/f{i}"), &data).unwrap();
    }
    // Read some back.
    for i in 0..8 {
        let _ = c.read_file(&format!("/a/f{i}")).unwrap();
    }
    // Inject a fault and heal it.
    c.crash_node(2);
    let _ = c.read_file("/a/f3").unwrap();
    c.stabilize();
    c.recover_node(2);
    c.stabilize();
    // Namespace churn.
    c.rename("/a/f0", "/a/b/moved").unwrap();
    c.delete("/a/f1").unwrap();

    (c.state_hash(), c.delivery_digest())
}

#[test]
fn same_seed_same_state_and_delivery() {
    for seed in [1u64, 2, 7, 42, 1000] {
        let a = scripted_run(seed);
        let b = scripted_run(seed);
        assert_eq!(a.0, b.0, "state diverged for seed {seed}");
        assert_eq!(a.1, b.1, "delivery order diverged for seed {seed}");
    }
}

#[test]
fn different_seeds_diverge_in_delivery() {
    // Not a correctness requirement, but a sanity check that the seed actually
    // drives the schedule. Different seeds should reorder deliveries.
    let a = scripted_run(1);
    let b = scripted_run(2);
    assert_ne!(a.1, b.1, "seeds produced identical delivery order");
}
