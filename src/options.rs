//! Cluster configuration.

use crate::erasure::Erasure;
use crate::net::LinkParams;

/// Tunable parameters for a cluster.
#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Maximum size of a chunk in bytes.
    pub chunk_size: usize,
    /// Number of replicas per chunk (R).
    pub replication_factor: usize,
    /// Replica acks required for a write to succeed.
    pub write_quorum: usize,
    /// Valid replicas required for a read to succeed. One means first good copy
    /// wins, with read-repair filling in the stragglers.
    pub read_quorum: usize,
    /// Erasure coding instead of replication. When `Some`, every chunk is
    /// Reed-Solomon encoded into `k + m` shards that are spread over distinct
    /// storage nodes, and a read needs any `k` of them. When `None`, chunks
    /// are replicated `replication_factor` times.
    pub erasure: Option<Erasure>,
    /// Number of storage nodes.
    pub node_count: u32,
    /// Number of metadata nodes.
    pub meta_count: u32,
    /// Metadata replicas required to commit an op (majority is the safe choice).
    pub meta_quorum: usize,
    /// Logical time budget for a single client operation before it gives up.
    pub op_deadline: u64,
    /// Network link behaviour.
    pub link: LinkParams,
}

impl Default for Options {
    /// Defaults: 64 KiB chunks, `R=3` with `write_quorum=2` and `read_quorum=1`.
    ///
    /// `R=3` tolerates the loss of any two replicas. `write_quorum=2` means a write
    /// is durable on a majority of replicas before it is acknowledged, so it
    /// survives one immediate replica loss. `read_quorum=1` with read-repair is
    /// safe because every returned chunk is verified against its content hash,
    /// so a single good copy is provably the right bytes.
    fn default() -> Self {
        Options {
            chunk_size: 64 * 1024,
            replication_factor: 3,
            write_quorum: 2,
            read_quorum: 1,
            erasure: None,
            node_count: 5,
            meta_count: 3,
            meta_quorum: 2,
            op_deadline: 10_000,
            link: LinkParams::default(),
        }
    }
}

impl Options {
    /// A small cluster tuned for fast tests.
    pub fn small() -> Self {
        Options {
            chunk_size: 1024,
            ..Options::default()
        }
    }

    /// A small cluster with erasure coding instead of replication. Every
    /// chunk becomes k data plus m parity shards spread over distinct nodes;
    /// a read needs any k of them.
/// # Panics
///
/// /// Panics when `k` and `m` are outside the bounds accepted by
/// /// [`crate::erasure::Erasure::new`].
    pub fn small_erasure(k: usize, m: usize) -> Self {
        Options {
            chunk_size: 1024,
            replication_factor: 1,
            erasure: Some(Erasure::new(k, m).expect("valid erasure parameters")),
            ..Options::default()
        }
    }
}
