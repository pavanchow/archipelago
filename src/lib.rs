//! Archipelago is a distributed file system that runs as a single deterministic
//! process, with zero external dependencies.
//!
//! Files are split into content-addressed chunks, chunks are replicated across
//! storage nodes chosen by rendezvous hashing, a metadata service holds the
//! namespace, and reads and writes use quorums. The novel part is that the
//! entire cluster, including the network, runs inside one process as a seeded
//! deterministic simulation. The [`net`] module can delay, reorder, drop, and
//! partition messages under a fixed seed, so a distributed system becomes a
//! function of its seed and its operation script. That is what makes it
//! fault-injectable and machine-checkable, in the style of deterministic
//! simulation testing.
//!
//! # Quickstart
//!
//! ```
//! use archipelago::{Cluster, Options};
//!
//! let mut c = Cluster::new(Options::default(), 42);
//! c.mkdir("/data").unwrap();
//! c.write_file("/data/hello", b"hello archipelago").unwrap();
//! assert_eq!(c.read_file("/data/hello").unwrap(), b"hello archipelago");
//!
//! // Lose a replica and the data is still there.
//! c.crash_node(0);
//! assert_eq!(c.read_file("/data/hello").unwrap(), b"hello archipelago");
//! ```

pub mod chunk;
pub mod client;
pub mod cluster;
pub mod encode;
pub mod error;
pub mod hash;
pub mod message;
pub mod metadata;
pub mod net;
pub mod options;
pub mod placement;
pub mod storagenode;
pub mod varint;

pub use chunk::{Chunk, Manifest};
pub use cluster::{Cluster, FileHealth, NodeStatus, Status};
pub use error::{Error, Result};
pub use hash::{sha256, Hash};
pub use message::{DirEntry, NodeId, StatInfo};
pub use options::Options;
