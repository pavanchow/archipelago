//! Crate-wide error type.

use std::fmt;

/// Errors returned by the Archipelago file system and its simulator.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Error {
    /// A path was requested that does not exist.
    NotFound(String),
    /// A path already exists where a new entry was requested.
    AlreadyExists(String),
    /// A path component that was expected to be a directory is not one.
    NotADirectory(String),
    /// A file operation was attempted on a directory path.
    IsADirectory(String),
    /// A directory delete was attempted while the directory still has children.
    DirectoryNotEmpty(String),
    /// A path was malformed (empty, relative, or containing bad components).
    InvalidPath(String),
    /// A write could not gather enough replica acknowledgements.
    WriteQuorumFailed {
        /// Replicas that were required.
        needed: usize,
        /// Replicas that acknowledged in time.
        got: usize,
    },
    /// A chunk could not be read from any live replica.
    ChunkUnavailable(String),
    /// The metadata service could not commit or answer within the deadline.
    MetadataUnavailable,
    /// Reassembled bytes did not match the recorded content hash.
    IntegrityError,
    /// A message could not be decoded off the wire.
    Decode(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound(p) => write!(f, "not found: {p}"),
            Error::AlreadyExists(p) => write!(f, "already exists: {p}"),
            Error::NotADirectory(p) => write!(f, "not a directory: {p}"),
            Error::IsADirectory(p) => write!(f, "is a directory: {p}"),
            Error::DirectoryNotEmpty(p) => write!(f, "directory not empty: {p}"),
            Error::InvalidPath(p) => write!(f, "invalid path: {p}"),
            Error::WriteQuorumFailed { needed, got } => {
                write!(f, "write quorum failed: needed {needed}, got {got}")
            }
            Error::ChunkUnavailable(h) => write!(f, "chunk unavailable: {h}"),
            Error::MetadataUnavailable => write!(f, "metadata service unavailable"),
            Error::IntegrityError => write!(f, "integrity error: content hash mismatch"),
            Error::Decode(m) => write!(f, "decode error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// Crate result alias.
pub type Result<T> = std::result::Result<T, Error>;
