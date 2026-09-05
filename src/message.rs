//! Wire messages exchanged between the client coordinator, the metadata
//! service, and the storage nodes, plus their serialization.
//!
//! Every value here round-trips through [`crate::encode`]. Messages travel the
//! simulated network as bytes and are decoded on delivery, so the format is
//! exercised on every hop.

use crate::chunk::Manifest;
use crate::encode::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::hash::Hash;

/// Identity of a participant in the cluster.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum NodeId {
    /// The client coordinator that drives file operations.
    Client,
    /// A storage node by index.
    Storage(u32),
    /// A metadata node by index.
    Meta(u32),
}

impl NodeId {
    fn encode(&self, e: &mut Encoder) {
        match self {
            NodeId::Client => e.put_u8(0),
            NodeId::Storage(i) => {
                e.put_u8(1);
                e.put_uvarint(u64::from(*i));
            }
            NodeId::Meta(i) => {
                e.put_u8(2);
                e.put_uvarint(u64::from(*i));
            }
        }
    }

    fn decode(d: &mut Decoder<'_>) -> Result<NodeId> {
        match d.get_u8()? {
            0 => Ok(NodeId::Client),
            1 => Ok(NodeId::Storage(d.get_uvarint()? as u32)),
            2 => Ok(NodeId::Meta(d.get_uvarint()? as u32)),
            t => Err(Error::Decode(format!("bad node tag {t}"))),
        }
    }
}

/// A directory listing entry.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DirEntry {
    /// Final path component.
    pub name: String,
    /// Whether the entry is a directory.
    pub is_dir: bool,
    /// File size in bytes (zero for directories).
    pub size: u64,
}

/// Metadata about a single path.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct StatInfo {
    /// Whether the path is a directory.
    pub is_dir: bool,
    /// File size in bytes (zero for directories).
    pub size: u64,
    /// Whole-file content hash (all zero for directories).
    pub content_hash: Hash,
}

/// A mutating metadata operation. These are the entries of the replicated log.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MetaOp {
    /// Create or overwrite a file with the given manifest.
    Put {
        /// Absolute file path.
        path: String,
        /// The file manifest.
        manifest: Manifest,
    },
    /// Create a directory.
    Mkdir {
        /// Absolute directory path.
        path: String,
    },
    /// Delete a file or empty directory.
    Delete {
        /// Absolute path.
        path: String,
    },
    /// Rename a file or directory subtree.
    Rename {
        /// Source path.
        from: String,
        /// Destination path.
        to: String,
    },
}

impl MetaOp {
    fn encode(&self, e: &mut Encoder) {
        match self {
            MetaOp::Put { path, manifest } => {
                e.put_u8(0);
                e.put_str(path);
                manifest.encode(e);
            }
            MetaOp::Mkdir { path } => {
                e.put_u8(1);
                e.put_str(path);
            }
            MetaOp::Delete { path } => {
                e.put_u8(2);
                e.put_str(path);
            }
            MetaOp::Rename { from, to } => {
                e.put_u8(3);
                e.put_str(from);
                e.put_str(to);
            }
        }
    }

    fn decode(d: &mut Decoder<'_>) -> Result<MetaOp> {
        match d.get_u8()? {
            0 => Ok(MetaOp::Put {
                path: d.get_str()?,
                manifest: Manifest::decode(d)?,
            }),
            1 => Ok(MetaOp::Mkdir { path: d.get_str()? }),
            2 => Ok(MetaOp::Delete { path: d.get_str()? }),
            3 => Ok(MetaOp::Rename {
                from: d.get_str()?,
                to: d.get_str()?,
            }),
            t => Err(Error::Decode(format!("bad metaop tag {t}"))),
        }
    }
}

/// A read-only metadata query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Query {
    /// Fetch the manifest for a file path.
    Get {
        /// Absolute file path.
        path: String,
    },
    /// List the immediate children of a directory.
    List {
        /// Absolute directory path.
        path: String,
    },
    /// Stat a single path.
    Stat {
        /// Absolute path.
        path: String,
    },
}

impl Query {
    fn encode(&self, e: &mut Encoder) {
        match self {
            Query::Get { path } => {
                e.put_u8(0);
                e.put_str(path);
            }
            Query::List { path } => {
                e.put_u8(1);
                e.put_str(path);
            }
            Query::Stat { path } => {
                e.put_u8(2);
                e.put_str(path);
            }
        }
    }

    fn decode(d: &mut Decoder<'_>) -> Result<Query> {
        match d.get_u8()? {
            0 => Ok(Query::Get { path: d.get_str()? }),
            1 => Ok(Query::List { path: d.get_str()? }),
            2 => Ok(Query::Stat { path: d.get_str()? }),
            t => Err(Error::Decode(format!("bad query tag {t}"))),
        }
    }
}

/// The outcome of a mutating metadata operation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum MetaResult {
    /// The op committed.
    Ok,
    /// The op was rejected. The code maps to an [`Error`] via [`meta_err`].
    Err(u8),
}

/// Result of a metadata query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum QueryResult {
    /// Manifest for [`Query::Get`], `None` when the file is missing.
    Manifest(Option<Manifest>),
    /// Listing for [`Query::List`].
    Listing(Vec<DirEntry>),
    /// Stat for [`Query::Stat`], `None` when the path is missing.
    Stat(Option<StatInfo>),
    /// The query was rejected (for example listing a non-directory).
    Err(u8),
}

/// Metadata error codes carried on the wire.
pub mod code {
    /// Path not found.
    pub const NOT_FOUND: u8 = 1;
    /// Path already exists.
    pub const ALREADY_EXISTS: u8 = 2;
    /// Expected a directory.
    pub const NOT_A_DIRECTORY: u8 = 3;
    /// Expected a file.
    pub const IS_A_DIRECTORY: u8 = 4;
    /// Directory still has children.
    pub const NOT_EMPTY: u8 = 5;
    /// Malformed path.
    pub const INVALID_PATH: u8 = 6;
}

/// Translate a metadata error code into an [`Error`], attaching `path`.
pub fn meta_err(c: u8, path: &str) -> Error {
    match c {
        code::NOT_FOUND => Error::NotFound(path.into()),
        code::ALREADY_EXISTS => Error::AlreadyExists(path.into()),
        code::NOT_A_DIRECTORY => Error::NotADirectory(path.into()),
        code::IS_A_DIRECTORY => Error::IsADirectory(path.into()),
        code::NOT_EMPTY => Error::DirectoryNotEmpty(path.into()),
        _ => Error::InvalidPath(path.into()),
    }
}

/// A message on the simulated wire.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Message {
    /// Coordinator asks a storage node to persist a chunk.
    StoreChunk {
        /// Chunk id.
        id: Hash,
        /// Chunk bytes.
        data: Vec<u8>,
    },
    /// Storage node confirms a chunk is durable.
    StoreAck {
        /// Chunk id.
        id: Hash,
    },
    /// Coordinator asks a storage node for a chunk.
    FetchChunk {
        /// Chunk id.
        id: Hash,
    },
    /// Storage node returns chunk bytes, or `None` if it does not hold it.
    ChunkData {
        /// Chunk id.
        id: Hash,
        /// Chunk bytes if held.
        data: Option<Vec<u8>>,
    },
    /// Metadata asks a source node to copy a chunk to `dest`.
    ReplicateOrder {
        /// Chunk id.
        id: Hash,
        /// Destination storage node index.
        dest: u32,
    },
    /// A source node ships chunk bytes to a destination node.
    Replicate {
        /// Chunk id.
        id: Hash,
        /// Chunk bytes.
        data: Vec<u8>,
    },
    /// A storage node advertises which chunks it currently holds.
    Heartbeat {
        /// Reporting storage node index.
        node: u32,
        /// Chunk ids held.
        chunks: Vec<Hash>,
    },
    /// Client submits a mutating metadata op to the primary.
    MetaOpMsg {
        /// Correlation id.
        req_id: u64,
        /// The operation.
        op: MetaOp,
    },
    /// Primary replicates a committed-order op to a backup.
    MetaReplicate {
        /// Log sequence number.
        seq: u64,
        /// The operation.
        op: MetaOp,
    },
    /// Backup acknowledges a replicated op.
    MetaReplicateAck {
        /// Log sequence number.
        seq: u64,
    },
    /// Primary reports the outcome of a metadata op to the client.
    MetaCommitted {
        /// Correlation id.
        req_id: u64,
        /// Outcome.
        result: MetaResult,
    },
    /// Client submits a read-only metadata query to the primary.
    MetaQueryMsg {
        /// Correlation id.
        req_id: u64,
        /// The query.
        q: Query,
    },
    /// Primary answers a metadata query.
    MetaQueryResp {
        /// Correlation id.
        req_id: u64,
        /// The answer.
        result: QueryResult,
    },
}

impl Message {
    /// A compact tag identifying the message kind, used in delivery logs.
    pub fn tag(&self) -> u8 {
        match self {
            Message::StoreChunk { .. } => 1,
            Message::StoreAck { .. } => 2,
            Message::FetchChunk { .. } => 3,
            Message::ChunkData { .. } => 4,
            Message::ReplicateOrder { .. } => 5,
            Message::Replicate { .. } => 6,
            Message::Heartbeat { .. } => 7,
            Message::MetaOpMsg { .. } => 8,
            Message::MetaReplicate { .. } => 9,
            Message::MetaReplicateAck { .. } => 10,
            Message::MetaCommitted { .. } => 11,
            Message::MetaQueryMsg { .. } => 12,
            Message::MetaQueryResp { .. } => 13,
        }
    }

    /// Serialize to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut e = Encoder::new();
        e.put_u8(self.tag());
        match self {
            Message::StoreChunk { id, data } => {
                e.put_hash(id);
                e.put_bytes(data);
            }
            Message::StoreAck { id } => e.put_hash(id),
            Message::FetchChunk { id } => e.put_hash(id),
            Message::ChunkData { id, data } => {
                e.put_hash(id);
                match data {
                    Some(d) => {
                        e.put_u8(1);
                        e.put_bytes(d);
                    }
                    None => e.put_u8(0),
                }
            }
            Message::ReplicateOrder { id, dest } => {
                e.put_hash(id);
                e.put_uvarint(u64::from(*dest));
            }
            Message::Replicate { id, data } => {
                e.put_hash(id);
                e.put_bytes(data);
            }
            Message::Heartbeat { node, chunks } => {
                e.put_uvarint(u64::from(*node));
                e.put_uvarint(chunks.len() as u64);
                for c in chunks {
                    e.put_hash(c);
                }
            }
            Message::MetaOpMsg { req_id, op } => {
                e.put_uvarint(*req_id);
                op.encode(&mut e);
            }
            Message::MetaReplicate { seq, op } => {
                e.put_uvarint(*seq);
                op.encode(&mut e);
            }
            Message::MetaReplicateAck { seq } => e.put_uvarint(*seq),
            Message::MetaCommitted { req_id, result } => {
                e.put_uvarint(*req_id);
                encode_meta_result(result, &mut e);
            }
            Message::MetaQueryMsg { req_id, q } => {
                e.put_uvarint(*req_id);
                q.encode(&mut e);
            }
            Message::MetaQueryResp { req_id, result } => {
                e.put_uvarint(*req_id);
                encode_query_result(result, &mut e);
            }
        }
        e.finish()
    }

    /// Deserialize from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Message> {
        let mut d = Decoder::new(bytes);
        let tag = d.get_u8()?;
        match tag {
            1 => Ok(Message::StoreChunk {
                id: d.get_hash()?,
                data: d.get_bytes()?,
            }),
            2 => Ok(Message::StoreAck { id: d.get_hash()? }),
            3 => Ok(Message::FetchChunk { id: d.get_hash()? }),
            4 => {
                let id = d.get_hash()?;
                let data = if d.get_u8()? == 1 {
                    Some(d.get_bytes()?)
                } else {
                    None
                };
                Ok(Message::ChunkData { id, data })
            }
            5 => Ok(Message::ReplicateOrder {
                id: d.get_hash()?,
                dest: d.get_uvarint()? as u32,
            }),
            6 => Ok(Message::Replicate {
                id: d.get_hash()?,
                data: d.get_bytes()?,
            }),
            7 => {
                let node = d.get_uvarint()? as u32;
                let n = d.get_uvarint()?;
                // Each chunk is a 32 byte hash, so a count larger than the
                // remaining bytes can hold is malformed. Bounding it keeps a
                // hostile count from requesting an absurd allocation.
                if n > (d.remaining() / 32) as u64 {
                    return Err(Error::Decode("heartbeat chunk count out of range".into()));
                }
                let n = n as usize;
                let mut chunks = Vec::with_capacity(n);
                for _ in 0..n {
                    chunks.push(d.get_hash()?);
                }
                Ok(Message::Heartbeat { node, chunks })
            }
            8 => Ok(Message::MetaOpMsg {
                req_id: d.get_uvarint()?,
                op: MetaOp::decode(&mut d)?,
            }),
            9 => Ok(Message::MetaReplicate {
                seq: d.get_uvarint()?,
                op: MetaOp::decode(&mut d)?,
            }),
            10 => Ok(Message::MetaReplicateAck {
                seq: d.get_uvarint()?,
            }),
            11 => Ok(Message::MetaCommitted {
                req_id: d.get_uvarint()?,
                result: decode_meta_result(&mut d)?,
            }),
            12 => Ok(Message::MetaQueryMsg {
                req_id: d.get_uvarint()?,
                q: Query::decode(&mut d)?,
            }),
            13 => Ok(Message::MetaQueryResp {
                req_id: d.get_uvarint()?,
                result: decode_query_result(&mut d)?,
            }),
            t => Err(Error::Decode(format!("bad message tag {t}"))),
        }
    }
}

fn encode_meta_result(r: &MetaResult, e: &mut Encoder) {
    match r {
        MetaResult::Ok => e.put_u8(0),
        MetaResult::Err(c) => {
            e.put_u8(1);
            e.put_u8(*c);
        }
    }
}

fn decode_meta_result(d: &mut Decoder<'_>) -> Result<MetaResult> {
    match d.get_u8()? {
        0 => Ok(MetaResult::Ok),
        1 => Ok(MetaResult::Err(d.get_u8()?)),
        t => Err(Error::Decode(format!("bad meta result tag {t}"))),
    }
}

fn encode_query_result(r: &QueryResult, e: &mut Encoder) {
    match r {
        QueryResult::Manifest(m) => {
            e.put_u8(0);
            match m {
                Some(m) => {
                    e.put_u8(1);
                    m.encode(e);
                }
                None => e.put_u8(0),
            }
        }
        QueryResult::Listing(entries) => {
            e.put_u8(1);
            e.put_uvarint(entries.len() as u64);
            for en in entries {
                e.put_str(&en.name);
                e.put_u8(u8::from(en.is_dir));
                e.put_uvarint(en.size);
            }
        }
        QueryResult::Stat(s) => {
            e.put_u8(2);
            match s {
                Some(s) => {
                    e.put_u8(1);
                    e.put_u8(u8::from(s.is_dir));
                    e.put_uvarint(s.size);
                    e.put_hash(&s.content_hash);
                }
                None => e.put_u8(0),
            }
        }
        QueryResult::Err(c) => {
            e.put_u8(3);
            e.put_u8(*c);
        }
    }
}

fn decode_query_result(d: &mut Decoder<'_>) -> Result<QueryResult> {
    match d.get_u8()? {
        0 => {
            let m = if d.get_u8()? == 1 {
                Some(Manifest::decode(d)?)
            } else {
                None
            };
            Ok(QueryResult::Manifest(m))
        }
        1 => {
            let n = d.get_uvarint()?;
            // Each entry needs at least three bytes on the wire (name length,
            // the is_dir byte, and the size varint), so a count larger than
            // half the remaining bytes is malformed. Bounding it keeps a
            // hostile count from requesting an absurd allocation.
            if n > (d.remaining() / 2) as u64 {
                return Err(Error::Decode("listing entry count out of range".into()));
            }
            let n = n as usize;
            let mut entries = Vec::with_capacity(n);
            for _ in 0..n {
                let name = d.get_str()?;
                let is_dir = d.get_u8()? != 0;
                let size = d.get_uvarint()?;
                entries.push(DirEntry { name, is_dir, size });
            }
            Ok(QueryResult::Listing(entries))
        }
        2 => {
            let s = if d.get_u8()? == 1 {
                Some(StatInfo {
                    is_dir: d.get_u8()? != 0,
                    size: d.get_uvarint()?,
                    content_hash: d.get_hash()?,
                })
            } else {
                None
            };
            Ok(QueryResult::Stat(s))
        }
        3 => Ok(QueryResult::Err(d.get_u8()?)),
        t => Err(Error::Decode(format!("bad query result tag {t}"))),
    }
}

/// Serialize a routed envelope (from, to, message) to bytes.
pub fn encode_envelope(from: NodeId, to: NodeId, msg: &Message) -> Vec<u8> {
    let mut e = Encoder::new();
    from.encode(&mut e);
    to.encode(&mut e);
    let body = msg.encode();
    e.put_bytes(&body);
    e.finish()
}

/// Deserialize a routed envelope produced by [`encode_envelope`].
pub fn decode_envelope(bytes: &[u8]) -> Result<(NodeId, NodeId, Message)> {
    let mut d = Decoder::new(bytes);
    let from = NodeId::decode(&mut d)?;
    let to = NodeId::decode(&mut d)?;
    let body = d.get_bytes()?;
    let msg = Message::decode(&body)?;
    Ok((from, to, msg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    fn m(size: u64) -> Manifest {
        Manifest {
            size,
            content_hash: sha256(b"file"),
            chunks: vec![sha256(b"a"), sha256(b"b")],
            erasure: None,
        }
    }

    #[test]
    fn all_messages_round_trip() {
        let msgs = vec![
            Message::StoreChunk {
                id: sha256(b"x"),
                data: vec![1, 2, 3],
            },
            Message::StoreAck { id: sha256(b"x") },
            Message::FetchChunk { id: sha256(b"x") },
            Message::ChunkData {
                id: sha256(b"x"),
                data: Some(vec![9, 9]),
            },
            Message::ChunkData {
                id: sha256(b"x"),
                data: None,
            },
            Message::ReplicateOrder {
                id: sha256(b"x"),
                dest: 4,
            },
            Message::Replicate {
                id: sha256(b"x"),
                data: vec![7],
            },
            Message::Heartbeat {
                node: 2,
                chunks: vec![sha256(b"a"), sha256(b"b")],
            },
            Message::MetaOpMsg {
                req_id: 5,
                op: MetaOp::Put {
                    path: "/a".into(),
                    manifest: m(10),
                },
            },
            Message::MetaReplicate {
                seq: 7,
                op: MetaOp::Rename {
                    from: "/a".into(),
                    to: "/b".into(),
                },
            },
            Message::MetaReplicateAck { seq: 7 },
            Message::MetaCommitted {
                req_id: 5,
                result: MetaResult::Err(code::ALREADY_EXISTS),
            },
            Message::MetaQueryMsg {
                req_id: 6,
                q: Query::List { path: "/".into() },
            },
            Message::MetaQueryResp {
                req_id: 6,
                result: QueryResult::Listing(vec![DirEntry {
                    name: "a".into(),
                    is_dir: false,
                    size: 10,
                }]),
            },
            Message::MetaQueryResp {
                req_id: 6,
                result: QueryResult::Stat(Some(StatInfo {
                    is_dir: true,
                    size: 0,
                    content_hash: sha256(b""),
                })),
            },
        ];
        for msg in msgs {
            let bytes = msg.encode();
            let back = Message::decode(&bytes).unwrap();
            assert_eq!(back, msg);
        }
    }

    #[test]
    fn envelope_round_trip() {
        let msg = Message::StoreAck { id: sha256(b"z") };
        let bytes = encode_envelope(NodeId::Client, NodeId::Storage(3), &msg);
        let (from, to, back) = decode_envelope(&bytes).unwrap();
        assert_eq!(from, NodeId::Client);
        assert_eq!(to, NodeId::Storage(3));
        assert_eq!(back, msg);
    }

    #[test]
    fn hostile_counts_are_errors_not_panics() {
        // A heartbeat whose chunk count varint decodes to u64::MAX must be
        // rejected, not turned into an absurd allocation.
        let mut bytes = vec![7u8];
        bytes.push(0);
        bytes.extend_from_slice(&[0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        assert!(Message::decode(&bytes).is_err());

        // A listing whose entry count varint decodes to u64::MAX as well.
        let mut bytes = vec![13u8];
        bytes.extend_from_slice(&[0u8]); // req_id
        bytes.push(1); // QueryResult::Listing
        bytes.extend_from_slice(&[0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        assert!(Message::decode(&bytes).is_err());
    }

    #[test]
    fn random_envelopes_never_panic() {
        // xorshift-driven malformed envelopes. Every decode must return an
        // outcome rather than panic, whatever the bytes contain.
        let mut state = 0xfeed_faceu64;
        for _ in 0..5000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 96) as usize;
            let buf: Vec<u8> = (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 16) as u8
                })
                .collect();
            let _ = decode_envelope(&buf);
        }
    }
}
