//! Fixed-size chunking and the file manifest.
//!
//! A file's bytes are split into fixed-size chunks. Each chunk is
//! content-addressed by its SHA-256. The ordered list of chunk hashes plus the
//! total size and the whole-file content hash is the file's [`Manifest`].
//! Reassembly concatenates the chunk bytes in manifest order and the result is
//! verified against the content hash.

use crate::encode::{Decoder, Encoder};
use crate::error::{Error, Result};
use crate::hash::{sha256, Hash};

/// A content-addressed block of bytes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Chunk {
    /// SHA-256 of `data`.
    pub id: Hash,
    /// The raw bytes of this chunk.
    pub data: Vec<u8>,
}

/// The recipe for reconstructing a file from chunks.
///
/// In replication mode `chunks` holds one content address per chunk position.
/// In erasure mode `erasure` is `Some((k, m))` and `chunks` holds the flat
/// list of shard content addresses, `k + m` per chunk group in order, so
/// chunk group g occupies positions `g * (k + m) .. (g + 1) * (k + m)`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Manifest {
    /// Total file size in bytes.
    pub size: u64,
    /// SHA-256 of the entire file, checked after reassembly.
    pub content_hash: Hash,
    /// Chunk ids in file order. May repeat when chunks are identical.
    pub chunks: Vec<Hash>,
    /// Erasure parameters (k, m) when the file is erasure coded.
    pub erasure: Option<(u8, u8)>,
}

impl Manifest {
    /// Serialize to bytes.
    pub fn encode(&self, e: &mut Encoder) {
        e.put_uvarint(self.size);
        e.put_hash(&self.content_hash);
        e.put_uvarint(self.chunks.len() as u64);
        for c in &self.chunks {
            e.put_hash(c);
        }
        match self.erasure {
            None => e.put_u8(0),
            Some((k, m)) => {
                e.put_u8(1);
                e.put_u8(k);
                e.put_u8(m);
            }
        }
    }

    /// Deserialize from bytes.
/// # Errors
///
/// /// Returns [`Error::Decode`] when any field is truncated, the chunk
/// /// count is out of range, or the erasure tag is malformed.
    pub fn decode(d: &mut Decoder<'_>) -> Result<Manifest> {
        let size = d.get_uvarint()?;
        let content_hash = d.get_hash()?;
        let n = d.get_uvarint()?;
        // Every chunk is a 32 byte hash, so a count larger than the remaining
        // bytes can hold is malformed. Bounding it here keeps a hostile count
        // from requesting an absurd allocation.
        if n > (d.remaining() / 32) as u64 {
            return Err(Error::Decode("manifest chunk count out of range".into()));
        }
        let n = n as usize;
        let mut chunks = Vec::with_capacity(n);
        for _ in 0..n {
            chunks.push(d.get_hash()?);
        }
        let erasure = match d.get_u8()? {
            0 => None,
            1 => {
                let k = d.get_u8()?;
                let m = d.get_u8()?;
                Some((k, m))
            }
            t => return Err(Error::Decode(format!("bad manifest erasure tag {t}"))),
        };
        Ok(Manifest {
            size,
            content_hash,
            chunks,
            erasure,
        })
    }
}

/// Split `data` into chunks of at most `chunk_size` bytes and build its manifest.
///
/// The returned chunk vector follows file order and may contain duplicates when
/// two positions hold identical bytes. Callers that store chunks should dedupe
/// by [`Chunk::id`].
/// # Panics
/// ///
/// /// Panics when `chunk_size` is zero, which cannot produce a layout.
pub fn chunk_bytes(data: &[u8], chunk_size: usize) -> (Vec<Chunk>, Manifest) {
    assert!(chunk_size > 0, "chunk_size must be positive");
    let mut chunks = Vec::new();
    let mut ids = Vec::new();
    for window in data.chunks(chunk_size) {
        let id = sha256(window);
        ids.push(id);
        chunks.push(Chunk {
            id,
            data: window.to_vec(),
        });
    }
    let manifest = Manifest {
        size: data.len() as u64,
        content_hash: sha256(data),
        chunks: ids,
        erasure: None,
    };
    (chunks, manifest)
}

/// Concatenate ordered chunk payloads back into the original byte stream.
pub fn reassemble(parts: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = parts.iter().map(Vec::len).sum();
    let mut out = Vec::with_capacity(total);
    for p in parts {
        out.extend_from_slice(p);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(data: &[u8], chunk_size: usize) {
        let (chunks, manifest) = chunk_bytes(data, chunk_size);
        assert_eq!(manifest.size as usize, data.len());
        assert_eq!(manifest.content_hash, sha256(data));
        assert_eq!(manifest.chunks.len(), chunks.len());
        for c in &chunks {
            assert_eq!(c.id, sha256(&c.data));
            assert!(c.data.len() <= chunk_size);
        }
        let parts: Vec<Vec<u8>> = chunks.iter().map(|c| c.data.clone()).collect();
        let back = reassemble(&parts);
        assert_eq!(back, data);
        assert_eq!(sha256(&back), manifest.content_hash);
    }

    #[test]
    fn sizes_including_edges() {
        let cs = 64;
        round_trip(b"", cs);
        round_trip(&[7u8], cs);
        round_trip(&vec![3u8; cs], cs);
        round_trip(&vec![9u8; cs - 1], cs);
        round_trip(&vec![9u8; cs + 1], cs);
        round_trip(&vec![9u8; cs * 3], cs);
        round_trip(&vec![9u8; cs * 3 + 5], cs);
    }

    #[test]
    fn pseudo_random_sizes() {
        let mut state = 0x1234_5678u64;
        for _ in 0..40 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 5000) as usize;
            let data: Vec<u8> = (0..len).map(|i| (i as u64 ^ state) as u8).collect();
            round_trip(&data, 100);
        }
    }

    #[test]
    fn manifest_serialization() {
        let (_c, m) = chunk_bytes(&vec![1u8; 250], 64);
        let mut e = Encoder::new();
        m.encode(&mut e);
        let bytes = e.finish();
        let mut d = Decoder::new(&bytes);
        let back = Manifest::decode(&mut d).unwrap();
        assert_eq!(back, m);
    }

    #[test]
    fn identical_chunks_share_id() {
        let (_c, m) = chunk_bytes(&[0u8; 64 * 3], 64);
        assert_eq!(m.chunks[0], m.chunks[1]);
        assert_eq!(m.chunks[1], m.chunks[2]);
    }

    #[test]
    fn hostile_chunk_count_is_error_not_panic() {
        // A manifest header whose chunk count varint decodes to u64::MAX must
        // be rejected, not turned into an absurd allocation.
        let mut bytes = vec![0u8]; // size
        bytes.extend_from_slice(&[0u8; 32]); // content hash
        bytes.extend_from_slice(&[0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
        let mut d = Decoder::new(&bytes);
        assert!(Manifest::decode(&mut d).is_err());

        // A truncated but sane-looking count is also an error.
        let (c, m) = chunk_bytes(&[1u8; 100], 64);
        let mut e = Encoder::new();
        m.encode(&mut e);
        let mut bytes = e.finish();
        bytes.truncate(bytes.len() - 1);
        let mut d = Decoder::new(&bytes);
        assert!(Manifest::decode(&mut d).is_err());
        let _ = c;
    }
}
