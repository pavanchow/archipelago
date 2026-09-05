//! Length-prefixed serialization helpers built on LEB128 varints.
//!
//! Every message and manifest that crosses the simulated wire is turned into
//! bytes with [`Encoder`] and read back with [`Decoder`]. Keeping this in one
//! place means the wire format has a single source of truth.

use crate::error::{Error, Result};
use crate::hash::Hash;
use crate::varint::{decode_uvarint, encode_uvarint};

/// A growable byte buffer with typed append helpers.
#[derive(Default)]
pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    /// A fresh empty encoder.
    pub fn new() -> Self {
        Encoder { buf: Vec::new() }
    }

    /// Consume the encoder and return the bytes.
    pub fn finish(self) -> Vec<u8> {
        self.buf
    }

    /// Append a single tag byte.
    pub fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    /// Append a varint.
    pub fn put_uvarint(&mut self, v: u64) {
        encode_uvarint(v, &mut self.buf);
    }

    /// Append a length-prefixed byte slice.
    pub fn put_bytes(&mut self, v: &[u8]) {
        self.put_uvarint(v.len() as u64);
        self.buf.extend_from_slice(v);
    }

    /// Append a length-prefixed UTF-8 string.
    pub fn put_str(&mut self, v: &str) {
        self.put_bytes(v.as_bytes());
    }

    /// Append a fixed 32 byte hash.
    pub fn put_hash(&mut self, h: &Hash) {
        self.buf.extend_from_slice(&h.0);
    }
}

/// A cursor that reads values written by [`Encoder`].
pub struct Decoder<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Decoder<'a> {
    /// Wrap a byte slice for reading from the front.
    pub fn new(buf: &'a [u8]) -> Self {
        Decoder { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Read one tag byte.
/// # Errors
///
/// /// Returns [`Error::Decode`] when the buffer is exhausted.
    pub fn get_u8(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            return Err(Error::Decode("eof reading u8".into()));
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    /// Read one varint.
/// # Errors
///
/// /// Returns [`Error::Decode`] when the varint is truncated or exceeds
/// /// 64 bits.
    pub fn get_uvarint(&mut self) -> Result<u64> {
        let (v, used) = decode_uvarint(&self.buf[self.pos..])?;
        self.pos += used;
        Ok(v)
    }

    /// Read a length-prefixed byte slice into an owned vector.
/// # Errors
///
/// /// Returns [`Error::Decode`] when the length prefix is out of range or
/// /// the buffer ends early.
    pub fn get_bytes(&mut self) -> Result<Vec<u8>> {
        // A malformed length prefix can claim nearly 2^64 bytes. Compute the
        // end offset without overflow so bad input is a Decode error, never a
        // panic.
        let len = self.get_uvarint()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Error::Decode("byte length out of range".into()))?;
        if end > self.buf.len() {
            return Err(Error::Decode("eof reading bytes".into()));
        }
        let out = self.buf[self.pos..end].to_vec();
        self.pos = end;
        Ok(out)
    }

    /// Read a length-prefixed UTF-8 string.
/// # Errors
///
/// /// Returns [`Error::Decode`] when the bytes are truncated or not UTF-8.
    pub fn get_str(&mut self) -> Result<String> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|_| Error::Decode("bad utf8".into()))
    }

    /// Read a fixed 32 byte hash.
/// # Errors
///
/// /// Returns [`Error::Decode`] when fewer than 32 bytes remain.
    pub fn get_hash(&mut self) -> Result<Hash> {
        if self.pos + 32 > self.buf.len() {
            return Err(Error::Decode("eof reading hash".into()));
        }
        let mut h = [0u8; 32];
        h.copy_from_slice(&self.buf[self.pos..self.pos + 32]);
        self.pos += 32;
        Ok(Hash(h))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    #[test]
    fn round_trip_mixed() {
        let h = sha256(b"chunk");
        let mut e = Encoder::new();
        e.put_u8(9);
        e.put_uvarint(123_456);
        e.put_str("/a/b/c");
        e.put_bytes(&[1, 2, 3, 4, 5]);
        e.put_hash(&h);
        let bytes = e.finish();

        let mut d = Decoder::new(&bytes);
        assert_eq!(d.get_u8().unwrap(), 9);
        assert_eq!(d.get_uvarint().unwrap(), 123_456);
        assert_eq!(d.get_str().unwrap(), "/a/b/c");
        assert_eq!(d.get_bytes().unwrap(), vec![1, 2, 3, 4, 5]);
        assert_eq!(d.get_hash().unwrap(), h);
        assert_eq!(d.remaining(), 0);
    }

    #[test]
    fn truncated_bytes_error() {
        let mut e = Encoder::new();
        e.put_bytes(&[1, 2, 3]);
        let mut bytes = e.finish();
        bytes.pop();
        let mut d = Decoder::new(&bytes);
        assert!(d.get_bytes().is_err());
    }

    #[test]
    fn hostile_length_is_error_not_panic() {
        // Ten byte varint whose top group claims bits above 63: the length
        // decodes to u64::MAX. The decoder must answer with an error rather
        // than overflow.
        let hostile = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f];
        assert!(decode_uvarint(&hostile).is_err());
        let mut d = Decoder::new(&hostile);
        assert!(d.get_bytes().is_err());
        assert!(d.get_str().is_err());
    }

    #[test]
    fn random_malformed_never_panics() {
        // xorshift-driven malformed buffers. Every decode must return, either
        // Ok or Err, and must never panic.
        let mut state = 0xdead_beefu64;
        for _ in 0..5000 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let len = (state % 64) as usize;
            let buf: Vec<u8> = (0..len)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    (state >> 24) as u8
                })
                .collect();
            let mut d = Decoder::new(&buf);
            let _ = d.get_u8();
            let _ = d.get_uvarint();
            let _ = d.get_bytes();
            let _ = d.get_str();
            let _ = d.get_hash();
        }
    }
}
