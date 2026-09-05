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
    pub fn get_u8(&mut self) -> Result<u8> {
        if self.pos >= self.buf.len() {
            return Err(Error::Decode("eof reading u8".into()));
        }
        let v = self.buf[self.pos];
        self.pos += 1;
        Ok(v)
    }

    /// Read one varint.
    pub fn get_uvarint(&mut self) -> Result<u64> {
        let (v, used) = decode_uvarint(&self.buf[self.pos..])?;
        self.pos += used;
        Ok(v)
    }

    /// Read a length-prefixed byte slice into an owned vector.
    pub fn get_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.get_uvarint()? as usize;
        if self.pos + len > self.buf.len() {
            return Err(Error::Decode("eof reading bytes".into()));
        }
        let out = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(out)
    }

    /// Read a length-prefixed UTF-8 string.
    pub fn get_str(&mut self) -> Result<String> {
        let bytes = self.get_bytes()?;
        String::from_utf8(bytes).map_err(|_| Error::Decode("bad utf8".into()))
    }

    /// Read a fixed 32 byte hash.
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
        e.put_uvarint(123456);
        e.put_str("/a/b/c");
        e.put_bytes(&[1, 2, 3, 4, 5]);
        e.put_hash(&h);
        let bytes = e.finish();

        let mut d = Decoder::new(&bytes);
        assert_eq!(d.get_u8().unwrap(), 9);
        assert_eq!(d.get_uvarint().unwrap(), 123456);
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
}
