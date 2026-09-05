//! LEB128 unsigned varint encoding.
//!
//! Used by [`crate::encode`] to length-prefix byte slices and to write compact
//! integers into messages and manifests.

use crate::error::{Error, Result};

/// Append the LEB128 encoding of `value` to `out`.
pub fn encode_uvarint(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Decode a LEB128 varint from the front of `buf`.
///
/// Returns the value and the number of bytes consumed.
pub fn decode_uvarint(buf: &[u8]) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    for (i, &byte) in buf.iter().enumerate() {
        if shift >= 64 {
            return Err(Error::Decode("varint too long".into()));
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(Error::Decode("varint truncated".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_edge_values() {
        for v in [0u64, 1, 127, 128, 255, 300, 16384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            encode_uvarint(v, &mut buf);
            let (got, used) = decode_uvarint(&buf).unwrap();
            assert_eq!(got, v);
            assert_eq!(used, buf.len());
        }
    }

    #[test]
    fn round_trip_sequential() {
        let mut buf = Vec::new();
        let values: Vec<u64> = (0..5000).map(|i| i * i * 7 + i).collect();
        for &v in &values {
            encode_uvarint(v, &mut buf);
        }
        let mut pos = 0;
        for &v in &values {
            let (got, used) = decode_uvarint(&buf[pos..]).unwrap();
            assert_eq!(got, v);
            pos += used;
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn truncated_is_error() {
        assert!(decode_uvarint(&[0x80]).is_err());
        assert!(decode_uvarint(&[]).is_err());
    }
}
