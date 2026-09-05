//! Reed-Solomon erasure coding over GF(2^8) as an alternative chunk
//! protection mode.
//!
//! A chunk is split into `k` equal-length data shards and `m` parity shards
//! are computed, for `k + m` shards in total. Any `k` of the `k + m` shards
//! reconstruct the chunk, so up to `m` shard losses are tolerated, where
//! replication would have needed every replica but one.
//!
//! The arithmetic is GF(2^8) with the primitive polynomial 0x11d and
//! generator 2, driven by log and antilog tables. The parity rows form a
//! Cauchy matrix, which has the MDS property: every square submatrix of the
//! full encoding matrix (identity rows for the data shards, Cauchy rows for
//! the parity shards) is invertible, so any k shards suffice, whatever their
//! positions. Inversion is Gaussian elimination in the field.
//!
//! Shards are content-addressed like everything else in the cluster, so a
//! corrupt or misplaced shard is detected by its hash mismatch at fetch time
//! and degrades to a missing shard, which the decoder tolerates up to m of.

use crate::error::{Error, Result};
use std::sync::OnceLock;

const PRIMITIVE: u16 = 0x11d;

struct Tables {
    exp: [u8; 512],
    log: [u8; 256],
}

fn tables() -> &'static Tables {
    static TABLES: OnceLock<Tables> = OnceLock::new();
    TABLES.get_or_init(|| {
        let mut exp = [0u8; 512];
        let mut log = [0u8; 256];
        let mut x: u16 = 1;
        for i in 0..255u16 {
            exp[i as usize] = x as u8;
            log[x as usize] = i as u8;
            // Multiply by the generator in the field.
            x <<= 1;
            if x & 0x100 != 0 {
                x ^= PRIMITIVE;
            }
        }
        // Wrap the table around so LOG[a] + LOG[b] can index without a mod.
        for i in 0..255usize {
            exp[i + 255] = exp[i];
        }
        Tables { exp, log }
    })
}

/// Multiply two field elements.
pub fn gf_mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        0
    } else {
        let t = tables();
        t.exp[t.log[a as usize] as usize + t.log[b as usize] as usize]
    }
}

/// Inverse of a nonzero field element.
pub fn gf_inv(a: u8) -> u8 {
    debug_assert!(a != 0, "zero has no inverse");
    let t = tables();
    t.exp[255 - t.log[a as usize] as usize]
}

/// Divide by a nonzero field element.
pub fn gf_div(a: u8, b: u8) -> u8 {
    debug_assert!(b != 0, "division by zero");
    if a == 0 {
        0
    } else {
        gf_mul(a, gf_inv(b))
    }
}

/// The erasure coding scheme for a fixed k and m.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Erasure {
    /// Number of data shards per chunk.
    pub k: usize,
    /// Number of parity shards per chunk.
    pub m: usize,
}

impl Erasure {
    /// Build a scheme. Both parts must be at least one and their sum must
    /// stay within what a Cauchy matrix over GF(2^8) can span (255) and
    /// within a sane shard count.
    pub fn new(k: usize, m: usize) -> Result<Self> {
        if k == 0 || m == 0 {
            return Err(Error::InvalidPath(format!(
                "erasure needs k and m of at least one, got k={k} m={m}"
            )));
        }
        if k + m > 64 {
            return Err(Error::InvalidPath(format!(
                "erasure k+m={}; the scheme is capped at 64 shards",
                k + m
            )));
        }
        Ok(Erasure { k, m })
    }

    /// Total shard count per chunk.
    pub fn total(&self) -> usize {
        self.k + self.m
    }

    /// The Cauchy encoding row for parity shard `i`: entry j is
    /// `1 / (x_i xor y_j)` with x over the top half and y over the bottom
    /// half of disjoint ranges, so no entry is zero and every k rows of the
    /// combined matrix are independent.
    fn parity_row(&self, i: usize) -> Vec<u8> {
        let xi = (self.k + i) as u8;
        (0..self.k)
            .map(|j| {
                let yj = j as u8;
                gf_inv(xi ^ yj)
            })
            .collect()
    }

    /// The full encoding row for shard position `pos`: the unit row for the
    /// systematic data positions, the Cauchy row for the parity positions.
    fn encoding_row(&self, pos: usize) -> Vec<u8> {
        if pos < self.k {
            let mut row = vec![0u8; self.k];
            row[pos] = 1;
            row
        } else {
            self.parity_row(pos - self.k)
        }
    }

    /// Shard length for a chunk of `chunk_len` bytes.
    pub fn shard_len(&self, chunk_len: usize) -> usize {
        chunk_len.div_ceil(self.k)
    }

    /// Encode a chunk into `k + m` shards. Data shards are the chunk padded
    /// to a multiple of the shard length; parity shards are the Cauchy
    /// combinations.
    pub fn encode(&self, chunk: &[u8]) -> Vec<Vec<u8>> {
        let sl = self.shard_len(chunk.len()).max(1);
        let mut data: Vec<Vec<u8>> = (0..self.k)
            .map(|i| {
                let start = i * sl;
                let end = (start + sl).min(chunk.len());
                let mut shard = vec![0u8; sl];
                if start < chunk.len() {
                    shard[..end - start].copy_from_slice(&chunk[start..end]);
                }
                shard
            })
            .collect();
        for i in 0..self.m {
            let row = self.parity_row(i);
            let mut parity = vec![0u8; sl];
            for (j, coef) in row.iter().enumerate() {
                if *coef == 0 {
                    continue;
                }
                let dj = &data[j];
                for (b, p) in parity.iter_mut().enumerate() {
                    *p ^= gf_mul(*coef, dj[b]);
                }
            }
            data.push(parity);
        }
        data
    }

    /// Re-encode a single shard position from the k data shards. Used by
    /// read repair to rebuild a lost shard from survivors.
    pub fn encode_position(&self, data_shards: &[Vec<u8>], pos: usize) -> Vec<u8> {
        let row = self.encoding_row(pos);
        let sl = data_shards.first().map(|d| d.len()).unwrap_or(0);
        let mut out = vec![0u8; sl];
        for (j, coef) in row.iter().enumerate() {
            if *coef == 0 {
                continue;
            }
            for (b, o) in out.iter_mut().enumerate() {
                *o ^= gf_mul(*coef, data_shards[j][b]);
            }
        }
        out
    }

    /// Invert a k x k matrix over GF(2^8) by Gaussian elimination.
    fn invert(matrix: &[Vec<u8>], k: usize) -> Option<Vec<Vec<u8>>> {
        let mut a: Vec<Vec<u8>> = matrix.to_vec();
        let mut inv_m: Vec<Vec<u8>> = (0..k)
            .map(|i| {
                let mut row = vec![0u8; k];
                row[i] = 1;
                row
            })
            .collect();
        for col in 0..k {
            // Find a pivot with a nonzero entry in this column.
            let pivot = (col..k).find(|&r| a[r][col] != 0)?;
            a.swap(pivot, col);
            inv_m.swap(pivot, col);
            let piv = a[col][col];
            let scale = gf_inv(piv);
            for c in 0..k {
                a[col][c] = gf_mul(a[col][c], scale);
                inv_m[col][c] = gf_mul(inv_m[col][c], scale);
            }
            for r in 0..k {
                if r == col || a[r][col] == 0 {
                    continue;
                }
                let factor = a[r][col];
                for c in 0..k {
                    a[r][c] ^= gf_mul(factor, a[col][c]);
                    inv_m[r][c] ^= gf_mul(factor, inv_m[col][c]);
                }
            }
        }
        Some(inv_m)
    }

    /// Decode a chunk from shards indexed by position. A `None` entry is a
    /// missing shard. Up to `m` shards may be missing; with more, or with
    /// duplicate positions filled in, this fails cleanly.
    ///
    /// Returns the reconstructed chunk truncated to `chunk_len`.
    pub fn decode(&self, shards: &[Option<&[u8]>], chunk_len: usize) -> Result<Vec<u8>> {
        if shards.len() != self.total() {
            return Err(Error::IntegrityError);
        }
        let available: Vec<usize> = (0..self.total())
            .filter(|&pos| shards[pos].is_some())
            .collect();
        if available.len() < self.k {
            return Err(Error::ChunkUnavailable(format!(
                "erasure decode needs {} shards, {} available",
                self.k,
                available.len()
            )));
        }
        // Select exactly k of the available positions; the MDS property
        // guarantees the encoding rows of any k positions are invertible.
        // Duplicate shard content at two positions is legal content
        // addressing, but the same position may only be filled once, which
        // the Option type already guarantees.
        let chosen: Vec<usize> = available[..self.k].to_vec();
        let rows: Vec<Vec<u8>> = chosen
            .iter()
            .map(|&pos| self.encoding_row(pos))
            .collect();
        let sl = shards[chosen[0]].unwrap().len();
        let Some(inv) = Self::invert(&rows, self.k) else {
            return Err(Error::IntegrityError);
        };
        // data_shard_c = sum_t inv[c][t] * shard_t, elementwise.
        let mut data: Vec<Vec<u8>> = vec![vec![0u8; sl]; self.k];
        for (t, &pos) in chosen.iter().enumerate() {
            let shard = shards[pos].unwrap();
            if shard.len() != sl {
                return Err(Error::IntegrityError);
            }
            for (c, row) in inv.iter().enumerate() {
                let coef = row[t];
                if coef == 0 {
                    continue;
                }
                for (b, d) in data[c].iter_mut().enumerate() {
                    *d ^= gf_mul(coef, shard[b]);
                }
            }
        }
        let mut chunk = Vec::with_capacity(chunk_len);
        for d in &data {
            chunk.extend_from_slice(d);
        }
        chunk.truncate(chunk_len);
        Ok(chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hash::sha256;

    struct Lcg(u64);
    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            self.0 >> 33
        }
    }

    #[test]
    fn field_arithmetic() {
        // Generator powers are a bijection over nonzero elements.
        let t = tables();
        let mut seen = [false; 256];
        for e in &t.exp[..255] {
            assert!(*e != 0);
            assert!(!seen[*e as usize], "generator cycle repeated early");
            seen[*e as usize] = true;
        }
        // Every nonzero element inverts to one.
        for a in 1..=255u8 {
            assert_eq!(gf_mul(a, gf_inv(a)), 1);
        }
        // Multiplication distributes over xor.
        for a in [0u8, 1, 3, 255, 128] {
            for b in [0u8, 2, 7, 200] {
                for c in [0u8, 1, 9] {
                    assert_eq!(gf_mul(a, b ^ c), gf_mul(a, b) ^ gf_mul(a, c));
                }
            }
        }
    }

    #[test]
    fn encode_decode_round_trip() {
        let mut rng = Lcg(12345);
        for k in 1..=6usize {
            for m in 1..=3usize {
                let e = Erasure::new(k, m).unwrap();
                for len in [0usize, 1, 7, k, k * 10, k * 10 + 1] {
                    let chunk: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
                    let shards = e.encode(&chunk);
                    assert_eq!(shards.len(), e.total());
                    let refs: Vec<Option<&[u8]>> = shards.iter().map(|s| Some(s.as_slice())).collect();
                    let back = e.decode(&refs, len).unwrap();
                    assert_eq!(back, chunk, "k={k} m={m} len={len}");
                }
            }
        }
    }

    #[test]
    fn any_k_of_k_plus_m_suffices() {
        let (k, m) = (3, 3);
        let e = Erasure::new(k, m).unwrap();
        let chunk: Vec<u8> = (0..1000u32).map(|i| (i * 7 % 251) as u8).collect();
        let shards = e.encode(&chunk);
        // Every subset of k positions must reconstruct the chunk.
        for mask in 0..(1u32 << (k + m)) {
            if mask.count_ones() != k as u32 {
                continue;
            }
            let refs: Vec<Option<&[u8]>> = (0..(k + m))
                .map(|pos| {
                    if mask & (1 << pos) != 0 {
                        Some(shards[pos].as_slice())
                    } else {
                        None
                    }
                })
                .collect();
            let back = e.decode(&refs, chunk.len()).unwrap();
            assert_eq!(back, chunk, "subset mask {mask:06b}");
        }
    }

    #[test]
    fn m_plus_one_losses_fail_cleanly() {
        let (k, m) = (2, 2);
        let e = Erasure::new(k, m).unwrap();
        let chunk = b"only two shards may vanish".to_vec();
        let shards = e.encode(&chunk);
        // Three of four missing: an error, never wrong bytes.
        let refs: Vec<Option<&[u8]>> = vec![Some(shards[0].as_slice()), None, None, None];
        assert!(matches!(e.decode(&refs, chunk.len()), Err(Error::ChunkUnavailable(_))));
        let refs: Vec<Option<&[u8]>> = vec![None, None, None, None];
        assert!(e.decode(&refs, chunk.len()).is_err());
        // All present: fine.
        let refs: Vec<Option<&[u8]>> = shards.iter().map(|s| Some(s.as_slice())).collect();
        assert_eq!(e.decode(&refs, chunk.len()).unwrap(), chunk);
    }

    #[test]
    fn duplicate_content_at_distinct_positions_is_fine() {
        let e = Erasure::new(2, 1).unwrap();
        // All-zero data makes every shard zero, so positions share content.
        let chunk = vec![0u8; 64];
        let shards = e.encode(&chunk);
        assert_eq!(shards[0], shards[1]);
        assert_eq!(sha256(&shards[0]), sha256(&shards[2]));
        // Passing the same bytes at both data positions still decodes; the
        // missing parity position is not needed with k shards present.
        let refs = vec![
            Some(shards[0].as_slice()),
            Some(shards[1].as_slice()),
            None,
        ];
        assert_eq!(e.decode(&refs, 64).unwrap(), chunk);
    }

    #[test]
    fn reencoded_shard_matches_original() {
        let e = Erasure::new(3, 2).unwrap();
        let chunk: Vec<u8> = (0..500u32).map(|i| (i % 253) as u8).collect();
        let shards = e.encode(&chunk);
        let data: Vec<Vec<u8>> = shards[..3].to_vec();
        for (pos, shard) in shards.iter().enumerate() {
            assert_eq!(e.encode_position(&data, pos), *shard, "position {pos}");
        }
    }

    #[test]
    fn length_mismatch_is_rejected() {
        let e = Erasure::new(2, 1).unwrap();
        let shards = e.encode(b"sixteen bytes..");
        let mut refs: Vec<Option<&[u8]>> = shards.iter().map(|s| Some(s.as_slice())).collect();
        // A shard of the wrong length must not decode silently.
        let short: Vec<u8> = shards[0][..3].to_vec();
        refs[1] = Some(&short);
        assert!(e.decode(&refs, 15).is_err());
    }

    #[test]
    fn bounds_are_enforced() {
        assert!(Erasure::new(0, 2).is_err());
        assert!(Erasure::new(2, 0).is_err());
        assert!(Erasure::new(33, 32).is_err());
        assert!(Erasure::new(16, 16).is_ok());
    }
}
