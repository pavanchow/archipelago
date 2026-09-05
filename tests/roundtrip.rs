//! Gate 3: chunking and content-addressing round-trip through the cluster.
//!
//! For random byte buffers, including the edge sizes, storing across the cluster
//! and reading back must reproduce the original bytes exactly, and the file's
//! content hash must match the SHA-256 of the bytes.

// Gate tests intentionally use terse helpers and magic seed constants, and
// their local helper functions do not carry rustdoc error sections. Casts
// come from bounded modulo draws and short loop names are conventional here.
#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::doc_markdown,
    clippy::must_use_candidate,
    clippy::match_same_arms,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::similar_names,
    clippy::items_after_statements,
    clippy::single_match_else,
    clippy::too_many_lines
)]

use archipelago::{sha256, Cluster, Options};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

fn buffer(seed: u64, len: usize) -> Vec<u8> {
    let mut r = Rng(seed | 1);
    (0..len).map(|_| (r.next() & 0xff) as u8).collect()
}

#[test]
fn round_trip_edges_and_random() {
    let chunk_size = 256;
    let opts = Options {
        chunk_size,
        ..Options::default()
    };
    let mut c = Cluster::new(opts, 12345);

    let mut sizes = vec![0usize, 1, chunk_size - 1, chunk_size, chunk_size + 1, chunk_size * 3];
    let mut r = Rng(9);
    for _ in 0..20 {
        sizes.push((r.next() % (chunk_size as u64 * 4 + 7)) as usize);
    }

    for (i, &len) in sizes.iter().enumerate() {
        let data = buffer(i as u64 + 1, len);
        let path = format!("/file{i}");
        c.write_file(&path, &data).unwrap();

        let back = c.read_file(&path).unwrap();
        assert_eq!(back, data, "round trip mismatch at len {len}");

        let info = c.stat(&path).unwrap();
        assert_eq!(info.size as usize, len);
        assert_eq!(info.content_hash, sha256(&data), "content hash mismatch at len {len}");
    }
}

#[test]
fn overwrite_replaces_content() {
    let mut c = Cluster::new(Options::small(), 1);
    c.write_file("/f", b"first version").unwrap();
    c.write_file("/f", b"second, longer version of the file").unwrap();
    assert_eq!(c.read_file("/f").unwrap(), b"second, longer version of the file");
}
