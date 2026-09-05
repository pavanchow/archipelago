//! Gate 6: boundary, negative, and malformed-path behavior.
//!
//! Explicit edge cases the randomized gates only hit by chance: the empty
//! file, exact chunk-size boundaries, hostile paths, rename corners, deep
//! nesting, and reads while part of the cluster is partitioned away.

use archipelago::{Cluster, Error, Options};

fn small(seed: u64) -> Cluster {
    Cluster::new(Options::small(), seed)
}

#[test]
fn empty_file_round_trip() {
    let mut c = small(1);
    c.mkdir("/d").unwrap();
    c.write_file("/d/empty", b"").unwrap();
    assert_eq!(c.read_file("/d/empty").unwrap(), b"");
    let s = c.stat("/d/empty").unwrap();
    assert_eq!(s.size, 0);
    c.delete("/d/empty").unwrap();
    assert!(c.read_file("/d/empty").is_err());
}

#[test]
fn chunk_boundary_sizes() {
    let cs = 256;
    let mut c = Cluster::new(
        Options {
            chunk_size: cs,
            ..Options::small()
        },
        2,
    );
    c.mkdir("/b").unwrap();
    let sizes = [cs - 1, cs, cs + 1, cs * 4, cs * 4 + 1, cs * 4 - 1];
    for (i, &len) in sizes.iter().enumerate() {
        let data: Vec<u8> = (0..len).map(|k| (k % 251) as u8).collect();
        let path = format!("/b/f{i}");
        c.write_file(&path, &data).unwrap();
        assert_eq!(c.read_file(&path).unwrap(), data, "len {len}");
        assert_eq!(c.stat(&path).unwrap().size as usize, len);
    }
    // Overwrite from the largest boundary down to the empty file: the
    // manifest must shrink correctly and no stale bytes may leak.
    let path = "/b/over";
    for &len in &sizes {
        let data: Vec<u8> = (0..len).map(|k| (k.wrapping_mul(7) % 253) as u8).collect();
        c.write_file(path, &data).unwrap();
        assert_eq!(c.read_file(path).unwrap(), data, "len {len}");
    }
    c.write_file(path, b"").unwrap();
    assert_eq!(c.read_file(path).unwrap(), b"");
}

#[test]
fn hostile_paths_rejected() {
    let mut c = small(3);
    c.mkdir("/d").unwrap();
    c.write_file("/d/f", b"x").unwrap();

    for bad in [
        "relative", "", "/a/", "/a//b", "/a/./b", "/a/../b", "/a/../..", "/..", "/.", "///",
    ] {
        assert!(
            matches!(c.write_file(bad, b"x"), Err(Error::InvalidPath(_))),
            "write accepted {bad:?}"
        );
        assert!(
            matches!(c.read_file(bad), Err(Error::InvalidPath(_))),
            "read accepted {bad:?}"
        );
        assert!(
            matches!(c.mkdir(bad), Err(Error::InvalidPath(_))),
            "mkdir accepted {bad:?}"
        );
        assert!(
            matches!(c.delete(bad), Err(Error::InvalidPath(_))),
            "delete accepted {bad:?}"
        );
        assert!(
            matches!(c.list(bad), Err(Error::InvalidPath(_))),
            "list accepted {bad:?}"
        );
        assert!(
            matches!(c.rename(bad, "/ok"), Err(Error::InvalidPath(_))),
            "rename accepted {bad:?}"
        );
    }

    // Root protections.
    assert!(matches!(c.write_file("/", b"x"), Err(Error::IsADirectory(_))));
    assert!(matches!(c.read_file("/"), Err(Error::IsADirectory(_))));
    assert!(matches!(c.delete("/"), Err(Error::InvalidPath(_))));
    assert!(matches!(c.rename("/", "/x"), Err(Error::InvalidPath(_))));
    assert!(matches!(c.rename("/d", "/"), Err(Error::InvalidPath(_))));
    // Root lists and stats as a directory.
    assert!(c.list("/").is_ok());
    assert!(c.stat("/").unwrap().is_dir);
}

#[test]
fn rename_under_itself_and_into_descendant() {
    let mut c = small(4);
    c.mkdir("/a").unwrap();
    c.mkdir("/a/sub").unwrap();
    c.write_file("/a/f", b"content").unwrap();
    c.write_file("/f", b"file").unwrap();

    // Rename onto itself is rejected for files and directories.
    assert!(matches!(
        c.rename("/f", "/f"),
        Err(Error::AlreadyExists(_))
    ));
    assert!(matches!(
        c.rename("/a", "/a"),
        Err(Error::AlreadyExists(_))
    ));
    // Renaming a directory into its own subtree is rejected.
    assert!(matches!(
        c.rename("/a", "/a/under"),
        Err(Error::InvalidPath(_))
    ));
    assert!(matches!(
        c.rename("/a", "/a/sub/deeper"),
        Err(Error::InvalidPath(_))
    ));
    // Renaming a file into a path below itself is rejected as invalid.
    assert!(matches!(c.rename("/f", "/f/x"), Err(Error::InvalidPath(_))));
    // The subtree is intact after the rejections.
    assert_eq!(c.read_file("/a/f").unwrap(), b"content");
    assert!(c.list("/a").is_ok());
}

#[test]
fn deep_nesting_and_subtree_rename() {
    let mut c = small(5);
    const DEPTH: usize = 40;
    let mut path = String::new();
    for i in 0..DEPTH {
        path.push_str(&format!("/d{i}"));
        c.mkdir(&path).unwrap_or_else(|e| panic!("mkdir {path}: {e:?}"));
    }
    let data = b"bottom of a deep chain".to_vec();
    let bottom = format!("{path}/bottom");
    c.write_file(&bottom, &data).unwrap();
    assert_eq!(c.read_file(&bottom).unwrap(), data);

    // Rename the top of the chain: every descendant moves.
    c.rename("/d0", "/moved").unwrap();
    let mut moved = String::from("/moved");
    for i in 1..DEPTH {
        moved.push_str(&format!("/d{i}"));
    }
    let moved_bottom = format!("{moved}/bottom");
    assert_eq!(c.read_file(&moved_bottom).unwrap(), data);
    assert!(c.read_file(&path).is_err());
    // The old subtree no longer lists.
    assert!(c.list("/d0").is_err());
}

#[test]
fn delete_semantics() {
    let mut c = small(6);
    c.mkdir("/d").unwrap();
    c.write_file("/d/f", b"x").unwrap();
    // Non-empty directory rejected.
    assert!(matches!(
        c.delete("/d"),
        Err(Error::DirectoryNotEmpty(_))
    ));
    // File delete works, then the directory becomes deletable.
    c.delete("/d/f").unwrap();
    c.delete("/d").unwrap();
    assert!(matches!(c.delete("/d"), Err(Error::NotFound(_))));
    assert!(matches!(c.delete("/missing"), Err(Error::NotFound(_))));
    // Missing parents are not created implicitly.
    assert!(matches!(
        c.write_file("/no/such/dir/f", b"x"),
        Err(Error::NotFound(_))
    ));
    assert!(matches!(c.mkdir("/no/such/dir"), Err(Error::NotFound(_))));
    // Writing over a directory is rejected.
    c.mkdir("/dir2").unwrap();
    assert!(matches!(
        c.write_file("/dir2", b"x"),
        Err(Error::IsADirectory(_))
    ));
    // Listing a file is rejected.
    c.write_file("/file2", b"x").unwrap();
    assert!(matches!(
        c.list("/file2"),
        Err(Error::NotADirectory(_))
    ));
    assert!(matches!(c.list("/missing"), Err(Error::NotFound(_))));
}

#[test]
fn reads_during_partition() {
    let mut c = Cluster::new(Options::default(), 77);
    c.mkdir("/p").unwrap();
    let data = b"partitioned read check".to_vec();
    c.write_file("/p/f", &data).unwrap();

    // Isolate one node: the file still reads (2 of 3 replicas reachable).
    let chunk = archipelago::sha256(&data);
    let holders = c.placement_of(&chunk);
    assert_eq!(holders.len(), 3);
    c.partition(&[holders[0]]);
    assert_eq!(c.read_file("/p/f").unwrap(), data);

    // Isolate a second holder: one replica remains, still readable.
    c.partition(&[holders[0], holders[1]]);
    assert_eq!(c.read_file("/p/f").unwrap(), data);

    // Isolate all three holders: the read must fail loudly as unavailable,
    // never as wrong bytes.
    c.partition(&holders);
    match c.read_file("/p/f") {
        Err(Error::ChunkUnavailable(_)) => {}
        Err(other) => panic!("expected ChunkUnavailable, got {other:?}"),
        Ok(bytes) => panic!("got {} bytes from a fully partitioned file", bytes.len()),
    }

    // Healing restores full access.
    c.heal();
    assert_eq!(c.read_file("/p/f").unwrap(), data);
}
