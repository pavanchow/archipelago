//! Shared helpers for the hardening gates: a seeded PRNG, path helpers, and a
//! trivially-correct in-memory oracle of the namespace semantics.
//!
//! The oracle is the independent reference the differential gates compare
//! against. It is deliberately dumb: a map of paths to bytes plus a set of
//! directories, implementing the documented semantics directly.
//!
//! This module is compiled once per integration-test binary, and each binary
//! uses a different subset of the helpers, so unused warnings here are noise.
#![allow(
    dead_code,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::must_use_candidate,
    clippy::cast_possible_truncation,
    clippy::similar_names
)]

use std::collections::{BTreeMap, BTreeSet};

/// A small deterministic PRNG (xorshift64). Never seed with zero.
pub struct Rng(u64);

impl Rng {
    /// Seed the generator. A zero seed is forced to one.
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }

    /// Next raw 64 bit value.
    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// A value in `0..n`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// A random byte buffer of length `0..=max_len`.
    pub fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.below(max_len as u64 + 1) as usize;
        (0..len).map(|_| (self.next() & 0xff) as u8).collect()
    }
}

/// The parent portion of an absolute path, `None` at the root.
pub fn parent(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/".into()),
        Some(i) => Some(path[..i].to_string()),
        None => None,
    }
}

/// The final component of a path.
pub fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

/// Whether `path` lies strictly below `ancestor`.
pub fn is_under(path: &str, ancestor: &str) -> bool {
    if ancestor == "/" {
        return path != "/";
    }
    path.starts_with(&format!("{ancestor}/"))
}

/// The reference implementation of the namespace semantics.
#[derive(Default, Clone)]
pub struct Oracle {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
}

impl Oracle {
    /// A fresh oracle containing only the root directory.
    pub fn new() -> Self {
        let mut o = Oracle::default();
        o.dirs.insert("/".into());
        o
    }

    /// Whether the path is a directory.
    pub fn is_dir(&self, p: &str) -> bool {
        p == "/" || self.dirs.contains(p)
    }

    /// Whether the path exists at all.
    pub fn exists(&self, p: &str) -> bool {
        self.is_dir(p) || self.files.contains_key(p)
    }

    /// Whether the path is a file.
    pub fn is_file(&self, p: &str) -> bool {
        self.files.contains_key(p)
    }

    /// File bytes, if the path is a file.
    pub fn read(&self, p: &str) -> Option<&[u8]> {
        self.files.get(p).map(Vec::as_slice)
    }

    /// All file paths, sorted.
    pub fn file_paths(&self) -> Vec<String> {
        self.files.keys().cloned().collect()
    }

    /// All directory paths including the root, sorted.
    pub fn dir_paths(&self) -> Vec<String> {
        let mut v: Vec<String> = self.dirs.iter().cloned().collect();
        v.push("/".into());
        v.sort();
        v.dedup();
        v
    }

    /// Documented write semantics: parent must be a directory, target must
    /// not be one.
    pub fn write(&mut self, p: &str, data: &[u8]) -> Result<(), ()> {
        if self.is_dir(p) {
            return Err(());
        }
        match parent(p) {
            Some(par) if self.is_dir(&par) => {
                self.files.insert(p.into(), data.to_vec());
                Ok(())
            }
            _ => Err(()),
        }
    }

    /// Whether a write to `p` would be accepted, without mutating.
    pub fn can_write(&self, p: &str) -> bool {
        !self.is_dir(p) && parent(p).is_some_and(|par| self.is_dir(&par))
    }

    /// Whether a mkdir of `p` would be accepted, without mutating.
    pub fn can_mkdir(&self, p: &str) -> bool {
        !self.exists(p) && parent(p).is_some_and(|par| self.is_dir(&par))
    }

    /// Whether a delete of `p` would be accepted, without mutating.
    pub fn can_delete(&self, p: &str) -> bool {
        if p == "/" {
            return false;
        }
        if self.files.contains_key(p) {
            return true;
        }
        if self.dirs.contains(p) {
            return self.children(p).is_empty();
        }
        false
    }

    /// Whether a rename from `from` to `to` would be accepted, without
    /// mutating.
    pub fn can_rename(&self, from: &str, to: &str) -> bool {
        if from == "/" || to == "/" || !self.exists(from) || self.exists(to) || is_under(to, from) {
            return false;
        }
        parent(to).is_some_and(|p| self.is_dir(&p))
    }

    /// Documented mkdir semantics: target must not exist, parent must be a
    /// directory.
    pub fn mkdir(&mut self, p: &str) -> Result<(), ()> {
        if self.exists(p) {
            return Err(());
        }
        match parent(p) {
            Some(par) if self.is_dir(&par) => {
                self.dirs.insert(p.into());
                Ok(())
            }
            _ => Err(()),
        }
    }

    /// Documented delete semantics: root is protected, directories must be
    /// empty.
    pub fn delete(&mut self, p: &str) -> Result<(), ()> {
        if p == "/" {
            return Err(());
        }
        if self.files.remove(p).is_some() {
            return Ok(());
        }
        if self.dirs.contains(p) {
            let has_child = !self.children(p).is_empty();
            if has_child {
                return Err(());
            }
            self.dirs.remove(p);
            return Ok(());
        }
        Err(())
    }

    /// Documented rename semantics: no root, no overwrite, no renaming into
    /// one's own subtree.
    pub fn rename(&mut self, from: &str, to: &str) -> Result<(), ()> {
        if from == "/" || to == "/" || !self.exists(from) || self.exists(to) || is_under(to, from) {
            return Err(());
        }
        let par_ok = parent(to).is_some_and(|p| self.is_dir(&p));
        if !par_ok {
            return Err(());
        }
        if let Some(data) = self.files.remove(from) {
            self.files.insert(to.into(), data);
        } else {
            self.dirs.remove(from);
            self.dirs.insert(to.into());
            let sub_dirs: Vec<String> = self
                .dirs
                .iter()
                .filter(|d| is_under(d, from))
                .cloned()
                .collect();
            for d in sub_dirs {
                self.dirs.remove(&d);
                self.dirs.insert(format!("{to}{}", &d[from.len()..]));
            }
            let sub_files: Vec<String> = self
                .files
                .keys()
                .filter(|f| is_under(f, from))
                .cloned()
                .collect();
            for f in sub_files {
                let data = self.files.remove(&f).unwrap();
                self.files.insert(format!("{to}{}", &f[from.len()..]), data);
            }
        }
        Ok(())
    }

    /// Immediate children as (name, is_dir) pairs.
    pub fn children(&self, dir: &str) -> Vec<(String, bool)> {
        let files = self
            .files
            .keys()
            .filter(|p| parent(p).as_deref() == Some(dir))
            .map(|p| (basename(p).to_string(), false));
        let dirs = self
            .dirs
            .iter()
            .filter(|d| **d != "/" && parent(d).as_deref() == Some(dir))
            .map(|d| (basename(d).to_string(), true));
        let mut v: Vec<(String, bool)> = files.chain(dirs).collect();
        v.sort();
        v
    }

    /// Documented list semantics: only directories list.
    pub fn listing(&self, dir: &str) -> Result<BTreeSet<(String, bool)>, ()> {
        if !self.is_dir(dir) {
            return Err(());
        }
        Ok(self.children(dir).into_iter().collect())
    }

    /// Absorb a file the cluster committed but the oracle never recorded
    /// (possible for a write that timed out client side yet committed). The
    /// bytes are content-hash verified by the cluster on read, so adopting
    /// them is safe.
    pub fn adopt_file(&mut self, p: &str, data: &[u8]) {
        self.files.insert(p.into(), data.to_vec());
    }

    /// Absorb a directory the cluster committed but the oracle never
    /// recorded, creating any missing ancestors as directories.
    pub fn adopt_dir(&mut self, p: &str) {
        if self.is_dir(p) {
            return;
        }
        self.dirs.insert(p.into());
        let mut cur = p.to_string();
        while let Some(par) = parent(&cur) {
            if self.is_dir(&par) {
                break;
            }
            self.dirs.insert(par.clone());
            cur = par;
        }
    }
}
