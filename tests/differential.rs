//! Gate 1: differential testing against an in-memory oracle.
//!
//! A random stream of file-system operations runs against both the real cluster
//! and a trivially-correct oracle (a map of paths to bytes plus a set of
//! directories). After every operation the two must agree: the operation
//! succeeds or fails on both, every file reads back byte for byte, and a full
//! recursive listing matches. Runs in reliable network mode across several
//! deterministic seeds. Op count is controllable with ARCH_FUZZ_OPS.

use archipelago::{Cluster, Options};
use std::collections::{BTreeMap, BTreeSet};

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
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn parent(path: &str) -> Option<String> {
    if path == "/" {
        return None;
    }
    match path.rfind('/') {
        Some(0) => Some("/".into()),
        Some(i) => Some(path[..i].to_string()),
        None => None,
    }
}

fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

fn is_under(path: &str, ancestor: &str) -> bool {
    if ancestor == "/" {
        path != "/"
    } else {
        path.starts_with(&format!("{ancestor}/"))
    }
}

/// The reference implementation. Every method returns Ok/Err mirroring the
/// documented semantics of the cluster.
#[derive(Default)]
struct Oracle {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeSet<String>,
}

impl Oracle {
    fn new() -> Self {
        let mut o = Oracle::default();
        o.dirs.insert("/".into());
        o
    }
    fn is_dir(&self, p: &str) -> bool {
        p == "/" || self.dirs.contains(p)
    }
    fn exists(&self, p: &str) -> bool {
        self.is_dir(p) || self.files.contains_key(p)
    }
    fn write(&mut self, p: &str, data: &[u8]) -> Result<(), ()> {
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
    fn read(&self, p: &str) -> Result<Vec<u8>, ()> {
        self.files.get(p).cloned().ok_or(())
    }
    fn mkdir(&mut self, p: &str) -> Result<(), ()> {
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
    fn delete(&mut self, p: &str) -> Result<(), ()> {
        if p == "/" {
            return Err(());
        }
        if self.files.remove(p).is_some() {
            return Ok(());
        }
        if self.dirs.contains(p) {
            let has_child = self.children(p).next().is_some();
            if has_child {
                return Err(());
            }
            self.dirs.remove(p);
            return Ok(());
        }
        Err(())
    }
    fn rename(&mut self, from: &str, to: &str) -> Result<(), ()> {
        if from == "/" || to == "/" || !self.exists(from) || self.exists(to) || is_under(to, from) {
            return Err(());
        }
        let par_ok = parent(to).map(|p| self.is_dir(&p)).unwrap_or(false);
        if !par_ok {
            return Err(());
        }
        if let Some(data) = self.files.remove(from) {
            self.files.insert(to.into(), data);
        } else {
            self.dirs.remove(from);
            self.dirs.insert(to.into());
            let sub_dirs: Vec<String> =
                self.dirs.iter().filter(|d| is_under(d, from)).cloned().collect();
            for d in sub_dirs {
                self.dirs.remove(&d);
                self.dirs.insert(format!("{to}{}", &d[from.len()..]));
            }
            let sub_files: Vec<String> =
                self.files.keys().filter(|f| is_under(f, from)).cloned().collect();
            for f in sub_files {
                let data = self.files.remove(&f).unwrap();
                self.files.insert(format!("{to}{}", &f[from.len()..]), data);
            }
        }
        Ok(())
    }
    fn children<'a>(&'a self, dir: &'a str) -> impl Iterator<Item = (String, bool)> + 'a {
        let d1 = dir.to_string();
        let files = self
            .files
            .keys()
            .filter(move |p| parent(p).as_deref() == Some(&d1))
            .map(|p| (basename(p).to_string(), false));
        let d2 = dir.to_string();
        let dirs = self
            .dirs
            .iter()
            .filter(move |d| *d != "/" && parent(d).as_deref() == Some(&d2))
            .map(|d| (basename(d).to_string(), true));
        files.chain(dirs)
    }
    fn listing(&self, dir: &str) -> Result<BTreeSet<(String, bool)>, ()> {
        if !self.is_dir(dir) {
            return Err(());
        }
        Ok(self.children(dir).collect())
    }
}

const PATHS: &[&str] = &[
    "/f0", "/f1", "/f2", "/d0", "/d1", "/d0/f0", "/d0/f1", "/d1/f0", "/d0/sub", "/d0/sub/f0",
];

fn full_consistency(c: &mut Cluster, o: &Oracle) {
    // Every oracle file reads back identically.
    for (path, data) in &o.files {
        let got = c.read_file(path).expect("oracle file must exist in cluster");
        assert_eq!(&got, data, "byte mismatch at {path}");
    }
    // Every oracle directory lists identically.
    let mut all_dirs: Vec<String> = o.dirs.iter().cloned().collect();
    all_dirs.push("/".into());
    for dir in all_dirs {
        let want = o.listing(&dir).unwrap();
        let got: BTreeSet<(String, bool)> = c
            .list(&dir)
            .unwrap()
            .into_iter()
            .map(|e| (e.name, e.is_dir))
            .collect();
        assert_eq!(got, want, "listing mismatch at {dir}");
    }
}

fn run_seed(seed: u64, ops: usize) {
    let mut c = Cluster::new(Options::small(), seed);
    let mut o = Oracle::new();
    let mut r = Rng(seed | 1);

    for step in 0..ops {
        let op = r.below(7);
        let p = PATHS[r.below(PATHS.len() as u64) as usize];
        let (sys_ok, ora_ok): (bool, bool) = match op {
            0 => {
                let len = r.below(2500) as usize;
                let mut rr = Rng(r.next() | 1);
                let data: Vec<u8> = (0..len).map(|_| (rr.next() & 0xff) as u8).collect();
                let s = c.write_file(p, &data).is_ok();
                let ok = o.write(p, &data).is_ok();
                (s, ok)
            }
            1 => {
                let s = c.read_file(p);
                let ok = o.read(p);
                assert_eq!(
                    s.is_ok(),
                    ok.is_ok(),
                    "read agreement broke at {p} step {step}"
                );
                if let (Ok(sb), Ok(ob)) = (&s, &ok) {
                    assert_eq!(sb, ob, "read bytes differ at {p}");
                }
                (s.is_ok(), ok.is_ok())
            }
            2 => (c.delete(p).is_ok(), o.delete(p).is_ok()),
            3 => (c.mkdir(p).is_ok(), o.mkdir(p).is_ok()),
            4 => {
                let q = PATHS[r.below(PATHS.len() as u64) as usize];
                (c.rename(p, q).is_ok(), o.rename(p, q).is_ok())
            }
            5 => {
                let s = c.list(p);
                let ok = o.listing(p);
                assert_eq!(s.is_ok(), ok.is_ok(), "list agreement broke at {p}");
                (s.is_ok(), ok.is_ok())
            }
            _ => {
                let s = c.stat(p).is_ok();
                let ok = o.exists(p);
                (s, ok)
            }
        };
        assert_eq!(
            sys_ok, ora_ok,
            "op {op} on {p} disagreed at step {step} seed {seed}: sys={sys_ok} oracle={ora_ok}"
        );
        full_consistency(&mut c, &o);
    }
}

#[test]
fn differential_against_oracle() {
    let ops: usize = std::env::var("ARCH_FUZZ_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(400);
    for seed in [1u64, 2, 3, 17, 250] {
        run_seed(seed, ops);
    }
}
