//! `arch` boots a simulated Archipelago cluster and drives it from the command
//! line. With no arguments it opens a REPL. `arch demo` runs a scripted
//! scenario that shows chunks placed across nodes, a node failure driving
//! chunks under-replicated, re-replication restoring them, and every file read
//! back verified.
//!
//! Argument parsing is pure std.

use archipelago::{Cluster, Options};
use std::io::{self, BufRead, Write};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("demo") => demo(),
        Some("help") | Some("-h") | Some("--help") => print_help(),
        Some("repl") | None => repl(),
        Some(other) => {
            eprintln!("unknown command: {other}");
            print_help();
            std::process::exit(2);
        }
    }
}

fn print_help() {
    println!(
        "arch - simulated Archipelago distributed file system

usage:
  arch            start an interactive session
  arch repl       same as above
  arch demo       run the scripted fault-tolerance demo

repl commands:
  mkdir <path>
  put <path> <string...>     write a file from the given text
  get <path>                 read a file and print it
  ls <dir>                   list a directory
  rm <path>                  delete a file or empty directory
  mv <from> <to>             rename
  stat <path>                show file or directory info
  fail <node>                crash a storage node
  heal <node>                recover a storage node
  partition <node...>        isolate storage nodes from the cluster
  reconnect                  heal all partitions
  stabilize                  run re-replication to convergence
  status                     show node and file health
  seed <n>                   restart the cluster with a new seed
  quit"
    );
}

fn print_status(c: &Cluster) {
    let s = c.status();
    println!("clock {}", s.clock);
    print!("nodes: ");
    for n in &s.nodes {
        let mark = if n.live { "up" } else { "DOWN" };
        print!("[n{} {} {}c] ", n.idx, mark, n.chunks);
    }
    println!();
    if s.files.is_empty() {
        println!("files: none");
    } else {
        for f in &s.files {
            println!(
                "  {} size={} chunks={} min_live_replicas={}",
                f.path, f.size, f.chunk_count, f.min_live_replicas
            );
        }
    }
}

fn demo() {
    let opts = Options {
        chunk_size: 1024,
        ..Options::default()
    };
    let mut c = Cluster::new(opts, 7);
    println!("== Archipelago demo ==");
    println!(
        "storage nodes: {}, replication_factor: {}, write_quorum: {}",
        opts.node_count, opts.replication_factor, opts.write_quorum
    );

    c.mkdir("/docs").unwrap();
    let files: [(&str, usize); 4] = [
        ("/docs/small.txt", 10),
        ("/docs/medium.txt", 3000),
        ("/docs/large.bin", 9000),
        ("/readme", 512),
    ];
    let mut contents = Vec::new();
    for (path, size) in files {
        let data: Vec<u8> = (0..size).map(|i| (i * 37 + path.len()) as u8).collect();
        c.write_file(path, &data).unwrap();
        contents.push((path, data));
    }
    println!("\nwrote {} files, chunks spread across nodes:", files.len());
    print_status(&c);

    println!("\ncrashing node 0 ...");
    c.crash_node(0);
    print_status(&c);
    println!("(some files now show fewer live replicas)");

    println!("\nreading every file back while node 0 is down ...");
    for (path, want) in &contents {
        let got = c.read_file(path).unwrap();
        assert_eq!(&got, want, "mismatch on {path}");
        println!("  {path}: {} bytes, verified", got.len());
    }

    println!("\nrunning stabilize to re-replicate onto the remaining live nodes ...");
    let ok = c.stabilize();
    println!("re-replication converged: {ok}");
    print_status(&c);

    println!("\nrecovering node 0 and stabilizing ...");
    c.recover_node(0);
    c.stabilize();
    print_status(&c);

    println!("\nfinal verified read of every file:");
    for (path, want) in &contents {
        let got = c.read_file(path).unwrap();
        assert_eq!(&got, want);
        println!("  {path}: OK");
    }
    println!("\ndemo complete, no data lost.");
}

fn repl() {
    let mut seed = 1u64;
    let mut c = Cluster::new(Options::small(), seed);
    println!("archipelago repl. type 'help' for commands, 'quit' to exit.");
    let stdin = io::stdin();
    loop {
        print!("arch> ");
        io::stdout().flush().ok();
        let mut line = String::new();
        if stdin.lock().read_line(&mut line).unwrap_or(0) == 0 {
            break;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        let Some(&cmd) = parts.first() else {
            continue;
        };
        match cmd {
            "quit" | "exit" => break,
            "help" => print_help(),
            "status" => print_status(&c),
            "stabilize" => println!("converged: {}", c.stabilize()),
            "reconnect" => {
                c.heal();
                println!("healed");
            }
            "seed" => {
                if let Some(n) = parts.get(1).and_then(|s| s.parse().ok()) {
                    seed = n;
                    c = Cluster::new(Options::small(), seed);
                    println!("restarted with seed {seed}");
                } else {
                    println!("usage: seed <n>");
                }
            }
            "mkdir" => report(parts.get(1).map(|p| c.mkdir(p))),
            "get" => match parts.get(1) {
                Some(p) => match c.read_file(p) {
                    Ok(b) => println!("{}", String::from_utf8_lossy(&b)),
                    Err(e) => println!("error: {e}"),
                },
                None => println!("usage: get <path>"),
            },
            "put" => {
                if parts.len() < 3 {
                    println!("usage: put <path> <string...>");
                } else {
                    let data = parts[2..].join(" ");
                    match c.write_file(parts[1], data.as_bytes()) {
                        Ok(()) => println!("wrote {} bytes", data.len()),
                        Err(e) => println!("error: {e}"),
                    }
                }
            }
            "ls" => match parts.get(1) {
                Some(p) => match c.list(p) {
                    Ok(entries) => {
                        for e in entries {
                            let kind = if e.is_dir { "dir " } else { "file" };
                            println!("  {kind} {} ({} bytes)", e.name, e.size);
                        }
                    }
                    Err(e) => println!("error: {e}"),
                },
                None => println!("usage: ls <dir>"),
            },
            "rm" => report(parts.get(1).map(|p| c.delete(p))),
            "mv" => {
                if parts.len() < 3 {
                    println!("usage: mv <from> <to>");
                } else {
                    report(Some(c.rename(parts[1], parts[2])));
                }
            }
            "stat" => match parts.get(1) {
                Some(p) => match c.stat(p) {
                    Ok(s) => println!(
                        "{} size={} hash={}",
                        if s.is_dir { "dir" } else { "file" },
                        s.size,
                        s.content_hash.short()
                    ),
                    Err(e) => println!("error: {e}"),
                },
                None => println!("usage: stat <path>"),
            },
            "fail" => match parts.get(1).and_then(|s| s.parse().ok()) {
                Some(n) => {
                    c.crash_node(n);
                    println!("crashed node {n}");
                }
                None => println!("usage: fail <node>"),
            },
            "heal" => match parts.get(1).and_then(|s| s.parse().ok()) {
                Some(n) => {
                    c.recover_node(n);
                    println!("recovered node {n}");
                }
                None => println!("usage: heal <node>"),
            },
            "partition" => {
                let nodes: Vec<u32> = parts[1..].iter().filter_map(|s| s.parse().ok()).collect();
                if nodes.is_empty() {
                    println!("usage: partition <node...>");
                } else {
                    c.partition(&nodes);
                    println!("partitioned {nodes:?} from the cluster");
                }
            }
            other => println!("unknown command: {other}"),
        }
    }
}

fn report(r: Option<archipelago::Result<()>>) {
    match r {
        Some(Ok(())) => println!("ok"),
        Some(Err(e)) => println!("error: {e}"),
        None => println!("missing argument"),
    }
}
