# Archipelago

Archipelago is a distributed file system written from scratch in Rust with zero
external dependencies. It does chunking, content addressing, replication across
nodes, a metadata namespace, quorum reads and writes, and self healing. The part
that makes it unusual is that the whole cluster, including the network, runs
inside a single process as a seeded deterministic simulation. You can inject
faults, replay a failure exactly from its seed, and check the result against a
machine oracle, all without threads or real sockets.

Playground: https://pavanchow.github.io/archipelago/

## The gap it fills

Real distributed file systems are hard to test because the interesting bugs live
in timing. A message arrives late, two nodes disagree for a moment, a replica
dies at the worst instant. Reproducing that on real hardware is slow and flaky.

Archipelago turns the cluster into a pure function of a seed and an operation
script. Latency, reordering, message loss, node crashes, and network partitions
are all decisions made by one seeded pseudo random generator. Same seed means
the identical run every time. That is the FoundationDB and TigerBeetle style of
deterministic simulation testing, applied to a file system you can read in an
afternoon.

Why a person would reach for it: to learn how chunking, rendezvous placement,
quorums, and re-replication actually fit together, in code small enough to hold
in your head. Why an AI agent would reach for it: it is a dependency free,
single process distributed system with a built in correctness oracle, so an
agent can run it, fault inject it, and verify data safety deterministically
inside one sandbox.

## Quickstart

```bash
cargo build
cargo test
cargo run --bin arch -- demo
```

The `demo` writes several files, spreads their chunks across five nodes, kills a
node so chunks go under replicated, re-replicates onto the survivors, then reads
every file back verified against its content hash.

Interactive session:

```bash
cargo run --bin arch
arch> mkdir /docs
arch> put /docs/hello hello archipelago
arch> status
arch> fail 0
arch> get /docs/hello
arch> stabilize
arch> status
```

## Library API

```rust
use archipelago::{Cluster, Options};

let mut c = Cluster::new(Options::default(), 42);
c.mkdir("/data")?;
c.write_file("/data/report", &bytes)?;
let back = c.read_file("/data/report")?;

// Fault controls, all deterministic.
c.crash_node(0);          // take a storage node offline, its bytes are retained
c.recover_node(0);        // bring it back
c.partition(&[1, 4]);     // isolate nodes from the client and the rest
c.heal();                 // remove the partition
c.stabilize();            // run re-replication to convergence
let s = c.status();       // per node chunk counts and per file replica health
```

File operations: `write_file`, `read_file`, `delete`, `mkdir`, `list`, `rename`,
`stat`. Every read verifies the reassembled bytes against the file content hash
before returning, so a read either yields the correct bytes or fails loudly.

Erasure coding is an alternative chunk protection mode. Instead of replicating
each chunk R times, each chunk is Reed Solomon encoded into k data plus m parity
shards that are spread over distinct storage nodes, and a read needs any k of
them.

```rust
use archipelago::Options;

// Every chunk becomes 2 data plus 2 parity shards over 4 of the 5 nodes.
let mut c = Cluster::new(Options::small_erasure(2, 2), 7);
c.write_file("/sharded", &big_bytes)?;
assert_eq!(c.read_file("/sharded")?, big_bytes);
```

## Configuration

`Options` controls chunk size, replication factor R, write and read quorums, the
storage and metadata node counts, and the network link behaviour. The defaults
are R equal to 3, write quorum 2, and read quorum 1 with read repair. R equal to
3 tolerates the loss of any two replicas. Write quorum 2 means a write is durable
on a majority of replicas before it is acknowledged. Read quorum 1 is safe
because every returned chunk is checked against its content hash, so one good
copy is provably the right bytes.

Setting `erasure` switches the write and read paths from replication to
Reed Solomon erasure coding over GF(2^8). With k data and m parity shards per
chunk, up to m shard holders can be lost and the file still reads, because any
k of the k+m shards reconstruct it. The write commits once every chunk group
has max of write quorum and k durable shard positions, since fewer than k shards
cannot reconstruct. A read verifies every fetched shard against its content
address, so a corrupt shard just counts as missing, and then re-encodes lost
shard positions onto live nodes as repair. The k+m shards of one chunk are
placed on distinct nodes, so the cluster needs at least k+m live nodes to accept
erasure writes.

## Correctness gates

The tests are the point. Each gate proves a specific property.

1. `tests/differential.rs` runs a random stream of file system operations
   against an in memory oracle and asserts byte for byte agreement and matching
   recursive listings after every operation. Op count is set with
   `ARCH_FUZZ_OPS`.
2. `tests/faults.rs` is the durability gate. It crashes and partitions up to R
   minus 1 nodes at adversarial points and asserts no file is lost, then heals
   and asserts re-replication restores R live replicas. It also crashes exactly
   R holders of one chunk and asserts the system reports unavailability rather
   than returning wrong bytes.
3. `tests/roundtrip.rs` checks chunking and content addressing round trip for
   random buffers including the edge sizes.
4. `tests/determinism.rs` asserts the same seed and script produce the identical
   delivery order and final state.
5. `tests/fault_differential.rs` runs the randomized differential while crash,
   recovery, and partition events are interleaved, always inside the fault
   tolerance of the tuning. A successful read must return exactly the oracle
   bytes unless a failed but possibly committed op touched the path, and every
   quiescent window must return to strict agreement with the oracle. A
   committed file that vanishes or comes back wrong is a hard failure.
6. `tests/boundaries.rs` pins the explicit edge cases: the empty file, exact
   chunk boundaries and shrinking overwrites, hostile and relative paths, root
   protections, rename onto itself and into its own subtree, deep nesting with
   a subtree rename, delete semantics, and reads while the holders of a chunk
   are partitioned away at one, two, and three failures.
7. `tests/stress.rs` samples the Options space with a mini differential per
   tuning and re-checks determinism under faults across tunings.
8. `tests/erasure.rs` is the erasure coding gate. It covers round trips across
   sizes, an oracle differential in erasure mode, tolerance of m holder losses,
   clean failure beyond m losses, read repair regenerating lost shards while
   the holder stays down, and the too few nodes rejection.

Run the heavy fuzz:

```bash
ARCH_FUZZ_OPS=5000 cargo test --test differential
ARCH_FAULT_FILES=400 cargo test --test faults
ARCH_STRESS_OPS=1500 cargo test --test stress
```

## Layout

```
src/error.rs        error type
src/hash.rs         SHA-256 from scratch and the content address type
src/varint.rs       LEB128 varints
src/encode.rs       length prefixed serialization
src/chunk.rs        chunking and the file manifest
src/erasure.rs      Reed Solomon erasure coding over GF(2^8)
src/placement.rs    rendezvous hashing for replica placement
src/message.rs      wire messages and their serialization
src/net.rs          the deterministic network simulator
src/storagenode.rs  a storage node
src/metadata.rs     the namespace and the replicated op-log
src/cluster.rs      the cluster handle, event loop, and fault controls
src/client.rs       the file system API over the cluster
src/options.rs      configuration
src/bin/arch.rs     the CLI
```

See `DESIGN.md` for the wire formats, the placement strategy, the quorum model,
and an argument for why each gate proves what it claims.

## License

MIT
