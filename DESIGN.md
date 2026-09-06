# Archipelago design

This document explains how Archipelago works, the on the wire formats, the
placement strategy, the replication and quorum model with its exact durability
guarantee, the deterministic simulation approach, and why each correctness gate
proves what it claims. There are no em dashes and no semicolons in this prose.

## Overview

A file is a stream of bytes. Archipelago splits it into fixed size chunks, names
each chunk by the SHA-256 of its bytes, replicates each chunk to several storage
nodes, and records the ordered list of chunk names as the file manifest in a
metadata service. Reading a file fetches its chunks, verifies each against its
name, concatenates them, and verifies the whole against the file content hash.

Three roles talk over a network:

- The client coordinator drives file operations. It chunks data, sends chunks to
  storage nodes, gathers quorum acknowledgements, and commits manifests to
  metadata.
- Storage nodes hold chunks in a content addressed local store and answer store,
  fetch, and replicate messages.
- Metadata nodes hold the namespace and a replicated operation log. One is the
  primary and the rest are backups.

Everything runs in one process. The network between the roles is a simulation.

## Content addressing and chunking

`hash.rs` implements SHA-256 from scratch and wraps a digest in a `Hash` type
used everywhere for content addressing. It is validated against the standard
vectors, including the empty string, "abc", the 56 byte multi block vector, and
the one million byte vector.

`chunk.rs` splits a file into chunks of at most `chunk_size` bytes. Each chunk id
is the SHA-256 of its bytes. Identical chunks collapse to one id, so duplicate
content is stored once. A `Manifest` records the total size, the SHA-256 of the
entire file, and the ordered vector of chunk ids. Reassembly concatenates chunk
payloads in manifest order. Because the chunk id is a hash of the bytes and the
content hash is a hash of the whole file, corruption cannot pass unnoticed. A
storage node even refuses to store a chunk whose bytes do not match the claimed
id, so bad bytes never enter a store.

## Wire formats

`varint.rs` encodes unsigned integers as LEB128. `encode.rs` builds length
prefixed serialization on top: an `Encoder` writes tag bytes, varints, length
prefixed byte slices and strings, and fixed 32 byte hashes, and a `Decoder`
reads them back. This is the single source of truth for the byte layout.

A `Manifest` on the wire is the size as a varint, then the 32 byte content hash,
then the chunk count as a varint, then that many 32 byte hashes, then one byte
that is zero for replication mode or one followed by the k and m bytes for
erasure mode. In erasure mode the hash list holds the shard content addresses,
k+m per chunk group in order.

Decoders never panic on malformed input. A length prefix that decodes to an
absurd value is rejected with a `Decode` error rather than overflowing an
offset computation, and a count field is bounded by the bytes actually
remaining before any allocation, so a hostile buffer can ask for neither a
slice out of range nor an allocation larger than the input could justify.
Varints that would lose bits above bit 63 are rejected rather than silently
wrapping.

`message.rs` defines every message as a tagged union. Each message serializes as
a one byte tag followed by its fields. The kinds are:

- `StoreChunk` and `StoreAck` for writing a chunk and confirming it.
- `FetchChunk` and `ChunkData` for reading a chunk. `ChunkData` carries an
  optional payload, absent when the node does not hold the chunk.
- `ReplicateOrder` and `Replicate` for healing. The primary orders a source node
  to copy a chunk to a destination, and the source ships the bytes.
- `Heartbeat` where a storage node advertises the chunks it holds.
- `MetaOpMsg`, `MetaReplicate`, `MetaReplicateAck`, and `MetaCommitted` for the
  metadata write path.
- `MetaQueryMsg` and `MetaQueryResp` for the metadata read path.

Every message travels the simulated network as bytes. A routed envelope is the
sender node id, the receiver node id, and the length prefixed message body. The
simulator encodes on send and decodes on delivery, so the serialization is
exercised on every hop, not just in a unit test.

## Placement by rendezvous hashing

`placement.rs` decides which nodes hold a chunk. For a chunk id and a node index
it computes a weight equal to the top eight bytes of `SHA-256(node || chunk)`.
The R nodes with the highest weight hold the chunk. Ties break by node index, so
the choice is fully deterministic.

Rendezvous hashing minimizes movement when membership changes. A chunk only
moves when the specific node it weighs onto is added or removed. Adding a node
pulls a chunk onto it only when that node now outweighs a former holder, and
removing a node reassigns only the chunks that lived on it. The placement tests
assert exactly this. When a node is removed, chunks that did not weigh onto it
keep the identical placement, and when a node is added, any chunk whose placement
does not include the new node is unchanged. This is why the live storage set can
shrink and grow during faults without reshuffling the whole cluster.

## Replication, quorum, and the durability guarantee

Chunk writes use quorum. The coordinator sends `StoreChunk` to the R placement
nodes and waits for `write_quorum` acknowledgements from distinct nodes before it
considers the chunk durable. Only after every chunk of the file reaches write
quorum does the coordinator commit the manifest to metadata.

Chunk reads use quorum too. With `read_quorum` equal to one, the coordinator asks
the live nodes for each chunk and accepts the first copy whose bytes hash to the
requested id. A single verified copy is provably the correct bytes, so one is
enough. After assembly the coordinator verifies the whole file against the
content hash. If any chunk cannot be produced by any live node, the read returns
`ChunkUnavailable`. If assembly somehow failed the content check, the read
returns `IntegrityError`. A read never returns wrong bytes.

The exact durability guarantee follows from R distinct replicas and content
verified reads. With replication factor R, the loss of fewer than R nodes leaves
at least one live replica of every chunk, because rendezvous placement puts the R
replicas on R distinct nodes. Therefore no committed file is lost while fewer
than R nodes are down. When exactly R replicas of a chunk are gone, that chunk is
unrecoverable, and the system says so with `ChunkUnavailable` rather than
inventing data. This is the "no data loss below R failures, never return corrupt
data" contract.

Self healing closes the gap after a failure. `stabilize` has each live storage
node send a `Heartbeat` listing its chunks to the primary. The primary tracks
which nodes hold which chunks, recomputes the desired placement over the current
live set, and for any under replicated chunk orders a live holder to copy it to a
node that lacks it. Running to convergence restores the target number of live
replicas, which is R or the live node count when fewer than R nodes remain.

Storage durability model. A crash takes a node offline so it neither sends nor
receives, but its local store is durable and survives the crash. Recovery brings
the node back with its chunks intact. The only volatile state is whatever was in
flight on the network, which the simulator drops. This is stated so the guarantees
above are unambiguous.

## Erasure coding

`erasure.rs` implements Reed Solomon coding over GF(2^8) with the primitive
polynomial 0x11d and generator 2, driven by log and antilog tables. A chunk is
split into k equal data shards, and m parity shards are computed as Cauchy
combinations. The Cauchy matrix has the MDS property, so every square selection
of k encoding rows is invertible and any k of the k+m shards reconstruct the
chunk, whatever their positions. Inversion is Gaussian elimination in the field.
Shards are content addressed like every other blob, so a corrupt or misplaced
shard fails its hash check at fetch time and degrades to a missing shard.

Setting `Options.erasure` switches the write and read paths. The write encodes
each chunk into k+m shards and sends each shard to a distinct storage node,
chosen greedily so the shards of one chunk do not share a primary. It commits
once every chunk group has max of write quorum and k durable positions, because
fewer than k shards cannot reconstruct. The manifest carries the flat shard id
list plus the k and m parameters. The read fetches every shard position from
the live nodes, verifies each against its content address, and reconstructs a
chunk from any k verified positions. It then re-encodes each missing position
from the reconstructed chunk and ships it to the best live node as fire and
forget repair, so one read regenerates what a crash destroyed and `stabilize`
can converge again.

The guarantee in erasure mode is the erasure guarantee. Losing fewer than m
shard holders leaves at least k shards, so the file still reads. Losing m+1
holders without an intervening repair read leaves fewer than k shards, and the
read fails with `ChunkUnavailable` rather than wrong bytes. Two honest limits
are worth stating. First, identical shard bytes collapse to one blob, so a
degenerate all zero chunk has less physical redundancy than its k+m positions
suggest. Second, a lost shard is regenerated on the next read that needs it,
not eagerly, so between the loss and the next read the file has one less shard
of margin.

## Metadata durability

`metadata.rs` holds the namespace as a map of file paths to manifests plus a set
of directories, and it maintains a replicated operation log. Mutations are Put,
Mkdir, Delete, and Rename. The primary validates an operation against its
committed state, assigns it a sequence number, and replicates it to the backups.
It commits and acknowledges the client only once a quorum of metadata nodes have
logged the operation. Reads are served from the primary. This is primary backup
replication with a replicated op-log, which is simpler than full consensus and
enough for the guarantee we want.

Two rules make those acknowledgements mean what a commit needs them to mean.
First, a backup acknowledges a replicated operation only after it has applied
it. An operation that is merely buffered behind a missing earlier sequence
number is not durable anywhere except the primary, and counting such an
acknowledgement toward a quorum would let a commit exist on a single node, so
one primary crash could lose a write the client saw succeed. A backup that
drains several operations at once acknowledges each of them individually.
Second, every role refresh catches all backups up to the committed state of the
most advanced node, modeled as an instant state transfer like the promotion
path itself. Without that catch up, a backup that missed an operation while
offline would hold a permanent gap and could never apply anything again. The
cost of this design is that a metadata operation which fails client side may
still commit later, the usual ambiguous failure of a write path, and that a
burned sequence number on a primary can wedge a stale backup until the next
role refresh heals it.

On the loss of the primary, the cluster promotes the lowest indexed live
metadata node and reconciles it by adopting the committed state of the most
advanced live node. A committed operation was applied on a quorum of metadata
nodes, so with the default of three metadata nodes and a quorum of two the
namespace survives the loss of one metadata node. The
`metadata_survives_primary_crash` test exercises this path directly, and
`committed_write_survives_meta_failure_sequence` exercises the sharper window
where a commit quorum would otherwise form around a backup with a gap.

## Deterministic simulation

`net.rs` is the heart of the testing story. It holds a priority queue of
envelopes keyed by delivery time and a monotonic sequence number for a stable tie
break, so no two envelopes ever compare equal and the delivery order is total and
deterministic. A `Prng`, an xorshift64 generator seeded through a splitmix64
mixer, drives per link latency and, outside reliable mode, message drops. Latency
plus jitter is what produces reordering. The generator is consumed in send order,
and all cluster logic iterates ordered maps and sorted sets, so the whole run is
a function of the seed and the operation script.

The simulator can crash a node so messages to or from it are dropped, partition
the nodes into two groups so cross group messages are dropped, and heal a
partition. Endpoint reachability is checked both when a message is sent and when
it is delivered, so a message in flight when a node crashes or a partition forms
is correctly lost. The client coordinator is included in the partition set, so an
isolated group is truly unreachable from the client.

Because the run is deterministic, a failing seed reproduces the exact same
failure every time, which is what makes the simulator a debugging tool and not
just a fuzzer.

## Why each gate proves what it claims

Gate 1, differential testing. A distributed file system is correct when it
behaves like a simple correct file system. The oracle is a map of paths to bytes
and a set of directories, which is trivially correct by inspection. The test runs
a random operation stream against both the cluster and the oracle in reliable
network mode and, after every operation, asserts that the operation succeeded or
failed on both and that every file reads back byte for byte and every directory
lists identically. If the cluster ever diverged from the oracle in namespace
semantics, chunk assembly, or overwrite behaviour, the very next consistency
check would fire. Running many operations across several seeds with a small
colliding path space forces the tricky cases, such as writing where a directory
exists, deleting a non empty directory, and renaming a subtree. This proves the
end to end file semantics.

Gate 2, faults and durability. This gate proves the durability contract directly.
It writes a corpus, then crashes and partitions up to R minus 1 nodes at
adversarial points, verifying after each fault that every file still reads back
with a matching content hash. Since the guarantee is that fewer than R failures
lose nothing, a single lost or corrupted file would fail the verification. It then
heals and asserts through `status` that re-replication restored R live replicas
for every chunk, which proves the self healing path. Finally it crashes exactly R
holders of one chunk and asserts the read returns `ChunkUnavailable` and never
bytes, which proves the system fails loudly rather than corrupting on total loss
of a chunk. The exact R case is the sharp edge of the guarantee, and testing it
is what separates a real durability claim from a hopeful one.

Gate 3, round trip. Content addressing is only useful if split then store then
fetch then reassemble is the identity on bytes. The gate runs that path through
the real cluster for random buffers including the empty file, one byte, exactly
one chunk, and several non multiples of the chunk size, and checks both that the
bytes match and that the stored content hash equals the SHA-256 of the input.
These are the sizes where off by one chunk boundaries hide, so passing them
proves the chunk math.

Gate 4, determinism. The simulator is only a valid testing tool if a seed pins
the run. The gate runs the identical script twice per seed and asserts both the
final state hash and the full delivery order hash are equal. The state hash
covers every storage node chunk set and the primary namespace, and the delivery
digest covers the ordered sequence of every delivered envelope. Equal digests
across independent runs prove that latency, reordering, crash, and heal are all
reproducible from the seed, which is the property that licenses gates 1 through 3
to be trusted and replayed. The stress suite re-runs this property across
sampled tunings and with faults inside the script, so a tuning dependent
nondeterminism cannot hide behind the default configuration.

Gate 5, differential under faults. The classic differential gate only exercises
the system with nothing broken. This gate interleaves the same kind of random
operation stream with crash, recovery, and partition events, kept inside the
fault tolerance of the tuning, so the system is always within its guarantees.
The oracle checks scale with the fault model. A read that succeeds must return
exactly the oracle bytes, unless the path was touched by a mutating operation
that failed client side and may have committed anyway, which is tracked as a
taint on the path. Failed operations are allowed while faults are active, but
never wrong results. After every fault burst the cluster is driven back to a
quiescent state, the oracle is re-derived with taint absorbing only the
ambiguous direction, and the gate asserts strict agreement again. This proves
the fault tolerance contract under continuous churn rather than only at
designed fault points, and it is the gate that would catch a durability hole
like a commit quorum forming around a node that has not applied an operation.

Gate 6, boundaries. Randomized gates drift toward the middle of the input
space. This gate pins the edges deterministically: the empty file, sizes at
exact chunk boundaries, overwrites that shrink the manifest, paths with
trailing slashes and dot components and relative forms, the root protections,
renaming a path onto itself and a directory into its own subtree, forty levels
of nesting with a subtree rename, the delete rules, and reads while the holders
of a chunk are partitioned away at one, two, and three failures. Passing these
proves the semantics at the edges, where the randomized gates only wander by
chance.

Gate 7, the options matrix and determinism under faults. A tuning dependent
defect can hide behind the single default configuration. This gate samples the
Options space, chunk size, replication, quorums, node and metadata counts, and
network jitter, and runs a mini differential per sampled tuning against the
shared oracle. A second test runs a scripted fault churn twice per tuning and
asserts identical state and delivery digests, which extends the determinism
claim to every sampled configuration rather than the default one.

Gate 8, erasure coding. The erasure module is checked against its field
algebra, its encode and decode contract including every subset of k positions
reconstructing, and clean failure at m+1 losses. The cluster level gate then
proves the integration: files round trip in erasure mode, the oracle
differential holds in erasure mode, any m holder losses keep files readable,
m+1 losses fail loudly on a fresh cluster, a read regenerates lost shards so
`stabilize` converges while the holder is still down, and a cluster too small
to hold a whole shard group rejects the write instead of misplacing it.
