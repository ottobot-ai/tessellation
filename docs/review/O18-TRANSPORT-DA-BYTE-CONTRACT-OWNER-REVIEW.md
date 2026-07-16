# O-18 Transport and DA Byte Contract Owner Review

**Status:** OWNER RESPONSE REQUIRED. No transport size, chunking, or compression
direction in this packet has runtime authority until the owner dispositions O18-01
through O18-08 and engineering freezes the resulting active-era parameters and
codecs.

**Runtime authority:** None. The bounded Brotli decoder and reserved ingress queue
described below are unwired preparation. They do not change a live endpoint, gossip
topic, queue, artifact-validity rule, state transition, or finality decision.

**Primary gates:** `NET-001`, `NET-002`, `NET-008`, `NET-012`, `NET-013`,
`NET-014`, `RESOURCE-001`

**Updated:** 2026-07-15

## 1. Purpose

O-18 asks one narrow question:

> What exact byte and delivery contract lets every required consensus artifact be
> transported, retained, recovered, and decoded with deterministic resource bounds,
> without making a transport acknowledgement or DA receipt state-validity evidence?

The current stack has mutually incompatible limits and no single active-era
definition of the bytes they meter. A producer can construct an application-valid
artifact which the next transport hop cannot carry. A compressed envelope can also
be small while its decoded object graph is arbitrarily large.

No finite maximum can be derived from the set of all theoretically schema-valid v4
values. For example, `StateChannelSnapshotBinary.content` is an unconstrained
`Array[Byte]` (`modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala:17-20`).
The protocol therefore needs explicit active-era maxima. A future migration must
inventory the exact selected source chain; it cannot claim compatibility with every
value the old Scala types could theoretically construct.

## 2. Architecture boundary

O-18 is a transport, resource, retention, and DA contract. It does not change the
layer topology or economic authority:

1. Every GL0 validator still independently executes and validates every direct
   native `GL1 -> GL0` DAG-token transition against the exact proposal parent.
   Signature admission, a descriptor, a chunk proof, or successful delivery never
   substitutes for that execution.
2. For sharded `CL1 -> ML0 -> checkpoint -> GL0`, the producer and every execution
   signer replay before signing, watchtowers replay independently, and ordinary
   noncommittee GL0 nodes verify the replay-backed certificate, apply the scoped
   diff, and reproduce its root. O-18 does not restore ordinary noncommittee CL1
   replay and does not make a diff authoritative.
3. Framework currency replay inputs must remain exactly recoverable. Opaque DL1
   custom bytes may use commitment/availability carriage, but possession or
   inclusion proves no custom semantic fact and cannot synthesize a framework
   economic effect.
4. A transport acknowledgement, content receipt, chunk custody proof, or DA
   threshold must use a type and signature domain that cannot satisfy execution,
   snapshot-validity, optimistic-finality, or ML0 state-validity thresholds.

## 3. Source-proven current state

| Boundary | Current behavior | Consequence |
|---|---|---|
| GL0 event cutter | Permits a `20 MiB` aggregate (`modules/dag-l0/src/main/resources/dag-l0.conf:21-23`). | This is an application aggregation budget, not proof that any single transport frame can carry the result. |
| State-channel binary | Configures `512000` bytes (`modules/node-shared/src/main/resources/application.conf:40-44`), but validation reserializes the already-decoded object through `JsonSerializer` (`StateChannelValidator.scala:137-146`; `SizeCalculator.scala:10-16`). | The limit neither bounds HTTP body materialization nor uncompressed decode work, and currently meters canonical Brotli/JSON bytes used by fee accounting. |
| GossipSub | `p2p` uses go-libp2p-pubsub `v0.15.0` (`p2p/go.mod:8`) and does not set `WithMaxMessageSize` (`p2p/internal/gossip/gossip.go:160-204`). The dependency default is `1 << 20`. | A single artifact above the GossipSub frame limit cannot be delivered as one publication. |
| GossipSub publish result | `Topic.Publish` returns after local validation and enqueue; the router later drops a single oversized RPC. The sidecar then adds the checkpoint to its outbox and returns `ok=true` (`p2p/internal/gossip/gossip.go:709-723`; `p2p/internal/grpcserver/server.go:328-348`). | Local success is a false delivery acknowledgement. An oversized checkpoint can be retained and retried while peers receive no checkpoint. |
| Go and JVM gRPC | Go creates the server without a max-message override (`p2p/internal/grpcserver/server.go:163-184`); the JVM channel also has no override (`SidecarClient.scala:197-210`). | Both retain their independent defaults, currently smaller than several application/local-buffer limits. |
| ChainSync | Caps one message at `16 MiB` (`p2p/internal/chainsync/protocol.go:32-38`). | A larger required artifact needs a chunk contract; increasing a different hop does not help. |
| JVM callback bridge | Locally reserves up to `32 MiB` per decoded envelope (`GossipStream.scala:37-50`). | This is a local memory bound, not evidence that GossipSub or gRPC can deliver such an envelope. |
| Brotli decode | Legacy `deserialize` still fully materializes Brotli output (`JsonBrotliBinarySerializer.scala:123-136`). An additive `deserializeBounded` stops after `max + 1` (`:75-103,138-143`). | The safe primitive exists but no live untrusted family supplies an owner-ratified limit or calls it yet. |
| Downstream queues | All eight dag-l0 sinks remain unbounded (`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/Queues.scala:23-32`). An additive `ReservedIngressQueue` holds item/byte reservations through processing (`ReservedIngressQueue.scala:10-18,38-119`). | The reservation primitive is unwired. It does not currently bound any sink or end-to-end artifact lifetime. |

The conflicting constants are not interchangeable. A consensus work budget, an
economic fee-size basis, an uncompressed decode limit, a compressed envelope limit,
a chunk size, and an aggregate retrieval budget protect different resources and
must be named separately.

## 4. Proposed contract shape

The recommendation is one active-era table, committed through the same rooted
`ProtocolEra`/network/genesis parameter authority as other consensus resource
rules. Each artifact family names at least:

| Field | Meaning |
|---|---|
| `artifactKind` | Domain-separated family and lane. Decoder success is never a lane selector. |
| `codecEra` | Exact canonical uncompressed codec and version. |
| `maxCanonicalBytes` | Maximum canonical uncompressed artifact bytes accepted in that era. |
| `maxCompressedBytes` | Maximum bytes accepted before opening a decompressor. |
| `maxItems` / family cardinalities | Structural bounds needed before allocation and expensive verification. |
| `maxArtifactChunks` | Maximum chunks for one artifact. |
| `maxAggregateFetchBytes` | Maximum bytes one request/session may cause the receiver to retain or fetch. |
| `maxConcurrentFetches` | Global, per-peer, per-topic, and per-artifact in-flight work. |
| retention class | Exact release condition: rejection, Phase 2, challenge/recovery horizon, or downstream acknowledgement. |

At minimum the inventory must cover native DAG, allow-spend, and token-lock blocks;
state-channel framework and framework-with-data envelopes; opaque/data-only
payloads; metagraph intake receipts; shard checkpoint proposals, execution
signatures, watchtower evidence, and certified checkpoints; GL0 snapshots and
optimistic attestations; tower proofs; fraud proofs; generic event rumors; and
ChainSync/bootstrap responses.

The numerical table does not exist yet. Engineering must measure worst intended
active-era values and adversarial overhead, propose values with headroom, and return
them for freeze. HOCON or environment overrides cannot decide whether an artifact
is protocol-valid.

## 5. Owner decisions required

### O18-01 Active-era maxima by family

**Question:** Should the protocol reject any artifact whose exact canonical
uncompressed bytes, compressed envelope bytes, structural cardinalities, or
aggregate retrieval cost exceed the rooted active-era family table?

**Recommendation:** Yes. Require an explicit table and fail before allocation,
decompression, signature verification, storage, or forwarding where the relevant
bound can be checked. Engineering derives numbers from the intended active-era
grammar and measured fixtures, not from every theoretically valid v4 object. Return
the proposed numerical matrix for owner review before activation.

### O18-02 Existing-chain migration compatibility

**Question:** When hard-fork migration is pursued, must the new transport accept
every theoretically schema-valid v4 object, or the exact artifacts present on the
selected source chain plus explicit headroom and exception handling?

**Recommendation:** Inventory the exact selected source chain and add justified
headroom. Produce a deterministic preflight report of any exceptional artifact
before the fork. Do not leave greenfield runtime decoding unbounded to support
values that never occurred. This does not reopen the deferred genesis/migration
epic or remove the ability to read prior local disk data through its historical-read
contract.

### O18-03 Canonical bytes and compression identity

**Question:** Which bytes define content identity, signatures, hashes, fees, and
deduplication in the active era?

**Recommendation:** Canonical `ScodecV1` uncompressed bytes define artifact identity.
Compression is a versioned transport transform only. The descriptor binds the
codec era, compression algorithm/version, canonical length, compressed length, and
canonical content hash. Two valid compression encodings of the same canonical bytes
identify the same artifact and cannot change signatures, economic fees, ordering,
or replay identity. No live switch is authorized until every producer and verifier
uses the same active-era bytes.

### O18-04 Descriptor and chunk proof

**Question:** What must a receiver authenticate before allocating or assembling a
large artifact?

**Recommendation:** A small canonical descriptor commits at least artifact kind and
lane, network/genesis/era domain, canonical content hash and length, transport
encoding and compressed length, fixed chunk size, chunk count, and a chunk Merkle
root. Each response binds descriptor hash, index, exact chunk length, and leaf proof.
Bound duplicate/conflicting chunks, peers, retries, timeouts, concurrent requests,
aggregate bytes, partial-file disk use, restart recovery, and retention. Verify the
fully reassembled canonical hash before publication, replay, execution, or DA credit.
Engineering must return the exact codec, hash domains, chunk size, limits, and
retention table for freeze.

### O18-05 Always-pull versus size threshold

**Question:** Should a variable-size consensus artifact be pushed directly when it
currently fits, with descriptor/pull used only above a threshold?

**Recommendation:** No runtime size-dependent semantic lane. Gossip a descriptor and
pull every variable/bulk content artifact. Only separately typed, fixed-small control
messages whose family maximum is below every hop may be pushed directly. This avoids
two delivery identities and prevents a configuration-dependent threshold from
changing availability or deduplication behavior.

### O18-06 Existing `512000` state-channel rule

**Question:** Is the current `512000` compressed JSON reserialization limit retained
as a consensus/economic rule, replaced, or treated only as an interim transport
limit?

**Recommendation:** Replace its byte definition in the new active era. If the owner
wants a per-binary economic/work cap and size-based fee, meter exact canonical
uncompressed `ScodecV1` bytes and freeze that value separately from compressed and
chunk limits. Preserve v4 behavior only in an explicit historical/import era. Do
not let compressor output choose an economic fee or consensus-validity result.

### O18-07 Existing `20 MiB` event-cutter rule

**Question:** Does `20 MiB` remain a GL0 proposal work/aggregation budget, and if so,
what exact canonical bytes does it meter?

**Recommendation:** Retain an owner-ratified per-proposal work/encoded-size budget,
but define it over exact active-era canonical event bytes plus deterministic envelope
overhead. It is not a permission to push one `20 MiB` GossipSub or gRPC message.
Engineering must measure and return the value; do not silently preserve `20 MiB`
merely because it is the current HOCON default.

### O18-08 Absolute expansion cap and ratio cap

**Question:** Is a compression-ratio limit sufficient to stop decompression bombs?

**Recommendation:** No. Require both a pre-decompression compressed-byte cap and an
absolute canonical/decompressed output cap, stopping at `limit + 1`. A ratio cap may
be added only as an explicit active-era validity/resource rule with legitimate
high-ratio vectors. It is never a substitute for the absolute output cap.

## 6. Required test and evidence plan

O-18 cannot close on unit tests for helper classes alone. Activation requires:

1. A generated family matrix proving `limit - 1`, exact limit, and `limit + 1`
   behavior for canonical bytes, compressed bytes, every cardinality, chunk count,
   aggregate fetch bytes, and concurrent work.
2. High-ratio valid and malicious Brotli vectors proving the decoder stops at
   `max + 1` without materializing the full expansion and that a rejected message
   does not terminate a worker generation.
3. Go/JVM integration proving the sidecar never returns delivery success for an
   artifact it cannot frame, and that every accepted descriptor can be fetched from
   at least one retained source after restart and peer failure.
4. Chunk corruption, wrong index/count/root/length/domain, duplicate, conflicting,
   out-of-order, truncated, timeout, cancellation, multi-peer retry, eclipse, disk
   exhaustion, and restart/reassembly vectors.
5. Exact identity vectors proving compression choice cannot change artifact hash,
   signature preimage, fee, ordering, deduplication, checkpoint input identity, or
   replay result.
6. End-to-end item and byte accounting that holds reservation until all retained
   object graphs, outbox entries, partial chunks, queue entries, mempool entries, and
   processing effects are released or durably transferred to another bounded owner.
7. A future migration corpus generated from the exact selected v4 source chain,
   with deterministic preflight failure for every artifact outside the ratified new
   era limits.
8. Architecture regressions proving native `GL1 -> GL0` remains universally
   executed by every GL0 validator and that the sharded CL1 certified-diff path does
   not gain an authority or universal-replay shortcut from transport code.

## 7. Response template

The owner may answer each item with `accept recommendation` or a replacement:

```text
O18-01:
O18-02:
O18-03:
O18-04:
O18-05:
O18-06:
O18-07:
O18-08:
```

Until all eight are dispositioned, bounded helpers may land only as unwired,
tested preparation. Live endpoint/queue wiring, active-era byte validity, chunk
codecs, fee-byte migration, and removal of existing limits remain frozen.
