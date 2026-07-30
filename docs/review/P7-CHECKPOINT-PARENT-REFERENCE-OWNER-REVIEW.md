# P7 Checkpoint Parent Reference Owner Review

**Status:** OWNER / ENGINEERING FREEZE REVIEW ONLY

**Activation:** NONE

**Authority:** NONE

**Scope:** the authority-free value that lets every verifier find the exact
Phase-2 GL0 snapshot containing a shard checkpoint's parent. This packet does
not freeze a checkpoint signing preimage, signature domain, FinalityGate proof,
branch resolver, inclusion proof, transport envelope, or runtime acceptance
path.

## 1. Decision Summary

P7.0 needs a portable shard-parent lineage reference before P4.4 can make
historical verification load-bearing. The required value is not fully frozen
today.

O-10 ratifies an exact `(ordinal, hash)` reference to the GL0 snapshot that
carried the parent checkpoint, extraction of that checkpoint under the same
`shardId`, and comparison of its canonical preimage hash with the child's
`parentCheckpointHash`
(`docs/review/CONSENSUS-OWNER-DECISIONS.md:212-228`). P4.4 subsequently states
that checkpoint bases and anchors use the complete
`(ordinal,hash,parentHash,mptRoot)` shape
(`docs/review/CONSENSUS-PROTOCOL-TEST-PLAN.md:98-100`).

Those statements settle the safety direction, but they do not yet settle:

1. whether the lineage pointer carries the minimum 40-byte `(ordinal,hash)` or
   the complete 104-byte `GlobalSnapshotStateRef`;
2. the exact genesis sentinel representation;
3. whether `checkpointHash`, `shardId`, or `shardOrdinal` are members of the
   lineage pointer or sibling checkpoint-preimage fields;
4. the lineage pointer's Scodec tag and field order; or
5. the target nonnegative `ShardOrdinal` representation and overflow rule.

No protocol bytes should be implemented until the response block in section 10
is settled.

## 2. Three Different Values

These values solve different problems and must not be collapsed.

### 2.1 Execution Base

`ShardCheckpoint.executionBase` is the exact GL0 state against which the
producer, every execution signer, and assigned watchtowers reproduce CL1
framework transitions. It is already a four-field `GlobalSnapshotStateRef`
(`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:63-79`).

It answers:

> Which exact GL0 state image was replayed?

It does not answer:

> Which GL0 snapshot carried the previous shard checkpoint?

The execution base and parent-containing snapshot can differ. No equality
between them may be inferred.

### 2.2 Parent Lineage Reference

The missing value points to the exact GL0 snapshot that embedded the previous
checkpoint in this shard chain. A verifier uses it to recover the actual parent
checkpoint, then derives the parent ordinal, slot, producer, and canonical hash
from that artifact.

It answers:

> Where is the exact parent checkpoint authenticated and recoverable?

It does not prove Phase 2. The hash-bound FinalityGate and P4.4 historical
resolver must separately establish canonical Phase-2 membership, state-root
agreement, registry/eta/roster context, and current lineage.

### 2.3 Checkpoint Identity And Preimage Siblings

The child checkpoint separately needs:

- `shardId`;
- `parentCheckpointHash`;
- the child's `shardOrdinal`;
- the child's `slot`;
- its exact execution base; and
- the remaining target checkpoint fields.

The current transitional preimage already carries the first four as sibling
fields, but has only a legacy `gl0AnchorOrdinal` and no portable parent lineage
reference
(`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:97-107,121-130`).

The lineage pointer must not duplicate a claimed parent ordinal or slot. Those
values come from the recovered parent checkpoint. A self-claimed tuple would
preserve the receiver-asymmetry defect because it would not prove that the
claimed parent was embedded in the referenced GL0 artifact.

## 3. Current Source State

The reusable context and exact state-reference primitives are already dark and
authority-free:

- `ConsensusArtifactContextV1` binds network, genesis, Scodec era, and
  consensus-parameter identity, but grants no hashing, signing, transport, or
  runtime authority
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/consensus/ConsensusArtifactContextV1.scala:200-216`).
- Its explicit Scodec layout is 97 bytes and registers no
  `ConsensusHashSchema`
  (`modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/ConsensusArtifactContextV1Codec.scala:11-19,82-107`).
- `GlobalSnapshotStateRef` carries ordinal, snapshot hash, parent hash, and MPT
  root, and explicitly authenticates none of them by itself
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/nakamoto/GlobalSnapshotStateRef.scala:11-22`).
- Its Scodec layout is one nonnegative 8-byte ordinal followed by three
  fixed-width 32-byte hashes
  (`modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/GlobalSnapshotStateRefCodec.scala:14-30`).

The current checkpoint schema is not the target:

- it is explicitly transitional and lacks the canonical namespace-confined byte
  diff
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:14-19`);
- `gl0AnchorOrdinal` is explicitly only a legacy scheduling hint
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:24-27,41-43`);
- its signing preimage still derives Circe JSON semantics
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardCheckpoint.scala:110-118`);
- its Scodec `ShardOrdinal` is a signed `int64`
  (`modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/instances/ShardingScodecCodecs.scala:50-53`); and
- the type allows negative values even though they are not a domain concept
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/sharding/ShardOrdinal.scala:9-18,24-34`).

The protobuf decoder already rejects a negative shard ordinal, so the current
JSON/Scodec/protobuf boundaries disagree
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/sharding/ShardCheckpointWireCodecs.scala:182-199`).

## 4. Parent Reference Options

### Option A: Minimum GL0 `(ordinal,hash)`

Shape:

```text
ordinal : SnapshotOrdinal
hash    : Hash
```

Encoded size: 40 bytes, before any sentinel tag.

Benefits:

- exactly matches O-10's minimum wording;
- smallest wire value; and
- the GL0 snapshot hash authenticates its signed snapshot body once the exact
  snapshot is retrieved and verified.

Costs:

- P4.4 must separately resolve and bind `parentHash` and `mptRoot`;
- consumers can accidentally combine the correct snapshot hash with a
  receiver-selected or sibling state image unless one later capability closes
  that gap; and
- checkpoint anchors would use a weaker structural type than execution bases,
  contrary to the current P4.4 dependency statement.

### Option B: Complete `GlobalSnapshotStateRef` (Recommended)

Shape:

```text
ordinal    : SnapshotOrdinal
hash       : Hash
parentHash : Hash
mptRoot    : MptRoot
```

Encoded size: 104 bytes, before any sentinel tag.

Benefits:

- one exact-ref value is used for currency binary contexts, checkpoint execution
  bases, and checkpoint parent anchors;
- same-ordinal sibling hash and state-root substitution are structurally
  testable before P4.4 grants authority;
- P4.4 can resolve branch, MPT image, historical registry, eta, roster, and stake
  from one complete identity; and
- it implements P4.4's stronger four-field requirement while remaining a strict
  superset of O-10's `(ordinal,hash)` minimum.

Costs:

- 64 more bytes than Option A in each non-genesis checkpoint preimage; and
- the owner must explicitly confirm that P4.4's stronger wording is intended to
  refine O-10 rather than merely describe the execution base.

### Option C: Embed Parent Checkpoint Or Inclusion Proof In The Pointer

This is not recommended for the reference value.

The exact parent checkpoint bytes or an authenticated inclusion proof may be
carried as separately typed, bounded retrieval evidence, as O-10 permits. Putting
variable evidence inside the lineage pointer would:

- make a simple identity variable-sized;
- couple the pointer to an unsettled proof format and O-18 transport bounds;
- risk recursive checkpoint growth; and
- make two byte-distinct evidence packages name the same parent.

The pointer should remain canonical and unique. Evidence should prove or supply
what it names.

### Option D: Claimed Parent `(hash,ordinal,slot)`

Reject.

This does not prove that the parent was embedded in a Phase-2 GL0 snapshot. It
would preserve the current receiver-local lookup defect documented by O-10 and
`SHARD-C-009`
(`docs/review/CONSENSUS-PROTOCOL-TEST-PLAN.md:398`).

## 5. Recommended Structural Contract

Subject to owner acceptance, add one dark shared ADT:

```scala
sealed trait ShardCheckpointParentLineageRefV1

object ShardCheckpointParentLineageRefV1 {
  case object Genesis extends ShardCheckpointParentLineageRefV1

  final case class Embedded(
    containingGl0: GlobalSnapshotStateRef
  ) extends ShardCheckpointParentLineageRefV1
}
```

The name includes `Lineage` to prevent confusion with:

- `executionBase`, which identifies replay state;
- `parentCheckpointHash`, which identifies the recovered parent checkpoint
  preimage; and
- the containing snapshot of the child, which is not known when the committee
  builds the child checkpoint.

The checkpoint's top-level `ConsensusArtifactContextV1` remains a sibling in the
future checkpoint preimage. It must not be duplicated inside this reference.
Likewise, `shardId`, `parentCheckpointHash`, child `shardOrdinal`, and child
`slot` remain sibling preimage fields.

Recommended candidate bytes, not authoritative until approved:

```text
Genesis  = 0x00
Embedded = 0x01 || GlobalSnapshotStateRefCodec
```

Consequences:

- `Genesis` is exactly one byte;
- `Embedded` is exactly 105 bytes;
- every other tag rejects;
- there is no untagged or decoder-probing fallback; and
- the codec is ScodecV1 only, with no Circe, Kryo, Java serialization, protobuf
  inference, `ConsensusHashSchema`, or standalone signing API.

The reference is not independently hashed or signed. The future complete
checkpoint preimage binds its bytes under the checkpoint's separately reviewed
domain after all target fields and bounds freeze.

## 6. Genesis Sentinel Rules

The tagged ADT is recommended over `Option[GlobalSnapshotStateRef]` or a
distinguished all-zero value because it states the protocol meaning directly
and reserves no invalid `GlobalSnapshotStateRef` as authority.

At whole-checkpoint validation:

```text
Genesis is valid iff:
  child.shardOrdinal == 1
  parentCheckpointHash == Hash.empty

Embedded is valid iff:
  child.shardOrdinal > 1
  parentCheckpointHash != Hash.empty
```

Additional rules:

- missing parent history never converts `Embedded` into `Genesis`;
- a first checkpoint carrying `Embedded` rejects;
- a later checkpoint carrying `Genesis` rejects;
- a nonempty parent hash with `Genesis` rejects;
- an empty parent hash with `Embedded` rejects; and
- genesis is a shard-lineage sentinel, not Phase-2 evidence and not a bypass for
  the checkpoint execution base.

There are two distinct genesis concepts:

1. `ShardCheckpointParentLineageRefV1.Genesis` means there is no previous shard
   checkpoint.
2. A `GlobalSnapshotStateRef` at global ordinal zero uses the global-chain parent
   sentinel.

They must not substitute for one another.

## 7. Shard Ordinal Contract

Recommended target:

- `ShardOrdinal` is a nonnegative 64-bit integer with fixed 8-byte big-endian
  Scodec encoding;
- synthetic shard root is `0`;
- no checkpoint is emitted at `0`;
- the first emitted checkpoint is `1`;
- every non-genesis child is exactly `parent.shardOrdinal + 1`; and
- successor calculation at the maximum value fails explicitly instead of
  wrapping to a negative number.

Transient local arithmetic that wants `-1` must use a separate local type,
`Option`, `BigInt`, or checked difference. It must not expand the consensus wire
domain.

Because this fork is greenfield, the target should replace the undeployed signed
`Long` shape rather than retain a fork-only compatibility decoder. The exact
Scala refactor may be staged after this packet, but the target Scodec decoder
must reject every value with the high bit set.

## 8. Verification Semantics

The future P4.4 verifier for `Embedded(containingGl0)` must:

1. validate the checkpoint's top-level network/genesis/era/parameter context;
2. resolve the exact four-field GL0 reference without ordinal fallback;
3. require hash-bound Phase 2 through the FinalityGate;
4. verify the referenced signed GL0 snapshot, parent continuity, and MPT root;
5. extract exactly the parent checkpoint under the child's signed `shardId`;
6. hash that parent's future canonical Scodec signing preimage;
7. require equality with the child's signed `parentCheckpointHash`;
8. require child shard ordinal to be the checked successor;
9. require parent-relative slot continuity and staircase duty using the
   authenticated historical roster and parameters; and
10. defer or enter authenticated recovery if exact history/evidence is
    unavailable.

It must never:

- read a parent only from a receiver-local shard gossip store;
- substitute `executionBase` for the parent lineage reference;
- accept ordinal equality without hash/root equality;
- treat reference receipt as Phase-2 proof;
- use receiver wall clock or local HOCON in validity; or
- map missing history to genesis.

The `containingGl0` reference is known when the child checkpoint is produced: it
names the exact Phase-2 GL0 snapshot that embedded the parent checkpoint. A
different, future GL0 snapshot may later embed the child checkpoint. That future
child-containing snapshot is not known at child production and is therefore not
part of this parent-lineage pointer. Any child-slot bound derived from the
child-containing snapshot is an inclusion-time validation rule whose exact
portable evidence and signed-preimage ownership remain to be frozen with the
complete checkpoint schema; this packet does not define a signed GL0 slot
certificate.

## 9. Required Gates

### 9.1 Golden And Strict Codec Gates

After owner acceptance:

- `Genesis` golden is exactly `00`;
- `Embedded` golden is exactly `01` followed by the accepted exact-ref golden;
- exact lengths are 1 and 105 bytes under Option B;
- every tag `0x02..0xff` rejects;
- every truncated length rejects;
- every trailing byte rejects;
- ordinal-zero/nonempty-global-parent and
  nonzero/empty-global-parent references reject;
- empty snapshot hash and empty MPT root reject;
- malformed/noncanonical hash values reject on encode;
- no JSON/Kryo/protobuf fallback decodes the value; and
- the type has no `ConsensusHashSchema` or signing instance.

`ShardOrdinal` gates:

- `0`, `1`, and `Long.MaxValue` round-trip;
- raw negative `int64` encodings reject;
- root cannot be emitted as a checkpoint;
- `Long.MaxValue.next` returns typed exhaustion; and
- no conversion silently narrows, wraps, or accepts a negative value.

### 9.2 RED Semantic Gates

- A node with the parent only in shard gossip and a node without it produce the
  same verdict from the referenced GL0 artifact.
- Same ordinal with wrong snapshot hash rejects.
- Same ordinal/hash claim with wrong parent hash or MPT root rejects.
- A valid execution base used as a false parent anchor rejects.
- A referenced GL0 snapshot containing no checkpoint for `shardId` rejects or
  defers according to authenticated availability; it never uses another shard.
- A recovered parent whose canonical preimage hash differs from
  `parentCheckpointHash` rejects.
- A claimed parent ordinal or slot not derived from the recovered bytes has zero
  effect.
- `Genesis` on checkpoint 2 or later rejects.
- `Embedded` on checkpoint 1 rejects.
- Missing `Embedded` history does not fall back to `Genesis`.
- P0/P1 reference receipt cannot satisfy the Phase-2 gate.
- A Phase-2 density replacement invalidates the old lineage capability and
  requires re-resolution.
- Context mismatch across network, genesis, era, or parameter identity rejects
  before history lookup.
- Negative, root-emitted, skipped, repeated, and overflow shard ordinals reject.

### 9.3 Integration Stop Lines

The dark value may land after this packet is accepted, but runtime integration
still waits for:

- P4.4 branch-historical verification;
- P6 hash-bound Phase-2 evidence and reorg invalidation;
- the complete target checkpoint preimage and Scodec hash domain;
- exact parent artifact/inclusion-proof retrieval and retention;
- rooted roster/eta/parameter resolution;
- one-outstanding checkpoint enforcement after restart/reorg; and
- `SHARD-C-009`, `SHARD-C-010`, `SHARD-C-011`, and `SHARD-C-012`.

Landing this value alone closes none of those gates.

## 10. Owner Response Template

Please answer each item explicitly.

```text
P7-CPREF-01 Exact GL0 anchor:
  [ ] Accept recommended full GlobalSnapshotStateRef.
  [ ] Use minimum (ordinal,hash).
  [ ] Revise:

P7-CPREF-02 Sentinel:
  [ ] Accept tagged Genesis=0x00 / Embedded=0x01.
  [ ] Revise:

P7-CPREF-03 Field ownership:
  [ ] Accept lineage ref contains only the containing GL0 ref.
      ConsensusArtifactContextV1, shardId, parentCheckpointHash,
      child shardOrdinal, and child slot remain preimage siblings.
  [ ] Revise:

P7-CPREF-04 ShardOrdinal:
  [ ] Accept nonnegative uint-domain encoded as validated 8-byte big-endian
      int64, root=0, first=1, checked successor, overflow reject.
  [ ] Revise:

P7-CPREF-05 Authority:
  [ ] Accept structural value only: no Phase-2 inference, standalone hash
      schema, signature, runtime resolver, or mutation authority.
  [ ] Revise:

P7-CPREF-06 Evidence and recovery:
  [ ] Accept separate bounded parent artifact/inclusion evidence; missing exact
      history defers/recovery and never becomes Genesis.
  [ ] Revise:
```

An answer accepting all recommended boxes freezes only the parent-lineage value
and shard-ordinal rules described here. It does not freeze the complete
checkpoint preimage or activate P4.4.
