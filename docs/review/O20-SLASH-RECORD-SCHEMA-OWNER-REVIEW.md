# O-20 Field-34 Slash Record Schema Owner Review

**Status:** OWNER RESPONSE REQUIRED. This packet does not select or activate a
field-34 value schema. The recommendation below has no runtime authority until
the owner dispositions `O20-01` and engineering freezes the resulting key,
value, codec, transition, and test contracts.

**Runtime authority:** None. The live field-34 writer and readers continue to
use `InvalidStateProofSlashManager.SlashedRegistryEntry` and its hand-written JSON
`ImmutableCodec`. No code change is authorized by this packet.

**Primary gates:** `ROOT-008-F34`, `SER-005`, `SER-006`, `WT-002`, E1.1, E1.2,
E6, E8

**Updated:** 2026-07-16

## 1. Decision required

O-20 asks one narrow question:

> Does the first ScodecV1 field-34 schema represent only the implemented
> invalid-state-proof slash, with later slash kinds added as variant-specific
> records, or does V1 freeze one generalized record for slash kinds whose
> evidence identity and ledger sinks are not implemented?

This is an owner decision because the answer fixes rooted MPT value bytes,
physical-key identity, duplicate detection, cooldown interpretation, and the
future protocol-era transition. Implementing a codec first would silently make
that protocol choice in code.

O-20 does not decide whether a particular fraud proof is valid, how much stake
is removed, or whether a future equivocation/non-participation detector is
correct. Those remain separate evidence, adjudication, and economic-effect
gates. A field-34 record is the rooted result of a completed adjudication; it is
not evidence that can authorize its own creation.

## 2. Source-proven current state

| Surface | Current behavior | Consequence |
|---|---|---|
| Rooted partition | `Slashings` is hypergraph field ID 34, and `consensusRootEntries` excludes only field 32 (`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:288-310,542-545`). | Its key and value bytes are consensus state, not an API/debug representation. |
| Live producer | The only production `SlashedRegistryEntry` construction is inside `InvalidStateProofSlashManager.applySlash`; it supplies `shardId`, `disputedCheckpointHash`, and hard-codes `reason = InvalidStateProof` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashManager.scala:96-108,159-169`). | Current runtime evidence supports one slash-record kind. |
| Latent reason tags | `SlashReason` also declares `CheckpointEquivocation`, `MetagraphEquivocation`, and `NonParticipation`, while its own comment calls those future tiers (`InvalidStateProofSlashManager.scala:199-209`). | Enum presence is not a designed evidence identity, key, sink, or activation proof for those tiers. |
| Shared record | `SlashedRegistryEntry` requires `peerId`, `shardId`, `disputedCheckpointHash`, `eventOrdinal`, `cooldownUntilEpoch`, `evidenceDigest`, and `reason` (`InvalidStateProofSlashManager.scala:212-241`). | `shardId` and `disputedCheckpointHash` are native to the implemented invalid-checkpoint dispute. Their required meaning is undefined for at least non-participation, and cannot be filled with a sentinel without creating protocol semantics. |
| Physical key | The current key hashes the interpolated string `peerId|shardId|disputedCheckpointHash` (`GlobalStateKey.scala:566-580`). | It is invalid-state-proof-specific and is not the frozen ROOT-008 canonical tuple codec. Different future evidence identities cannot safely overload it. |
| Duplicate reader | The invalid-state-proof reader scans the partition and treats any value matching `(shardId, disputedCheckpointHash)` as already slashed (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/InvalidStateProofSlashedReader.scala:70-93`). | A generalized record could collide with the invalid-state-proof duplicate domain unless variants have distinct typed identities and readers. |
| Cooldown reader | Committee exclusion consumes only the common `peerId`, `eventOrdinal`, and `cooldownUntilEpoch` projection (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/slashing/SlashCooldownReader.scala:114-125,137-150`). | Future variants may share a cooldown projection without sharing their evidence-specific key/value fields. |
| Value bytes | The field-34 value is UTF-8 Circe JSON behind an `ImmutableCodec` (`InvalidStateProofSlashedReader.scala:52-68`). | This cannot be the target ScodecV1 leaf. A canonical value decision is required before the field-34 accumulator/change-set repair freezes bytes. |
| Producer write | GSAM derives the composite key and writes these records directly after the ordinary accumulator because field 34 is absent from `StateChangesAccumulator` (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:2967-2992`). | Adding field 34 to the typed accumulator must reproduce the selected record/key bytes; it cannot choose the schema implicitly. |

Repository search finds no production construction of a record with
`CheckpointEquivocation`, `MetagraphEquivocation`, or `NonParticipation`. The
only production constructor is the invalid-state-proof constructor cited above.
Tests constructing the generic case class do not activate the other reason tags.

## 3. Non-negotiable invariants under either option

1. The field-34 value uses one explicit, strict, bounded ScodecV1 codec. JSON,
   generic derivation, decoder probing, unknown tags, trailing bytes, and
   invalid refinement values reject.
2. A verifier derives the physical key from the decoded typed identity and
   requires exact key/value agreement. A wire-supplied `GlobalStateKey`, raw
   hash, map key, or record `reason` is never independent authority.
3. Duplicate identity is defined per slash variant. A record for one variant
   cannot suppress, alias, or satisfy duplicate detection for another variant.
4. Common fields such as operator, event ordinal, and cooldown may have a common
   projection, but evidence-specific fields are required only by the variant
   whose validator proved them. There are no sentinel hashes, zero shard IDs,
   overloaded addresses, or optional-field combinations with undefined meaning.
5. A record is written only after deterministic adjudication against the exact
   historical evidence/key/committee view. Missing history defers and cannot
   slash. Decoding a structurally valid record cannot authorize a slash.
6. Every producer, adopter, recovery path, follower change set, cooldown reader,
   duplicate reader, and proof/index reader uses the same active-era key/value
   contract. The complete resulting MPT root must match the signed root.
7. This is a greenfield fork. The target runtime retains no fork-only JSON or
   generalized-record compatibility path. A future protocol-era upgrade must
   define its historical read and state transition explicitly.

## 4. O20-01 options

### Option A - invalid-state-proof-only V1, future variant-specific ADT

**Recommendation.** Freeze the first field-34 value as a closed slash-record ADT
with exactly one accepted V1 variant, `InvalidStateProof`. Its payload contains:

- `peerId`;
- `shardId`;
- `disputedCheckpointHash`;
- `eventOrdinal`;
- `cooldownUntilEpoch`; and
- `evidenceDigest` with one frozen evidence-digest preimage.

The variant tag replaces the independently writable generic `reason` field. V1
rejects every other tag. The physical identity is domain-separated as an
invalid-state-proof slash and canonically binds exactly
`(peerId, shardId, disputedCheckpointHash)`. Decoder/adopter validation requires
the map/key identity and payload identity to agree before mutation.

When another slash kind is ready, a protocol-era change adds a record variant
whose payload and physical-key identity match that kind's proved evidence:

- checkpoint equivocation gets a checkpoint-equivocation-specific identity;
- metagraph equivocation gets a metagraph/parent/conflicting-artifact identity;
- non-participation gets an epoch/duty/assignment identity; and
- every new variant freezes its evidence digest, deduplication rule, economic
  effect, cooldown projection, resource bounds, and RED/false-slash corpus before
  activation.

The exact future identities above are intentionally not selected by O-20. Their
own evidence designs must derive them. The common reader may project
`(peerId,eventOrdinal,cooldownUntilEpoch)` only after strict variant decoding.

**Consequences:**

- V1 freezes only semantics the live producer actually implements.
- Field-34 accumulator work can use a typed invalid-state-proof delta without
  inventing placeholder values for unimplemented tiers.
- A future slash variant requires an explicit protocol-era schema/key expansion
  and migration/recovery proof. That is deliberate review work, not accidental
  backward compatibility.
- The current generic reason enum and JSON leaf are removed from active
  consensus use; future enum cases cannot become live merely by reaching a
  writer branch.

### Option B - freeze one generalized multi-reason record in V1

Freeze the current shared shape, or an optional-field expansion of it, with all
four reason tags active in one V1 value codec. This option is valid only if the
owner also defines now, for every reason:

- the exact meaning and required/forbidden status of `shardId`, checkpoint hash,
  metagraph identity, epoch/duty identity, and evidence digest;
- a domain-separated physical-key preimage and duplicate rule;
- cross-variant collision behavior;
- the evidence validator and historical inputs that authorize the record;
- stake/bounty/burn/cooldown effects; and
- strict rejection of every invalid reason/field combination.

**Consequences:**

- It attempts to avoid a later field-34 schema expansion.
- It makes unimplemented slash tiers part of the V1 rooted grammar before their
  evidence identities and sinks are proven.
- Retaining required checkpoint fields forces sentinel or overloaded meanings
  for non-checkpoint variants; making them optional creates a combination matrix
  whose validity rules become consensus logic.
- A generic duplicate reader can cause one variant to suppress another or can
  false-deduplicate distinct faults unless every variant is separately keyed and
  dispatched. At that point the shape has become a variant-specific ADT in all
  but name.
- Freezing speculative fields now makes later correction a protocol-era
  migration instead of allowing each future variant to land with its actual
  evidence contract.

## 5. Recommendation rationale

Option A minimizes consensus authority. It does not remove future slashing; it
requires a future slash kind to prove its own evidence identity before obtaining
a rooted consequence. That matches the current source: one implemented ledger
producer, one invalid-checkpoint composite identity, and readers whose broader
cooldown need is only a three-field common projection.

Option B is not justified merely because four reason case objects already exist.
Those tags do not specify canonical key bytes, evidence preimages, duplicate
domains, or ledger effects. Treating them as a finished schema would convert
placeholder source into protocol authority.

## 6. Activation evidence after an owner answer

The selected option remains nonactivating until all applicable gates pass:

1. Exact golden and independent vectors for the field-34 variant tag, payload,
   evidence digest, logical identity, physical key, MPT leaf, and complete root.
2. Negative vectors for every unknown tag, trailing byte, malformed refinement,
   duplicate identity, key/value mismatch, cross-variant collision, and
   unavailable-history case.
3. Producer/validator byte parity and insertion-order independence.
4. `StateChangesAccumulator` and `GlobalChangeSetResponse` replay reproducing an
   ordinal with a nonempty field-34 delta, with tampering rolling back before
   follower state advances.
5. Reorg, restart, catch-up, exact-byte recovery, and Phase-2 replacement tests
   proving orphan records disappear and canonical records remain byte-identical.
6. Invalid-state-proof per-signer deduplication, cooldown exclusion, and economic
   conservation tests, including an honest-node false-slash corpus.
7. Source guards proving the JSON leaf codec, interpolated-string key, generic
   reason writer, and direct-outside-accumulator field-34 path are unreachable in
   the active era.

O-18 independently owns transport/page/resource bounds for changeset delivery.
It cannot alter the selected field-34 identity or make transport bytes state
authority.

## 7. Response template

The owner may answer with `accept recommendation` or select/replace Option B:

```text
O20-01:
```

Until `O20-01` is answered, engineering may add RED tests and dark codec
experiments, but must not freeze or activate field-34 Scodec value/key bytes,
generalize the live slash writer, or treat the latent reason tags as designed
slash variants.
