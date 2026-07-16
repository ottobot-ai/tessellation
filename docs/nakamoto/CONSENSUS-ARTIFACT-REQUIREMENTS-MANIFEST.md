# Consensus Artifact Requirements Manifest

## Status

This is a dark, declaration-only E1.1a inventory. It does **not** close E1.1 or
E1.3, define canonical bytes, provide a codec, authorize a signature, or activate
runtime protocol-era selection.

The source declarations are:

- `schema/consensus/ArtifactAuthority.scala`: distinct authority meanings;
- `schema/consensus/ArtifactBinding.scala`: fields and exact anchors an eventual
  identity/preimage must bind;
- `schema/consensus/ConsensusArtifactKind.scala`: grounded artifact families;
- `schema/consensus/ConsensusTranscriptKind.scala`: randomness/selection
  transcript subjects;
- `serde/consensus/ConsensusArtifactRequirementsManifest.scala`: pure contracts
  and structural validation.

The labels are review identifiers only. They are not wire tags or encoded domain
separators. The manifest deliberately imports no runtime hasher, immutable codec,
or protocol-era selector.

## Authority Classes

| Class | Meaning |
|---|---|
| State validity | The signer locally reproduced the complete layer transition or result before signing. |
| Finality qualification | Decided-attestation `T_weight` or depth-`k1` evidence qualifies one exact locally authenticated and executed GL0 reference; it does not validate economics or select a fork. |
| Source authorization | The signer authenticates an exact input, parent, and lane. It cannot satisfy execution quorum. |
| Eligibility | KES/VRF/registry/slot evidence proves eligibility only. |
| Custody/availability | The signer attests bounded authenticated custody or retrievability, never state validity. |
| Objective evidence | A verifier evaluates the exact proof/evidence; the assertion itself is not authoritative. |
| Commitment | Root, diff, key, value, or content identity with no independent signature authority. |
| Local durability | Journal/recovery integrity only; no network-validity claim. |
| Migration/genesis | Offline hard-fork source/transform/target evidence; it has no live runtime authority. |

Every declared artifact binds network, genesis identity, protocol era, parameter
hash, artifact kind, canonical content, and a non-optional exact anchor appropriate
to its authority. Every selection transcript separately binds its transcript kind,
exact eligibility parent, registered atomic KES/VRF pair, branch-authenticated N-2
roster/stake/key view, N-1 eta evidence, period/slot, purpose domain, and the exact
subsystem context. A sender-carried key, live peer set, receiver head, or current
stake map cannot satisfy those requirements. The manifest records requirements
only; how those fields are encoded remains open.

`NativeDagBlock` is source authorization from GL1. It never substitutes for every
GL0 validator independently executing and validating the native DAG transition
against the exact global proposal parent.

The layer topology remains:

```text
GL1 -> GL0
CL1 / DL1 -> ML0 -> shard checkpoint -> GL0
exact Phase-2 (Operational) GL0 state -> GL1 / ML0 / CL1 / DL1
```

Phase 2 remains density-reorgable. Every downstream derivative is tied to the
exact `(ordinal, hash, parentHash, mptRoot)` reference and must invalidate,
rollback, or deterministically rebase when that reference leaves the selected
chain; an ordinal-only or monotone `finalized` watermark is not authority.

For sharded CL1 framework economics, the checkpoint producer and every execution-
committee signer replay before signing, selected watchtowers independently replay,
and a noncommittee GL0 adopter verifies the replay-backed certificate, applies the
namespace-confined diff, reproduces the committed metagraph root, and executes the
deterministic global conflict/settlement kernel. A signature count, checkpoint root,
or custody/availability receipt never authorizes roots-only adoption. Opaque DL1
content may receive source/custody/availability treatment, but it cannot synthesize
or authorize a framework-economic transition.

An optimistic tip attestation is a separate state-validity signature: its signer
must first authenticate and locally execute the exact snapshot. Aggregated
decided-attestation `T_weight` or depth-`k1` evidence then qualifies an exact
canonical snapshot for Phase 2 without a QC, lock, vote lifecycle, or fork-choice
authority. Neither qualification path replaces GL0 execution.

## Grounded Families

These are semantic requirements families, not the final transport/subtype codec
inventory. E1.1 must still add generic rumor and ChainSync/bootstrap carriers,
decompose every `FinalityArtifactKind` payload variant, and give every family a
total exact-binding/codec/vector row before activation.

The 48 rows cover parameters/operator identity; native GL1 transactions, DAG,
allow-spend, and token-lock blocks plus their signed inner operations; direct GL0
node-parameter, delegated-stake, and collateral events; GL0 and ML0 genesis,
incremental, and state objects; the three explicit
state-channel lanes; ML0 source authentication; admission and DA custody; framework
economic intents; shard proposal/diff/execution/certificate/watchtower/evidence;
global settlement and follower deltas; per-validator optimistic tip attestations,
decided optimistic-attestation evidence, depth-`k1`, and separate fork-choice
evidence; tower
proofs; MPT keys, values, nodes, roots, and accumulator; durability records; and the
offline migration manifest.

The 10 transcript subjects are GL0 leader VRF, admission VRF, execution-committee
VK-hash draw, shard-eta derivation, staircase rank, checkpoint VRF possession,
execution-signature VRF possession, watchtower assignment, optimistic sampling,
and tower eligibility. Their transcript bytes and domains remain open.

## Finality Payload Subtypes

The broader `FinalityDurabilityRecord` family is not evidence that every payload
named by `FinalityArtifactKind` has a schema. The dark subtype inventory is total
over all 14 live kinds:

| Kind | Authority requirement | Current payload evidence | Status |
|---|---|---|---|
| CoreBatch | Local durability | `FinalityCoreBatch` codec and frozen vector | Existing shape needs audit |
| ReleasedCoreRecord | Local durability | `ReleasedCoreRecordPayload` codec and frozen vector | Existing shape needs audit |
| PathManifest | Commitment | `PathManifestPayload` codec and frozen vector | Existing shape needs audit |
| PathChunk | Commitment | `PathChunk` codec and frozen vector | Existing shape needs audit |
| DecidedAttestationEvidence | Finality qualification | Opaque pointer only; no payload codec/vector | Target shape open |
| DepthK1Evidence | Finality qualification | Opaque pointer only; no payload codec/vector | Target shape open |
| ForkChoiceDecisionEvidence | Objective evidence | Opaque pointer only; no payload codec/vector | Target shape open |
| PreparedSemanticState | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| AuthenticatedTargetAnchor | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| AppliedSemanticStateReceipt | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| AuthenticatedAnchorReceipt | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| PriorSemanticStateReceipt | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| PriorAnchorReceipt | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |
| EffectPayload | Local durability | Opaque pointer only; no payload codec/vector | Target shape open |

An opaque pointer commits kind, encoding identifier, digest, and byte length. It
does not make the bytes canonical and does not verify their semantics. Only the
decided-attestation and depth-k1 rows may qualify Phase 2. Fork-choice evidence
remains a separately verified objective chain-selection input; it is not a third
qualification rail and is not a QC, vote, or lock.

Every row declares the eventual common network/genesis/era/parameter/content
bindings. The dark finality composite domain now actually commits a required
nonzero consensus-parameter hash alongside network, genesis, and era, and exact
identity/vector tests cover that field. The four concrete payload rows remain
`ExistingShapeNeedsAudit`; having a codec/vector does not prove every declared
binding or activate runtime finality.

## Recorded Blockers

- Ordinal-zero GL0 and ML0 full genesis objects still embed legacy V1 state
  (`GlobalSnapshot.scala:42-70`; `currency.scala:206-219,325-346`).
- Explicit signed state-channel lane schemas and canonical shard diff/positive
  replay-coverage schemas remain open. O18 still controls canonical/transport
  bytes, compression, limits, chunking, and fee-byte rules.
- The economic grammar must enumerate the exact operation subtypes before the
  framework-intent family can become a codec manifest.
- `StateChangesAccumulator` carries 31 fields and omits MPT-native
  `ConsumedAllowSpends` and `Slashings` (`GlobalStateConverter.scala:74-115`;
  `StateChangesAccumulatorCodec.scala:49-56,133-186`; the rooted fields are
  `GlobalStateKey.scala:265-310`). `GlobalChangeSetResponse` therefore cannot yet
  satisfy its claim to carry the exact applied global delta
  (`GlobalChangeSetResponse.scala:11-13,34-39`).
- The dark typed `GlobalStateKeyCodec` now gives `SystemNamespace` outer tag
  `0x05` and closed label tags `0x00` through `0x03`, with exact vectors,
  unknown/trailing rejection, and a source guard that forbids production use
  outside the codec (`GlobalStateKeyCodec.scala:14-43,65-84`). This does not
  implement the consensus physical-key grammar: live MPT keys still use the
  separate lossy `GlobalStateKey.toHex` path, and ROOT-008 whole-image shape,
  key/value identity, and duplicate-logical-identity enforcement remain open
  (`GlobalStateKey.scala:603-680`; `ROOT-008-GL0-PARTITION-GRAMMAR.md:3-28`).
- `SlashedRegistryEntry` MPT values still use hand-written Circe JSON bytes under
  an `ImmutableCodec` facade (`InvalidStateProofSlashedReader.scala:52-67`).
- The live `TipAttestation` body omits the exact parent/root/era/parameter context
  required by FIN-S-002 and has no canonical active-era codec
  (`schema/nakamoto/attestation.scala:10-32`). It also carries wall-clock
  `attestedAt`, which cannot remain a preference/finality ordering input. The
  per-attester signature is not interchangeable with aggregate decided-`T_weight`
  evidence.
- A portable tower proof still lacks authenticated branch-historical N-2
  roster/stake/key membership, N-1 eta, and the load-bearing historical root
  witness. Nonempty proofs therefore remain fail-closed
  (`TowerVerifier.scala:343-349,394-400`).
- O19 still controls source transformation, conservation, and target-genesis
  policy. The manifest declares one migration family but chooses none of those
  policies.
- O18 still requires explicit generic-rumor and ChainSync/bootstrap transport
  families. The finality durability umbrella now has exact subordinate inventory
  coverage for all 14 `FinalityArtifactKind` variants, but ten payload schemas and
  their semantic verifiers remain open.
- The validator's authority-class fallback is intentionally coarse outside the
  critical shard, optimistic-tip, and tower rows. E1.1 must make the per-kind
  binding table total and mutation-test every family.

## Activation Gate

`ManifestCodecStatus.Open` and `ManifestActivationStatus.DarkOnly` are load-bearing
status markers. Activation still requires the atomic GL0/GL1/ML0/CL1/DL1 cutover,
complete bounded codecs and vectors, one branch-bound era service, removal of
active JSON/Kryo authority, and the protocol test-plan SER gates.
