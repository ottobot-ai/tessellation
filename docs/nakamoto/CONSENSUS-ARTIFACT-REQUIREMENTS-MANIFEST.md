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
- `schema/consensus/ConsensusCarrierKind.scala`: transport/coordination carrier
  families, separate from semantic artifact families;
- `schema/consensus/ConsensusTranscriptKind.scala`: randomness/selection
  transcript subjects;
- `serde/consensus/ConsensusArtifactRequirementsManifest.scala`: pure contracts
  and structural validation, including the separate `CarrierContract` table.

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
| Transport coordination only | Routing, delivery, request correlation, bootstrap carriage, or an unauthenticated hint only. It has zero state-validity, finality, fork-choice, execution, source-admission, or nested-payload authority. |

Every declared artifact binds network, genesis identity, protocol era, parameter
hash, artifact kind, canonical content, and a non-optional exact anchor appropriate
to its authority. Every selection transcript separately binds its transcript kind,
exact eligibility parent, registered atomic KES/VRF pair, branch-authenticated N-2
roster/stake/key view, N-1 eta evidence, period/slot, purpose domain, and the exact
subsystem context. A sender-carried key, live peer set, receiver head, or current
stake map cannot satisfy those requirements. The manifest records requirements
only; how those fields are encoded remains open.

Every transport carrier separately binds carrier kind and canonical carrier
content. A carrier that contains an artifact also binds the exact nested artifact
identity and type. Peer rumors additionally bind origin and sequence. A response
carrier binds an exact request/response correlation identity. Once implemented and
authenticated, these bindings will stop the carrier from changing what it
transports; they will not confer the nested artifact's authority on the carrier.

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
inventory. The 29 carrier rows below and the 14 finality-payload rows are separate
subordinate inventories; they do not inflate the 48 semantic artifact families.
E1.1 must still give every family final exact-binding/codec/vector rows before
activation.

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

## Transport Carrier Families

All 29 declaration-only manifest rows have `TransportCoordinationOnly` authority and remain dark. A carrier
cannot satisfy a state-validity, finality-qualification, objective-evidence,
source-authorization, execution, admission, or fork-choice threshold. This is true
even when the nested object is a signature or evidence artifact. This is a target
decomposition, not a claim that the live implementation already complies: current
Peer/Common rumor signatures, origin, seedlist, and collateral checks are
load-bearing source-admission inputs before typed dispatch.

The 29-row count is a closed semantic-family inventory only for the audited generic
rumor, sidecar subscription, event-gossip, ML0-consensus-rumor, ChainSync, and
bootstrap boundaries. It is not a concrete wire-subtype completeness claim. The
current 12 ChainSync protobuf source shapes are listed separately below; their final
wire tags, framing, correlation, and bounded-stream contract remain O18. Other
artifact-specific sidecar protobuf wrappers compose their corresponding semantic
artifact with transport and still require O18 rows. Libp2p's internal mesh-control
messages are implementation protocol traffic, not JVM consensus carriers.

| Carrier kind | Grounded current source or target | Required target bindings beyond carrier-common | Recorded current gap |
|---|---|---|---|
| `PeerRumorEnvelope` | `PeerRumorRaw` (`schema/gossip.scala:98-109`) | Exact nested identity/type; origin and sequence | Signature preimage is delimiter-free and the discriminator is a runtime Scala type string. Live signature/origin/seedlist/collateral source admission must be decomposed into its own artifact/preimage before this carrier can have zero authority; O18 open. |
| `CommonRumorEnvelope` | `CommonRumorRaw` (`schema/gossip.scala:85-91`) | Exact nested identity/type | Same preimage/discriminator gaps. Live signature/seedlist/collateral source admission must be decomposed before zero-authority activation; O18 open. |
| `SidecarRumorEnvelope` | outer protobuf `Rumor` around the signed raw bytes (`p2p/proto/sidecar.proto:61-75`) | Exact nested identity/type | Outer `content_type` and `origin_id` are not bound by the nested signature. Mutating either changes the full-proto GossipSub message ID while the receiver decodes only `signed_rumor_bytes`, bypassing dedup for repeated delivery/flood. A full queue drops peer rumors while Nakamoto disables HTTP repair; O18 open. |
| `EventGossipEnvelope` | generic HTTP `EventPush[Event]` (`EventGossipMessage.scala:26-35`; `EventGossipRoutes.scala:36-45`) | Exact nested identity/type | The route ignores the carried `eventHash` and admits the signed event under its recomputed identity; outer-to-inner identity is not enforced; O18 open. |
| `EventGossipIHave` | generic `IHave` hash/chain-tip offer (`EventGossipMessage.scala:37-46`) | Exact request/response correlation | The HTTP response is peer-signature verified, but there is no request identity and the fields are not portable artifact-bound fork-choice evidence; O18 open. |
| `EventGossipIWantRequest` | generic `IWantRequest` (`EventGossipMessage.scala:48-55`) | Exact request/response correlation | No request identity; O18 open. |
| `EventGossipIWantResponse` | generic `IWantResponse` (`EventGossipMessage.scala:57-65`) | Exact request/response correlation and nested identity/type | No request identity; the consumer marks the carried tuple hash seen without comparing it to the signed event's recomputed hash (`EventGossipDaemon.scala:280-293`); O18 open. |
| `Ml0EventAnnouncementEnvelope` | ML0 peer rumor `ConsensusEvent` (`ConsensusRumorHandlers.scala:24-26`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested event remains a separate semantic artifact/preimage gap; O18 open. |
| `Ml0FacilityEnvelope` | ML0 peer declaration (`ConsensusRumorHandlers.scala:28-30`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested load-bearing declaration remains a semantic-manifest gap; O18 open. |
| `Ml0ProposalEnvelope` | ML0 peer declaration (`ConsensusRumorHandlers.scala:32-34`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested load-bearing proposal remains a semantic-manifest gap; O18 open. |
| `Ml0MajoritySignatureEnvelope` | ML0 peer declaration (`ConsensusRumorHandlers.scala:36-38`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested load-bearing signature remains a semantic-manifest gap; O18 open. |
| `Ml0BinarySignatureEnvelope` | ML0 peer declaration (`ConsensusRumorHandlers.scala:40-42`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested load-bearing signature remains a semantic-manifest gap; O18 open. |
| `Ml0AckEnvelope` | ML0 peer declaration acknowledgement (`ConsensusRumorHandlers.scala:44-46`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested acknowledgement remains a semantic-manifest gap; O18 open. |
| `Ml0WithdrawEnvelope` | ML0 peer declaration withdrawal (`ConsensusRumorHandlers.scala:48-50`) | Exact nested identity/type; origin and sequence | Inherits peer-rumor gaps. The nested withdrawal remains a semantic-manifest gap; O18 open. |
| `Ml0ArtifactAnnouncementEnvelope` | ML0 common rumor `ConsensusArtifact` (`ConsensusRumorHandlers.scala:52-54`) | Exact nested identity/type | Inherits common-rumor gaps. The nested ML0 artifact remains a semantic-manifest gap; O18 open. |
| `PeerRumorInquiryRequest` | generic peer-rumor anti-entropy request (`schema/gossip.scala:118-122`; `GossipRoutes.scala:35-48`) | Exact request/response correlation; origin and sequence cursors | No request identity; O18 open. |
| `CommonRumorOfferResponse` | common-rumor hash offer (`schema/gossip.scala:124-127`; `GossipRoutes.scala:56-61`) | Exact request/response correlation | Session-token response verification does not bind a request identity or turn offered hashes into portable artifact evidence; O18 open. |
| `QueryCommonRumorsRequest` | common-rumor fetch request (`schema/gossip.scala:129-132`; `GossipRoutes.scala:63-68`) | Exact request/response correlation | No request identity; O18 open. |
| `CommonRumorInitResponse` | common-rumor seen-hash bootstrap (`schema/gossip.scala:134-137`; `GossipRoutes.scala:70-74`) | Exact request/response correlation | Session-token response verification does not bind a request identity or turn seen hashes into portable artifact evidence; O18 open. |
| `PeerRumorResponseStream` | peer query and empty-body init streams (`GossipRoutes.scala:35-54`; `GossipClient.scala:51-63`) | Exact request/response correlation; exact nested identity/type; origin and sequence | No request identity or aggregate response byte/count cap. Every returned signed rumor still enters the live source-authentication path; O18 open. |
| `CommonRumorResponseStream` | common query response stream (`GossipRoutes.scala:63-68`; `GossipClient.scala:70-75`) | Exact request/response correlation and nested identity/type | No request identity or aggregate response byte/count cap. Every returned signed rumor still enters the live source-authentication path; O18 open. |
| `SidecarSubscribeRequest` | local gRPC subscription profile (`p2p/proto/sidecar.proto:383-419`) | Exact request/response correlation; subscription topics and role | No explicit request identity. This local control is production-load-bearing because readiness pauses production until both lanes acknowledge; it is not portable consensus evidence; O18 open. |
| `SidecarSubscribeStarted` | mandatory first local stream response (`p2p/proto/sidecar.proto:300-321`) | Exact request/response correlation; subscription topics, role, session, and generation | JVM validation checks exact role/topics and positive session/generation, but no request ID exists. This is local production gating only, never network or consensus authority; O18 open. |
| `ChainSyncSnapshotCarrier` | streamed protobuf `Snapshot` (`p2p/proto/sidecar.proto:26-44,473-475,547-550`) | Exact request/response correlation and nested identity/type | The libp2p connection authenticates the immediate peer, but request identity and returned-hash equality are not enforced; `finalized`/branch disposition lacks portable artifact binding; per-frame 16 MiB cap has no aggregate stream cap; O18 open. |
| `ChainSyncMetagraphBinaryCarrier` | streamed `MetagraphBinaryResponse` (`p2p/proto/sidecar.proto:529-542,551-563`) | Exact request/response correlation and nested identity/type | The libp2p connection authenticates the immediate peer, but no request identity exists and the response omits metagraph address/hash, so the caller reattaches requested context; no aggregate stream cap; O18 open. |
| `ChainSyncSelectionHint` | intersection, tip, and chain-point responses (`p2p/proto/sidecar.proto:478-510`) | Exact request/response correlation | Immediate-peer channel authentication does not make these unbound fields portable fork-choice evidence. No request identity; intersection ordinal is echoed from the requester and the server leaves declared peer-tip slot unset; O18 open. |
| `BootstrapMetadataHint` | latest snapshot/GSI and MPT tuple responses (`GlobalL0Service.scala:41-59,172-208`; `SnapshotClient.scala:64-125`) | Exact request/response correlation and nested identity/type | Peer-response middleware authenticates the serving peer, but no request identity or portable exact-Phase-2/component binding exists; metadata cannot authorize installation; O18 open. |
| `BootstrapPhase2StateBundle` | Target exact-state bundle; absent today (`CONSENSUS-PROTOCOL-TEST-PLAN.md:556-560`) | Exact request/response correlation, nested identity/type, exact Phase-2 checkpoint, historical qualification evidence, current density-selected-chain witness, and component completeness | Bundle and portable current-chain witness schemas plus transactional component contract are absent; no request identity; O18 open. |
| `BootstrapGenesisBundle` | Target exact genesis bundle; absent today | Exact request/response correlation, nested identity/type, exact genesis declaration, and component completeness | Schema and component contract absent; no request identity; O18 open. |

The event-gossip family is generic, not GL0-specific. Currency ML0 instantiates it
for `CurrencySnapshotEvent` (`modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/modules/HttpApi.scala:141-148`),
and GL0 instantiates the same carrier for `GlobalSnapshotEvent`
(`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/modules/HttpApi.scala:214-220`).
The carrier label therefore cannot imply either event's semantic authority.

The rumor carrier split is also not live yet. Every incoming signed raw rumor is
signature-validated, origin/seedlist checked, collateral checked, stored, and only
then dispatched (`GossipDaemon.scala:89-110,128-180`; `RumorValidator.scala:35-62`).
Peer-rumor origin/ordinal then keys ML0 declaration/signature/ack processing
(`infrastructure/consensus/state/RumorHandler.scala:63-103`). The target must define
a separate source-authentication artifact and canonical preimage before the outer
carrier can truthfully become `TransportCoordinationOnly`; deleting those checks
would be a security regression. Nakamoto mode skips the legacy HTTP pull rounds
(`GossipDaemon.scala:56-70`), while the sidecar path drops inbound rumors on a full
queue (`SidecarRumorBridge.scala:91-105`) and exact counter sequencing rejects a
later rumor after a gap (`RumorStorage.scala:162-200`). Recovery for that loss is
therefore an explicit open transport gap.

`SubscribeStarted` is not harmless bookkeeping merely because it is local. The JVM
requires it to be first, verifies the exact role/topics and positive session/generation
(`GossipStream.scala:299-349`), and the readiness state pauses or resumes snapshot
production based on both lanes (`SidecarSubscriptionReadiness.scala:130-159,161-213`).
That makes it a production-gating control, but never portable state-validity,
finality, or source-authorization evidence.

### ChainSync Source Shapes

This subordinate list is closed over the 12 protobuf message shapes referenced by
the current `ChainSyncOutbound` and `ChainSyncInbound` services. It is a source-shape
inventory, not a claim that any shape has final canonical bytes or authority.

| Current shape | Semantic parent | Current role/gap |
|---|---|---|
| `ChainPoint` | `ChainSyncSelectionHint` | Embedded `(hash, ordinal)` only; no root/parent or portable fork-choice proof. |
| `FetchSnapshotsRequest` | `ChainSyncSnapshotCarrier` | Outbound hash list; bounded to 64 by Go, but has no request identity. |
| `Snapshot` response | `ChainSyncSnapshotCarrier` | One response frame; returned identity/disposition is not correlated to the requested hash. |
| `FindIntersectionRequest` | `ChainSyncSelectionHint` | Up to 64 requester-supplied points; no request identity. |
| `FindIntersectionResponse` | `ChainSyncSelectionHint` | Peer/channel hint; matched ordinal is copied from the request (`protocol.go:262-279`). |
| `GetPeerTipRequest` | `ChainSyncSelectionHint` | Empty request; no correlation identity. |
| `PeerTipResponse` | `ChainSyncSelectionHint` | Declares `slot`, but the server populates only hash/ordinal (`protocol.go:302-305`). |
| `ServeSnapshotsRequest` | `ChainSyncSnapshotCarrier` | Local sidecar-to-JVM hash-list request; no request identity. |
| `ServeChainPointsRequest` | `ChainSyncSelectionHint` | Empty local request; no request identity. |
| `ServeChainPointsResponse` | `ChainSyncSelectionHint` | Local chain-point/tip response, not portable fork-choice evidence. |
| `FetchMetagraphBinariesRequest` | `ChainSyncMetagraphBinaryCarrier` | Address plus value hashes; no request identity. |
| `MetagraphBinaryResponse` | `ChainSyncMetagraphBinaryCarrier` | Contains bytes only, so address/hash context is reattached by the caller. |

The definitions and service edges are
`p2p/proto/sidecar.proto:467-563`. The libp2p stream authenticates its immediate
peer, but it does not individually sign or bind the response fields as portable
consensus evidence (`p2p/internal/chainsync/protocol.go:106-183`). A 16 MiB frame
cap is not an aggregate response bound.

`ExactPhase2Checkpoint` means the one hash-bound
`(ordinal, hash, parentHash, mptRoot)` target. `ExactPhase2QualificationEvidence`
means the decided-attestation or depth-`k1` evidence for that same target, not a
peer-supplied `finalized` boolean or ordinal watermark. `ComponentCompleteness`
requires the declared bundle population to be complete and identity-bound; a
receiver cannot assemble authoritative state by mixing independently selected
components or peers.

Historical qualification is insufficient for bootstrap because it remains valid
after the exact hash is orphaned. `ChainSelectionWitness` therefore requires an
objective portable tower/fork-choice proof that the target is on the verifier's
current density-selected lineage (`CONSENSUS-ARTIFACT-LIFECYCLE.md:317-346`). This
is maxvalid/tower evidence, not a BFT vote, lock, or QC. The receiver verifies the
portable witness, acquires its own non-serializable `CanonicalPhase2Lease`, and
installs only through local `commitIfCurrent`. A peer-carried local lineage revision
would have no portable meaning and is deliberately not a carrier binding.

The current rumor discriminator is `typeOf[A].dealias.toString`
(`schema/gossip.scala:57-63`), and both raw rumor preimages concatenate variable
fields without framing (`:85-107`). ChainSync is length-prefixed protobuf with a
current 16 MiB message limit, but has no request identity in the response shapes
(`p2p/internal/chainsync/protocol.go:1-39`). That limit is per frame: both response
loops read until EOF and accumulate without `maxAggregateFetchBytes`
(`protocol.go:346-364,524-541`), and the JVM materializes both streams with
`.toList` (`ChainSyncManager.scala:72-90,98-119`). Those facts are audit evidence,
not the target canonical contract. O18 still decides final tags,
canonical/transport bytes, per-frame and aggregate limits, compression,
descriptors, chunking, correlation encoding, and retention.

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
- O18 now has explicit semantic-family inventory coverage for all 29 audited
  generic-rumor, sidecar-subscription, event-gossip, ML0-consensus-rumor,
  ChainSync, and bootstrap carrier rows, plus the 12 current ChainSync source shapes.
  Their final tags, bytes, limits, compression, correlation, descriptors, chunking,
  and retention remain owner-blocked. The finality durability umbrella has exact
  subordinate inventory coverage for all 14 `FinalityArtifactKind` variants, but
  ten payload schemas and their semantic verifiers remain open.
- The validator's authority-class fallback is intentionally coarse outside the
  critical shard, optimistic-tip, and tower rows. E1.1 must make the per-kind
  binding table total and mutation-test every family.

## Activation Gate

`ManifestCodecStatus.Open` and `ManifestActivationStatus.DarkOnly` are load-bearing
status markers. Activation still requires the atomic GL0/GL1/ML0/CL1/DL1 cutover,
complete bounded codecs and vectors, one branch-bound era service, removal of
active JSON/Kryo authority, and the protocol test-plan SER gates.
