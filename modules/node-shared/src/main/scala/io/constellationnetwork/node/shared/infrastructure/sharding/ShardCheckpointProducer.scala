package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.charset.StandardCharsets
import java.security.{KeyPair, MessageDigest}

import cats.Applicative
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.Try

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.node.shared.domain.nakamoto.{ActiveOperatorConsensusKeys, OperatorConsensusKeyRegistry}
import io.constellationnetwork.node.shared.domain.snapshot.finality.CanonicalLineageRevision
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}
import io.constellationnetwork.serde.codecs.EitherCodec.either
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs.{currencyIncrementalSnapshotCodec, currencySnapshotCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoCodec
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateRefCodec.{codec => globalSnapshotStateRefCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger
import scodec.Codec
import scodec.bits.ByteVector
import shapeless.{::, HNil}

/** Sidecar publisher for `Signed[ShardCheckpoint]` envelopes produced by [[ShardCheckpointProducer]].
  *
  * Decouples the producer's pure logic from the on-wire gossip path. Production wiring will pass a sidecar-backed publisher (Slice 14 —
  * `pb.ShardCheckpoint` over the per-shard libp2p GossipSub topic `shard-checkpoint-<shardId>`); tests pass [[noop]] or [[recording]].
  *
  * '''Why a single-method trait''' — the producer is content-agnostic; it has one job, "hand the envelope to whoever is responsible for
  * gossiping it". Matching the [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.Publisher]] shape so the same
  * pattern is consistent across slices.
  */
trait ShardCheckpointPublisher[F[_]] {

  /** Publish a produced (signed) checkpoint envelope. Failures are the publisher's responsibility to log + swallow — they MUST NOT block
    * the producer's slot-leader path (mirrors the `MetagraphCommitteeGate.Publisher` failure model).
    */
  def publish(checkpoint: Signed[ShardCheckpoint]): F[Unit]
}

object ShardCheckpointPublisher {

  /** No-op publisher. Useful when the producer is wired in a context that doesn't gossip (e.g. unit tests that only inspect the produced
    * envelope via `produce(...)`'s return value).
    */
  def noop[F[_]: Applicative]: ShardCheckpointPublisher[F] =
    (_: Signed[ShardCheckpoint]) => Applicative[F].unit

  /** Recording publisher: every published checkpoint is appended (in publish order) to a `Ref`. The returned
    * `F[List[Signed[ShardCheckpoint]]]` snapshots the current recorded list — repeated calls reflect any new publishes that happened in
    * between. Pattern matches the `Ref`-backed publisher stubs already in use by `MetagraphCommitteeGateSuite` (see `Publisher.recording`
    * in tests).
    */
  def recording[F[_]: Sync]: F[(ShardCheckpointPublisher[F], F[List[Signed[ShardCheckpoint]]])] =
    Ref.of[F, List[Signed[ShardCheckpoint]]](List.empty).map { ref =>
      val publisher: ShardCheckpointPublisher[F] =
        (cp: Signed[ShardCheckpoint]) => ref.update(_ :+ cp)
      val read: F[List[Signed[ShardCheckpoint]]] = ref.get
      (publisher, read)
    }

  /** Slice 14: sidecar-backed publisher. Encodes the envelope via [[ShardCheckpointWireCodecs.signedShardCheckpointToWire]] and forwards to
    * `SidecarClient.publishShardCheckpoint`.
    *
    * '''Failure model''' (mirrors `MetagraphCommitteeGate.Publisher`):
    *   - Wire-codec failures (`JsonSerializer` round-trip throws): logged + swallowed. The producer's slot-leader path MUST NOT block on a
    *     publish failure.
    *   - gRPC failure (`ok = false` response or future-side exception): logged + swallowed.
    *
    * Failures are intentionally not propagated up — the producer's contract is "fire-and-forget once we've signed". A persistent publisher
    * failure is observable in cluster-level metrics (Slice 9's gl0 acceptance manager will record absent shard checkpoints), not by raising
    * errors at the producer.
    */
  def sidecar[F[_]: Async: io.constellationnetwork.json.JsonSerializer: org.typelevel.log4cats.Logger](
    sidecarClient: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra[F]
  ): ShardCheckpointPublisher[F] = {
    val logger = org.typelevel.log4cats.Logger[F]
    new ShardCheckpointPublisher[F] {
      def publish(checkpoint: Signed[ShardCheckpoint]): F[Unit] =
        ShardCheckpointWireCodecs
          .signedShardCheckpointToWire[F](checkpoint)
          .flatMap { wire =>
            sidecarClient.publishShardCheckpoint(wire).flatMap { resp =>
              if (resp.ok)
                // Observability seam: the publish itself is otherwise silent (the producer's
                // `produce: emitted ...` INFO fires regardless of whether this sidecar call
                // succeeded). A dedicated INFO here makes the actual on-wire gossip publish
                // visible per shard/ordinal so cross-node propagation can be traced end-to-end.
                logger.info(
                  s"ShardCheckpointPublisher.sidecar: published " +
                    s"shard=${checkpoint.value.shardId.value.value} shardOrd=${checkpoint.value.shardOrdinal.value} " +
                    s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}"
                )
              else
                logger.warn(
                  s"ShardCheckpointPublisher.sidecar: sidecar PublishShardCheckpoint returned not-ok " +
                    s"(shardId=${checkpoint.value.shardId.value.value} shardOrdinal=${checkpoint.value.shardOrdinal.value} " +
                    s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}): ${resp.error}"
                )
            }
          }
          .handleErrorWith { err =>
            logger.warn(
              s"ShardCheckpointPublisher.sidecar: publish failed for " +
                s"shardId=${checkpoint.value.shardId.value.value} shardOrdinal=${checkpoint.value.shardOrdinal.value} " +
                s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}: ${err.getMessage}"
            )
          }
    }
  }
}

/** Produces (and signs, and publishes) one [[ShardCheckpoint]] per call — slice 8 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.
  *
  * '''Scope''' (per slice 8 task spec):
  *   1. Take the queue of pending per-MG SC binary snapshots assigned to this shard (input — caller manages the queue).
  *   1. Derive the shuffled staircase duty order for `(shardEta, shardOrdinal)`. If this node does not own the current duty window, return
  *      `None` and do not publish.
  *   1. If on duty, compute the [[ShardDerivedStateDelta]] via the injected complete-batch derivation callback (slice 8 doesn't reach into
  *      `TokenLockStateManager` etc. directly — that wiring is slice 9 / 13). The scalar callback remains only as a compatibility fallback.
  *   1. Read `parentCheckpointHash` and `parentShardOrdinal` from `chainStore.bestTip` (genesis = `Hash.empty` + `ShardOrdinal.Root` for
  *      the first checkpoint).
  *   1. Build a single [[CommitteeMemberSignature]] attaching this node's VRF proof + Ed25519 sig + KES product sig over the canonical
  *      [[ShardCheckpointSigPreimage]] bytes (`signingPreimage`).
  *   1. Wrap in [[Signed]] with the operator's standard long-term Ed25519 [[SignatureProof]] (consumer-facing envelope shape, mirrors how
  *      the gl0 leader signs `GlobalIncrementalSnapshot`).
  *   1. Publish via [[ShardCheckpointPublisher.publish]] and return the produced envelope.
  *
  * '''What this slice does NOT do''':
  *   - Manage the pending-snapshot queue (caller-supplied via `pendingSnapshots`).
  *   - Reach directly into `TokenLockStateManager`, `MetagraphSyncManager` etc. — those slot in via `derivePerMgState` callback. Slice 9 /
  *     13 will wire the actual derivation closure.
  *   - Aggregate signatures from other committee members. v1 only attaches THIS node's sig; others sign + gossip in slice 14.
  *   - Wire the GossipSub side — that's slice 14. Uses [[ShardCheckpointPublisher]] abstraction here.
  *   - Modify `GlobalSnapshotAcceptanceManager` (GSAM). Wiring into GSAM is slice 9 / 13.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh producer for the shard layer. No compat ceremony.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - All knobs come from constructor params; production wiring will pass typed `SharedConfig.nakamoto.sharding.*` values. No
  *     `sys.env.get` anywhere.
  *
  * '''Use Hasher rule''' (per `[[feedback-use-hasher-no-manual-serialize]]`):
  *   - Canonical preimage hash routes through `Hasher[F].hash(ShardCheckpointSigPreimage)` via `checkpoint.signingPreimage` (defined on the
  *     envelope itself in slice 1). No hand-rolled Blake2b / byte-concat surface for the signing bytes.
  */
trait ShardCheckpointProducer[F[_]] {

  /** Try to produce + sign + publish one checkpoint for this shard.
    *
    * Inputs:
    *   - `pendingSnapshots`: per-MG queued SC binary chains for the MGs in this shard. Caller manages the queue; producer reads it once,
    *     emits the checkpoint, leaves the queue alone (caller drops accepted entries after observing the produced envelope).
    *   - `gl0AnchorOrdinal`: legacy-named inclusion-height hint. It is not the replay base and never permits execution against a later
    *     receiver head; [[ShardCheckpoint.executionBase]] carries the complete claimed replay-state identity, whose Phase-2 authority must
    *     be authenticated separately.
    *   - `epoch`: the sortition epoch the local committee was drawn from (passed through to the envelope's `epoch` field so verifiers can
    *     look up the right active set for VRF verification).
    *
    * Returns:
    *   - `Some(checkpoint)` if this node owns the deterministic staircase duty window and has replayable content.
    *   - `None` if another committee member is on duty or `pendingSnapshots` is empty. Empty checkpoints are never emitted.
    */
  def produce(
    pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    gl0AnchorOrdinal: SnapshotOrdinal,
    epoch: EtaPeriod,
    currentSlot: Slot,
    committee: Set[PeerId]
  ): F[Option[Signed[ShardCheckpoint]]]
}

object ShardCheckpointProducer {

  /** Tier-1 idempotence memo (task #45, run-19). The single in-flight checkpoint this node has already minted for the ordinal it would mint
    * next, plus the produce-tick at which it was last (re-)published. While held, the producer re-publishes these exact bytes
    * (cadence-gated) instead of re-minting — re-minting churns `gl0AnchorOrdinal`+`slot` into a fresh hash every tick (run-19: 34 variants
    * for one ordinal), splitting committee attestations below `kQuorum`. Single (not a `Map`) because the protocol permits exactly one
    * outstanding checkpoint: no child is minted until the exact containing GL0 snapshot reaches Phase 2. Dropped when that checkpoint
    * becomes operational or the Phase-2 shard anchor moves out from under it (detected via `parentCheckpointHash`, including sibling
    * reorg).
    */
  private final case class HeldCheckpoint(
    signed: Signed[ShardCheckpoint],
    lastPublishedTick: Long,
    localGlobalLineageRevision: CanonicalLineageRevision
  )

  /** Exact retained GL0 execution base used by one checkpoint attempt. The state reference, rooted state-channel tips, complete prior
    * currency states, and rooted global balances must be materialized from the same immutable reader view; `None` means that view is
    * unavailable and production must defer without replaying or signing.
    */
  final case class PinnedExecutionBase(
    stateRef: GlobalSnapshotStateRef,
    perMgTips: SortedMap[Address, Hash],
    priorCurrencySnapshots: SortedMap[
      Address,
      Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
    ],
    balances: SortedMap[Address, Balance]
  )

  private type CurrencyState =
    Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]

  private val signedCurrencySnapshotCodec: Codec[Signed[CurrencySnapshot]] =
    signedCodecFor(currencySnapshotCodec)
  private val signedCurrencyIncrementalSnapshotCodec: Codec[Signed[CurrencyIncrementalSnapshot]] =
    signedCodecFor(currencyIncrementalSnapshotCodec)
  private val incrementalWithInfoCodec: Codec[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)] =
    (signedCurrencyIncrementalSnapshotCodec :: currencySnapshotInfoCodec)
      .xmap[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)](
        { case snapshot :: info :: HNil => (snapshot, info) },
        { case (snapshot, info) => snapshot :: info :: HNil }
      )
  private val currencyStateCodec: Codec[CurrencyState] =
    either(signedCurrencySnapshotCodec, incrementalWithInfoCodec)
  private val pinnedExecutionBaseCodec: Codec[PinnedExecutionBase] =
    (globalSnapshotStateRefCodec ::
      sortedMap(addressCodec, hashCodec) ::
      sortedMap(addressCodec, currencyStateCodec) ::
      sortedMap(addressCodec, Codec[Balance]))
      .xmap[PinnedExecutionBase](
        { case stateRef :: tips :: currency :: balances :: HNil => PinnedExecutionBase(stateRef, tips, currency, balances) },
        base => base.stateRef :: base.perMgTips :: base.priorCurrencySnapshots :: base.balances :: HNil
      )
  private val pinnedExecutionBaseIdentityDomainV1: ByteVector =
    ByteVector.view("tessellation/shard-checkpoint/pinned-execution-base/v1\u0000".getBytes(StandardCharsets.US_ASCII))

  /** Capture the complete retained base as domain/version-tagged canonical bytes. Case-class equality is invalid here because
    * framework-with-data currency snapshots contain `Array[Byte]`; independent decoding produces byte-identical arrays with
    * reference-unequal JVM objects. Encoding is fallible at this boundary so a future shape/codec violation defers instead of signing.
    */
  private def pinnedExecutionBaseIdentity(base: PinnedExecutionBase): Either[String, Array[Byte]] =
    Try(pinnedExecutionBaseCodec.encode(base)).toEither
      .leftMap(error => s"exception: ${error.getMessage}")
      .flatMap(_.toEither.leftMap(error => s"scodec: ${error.messageWithContext}"))
      .map(bits => (pinnedExecutionBaseIdentityDomainV1 ++ bits.toByteVector).toArray)

  /** Algebra describing the KES product signer for this operator. Mirrors
    * [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSigner]] — the producer doesn't depend on the concrete
    * `OperationalKeyMakerAlgebra` so the test path can stub it.
    *
    * '''Why we use this trait instead of a raw `SecretKeyKesProduct`.''' The design-doc-style signature in the slice 8 task spec proposes
    * passing the secret-key bytes directly. That doesn't work for our codebase because `SecretKeyKesProduct` is `private[kes]` (see
    * `models.scala` scaladoc — "package-private: callers must not handle secret-key material directly — the read-once lifecycle is enforced
    * by [[OperationalKeyMakerAlgebra]]"). The right abstraction is the existing `KesSigner` trait, which `OperationalKeyMakerAlgebra` is a
    * natural producer of. The production adapter resolves the preregistered operator pair and derives the exact tree-relative step from the
    * checkpoint epoch before signing; mutable local `currentPeriod` is never the wire-step authority.
    */
  trait KesSigner[F[_]] {
    def sign(operatorKeys: OperatorConsensusKeys, checkpointEpoch: EtaPeriod, message: Array[Byte]): F[Option[KesSignature]]
  }

  final case class KesSignature(treeStep: Int, bytes: Array[Byte])

  /** Construct a [[ShardCheckpointProducer]].
    *
    * @param shardId
    *   the shard this producer is scoped to. Immutable; one producer instance per shard the operator participates in.
    * @param chainStore
    *   per-shard chain store (Slice 5). Read to determine `parentCheckpointHash` + `parentShardOrdinal` for the new checkpoint.
    * @param slotLeader
    *   per-shard eta, staircase-duty, and membership-proof helper.
    * @param publisher
    *   gossip-path abstraction. Production wires a sidecar-backed impl (slice 14); tests pass [[ShardCheckpointPublisher.recording]] or
    *   [[ShardCheckpointPublisher.noop]].
    * @param selfPeerId
    *   this operator's identity. Carried on the produced [[CommitteeMemberSignature.peerId]].
    * @param selfKeyPair
    *   this operator's long-term Ed25519 keypair. Used for both the outer [[Signed]] envelope's [[SignatureProof]] and the inner
    *   [[CommitteeMemberSignature.ed25519Sig]] over the canonical preimage hash.
    * @param selfVrfSk
    *   this operator's registered VRF secret. Supplies the key-possession proof carried on [[CommitteeMemberSignature.vrfProof]]; it does
    *   not decide execution-shard membership or producer duty.
    * @param selfVrfVk
    *   the verification key derived locally with `selfVrfSk`. Production and re-publication remain disabled unless this is exactly the
    *   registered 32-byte VRF key for `selfPeerId`.
    * @param operatorKeyRegistry
    *   frozen-genesis atomic KES+VRF identity registry used by the producer-side fail-closed gate. Historical candidate-parent activation
    *   remains required before runtime key rotation can replace this lookup.
    * @param kesSigner
    *   this operator's KES product signer. Yields the `kesTreeStep` + `kesProductSig` carried on the envelope's
    *   [[CommitteeMemberSignature]]. The injectable trait shape keeps the producer testable without a full KES bootstrap.
    * @param shardEtaFor
    *   `epoch => F[shardEta]` — resolves the 32-byte shard eta for the given eta period. The producer calls this once per `produce(...)`
    *   keyed on the checkpoint's own `epoch`; production wiring closes over `etaForPeriod(epoch) => computeShardEta(shardId, gl0Eta)`.
    *
    * '''Determinism invariant (Slice S4).''' The eta MUST be resolved for the CHECKPOINT'S epoch (the `epoch` arg threaded onto the
    * envelope), NOT the current wall-clock period — a checkpoint produced near an eta boundary may be verified after the boundary, and
    * producer + every verifier must derive the same shard eta or registered-key proof verification disagrees and the shard chain stalls.
    * The caller derives the checkpoint epoch from the shard fan-out's consensus chain-period input. The corresponding eta is fixed before
    * the period becomes usable and remains resolvable after a boundary.
    * @param staircaseDeltaSlots
    *   width of each staircase rank's proposal window, in slots (design §5.7 rev 2; owner default 5). HOCON
    *   `nakamoto.sharding.checkpoint.staircase-delta-slots`.
    * @param derivePerMgState
    *   injectable per-MG derivation that re-runs the metagraph's currency derivation over its full included SC-binary chain at the pinned
    *   finalized base and returns the canonical per-MG MPT root. Current ordinary GL0 adoption independently replays the same included
    *   snapshots and compares its local root with `perMetagraphMptRoots`; that is transitional containment, not the target sharding model.
    *   Target execution signers and assigned watchtowers replay, while an ordinary noncommittee GL0 adopter verifies the replay
    *   certificate, applies the namespace-confined canonical diff, and recomputes the root. Universal GL0 execution of direct native
    *   GL1/DAG-token transitions is a separate invariant and is never removed by this path.
    *
    * The implicit `Hasher[F]` is required for the canonical preimage hash; `SecurityProvider[F]` is required for the Ed25519 sign path
    * (`Signing.signData`).
    */
  def make[F[_]: Async: Hasher: SecurityProvider](
    shardId: ShardId,
    chainStore: ShardChainStore[F],
    /** '''S2 — PINNED BASE-ANCHORED window (VERSION-MODEL §4).''' The ordinal, per-MG state-channel tips, prior currency states, and rooted
      * global balances come from one retained immutable GL0 reader. `chainLinkOrder` anchors each MG's binary window on those tips, and the
      * batch derivation consumes that exact captured base, so the first outer parent and execution prior cannot be sampled from different
      * retained views. `None` defers without replay or signature.
      */
    executionBaseF: F[Option[PinnedExecutionBase]],
    /** Re-resolves the exact captured execution-base identity immediately before signing. This must not resolve "latest": a newer canonical
      * descendant appearing during replay does not invalidate immutable base N and must not starve checkpoint production. Missing,
      * malformed, or byte-different state at the captured reference still defers before every signature.
      */
    executionBaseAt: GlobalSnapshotStateRef => F[Option[PinnedExecutionBase]],
    /** '''NEWNESS GATE — chain-wide per-MG checkpoint frontier (S2-deadlock fix, 2026-06-15; ord-26 re-freeze fix).''' Per-MG the latest
      * binary this chain has CHECKPOINTED across the noteAnchor-followed bestTip ancestry (`chainStore.lastCheckpointedPerMgTip`). Used
      * SOLELY to decide WHETHER an MG has content gl0 has not yet adopted; it does NOT anchor the window (that stays on the pinned
      * execution base for §4 replay correctness). The producer OMITS any MG whose finalized-base-anchored window does not extend PAST this
      * frontier (i.e. contains no binary carrying `lastSnapshotHash == frontier(mg)` — the frontier's child). Such an MG is a "stale
      * re-include": every binary in its window was already checkpointed (hence already adopted by gl0), so gl0's embed-match
      * (`GlobalSnapshotConsensusFunctions` `pick`, keyed on gl0's adopt tip) finds no continuation and DEFERS it — yet the checkpoint still
      * advanced the shard bestTip past `adoptedShardOrd`, tripping the outstanding-checkpoint gate into a permanent freeze (run-19..22
      * ord16, then ord26; verified live).
      *
      * '''Why the chain-wide frontier and NOT `lastNGlobalSnapshotStorage.getCombined`.''' The frontier is at-or-AHEAD of gl0's per-MG
      * adopt tip (gl0 only adopts checkpoints THIS chain minted, so minted ≥ adopted), is reorg-safe (bestTip follows `noteAnchor` = gl0's
      * adopted lineage), and does NOT revert when a checkpoint omits an MG (it walks back to the most recent checkpoint including that MG —
      * unlike `chainStore.perMgTip`, which reads only bestTip and reverts to genesis under a partial checkpoint, the run-27e regress
      * hazard). The latest-PRODUCED global GSI (`getCombined`) instead LAGS the in-flight per-MG adoptions, so it sits BEHIND the adopt tip
      * and lets a stale re-include slip the gate — the ord-26 re-freeze (the producer minted a window ending at gl0's just-advanced adopt
      * tip because `getCombined` had not caught up). Requiring the window to extend past the (≥ adopt-tip) frontier makes gl0's embed-match
      * continuation a guarantee, not a race. Tests wire this == the pinned base tips so the gate is a no-op (frontier == windowAnchor ⇒ the
      * window head is already the frontier's child).
      */
    adoptedPerMgTip: F[SortedMap[Address, Hash]],
    slotLeader: ShardSlotLeader[F],
    publisher: ShardCheckpointPublisher[F],
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    selfVrfSk: Array[Byte],
    selfVrfVk: Array[Byte],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    kesSigner: KesSigner[F],
    shardEtaFor: EtaPeriod => F[Array[Byte]],
    staircaseDeltaSlots: Int,
    derivePerMgState: (
      Address,
      NonEmptyList[Signed[StateChannelSnapshotBinary]],
      SnapshotOrdinal,
      GlobalSnapshotStateRef
    ) => F[Option[Hash]],
    derivePerMgStates: Option[
      (
        SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        SnapshotOrdinal,
        PinnedExecutionBase
      ) => F[SortedMap[Address, Option[Hash]]]
    ] = None,
    /** Process-local GL0 canonical-lineage observation. This is conservative retry invalidation only: the value is never serialized or
      * treated as portable finality evidence. A held checkpoint may be re-published only when both its mint observation and the current
      * observation are present and equal. Replacement, missing observation, or mismatch discards the held bytes without publishing.
      */
    localGlobalLineageRevision: F[Option[CanonicalLineageRevision]],
    /** Atomic `(ordinal, checkpointHash)` of the latest shard checkpoint whose exact containing GL0 snapshot reached Phase 2. Production
      * policy input only, never an artifact-validity condition. V1 permits exactly one outstanding checkpoint per shard: a producer may
      * mint the successor only when the selected shard tip exactly matches this anchor.
      */
    lastPhase2Checkpoint: F[Option[(ShardOrdinal, Hash)]],
    /** Loss-recovery re-publish cadence (task #45). While a checkpoint is held (already minted for the next ordinal), re-publish its exact
      * bytes every this-many `produce` ticks instead of re-minting. `cfg.nakamoto.sharding.checkpoint.republishEveryTicks`; 1 = every tick.
      */
    republishEveryTicks: Int
  ): F[ShardCheckpointProducer[F]] = Async[F].delay {
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardCheckpointProducer[$shardId]")

    // Tier-1 idempotence state (task #45): the single held checkpoint + a monotone produce-tick counter, both per-shard (one producer per
    // shard). `Ref.unsafe` inside this `delay` is the codebase idiom for producer-local state (see AbandonmentTracker / ViewChangeManager) —
    // the allocation is suspended with the rest of the block, so it's pure at the `make` boundary.
    val heldRef: Ref[F, Option[HeldCheckpoint]] = Ref.unsafe(None)
    val tickRef: Ref[F, Long] = Ref.unsafe(0L)

    // The producer-local memo and logger are allocated once per shard.
    new ShardCheckpointProducer[F] {

      /** Frozen-genesis identity gate. A checkpoint validity signature, including a held-checkpoint re-publish, is permitted only while the
        * locally derived VRF VK exactly matches the key registered for this operator. The candidate-parent historical E2K registry must
        * replace this frozen lookup before runtime key activation/rotation is consensus-load-bearing.
        */
      private def resolveLocalOperatorKeys(epoch: EtaPeriod): F[Option[OperatorConsensusKeys]] =
        ActiveOperatorConsensusKeys
          .resolve(operatorKeyRegistry, selfPeerId, epoch)
          .map(
            _.filter(keys =>
              PeerId.fromPublic(selfKeyPair.getPublic) === selfPeerId &&
                java.security.MessageDigest.isEqual(keys.vrfPublicKey.toBytes, selfVrfVk)
            )
          )

      private def withStableExecutionBase(
        beforeBase: PinnedExecutionBase,
        beforeIdentity: Array[Byte],
        gl0AnchorOrdinal: SnapshotOrdinal
      )(onStable: => F[Option[Signed[ShardCheckpoint]]]): F[Option[Signed[ShardCheckpoint]]] =
        executionBaseAt(beforeBase.stateRef).flatMap {
          case None =>
            logger
              .info(
                s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=execution-base-moved " +
                  s"before=(${beforeBase.stateRef.ordinal.value.value},${beforeBase.stateRef.hash.value.take(12)},${beforeBase.perMgTips.size}) " +
                  "after=None; defer to next slot"
              )
              .as(None: Option[Signed[ShardCheckpoint]])
          case Some(afterBase) =>
            pinnedExecutionBaseIdentity(afterBase) match {
              case Left(error) =>
                logger
                  .warn(
                    s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                      s"reason=execution-base-identity-encode-failed detail=$error"
                  )
                  .as(None: Option[Signed[ShardCheckpoint]])
              case Right(afterIdentity) if !MessageDigest.isEqual(beforeIdentity, afterIdentity) =>
                logger
                  .info(
                    s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=execution-base-moved " +
                      s"before=(${beforeBase.stateRef.ordinal.value.value},${beforeBase.stateRef.hash.value.take(12)},${beforeBase.perMgTips.size}) " +
                      s"after=(${afterBase.stateRef.ordinal.value.value},${afterBase.stateRef.hash.value.take(12)},${afterBase.perMgTips.size}); " +
                      "defer to next slot"
                  )
                  .as(None: Option[Signed[ShardCheckpoint]])
              case Right(_) => onStable
            }
        }

      def produce(
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        epoch: EtaPeriod,
        currentSlot: Slot,
        committee: Set[PeerId]
      ): F[Option[Signed[ShardCheckpoint]]] =
        resolveLocalOperatorKeys(epoch).flatMap {
          case None =>
            logger
              .warn(
                s"produce-skip shard=${shardId.value.value} reason=unregistered-or-mismatched-local-vrf-identity " +
                  s"peer=${selfPeerId.value.value.take(16)}"
              )
              .as(None: Option[Signed[ShardCheckpoint]])
          case Some(operatorKeys) =>
            // Tier-1 idempotence (task #45): bump the per-produce tick once, then resolve the parent. This path must run even when the current
            // binary buffer is empty: an already-held checkpoint is an outbox item and must continue to be re-published until its exact
            // Phase-2 anchor arrives. Empty input prevents only a NEW mint. The identity gate above intentionally also covers this re-publish.
            tickRef.updateAndGet(_ + 1L).flatMap { tick =>
              (chainStore.bestTip, adoptedPerMgTip, lastPhase2Checkpoint, localGlobalLineageRevision, heldRef.get).tupled.flatMap {
                case (bestTipOpt, adoptedTips, phase2CheckpointOpt, currentLineageRevision, heldOpt) =>
                  val phase2Ordinal = phase2CheckpointOpt.map(_._1.value).getOrElse(ShardOrdinal.Root.value)
                  val bestOrdinal = bestTipOpt.map(_.signed.value.shardOrdinal.value).getOrElse(ShardOrdinal.Root.value)
                  val bestMatchesPhase2 = (bestTipOpt, phase2CheckpointOpt) match {
                    case (None, None)                   => true
                    case (Some(tip), Some((ord, hash))) => tip.signed.value.shardOrdinal === ord && tip.hash === hash
                    case _                              => false
                  }
                  val phase2AnchorNeedsRecovery = phase2CheckpointOpt.exists {
                    case (ord, hash) =>
                      bestTipOpt.forall { tip =>
                        tip.signed.value.shardOrdinal.value < ord.value ||
                        (tip.signed.value.shardOrdinal === ord && tip.hash =!= hash)
                      }
                  }

                  def awaitEmbed: F[Option[Signed[ShardCheckpoint]]] =
                    logger
                      .info(
                        s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=awaiting-embed " +
                          s"tipShardOrd=$bestOrdinal phase2ShardOrd=$phase2Ordinal"
                      )
                      .as(None: Option[Signed[ShardCheckpoint]])

                  def awaitShardRecovery: F[Option[Signed[ShardCheckpoint]]] =
                    logger
                      .warn(
                        s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=phase2-anchor-missing-or-mismatch " +
                          s"tipShardOrd=$bestOrdinal phase2ShardOrd=$phase2Ordinal"
                      )
                      .as(None: Option[Signed[ShardCheckpoint]])

                  def mintNext: F[Option[Signed[ShardCheckpoint]]] =
                    if (pendingSnapshots.isEmpty)
                      logger
                        .info(s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=empty-pending")
                        .as(None: Option[Signed[ShardCheckpoint]])
                    else
                      currentLineageRevision match {
                        case None =>
                          logger
                            .warn(
                              s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                "reason=global-lineage-unavailable-before-replay"
                            )
                            .as(None: Option[Signed[ShardCheckpoint]])
                        case Some(mintLineageRevision) =>
                          produceInner(
                            bestTipOpt,
                            adoptedTips,
                            pendingSnapshots,
                            gl0AnchorOrdinal,
                            epoch,
                            currentSlot,
                            committee,
                            operatorKeys,
                            mintLineageRevision
                          ).flatMap {
                            case some @ Some(signed) =>
                              heldRef.set(Some(HeldCheckpoint(signed, tick, mintLineageRevision))).as(some)
                            case None => Async[F].pure(None: Option[Signed[ShardCheckpoint]])
                          }
                      }

                  def sameAvailableLineage(
                    minted: CanonicalLineageRevision,
                    observed: Option[CanonicalLineageRevision]
                  ): Boolean =
                    observed.contains(minted)

                  def discardHeldForLineage(
                    held: HeldCheckpoint,
                    observed: Option[CanonicalLineageRevision]
                  ): F[Option[Signed[ShardCheckpoint]]] =
                    heldRef.set(None) *>
                      logger
                        .warn(
                          s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=global-lineage-moved-or-unavailable " +
                            s"shardOrdinal=${held.signed.value.shardOrdinal.value} " +
                            s"mintLineage=${held.localGlobalLineageRevision.value.value} " +
                            s"currentLineage=${observed.map(_.value.value)}; discarded held checkpoint"
                        )
                        .as(None: Option[Signed[ShardCheckpoint]])

                  def republish(held: HeldCheckpoint): F[Option[Signed[ShardCheckpoint]]] =
                    if (tick - held.lastPublishedTick >= republishEveryTicks.toLong)
                      // Re-read immediately before the side effect. This local generation is a discard-only containment check, not a
                      // portable lease or consensus proof; full publish commit-if-current remains separate work.
                      localGlobalLineageRevision.flatMap { observedLineageRevision =>
                        if (sameAvailableLineage(held.localGlobalLineageRevision, observedLineageRevision))
                          publisher.publish(held.signed) *>
                            heldRef.set(Some(held.copy(lastPublishedTick = tick))) *>
                            logger
                              .info(
                                s"produce: re-publish-held shardOrdinal=${held.signed.value.shardOrdinal.value} " +
                                  s"gl0Anchor=${held.signed.value.gl0AnchorOrdinal.value.value} " +
                                  s"slot=${held.signed.value.slot.value.value} tick=$tick (single outstanding checkpoint)"
                              )
                              .as(Some(held.signed): Option[Signed[ShardCheckpoint]])
                        else discardHeldForLineage(held, observedLineageRevision)
                      }
                    else
                      logger
                        .info(
                          s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=held-cadence-wait " +
                            s"shardOrdinal=${held.signed.value.shardOrdinal.value} tick=$tick " +
                            s"lastPublished=${held.lastPublishedTick} republishEvery=$republishEveryTicks"
                        )
                        .as(None: Option[Signed[ShardCheckpoint]])

                  def withoutHeld: F[Option[Signed[ShardCheckpoint]]] =
                    if (phase2AnchorNeedsRecovery) awaitShardRecovery
                    else if (bestMatchesPhase2) mintNext
                    else awaitEmbed

                  heldOpt match {
                    case Some(held) if !sameAvailableLineage(held.localGlobalLineageRevision, currentLineageRevision) =>
                      discardHeldForLineage(held, currentLineageRevision)

                    case Some(held) =>
                      Hasher[F].hash(held.signed.value.signingPreimage).flatMap { heldHash =>
                        val heldIsSelected = bestTipOpt match {
                          case None =>
                            held.signed.value.shardOrdinal === ShardOrdinal.Root.next &&
                            held.signed.value.parentCheckpointHash === Hash.empty
                          case Some(tip) if tip.hash === heldHash => true
                          case Some(tip) =>
                            tip.signed.value.shardOrdinal.next === held.signed.value.shardOrdinal &&
                            held.signed.value.parentCheckpointHash === tip.hash
                        }
                        val heldReachedPhase2 = phase2CheckpointOpt.exists {
                          case (ord, checkpointHash) => ord === held.signed.value.shardOrdinal && checkpointHash === heldHash
                        }

                        if (heldReachedPhase2) heldRef.set(None) *> withoutHeld
                        else if (phase2AnchorNeedsRecovery) awaitShardRecovery
                        else if (heldIsSelected) republish(held)
                        else
                          // A competing checkpoint won at this ordinal (or the selected chain advanced). Never extend or republish the loser.
                          heldRef.set(None) *> withoutHeld
                      }

                    case None => withoutHeld
                  }
              }
            }
        }

      private def produceInner(
        bestTipOpt: Option[io.constellationnetwork.security.Hashed[ShardCheckpoint]],
        adoptedTips: SortedMap[Address, Hash],
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        epoch: EtaPeriod,
        currentSlot: Slot,
        committee: Set[PeerId],
        operatorKeys: OperatorConsensusKeys,
        expectedGlobalLineageRevision: CanonicalLineageRevision
      ): F[Option[Signed[ShardCheckpoint]]] = executionBaseF.flatMap {
        case None =>
          logger
            .warn(
              s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=pinned-execution-base-unavailable"
            )
            .as(None: Option[Signed[ShardCheckpoint]])
        case Some(executionBase) =>
          val windowAnchorTips = executionBase.perMgTips
          // Capture one retained gl0 reader view, use its complete currency/tip/balance state for the checkpoint batch, stamp the ordinal,
          // then re-read and byte-compare the complete canonical base identity after replay. Any change defers before validity signing.
          val parentHash: Hash = bestTipOpt.map(_.hash).getOrElse(Hash.empty)
          val parentOrd: ShardOrdinal = bestTipOpt.map(_.signed.value.shardOrdinal).getOrElse(ShardOrdinal.Root)
          // Parent slot is the signed wire slot. Staircase windows advance on the shared slot grid, not on GL0 snapshot cadence.
          val parentSlotOpt: Option[Slot] = bestTipOpt.map(_.signed.value.slot)
          val nextShardOrdinal: ShardOrdinal = parentOrd.next

          // R-2: chain-link-order the shard-buffered binaries from the pinned GL0 execution-base tips. MGs with no admissible continuation
          // this round are omitted; if nothing continues the base, there is nothing to checkpoint. The inversion concerns the input source:
          // binaries come from the shard buffer rather than GL0's post-chain-link `stateChannelSnapshots`, while execution lineage remains
          // anchored to the exact retained GL0 base being replayed.
          chainLinkOrder(pendingSnapshots, windowAnchorTips).flatMap { chained =>
            // ── NEWNESS GATE (S2-deadlock fix, 2026-06-15; runs 19-22) ───────────────────────────────────────────────────────────
            // Keep only MGs whose finalized-base-anchored window EXTENDS PAST gl0's adopt tip — i.e. carries a binary whose
            // `lastSnapshotHash == adoptTip(mg)` (the adopt tip's child = genuine new content gl0 hasn't adopted). An MG that fails
            // this is a STALE RE-INCLUDE: every binary in its window is already adopted, so gl0's embed-match (`pick`, keyed on the
            // SAME adopt tip) finds no continuation and DEFERS it — yet minting it advances the shard bestTip past `adoptedShardOrd`
            // and trips the outstanding-checkpoint gate into a permanent freeze (verified live: ord16 mgs=1 minted in the 0.6s post-adoption
            // gossip race, frozen ~1.5h while ml0 kept producing). The window for surviving MGs is UNCHANGED (full base->latest, §4 replay
            // input intact) — the gate only decides inclusion, never trims. `adoptTip` defaults to `Hash.empty` at genesis, where the
            // window head IS the genesis binary so the gate passes. Omitting a no-new-content MG cannot regress it (gl0 keeps its tip)
            // and is NOT the run-27e hazard (which omitted a DERIVABLE-new MG on a genesis-derive race — caught later in assembleDelta).
            val orderedSnapshots = chained.filter {
              case (mg, nel) => nel.exists(_.value.lastSnapshotHash === adoptedTips.getOrElse(mg, Hash.empty))
            }
            val staleOmitted: List[Address] = (chained.keySet -- orderedSnapshots.keySet).toList
            val logStaleOmit: F[Unit] =
              logger
                .info(
                  s"produce: newness-gate omitted ${staleOmitted.size} stale-re-include mg(s) (window already fully adopted by gl0) " +
                    s"gl0Anchor=${gl0AnchorOrdinal.value.value} omittedMgs=${staleOmitted.map(_.value.value.take(8)).mkString(",")} " +
                    s"adoptTips=${adoptedTips.toList.map { case (mg, h) => s"${mg.value.value.take(8)}:${h.value.take(8)}" }.mkString(",")}"
                )
                .whenA(staleOmitted.nonEmpty)
            if (orderedSnapshots.isEmpty)
              logStaleOmit *> logger
                .info(
                  s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                    s"reason=${if (chained.isEmpty) "no-chain-link" else "nothing-new-past-adopt-tip"} " +
                    s"pendingMgs=${pendingSnapshots.size} " +
                    s"pendingCounts=${pendingSnapshots.toList.map { case (mg, nel) => s"${mg.value.value.take(8)}:${nel.size}" }
                        .mkString(",")} " +
                    s"baseTips=${windowAnchorTips.toList.map { case (mg, h) => s"${mg.value.value.take(8)}:${h.value.take(8)}" }.mkString(",")}"
                )
                .as(None: Option[Signed[ShardCheckpoint]])
            else
              logStaleOmit *>
                // Resolve the shard registered-key proof eta for the checkpoint's own `epoch`, not a wall-clock period. The eta is fixed
                // before use and every verifier keys the same lookup on the
                // wire-carried `checkpoint.epoch` — derives byte-identical bytes. This is the load-bearing determinism invariant: if
                // producer + verifier disagreed on shardEta, registered-key proof verification would reject and the shard chain stalls.
                shardEtaFor(epoch).flatMap { shardEta =>
                  // ─── SHUFFLED STAIRCASE (owner, 2026-06-12; design §5.7 rev 2 — replaces the LDD lottery) ───
                  // Deterministic duty schedule: the committee is hash-sorted per (shardEta, NEXT shardOrdinal); rank r
                  // is on duty for delta slots starting 1 slot after the parent's wire slot, wrapping modulo committee
                  // size (liveness needs ONE live member; censorship bounded by rotation). UNIQUE producer per window —
                  // genesis included — which is what a lottery can never give a small committee at per-slot draws
                  // (run-13/14: genesis forks + same-ord sibling lineages split attestations below kQuorum).
                  // Window selection is delegated to the same pure function receive-side duty validation uses.
                  slotLeader.dutyOrder(committee.toList.sortBy(_.value.value), shardEta, nextShardOrdinal).flatMap { ordered =>
                    ShardSlotLeader.scheduledDuty(ordered, currentSlot, parentSlotOpt, staircaseDeltaSlots) match {
                      case Left(error) =>
                        logger
                          .warn(
                            s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=invalid-duty-input " +
                              s"slot=${currentSlot.value.value} detail=${error.diagnostic}"
                          )
                          .as(None: Option[Signed[ShardCheckpoint]])
                      case Right(duty) if duty.peerId =!= selfPeerId =>
                        logger
                          .info(
                            s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=not-on-duty " +
                              s"slot=${currentSlot.value.value} gap=${duty.slotGap} dutyRank=${duty.rank} " +
                              s"onDuty=${duty.peerId.value.value.take(12)} chainedMgs=${orderedSnapshots.size}"
                          )
                          .as(None: Option[Signed[ShardCheckpoint]])
                      case Right(_) =>
                        slotLeader.membershipProof(selfVrfSk, shardEta, currentSlot).flatMap { vrfProof =>
                          // Capture the complete canonical base identity BEFORE replay. Framework-with-data snapshots contain raw arrays,
                          // so JVM object equality would reject independently decoded identical state and miss in-place byte mutation.
                          pinnedExecutionBaseIdentity(executionBase) match {
                            case Left(error) =>
                              logger
                                .warn(
                                  s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                    s"reason=execution-base-identity-encode-failed detail=$error"
                                )
                                .as(None: Option[Signed[ShardCheckpoint]])
                            case Right(beforeBaseIdentity) =>
                              // On duty — build the checkpoint, then independently re-read/re-encode the base before any signature.
                              assembleDelta(orderedSnapshots, gl0AnchorOrdinal, executionBase).flatMap {
                                case None =>
                                  // assembleDelta already logged the omit-defer reason; mint NOTHING this round.
                                  (None: Option[Signed[ShardCheckpoint]]).pure[F]
                                case Some(delta) =>
                                  withStableExecutionBase(executionBase, beforeBaseIdentity, gl0AnchorOrdinal) {
                                    // Replay may span a density replacement. Re-read the local lineage generation after replay and exact-base
                                    // stability, before any validity signature or publication. This remains a best-effort containment check;
                                    // only the future short commit-if-current can close the final check-to-sign/publish race.
                                    localGlobalLineageRevision.flatMap {
                                      case Some(current) if current == expectedGlobalLineageRevision =>
                                        val checkpoint = ShardCheckpoint(
                                          shardId = shardId,
                                          parentCheckpointHash = parentHash,
                                          shardOrdinal = nextShardOrdinal,
                                          gl0AnchorOrdinal = gl0AnchorOrdinal,
                                          slot = currentSlot,
                                          derivedStateDelta = delta,
                                          // Placeholder — populated below by replacing with the real committee-member sig. NonEmptyList requires at
                                          // least one element to construct; we use a throwaway sentinel and overwrite in `.copy(...)`. Cleaner than
                                          // threading the signing into the case-class constructor.
                                          committeeSignatures = NonEmptyList.of(placeholderSig),
                                          epoch = epoch,
                                          // Pinned claimed state identity used by producer and verifier for the same snapshot re-execution.
                                          executionBase = executionBase.stateRef
                                        )
                                        for {
                                          // Compute the canonical preimage hash via Hasher[F]. This is the bytes every committee member signs (§3.3).
                                          preimageHash <- Hasher[F].hash(checkpoint.signingPreimage)
                                          msgBytes = preimageHash.getBytes
                                          kesEvidence <- kesSigner.sign(operatorKeys, epoch, msgBytes)
                                          result <- kesEvidence match {
                                            case None =>
                                              logger
                                                .warn(
                                                  s"produce-skip shard=${shardId.value.value} reason=no-preregistered-kes-signing-capability " +
                                                    s"checkpointEpoch=${epoch.value}"
                                                )
                                                .as(None: Option[Signed[ShardCheckpoint]])
                                            case Some(KesSignature(kesStep, kesSig)) =>
                                              for {
                                                // The KES period gate above is passed before any long-term or outer-envelope signature is emitted.
                                                edSig <- Signing.signData[F](msgBytes)(selfKeyPair.getPrivate)
                                                committeeSig = CommitteeMemberSignature(
                                                  peerId = selfPeerId,
                                                  vrfProof = Hex.fromBytes(vrfProof),
                                                  ed25519Sig = Hex.fromBytes(edSig),
                                                  kesProductSig = Hex.fromBytes(kesSig),
                                                  kesTreeStep = kesStep
                                                )
                                                finalCheckpoint = checkpoint.copy(committeeSignatures = NonEmptyList.of(committeeSig))
                                                proof <- SignatureProof.fromHash(selfKeyPair, preimageHash)
                                                signedCheckpoint = Signed(finalCheckpoint, NonEmptySet.of(proof))
                                                _ <- publisher.publish(signedCheckpoint)
                                                _ <- logger.info(
                                                  s"produce: emitted shardOrdinal=${nextShardOrdinal.value} gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                                    s"slot=${currentSlot.value.value} mgs=${orderedSnapshots.keys.size} kesStep=$kesStep"
                                                )
                                              } yield Some(signedCheckpoint): Option[Signed[ShardCheckpoint]]
                                          }
                                        } yield result
                                      case observed =>
                                        logger
                                          .warn(
                                            s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                              s"reason=global-lineage-moved-after-replay expected=${expectedGlobalLineageRevision.value.value} " +
                                              s"observed=${observed.map(_.value.value)}"
                                          )
                                          .as(None: Option[Signed[ShardCheckpoint]])
                                    }
                                  } // close canonical execution-base stability re-check
                              } // close assembleDelta.flatMap
                          } // close before-base identity capture
                        }
                    }
                  }
                }
          }
      }

      /** Build the [[ShardDerivedStateDelta]] from the already chain-link-ordered per-MG snapshots.
        *
        * Current ordinary GL0 adoption authenticates and replays `includedSnapshots`, compares the resulting local roots, and derives the
        * economic effects. This is transitional until the signed diff/intents contract lands. In the target, producer plus every execution
        * signer replay, assigned watchtowers provide positive replay coverage, and ordinary noncommittee GL0 applies the verified scoped
        * diff, recomputes the root, and runs only the global conflict/nullifier/settlement kernel for CL1-derived global intents.
        *
        * '''Input is pre-ordered (R-2).''' `orderedSnapshots` has already been chain-link-ordered by [[chainLinkOrder]] off the gl0
        * FINALIZED-base per-MG tip (S2), so each MG's `NonEmptyList` is strictly parent→child from base->latest. `derivePerMgState` is
        * therefore re-executed over a correct chain, and `includedSnapshots` carries the SAME ordered chain current GL0 adoption replays.
        */
      private def assembleDelta(
        orderedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        executionBase: PinnedExecutionBase
      ): F[Option[ShardDerivedStateDelta]] =
        // Re-execute each full per-MG chain at the pinned base. Only the locally derived root is carried as a claim for GL0 to compare with
        // its own full re-execution.
        derivePerMgStates
          .fold(
            orderedSnapshots.toList.traverse {
              case (mg, snaps) =>
                derivePerMgState(mg, snaps, gl0AnchorOrdinal, executionBase.stateRef).map(mg -> _)
            }
          )(_(orderedSnapshots, gl0AnchorOrdinal, executionBase).map(_.toList))
          .flatMap { perMgPairs =>
            val returnedKeys = perMgPairs.iterator.map(_._1).toSet
            val exactBatchKeyset = returnedKeys === orderedSnapshots.keySet
            // DEFER-THE-WHOLE-CHECKPOINT-ON-OMIT (run-27e, 2026-06-14 — review-workflow root cause). `derivePerMgState` returns `None` for an
            // ACTIVE MG (one with pending binaries in this window) whose currency state can't be derived this round: the genesis-bootstrap
            // ADOPT-vs-DERIVE race — this producer has not yet observed gl0 ADOPT the MG's prior shard-ord, so `priorOpt=None` and
            // `processCurrencySnapshots`'s genesis-window guard drops the window. The PRIOR design OMITted just that MG and minted a PARTIAL
            // checkpoint — but a partial (OMITting) checkpoint can WIN `maxvalid-tk` over a sibling that derived the MG correctly (earlier
            // slot wins), become the SOLE canonical shard-ord, REGRESS the MG's `perMgTip` to genesis, and wedge it PERMANENTLY behind the
            // awaiting-embed pipeline gate (the run-27d deadlock; the "pipeline self-heals" assumption was empirically false). So instead we
            // DEFER the WHOLE checkpoint: a producer that cannot derive EVERY active MG mints NOTHING this round and stays silent, so the
            // caught-up producer's COMPLETE checkpoint wins fork-choice; this producer re-attempts next slot once its own adopt catches up.
            // Production-policy only (single-leader WHETHER-to-mint, never the included bytes/root claim) ⇒ no determinism/split risk.
            val omittedMgs: List[Address] = perMgPairs.collect { case (mg, None) => mg }
            if (!exactBatchKeyset)
              logger
                .warn(
                  s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=batch-keyset-mismatch " +
                    s"expected=${orderedSnapshots.keySet.size} returned=${returnedKeys.size}"
                )
                .as(none[ShardDerivedStateDelta])
            else if (omittedMgs.nonEmpty)
              logger
                .info(
                  s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=omit-defer (cannot derive every active MG — not minting a " +
                    s"partial checkpoint) omittedMgs=${omittedMgs.map(_.value.value.take(8)).mkString(",")} totalMgs=${orderedSnapshots.size}"
                )
                .as(none[ShardDerivedStateDelta])
            else {
              val perMgRoots: List[(Address, Hash)] = perMgPairs.collect { case (mg, Some(root)) => mg -> root }
              val delta = ShardDerivedStateDelta(
                perMetagraphMptRoots = SortedMap.from(perMgRoots),
                // No omission reached this branch — every MG in `orderedSnapshots` derived — so include them all.
                includedSnapshots = orderedSnapshots
              )
              delta.some.pure[F]
            }
          }

      /** Chain-link-order the buffered binaries off the gl0 DEPTH-K-FINALIZED-base per-MG tip (S2; EXECUTION-SHARDING design R-2).
        *
        * For each metagraph, anchored on `windowAnchorTips.getOrElse(mg, Hash.empty)` (genesis = `Hash.empty`), unfold the longest
        * parent→child chain over `pendingSnapshots(mg)`: the first binary must carry `lastSnapshotHash == anchor`, the next must reference
        * the first's `Hasher[F]` hash, and so on. This is the SAME unfold `GlobalSnapshotStateChannelAcceptanceManager.selectStateChannels`
        * runs, but (a) on raw `Signed[StateChannelSnapshotBinary]` rather than gl0's `StateChannelOutputWithHash`, and (b) anchored on
        * gl0's FINALIZED-base `lastStateChannelSnapshotHashes` (S2 — VERSION-MODEL §4), so the window covers base->latest and RE-INCLUDES
        * adopted-but-unfinalized binaries. (Before S2 the anchor was the bestTip-derived shard `perMgTip`, which ran AHEAD of base at a
        * unanchored child and skipped the base->adopted span — the §4 window-anchor violation.) The node-local firstSeen / pull-delay
        * registry is intentionally NOT brought over — single leader, no multi-proposer convergence needed (see [[ShardBinaryBuffer]]).
        *
        * A metagraph with no binary chaining off its tip this round is OMITTED from the result. When multiple binaries share the same
        * parent (honest metagraphs produce a linear chain, so this is rare), the one with the most signatures wins, ties broken by lowest
        * canonical hash — a deterministic choice so the leader's own re-derivation is well-defined.
        */
      private def chainLinkOrder(
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        windowAnchorTips: SortedMap[Address, Hash]
      ): F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] = {
        import io.constellationnetwork.security.signature.Signed.SignedOps
        pendingSnapshots.toList.traverse {
          case (mg, binaries) =>
            val anchor = windowAnchorTips.getOrElse(mg, Hash.empty)
            // Hash each candidate once, then group by the parent it references (its `lastSnapshotHash`).
            binaries.toList
              .traverse(b => SignedOps(b).toHashed[F].map(h => (h.hash, b)))
              .map { hashed =>
                val byParent: Map[Hash, List[(Hash, Signed[StateChannelSnapshotBinary])]] =
                  hashed.groupBy(_._2.value.lastSnapshotHash)

                // FOLLOW-THE-EXTENSION fork choice (2026-06-10): when a parent has multiple children (an
                // mg-level fork — ml0 re-emitted a binary, run bfb233uly DAG3CNj), prefer the child with the
                // LONGEST descendant chain in the buffer: the branch the metagraph itself kept extending is
                // its canonical one ("the next binary picks the parent"). The previous (-sigs, +hash)
                // tiebreak could deterministically commit the committee to a DEAD branch forever — windows
                // then never contain the live chain's continuation and the mg's gl0 mirror wedges
                // (DEFER-ANCHOR loop). Signature count then hash remain as the residual tiebreaks.
                // Depth is memoized; the buffer is a DAG under byParent (cycles impossible — a binary's
                // parent hash is fixed at signing), so the recursion terminates.
                val depthMemo = scala.collection.mutable.HashMap.empty[Hash, Int]
                def descendantDepth(h: Hash): Int =
                  depthMemo.getOrElseUpdate(
                    h,
                    byParent.get(h) match {
                      case None | Some(Nil) => 0
                      case Some(children)   => 1 + children.map { case (ch, _) => descendantDepth(ch) }.max
                    }
                  )

                @annotation.tailrec
                def unfold(
                  current: Hash,
                  acc: List[Signed[StateChannelSnapshotBinary]]
                ): List[Signed[StateChannelSnapshotBinary]] =
                  byParent.get(current) match {
                    case None | Some(Nil) => acc.reverse
                    case Some(candidates) =>
                      val (pickedHash, pickedBinary) =
                        candidates.sortBy { case (h, b) => (-descendantDepth(h), -b.proofs.size, h.value) }.head
                      unfold(pickedHash, pickedBinary :: acc)
                  }

                NonEmptyList.fromList(unfold(anchor, Nil)).map(mg -> _)
              }
        }
          .map(pairs => SortedMap.from(pairs.flatten)(Address.OrderingInstance))
      }

      /** Placeholder committee-member signature used at envelope-construction time; immediately overwritten via `.copy(committeeSignatures
        * \= ...)` after the real signature is computed. The signing flow needs the envelope shape (sans the signature itself) to compute
        * the canonical preimage hash, so this is a chicken-and-egg dance: we build a sentinel envelope, hash its preimage (the preimage
        * excludes `committeeSignatures` per slice 1 design), sign the hash, then replace the sentinel with the real signature.
        *
        * '''Why a constant placeholder instead of `Option[NonEmptyList[...]]`.''' The envelope's `committeeSignatures: NonEmptyList[...]`
        * is consensus-load-bearing (slice 1 scaladoc — "empty would be vacuously safe but is a protocol-violation signal — keep the wire
        * shape forbid it"). Wrapping in `Option` for producer-internal convenience would force every downstream consumer to handle the
        * `None` case. Cleaner to use a placeholder that's never seen outside this method.
        */
      private val placeholderSig: CommitteeMemberSignature =
        CommitteeMemberSignature(
          peerId = selfPeerId,
          vrfProof = Hex(""),
          ed25519Sig = Hex(""),
          kesProductSig = Hex(""),
          kesTreeStep = 0
        )
    }
  }

  /** Convenience: derive a producer's expected output hash without running `produce(...)`. The canonical preimage hash IS the bytes the
    * committee member's signatures cover, and IS the chain-link `parentCheckpointHash` the next checkpoint will reference.
    *
    * Exposed for tests / callers that want to track the produced hash without re-deriving the preimage projection at the call site. Pure
    * delegation to `checkpoint.signingPreimage` + `Hasher[F].hash`.
    */
  def hashOf[F[_]: Hasher](checkpoint: ShardCheckpoint): F[Hash] =
    Hasher[F].hash(checkpoint.signingPreimage)

  /** Convenience: pull out the (hash, ordinal) of a produced envelope's canonical signing target. Returns the same `Hashed` shape the
    * chain-store consumes. Exposed for tests / callers that want to inspect the produced envelope without re-running the hash derivation.
    *
    * NOT load-bearing for the producer's main flow (the publisher just gets the `Signed[ShardCheckpoint]` envelope); separate convenience
    * helper for symmetry with `ShardChainStore.StoredShardCheckpoint.toHashed`.
    */
  def hashed[F[_]: Async: Hasher](signed: Signed[ShardCheckpoint]): F[Hashed[ShardCheckpoint]] =
    for {
      hash <- Hasher[F].hash(signed.value.signingPreimage)
      proofsHash <- {
        import io.constellationnetwork.security.signature.Signed.SignedOps
        SignedOps(signed).proofsHash[F]
      }
    } yield Hashed(signed, hash, proofsHash)

  // Unused convenience for matching against `SortedSet`-style codecs in test fixtures. Kept here so test files can refer to
  // `ShardCheckpointProducer.sortedSet(...)` for readability without re-importing in three places.
  private[sharding] def sortedSet[A: Ordering](xs: A*): SortedSet[A] = SortedSet(xs: _*)
}
