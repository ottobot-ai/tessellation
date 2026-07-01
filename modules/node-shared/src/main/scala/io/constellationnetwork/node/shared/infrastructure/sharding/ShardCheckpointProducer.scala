package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.KeyPair

import cats.Applicative
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.ChangeSet
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SharedArtifact, SpendAction}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

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
  *   1. Run [[ShardSlotLeader.isLeader]] for this node's `(vrfSk, shardEta, slot, σ_in_committee)` draw. If we don't win this slot, return
  *      `None` and do NOT publish (per design doc §6.3 — "only winning slot leaders fire checkpoints; other committee members attest later
  *      via gossip", which is slice 14 territory).
  *   1. If we win, compute the [[ShardDerivedStateDelta]] via the injectable per-MG derivation callback (slice 8 doesn't reach into
  *      `TokenLockStateManager` etc. directly — that wiring is slice 9 / 13).
  *   1. Read `parentCheckpointHash` and `parentShardOrdinal` from `chainStore.bestTip` (genesis = `Hash.empty` + `ShardOrdinal.Genesis` for
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
  *   - All knobs (`kTarget`, `k1Shard`, etc. — though this slice's producer itself doesn't read any of those) come from constructor params;
  *     production wiring will pass typed `SharedConfig.nakamoto.sharding.*` values. No `sys.env.get` anywhere.
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
    *   - `gl0AnchorOrdinal`: the gl0 ord this checkpoint will ride into (loose coupling; gl0 accepts at this ord or any later — see design
    *     doc §7.2).
    *   - `epoch`: the sortition epoch the local committee was drawn from (passed through to the envelope's `epoch` field so verifiers can
    *     look up the right active set for VRF verification).
    *
    * Returns:
    *   - `Some(checkpoint)` if this node won the slot lottery AND there's something to publish — the caller can take the produced hash (via
    *     `checkpoint.signingPreimage` ⇒ `Hasher[F]`) for local tracking.
    *   - `None` if this node didn't win the slot OR `pendingSnapshots` is empty AND no T_alive liveness ping is due (slice 8 v1 — T_alive
    *     liveness pings are handled at the caller layer; this slice returns `None` on empty input regardless. Future slice can introduce a
    *     T_alive override flag if needed).
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
    * for one ordinal), splitting committee attestations below `kQuorum`. Single (not a `Map`): the pipeline gate + `nextShardOrdinal =
    * bestTip.next` mean the producer only ever has one ordinal it would mint at a time. Dropped when that ordinal is adopted or the tip
    * moves out from under it (detected via `parentCheckpointHash` — the anchor-reorg/sibling-flip case).
    */
  private final case class HeldCheckpoint(signed: Signed[ShardCheckpoint], lastPublishedTick: Long)

  /** Algebra describing the KES product signer for this operator. Mirrors
    * [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSigner]] — the producer doesn't depend on the concrete
    * `OperationalKeyMakerAlgebra` so the test path can stub it.
    *
    * '''Why we use this trait instead of a raw `SecretKeyKesProduct`.''' The design-doc-style signature in the slice 8 task spec proposes
    * passing the secret-key bytes directly. That doesn't work for our codebase because `SecretKeyKesProduct` is `private[kes]` (see
    * `models.scala` scaladoc — "package-private: callers must not handle secret-key material directly — the read-once lifecycle is enforced
    * by [[OperationalKeyMakerAlgebra]]"). The right abstraction is the existing `KesSigner` trait, which `OperationalKeyMakerAlgebra` is a
    * natural producer of. Wiring code (slice 9/13) constructs the adapter from `operationalKeyMaker.currentPeriod` + `signAt`.
    */
  trait KesSigner[F[_]] {

    /** Current KES tree-internal step the in-memory key holds. The producer embeds this on the envelope's
      * [[CommitteeMemberSignature.kesTreeStep]] field so receivers can verify non-interactively (same #211 wire-step pattern).
      */
    def currentPeriod: F[Int]

    /** Sign `message` at `kesStep`. Returns empty bytes on signer failure — the produced checkpoint will carry an empty `kesProductSig` in
      * that case, and the receiver-side verifier will reject. Callers should always pass `currentPeriod` here.
      */
    def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]]
  }

  object KesSigner {

    /** A KES signer that returns a fixed pre-supplied byte payload. Useful in tests that don't want to bootstrap a full KES key; the
      * payload bytes are placeholders, the produced checkpoint just carries them verbatim on `kesProductSig`. Period is also fixed.
      */
    def fixed[F[_]: Applicative](period: Int, signatureBytes: Array[Byte]): KesSigner[F] =
      new KesSigner[F] {
        def currentPeriod: F[Int] = Applicative[F].pure(period)
        def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]] = Applicative[F].pure(signatureBytes)
      }
  }

  /** Construct a [[ShardCheckpointProducer]].
    *
    * @param shardId
    *   the shard this producer is scoped to. Immutable; one producer instance per shard the operator participates in.
    * @param chainStore
    *   per-shard chain store (Slice 5). Read to determine `parentCheckpointHash` + `parentShardOrdinal` for the new checkpoint.
    * @param slotLeader
    *   per-shard slot leader (Slice 7). Drives the VRF gating that decides whether this node may produce in the current slot.
    * @param publisher
    *   gossip-path abstraction. Production wires a sidecar-backed impl (slice 14); tests pass [[ShardCheckpointPublisher.recording]] or
    *   [[ShardCheckpointPublisher.noop]].
    * @param selfPeerId
    *   this operator's identity. Carried on the produced [[CommitteeMemberSignature.peerId]].
    * @param selfKeyPair
    *   this operator's long-term Ed25519 keypair. Used for both the outer [[Signed]] envelope's [[SignatureProof]] and the inner
    *   [[CommitteeMemberSignature.ed25519Sig]] over the canonical preimage hash.
    * @param selfVrfSk
    *   this operator's slot-leader VRF secret. Drives the `slotLeader.isLeader(...)` draw and supplies the VRF proof carried on
    *   [[CommitteeMemberSignature.vrfProof]].
    * @param kesSigner
    *   this operator's KES product signer. Yields the `kesTreeStep` + `kesProductSig` carried on the envelope's
    *   [[CommitteeMemberSignature]]. The injectable trait shape keeps the producer testable without a full KES bootstrap.
    * @param shardEtaFor
    *   `epoch => F[shardEta]` — resolves the 32-byte shard-leader-VRF eta for the GIVEN eta-period (Slice S4). The producer calls this once
    *   per `produce(...)` keyed on the checkpoint's own `epoch` arg, so the shard-leader lottery rotates in lockstep with the gl0 eta
    *   instead of being pinned to genesis randomness. Production wiring closes over `etaForPeriod(epoch) ⇒ computeShardEta(shardId,
    *   gl0Eta)` (the per-period rotated gl0 eta from `EtaStateManager.getEta`); tests pass a closure returning a fixed precomputed eta.
    *
    * '''Determinism invariant (Slice S4).''' The eta MUST be resolved for the CHECKPOINT'S epoch (the `epoch` arg threaded onto the
    * envelope), NOT the current wall-clock period — a checkpoint produced near an eta boundary may be verified after the boundary, and
    * producer + every verifier must derive the SAME shard-leader eta or `verifyLeader` disagrees and the shard chain stalls. The eta for
    * the checkpoint's epoch is always knowable here: `epoch == rotationPeriod(gl0AnchorOrdinal)` and `eta_N` is fixed at the 2/3-mark of
    * period `N-1` (per `EtaCalculation`), strictly before any ordinal in period `N`.
    * @param staircaseDeltaSlots
    *   width of each staircase rank's proposal window, in slots (design §5.7 rev 2; owner default 5). HOCON
    *   `nakamoto.sharding.checkpoint.staircase-delta-slots`.
    * @param slotGapFor
    *   pure function from `(currentSlot, parentSlotOpt)` returning the LDD slot-gap. Genesis case (no parent): caller supplies a sensible
    *   default — typically the slot itself (matches `EligibilityChecker`'s "first wins always" semantics at the chain seed).
    * @param derivePerMgState
    *   injectable per-MG derivation `(metagraphAddress, includedChain, gl0AnchorOrdinal) => F[(Hash, ChangeSet)]` that re-runs the
    *   metagraph's currency derivation over its full included SC-binary chain against the PRIOR shard-checkpoint's cumulative state `S(N)`
    *   (read from the adopted, chain-linked best-tip) and returns BOTH the canonical per-MG MPT root AND the minimal `CurrencySnapshotInfo`
    *   byte-diff vs `S(N)` (step 6 of the unroll workstream). The `gl0AnchorOrdinal` (the checkpoint's own anchor) feeds the fee-required
    *   cutover so the producer + verifier agree. Production wiring (S3) closes over `ShardCheckpointWiring.reExecDerivationWithDiff` (built
    *   from the SAME `GlobalSnapshotStateChannelEventsProcessor` gl0 uses for metagraph snapshots + the best-tip `GlobalStateReader`), so
    *   the `Hash` (the COMPONENT-ADDRESSABLE `GlobalStateConverter.currencySnapshotMgRoot` — the per-MG sub-trie rootHash) feeds
    *   `perMetagraphMptRoots` and the `ChangeSet` (8 `Mg*` ⊕ fieldId-7 allow-spends, minimal) feeds `perMetagraphStateDiff`. The gl0
    *   verifier APPLIES the diff and recomputes the IDENTICAL root over its post-apply state. For tests, pass a fake closure that returns a
    *   deterministic stub `(Hash, ChangeSet)` per MG.
    *
    * The implicit `Hasher[F]` is required for the canonical preimage hash; `SecurityProvider[F]` is required for the Ed25519 sign path
    * (`Signing.signData`).
    */
  def make[F[_]: Async: Hasher: SecurityProvider: JsonSerializer](
    shardId: ShardId,
    chainStore: ShardChainStore[F],
    /** Cluster-wide static metagraph→shard map (Slice 3). Used by [[assembleDelta]] to classify each cross-MG `SpendAction` target: a
      * `SpendTransaction` whose `currencyId` resolves to a metagraph in a DIFFERENT shard than this producer's [[shardId]] is a cross-shard
      * write, surfaced as a [[CrossShardReceipt.MetagraphSyncDataWrite]] in `emittedReceipts` (design §8.4). MUST be constructed from the
      * SAME `cfg.nakamoto.sharding.numShards` every operator runs — the assignment is consensus-load-bearing here because `emittedReceipts`
      * is inside the signed `ShardCheckpointSigPreimage`, so a `numShards` disagreement would split the committee's receipt list. At
      * `numShards = 1` every target maps to shard 0 == this shard ⇒ no cross-shard target ⇒ `emittedReceipts` stays empty (byte-identical
      * to the pre-receipts path).
      */
    shardAssignment: ShardAssignment[F],
    /** '''S2 — BASE-ANCHORED window (VERSION-MODEL §4).''' Per-MG gl0 DEPTH-K-FINALIZED SC tip — the finalized base's
      * `lastStateChannelSnapshotHashes` (`mptStore.getAllLastStateChannelSnapshotHashes`), the SAME finalized base `derivePerMgState`'s
      * diff-prior reads. `chainLinkOrder` anchors each MG's binary window here (NOT `chainStore.perMgTip`, which is bestTip-derived and
      * runs AHEAD of base at pipelineDepth>1), so the window covers base->latest — RE-INCLUDING adopted-but-unfinalized binaries — and the
      * producer's window-anchor, its diff-prior, and the follower's apply-prior (S1) all read the same finalized base. The window-anchor
      * advances on gl0 depth-k FINALIZATION (reorg-safe), dissolving the perMgTip self-referential fixed point. `chainStore.perMgTip` stays
      * available for legacy chain-link admission elsewhere; it is NO LONGER the producer's window anchor.
      */
    finalizedBasePerMgTip: F[SortedMap[Address, Hash]],
    /** '''NEWNESS GATE — chain-wide per-MG checkpoint frontier (S2-deadlock fix, 2026-06-15; ord-26 re-freeze fix).''' Per-MG the latest
      * binary this chain has CHECKPOINTED across the noteAnchor-followed bestTip ancestry (`chainStore.lastCheckpointedPerMgTip`). Used
      * SOLELY to decide WHETHER an MG has content gl0 has not yet adopted; it does NOT anchor the window (that stays on
      * [[finalizedBasePerMgTip]] for §4 diff-correctness). The producer OMITS any MG whose finalized-base-anchored window does not extend
      * PAST this frontier (i.e. contains no binary carrying `lastSnapshotHash == frontier(mg)` — the frontier's child). Such an MG is a
      * "stale re-include": every binary in its window was already checkpointed (hence already adopted by gl0), so gl0's embed-match
      * (`GlobalSnapshotConsensusFunctions` `pick`, keyed on gl0's adopt tip) finds no continuation and DEFERS it — yet the checkpoint still
      * advanced the shard bestTip past `adoptedShardOrd`, tripping the `pipelineDepth` gate into a permanent freeze (run-19..22 ord16, then
      * ord26; verified live).
      *
      * '''Why the chain-wide frontier and NOT `lastNGlobalSnapshotStorage.getCombined`.''' The frontier is at-or-AHEAD of gl0's per-MG
      * adopt tip (gl0 only adopts checkpoints THIS chain minted, so minted ≥ adopted), is reorg-safe (bestTip follows `noteAnchor` = gl0's
      * adopted lineage), and does NOT revert when a checkpoint omits an MG (it walks back to the most recent checkpoint including that MG —
      * unlike `chainStore.perMgTip`, which reads only bestTip and reverts to genesis under a partial checkpoint, the run-27e regress
      * hazard). The latest-PRODUCED global GSI (`getCombined`) instead LAGS the in-flight per-MG adoptions, so it sits BEHIND the adopt tip
      * and lets a stale re-include slip the gate — the ord-26 re-freeze (the producer minted a window ending at gl0's just-advanced adopt
      * tip because `getCombined` had not caught up). Requiring the window to extend past the (≥ adopt-tip) frontier makes gl0's embed-match
      * continuation a guarantee, not a race. Tests wire this == `finalizedBasePerMgTip` so the gate is a no-op (frontier == windowAnchor ⇒
      * the window head is already the frontier's child).
      */
    adoptedPerMgTip: F[SortedMap[Address, Hash]],
    slotLeader: ShardSlotLeader[F],
    publisher: ShardCheckpointPublisher[F],
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    selfVrfSk: Array[Byte],
    kesSigner: KesSigner[F],
    shardEtaFor: EtaPeriod => F[Array[Byte]],
    staircaseDeltaSlots: Int,
    slotGapFor: (Slot, Option[Slot]) => Long,
    derivePerMgState: (
      Address,
      NonEmptyList[Signed[StateChannelSnapshotBinary]],
      SnapshotOrdinal,
      SnapshotOrdinal
    ) => F[Option[(Hash, ChangeSet)]],
    /** '''Track-1 diff-base-pin — the atomic savepoint source.''' Reads the gl0 finalized base's `mptStore.lastPersistedOrdinal` (defaulted
      * to [[io.constellationnetwork.schema.SnapshotOrdinal.MinValue]] before any finalization). Captured ONCE at the top of `produceInner`,
      * threaded into every per-MG [[derivePerMgState]] (as its 4th arg — the pinned diff base), and stamped into
      * [[io.constellationnetwork.schema.sharding.ShardCheckpoint.diffBaseOrdinal]]. After the derivation, `produceInner` re-reads this and
      * DEFERS (mints nothing) if the base folded forward mid-derivation — so the stamped `diffBaseOrdinal` is guaranteed to name the exact
      * base the diff was cut over (adopters that read at that ordinal reconstruct the byte-identical prior; a stale stamp would fail-close
      * them). Tests wire `Async[F].pure(SnapshotOrdinal.MinValue)` (the diff base is irrelevant to producer-plumbing coverage).
      */
    diffBaseOrdinalF: F[SnapshotOrdinal],
    /** Bounded checkpoint pipeline (2026-06-11, run bpc2yyegf): highest shard ordinal gl0 has ADOPTED for this shard on this node (the
      * acceptance manager's watermark). Production POLICY input only — never a validity condition.
      */
    lastAdoptedOrd: F[Option[ShardOrdinal]],
    /** Max unadopted checkpoints in flight before the producer stops minting new windows. Checkpoint production (1 per anchor) and gl0
      * embedding (1 per shard per snapshot) run at EXACTLY the same rate, so without this gate an admission gap (e.g. the 5-min genesis
      * quorum warmup) persists forever — the mirror trailed ml0 by ~50 ordinals and allow-spend windows died (run bpc2yyegf). Holding
      * production while >= pipelineDepth windows await embed makes pending binaries accumulate into ONE bigger window, so a single embed
      * drains the whole backlog: catch-up margin = window growth.
      */
    pipelineDepth: Int,
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

    // Capture the slot-leader VRF cache state — keyed by (slot, gl0AnchorOrdinal). Slice 8 v1 doesn't need cross-call state, but the
    // capture is required because `Slf4jLogger` returns a fresh instance every time and we want one stable logger per producer.
    new ShardCheckpointProducer[F] {

      def produce(
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        epoch: EtaPeriod,
        currentSlot: Slot,
        committee: Set[PeerId]
      ): F[Option[Signed[ShardCheckpoint]]] =
        // Slice 8 v1: empty input ⇒ nothing to checkpoint. T_alive liveness pings (which permit empty payloads) are deferred to a future
        // slice that introduces an explicit `forceEmptyAlive: Boolean` flag — slice 8 keeps the contract simple.
        if (pendingSnapshots.isEmpty) {
          // INFO (not debug) on every skip path — the 2026-06-10 silent stall was invisible because all
          // produce-skips logged at debug. One line per ord per shard; drop to a metric when task #25 lands.
          logger
            .info(s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=empty-pending")
            .as(None: Option[Signed[ShardCheckpoint]])
        } else {
          // Tier-1 idempotence (task #45): bump the per-produce tick once, then resolve the parent. `bestTip` `None` ⇒ genesis
          // (parent = Hash.empty, parent ord = Genesis). `perMgTip` is derived from the SAME best tip so the chain-link anchor and
          // the parent envelope are read consistently.
          tickRef.updateAndGet(_ + 1L).flatMap { tick =>
            (chainStore.bestTip, finalizedBasePerMgTip, adoptedPerMgTip, lastAdoptedOrd, heldRef.get).tupled.flatMap {
              case (bestTipOpt, windowAnchorTips, adoptedTips, adoptedOrdOpt, heldOpt) =>
                val parentHash: Hash = bestTipOpt.map(_.hash).getOrElse(Hash.empty)
                val nextShardOrdinal: ShardOrdinal =
                  bestTipOpt.map(_.signed.value.shardOrdinal).getOrElse(ShardOrdinal.Genesis).next

                heldOpt match {
                  // ── MEMO HIT (task #45): we already minted this EXACT (shardOrdinal, parent) — re-publish the SAME bytes,
                  // cadence-gated, NEVER re-mint. Re-minting would churn `gl0AnchorOrdinal`+`slot` into a fresh hash every tick
                  // (run-19: 34 variants for one ordinal), splitting committee attestations below kQuorum. The anchor is loosely
                  // coupled (gl0 accepts at the pinned ord or any later), so the held bytes still embed. The held checkpoint was
                  // already published once at mint; FanOut's self-store is hash-idempotent, so returning it is a safe no-op there.
                  case Some(held)
                      if held.signed.value.shardOrdinal.value === nextShardOrdinal.value &&
                        held.signed.value.parentCheckpointHash === parentHash =>
                    if (tick - held.lastPublishedTick >= republishEveryTicks.toLong)
                      publisher.publish(held.signed) *>
                        heldRef.set(Some(held.copy(lastPublishedTick = tick))) *>
                        logger
                          .info(
                            s"produce: re-publish-held shardOrdinal=${held.signed.value.shardOrdinal.value} " +
                              s"gl0Anchor=${held.signed.value.gl0AnchorOrdinal.value.value} slot=${held.signed.value.slot.value.value} " +
                              s"tick=$tick (idempotent hold — task #45)"
                          )
                          .as(Some(held.signed): Option[Signed[ShardCheckpoint]])
                    else
                      logger
                        .info(
                          s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=held-cadence-wait " +
                            s"shardOrdinal=${held.signed.value.shardOrdinal.value} tick=$tick " +
                            s"lastPublished=${held.lastPublishedTick} republishEvery=$republishEveryTicks"
                        )
                        .as(None: Option[Signed[ShardCheckpoint]])

                  // ── MEMO MISS or STALE: no memo, OR the held one is for a different ordinal / parent (adopted past it, or an
                  // anchor-reorg moved the tip out from under it). Drop any stale memo and run the normal gate + mint-ONCE path; the
                  // fresh mint is recorded in the memo on success so subsequent ticks re-publish instead of re-minting.
                  case _ =>
                    val clearStale = if (heldOpt.isDefined) heldRef.set(None) else Async[F].unit
                    val unadoptedDepth: Long =
                      bestTipOpt.map(_.signed.value.shardOrdinal.value).getOrElse(0L) - adoptedOrdOpt.map(_.value).getOrElse(0L)
                    clearStale *> {
                      if (bestTipOpt.isDefined && unadoptedDepth >= pipelineDepth.toLong)
                        // Bounded pipeline: gl0 hasn't embedded our recent windows yet — let pending batch into the next one.
                        logger
                          .info(
                            s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=awaiting-embed " +
                              s"unadoptedDepth=$unadoptedDepth pipelineDepth=$pipelineDepth " +
                              s"tipShardOrd=${bestTipOpt.map(_.signed.value.shardOrdinal.value).getOrElse(0L)} " +
                              s"adoptedShardOrd=${adoptedOrdOpt.map(_.value).getOrElse(0L)}"
                          )
                          .as(None: Option[Signed[ShardCheckpoint]])
                      else
                        produceInner(
                          bestTipOpt,
                          windowAnchorTips,
                          adoptedTips,
                          pendingSnapshots,
                          gl0AnchorOrdinal,
                          epoch,
                          currentSlot,
                          committee
                        ).flatMap {
                          case some @ Some(signed) => heldRef.set(Some(HeldCheckpoint(signed, tick))).as(some)
                          case None                => Async[F].pure(None: Option[Signed[ShardCheckpoint]])
                        }
                    }
                }
            }
          }
        }

      private def produceInner(
        bestTipOpt: Option[io.constellationnetwork.security.Hashed[ShardCheckpoint]],
        windowAnchorTips: SortedMap[Address, Hash],
        adoptedTips: SortedMap[Address, Hash],
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        epoch: EtaPeriod,
        currentSlot: Slot,
        committee: Set[PeerId]
      ): F[Option[Signed[ShardCheckpoint]]] = diffBaseOrdinalF.flatMap { diffBaseOrdinal =>
        // Track-1 diff-base-pin ATOMIC SAVEPOINT: capture the gl0 finalized base ordinal ONCE here, thread it into every per-MG
        // `derivePerMgState` (so all diffs are cut over THIS exact base), stamp it on the checkpoint, and re-check it after the derivation
        // (below) — deferring if the base folded forward mid-produce so the stamp can never name a base the diff was not cut over.
        val parentHash: Hash = bestTipOpt.map(_.hash).getOrElse(Hash.empty)
        val parentOrd: ShardOrdinal = bestTipOpt.map(_.signed.value.shardOrdinal).getOrElse(ShardOrdinal.Genesis)
        // Parent slot = the parent envelope's WIRE slot (design §5.7, owner-corrected 2026-06-11): the lottery clock is the
        // shared wall-clock slot grid, carried on the envelope and signed. The old anchor-derived approximation
        // (`slotForGl0Anchor(parent.gl0AnchorOrdinal)`) downsampled the lottery to gl0-snapshot cadence — the run-10 Gap-A
        // cadence inversion.
        val parentSlotOpt: Option[Slot] = bestTipOpt.map(_.signed.value.slot)
        val nextShardOrdinal: ShardOrdinal = parentOrd.next
        val slotGap: Long = slotGapFor(currentSlot, parentSlotOpt)

        // R-2: chain-link-order the buffered binaries off the shard's OWN tip (NOT gl0's). MGs with no admissible chain this round
        // are omitted; if NOTHING chains off the tip, there is nothing to checkpoint → return None (don't emit an empty checkpoint
        // and don't burn the slot lottery on it). This is the inversion's core: the admissible set comes from the shard's prior
        // checkpoint, not gl0's post-chain-link `stateChannelSnapshots`.
        chainLinkOrder(pendingSnapshots, windowAnchorTips).flatMap { chained =>
          // ── NEWNESS GATE (S2-deadlock fix, 2026-06-15; runs 19-22) ───────────────────────────────────────────────────────────
          // Keep only MGs whose finalized-base-anchored window EXTENDS PAST gl0's adopt tip — i.e. carries a binary whose
          // `lastSnapshotHash == adoptTip(mg)` (the adopt tip's child = genuine new content gl0 hasn't adopted). An MG that fails
          // this is a STALE RE-INCLUDE: every binary in its window is already adopted, so gl0's embed-match (`pick`, keyed on the
          // SAME adopt tip) finds no continuation and DEFERS it — yet minting it advances the shard bestTip past `adoptedShardOrd`
          // and trips the `pipelineDepth` gate into a permanent freeze (verified live: ord16 mgs=1 minted in the 0.6s post-adoption
          // gossip race, frozen ~1.5h while ml0 kept producing). The window for surviving MGs is UNCHANGED (full base->latest, §4
          // diff intact) — the gate only decides inclusion, never trims. `adoptTip` defaults to `Hash.empty` at genesis, where the
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
              // Slice S4: resolve the shard-leader-VRF eta for the CHECKPOINT'S OWN `epoch` (the one stamped on the envelope below),
              // NOT a wall-clock period. `epoch == rotationPeriod(gl0AnchorOrdinal)`, and `eta_epoch` is fixed at the 2/3-mark of the
              // prior period (`EtaCalculation`), so it is knowable here and every verifier — who keys the same lookup on the
              // wire-carried `checkpoint.epoch` — derives byte-identical bytes. This is the load-bearing determinism invariant: if
              // producer + verifier disagreed on shardEta, `ShardSlotLeader.verifyLeader` would reject and the shard chain stalls.
              shardEtaFor(epoch).flatMap { shardEta =>
                // ─── SHUFFLED STAIRCASE (owner, 2026-06-12; design §5.7 rev 2 — replaces the LDD lottery) ───
                // Deterministic duty schedule: the committee is hash-sorted per (shardEta, NEXT shardOrdinal); rank r
                // is on duty for delta slots starting 1 slot after the parent's wire slot, wrapping modulo committee
                // size (liveness needs ONE live member; censorship bounded by rotation). UNIQUE producer per window —
                // genesis included — which is what a lottery can never give a small committee at per-slot draws
                // (run-13/14: genesis forks + same-ord sibling lineages split attestations below kQuorum).
                // slotGap >= 1 by construction (slotGapFor clamps); window index = (slotGap - 1) / delta.
                slotLeader.dutyOrder(committee.toList.sortBy(_.value.value), shardEta, nextShardOrdinal).flatMap { ordered =>
                  val k = math.max(1, ordered.size)
                  // GENESIS WINDOW WIDENING (run-15 post-mortem): at genesis (no parent) the duty windows are 12x wider.
                  // At boot the gossip meshes are still forming — the first checkpoint can take tens of seconds to reach
                  // peers — and a 5-slot handoff let ranks 1 and 2 mint rival genesis checkpoints before rank-0's arrived
                  // (three ord-1s at slots 0/6/11 = the run-15 genesis fork). Slot-grid sync makes the wide handoff exact;
                  // rotation still wraps, so liveness needs one live member even at genesis.
                  val effectiveDelta: Long =
                    if (parentSlotOpt.isEmpty) math.max(1L, staircaseDeltaSlots.toLong) * 12L
                    else math.max(1L, staircaseDeltaSlots.toLong)
                  val dutyIdx = (((slotGap - 1L) / effectiveDelta) % k.toLong).toInt
                  val onDuty = ordered(dutyIdx)
                  if (onDuty =!= selfPeerId)
                    logger
                      .info(
                        s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=not-on-duty " +
                          s"slot=${currentSlot.value.value} gap=$slotGap dutyRank=$dutyIdx " +
                          s"onDuty=${onDuty.value.value.take(12)} chainedMgs=${orderedSnapshots.size}"
                      )
                      .as(None: Option[Signed[ShardCheckpoint]])
                  else
                    slotLeader.membershipProof(selfVrfSk, shardEta, currentSlot).flatMap { vrfProof =>
                      // On duty — build the checkpoint, sign it, publish it.
                      assembleDelta(orderedSnapshots, gl0AnchorOrdinal, parentHash, diffBaseOrdinal).flatMap {
                        case None =>
                          // assembleDelta already logged the omit-defer reason; mint NOTHING this round.
                          (None: Option[Signed[ShardCheckpoint]]).pure[F]
                        case Some((delta, emittedReceipts)) =>
                          // Track-1 diff-base-pin ATOMICITY GUARD: re-read the finalized base. If it folded forward while the per-MG diffs
                          // were being cut, the diffs are over `diffBaseOrdinal` but the store now reflects a LATER base — minting a
                          // checkpoint stamped `diffBaseOrdinal` would be honest, BUT the derivation `priorReaderAt(diffBaseOrdinal)` may
                          // have observed the moved base (the producer wires a live reader), so the two could disagree. DEFER (mint nothing)
                          // — a single-leader whether-to-mint decision, no split/determinism risk; the next slot re-attempts on a stable base.
                          diffBaseOrdinalF.flatMap { afterOrd =>
                            if (afterOrd =!= diffBaseOrdinal)
                              logger
                                .info(
                                  s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=diff-base-moved " +
                                    s"(${diffBaseOrdinal.value.value} -> ${afterOrd.value.value}); defer to next slot"
                                )
                                .as(None: Option[Signed[ShardCheckpoint]])
                            else {
                              val checkpoint = ShardCheckpoint(
                                shardId = shardId,
                                parentCheckpointHash = parentHash,
                                shardOrdinal = nextShardOrdinal,
                                gl0AnchorOrdinal = gl0AnchorOrdinal,
                                slot = currentSlot,
                                derivedStateDelta = delta,
                                emittedReceipts = emittedReceipts,
                                // Placeholder — populated below by replacing with the real committee-member sig. NonEmptyList requires at
                                // least one element to construct; we use a throwaway sentinel and overwrite in `.copy(...)`. Cleaner than
                                // threading the signing into the case-class constructor.
                                committeeSignatures = NonEmptyList.of(placeholderSig),
                                epoch = epoch,
                                // Track-1 diff-base-pin: the base the committee cut `perMetagraphStateDiff` over (verified stable just above).
                                diffBaseOrdinal = diffBaseOrdinal
                              )
                              for {
                                // Compute the canonical preimage hash via Hasher[F]. This is the bytes every committee member signs (§3.3).
                                preimageHash <- Hasher[F].hash(checkpoint.signingPreimage)
                                // Sign with Ed25519 long-term + KES product. Both sign the canonical preimage hash's UTF-8 bytes (`getBytes`
                                // matches `MetagraphCommitteeGate.messageBytes` — Hasher result's UTF-8 byte form is what other sign paths use).
                                msgBytes = preimageHash.getBytes
                                edSig <- Signing.signData[F](msgBytes)(selfKeyPair.getPrivate)
                                kesStep <- kesSigner.currentPeriod
                                kesSig <- kesSigner.signAt(kesStep, msgBytes)
                                committeeSig = CommitteeMemberSignature(
                                  peerId = selfPeerId,
                                  vrfProof = Hex.fromBytes(vrfProof),
                                  ed25519Sig = Hex.fromBytes(edSig),
                                  kesProductSig = Hex.fromBytes(kesSig),
                                  kesTreeStep = kesStep
                                )
                                finalCheckpoint = checkpoint.copy(committeeSignatures = NonEmptyList.of(committeeSig))
                                // Wrap in Signed envelope. The outer Signed contract uses the operator's long-term Ed25519 signature over the
                                // envelope's value bytes; this is the canonical "this operator authored this message" attestation that gl0 and
                                // other peers use to authenticate the gossip path. Mirrors how `GlobalIncrementalSnapshot` is signed.
                                proof <- SignatureProof.fromHash(selfKeyPair, preimageHash)
                                signedCheckpoint = Signed(finalCheckpoint, NonEmptySet.of(proof))
                                _ <- publisher.publish(signedCheckpoint)
                                _ <- logger.info(
                                  s"produce: emitted shardOrdinal=${nextShardOrdinal.value} gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                    s"slot=${currentSlot.value.value} mgs=${orderedSnapshots.keys.size} " +
                                    s"crossShardReceipts=${emittedReceipts.size} kesStep=$kesStep"
                                )
                              } yield Some(signedCheckpoint): Option[Signed[ShardCheckpoint]]
                            } // close else (diff-base stable)
                          } // close diffBaseOrdinalF.flatMap (atomicity re-check)
                      }
                    }
                }
              }
        }
      }

      /** Build the [[ShardDerivedStateDelta]] AND the `emittedReceipts` list from the already chain-link-ordered per-MG snapshots.
        *
        * '''Scope''': `perMetagraphMptRoots` AND `perMetagraphStateDiff` (both from the injectable re-exec callback's `(Hash, ChangeSet)`
        * pair) and `includedSnapshots` (the chain-ordered chains) are populated. The other delta fields (`tokenLockBalancesDelta`,
        * `perMetagraphArtifacts`, `perMetagraphSyncDataDelta`) are left empty — those per-MG derivations migrate from gl0 to the shard in a
        * later slice. The SECOND tuple element is `emittedReceipts` (design §8.4 — see [[crossShardReceipts]]).
        *
        * '''Cross-shard receipts (§8.4).''' Alongside the per-MG roots/diffs we scan each MG's included `SpendAction`s for cross-shard
        * writes: a `SpendTransaction` whose `currencyId` resolves (via [[shardAssignment]]) to a metagraph in a DIFFERENT shard than this
        * producer's [[shardId]] is a cross-shard `MetagraphSyncData` write the source shard cannot apply directly (the target's MPT subtree
        * is the target shard's authority). It is recorded as a [[CrossShardReceipt.MetagraphSyncDataWrite]] for gl0 to drain into the
        * target's pending sync-data (`MetagraphSyncManager.consumeReceipts`). At `numShards = 1` every target maps to this same shard ⇒ the
        * receipt list is empty ⇒ the produced envelope is byte-identical to the pre-receipts path.
        *
        * '''Input is pre-ordered (R-2).''' `orderedSnapshots` has already been chain-link-ordered by [[chainLinkOrder]] off the gl0
        * FINALIZED-base per-MG tip (S2), so each MG's `NonEmptyList` is strictly parent→child from base->latest. `derivePerMgState` is
        * therefore re-executed over a correct chain, and `includedSnapshots` carries the SAME ordered chain gl0 adopts / re-derives.
        */
      private def assembleDelta(
        orderedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        parentCheckpointHash: Hash,
        // Track-1 diff-base-pin: the finalized ordinal the per-MG diffs are cut over (captured atomically in produceInner), passed to every
        // `derivePerMgState` so its prior reader resolves S(N) at THIS base — the same base every adopter/re-executor reads at.
        diffBaseOrdinal: SnapshotOrdinal
      ): F[Option[(ShardDerivedStateDelta, List[CrossShardReceipt])]] =
        // For each MG, call the injectable `derivePerMgState(mg, includedChain, gl0AnchorOrdinal)` over the FULL per-MG chain (S3). The
        // closure re-runs the metagraph's currency derivation against the prior shard-checkpoint's cumulative state S(N) (from the adopted
        // best-tip) and returns BOTH the canonical per-MG MPT root AND the minimal `CurrencySnapshotInfo` byte-diff (`ChangeSet`) vs S(N).
        // Passing the whole ordered `NonEmptyList` (not just the head) re-executes a multi-binary window correctly; passing the
        // wire-carried `gl0AnchorOrdinal` makes the fee cutover byte-identical to what the gl0 verifier re-runs / applies over the same
        // `includedSnapshots(mg)`. The diff travels in `perMetagraphStateDiff` (wire form `ShardCurrencyStateDiff` via `ChangeSet.toWire`)
        // and gl0 APPLIES-and-verifies it against `perMetagraphMptRoots(mg)` — no re-derive (step 6 of the unroll workstream).
        orderedSnapshots.toList.traverse {
          case (mg, snaps) =>
            derivePerMgState(mg, snaps, gl0AnchorOrdinal, diffBaseOrdinal).map(mg -> _)
        }.flatMap { perMgPairs =>
          // DEFER-THE-WHOLE-CHECKPOINT-ON-OMIT (run-27e, 2026-06-14 — review-workflow root cause). `derivePerMgState` returns `None` for an
          // ACTIVE MG (one with pending binaries in this window) whose currency state can't be derived this round: the genesis-bootstrap
          // ADOPT-vs-DERIVE race — this producer has not yet observed gl0 ADOPT the MG's prior shard-ord, so `priorOpt=None` and
          // `processCurrencySnapshots`'s genesis-window guard drops the window. The PRIOR design OMITted just that MG and minted a PARTIAL
          // checkpoint — but a partial (OMITting) checkpoint can WIN `maxvalid-tk` over a sibling that derived the MG correctly (earlier
          // slot wins), become the SOLE canonical shard-ord, REGRESS the MG's `perMgTip` to genesis, and wedge it PERMANENTLY behind the
          // awaiting-embed pipeline gate (the run-27d deadlock; the "pipeline self-heals" assumption was empirically false). So instead we
          // DEFER the WHOLE checkpoint: a producer that cannot derive EVERY active MG mints NOTHING this round and stays silent, so the
          // caught-up producer's COMPLETE checkpoint wins fork-choice; this producer re-attempts next slot once its own adopt catches up.
          // Production-policy only (single-leader WHETHER-to-mint, never the diff/root bytes) ⇒ no determinism/split risk.
          val omittedMgs: List[Address] = perMgPairs.collect { case (mg, None) => mg }
          if (omittedMgs.nonEmpty)
            logger
              .info(
                s"produce-skip gl0Anchor=${gl0AnchorOrdinal.value.value} reason=omit-defer (cannot derive every active MG — not minting a " +
                  s"partial checkpoint) omittedMgs=${omittedMgs.map(_.value.value.take(8)).mkString(",")} totalMgs=${orderedSnapshots.size}"
              )
              .as(none[(ShardDerivedStateDelta, List[CrossShardReceipt])])
          else {
            val perMg: List[(Address, (Hash, ChangeSet))] = perMgPairs.collect { case (mg, Some(rootDiff)) => mg -> rootDiff }
            val delta = ShardDerivedStateDelta(
              perMetagraphMptRoots = SortedMap.from(perMg.map { case (mg, (root, _)) => mg -> root }),
              perMetagraphStateDiff = SortedMap.from(perMg.map { case (mg, (_, diff)) => mg -> ChangeSet.toWire(diff) }),
              // No omission reached this branch — every MG in `orderedSnapshots` derived — so include them all.
              includedSnapshots = orderedSnapshots,
              tokenLockBalancesDelta = SortedMap.empty,
              perMetagraphArtifacts = SortedMap.empty,
              perMetagraphSyncDataDelta = SortedMap.empty
            )
            crossShardReceipts(orderedSnapshots, gl0AnchorOrdinal, parentCheckpointHash).map(receipts => (delta, receipts).some)
          }
        }

      /** Detect cross-shard `MetagraphSyncData` writes in this checkpoint's included snapshots and build the `emittedReceipts` list (design
        * §8.4 — `MetagraphSyncManager.updateFromSpendActions` under sharding).
        *
        * '''Source of `SpendAction`s.''' Each MG's included `Signed[StateChannelSnapshotBinary]` chain is decoded to
        * `Signed[CurrencyIncrementalSnapshot]` (the SAME `JsonSerializer[F].deserialize` over `binary.value.content` the gl0 acceptance
        * path uses at `GlobalSnapshotAcceptanceManager` line ~1959), and its `artifacts` are read off — byte-equivalent to what an
        * unsharded gl0 op reads. A binary that does not decode as a currency incremental contributes no artifacts (tolerant — mirrors the
        * processor's `deserialize(...).map(_.toOption)`), so a non-currency / malformed binary simply yields no receipts (never a crash,
        * never a stall).
        *
        * '''Cross-shard predicate.''' A `SpendTransaction` carries `currencyId: Option[CurrencyId]`. `None` ⇒ DAG-hypergraph scope (no
        * metagraph target ⇒ never a cross-shard MG write). `Some(M_y)` ⇒ the write targets metagraph `M_y`'s sync-data; it is cross-shard
        * iff `shardAssignment.shardIdFor(M_y) =!= shardId` (this producer's own shard). The source MG is the included-chain key (`mg`),
        * which belongs to this shard by construction (the shard-scoped binary buffer + newness gate). Same-shard targets are NOT emitted as
        * receipts: gl0 still applies them via the in-shard `perMetagraphSyncDataDelta` path (a later slice) exactly as today's
        * `updateFromSpendActions` does for co-located MGs.
        *
        * '''Increment shape (consumer-merge-compatible).''' The consumer folds each receipt via `MetagraphSyncManager.mergeSyncDataInfo`:
        * monotone-max on the two scalar watermarks, UNION on `unappliedGlobalChangeOrdinals`. The unsharded `updateFromSpendActions`
        * touches ONLY `unappliedGlobalChangeOrdinals` (adding the current global ordinal; the two scalars carry forward from the target's
        * prior info). So the producer-side increment carries exactly the new information: `MetagraphSyncDataInfo.empty` (both scalars at
        * `MinValue`) with `unappliedGlobalChangeOrdinals = SortedSet(gl0AnchorOrdinal)`. Under the consumer's max+union merge this adds
        * `gl0AnchorOrdinal` to the target's set and leaves its scalars at their (larger) prior values — byte-identical end state to the
        * unsharded path, and idempotent (re-applying the same set element is a no-op).
        *
        * '''Determinism (the §3.3 split invariant).''' `emittedReceipts` is inside the signed `ShardCheckpointSigPreimage`, so every
        * committee member MUST produce the identical list. Every input is cluster-uniform: the decoded artifacts (a pure function of the
        * signed binaries every member re-includes), `shardAssignment` (the cluster-wide `numShards` map), `gl0AnchorOrdinal` and
        * `parentCheckpointHash` (both fixed on the envelope). The result is emitted one receipt per `(sourceMg, targetMg)` cross-shard
        * pair, sorted canonically by `(sourceMg, targetMg)` so the list bytes are order-independent of map/iteration order.
        * `sourceCheckpointHash` is set to `parentCheckpointHash` — a deterministic chain-link identifier for traceability (the receipt's
        * `sourceCheckpointHash` is explicitly NOT consensus-load-bearing per the schema scaladoc; it cannot be this checkpoint's own hash
        * because that hash is computed OVER `emittedReceipts`).
        */
      private def crossShardReceipts(
        orderedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        parentCheckpointHash: Hash
      ): F[List[CrossShardReceipt]] = {
        // The only new information this cross-shard write contributes to the target's sync-data: `gl0AnchorOrdinal` joins the target's
        // `unappliedGlobalChangeOrdinals`. Empty scalars (MinValue) are absorbed by the consumer's monotone-max merge — see scaladoc.
        val increment: MetagraphSyncDataInfo =
          MetagraphSyncDataInfo.empty.copy(unappliedGlobalChangeOrdinals = SortedSet(gl0AnchorOrdinal))

        orderedSnapshots.toList.flatTraverse {
          case (sourceMg, snaps) =>
            // Decode each binary to its currency incremental, read the SpendAction artifacts, and collect the cross-shard target MGs.
            // `decodeArtifacts` is tolerant: a binary that is not a currency incremental yields no artifacts (no crash, no stall).
            snaps.toList.flatTraverse(decodeArtifacts).flatMap { artifacts =>
              val targets: List[Address] =
                artifacts.collect { case sa: SpendAction => sa }.flatMap(_.spendTransactions.toList).flatMap(_.currencyId).map(_.value)
              // Dedup target MGs per source — multiple SpendTransactions to the same target collapse to ONE receipt (the consumer groups
              // by target anyway; one receipt per (source, target) keeps source traceability without redundant receipts).
              targets.distinct.traverse { targetMg =>
                (shardAssignment.shardIdFor(sourceMg), shardAssignment.shardIdFor(targetMg)).mapN { (srcShard, tgtShard) =>
                  // Cross-shard iff the target metagraph lives in a DIFFERENT shard than this producing shard. Use the producer's own
                  // `shardId` for the source side (the source MG belongs to this shard by construction); `srcShard` is computed only to
                  // populate the receipt's `sourceShardId` and is == `shardId` here.
                  Option.when(tgtShard =!= shardId) {
                    CrossShardReceipt.MetagraphSyncDataWrite(
                      sourceShardId = srcShard,
                      sourceMetagraph = sourceMg,
                      sourceCheckpointHash = parentCheckpointHash,
                      targetShardId = tgtShard,
                      targetMetagraph = targetMg,
                      increment = increment
                    )
                  }
                }
              }.map(_.flatten)
            }
        }.map { writes =>
          // CANONICAL ORDER (§3.3 determinism): sort by (sourceMg, targetMg) so the list bytes are independent of decode / iteration order.
          // Every committee member sorts the same key tuple over the same receipt set ⇒ byte-identical `emittedReceipts`. Sorting the
          // concrete `MetagraphSyncDataWrite` (the only `CrossShardReceipt` variant the producer emits) before widening keeps the sort key
          // total — no sealed-trait match in total-function position.
          writes.sortBy(w => (w.sourceMetagraph.value.value, w.targetMetagraph.value.value)): List[CrossShardReceipt]
        }
      }

      /** Decode one included `Signed[StateChannelSnapshotBinary]` to its `Signed[CurrencyIncrementalSnapshot]` and return its
        * `SharedArtifact`s (empty when the binary does not decode as a currency incremental — e.g. a genesis full-snapshot binary or a test
        * stub). Mirrors `GlobalSnapshotStateChannelEventsProcessor.deserialize`
        * (`JsonSerializer[F].deserialize[A](binary.value.content).map(_.toOption)`), so the producer reads byte-equivalent artifacts to the
        * gl0 acceptance path.
        */
      private def decodeArtifacts(binary: Signed[StateChannelSnapshotBinary]): F[List[SharedArtifact]] =
        JsonSerializer[F]
          .deserialize[Signed[CurrencyIncrementalSnapshot]](binary.value.content)
          .map(_.toOption.fold(List.empty[SharedArtifact])(_.value.artifacts.fold(List.empty[SharedArtifact])(_.toList)))

      /** Chain-link-order the buffered binaries off the gl0 DEPTH-K-FINALIZED-base per-MG tip (S2; EXECUTION-SHARDING design R-2).
        *
        * For each metagraph, anchored on `windowAnchorTips.getOrElse(mg, Hash.empty)` (genesis = `Hash.empty`), unfold the longest
        * parent→child chain over `pendingSnapshots(mg)`: the first binary must carry `lastSnapshotHash == anchor`, the next must reference
        * the first's `Hasher[F]` hash, and so on. This is the SAME unfold `GlobalSnapshotStateChannelAcceptanceManager.selectStateChannels`
        * runs, but (a) on raw `Signed[StateChannelSnapshotBinary]` rather than gl0's `StateChannelOutputWithHash`, and (b) anchored on
        * gl0's FINALIZED-base `lastStateChannelSnapshotHashes` (S2 — VERSION-MODEL §4), so the window covers base->latest and RE-INCLUDES
        * adopted-but-unfinalized binaries. (Before S2 the anchor was the bestTip-derived shard `perMgTip`, which ran AHEAD of base at
        * pipelineDepth>1 and skipped the base->adopted span — the §4 window-anchor violation.) The node-local firstSeen / pull-delay
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
