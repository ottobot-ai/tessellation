package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Clock, Ref}
import cats.effect.std.Supervisor
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, MptTxAction}
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Shared epoch state — used by BOTH SnapshotLeaderLoop (production) and NakamotoSyncDaemon (gossip).
  *
  * VRF outputs from ALL sources (own production + received gossip) are accumulated here. When 2/3 of slotsPerEpoch outputs are collected,
  * eta rotates — this happens uniformly across all validators, not just producers.
  */
final case class SharedEpochState(
  currentEta: Array[Byte],
  genesisEta: Array[Byte],
  vrfAccumulator: List[Array[Byte]]
)

object SharedEpochState {
  def initial(genesisEta: Array[Byte]): SharedEpochState =
    SharedEpochState(currentEta = genesisEta, genesisEta = genesisEta, vrfAccumulator = Nil)

  /** Accumulate a VRF output and rotate eta if threshold reached. Eta rotates every `etaRotationSnapshots` (default 2550 = 10·k₁), not
    * every epoch. Rotation is keyed on **ordinal** to satisfy the Praos R ≥ 3·k₁ stability bound (slots are LDD-paced and lumpy; ordinals
    * give a stable R). See `docs/nakamoto/attestation-and-finality.md` §1.
    */
  def accumulate(
    state: SharedEpochState,
    vrfOutput: Array[Byte],
    currentOrdinal: Long,
    etaRotationSnapshots: Long
  ): SharedEpochState = {
    val newAcc = state.vrfAccumulator :+ vrfOutput
    if (newAcc.size >= (etaRotationSnapshots * 2 / 3).toInt) {
      val rotationEpoch = currentOrdinal / etaRotationSnapshots
      val nextEta = EligibilityChecker.computeNextEta(state.currentEta, rotationEpoch, newAcc)
      SharedEpochState(currentEta = nextEta, genesisEta = state.genesisEta, vrfAccumulator = Nil)
    } else
      state.copy(vrfAccumulator = newAcc)
  }
}

/** Pure attestation-based Nakamoto consensus loop.
  *
  * Replaces the entire BFT round system (Facility → Proposal → Signature → Finished). No multi-party coordination. No rounds. No
  * facilitators.
  *
  * Flow:
  *   1. Every 1s slot tick → evaluate VRF eligibility via LDD snowplow 2. On win → drain mempool → call createProposalArtifact → sign →
  *      store → publish via sidecar 3. Other validators receive via GossipSub → validate → broadcast TipAttestation 4. TipTracker
  *      accumulates attestation weight → ≥ 2/3+1 = finalized
  *
  * The `activePoolSize` and `activePoolHash` are embedded in the SlotCertificate so verifiers know what "2/3+1" means for that snapshot
  * without needing global state.
  */
object SnapshotLeaderLoop {

  /** Mutable state tracked across slots. */
  final case class LoopState(
    genesisTimeMs: Long,
    lastProducedSlot: Option[Long],
    lastKnownSlot: Option[Long], // updated on both produce AND gossip receive
    totalProduced: Long
  )

  object LoopState {
    def initial(genesisTimeMs: Long): LoopState =
      LoopState(
        genesisTimeMs = genesisTimeMs,
        lastProducedSlot = None,
        lastKnownSlot = None,
        totalProduced = 0L
      )
  }

  // ── Task #12 (ml0 adopt) changeset-ring helpers ──────────────────────────────────────────────
  // Extracted PURE so the produce→finalize→ring path is unit-testable end-to-end (the whole point of
  // slice 2b's blocker: an accumulator staged under the RAW artifact hash must be rekeyed raw→with-cert
  // at the produce path and then promoted at a finalize sink, or the served ring never populates for
  // self-produced snapshots). The production sites below call these; a focused suite drives them and
  // asserts the promoted delta surfaces via `GlobalChangeSetService.changeSetSince`.

  /** Produce-path rekey: an accumulator was staged under the RAW artifact hash (pre slotCertificate+eta), but the finalize-sink promotion
    * ([[promoteFinalizedAccumulator]]) looks it up under the with-cert canonical hash (`signedHashed.hash`). Move the staged entry `rawHash
    * → withCertHash` (no-op if nothing is staged under `rawHash`). Mirrors the overlay branch rekey done alongside it.
    */
  def rekeyStagedAccumulator(
    staged: Map[Hash, StateChangesAccumulator],
    rawHash: Hash,
    withCertHash: Hash
  ): Map[Hash, StateChangesAccumulator] =
    staged
      .get(rawHash)
      .fold(staged)(acc => (staged - rawHash).updated(withCertHash, acc))

  /** Finalize-sink ring insert: put `acc` at `ordinal` into the served ordinal-keyed ring, trimmed to the last
    * [[GlobalChangeSetService.recentAccumulatorsToKeep]] (dropping the lowest ordinals). The promote at the finalize sink pulls `acc` out
    * of staging atomically and then calls this to insert it; idempotent on re-finalize of the same ordinal (overwrites with the same
    * value).
    */
  def ringInsertTrimmed(
    ring: scala.collection.immutable.SortedMap[SnapshotOrdinal, StateChangesAccumulator],
    ordinal: SnapshotOrdinal,
    acc: StateChangesAccumulator
  ): scala.collection.immutable.SortedMap[SnapshotOrdinal, StateChangesAccumulator] = {
    val withNew = ring.updated(ordinal, acc)
    if (withNew.size > GlobalChangeSetService.recentAccumulatorsToKeep)
      withNew.drop(withNew.size - GlobalChangeSetService.recentAccumulatorsToKeep)
    else withNew
  }

  /** Emit the chain-quality gauge + per-kind "fired" counters for task #138.
    *
    * Called from both finalize sites (depth-k and attestation-2/3). Counter names are spelled out literally so the [[Metrics.MetricKey]]
    * refinement (compile-time regex match) is satisfied — a `s"…${k.name}…"` interpolation can't be refined at compile time. Kept private
    * to this object so neither the metric names nor the choice of "which triggers count" leak into the shared `FinalityTrigger` API (which
    * would create a circular dep on the metric refinement).
    *
    * T_depth2 is intentionally not emitted as a fired counter here — that's the Phase 2→3 archival trigger and gets its own
    * `dag_nakamoto_archival_finalized` counter from the lastArchivalOrdinalRef branch below.
    *
    * '''Per-ordinal companion counters.''' If `finalizedOrdinal` is supplied, ALSO emits `*_per_ordinal_total` counters tagged with
    * `snapshot_ordinal`. The original (unlabelled) counters stay so existing alerts continue to fire. Per-ordinal cardinality is bounded —
    * each ordinal contributes at most one fire per kind, and Prometheus retention (default 15d) bounds the series lifetime.
    */
  private def emitChainQuality[F[_]: cats.Monad: Metrics](
    qualifyingSet: Set[FinalityTrigger.Kind],
    finalizedOrdinal: Option[Long] = None
  ): F[Unit] = {
    val gauge = Metrics[F].updateGauge("dag_nakamoto_chain_quality", qualifyingSet.size)
    val ordTagOpt: Seq[(Metrics.LabelName, String)] =
      finalizedOrdinal.fold(Seq.empty[(Metrics.LabelName, String)])(o => Seq(Metrics.unsafeLabelName("snapshot_ordinal") -> o.toString))
    val tWeight =
      if (qualifyingSet.contains(FinalityTrigger.Kind.TWeight))
        Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_weight_total") >> {
          if (ordTagOpt.nonEmpty)
            Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_weight_per_ordinal_total", ordTagOpt)
          else cats.Applicative[F].unit
        }
      else cats.Applicative[F].unit
    val tCount =
      if (qualifyingSet.contains(FinalityTrigger.Kind.TCount))
        Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_count_total") >> {
          if (ordTagOpt.nonEmpty)
            Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_count_per_ordinal_total", ordTagOpt)
          else cats.Applicative[F].unit
        }
      else cats.Applicative[F].unit
    val tDepth1 =
      if (qualifyingSet.contains(FinalityTrigger.Kind.TDepth1))
        Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_depth1_total") >> {
          if (ordTagOpt.nonEmpty)
            Metrics[F].incrementCounter("dag_nakamoto_finality_triggers_fired_t_depth1_per_ordinal_total", ordTagOpt)
          else cats.Applicative[F].unit
        }
      else cats.Applicative[F].unit
    gauge >> tWeight >> tCount >> tDepth1
  }

  /** Derive VRF keys from node's secp256k1 identity key.
    *
    * Visibility widened to `private[snapshot]` (Gap A, shard-checkpoint producer wiring) so `GlobalSnapshotConsensus.make` (in the parent
    * `...infrastructure.snapshot` package) can derive the SAME (vrfSeed, vrfPk) pair this loop uses, to seed each per-shard
    * `ShardCheckpointProducer`'s slot-leader VRF. v1 reuses the gl0 leader VRF identity for the shard slot lottery (per-operator-key VRF
    * lands later, #180) — so the seed must be derived identically on both sides.
    */
  private[snapshot] def deriveVrfKeys(keyPair: KeyPair): (Array[Byte], Array[Byte]) =
    // Delegates to the single canonical derivation (normalize EC scalar → deriveVrfSeed → getVerificationKey)
    // so the gl0 runtime and the genesis tool (`GenesisGenerator`, which populates `L0GenesisOperator.vrfPublicKey`)
    // produce byte-identical VRF VKs. See `VrfKeyDeriver.deriveVrfKeyPair` for the determinism contract (Slice S1).
    VrfKeyDeriver.deriveVrfKeyPair(keyPair)

  /** (#196) Notify the sidecar outbox that the entries included in `stored` are durably committed at network scale and may be dropped from
    * the periodic re-gossip loop.
    *
    * Per-topic id derivation MUST match what `Publish*` registered in the outbox:
    *   - AllowSpendBlock: id = sha256(JsonSerializer.serialize(signedBlock))
    *   - StateChannelSnapshot binary: id = sha256(JsonSerializer.serialize(signedBinary)) Both paths route through
    *     `JsonSerializer[F].serialize` so the senders (Swap.scala and StateChannelRoutes.broadcastMetagraphBinary) produce the same byte
    *     sequence. The sha256-truncation lives in the JVM here (mirrors the Go sidecar's MsgIDFor) and is invoked via `Hasher[F].hashBytes`
    *     to keep the project rule of "Hasher[F] is the only hash surface" — even though the type is fixed to JSON here, the project
    *     standardises on the Hasher typeclass for every hashing site.
    *
    * Errors are logged and swallowed — the outbox TTL is the safety net.
    */
  private def confirmSnapshotOutbox[F[_]: Async: HasherSelector: JsonSerializer](
    stored: NakamotoChainStore.StoredSnapshot,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.OutboxTopic

    val snapshot = stored.signedSnapshot.value
    val allowSpendBlocks: List[Signed[io.constellationnetwork.schema.swap.AllowSpendBlock]] =
      snapshot.allowSpendBlocks.fold(List.empty[Signed[io.constellationnetwork.schema.swap.AllowSpendBlock]])(_.toList)
    val stateChannelBinaries: List[Signed[io.constellationnetwork.statechannel.StateChannelSnapshotBinary]] =
      snapshot.stateChannelSnapshots.values.toList.flatMap(_.toList)
    // (#196 follow-up) DAGBlock + TokenLockBlock finality. DAG blocks live inside
    // `BlockAsActiveTip` wrappers; we ack on the inner `Signed[Block]` since that's
    // what the JVM serialized at publish time (and what the sidecar's outbox keys on).
    val dagBlocks: List[Signed[io.constellationnetwork.schema.Block]] =
      snapshot.blocks.toList.map(_.block)
    val tokenLockBlocks: List[Signed[io.constellationnetwork.schema.tokenLock.TokenLockBlock]] =
      snapshot.tokenLockBlocks.fold(List.empty[Signed[io.constellationnetwork.schema.tokenLock.TokenLockBlock]])(_.toList)

    if (allowSpendBlocks.isEmpty && stateChannelBinaries.isEmpty && dagBlocks.isEmpty && tokenLockBlocks.isEmpty)
      Async[F].unit
    else
      HasherSelector[F].withCurrent { implicit hasher =>
        // Convert Hasher's hex Hash value back to the raw 32-byte sha256 the
        // sidecar outbox keys on (sidecar `outbox.MsgIDFor` = sha256.Sum256
        // raw bytes). `Hash.value` is `toHexString(sha256(bytes))` length 64;
        // we decode hex → 32 bytes. Using Hasher[F].hashBytes keeps the
        // project rule of "Hasher is the only hash surface" intact even
        // though the wire format is raw bytes.
        def hexToBytes(hex: String): Array[Byte] = {
          val out = new Array[Byte](hex.length / 2)
          var i = 0
          while (i < out.length) {
            out(i) = ((Character.digit(hex.charAt(i * 2), 16) << 4)
              + Character.digit(hex.charAt(i * 2 + 1), 16)).toByte
            i += 1
          }
          out
        }
        def idFor(bytes: Array[Byte]): F[Array[Byte]] =
          Hasher[F].hashBytes(bytes).map(h => hexToBytes(h.value))

        for {
          asbIds <- allowSpendBlocks.traverse { signed =>
            JsonSerializer[F].serialize(signed).flatMap(idFor)
          }
          scbIds <- stateChannelBinaries.traverse { signed =>
            JsonSerializer[F].serialize(signed).flatMap(idFor)
          }
          dagIds <- dagBlocks.traverse { signed =>
            JsonSerializer[F].serialize(signed).flatMap(idFor)
          }
          tlbIds <- tokenLockBlocks.traverse { signed =>
            JsonSerializer[F].serialize(signed).flatMap(idFor)
          }
          _ <- Async[F].whenA(asbIds.nonEmpty) {
            sidecarClient
              .confirmFinalized(OutboxTopic.AllowSpendBlock, asbIds)
              .flatMap(resp => logger.debug(s"outbox confirm allow-spend-block: dropped=${resp.dropped}/${asbIds.size}"))
              .handleErrorWith(e => logger.warn(s"⚠️ confirmFinalized(allow-spend-block) failed: ${e.getMessage}"))
          }
          _ <- Async[F].whenA(scbIds.nonEmpty) {
            sidecarClient
              .confirmFinalized(OutboxTopic.MetagraphBinary, scbIds)
              .flatMap(resp => logger.debug(s"outbox confirm metagraph-binary: dropped=${resp.dropped}/${scbIds.size}"))
              .handleErrorWith(e => logger.warn(s"⚠️ confirmFinalized(metagraph-binary) failed: ${e.getMessage}"))
          }
          _ <- Async[F].whenA(dagIds.nonEmpty) {
            sidecarClient
              .confirmFinalized(OutboxTopic.DAGBlock, dagIds)
              .flatMap(resp => logger.debug(s"outbox confirm dag-block: dropped=${resp.dropped}/${dagIds.size}"))
              .handleErrorWith(e => logger.warn(s"⚠️ confirmFinalized(dag-block) failed: ${e.getMessage}"))
          }
          _ <- Async[F].whenA(tlbIds.nonEmpty) {
            sidecarClient
              .confirmFinalized(OutboxTopic.TokenLockBlock, tlbIds)
              .flatMap(resp => logger.debug(s"outbox confirm token-lock-block: dropped=${resp.dropped}/${tlbIds.size}"))
              .handleErrorWith(e => logger.warn(s"⚠️ confirmFinalized(token-lock-block) failed: ${e.getMessage}"))
          }
        } yield ()
      }
  }

  /** §3 NIPoPoW S3 — bound on the number of newly-archival ordinals processed per `finalityMonitor` tick. Catch-up after a process restart
    * walks (`prev`, `archivalQualifying`] which could be many K₂ values if the leader-loop was offline; capping keeps each tick's wall-time
    * predictable. Remaining ordinals roll forward on the next tick — the finalizer's high-water-mark guarantees forward-only progression
    * regardless of how many ticks the catch-up spans.
    */
  private val TowerFinalizerMaxCatchupPerTick: Long = 100L

  /** §3 NIPoPoW S3 — drive `towerFinalizer.finalize` for each newly-archival ordinal in `(prev, archivalQualifying]`. Resolves the
    * canonical hash via `chainStore.walkBackTo(tipHash, ord)`, fetches the snapshot via `snapshotStorage.get(hash)`, and computes the
    * content-address via `Signed.toHashed`. Errors are logged and swallowed — the next tick will retry remaining ordinals.
    */
  private def driveTowerFinalizer[F[_]: Async: HasherSelector](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    towerFinalizer: io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerFinalizer[F],
    tipHash: Hash,
    prevWatermark: SnapshotOrdinal,
    archivalQualifying: SnapshotOrdinal,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val firstOrd = prevWatermark.value.value + 1L
    val lastOrd = math.min(archivalQualifying.value.value, firstOrd + TowerFinalizerMaxCatchupPerTick - 1L)
    if (lastOrd < firstOrd) Async[F].unit
    else
      (firstOrd to lastOrd).toList.traverse_ { ordValue =>
        chainStore.walkBackTo(tipHash, ordValue).flatMap {
          case Some(canonicalHash) =>
            snapshotStorage.get(canonicalHash).flatMap {
              case Some(signed) =>
                HasherSelector[F].withCurrent { implicit hasher =>
                  signed.toHashed.flatMap(towerFinalizer.finalize)
                }
              case None =>
                logger.debug(
                  s"[TowerFinalizer] No snapshot at ord=$ordValue hash=${canonicalHash.value.take(16)} (file missing); will retry next tick"
                )
            }
          case None =>
            logger.debug(s"[TowerFinalizer] No canonical hash at ord=$ordValue from tip=${tipHash.value.take(16)} (off canonical chain)")
        }
      }.handleErrorWith { err =>
        logger.warn(err)(s"[TowerFinalizer] catch-up at range ($firstOrd..$lastOrd] failed")
      }
  }

  /** Run the pure attestation snapshot leader loop.
    *
    * This is the main consensus loop — call it instead of starting the BFT ConsensusEventLoop.
    *
    * @param consensusFns
    *   existing createProposalArtifact (reused from BFT, not reimplemented)
    * @param snapshotStorage
    *   snapshot chain storage (head, prepend)
    * @param eventMempool
    *   pending events (drain on win)
    * @param sidecarClient
    *   GossipSub sidecar for publishing snapshots
    * @param tipTracker
    *   attestation accumulator for finality
    * @param stakeRegistry
    *   validator weights
    * @param hasherSelector
    *   deterministic hashing (ordinal-aware)
    * @param keyPair
    *   node's secp256k1 identity keypair
    * @param selfId
    *   this node's PeerId
    * @param lddConfig
    *   LDD snowplow parameters
    * @param slotsPerEpoch
    *   slots per epoch (default 60, for time labels only)
    * @param etaRotationSnapshots
    *   snapshots per eta rotation period. Production default 2550 = 10·k₁ (matches Cardano R/k ratio). Eta is long-lived — Cardano uses ~5
    *   days. Rotation is keyed on **ordinal**, not slot — see `docs/nakamoto/attestation-and-finality.md` §1 for the R ≥ 3·k₁ stability
    *   bound rationale.
    */
  def run[F[_]: Async: SecurityProvider: HasherSelector: JsonSerializer: Metrics](
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    nodeStorage: NodeStorage[F],
    keyPair: KeyPair,
    selfId: PeerId,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    slotsPerEpoch: Long = 60L,
    etaRotationSnapshots: Long = 2550L,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    genesisTimeMs: Long = 0L,
    snapshotSemaphore: cats.effect.std.Semaphore[F],
    productionGate: ProductionGate[F],
    // MPT store — wraps leader-build (createProposalArtifact + gate decision) in
    // withTransaction so an abandoned proposal (gate closed pre-sign or chainStore
    // refused the write) restores the canonical MPT state instead of leaving the
    // proposal's mid-flight mutations behind.
    mptStore: MptStore[F, GlobalStateKey],
    // MPT overlay (#56.6 wiring). Receives `finalizeBranch(canonicalHash, ordinal)` after
    // every successful `chainStore.finalize` call. With accept() not yet migrated to use
    // the overlay (#56.10), pending-branches is always empty here so finalize returns
    // `NoOp` at runtime — but the wiring is in place so flipping `MPT_OVERLAY_ENABLED`
    // and migrating accept() activates fold-forward without touching this loop.
    mptOverlay: MptOverlay[F, GlobalStateKey],
    // Tracks the highest finalized ordinal so HttpApi can expose it via
    // /global-snapshots/latest/finalized-ordinal. Updated after every successful
    // chainStore.finalize call (depth-k or attestation-2/3, whichever fires first).
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    // Axis 2 (gl1 inclusion-proof follow) — the latest-FINALIZED `(ordinal, GlobalSnapshotInfo)` the gl0-side
    // slice producer (`GlobalFollowSliceService`) serves. Captured at the SAME two finalize sinks that advance
    // `nakamotoFinalizedOrdinalRef`, from the `StoredSnapshot.context` of the snapshot just finalized — that GSI
    // is genuinely in-memory at the sink (the `chainStore.get` that resolved the canonical snapshot returns the
    // real in-memory `context`, never the disk-fallback placeholder, which only `getWithOrdinalFallback` uses).
    // Updated MONOTONICALLY (only on ordinal advance) so a stale-by-a-tick reorg can't move the served slice
    // backward. The producer MUST serve a finalized ordinal: gl1 is finality-gated (#122) and can only resolve a
    // snapshot at the slice's ordinal — the latest PRODUCED GSI (`lastNGlobalSnapshotStorage.getCombined`) is
    // ahead of the finalized watermark and therefore unresolvable by a follower (the bug this closes).
    latestFinalizedSliceSourceRef: Ref[F, Option[(SnapshotOrdinal, GlobalSnapshotInfo)]],
    // Axis 2 (gl1 follow), #287 "send diffs" — the bounded ring of recent finalized 5-field PROJECTIONS keyed by
    // ordinal (NOT full GSIs). Filled at the SAME two finalize sinks that update `latestFinalizedSliceSourceRef`,
    // storing `GlobalFollowSliceService.sliceFromGsi(finalizedGsi)` and trimmed to the last
    // `GlobalFollowSliceService.recentProjectionsToKeep`. `GlobalFollowSliceService.sliceSince` reads it to compute
    // the incremental diff a gl1 follower requests via `GET /global-follow/slice?since=<ordinal>`. Pure transport
    // optimization: a follower whose `since` fell out of the ring re-fetches the full slice, and field-root equality
    // rejects a wrong base — so the ring bound is a memory bound, not a consensus parameter.
    recentFollowProjectionsRef: Ref[F, scala.collection.immutable.SortedMap[
      SnapshotOrdinal,
      io.constellationnetwork.schema.nakamoto.follow.ConsumedFieldDelta
    ]],
    // Task #12 slice 2b (ml0 changeset-adopt follow, the FULL-state analogue of the #287 gl1 slice ring
    // above). STAGING map the gl0 producer (`GlobalSnapshotConsensusFunctions`) fills hash-keyed when it
    // builds a snapshot; read here at the two finalize sinks to PROMOTE the just-finalized snapshot's
    // accumulator into the served ring and then remove that hash. A finalized hash absent from the staging
    // map (a snapshot this node did NOT itself produce, so it never staged) is skipped — the ml0 follower
    // full-GSI-adopts that gap; absence MUST NOT error. Same instance `GlobalSnapshotConsensus.make` injects
    // into the consensus functions.
    pendingAccumulatorsRef: Ref[F, Map[Hash, StateChangesAccumulator]],
    // Task #12 slice 2b — the SERVED bounded ring of recent FINALIZED per-ordinal accumulators keyed by
    // ordinal (the ml0-side analogue of `recentFollowProjectionsRef`). Promoted at BOTH finalize sinks from
    // `pendingAccumulatorsRef`, trimmed to the last `GlobalChangeSetService.recentAccumulatorsToKeep` (= 256,
    // drop lowest ordinals). `GlobalChangeSetService.changeSetSince` (wired in a later slice) reads it to
    // serve the per-ordinal deltas an ml0 follower adopts-and-verifies. Pure transport optimization: a
    // follower whose `since` fell out of the ring re-fetches via full-GSI adopt, and the signed `mptRoot` at
    // each delta's ordinal rejects a wrong base — so this bound is a memory bound, NOT a consensus parameter.
    recentFinalizedAccumulatorsRef: Ref[F, scala.collection.immutable.SortedMap[
      SnapshotOrdinal,
      StateChangesAccumulator
    ]],
    // Fire-and-forget ChainSync trigger for the finality walkback path. When the
    // finality monitor tries to confirm ancestry at an attested ordinal that we
    // don't have on our local canonical chain (we're on a fork), we enqueue a
    // request here instead of silently waiting for the next periodic sync. See
    // ChainSyncRequestQueue for why this is a queue rather than a direct call.
    chainSyncRequestQueue: ChainSyncRequestQueue[F],
    // Chain-quality observable seam (#138). Populated AFTER the four triggers
    // (T_weight, T_count, T_depth1, T_depth2) are constructed in this stream so the
    // HTTP route /global-snapshots/{ord}/finality-triggers can answer "which triggers
    // qualified ord N?" without taking on the leader-loop's internal state. Caller
    // creates the Ref before HttpApi wiring; we set it once at startup. Pure
    // observability — never feeds back into consensus.
    finalityTriggerViewRef: Ref[F, Option[FinalityTriggerView[F]]],
    // §1.2 Slice 5/6: KES parallel-signing for attestations + snapshots. `operationalKeyMaker`
    // signs the attestation hash + snapshot hash at the period derived from the rotation
    // function; `etaRotationSnapshots` is already in this signature above so we don't add it.
    // Receivers (NakamotoSyncDaemon) re-derive the same period from the wire ordinal and
    // verify with the master VK looked up in the registry — warn-only this slice.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    // Slice S3: invoked at finalize sinks (depth-k AND attestation-2/3) with the canonical
    // GlobalIncrementalSnapshot. Iterates `stateChannelSnapshots` to drop `(metagraphAddress,
    // parentHash)` tally entries from the committee-attestation aggregator once their binary
    // has rolled into a finalized global snapshot. Without this the aggregator grows
    // monotonically (one entry per `(metagraph, parent, binary)` seen on the wire). Callers
    // that haven't wired the gate yet can pass `_ => Async[F].unit`.
    onFinalize: io.constellationnetwork.schema.GlobalIncrementalSnapshot => F[Unit],
    // §3 NIPoPoW S3: Phase-3 sink that grows the local TowerStore from finalized snapshots.
    // Invoked next to the overlay history prune in `finalityMonitor` when T_depth2's archival
    // watermark advances. The finalizer's high-water-mark Ref guarantees no double-write, so
    // missed ticks (e.g. process restart) are safely re-driven from chain replay. Layers that
    // don't run the Phase-3 sink (followers without the tower partition) pass `TowerFinalizer.noop`.
    towerFinalizer: io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerFinalizer[F],
    // ─── Hierarchical-shard-checkpoints v1 — Gap A producer loop ───────────────────────────────
    // Per-shard checkpoint producers, keyed by `ShardId`. EMPTY at `numShards = 1` (the regression
    // bar — the default below), so the `onSlotWon` shard fan-out is a no-op `traverse_` over an empty
    // map: byte-identical to the pre-wiring loop. When `numShards > 1`, `GlobalSnapshotConsensus.make`
    // populates one producer per shard the operator tracks, each reusing the SAME per-shard
    // `ShardChainStore` the acceptance side reads (so producer writes ⇒ consumer reads ⇒ chain grows).
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[
        F
      ]
    ] = Map.empty,
    // Per-shard chain stores (the SAME instances `shardProducers` write into). After a producer wins
    // its shard slot lottery and returns `Some(checkpoint)`, the producing node stores its own
    // checkpoint here (it has the full `Signed[ShardCheckpoint]` + can recompute slot/vrfOutput
    // locally) so the local chain advances toward finality without waiting for its own gossip echo.
    // Empty at `numShards = 1`.
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[
        F
      ]
    ] = Map.empty,
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers (the producer fan-out input — the inversion). The SAME
    // instances the daemon's gossip-intake writes into, projected off `shardAcceptanceDeps.registry`. Empty at
    // `numShards = 1` ⇒ the fan-out stays a no-op `traverse_`.
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[
        F
      ]
    ] = Map.empty,
    // Static metagraph→shard mapping. `None` at `numShards = 1` (no fan-out). `Some(...)` when sharding is active.
    // Used by the fan-out gate (`shardAssignment.isDefined`); per-shard binary partitioning now lives in the
    // daemon's gossip-intake (R-1), so it is no longer used to partition here.
    shardAssignment: Option[ShardAssignment[F]] = None,
    // EXECUTION-SHARDING Task 2: the deterministic `committeeFor(shardId, epoch)` draw, threaded into the producer fan-out so a
    // node only produces a checkpoint for shards it is a committee member of. Required (no default — `Async[F]` of an empty-set
    // default can't resolve at the default-arg site); `GlobalSnapshotConsensus.make` passes the real draw, or a numShards=1 empty-set
    // closure (never reached: the fan-out is gated on `shardProducers.nonEmpty` first).
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      EtaPeriod
    ) => F[Set[io.constellationnetwork.schema.peer.PeerId]]
  )(
    // Slice S6: app-scoped Supervisor used to run the producer shard-checkpoint fan-out OFF the
    // `snapshotSemaphore.permit` critical path in `onSlotWon`. Implicit so the existing
    // `supervisor.supervise(SnapshotLeaderLoop.run[F](...))` call site in `GlobalSnapshotConsensus.make`
    // resolves it from the SAME in-scope Supervisor with no positional change. The numShards=1 path is
    // unaffected — the fan-out stays gated on `shardProducers.nonEmpty`, so the supervised block is never
    // entered there regardless of this param.
    implicit supervisor: Supervisor[F]
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("SnapshotLeaderLoop")
    val (vrfSeed, vrfPK) = deriveVrfKeys(keyPair)

    // #287 "send diffs": record a just-finalized GSI's 5-field projection into the bounded ring keyed by ordinal,
    // trimmed to the last `recentProjectionsToKeep` (drop the lowest ordinals). `GlobalFollowSliceService.sliceSince`
    // diffs `proj@since → proj@latest` from this ring. Pure projection (no hashing); idempotent on re-finalize of
    // the same ordinal. A bound miss only forces a follower back to the full slice — no consensus effect.
    def recordFollowProjection(ordinal: SnapshotOrdinal, gsi: GlobalSnapshotInfo): F[Unit] =
      recentFollowProjectionsRef.update { ring =>
        val updated = ring.updated(ordinal, GlobalFollowSliceService.sliceFromGsi(gsi))
        if (updated.size > GlobalFollowSliceService.recentProjectionsToKeep)
          updated.drop(updated.size - GlobalFollowSliceService.recentProjectionsToKeep)
        else updated
      }

    // Task #12 slice 2b — PROMOTE the just-finalized snapshot's per-ordinal accumulator from the producer's
    // hash-keyed staging map into the served ordinal-keyed changeset ring (the ml0-adopt analogue of
    // `recordFollowProjection`). Look up `finalizedHash` in `pendingAccumulatorsRef`:
    //   - present (this node produced/validated and staged it) ⇒ insert ordinal→acc into the served ring
    //     trimmed to the last `GlobalChangeSetService.recentAccumulatorsToKeep` (drop lowest ordinals), AND
    //     remove the hash from staging so it can't leak;
    //   - absent (a snapshot this node did NOT itself stage) ⇒ skip — the ml0 follower full-GSI-adopts that
    //     gap. Absence MUST NOT error; this is a best-effort transport optimization.
    // Idempotent on re-finalize of the same ordinal (re-promote is a no-op once staging is drained). Called at
    // BOTH finalize sinks (depth-k AND attestation-2/3), exactly paralleling `recordFollowProjection`.
    def recordFinalizedAccumulator(ordinal: SnapshotOrdinal, finalizedHash: Hash): F[Unit] =
      // Atomically pull the staged accumulator out (removing it so fork candidates can't leak), then atomically
      // insert it into the served ring via the pure `ringInsertTrimmed` helper. Two separate atomic ops on the two
      // refs preserves the original `.modify`/`.update` semantics; the pure trim helper is what the regression suite
      // drives directly (after the produce-path `rekeyStagedAccumulator`).
      pendingAccumulatorsRef.modify { staged =>
        staged.get(finalizedHash) match {
          case Some(acc) => (staged - finalizedHash, Some(acc))
          case None      => (staged, None)
        }
      }.flatMap {
        case Some(acc) => recentFinalizedAccumulatorsRef.update(ring => ringInsertTrimmed(ring, ordinal, acc))
        case None      => Async[F].unit
      }
    // Use shared genesis time if provided, else fall back to wall clock
    val effectiveGenesisTime = if (genesisTimeMs > 0) genesisTimeMs else System.currentTimeMillis()

    // Ordinal → wall-clock ms when the snapshot was produced. Populated in onSlotWon,
    // consumed in the finalityMonitor to compute finality latency. Bounded to last 1000
    // entries to prevent unbounded memory growth.
    Stream.eval(Ref.of[F, Map[Long, Long]](Map.empty)).flatMap { productionTimestamps =>
      Stream.eval(Ref.of[F, LoopState](LoopState.initial(effectiveGenesisTime))).flatMap { stateRef =>
        // Seed the chain store with the genesis/initial snapshot so gossip children
        // can find their parent. Retries until the head is available — the SnapshotLeaderLoop
        // fiber starts before genesis initialization in Main.scala completes.
        val seedChainStore: Stream[F, Unit] = Stream.eval {
          def attempt: F[Unit] = snapshotStorage.head.flatMap {
            case Some((headSigned, headCtx)) =>
              HasherSelector[F].withCurrent { implicit hasher =>
                headSigned.toHashed[F].flatMap { hashed =>
                  chainStore.store(headSigned, headCtx, hashed.ordinal.value.value, 0L, hashed.lastSnapshotHash, Array.empty).flatMap {
                    case true =>
                      logger.info(
                        s"🌱 Seeded chain store with genesis: ordinal=${hashed.ordinal} hash=${hashed.hash.value.take(16)}"
                      )
                    case false =>
                      logger.debug(s"Chain store already has genesis at ordinal=${hashed.ordinal}")
                  }
                }
              }
            case None =>
              Async[F].sleep(1.second) >> attempt
          }
          attempt
        }

        // Slot duration is fixed at 1s by default. Tests can override via `NAKAMOTO_SLOT_DURATION_MS`
        // (e.g. 500ms for faster bigset cadence). Tick rate and slot derivation share the same value
        // so cluster nodes agree on slot index for the same wall-clock instant.
        val slotDurationMs: Long =
          sys.env.get("NAKAMOTO_SLOT_DURATION_MS").flatMap(_.toLongOption).getOrElse(1000L)
        val slotTick: Stream[F, Unit] = Stream
          .awakeEvery[F](FiniteDuration(slotDurationMs, MILLISECONDS))
          .evalMap { _ =>
            for {
              // Only produce when node is Ready and past genesis time
              nodeState <- nodeStorage.getNodeState
              state <- stateRef.get
              wallClockMs = System.currentTimeMillis()
              currentSlot = (wallClockMs - state.genesisTimeMs) / slotDurationMs
              gateOpen <- productionGate.isOpen
              _ <-
                if (nodeState =!= NodeState.Ready) Async[F].unit
                else if (!gateOpen) {
                  Async[F].whenA(currentSlot % 30 == 0) {
                    productionGate.pauseReasons
                      .flatMap(reasons => logger.debug(s"Slot $currentSlot: production paused (${reasons.mkString(", ")})"))
                  }
                } else if (currentSlot < 0) {
                  // Still waiting for coordinated genesis time
                  Async[F].whenA(currentSlot % 10 == 0)(logger.info(s"⏳ Waiting for genesis (${-currentSlot}s remaining)"))
                } else
                  for {
                    // Slot gap from last stored chain tip. Used for LDD eligibility:
                    // higher gap = more likely to be eligible (compensates for missed slots).
                    // Clamped to minimum 1 to prevent negative eligibility thresholds that
                    // can occur when catch-up sets lastKnownSlotRef to a future network slot.
                    lastFinalizedSlot <- lastKnownSlotRef.get
                    slotGap = Math.max(1L, lastFinalizedSlot.fold(currentSlot)(currentSlot - _))

                    slotRefined = Slot(NonNegLong.unsafeFrom(Math.max(0L, currentSlot)))

                    // §3 NIPoPoW S0.4 — N-2 epoch staggering: relative stake is read from the
                    // distribution recorded at the boundary of `currentEtaPeriod - 2`, NOT from the
                    // current GSI. This matches Cardano's mark/set/go pipeline: snapshot at the end
                    // of period N is used for slot eligibility in period N+2. The 2-period gap gives
                    // finality time for the snapshot to lock in before consensus relies on it.
                    //
                    // Fall-through semantics (in `StakeRegistry.relativeStakeAt`): for negative
                    // lookback periods (the first 2 eta periods after genesis) and for periods that
                    // EpochStakeHistory hasn't yet recorded (e.g., post-restart before backfill), the
                    // impl falls back to the current GSI — correct because the genesis distribution
                    // is unchanged during warmup.
                    lastChainOrdinal <- chainStore.bestTipOrdinal.map(_.getOrElse(0L))
                    currentPeriod = EtaCalculation.rotationPeriod(lastChainOrdinal, etaRotationSnapshots)
                    lookbackPeriod = EtaPeriod(currentPeriod - 2L)
                    myStake <- stakeRegistry.relativeStakeAt(selfId, lookbackPeriod)

                    // Chain-derived eta: deterministic from stored chain, no in-memory accumulator.
                    // Period 0: genesis eta (constant). Period N>=1: derived from VRF outputs in period N-1.
                    // All nodes seeing the same chain derive the same eta — no divergence.
                    //
                    // Rotation period is keyed on **ordinal**, not slot — slots are LDD-paced and lumpy;
                    // ordinals are 1:1 with snapshots and give a stable R that satisfies the R ≥ 3·k₁
                    // bound. See `docs/nakamoto/attestation-and-finality.md` §1.
                    genesisEta <- epochStateRef.get.map(_.genesisEta)
                    eta <-
                      if (currentPeriod <= 0) {
                        Async[F].pure(genesisEta)
                      } else {
                        chainStore.vrfOutputsForPeriod(currentPeriod - 1, etaRotationSnapshots).map { chainOutputs =>
                          if (chainOutputs.nonEmpty) {
                            EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2))
                          } else {
                            // No chain data yet for previous period — stay on genesis eta
                            genesisEta
                          }
                        }
                      }

                    // Raw VRF-trial counter (incremented BEFORE win/loss check) — pairs with `dag_nakamoto_slots_won` to surface silently-degraded validators (clock skew, missing eta, bad keystore) that would otherwise vanish from finality counters; Prometheus scrapes per node so {node} is added by the collector.
                    _ <- Metrics[F].incrementCounter("dag_nakamoto_slots_trialed_total")

                    result <- eligibilityChecker.checkEligibility(
                      vrfSK = vrfSeed,
                      slot = slotRefined,
                      slotGap = slotGap,
                      eta = eta,
                      relativeStake = myStake,
                      config = lddConfig
                    )

                    _ <- result match {
                      case Some((proof, vrfOutput)) =>
                        snapshotSemaphore.permit.use { _ =>
                          onSlotWon(
                            stateRef,
                            consensusFns,
                            snapshotStorage,
                            chainStore,
                            eventMempool,
                            sidecarClient,
                            tipTracker,
                            stakeRegistry,
                            lastGlobalSnapshotStorage,
                            lastNGlobalSnapshotStorage,
                            keyPair,
                            selfId,
                            vrfSeed,
                            vrfPK,
                            proof,
                            vrfOutput,
                            eta,
                            currentSlot,
                            slotGap,
                            slotRefined,
                            lddConfig,
                            etaRotationSnapshots,
                            lastKnownSlotRef,
                            epochStateRef,
                            productionGate,
                            productionTimestamps,
                            nakamotoFinalizedOrdinalRef,
                            mptStore,
                            mptOverlay,
                            operationalKeyMaker,
                            shardProducers,
                            shardChainStores,
                            shardBinaryBuffers,
                            shardAssignment,
                            shardCommitteeMembership,
                            pendingAccumulatorsRef,
                            logger
                          )
                        } // snapshotSemaphore.permit

                      case None =>
                        // Periodic debug log + gauge
                        Metrics[F].updateGauge("dag_nakamoto_slot", currentSlot) >>
                          Async[F].whenA(currentSlot % 30 == 0) {
                            logger.debug(s"Slot $currentSlot: not eligible (gap=$slotGap, stake=$myStake)")
                          }
                    }
                  } yield ()
            } yield ()
          }

        // Dual finality: attestation weight (fast) OR confirmation depth k (safety fallback)
        //
        //   - Fast path: tip attestation weight ≥ TipTracker.FinalityThreshold (default 2/3,
        //     with optimistic dynamic-quorum sizing for partial cluster availability).
        //   - Fallback: a snapshot is depth-finalized once at least k snapshots sit above it
        //     on the canonical chain — i.e. `tip.ordinal - snapshotOrdinal > k`. This is the
        //     Bitcoin-style confirmation rule, and k is measured in **snapshots (ordinals)**,
        //     not slots. With LDD targeting ~15% slot fill, slots run ~6× sparser than
        //     snapshots, but the depth gate is purely an ordinal-distance check.
        //
        // Both gates always run; whichever fires first finalizes. The depth gate is the
        // safety net against a 1/3 adversary in small clusters where the attestation gate
        // may stall or be gameable: simulation shows k=6 has too high a fork rate at
        // adversarial 1/3, while k=31 gives a comfortable Bitcoin-equivalent safety margin.
        //
        // Cluster sizing notes:
        //   - 1 node: depth-only (no attestations possible)
        //   - 2 nodes: depth-only (can't reach 2/3+1)
        //   - 3+ nodes: attestation finality kicks in fast, depth is the safety net
        //
        // Override via NAKAMOTO_CONFIRMATION_DEPTH. Default 255 chosen to approximate Cardano-
        // equivalent 10⁻¹² common-prefix violation against a 1/3 adversary under the LDD
        // snowplow (ψ=0, γ=15, fA=0.5, fB=0.05). The k=31 sim result is 0.91% per-attempt;
        // extrapolating the ~1-log-per-24-blocks slope puts 10⁻¹² at k≈271, so 255 is a
        // deliberately-conservative operating point pending expanded-range sim verification
        // (research-nipopos-2026, sim/adv-7block-private, adv_depth_optimization.py).
        //
        // Attestation-based BFT finality (≥ 2/3 stake weight, FinalityThreshold in TipTracker)
        // is the hot path in healthy networks and fires in seconds. Depth-k is the fallback
        // for adversarial / partition conditions and only binds when attestation finality
        // stalls. Raising k therefore increases worst-case finality time during degraded
        // operation without affecting normal-case latency.
        val ConfirmationDepthK: Long =
          sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH").flatMap(_.toLongOption).getOrElse(255L)

        // Archival depth k₂ — Phase 2 → Phase 3 boundary (`T_depth2`, task #137).
        //
        // Where ConfirmationDepthK (k₁ ≈ 255) is the operational "you'll never see a reorg
        // past this point" finality gate, k₂ is the cryptographic "common prefix violation
        // is negligibly unlikely" archival gate. At k₂ = 2¹⁶ = 65536 snapshots, CP-violation
        // probability under a 1/3 adversary is well below 10⁻¹² (Cardano-equivalent),
        // making it safe to prune the undo journal, emit aggregate-signature certificates
        // (future Mithril-equivalent), and publish light-client trust anchors.
        //
        // For the current trigger landing, the only downstream effects are a log line and
        // Prometheus counters — Phase-3 sinks (overlay history pruning #139, Mithril, light
        // client) plug in separately. The Ref-backed monotone-advance tracker is in scope
        // for them once they land.
        //
        // See `docs/nakamoto/attestation-and-finality.md` §0.3 for the formal target.
        val ArchivalDepthK: Long =
          sys.env.get("NAKAMOTO_ARCHIVAL_DEPTH").flatMap(_.toLongOption).getOrElse(65536L)

        // FinalityTrigger[F] construction (#135 / #136). T_weight, T_count and T_depth1
        // are built ONCE per leader-loop instance; each one wraps a `Ref[F, SnapshotOrdinal]`
        // that tracks its monotone latest-qualifying-ordinal across all ticks. The downstream
        // `FinalityTrigger.triggersFor(triggers, ord)` lookup answers "which triggers
        // qualified ordinal N?" for free — used by the future chain-quality observable
        // (#138) and post-mortem finality analysis without re-walking the chain.
        //
        // T_weight in the post-Snowball wiring reads the sibling [[SnowballAccumulator]]
        // (constructed inside `TipTracker.make`) — decision is margin-based per Snowball
        // semantics (proposal §2, §3.1), not the 2/3 weight-sum gate. The
        // `TipTracker.FinalityThreshold` value (2/3, env `NAKAMOTO_ATTESTATION_THRESHOLD`) is
        // retained for the T_count rule (1-validator-1-vote ≥ 2/3 distinct attesters), the
        // legacy fallback evidence at the same position, and the ATTEST-FINALIZED log line.
        //
        // Snowball parameters (K, α, β) are documented and defaulted on the
        // [[SnowballAccumulator]] companion (env: `NAKAMOTO_SNOWBALL_K=8`,
        // `NAKAMOTO_SNOWBALL_ALPHA=5`, `NAKAMOTO_SNOWBALL_BETA=10`). Empirical floor from the
        // GPU dual-mode sweep at commit `5ace3d36` of `~/repos/research-nipopos-2026` —
        // 0 safety violations across all measured cluster sizes N ∈ {16, 32, 100, 500, 1000}
        // at f_adv=0.33 under all three adversary modes (coordinated_lie, split_honest,
        // random_honest). See `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` §0.A.
        //
        // The triggers are READ-ONLY surfaces here. Side effects (chainStore.finalize,
        // mptOverlay.finalizeBranch, log lines, metrics, mempool prune) still live in the
        // monitor body below, driven by the triggers' qualifying ordinals.
        Stream.eval(TWeightTrigger.make[F](tipTracker, TipTracker.FinalityThreshold)).flatMap { tWeight =>
          Stream.eval(TCountTrigger.make[F](tipTracker, stakeRegistry, TipTracker.FinalityThreshold)).flatMap { tCount =>
            Stream.eval(TDepth1Trigger.make[F](ConfirmationDepthK)).flatMap { tDepth1 =>
              Stream.eval(TDepth2Trigger.make[F](ArchivalDepthK)).flatMap { tDepth2 =>
                // Phase 1→2 triggers — used both for the chain-quality gauge sample (sized
                // 1..3 at finalize time) and the per-kind "fired" counter increments. T_depth2
                // is Phase 2→3 archival and is intentionally excluded here.
                val phase12Triggers: List[FinalityTrigger[F]] = List(tWeight, tCount, tDepth1)
                // Publish the trigger view (#138) so the HTTP route can answer "which
                // triggers qualified ord N?" without owning trigger references. Set once
                // at startup; reads are lock-free Ref.get from the route handler.
                Stream
                  .eval(
                    finalityTriggerViewRef
                      .set(Some(FinalityTriggerView.fromTriggers[F](List(tWeight, tCount, tDepth1, tDepth2))))
                  )
                  .flatMap { _ =>
                    // Highest Phase-3 (ARCHIVAL) qualifying ordinal seen so far. Driven by
                    // `tDepth2` and advanced strictly forward — Phase-3 sinks (overlay history
                    // pruning #139, future Mithril cert, future light-client anchor) will read
                    // this Ref to decide what's safe to prune / publish.
                    //
                    // TODO(#139): expose this Ref to the rest of the system once overlay
                    // history pruning lands. For now it's local to the leader loop's scope —
                    // adding it to the SnapshotLeaderLoop signature today would just be dead
                    // plumbing. The Ref is constructed inside this Stream so its lifetime
                    // matches the leader loop's fiber.
                    Stream.eval(Ref.of[F, SnapshotOrdinal](SnapshotOrdinal.MinValue)).flatMap { lastArchivalOrdinalRef =>
                      // Heap-leak Fix A: operational-k₁ overlay-prune watermark. Drives
                      // `mptOverlay.pruneBelow` once per advance of `T_depth1.latestQualifyingOrdinal`
                      // (the operational k₁ finality trigger). Decoupling from the k₂ archival watermark
                      // means undoJournalRef + finalizedRef are bounded by k₁ ords instead of k₂ — a
                      // ~257× reduction in worst-case retention (255 vs 65536).
                      //
                      // Safety: reorg-replace at depth > k₁ is excluded by `ConfirmationDepthK`
                      // semantics, so the journal entry at `ord` for `ord < tip.ord - k₁` cannot ever
                      // be replayed. The reorg-replace finalize paths
                      // (`MptOverlay.finalizeBranch`'s `case Some(prev) =>` arms) only consult
                      // `undoJournalRef[ord]` when re-finalizing at the same ord — beyond k₁ depth
                      // that's a CP violation, not a normal reorg.
                      //
                      // Monotone advance via `Ref` mirrors the archival pattern. The overlay's
                      // `pruneBelow` is itself idempotent and monotone — the local Ref just dedups
                      // the call so we don't emit OVERLAY-PRUNE-BELOW log noise per finality tick.
                      Stream.eval(Ref.of[F, SnapshotOrdinal](SnapshotOrdinal.MinValue)).flatMap { lastOperationalPruneOrdinalRef =>
                        // Tick at 5× slot duration: 5s in prod (1000ms slot), 2.5s in e2e (500ms slot).
                        // Slightly faster than the expected snapshot arrival rate so the finality
                        // monitor catches new tips reactively, but still amortizes the chain-walk +
                        // weight-fold cost over multiple slots.
                        val finalityMonitor: Stream[F, Unit] = Stream
                          .awakeEvery[F](FiniteDuration(5L * slotDurationMs, MILLISECONDS))
                          .evalMap { _ =>
                            for {
                              allAtts <- tipTracker.allAttestations
                              validatorCount <- stakeRegistry.validatorCount
                              activeCount <- stakeRegistry.observedActiveCount
                              bestTip <- chainStore.bestTip

                              // §5.1 visibility ticker. If our last self-attestation doesn't match current
                              // bestTip — because chain-selection switched after a fork-branch arrived, a reorg
                              // promoted a different tip, or we never attested anything yet — emit a fresh
                              // self-attestation pointing at canonical. Without this, a stale self-att gets
                              // filtered to zero weight by TipTracker.highestFinalizedOrdinal:143-151 (canonical-
                              // hash filter) and our own vote never contributes to bestTip finality.
                              //
                              // `attestedAt` is wall-clock epoch ms via `Clock[F].realTime` — same Chronos-prep
                              // semantics as the peer-receive becameBestTip emit. Not a consensus slot.
                              _ <- bestTip match {
                                case Some(tip) if !allAtts.get(selfId).exists(_.tipHash === tip.hash) =>
                                  Clock[F].realTime.map(_.toMillis).flatMap { attestedAt =>
                                    NakamotoSyncDaemon.emitTipAttestation[F](
                                      tipHash = tip.hash,
                                      tipSlot = Slot(NonNegLong.unsafeFrom(tip.slot)),
                                      tipOrdinal = tip.ordinal,
                                      attestedAt = attestedAt,
                                      sidecarClient = sidecarClient,
                                      tipTracker = tipTracker,
                                      selfId = selfId,
                                      keyPair = keyPair,
                                      operationalKeyMaker = operationalKeyMaker,
                                      etaRotationSnapshots = etaRotationSnapshots,
                                      logger = logger
                                    ) >> logger.info(
                                      s"RE-ATTEST bestTip change: ord=${tip.ordinal} slot=${tip.slot} hash=${tip.hash.value.take(12)} " +
                                        s"attestedAt=${attestedAt} " +
                                        s"(prior self-att=${allAtts.get(selfId).map(_.tipHash.value.take(12)).getOrElse("none")})"
                                    )
                                  }
                                case _ => Async[F].unit
                              }

                              // The last-finalized **ordinal** must come from chainStore — tipTracker.lastFinalized
                              // only carries (Hash, Slot), and slots are LDD-paced not 1:1 with ordinals. The
                              // previous code derived this from the slot value which silently disabled the depth
                              // gate (as soon as any attestation finality fired, the slot value far outran any
                              // tip.ordinal under LDD slot fill, making `tip.ordinal - lastFinalizedSlot > k`
                              // perpetually false).
                              lastFinalizedOrdinal <- chainStore.lastFinalizedOrdinal

                              // Evaluate every trigger against the current ConsensusState. Each
                              // `evaluateAndAdvance` updates that trigger's monotone Ref so
                              // `FinalityTrigger.triggersFor(ord)` correctly reports membership for
                              // later lookups. We still re-fetch `t_weight`'s `(ordinal, weight)`
                              // tuple separately below for the ATTEST-FINALIZED log line, which
                              // requires the cumulative-weight value (idempotent pure read).
                              _ <- bestTip match {
                                case Some(tip) =>
                                  val state = FinalityTrigger.ConsensusState[F](
                                    selfId = selfId,
                                    bestTipOrdinal = SnapshotOrdinal.unsafeApply(tip.ordinal),
                                    bestTipHash = tip.hash,
                                    canonicalHashAt = ord => chainStore.walkBackTo(tip.hash, ord)
                                  )
                                  tWeight.evaluateAndAdvance(state) >>
                                    tCount.evaluateAndAdvance(state) >>
                                    tDepth1.evaluateAndAdvance(state) >>
                                    tDepth2.evaluateAndAdvance(state).void
                                case None =>
                                  Async[F].unit
                              }

                              // Depth-based finality: a snapshot is final once k+ snapshots sit above it on
                              // the canonical chain — `tip.ordinal - snapshotOrdinal > k`.
                              //
                              // Driven by `tDepth1.latestQualifyingOrdinal` (the trigger). The trigger's
                              // monotone-advance Ref was just updated above; we read it here to drive
                              // the side-effecting finalization below.
                              depthQualifying <- tDepth1.latestQualifyingOrdinal
                              depthFinalized <- bestTip match {
                                case Some(tip) if depthQualifying.value.value > lastFinalizedOrdinal =>
                                  val finalizeAtOrdinal = depthQualifying.value.value
                                  // Walk the canonical chain from best tip to the hash at finalizeAtOrdinal,
                                  // then look up its actual slot from the chain store. The slot is needed by
                                  // tipTracker.markFinalized / pruneBelow which key attestations by slot. Do
                                  // NOT compute it as `tip.slot - k` — that mixes slot-units with ordinal-units
                                  // (the old bug) and produces a slot far above the canonical snapshot's real
                                  // slot, over-pruning attestations.
                                  chainStore.walkBackTo(tip.hash, finalizeAtOrdinal).flatMap {
                                    case Some(canonicalHash) =>
                                      chainStore.get(canonicalHash).flatMap {
                                        case Some(canonicalSnapshot) =>
                                          val finalizeAtSlot = Slot(NonNegLong.unsafeFrom(canonicalSnapshot.slot))
                                          tipTracker.markFinalized(canonicalHash, finalizeAtSlot) >>
                                            tipTracker.pruneBelow(finalizeAtSlot) >>
                                            chainStore.finalize(canonicalHash, finalizeAtOrdinal) >>
                                            // (#196) Phase-3 ack to the sidecar outbox. After this finalize call
                                            // the snapshot's AllowSpendBlocks + StateChannelSnapshots are durably
                                            // committed; the sidecar can drop the corresponding outbox entries
                                            // and stop re-gossiping. Failure here is non-fatal — the outbox TTL
                                            // catches it eventually and the next finalize call would re-confirm
                                            // anyway (Confirm is idempotent on the sidecar side).
                                            confirmSnapshotOutbox(canonicalSnapshot, sidecarClient, logger) >>
                                            // Slice S3: drop committee-attestation tally entries for `(metagraphAddress,
                                            // parentHash)` pairs whose binary just rolled into a finalized gl0 snapshot.
                                            // Default is a no-op; the gate-wired path iterates `stateChannelSnapshots`.
                                            onFinalize(canonicalSnapshot.signedSnapshot.value) >>
                                            // #56.6: notify the MPT overlay that this branch is finalized. With
                                            // accept() not yet migrated, this is a runtime no-op (pending=empty
                                            // returns NoOp). When #56.10 migrates accept() to commit branches,
                                            // this becomes the fold-forward sink without touching this site.
                                            mptOverlay
                                              .finalizeBranch(BranchId(canonicalHash), SnapshotOrdinal.unsafeApply(finalizeAtOrdinal))
                                              .void >>
                                            // Advance the finalized-ordinal tracker so HttpApi
                                            // /latest/finalized-ordinal reflects the new high-water mark. CL0
                                            // polls this to gate state-channel-binary pruning on actual finality
                                            // (not just first sight).
                                            nakamotoFinalizedOrdinalRef
                                              .update(prev =>
                                                cats.Order[SnapshotOrdinal].max(prev, SnapshotOrdinal.unsafeApply(finalizeAtOrdinal))
                                              ) >>
                                            // Axis 2 (gl1 follow): capture this just-finalized snapshot's GSI as the slice
                                            // producer's source. `canonicalSnapshot.context` is the in-memory finalized GSI
                                            // (the `chainStore.get(canonicalHash)` above returned it). Monotone: only advance
                                            // when this ordinal is strictly higher than the stored one.
                                            latestFinalizedSliceSourceRef.update {
                                              case Some((o, _)) if o.value.value >= finalizeAtOrdinal =>
                                                Some((o, canonicalSnapshot.context))
                                              case _ =>
                                                Some((SnapshotOrdinal.unsafeApply(finalizeAtOrdinal), canonicalSnapshot.context))
                                            } >>
                                            // #287: record this finalized projection in the bounded diff ring (trimmed to the
                                            // last K), so `GlobalFollowSliceService.sliceSince` can serve incremental gl1 diffs.
                                            recordFollowProjection(
                                              SnapshotOrdinal.unsafeApply(finalizeAtOrdinal),
                                              canonicalSnapshot.context
                                            ) >>
                                            // Task #12 slice 2b: promote this finalized snapshot's per-ordinal accumulator
                                            // (staged hash-keyed by the producer under `canonicalHash`) into the served
                                            // changeset ring for the ml0 adopt-and-verify follow path. No-op if this node
                                            // never staged it (didn't produce it) — the follower full-GSI-adopts that gap.
                                            recordFinalizedAccumulator(
                                              SnapshotOrdinal.unsafeApply(finalizeAtOrdinal),
                                              canonicalHash
                                            ) >>
                                            logger
                                              .info(
                                                s"DEPTH-FINALIZED ordinal=$finalizeAtOrdinal slot=${canonicalSnapshot.slot} (tip ord=${tip.ordinal} slot=${tip.slot}, k=$ConfirmationDepthK)"
                                              ) >>
                                            Metrics[F].incrementCounter("dag_nakamoto_finalized") >>
                                            Metrics[F].updateGauge("dag_nakamoto_finalized_ordinal", finalizeAtOrdinal) >>
                                            productionTimestamps.getAndUpdate(_ - finalizeAtOrdinal).flatMap { ts =>
                                              ts.get(finalizeAtOrdinal).traverse_ { prodMs =>
                                                val latencyMs = System.currentTimeMillis() - prodMs
                                                Metrics[F].recordDistribution("dag_nakamoto_finality_latency_ms", latencyMs.toInt)
                                              }
                                            } >>
                                            // Chain-quality observable (#138): sample which Phase 1→2 triggers
                                            // qualified this ordinal at finalize time. Pure observability — the
                                            // qualifying-set lookup walks each trigger's monotone Ref (no chain
                                            // walk). Value ∈ {1, 2, 3}: 1 = sketchy single-trigger evidence
                                            // (typically depth-only in a small-cluster partition), 3 = rock-solid
                                            // unanimous evidence. The per-kind counter increments let dashboards
                                            // break down "what fired" over time.
                                            FinalityTrigger
                                              .triggersFor(phase12Triggers, SnapshotOrdinal.unsafeApply(finalizeAtOrdinal))
                                              .flatMap(qs => emitChainQuality[F](qs, finalizedOrdinal = Some(finalizeAtOrdinal))) >>
                                            Async[F].pure(true)
                                        case None =>
                                          logger.warn(
                                            s"⚠️ DEPTH-FINALIZE: walked back to ordinal=$finalizeAtOrdinal but chainStore.get returned None for hash=${canonicalHash.value
                                                .take(12)}"
                                          ) >> Async[F].pure(false)
                                      }
                                    case None =>
                                      logger.warn(
                                        s"⚠️ DEPTH-FINALIZE: could not find canonical hash at ordinal=$finalizeAtOrdinal from tip=${tip.hash.value
                                            .take(12)}"
                                      ) >> Async[F].pure(false)
                                  }
                                case _ => Async[F].pure(false)
                              }

                              // GRANDPA-style chain finality, chain-aware: attesting to ordinal N with a hash on
                              // OUR chain implicitly attests to all ancestors. Attestations with a different hash
                              // at ordinal N are on a different fork and must NOT contribute weight here — the
                              // canonicalHashAt predicate filters them out. Walk attestation ordinals from highest
                              // down, accumulating only matching-hash weight. The highest ordinal where cumulative
                              // weight >= 2/3 is finalized.
                              //
                              // The trigger's `tWeight.latestQualifyingOrdinal` already reflects this evaluation
                              // (from `evaluateAndAdvance` above) — but `tWeight` now reads the Snowball
                              // accumulator, not the legacy weight-sum path. The legacy
                              // `highestFinalizedOrdinal` call below is retained for the ATTEST-FINALIZED log
                              // line's cumulative `weight` value (downstream log-parsing depends on
                              // `weight=X.XX` exactly) and as parallel evidence at the same ordinal. The work is
                              // idempotent — a pure read against the attestations map + canonical chain walk.
                              //
                              // '''P-11b rolled back (Snowball commit).''' This call NO LONGER passes `selfId`.
                              // Snowball's observer-independent decision rule is the primary T_weight driver;
                              // the legacy weight-sum here can safely include self again. NID is restored at the
                              // T_weight position. See `docs/nakamoto/AVALANCHE-ATTESTATION-PROPOSAL.md` §3.1.
                              chainFinalizedOrdinal <- bestTip match {
                                case Some(tip) =>
                                  tipTracker.highestFinalizedOrdinal(
                                    TipTracker.FinalityThreshold,
                                    ord => chainStore.walkBackTo(tip.hash, ord)
                                  )
                                case None =>
                                  Async[F].pure(Option.empty[(Long, Ratio)])
                              }
                              _ <- (chainFinalizedOrdinal, bestTip) match {
                                case (Some((finalOrdinal, weight)), Some(tip)) if finalOrdinal > lastFinalizedOrdinal && !depthFinalized =>
                                  // Walk the canonical chain to find the hash at finalOrdinal
                                  chainStore.walkBackTo(tip.hash, finalOrdinal).flatMap {
                                    case Some(canonicalHash) =>
                                      chainStore.get(canonicalHash).flatMap {
                                        case Some(stored) =>
                                          val finalSlot = Slot(NonNegLong.unsafeFrom(stored.slot))
                                          tipTracker.markFinalized(canonicalHash, finalSlot) >>
                                            tipTracker.pruneBelow(finalSlot) >>
                                            chainStore.finalize(canonicalHash, finalOrdinal) >>
                                            // (#196) Phase-3 outbox ack — see DEPTH-FINALIZED branch for rationale.
                                            confirmSnapshotOutbox(stored, sidecarClient, logger) >>
                                            // Slice S3: drop committee-attestation tally entries for `(metagraphAddress,
                                            // parentHash)` pairs whose binary just rolled into a finalized gl0 snapshot.
                                            // Same site as the depth-finality path above.
                                            onFinalize(stored.signedSnapshot.value) >>
                                            // #56.6: see depth-k branch above. Same wiring at the attestation-2/3 sink.
                                            mptOverlay
                                              .finalizeBranch(BranchId(canonicalHash), SnapshotOrdinal.unsafeApply(finalOrdinal))
                                              .void >>
                                            nakamotoFinalizedOrdinalRef
                                              .update(prev =>
                                                cats.Order[SnapshotOrdinal].max(prev, SnapshotOrdinal.unsafeApply(finalOrdinal))
                                              ) >>
                                            // Axis 2 (gl1 follow): same finalized-GSI capture as the DEPTH-FINALIZED branch
                                            // above. `stored.context` is the in-memory finalized GSI. Monotone advance.
                                            latestFinalizedSliceSourceRef.update {
                                              case Some((o, _)) if o.value.value >= finalOrdinal => Some((o, stored.context))
                                              case _ => Some((SnapshotOrdinal.unsafeApply(finalOrdinal), stored.context))
                                            } >>
                                            // #287: same recent-projection ring update as the DEPTH-FINALIZED branch above.
                                            recordFollowProjection(SnapshotOrdinal.unsafeApply(finalOrdinal), stored.context) >>
                                            // Task #12 slice 2b: same changeset-ring promotion as the DEPTH-FINALIZED branch
                                            // above. No-op when this node didn't stage `canonicalHash` (didn't produce it).
                                            recordFinalizedAccumulator(SnapshotOrdinal.unsafeApply(finalOrdinal), canonicalHash) >>
                                            snapshotStorage.pruneTentative(SnapshotOrdinal(NonNegLong.unsafeFrom(finalOrdinal))) >>
                                            logger.info(
                                              s"ATTEST-FINALIZED ordinal=$finalOrdinal slot=${stored.slot} (weight=${"%.2f".format(weight.toDouble)}, " +
                                                s"${allAtts.size}/${activeCount} active of ${validatorCount} seedlist)"
                                            ) >>
                                            Metrics[F].incrementCounter("dag_nakamoto_finalized") >>
                                            Metrics[F].updateGauge("dag_nakamoto_finalized_ordinal", finalOrdinal) >>
                                            productionTimestamps.getAndUpdate(_ - finalOrdinal).flatMap { ts =>
                                              ts.get(finalOrdinal).traverse_ { prodMs =>
                                                val latencyMs = System.currentTimeMillis() - prodMs
                                                Metrics[F].recordDistribution("dag_nakamoto_finality_latency_ms", latencyMs.toInt)
                                              }
                                            } >>
                                            // Chain-quality observable (#138): see the matching block in the
                                            // DEPTH-FINALIZED branch above for the rationale. Same sample at
                                            // the attestation-2/3 sink so both finalize paths produce a
                                            // gauge update + per-kind counter increments.
                                            FinalityTrigger
                                              .triggersFor(phase12Triggers, SnapshotOrdinal.unsafeApply(finalOrdinal))
                                              .flatMap(qs => emitChainQuality[F](qs, finalizedOrdinal = Some(finalOrdinal)))
                                        case None =>
                                          logger.warn(
                                            s"⚠️ ATTEST-FINALIZE: chainStore.get returned None for hash=${canonicalHash.value.take(12)} at ordinal=$finalOrdinal"
                                          )
                                      }
                                    case None =>
                                      // We're on a fork that doesn't contain the attested ordinal. The periodic sync will notice eventually,
                                      // but in practice that takes ~tens of seconds (observed 48s stall in one incident). Enqueue a targeted
                                      // ChainSync request so we start pulling the better chain now. Idempotent: repeated 5s ticks over the same
                                      // gap collapse into one in-flight request.
                                      logger
                                        .warn(
                                          s"⚠️ ATTEST-FINALIZE: walkBackTo found no hash at ordinal=$finalOrdinal from tip=${tip.hash.value.take(12)}"
                                        ) >>
                                        chainSyncRequestQueue.request(finalOrdinal)
                                  }
                                case _ =>
                                  Async[F].whenA(allAtts.nonEmpty) {
                                    val ordinals = allAtts.values.map(_.tipOrdinal).toList.sorted
                                    logger.debug(
                                      s"Attestations: ${allAtts.size} attesters, ordinals=[${ordinals.mkString(",")}], lastFinalized=$lastFinalizedOrdinal"
                                    )
                                  }
                              }

                              // T_count observability: log when T_count strictly outruns both T_weight AND
                              // T_depth1 (i.e. it would have driven the finalize on its own, if the max-of
                              // semantics ever consumed it). Today's sinks are driven by T_weight / T_depth1;
                              // this log line is purely diagnostic so we can see when count-based finality
                              // would beat the others. Under equal stake T_count ties with T_weight (same
                              // 2/3 threshold, equal weights ⇒ count is just `weight * validatorCount`), so
                              // this branch is silent on healthy clusters. It only fires once stake-weighted
                              // VRF lands and a high-stake validator can hit 2/3 weight alone while count
                              // remains below threshold (T_weight > T_count) OR when an attestation set has
                              // wide count coverage but low aggregate weight (T_count > T_weight).
                              countQualifying <- tCount.latestQualifyingOrdinal
                              weightQualifying <- tWeight.latestQualifyingOrdinal
                              _ <- Async[F].whenA(
                                countQualifying.value.value > lastFinalizedOrdinal &&
                                  countQualifying.value.value > weightQualifying.value.value &&
                                  countQualifying > depthQualifying
                              ) {
                                // Recompute the contributing count on the current canonical chain, purely for
                                // the log line (does not drive state). Mirrors TCountTrigger's filter so the
                                // logged K/V matches what the trigger saw.
                                val countSnapshotF = bestTip match {
                                  case Some(tip) =>
                                    allAtts.iterator.filter { case (peerId, _) => peerId =!= selfId }.toList
                                      .traverse[F, Boolean] {
                                        case (_, att) =>
                                          chainStore.walkBackTo(tip.hash, att.tipOrdinal).map {
                                            case Some(localHash) if localHash === att.tipHash => true
                                            case _                                            => false
                                          }
                                      }
                                      .map(_.count(identity))
                                  case None => Async[F].pure(0)
                                }
                                countSnapshotF.flatMap { k =>
                                  logger.info(
                                    s"T_COUNT-FINALIZED ordinal=${countQualifying.value.value} count=$k/$validatorCount " +
                                      s"(V=$validatorCount, T_weight=${weightQualifying.value.value}, T_depth1=${depthQualifying.value.value})"
                                  )
                                }
                              }

                              // Heap-leak Fix A — operational-k₁ overlay prune.
                              //
                              // Drives `mptOverlay.pruneBelow` once per advance of `tDepth1.latestQualifyingOrdinal`
                              // (the operational k₁ finality trigger). Previously the only `pruneBelow` site was
                              // wired to `tDepth2` (k₂ ≈ 65536 ≈ 6 days at ~445 snapshots/h), so the overlay's
                              // `undoJournalRef` + `finalizedRef` accumulators grew linearly across every 12h soak.
                              // Decoupling to k₁ (≈ 255 ≈ 30 min at the same rate) bounds the in-memory overlay-
                              // history sets and was the dominant contributor to the leak documented in
                              // `test-runs/soak-12h-preserve-20260521-083116/HEAP-LEAK-DIAGNOSIS.md` §1.
                              //
                              // Safety: reorg-replace at depth > k₁ is excluded by `ConfirmationDepthK` semantics.
                              // The reorg-replace finalize paths in `MptOverlay.finalizeBranch` (`case Some(prev) =>`
                              // arms at MptOverlay.scala:703, :768) consult `undoJournalRef[ord]` only when a
                              // different canonical re-finalizes at the same `ord`. Beyond k₁ depth that would be
                              // a common-prefix violation — outside our adversarial model — not a recoverable
                              // chain switch. Dropping journal entries below the operational k₁ watermark
                              // therefore cannot break any reachable code path.
                              //
                              // The k₂ archival prune below this block is now a strict subset of what this k₁
                              // prune already did and is retained only for its side-effects (NIPoPoW tower
                              // finalizer drive at archival depth + ARCHIVAL-FINALIZED log line). The
                              // `mptOverlay.pruneBelow(archivalQualifying)` call there becomes a no-op once this
                              // k₁ block has already dropped its slice.
                              operationalPruneQualifying <- tDepth1.latestQualifyingOrdinal
                              _ <- lastOperationalPruneOrdinalRef.get.flatMap { prev =>
                                if (operationalPruneQualifying.value.value > prev.value.value)
                                  lastOperationalPruneOrdinalRef.set(operationalPruneQualifying) >>
                                    mptOverlay.pruneBelow(operationalPruneQualifying) >>
                                    Metrics[F].updateGauge(
                                      "dag_nakamoto_overlay_prune_ordinal",
                                      operationalPruneQualifying.value.value
                                    )
                                else Async[F].unit
                              }

                              // T_depth2 (Phase 2 → Phase 3, ARCHIVAL) observability + scaffolding.
                              //
                              // When `tDepth2.latestQualifyingOrdinal` strictly outruns our local archival
                              // watermark, advance the watermark, emit the log line + counters, AND drive
                              // the Phase-3 overlay-history prune sink (#139) so long-running nodes don't
                              // leak the per-ordinal undo journal / finalizedRef accumulators that grow
                              // monotonically with every finalize. Future Phase-3 sinks (Mithril aggregate
                              // cert, light-client trust anchor) plug in here too.
                              //
                              // Heap-leak Fix A note: the `mptOverlay.pruneBelow(archivalQualifying)` call
                              // below is now effectively a no-op for `undoJournalRef`/`finalizedRef` because
                              // the k₁-driven prune above already dropped entries below `tDepth1` (which is
                              // strictly less than `tDepth2`). It remains in place so the archival pattern
                              // is still self-contained — adding Mithril / light-client sinks later won't
                              // need to coordinate with the operational-prune block.
                              archivalQualifying <- tDepth2.latestQualifyingOrdinal
                              _ <- lastArchivalOrdinalRef.get.flatMap { prev =>
                                if (archivalQualifying.value.value > prev.value.value)
                                  bestTip match {
                                    case Some(tip) =>
                                      lastArchivalOrdinalRef.set(archivalQualifying) >>
                                        logger.info(
                                          s"ARCHIVAL-FINALIZED ordinal=${archivalQualifying.value.value} " +
                                            s"(tip ord=${tip.ordinal}, k₂=$ArchivalDepthK)"
                                        ) >>
                                        Metrics[F].incrementCounter("dag_nakamoto_archival_finalized") >>
                                        Metrics[F].updateGauge("dag_nakamoto_archival_ordinal", archivalQualifying.value.value) >>
                                        // Phase-3 archival prune (#139). `pruneBelow` drops overlay history
                                        // entries strictly below the new archival watermark — irreversible
                                        // and safe by depth-k₂ definition (reorgs at this depth excluded).
                                        // Logged at INFO inside the overlay when entries are dropped.
                                        mptOverlay.pruneBelow(archivalQualifying) >>
                                        // §3 NIPoPoW S3 Phase-3 sink. Walk newly-archival ordinals
                                        // `(prev, archivalQualifying]` and feed each to the tower
                                        // finalizer. The walk uses chainStore.walkBackTo from bestTip to
                                        // resolve canonical hashes; snapshotStorage.get fetches the
                                        // signed snapshot; toHashed re-computes the content hash. The
                                        // finalizer's high-water-mark Ref guards against double-write.
                                        //
                                        // Bounded by `TowerFinalizerMaxCatchupPerTick` so catch-up after
                                        // a process restart doesn't stall the leader loop's 5s tick.
                                        // Remaining ordinals roll forward on the next tick — the
                                        // finalizer's high-water-mark ensures forward-only progression
                                        // even if the loop misses a window.
                                        //
                                        // Background-fire (`Async.start`) so the chain walk + L-1
                                        // continued-fraction trial computations don't block the
                                        // finality monitor's other sinks.
                                        Async[F]
                                          .start(
                                            driveTowerFinalizer(
                                              chainStore = chainStore,
                                              snapshotStorage = snapshotStorage,
                                              towerFinalizer = towerFinalizer,
                                              tipHash = tip.hash,
                                              prevWatermark = prev,
                                              archivalQualifying = archivalQualifying,
                                              logger = logger
                                            )
                                          )
                                          .void
                                    case None =>
                                      // Trigger advanced without a tip — shouldn't happen because the
                                      // trigger's evaluator returns MinValue when bestTip is None.
                                      // Still, advance the Ref idempotently so we don't re-log on the
                                      // next tick.
                                      lastArchivalOrdinalRef.set(archivalQualifying)
                                  }
                                else Async[F].unit
                              }
                            } yield ()
                          }

                        seedChainStore ++ slotTick.merge(finalityMonitor)
                      }
                    }
                  }
              }
            }
          }
        }
      }
    }
  }

  /** Called when VRF lottery is won for a slot. Produces, signs, stores, and publishes a snapshot. */
  private def onSlotWon[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    stateRef: Ref[F, LoopState],
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    keyPair: KeyPair,
    selfId: PeerId,
    vrfSeed: Array[Byte],
    vrfPK: Array[Byte],
    proof: Array[Byte],
    vrfOutput: Array[Byte],
    currentEta: Array[Byte],
    currentSlot: Long,
    slotGap: Long,
    slotRefined: Slot,
    lddConfig: LddConfig,
    etaRotationSnapshots: Long,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    productionGate: ProductionGate[F],
    productionTimestamps: Ref[F, Map[Long, Long]],
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: MptOverlay[F, GlobalStateKey],
    // §1.2 Slice 6: KES parallel-signing for the published snapshot's `kes_signature` field.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    // Gap A — per-shard checkpoint producers + the SAME chain stores they write into + the static
    // metagraph→shard assignment. Empty / None at numShards=1 (regression bar) ⇒ the fan-out below is
    // a no-op `traverse_` over the empty map. Passed through from `run`'s same-named params.
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers (the producer fan-out input — the inversion).
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded into the producer fan-out (membership gate). See `run`'s param.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      EtaPeriod
    ) => F[Set[io.constellationnetwork.schema.peer.PeerId]],
    // Task #12 slice 2b — the gl0 changeset STAGING map (threaded from `run`'s same-named param). The
    // produce path below rekeys the staged accumulator raw->with-cert alongside the overlay rekey so the
    // finalize-sink promotion (`recordFinalizedAccumulator`, in `run`) can find it under the canonical hash.
    pendingAccumulatorsRef: Ref[F, Map[Hash, StateChangesAccumulator]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    // Slice S6: app-scoped Supervisor for the off-critical-path producer fan-out (see Gap A below).
    implicit supervisor: Supervisor[F]
  ): F[Unit] = {
    HasherSelector[F].withCurrent { implicit hasher =>
      for {
        state <- stateRef.get
        // Build SlotCertificate — use the chain-derived eta that was used for VRF evaluation
        proofHex = Hex(proof.map("%02x".format(_)).mkString)
        vrfOutputHex = Hex(vrfOutput.map("%02x".format(_)).mkString)
        pkHex = Hex(vrfPK.map("%02x".format(_)).mkString)
        etaHash = Hash(currentEta.map("%02x".format(_)).mkString)

        activePool <- stakeRegistry.activeValidators
        activePoolSize = activePool.size
        activePoolHashBytes = java.security.MessageDigest
          .getInstance("SHA-256")
          .digest(activePool.toList.map(_.value.value).sorted.mkString(",").getBytes("UTF-8"))
        activePoolHash = Hash(activePoolHashBytes.map("%02x".format(_)).mkString)

        parentSlotValue = currentSlot - slotGap // = lastKnownSlot at time of VRF evaluation
        cert = SlotCertificate(
          slot = slotRefined,
          parentSlot = Slot(NonNegLong.unsafeFrom(math.max(0L, parentSlotValue))),
          vrfProof = VrfProof(proofHex),
          vrfOutput = VrfOutput(vrfOutputHex),
          vrfPublicKey = VrfPublicKey(pkHex),
          eta = etaHash,
          activePoolSize = activePoolSize,
          activePoolHash = activePoolHash,
          // §3 NIPoPoW S2 phase 2b-1: plumb the field with zeros. Phase 2b-2 wires the producer to
          // compute via SubchainStateUpdater.updateFrom(parent.subchainLevelCounts, levelTrials).
          subchainLevelCounts = SlotCertificate.ZeroSubchainLevelCounts
        )

        _ <- logger.info(s"WON slot $currentSlot (gap=$slotGap, parentSlot=$parentSlotValue, pool=$activePoolSize) — producing snapshot")
        _ <- Metrics[F].incrementCounter("dag_nakamoto_slots_won")
        // Per-(self, eta_period, phase) win counter: decomposes win-count skew across nodes into
        // within-period (eta-sk pairing luck) vs across-period (persistent sk bias), AND across the
        // two LDD threshold regimes. Ramp and baseline phases have very different per-slot success
        // probabilities (ramp: 0…fA across gap range; baseline: fB ≈ 5%), so a mixed counter compares
        // apples to oranges. Tagging by phase lets us run a uniformity test independently in each
        // regime — both should be ≈ uniform across nodes under unbiased VRF + equal stake.
        //
        // etaPeriod is keyed on ordinal (matches the rotation unit migration; see
        // `docs/nakamoto/attestation-and-finality.md` §1).
        lastChainOrdinal <- chainStore.bestTipOrdinal.map(_.getOrElse(0L))
        etaPeriod = lastChainOrdinal / etaRotationSnapshots
        phase =
          if (slotGap < lddConfig.offset.toLong) "dormant"
          else if (slotGap < lddConfig.lddCutoff.toLong) "ramp"
          else "baseline"
        _ <- Metrics[F].incrementCounter(
          "dag_nakamoto_slots_won_by_period",
          Seq(
            Metrics.unsafeLabelName("eta_period") -> etaPeriod.toString,
            Metrics.unsafeLabelName("phase") -> phase
          )
        )
        _ <- Metrics[F].updateGauge("dag_nakamoto_slot", currentSlot)
        _ <- Metrics[F].recordDistribution("dag_nakamoto_slot_gap", slotGap.toInt)

        // Get last snapshot from storage
        headOpt <- snapshotStorage.head
        finalizedOrdinal <- nakamotoFinalizedOrdinalRef.get

        _ <- headOpt match {
          case Some((lastSigned, lastContext)) =>
            lastSigned.toHashed[F].flatMap { lastHashed =>
              val lastKey = lastHashed.ordinal
              val wouldProduceOrdinal = SnapshotOrdinal.next.next(lastKey)
              // Finality-safety: never produce a snapshot at-or-below the network-finalized
              // ordinal. If we won a slot but our local head is stale (below finalized),
              // producing would emit a doomed snapshot — refused downstream by
              // NakamotoChainStore's finality guard, and in a past storage-layer regression
              // could have silently overwritten finalized content (the gl0-2-divergent-517
              // class of bug). Skip and let the sync daemon catch us up via gossip/chainsync;
              // the next slot win after catch-up produces the correct (above-finalized) ordinal.
              if (wouldProduceOrdinal <= finalizedOrdinal) {
                logger.warn(
                  s"⛔ Skipping slot-win production: would-be ordinal=${wouldProduceOrdinal.show} ≤ finalized=${finalizedOrdinal.show}. " +
                    s"Local head=${lastKey.show} is stale; waiting for gossip/chainsync to catch up."
                )
              } else
                for {
                  productionStartMs <- Async[F].delay(System.currentTimeMillis())

                  // Drain mempool — getMultiple returns Hashed[Event]
                  // Hashed[A].signed.value gives the raw A
                  eventHashes <- eventMempool.getEventHashes
                  hashedEvents <- eventMempool.getMultiple(eventHashes)
                  eventSet: Set[GlobalSnapshotEvent] = hashedEvents.values.map(_.signed.value).toSet
                  _ <- Metrics[F].recordDistribution("dag_nakamoto_events_drained", eventSet.size)

                  // Wrap createProposalArtifact + gate check + chainStore.store in a single
                  // MPT transaction. createProposalArtifact calls accept() which mutates the
                  // MPT (apply deltas to compute stateProof). We Commit only when we both
                  // pass the pre-sign gate AND chainStore accepts the write — i.e. this
                  // proposal is going on the chain. On gate-closed abandonment OR a refused
                  // chainStore write, Rollback so the canonical MPT is unchanged.
                  txOutcome <- mptStore.withTransaction {
                    for {
                      result <- consensusFns.createProposalArtifact(
                        lastKey = lastKey,
                        lastArtifact = lastSigned,
                        lastContext = lastContext,
                        lastArtifactHasher = hasher,
                        trigger = TimeTrigger, // epoch progress increments on TimeTrigger
                        events = eventSet,
                        facilitators = Set(selfId), // single producer, no facilitator set
                        getGlobalSnapshotByOrdinal = ordinal =>
                          snapshotStorage.get(ordinal).flatMap {
                            case Some(s) => s.toHashed[F].map(_.some)
                            case None =>
                              chainStore.getByOrdinal(ordinal.value.value).flatMap {
                                case Some(stored) => stored.signedSnapshot.toHashed[F].map(_.some)
                                case None         => none[Hashed[GlobalIncrementalSnapshot]].pure[F]
                              }
                          }
                      )
                      (rawArtifact, context, returnedEvents) = result
                      _ <- Metrics[F].recordDistribution("dag_nakamoto_events_returned", returnedEvents.size)
                      _ <- Metrics[F].recordDistribution("dag_nakamoto_events_accepted", eventSet.size - returnedEvents.size)

                      // Pre-sign gate check — abandon if a better gossip snapshot arrived
                      // during the (slow) acceptance pipeline. Skips signature + chain store write.
                      gateOpenPreSign <- productionGate.isOpen
                      _ <-
                        if (!gateOpenPreSign)
                          productionGate.pauseReasons.flatMap(reasons =>
                            logger.info(
                              s"🛑 Abandoning production at slot $currentSlot before sign (gate closed: ${reasons.mkString(", ")}). MPT rolled back."
                            ) >>
                              Metrics[F].incrementCounter("dag_nakamoto_production_abandoned_total")
                          )
                        else Async[F].unit

                      // Hash the rawArtifact: this is the BranchId under which `createProposalArtifact`
                      // committed the overlay handle (line 502 of GlobalSnapshotConsensusFunctions:
                      // `currentSnapshotHash <- hasher.hash(globalSnapshot)`). The chain's canonical
                      // reference for this ordinal is the with-cert hash (`signedHashed.hash`); we
                      // re-key the overlay branch from raw → with-cert so ord N+1's
                      // `checkout(BranchId(getLastArtifactHash))` finds the parent.
                      rawArtifactHash <- hasher.hash(rawArtifact)
                      // §3 NIPoPoW S2 phase 2b-2: carry forward parent's subchainLevelCounts. Until
                      // S3 (TowerStore) lands and the producer can compute real level-trial passes,
                      // the L-vector is a no-op pass-through — parent's value flows verbatim into
                      // each new snapshot. Genesis (no parent cert) anchors at ZeroSubchainLevelCounts.
                      // The verifier (NakamotoSnapshotValidator) enforces parity: incoming
                      // subchainLevelCounts must equal parent's.
                      parentSubchainLevelCounts = lastSigned.value.slotCertificate
                        .fold(SlotCertificate.ZeroSubchainLevelCounts)(_.subchainLevelCounts)
                      certWithSubchain = cert.copy(subchainLevelCounts = parentSubchainLevelCounts)
                      artifact = rawArtifact.copy(slotCertificate = Some(certWithSubchain), eta = Some(etaHash))
                      signed <- Signed.forAsyncHasher[F, GlobalIncrementalSnapshot](artifact, keyPair)
                      snapshotHashedForStorage <- signed.toHashed[F]
                      parentHashValue = lastHashed.hash
                      stored <-
                        if (gateOpenPreSign)
                          chainStore.store(
                            signed,
                            context,
                            lastKey.value.value + 1,
                            currentSlot,
                            parentHashValue,
                            vrfOutput
                          )
                        else Async[F].pure(false)
                      // Rekey the overlay branch on success; discard it on abandonment.
                      // Done inside the `mptStore.withTransaction` block so the overlay update is
                      // bundled with the underlying-MPT mutation: if the tx rolls back (action =
                      // Rollback), the underlying writes are reverted but the overlay's
                      // `discardBranch` has already cleaned up the now-orphan `pendingRef` entry.
                      _ <-
                        if (stored)
                          // Rekey the overlay branch AND the gl0 changeset staging map raw -> with-cert. The
                          // accumulator was staged (GlobalSnapshotConsensusFunctions) under the RAW artifact hash
                          // (== `rawArtifactHash`, pre slotCertificate+eta), but the finalize-sink promotion
                          // (`recordFinalizedAccumulator`) looks it up under the with-cert canonical hash
                          // (`snapshotHashedForStorage.hash`). Without this rekey the served ring never populates
                          // for self-produced snapshots — the same raw->with-cert problem the overlay rekey beside
                          // it already solves.
                          mptOverlay.rekey(BranchId(rawArtifactHash), BranchId(snapshotHashedForStorage.hash)) >>
                            pendingAccumulatorsRef.update(rekeyStagedAccumulator(_, rawArtifactHash, snapshotHashedForStorage.hash))
                        else
                          // Abandoned fork: drop both the overlay branch and its staged accumulator.
                          mptOverlay.discardBranch(BranchId(rawArtifactHash)) >>
                            pendingAccumulatorsRef.update(_ - rawArtifactHash)
                      action: MptTxAction = if (stored) MptTxAction.Commit else MptTxAction.Rollback
                    } yield ((signed, context, returnedEvents, stored, snapshotHashedForStorage, parentHashValue), action)
                  }
                  (signed, context, returnedEvents, stored, snapshotHashedForStorage, parentHashValue) = txOutcome

                  // Update ALL snapshot storages — snapshotStorage.head is what the leader loop
                  // reads on the next slot to determine the parent ordinal. Without this, the
                  // leader loop re-reads the last GOSSIP ordinal and re-produces the same ordinal.
                  _ <- Async[F].whenA(stored) {
                    snapshotStorage.setHeadForRecovery(signed, context) >>
                      lastGlobalSnapshotStorage.setForRecovery(snapshotHashedForStorage, context) >>
                      lastNGlobalSnapshotStorage.setForRecovery(snapshotHashedForStorage, context) >>
                      lastKnownSlotRef.set(Some(currentSlot)) >>
                      snapshotStorage.confirmHead(parentHashValue)
                  }

                  // Clear included events from mempool (returned events were NOT included).
                  // Only when stored — if we abandoned pre-sign, the events must stay in the
                  // mempool so the next slot winner (or our next slot) can include them.
                  includedHashes = hashedEvents.collect {
                    case (h, hashed) if !returnedEvents.contains(hashed.signed.value) => h
                  }.toSet
                  _ <- Async[F].whenA(stored)(eventMempool.clearIncluded(includedHashes))

                  // Check gate before publishing — a better gossip snapshot may have arrived
                  // during proposal creation. If gate is closed, abandon this production.
                  stillOpen <- productionGate.isOpen
                  _ <-
                    if (!stillOpen)
                      productionGate.pauseReasons.flatMap(reasons =>
                        logger
                          .info(s"🛑 Abandoning production at slot $currentSlot before publish (gate closed: ${reasons.mkString(", ")})")
                      )
                    else Async[F].unit

                  // Publish + self-attest only if gate is still open
                  snapshotHash = snapshotHashedForStorage.hash
                  producedOrdinal = lastKey.value.value + 1
                  // §1.2 Slice 6: parallel-sign the snapshot's hash bytes with KES BEFORE we
                  // publish, then embed the resulting bytes in the pb.Snapshot's `kes_signature`
                  // field. Period is derived from the produced ordinal (`lastKey + 1`) — receivers
                  // re-derive the same period from `snap.ordinal` so verification stays self-
                  // consistent. Sender failure → empty wire field, receiver treats as no-sig.
                  //
                  // The message bytes ARE the snapshot hash as a UTF-8 string (matching the
                  // sidecar's existing `hash` byte representation and the receiver's
                  // `snap.hash.toByteArray` extraction in NakamotoSyncDaemon.verifyKesSnapshot).
                  kesPeriodSnap = EtaCalculation.rotationPeriod(producedOrdinal, etaRotationSnapshots).toInt
                  snapshotHashBytes = snapshotHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                  kesSnapAttempt <-
                    if (stillOpen) operationalKeyMaker.signAt(kesPeriodSnap, snapshotHashBytes)
                    else
                      Async[F]
                        .pure[
                          Either[io.constellationnetwork.security.kes.KesError, io.constellationnetwork.security.kes.SignatureKesProduct]
                        ](
                          Left(io.constellationnetwork.security.kes.KesError.MalformedTree("skipped — gate closed"))
                        )
                  kesSnapSigBytes <- kesSnapAttempt match {
                    case Right(kSig) =>
                      val bytes = io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(kSig)
                      Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_signed_total") >>
                        logger
                          .info(
                            s"🔐 KES-SNAP ord=$producedOrdinal period=$kesPeriodSnap sub-sig=${bytes.take(8).map("%02x".format(_)).mkString} (${bytes.length}B)"
                          )
                          .as(bytes)
                    case Left(err) if stillOpen =>
                      Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_sign_failed_total") >>
                        logger
                          .warn(
                            s"⚠️ KES-SNAP sign failed for ord=$producedOrdinal period=$kesPeriodSnap: ${err.message} — emitting unsigned wire field"
                          )
                          .as(Array.empty[Byte])
                    case Left(_) =>
                      // gate closed → no publish, sig not needed; return empty for path uniformity
                      Async[F].pure(Array.empty[Byte])
                  }
                  _ <- Async[F].whenA(stillOpen) {
                    sidecarClient
                      .publishSnapshot(
                        SidecarClient.mkSnapshot(
                          hash = snapshotHash.value.getBytes,
                          slot = currentSlot,
                          ordinal = producedOrdinal,
                          parentHash = lastHashed.hash.value.getBytes,
                          vrfProof = proof,
                          vrfPublicKey = vrfPK,
                          eta = currentEta,
                          payload = {
                            import io.circe.syntax._
                            val snapshotJson = signed.asJson
                            val contextJson = context.asJson
                            val combined = io.circe.Json.obj("snapshot" -> snapshotJson, "context" -> contextJson)
                            combined.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                          },
                          producerId = selfId.value.toBytes,
                          parentSlot = parentSlotValue,
                          kesSignature = kesSnapSigBytes
                        )
                      )
                      .void
                      .handleErrorWith(e => logger.warn(s"Sidecar publish failed: ${e.getMessage}")) >>
                      // Unify self-attestation with peer-attestation: route through the same emit
                      // function the SyncDaemon uses on processValidSnapshot. Eliminates the
                      // double-implementation where the two paths used different `attestedAt` units
                      // and peer attestations shadowed self-attestations under TipTracker's "newer
                      // wins" rule (TipTracker:96-105).
                      //
                      // `attestedAt` is wall-clock epoch ms via `Clock[F].realTime` (NOT
                      // System.currentTimeMillis()). Wall-clock semantics here are deliberate —
                      // keeps the `attestedAt` field reusable as a future Ouroboros-Chronos-style
                      // timestamp gossip surface, per docs §3.1.
                      Clock[F].realTime.map(_.toMillis).flatMap { attestedAt =>
                        NakamotoSyncDaemon.emitTipAttestation[F](
                          tipHash = snapshotHash,
                          tipSlot = slotRefined,
                          tipOrdinal = producedOrdinal,
                          attestedAt = attestedAt,
                          sidecarClient = sidecarClient,
                          tipTracker = tipTracker,
                          selfId = selfId,
                          keyPair = keyPair,
                          operationalKeyMaker = operationalKeyMaker,
                          etaRotationSnapshots = etaRotationSnapshots,
                          logger = logger
                        )
                      } >>
                      logger.info(
                        s"Produced snapshot ordinal=$producedOrdinal slot=$currentSlot " +
                          s"events=${eventSet.size} returned=${returnedEvents.size} pool=$activePoolSize"
                      ) >>
                      Metrics[F].incrementCounter("dag_nakamoto_snapshots_produced") >>
                      Metrics[F].updateGauge("dag_nakamoto_ordinal", producedOrdinal) >>
                      Async[F].delay(System.currentTimeMillis()).flatMap { nowMs =>
                        Metrics[F].recordDistribution("dag_nakamoto_production_duration_ms", (nowMs - productionStartMs).toInt) >>
                          productionTimestamps.update { ts =>
                            val updated = ts + (producedOrdinal -> nowMs)
                            // Evict entries older than 1000 ordinals to bound memory
                            if (updated.size > 1000) updated.toList.sortBy(-_._1).take(1000).toMap
                            else updated
                          }
                      }
                  }

                  // ─── Gap A — shard-checkpoint producer fan-out (gl0-leader self-call) ───────────
                  // After the gl0 snapshot is produced (+ published when the gate stayed open), drive each
                  // per-shard checkpoint producer for THIS leader's own produced ord, via the shared
                  // `ShardCheckpointFanOut.run` body (the SAME body the daemon invokes for gossip-received
                  // ords — see `NakamotoSyncDaemon.processValidSnapshotInner`).
                  //
                  // '''Why this self-call is required, not redundant.''' GossipSub does NOT echo a publisher
                  // its own message, so the gl0 leader never sees its own produced ord arrive on the daemon's
                  // gossip path. Without this seam the leader would skip producing shard checkpoints for every
                  // ord it produced. Together with the daemon's per-ord hook this fires the fan-out exactly
                  // once per canonical ord per node (leader here for its own ord; everyone else via the daemon).
                  //
                  // EMPTY map at `numShards = 1` ⇒ `whenA(false)` ⇒ this is never entered (regression bar).
                  // `produce(...)` returns `None` unless THIS node won the shard slot lottery, and `None` on
                  // an empty per-shard slice (§15.5 content-only), so it's a no-op on non-leader shards /
                  // empty shards.
                  //
                  // Gated on `stillOpen` so we only fan out when the gl0 snapshot was actually published +
                  // chain-stored — abandoning gl0 production (gate closed pre-publish, MPT rolled back) must
                  // NOT produce a shard checkpoint anchored to a gl0 ord that never committed.
                  //
                  // ─── Slice S6 — OFF the snapshot-processing critical path ───────────────────────────
                  // `onSlotWon` runs INSIDE `snapshotSemaphore.permit.use` (see the call site in `run`). After
                  // S3 the fan-out → `producer.produce` → `derivePerMgState` runs a full
                  // `processCurrencySnapshots` re-execution per shard per ord — heavy. Running it inline under
                  // the permit serialized that re-exec into every leader's per-ord production window and made
                  // straggler nodes lag (one fell ~13 ords behind, tripping the ±3 cluster-sync check). We
                  // launch it on the app-scoped `Supervisor` (lifetime-scoped — NOT a bare `Async.start`,
                  // which previously leaked a fiber outliving its owner).
                  //
                  // DETERMINISM (why async is safe for the gl0 snapshot):
                  //   1. Inputs captured BY VALUE here (`capturedStateChannelSnapshots`, `producedOrd`,
                  //      `epoch`) before `supervise`, so the fiber sees the snapshot for THIS produced ord,
                  //      not a later one. `signed.value.stateChannelSnapshots` is an immutable `SortedMap`.
                  //   2. We're past the gl0 publish + chainStore write here (`stillOpen` confirms the gl0
                  //      snapshot committed). The fan-out only SELF-stores its checkpoint into the per-shard
                  //      `ShardChainStore` and publishes it — it never feeds back into the gl0 snapshot just
                  //      produced, so inline-vs-fiber cannot change the gl0 snapshot's bytes.
                  //   3. `ShardChainStore.store` is idempotent by hash + monotonic by shard ordinal, so the
                  //      async write racing the next ord's read dedups safely.
                  _ <- Async[F].whenA(stillOpen && shardProducers.nonEmpty && shardAssignment.isDefined) {
                    // R-1: the fan-out reads each shard's `shardBinaryBuffers` (the inversion), NOT the just-produced
                    // snapshot's `stateChannelSnapshots`. We still only fan out when the gl0 snapshot actually
                    // committed (`stillOpen`), so a checkpoint is never anchored to a gl0 ord that never committed.
                    val producedOrd =
                      SnapshotOrdinal(NonNegLong.unsafeFrom(producedOrdinal))
                    val rotationPeriod = EtaCalculation.rotationPeriod(producedOrdinal, etaRotationSnapshots)
                    val epoch = EtaPeriod(rotationPeriod)
                    supervisor
                      .supervise(
                        HasherSelector[F].withCurrent { implicit hasher =>
                          io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointFanOut.run[F](
                            shardBinaryBuffers = shardBinaryBuffers,
                            producedOrd = producedOrd,
                            epoch = epoch,
                            shardProducers = shardProducers,
                            shardChainStores = shardChainStores,
                            selfPeerId = selfId,
                            committeeMembership = shardCommitteeMembership,
                            logger = logger
                          )
                        }.handleErrorWith(e => logger.warn(e)(s"🧩 Shard producer fan-out failed for ord=$producedOrdinal"))
                      )
                      .void
                  }
                } yield ()
            }

          case None =>
            logger.warn(s"No head snapshot in storage — skipping slot $currentSlot (genesis not yet loaded?)")
        }

        // No in-memory epoch state update needed — eta is chain-derived.
        // VRF output is stored in NakamotoChainStore as part of the snapshot.

        _ <- stateRef.update { s =>
          s.copy(
            lastProducedSlot = Some(currentSlot),
            lastKnownSlot = Some(currentSlot),
            totalProduced = s.totalProduced + 1
          )
        }

      } yield ()
    } // withCurrent
  }
}
