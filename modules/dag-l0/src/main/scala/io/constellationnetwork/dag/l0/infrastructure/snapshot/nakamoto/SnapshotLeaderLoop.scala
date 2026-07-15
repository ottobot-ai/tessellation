package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Supervisor
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
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
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Shared epoch state — used by BOTH SnapshotLeaderLoop (production) and NakamotoSyncDaemon (gossip).
  *
  * VRF outputs from ALL sources (own production + received gossip) are accumulated here. When 2/3 of the eta-rotation EPOCH
  * (`etaRotationSnapshots` = R = round(3.1·k₁) SNAPSHOTS) outputs are collected, eta rotates — uniformly across all validators, not just
  * producers. (This is the eta EPOCH, measured in snapshots — NOT epoch progress, which is a separate slot-based counter.)
  */
final case class SharedEpochState(
  currentEta: Array[Byte],
  genesisEta: Array[Byte],
  vrfAccumulator: List[Array[Byte]]
)

object SharedEpochState {
  def initial(genesisEta: Array[Byte]): SharedEpochState =
    SharedEpochState(currentEta = genesisEta, genesisEta = genesisEta, vrfAccumulator = Nil)

  /** Accumulate a VRF output and rotate eta if threshold reached. Eta rotates every `etaRotationSnapshots` = R = round(3.1·k₁) (threaded
    * from config; no source default), not every epoch-progress tick. Rotation is keyed on **ordinal** to satisfy the R ≥ 3·k₁ stability
    * bound (slots are LDD-paced and lumpy; ordinals give a stable R). See `docs/nakamoto/attestation-and-finality.md` §1.
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

/** Nakamoto/Taktikos global consensus loop.
  *
  * Replaces the entire BFT round system (Facility → Proposal → Signature → Finished). No multi-party coordination. No rounds. No
  * facilitators.
  *
  * Flow:
  *   1. Every slot tick evaluates VRF eligibility via the LDD snowplow. 2. A winner drains the mempool, executes and validates the
  *      proposal, signs, stores, and publishes it. 3. Other validators receive it through GossipSub and independently validate it. 4. The
  *      target FinalityGate makes a canonical exact hash operational after a portable K/alpha/beta decided-attestation result or canonical
  *      k1 depth. This is an optimistic finality gadget over Nakamoto fork choice, not a BFT vote/lock/QC.
  *
  * Optimistic emission and its state-changing sink remain dark until replay-and-preference capabilities and the sampled cascade are
  * complete. Canonical k1 depth is the sole live Phase-2 rail in the interim.
  */
object SnapshotLeaderLoop {

  /** Fail-closed control-flow seam for the producer's slot-lineage gate. `validated` is by-name so an invalid locally constructed artifact
    * cannot evaluate any Ed25519/KES signing or chain-store effect. The caller owns branch/staging cleanup and the enclosing MPT
    * transaction returns `Rollback`; a rejected slot is an ordinary `None`, not an exception that can terminate the slot stream.
    */
  private[nakamoto] def afterProducerSlotLineageValidation[F[_]: cats.Monad, A](
    lineage: Either[String, Unit]
  )(
    onInvalid: String => F[Unit]
  )(
    validated: => F[A]
  ): F[Option[A]] =
    lineage match {
      case Left(reason) => onInvalid(reason).as(none[A])
      case Right(_)     => validated.map(_.some)
    }

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
    staged: Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)],
    rawHash: Hash,
    withCertHash: Hash
  ): Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)] =
    staged
      .get(rawHash)
      .fold(staged)(entry => (staged - rawHash).updated(withCertHash, entry))

  /** 3c-A enabler — the signed-bytes analogue of [[rekeyStagedAccumulator]]. The signed `postBytes` are staged under the RAW artifact hash
    * at the `overlay.commit` site; the finalize-sink promotion looks them up under the with-cert canonical hash. Move `rawHash →
    * withCertHash` (no-op if nothing staged). MUST be called everywhere `rekeyStagedAccumulator` is, against `pendingPostBytesRef`, or the
    * bytes never promote (the served store stays empty and the byte route falls back to legacy — safe but inert).
    */
  def rekeyStagedPostBytes(
    staged: Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])],
    rawHash: Hash,
    withCertHash: Hash
  ): Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])] =
    staged
      .get(rawHash)
      .fold(staged)(entry => (staged - rawHash).updated(withCertHash, entry))

  /** Finalize-sink FINALIZED-WATERMARK prune: keep only staged entries STRICTLY ABOVE the just-finalized `finalizedOrdinal` (still in
    * flight). Every entry at-or-below it is either the snapshot we just promoted or a fork candidate at a now-finalized height that can
    * never finalize on the canonical chain (depth-k finality settles ≤ finalized-tip), so it is dead. This is the correct-by-construction
    * replacement for the earlier arbitrary size-`.drop` eviction (which could discard a not-yet-finalized entry under the depth-k retention
    * window + reorg churn). Pure; idempotent; never errors.
    */
  def pruneStagedAtOrBelow(
    staged: Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)],
    finalizedOrdinal: SnapshotOrdinal
  ): Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)] =
    staged.filter { case (_, (o, _)) => o.value.value > finalizedOrdinal.value.value }

  /** 3c-A enabler — signed-bytes analogue of [[pruneStagedAtOrBelow]]. Keep only staged byte maps STRICTLY ABOVE the finalized tip (a reorg
    * loser at-or-below the finalized height is dead and dropped). Pure; idempotent; never errors.
    */
  def pruneStagedPostBytesAtOrBelow(
    staged: Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])],
    finalizedOrdinal: SnapshotOrdinal
  ): Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])] =
    staged.filter { case (_, (o, _)) => o.value.value > finalizedOrdinal.value.value }

  /** Finalize-sink ring insert: put `acc` at `ordinal` into the served ordinal-keyed ring, trimmed to the last `recentAccumulatorsToKeep`
    * (dropping the lowest ordinals). The cap is the typed `nakamoto.changeset-ring-depth` HOCON value
    * (`SharedConfig.nakamoto.changesetRingDepth`, default 1024), threaded in from `GlobalSnapshotConsensus.make` — a pure transport memory
    * bound, NOT a consensus parameter. The promote at the finalize sink pulls `acc` out of staging atomically and then calls this to insert
    * it; idempotent on re-finalize of the same ordinal (overwrites with the same value).
    */
  def ringInsertTrimmed(
    ring: scala.collection.immutable.SortedMap[SnapshotOrdinal, StateChangesAccumulator],
    ordinal: SnapshotOrdinal,
    acc: StateChangesAccumulator,
    recentAccumulatorsToKeep: Int
  ): scala.collection.immutable.SortedMap[SnapshotOrdinal, StateChangesAccumulator] = {
    val withNew = ring.updated(ordinal, acc)
    if (withNew.size > recentAccumulatorsToKeep)
      withNew.drop(withNew.size - recentAccumulatorsToKeep)
    else withNew
  }

  /** Emit the chain-quality gauge + per-kind "fired" counters for task #138.
    *
    * Called from the current depth-k1 finalize site. Counter names are spelled out literally so the [[Metrics.MetricKey]] refinement
    * (compile-time regex match) is satisfied — a `s"…${k.name}…"` interpolation can't be refined at compile time. Kept private to this
    * object so neither the metric names nor the choice of "which triggers count" leak into the shared `FinalityTrigger` API (which would
    * create a circular dep on the metric refinement).
    *
    * The legacy `T_depth2` kind is excluded here. Its current counter is retention telemetry only: target GL0 has no Phase 3, and k2 never
    * qualifies a snapshot or constrains fork choice.
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
    * `ShardCheckpointProducer`'s registered-key possession proof. Public execution membership and staircase duty do not use this secret as
    * a lottery input.
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
    * @param etaRotationSnapshots
    *   R — the eta-rotation EPOCH length in SNAPSHOTS, = round(3.1·k₁) (derived from k₁; the last R/3 ≥ k₁ finalizes before the nonce is
    *   consumed ⇒ R ≥ 3·k₁). Rotation is keyed on **ordinal**, not slot — see `docs/nakamoto/attestation-and-finality.md` §1. (NOT 10·k₁ —
    *   the old "2550 = 10·k₁" label was stale.)
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
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    nodeStorage: NodeStorage[F],
    keyPair: KeyPair,
    selfId: PeerId,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    // The four consensus-time params below are REQUIRED — no source-level default. The sole runtime caller
    // (`GlobalSnapshotConsensus.make`) threads each from the per-env HOCON config; a stale literal here would silently
    // diverge from that config for any future caller (k is our primary security knob — it must never default). Zero
    // test callers construct `run` (tests exercise the static helpers only), so requiring them is safe.
    // R = eta-rotation EPOCH length in SNAPSHOTS = round(3.1·k₁), derived in `NakamotoConfig` from the per-env k₁
    // (the last R/3 ≥ k₁ finalizes before the nonce is consumed ⇒ R ≥ 3·k₁). Threaded from
    // `sharedCfg.nakamoto.etaRotationSnapshots(env).value`. (NOT 10·k₁ = 2550 — that was a stale pre-2026 label.)
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ — threaded from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value`
    // (replaces the prior `sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH")` read; HOCON over scattered sys.env). Drives the
    // depth-k finality gate (`ConfirmationDepthK` below).
    confirmationDepthK: Long,
    // Legacy parameter name for k2 (= 100*k1), the recommended local retention/proof/recovery
    // capacity. Current code still feeds a TDepth2Trigger; that must remain telemetry/local tower
    // scheduling only and must not create Phase 3 or a fork-choice floor.
    archivalDepthK: Long,
    // Slot duration ms — threaded from `sharedCfg.nakamoto.slotDurationMs.value` at the call site (§5.7: the
    // consensus time UNIT; replaces the prior `sys.env.get("NAKAMOTO_SLOT_DURATION_MS")` read here).
    slotDurationMs: Long,
    // Shard boot grace (run-17): slots a node must have been Ready before taking ANY shard duty
    // (HOCON nakamoto.sharding.checkpoint.boot-grace-slots at the call site) — intake must drain
    // first or a late-booting rank takes genesis duty blind and seeds a rival lineage.
    shardBootGraceSlots: Long = 30L,
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
    // current canonical-depth chainStore.finalize call.
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
    // accumulator into the served ring and then remove that hash. EVERY gl0 node stages a candidate's accumulator
    // here — the PRODUCER via the produce path, and a NON-producer via `NakamotoSnapshotValidator.validate`'s
    // `validateArtifact` re-derivation (rekeyed stripped->canonical there, mirroring the producer's raw->with-cert
    // rekey) — so the promotion finds it under the canonical hash on every finalizer and the served ring is COMPLETE
    // cluster-wide (was per-producer-sparse → ml0 whiffed ~7/8). A finalized hash still absent (e.g. dropped pre-
    // finalize) is skipped — the ml0 follower full-GSI-adopts that gap; absence MUST NOT error. VALUE carries the
    // ordinal for the finalize-sink watermark prune. Same instance `GlobalSnapshotConsensus.make` injects into the
    // consensus functions AND the sync daemon.
    pendingAccumulatorsRef: Ref[F, Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)]],
    // 3c-A enabler — STAGING map for the signed MPT byte map (the SAME Ref the consensus functions stage into at the
    // `overlay.commit` site). `recordFinalizedAccumulator` promotes the finalized hash's bytes into `signedBytesStore`
    // and watermark-prunes. Same `(ordinal, value)` shape as `pendingAccumulatorsRef`.
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    // 3c-A enabler — the SERVED authoritative signed-bytes store (`mpt_signed_snapshot_info/<ordinal>`). At each
    // depth-k1 finalize sink the promoted SIGNED `postBytes` for the finalized hash are written here at the finalized ordinal,
    // so the byte map served to followers reproduces the signed `mptRoot` BY CONSTRUCTION — kept SEPARATE from the
    // producer's own `mpt_snapshot_info` re-fold store (which the producer writes async at finalize and can diverge
    // from the signed bytes under MultiBranch).
    signedBytesStore: MptStateStorage[F],
    // Task #12 slice 2b — the SERVED bounded ring of recent FINALIZED per-ordinal accumulators keyed by
    // ordinal (the ml0-side analogue of `recentFollowProjectionsRef`). Promoted at the depth-k1 finalize sink from
    // `pendingAccumulatorsRef`, trimmed to the last `GlobalChangeSetService.recentAccumulatorsToKeep` (= 256,
    // drop lowest ordinals). `GlobalChangeSetService.changeSetSince` (wired in a later slice) reads it to
    // serve the per-ordinal deltas an ml0 follower adopts-and-verifies. Pure transport optimization: a
    // follower whose `since` fell out of the ring re-fetches via full-GSI adopt, and the signed `mptRoot` at
    // each delta's ordinal rejects a wrong base — so this bound is a memory bound, NOT a consensus parameter.
    recentFinalizedAccumulatorsRef: Ref[F, scala.collection.immutable.SortedMap[
      SnapshotOrdinal,
      StateChangesAccumulator
    ]],
    // Task #12 — bound on `recentFinalizedAccumulatorsRef` (the SERVED changeset ring). The typed
    // `nakamoto.changeset-ring-depth` HOCON value (`SharedConfig.nakamoto.changesetRingDepth`, default 1024),
    // threaded from `GlobalSnapshotConsensus.make` into the `ringInsertTrimmed` trim at the depth-k1 finalize sink.
    // Pure transport memory bound — a follower past this lag falls back to a full-GSI resync, never an
    // incorrect adopt (the signed `mptRoot` at each delta rejects a wrong base). NOT a consensus parameter.
    changesetRingDepth: Int,
    // Chain-quality observable seam (#138). Current telemetry exposes four legacy trigger kinds;
    // target Phase 2 uses only decided-attestation T_weight OR T_depth1, so T_count/T_depth2
    // must not be interpreted as additional finality rails. Populated after construction so the
    // HTTP route /global-snapshots/{ord}/finality-triggers can answer "which triggers
    // qualified ord N?" without taking on the leader-loop's internal state. Caller
    // creates the Ref before HttpApi wiring; we set it once at startup. Pure
    // observability — never feeds back into consensus.
    finalityTriggerViewRef: Ref[F, Option[FinalityTriggerView[F]]],
    // Write-restricted view over the legacy k2 watermark. In the target architecture this is
    // local retention/tower telemetry only, never a settled consensus phase or production/fork floor.
    settledOrdinalTracker: SettledOrdinalTracker[F],
    // §1.2 Slice 5/6: KES parallel-signing for attestations + snapshots. `operationalKeyMaker`
    // signs the attestation hash + snapshot hash at the period derived from the rotation
    // function; `etaRotationSnapshots` is already in this signature above so we don't add it.
    // Receivers (NakamotoSyncDaemon) re-derive the same period from the wire ordinal and
    // verify with the master VK looked up in the registry — warn-only this slice.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    verifiedLocalOperatorKeys: LocalOperatorKeyPairGate.VerifiedLocalOperatorKeys,
    // Public KES evidence is persisted beside snapshot storage so an evicted ancestor remains fully verifiable over ChainSync.
    dataDir: java.nio.file.Path,
    // Slice S3: invoked at the canonical-depth finalize sink with the canonical
    // GlobalIncrementalSnapshot. Iterates `stateChannelSnapshots` to drop `(metagraphAddress,
    // parentHash)` tally entries from the committee-attestation aggregator once their binary
    // has rolled into a finalized global snapshot. Without this the aggregator grows
    // monotonically (one entry per `(metagraph, parent, binary)` seen on the wire). Callers
    // that haven't wired the gate yet can pass `_ => Async[F].unit`.
    onFinalize: io.constellationnetwork.schema.GlobalIncrementalSnapshot => F[Unit],
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
    // Per-shard chain stores (the same instances `shardProducers` write into). After a producer owns
    // the active staircase duty and returns `Some(checkpoint)`, the producing node stores its own
    // checkpoint here (it has the full `Signed[ShardCheckpoint]` + can recompute slot/vrfOutput
    // locally) so the local chain advances toward finality without waiting for its own gossip echo.
    // Empty at `numShards = 1`.
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[
        F
      ]
    ] = Map.empty,
    // Per-shard admission-approved binary buffers (the producer fan-out input). The SAME
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
    // the current depth-k1 finalize sink, exactly paralleling `recordFollowProjection`.
    def recordFinalizedAccumulator(ordinal: SnapshotOrdinal, finalizedHash: Hash): F[Unit] =
      // Atomically (a) pull the staged accumulator out under the finalized (with-cert canonical) hash, AND (b)
      // FINALIZED-WATERMARK prune: drop every OTHER staged entry whose ordinal is at-or-below this just-finalized
      // ordinal — each such entry is either the one we just promoted (removed here) or a fork candidate at a now-
      // finalized height that can never finalize on the canonical chain, so it is dead. This replaces the earlier
      // arbitrary size-`.drop` eviction (which could discard a not-yet-finalized entry under the depth-k retention
      // window + reorg churn) with a correct-by-construction bound. Then atomically insert the promoted accumulator
      // into the served ring via the pure `ringInsertTrimmed` helper. The pure trim helper is what the regression
      // suite drives directly (after the produce-path `rekeyStagedAccumulator`).
      pendingAccumulatorsRef.modify { staged =>
        val promoted = staged.get(finalizedHash).map { case (_, acc) => acc }
        // Watermark prune via the pure helper: keep only entries strictly ABOVE the finalized tip (still in flight).
        // This also removes `finalizedHash` itself (its ordinal == this ordinal, not strictly above) — the drain.
        val pruned = pruneStagedAtOrBelow(staged, ordinal)
        (pruned, promoted)
      }.flatMap {
        case Some(acc) =>
          recentFinalizedAccumulatorsRef.update(ring => ringInsertTrimmed(ring, ordinal, acc, changesetRingDepth))
        case None => Async[F].unit
      } >>
        // 3c-A enabler: mirror the promote+watermark-prune for the SIGNED byte map. Promote the finalized hash's staged
        // signed bytes into the served signed-bytes store at `ordinal`; watermark-prune the rest (keep only entries
        // strictly above the finalized tip — drains `finalizedHash` + dead forks). Only a FINALIZED branch's signed
        // bytes ever reach the store, so a follower's `consensusMptRoot(served) === signed mptRoot` holds by
        // construction. Absent (this node didn't stage this hash) ⇒ skip; the follower's byte route 404s and falls
        // back to the legacy GSI path (best-effort transport, never an incorrect adopt).
        pendingPostBytesRef.modify { staged =>
          val promoted = staged.get(finalizedHash).map { case (_, bytes) => bytes }
          val pruned = pruneStagedPostBytesAtOrBelow(staged, ordinal)
          (pruned, promoted)
        }.flatMap {
          // Write the finalized signed bytes, then prune the store to its contiguous recent window. The store is written at every
          // finalized ordinal and was previously UNBOUNDED — `MptStateStorage.writeState` does not self-prune. `applyCutoff` here is
          // the ONLY prune site for the signed store; it uses the store's `ContiguousOrdinalCutoff` (wired in `GlobalSnapshotConsensus`)
          // so the kept set is the contiguous `{ordinal-depth+1 .. ordinal}` window — guaranteeing the 3c-A serve route's resolved
          // (recent) ordinal stays present while bounding disk growth. Prune is best-effort: a cutoff failure must not abort finalize,
          // so it is swallowed (the next finalized ordinal retries).
          case Some(bytes) =>
            signedBytesStore.writeState(ordinal, bytes) >>
              signedBytesStore
                .applyCutoff(ordinal)
                .handleErrorWith(e => logger.warn(e)(s"[3c-A] signed-bytes store cutoff failed at ordinal=$ordinal (non-fatal)"))
          case None => Async[F].unit
        }

    // Task #12 staging-completeness fix — promote the accumulator of EVERY ordinal that this finalize tick made
    // final, not just the single highest one. Both finalize sinks jump straight to a single `finalizeAtOrdinal`
    // (depth-k's `tip.ordinal - k`, or the legacy cumulative-weight qualifying ordinal) which can advance by MORE than one
    // ordinal per tick whenever the tip grew several ordinals between two loop iterations. Finalizing the highest
    // ordinal implicitly finalizes its canonical ancestors, but the single-ordinal `recordFinalizedAccumulator`
    // call above promotes ONLY that highest ordinal AND its watermark prune then DROPS every staged ancestor
    // (ordinal ≤ finalizeAtOrdinal). Those skipped intermediate ordinals were staged (produce or validate) but
    // never reach the served ring → the ml0 follower hits "ring-gap: no delta for ordinal N" and is forced into a
    // heavy full-GSI resync. Walk the canonical chain from `tipHash` for each ordinal in `(fromExclusive, toInclusive]`
    // (ASCENDING, so the per-ordinal watermark prune never discards a not-yet-processed higher ancestor), resolve its
    // canonical hash, and promote via the same atomic per-ordinal helper. A hash we never staged (or whose chain
    // walk misses — e.g. mid-reorg) is skipped exactly as the single-ordinal path skips an absent hash: the follower
    // full-GSI-adopts that one gap, but the common multi-ordinal-jump case is now COMPLETE. Idempotent on re-finalize
    // (each ordinal drains its staging entry); absence never errors.
    def recordFinalizedRange(
      fromExclusive: Long,
      toInclusive: Long,
      tipHash: Hash
    ): F[List[Hash]] =
      if (toInclusive <= fromExclusive) List.empty[Hash].pure[F]
      else
        (fromExclusive + 1L to toInclusive).toList.traverse { o =>
          chainStore.walkBackTo(tipHash, o).flatMap {
            case Some(hashAtOrdinal) =>
              recordFinalizedAccumulator(SnapshotOrdinal.unsafeApply(o), hashAtOrdinal).as(hashAtOrdinal.some)
            case None => none[Hash].pure[F]
          }
        }.map(_.flatten)
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
                    case NakamotoChainStore.StoreOutcome.BecameSelected(_, _) =>
                      logger.info(
                        s"🌱 Seeded chain store with genesis: ordinal=${hashed.ordinal} hash=${hashed.hash.value.take(16)}"
                      )
                    case NakamotoChainStore.StoreOutcome.Duplicate(_, _) =>
                      logger.debug(s"Chain store already has genesis at ordinal=${hashed.ordinal}")
                    case other =>
                      Async[F].raiseError[Unit](
                        new IllegalStateException(s"Recovery head could not become selected: outcome=$other")
                      )
                  }
                }
              }
            case None =>
              Async[F].sleep(1.second) >> attempt
          }
          attempt
        }

        // Slot duration: the consensus time UNIT (§5.7 — a parameter, not a constant; 1000 ms prod,
        // 500 ms fast-test). Threaded from HOCON `nakamoto.slot-duration-ms` (env override applies at
        // the conf layer via ${?NAKAMOTO_SLOT_DURATION_MS}) — migrated off the sys.env read here per
        // the project HOCON rule. Tick rate and slot derivation share the same value so cluster nodes
        // agree on slot index for the same wall-clock instant.
        // `shardFanOutGate` (design §5.7): at most ONE shard-checkpoint fan-out in flight per node. The per-slot
        // lottery can win consecutive slots (LDD ramp); without the gate two overlapping fan-out fibers would read
        // the same shard tip and mint two same-ord sibling checkpoints from the SAME producer — self-equivocation.
        // tryAcquire-skip (not block): a missed tick is just a lottery draw deferred to the next slot.
        val slotTick: Stream[F, Unit] = Stream.eval(cats.effect.std.Semaphore[F](1)).flatMap { shardFanOutGate =>
          Stream.eval(Ref.of[F, Option[Long]](None)).flatMap { readySinceSlotRef =>
            Stream
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
                        bestTip <- chainStore.bestTip
                        lastChainOrdinal = bestTip.fold(0L)(_.ordinal)
                        currentPeriod = EtaCalculation.rotationPeriod(lastChainOrdinal, etaRotationSnapshots)
                        lookbackPeriod = EtaCalculation.leaderStakeLookbackPeriod(lastChainOrdinal, etaRotationSnapshots)
                        myStake <- stakeRegistry.relativeStakeAt(selfId, lookbackPeriod)

                        // Chain-derived eta: deterministic from stored chain, no in-memory accumulator.
                        // Periods 0 and 1: genesis-derivable bootstrapEta (distinct per period, no VRF
                        // dependency). Period N>=2: derived from VRF outputs in period N-1. All nodes
                        // seeing the same chain derive the same eta — no divergence.
                        //
                        // Rotation period is keyed on **ordinal**, not slot — slots are LDD-paced and lumpy;
                        // ordinals are 1:1 with snapshots and give a stable R that satisfies the R ≥ 3·k₁
                        // bound. See `docs/nakamoto/attestation-and-finality.md` §1.
                        genesisEta <- epochStateRef.get.map(_.genesisEta)
                        eta <-
                          if (currentPeriod <= 1) {
                            // Cardano/Praos bootstrap: periods 0 and 1 are genesis-derivable (distinct, no
                            // VRF-output dependency). First VRF-folded eta is period 2. Removes the period
                            // 0→1 boundary fork (#259 folded period 0's unsettled outputs here).
                            Async[F].pure(EtaCalculation.bootstrapEta(genesisEta, currentPeriod).some)
                          } else {
                            bestTip match {
                              case None => Async[F].pure(none[Array[Byte]])
                              case Some(tip) =>
                                chainStore
                                  .vrfOutputRangeForPeriodFrom(currentPeriod - 1, etaRotationSnapshots, tip.hash)
                                  .flatMap {
                                    case NakamotoChainStore.VrfOutputRange.Complete(chainOutputs) if chainOutputs.nonEmpty =>
                                      Async[F].pure(
                                        EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2)).some
                                      )
                                    case NakamotoChainStore.VrfOutputRange.Complete(_) =>
                                      logger
                                        .warn(
                                          s"Slot $currentSlot: refusing leader trial because eta source period=${currentPeriod - 1L} " +
                                            s"is complete but contains no VRF outputs"
                                        )
                                        .as(none[Array[Byte]])
                                    case incomplete: NakamotoChainStore.VrfOutputRange.Incomplete =>
                                      logger
                                        .warn(
                                          s"Slot $currentSlot: refusing leader trial because eta source period=${currentPeriod - 1L} " +
                                            s"is incomplete at hash=${incomplete.missingHash.value.take(12)} " +
                                            s"ordinal=${incomplete.expectedOrdinal.fold("unknown")(_.toString)}"
                                        )
                                        .as(none[Array[Byte]])
                                  }
                            }
                          }

                        localSigningKey <- LocalOperatorKeyPairGate.registeredSigningKey(
                          operationalKeyMaker,
                          selfId,
                          keyPair,
                          operatorKeyRegistry,
                          currentPeriod
                        )

                        _ <- (eta, localSigningKey) match {
                          case (None, _) =>
                            Metrics[F].incrementCounter("dag_nakamoto_slots_eta_unavailable_total")
                          case (Some(_), Left(error)) =>
                            Metrics[F].incrementCounter("dag_nakamoto_slots_invalid_local_kes_total") >>
                              Async[F].whenA(currentSlot % 30 == 0) {
                                logger.warn(
                                  s"Slot $currentSlot: refusing leader trial because the local atomic KES+VRF pair is unavailable: ${error.getMessage}"
                                )
                              }
                          case (Some(verifiedEta), Right(signingKey)) =>
                            // Raw VRF-trial counter (incremented BEFORE win/loss check) — pairs with
                            // `dag_nakamoto_slots_won` to surface silently-degraded validators.
                            Metrics[F].incrementCounter("dag_nakamoto_slots_trialed_total") >>
                              eligibilityChecker
                                .checkEligibility(
                                  vrfSK = vrfSeed,
                                  slot = slotRefined,
                                  slotGap = slotGap,
                                  eta = verifiedEta,
                                  relativeStake = myStake,
                                  config = lddConfig
                                )
                                .flatMap {
                                  case Some((proof, vrfOutput)) =>
                                    snapshotSemaphore.permit.use { _ =>
                                      onSlotWon(
                                        stateRef,
                                        consensusFns,
                                        snapshotStorage,
                                        chainStore,
                                        eventMempool,
                                        sidecarClient,
                                        stakeRegistry,
                                        lastGlobalSnapshotStorage,
                                        lastNGlobalSnapshotStorage,
                                        keyPair,
                                        selfId,
                                        vrfPK,
                                        proof,
                                        vrfOutput,
                                        verifiedEta,
                                        currentSlot,
                                        slotGap,
                                        slotRefined,
                                        lddConfig,
                                        etaRotationSnapshots,
                                        lastKnownSlotRef,
                                        productionGate,
                                        productionTimestamps,
                                        nakamotoFinalizedOrdinalRef,
                                        mptStore,
                                        mptOverlay,
                                        operationalKeyMaker,
                                        signingKey,
                                        verifiedLocalOperatorKeys,
                                        dataDir,
                                        pendingAccumulatorsRef,
                                        pendingPostBytesRef,
                                        logger
                                      )
                                    }

                                  case None =>
                                    Metrics[F].updateGauge("dag_nakamoto_slot", currentSlot) >>
                                      Async[F].whenA(currentSlot % 30 == 0) {
                                        logger.debug(s"Slot $currentSlot: not eligible (gap=$slotGap, stake=$myStake)")
                                      }
                                }
                        }

                        // ─── §5.7 per-SLOT shard-checkpoint fan-out (owner-corrected 2026-06-11) ─────────────
                        // Execution-shard staircase duty advances every slot on the shared wall-clock grid, independently of
                        // GL0 snapshot production. This replaces the anchor-driven triggers, which advanced duty once per GL0 snapshot (~6.5 slots
                        // observed mean) — the run-10 Gap-A cadence inversion: shards ticked ~6.5× slower than the
                        // layer they feed. Anchoring data: `producedOrd` = current canonical tip ord (the checkpoint
                        // rides into this or any later GL0 ord); the duty clock is `slotRefined`, carried
                        // on the envelope's `slot` field. Supervised + gated: heavy on a WIN only (derivePerMgState);
                        // the skip paths (not-on-duty / awaiting-embed / empty) are cheap deterministic checks.
                        // Boot grace (run-17): record the first Ready slot (this branch only runs when Ready);
                        // take NO shard duty until intake has had `shardBootGraceSlots` to drain — a late-booting
                        // rank otherwise takes genesis duty blind and seeds a rival lineage (second ord-1 at
                        // exactly slot 61 = rank-1's first genesis-window slot).
                        readySince <- readySinceSlotRef.modify {
                          case None        => (Some(currentSlot), currentSlot)
                          case s @ Some(v) => (s, v)
                        }
                        bootGraceElapsed = (currentSlot - readySince) >= shardBootGraceSlots
                        // PHASE-2 ANCHOR (2026-06-17): the shard checkpoint MUST anchor to the FINALIZED gl0 ordinal,
                        // NOT `chainStore.bestTipOrdinal` (phase-1, reorgable). `gl0AnchorOrdinal` is the re-exec
                        // derivation CONTEXT and part of each per-MG root (ShardCheckpointWiring §6); a best-tip anchor
                        // that REORGS before it finalizes strands the derived metagraph state on a dead branch — the
                        // `parentHash` mismatch + "parent NOT in chain store" + orphan-buffer wedge. Followers (ml0)
                        // already consume only finalized gl0 snapshots (40dc4c2b2's finality gate), so anchoring here to
                        // finalized RESTORES the phase-2-only invariant (design §7.2) and aligns the anchor with the
                        // binaries it derives over. The §7.2 acceptance window is upper-bounded only (checkpoint rides
                        // into anchor-or-later), so an older finalized anchor is never rejected as stale. Regression
                        // introduced by 86ee3dbef (per-slot best-tip fan-out). Execution-shard membership is derived from
                        // this same finalized anchor: using the reorgable best-tip period here can rotate two honest nodes
                        // at different times and make them compute different committees for the same checkpoint window.
                        nakamotoFinalizedAnchor <- nakamotoFinalizedOrdinalRef.get
                        _ <- Async[F].whenA(shardProducers.nonEmpty && shardAssignment.isDefined && bootGraceElapsed) {
                          val anchorOrd = nakamotoFinalizedAnchor
                          val shardEpoch = EtaCalculation.executionShardEpoch(anchorOrd, etaRotationSnapshots)
                          supervisor
                            .supervise(
                              shardFanOutGate.tryAcquire.flatMap {
                                case false => Async[F].unit // previous fan-out still running — skip this slot's draw
                                case true =>
                                  cats.effect
                                    .MonadCancel[F]
                                    .guarantee(
                                      HasherSelector[F].withCurrent { implicit hasher =>
                                        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointFanOut.run[F](
                                          shardBinaryBuffers = shardBinaryBuffers,
                                          producedOrd = anchorOrd,
                                          epoch = shardEpoch,
                                          currentSlot = slotRefined,
                                          shardProducers = shardProducers,
                                          shardChainStores = shardChainStores,
                                          selfPeerId = selfId,
                                          committeeMembership = shardCommitteeMembership,
                                          logger = logger
                                        )
                                      }.handleErrorWith(e => logger.warn(e)(s"🧩 Shard fan-out failed at slot=$currentSlot")),
                                      shardFanOutGate.release
                                    )
                              }
                            )
                            .void
                        }
                      } yield ()
                } yield ()
              }
          }
        }

        // Dual finality: attestation weight (fast) OR confirmation depth k (safety fallback)
        //
        //   - Fast path: tip attestation weight ≥ TipTracker.FinalityThreshold (default 2/3,
        //     with optimistic dynamic-quorum sizing for partial cluster availability).
        //   - Fallback: a snapshot is depth-finalized once at least k snapshots sit above it
        //     on the canonical chain — i.e. `tip.ordinal - snapshotOrdinal >= k`. This is the
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
        // Sourced from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` (HOCON per-env
        // `nakamoto.confirmation-depth-k`, dev overridable via `${?NAKAMOTO_CONFIRMATION_DEPTH}`) — threaded in as the `confirmationDepthK` run
        // parameter rather than read from sys.env here (project rule: HOCON over scattered env reads).
        // Default 255 chosen to approximate Cardano-equivalent 10⁻¹² common-prefix violation against a 1/3
        // adversary under the LDD snowplow (ψ=0, γ=15, fA=0.5, fB=0.05). The k=31 sim result is 0.91%
        // per-attempt; extrapolating the ~1-log-per-24-blocks slope puts 10⁻¹² at k≈271, so 255 is a
        // deliberately-conservative operating point pending expanded-range sim verification
        // (research-nipopos-2026, sim/adv-7block-private, adv_depth_optimization.py).
        //
        // The target optimistic hot path is an exact-hash Avalanche/Snowball decision followed by
        // decided-attestation T_weight. It is not a BFT vote/lock/QC. Canonical k1 depth is the
        // fallback when the optimistic rail stalls; either rail makes reversible Phase-2 state.
        val ConfirmationDepthK: Long = confirmationDepthK

        // `archivalDepthK` is a legacy variable name for the recommended local k2 = 100*k1
        // retention/proof/automatic-rollback capacity. There is no protocol Phase 3 and neither
        // k1 nor k2 is an absolute no-reorg floor: Phase 2 remains density-reorgable. If objective
        // comparison reaches unavailable history, production/mutation stops in RecoveryRequired
        // until exact authenticated reconstruction completes. Live TDepth2/prune/floor behavior
        // below is transitional implementation debt, not the target finality contract.

        // FinalityTrigger construction. Target P2 uses T_weight OR T_depth1. Live T_count and
        // T_depth2 instances are retained temporarily for legacy observability and must not
        // authorize another finality rail or phase. Each trigger wraps a `Ref[F, SnapshotOrdinal]`
        // that tracks its monotone latest-qualifying-ordinal across all ticks. The downstream
        // `FinalityTrigger.triggersFor(triggers, ord)` lookup answers "which triggers
        // qualified ordinal N?" for free — used by the future chain-quality observable
        // (#138) and post-mortem finality analysis without re-walking the chain.
        //
        // Transitional T_weight reads the sibling [[SnowballAccumulator]] constructed inside
        // `TipTracker.make`. That implementation is an arrival-order-sensitive sticky margin,
        // not the intended K/alpha/beta cascade. The
        // `TipTracker.FinalityThreshold` value (2/3, env `NAKAMOTO_ATTESTATION_THRESHOLD`) is
        // retained for the telemetry-only T_count rule and diagnostics. The legacy cumulative
        // sum is removed and cannot act as fallback evidence.
        //
        // `SnowballAccumulator.K` and `.Alpha` are currently unused. Beta is the only value this
        // implementation consumes. Simulations of a real K/alpha cascade do not validate this
        // different executable rule.
        //
        // The trigger objects are read-only calculators. State-changing side effects remain in
        // the monitor below; its optimistic branch currently bypasses T_weight and reads the
        // legacy cumulative-weight query directly.
        Stream.eval(TWeightTrigger.make[F](tipTracker, TipTracker.FinalityThreshold)).flatMap { tWeight =>
          Stream.eval(TCountTrigger.make[F](tipTracker, stakeRegistry, TipTracker.FinalityThreshold)).flatMap { tCount =>
            Stream.eval(TDepth1Trigger.make[F](ConfirmationDepthK)).flatMap { tDepth1 =>
              Stream.eval(TDepth2Trigger.make[F](archivalDepthK)).flatMap { tDepth2 =>
                // Transitional telemetry list. T_count is still sampled by live code, but the
                // locked P2 predicate is decided-attestation T_weight OR T_depth1. T_depth2 is
                // retention telemetry and cannot advance a phase.
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
                    // Legacy monotone k2 watermark used by current retention/tower telemetry. It is
                    // deliberately distinct from the P2 ordinal ref, but the target removes its
                    // Phase-3/settled meaning entirely. The trivial Stream keeps current wiring intact.
                    Stream.eval(Async[F].unit).flatMap { _ =>
                      // Keep the historical stream shape while the durable finality coordinator is
                      // introduced. Overlay pruning now occurs only from an exact successful chain-store
                      // finalization receipt; sticky trigger telemetry cannot prune canonical state.
                      Stream.eval(Async[F].unit).flatMap { _ =>
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
                              selectedTip <- chainStore.selectedTip
                              bestTip = selectedTip.map(_.snapshot)

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
                              // later telemetry lookups. Only T_depth1 has a state-changing sink.
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

                              // Depth-based finality: a snapshot is final once at least k descendants sit above it on
                              // the canonical chain — `tip.ordinal - snapshotOrdinal >= k`.
                              //
                              // `tDepth1.latestQualifyingOrdinal` remains telemetry only. Its Ref is
                              // monotone and therefore cannot represent a denser-but-shorter replacement.
                              depthQualifying <- tDepth1.latestQualifyingOrdinal
                              finalizedOutbox <- selectedTip match {
                                case Some(_) =>
                                  // The outer semaphore serializes the multi-store Phase-2 effects with
                                  // producer/receiver canonical effects. Re-read the selected tip and
                                  // watermark only after acquiring it; no sticky ordinal is an input.
                                  snapshotSemaphore.permit.use { _ =>
                                    (chainStore.selectedTip, chainStore.lastFinalizedOrdinal).tupled.flatMap {
                                      case (Some(expected), currentLastFinalizedOrdinal) =>
                                        val finalizeAtOrdinal = math.max(0L, expected.snapshot.ordinal - ConfirmationDepthK)

                                        if (finalizeAtOrdinal <= currentLastFinalizedOrdinal)
                                          Async[F].pure(none[NakamotoChainStore.StoredSnapshot])
                                        else
                                          chainStore.finalizeSelectedAt(expected, finalizeAtOrdinal).flatMap {
                                            case NakamotoChainStore.FinalizeOutcome.Finalized(
                                                  canonicalSnapshot,
                                                  selectedAtCommit,
                                                  previousFinalizedOrdinal,
                                                  _
                                                ) =>
                                              val canonicalHash = canonicalSnapshot.hash
                                              val canonicalOrdinal = SnapshotOrdinal.unsafeApply(finalizeAtOrdinal)
                                              val finalizeAtSlot = Slot(NonNegLong.unsafeFrom(canonicalSnapshot.slot))
                                              val tip = selectedAtCommit.snapshot

                                              // Every downstream canonical effect is gated on the successful exact
                                              // chain-store CAS. They are not yet crash-atomic; the durable finality
                                              // journal remains an activation blocker. Publish the Phase-2 watermark
                                              // only after local state/serve data is ready, then invoke consumers that
                                              // are required to observe that watermark.
                                              for {
                                                _ <- mptOverlay.finalizeBranch(BranchId(canonicalHash), canonicalOrdinal).void
                                                _ <- mptOverlay.pruneBelow(canonicalOrdinal)
                                                _ <- Metrics[F].updateGauge(
                                                  "dag_nakamoto_overlay_prune_ordinal",
                                                  finalizeAtOrdinal
                                                )
                                                finalizedHashes <- recordFinalizedRange(
                                                  previousFinalizedOrdinal,
                                                  finalizeAtOrdinal,
                                                  tip.hash
                                                )
                                                _ <- recordFollowProjection(canonicalOrdinal, canonicalSnapshot.context)
                                                _ <- latestFinalizedSliceSourceRef.update {
                                                  case Some((o, _)) if o.value.value >= finalizeAtOrdinal =>
                                                    Some((o, canonicalSnapshot.context))
                                                  case _ => Some((canonicalOrdinal, canonicalSnapshot.context))
                                                }
                                                _ <- tipTracker.markFinalized(canonicalHash, finalizeAtSlot)
                                                _ <- tipTracker.pruneBelow(finalizeAtSlot)
                                                _ <- nakamotoFinalizedOrdinalRef
                                                  .update(prev => cats.Order[SnapshotOrdinal].max(prev, canonicalOrdinal))
                                                // Metagraph orphan release rechecks the public Phase-2 gate. Running
                                                // this before the update above can re-buffer a valid child after the
                                                // sole finalize-driven drain.
                                                // The chain store may already have evicted intermediate
                                                // ancestors. Resolve callbacks from canonical snapshot storage
                                                // after publication while retaining only lightweight hashes.
                                                _ <- finalizedHashes.traverse_ { finalizedHash =>
                                                  snapshotStorage
                                                    .get(finalizedHash)
                                                    .flatMap(_.traverse_(stored => onFinalize(stored.value)))
                                                }
                                                _ <- logger.info(
                                                  s"DEPTH-FINALIZED ordinal=$finalizeAtOrdinal slot=${canonicalSnapshot.slot} " +
                                                    s"(tip ord=${tip.ordinal} slot=${tip.slot}, k=$ConfirmationDepthK)"
                                                )
                                                _ <- Metrics[F].incrementCounter("dag_nakamoto_finalized")
                                                _ <- Metrics[F].updateGauge("dag_nakamoto_finalized_ordinal", finalizeAtOrdinal)
                                                _ <- productionTimestamps.getAndUpdate(_ - finalizeAtOrdinal).flatMap { ts =>
                                                  ts.get(finalizeAtOrdinal).traverse_ { prodMs =>
                                                    val latencyMs = System.currentTimeMillis() - prodMs
                                                    Metrics[F].recordDistribution("dag_nakamoto_finality_latency_ms", latencyMs.toInt)
                                                  }
                                                }
                                                _ <- FinalityTrigger
                                                  .triggersFor(phase12Triggers, canonicalOrdinal)
                                                  .flatMap(qs => emitChainQuality[F](qs, finalizedOrdinal = Some(finalizeAtOrdinal)))
                                              } yield canonicalSnapshot.some

                                            case NakamotoChainStore.FinalizeOutcome.StaleSelection(current) =>
                                              logger
                                                .debug(
                                                  s"DEPTH-FINALIZE deferred stale selection at ordinal=$finalizeAtOrdinal: " +
                                                    s"expected=${expected.snapshot.hash.value.take(12)}, " +
                                                    s"current=${current.fold("none")(_.snapshot.hash.value.take(12))}"
                                                )
                                                .as(none[NakamotoChainStore.StoredSnapshot])

                                            case NakamotoChainStore.FinalizeOutcome.AlreadyFinalized(currentOrdinal, currentHash) =>
                                              logger
                                                .debug(
                                                  s"DEPTH-FINALIZE already applied through ordinal=$currentOrdinal " +
                                                    s"hash=${currentHash.fold("none")(_.value.take(12))}"
                                                )
                                                .as(none[NakamotoChainStore.StoredSnapshot])

                                            case NakamotoChainStore.FinalizeOutcome.TargetUnavailable(targetOrdinal) =>
                                              Metrics[F].incrementCounter("dag_nakamoto_finality_target_unavailable") >>
                                                logger
                                                  .warn(
                                                    s"DEPTH-FINALIZE recovery required: selected exact ancestry does not contain " +
                                                      s"ordinal=$targetOrdinal from tip=${expected.snapshot.hash.value.take(12)}"
                                                  )
                                                  .as(none[NakamotoChainStore.StoredSnapshot])
                                          }

                                      case (None, _) => Async[F].pure(none[NakamotoChainStore.StoredSnapshot])
                                    }
                                  }
                                case None => Async[F].pure(none[NakamotoChainStore.StoredSnapshot])
                              }
                              // Sidecar acknowledgement is transport cleanup, not a validity or finality
                              // prerequisite. Keep the unbounded RPC outside the canonical semaphore; its
                              // durable outbox TTL remains the retry/backstop until the effect journal lands.
                              _ <- finalizedOutbox.traverse_ { canonicalSnapshot =>
                                supervisor
                                  .supervise(
                                    confirmSnapshotOutbox(canonicalSnapshot, sidecarClient, logger).handleErrorWith { error =>
                                      logger.warn(error)(
                                        s"Finalized outbox acknowledgement failed at ordinal=${canonicalSnapshot.ordinal}; " +
                                          s"sidecar TTL cleanup remains active"
                                      )
                                    }
                                  )
                                  .void
                              }

                              // Optimistic Phase 2 is deliberately dark here. The removed predecessor
                              // finalized from one receiver-local cumulative 2/3 weight read. That was
                              // neither the ratified sampled K/alpha/beta transcript nor portable evidence.
                              // Remote attestations remain observable below, but only the depth block above
                              // may call chainStore.finalizeSelectedAt until the replay-gated exact-hash rail lands.
                              _ <- Async[F].whenA(allAtts.nonEmpty) {
                                val ordinals = allAtts.values.map(_.tipOrdinal).toList.sorted
                                logger.debug(
                                  s"Attestation telemetry (optimistic rail dark): ${allAtts.size} attesters, " +
                                    s"ordinals=[${ordinals.mkString(",")}], lastDepthFinalized=$lastFinalizedOrdinal"
                                )
                              }

                              // T_count observability: log when T_count strictly outruns both T_weight AND
                              // T_depth1 (i.e. it would have driven the finalize on its own, if the max-of
                              // semantics ever consumed it). Today's sole live sink is T_depth1; this log
                              // line is purely diagnostic so we can see when count-based telemetry
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
                                    s"T_COUNT-TELEMETRY ordinal=${countQualifying.value.value} count=$k/$validatorCount " +
                                      s"(V=$validatorCount, T_weight=${weightQualifying.value.value}, T_depth1=${depthQualifying.value.value})"
                                  )
                                }
                              }

                              // Track-3 S2: RAM undo-journal bound telemetry. The disk `signedBytesStore` now holds the deep
                              // signed bytes to k₂ (`ContiguousOrdinalCutoff(keepDepthBehindFinalized)`); the in-memory
                              // `undoJournalRef` MUST stay bounded to the operational-k₁ fast-path window (the exact-finality
                              // prune above). Emit its size EVERY tick as a gauge, and fire an over-bound counter if it exceeds the k₁
                              // window — the sentinel that the RAM journal is NOT silently regressing toward the k₂ disk depth
                              // (the pre-Fix-A heap leak). Read-only snapshot: no mutex, no consensus effect.
                              overlayJournalSizes <- mptOverlay.journalSizes
                              _ <- Metrics[F].updateGauge(
                                "dag_nakamoto_overlay_undo_journal_size",
                                overlayJournalSizes.undoJournal.toLong
                              )
                              _ <-
                                if (overlayJournalSizes.overBound(ConfirmationDepthK))
                                  Metrics[F].incrementCounter("dag_nakamoto_overlay_undo_journal_overbound_total") >>
                                    logger.warn(
                                      s"[MptOverlay] undo-journal OVER-BOUND: size=${overlayJournalSizes.undoJournal} > k₁=$ConfirmationDepthK " +
                                        s"(finalizedMarkers=${overlayJournalSizes.finalizedMarkers}, pendingBranches=${overlayJournalSizes.pendingBranches}). " +
                                        s"The RAM fast-path window is exceeded — Fix A prune should bound this to k₁; investigate a prune stall " +
                                        s"before RAM drifts toward the k₂ disk depth."
                                    )
                                else Async[F].unit

                              // Legacy T_depth2 local-retention observability. There is no Phase 3.
                              //
                              // When `tDepth2.latestQualifyingOrdinal` advances the current local watermark,
                              // live code emits retention telemetry only. It cannot prune the overlay, upgrade
                              // snapshot validity, or freeze fork choice. NIPoPoW tower catch-up is deliberately
                              // not driven here until bounded exact-hash recovery is implemented.
                              archivalQualifying <- tDepth2.latestQualifyingOrdinal
                              _ <- settledOrdinalTracker.settledOrdinal.flatMap { prev =>
                                if (archivalQualifying.value.value > prev.value.value)
                                  bestTip match {
                                    case Some(tip) =>
                                      settledOrdinalTracker.markSettled(archivalQualifying) >>
                                        logger.info(
                                          s"ARCHIVAL-FINALIZED ordinal=${archivalQualifying.value.value} " +
                                            s"(tip ord=${tip.ordinal}, k₂=$archivalDepthK)"
                                        ) >>
                                        Metrics[F].incrementCounter("dag_nakamoto_archival_finalized") >>
                                        Metrics[F].updateGauge("dag_nakamoto_archival_ordinal", archivalQualifying.value.value)
                                    case None =>
                                      // Trigger advanced without a tip — shouldn't happen because the
                                      // trigger's evaluator returns MinValue when bestTip is None.
                                      // Still, advance the marker idempotently so we don't re-log on the
                                      // next tick.
                                      settledOrdinalTracker.markSettled(archivalQualifying)
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
    stakeRegistry: StakeRegistry[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    keyPair: KeyPair,
    selfId: PeerId,
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
    productionGate: ProductionGate[F],
    productionTimestamps: Ref[F, Map[Long, Long]],
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: MptOverlay[F, GlobalStateKey],
    // §1.2 Slice 6: KES parallel-signing for the published snapshot's `kes_signature` field.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    slotSigningKey: LocalOperatorKeyPairGate.RegisteredSigningKey,
    verifiedLocalOperatorKeys: LocalOperatorKeyPairGate.VerifiedLocalOperatorKeys,
    dataDir: java.nio.file.Path,
    // Task #12 slice 2b — the gl0 changeset STAGING map (threaded from `run`'s same-named param). The
    // produce path below rekeys the staged accumulator raw->with-cert alongside the overlay rekey so the
    // finalize-sink promotion (`recordFinalizedAccumulator`, in `run`) can find it under the canonical hash.
    pendingAccumulatorsRef: Ref[F, Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)]],
    // 3c-A enabler — signed-bytes staging, rekeyed raw->with-cert alongside `pendingAccumulatorsRef` on the produce path.
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    HasherSelector[F].withCurrent { implicit hasher =>
      for {
        activePool <- stakeRegistry.activeValidators
        // Build SlotCertificate — use the chain-derived eta that was used for VRF evaluation
        proofHex = Hex(proof.map("%02x".format(_)).mkString)
        vrfOutputHex = Hex(vrfOutput.map("%02x".format(_)).mkString)
        pkHex = Hex(vrfPK.map("%02x".format(_)).mkString)
        etaHash = Hash(currentEta.map("%02x".format(_)).mkString)

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
              // Transitional ordinal production guard: never produce at-or-below the local P2
              // watermark. If we won a slot but our local head is stale,
              // producing would emit a doomed snapshot — refused downstream by
              // NakamotoChainStore's finality guard, and in a past storage-layer regression
              // could have silently overwritten finalized content (the gl0-2-divergent-517
              // class of bug). Skip and let the sync daemon catch us up via gossip/chainsync;
              // the next slot win after catch-up produces the correct (above-finalized) ordinal.
              //
              // Target production is keyed to the exact canonical P2 hash, not a monotone ordinal
              // floor. A density reorg must replace that anchor and force rollback/re-follow before
              // production resumes. This ordinal-only guard is transitional and cannot express that.
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
                      // Producer-side artifact-validity gate. The receiver runs this same pure validator, but
                      // producing an invalid artifact must be impossible too: validate the exact raw artifact's
                      // embedded checkpoint slots against the exact retained parent and the certificate that will
                      // be embedded in this artifact. No signature or store effect is reachable on Left.
                      slotLineage = NakamotoSnapshotValidator.validateSlotLineage(
                        certWithSubchain,
                        lastSigned.value.slotCertificate,
                        rawArtifact.shardCheckpoints.iterator.map { case (shardId, checkpoint) => shardId -> checkpoint.slot }.toList
                      )
                      attemptOpt <-
                        if (gateOpenPreSign)
                          afterProducerSlotLineageValidation(slotLineage)(reason =>
                            logger.warn(
                              s"Refusing locally constructed snapshot at slot=$currentSlot before signing: invalid slot lineage: $reason"
                            ) >> Metrics[F].incrementCounter("dag_nakamoto_production_abandoned_total")
                          ) {
                            for {
                              signed <- Signed.forAsyncHasher[F, GlobalIncrementalSnapshot](artifact, keyPair)
                              snapshotHashedForStorage <- signed.toHashed[F]
                              producedOrdinal = lastKey.value.value + 1
                              kesGlobalPeriod = EtaCalculation.globalSnapshotArtifactPeriod(producedOrdinal, etaRotationSnapshots).value
                              snapshotHashBytes = snapshotHashedForStorage.hash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                              kesSnapAttempt <-
                                if (
                                  io.constellationnetwork.node.shared.domain.nakamoto.ActiveOperatorConsensusKeys
                                    .treeStep(
                                      slotSigningKey.operatorKeys,
                                      io.constellationnetwork.schema.nakamoto.EtaPeriod(kesGlobalPeriod)
                                    )
                                    .contains(slotSigningKey.treeStep)
                                )
                                  operationalKeyMaker
                                    .signAt(slotSigningKey.treeStep, snapshotHashBytes)
                                    .map(
                                      _.leftMap(_.message): Either[String, io.constellationnetwork.security.kes.SignatureKesProduct]
                                    )
                                else
                                  Async[F].pure[
                                    Either[String, io.constellationnetwork.security.kes.SignatureKesProduct]
                                  ](Left("slot signing capability does not match produced snapshot period"))
                              kesSnapSigBytes <- kesSnapAttempt match {
                                case Right(kSig) =>
                                  val bytes = io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(kSig)
                                  Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_signed_total") >>
                                    logger
                                      .info(
                                        s"KES-SNAP ord=$producedOrdinal globalPeriod=$kesGlobalPeriod offset=${verifiedLocalOperatorKeys.kesPeriodOffset} " +
                                          s"sub-sig=${bytes.take(8).map("%02x".format(_)).mkString} (${bytes.length}B)"
                                      )
                                      .as(bytes)
                                case Left(err) =>
                                  Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_sign_failed_total") >>
                                    logger
                                      .warn(
                                        s"KES-SNAP sign failed for ord=$producedOrdinal globalPeriod=$kesGlobalPeriod: $err; refusing chain-store write"
                                      )
                                      .as(Array.empty[Byte])
                              }
                              kesReady = kesSnapAttempt.isRight
                              parentHashValue = lastHashed.hash
                              canonicalBranch = BranchId(snapshotHashedForStorage.hash)
                              rawBranch = BranchId(rawArtifactHash)
                              discardPreparedBranch =
                                mptOverlay.discardBranch(canonicalBranch) >>
                                  mptOverlay.discardBranch(rawBranch) >>
                                  pendingAccumulatorsRef.update(_ - snapshotHashedForStorage.hash - rawArtifactHash) >>
                                  pendingPostBytesRef.update(_ - snapshotHashedForStorage.hash - rawArtifactHash)
                              prepareCanonicalBranch =
                                mptOverlay.rekey(rawBranch, canonicalBranch) >>
                                  pendingAccumulatorsRef.update(
                                    rekeyStagedAccumulator(_, rawArtifactHash, snapshotHashedForStorage.hash)
                                  ) >>
                                  pendingPostBytesRef.update(
                                    rekeyStagedPostBytes(_, rawArtifactHash, snapshotHashedForStorage.hash)
                                  )
                              _ <- Async[F].whenA(kesReady) {
                                SnapshotKesStorage.put[F](dataDir, snapshotHashedForStorage.hash, kesSnapSigBytes)
                              }
                              storeOutcome <-
                                if (kesReady)
                                  // Prepare the canonical-hash branch before selection. The store and its production fence are then one
                                  // uncancelable handoff; explicit nonselection or a preselection store failure discards the inert staging.
                                  Async[F].uncancelable { _ =>
                                    prepareCanonicalBranch.attempt.flatMap {
                                      case Left(error) =>
                                        discardPreparedBranch >>
                                          Async[F].raiseError[NakamotoChainStore.StoreOutcome](error)
                                      case Right(()) =>
                                        chainStore
                                          .store(
                                            signed,
                                            context,
                                            lastKey.value.value + 1,
                                            currentSlot,
                                            parentHashValue,
                                            vrfOutput
                                          )
                                          .attempt
                                          .flatMap {
                                            case Left(error) =>
                                              discardPreparedBranch >>
                                                Async[F].raiseError[NakamotoChainStore.StoreOutcome](error)
                                            case Right(outcome @ NakamotoChainStore.StoreOutcome.BecameSelected(_, _)) =>
                                              // If pausing raises after updating the gate, retain the selected branch and fail-stop. It is
                                              // no longer safe to discard state after the chain store has published this exact selection.
                                              productionGate
                                                .pause(ProductionGate.ReorgInProgress)
                                                .as(outcome: NakamotoChainStore.StoreOutcome)
                                            case Right(other) =>
                                              discardPreparedBranch.as(other: NakamotoChainStore.StoreOutcome)
                                          }
                                    }
                                  }.map(_.some)
                                else none[NakamotoChainStore.StoreOutcome].pure[F]
                            } yield (signed, snapshotHashedForStorage, parentHashValue, kesSnapSigBytes, storeOutcome)
                          }
                        else
                          none[
                            (
                              Signed[GlobalIncrementalSnapshot],
                              Hashed[GlobalIncrementalSnapshot],
                              Hash,
                              Array[Byte],
                              Option[NakamotoChainStore.StoreOutcome]
                            )
                          ].pure[F]
                      storedOutcomeOpt = attemptOpt.collect {
                        case (
                              _,
                              snapshotHashedForStorage,
                              _,
                              kesSnapSigBytes,
                              Some(NakamotoChainStore.StoreOutcome.BecameSelected(selected, _))
                            ) =>
                          (selected, returnedEvents, snapshotHashedForStorage, kesSnapSigBytes)
                      }
                      // The successful branch was rekeyed to the exact signed hash before `chainStore.store`, so selection never points at
                      // raw-hash staging. Abandoned paths which never reached that preparation still need their raw staging removed.
                      _ <- storedOutcomeOpt match {
                        case Some(_) => Async[F].unit
                        case None    =>
                          // Gate-closed, invalid-lineage, or KES-unavailable abandonment never prepared a canonical branch.
                          mptOverlay.discardBranch(BranchId(rawArtifactHash)) >>
                            pendingAccumulatorsRef.update(_ - rawArtifactHash) >>
                            pendingPostBytesRef.update(_ - rawArtifactHash)
                      }
                      action: MptTxAction = if (storedOutcomeOpt.nonEmpty) MptTxAction.Commit else MptTxAction.Rollback
                    } yield (storedOutcomeOpt, action)
                  }
                  _ <- txOutcome.traverse_ {
                    case (selected, returnedEvents, _, kesSnapSigBytes) =>
                      val includedHashes = hashedEvents.collect {
                        case (h, hashed) if !returnedEvents.contains(hashed.signed.value) => h
                      }.toSet

                      chainStore
                        .runCanonicalEffectsIfCurrent(selected) { canonical =>
                          for {
                            canonicalHashed <- canonical.signedSnapshot.toHashed[F]
                            // These are revision-unaware local projections. Keep them under the exact-selection lock so a competing
                            // branch cannot interleave and leave storage or the mempool pointing at the stale producer result.
                            _ <- snapshotStorage.setHeadForRecovery(canonical.signedSnapshot, canonical.context)
                            _ <- lastGlobalSnapshotStorage.setForRecovery(canonicalHashed, canonical.context)
                            _ <- lastNGlobalSnapshotStorage.setForRecovery(canonicalHashed, canonical.context)
                            _ <- lastKnownSlotRef.set(Some(canonical.slot))
                            _ <- eventMempool.clearIncluded(includedHashes)
                            _ <- productionGate.resume(ProductionGate.ReorgInProgress)
                          } yield (canonical, canonicalHashed)
                        }
                        .flatMap {
                          case NakamotoChainStore.CanonicalEffectsOutcome.StaleSelection(current) =>
                            logger.info(
                              s"Skipped stale producer canonical effects for ordinal=${selected.snapshot.ordinal}; " +
                                s"current=${current.fold("none")(tip => s"${tip.snapshot.ordinal}:${tip.snapshot.hash.value.take(12)}")}"
                            ) >> Metrics[F].incrementCounter("dag_nakamoto_production_abandoned_total")

                          case NakamotoChainStore.CanonicalEffectsOutcome.Applied(_, (canonical, canonicalHashed)) =>
                            Metrics[F].updateGauge("dag_nakamoto_ordinal", canonical.ordinal) >>
                              (canonical.signedSnapshot.value.slotCertificate match {
                                case None =>
                                  productionGate.pause(ProductionGate.ReorgInProgress) >>
                                    logger.error(
                                      s"Refusing publication and pausing production for locally selected ordinal=${canonical.ordinal}: " +
                                        "stored snapshot has no slot certificate"
                                    ) >> Metrics[F].incrementCounter("dag_nakamoto_production_abandoned_total")

                                case Some(certificate) =>
                                  for {
                                    // This check can race a later selection change after the CAS lock is released. Publication therefore
                                    // disseminates only a replay-valid branch candidate; it is not a canonicality, preference, finality, or
                                    // state-validity assertion and cannot feed optimistic attestation emission.
                                    gateOpenBeforePublish <- productionGate.isOpen
                                    _ <-
                                      if (!gateOpenBeforePublish)
                                        productionGate.pauseReasons.flatMap(reasons =>
                                          logger.info(
                                            s"🛑 Abandoning production at slot ${canonical.slot} before publish " +
                                              s"(gate closed: ${reasons.mkString(", ")})"
                                          )
                                        )
                                      else Async[F].unit
                                    _ <- Async[F].whenA(gateOpenBeforePublish) {
                                      sidecarClient
                                        .publishSnapshot(
                                          SidecarClient.mkSnapshot(
                                            hash = canonicalHashed.hash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                            slot = canonical.slot,
                                            ordinal = canonical.ordinal,
                                            parentHash = canonical.parentHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                                            vrfProof = certificate.vrfProof.toBytes,
                                            vrfPublicKey = certificate.vrfPublicKey.toBytes,
                                            eta = Hex(certificate.eta.value).toBytes,
                                            payload = {
                                              import io.circe.syntax._
                                              val snapshotJson = canonical.signedSnapshot.asJson
                                              val contextJson = canonical.context.asJson
                                              val combined = io.circe.Json.obj("snapshot" -> snapshotJson, "context" -> contextJson)
                                              combined.noSpaces.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                                            },
                                            producerId = selfId.value.toBytes,
                                            parentSlot = certificate.parentSlot.value.value,
                                            kesSignature = kesSnapSigBytes
                                          )
                                        )
                                        .void
                                        .handleErrorWith(e => logger.warn(s"Sidecar publish failed: ${e.getMessage}")) >>
                                        logger.info(
                                          s"Produced snapshot ordinal=${canonical.ordinal} slot=${canonical.slot} " +
                                            s"events=${eventSet.size} returned=${returnedEvents.size} pool=$activePoolSize"
                                        ) >>
                                        Metrics[F].incrementCounter("dag_nakamoto_snapshots_produced") >>
                                        Async[F].delay(System.currentTimeMillis()).flatMap { nowMs =>
                                          Metrics[F].recordDistribution(
                                            "dag_nakamoto_production_duration_ms",
                                            (nowMs - productionStartMs).toInt
                                          ) >>
                                            productionTimestamps.update { ts =>
                                              val updated = ts + (canonical.ordinal -> nowMs)
                                              // Evict entries older than 1000 ordinals to bound memory
                                              if (updated.size > 1000) updated.toList.sortBy(-_._1).take(1000).toMap
                                              else updated
                                            }
                                        }
                                    }
                                  } yield ()
                              })
                        }
                  }

                  // ─── Shard-checkpoint fan-out: MOVED to the per-slot tick (design §5.7, 2026-06-12) ───
                  // The GL0-leader self-call that lived here advanced shard duty once per produced GL0 ordinal;
                  // half of the run-10 Gap-A cadence inversion (the daemon's becameBestTip hook was the other
                  // half). The lottery now draws every wall-clock slot in the slot-tick loop above, on every node,
                  // gated by committee membership + the single-outstanding-checkpoint watermark. Nothing to do here.
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
