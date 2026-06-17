package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Clock, Ref}
import cats.effect.std.{Semaphore, Supervisor}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.{DAGEvent, GlobalSnapshotEvent}
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{GossipStream, SidecarClient}
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointAcceptResult
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.serde.codecs.instances.CompatCodecs._
import io.constellationnetwork.validator.StateProofValidator

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.estatico.newtype.ops._
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Subscribes to sidecar GossipSub for live snapshots and attestations.
  *
  * Uses NakamotoChainStore for fork-aware storage instead of raw SnapshotStorage.prepend. Records attestations in TipTracker for finality
  * tracking.
  */
object NakamotoSyncDaemon {

  private val CatchUpThreshold = 6L
  private val CatchUpCooldownMs = 10000L // Don't retry catch-up more often than every 10s

  // #259 metagraph-binary active-recovery (stuck-detection tick) tuning. Fixed internal cadence —
  // NOT a consensus-critical value (recovery is additive + re-gated), so kept as plain constants
  // rather than HOCON config (mirrors CatchUpThreshold/CatchUpCooldownMs above).
  //   - Tick every 20s: the orphan-buffer pending-parents poll cadence.
  //   - A parent must persist across ≥2 ticks (≥~20s genuinely stuck, not a mid-drain blip) before
  //     we fetch — encoded as `consecutiveTicks >= 2` at the call site.
  //   - After a fetch attempt, don't refetch the same (mg, parentHash) for 60s even if still pending.
  private val StuckRefetchTickInterval: scala.concurrent.duration.FiniteDuration = 20.seconds
  private val StuckRefetchCooldown: scala.concurrent.duration.FiniteDuration = 60.seconds

  /** Per-(metagraph, parentHash) bookkeeping for the #259 stuck-detection tick. `consecutiveTicks` counts how many consecutive ticks this
    * parent has stayed pending in the orphan buffer (reset to 0 — by omission from the carry-forward map — once it resolves);
    * `lastFetchAtMillis` is the wall-clock ms of the last active fetch attempt, used for the per-pair refetch cooldown. Wall-clock here is
    * fine: it gates a best-effort recovery cadence, not consensus.
    */
  final case class StuckParentState(consecutiveTicks: Int, lastFetchAtMillis: Long)

  // Confirmation depth k₁ — same as SnapshotLeaderLoop.ConfirmationDepthK; the boundary between Tier 2 (sequential
  // walk-back) and Tier 3 (full catch-up + backfill): gaps > k mean the network has finalized past our tip, so
  // sequential fetch won't work. NO module-level sys.env read here anymore — the value is threaded in as the
  // `confirmationDepthK` parameter of `run` -> `handleSnapshot` from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value`
  // (project rule: HOCON over scattered env reads).

  private val vrf = EcVrf25519.default

  /** Derive VRF output from proof bytes. The chain store needs the output (not the proof) for eta computation. The producer stores
    * vrfOutput directly, but gossip only carries the proof — we must derive the output here to match what the producer stored.
    *
    * Visibility `private[nakamoto]` (Gap A/B shard wiring): `SnapshotLeaderLoop`'s shard-producer store path uses the SAME derivation to
    * recompute the producer's own checkpoint vrfOutput, so producer + receiver store byte-identical values for the same checkpoint.
    */
  private[nakamoto] def vrfOutputFromProof(proofBytes: Array[Byte]): Array[Byte] =
    vrf.vrfProofToHash(proofBytes).getOrElse(proofBytes) // fallback to raw proof if derivation fails

  /** S3 attestation-inversion gate: decides whether a re-exec/validated `ShardCheckpoint` may be adopted as best-tip and attested.
    *
    * Mirrors the global chain's validate-by-replay → adopt → attest discipline: a node adopts (+ counts the signers' attestations + emits
    * its own attestation) ONLY for a checkpoint that passed `ShardCheckpointGl0AcceptanceManager.evaluate`'s pre-checks AND
    * (quorum-attested OR re-exec matched). A `Rejected` (pre-check fail) or `RejectedReExecutionMismatch` (wrong-derivation) checkpoint is
    * NOT admissible — never adopted, signers never counted toward quorum, no attestation emitted. `PendingMoreAttestations` is admissible
    * (valid-so-far; the chain must advance toward quorum/depth, and re-exec — if it ever fires on the degraded path — gates a later eval).
    *
    * Pure + package-visible so the inversion gate is unit-testable without standing up the full gossip handler.
    */
  private[nakamoto] def shardCheckpointAdmissible(result: ShardCheckpointAcceptResult): Boolean =
    result match {
      case ShardCheckpointAcceptResult.Accepted                          => true
      case ShardCheckpointAcceptResult.PendingMoreAttestations           => true
      case ShardCheckpointAcceptResult.Rejected(_)                       => false
      case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(_, _) => false
    }

  /** Verdict of the parent-missing catch-up admission check ([[verifyCatchUpSnapshot]]). The deep-catch-up path adopts a gossiped
    * `(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)` as the node's ENTIRE canonical gl0 state (balances/txRefs/stakes/locks +
    * MPT), so — unlike the happy path — the carried snapshot's parent is not in the local chain store and the full
    * `NakamotoSnapshotValidator.validate` (VRF + slot-cert) is unreachable. Without a gate, a single peer gossiping a forged tuple at
    * `ordinal > k` would unilaterally reset the victim's state. The two gates below are each a PURE function of the gossiped bytes + the
    * carried context (identical verdict on every honest node), and BOTH must pass before any canonical write.
    */
  sealed private[nakamoto] trait CatchUpVerdict
  private[nakamoto] object CatchUpVerdict {

    /** Both gates passed: envelope signature is valid AND the carried `GlobalSnapshotInfo` rebuilds to the exact `stateProof` the snapshot
      * commits to. Safe to adopt. Carries the verified `Hashed` (sig-checked) so the caller reuses it for canonical storage instead of
      * re-hashing via the no-check `toHashed`.
      */
    case class Accept(hashed: Hashed[GlobalIncrementalSnapshot]) extends CatchUpVerdict

    /** Gate 1 failed: the envelope signature does not verify against the snapshot hash. Forged / unauthorized signer. Do NOT adopt. */
    case object RejectedInvalidSignature extends CatchUpVerdict

    /** Gate 2 failed: the carried `GlobalSnapshotInfo` does NOT rebuild to the `stateProof` baked into the (validly-signed) snapshot — the
      * attacker (or a corrupt/mis-paired payload) supplied a context inconsistent with what was signed. Do NOT adopt the context into the
      * MPT. Carries the verified `Hashed` for logging the ordinal/hash.
      */
    case class RejectedStateProofMismatch(hashed: Hashed[GlobalIncrementalSnapshot]) extends CatchUpVerdict
  }

  /** Parent-missing catch-up admission gate. Verifies a gossiped catch-up snapshot WITHOUT needing its parent (which the node lacks during
    * deep catch-up), so the unilateral-state-reset attack is closed even though `NakamotoSnapshotValidator.validate` can't run here.
    *
    * Gate 1 — '''envelope signature''' (`toHashedWithSignatureCheck`, the SAME callable the normal pull path uses at
    * `GlobalL0Service.pullLatestSnapshotFromPeer` / `pullSnapshots`): rejects a tuple whose `Signed[GlobalIncrementalSnapshot]` proofs
    * don't verify against the snapshot hash. A forged snapshot from an unauthorized signer cannot pass.
    *
    * Gate 2 — '''stateProof consistency''': rebuilds the `GlobalSnapshotStateProof` from the carried `GlobalSnapshotInfo` alone (via
    * `StateProofValidator.forGlobal(producer = None)` → `GlobalSnapshotInfo.mptStateProof`, a PURE function of the GSI bytes — it does NOT
    * read or mutate the live `MptStore`) and requires it `equivalent` to the `stateProof` the signed snapshot commits to, using the EXACT
    * symmetric `StateProofComparison` the producer/follower already use (`StateProofValidator.validateProof`). This binds the
    * MPT-to-be-written GSI to the signed snapshot: an attacker cannot pair a validly-signed snapshot with an attacker-chosen state.
    *
    * Both gates are deterministic and side-effect-free w.r.t. canonical storage / MPT, so an honest snapshot (valid sig, GSI matching its
    * own committed stateProof) ALWAYS yields `Accept` and legitimate catch-up still recovers.
    *
    * '''Why gates 1+2 suffice to close the unilateral-state-reset attack.''' The attack adopts an attacker-chosen `GlobalSnapshotInfo` as
    * canonical state. Gate 1 forces the carried snapshot to be signed by a key whose proof verifies the snapshot hash; gate 2 forces the
    * carried GSI to be EXACTLY the state that snapshot committed to (`stateProof` incl. `mptRoot`). So the only state an attacker can
    * install is one already bound to a validly-signed snapshot — i.e. real consensus output, not a fabrication.
    *
    * '''Gate 3 (majority-hash) — DEFERRED, follow-up.''' The normal BFT pull path cross-checks a snapshot's hash against majority peers
    * (`GlobalL0Service.getMajorityHash`). That machinery lives in node-shared's pull-mode `GlobalL0Service` and is NOT wired into this
    * push-based sidecar-gossip daemon (`catchUpFromGossip` receives no `L0ClusterStorage` / snapshot client). Adding it means threading new
    * peer-query infrastructure through the daemon constructor; out of scope here. Impact of omission: a validly-signed-but-MINORITY-fork
    * snapshot could still be adopted during catch-up. That is bounded — it must be real signed consensus output, and on the next local
    * production / gossip wave normal fork-choice (ChainSelection) reorgs to the denser chain — whereas the closed hole allowed adopting a
    * fabrication with NO signer at all.
    *
    * '''Gate 4 (VRF / slot-cert) — DEFERRED, sound reason.''' Eligibility verification needs the period `eta` (and active-set/stake) at the
    * snapshot's period; during deep catch-up (gap > k) the node lacks the chain history to derive that eta deterministically.
    * `handleSnapshot` already falls back to the snapshot's OWN self-reported `eta` when the parent chain is absent — using that
    * attacker-supplied eta to verify the attacker's own VRF is circular and adds no security. So VRF/slot-cert is not soundly checkable in
    * the parent-missing case and is intentionally not attempted here; gates 1+2 carry the safety.
    */
  private[nakamoto] def verifyCatchUpSnapshot[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector](
    signedSnapshot: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[CatchUpVerdict] =
    HasherSelector[F].withCurrent { implicit hasher =>
      // Gate 1: envelope signature. `toHashedWithSignatureCheck` returns Left(InvalidSignatureForHash) when any proof fails.
      signedSnapshot.toHashedWithSignatureCheck.flatMap {
        case Left(_) =>
          (CatchUpVerdict.RejectedInvalidSignature: CatchUpVerdict).pure[F]
        case Right(hashed) =>
          // Gate 2: rebuild the state proof from the carried GSI (no producer ⇒ pure, no live-store access) and compare against the
          // snapshot's committed `stateProof` via the shared symmetric comparison.
          StateProofValidator
            .forGlobal[F](None)
            .validate(hashed, context)
            .map {
              case cats.data.Validated.Valid(_)   => CatchUpVerdict.Accept(hashed)
              case cats.data.Validated.Invalid(_) => CatchUpVerdict.RejectedStateProofMismatch(hashed)
            }
      }
    }

  /** Ethereum-style mempool reconciliation after catch-up/reorg.
    *
    * Evicts DAG blocks whose transactions reference a lastTxRef that no longer matches the new context. Keeps events whose transactions are
    * still unconfirmed in the new state — they should be included in the next snapshot.
    *
    * Non-DAG events (state channel, allow spend, etc.) are preserved since they have separate validation semantics handled by the
    * acceptance manager.
    */
  private def reconcileMempool[F[_]: Async](
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    context: GlobalSnapshotInfo,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      hashes <- eventMempool.getEventHashes
      events <- eventMempool.getMultiple(hashes)
      staleHashes = events.collect {
        case (hash, hashed) =>
          hashed.signed.value match {
            case DAGEvent(signedBlock) =>
              val isStale = signedBlock.value.transactions.exists { signedTx =>
                val tx = signedTx.value
                val contextRef = context.lastTxRefs.getOrElse(tx.source, TransactionReference.empty)
                // Block is stale if the context's lastTxRef for this source
                // differs from the transaction's parent ref — meaning the context
                // has already processed a different transaction chain for this address.
                contextRef =!= TransactionReference.empty && contextRef =!= tx.parent
              }
              if (isStale) Some(hash) else None
            case _ => None // keep non-DAG events
          }
      }.flatten.toSet
      poolSize <- eventMempool.size
      _ <-
        if (staleHashes.nonEmpty)
          logger.info(
            s"🧹 Mempool reconciliation: evicting ${staleHashes.size} stale DAG blocks, keeping ${poolSize - staleHashes.size} events"
          ) >> eventMempool.remove(staleHashes)
        else
          logger.info(s"🧹 Mempool reconciliation: all $poolSize events valid for new context, keeping all")
    } yield ()

  /** After storing a snapshot, check if any buffered gossip snapshots were waiting for it as their parent. If so, process them via
    * handleSnapshot (which will now find the parent in the chain store and validate successfully). This creates a validation cascade from
    * the shared genesis ancestor through the gossip chain.
    */
  private def drainPendingChildren[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    storedHash: Hash,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ (Tier-2 vs Tier-3 gap boundary). Forwarded from `run`; sourced from
    // `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` (replaces the prior module-level sys.env read).
    confirmationDepthK: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-staged accumulator
    // stripped->canonical and thus promotes on finalize (complete served ring; was per-producer-sparse).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    // Hierarchical-shard-checkpoints v1 — per-ord producer fan-out, threaded through `handleSnapshot`
    // so drained children also fan out shard checkpoints on their becameBestTip path. EMPTY / `None` at
    // numShards=1 (regression bar).
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers — the producer fan-out input (the inversion).
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded through the drain → handleSnapshot cascade.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    pendingParentRef.modify { m =>
      val children = m.getOrElse(storedHash, List.empty)
      (m - storedHash, children)
    }.flatMap { children =>
      if (children.isEmpty) Async[F].unit
      else
        logger.info(s"🔗 Draining ${children.size} buffered snapshot(s) whose parent ${storedHash.value.take(12)} is now available") >>
          children.traverse_ { childSnap =>
            handleSnapshot(
              childSnap,
              stateRef,
              pendingParentRef,
              chainStore,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              sidecarClient,
              selfId,
              keyPair,
              lddConfig,
              eligibilityChecker,
              lastKnownSlotRef,
              epochStateRef,
              etaRotationSnapshots,
              confirmationDepthK,
              consensusFns,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              mptStore,
              mptOverlay,
              pendingAccumulatorsRef,
              eventMempool,
              chainSyncManager,
              channel,
              dataDir,
              operationalKeyMaker,
              kesRegistry,
              shardProducers,
              shardChainStores,
              shardBinaryBuffers,
              shardAssignment,
              shardCommitteeMembership,
              logger
            )
          }
    }

  final case class SyncState(
    networkTipOrdinal: Long,
    networkTipHash: Option[Hash],
    localTipOrdinal: Long,
    isReady: Boolean,
    lastCatchUpAttemptMs: Long = 0L
  )

  object SyncState {
    def initial: SyncState = SyncState(0L, None, 0L, isReady = false)
  }

  def run[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    channel: ManagedChannel,
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ — threaded from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` at the
    // GlobalSnapshotConsensus.make call site (replaces the prior module-level
    // `sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH")` read; project rule: HOCON over scattered env reads).
    // Used as the Tier-2 (sequential walk-back) vs Tier-3 (full catch-up) gap boundary in `handleSnapshot`.
    confirmationDepthK: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    snapshotSemaphore: Semaphore[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `handleSnapshot` -> `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-
    // staged accumulator stripped->canonical and thus promotes on finalize (complete served ring).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    dataDir: java.nio.file.Path,
    // (#196) Sink for inbound AllowSpendBlock gossip — same queue
    // GlobalSnapshotEventsPublisherDaemon drains into the event mempool.
    // Replaces the HTTP POST path from AllowSpendBlockRoutes (which was
    // wired to `queues.l1AllowSpendOutput`); the new outbox-backed gossip
    // path feeds the same queue from a different transport.
    enqueueAllowSpendBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    // (#196 follow-up) Sinks for inbound DAGBlock + TokenLockBlock gossip.
    // Replace the HTTP POST paths from DAGBlockRoutes (`/dag/l1-output`, via Cell
    // pipeline → `queues.l1Output`) and TokenLockBlockRoutes (`/dag/l1-token-lock-output`,
    // via direct offer → `queues.l1TokenLockOutput`). The Cell pipeline for DAGBlock
    // was a pure pass-through in the L0Cell (processDAGL1 → enqueueDAGL1Data →
    // queue.offer); the new gossip path skips that no-op layer.
    enqueueDAGBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.Block
    ] => F[Unit],
    enqueueTokenLockBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.tokenLock.TokenLockBlock
    ] => F[Unit],
    // Shared `Option` ref so other components (e.g. the reactive finality-walkback
    // ChainSyncRequestQueue) can route through the same hash-keyed dedup + fetch
    // machinery without needing their own `ChainSyncManager`. We publish our
    // internally-constructed manager into this Ref once it's built; consumers
    // read-through it and no-op if the producer hasn't bound yet.
    sharedChainSyncManagerRef: Ref[F, Option[ChainSyncManager.ChainSyncManagerAlgebra[F]]],
    // §1.2 Slice 5/6/9: KES parallel-signing infrastructure (sender) + load-bearing
    // receiver-side verify. The OperationalKeyMaker signs attestations + snapshots with
    // the operator's KES product key at the period derived from `EtaCalculation.rotationPeriod`.
    // The KesRegistry holds the (peerId → (kesMasterVk, offset)) map needed by receivers to
    // verify incoming KES sigs and drop them when verification fails.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    // Slice S3: receiver-side path for `pb.MetagraphAttestation` gossip — verify (Ed25519 +
    // KES + committee VRF) then record. Decoupled from the gate's sender path here; the gate's
    // `attestAndAdmit` is wired in front of `processMetagraphBinary` in `Services.scala`. The
    // daemon only needs the receiver hook + the eta resolver to feed the verifier.
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    // #213/#290: resolve `(metagraphAddress, parentHash, binaryContent) → metagraph parent ordinal`
    // by deriving it from the INCOMING binary's OWN content (ordinal − 1), NOT from gl0's currency
    // partitions (which `calculateLastCurrencySnapshots` drops via `.filterNot(_.isEmpty)` whenever
    // the mg produced no state in the window → the GSI-only resolver returned `None` and every
    // inbound attestation was dropped as `UnknownParentOrdinal` — the receiver-side half of the
    // 8gl0+4mg deadlock). The third arg is the attested binary's `signed.value.content`; the handler
    // looks the binary up in the orphan buffer by its wire digest (`att.binaryHash`), so a forged
    // ordinal can never be trusted — the receiver derives the ordinal from the content it holds, and
    // fails closed (drops the attestation) when it does not yet hold the binary. The identity guard
    // (`lastStateChannelSnapshotHashes[mg] == parentHash`) still runs inside the resolver. Handlers
    // chain this into `etaForParentOrdinal` and fail-closed on `None` (no eta to verify against).
    parentOrdinalFor: (
      io.constellationnetwork.schema.address.Address,
      io.constellationnetwork.security.hash.Hash,
      Array[Byte]
    ) => F[Option[Long]],
    // Resolves the canonical `eta` (32 bytes) for the committee VRF input from the metagraph
    // parent ordinal. The daemon's handlers resolve the ordinal via `parentOrdinalFor` first then
    // pass it here — the receiver-side eta must be byte-equivalent to the sender's eta, and the
    // sender derived its eta from the same ordinal. Mismatch surfaces as `InvalidCommitteeVrf` on
    // the gate's verify path.
    etaForParentOrdinal: Long => F[Array[Byte]],
    // Receiver-side σ lookup. The committee VRF threshold is `K · σ_sender`, so the verifier
    // needs the SENDER's stake, not ours. Passed as a callback to keep the daemon agnostic of
    // the StakeRegistry's flavor.
    senderStakeLookup: io.constellationnetwork.schema.peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    // #214: pre-built gate-aware closure that processes a single `(metagraphAddress, wireBytes)`
    // pair through the same committee gate + orphan buffer path. Constructed in
    // `GlobalSnapshotConsensus.make` via `makeMetagraphBinaryProcessor`, so the SAME instance is
    // shared with the `SnapshotLeaderLoop.onFinalize` orphan-drain hook. The shared instance is
    // what makes the drain work: both sites must address the same in-memory orphan buffer.
    processOrphanedMetagraphBinary: (
      io.constellationnetwork.schema.address.Address,
      Array[Byte]
    ) => F[Unit],
    // #259: the SAME orphan-buffer instance the `processOrphanedMetagraphBinary` closure writes into
    // (built in `GlobalSnapshotConsensus.make`). The stuck-detection tick below reads its pending
    // parents via `listPendingParents` to decide which missing binaries to actively pull from peers.
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    // ─── Hierarchical-shard-checkpoints v1 — Gap B receiver routing ──────────────────────────────
    // Acceptance-side per-shard deps (registry of `(chainStore, tipTracker, finalityTriggers)` +
    // acceptance manager). `None` at `numShards = 1` (regression bar) ⇒ incoming `ShardCheckpoint` /
    // `ShardCheckpointAttestation` gossip is dropped with a single debug log, byte-identical to the
    // pre-wiring daemon. `Some(deps)` activates cross-node checkpoint reconstruction + chain growth.
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ] = None,
    // Chain-sync recovery (run-20, task #A): pull a missed shard checkpoint from a peer over HTTP instead of
    // waiting minutes for GossipSub re-gossip (which dedups Tier-1's identical re-publish bytes). Drives the T1
    // orphan trigger (missing parent on receipt) + the T2 absence stream (stuck/empty tip ⇒ pull tip+1; an empty
    // store pulls ordinal 1, the genesis-miss case). `None` at numShards=1 (regression bar) ⇒ no pull machinery.
    shardCheckpointFetcher: Option[ShardCheckpointFetcher[F]] = None,
    // Hierarchical-shard-checkpoints v1 — `T_count_shard` quorum closure. When a received `ShardCheckpoint`
    // becomes this node's best tip, `handleShardCheckpoint` invokes this emitter to sign + gossip a
    // `ShardCheckpointAttestation` (mirrors the gl0 `emitAttestation` becameBestTip seam). `None` at
    // `numShards = 1` (regression bar) ⇒ no emit; byte-identical to the pre-wiring daemon. Built only on the
    // gl0-leader-produce path (`GlobalSnapshotConsensus.make`), where the operator's signing material lives.
    shardCheckpointAttestationEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]
    ] = None,
    // ─── Hierarchical-shard-checkpoints v1 — per-ord producer fan-out (decoupled from gl0-leader win) ──
    // Per-shard checkpoint producers + the static metagraph→shard assignment, threaded so EVERY node fans
    // out shard checkpoints for each canonical (best-tip) gl0 ord it receives via gossip — NOT only the
    // gl0 slot winner. The gl0 leader still fans out for its OWN produced ord via `SnapshotLeaderLoop`
    // (GossipSub doesn't echo a publisher its own message), so together these fire the fan-out exactly
    // once per canonical ord per node. `shardChainStores` is derived in-daemon from
    // `shardAcceptanceDeps.registry` (the SAME instances the acceptance side + producers share). EMPTY /
    // `None` at `numShards = 1` (regression bar) ⇒ the per-ord hook is gated `whenA(false)` ⇒ no allocation.
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ] = Map.empty,
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]] = None
  )(
    implicit globalStateProofSelector: io.constellationnetwork.schema.GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    // Per-shard chain stores derived from the acceptance-side registry (the SAME instances the producers
    // write into + the consumer reads). Empty at numShards=1 (`shardAcceptanceDeps = None`). Mirrors the
    // projection `GlobalSnapshotConsensus.make` does for the leader loop's `shardChainStores` param.
    val shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ] =
      shardAcceptanceDeps
        .map(_.registry.map { case (sid, entry) => sid -> entry.chainStore })
        .getOrElse(Map.empty)

    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers, also projected off the SAME registry. The
    // gossip-intake (`MetagraphBinary` handler) writes into these; the producer fan-out reads them. This
    // shared instance is the inversion — the producer's input comes from buffered binaries, not gl0's
    // post-chain-link `stateChannelSnapshots`. Empty at numShards=1.
    val shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ] =
      shardAcceptanceDeps
        .map(_.registry.map { case (sid, entry) => sid -> entry.binaryBuffer })
        .getOrElse(Map.empty)

    // EXECUTION-SHARDING Task 2: the deterministic committee draw, projected from the SAME `shardAcceptanceDeps` the acceptance
    // manager closes over, so the daemon's producer fan-out gates produce on the IDENTICAL committee the verifier admits against.
    // `None` (numShards=1) ⇒ empty-set draw (never reached: the fan-out is gated on `shardProducers.nonEmpty` first).
    val shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]] =
      shardAcceptanceDeps
        .map(_.committeeMembership)
        .getOrElse((_: io.constellationnetwork.schema.sharding.ShardId, _: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
          Async[F].pure(Set.empty[peer.PeerId])
        )

    // Buffer for gossip snapshots whose parent isn't in the chain store yet.
    // Keyed by missing parent hash → list of raw gossip snapshots waiting for that parent.
    // When a snapshot is stored in the chain store, we check this buffer and validate any
    // snapshots that were waiting for it. This creates a validation cascade from genesis.
    fs2.Stream.eval(Ref.of[F, Map[Hash, List[pb.Snapshot]]](Map.empty)).flatMap { pendingParentRef =>
      // Metagraph state-channel binary orphan buffer (#213) is now constructed by the caller
      // (GlobalSnapshotConsensus.make, #214) and threaded into this daemon as
      // `processOrphanedMetagraphBinary`. The caller shares the same buffer instance with the
      // SnapshotLeaderLoop.onFinalize drain hook so finalized-parent children re-enter through
      // the same gate-aware path used here.
      fs2.Stream.eval(Ref.of[F, SyncState](SyncState.initial)).flatMap { stateRef =>
        // ChainSyncManager for active parent fetching. Uses a Ref to break the
        // circular dependency: handleSnapshot needs chainSyncManager, but
        // chainSyncManager's callback needs handleSnapshot.
        fs2.Stream
          .eval(
            Ref.of[F, Option[ChainSyncManager.ChainSyncManagerAlgebra[F]]](None).flatMap { csRef =>
              ChainSyncManager
                .make[F](
                  channel,
                  { resp: io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse[pb.Snapshot] =>
                    // #56.8: ADT pattern-match on the chain-sync disposition. Today's `handleSnapshot`
                    // pipeline doesn't yet differentiate finalized vs provisional — both are validated
                    // and stored identically. The structural distinction lands here so #56.10's
                    // overlay-aware accept() can route Provisional fetches to overlay-local commits
                    // (matching codex's NakamotoSyncDaemon catch in plan rev4 phase E) and Finalized
                    // fetches straight to base. NotFound logs and unwinds the inflight tracker.
                    import io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse
                    val fetchedSnap: Option[pb.Snapshot] = resp match {
                      case ChainSyncStateResponse.Finalized(snap)      => Some(snap)
                      case ChainSyncStateResponse.Provisional(snap, _) => Some(snap)
                      case ChainSyncStateResponse.NotFound(_)          => None
                    }
                    fetchedSnap match {
                      case Some(snap) =>
                        csRef.get.flatMap {
                          case Some(csm) =>
                            snapshotSemaphore.permit.use { _ =>
                              handleSnapshot(
                                snap,
                                stateRef,
                                pendingParentRef,
                                chainStore,
                                nodeStorage,
                                tipTracker,
                                stakeRegistry,
                                sidecarClient,
                                selfId,
                                keyPair,
                                lddConfig,
                                eligibilityChecker,
                                lastKnownSlotRef,
                                epochStateRef,
                                etaRotationSnapshots,
                                confirmationDepthK,
                                consensusFns,
                                snapshotStorage,
                                lastGlobalSnapshotStorage,
                                lastNGlobalSnapshotStorage,
                                productionGate,
                                mptStore,
                                mptOverlay,
                                pendingAccumulatorsRef,
                                eventMempool,
                                csm,
                                channel,
                                dataDir,
                                operationalKeyMaker,
                                kesRegistry,
                                shardProducers,
                                shardChainStores,
                                shardBinaryBuffers,
                                shardAssignment,
                                shardCommitteeMembership,
                                logger
                              )
                            }
                          case None => Async[F].unit
                        }
                      case None =>
                        // NotFound — peer doesn't have the requested hash. Inflight tracking is
                        // unwound by ChainSyncManager's `guaranteeCase`; nothing further to do here.
                        Async[F].unit
                    }
                  }
                )
                .flatMap(csm => csRef.set(Some(csm)) >> sharedChainSyncManagerRef.set(Some(csm)).as(csm))
            }
          )
          .flatMap { chainSyncManager =>
            // Gossip stream with two-layer reconnection:
            //
            // Layer 1 (sidecar-triggered): When the sidecar's mesh health monitor
            //   recovers from degradation, it closes the gRPC Subscribe stream.
            //   handleErrorWith catches the error and re-subscribes.
            //
            // Layer 2 (idle watchdog): If the gRPC connection dies silently (e.g.,
            //   Docker network disconnect — gRPC Java doesn't propagate channel
            //   failures to blocking server-stream reads), the stream hangs forever.
            //   A concurrent watchdog checks the last-received timestamp every 30s
            //   and raises an error after 120s of idle. In normal operation (messages
            //   every ~10s), the watchdog never fires.
            //
            // Shared state (pendingParentRef, stateRef, chainSyncManager) survives.
            def gossipStream: fs2.Stream[F, Unit] =
              fs2.Stream
                // Idle-watchdog clock: MONOTONIC via the Clock typeclass (never System.currentTimeMillis — an NTP step
                // would fire or suppress the idle timeout spuriously, and the typeclass keeps the effect lawful/testable;
                // cats-idiomatic time rule, owner 2026-06-12).
                .eval(Clock[F].monotonic.map(_.toMillis).flatMap(Ref.of[F, Long](_)))
                .flatMap { lastMsgRef =>
                  fs2.Stream.eval(cats.effect.std.Queue.bounded[F, pb.Snapshot](1024)).flatMap { snapshotIntakeQ =>
                    val watchdog = fs2.Stream.fixedRate[F](30.seconds).evalMap { _ =>
                      Clock[F].monotonic.map(_.toMillis).flatMap { now =>
                        lastMsgRef.get.flatMap { lastMsg =>
                          val idleMs = now - lastMsg
                          if (idleMs > 120000L)
                            logger.warn(s"Gossip stream idle for ${idleMs / 1000}s, forcing reconnect") >>
                              Async[F].raiseError[Unit](new RuntimeException(s"Gossip idle timeout (${idleMs / 1000}s)"))
                          else
                            Async[F].unit
                        }
                      }
                    }

                    // Serial snapshot consumer — preserves the old ordering + semaphore semantics, but on its OWN lane so
                    // checkpoints/attestations/binaries never wait behind snapshot catch-up (intake demux, run-16).
                    val snapshotWorker: fs2.Stream[F, Unit] =
                      fs2.Stream
                        .fromQueueUnterminated(snapshotIntakeQ)
                        .evalMap { snap =>
                          val incomingOrdinal = snap.ordinal
                          chainStore.bestTipOrdinal.flatMap { currentBestOrdinal =>
                            val wouldWin = incomingOrdinal > currentBestOrdinal.getOrElse(0L)
                            (if (wouldWin) productionGate.pause(ProductionGate.BetterGossipReceived)
                             else Async[F].unit) >>
                              snapshotSemaphore.permit.use { _ =>
                                handleSnapshot(
                                  snap,
                                  stateRef,
                                  pendingParentRef,
                                  chainStore,
                                  nodeStorage,
                                  tipTracker,
                                  stakeRegistry,
                                  sidecarClient,
                                  selfId,
                                  keyPair,
                                  lddConfig,
                                  eligibilityChecker,
                                  lastKnownSlotRef,
                                  epochStateRef,
                                  etaRotationSnapshots,
                                  confirmationDepthK,
                                  consensusFns,
                                  snapshotStorage,
                                  lastGlobalSnapshotStorage,
                                  lastNGlobalSnapshotStorage,
                                  productionGate,
                                  mptStore,
                                  mptOverlay,
                                  pendingAccumulatorsRef,
                                  eventMempool,
                                  chainSyncManager,
                                  channel,
                                  dataDir,
                                  operationalKeyMaker,
                                  kesRegistry,
                                  shardProducers,
                                  shardChainStores,
                                  shardBinaryBuffers,
                                  shardAssignment,
                                  shardCommitteeMembership,
                                  logger
                                )
                              } >>
                              productionGate.resume(ProductionGate.BetterGossipReceived)
                          }

                        }

                    val gossip = GossipStream
                      .subscribe[F](channel)
                      .evalMap { msg =>
                        Clock[F].monotonic.map(_.toMillis).flatMap(lastMsgRef.set) >>
                          (msg.body match {
                            case pb.GossipMessage.Body.Snapshot(snap) =>
                              // INTAKE DEMUX (run-16 post-mortem, 2026-06-12): snapshot processing is seconds-to-minutes during
                              // catch-up and used to run INLINE here, serializing the ONE gossip stream — on the slowest-booting
                              // node every checkpoint/attestation queued behind it for MINUTES (gl0-2: sidecar got the genesis
                              // checkpoint at 03:26:35, the JVM processed it at 03:35:32 — the head-of-line block behind boot
                              // catch-up that re-forked every staircase run). Snapshots now route to a dedicated bounded queue
                              // drained serially by `snapshotWorker` below (same semaphore, same ordering); all other bodies
                              // flow past without waiting. On a full queue the snapshot is DROPPED with a WARN — snapshot
                              // recovery is pull-based (ChainSync / pullFinalityGated), so a dropped gossip copy is re-fetched,
                              // whereas blocking here would re-introduce the head-of-line stall this demux removes.
                              snapshotIntakeQ.tryOffer(snap).flatMap {
                                case true => Async[F].unit
                                case false =>
                                  logger.warn(
                                    s"Gossip intake: snapshot queue FULL — dropped gossiped ord=${snap.ordinal} (pull-based recovery will refetch)"
                                  )
                              }

                            case pb.GossipMessage.Body.Attestation(att) =>
                              // Background-fire (intake demux completion, run-20): KES + Ed25519 verify per attestation is
                              // tens of ms; inline on the single gossip `evalMap` thread it serializes EVERY later message —
                              // during the boot attestation burst that is the exact head-of-line the demux was meant to remove
                              // (the same skew-rejection failure the MetagraphBinary case documents below, and the channel by
                              // which a shard genesis checkpoint queued behind the burst waited MINUTES in run-20). The
                              // `tipTracker` tally is `Ref`-backed (concurrent-safe), so order-independence holds.
                              Async[F].start(handleAttestation(att, tipTracker, kesRegistry, etaRotationSnapshots, logger)).void

                            case pb.GossipMessage.Body.MetagraphBinary(mb) =>
                              // Background-fire: the gate's `attestAndAdmit` blocks up to gateTimeoutMs
                              // (30s default) waiting for ⌈2K/3⌉ committee attestations. Running it on
                              // the gossip stream's `evalMap` thread serializes EVERY message behind
                              // every pending gate — gl0 TipAttestations from peers then arrive past
                              // `TipTracker.MaxAttestationSkewMs` and get rejected (skew=200+s observed
                              // in iter-s3prep-baseline). Gate work is naturally concurrent-safe: the
                              // aggregator is a `Ref[F, ...]`, `processMetagraphBinary` writes to a
                              // queue, and per-binary state is keyed by (mgAddr, parentHash, binaryHash).
                              //
                              // EXECUTION-SHARDING R-1 (additive — the inversion intake): when sharding is active
                              // (`numShards > 1` ⇒ `shardBinaryBuffers.nonEmpty`), ALSO buffer this raw binary into
                              // the buffer for its shard, off the EXISTING global binaries gossip topic (no Go
                              // per-shard topic — that's a later load-shedding optimization). The shard producer
                              // reads `snapshotPending` from this SAME buffer. The legacy `handleMetagraphBinary`
                              // chain-link admission path is kept UNCHANGED — this is purely additive (R-3 retires
                              // legacy later). At `numShards = 1` the buffer map is empty ⇒ this is a no-op, the
                              // legacy path is the sole path, byte-identical.
                              Async[F]
                                .start(
                                  handleMetagraphBinary(mb, processOrphanedMetagraphBinary, logger)
                                )
                                .void *>
                                Async[F]
                                  .whenA(shardBinaryBuffers.nonEmpty)(
                                    Async[F]
                                      .start(
                                        bufferReceivedBinaryForShard(mb, shardBinaryBuffers, shardAssignment, logger)
                                      )
                                      .void
                                  )

                            case pb.GossipMessage.Body.MetagraphAttestation(att) =>
                              // Background-fire: VRF verify + KES verify + Ed25519 verify add up to
                              // tens of ms per attestation. In bursts (each peer attests each binary),
                              // this would queue up behind the stream's serial evalMap. Aggregator
                              // record is concurrent-safe.
                              Async[F]
                                .start(
                                  handleMetagraphAttestation(
                                    att,
                                    committeeGate,
                                    parentOrdinalFor,
                                    etaForParentOrdinal,
                                    senderStakeLookup,
                                    orphanBuffer,
                                    logger
                                  )
                                )
                                .void

                            // Background-fire (intake demux completion, run-20): these handlers `enqueue*` into a mempool
                            // queue (a possibly-bounded `offer`); inline on the gossip `evalMap` a full queue blocks EVERY
                            // later message behind it (the head-of-line the demux removes for the metagraph/shard cases).
                            // Block acceptance reorders by parent ref downstream, so a per-fiber enqueue race is harmless.
                            case pb.GossipMessage.Body.AllowSpendBlock(asb) =>
                              Async[F].start(handleAllowSpendBlock(asb, enqueueAllowSpendBlock, logger)).void

                            case pb.GossipMessage.Body.DagBlock(blk) =>
                              Async[F].start(handleDAGBlock(blk, enqueueDAGBlock, logger)).void

                            case pb.GossipMessage.Body.TokenLockBlock(blk) =>
                              Async[F].start(handleTokenLockBlock(blk, enqueueTokenLockBlock, logger)).void

                            // Gap B: shard-checkpoint envelope + attestation gossip routing (load-bearing cross-node
                            // reconstruction). `shardAcceptanceDeps = None` (numShards=1, regression bar) ⇒ both
                            // handlers drop with a single debug log. When active, the checkpoint is decoded,
                            // reconstructed into a `Signed[ShardCheckpoint]` (the wire drops slot/vrfOutput — both are
                            // recovered deterministically), stored into the per-shard chain store so the chain grows
                            // cross-node, each committee signer is recorded into the tip tracker (so `T_count_shard`
                            // reaches quorum), and the acceptance manager is run diagnostically. Background-fire with
                            // `Async.start` so the multi-step verify never blocks the gossip evalMap thread (mirrors
                            // the MetagraphAttestation handling above).
                            case pb.GossipMessage.Body.ShardCheckpoint(cp) =>
                              Async[F]
                                .start(
                                  handleShardCheckpoint(
                                    cp,
                                    shardAcceptanceDeps,
                                    shardCheckpointAttestationEmitter,
                                    shardCheckpointFetcher,
                                    selfId,
                                    logger
                                  )
                                )
                                .void

                            case pb.GossipMessage.Body.ShardCheckpointAttestation(att) =>
                              Async[F].start(handleShardCheckpointAttestation(att, shardAcceptanceDeps, logger)).void

                            case _: pb.GossipMessage.Body.Rumor =>
                              Async[F].unit

                            case pb.GossipMessage.Body.Empty =>
                              Async[F].unit
                          })
                      }

                    gossip.concurrently(watchdog).concurrently(snapshotWorker)
                  }
                }
                .handleErrorWith { e =>
                  fs2.Stream.eval(
                    logger.warn(s"Gossip stream error: ${e.getMessage}. Reconnecting in 5s...")
                  ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream
                } ++ fs2.Stream.eval(
                // Normal termination: gRPC StreamObserver.onError puts None in the
                // queue, causing fromQueueNoneTerminated to end the stream normally
                // (not as an error). This happens when the sidecar connection dies
                // during a partition. Restart after a short delay.
                logger.warn("Gossip stream terminated (sidecar connection lost). Reconnecting in 5s...")
              ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream

            // #259: stuck-parent active-recovery tick. A metagraph binary whose parent the local
            // committee gate never admitted (and whose orphan-drain never resolved) sits in the
            // orphan buffer forever — every later binary chains off it and re-buffers, so the
            // metagraph stalls from this node's view. This tick detects parents that persist across
            // ≥2 ticks (a real timeout, not a transient mid-drain blip) and actively PULLS the
            // missing binary by its value-hash from a peer over the existing ChainSync protocol,
            // then re-feeds it through the SAME gate-aware path gossip uses (`processOrphanedMetagraphBinary`
            // via `handleMetagraphBinary`). Purely additive recovery — never trust-on-fetch (the
            // re-fed binary is re-validated by the committee gate).
            //
            // Rate-limited per `(mg, parentHash)`: after a fetch attempt we don't retry that pair
            // for `StuckRefetchCooldown` (60s) even if it stays pending, so a genuinely-missing
            // binary (no peer has it) doesn't hammer the mesh.
            //
            // The orphan's parent hash IS the value-hash of the un-admitted binary we need (orphan
            // buffer keys on `parentHash == missing-binary.value.hash`), so we request
            // `binaryHashes = [parentHash]` directly — the serve handler matches by value-hash.
            def stuckDetectionStream: fs2.Stream[F, Unit] =
              fs2.Stream
                .eval(
                  Ref.of[F, Map[
                    (io.constellationnetwork.schema.address.Address, Hash),
                    NakamotoSyncDaemon.StuckParentState
                  ]](Map.empty)
                )
                .flatMap { stuckStateRef =>
                  fs2.Stream.fixedRate[F](StuckRefetchTickInterval).evalMap { _ =>
                    Async[F].realTimeInstant.map(_.toEpochMilli).flatMap { now =>
                      orphanBuffer.listPendingParents.flatMap { pending =>
                        val pendingSet = pending.toSet
                        stuckStateRef.modify { prev =>
                          // Carry forward only still-pending parents (a resolved parent resets its
                          // tick count). Bump consecutiveTicks for each currently-pending key.
                          val bumped = pendingSet.iterator.map { key =>
                            val prior = prev.getOrElse(key, NakamotoSyncDaemon.StuckParentState(0, 0L))
                            key -> prior.copy(consecutiveTicks = prior.consecutiveTicks + 1)
                          }.toMap
                          // Due-to-fetch: seen across ≥2 ticks AND past the per-pair cooldown.
                          val due = bumped.collect {
                            case (key, st) if st.consecutiveTicks >= 2 && (now - st.lastFetchAtMillis) >= StuckRefetchCooldown.toMillis =>
                              key
                          }.toList
                          // Stamp lastFetchAt on the due keys so the cooldown starts now.
                          val withStamp = due.foldLeft(bumped) { (acc, key) =>
                            acc.updated(key, acc(key).copy(lastFetchAtMillis = now))
                          }
                          (withStamp, due)
                        }.flatMap { due =>
                          if (due.isEmpty) Async[F].unit
                          else
                            logger.info(
                              s"🔎 ChainSync stuck-detection: ${due.size} stuck metagraph parent(s) past timeout — actively fetching: " +
                                due.map { case (mg, h) => s"$mg/${h.value.take(12)}" }.mkString(", ")
                            ) >>
                              due.traverse_ {
                                case (mg, parentHash) =>
                                  chainSyncManager.fetchMetagraphBinaries(mg, List(parentHash)).flatMap { responses =>
                                    if (responses.isEmpty)
                                      logger.info(
                                        s"🔎 ChainSync stuck-detection: no peer had mg=$mg parent=${parentHash.value.take(12)} (will retry after cooldown)"
                                      )
                                    else
                                      responses.traverse_ { resp =>
                                        val mb = pb.MetagraphBinary(address = mg.value.value, binary = resp.signedBinary)
                                        // Re-feed through the SAME gossip entry point — goes through the
                                        // committee gate + drains buffered children on admit.
                                        handleMetagraphBinary(
                                          mb,
                                          processOrphanedMetagraphBinary,
                                          logger
                                        ) *>
                                          // GENESIS-BRIDGE (#28): ALSO feed the shard buffer, mirroring the gossip
                                          // intake (the `MetagraphBinary` handler buffers for the shard right after
                                          // `handleMetagraphBinary`). ChainSync recovers binaries whose gossip
                                          // broadcast raced gl0 readiness — notably a FRESH metagraph's genesis-full
                                          // `CurrencySnapshot` (sent once, un-retried, before gl0's head was ready, so
                                          // `StateChannelRoutes` returned ServiceUnavailable and never re-broadcast it
                                          // onto the topic that feeds the shard buffer). Without this the shard
                                          // checkpoint's `includedSnapshots` is incremental-only → the gl0 adopt path
                                          // `deriveAdoptedCurrencyState` cannot seed a fresh MG's currency (the
                                          // genesis-FULL is mandatory as the chain head) → `lastCurrencySnapshots`
                                          // freezes at genesis → cl1's first-currency-snapshot bootstrap 90s-times-out
                                          // → L0-token transfers fail. Buffering the ChainSync-fetched genesis here
                                          // lands it in the LEADER's checkpoint candidate (the leader ChainSyncs it via
                                          // its own orphan stuck-detection); followers verify+attest the candidate.
                                          // Split-safe: the genesis rides in the committee-SIGNED checkpoint, never a
                                          // node-local read. Dynamic: fires for any metagraph onboarding (e2e-genesis
                                          // OR a new metagraph joining a running gl0), keyed on "genesis binary fetched".
                                          Async[F].whenA(shardBinaryBuffers.nonEmpty)(
                                            bufferReceivedBinaryForShard(mb, shardBinaryBuffers, shardAssignment, logger)
                                          )
                                      }
                                  }
                              }
                        }
                      }
                    }
                  }
                }
                .handleErrorWith { e =>
                  // Never let a recovery-tick failure kill the daemon; log and restart the ticker.
                  fs2.Stream.eval(
                    logger.warn(s"ChainSync stuck-detection tick error: ${e.getMessage}. Restarting in 30s...")
                  ) ++ fs2.Stream.sleep_[F](30.seconds) ++ stuckDetectionStream
                }

            // ─── Shard-checkpoint chain-sync: T2 absence detection (run-20, task #A) ───────────────────────────
            // Per active shard, every `absenceTickIntervalMs`, if the local tip has not advanced for ≥ `stuckMs`
            // (an EMPTY store counts as stuck at ordinal 0), PULL `tip+1` from a peer (empty ⇒ ordinal 1, the
            // genesis-miss case) and re-feed it through the normal accept path. This is what gossip alone cannot
            // do: a missed checkpoint can't be re-gossip-recovered (Tier-1 re-publishes identical bytes that
            // GossipSub dedups), so it waited minutes for the seen-cache TTL — the run-20 stall. The fetcher's
            // own `(shard, ordinal)` cooldown rate-limits re-requests, so firing `due` every tick is safe.
            def shardAbsenceStream: fs2.Stream[F, Unit] =
              (shardAcceptanceDeps, shardCheckpointFetcher) match {
                case (Some(deps), Some(fetcher)) if deps.registry.nonEmpty =>
                  val cfg = deps.shardingConfig.checkpoint
                  fs2.Stream
                    .eval(Ref.of[F, Map[io.constellationnetwork.schema.sharding.ShardId, (Long, Long)]](Map.empty))
                    .flatMap { stallRef =>
                      fs2.Stream.fixedRate[F](cfg.absenceTickIntervalMs.millis).evalMap { _ =>
                        Async[F].realTimeInstant.map(_.toEpochMilli).flatMap { now =>
                          deps.registry.toList.traverse_ {
                            case (shardId, entry) =>
                              // Track the ADOPTED watermark (NOT the local tip): the run-22 stall was adopted frozen at
                              // 51 while the node kept producing its own tip to 53 — a tip-based trigger never fires
                              // there. When the adopted watermark stalls for `stuckMs`, PULL `adopted+1` from a peer (the
                              // canonical checkpoint reaching quorum elsewhere) so this node reorgs onto it and quorum
                              // concentrates. Handles BOTH cases: genesis-miss (adopted=0 ⇒ pull ord 1) and the
                              // cross-node convergence stall (adopted=51 ⇒ pull 52). `entry` retained for symmetry.
                              val _ = entry
                              deps.acceptanceManager.lastAdoptedOrd(shardId).flatMap { adoptedOpt =>
                                val curOrd = adoptedOpt.map(_.value).getOrElse(0L)
                                stallRef.modify { m =>
                                  val (lastOrd, lastAdvance) = m.getOrElse(shardId, (curOrd, now))
                                  if (curOrd > lastOrd) (m.updated(shardId, (curOrd, now)), false) // advanced — reset
                                  else (m.updated(shardId, (lastOrd, lastAdvance)), (now - lastAdvance) >= cfg.stuckMs)
                                }.flatMap { due =>
                                  Async[F].whenA(due) {
                                    val nextOrd = io.constellationnetwork.schema.sharding.ShardOrdinal(curOrd + 1L)
                                    Async[F]
                                      .start(
                                        fetcher.fetchByOrdinal(shardId, nextOrd).flatMap {
                                          case Some(signed) =>
                                            io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                              .signedShardCheckpointToWire[F](signed)
                                              .flatMap(w =>
                                                handleShardCheckpoint(
                                                  w,
                                                  shardAcceptanceDeps,
                                                  shardCheckpointAttestationEmitter,
                                                  shardCheckpointFetcher,
                                                  selfId,
                                                  logger
                                                )
                                              )
                                          case None => Async[F].unit
                                        }
                                      )
                                      .void
                                  }
                                }
                              }
                          }
                        }
                      }
                    }
                    .handleErrorWith { e =>
                      fs2.Stream.eval(
                        logger.warn(s"shard absence-detection tick error: ${e.getMessage}. Restarting in 30s...")
                      ) ++ fs2.Stream.sleep_[F](30.seconds) ++ shardAbsenceStream
                    }
                case _ => fs2.Stream.empty
              }

            gossipStream.concurrently(stuckDetectionStream).concurrently(shardAbsenceStream)
          }
      }
    }
  }

  private def handleSnapshot[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ (Tier-2 vs Tier-3 gap boundary). Forwarded from `run`; sourced from
    // `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` (replaces the prior module-level sys.env read).
    confirmationDepthK: Long,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-staged accumulator
    // stripped->canonical and thus promotes on finalize (complete served ring; was per-producer-sparse).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    // §1.2 Slice 5/6/9: KES infrastructure for sender-side signing + receiver-side load-bearing verify.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    // Hierarchical-shard-checkpoints v1 — per-ord producer fan-out, threaded through to
    // `processValidSnapshotInner`'s becameBestTip branch. EMPTY / `None` at numShards=1 (regression bar).
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers — the producer fan-out input (the inversion).
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded through to `processValidSnapshotInner`'s fan-out gate.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    for {
      producerShort <- Async[F].pure(snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString)
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} parentSlot=${snap.parentSlot} from=$producerShort"
      )

      // Per-producer counter for cross-peer eligibility / dominance analysis. Tagged
      // with the 8-char producer prefix so it can be queried as
      // `topk(N, dag_nakamoto_snapshots_received_by_producer) by (producer_id)`.
      _ <- Metrics[F].incrementCounter(
        "dag_nakamoto_snapshots_received_by_producer",
        Seq(Metrics.unsafeLabelName("producer_id") -> producerShort)
      )

      // Deserialize payload
      parsed =
        if (snap.payload.size() > 0) {
          val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
          (for {
            json <- io.circe.parser.parse(payloadStr)
            snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
            contextJson <- json.hcursor.get[io.circe.Json]("context")
            snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
            context <- contextJson.as[GlobalSnapshotInfo]
          } yield (snapshot, context)).toOption
        } else None

      genesisEta <- epochStateRef.get.map(_.genesisEta)
      // Use parentSlot from gossip message for gap (same inputs as producer used)
      slotGap = snap.slot - snap.parentSlot
      parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))

      // Chain-derived eta: must be computed from the INCOMING snapshot's chain,
      // not our local best tip. The producer used eta derived from its own chain
      // (walking back from its parent). If we use our local tip's chain, we'll
      // compute a different eta when forks diverge → VRF verification fails.
      //
      // Strategy: walk from the incoming snapshot's parentHash.
      // Fallback: if the incoming snapshot carries an eta field, use it directly
      // (trust-but-verify: we verify the snapshot's chain ancestry separately).
      //
      // Rotation period is keyed on the **predecessor ordinal** (`snap.ordinal - 1`),
      // mirroring the producer side at `SnapshotLeaderLoop:281` where `lastChainOrdinal`
      // (= bestTipOrdinal at production time, = N-1 for snap N) is the divisor input.
      // Period assignment must be a function of the snapshot itself so every honest
      // verifier reaches the same conclusion regardless of where their local bestTip is.
      // Using `snap.ordinal` directly causes an off-by-one at every R boundary: the
      // producer of ord=R uses (R-1)/R = period (R-1)/R → 0, but a naive verifier
      // computes R/R = 1, expecting derived eta where the SlotCertificate carries
      // genesisEta. This caused iter35's finalization stall at ord=99 (R=100 boundary).
      // See `docs/nakamoto/attestation-and-finality.md` §1.
      currentPeriod = EtaCalculation.rotationPeriod(math.max(0L, snap.ordinal - 1), etaRotationSnapshots)
      eta <-
        if (currentPeriod <= 0) {
          Async[F].pure(genesisEta)
        } else {
          chainStore.vrfOutputsForPeriodFrom(currentPeriod - 1, etaRotationSnapshots, parentHash).flatMap { chainOutputs =>
            if (chainOutputs.nonEmpty) {
              Async[F].pure(EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2)))
            } else {
              // Parent chain not in our store (different fork or pruned).
              // Fall back to the eta embedded in the snapshot itself.
              // This is safe: VRF proof verification ensures the producer
              // was eligible under THIS eta, and content validation later
              // ensures the snapshot's state transitions are correct.
              parsed.flatMap(_._1.value.eta) match {
                case Some(etaHash) =>
                  // Hash wraps a hex string — decode to 32 bytes
                  val hexStr = etaHash.value
                  val decoded = hexStr.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
                  logger
                    .info(
                      s"Using embedded eta from snapshot (parent chain not in store, period=$currentPeriod, eta=${hexStr.take(16)}..)"
                    )
                    .as(decoded)
                case None =>
                  // No embedded eta and no chain data — use genesis
                  Async[F].pure(genesisEta)
              }
            }
          }
        }

      // Validation pipeline + chainStore.store. Pre-Phase E this body ran inside
      // `mptStore.withTransaction` so `accept()`'s mid-flight MPT mutations could be
      // rolled back when the snapshot didn't become the new bestTip. Phase D
      // (commit caa3559e) routed `accept()` through the overlay algebra
      // (`overlay.checkout` / `overlay.commit`); under `MptOverlay.OverlayMode.MultiBranch`
      // each download lands in its own pending `ChangeSet` — non-canonical branches sit
      // in pending until eviction (#56.9) drops them or `finalizeBranch` promotes the
      // canonical chain. The savepoint bracket is obsolete and removed here: the
      // overlay's branching IS the rollback. Under the current `Passthrough` wiring,
      // writes still land in base immediately and we lose the daemon-side rollback
      // semantics — that's intentional migration scaffolding, byte-equivalent to the
      // legacy path under #107's parity gate. The proper isolation arrives when
      // Phase J flips production wiring to `MultiBranch`.
      validationResult <- parsed match {
        case Some((signedSnapshot, context)) =>
          chainStore.get(parentHash).flatMap {
            case Some(parentStored) =>
              NakamotoSnapshotValidator.validate[F](
                signedSnapshot = signedSnapshot,
                context = context,
                slot = snap.slot,
                vrfProof = snap.vrfProof.toByteArray,
                vrfPublicKey = snap.vrfPublicKey.toByteArray,
                producerIdBytes = snap.producerId.toByteArray,
                eta = eta,
                slotGap = slotGap,
                stakeRegistry = stakeRegistry,
                lddConfig = lddConfig,
                eligibilityChecker = eligibilityChecker,
                consensusFns = consensusFns,
                lastSignedArtifact = parentStored.signedSnapshot,
                lastContext = parentStored.context,
                getByOrdinal = { (ordinal: SnapshotOrdinal) =>
                  snapshotStorage.get(ordinal).flatMap {
                    case Some(s) => HasherSelector[F].withCurrent(implicit h => s.toHashed[F].map(_.some))
                    case None =>
                      chainStore.getByOrdinal(ordinal.value.value).flatMap {
                        case Some(stored) =>
                          HasherSelector[F].withCurrent(implicit h => stored.signedSnapshot.toHashed[F].map(_.some))
                        case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                      }
                  }
                },
                mptOverlay = mptOverlay,
                pendingAccumulatorsRef = pendingAccumulatorsRef
              )
            case None =>
              // Parent not in chain store. Three-tier gap handling:
              // Tier 1 (<=6): buffer + ChainSync parent fetch (normal gossip latency)
              // Tier 2 (>6, <=k): sequential walk-back via ChainSync (moderate drift)
              // Tier 3 (>k): full catch-up — network finalized past us
              chainStore.bestTipOrdinal.flatMap { localBestOrdinal =>
                val localOrd = localBestOrdinal.getOrElse(0L)
                val gap = snap.ordinal - localOrd
                if (gap > confirmationDepthK) {
                  logger.warn(
                    s"🔄 Tier 3: gap=$gap > k=$confirmationDepthK for ordinal=${snap.ordinal}. Triggering full catch-up."
                  ) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentNotFound: NakamotoSnapshotValidator.ValidationResult
                    )
                } else if (gap > CatchUpThreshold) {
                  logger.info(
                    s"⏳ Tier 2: gap=$gap (>$CatchUpThreshold, <=$confirmationDepthK) for ordinal=${snap.ordinal}. " +
                      s"Sequential walk-back from parent ${parentHash.value.take(12)}."
                  ) >>
                    pendingParentRef.update { m =>
                      val existing = m.getOrElse(parentHash, List.empty)
                      m.updated(parentHash, existing :+ snap)
                    } >>
                    chainSyncManager.requestMissing(parentHash) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                    )
                } else {
                  logger.info(
                    s"⏳ Tier 1: Parent ${parentHash.value.take(12)} not in chain store for ordinal=${snap.ordinal} (gap=$gap). Buffering."
                  ) >>
                    pendingParentRef.update { m =>
                      val existing = m.getOrElse(parentHash, List.empty)
                      m.updated(parentHash, existing :+ snap)
                    } >>
                    chainSyncManager.requestMissing(parentHash) >>
                    Async[F].pure(
                      NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                    )
                }
              }
          }
        case None =>
          // No payload — fall back to VRF-only validation (legacy/PoC). No accept() and
          // therefore no MPT mutation.
          val vrfVK = snap.vrfPublicKey.toByteArray
          val proof = snap.vrfProof.toByteArray
          val producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
          val producerId = peer.PeerId(producerHex)
          stakeRegistry.relativeStake(producerId).flatMap { producerStake =>
            val vrfValidF =
              if (vrfVK.isEmpty || proof.isEmpty) false.pure[F]
              else
                eligibilityChecker.verifyEligibility(
                  vrfVK = vrfVK,
                  slot = Slot(NonNegLong.unsafeFrom(snap.slot)),
                  slotGap = slotGap,
                  eta = eta,
                  relativeStake = producerStake,
                  config = lddConfig,
                  proof = proof
                )
            vrfValidF.map { vrfValid =>
              if (vrfValid) NakamotoSnapshotValidator.Valid(null, null) // VRF-only, no snapshot data
              else NakamotoSnapshotValidator.VrfOnlyFailed(snap.slot)
            }
          }
      }
      storeOutcome <- validationResult match {
        case v: NakamotoSnapshotValidator.Valid if v.snapshot != null && v.context != null =>
          for {
            isNew <- chainStore.store(
              v.snapshot,
              v.context,
              snap.ordinal,
              snap.slot,
              parentHash,
              vrfOutputFromProof(snap.vrfProof.toByteArray)
            )
            bestTipOpt <- chainStore.bestTip
            thisHash <- HasherSelector[F].withCurrent(implicit h => v.snapshot.toHashed[F].map(_.hash))
            becameBest = isNew && bestTipOpt.exists(_.hash === thisHash)
            _ <- Async[F].whenA(isNew && !becameBest) {
              logger.info(
                s"🔀 Stored Nakamoto snapshot at ordinal=${snap.ordinal} as fork branch (not bestTip) — overlay isolates pending branches from canonical."
              )
            }
          } yield (becameBest, v.snapshot.some, v.context.some)
        case _ =>
          (false, none[Signed[GlobalIncrementalSnapshot]], none[GlobalSnapshotInfo]).pure[F]
      }
      (becameBest, signedOpt, ctxOpt) = storeOutcome

      _ <- (validationResult: NakamotoSnapshotValidator.ValidationResult) match {
        case NakamotoSnapshotValidator.Valid(_, _) =>
          processValidSnapshot(
            snap,
            signedOpt,
            ctxOpt,
            becameBest,
            stateRef,
            chainStore,
            nodeStorage,
            tipTracker,
            sidecarClient,
            selfId,
            keyPair,
            lastKnownSlotRef,
            epochStateRef,
            etaRotationSnapshots,
            snapshotStorage,
            lastGlobalSnapshotStorage,
            lastNGlobalSnapshotStorage,
            productionGate,
            operationalKeyMaker,
            kesRegistry,
            shardProducers,
            shardChainStores,
            shardBinaryBuffers,
            shardAssignment,
            shardCommitteeMembership,
            logger
          ) >> {
            // This snapshot is now stored — drain any children that were waiting for it.
            val storedHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            drainPendingChildren(
              storedHash,
              stateRef,
              pendingParentRef,
              chainStore,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              sidecarClient,
              selfId,
              keyPair,
              lddConfig,
              eligibilityChecker,
              lastKnownSlotRef,
              epochStateRef,
              etaRotationSnapshots,
              confirmationDepthK,
              consensusFns,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              mptStore,
              mptOverlay,
              pendingAccumulatorsRef,
              eventMempool,
              chainSyncManager,
              channel,
              dataDir,
              operationalKeyMaker,
              kesRegistry,
              shardProducers,
              shardChainStores,
              shardBinaryBuffers,
              shardAssignment,
              shardCommitteeMembership,
              logger
            )
          }
        case NakamotoSnapshotValidator.ParentNotFound =>
          // Parent not found — network is ahead of us (restart scenario).
          catchUpFromGossip(
            snap,
            parsed,
            stateRef,
            chainStore,
            snapshotStorage,
            lastGlobalSnapshotStorage,
            lastNGlobalSnapshotStorage,
            lastKnownSlotRef,
            mptStore,
            eventMempool,
            productionGate,
            channel,
            dataDir,
            logger
          )
        case _: NakamotoSnapshotValidator.ContentMismatch =>
          // Content mismatch — could be normal fork or restart scenario.
          // Only catch up if incoming ordinal is significantly ahead of our canonical tip.
          snapshotStorage.head.flatMap {
            case Some((localTip, _)) =>
              val localOrd = localTip.ordinal.value.value
              val gap = snap.ordinal - localOrd
              if (gap >= CatchUpThreshold) {
                logger.warn(
                  s"\uD83D\uDD04 Content mismatch with ordinal gap=$gap (local=$localOrd, incoming=${snap.ordinal}). Triggering catch-up."
                ) >>
                  catchUpFromGossip(
                    snap,
                    parsed,
                    stateRef,
                    chainStore,
                    snapshotStorage,
                    lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage,
                    lastKnownSlotRef,
                    mptStore,
                    eventMempool,
                    productionGate,
                    channel,
                    dataDir,
                    logger
                  )
              } else {
                // Normal Nakamoto fork — store the snapshot as an alternative branch in
                // chainStore WITHOUT updating canonical state (snapshotStorage,
                // lastGlobalSnapshotStorage, MPT). Content hasn't been validated against
                // this fork's parent state yet — validation is deferred to reorg time.
                //
                // VRF + signature + slot-cert are already validated (proof of eligibility).
                // Content validation (state proof match) requires the fork's parent context
                // which we don't have locally. If ChainSelection later picks this fork as
                // denser (reorg), we validate by triggering catch-up which resets state to
                // the fork's context + MPT self-healing.
                logger.info(
                  s"🔀 Fork at ordinal=${snap.ordinal} slot=${snap.slot} (gap=$gap). Storing as tentative branch (deferred validation)."
                ) >>
                  Metrics[F].incrementCounter("dag_nakamoto_forks_stored") >>
                  storeForkBranch(
                    snap,
                    stateRef,
                    chainStore,
                    tipTracker,
                    snapshotStorage,
                    lastGlobalSnapshotStorage,
                    lastNGlobalSnapshotStorage,
                    lastKnownSlotRef,
                    mptStore,
                    mptOverlay,
                    eventMempool,
                    productionGate,
                    logger
                  )
              }
            case None =>
              catchUpFromGossip(
                snap,
                parsed,
                stateRef,
                chainStore,
                snapshotStorage,
                lastGlobalSnapshotStorage,
                lastNGlobalSnapshotStorage,
                lastKnownSlotRef,
                mptStore,
                eventMempool,
                productionGate,
                channel,
                dataDir,
                logger
              )
          }
        case NakamotoSnapshotValidator.ParentBuffered =>
          // Already buffered for validation when parent arrives — nothing more to do
          Async[F].unit
        case invalid: NakamotoSnapshotValidator.Invalid =>
          Metrics[F].incrementCounter("dag_nakamoto_snapshots_rejected") >>
            logger.warn(s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal}: $invalid")
      }
    } yield ()

  /** Process a VRF-validated snapshot AFTER the validate+chainStore.store bracket has decided whether to commit MPT.
    *
    *   - `signedSnapshot`/`context` are `None` for VRF-only payloads (no body to thread through canonical state).
    *   - `becameBestTip` gates the canonical storage updates (snapshotStorage / last*Snapshot / lastKnownSlot). When false, the snapshot
    *     was stored as a fork branch (or duplicate) in chainStore and MPT was rolled back; we still update tip-tracking, attestation, and
    *     ready-transition.
    */
  private def processValidSnapshot[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    signedSnapshot: Option[Signed[GlobalIncrementalSnapshot]],
    context: Option[GlobalSnapshotInfo],
    becameBestTip: Boolean,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    // Hierarchical-shard-checkpoints v1 — per-ord producer fan-out (becameBestTip-gated). EMPTY / `None`
    // at numShards=1 ⇒ the fan-out is `whenA(false)` in `processValidSnapshotInner`.
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers — the producer fan-out input (the inversion).
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded into `processValidSnapshotInner`'s producer fan-out gate.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    // Slice S6: the producer shard-checkpoint fan-out is moved OFF the snapshot-processing
    // critical path (it's launched on this app-scoped Supervisor inside `processValidSnapshotInner`,
    // not run inline under `snapshotSemaphore.permit`). Implicit so the existing positional call
    // site needs no change — it resolves from `run`'s implicit `Supervisor[F]`.
    implicit supervisor: Supervisor[F]
  ): F[Unit] = {
    // §1.2 Slice 9: snapshot KES gate runs BEFORE any state mutation. On no-sig /
    // decode-fail / verify-fail / step-out-of-range, drop the snapshot entirely — no
    // networkTip update, no canonical-storage write, no self-attestation emit. The
    // no-registry-entry carve-out accepts so Ed25519-authenticated peers awaiting
    // their Slice 10 (#179) reg-cert finality aren't silently dropped.
    val kesGate: F[Boolean] = KesGossipVerification.verifySnapshot(
      messageBytes = snap.hash.toByteArray,
      kesSigBytes = snap.kesSignature.toByteArray,
      producerId = peer.PeerId(Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)),
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString),
      ordinal = snap.ordinal,
      kesRegistry = kesRegistry,
      etaRotationSnapshots = etaRotationSnapshots,
      logger = logger
    )
    kesGate.flatMap { kesOk =>
      if (!kesOk)
        logger.warn(s"⚠️ KES gate dropped snapshot ord=${snap.ordinal} — no state mutation").as(())
      else
        processValidSnapshotInner(
          snap,
          signedSnapshot,
          context,
          becameBestTip,
          stateRef,
          chainStore,
          nodeStorage,
          tipTracker,
          sidecarClient,
          selfId,
          keyPair,
          lastKnownSlotRef,
          epochStateRef,
          etaRotationSnapshots,
          snapshotStorage,
          lastGlobalSnapshotStorage,
          lastNGlobalSnapshotStorage,
          productionGate,
          operationalKeyMaker,
          kesRegistry,
          shardProducers,
          shardChainStores,
          shardBinaryBuffers,
          shardAssignment,
          shardCommitteeMembership,
          logger
        )
    }
  }

  private def processValidSnapshotInner[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    signedSnapshot: Option[Signed[GlobalIncrementalSnapshot]],
    context: Option[GlobalSnapshotInfo],
    becameBestTip: Boolean,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING R-1: per-shard raw-binary buffers — the producer fan-out input (the inversion).
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw — the producer fan-out runs for a shard only if this node is in its
    // committee (membership gate in `ShardCheckpointFanOut.run`). The SAME draw the verifier admits against.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    // Slice S6: app-scoped Supervisor for the off-critical-path producer fan-out (see below).
    implicit supervisor: Supervisor[F]
  ): F[Unit] =
    for {
      // Update network tip tracking
      _ <- stateRef.update { s =>
        if (snap.ordinal > s.networkTipOrdinal)
          s.copy(
            networkTipOrdinal = snap.ordinal,
            networkTipHash = Some(Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))
          )
        else s
      }

      // Canonical-state updates only when this snapshot became the bestTip in the chainStore.
      // After Phase E, the validate+store path no longer wraps MPT writes in a savepoint
      // bracket — accept() routes through the overlay algebra (Phase D, commit caa3559e),
      // so the MPT mutations for this snapshot are already in flight (Passthrough → base
      // immediately; MultiBranch → pending ChangeSet under the parent's branch). On the
      // fork-branch path we still must NOT update canonical storage; the overlay's
      // pending isolation (or the eventual finalizeBranch promotion) handles state.
      _ <- (signedSnapshot, context) match {
        case (Some(signed), Some(ctx)) if becameBestTip =>
          productionGate.pause(ProductionGate.ReorgInProgress) >>
            HasherSelector[F].withCurrent { implicit hasher =>
              signed.toHashed[F].flatMap { hashed =>
                snapshotStorage.setHeadForRecovery(signed, ctx) >>
                  lastGlobalSnapshotStorage.setForRecovery(hashed, ctx) >>
                  lastNGlobalSnapshotStorage.setForRecovery(hashed, ctx) >>
                  logger.debug(s"Updated canonical storage to ordinal=${snap.ordinal} slot=${snap.slot}") >>
                  Metrics[F].incrementCounter("dag_nakamoto_snapshots_received") >>
                  Metrics[F].updateGauge("dag_nakamoto_ordinal", snap.ordinal) >>
                  Metrics[F].recordDistribution("dag_nakamoto_slot_gap", (snap.slot - snap.parentSlot).toInt)
              }
            } >>
            chainStore.bestTipSlot.flatMap {
              case Some(bestSlot) => lastKnownSlotRef.set(Some(bestSlot))
              case None           => Async[F].unit
            } >>
            productionGate.resume(ProductionGate.ReorgInProgress)
        case _ => Async[F].unit
      }

      // ─── Shard-checkpoint fan-out: MOVED to the per-slot tick in SnapshotLeaderLoop (design §5.7,
      // 2026-06-12). The becameBestTip hook that lived here was the second half of the run-10 Gap-A
      // cadence inversion: it sampled the shard-leader lottery once per canonical gl0 ORD (the gl0-leader
      // onSlotWon self-call was the first half), so shards drew ~6.5× slower than gl0's slot grid. The
      // lottery now draws every wall-clock slot on every node, with the envelope carrying its production
      // `slot` (signed). Nothing to do on the snapshot-receive path.

      // Record in TipTracker (snapshot producer attests to their own tip).
      // `attestedAt` is OUR wall-clock receive time (epoch ms via Clock[F].realTime,
      // NOT System.currentTimeMillis()) — so TipTracker's "newer wins" rule sees
      // the latest arrival from each peer. The unit is wall-clock millis, not a
      // consensus slot — kept open as the future Ouroboros-Chronos timestamp
      // gossip surface, per `docs/nakamoto/attestation-and-finality.md`.
      tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
      tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
      producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
      producerId = peer.PeerId(producerHex)
      nowMs <- Clock[F].realTime.map(_.toMillis)
      att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, nowMs)
      // The producer-implicit attestation uses OUR local clock for `attestedAt`, so the
      // skew gate (`TipTracker.MaxAttestationSkewMs`) is trivially satisfied here.
      _ <- tipTracker.recordAttestation(producerId, att, nowMs)

      // Check if we should transition to Ready
      state <- stateRef.get
      nodeState <- nodeStorage.getNodeState
      _ <- Async[F].whenA(!state.isReady && nodeState =!= NodeState.Ready) {
        val caughtUp = state.networkTipOrdinal - snap.ordinal <= CatchUpThreshold
        Async[F].whenA(caughtUp) {
          logger.info(s"Caught up (local=${snap.ordinal}, network=${state.networkTipOrdinal}). → Ready.") >>
            stateRef.update(_.copy(isReady = true, localTipOrdinal = snap.ordinal)) >>
            nodeStorage.setNodeState(NodeState.Ready) >>
            logger.info(s"🟢 Node Ready — VRF production begins")
        }
      }

      // Emit OUR attestation only when chain-selection promoted this peer snapshot
      // to our local bestTip (a Phase 0 → 1 transition for us, per
      // `docs/nakamoto/attestation-and-finality.md` §0/§4). The earlier
      // unconditional emit (commit `6e49b7d5`) moved selfId.tipHash onto any
      // arriving fork-branch snap, which the canonical-hash filter in
      // `TipTracker.highestFinalizedOrdinal` then zeroed out — disenfranchising
      // us on our own canonical chain. The 5s re-attestation ticker
      // (`SnapshotLeaderLoop.scala:396-412`) remains as a safety net for the
      // case where bestTip flips between this branch and the ticker firing.
      //
      // The producer-implicit attestation above (recordAttestation for the
      // producer's `peerId`) stays unconditional — it credits the producer
      // for what they produced, independent of our chain selection.
      //
      // `attestedAt` is wall-clock epoch ms via `Clock[F].realTime` (NOT
      // `System.currentTimeMillis()`, NOT a consensus slot). Wall-clock
      // semantics are deliberate, keeping the field reusable as a future
      // Ouroboros-Chronos-style timestamp-claim surface.
      _ <- Async[F].whenA(becameBestTip) {
        Clock[F].realTime.map(_.toMillis).flatMap { attestedAt =>
          emitAttestation(
            snap,
            attestedAt,
            sidecarClient,
            tipTracker,
            selfId,
            keyPair,
            operationalKeyMaker,
            etaRotationSnapshots,
            logger
          )
        }
      }

      // §1.2 Slice 9: KES verification of the incoming snapshot's `kes_signature` field is
      // run BEFORE this body via `processValidSnapshot`'s outer `kesGate`. By the time we
      // reach here the gate has already passed (otherwise the snapshot was rejected and
      // this code is unreachable).

    } yield ()

  /** Store a fork-branch snapshot WITHOUT updating canonical state.
    *
    * The snapshot has valid VRF + signature + slot-cert but its content doesn't match our local state (it was built on a different fork).
    * We store it tentatively in the chain store. If ChainSelection picks this fork as denser (reorg), we trigger catch-up to adopt the new
    * state — this resets our canonical view to the fork's context and performs MPT self-healing, which is the deferred validation step.
    *
    * Stale fork branches are pruned when finality advances past them (chainStore.finalize).
    */
  private def storeForkBranch[F[
    _
  ]: Async: cats.Parallel: HasherSelector: io.constellationnetwork.json.JsonSerializer: Metrics: SecurityProvider](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    productionGate: ProductionGate[F],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[Unit] =
    if (snap.payload.size() == 0) Async[F].unit
    else {
      val payloadStr = snap.payload.toByteArray.map(_.toChar).mkString
      val result = for {
        json <- io.circe.parser.parse(payloadStr)
        snapshotJson <- json.hcursor.get[io.circe.Json]("snapshot")
        contextJson <- json.hcursor.get[io.circe.Json]("context")
        snapshot <- snapshotJson.as[Signed[GlobalIncrementalSnapshot]]
        context <- contextJson.as[GlobalSnapshotInfo]
      } yield (snapshot, context)

      result match {
        case Right((signedSnapshot, context)) =>
          val parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
          for {
            // Record the previous best tip so we can detect reorgs
            prevBestTip <- chainStore.bestTip.map(_.map(_.hash))

            // Store in chainStore — ChainSelection may switch bestTip if this fork is denser
            isNew <- chainStore.store(
              signedSnapshot,
              context,
              snap.ordinal,
              snap.slot,
              parentHash,
              vrfOutputFromProof(snap.vrfProof.toByteArray)
            )

            // Check if a reorg happened (bestTip changed to a different chain)
            newBestTip <- chainStore.bestTip.map(_.map(_.hash))
            reorgHappened = isNew && prevBestTip =!= newBestTip

            _ <-
              if (reorgHappened) {
                // ChainSelection picked this fork — adopt its state via catch-up.
                // This is the deferred validation: catch-up resets canonical state to the fork's
                // context and performs MPT self-healing. Subsequent snapshots that build on the
                // new canonical tip will go through the normal Valid path (full content validation).
                // At depth < k1 the chosen rule is Taktikos `maxvalid-tk` (length, then lower
                // head-slot tiebreaker); the Genesis density rule fires only on deep forks (>=k1).
                logger.info(
                  s"🔄 Reorg to fork at ordinal=${snap.ordinal} slot=${snap.slot} (ChainSelection.standardCompare picked it: maxvalid-tk for shallow forks, density only fires at depth>=k1). " +
                    s"prevBestTip=${prevBestTip.map(_.value.take(12)).getOrElse("none")} newBestTip=${newBestTip.map(_.value.take(12)).getOrElse("none")}. Validating via catch-up."
                ) >>
                  productionGate.pause(ProductionGate.ReorgInProgress) >>
                  HasherSelector[F].withCurrent { implicit hasher =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      snapshotStorage.setTentativeHead(signedSnapshot, context) >>
                        lastGlobalSnapshotStorage.setForRecovery(hashed, context) >>
                        lastNGlobalSnapshotStorage.setForRecovery(hashed, context)
                    }
                  } >>
                  // MPT self-healing: rebuild from the fork's state.
                  //
                  // Before re-syncing the underlying base store from the fork's GlobalSnapshotInfo,
                  // we MUST clean up the overlay's pending/finalized refs. `syncFromGlobalSnapshotInfo`
                  // operates on the base `MptStore` directly (clears + repopulates) and bypasses the
                  // overlay entirely. Without this `finalizeBranch` call the overlay's `pendingRef`
                  // would still hold orphan local-fork branches whose deltas were built against the
                  // pre-reorg base — those branches show up in `bestTipsFn`, get marked protected by
                  // `walkAncestorsInPending`, and block `evictIfOverCap` from making progress
                  // (pendingRef grows unboundedly past cap, see #116 iter14 forensics).
                  //
                  // The reorg-replace `case None` arm in `MptOverlay.finalizeBranch` handles the
                  // expected case here ("canonical not in pending, locally-rejected fork branches
                  // still resident"): it clears `pendingRef`, resets `lastCommittedBranchRef`, and
                  // emits a WARN that the caller is responsible for base resync (which is exactly
                  // what we do next via `syncFromGlobalSnapshotInfo`).
                  HasherSelector[F].withCurrent { implicit hasher =>
                    signedSnapshot.toHashed[F].flatMap { hashed =>
                      mptOverlay
                        .finalizeBranch(
                          io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(hashed.hash),
                          SnapshotOrdinal.unsafeApply(snap.ordinal)
                        )
                        .void >>
                        mptStore.syncFromGlobalSnapshotInfo(context, SnapshotOrdinal.unsafeApply(snap.ordinal))
                    }
                  } >>
                  // Reconcile event mempool — evict stale DAG blocks, keep unconfirmed.
                  reconcileMempool(eventMempool, context, logger) >>
                  chainStore.bestTipSlot.flatMap {
                    case Some(bestSlot) => lastKnownSlotRef.set(Some(bestSlot))
                    case None           => Async[F].unit
                  } >>
                  productionGate.resume(ProductionGate.ReorgInProgress) >>
                  Metrics[F].incrementCounter("dag_nakamoto_reorgs")
              } else if (isNew) {
                // Fork stored but not canonical — just log
                Metrics[F].incrementCounter("dag_nakamoto_forks_stored")
              } else Async[F].unit

            // Record attestation regardless (producer attests their own tip). `attestedAt`
            // is OUR wall-clock receive time via Clock[F].realTime — same Chronos-prep
            // semantics as the becameBestTip-branch site above. Skew gate trivially
            // passes since `attestedAt` and `now` are both this node's local clock.
            tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
            producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)
            producerId = peer.PeerId(producerHex)
            nowMs <- Clock[F].realTime.map(_.toMillis)
            att = DomainTipAttestation(tipHash, tipSlot, snap.ordinal, nowMs)
            _ <- tipTracker.recordAttestation(producerId, att, nowMs)
          } yield ()

        case Left(err) =>
          logger.warn(s"⚠️ Failed to deserialize fork-branch payload: ${err.getMessage}")
      }
    }

  private def handleAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // tipHash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(att.tipHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(att.tipSlot))
    val attesterHex = Hex(att.attesterId.toByteArray.map("%02x".format(_)).mkString)
    val attesterId = peer.PeerId(attesterHex)
    val sigBytes = att.signature.toByteArray
    val kesSigBytes = att.kesSignature.toByteArray
    // `att.attestedAt` is wall-clock epoch ms set by the peer (Clock[F].realTime).
    // Used purely for "newer-wins" ordering in TipTracker; no slot interpretation.
    val domainAtt = DomainTipAttestation(tipHash, tipSlot, att.tipOrdinal, att.attestedAt)

    if (sigBytes.isEmpty) {
      logger.warn(s"⚠️ Rejecting unsigned attestation for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}...")
    } else {
      // Verify signature using the same Hasher pipeline: JSON-encode the domain TipAttestation → hash → verify
      HasherSelector[F].withCurrent { implicit hasher =>
        for {
          attHash <- domainAtt.hash
          publicKey <- attesterHex.toPublicKey[F]
          valid <- Signing.verifySignature(attHash.getBytes, sigBytes)(publicKey)
          // Chronos-prep: capture OUR local clock when the peer's attestation lands so
          // `TipTracker.recordAttestation` can compare against the peer's claimed
          // `attestedAt`. Badly-skewed peers (or attackers forging timestamps) are
          // dropped inside the tracker before they pollute `T_count` finality (#136).
          nowMs <- Clock[F].realTime.map(_.toMillis)
          _ <-
            if (valid)
              // §1.2 Slice 9: KES verification runs BEFORE TipTracker.recordAttestation so we
              // can drop the attestation without un-recording it. Returns false on no-sig /
              // decode-fail / verify-fail / step-out-of-range; the no-registry-entry carve-out
              // returns true so Ed25519-authenticated peers that haven't registered yet
              // (Slice 10 mid-life joiners) aren't silently dropped.
              KesGossipVerification
                .verifyAttestation(
                  messageBytes = attHash.getBytes,
                  kesSigBytes = kesSigBytes,
                  attesterId = attesterId,
                  attesterHex = attesterHex,
                  tipOrdinal = att.tipOrdinal,
                  kesRegistry = kesRegistry,
                  etaRotationSnapshots = etaRotationSnapshots,
                  logger = logger
                )
                .flatMap { kesOk =>
                  if (kesOk)
                    tipTracker.recordAttestation(attesterId, domainAtt, nowMs) >>
                      logger.info(
                        s"📨 Attestation for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}..."
                      )
                  else
                    Async[F].unit // rejection already logged inside verifyAttestation
                }
            else
              logger.warn(
                s"⚠️ Rejecting attestation with invalid signature for ordinal=${att.tipOrdinal} from=${attesterHex.value.take(16)}..."
              )
        } yield ()
      }
    }
  }

  /** Route an incoming AllowSpendBlock from gossip into the same `l1AllowSpendOutput` queue the (now-removed) HTTP POST endpoint populated.
    * Sender serializes `Signed[AllowSpendBlock]` via JsonSerializer in `Swap.sendBlockToL0`; we use the same typeclass to deserialize.
    * Errors (decode failure) are logged and swallowed — gossip is fire-and-forget. (#196)
    */
  private def handleAllowSpendBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    asb: pb.AllowSpendBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.swap.AllowSpendBlock
    import io.constellationnetwork.security.signature.Signed

    val bytes = asb.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[AllowSpendBlock]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting allow-spend-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueAllowSpendBlock failed: ${e.getMessage}"))
      }
  }

  /** Route an incoming DAG block from gossip into the same `l1Output` queue the (now-removed) HTTP POST endpoint populated. The
    * DAGBlockRoutes pipeline ran through L0Cell.processDAGL1 → EnqueueDAGL1Data → queue.offer; that path was a pure pass-through with no
    * extra validation, so the new gossip path enqueues directly. Sender serializes `Signed[Block]` via JsonSerializer in
    * `StateChannel.sendBlockToL0`; we use the same typeclass to deserialize. Errors are logged and swallowed — gossip is fire-and-forget.
    * (#196 follow-up)
    */
  private def handleDAGBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    blk: pb.DAGBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.Block
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.Block
    import io.constellationnetwork.security.signature.Signed

    val bytes = blk.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[Block]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting dag-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueDAGBlock failed: ${e.getMessage}"))
      }
  }

  /** Route an incoming TokenLockBlock from gossip into the same `l1TokenLockOutput` queue the (now-removed) HTTP POST endpoint populated.
    * Sender serializes `Signed[TokenLockBlock]` via JsonSerializer in `TokenLock.sendBlockToL0`; we use the same typeclass to deserialize.
    * Errors are logged and swallowed — gossip is fire-and-forget. (#196 follow-up)
    */
  private def handleTokenLockBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    blk: pb.TokenLockBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.tokenLock.TokenLockBlock
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.tokenLock.TokenLockBlock
    import io.constellationnetwork.security.signature.Signed

    val bytes = blk.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[TokenLockBlock]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting token-lock-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueTokenLockBlock failed: ${e.getMessage}"))
      }
  }

  /** Slice S3 receiver: route an incoming `pb.MetagraphAttestation` into the [[MetagraphCommitteeGate]] verifier + tally. Verification
    * failures are logged inside the gate; this function only translates the proto wire shape into the gate's `IncomingAttestation` domain
    * value and looks up the canonical eta for the sender's metagraph-parent ordinal.
    *
    * The proto fields map 1:1 to `IncomingAttestation`:
    *   - `att.peerId` (UTF-8 hex bytes) → `senderPeerId`
    *   - `att.vrfPublicKey` (32 bytes) → `senderVrfVk`
    *   - `att.metagraphAddress` (DAG base58 string) → `metagraphAddress`
    *   - `att.parentHash` (UTF-8 bytes of canonical Hash hex) → `parentHash`
    *   - `att.binaryHash` (UTF-8 bytes of canonical Hash hex) → `binaryHash`
    *   - `att.committeeVrfProof` → `committeeVrfProof`
    *   - `att.signature` → `longTermSignature`
    *   - `att.kesSignature` → `kesSignature`
    *   - `att.senderTreeStep` (uint32 wire field) → `senderTreeStep`
    *
    * '''Why `vrf_public_key` is on the wire.''' The receiver can't re-derive the sender's VRF VK from `senderPeerId` alone: `VrfKeyDeriver`
    * needs the sender's PRIVATE key. The S2/S3 wire format carries the sender's VRF VK directly so verification works for any operator the
    * receiver hasn't yet observed. The gate's committee-VRF verifier checks `proof` against this VK; if the published VK is not the
    * sender's true VK, the proof fails to verify and the attestation is dropped. When per-operator-key VRF keys land (#180), the field
    * plumbs through unchanged — only the sender's source-of-VK shifts to the registration table.
    */
  private def handleMetagraphAttestation[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector](
    att: pb.MetagraphAttestation,
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    // #213/#290 content-derived parent-ordinal resolver — `(mg, parentHash, binaryContent)`. Unused in this
    // handler since #29 (verify-on-attach): the receiver now buffers attestations un-verified and the admit
    // path (`makeMetagraphBinaryProcessor`) resolves the ordinal + verifies them at attach time. Retained for
    // signature parity with that processor; drop along the call chain in a later cleanup.
    parentOrdinalFor: (
      io.constellationnetwork.schema.address.Address,
      io.constellationnetwork.security.hash.Hash,
      Array[Byte]
    ) => F[Option[Long]],
    etaForParentOrdinal: Long => F[Array[Byte]],
    senderStakeLookup: peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    // #213/#290: holds the inbound metagraph binaries (keyed by their parentHash) the local gate could
    // not yet admit — exactly the binaries inbound attestations are about. The receiver looks the
    // attested binary up here by its wire digest (`att.binaryHash`) to obtain its content, then derives
    // the parent ordinal from THAT content (never from the sender's claim). Fail-closed if absent.
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.IncomingAttestation
    import eu.timepit.refined.refineV

    refineV[DAGAddressRefined](att.metagraphAddress) match {
      case Left(err) =>
        logger.warn(s"⚠️ Rejecting metagraph-attestation: invalid address '${att.metagraphAddress}' ($err)")
      case Right(refined) =>
        val metagraphAddress = Address(refined)
        val senderHex = Hex(att.peerId.toByteArray.map("%02x".format(_)).mkString)
        val senderPeerId = peer.PeerId(senderHex)
        val parentHash = Hash(new String(att.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val binaryHash = Hash(new String(att.binaryHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val senderVrfVk = att.vrfPublicKey.toByteArray
        val incoming = IncomingAttestation(
          senderPeerId = senderPeerId,
          senderVrfVk = senderVrfVk,
          metagraphAddress = metagraphAddress,
          parentHash = parentHash,
          binaryHash = binaryHash,
          committeeVrfProof = att.committeeVrfProof.toByteArray,
          longTermSignature = att.signature.toByteArray,
          kesSignature = att.kesSignature.toByteArray,
          senderTreeStep = att.senderTreeStep
        )
        // #213/#290 + #29 (verify-on-attach): resolve the committee-VRF eta WITHOUT trusting the sender's
        // claimed ordinal AND without spending crypto on tines we aren't attached to. Two paths (below):
        //
        //   TIER 1 — IN-FLIGHT → eager verify. When this node processed the same gossiped binary in
        //       `processBytes` it cached `(wireHash → parentOrdinal)` (`recordPendingParentOrdinal`) before
        //       attesting. A `Some` from `lookupPendingParentOrdinal` means the binary's parent is at the tip —
        //       it is attaching NOW (running the `resolveParent == Some` admit path) — so it is a canonical
        //       candidate and its attestation is worth verifying immediately, so `attestAndAdmit`'s threshold
        //       sees it. `att.binaryHash` == the wire digest this node hashed in `processBytes`, so keys match.
        //   PHASE 0 — NOT IN-FLIGHT → buffer, defer verify (#29). Otherwise the binary is orphan-buffered (its
        //       parent isn't admitted — a tine we aren't attached to) or hasn't arrived. We do NOT verify now:
        //       speculatively crypto-checking attestations for fork-storm tines that mostly get evicted is the
        //       committee-gate CPU sink that starved gl0 producers off the air. `bufferAttestation` holds it
        //       un-verified; when the binary ATTACHES (drains into the admit path) `drainAttestations` replays
        //       it for verification right before the threshold gate. Losing tines' attestations expire
        //       un-verified — zero crypto. (This subsumes the old buffered-content-deserialize tier AND the
        //       fail-closed drop: a not-yet-arrived binary's attestation now waits in the buffer rather than
        //       being dropped, and verifies if/when the binary lands and attaches.)
        //
        // SAFETY (anti-forgery): Tier 1 reads gl0's OWN cached ordinal for THIS binary, never the sender's
        // claim — a forged ordinal yields a wrong eta and the committee-VRF verify rejects it. Phase-0
        // attestations are likewise verified at attach time against gl0's own resolved ordinal.
        val recordWithOrdinal: Long => F[Unit] = parentOrdinal =>
          etaForParentOrdinal(parentOrdinal).flatMap { eta =>
            committeeGate.recordReceivedAttestation(incoming, eta, senderStakeLookup)
          }
        orphanBuffer.lookupPendingParentOrdinal(metagraphAddress, binaryHash).flatMap {
          case Some(parentOrdinal) =>
            // Tier 1 — the binary is IN-FLIGHT: its parent is at the tip, so it is attaching NOW (running the
            // `resolveParent == Some` admit path in `processBytes`). Verify eagerly so `attestAndAdmit`'s
            // threshold sees this attestation immediately. `recordPendingParentOrdinal` seeded this lookup at
            // resolve time, so a `Some` here means the binary is canonical-candidate — worth the crypto.
            recordWithOrdinal(parentOrdinal)
          case None =>
            // Phase-0 of verify-on-attach (#29). The binary is NOT in-flight — it is either orphan-buffered
            // (its parent isn't admitted: a tine we aren't attached to) or simply hasn't arrived yet. EITHER
            // way we must NOT spend Ed25519+KES+VRF on it now: speculatively verifying attestations for tines
            // that mostly get evicted during an eta-rotation fork storm is the committee-gate CPU sink that
            // starved gl0 producers off the air. Buffer the attestation UN-VERIFIED (keyed by the wire hash);
            // when the binary ATTACHES (drains into the `resolveParent == Some` admit path) `drainAttestations`
            // releases it for verification (Phase 1), right before the threshold gate. If the binary never
            // attaches (losing tine) the buffered attestation is FIFO-evicted un-verified — zero crypto spent.
            // Replaces the prior peekForWireHash-scan + eager recordWithOrdinal (the speculative-verify bug).
            orphanBuffer.bufferAttestation(metagraphAddress, binaryHash, incoming) >>
              logger.debug(
                s"📥 Phase-0 buffered committee-attestation (un-verified) mg=$metagraphAddress " +
                  s"binary=${binaryHash.value.take(12)}... parent=${parentHash.value.take(12)}... peer=$senderPeerId " +
                  s"(verify deferred to attach; #29)"
              )
        }
    }
  }

  /** Gap B — handle an incoming `ShardCheckpointWire` from gossip (load-bearing cross-node reconstruction).
    *
    * Steps (per the wiring plan §B):
    *   1. Decode the wire into the schema-side `ShardCheckpoint` via `ShardCheckpointWireCodecs.shardCheckpointFromWire`.
    *   1. Look up `deps.registry.get(shardId)`; drop if the shard is untracked locally.
    *   1. Reconstruct slot + vrfOutput (the wire drops both): the slot is derived the SAME deterministic way the producer uses —
    *      `Slot(gl0AnchorOrdinal.value)` (shard-LOCAL, not gl0 wall-clock slot); vrfOutput is recovered from the producer's first
    *      `CommitteeMemberSignature.vrfProof` via `vrfOutputFromProof` (the same recovery the gl0 snapshot path uses), so both the
    *      producer's own store and every receiver store byte-identical vrfOutput. Re-wrap the bare `ShardCheckpoint` into a
    *      `Signed[ShardCheckpoint]` from the committee signatures present (the codec scaladoc describes the re-wrap — the producer's own
    *      committee `ed25519Sig` IS `signData(preimageHash)`, byte-identical to the outer `Signed` proof it built).
    *   1. Store into `entry.chainStore` so the chain grows + depth/attestation finality can advance across nodes.
    *   1. Record each `committeeSignatures` signer into `entry.tipTracker.recordAttestation` so `T_count_shard` can reach quorum.
    *   1. Run `deps.acceptanceManager.evaluate(checkpoint)` and log the result diagnostically.
    *
    * `None` deps (numShards=1, regression bar) ⇒ single debug log + drop.
    */
  /** Bound on the retroactive ancestor-attestation walk (chain-not-tip, 2026-06-11). Genesis stalls involve a handful of ordinals; anything
    * deeper is covered progressively by subsequent best-tip events.
    */
  private val MaxAncestorAttestWalk: Int = 16

  private def handleShardCheckpoint[F[_]: Async: JsonSerializer: HasherSelector: Metrics](
    cp: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ShardCheckpointWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    shardCheckpointAttestationEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]
    ],
    // T1 orphan-by-hash chain-sync (task #A): drives the parent-pull below — a received checkpoint whose parent is
    // absent pulls the missing parent and re-feeds it through THIS method, converging a divergent-sibling node onto
    // canonical. `None` ⇒ no pull (numShards=1 or unwired).
    shardCheckpointFetcher: Option[ShardCheckpointFetcher[F]],
    selfId: peer.PeerId,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAcceptanceDeps match {
      case None =>
        logger.debug("Received ShardCheckpoint but sharding inactive (numShards=1); dropping")
      case Some(deps) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
          .shardCheckpointFromWire[F](cp)
          .flatMap { checkpoint =>
            deps.registry.get(checkpoint.shardId) match {
              case None =>
                logger.debug(
                  s"Received ShardCheckpoint for untracked shard=${checkpoint.shardId.value.value}; dropping"
                )
              case Some(entry) =>
                // Reconstruct the Signed[ShardCheckpoint] envelope from the committee signatures. Each
                // CommitteeMemberSignature carries (peerId, ed25519Sig); SignatureProof(peerId.toId,
                // Signature(ed25519Sig)) is byte-faithful to what the producer built (the producer's own
                // committee ed25519Sig == its outer Signed proof signature; see codec scaladoc).
                val proofs = checkpoint.committeeSignatures.map { sig =>
                  io.constellationnetwork.security.signature.signature.SignatureProof(
                    sig.peerId.toId,
                    io.constellationnetwork.security.signature.signature.Signature(sig.ed25519Sig)
                  )
                }
                val proofSet = cats.data.NonEmptySet.of(proofs.head, proofs.tail: _*)
                val signedCheckpoint = Signed(checkpoint, proofSet)
                // Slot = the envelope's WIRE slot (design §5.7) — the signed lottery clock, byte-identical on every
                // node (the old anchor-derived reconstruction downsampled the lottery to gl0-snapshot cadence).
                // vrfOutput is still reconstructed from the producer's committee VRF proof (the wire drops it).
                val localSlot = checkpoint.slot.value.value
                val vrfOut = vrfOutputFromProof(checkpoint.committeeSignatures.head.vrfProof.toBytes)
                HasherSelector[F].withCurrent { implicit hasher =>
                  hasher.hash(checkpoint.signingPreimage).flatMap { checkpointHash =>
                    // §5.7 slot validity bound — strictly monotone vs the parent's wire slot (when the parent is
                    // known; orphans are bounded retroactively when the parent connects via the chain-store's
                    // connectivity gate). Deflating the slot is the only profitable direction (maxvalid-tk prefers
                    // LOWER slot on ties) and this check closes it; inflating is self-defeating. The wall-clock skew
                    // bound (`slot <= now + eps`) lands with the Slice-13 cryptographic verifyLeader.
                    // T1 orphan-by-hash chain-sync (task #A, run-22 cross-node convergence stall): if this checkpoint's
                    // parent is absent locally (and it's not genesis), background-PULL the missing parent from a peer and
                    // re-feed it through this same accept path. This is what converges a node sitting on a divergent
                    // sibling lineage onto canonical — the pulled parent connects the received checkpoint, maxvalid-tk
                    // reorgs the chain, and the committee's attestations concentrate so quorum forms. Gossip alone can't
                    // recover it (Tier-1 re-publish is dedup'd at the gossip layer). A second `getByHash` (a cheap
                    // Ref.get) keeps the trigger out of the main accept block — no wrapping of the big block below.
                    Async[F].whenA(shardCheckpointFetcher.isDefined && checkpoint.parentCheckpointHash =!= Hash.empty) {
                      entry.chainStore.getByHash(checkpoint.parentCheckpointHash).flatMap { p0 =>
                        Async[F].whenA(p0.isEmpty) {
                          shardCheckpointFetcher.fold(Async[F].unit) { fetcher =>
                            Async[F]
                              .start(
                                fetcher.fetchByHash(checkpoint.shardId, checkpoint.parentCheckpointHash).flatMap {
                                  case Some(parentSigned) =>
                                    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                      .signedShardCheckpointToWire[F](parentSigned)
                                      .flatMap(w =>
                                        handleShardCheckpoint(
                                          w,
                                          shardAcceptanceDeps,
                                          shardCheckpointAttestationEmitter,
                                          shardCheckpointFetcher,
                                          selfId,
                                          logger
                                        )
                                      )
                                  case None => Async[F].unit
                                }
                              )
                              .void
                          }
                        }
                      }
                    } >>
                      entry.chainStore.getByHash(checkpoint.parentCheckpointHash).flatMap { parentOpt =>
                        val slotMonotone = parentOpt.forall(_.signed.value.slot.value.value < checkpoint.slot.value.value)
                        if (!slotMonotone)
                          logger.warn(
                            s"🧩 ShardCheckpoint REJECTED (slot not monotone vs parent): shard=${checkpoint.shardId.value.value} " +
                              s"shardOrd=${checkpoint.shardOrdinal.value} slot=${checkpoint.slot.value.value} " +
                              s"parentSlot=${parentOpt.map(_.signed.value.slot.value.value).getOrElse(-1L)}"
                          )
                        else
                          // S3 — FIX THE ATTESTATION INVERSION. Mirror the global chain's gate
                          // (validate-by-replay → adopt as best-tip → emitAttestation): re-exec/validate the checkpoint FIRST, and
                          // only adopt it into the fork DAG + count its signers' attestations + emit OUR own attestation if the
                          // derivation matched. Previously the emit fired on `becameBestTip` BEFORE `evaluate`, so a node could attest
                          // (and adopt) a checkpoint whose derivation it had not re-run. `evaluate` runs the committee re-execution
                          // (`reExecuteDerivation`) on the degraded `T_depth1_shard` path; on the `T_count_shard` fast path it is
                          // quorum-attested (already cryptographically pre-checked). A `Rejected`/`RejectedReExecutionMismatch` result
                          // is DROPPED — not stored as adoptable, signers NOT counted, NO attestation emitted (a re-exec deviator must
                          // not have its checkpoint adopted nor be rewarded with our attestation). Per Q4 the handler stays
                          // `Async.start`-ed off the gossip thread (see the caller), but WITHIN it the emit is gated on re-exec success.
                          // S1 — gl0-FINALITY HARD ANCHOR (run-24; docs/nakamoto/SHARD-FINALITY-ANCHOR-DESIGN.md).
                          // Pull gl0's latest phase-2-finalized checkpoint hash for this shard into the fork choice. When the
                          // finalized anchor IS in the local store, `noteAnchor` reorgs onto it (the #42 heal). When it is
                          // ABSENT — this node followed a divergent LONGER tine and never received the finalized one (the run-24
                          // freeze: gl0 finalized C7A on tine α while local maxvalid-tk kept extending tine β) — `noteAnchor`
                          // silently REFUSES a not-yet-stored hash (`ShardChainStore`: "never replace a known anchor with a
                          // not-yet-stored hash"), so the anchor can never bite and the longer rogue tine wins forever. Fix:
                          // background-FETCH the finalized anchor by hash and re-feed it; the re-feed recursively pulls its
                          // ancestry via the T1 trigger above until it connects, and the NEXT receipt's `noteAnchor` then succeeds
                          // and `compareAnchoredMaxvalid` collapses the fork onto the finalized tine. Receipt-piggybacked +
                          // idempotent; the fetch is best-effort/deduped in `ShardCheckpointFetcher`.
                          deps.acceptanceManager.lastAdoptedAnchor(checkpoint.shardId).flatMap {
                            case None => Async[F].unit
                            case Some(anchorHash) =>
                              entry.chainStore.getByHash(anchorHash).flatMap {
                                case Some(_) => entry.chainStore.noteAnchor(anchorHash)
                                case None =>
                                  shardCheckpointFetcher.fold(Async[F].unit) { fetcher =>
                                    Async[F]
                                      .start(
                                        fetcher.fetchByHash(checkpoint.shardId, anchorHash).flatMap {
                                          case Some(anchorSigned) =>
                                            logger.info(
                                              s"🧩 ShardCheckpoint finality-anchor FETCH: shard=${checkpoint.shardId.value.value} " +
                                                s"anchor=${anchorHash.value.take(12)} absent locally — pulling gl0-finalized tine (run-24 S1)"
                                            ) >>
                                              io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                                .signedShardCheckpointToWire[F](anchorSigned)
                                                .flatMap(w =>
                                                  handleShardCheckpoint(
                                                    w,
                                                    shardAcceptanceDeps,
                                                    shardCheckpointAttestationEmitter,
                                                    shardCheckpointFetcher,
                                                    selfId,
                                                    logger
                                                  )
                                                )
                                          case None => Async[F].unit
                                        }
                                      )
                                      .void
                                  }
                              }
                          } >>
                            deps.acceptanceManager.evaluate(checkpoint).flatMap { result =>
                              // Inversion gate (see `shardCheckpointAdmissible`): adopt + count signers + emit ONLY on a non-rejecting result.
                              val admissible = shardCheckpointAdmissible(result)
                              val adoptAndAttest =
                                entry.chainStore
                                  .store(signedCheckpoint, checkpoint.parentCheckpointHash, checkpoint.shardOrdinal, localSlot, vrfOut)
                                  .flatMap { stored =>
                                    // Became-best-tip gate — byte-identical to the gl0 `becameBest = isNew && bestTipOpt.exists(_.hash ===
                                    // thisHash)` seam. `store` returns `isNew`; if the (re-exec-validated) checkpoint is now the canonical
                                    // bestTip (per ShardChainStore maxvalid-tk fork choice), THIS node attests once for this winning hash.
                                    entry.chainStore.bestTip.flatMap { bestTipOpt =>
                                      val becameBestTip = stored && bestTipOpt.exists(_.hash === checkpointHash)
                                      // Record every committee signer (the producer's own sig seeds 1 attestation) into the tip tracker —
                                      // only now that re-exec validated the checkpoint (so a wrong-derivation envelope never inflates quorum).
                                      checkpoint.committeeSignatures.toList
                                        .traverse_(sig => entry.tipTracker.recordAttestation(checkpointHash, sig.peerId, sig)) >>
                                        // T_count_shard quorum closure: sign + gossip OUR attestations so every OTHER node's tracker
                                        // crosses ⌈2·K_S/3⌉. Self-exclusion (P-11b) keeps them out of our own threshold count. `None`
                                        // emitter (numShards=1 regression bar) ⇒ no emit. Background-fire so the multi-step sign+publish
                                        // never blocks this handler (the handler is already inside an `Async.start`).
                                        //
                                        // ATTEST ON EVERY ADMISSIBLE RECEIPT, not only on becameBestTip (2026-06-11, run bc5a17r12):
                                        // the best-tip-only gate + the tip-triggered ancestor walk still missed OUT-OF-ORDER arrivals —
                                        // when ord 12 gossips in before ord 11, 12 becomes best tip while 11 is unknown (the ancestor
                                        // walk hits getByHash=None and stops), and when 11 lands later it is NOT a new best tip, so no
                                        // emit ever fires for it. Checkpoints 11/12 then sat below quorum for ~3 min (19 consecutive
                                        // embed-none ords) — the admission plateaus that expired DoubleUse's allow-spend mid-flight.
                                        // The canonical-chain walk below starts from the CURRENT best tip on every receipt and attests
                                        // anything stored + not yet self-attested, so a late-arriving parent is attested the moment it
                                        // lands. Idempotent (tracker self-record) and cheap (≤16 tracker lookups per received envelope).
                                        Async[F]
                                          .whenA(true) {
                                            shardCheckpointAttestationEmitter match {
                                              case None          => Async[F].unit
                                              case Some(emitter) =>
                                                // EXECUTION-SHARDING Task 2 attest gate: emit OUR attestation only if THIS node is in shard
                                                // `s`'s committee for the checkpoint's epoch. A non-member's attestation is rejected by every
                                                // verifier's `verifyEmbedded` membership pre-check (so it can't count toward quorum) — gating
                                                // here skips the wasted sign+gossip. The SAME deterministic draw the verifier admits against.
                                                deps.committeeMembership(checkpoint.shardId, checkpoint.epoch).flatMap { committee =>
                                                  if (!committee.contains(selfId))
                                                    logger.debug(
                                                      s"🧩 ShardCheckpoint attest: self not in committee for shard=${checkpoint.shardId.value.value} " +
                                                        s"epoch=${checkpoint.epoch.value}; skipping attestation"
                                                    )
                                                  else {

                                                    // ATTEST THE CHAIN, NOT JUST THE TIP (2026-06-11, run bp4wrh5zq): members previously attested
                                                    // only the checkpoint that was the best tip AT THE MOMENT IT ARRIVED. During bootstrap, nodes
                                                    // join the shard topics staggered over minutes while the producer keeps extending, so early
                                                    // shardOrds scroll past before most members are listening — the genesis checkpoint sat at
                                                    // signers=1 for 7+ minutes, and because gl0 embedding is ancestor-first, the WHOLE shard chain's
                                                    // admission was blocked behind the under-attested ancestor (the run-6 DoubleUse spend-action
                                                    // timeout). On becoming best tip, also attest every stored canonical ancestor this node has not
                                                    // yet attested (walk bounded; per-ancestor committee gate on the ancestor's OWN epoch). More
                                                    // attestations are always safe — verifiers dedupe by peerId and check membership — and quorum
                                                    // closes retroactively, unblocking ancestor-first embedding.
                                                    def attestMissingAncestors(h: Hash, remaining: Int): F[Unit] =
                                                      if (remaining <= 0 || h === Hash.empty) Async[F].unit
                                                      else
                                                        entry.chainStore.getByHash(h).flatMap {
                                                          case None => Async[F].unit // deeper than our stored view — stop
                                                          case Some(anc) =>
                                                            val ancCp = anc.signed.value
                                                            entry.tipTracker.signaturesFor(anc.hash).flatMap { sigs =>
                                                              Async[F].whenA(!sigs.contains(selfId)) {
                                                                deps.committeeMembership(ancCp.shardId, ancCp.epoch).flatMap {
                                                                  ancCommittee =>
                                                                    Async[F].whenA(ancCommittee.contains(selfId)) {
                                                                      logger.info(
                                                                        s"🧩 ShardCheckpoint attest-ancestor: shard=${ancCp.shardId.value.value} " +
                                                                          s"shardOrd=${ancCp.shardOrdinal.value} slot=${ancCp.slot.value.value} " +
                                                                          s"— retroactive attestation (chain-not-tip)"
                                                                      ) >>
                                                                        emitter.emit(ancCp.shardId, anc.hash, ancCp.slot, ancCp.epoch)
                                                                    }
                                                                }
                                                              } >> attestMissingAncestors(ancCp.parentCheckpointHash, remaining - 1)
                                                            }
                                                        }

                                                    // Walk from the CURRENT canonical tip (not the received envelope): covers the received
                                                    // checkpoint when it IS the tip, late-arriving ancestors when it is not, and any other
                                                    // unattested canonical entries in between.
                                                    entry.chainStore.bestTip.flatMap {
                                                      case None      => Async[F].unit
                                                      case Some(tip) => attestMissingAncestors(tip.hash, MaxAncestorAttestWalk)
                                                    }
                                                  }
                                                }
                                            }
                                          }
                                          .as(becameBestTip)
                                    }
                                  }
                              val logSkipped =
                                logger
                                  .info(
                                    s"🧩 ShardCheckpoint rx shard=${checkpoint.shardId.value.value} " +
                                      s"shardOrd=${checkpoint.shardOrdinal.value} gl0Anchor=${checkpoint.gl0AnchorOrdinal.value.value} " +
                                      s"signers=${checkpoint.committeeSignatures.size} evaluate=$result — NOT adopted/attested (re-exec/pre-check reject)"
                                  )
                                  .as(false)
                              (if (admissible) adoptAndAttest else logSkipped).flatMap { becameBestTip =>
                                Async[F].whenA(admissible) {
                                  logger.info(
                                    s"🧩 ShardCheckpoint rx shard=${checkpoint.shardId.value.value} " +
                                      s"shardOrd=${checkpoint.shardOrdinal.value} gl0Anchor=${checkpoint.gl0AnchorOrdinal.value.value} " +
                                      s"signers=${checkpoint.committeeSignatures.size} becameBestTip=$becameBestTip evaluate=$result (re-exec validated)"
                                  )
                                }
                              }
                            }
                      }
                  }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle ShardCheckpoint: ${err.getMessage}")
          }
    }

  /** Gap B — handle an incoming `ShardCheckpointAttestationWire` from gossip. Decode, '''verify the attester's Ed25519 signature over the
    * checkpoint hash''', then record the attester into the per-shard tip tracker so `T_count_shard` can reach quorum from after-the-fact
    * (non-producing) committee attestations. `None` deps (numShards=1) ⇒ drop with a debug log.
    *
    * '''Why verify before recording (mirrors the gl0 `handleAttestation` path).''' `T_count_shard` counts DISTINCT attester peerIds;
    * without a signature check any peer could forge an attestation under another operator's `peerId` and inflate the count toward false
    * quorum (a cluster-split risk). The emitter signs the canonical checkpoint hash's UTF-8 bytes with the operator's long-term Ed25519 key
    * (the SAME bytes + key the producer's committee sig uses), so we recover the VK from `attesterSignature.peerId` and verify exactly as
    * `handleAttestation` does (`Signing.verifySignature`). Unsigned / invalid-sig attestations are dropped (WARN) and never reach the
    * tracker — matching the slashing safety bar (only cryptographically verifiable evidence counts).
    */
  private def handleShardCheckpointAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    att: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ShardCheckpointAttestationWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAcceptanceDeps match {
      case None =>
        logger.debug("Received ShardCheckpointAttestation but sharding inactive (numShards=1); dropping")
      case Some(deps) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
          .shardCheckpointAttestationFromWire[F](att)
          .flatMap { attestation =>
            deps.registry.get(attestation.shardId) match {
              case None =>
                logger.debug(
                  s"Received ShardCheckpointAttestation for untracked shard=${attestation.shardId.value.value}; dropping"
                )
              case Some(entry) =>
                val attesterId = attestation.attesterSignature.peerId
                val sigBytes = attestation.attesterSignature.ed25519Sig.toBytes
                // The attester signed the canonical checkpoint hash's UTF-8 bytes (design doc §3.3) — the same bytes the producer's
                // committee `ed25519Sig` covers. Recover the VK from the peerId and verify; mirrors `handleAttestation`.
                val msgBytes = attestation.checkpointHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8)
                if (sigBytes.isEmpty)
                  logger.warn(
                    s"⚠️ Rejecting unsigned ShardCheckpointAttestation shard=${attestation.shardId.value.value} " +
                      s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}..."
                  )
                else
                  (for {
                    pubKey <- attesterId.value.toPublicKey[F]
                    valid <- Signing.verifySignature[F](msgBytes, sigBytes)(pubKey)
                  } yield valid).handleError(_ => false).flatMap {
                    case false =>
                      logger.warn(
                        s"⚠️ Rejecting ShardCheckpointAttestation with invalid signature shard=${attestation.shardId.value.value} " +
                          s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}..."
                      )
                    case true =>
                      entry.tipTracker.recordAttestation(attestation.checkpointHash, attesterId, attestation.attesterSignature) >>
                        logger.debug(
                          s"🧩 ShardCheckpointAttestation rx shard=${attestation.shardId.value.value} " +
                            s"checkpoint=${attestation.checkpointHash.value.take(12)} attester=${attesterId.value.value.take(12)}"
                        )
                  }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle ShardCheckpointAttestation: ${err.getMessage}")
          }
    }

  /** Route an incoming state channel binary from gossip into the [[MetagraphCommitteeGate]] (Slice S3, load-bearing) — the gate computes
    * the committee sortition for THIS node, emits an attestation if we're in the committee, and waits for ≥ ⌈2 K_target / 3⌉ attestations
    * to land in the aggregator before admitting the binary into the local acceptance pipeline.
    *
    * The sender serialized Signed[StateChannelSnapshotBinary] via the project's JsonSerializer (JSON + Brotli); we use the same typeclass
    * to deserialize. Decode failures and gate timeouts both result in the binary being dropped (WARN-logged with enough detail for an
    * operator to diagnose).
    *
    * '''Why the gate runs here, not deeper.''' `processMetagraphBinary` is invoked from two call sites: (a) this gossip handler, and (b)
    * `StateChannelRoutes` over HTTP for CL0-originated binaries. The HTTP path is local-only on the receiving gl0; that gl0's local state
    * isn't load-bearing without entering a finalized global snapshot, and a global snapshot only finalizes once 2/3 of gl0s attest. Each
    * peer gl0 receiving the snapshot proposal applies the same gate on the snapshot's `stateChannelSnapshots` entries — so a binary that
    * skipped the local HTTP gate at gl0-A will still be gated at every other gl0 when it arrives via gossip. The cluster-wide safety
    * property holds against an adversarial submission at a single gl0.
    */
  /** Build the gate-aware metagraph-binary processor: a closure that takes `(metagraphAddress, wireBytes)` and runs the binary through the
    * committee gate (#192 Slice S3), processing it on admit and orphan-buffering it on parent-not-yet-resolved (#213). The same closure is
    * invoked from both call sites that need this path:
    *
    *   - `handleMetagraphBinary` (the daemon's gossip handler) — for fresh `pb.MetagraphBinary` arrivals on the wire.
    *   - `SnapshotLeaderLoop.onFinalize` (via `GlobalSnapshotConsensus.make`) — for orphan-drain on gl0 snapshot finalize (#214). When gl0
    *     finalizes a global snapshot containing a metagraph binary that some peers' local gates dropped, those local nodes' orphan-buffered
    *     children of the just-finalized binary become resolvable (the parent's value-hash now lives in gl0's GSI). Re-feeding them through
    *     THIS gate-aware processor — not direct `processMetagraphBinary` — is the load-bearing safety property: the local gate still
    *     attests and the child gets cluster-confirmed normally. The earlier attempt (reverted, ad7d4f041 → fea66fc7d) bypassed the gate and
    *     polluted the state-channel queue.
    *
    * '''Why a factory.''' Both call sites need the same closure, and the closure recursively re-enters itself when draining children of a
    * just-admitted parent. Defining the closure top-level (rather than nested inside `handleMetagraphBinary`) lets the finalize-hook
    * callable share the exact same shape — same orphan buffer, same admission cache, same recursive drain.
    */
  def makeMetagraphBinaryProcessor[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector](
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    // #213/#290: CONTENT-DERIVED parent-ordinal resolver (`MetagraphParentOrdinalResolver.resolveFromBinary`).
    // The third arg is the incoming binary's OWN `content` bytes; the resolver derives the parent ordinal
    // as (this binary's currency-snapshot ordinal − 1), a deterministic pure function of the content, NOT
    // read from gl0's currency partitions (which `calculateLastCurrencySnapshots` drops via
    // `.filterNot(_.isEmpty)` whenever the mg produced no state in the window → the permanent admission
    // deadlock at 8gl0+4mg). The identity guard (`lastStateChannelSnapshotHashes[mg] == parentHash`) still
    // runs inside the resolver. The SAME content-derived resolver is now wired into the inbound-attestation
    // receiver (`NakamotoSyncDaemon.run`'s `parentOrdinalFor`), so sender (admission) and receiver derive
    // byte-identical etas; the committee gate itself no longer re-resolves the ordinal.
    parentOrdinalFor: (
      io.constellationnetwork.schema.address.Address,
      io.constellationnetwork.security.hash.Hash,
      Array[Byte]
    ) => F[Option[Long]],
    etaForParentOrdinal: Long => F[Array[Byte]],
    selfStake: F[io.constellationnetwork.numerics.Ratio],
    // #29 verify-on-attach: per-sender stake lookup used to verify the attestations we BUFFERED un-verified
    // while a binary was an orphan, replayed at attach time below (`drainAttestations`). Same lookup the
    // inbound-attestation receiver uses — `stakeRegistry.committeeStake`.
    senderStakeLookup: io.constellationnetwork.schema.peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): (io.constellationnetwork.schema.address.Address, Array[Byte]) => F[Unit] = {
    import io.constellationnetwork.schema.address.Address
    import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}
    import io.constellationnetwork.security.signature.Signed

    // Resolver wrapper that bridges gl0's GSI lag. After we admit a binary X via the committee gate,
    // X is enqueued for processing but won't land in gl0's `lastStateChannelSnapshotHashes` until the
    // next global snapshot finalizes (~7s later). Without a shortcut, any drained child of X would
    // hit `parentOrdinalFor` → None and immediately re-buffer, never making progress.
    // The orphan buffer's admission cache holds `(mg, X.value.hash) → mgOrd_X` for that GSI-lag window.
    //
    // ARITHMETIC (cache fast-path == content derivation, end-to-end): on admit of binary B we record
    // `(mg, valueHash(B)) → parentOrdinal + 1 = ord(B)` (B's own ordinal). The next child C chains off B
    // (`C.parentHash = valueHash(B)`, `ord(C) = ord(B)+1`). Resolving C's parent ordinal: cache hit on
    // `valueHash(B)` → `ord(B)`; content path → `ord(C) − 1 = ord(B)`. Identical. (Genesis ord 0 → first
    // incremental ord 1 has parent ordinal 0; admitted ord 1 records admission value 1; child ord 2 resolves 1.)
    // `incomingContent` is THIS binary's `content` bytes, fed to the content-derivation resolver on cache miss.
    def resolveParent(address: Address, parentHash: Hash, incomingContent: Array[Byte]): F[Option[Long]] =
      orphanBuffer.lookupAdmittedOrd(address, parentHash).flatMap {
        case s @ Some(_) => Async[F].pure(s)
        case None        => parentOrdinalFor(address, parentHash, incomingContent)
      }

    // After a binary is admitted, drain any orphans whose parent equals the just-accepted binary's
    // *value*-hash (`signed.value.hash`) — same hash gl0's GSAM writes into `lastStateChannelSnapshotHashes`.
    // Each drained child runs through the same path, which may itself unblock further descendants, so
    // the chain unwinds in order. #213.
    def processBytes(address: Address, bytes: Array[Byte]): F[Unit] =
      io.constellationnetwork.json
        .JsonSerializer[F]
        .deserialize[Signed[StateChannelSnapshotBinary]](bytes)
        .flatMap {
          case Left(err) =>
            logger.warn(s"⚠️ Rejecting metagraph-binary gossip: decode failed for $address (${err.getMessage})")
          case Right(signed) =>
            val output = StateChannelOutput(address, signed)
            val parentHash = signed.value.lastSnapshotHash
            // Hash the wire bytes to produce the binary hash — uses the same `Hasher[F]` surface that
            // the gate's verifier uses to match `binaryHash` across observers. The bytes here are the
            // serialized Signed[StateChannelSnapshotBinary]; both sender and receiver hash the same
            // wire payload so the binary-hash agrees byte-for-byte.
            HasherSelector[F].withCurrent { implicit hasher =>
              Hasher[F].hashBytes(bytes).flatMap { binaryHash =>
                // Gate inputs: σ_self for our committee threshold + eta derived from the actual
                // metagraph parent ordinal (#202). The selfStake lookup is the same StakeRegistry
                // path the leader VRF uses.
                //
                // Resolver returns None when gl0's GSI doesn't yet have a metagraph snapshot under
                // this parent hash AND the admission cache doesn't either. At 4-mg+committee-gate
                // scale this is the normal state for any binary chained off something we haven't
                // admitted yet: ml0 races ahead of gl0's gate cadence and chains forward off
                // binaries gl0 hasn't seen yet. Buffer in the orphan pool keyed by parentHash;
                // when the matching binary IS admitted, drainChildren replays them in chronological
                // order. #213.
                resolveParent(address, parentHash, signed.value.content).flatMap {
                  case None =>
                    orphanBuffer.record(address, parentHash, bytes).flatMap { sz =>
                      logger.info(
                        s"📦 Orphan-buffered mg=$address parent=${parentHash.value.take(12)}... (parent not yet admitted) bufferSize=$sz"
                      )
                    }
                  case Some(parentOrdinal) =>
                    for {
                      // #213/#290 liveness: cache (this binary's WIRE hash → its parent ordinal) BEFORE we
                      // attest, so inbound committee attestations for this SAME in-flight binary can recover
                      // the eta on the receive path. The binary is NOT buffered here (its parent is at the
                      // tip — that's why `resolveParent` returned `Some`), so the receiver's `peekForWireHash`
                      // (orphan-buffer scan) would miss it → without this cache the committee threshold is
                      // never reached and the chain freezes at genesis. `binaryHash` is the wire-bytes digest,
                      // == `pb.MetagraphAttestation.binaryHash` on the receive side.
                      _ <- orphanBuffer.recordPendingParentOrdinal(address, binaryHash, parentOrdinal)
                      // The just-resolved binary's *value* hash — the chain-link identity the NEXT binary's
                      // `lastSnapshotHash` points at (GlobalSnapshotAcceptanceManager.scala:908 /
                      // StateChannelSnapshotService.scala:112). NOT `binaryHash` (the wire-bytes digest). Uses
                      // the `implicit hasher` already in scope from the enclosing `HasherSelector.withCurrent`.
                      valueHash <- signed.toHashed.map(_.hash)
                      // #213/#290 admission-lag fix: seed the value-hash→ordinal admission cache the instant we
                      // RESOLVE the binary (we are in the `Some` branch, so `resolveParent` already verified its
                      // parent matched this peer's recorded tip / a cached ancestor — the identity guard ran),
                      // NOT only after the local committee gate admits it below. `parentOrdinal + 1` is THIS
                      // binary's own metagraph ordinal — a pure function of its content (ordinal − 1, then + 1),
                      // so every honest node caches the byte-identical value; recording it neither weakens the
                      // identity guard nor can induce a fork (it only feeds the committee-VRF eta downstream — a
                      // wrong ordinal makes that verify FAIL, fail-safe, never fork — and v1 metagraph chains do
                      // not reorg). WHY at resolve-time, not admit-time: this node's local gate can TIME OUT (a
                      // few peers were transiently behind and buffered this binary instead of attesting, so the
                      // kQuorum was not reached) even though the binary's place in the chain is fixed and the
                      // cluster admits it via 2/3-attestation or depth-k. If we cached only on local-admit, the
                      // very next child would miss `lookupAdmittedOrd`, fall through to the tip-guarded resolver,
                      // find gl0's GSI tip still trailing this not-yet-finalized binary → `parentHash mismatch`
                      // → orphan-buffer; every successor then re-buffers off it (the 733-mismatch / orphan
                      // re-buffer loop with gl0 trailing ml0). Caching the deterministic ordinal here lets the
                      // child resolve and enter `attestAndAdmit` regardless of THIS node's gate outcome on the
                      // parent. The post-admit `recordAdmission` is now redundant and folded into this single
                      // unconditional write.
                      _ <- orphanBuffer.recordAdmission(address, valueHash, parentOrdinal + 1L)
                      eta <- etaForParentOrdinal(parentOrdinal)
                      // Phase 1 of verify-on-attach (#29). The binary has ATTACHED (its parent resolved → we're on
                      // the admit path). Verify NOW the attestations we buffered un-verified while it was an orphan,
                      // so `attestAndAdmit`'s threshold below counts them. Only this canonical (attached) binary's
                      // attestations get the crypto; losing tines' buffered attestations are never drained here and
                      // expire un-verified — that is the fork-storm CPU the speculative path was burning. A single
                      // forged/stale buffered attestation is logged + skipped, never aborts admission.
                      _ <- orphanBuffer.drainAttestations(address, binaryHash).flatMap { buffered =>
                        buffered.traverse_ { bufferedAtt =>
                          committeeGate
                            .recordReceivedAttestation(bufferedAtt, eta, senderStakeLookup)
                            .handleErrorWith(e => logger.debug(s"⚠️ buffered attestation verify failed mg=$address: ${e.getMessage}"))
                        }
                      }
                      sigma <- selfStake
                      admitted <- committeeGate.attestAndAdmit(address, parentHash, binaryHash, eta, sigma)
                      _ <-
                        if (admitted)
                          processMetagraphBinary(output)
                            .handleErrorWith(e => logger.warn(s"⚠️ processMetagraphBinary failed for $address: ${e.getMessage}"))
                            .flatMap(_ => orphanBuffer.drainChildren(address, valueHash))
                            .flatMap(_.traverse_(child => processBytes(address, child)))
                        else Async[F].unit
                    } yield ()
                }
              }
            }
        }

    processBytes
  }

  private def handleMetagraphBinary[F[_]](
    mb: pb.MetagraphBinary,
    processOrphanedBinary: (io.constellationnetwork.schema.address.Address, Array[Byte]) => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import eu.timepit.refined.refineV

    refineV[DAGAddressRefined](mb.address) match {
      case Left(err) =>
        logger.warn(s"⚠️ Rejecting metagraph-binary gossip: invalid address '${mb.address}' ($err)")
      case Right(refined) =>
        val address = Address(refined)
        val bytes = mb.binary.toByteArray
        processOrphanedBinary(address, bytes)
    }
  }

  /** EXECUTION-SHARDING R-1 (additive intake): buffer a received raw metagraph binary into the buffer for ITS shard, off the EXISTING
    * global binaries gossip topic. Runs ALONGSIDE the legacy `handleMetagraphBinary` chain-link admission path (which is unchanged) — the
    * producer fan-out reads `snapshotPending` from the SAME buffer instances projected from `shardAcceptanceDeps.registry`.
    *
    * '''Determinism model.''' Leader-proposes / members-attest: the buffer is node-local and need NOT converge across committee members —
    * only the shard slot leader builds the checkpoint from its own buffer; others attest the gossiped envelope (see
    * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer]]). So we just deserialize, resolve the shard via the
    * SAME deterministic `ShardAssignment.shardIdFor` mapping, and `bufferBinary` (idempotent by hash) — no multi-proposer machinery.
    *
    * Only invoked when `shardBinaryBuffers.nonEmpty` (`numShards > 1`); at `numShards = 1` the caller's `whenA` gate makes this a no-op.
    */
  private def bufferReceivedBinaryForShard[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector](
    mb: pb.MetagraphBinary,
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import io.constellationnetwork.security.signature.Signed
    import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
    import eu.timepit.refined.refineV

    (refineV[DAGAddressRefined](mb.address), shardAssignment) match {
      case (Right(refined), Some(assignment)) =>
        val address = Address(refined)
        val bytes = mb.binary.toByteArray
        // Deserialize with the SAME typeclass + wire format the legacy `processMetagraphBinary` path uses
        // (`JsonSerializer` = JSON + Brotli). On decode failure, log + drop — the legacy path logs the same.
        io.constellationnetwork.json
          .JsonSerializer[F]
          .deserialize[Signed[StateChannelSnapshotBinary]](bytes)
          .flatMap {
            case Left(err) =>
              logger.debug(s"R-1 shard-buffer: decode failed for $address (${err.getMessage}); not buffering")
            case Right(signed) =>
              HasherSelector[F].withCurrent { implicit hasher =>
                assignment.shardIdFor(address).flatMap { sid =>
                  shardBinaryBuffers.get(sid) match {
                    case Some(buffer) => buffer.bufferBinary(address, signed)
                    case None         =>
                      // Address maps to a shard this operator doesn't track a buffer for — drop quietly (the
                      // legacy admission path still ran). Expected only if numShards/registry disagree.
                      logger.debug(s"R-1 shard-buffer: no buffer for shard=${sid.value.value} (mg=$address); skipping")
                  }
                }
              }
          }
      case _ =>
        // Invalid address (legacy path already warned) or no assignment (numShards=1) — nothing to buffer.
        Async[F].unit
    }
  }

  // Package-visible so SnapshotLeaderLoop can route producer-self-attestation through the
  // same code path peer-received snapshots use. Unifies the two attestation sites: any future
  // gating, signing semantics, or broadcast policy applies uniformly.
  //
  // `attestedAt` is **wall-clock epoch milliseconds** sourced via `Clock[F].realTime` by the
  // caller (NEVER `System.currentTimeMillis()`). It is NOT a consensus slot. Wall-clock
  // semantics here are deliberate — keeps the field reusable as a future Ouroboros-Chronos-style
  // timestamp-claim surface (gossiping `attestedAt` values lets the network distill a consensus
  // time without needing NTP). Today the value is only used by `TipTracker.recordAttestation`
  // for the "newer wins" rule, which compares Longs.
  def emitAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    attestedAt: Long,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // snap.hash bytes are the UTF-8 encoding of the hex hash string — decode back to string
    val tipHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
    val tipSlot = Slot(NonNegLong.unsafeFrom(snap.slot))
    emitTipAttestation(
      tipHash,
      tipSlot,
      snap.ordinal,
      attestedAt,
      sidecarClient,
      tipTracker,
      selfId,
      keyPair,
      operationalKeyMaker,
      etaRotationSnapshots,
      logger
    )
  }

  // Primitive variant for callers that have tip hash/slot/ordinal directly (e.g. the finality
  // monitor's bestTip-change ticker re-attesting after chainSelection moves).
  // See `emitAttestation` comment re: `attestedAt` semantics — wall-clock epoch ms via
  // `Clock[F].realTime`, sourced by the caller; this function does not read the clock.
  def emitTipAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    tipHash: Hash,
    tipSlot: Slot,
    tipOrdinal: Long,
    attestedAt: Long,
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    // Record locally first (so our own TipTracker sees it). The caller already sourced
    // `attestedAt` from `Clock[F].realTime`, so passing the same value as `now` makes
    // the skew gate (`TipTracker.MaxAttestationSkewMs`) trivially pass on self-emit.
    val localAtt = DomainTipAttestation(tipHash, tipSlot, tipOrdinal, attestedAt)
    tipTracker.recordAttestation(selfId, localAtt, attestedAt) >>
      // Sign via the standard Hasher pipeline: JSON-encode the domain TipAttestation → hash → sign the hash.
      // Same path as SignatureProof.fromData / Signed.forAsyncHasher — no custom serialization.
      HasherSelector[F].withCurrent { implicit hasher =>
        for {
          attHash <- localAtt.hash
          sig <- Signature.fromHash[F](keyPair.getPrivate, attHash)
          sigBytes = sig.coerce.toBytes
          // §1.2 Slice 5: parallel-sign the attestation hash with KES. Period is derived from
          // the tip's ordinal, NOT slot — slots are LDD-paced and lumpy. The receiver re-derives
          // the same period from `tipOrdinal` in the wire message so verification stays
          // self-consistent without any extra wire field.
          //
          // Sender failure (signAt returns Left) → empty wire field, receiver treats as no-sig.
          // Don't drop the attestation. Period eta-aligned evolution lands in Slice 8; this slice
          // signs at the period without any pre-evolve step.
          kesPeriod = EtaCalculation.rotationPeriod(tipOrdinal, etaRotationSnapshots).toInt
          kesAttempt <- operationalKeyMaker.signAt(kesPeriod, attHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8))
          kesSigBytes <- kesAttempt match {
            case Right(kSig) =>
              val bytes = io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(kSig)
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_signed_total") >>
                logger
                  .info(
                    s"🔐 KES-ATT ord=$tipOrdinal period=$kesPeriod sub-sig=${bytes.take(8).map("%02x".format(_)).mkString} (${bytes.length}B)"
                  )
                  .as(bytes)
            case Left(err) =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_sign_failed_total") >>
                logger
                  .warn(s"⚠️ KES-ATT sign failed for ord=$tipOrdinal period=$kesPeriod: ${err.message} — emitting unsigned wire field")
                  .as(Array.empty[Byte])
          }
          att = SidecarClient.mkAttestation(
            tipHash = tipHash.value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
            tipSlot = tipSlot.value.value,
            tipOrdinal = tipOrdinal,
            attestedAt = attestedAt,
            attesterId = selfId.value.toBytes,
            signature = sigBytes,
            kesSignature = kesSigBytes
          )
          _ <- sidecarClient.publishAttestation(att).void.handleErrorWith { e =>
            logger.warn(s"Failed to emit attestation: ${e.getMessage}")
          }
        } yield ()
      }
  }

  /** Catch up from a gossip payload when parent is missing (restart scenario).
    *
    * Instead of rejecting the snapshot, use it to reset local state to the network tip. The gossip message already contains the full
    * Signed[GlobalIncrementalSnapshot] + GlobalSnapshotInfo. The parent is absent so the full `NakamotoSnapshotValidator.validate` (VRF +
    * slot-cert) can't run, but the two parent-free, deterministic gates in [[verifyCatchUpSnapshot]] (envelope signature +
    * stateProof-vs-GSI consistency) DO run BEFORE any canonical/MPT write — an unsigned/forged or GSI-inconsistent tuple is rejected and
    * nothing is written (the node retries on a later gossip wave; the 10s cooldown rate-limits). An honest snapshot passes both gates by
    * construction, so we reset our canonical storage + MPT and subsequent gossip messages WILL have parents we recognize. See
    * [[verifyCatchUpSnapshot]] for why majority-hash (gate 3) and VRF/slot-cert (gate 4) are NOT enforced on this path.
    */
  private def catchUpFromGossip[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    parsed: Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)],
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    mptStore: MptStore[F, GlobalStateKey],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    productionGate: ProductionGate[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] = {
    val now = System.currentTimeMillis()
    stateRef.get.flatMap { state =>
      if (now - state.lastCatchUpAttemptMs < CatchUpCooldownMs) {
        // Cooldown — don't spam catch-up attempts
        logger.debug(
          s"⏳ Catch-up cooldown (${(CatchUpCooldownMs - (now - state.lastCatchUpAttemptMs)) / 1000}s remaining), " +
            s"skipping ordinal=${snap.ordinal}"
        )
      } else {
        parsed match {
          case Some((signedSnapshot, context)) =>
            val parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
            for {
              _ <- stateRef.update(_.copy(lastCatchUpAttemptMs = now))
              // SECURITY: the parent is missing here, so `NakamotoSnapshotValidator.validate` (VRF + slot-cert) cannot run. Adopting an
              // UNVERIFIED gossiped tuple as the entire canonical gl0 state would let any single peer reset our balances/txRefs/stakes/
              // locks. Gate adoption on the two parent-free, deterministic checks (`verifyCatchUpSnapshot`) BEFORE touching any canonical
              // storage or the MPT: a forged-signature tuple (gate 1) or a GSI inconsistent with the signed `stateProof` (gate 2) is
              // rejected and nothing is written; the node retries on a later gossip wave (the 10s cooldown rate-limits). An HONEST
              // snapshot (valid sig + GSI matching its committed stateProof) passes both gates by construction, so legitimate catch-up
              // still recovers.
              verdict <- verifyCatchUpSnapshot[F](signedSnapshot, context)
              _ <- verdict match {
                case CatchUpVerdict.RejectedInvalidSignature =>
                  logger.warn(
                    s"⛔ Rejecting catch-up snapshot ordinal=${snap.ordinal} slot=${snap.slot}: INVALID envelope signature. " +
                      s"Not adopting (no canonical/MPT write). Will retry on next gossip wave."
                  ) >>
                    Metrics[F].incrementCounter(
                      "dag_nakamoto_catchup_rejected",
                      Seq(Metrics.unsafeLabelName("reason") -> "invalid_signature")
                    )
                case CatchUpVerdict.RejectedStateProofMismatch(rejectedHashed) =>
                  logger.warn(
                    s"⛔ Rejecting catch-up snapshot ordinal=${snap.ordinal} slot=${snap.slot} " +
                      s"hash=${rejectedHashed.hash.value.take(12)}: carried GlobalSnapshotInfo does NOT match the snapshot's " +
                      s"committed stateProof (state-proof mismatch). Not adopting (no canonical/MPT write). Retrying on next gossip wave."
                  ) >>
                    Metrics[F].incrementCounter(
                      "dag_nakamoto_catchup_rejected",
                      Seq(Metrics.unsafeLabelName("reason") -> "state_proof_mismatch")
                    )
                case CatchUpVerdict.Accept(verifiedHashed) =>
                  for {
                    _ <- logger.warn(
                      s"\uD83D\uDD04 CATCH-UP: No parent found for ordinal=${snap.ordinal} slot=${snap.slot} " +
                        s"(verified: signature OK, stateProof matches). Resetting local state to network tip."
                    )
                    _ <- productionGate.pause("catch-up-sync")

                    // Store in chain store (seed this snapshot as our new starting point)
                    _ <- chainStore.store(
                      signedSnapshot,
                      context,
                      snap.ordinal,
                      snap.slot,
                      parentHash,
                      vrfOutputFromProof(snap.vrfProof.toByteArray)
                    )

                    // Update ALL canonical storages — snapshotStorage is what SnapshotLeaderLoop
                    // reads for its parent. Without this, the leader loop produces at the OLD ordinal
                    // after catch-up, causing the node to fall behind again immediately. Reuse the
                    // signature-verified `Hashed` from `verifyCatchUpSnapshot` rather than re-hashing
                    // via the no-signature-check `toHashed` (`setHeadForRecovery` needs the implicit Hasher).
                    _ <- HasherSelector[F].withCurrent { implicit hasher =>
                      snapshotStorage.setHeadForRecovery(signedSnapshot, context) >>
                        lastGlobalSnapshotStorage.setForRecovery(verifiedHashed, context) >>
                        lastNGlobalSnapshotStorage.setForRecovery(verifiedHashed, context)
                    }

                    // MPT full sync from the context we received — critical for
                    // the acceptance manager to validate subsequent snapshots
                    _ <- logger.info(s"\uD83D\uDD04 CATCH-UP: Syncing MPT from received context...")
                    _ <- HasherSelector[F].withCurrent { implicit hasher =>
                      mptStore.syncFromGlobalSnapshotInfo(context, SnapshotOrdinal(NonNegLong.unsafeFrom(snap.ordinal)))
                    }
                    // Don't overwrite lastKnownSlotRef with the gossip slot — it may be
                    // ahead of our wall clock, making slotGap negative and blocking VRF
                    // eligibility. Production will update it after its next successful store.

                    // Reconcile event mempool — evict DAG blocks whose transactions are
                    // already consumed in the new context's lastTxRefs (prevents double-spend).
                    // Keep events whose transactions are still unconfirmed (prevents starvation).
                    _ <- reconcileMempool(eventMempool, context, logger)

                    _ <- stateRef.update(
                      _.copy(
                        networkTipOrdinal = snap.ordinal,
                        networkTipHash = Some(Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))),
                        localTipOrdinal = snap.ordinal
                      )
                    )

                    _ <- productionGate.resume("catch-up-sync")
                    _ <- Metrics[F].incrementCounter("dag_nakamoto_catchups")
                    _ <- logger.info(
                      s"\u2705 CATCH-UP complete: now at ordinal=${snap.ordinal} slot=${snap.slot}. " +
                        s"Subsequent gossip should find parents."
                    )

                    // Start backfill daemon in background to fill the gap from caught-up tip down to genesis.
                    // The parent hash of the caught-up snapshot is the starting point for the walk-back.
                    _ <- {
                      val cursor = BackfillDaemon.BackfillCursor(
                        nextHashToFetch = parentHash.value,
                        targetOrdinal = 1L,
                        currentOrdinal = snap.ordinal,
                        startedAtOrdinal = snap.ordinal,
                        completedChunks = Set.empty,
                        createdAtMs = System.currentTimeMillis()
                      )
                      // Scope the backfill fiber to the app Supervisor so shutdown cancels cleanly.
                      // Previously a raw Async.start leaked a fiber that outlived the owning daemon.
                      supervisor
                        .supervise(
                          BackfillDaemon
                            .run[F](cursor, channel, snapshotStorage, productionGate, dataDir)
                            .handleErrorWith(e => logger.warn(s"Backfill daemon failed: ${e.getMessage}"))
                        )
                        .void
                    }
                  } yield ()
              }
            } yield ()
          case None =>
            logger.warn(
              s"\u274c No parent found for ordinal=${snap.ordinal} and no payload to catch up from"
            )
        }
      }
    }
  }
}
