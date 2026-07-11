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
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointAcceptResult
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.EcVrf25519

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

  // #259 metagraph-binary active-recovery (stuck-detection tick) tuning. Fixed internal cadence —
  // NOT a consensus-critical value (recovery is additive + re-gated), so kept as plain constants
  // rather than HOCON config (mirrors CatchUpThreshold above).
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

  /** Decode the greenfield ChainSync payload shape. `snapshot` is required and `context` is optional; the latter is transport metadata
    * only. The caller requires a validated parent context and runs full replay before any chain or MPT write.
    */
  private[nakamoto] def decodeFetchedSnapshotPayload(
    payload: Array[Byte]
  ): Option[Signed[GlobalIncrementalSnapshot]] =
    if (payload.isEmpty) None
    else
      io.circe.parser
        .parse(new String(payload, java.nio.charset.StandardCharsets.UTF_8))
        .toOption
        .flatMap(_.hcursor.get[Signed[GlobalIncrementalSnapshot]]("snapshot").toOption)

  /** Bind every routing/eligibility field in the protobuf envelope to the signed snapshot body before the envelope can select a parent,
    * borrow a producer's stake, pass KES, or provide chain-store metadata. The sidecar treats protobuf fields as opaque and
    * unauthenticated; the signed body and its slot certificate are the only source of truth.
    */
  private[nakamoto] def validateSnapshotEnvelope[F[_]: Async: HasherSelector](
    snap: pb.Snapshot,
    signedSnapshot: Signed[GlobalIncrementalSnapshot]
  ): F[Either[String, Unit]] =
    HasherSelector[F].withCurrent { implicit hasher =>
      signedSnapshot.toHashed[F].map { hashed =>
        val body = signedSnapshot.value
        val transportHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val transportParent = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val producerBytes = snap.producerId.toByteArray
        val signerMatches = signedSnapshot.proofs.toNonEmptyList.exists { proof =>
          java.util.Arrays.equals(proof.id.hex.toBytes, producerBytes)
        }

        def sameBytes(left: Array[Byte], right: Array[Byte]): Boolean =
          java.util.Arrays.equals(left, right)

        val commonChecks = List(
          Either.cond(transportHash === hashed.hash, (), s"transport hash ${transportHash.value} != signed body hash ${hashed.hash.value}"),
          Either.cond(
            snap.ordinal == body.ordinal.value.value,
            (),
            s"transport ordinal ${snap.ordinal} != signed ordinal ${body.ordinal.value.value}"
          ),
          Either.cond(
            transportParent === body.lastSnapshotHash,
            (),
            s"transport parent ${transportParent.value} != signed parent ${body.lastSnapshotHash.value}"
          ),
          Either.cond(signerMatches, (), "transport producer_id is not a signer of the snapshot body")
        )

        val certificateChecks = body.slotCertificate match {
          case None =>
            List(Left("signed snapshot is missing its required slot certificate"))
          case Some(cert) =>
            val derivedVrfOutput = vrf.vrfProofToHash(snap.vrfProof.toByteArray)
            List(
              Either
                .cond(cert.slot.value.value == snap.slot, (), s"certificate slot ${cert.slot.value.value} != transport slot ${snap.slot}"),
              Either.cond(
                cert.parentSlot.value.value == snap.parentSlot,
                (),
                s"certificate parentSlot ${cert.parentSlot.value.value} != transport parentSlot ${snap.parentSlot}"
              ),
              Either.cond(sameBytes(cert.vrfProof.toBytes, snap.vrfProof.toByteArray), (), "certificate VRF proof != transport proof"),
              Either.cond(
                sameBytes(cert.vrfPublicKey.toBytes, snap.vrfPublicKey.toByteArray),
                (),
                "certificate VRF public key != transport key"
              ),
              Either.cond(
                Hex.fromBytes(snap.eta.toByteArray).value == cert.eta.value,
                (),
                "certificate eta != transport eta"
              ),
              Either.cond(body.eta.contains(cert.eta), (), "snapshot eta != slot-certificate eta"),
              Either.cond(
                derivedVrfOutput.exists(sameBytes(_, cert.vrfOutput.toBytes)),
                (),
                "certificate VRF output is not derived from its proof"
              )
            )
        }

        (commonChecks ++ certificateChecks).collectFirst { case Left(reason) => reason }.toLeft(())
      }
    }

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
    * its own attestation) ONLY for a checkpoint that passed `ShardCheckpointGl0AcceptanceManager.evaluate`'s signer checks and mandatory
    * replay. A `Rejected` (pre-check/unavailable-replay failure) or `RejectedReExecutionMismatch` (wrong derivation) checkpoint is NOT
    * admissible — never adopted, signers never counted toward quorum, no attestation emitted. `PendingMoreAttestations` is admissible
    * (replay-valid but still waiting for quorum/depth selection finality).
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

  /** Signed-byte-store backfill transport (2026-07-09) — pull a peer's SIGNED MPT byte map at an EXACT finalized `ordinal` over the
    * by-ordinal `/global-snapshots/<ord>/mpt-entries` route. Consumed by `PinnedCurrencyInfoReader.PinnedByteBackfill` to heal HOLES in the
    * local signed store at a stamped shard-checkpoint `executionBaseOrdinal`.
    *
    * TRANSPORT-ONLY, deliberately UNVERIFIED here: the pinned reader verifies `sidecarFreeMptRoot(fetched) === its OWN locally-committed
    * `stateProof.mptRoot@ordinal`` before anything is staged or served. Byte integrity comes from that root gate, not the transport. Tries
    * up to `maxPeers` responsive peers (sorted by peer id for stable behavior — the VERIFIED outcome is peer-independent, root-determined)
    * with a per-try `perPeerTimeout` so a hung peer cannot stall the accept fold; a peer without the ordinal 404s and the next is tried.
    * `None` on total miss — the caller stays fail-closed. `maxPeers = 0` disables the transport outright (config kill-switch). Never
    * raises.
    */
  private[snapshot] def pullMptEntriesAtOrdinalFromPeer[F[_]: Async](
    l0GlobalSnapshotClient: io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient[F],
    clusterStorage: io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage[F],
    maxPeers: Int,
    perPeerTimeout: scala.concurrent.duration.FiniteDuration,
    logger: org.typelevel.log4cats.Logger[F]
  )(ordinal: SnapshotOrdinal): F[Option[Map[Hex, Array[Byte]]]] = {
    def pullFrom(p: io.constellationnetwork.schema.peer.Peer): F[Option[Map[Hex, Array[Byte]]]] =
      Async[F]
        .timeoutTo(
          l0GlobalSnapshotClient
            .getMptEntriesAt(ordinal)
            .run(io.constellationnetwork.schema.peer.L0Peer.fromPeer(p))
            .map(_.some),
          perPeerTimeout,
          (None: Option[Map[Hex, Array[Byte]]]).pure[F]
        )
        .handleErrorWith { e =>
          logger
            .debug(
              s"[backfill] signed-bytes pull ord=${ordinal.show} from peer ${p.id.show.take(16)} failed (${e.getMessage}), trying next"
            )
            .as(None: Option[Map[Hex, Array[Byte]]])
        }

    def tryInOrder(remaining: List[io.constellationnetwork.schema.peer.Peer]): F[Option[Map[Hex, Array[Byte]]]] =
      remaining match {
        case Nil => (None: Option[Map[Hex, Array[Byte]]]).pure[F]
        case p :: tail =>
          pullFrom(p).flatMap {
            case some @ Some(_) => (some: Option[Map[Hex, Array[Byte]]]).pure[F]
            case None           => tryInOrder(tail)
          }
      }

    if (maxPeers <= 0) (None: Option[Map[Hex, Array[Byte]]]).pure[F]
    else
      clusterStorage.getResponsivePeers
        .flatMap(peers => tryInOrder(peers.toList.sortBy(_.id.show).take(maxPeers)))
        .handleErrorWith(e =>
          logger.warn(e)(s"[backfill] could not pull signed MPT bytes at ord=${ordinal.show} from any peer (fail-closed defer)").as(None)
        )
  }

  /** Append a parent-missing item to the orphan buffer without authorizing any state transition. The only production consumer of the
    * buffered value is [[drainBufferedChildren]], which re-enters the normal validation path after its parent has been stored.
    */
  private[nakamoto] def bufferPendingChild[F[_], A](
    missingParent: Hash,
    child: A,
    pendingParentRef: Ref[F, Map[Hash, List[A]]]
  ): F[Unit] =
    pendingParentRef.update { pending =>
      pending.updated(missingParent, pending.getOrElse(missingParent, List.empty) :+ child)
    }

  /** Run storage, canonical writes, and attestation effects only after full replay validation returned [[NakamotoSnapshotValidator.Valid]].
    * Non-valid results have no callback and therefore cannot reach those effects. Returns whether the callback ran so the caller can emit
    * result-specific diagnostics without duplicating the validity gate.
    */
  private[nakamoto] def commitReplayValidated[F[_]: cats.Monad](
    result: NakamotoSnapshotValidator.ValidationResult
  )(
    commit: NakamotoSnapshotValidator.Valid => F[Unit]
  ): F[Boolean] =
    result match {
      case valid: NakamotoSnapshotValidator.Valid => commit(valid).as(true)
      case _                                      => false.pure[F]
    }

  /** Atomically remove children waiting on `storedHash`, then process them sequentially in insertion order. `processChild` is the normal
    * snapshot handler in production; a child that fails replay never stores and therefore never invokes another drain, leaving its
    * descendants buffered behind the invalid hash.
    */
  private[nakamoto] def drainBufferedChildren[F[_]: cats.Monad, A](
    storedHash: Hash,
    pendingParentRef: Ref[F, Map[Hash, List[A]]]
  )(
    onDrain: List[A] => F[Unit],
    processChild: A => F[Unit]
  ): F[Unit] =
    pendingParentRef.modify { pending =>
      val children = pending.getOrElse(storedHash, List.empty)
      (pending - storedHash, children)
    }.flatMap { children =>
      onDrain(children) >> children.traverse_(processChild)
    }

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
    // 3c-A enabler — signed-bytes staging map. NO LONGER validate-only: the ADOPT paths (reorg / realign / legacy catch-up) now stage
    // their root-verified byte maps here too (signed-byte-store FIDELITY, 2026-07-09 — adopted ordinals must not stay permanent holes in
    // `mpt_snapshot_info_signed`, or `pinnedReaderAt(executionBaseOrdinal)` fail-closes on every node that adopted that ordinal).
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
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
    // EXECUTION-SHARDING: per-shard admission-gated binary buffers — the producer fan-out input.
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
    drainBufferedChildren(storedHash, pendingParentRef)(
      children =>
        if (children.isEmpty) Async[F].unit
        else
          logger.info(
            s"🔗 Draining ${children.size} buffered snapshot(s) whose parent ${storedHash.value.take(12)} is now available"
          ),
      childSnap =>
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
          pendingPostBytesRef,
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
    )

  /** Reconcile DAG events after a replay-validated snapshot becomes the canonical tip.
    *
    * Transactions whose parent no longer matches canonical GL0 state cannot become valid on the selected branch. Other event types remain
    * queued for their own validation paths.
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
                val transaction = signedTx.value
                val canonicalRef = context.lastTxRefs.getOrElse(transaction.source, TransactionReference.empty)
                canonicalRef =!= TransactionReference.empty && canonicalRef =!= transaction.parent
              }
              Option.when(isStale)(hash)
            case _ => None
          }
      }.flatten.toSet
      _ <- eventMempool.remove(staleHashes).whenA(staleHashes.nonEmpty)
      _ <- logger.info(s"Mempool reconciliation evicted ${staleHashes.size} stale DAG event(s)")
    } yield ()

  final case class SyncState(
    networkTipOrdinal: Long,
    networkTipHash: Option[Hash],
    localTipOrdinal: Long,
    isReady: Boolean,
    lastCatchUpAttemptMs: Long = 0L,
    // Highest ordinal a Tier-3 catch-up has ADOPTED (max-monotone). A catch-up stores the pulled snapshot as an ORPHAN — its
    // ancestors aren't in the chain store until the BackfillDaemon connects them — so `chainStore.bestTipOrdinal` keeps reporting the
    // OLD connected tip (chain-selection prefers the connected chain over the orphan). The Tier-3 gap-check maxes bestTip with THIS so
    // a just-adopted catch-up can't immediately re-fire Tier-3 over the same window (the orphan-adopt livelock); subsequent gossip
    // falls to Tier-2 walk-back, which connects the gap, instead of re-teleporting forever.
    lastCatchUpAdoptedOrdinal: Long = 0L
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
    // 3c-A enabler — signed-bytes staging map used by replay validation. Authoritative recovery paths do not write it.
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
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
    // WATCHTOWER (fraud-proof part 1): re-execute each ADOPTED checkpoint on the quorum path and gossip a
    // FraudProofEnvelope on a per-MG root mismatch (catches a quorum-signed wrong root). `None` at numShards=1
    // or `watchtower-enabled=false`. Built on the gl0-leader-produce path where the operator's signing material
    // + the SAME acceptance manager (its PIN-1 re-exec closure) live.
    watchtowerFraudProofEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]
    ] = None,
    // WATCHTOWER (fraud-proof part 2): DETERMINISTIC dispute verdict. On receiving a `FraudProofEnvelope`, every
    // gl0 INDEPENDENTLY re-runs this over the disputed checkpoint's OWN bytes (never trusting the challenger) and
    // decides UPHELD iff attested ≠ honest-re-derived. `None` at numShards=1.
    invalidStateProofValidator: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]
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
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]] = None,
    // WATCHTOWER fraud-proof POOL (W3a): `handleFraudProof` OFFERS a locally-UPHELD inbound dispute here so the gl0 leader producer embeds it
    // in the next snapshot's `fraudProofs` consensus field (where EVERY node re-validates + slashes it deterministically). The sole
    // production wiring passes the shared instance; at `numShards = 1` it passes `WatchtowerFraudProofPool.noop[F]` ⇒ offers discard ⇒ no
    // fraud proofs ever embedded ⇒ byte-identical regression bar. Required (no default — `noop` needs `Sync[F]`, not summonable at a
    // default-arg site).
    fraudProofPool: io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool[F]
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

    // Per-shard admission-approved binary buffers, projected off the SAME registry. The metagraph gate
    // writes into these only after quorum admission; the producer fan-out reads them. This
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
                                pendingPostBytesRef,
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
                                  pendingPostBytesRef,
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

                    // FINDING-F1: the daemon declares the exact message families it consumes — every family
                    // EXCEPT rumor (rumors belong to SidecarRumorBridge). The shard-checkpoint families ride
                    // the sidecar's SHARED fan-in channels (exactly-one-drainer semantics); before this filter
                    // the rumor bridge's subscribe-all stream race-drained ~half of them and its `isRumor`
                    // collect silently discarded the wins. With explicit topic sets on both streams the daemon
                    // is the shard channels' only drainer by construction.
                    val gossip = GossipStream
                      .subscribe[F](channel, SidecarClient.SubscribeTopics.daemonTopics)
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
                              // (30s default) waiting for configured `kQuorum` committee attestations. Running it on
                              // the gossip stream's `evalMap` thread serializes EVERY message behind
                              // every pending gate — gl0 TipAttestations from peers then arrive past
                              // `TipTracker.MaxAttestationSkewMs` and get rejected (skew=200+s observed
                              // in iter-s3prep-baseline). Gate work is naturally concurrent-safe: the
                              // aggregator is a `Ref[F, ...]`, `processMetagraphBinary` writes to a
                              // queue, and per-binary state is keyed by (mgAddr, parentHash, binaryHash).
                              // The gate-aware processor is also the sole writer to execution-shard buffers. A raw
                              // gossip receipt is never enough to make a binary eligible for a shard checkpoint.
                              Async[F]
                                .start(
                                  handleMetagraphBinary(mb, processOrphanedMetagraphBinary, logger)
                                )
                                .void

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
                                    watchtowerFraudProofEmitter,
                                    shardCheckpointFetcher,
                                    selfId,
                                    logger
                                  )
                                )
                                .void

                            case pb.GossipMessage.Body.ShardCheckpointAttestation(att) =>
                              Async[F].start(handleShardCheckpointAttestation(att, shardAcceptanceDeps, logger)).void

                            case pb.GossipMessage.Body.FraudProof(fp) =>
                              // WATCHTOWER dispute consumer (part 2): background-fire the DETERMINISTIC verdict. Every gl0 runs the
                              // identical re-derivation over the disputed checkpoint's own bytes; the verdict is recomputed, never trusted.
                              // On a locally-UPHELD verdict the validated evidence is OFFERED into `fraudProofPool` so the gl0 leader embeds it
                              // as the `fraudProofs` consensus artifact (W3a) — where the on-chain GSAM re-validates it + applies the slash.
                              Async[F]
                                .start(handleFraudProof(fp, shardAcceptanceDeps, invalidStateProofValidator, fraudProofPool, logger))
                                .void

                            case _: pb.GossipMessage.Body.Rumor =>
                              // Not requested by `daemonTopics` (rumors are the SidecarRumorBridge's family);
                              // kept as a defensive no-op should the sidecar ever misroute one.
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
                                                  watchtowerFraudProofEmitter,
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
    // 3c-A enabler — signed-bytes staging map. NO LONGER validate-only: the ADOPT paths (reorg / realign / legacy catch-up) now stage
    // their root-verified byte maps here too (signed-byte-store FIDELITY, 2026-07-09 — adopted ordinals must not stay permanent holes in
    // `mpt_snapshot_info_signed`, or `pinnedReaderAt(executionBaseOrdinal)` fail-closes on every node that adopted that ordinal).
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
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
    // Per-shard admission-approved binary buffers — the producer fan-out input.
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
      parsed = decodeFetchedSnapshotPayload(snap.payload.toByteArray)
      envelopeError <- parsed match {
        case Some(signedSnapshot) =>
          validateSnapshotEnvelope[F](snap, signedSnapshot).map(_.left.toOption)
        case None =>
          Async[F].pure(Option.empty[String])
      }
      metadataBound = parsed.nonEmpty && envelopeError.isEmpty

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
      // Key on the PARENT ordinal (snap.ordinal - 1) to match the producer, which keys on
      // its bestTip (= parent) ordinal — using `snap.ordinal` directly causes an off-by-one
      // at every R boundary. Combined with the Cardano/Praos bootstrap (periods 0 and 1 =
      // bootstrapEta, no VRF fold), the first rotation no longer depends on period-0 outputs
      // at all. See `docs/nakamoto/attestation-and-finality.md` §1.
      currentPeriod = EtaCalculation.rotationPeriod(math.max(0L, snap.ordinal - 1), etaRotationSnapshots)
      eta <-
        if (!metadataBound) {
          Async[F].pure(Array.emptyByteArray)
        } else if (currentPeriod <= 1) {
          // Cardano/Praos bootstrap: periods 0 and 1 are genesis-derivable (distinct, no VRF dependency).
          // Every honest verifier computes the identical value with no chain-walk, so the period 0→1
          // boundary can't fork on disagreement about period 0's VRF outputs (the ord≈R / iter35 wedge).
          Async[F].pure(EtaCalculation.bootstrapEta(genesisEta, currentPeriod))
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
              parsed.flatMap(_.value.eta) match {
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
                  // No embedded eta and no chain data (period >= 2) — per-period bootstrap value
                  Async[F].pure(EtaCalculation.bootstrapEta(genesisEta, currentPeriod))
              }
            }
          }
        }

      // KES is a store-boundary validity condition. Verify it before global replay and before `chainStore.store`; otherwise an invalid-KES
      // candidate can become bestTip and later receive attestations even though `processValidSnapshot` declines its state update.
      kesOk <-
        if (!metadataBound) Async[F].pure(false)
        else
          KesGossipVerification.verifySnapshot(
            messageBytes = snap.hash.toByteArray,
            kesSigBytes = snap.kesSignature.toByteArray,
            producerId = peer.PeerId(Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString)),
            producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString),
            ordinal = snap.ordinal,
            kesRegistry = kesRegistry,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )

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
      validationResult <-
        if (parsed.isEmpty)
          Async[F].pure(NakamotoSnapshotValidator.PayloadMissing(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
        else if (envelopeError.nonEmpty)
          logger
            .warn(s"Rejecting snapshot with unbound transport metadata: ${envelopeError.get}")
            .as(NakamotoSnapshotValidator.ContentMismatch(s"transport: ${envelopeError.get}"): NakamotoSnapshotValidator.ValidationResult)
        else if (!kesOk)
          Async[F].pure(NakamotoSnapshotValidator.KesInvalid(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
        else
          parsed match {
            case Some(signedSnapshot) =>
              chainStore.get(parentHash).flatMap {
                case Some(parentStored) =>
                  NakamotoSnapshotValidator.validate[F](
                    signedSnapshot = signedSnapshot,
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
                    pendingAccumulatorsRef = pendingAccumulatorsRef,
                    pendingPostBytesRef = pendingPostBytesRef
                  )
                case None =>
                  // Parent not in chain store. Three-tier gap handling:
                  // Tier 1 (<=6): buffer + ChainSync parent fetch (normal gossip latency)
                  // Tier 2 (>6, <=k): sequential walk-back via ChainSync (moderate drift)
                  // Tier 3 (>k): full catch-up — network finalized past us
                  (chainStore.bestTipOrdinal, stateRef.get).flatMapN { (localBestOrdinal, syncSt) =>
                    // Orphan-adopt livelock guard: a Tier-3 catch-up stores the pulled snapshot as an ORPHAN, so `bestTipOrdinal` keeps
                    // reporting the OLD connected tip until the BackfillDaemon links the ancestors. Max it with the highest ordinal a
                    // catch-up has already adopted so we DON'T re-teleport over the same window every gossip (the livelock observed under a
                    // gossip flood); once a catch-up has adopted ord M, the residual gap to the tip is closed by Tier-2 walk-back instead.
                    val localOrd = math.max(localBestOrdinal.getOrElse(0L), syncSt.lastCatchUpAdoptedOrdinal)
                    val gap = snap.ordinal - localOrd
                    if (gap > confirmationDepthK) {
                      logger.warn(
                        s"Tier 3: gap=$gap > k=$confirmationDepthK for ordinal=${snap.ordinal}. " +
                          s"Buffering until ancestry is available for full global replay; producer-carried state is not authoritative."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    } else if (gap > CatchUpThreshold) {
                      logger.info(
                        s"⏳ Tier 2: gap=$gap (>$CatchUpThreshold, <=$confirmationDepthK) for ordinal=${snap.ordinal}. " +
                          s"Sequential walk-back from parent ${parentHash.value.take(12)}."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    } else {
                      logger.info(
                        s"⏳ Tier 1: Parent ${parentHash.value.take(12)} not in chain store for ordinal=${snap.ordinal} (gap=$gap). Buffering."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    }
                  }
              }
            case None =>
              Async[F].pure(NakamotoSnapshotValidator.PayloadMissing(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
          }
      replayCommitted <- commitReplayValidated(validationResult) { valid =>
        for {
          validHash <- HasherSelector[F].withCurrent(implicit h => valid.snapshot.toHashed[F].map(_.hash))
          _ <- SnapshotKesStorage.put[F](dataDir, validHash, snap.kesSignature.toByteArray)
          isNew <- chainStore.store(
            valid.snapshot,
            valid.context,
            snap.ordinal,
            snap.slot,
            parentHash,
            vrfOutputFromProof(snap.vrfProof.toByteArray)
          )
          bestTipOpt <- chainStore.bestTip
          becameBest = isNew && bestTipOpt.exists(_.hash === validHash)
          _ <- Async[F].whenA(isNew && !becameBest) {
            logger.info(
              s"🔀 Stored Nakamoto snapshot at ordinal=${snap.ordinal} as fork branch (not bestTip) — overlay isolates pending branches from canonical."
            )
          }
          _ <- processValidSnapshot(
            snap,
            valid.snapshot.some,
            valid.context.some,
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
            eventMempool,
            operationalKeyMaker,
            kesRegistry,
            shardProducers,
            shardChainStores,
            shardBinaryBuffers,
            shardAssignment,
            shardCommitteeMembership,
            logger
          )
          // A replay-valid snapshot has passed the same store/processing boundary as live gossip. Re-enter every waiting child through
          // `handleSnapshot`; a rejected child cannot invoke this drain for its own descendants.
          storedHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
          _ <- drainPendingChildren(
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
            pendingPostBytesRef,
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
        } yield ()
      }
      _ <- Async[F].unlessA(replayCommitted) {
        (validationResult: NakamotoSnapshotValidator.ValidationResult) match {
          case NakamotoSnapshotValidator.ParentNotFound =>
            // Fail closed. Parentless state cannot be authorized by the producer's signature or by reproducing its self-claimed root.
            Metrics[F].incrementCounter("dag_nakamoto_parentless_snapshots_rejected") >>
              logger.warn(
                s"Rejecting parentless snapshot ordinal=${snap.ordinal} slot=${snap.slot}; ancestry must be fetched and the transition replayed"
              )
          case cm: NakamotoSnapshotValidator.ContentMismatch =>
            Metrics[F].incrementCounter("dag_nakamoto_content_mismatch_rejected") >>
              logger.warn(
                s"Rejecting globally replay-invalid snapshot ordinal=${snap.ordinal} slot=${snap.slot}: ${cm.detail}"
              )
          case NakamotoSnapshotValidator.ParentBuffered =>
            // Already buffered for validation when parent arrives — nothing more to do.
            Async[F].unit
          case invalid: NakamotoSnapshotValidator.Invalid =>
            Metrics[F].incrementCounter("dag_nakamoto_snapshots_rejected") >>
              logger.warn(s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal}: $invalid")
          case _: NakamotoSnapshotValidator.Valid =>
            // `commitReplayValidated` returns true for every Valid result unless its effect raises.
            Async[F].unit
        }
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
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
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
    // Per-shard admission-approved binary buffers — the producer fan-out input.
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
  ): F[Unit] =
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
      eventMempool,
      operationalKeyMaker,
      kesRegistry,
      shardProducers,
      shardChainStores,
      shardBinaryBuffers,
      shardAssignment,
      shardCommitteeMembership,
      logger
    )

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
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
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
    // Per-shard admission-approved binary buffers — the producer fan-out input.
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
                  Metrics[F].recordDistribution("dag_nakamoto_slot_gap", (snap.slot - snap.parentSlot).toInt) >>
                  reconcileMempool(eventMempool, ctx, logger)
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
      // cadence inversion: it advanced shard producer duty only once per canonical GL0 ordinal (the GL0 leader
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
    // WATCHTOWER (fraud-proof part 1): re-execute each adopted checkpoint EVEN on the quorum path and gossip a
    // FraudProofEnvelope on a per-MG root mismatch. `None` at numShards=1 or when watchtower-enabled=false ⇒ no
    // approval-check. Fired on the became-best-tip adopt seam (same gate as the attestation emitter).
    watchtowerFraudProofEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]
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
                // Slot is the signed wire clock used by deterministic staircase duty and the registered-key possession proof.
                // `vrfOutput` is reconstructed only as a chain-selection input from that proof.
                val localSlot = checkpoint.slot.value.value
                val vrfOut = vrfOutputFromProof(checkpoint.committeeSignatures.head.vrfProof.toBytes)
                HasherSelector[F].withCurrent { implicit hasher =>
                  hasher.hash(checkpoint.signingPreimage).flatMap { checkpointHash =>
                    // §5.7 slot validity bound — strictly monotone vs the parent's wire slot (when the parent is
                    // known; orphans are bounded retroactively when the parent connects via the chain-store's
                    // connectivity gate). Deflating the slot is the only profitable direction (maxvalid-tk prefers
                    // LOWER slot on ties) and this check closes it; inflating is self-defeating. The wall-clock skew
                    // bound (`slot <= now + eps`) is enforced during cryptographic checkpoint validation.
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
                                          watchtowerFraudProofEmitter,
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
                          // (and adopt) a checkpoint whose derivation it had not re-run. `evaluate` now runs `reExecuteDerivation`
                          // unconditionally, before either quorum or depth can qualify selection. A `Rejected`/`RejectedReExecutionMismatch` result
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
                                                    watchtowerFraudProofEmitter,
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
                                      // WATCHTOWER approval-check (fraud-proof part 1): on adopting this checkpoint as canonical best tip,
                                      // re-execute its per-MG derivations EVEN THOUGH it was quorum-admitted (the whole point — catch a
                                      // quorum-signed wrong root) and gossip a FraudProofEnvelope on any mismatch. Background-fire so the
                                      // multi-step re-exec + sign + publish never blocks this handler; `None` (numShards=1 / watchtower
                                      // disabled) ⇒ skipped. Only on becameBestTip: a non-canonical sibling is not adopted, so its effects
                                      // are never applied — no need to dispute it (and re-deriving it against our S(N) could false-mismatch).
                                      Async[F].whenA(becameBestTip) {
                                        watchtowerFraudProofEmitter match {
                                          case None          => Async[F].unit
                                          case Some(emitter) => Async[F].start(emitter.emit(checkpoint)).void
                                        }
                                      } >>
                                        // Record every committee signer (the producer's own sig seeds 1 attestation) into the tip tracker —
                                        // only now that re-exec validated the checkpoint (so a wrong-derivation envelope never inflates quorum).
                                        checkpoint.committeeSignatures.toList
                                          .traverse_(sig => entry.tipTracker.recordAttestation(checkpointHash, sig.peerId, sig)) >>
                                        // T_count_shard closure: sign + gossip our attestations so every other node's tracker
                                        // can reach configured `kQuorum`. `None`
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
                                                                      // Ancestors are subject to the same execution gate as the received tip. A stored/best-chain
                                                                      // position is not evidence that its currency transition is valid.
                                                                      deps.acceptanceManager.verifyEmbedded(ancCp).flatMap {
                                                                        case ShardCheckpointAcceptResult.Accepted =>
                                                                          logger.info(
                                                                            s"ShardCheckpoint attest-ancestor after re-exec: " +
                                                                              s"shard=${ancCp.shardId.value.value} shardOrd=${ancCp.shardOrdinal.value} " +
                                                                              s"slot=${ancCp.slot.value.value}"
                                                                          ) >> emitter
                                                                            .emit(ancCp.shardId, anc.hash, ancCp.slot, ancCp.epoch)
                                                                        case result =>
                                                                          logger.warn(
                                                                            s"ShardCheckpoint skip ancestor attestation: " +
                                                                              s"shard=${ancCp.shardId.value.value} shardOrd=${ancCp.shardOrdinal.value} " +
                                                                              s"executionResult=$result"
                                                                          )
                                                                      }
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

  /** Gap B — handle an incoming `ShardCheckpointAttestationWire` from gossip. Decode, resolve the referenced checkpoint from the local
    * shard store, then run the SAME full per-signer pre-check as checkpoint admission (committee membership + Ed25519 + KES + VRF) before
    * recording the attester into the per-shard tip tracker. `None` deps (numShards=1) ⇒ drop with a debug log.
    *
    * '''Why every predicate runs before recording.''' The gl0 leader splices every tracker signature into a candidate checkpoint, and
    * `verifyEmbedded` rejects the whole checkpoint if any signer fails any pre-check. Recording an Ed25519-valid outsider (or a committee
    * member's first-seen signature with invalid KES/VRF bytes) would therefore poison that checkpoint indefinitely. Unknown hashes are
    * dropped rather than cached: without the checkpoint bytes there is no shard/epoch/slot context in which committee, KES, or VRF can be
    * verified. Only a signature accepted by `verifyCommitteeSignature` reaches the tracker.
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
                entry.chainStore.getByHash(attestation.checkpointHash).flatMap {
                  case None =>
                    logger.warn(
                      s"Rejecting ShardCheckpointAttestation for unknown checkpoint shard=${attestation.shardId.value.value} " +
                        s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}..."
                    )
                  case Some(stored) if stored.signed.value.shardId =!= attestation.shardId =>
                    logger.warn(
                      s"Rejecting ShardCheckpointAttestation with shard mismatch wire=${attestation.shardId.value.value} " +
                        s"checkpoint=${stored.signed.value.shardId.value.value} hash=${attestation.checkpointHash.value.take(12)}"
                    )
                  case Some(stored) =>
                    deps.acceptanceManager
                      .verifyCommitteeSignature(stored.signed.value, attestation.attesterSignature)
                      .flatMap {
                        case Left(reason) =>
                          logger.warn(
                            s"Rejecting invalid ShardCheckpointAttestation shard=${attestation.shardId.value.value} " +
                              s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}... " +
                              s"reason=$reason"
                          )
                        case Right(()) =>
                          entry.tipTracker.recordAttestation(attestation.checkpointHash, attesterId, attestation.attesterSignature) >>
                            logger.debug(
                              s"ShardCheckpointAttestation rx shard=${attestation.shardId.value.value} " +
                                s"checkpoint=${attestation.checkpointHash.value.take(12)} attester=${attesterId.value.value.take(12)}"
                            )
                      }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle ShardCheckpointAttestation: ${err.getMessage}")
          }
    }

  /** WATCHTOWER dispute consumer (fraud-proof part 2) — handle an incoming `FraudProofEnvelopeWire`.
    *
    * '''Deterministic verdict, recomputed not trusted.''' Decode the envelope, resolve the disputed `ShardCheckpoint` from the local
    * per-shard chain store BY HASH (the checkpoint the committee signed — its bytes are the verdict's inputs), build the
    * [[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]], and run
    * [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator]] — which re-derives the honest per-MG root
    * from the checkpoint's OWN `includedSnapshots` (pure / finalized-base) and upholds iff it differs from the committee-attested root. The
    * verdict is identical on every gl0 because it recomputes; the challenger's claimed roots in the envelope are never read for the
    * verdict.
    *
    * '''On UPHELD''': log loudly (this slice surfaces the verdict + the slash-target committee). The AUTHORITATIVE 100% slash is applied by
    * the GSAM accept path when an `InvalidStateProofEvidence` (carrying the full checkpoint) lands in a global snapshot — submitting that
    * evidence to the L0 mempool is the remaining wiring (see the GSAM `InvalidStateProofSlashManager` sink + its TODO). On NOT-upheld (the
    * honest-committee floor) / any rejection, log + drop — a frivolous or forged fraud proof has no effect.
    *
    * '''Checkpoint not in local store''': we cannot re-derive (the checkpoint may have been pruned, or we never tracked this shard). Drop
    * with a debug log — the on-chain evidence path carries the full checkpoint and does NOT depend on local availability.
    *
    * `None` deps / validator (numShards=1) ⇒ drop with a debug log.
    */
  private def handleFraudProof[F[_]: Async: JsonSerializer: HasherSelector: SecurityProvider: Metrics](
    fpWire: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.FraudProofEnvelopeWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    invalidStateProofValidator: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]
    ],
    fraudProofPool: io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    (shardAcceptanceDeps, invalidStateProofValidator) match {
      case (Some(deps), Some(validator)) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.FraudProofWireCodecs
          .fromWire[F](fpWire)
          .flatMap { fp =>
            deps.registry.get(fp.shardId) match {
              case None =>
                logger.debug(s"🛡️ FraudProof for untracked shard=${fp.shardId.value.value}; dropping")
              case Some(entry) =>
                entry.chainStore.getByHash(fp.disputedCheckpointHash).flatMap {
                  case None =>
                    logger.debug(
                      s"🛡️ FraudProof: disputed checkpoint ${fp.disputedCheckpointHash.value.take(12)} not in local store " +
                        s"(shard=${fp.shardId.value.value}); cannot re-derive verdict locally — dropping (on-chain evidence carries it)"
                    )
                  case Some(hashedCp) =>
                    val cp = hashedCp.signed.value
                    val attested = cp.derivedStateDelta.perMetagraphMptRoots
                      .get(fp.metagraphAddress)
                      .getOrElse(io.constellationnetwork.security.hash.Hash.empty)
                    val evidence = io.constellationnetwork.schema.slashing.InvalidStateProofEvidence(
                      shardId = fp.shardId,
                      disputedCheckpoint = cp,
                      metagraphAddress = fp.metagraphAddress,
                      attestedRoot = attested,
                      fraudProof = fp
                    )
                    validator.validate(evidence).flatMap {
                      case Right(upheld) =>
                        // UPHELD locally — OFFER the validated evidence into the node-local pool so the gl0 leader embeds it as the
                        // `fraudProofs` consensus artifact (W3a). The on-chain GSAM accept path re-validates it DETERMINISTICALLY on every
                        // node and applies the 100% slash + bounty to the challenger; this offer is a pure liveness aid (the authoritative
                        // verdict + slash are the consensus fold, never this pool). The set keys on the dispute's `(shardId, checkpointHash)`.
                        fraudProofPool.offer(upheld) >>
                          logger.warn(
                            s"🛡️ WATCHTOWER dispute UPHELD: shard=${fp.shardId.value.value} " +
                              s"checkpoint=${fp.disputedCheckpointHash.value.take(12)} mg=${fp.metagraphAddress.value.value.take(10)} " +
                              s"slashTargets=${evidence.slashTargets.size} — committee signed a wrong derivation (queued for on-chain 100% " +
                              s"InvalidStateProof slash via the fraudProofs consensus artifact)"
                          )
                      case Left(rejection) =>
                        logger.info(
                          s"🛡️ WATCHTOWER dispute NOT upheld (no slash): shard=${fp.shardId.value.value} " +
                            s"checkpoint=${fp.disputedCheckpointHash.value.take(12)} reason=$rejection"
                        )
                    }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle FraudProof: ${err.getMessage}")
          }
      case _ =>
        logger.debug("Received FraudProof but sharding/watchtower inactive (numShards=1); dropping")
    }

  /** Route an incoming state channel binary from gossip into the [[MetagraphCommitteeGate]] — the gate computes the committee sortition for
    * this node, emits an attestation if selected, and waits for configured `kQuorum` attestations to land in the aggregator before
    * admitting the binary into the local acceptance pipeline.
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
  def makeMetagraphBinaryProcessor[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector: Metrics](
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
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ] = Map.empty[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]] = None,
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
                      // Occupancy gauge (eval-instrumentation): the orphan backlog is the leading indicator
                      // of a metagraph that can't get its binaries admitted (committee-gate timeout cascade
                      // → chain-link break → orphans pile up). Watch this approach `nakamoto.orphan-buffer-cap`
                      // (default 1024) — a monotonic climb is the "buffer filling up" wedge.
                      Metrics[F].updateGauge("dag_nakamoto_orphan_buffer_size", sz) >>
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
                          (processMetagraphBinary(output) *>
                            Async[F].whenA(shardBinaryBuffers.nonEmpty)(
                              bufferAdmittedBinaryForShard(output, shardBinaryBuffers, shardAssignment, logger)
                            ))
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

  /** Buffer an admission-approved metagraph binary into the buffer for its execution shard. This function is called only after
    * `MetagraphCommitteeGate.attestAndAdmit` returns true; raw gossip receipt and ChainSync fetch are not admission conditions.
    *
    * '''Determinism model.''' Leader-proposes / members-attest: the buffer is node-local and need NOT converge across committee members —
    * only the shard slot leader builds the checkpoint from its own buffer; others attest the gossiped envelope (see
    * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer]]). So we just deserialize, resolve the shard via the
    * SAME deterministic `ShardAssignment.shardIdFor` mapping, and `bufferBinary` (idempotent by hash) — no multi-proposer machinery.
    *
    * Only invoked when `shardBinaryBuffers.nonEmpty` (`numShards > 1`).
    */
  private def bufferAdmittedBinaryForShard[F[_]: Async: HasherSelector: Metrics](
    output: io.constellationnetwork.statechannel.StateChannelOutput,
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAssignment match {
      case Some(assignment) =>
        val address = output.address
        HasherSelector[F].withCurrent { implicit hasher =>
          assignment.shardIdFor(address).flatMap { sid =>
            // Metagraph→shard mapping gauge (eval-instrumentation): emit one series per (mg, shard) so
            // `count by (shard_id) (dag_nakamoto_metagraph_shard_assignment)` shows the per-shard
            // metagraph load at a glance — a shard carrying >1 mg while another sits idle is the
            // imbalance that overloads one committee (the data-with-fee wedge). Idempotent (value 1).
            Metrics[F].updateGauge(
              "dag_nakamoto_metagraph_shard_assignment",
              1,
              Seq(
                Metrics.unsafeLabelName("shard_id") -> sid.value.value.toString,
                Metrics.unsafeLabelName("metagraph") -> address.value.value
              )
            ) >>
              (shardBinaryBuffers.get(sid) match {
                case Some(buffer) => buffer.bufferBinary(address, output.snapshotBinary)
                case None =>
                  logger.debug(s"Shard buffer missing for shard=${sid.value.value} (mg=$address); skipping admitted binary")
              })
          }
        }
      case None => Async[F].unit
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

}
