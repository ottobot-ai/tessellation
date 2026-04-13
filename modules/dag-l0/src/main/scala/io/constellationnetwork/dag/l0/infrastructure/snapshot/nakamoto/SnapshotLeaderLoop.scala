package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature => SigValue}
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import io.estatico.newtype.ops._
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

  /** Accumulate a VRF output and rotate eta if threshold reached. Eta rotates every `etaRotationSlots` (default 600 = 10 minutes), not
    * every epoch. This follows Cardano/Bifrost pattern where eta is long-lived (Cardano uses ~5 days).
    */
  def accumulate(
    state: SharedEpochState,
    vrfOutput: Array[Byte],
    currentSlot: Long,
    etaRotationSlots: Long
  ): SharedEpochState = {
    val newAcc = state.vrfAccumulator :+ vrfOutput
    if (newAcc.size >= (etaRotationSlots * 2 / 3).toInt) {
      val rotationEpoch = currentSlot / etaRotationSlots
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

  /** Derive VRF keys from node's secp256k1 identity key. */
  private def deriveVrfKeys(keyPair: KeyPair): (Array[Byte], Array[Byte]) = {
    val rawPrivKey: Array[Byte] = keyPair.getPrivate match {
      case ecKey: java.security.interfaces.ECPrivateKey =>
        val bytes = ecKey.getS.toByteArray
        if (bytes.length > 32) bytes.drop(bytes.length - 32)
        else if (bytes.length < 32) Array.fill(32 - bytes.length)(0.toByte) ++ bytes
        else bytes
      case other =>
        other.getEncoded.takeRight(32)
    }
    val seed = VrfKeyDeriver.deriveVrfSeed(rawPrivKey)
    val pk = new io.constellationnetwork.security.vrf.EcVrf25519().getVerificationKey(seed)
    (seed, pk)
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
    * @param etaRotationSlots
    *   slots per eta rotation period (default 600 = 10 minutes). Eta is long-lived — Cardano uses ~5 days.
    */
  def run[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
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
    slotsPerEpoch: Long = 60L,
    etaRotationSlots: Long = 600L,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    genesisTimeMs: Long = 0L,
    snapshotSemaphore: cats.effect.std.Semaphore[F],
    productionGate: ProductionGate[F],
    // Tracks the highest finalized ordinal so HttpApi can expose it via
    // /global-snapshots/latest/finalized-ordinal. Updated after every successful
    // chainStore.finalize call (depth-k or attestation-2/3, whichever fires first).
    nakamotoFinalizedOrdinalRef: Ref[F, Long]
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("SnapshotLeaderLoop")
    val (vrfSeed, vrfPK) = deriveVrfKeys(keyPair)
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

        val slotTick: Stream[F, Unit] = Stream
          .awakeEvery[F](1.second)
          .evalMap { _ =>
            for {
              // Only produce when node is Ready and past genesis time
              nodeState <- nodeStorage.getNodeState
              state <- stateRef.get
              wallClockMs = System.currentTimeMillis()
              currentSlot = (wallClockMs - state.genesisTimeMs) / 1000L
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

                    // Query actual relative stake from registry
                    myStake <- stakeRegistry.relativeStake(selfId)

                    // Chain-derived eta: deterministic from stored chain, no in-memory accumulator.
                    // Period 0: genesis eta (constant). Period N>=1: derived from VRF outputs in period N-1.
                    // All nodes seeing the same chain derive the same eta — no divergence.
                    currentPeriod = EtaCalculation.rotationPeriod(currentSlot, etaRotationSlots)
                    genesisEta <- epochStateRef.get.map(_.genesisEta)
                    eta <-
                      if (currentPeriod <= 0) {
                        Async[F].pure(genesisEta)
                      } else {
                        chainStore.vrfOutputsForPeriod(currentPeriod - 1, etaRotationSlots).map { chainOutputs =>
                          if (chainOutputs.nonEmpty) {
                            EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2))
                          } else {
                            // No chain data yet for previous period — stay on genesis eta
                            genesisEta
                          }
                        }
                      }

                    result = EligibilityChecker.checkEligibility(
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
                            etaRotationSlots,
                            lastKnownSlotRef,
                            epochStateRef,
                            productionGate,
                            productionTimestamps,
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
        // Override via NAKAMOTO_CONFIRMATION_DEPTH. Default 31 chosen for adversarial-safety
        // margin in small (3-7 node) test clusters per simulation results.
        val ConfirmationDepthK: Long =
          sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH").flatMap(_.toLongOption).getOrElse(31L)

        val finalityMonitor: Stream[F, Unit] = Stream
          .awakeEvery[F](5.seconds)
          .evalMap { _ =>
            for {
              allAtts <- tipTracker.allAttestations
              validatorCount <- stakeRegistry.validatorCount
              activeCount <- stakeRegistry.observedActiveCount
              bestTip <- chainStore.bestTip
              alreadyFinalized <- tipTracker.lastFinalized
              // The last-finalized **ordinal** must come from chainStore — tipTracker.lastFinalized
              // only carries (Hash, Slot), and slots are LDD-paced not 1:1 with ordinals. The
              // previous code derived this from the slot value which silently disabled the depth
              // gate (as soon as any attestation finality fired, the slot value far outran any
              // tip.ordinal under LDD slot fill, making `tip.ordinal - lastFinalizedSlot > k`
              // perpetually false).
              lastFinalizedOrdinal <- chainStore.lastFinalizedOrdinal

              // Depth-based finality: a snapshot is final once k+ snapshots sit above it on
              // the canonical chain — `tip.ordinal - snapshotOrdinal > k`.
              depthFinalized <- bestTip match {
                case Some(tip) if tip.ordinal - lastFinalizedOrdinal > ConfirmationDepthK =>
                  // Finalize the snapshot at (tip.ordinal - k)
                  val finalizeAtOrdinal = tip.ordinal - ConfirmationDepthK
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
                            // Advance the finalized-ordinal tracker so HttpApi
                            // /latest/finalized-ordinal reflects the new high-water mark. CL0
                            // polls this to gate state-channel-binary pruning on actual finality
                            // (not just first sight).
                            nakamotoFinalizedOrdinalRef.update(prev => math.max(prev, finalizeAtOrdinal)) >>
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
                            Async[F].pure(true)
                        case None =>
                          logger.warn(
                            s"⚠️ DEPTH-FINALIZE: walked back to ordinal=$finalizeAtOrdinal but chainStore.get returned None for hash=${canonicalHash.value
                                .take(12)}"
                          ) >> Async[F].pure(false)
                      }
                    case None =>
                      logger.warn(
                        s"⚠️ DEPTH-FINALIZE: could not find canonical hash at ordinal=$finalizeAtOrdinal from tip=${tip.hash.value.take(12)}"
                      ) >> Async[F].pure(false)
                  }
                case _ => Async[F].pure(false)
              }

              // GRANDPA-style chain finality: attesting to ordinal N implicitly attests to all
              // ancestors. Walk attestation ordinals from highest down, accumulating weight.
              // The highest ordinal where cumulative weight >= 2/3 is finalized.
              chainFinalizedOrdinal <- tipTracker.highestFinalizedOrdinal(TipTracker.FinalityThreshold)
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
                            nakamotoFinalizedOrdinalRef.update(prev => math.max(prev, finalOrdinal)) >>
                            snapshotStorage.pruneTentative(SnapshotOrdinal(NonNegLong.unsafeFrom(finalOrdinal))) >>
                            logger.info(
                              s"ATTEST-FINALIZED ordinal=$finalOrdinal slot=${stored.slot} (weight=${"%.2f".format(weight)}, " +
                                s"${allAtts.size}/${activeCount} active of ${validatorCount} seedlist)"
                            ) >>
                            Metrics[F].incrementCounter("dag_nakamoto_finalized") >>
                            Metrics[F].updateGauge("dag_nakamoto_finalized_ordinal", finalOrdinal) >>
                            productionTimestamps.getAndUpdate(_ - finalOrdinal).flatMap { ts =>
                              ts.get(finalOrdinal).traverse_ { prodMs =>
                                val latencyMs = System.currentTimeMillis() - prodMs
                                Metrics[F].recordDistribution("dag_nakamoto_finality_latency_ms", latencyMs.toInt)
                              }
                            }
                        case None =>
                          logger.warn(
                            s"⚠️ ATTEST-FINALIZE: chainStore.get returned None for hash=${canonicalHash.value.take(12)} at ordinal=$finalOrdinal"
                          )
                      }
                    case None =>
                      logger
                        .warn(s"⚠️ ATTEST-FINALIZE: walkBackTo found no hash at ordinal=$finalOrdinal from tip=${tip.hash.value.take(12)}")
                  }
                case _ =>
                  Async[F].whenA(allAtts.nonEmpty) {
                    val ordinals = allAtts.values.map(_.tipOrdinal).toList.sorted
                    logger.debug(
                      s"Attestations: ${allAtts.size} attesters, ordinals=[${ordinals.mkString(",")}], lastFinalized=$lastFinalizedOrdinal"
                    )
                  }
              }
            } yield ()
          }

        seedChainStore ++ slotTick.merge(finalityMonitor)
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
    etaRotationSlots: Long,
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    productionGate: ProductionGate[F],
    productionTimestamps: Ref[F, Map[Long, Long]],
    logger: org.typelevel.log4cats.Logger[F]
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
          activePoolHash = activePoolHash
        )

        _ <- logger.info(s"WON slot $currentSlot (gap=$slotGap, parentSlot=$parentSlotValue, pool=$activePoolSize) — producing snapshot")
        _ <- Metrics[F].incrementCounter("dag_nakamoto_slots_won")
        _ <- Metrics[F].updateGauge("dag_nakamoto_slot", currentSlot)
        _ <- Metrics[F].recordDistribution("dag_nakamoto_slot_gap", slotGap.toInt)

        // Get last snapshot from storage
        headOpt <- snapshotStorage.head

        _ <- headOpt match {
          case Some((lastSigned, lastContext)) =>
            for {
              lastHashed <- lastSigned.toHashed[F]
              lastKey = lastHashed.ordinal

              productionStartMs <- Async[F].delay(System.currentTimeMillis())

              // Drain mempool — getMultiple returns Hashed[Event]
              // Hashed[A].signed.value gives the raw A
              eventHashes <- eventMempool.getEventHashes
              hashedEvents <- eventMempool.getMultiple(eventHashes)
              eventSet: Set[GlobalSnapshotEvent] = hashedEvents.values.map(_.signed.value).toSet
              _ <- Metrics[F].recordDistribution("dag_nakamoto_events_drained", eventSet.size)

              // Create snapshot using existing infrastructure — no reimplementation
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

              // Pre-sign gate check — proposal creation can be slow (mempool drain + acceptance
              // pipeline). If a better gossip snapshot arrived during that work, abandon now
              // before sealing (signing) anything. Saves a useless signature + chain store write
              // and prevents this node from briefly emitting a fork that immediately gets reorged.
              gateOpenPreSign <- productionGate.isOpen
              _ <-
                if (!gateOpenPreSign)
                  productionGate.pauseReasons.flatMap(reasons =>
                    logger.info(s"🛑 Abandoning production at slot $currentSlot before sign (gate closed: ${reasons.mkString(", ")})") >>
                      Metrics[F].incrementCounter("dag_nakamoto_production_abandoned_total")
                  )
                else Async[F].unit

              // Attach SlotCertificate and eta to artifact before signing
              artifact = rawArtifact.copy(slotCertificate = Some(cert), eta = Some(etaHash))

              // Sign it (single producer signature — attestations come separately)
              signed <- Signed.forAsyncHasher[F, GlobalIncrementalSnapshot](artifact, keyPair)

              // Store via NakamotoChainStore (handles forks + reorgs).
              // Skipped if the production gate closed during proposal creation — a better
              // gossip snapshot arrived and we are abandoning this round before committing it
              // to our local chain store. Prevents this node from briefly emitting a fork
              // that would immediately get reorged.
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
                    logger.info(s"🛑 Abandoning production at slot $currentSlot before publish (gate closed: ${reasons.mkString(", ")})")
                  )
                else Async[F].unit

              // Publish + self-attest only if gate is still open
              snapshotHash = snapshotHashedForStorage.hash
              _ <- Async[F].whenA(stillOpen) {
                sidecarClient
                  .publishSnapshot(
                    SidecarClient.mkSnapshot(
                      hash = snapshotHash.value.getBytes,
                      slot = currentSlot,
                      ordinal = lastKey.value.value + 1,
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
                      parentSlot = parentSlotValue
                    )
                  )
                  .void
                  .handleErrorWith(e => logger.warn(s"Sidecar publish failed: ${e.getMessage}")) >> {
                  // Self-attest (producer always attests to own snapshot) and broadcast
                  val selfAttestation = io.constellationnetwork.schema.nakamoto.TipAttestation(
                    tipHash = snapshotHash,
                    tipSlot = slotRefined,
                    tipOrdinal = lastKey.value.value + 1,
                    attestedAt = slotRefined
                  )
                  tipTracker.recordAttestation(selfId, selfAttestation) >>
                    // Sign via the standard Hasher pipeline (JSON-encode → hash → sign)
                    // and broadcast to the network via sidecar GossipSub
                    (for {
                      attHash <- selfAttestation.hash
                      sig <- SigValue.fromHash[F](keyPair.getPrivate, attHash)
                      sigBytes = sig.coerce.toBytes
                      att = SidecarClient.mkAttestation(
                        tipHash = snapshotHash.value.getBytes,
                        tipSlot = currentSlot,
                        tipOrdinal = lastKey.value.value + 1,
                        attestedAt = currentSlot,
                        attesterId = selfId.value.toBytes,
                        signature = sigBytes
                      )
                      _ <- sidecarClient.publishAttestation(att).void.handleErrorWith { e =>
                        logger.warn(s"Failed to broadcast attestation: ${e.getMessage}")
                      }
                    } yield ())
                } >>
                  logger.info(
                    s"Produced snapshot ordinal=${lastKey.value.value + 1} slot=$currentSlot " +
                      s"events=${eventSet.size} returned=${returnedEvents.size} pool=$activePoolSize"
                  ) >>
                  Metrics[F].incrementCounter("dag_nakamoto_snapshots_produced") >>
                  Metrics[F].updateGauge("dag_nakamoto_ordinal", lastKey.value.value + 1) >>
                  Async[F].delay(System.currentTimeMillis()).flatMap { nowMs =>
                    val producedOrdinal = lastKey.value.value + 1
                    Metrics[F].recordDistribution("dag_nakamoto_production_duration_ms", (nowMs - productionStartMs).toInt) >>
                      productionTimestamps.update { ts =>
                        val updated = ts + (producedOrdinal -> nowMs)
                        // Evict entries older than 1000 ordinals to bound memory
                        if (updated.size > 1000) updated.toList.sortBy(-_._1).take(1000).toMap
                        else updated
                      }
                  }
              }
            } yield ()

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
