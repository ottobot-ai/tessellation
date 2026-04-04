package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto.{EligibilityChecker, StakeRegistry, TipTracker}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

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
    currentEta: Array[Byte],
    vrfAccumulator: List[Array[Byte]],
    totalProduced: Long
  )

  object LoopState {
    def initial(genesisTimeMs: Long, genesisEta: Array[Byte]): LoopState =
      LoopState(
        genesisTimeMs = genesisTimeMs,
        lastProducedSlot = None,
        currentEta = genesisEta,
        vrfAccumulator = Nil,
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
    *   slots per epoch for eta rotation (default 60)
    */
  def run[F[_]: Async: SecurityProvider: HasherSelector](
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    keyPair: KeyPair,
    selfId: PeerId,
    lddConfig: LddConfig,
    slotsPerEpoch: Long = 60L
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("SnapshotLeaderLoop")
    val (vrfSeed, vrfPK) = deriveVrfKeys(keyPair)

    Stream.eval(Ref.of[F, LoopState](LoopState.initial(System.currentTimeMillis(), vrfSeed.take(32)))).flatMap { stateRef =>
      val slotTick: Stream[F, Unit] = Stream
        .awakeEvery[F](1.second)
        .evalMap { _ =>
          for {
            state <- stateRef.get
            wallClockMs = System.currentTimeMillis()
            currentSlot = (wallClockMs - state.genesisTimeMs) / 1000L
            slotGap = state.lastProducedSlot.fold(currentSlot)(currentSlot - _)

            slotRefined = Slot(NonNegLong.unsafeFrom(currentSlot))

            // VRF eligibility check (equal weight 1.0 for now)
            result = EligibilityChecker.checkEligibility(
              vrfSK = vrfSeed,
              slot = slotRefined,
              slotGap = slotGap,
              eta = state.currentEta,
              relativeStake = 1.0,
              config = lddConfig
            )

            _ <- result match {
              case Some((proof, vrfOutput)) =>
                onSlotWon(
                  stateRef,
                  consensusFns,
                  snapshotStorage,
                  eventMempool,
                  sidecarClient,
                  tipTracker,
                  stakeRegistry,
                  keyPair,
                  selfId,
                  vrfSeed,
                  vrfPK,
                  proof,
                  vrfOutput,
                  currentSlot,
                  slotGap,
                  slotRefined,
                  lddConfig,
                  slotsPerEpoch,
                  logger
                )

              case None =>
                // Periodic debug log
                Async[F].whenA(currentSlot % 30 == 0) {
                  logger.debug(s"Slot $currentSlot: not eligible (gap=$slotGap)")
                }
            }
          } yield ()
        }

      // Finality monitor: check attestation threshold periodically
      val finalityMonitor: Stream[F, Unit] = Stream
        .awakeEvery[F](2.seconds)
        .evalMap { _ =>
          for {
            heaviest <- tipTracker.heaviestTip
            _ <- heaviest match {
              case Some((hash, slot, weight)) if weight > TipTracker.FinalityThreshold =>
                for {
                  alreadyFinalized <- tipTracker.lastFinalized
                  isNew = alreadyFinalized.forall { case (fh, _) => fh =!= hash }
                  _ <- Async[F].whenA(isNew) {
                    tipTracker.markFinalized(hash, slot) >>
                      tipTracker.pruneBelow(slot) >>
                      logger.info(
                        s"✅ FINALIZED snapshot at slot ${slot.value.value} (hash=${hash.value.take(16)}..., weight=${"%.2f".format(weight)})"
                      )
                  }
                } yield ()
              case _ => Async[F].unit
            }
          } yield ()
        }

      slotTick.merge(finalityMonitor)
    }
  }

  /** Called when VRF lottery is won for a slot. Produces, signs, stores, and publishes a snapshot. */
  private def onSlotWon[F[_]: Async: SecurityProvider: HasherSelector](
    stateRef: Ref[F, LoopState],
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    keyPair: KeyPair,
    selfId: PeerId,
    vrfSeed: Array[Byte],
    vrfPK: Array[Byte],
    proof: Array[Byte],
    vrfOutput: Array[Byte],
    currentSlot: Long,
    slotGap: Long,
    slotRefined: Slot,
    lddConfig: LddConfig,
    slotsPerEpoch: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    HasherSelector[F].withCurrent { implicit hasher =>
      for {
        state <- stateRef.get

        // Build SlotCertificate with active pool info
        // SlotCertificate fields — will be embedded in snapshot once GlobalIncrementalSnapshot is extended
        _proofHex = Hex(proof.map("%02x".format(_)).mkString)
        _pkHex = Hex(vrfPK.map("%02x".format(_)).mkString)
        _etaHash = Hash(state.currentEta.map("%02x".format(_)).mkString)

        activePool <- stakeRegistry.activeValidators
        activePoolSize = activePool.size
        activePoolHashBytes = java.security.MessageDigest
          .getInstance("SHA-256")
          .digest(activePool.toList.map(_.value.value).sorted.mkString(",").getBytes("UTF-8"))
        _activePoolHash = Hash(activePoolHashBytes.map("%02x".format(_)).mkString)

        _ <- logger.info(s"🎰 WON slot $currentSlot (gap=$slotGap, pool=$activePoolSize) — producing snapshot")

        // Get last snapshot from storage
        headOpt <- snapshotStorage.head

        _ <- headOpt match {
          case Some((lastSigned, lastContext)) =>
            for {
              lastHashed <- lastSigned.toHashed[F]
              lastKey = lastHashed.ordinal

              // Drain mempool — getMultiple returns Hashed[Event]
              // Hashed[A].signed.value gives the raw A
              eventHashes <- eventMempool.getEventHashes
              hashedEvents <- eventMempool.getMultiple(eventHashes)
              eventSet: Set[GlobalSnapshotEvent] = hashedEvents.values.map(_.signed.value).toSet

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
                    case None    => none[Hashed[GlobalIncrementalSnapshot]].pure[F]
                  }
              )

              (artifact, context, returnedEvents) = result

              // Sign it (single producer signature — attestations come separately)
              signed <- Signed.forAsyncHasher[F, GlobalIncrementalSnapshot](artifact, keyPair)

              // Store in local snapshot chain
              stored <- snapshotStorage.prepend(signed, context)
              _ <- Async[F].whenA(!stored) {
                logger.warn(s"Snapshot at slot $currentSlot failed to store (rejected by sequential check)")
              }

              // Clear included events from mempool (returned events were NOT included)
              includedHashes = hashedEvents.collect {
                case (h, hashed) if !returnedEvents.contains(hashed.signed.value) => h
              }.toSet
              _ <- eventMempool.clearIncluded(includedHashes)

              // Publish via GossipSub sidecar
              snapshotHashed <- signed.toHashed[F]
              snapshotHash = snapshotHashed.hash
              _ <- sidecarClient
                .publishSnapshot(
                  SidecarClient.mkSnapshot(
                    hash = snapshotHash.value.getBytes,
                    slot = currentSlot,
                    ordinal = lastKey.value.value + 1,
                    parentHash = lastHashed.hash.value.getBytes,
                    vrfProof = proof,
                    vrfPublicKey = vrfPK,
                    eta = state.currentEta,
                    payload = Array.empty, // full snapshot travels via separate mechanism
                    producerId = selfId.value.value.getBytes
                  )
                )
                .void
                .handleErrorWith(e => logger.warn(s"Sidecar publish failed: ${e.getMessage}"))

              // Self-attest (producer always attests to own snapshot)
              selfAttestation = io.constellationnetwork.schema.nakamoto.TipAttestation(
                tipHash = snapshotHash,
                tipSlot = slotRefined,
                tipOrdinal = lastKey.value.value + 1,
                attestedAt = slotRefined
              )
              _ <- tipTracker.recordAttestation(selfId, selfAttestation)

              _ <- logger.info(
                s"📦 Produced snapshot ordinal=${lastKey.value.value + 1} slot=$currentSlot " +
                  s"events=${eventSet.size} returned=${returnedEvents.size} pool=$activePoolSize"
              )
            } yield ()

          case None =>
            logger.warn(s"No head snapshot in storage — skipping slot $currentSlot (genesis not yet loaded?)")
        }

        // Update VRF state (epoch rotation)
        _ <- stateRef.update { s =>
          val newAcc = s.vrfAccumulator :+ vrfOutput
          val (nextEta, nextAcc) =
            if (newAcc.size >= (slotsPerEpoch * 2 / 3).toInt) {
              val epoch = currentSlot / slotsPerEpoch
              (EligibilityChecker.computeNextEta(s.currentEta, epoch, newAcc), Nil)
            } else
              (s.currentEta, newAcc)

          s.copy(
            lastProducedSlot = Some(currentSlot),
            currentEta = nextEta,
            vrfAccumulator = nextAcc,
            totalProduced = s.totalProduced + 1
          )
        }
      } yield ()
    } // withCurrent
  }
}
