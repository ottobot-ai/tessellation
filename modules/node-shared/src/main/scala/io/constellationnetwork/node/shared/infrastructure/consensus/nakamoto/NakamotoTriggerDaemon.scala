package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import java.security.KeyPair

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Queue
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.ConsensusCommand
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.TimeTrigger
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.VrfKeyDeriver

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** VRF slot-clock daemon that replaces EventTrigger + TimeTrigger.
  *
  * Every 1 second (1 slot), evaluates VRF eligibility. On a win, feeds `StartRound(TimeTrigger)` into the BFT consensus command queue. The
  * BFT round machinery (Facility → Proposal → Signature → Finished) then runs as normal, but triggered by VRF lottery instead of mempool
  * threshold or wall-clock timer.
  *
  * This keeps the BFT finalization intact while replacing the leader election mechanism with Taktikos-style LDD snowplow.
  */
object NakamotoTriggerDaemon {

  /** Mutable state tracked across slots. */
  final case class NakamotoTriggerState(
    genesisTimeMs: Long,
    lastProducedSlot: Option[Long],
    currentEta: Array[Byte],
    vrfAccumulator: List[Array[Byte]],
    totalProduced: Long,
    currentSlotCertificate: Option[SlotCertificate]
  )

  object NakamotoTriggerState {
    def initial(genesisTimeMs: Long, genesisEta: Array[Byte]): NakamotoTriggerState =
      NakamotoTriggerState(
        genesisTimeMs = genesisTimeMs,
        lastProducedSlot = None,
        currentEta = genesisEta,
        vrfAccumulator = Nil,
        totalProduced = 0L,
        currentSlotCertificate = None
      )
  }

  /** VRF keys derived from node's secp256k1 identity. */
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

  /** Run the slot clock. Evaluates VRF eligibility every 1s; on win, enqueues StartRound. */
  def run[F[_]: Async](
    consensusQueue: cats.effect.std.Queue[F, ConsensusCommand],
    stateRef: Ref[F, NakamotoTriggerState],
    keyPair: KeyPair,
    selfId: PeerId,
    lddConfig: LddConfig,
    slotsPerEpoch: Long
  ): Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoTriggerDaemon")
    val (vrfSeed, vrfPK) = deriveVrfKeys(keyPair)

    Stream
      .awakeEvery[F](1.second)
      .evalMap { _ =>
        for {
          state <- stateRef.get
          wallClockMs = System.currentTimeMillis()
          currentSlot = (wallClockMs - state.genesisTimeMs) / 1000L
          slotGap = state.lastProducedSlot.fold(currentSlot)(currentSlot - _)

          slotRefined = Slot(NonNegLong.unsafeFrom(currentSlot))

          // Equal weight (1.0) for now — StakeRegistry will supply real weights later
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
              val proofHex = Hex(proof.map("%02x".format(_)).mkString)
              val pkHex = Hex(vrfPK.map("%02x".format(_)).mkString)
              val etaHash = Hash(state.currentEta.map("%02x".format(_)).mkString)
              val cert = SlotCertificate(slotRefined, VrfProof(proofHex), VrfPublicKey(pkHex), etaHash, 1, Hash("0" * 64))

              for {
                // Update state with slot win
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
                    totalProduced = s.totalProduced + 1,
                    currentSlotCertificate = Some(cert)
                  )
                }

                // Feed StartRound into the BFT consensus queue — this triggers a normal round
                // but with this node as the self-selected leader
                _ <- consensusQueue.offer(ConsensusCommand.StartRound(Some(TimeTrigger)))
                _ <- logger.info(s"🎰 WON slot $currentSlot (gap=$slotGap) — triggering BFT round")
              } yield ()

            case None =>
              // Periodic debug log (every 30 slots)
              Async[F].whenA(currentSlot % 30 == 0) {
                logger.debug(s"Slot $currentSlot: not eligible (gap=$slotGap)")
              }
          }
        } yield ()
      }
  }
}
