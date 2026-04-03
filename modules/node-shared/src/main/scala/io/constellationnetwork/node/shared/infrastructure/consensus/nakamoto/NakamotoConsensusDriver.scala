package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Drives the Nakamoto slot-based consensus.
  *
  * Each slot tick (1s):
  *   1. Compute current slot from wall clock
  *   2. Compute slot gap since last produced snapshot
  *   3. Evaluate VRF eligibility via LDD threshold
  *   4. If winner: invoke onSlotWon callback (which drains mempool and builds snapshot)
  *   5. Update epoch state with VRF output
  */
object NakamotoConsensusDriver {

  final case class DriverConfig(
    slotDurationMs: Long = 1000L,
    genesisTimeMs: Long,
    ldd: LddConfig = LddConfig.Default,
    slotsPerEpoch: Long = 60L
  )

  final case class DriverState(
    currentSlot: Long,
    lastProducedSlot: Option[Long],
    lastFinalizedOrdinal: Long,
    lastFinalizedHash: Hash,
    currentEta: Array[Byte],
    vrfAccumulator: List[Array[Byte]],
    totalProduced: Long,
    totalFinalized: Long,
    isActive: Boolean
  )

  object DriverState {
    def initial(startOrdinal: Long, startHash: Hash, genesisEta: Array[Byte]): DriverState =
      DriverState(
        currentSlot = 0L,
        lastProducedSlot = None,
        lastFinalizedOrdinal = startOrdinal,
        lastFinalizedHash = startHash,
        currentEta = genesisEta,
        vrfAccumulator = Nil,
        totalProduced = 0L,
        totalFinalized = 0L,
        isActive = false
      )
  }

  /** The slot evaluation loop. Returns an fs2 Stream that ticks every slot. */
  def slotLoop[F[_]: Async](
    selfId: PeerId,
    vrfSecretKey: Array[Byte],
    vrfPublicKey: Array[Byte],
    stateRef: Ref[F, DriverState],
    config: DriverConfig,
    stakeWeight: F[BigDecimal],
    onSlotWon: (Long, SlotCertificate) => F[Unit]
  ): Stream[F, Unit] = {
    implicit val logger: SelfAwareStructuredLogger[F] =
      Slf4jLogger.getLoggerFromClass[F](classOf[NakamotoConsensusDriver.type])

    Stream
      .awakeEvery[F](config.slotDurationMs.millis)
      .evalMap { _ =>
        stateRef.get.flatMap { state =>
          if (!state.isActive) Async[F].unit
          else evaluateSlot(selfId, vrfSecretKey, vrfPublicKey, stateRef, config, stakeWeight, onSlotWon, state)
        }
      }
  }

  private def evaluateSlot[F[_]: Async](
    selfId: PeerId,
    vrfSecretKey: Array[Byte],
    vrfPublicKey: Array[Byte],
    stateRef: Ref[F, DriverState],
    config: DriverConfig,
    stakeWeight: F[BigDecimal],
    onSlotWon: (Long, SlotCertificate) => F[Unit],
    state: DriverState
  )(implicit logger: SelfAwareStructuredLogger[F]): F[Unit] = {
    val wallClockMs = System.currentTimeMillis()
    val currentSlot = (wallClockMs - config.genesisTimeMs) / config.slotDurationMs
    val slotGap = state.lastProducedSlot.fold(currentSlot)(currentSlot - _)

    for {
      _ <- stateRef.update(_.copy(currentSlot = currentSlot))
      weight <- stakeWeight

      slotRefined = Slot(NonNegLong.unsafeFrom(currentSlot))

      result = EligibilityChecker.checkEligibility(
        vrfSK = vrfSecretKey,
        slot = slotRefined,
        slotGap = slotGap,
        eta = state.currentEta,
        relativeStake = weight.toDouble,
        config = config.ldd
      )

      _ <- result match {
        case Some((proof, vrfOutput)) =>
          val proofHex = Hex(proof.map("%02x".format(_)).mkString)
          val pkHex = Hex(vrfPublicKey.map("%02x".format(_)).mkString)
          val etaHash = Hash(state.currentEta.map("%02x".format(_)).mkString)
          val cert = SlotCertificate(
            slotRefined,
            VrfProof(proofHex),
            VrfPublicKey(pkHex),
            etaHash
          )

          logger.info(s"🎰 WON slot $currentSlot (gap=$slotGap)") >>
            onSlotWon(currentSlot, cert) >>
            stateRef.update { s =>
              val newAcc = s.vrfAccumulator :+ vrfOutput
              val (nextEta, nextAcc) =
                if (newAcc.size >= (config.slotsPerEpoch * 2 / 3).toInt) {
                  val epoch = currentSlot / config.slotsPerEpoch
                  (EligibilityChecker.computeNextEta(s.currentEta, epoch, newAcc), Nil)
                } else
                  (s.currentEta, newAcc)
              s.copy(
                lastProducedSlot = Some(currentSlot),
                totalProduced = s.totalProduced + 1,
                currentEta = nextEta,
                vrfAccumulator = nextAcc
              )
            }

        case None =>
          if (currentSlot % 30 == 0)
            logger.debug(s"Slot $currentSlot: not eligible (gap=$slotGap)")
          else
            Async[F].unit
      }
    } yield ()
  }
}
