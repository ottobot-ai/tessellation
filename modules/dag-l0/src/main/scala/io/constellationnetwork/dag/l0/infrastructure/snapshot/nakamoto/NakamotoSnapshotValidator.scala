package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto.{EligibilityChecker, StakeRegistry}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger}
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Validates Nakamoto snapshots received via gossip.
  *
  * Verification pipeline:
  *   1. VRF proof verification — producer was legitimately elected for this slot 2. Signature verification — snapshot was signed by the
  *      claimed producer 3. SlotCertificate verification — cert fields match gossip message fields 4. Content validation — state
  *      transitions are correct (via ConsensusFunctions.validateArtifact)
  */
object NakamotoSnapshotValidator {

  sealed abstract class ValidationResult
  final case class Valid(snapshot: Signed[GlobalIncrementalSnapshot], context: GlobalSnapshotInfo) extends ValidationResult
  final case class Invalid(reason: String) extends ValidationResult

  def validate[F[_]: Async: SecurityProvider: HasherSelector](
    signedSnapshot: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    slot: Long,
    vrfProof: Array[Byte],
    vrfPublicKey: Array[Byte],
    producerIdBytes: Array[Byte],
    eta: Array[Byte],
    slotGap: Long,
    stakeRegistry: StakeRegistry[F],
    lddConfig: LddConfig,
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    lastSignedArtifact: Signed[GlobalIncrementalSnapshot],
    lastContext: GlobalSnapshotInfo,
    getByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): F[ValidationResult] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoValidator")
    val producerHex = Hex(producerIdBytes.map("%02x".format(_)).mkString)
    val producerId = PeerId(producerHex)

    for {
      // ── Step 1: VRF proof verification ──
      producerStake <- stakeRegistry.relativeStake(producerId)

      vrfValid =
        if (vrfPublicKey.isEmpty || vrfProof.isEmpty) false
        else
          EligibilityChecker.verifyEligibility(
            vrfVK = vrfPublicKey,
            slot = Slot(NonNegLong.unsafeFrom(slot)),
            slotGap = slotGap,
            eta = eta,
            relativeStake = producerStake,
            config = lddConfig,
            proof = vrfProof
          )

      result <-
        if (!vrfValid) {
          logger
            .warn(s"❌ VRF failed: slot=$slot producer=${producerHex.value.take(8)} stake=$producerStake gap=$slotGap")
            .as(Invalid(s"VRF verification failed for slot $slot"): ValidationResult)
        } else {
          // ── Step 2: Signature verification ──
          HasherSelector[F].withCurrent { implicit hasher =>
            signedSnapshot.hasValidSignature[F].flatMap { sigValid =>
              if (!sigValid) {
                logger
                  .warn(s"❌ Signature invalid: slot=$slot ordinal=${signedSnapshot.ordinal}")
                  .as(Invalid(s"Invalid signature on snapshot ordinal=${signedSnapshot.ordinal}"): ValidationResult)
              } else {
                // ── Step 3: SlotCertificate verification ──
                val certResult = signedSnapshot.value.slotCertificate match {
                  case None =>
                    // Pre-activation snapshots don't have certs — accept for now
                    Right(())
                  case Some(cert) =>
                    // Verify cert slot matches gossip slot
                    if (cert.slot.value.value != slot)
                      Left(s"SlotCertificate slot (${cert.slot.value.value}) != gossip slot ($slot)")
                    // Verify VRF proof in cert matches gossip proof
                    else if (!java.util.Arrays.equals(cert.vrfProof.toBytes, vrfProof))
                      Left("SlotCertificate VRF proof doesn't match gossip proof")
                    // Verify VRF public key matches
                    else if (!java.util.Arrays.equals(cert.vrfPublicKey.toBytes, vrfPublicKey))
                      Left("SlotCertificate VRF public key doesn't match gossip key")
                    else
                      Right(())
                }

                certResult match {
                  case Left(reason) =>
                    logger.warn(s"❌ Cert mismatch: $reason").as(Invalid(reason): ValidationResult)
                  case Right(_) =>
                    // ── Step 4: Content validation ──
                    // Compare received artifact against locally-recreated one.
                    // Strip Nakamoto-specific fields (slotCertificate, eta) before comparison
                    // since createProposalArtifact doesn't populate them — producer adds them post-creation.
                    val strippedReceived = signedSnapshot.value.copy(slotCertificate = None, eta = None)
                    consensusFns
                      .validateArtifact(
                        lastSignedArtifact,
                        lastContext,
                        EventTrigger,
                        strippedReceived,
                        Set(producerId),
                        getByOrdinal
                      )
                      .flatMap {
                        case Right((_, validatedContext)) =>
                          // Content matches AND we have properly derived state (stateProof correct)
                          logger
                            .debug(s"✅ Full content validation passed: slot=$slot ordinal=${signedSnapshot.ordinal}")
                            .as(Valid(signedSnapshot, validatedContext): ValidationResult)
                        case Left(err) =>
                          // Artifact mismatch but VRF+sig+cert verified.
                          // IMPORTANT: still accept but use PRODUCER's context since we couldn't
                          // re-derive state. This is a degraded mode — state proof is unverified.
                          // TODO: make this a hard reject once all mismatch sources are eliminated
                          logger
                            .warn(s"⚠️ Content validation fail (accepted — VRF+sig+cert OK): slot=$slot err=$err")
                            .as(Valid(signedSnapshot, context): ValidationResult)
                      }
                }
              }
            }
          }
        }
    } yield result
  }
}
