package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.EcVrf25519

/** Nakamoto-style slot-based snapshot proposer.
  *
  * Integrates slot clock, eligibility checker, stake registry, and epoch state into a unified snapshot production loop. Replaces
  * tessellation's ConsensusState FSM with a simpler lottery-based model.
  *
  * Each slot, validators:
  *   1. Check if they're eligible via VRF lottery 2. If eligible, produce a SlotCertificate proving eligibility 3. Broadcast snapshot with
  *      certificate 4. Upon finalization, record production and accumulate VRF output for epoch rotation
  */
trait NakamotoProposer[F[_]] {

  /** Evaluate the current slot for eligibility and produce a SlotCertificate if eligible.
    *
    * @param currentSlot
    *   the slot to evaluate
    * @return
    *   Some(SlotCertificate) if this node is eligible to produce a snapshot, None otherwise
    */
  def evaluateSlot(currentSlot: Slot): F[Option[SlotCertificate]]

  /** Verify a received snapshot's SlotCertificate.
    *
    * Checks:
    *   - VRF proof verifies against (eta || slot) with the producer's public key
    *   - VRF output is below the LDD threshold for the given slot gap
    *   - Slot is within clock tolerance (caller must verify separately)
    *
    * @param cert
    *   the certificate to verify
    * @param producerPeerId
    *   the peer who produced the snapshot
    * @param slotGap
    *   slots since last finalized snapshot (δ = cert.slot - lastProducedSlot)
    * @return
    *   true if the certificate is valid
    */
  def verifyCertificate(cert: SlotCertificate, producerPeerId: PeerId, slotGap: Long): F[Boolean]

  /** Record that a snapshot was finalized at the given slot.
    *
    * Updates epoch state: records the production slot and accumulates VRF output for next epoch's eta. Triggers epoch rotation if the slot
    * crosses an epoch boundary.
    *
    * @param slot
    *   the slot of the finalized snapshot
    * @param vrfOutput
    *   the VRF output from the certificate (used for epoch rotation)
    */
  def recordFinalization(slot: Slot, vrfOutput: Array[Byte]): F[Unit]
}

object NakamotoProposer {

  /** Configuration for epoch management */
  case class EpochConfig(
    slotsPerEpoch: Long = 60L // Epoch rotates every 60 slots (60 seconds)
  )

  private val vrf = new EcVrf25519()

  /** Create a NakamotoProposer instance.
    *
    * @param vrfSK
    *   this node's VRF secret key (32 bytes)
    * @param vrfVK
    *   this node's VRF verification key (32 bytes)
    * @param peerId
    *   this node's peer identity
    * @param epochState
    *   mutable epoch state (tracks eta, lastProducedSlot, VRF accumulator)
    * @param stakeRegistry
    *   read-only view of validator stakes
    * @param slotClock
    *   wall-clock to slot mapping (for tolerance checks)
    * @param lddConfig
    *   LDD snowplow parameters
    * @param epochConfig
    *   epoch rotation parameters
    */
  def make[F[_]: Sync](
    vrfSK: Array[Byte],
    vrfVK: Array[Byte],
    peerId: PeerId,
    epochState: EpochState[F],
    stakeRegistry: StakeRegistry[F],
    slotClock: SlotClock[F],
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    epochConfig: EpochConfig = EpochConfig()
  ): NakamotoProposer[F] = new NakamotoProposer[F] {

    def evaluateSlot(currentSlot: Slot): F[Option[SlotCertificate]] =
      for {
        eta <- epochState.currentEta
        lastSlot <- epochState.lastProducedSlot
        stake <- stakeRegistry.relativeStake(peerId)
        slotGap = currentSlot.value.value - lastSlot.value.value
        result <-
          if (stake == io.constellationnetwork.numerics.Ratio.Zero) Sync[F].pure(Option.empty[SlotCertificate])
          else
            eligibilityChecker.checkEligibility(vrfSK, currentSlot, slotGap, eta, stake, lddConfig).map {
              case None => Option.empty[SlotCertificate]
              case Some((proof, vrfOut)) =>
                Some(
                  SlotCertificate(
                    slot = currentSlot,
                    // `lastSlot` is the slot of the parent snapshot (the last one this proposer
                    // recorded as finalized via `recordFinalization` / `epochState.recordProduction`).
                    // The verifier reconstructs slotGap = cert.slot - cert.parentSlot — for that
                    // identity to hold, parentSlot MUST match the slot used to derive `slotGap` here.
                    parentSlot = lastSlot,
                    vrfProof = VrfProof.fromBytes(proof),
                    vrfOutput = VrfOutput.fromBytes(vrfOut),
                    vrfPublicKey = VrfPublicKey.fromBytes(vrfVK),
                    eta = Hash(Hex.fromBytes(eta).value),
                    activePoolSize = 1,
                    activePoolHash = Hash("0" * 64)
                  )
                )
            }
      } yield result

    def verifyCertificate(cert: SlotCertificate, producerPeerId: PeerId, slotGap: Long): F[Boolean] =
      for {
        stake <- stakeRegistry.relativeStake(producerPeerId)
        result <-
          if (stake == io.constellationnetwork.numerics.Ratio.Zero) Sync[F].pure(false)
          else {
            // Derive expected VRF VK from producer's secp256k1 identity
            // Note: In production, this would use VrfKeyDeriver with the peer's private key
            // For verification, we use the VRF VK embedded in the certificate
            // (The caller must verify that cert.vrfPublicKey matches the producer's registered key)
            val eta = Hex(cert.eta.value).toBytes
            val proof = cert.vrfProof.toBytes
            val vk = cert.vrfPublicKey.toBytes

            eligibilityChecker.verifyEligibility(
              vrfVK = vk,
              slot = cert.slot,
              slotGap = slotGap,
              eta = eta,
              relativeStake = stake,
              config = lddConfig,
              proof = proof
            )
          }
      } yield result

    def recordFinalization(slot: Slot, vrfOutput: Array[Byte]): F[Unit] =
      for {
        lastSlot <- epochState.lastProducedSlot
        _ <- epochState.recordProduction(slot, vrfOutput)
        // Check for epoch rotation
        lastEpoch = lastSlot.value.value / epochConfig.slotsPerEpoch
        currentEpoch = slot.value.value / epochConfig.slotsPerEpoch
        _ <-
          if (currentEpoch > lastEpoch)
            epochState.rotateEpoch(currentEpoch).void
          else
            Sync[F].unit
      } yield ()
  }

  /** Derive VRF output from a SlotCertificate's proof. Used for epoch rotation accumulation.
    *
    * @param cert
    *   the certificate containing the VRF proof
    * @return
    *   64-byte VRF output if proof is valid, None otherwise
    */
  def extractVrfOutput(cert: SlotCertificate): Option[Array[Byte]] =
    vrf.vrfProofToHash(cert.vrfProof.toBytes)
}
