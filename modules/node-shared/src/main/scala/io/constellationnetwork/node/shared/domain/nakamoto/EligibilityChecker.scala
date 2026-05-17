package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.{Exp, Log1p}
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.security.vrf.EcVrf25519

import org.bouncycastle.crypto.digests.Blake2bDigest

/** VRF-based leader eligibility checker using LDD snowplow threshold.
  *
  * For each slot, a validator:
  *   1. Computes VRF proof over (eta || slot) 2. Extracts VRF output hash (64 bytes) 3. Interprets it as a `Ratio` testValue ∈ [0, 1) 4.
  *      Computes threshold from LDD snowplow + relative stake 5. Eligible if threshold > testValue
  *
  * '''All consensus arithmetic is exact `Ratio`''' (no Double, no `math.pow`/`math.log`/`math.exp`). The threshold comes from
  * Bifrost-derived continued-fraction approximations of `log1p` and `exp` — byte-identical across all JVMs/CPUs.
  */
class EligibilityChecker[F[_]: Monad](log1p: Log1p[F], exp: Exp[F]) {

  /** Compute LDD snowplow threshold. threshold = 1 - (1 - f(δ))^relativeStake, all in `Ratio`.
    *
    * Edge cases:
    *   - difficulty == 0 (gap < offset, or gap == offset on the ramp): threshold = 0 (never eligible).
    *   - difficulty == 1 (degenerate config): threshold = 1 (always eligible).
    */
  def threshold(relativeStake: Ratio, slotGap: Long, config: LddConfig): F[Ratio] = {
    val difficulty: Ratio =
      if (slotGap < config.offset.toLong) Ratio.Zero
      else if (slotGap < config.lddCutoff.toLong)
        Ratio(BigInt(slotGap - config.offset.toLong), BigInt(config.lddCutoff.toLong - config.offset.toLong)) * config.amplitude
      else config.baselineDifficulty

    if (difficulty == Ratio.Zero) Monad[F].pure(Ratio.Zero)
    else if (difficulty == Ratio.One) Monad[F].pure(Ratio.One)
    else
      for {
        coefficient <- log1p.evaluate(-difficulty)
        result <- exp.evaluate(coefficient * relativeStake)
      } yield Ratio.One - result
  }

  /** Compute VRF proof for a slot. Message = eta (32 bytes) || slot (8 bytes big-endian). Pure byte arithmetic; no Double involved. */
  def vrfProofForSlot(vrfSK: Array[Byte], slot: Slot, eta: Array[Byte]): Array[Byte] = {
    require(eta.length == 32, s"Eta must be 32 bytes, got ${eta.length}")
    val slotBytes = java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
    val message = eta ++ slotBytes
    EligibilityChecker.vrf.vrfProof(vrfSK, message)
  }

  /** Check if a validator is eligible to produce a snapshot in the given slot. */
  def checkEligibility(
    vrfSK: Array[Byte],
    slot: Slot,
    slotGap: Long,
    eta: Array[Byte],
    relativeStake: Ratio,
    config: LddConfig
  ): F[Option[(Array[Byte], Array[Byte])]] = {
    val proof = vrfProofForSlot(vrfSK, slot, eta)
    EligibilityChecker.vrf.vrfProofToHash(proof) match {
      case None => Monad[F].pure(None)
      case Some(vrfOutput) =>
        val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
        threshold(relativeStake, slotGap, config).map { thresh =>
          if (thresh > testValue) Some((proof, vrfOutput)) else None
        }
    }
  }

  /** Verify that a snapshot's VRF proof is valid and the producer was eligible. */
  def verifyEligibility(
    vrfVK: Array[Byte],
    slot: Slot,
    slotGap: Long,
    eta: Array[Byte],
    relativeStake: Ratio,
    config: LddConfig,
    proof: Array[Byte]
  ): F[Boolean] = {
    val slotBytes = java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
    val message = eta ++ slotBytes

    if (!EligibilityChecker.vrf.vrfVerify(vrfVK, message, proof)) Monad[F].pure(false)
    else
      EligibilityChecker.vrf.vrfProofToHash(proof) match {
        case None => Monad[F].pure(false)
        case Some(vrfOutput) =>
          val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
          threshold(relativeStake, slotGap, config).map(_ > testValue)
      }
  }
}

object EligibilityChecker {

  private val vrf = EcVrf25519.default

  /** 2^512 — denominator for normalizing a 64-byte VRF output into [0, 1) as a `Ratio`. Computed once. */
  private val NormalizationConstant: BigInt = BigInt(2).pow(512)

  /** Interpret the 64-byte VRF output (unsigned, big-endian) as an exact `Ratio` ∈ [0, 1). No Double, no rounding. */
  def vrfOutputAsRatio(vrfOutput: Array[Byte]): Ratio =
    Ratio(BigInt(1, vrfOutput), NormalizationConstant)

  /** Compute Blake2b-256 epoch randomness from previous eta and VRF outputs. eta_{e+1} = Blake2b-256(eta_e || epoch ||
    * concat(rhoNonceHashes)).
    *
    * Hash function only — no Double, no Ratio. Consensus-deterministic by construction.
    */
  def computeNextEta(previousEta: Array[Byte], epoch: Long, rhoNonceHashes: List[Array[Byte]]): Array[Byte] = {
    val digest = new Blake2bDigest(256)
    digest.update(previousEta, 0, previousEta.length)
    val epochBytes = java.nio.ByteBuffer.allocate(8).putLong(epoch).array()
    digest.update(epochBytes, 0, epochBytes.length)
    rhoNonceHashes.foreach(h => digest.update(h, 0, h.length))
    val out = new Array[Byte](32)
    digest.doFinal(out, 0)
    out
  }

  /** Build an `EligibilityChecker[F]` with the given `Log1p` and `Exp` interpreters. */
  def make[F[_]: Monad](log1p: Log1p[F], exp: Exp[F]): EligibilityChecker[F] =
    new EligibilityChecker[F](log1p, exp)
}
