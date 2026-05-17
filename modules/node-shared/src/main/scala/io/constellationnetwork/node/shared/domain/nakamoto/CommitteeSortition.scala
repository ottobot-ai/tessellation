package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets

import cats.Applicative
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.vrf.EcVrf25519

import org.bouncycastle.crypto.digests.Blake2bDigest

/** Per-metagraph committee sortition (Algorand-style VRF-threshold).
  *
  * Each operator key independently checks whether its VRF output for the message `Blake2b-256(eta ‖ metagraphAddress ‖ snapshotOrd ‖
  * "committee")` falls below the stake-weighted threshold `K_target · σ_i`. Operators whose VRF output is below the threshold are committee
  * members; non-committee operators ignore the metagraph snapshot.
  *
  * See `docs/nakamoto/COMMITTEE-SORTITION-DESIGN.md` for the design rationale, threat model, and Chernoff honest-majority bound. This file
  * is Slice S1: pure primitive + property tests. No consensus wiring yet — that lands in S2/S3 (warn-only gossip → load-bearing
  * pre-inclusion gate).
  *
  * '''Domain separation.''' The `"committee"` byte-suffix in the hash input keeps this VRF independent of the leader VRF in
  * `EligibilityChecker`. A leader winning a slot reveals their leader-VRF output but not their committee-VRF output for any future
  * metagraph snapshot.
  *
  * '''Determinism.''' All arithmetic is exact `Ratio` over `BigInt`; no `Double`. Reuses `EligibilityChecker.vrfOutputAsRatio` so committee
  * and leader VRF outputs share the same `[0, 1)` interpretation. The threshold is multiplicative (`K · σ`) not LDD — no `Log1p`/`Exp`
  * needed.
  */
trait CommitteeSortition[F[_]] {

  /** Check whether the holder of `vrfSk` is in the committee for `(metagraphAddress, snapshotOrd)` given their `sigmaOperatorKey` stake
    * share and the target committee size `kTarget`.
    *
    * Returns the VRF proof + output on success (caller signs it with their KES key and gossips it as their committee-attestation
    * contribution). Returns `None` if the operator key is not in the committee for this `(eta, metagraphAddress, snapshotOrd)`.
    */
  def isInCommittee(
    vrfSk: Array[Byte],
    eta: Array[Byte],
    metagraphAddress: Address,
    snapshotOrd: Long,
    sigmaOperatorKey: Ratio,
    kTarget: Int
  ): F[Option[(Array[Byte], Array[Byte])]]

  /** Verifier counterpart. Given a published VRF proof from a claimed committee member, confirms (a) the proof verifies under `vrfVk` for
    * the canonical message and (b) the proof's output falls below `K · σ`. Both must hold; otherwise reject.
    */
  def verifyMembership(
    vrfVk: Array[Byte],
    eta: Array[Byte],
    metagraphAddress: Address,
    snapshotOrd: Long,
    sigmaOperatorKey: Ratio,
    kTarget: Int,
    proof: Array[Byte]
  ): F[Boolean]
}

object CommitteeSortition {

  private val vrf = EcVrf25519.default

  /** Domain-separation suffix. Must NOT collide with anything `EligibilityChecker` hashes. */
  private val DomainTag: Array[Byte] = "committee".getBytes(StandardCharsets.UTF_8)

  /** Build the canonical VRF message: `Blake2b-256(eta ‖ metagraphAddress ‖ snapshotOrd ‖ "committee")`.
    *
    * `eta` MUST be 32 bytes (epoch randomness). `metagraphAddress` is hashed in its canonical UTF-8 text form, which is identical across
    * all serializers (`DAG{par}{36-base58}`). `snapshotOrd` is 8-byte big-endian. Hashing the whole message into a fixed-size digest
    * (rather than passing it raw to the VRF) keeps the VRF input length bounded.
    */
  def message(eta: Array[Byte], metagraphAddress: Address, snapshotOrd: Long): Array[Byte] = {
    require(eta.length == 32, s"Eta must be 32 bytes, got ${eta.length}")
    val digest = new Blake2bDigest(256)
    digest.update(eta, 0, eta.length)
    val addrBytes = metagraphAddress.value.value.getBytes(StandardCharsets.UTF_8)
    digest.update(addrBytes, 0, addrBytes.length)
    val ordBytes = java.nio.ByteBuffer.allocate(8).putLong(snapshotOrd).array()
    digest.update(ordBytes, 0, ordBytes.length)
    digest.update(DomainTag, 0, DomainTag.length)
    val out = new Array[Byte](32)
    digest.doFinal(out, 0)
    out
  }

  /** Committee threshold: `min(K · σ, 1)`. Saturates at one — an operator whose `K · σ` ≥ 1 is always in the committee. Per the design doc
    * §3 this is fine for v1 since the sortition unit is per-operator-key (an operator with 40% total stake splits it across multiple keys
    * to restore the sampling property at the cluster's K).
    */
  def threshold(kTarget: Int, sigmaOperatorKey: Ratio): Ratio = {
    require(kTarget > 0, s"K_target must be positive, got $kTarget")
    val raw = Ratio(kTarget) * sigmaOperatorKey
    if (raw >= Ratio.One) Ratio.One else raw
  }

  def make[F[_]: Applicative]: CommitteeSortition[F] = new CommitteeSortition[F] {

    def isInCommittee(
      vrfSk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      snapshotOrd: Long,
      sigmaOperatorKey: Ratio,
      kTarget: Int
    ): F[Option[(Array[Byte], Array[Byte])]] = {
      val msg = message(eta, metagraphAddress, snapshotOrd)
      val proof = vrf.vrfProof(vrfSk, msg)
      vrf.vrfProofToHash(proof) match {
        case None => Applicative[F].pure(None)
        case Some(vrfOutput) =>
          val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
          val thresh = threshold(kTarget, sigmaOperatorKey)
          val result = if (testValue < thresh) Some((proof, vrfOutput)) else None
          Applicative[F].pure(result)
      }
    }

    def verifyMembership(
      vrfVk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      snapshotOrd: Long,
      sigmaOperatorKey: Ratio,
      kTarget: Int,
      proof: Array[Byte]
    ): F[Boolean] = {
      val msg = message(eta, metagraphAddress, snapshotOrd)
      if (!vrf.vrfVerify(vrfVk, msg, proof)) Applicative[F].pure(false)
      else
        vrf.vrfProofToHash(proof) match {
          case None => Applicative[F].pure(false)
          case Some(vrfOutput) =>
            val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
            Applicative[F].pure(testValue < threshold(kTarget, sigmaOperatorKey))
        }
    }
  }
}
