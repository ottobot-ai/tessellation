package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.EcVrf25519

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

/** Per-metagraph committee sortition (Algorand-style VRF-threshold).
  *
  * Each operator key independently checks whether its VRF output for the canonical message `Hasher.hash(CommitteeVrfInput("committee", eta,
  * metagraphAddress, parentHash))` falls below the stake-weighted threshold `K_target · σ_i`. Operators whose VRF output is below the
  * threshold are committee members; non-committee operators ignore the metagraph snapshot.
  *
  * See `docs/nakamoto/COMMITTEE-SORTITION-DESIGN.md` for the design rationale, threat model, and Chernoff honest-majority bound. This file
  * is Slice S1 plus the S3-prep refactor to `parentHash` keying.
  *
  * '''Slot identity = lastSnapshotHash.''' We key the committee VRF on the metagraph snapshot's parent hash rather than its ordinal. Two
  * competing binaries on the same parent share a single committee VRF input — the committee elects one binary per fork point. That is also
  * exactly the slashable surface (S4): "same `(metagraph_address, parentHash)` + two different `binary_hash` from one operator" is
  * equivocation evidence.
  *
  * '''Domain separation.''' The `"committee"` `tag` field on the encoded `CommitteeVrfInput` keeps this VRF independent of any other VRF
  * input in the system. A leader winning a slot reveals their leader-VRF output but not their committee-VRF output for any future metagraph
  * snapshot.
  *
  * '''Determinism.''' All arithmetic is exact `Ratio` over `BigInt`; no `Double`. Reuses `EligibilityChecker.vrfOutputAsRatio` so committee
  * and leader VRF outputs share the same `[0, 1)` interpretation. The threshold is multiplicative (`K · σ`) not LDD — no `Log1p`/`Exp`
  * needed.
  *
  * '''Serialization.''' Goes through the standard `Hasher[F]` typeclass (canonical Circe JSON + SHA-256). We do not hand-roll Blake2b
  * concatenation here — that would create a second serialization surface that producers and verifiers (including the Go sidecar) would have
  * to keep byte-identical with the rest of the codebase.
  */
trait CommitteeSortition[F[_]] {

  /** Check whether the holder of `vrfSk` is in the committee for `(metagraphAddress, parentHash)` given their `sigmaOperatorKey` stake
    * share and the target committee size `kTarget`.
    *
    * Returns the VRF proof + output on success (caller signs it with their KES key and gossips it as their committee-attestation
    * contribution). Returns `None` if the operator key is not in the committee for this `(eta, metagraphAddress, parentHash)`.
    */
  def isInCommittee(
    vrfSk: Array[Byte],
    eta: Array[Byte],
    metagraphAddress: Address,
    parentHash: Hash,
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
    parentHash: Hash,
    sigmaOperatorKey: Ratio,
    kTarget: Int,
    proof: Array[Byte]
  ): F[Boolean]
}

object CommitteeSortition {

  private val vrf = EcVrf25519.default

  /** Domain-separation tag carried in-band on the encoded `CommitteeVrfInput`. Must NOT collide with any other VRF input tag in the
    * codebase — keeps this VRF independent of `EligibilityChecker`'s leader-VRF and of any future per-metagraph VRF use.
    */
  private val DomainTag: String = "committee"

  /** Canonical hash input for the committee VRF. The `tag` field provides domain-separation; the remaining three fields carry per-snapshot
    * identity (epoch randomness, subject metagraph, parent snapshot pointer). Encoded via the standard `Hasher` surface so the JVM-side
    * hash and any cross-implementation verifier (e.g. the Go sidecar) agree byte-for-byte.
    */
  final case class CommitteeVrfInput(
    tag: String,
    eta: Hex,
    metagraphAddress: Address,
    parentHash: Hash
  )

  object CommitteeVrfInput {
    implicit val encoder: Encoder[CommitteeVrfInput] = deriveEncoder
  }

  /** Build the canonical VRF message bytes via `Hasher`: SHA-256 of the canonical JSON encoding of `CommitteeVrfInput`. Result is the
    * 64-byte UTF-8 representation of the hex hash string — already the canonical `Hash` byte form across the codebase.
    *
    * `eta` MUST be 32 bytes (epoch randomness). `metagraphAddress` and `parentHash` are encoded via their derived Circe encoders.
    */
  def message[F[_]: Sync: Hasher](eta: Array[Byte], metagraphAddress: Address, parentHash: Hash): F[Array[Byte]] = {
    require(eta.length == 32, s"Eta must be 32 bytes, got ${eta.length}")
    val input = CommitteeVrfInput(DomainTag, Hex.fromBytes(eta), metagraphAddress, parentHash)
    Hasher[F].hash(input).map(_.getBytes)
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

  def make[F[_]: Sync: Hasher]: CommitteeSortition[F] = new CommitteeSortition[F] {

    def isInCommittee(
      vrfSk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      parentHash: Hash,
      sigmaOperatorKey: Ratio,
      kTarget: Int
    ): F[Option[(Array[Byte], Array[Byte])]] =
      message[F](eta, metagraphAddress, parentHash).map { msg =>
        val proof = vrf.vrfProof(vrfSk, msg)
        vrf.vrfProofToHash(proof) match {
          case None => None
          case Some(vrfOutput) =>
            val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
            val thresh = threshold(kTarget, sigmaOperatorKey)
            if (testValue < thresh) Some((proof, vrfOutput)) else None
        }
      }

    def verifyMembership(
      vrfVk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      parentHash: Hash,
      sigmaOperatorKey: Ratio,
      kTarget: Int,
      proof: Array[Byte]
    ): F[Boolean] =
      message[F](eta, metagraphAddress, parentHash).map { msg =>
        if (!vrf.vrfVerify(vrfVk, msg, proof)) false
        else
          vrf.vrfProofToHash(proof) match {
            case None => false
            case Some(vrfOutput) =>
              val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
              testValue < threshold(kTarget, sigmaOperatorKey)
          }
      }
  }
}
