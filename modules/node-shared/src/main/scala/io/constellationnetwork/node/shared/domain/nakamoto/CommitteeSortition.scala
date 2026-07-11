package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.EcVrf25519

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

/** Per-metagraph committee sortition (Algorand-style VRF-threshold).
  *
  * Each operator key independently checks whether its VRF output for the canonical message `Hasher.hash(CommitteeVrfInput("committee", eta,
  * metagraphAddress, parentHash))` falls below `K_draw * sigma_i`. The live gate supplies uniform `sigma_i = 1/N`; the algebra retains a
  * ratio parameter but current admission is not stake weighted. Operators whose VRF output is below the threshold are committee members;
  * non-committee operators ignore the metagraph snapshot.
  *
  * '''Draw/quorum decouple.''' `K_draw` is the committee DRAW target — it sizes the elected committee (`E[|committee|] ≈ K_draw · σ · N`).
  * It is deliberately distinct from the admit-quorum the gate waits for (`MetagraphAttestationAggregator.thresholdReached`'s
  * `requiredQuorum`). Coupling the two (expected-committee == admit-quorum) made ~36% of binaries draw a committee SMALLER than the quorum
  * they could never reach → committee-gate timeout → metagraph-admission lag. Keeping `K_draw` large (≥ ~1.5·quorum, or `= N` so `K_draw ·
  * σ = 1` saturates and the committee is everyone) restores `P(|committee| ≥ quorum) ≈ 1`. This file owns ONLY the draw — the quorum lives
  * in the aggregator / shard acceptance manager.
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

  /** Check whether the holder of `vrfSk` is in the committee for `(metagraphAddress, parentHash)` given their uniform
    * `sigmaOperatorKey` draw weight and the committee DRAW target `kDraw`.
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
    kDraw: Int
  ): F[Option[(Array[Byte], Array[Byte])]]

  /** Verifier counterpart. Given a published VRF proof from a claimed committee member, confirms (a) the proof verifies under `vrfVk` for
    * the canonical message and (b) the proof's output falls below `K_draw · σ`. Both must hold; otherwise reject.
    */
  def verifyMembership(
    vrfVk: Array[Byte],
    eta: Array[Byte],
    metagraphAddress: Address,
    parentHash: Hash,
    sigmaOperatorKey: Ratio,
    kDraw: Int,
    proof: Array[Byte]
  ): F[Boolean]

  /** Same as `verifyMembership` but returns the richer outcome distinguishing VRF-proof verification failure from threshold-bound failure.
    * Used by the gate's diagnostic path to localize the InvalidCommitteeVrf cause (#216).
    */
  def verifyMembershipDetailed(
    vrfVk: Array[Byte],
    eta: Array[Byte],
    metagraphAddress: Address,
    parentHash: Hash,
    sigmaOperatorKey: Ratio,
    kDraw: Int,
    proof: Array[Byte]
  ): F[CommitteeSortition.VerifyOutcome]
}

object CommitteeSortition {

  /** Outcome of `verifyMembershipDetailed` — distinguishes the two reject paths inside [[CommitteeSortition.verifyMembership]] so the
    * gate's `InvalidCommitteeVrf` log can name the actual failing predicate (#216 diagnostic). `Valid` means both predicates passed; the
    * other two mean exactly one failed (VRF cryptographic verification, or threshold-bound check).
    */
  sealed trait VerifyOutcome
  object VerifyOutcome {
    case object Valid extends VerifyOutcome
    case object InvalidProof extends VerifyOutcome
    case class BelowThreshold(testValue: Ratio, threshold: Ratio) extends VerifyOutcome
  }

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

  /** Committee threshold: `min(K_draw · σ, 1)`. Saturates at one — an operator whose `K_draw · σ` ≥ 1 is always in the committee (when
    * `kDraw = N` and uniform σ = 1/N this is `= 1` exactly, so the committee is everyone). `kDraw` is the draw target, decoupled from the
    * admission quorum. Both live committee mechanisms currently supply identity-uniform `σ = 1/N`; this is not stake weighting.
    */
  def threshold(kDraw: Int, sigmaOperatorKey: Ratio): Ratio = {
    require(kDraw > 0, s"K_draw must be positive, got $kDraw")
    val raw = Ratio(kDraw) * sigmaOperatorKey
    if (raw >= Ratio.One) Ratio.One else raw
  }

  // ─── Shard-committee sortition (EXECUTION-SHARDING — `ShardCheckpointWiring.committeeFor`) ──────────────────────────
  //
  // The per-metagraph committee VRF above (`message` / `isInCommittee` / `verifyMembership`) is a TRUE VRF: each operator
  // evaluates its own VRF SK over `(eta, metagraphAddress, parentHash)` and attaches an unforgeable proof a verifier checks
  // against that operator's VK. That mechanism elects a committee from an ARRIVING proof — a verifier confirms a single claim,
  // it cannot ENUMERATE the whole committee (a VRF output is not computable from the VK alone).
  //
  // The gl0 shard-checkpoint adopt path needs a different shape: `committeeFor(shardId, epoch)` must DETERMINISTICALLY
  // ENUMERATE the committee SET on every node (it feeds the per-signer set-membership pre-check). The configured `kQuorum` is a separate
  // selection-finality count and never bypasses `verifyEmbedded`. Enumeration from
  // public material only is the hard requirement (no node holds peers' VRF SKs). So the shard draw is a DETERMINISTIC
  // PSEUDO-RANDOM draw keyed on each operator's registered VRF *VK* (a public PRF), NOT a per-operator VRF evaluation:
  // `H(tag, eta, shardId, epoch, vrfVk)` interpreted as a Ratio in `[0,1)` (the SAME interpretation
  // `EligibilityChecker.vrfOutputAsRatio` uses) compared against the SAME `threshold(kDraw, σ)`. Reusing the registered VK
  // as the per-operator seed makes the draw (a) enumerable from the cluster-wide-identical VRF-VK registry + the
  // cluster-wide-identical eta, and (b) per-operator (different VKs ⇒ different draws) and per-shard (shardId is in the preimage).
  // Predictability is an explicit v1 property, not secret sortition. A non-drawn operator cannot place itself in the set because every node
  // recomputes the same predicate, and it cannot forge a drawn operator's Ed25519/KES signatures.

  /** Domain-separation tag for the shard-committee VK-seeded draw. Distinct from [[DomainTag]] so the shard draw can never collide with the
    * per-metagraph committee VRF or the leader VRF.
    */
  private val ShardDomainTag: String = "shard-committee"

  /** Canonical hash preimage for the shard-committee VK-seeded draw. `vrfVk` is the operator's registered VRF verification key (the
    * per-operator seed); `shardId` + `epoch` + `eta` scope the draw to one `(shard, epoch)` with that epoch's randomness. Encoded via the
    * standard `Hasher` surface (canonical Circe JSON + SHA-256) so every node derives byte-identical bytes.
    */
  final case class CommitteeShardVrfInput(
    tag: String,
    eta: Hex,
    shardId: ShardId,
    epoch: EtaPeriod,
    vrfVk: Hex
  )

  object CommitteeShardVrfInput {
    implicit val encoder: Encoder[CommitteeShardVrfInput] = deriveEncoder
  }

  /** Interpret a SHA-256 [[Hash]] as an unsigned, big-endian value in `[0, 1)`. [[Hash.getBytes]] is deliberately not used here: it returns
    * the UTF-8 bytes of the 64-character hex rendering, not the 32 digest bytes. Treating that text as a 512-bit integer confines draws to
    * approximately `[0.1875, 0.4023)` and destroys the uniform threshold distribution.
    */
  private[nakamoto] def hashDigestAsRatio(hash: Hash): Ratio = {
    val digestBytes = Hex(hash.value).toBytes
    require(digestBytes.length == 32, s"SHA-256 hash must decode to 32 bytes, got ${digestBytes.length}")
    Ratio(BigInt(1, digestBytes), BigInt(2).pow(8 * digestBytes.length))
  }

  /** Deterministic per-operator draw value for the shard committee: `Hasher.hash(CommitteeShardVrfInput(...))` decoded from its hex
    * rendering to the actual 32-byte SHA-256 digest, then interpreted as a `Ratio ∈ [0,1)` via the unsigned `BigInt(1, digestBytes) /
    * 2^256` convention. Pure + deterministic in `(eta, shardId, epoch, vrfVk)`.
    *
    * `eta` MUST be 32 bytes (epoch randomness), matching the per-metagraph [[message]] contract.
    */
  def shardDrawValue[F[_]: Sync: Hasher](
    eta: Array[Byte],
    shardId: ShardId,
    epoch: EtaPeriod,
    vrfVk: Array[Byte]
  ): F[Ratio] = {
    require(eta.length == 32, s"Eta must be 32 bytes, got ${eta.length}")
    val input = CommitteeShardVrfInput(ShardDomainTag, Hex.fromBytes(eta), shardId, epoch, Hex.fromBytes(vrfVk))
    Hasher[F].hash(input).map(hashDigestAsRatio)
  }

  /** Deterministic shard-committee membership predicate for the holder of `vrfVk`: `shardDrawValue(...) < threshold(kDraw, σ)`. Enumerated
    * over all active operators by [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.committeeFor]] to
    * materialize the committee SET — identical on every node because every input is cluster-wide-identical (registry VK + eta + HOCON kDraw
    * + uniform sigma). `kDraw * sigma >= 1` saturates to "always a member" via [[threshold]]. `kDraw` is the draw target; configured
    * `kQuorum` is decoupled selection finality in [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardFinalityTriggers]].
    */
  def isInShardCommittee[F[_]: Sync: Hasher](
    vrfVk: Array[Byte],
    eta: Array[Byte],
    shardId: ShardId,
    epoch: EtaPeriod,
    sigmaOperatorKey: Ratio,
    kDraw: Int
  ): F[Boolean] =
    shardDrawValue[F](eta, shardId, epoch, vrfVk).map(_ < threshold(kDraw, sigmaOperatorKey))

  def make[F[_]: Sync: Hasher]: CommitteeSortition[F] = new CommitteeSortition[F] {

    def isInCommittee(
      vrfSk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      parentHash: Hash,
      sigmaOperatorKey: Ratio,
      kDraw: Int
    ): F[Option[(Array[Byte], Array[Byte])]] =
      message[F](eta, metagraphAddress, parentHash).map { msg =>
        val proof = vrf.vrfProof(vrfSk, msg)
        vrf.vrfProofToHash(proof) match {
          case None => None
          case Some(vrfOutput) =>
            val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
            val thresh = threshold(kDraw, sigmaOperatorKey)
            if (testValue < thresh) Some((proof, vrfOutput)) else None
        }
      }

    def verifyMembership(
      vrfVk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      parentHash: Hash,
      sigmaOperatorKey: Ratio,
      kDraw: Int,
      proof: Array[Byte]
    ): F[Boolean] =
      verifyMembershipDetailed(vrfVk, eta, metagraphAddress, parentHash, sigmaOperatorKey, kDraw, proof).map {
        case VerifyOutcome.Valid => true
        case _                   => false
      }

    def verifyMembershipDetailed(
      vrfVk: Array[Byte],
      eta: Array[Byte],
      metagraphAddress: Address,
      parentHash: Hash,
      sigmaOperatorKey: Ratio,
      kDraw: Int,
      proof: Array[Byte]
    ): F[VerifyOutcome] =
      message[F](eta, metagraphAddress, parentHash).map { msg =>
        if (!vrf.vrfVerify(vrfVk, msg, proof)) VerifyOutcome.InvalidProof
        else
          vrf.vrfProofToHash(proof) match {
            case None => VerifyOutcome.InvalidProof
            case Some(vrfOutput) =>
              val testValue = EligibilityChecker.vrfOutputAsRatio(vrfOutput)
              val thresh = threshold(kDraw, sigmaOperatorKey)
              if (testValue < thresh) VerifyOutcome.Valid
              else VerifyOutcome.BelowThreshold(testValue, thresh)
          }
      }
  }
}
