package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import java.nio.ByteBuffer
import java.security.MessageDigest

import cats.Monad
import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{ActiveOperatorConsensusKeys, OperatorConsensusKeyRegistry}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.RatioInstances._
import io.constellationnetwork.numerics.algebras.{Exp, Log1p}
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.vrf.EcVrf25519

import io.circe.syntax._
import io.circe.{Encoder, Json}

/** §3 NIPoPoW S4.3 — structured error type for proof rejection. Each variant carries enough context for diagnostic logging without exposing
  * the validator internals.
  */
sealed trait ProofError extends Product with Serializable

object ProofError {

  /** L0 suffix is empty when the proof is non-empty (i.e. since != tipOrdinal). */
  case object EmptyL0Suffix extends ProofError

  /** Headers within a level-µ chain (or the L0 suffix) are out of order or duplicated by ordinal. */
  final case class NonMonotonicOrdinals(level: Int) extends ProofError

  /** A header's claimed level-µ trial doesn't actually pass when re-run by the verifier. */
  final case class TrialFailed(level: Int, ordinal: SnapshotOrdinal, observedTau: Ratio, threshold: Ratio) extends ProofError

  /** Per-level density observed in the chain deviates from `targetDensity` by more than the relative-error bound. */
  final case class DensityViolation(level: Int, observed: Ratio, target: Ratio, relativeError: Ratio) extends ProofError

  /** A tower header's carried key is absent from or differs from the producer's canonical registry entry. */
  final case class VrfKeyNotRegistered(ordinal: SnapshotOrdinal, producerId: PeerId) extends ProofError

  /** A tower header's proof is empty, malformed, or invalid for its registry-resolved key and exact `(eta, slot)` message. */
  final case class VrfProofInvalid(ordinal: SnapshotOrdinal, producerId: PeerId) extends ProofError

  /** A tower header's carried output is not the deterministic hash of its cryptographically verified VRF proof. */
  final case class VrfOutputMismatch(ordinal: SnapshotOrdinal, producerId: PeerId) extends ProofError

  /** The proof does not carry an authenticated historical roster/stake witness, so L0 leader eligibility cannot be established. Sender-
    * claimed `activePoolSize` is diagnostic evidence only and is never accepted as authority.
    */
  final case class HistoricalEligibilityUnavailable(ordinal: SnapshotOrdinal, producerId: PeerId) extends ProofError

  /** The eta-rotation chain across L0 suffix headers is inconsistent (one header's eta doesn't match what the prior headers' VRF outputs
    * would produce).
    */
  final case class EtaChainInconsistent(ordinal: SnapshotOrdinal) extends ProofError

  /** A header's `subchainLevelCounts` size doesn't equal `SuperLevelParams.SuperLevelCount` — wire-shape violation. */
  final case class SubchainStateShape(ordinal: SnapshotOrdinal, observedSize: Int) extends ProofError

  /** §3 NIPoPoW S5 — JSON encoder for `ProofError`, used by the HTTP verify endpoint. Hand-rolled (rather than derevo-derived) so we keep a
    * stable `kind` discriminator field across all variants — the TS verifier matches on it. The discriminator name `kind` matches the
    * trigger-name convention already exposed by `FinalityTriggersRoutes`.
    */
  implicit val encoder: Encoder[ProofError] = Encoder.instance {
    case EmptyL0Suffix =>
      Json.obj("kind" -> Json.fromString("empty_l0_suffix"))
    case NonMonotonicOrdinals(level) =>
      Json.obj("kind" -> Json.fromString("non_monotonic_ordinals"), "level" -> Json.fromInt(level))
    case TrialFailed(level, ordinal, observedTau, threshold) =>
      Json.obj(
        "kind" -> Json.fromString("trial_failed"),
        "level" -> Json.fromInt(level),
        "ordinal" -> ordinal.asJson,
        "observed_tau" -> observedTau.asJson,
        "threshold" -> threshold.asJson
      )
    case DensityViolation(level, observed, target, relativeError) =>
      Json.obj(
        "kind" -> Json.fromString("density_violation"),
        "level" -> Json.fromInt(level),
        "observed" -> observed.asJson,
        "target" -> target.asJson,
        "relative_error" -> relativeError.asJson
      )
    case VrfKeyNotRegistered(ordinal, producerId) =>
      Json.obj(
        "kind" -> Json.fromString("vrf_key_not_registered"),
        "ordinal" -> ordinal.asJson,
        "producer_id" -> producerId.asJson
      )
    case VrfProofInvalid(ordinal, producerId) =>
      Json.obj(
        "kind" -> Json.fromString("vrf_proof_invalid"),
        "ordinal" -> ordinal.asJson,
        "producer_id" -> producerId.asJson
      )
    case VrfOutputMismatch(ordinal, producerId) =>
      Json.obj(
        "kind" -> Json.fromString("vrf_output_mismatch"),
        "ordinal" -> ordinal.asJson,
        "producer_id" -> producerId.asJson
      )
    case HistoricalEligibilityUnavailable(ordinal, producerId) =>
      Json.obj(
        "kind" -> Json.fromString("historical_eligibility_unavailable"),
        "ordinal" -> ordinal.asJson,
        "producer_id" -> producerId.asJson
      )
    case EtaChainInconsistent(ordinal) =>
      Json.obj("kind" -> Json.fromString("eta_chain_inconsistent"), "ordinal" -> ordinal.asJson)
    case SubchainStateShape(ordinal, observedSize) =>
      Json.obj(
        "kind" -> Json.fromString("subchain_state_shape"),
        "ordinal" -> ordinal.asJson,
        "observed_size" -> Json.fromInt(observedSize)
      )
  }
}

/** §3 NIPoPoW S4.3 — verifier for a [[TowerProof]].
  *
  * '''Checks performed''' (in order; first failure wins):
  *   1. Structural invariants — non-empty L0 suffix (if proof is non-empty), monotone ordinals per-level + suffix, subchainLevelCounts size
  *      matches `SuperLevelParams.SuperLevelCount`. 2. Registered identity binding — every header's producer must resolve in the frozen
  *      genesis registry and its carried VRF key must match exactly. 3. Cryptographic VRF binding — every suffix and super-level occurrence
  *      verifies under that resolved key over exact `(eta, slot)`, and its carried output must equal `vrfProofToHash(proof)`. 4. Per-level
  *      trial validity — for each header `h` at level µ, recompute the `(τ_µ, θ_µ^eff)` pair from `(h.vrfOutput, g_µ_from_chain,
  *      h.deltaSlot)` and assert `τ < θ_eff` (the trial passes). `g_µ` is reconstructed from prior chain headers at the same level (chain
  *      is self-consistent); first header anchors at `since`. 5. Density check — observed level-µ density `(chain length) / (level-0
  *      reference length)` must fall within `targetDensity * (1 ± DensityRelativeErrorBound)`. Uses the L0 suffix length as the reference
  *      (this is a coarse approximation for short proofs; acceptable for v1 per the empirical 5% bound at 10M slots in `paper/main.tex`).
  *      6. Eta-rotation reconstruction — verify that within the L0 suffix, consecutive headers in the same rotation period carry the same
  *      eta (eta is a per-period invariant). Cross-period eta transitions cannot be verified from suffix alone; we do not treat the
  *      producer's claimed eta as authoritative. 7. Historical L0 eligibility — fail closed because this wire shape does not yet
  *      authenticate the exact branch-relative roster/stake view. `activePoolSize` is never an eligibility input.
  *
  * '''What this does NOT check''' (deferred to S5 + tower-anchored proofs):
  *   - Historical registry membership/activation against the proved branch root; the current injected registry is the frozen-genesis cut.
  *   - State proof inclusion (no MPT path verification in v1 — that's S5).
  *   - A level-chain header's `snapshotHash` against an authenticated tower-entry inclusion witness.
  *   - Cross-period eta correctness against a held-checkpoint genesis eta (light client doesn't have it).
  */
trait TowerVerifier[F[_]] {

  /** Verify a proof anchored at `since`. Returns `Right(())` on accept, `Left(error)` on first failure.
    *
    * @param proof
    *   the [[TowerProof]] to validate.
    * @param genesisEta
    *   the canonical genesis eta byte string (32 bytes). Reserved for future authenticated cross-period reconstruction; the current proof
    *   lacks enough history to use it as an authority.
    * @param etaRotationSnapshots
    *   the period length in snapshots. Used to bucket headers into eta-rotation periods.
    * @param lddConfig
    *   bound consensus LDD configuration. The current verifier uses `lddCutoff` for super-level trials. Historical L0 eligibility remains
    *   fail-closed until the proof authenticates the exact branch roster/stake view needed for the full threshold calculation.
    */
  def verify(
    proof: TowerProof,
    genesisEta: Array[Byte],
    etaRotationSnapshots: Long,
    lddConfig: LddConfig
  ): F[Either[ProofError, Unit]]
}

object TowerVerifier {

  /** Acceptance bound for the per-level density relative-error check (proposal §5.3 / S6 acceptance). Defaults to 5% — verified at 10M
    * slots in `paper/main.tex` Table 1 (per-level achieved-vs-target rates within 1-13%; 5% is a tight gate that catches systematic drift
    * but tolerates short-horizon variance).
    */
  val DensityRelativeErrorBound: Ratio = Ratio(BigInt(5), BigInt(100))

  /** Construct a verifier from the deterministic numerics interpreters. */
  def make[F[_]: Sync](
    log1p: Log1p[F],
    exp: Exp[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F]
  ): TowerVerifier[F] = {
    // Retained in the construction API until an authenticated historical stake view can safely re-enable L0 threshold verification.
    val _ = log1p
    val computer = LevelTrialComputer.make[F](exp)
    new Impl[F](computer, operatorKeyRegistry)
  }

  private class Impl[F[_]: Sync](
    computer: LevelTrialComputer[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F]
  ) extends TowerVerifier[F] {

    private val vrf = EcVrf25519.default

    private def allHeaderOccurrences(proof: TowerProof): Vector[TowerProofHeader] =
      proof.level0Suffix ++ proof.levelChains.toVector.sortBy(_._1).flatMap(_._2)

    private def registeredVrfKey(
      header: TowerProofHeader,
      etaRotationSnapshots: Long
    ): F[Either[ProofError, Array[Byte]]] = {
      val artifactPeriod = io.constellationnetwork.schema.nakamoto.EtaPeriod(header.ordinal.value.value / etaRotationSnapshots)
      ActiveOperatorConsensusKeys.resolve(operatorKeyRegistry, header.producerId, artifactPeriod).map {
        case Some(registeredPair)
            if registeredPair.vrfPublicKey.toBytes.length == VrfPublicKey.ExpectedLength &&
              header.vrfPublicKey.toBytes.length == VrfPublicKey.ExpectedLength &&
              java.security.MessageDigest.isEqual(registeredPair.vrfPublicKey.toBytes, header.vrfPublicKey.toBytes) =>
          Right(registeredPair.vrfPublicKey.toBytes)
        case _ => Left(ProofError.VrfKeyNotRegistered(header.ordinal, header.producerId))
      }
    }

    private def checkRegisteredVrfKeys(proof: TowerProof, etaRotationSnapshots: Long): F[Either[ProofError, Unit]] =
      allHeaderOccurrences(proof).foldLeftM[F, Either[ProofError, Unit]](Right(())) {
        case (Right(()), header) => registeredVrfKey(header, etaRotationSnapshots).map(_.void)
        case (left, _)           => Monad[F].pure(left)
      }

    /** Bind every carried output to a valid proof under the exact preregistered key and `(eta, slot)` message. Occurrences are checked, not
      * ordinal-deduplicated headers, because an attacker can repeat an ordinal with conflicting bytes across tower levels.
      */
    private def checkVrfProofBindings(proof: TowerProof, etaRotationSnapshots: Long): F[Either[ProofError, Unit]] =
      allHeaderOccurrences(proof).foldLeftM[F, Either[ProofError, Unit]](Right(())) {
        case (Right(()), header) =>
          registeredVrfKey(header, etaRotationSnapshots).map {
            case Left(error) => Left(error)
            case Right(registeredVrfKey) =>
              val proofBytes = header.vrfProof.toBytes
              val message = parseEtaHex(header.eta.value).map { eta =>
                eta ++ ByteBuffer.allocate(java.lang.Long.BYTES).putLong(header.slot.value.value).array()
              }
              val verifiedOutput =
                message.flatMap { msg =>
                  scala.util.Try {
                    if (proofBytes.nonEmpty && vrf.vrfVerify(registeredVrfKey, msg, proofBytes)) vrf.vrfProofToHash(proofBytes)
                    else None
                  }.toOption.flatten
                }

              verifiedOutput match {
                case None => Left(ProofError.VrfProofInvalid(header.ordinal, header.producerId))
                case Some(output)
                    if output.length == header.vrfOutput.toBytes.length &&
                      MessageDigest.isEqual(output, header.vrfOutput.toBytes) =>
                  Right(())
                case Some(_) => Left(ProofError.VrfOutputMismatch(header.ordinal, header.producerId))
              }
          }
        case (left, _) => Monad[F].pure(left)
      }

    /** Verify the L0 suffix headers are strictly ordinal-ascending and that each one's `subchainLevelCounts.size == SuperLevelCount`. */
    private def checkStructuralInvariants(proof: TowerProof): Either[ProofError, Unit] =
      if (proof.level0Suffix.isEmpty) Left(ProofError.EmptyL0Suffix)
      else {
        // L0 suffix: monotone ordinals
        val suffixMonotonic = proof.level0Suffix.sliding(2).forall {
          case Vector(a, b) => a.ordinal.value.value < b.ordinal.value.value
          case _            => true
        }
        if (!suffixMonotonic) Left(ProofError.NonMonotonicOrdinals(0))
        else {
          // Each level-µ chain: monotone ordinals
          val levelMonotonic = proof.levelChains.toList.find {
            case (_, chain) =>
              !chain.sliding(2).forall {
                case Vector(a, b) => a.ordinal.value.value < b.ordinal.value.value
                case _            => true
              }
          }
          levelMonotonic match {
            case Some((µ, _)) => Left(ProofError.NonMonotonicOrdinals(µ))
            case None         =>
              // Subchain state shape
              val badShape = allHeaderOccurrences(proof).find(_.subchainLevelCounts.size != SuperLevelParams.SuperLevelCount)
              badShape match {
                case Some(h) => Left(ProofError.SubchainStateShape(h.ordinal, h.subchainLevelCounts.size))
                case None    => Right(())
              }
          }
        }
      }

    /** Re-run each level-µ trial: for header `h_i` at level µ in chain, compute `g_µ = h_i.ordinal - h_{i-1}.ordinal` (or `h_i.ordinal -
      * since` for the first), then run `runTrial(µ, h_i.vrfOutput, g_µ, h_i.deltaSlot, lddCutoff)`. Trial must pass.
      */
    private def checkPerLevelTrials(proof: TowerProof, lddCutoff: Long): F[Either[ProofError, Unit]] = {
      val perLevelChecks: List[F[Either[ProofError, Unit]]] = proof.levelChains.toList.map {
        case (µ, chain) =>
          // Sliding window of (prevOrdinal, header). prevOrdinal anchors at `proof.since` for the first header.
          val withPrev: Vector[(Long, TowerProofHeader)] = chain
            .foldLeft((proof.since.value.value, Vector.empty[(Long, TowerProofHeader)])) {
              case ((prev, acc), h) => (h.ordinal.value.value, acc :+ ((prev, h)))
            }
            ._2
          withPrev.foldLeftM[F, Either[ProofError, Unit]](Right(())) {
            case (Right(()), (prevOrd, h)) =>
              val gMu = h.ordinal.value.value - prevOrd
              computer.runTrial(h.vrfOutput.toBytes, µ, gMu, h.deltaSlot, lddCutoff).map { trial =>
                if (trial.passed) Right(())
                else Left(ProofError.TrialFailed(µ, h.ordinal, trial.tau, trial.effectiveThreshold))
              }
            case (left, _) => Monad[F].pure(left)
          }
      }
      // First-failure-wins fold across levels.
      perLevelChecks.foldLeftM[F, Either[ProofError, Unit]](Right(())) {
        case (Right(()), next) => next
        case (left, _)         => Monad[F].pure(left)
      }
    }

    /** Per-level density check. Observed = `chain.size / l0SuffixSize`. Target = `targetDensity` from `SuperLevelParams`. Allowed rel-err =
      * `DensityRelativeErrorBound` (5%).
      *
      * For very short proofs (suffix < 20 headers), the relative-error denominator becomes noisy; we skip the density check below this
      * threshold to avoid false rejects on small samples. The empirical 5% bound from the paper holds at 10M slots — short proofs are
      * outside the regime where the bound is meaningful.
      */
    private def checkDensities(proof: TowerProof): Either[ProofError, Unit] = {
      val l0SuffixSize = proof.level0Suffix.size.toLong
      val DensitySampleFloor = 20L
      if (l0SuffixSize < DensitySampleFloor) Right(())
      else {
        proof.levelChains.toList.foldLeft[Either[ProofError, Unit]](Right(())) {
          case (Right(()), (µ, chain)) =>
            SuperLevelParams.at(µ) match {
              case None => Right(()) // unknown level — defer to upstream wire-shape; not an error here
              case Some(params) =>
                val observed = Ratio(BigInt(chain.size.toLong), BigInt(l0SuffixSize))
                val relErr = DensityChecker.relativeError(observed, params.targetDensity)
                if (relErr <= DensityRelativeErrorBound) Right(())
                else Left(ProofError.DensityViolation(µ, observed, params.targetDensity, relErr))
            }
          case (left, _) => left
        }
      }
    }

    /** This verifier currently has no authenticated branch-relative roster/stake witness. Rejecting here is deliberate: a carried
      * `activePoolSize` cannot establish relative stake and must never become an eligibility authority.
      */
    private def checkHistoricalL0Eligibility(proof: TowerProof): Either[ProofError, Unit] =
      proof.level0Suffix.headOption
        .map(h => Left(ProofError.HistoricalEligibilityUnavailable(h.ordinal, h.producerId)))
        .getOrElse(Right(()))

    /** Eta-rotation consistency: within a single rotation period, every header must carry the same eta. */
    private def checkEtaChain(proof: TowerProof, etaRotationSnapshots: Long): Either[ProofError, Unit] =
      proof.level0Suffix
        .groupBy(h => h.ordinal.value.value / etaRotationSnapshots)
        .toList
        .foldLeft[Either[ProofError, Unit]](Right(())) {
          case (Right(()), (_, headersInPeriod)) =>
            val etas = headersInPeriod.map(_.eta).distinct
            if (etas.size <= 1) Right(())
            else Left(ProofError.EtaChainInconsistent(headersInPeriod.head.ordinal))
          case (left, _) => left
        }

    override def verify(
      proof: TowerProof,
      genesisEta: Array[Byte],
      etaRotationSnapshots: Long,
      lddConfig: LddConfig
    ): F[Either[ProofError, Unit]] = {
      val _ = genesisEta // Reserved for cross-period eta reconstruction in S5 (light-client checkpoint-anchored verification).
      // Step 1 — structural invariants (pure).
      if (proof == TowerProof.Empty) Monad[F].pure(Right(()))
      else
        checkStructuralInvariants(proof) match {
          case Left(err) => Monad[F].pure(Left(err))
          case Right(()) =>
            // Step 2 — bind every carried tower key to the producer's canonical
            // registry identity before any claimed trial can be considered.
            checkRegisteredVrfKeys(proof, etaRotationSnapshots).flatMap {
              case Left(err) => Monad[F].pure(Left(err))
              case Right(()) =>
                // Step 3 — cryptographically bind every carried output to its proof and exact registered identity.
                checkVrfProofBindings(proof, etaRotationSnapshots).flatMap {
                  case Left(err) => Monad[F].pure(Left(err))
                  case Right(()) =>
                    // Step 4 — per-level trials.
                    checkPerLevelTrials(proof, lddConfig.lddCutoff.toLong).flatMap {
                      case Left(err) => Monad[F].pure(Left(err))
                      case Right(()) =>
                        // Step 5 — densities.
                        checkDensities(proof) match {
                          case Left(err) => Monad[F].pure(Left(err))
                          case Right(()) =>
                            // Step 6 — internally consistent eta claims are necessary but not historical authority.
                            checkEtaChain(proof, etaRotationSnapshots) match {
                              case Left(err) => Monad[F].pure(Left(err))
                              case Right(()) =>
                                // Step 7 — fail closed until an authenticated historical roster/stake witness is part of the proof.
                                Monad[F].pure(checkHistoricalL0Eligibility(proof))
                            }
                        }
                    }
                }
            }
        }
    }

    /** Parse a 64-character hex eta hash into 32 bytes; returns `None` on bad hex. */
    private def parseEtaHex(hex: String): Option[Array[Byte]] =
      scala.util.Try {
        require(hex.length == 64, s"eta hex must be 64 chars, got ${hex.length}")
        hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
      }.toOption
  }
}
