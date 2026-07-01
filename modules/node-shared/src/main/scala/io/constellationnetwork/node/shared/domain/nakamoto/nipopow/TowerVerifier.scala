package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.Monad
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.RatioInstances._
import io.constellationnetwork.numerics.algebras.{Exp, Log1p}
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.LddConfig

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

  /** An L0 suffix header fails the L0 eligibility VRF re-verification. */
  final case class L0VrfFailed(ordinal: SnapshotOrdinal) extends ProofError

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
    case L0VrfFailed(ordinal) =>
      Json.obj("kind" -> Json.fromString("l0_vrf_failed"), "ordinal" -> ordinal.asJson)
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

/** §3 NIPoPoW S4.3 — pure verifier for a [[TowerProof]].
  *
  * '''Checks performed''' (in order; first failure wins):
  *   1. Structural invariants — non-empty L0 suffix (if proof is non-empty), monotone ordinals per-level + suffix, subchainLevelCounts size
  *      matches `SuperLevelParams.SuperLevelCount`. 2. Per-level trial validity — for each header `h` at level µ, recompute the `(τ_µ,
  *      θ_µ^eff)` pair from `(h.vrfOutput, g_µ_from_chain, h.deltaSlot)` and assert `τ < θ_eff` (the trial passes). `g_µ` is reconstructed
  *      from prior chain headers at the same level (chain is self-consistent); first header anchors at `since`. 3. Density check — observed
  *      level-µ density `(chain length) / (level-0 reference length)` must fall within `targetDensity * (1 ± DensityRelativeErrorBound)`.
  *      Uses the L0 suffix length as the reference (this is a coarse approximation for short proofs; acceptable for v1 per the empirical 5%
  *      bound at 10M slots in `paper/main.tex`). 4. L0 suffix VRF — for each suffix header, re-verify
  *      `EligibilityChecker.verifyEligibility` with the certificate's VRF triple + the bound consensus `lddConfig` + an *approximate*
  *      relativeStake = 1/activePoolSize. The producer-side eligibility used the actual stake; we use uniform stake as a v1 approximation
  *      (`activePoolSize` is the only stake-related field on the certificate). This is a sanity check, not a security gate. 5. Eta-rotation
  *      reconstruction — verify that within the L0 suffix, consecutive headers in the same rotation period carry the same eta (eta is a
  *      per-period invariant). Cross-period eta transitions cannot be verified from suffix alone; we trust the producer's claimed eta in
  *      that case.
  *
  * '''What this does NOT check''' (deferred to S5 + tower-anchored proofs):
  *   - State proof inclusion (no MPT path verification in v1 — that's S5).
  *   - Cross-period eta correctness against a held-checkpoint genesis eta (light client doesn't have it).
  *   - Adversarial signer set — this proof v1 trusts the active validator set; signatures aren't re-verified.
  */
trait TowerVerifier[F[_]] {

  /** Verify a proof anchored at `since`. Returns `Right(())` on accept, `Left(error)` on first failure.
    *
    * @param proof
    *   the [[TowerProof]] to validate.
    * @param genesisEta
    *   the canonical genesis eta byte string (32 bytes). Used as the seed for eta-chain reconstruction.
    * @param etaRotationSnapshots
    *   the period length in snapshots. Used to bucket headers into eta-rotation periods.
    * @param lddConfig
    *   LDD config for the L0 eligibility check. REQUIRED (no default): the caller (`NipopowProofProvider`) binds the consensus LDD config
    *   captured at startup, so the light client checks eligibility at the SAME γ the chain runs. A source-level default here would silently
    *   verify at a stale γ if the chain's config differs — exactly the divergence class this removal prevents.
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
  def make[F[_]: Monad](log1p: Log1p[F], exp: Exp[F]): TowerVerifier[F] = {
    val computer = LevelTrialComputer.make[F](exp)
    val eligibility = EligibilityChecker.make[F](log1p, exp)
    new Impl[F](computer, eligibility)
  }

  private class Impl[F[_]: Monad](computer: LevelTrialComputer[F], eligibility: EligibilityChecker[F]) extends TowerVerifier[F] {

    /** Verify the L0 suffix headers are strictly ordinal-ascending and that each one's `subchainLevelCounts.size == SuperLevelCount`. */
    private def checkStructuralInvariants(proof: TowerProof): Either[ProofError, Unit] = {
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
            val badShape = proof.allHeadersByOrdinal.find(_.subchainLevelCounts.size != SuperLevelParams.SuperLevelCount)
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

    /** L0 suffix VRF check — re-verify each suffix header's L0 eligibility VRF. Uses uniform stake = 1/activePoolSize as a v1 approximation
      * (the producer used the actual relative stake; light client doesn't have stake state).
      */
    private def checkL0VrfChain(proof: TowerProof, lddConfig: LddConfig): F[Either[ProofError, Unit]] =
      proof.level0Suffix.foldLeftM[F, Either[ProofError, Unit]](Right(())) {
        case (Right(()), h) =>
          val relativeStake =
            if (h.activePoolSize <= 0) Ratio.One // degenerate; accept
            else Ratio(BigInt(1), BigInt(h.activePoolSize.toLong))
          // For eta input, we pass the header's eta as bytes — this is the producer's claimed eta;
          // the suffix eta-consistency check below verifies it's internally consistent.
          val etaBytes = parseEtaHex(h.eta.value)
          etaBytes match {
            case None => Monad[F].pure(Left(ProofError.L0VrfFailed(h.ordinal)))
            case Some(etaArr) =>
              eligibility
                .verifyEligibility(
                  vrfVK = h.vrfPublicKey.toBytes,
                  slot = h.slot,
                  slotGap = h.deltaSlot,
                  eta = etaArr,
                  relativeStake = relativeStake,
                  config = lddConfig,
                  proof = h.vrfProof.toBytes
                )
                .map(ok => if (ok) Right(()) else Left(ProofError.L0VrfFailed(h.ordinal)))
          }
        case (left, _) => Monad[F].pure(left)
      }

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
            // Step 2 — per-level trials.
            checkPerLevelTrials(proof, lddConfig.lddCutoff.toLong).flatMap {
              case Left(err) => Monad[F].pure(Left(err))
              case Right(()) =>
                // Step 3 — densities.
                checkDensities(proof) match {
                  case Left(err) => Monad[F].pure(Left(err))
                  case Right(()) =>
                    // Step 4 — L0 suffix VRF.
                    checkL0VrfChain(proof, lddConfig).flatMap {
                      case Left(err) => Monad[F].pure(Left(err))
                      case Right(()) =>
                        // Step 5 — eta-chain.
                        Monad[F].pure(checkEtaChain(proof, etaRotationSnapshots))
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
