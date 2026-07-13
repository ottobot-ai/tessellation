package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{CommitteeSortition, MetagraphCommitteeGate}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.slashing.SlashableEvidence.BountyDigestPreimage
import io.constellationnetwork.schema.slashing.{MetagraphAttestation, SlashableEvidence, SlashingRejection}
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Slice S4a — accept-time validator for a [[SlashableEvidence]] L0 transaction.
  *
  * Each of the 9 steps from `SLASHING-DESIGN.md` §4.1 maps to a single `F[Either[SlashingRejection, ...]]` short-circuit. All steps are
  * pure derivations from the evidence bytes + a small amount of finalized chain state:
  *
  *   1. identity match (peerId equality on the two [[MetagraphAttestation]] bodies)
  *   1. subject match (metagraph address equality)
  *   1. parent match, followed by exact historical eta-period equality (both are part of the committee draw identity)
  *   1. distinct binaries (different `binaryHash` ⇒ equivocation, equal ⇒ duplicate retransmission)
  *   1. exact offence-parent state resolves one active atomic KES+VRF pair for the offender
  *   1. both KES-signed by that pair at `offencePeriod - registeredOffset`
  *   1. both committee VRF proofs verify under the offender's registered VRF key (reuses `CommitteeSortition.verifyMembership`)
  *   1. not already slashed (reads [[SlashedSeenReader]] — stubbed in S4a, MPT-backed in S4c)
  *   1. within evidence window (`currentEpoch ≤ eventEpoch + windowEpochs`)
  *   1. submitter bounty signature verifies under `submitterId`'s long-term Ed25519 key
  *
  * '''Safety bar.''' Every check is cryptographically verifiable from the evidence + chain state. No behavioral heuristics, no
  * consensus-state inferences. All honest nodes computing the validator over the same inputs produce byte-identical accept/reject results.
  * See `feedback_slashing_safety_bar`.
  *
  * '''Determinism.''' Key material, offence period, and VRF eta come from [[SlashingOperatorKeyResolver]], whose implementation must prove
  * the exact historical state governing the signed parent. Missing/pruned/ambiguous history returns `Unverifiable` and can never slash.
  *
  * '''What's stubbed in S4a.''' The MPT partition `slashings/<peer>/<metagraph>/<parent>` is not yet implemented; this validator depends on
  * [[SlashedSeenReader]] which the test suite stubs and S4c will back with a `GlobalStateReader`-derived implementation. The validator
  * itself is unchanged when that lands — only its constructor argument shape.
  *
  * '''What's out of scope.''' This is acceptance validation only. The accept-time ledger effects (stake reduction, cooldown, bounty credit,
  * burn) per `SLASHING-DESIGN.md` §5 belong to GSAM and land in S4c. The observer/detector (S4b) submits the evidence; this validator
  * decides whether to admit it.
  */
trait SlashableEvidenceValidator[F[_]] {

  /** Validate a [[SlashableEvidence]] under exact historical key/randomness state plus caller-supplied stake fractions.
    *
    * The caller is responsible for sourcing these consistently with the rest of the consensus path:
    *   - `sigmaForEvidenceA` / `sigmaForEvidenceB` MUST be the operator's N-2 frozen stake fraction at the same period,
    *   - `kTarget` MUST match what was in effect when the attestations were produced,
    *   - `currentEpoch` MUST be the current chain epoch progress. The evidence epoch is resolved from the signed parent context; it is not
    *     accepted out of band.
    *
    * Different inputs across nodes ⇒ different accept/reject ⇒ consensus split. The validator does not re-source these — that's the
    * caller's contract, same as every other gl0 acceptance validator.
    */
  def validate(
    evidence: SlashableEvidence,
    sigmaForEvidenceA: Ratio,
    sigmaForEvidenceB: Ratio,
    kTarget: Int,
    currentEpoch: Long
  ): F[SlashingValidationResult[SlashingRejection, SlashableEvidence]]
}

object SlashableEvidenceValidator {

  /** Consensus evidence window. This cannot come from a node-local environment variable: differing windows would split acceptance and could
    * slash an operator on one node while rejecting the same proof on another.
    */
  val DefaultEvidenceWindowEpochs: Long = 100L

  /** Construct the validator. There is deliberately no constructor accepting separate/current KES or VRF registries.
    *
    * @param keyResolver
    *   resolves the atomic active KES+VRF pair, offence period, and eta from the exact signed-parent historical context
    * @param sortition
    *   used by step #6 to re-verify the committee VRF proofs
    * @param slashedReader
    *   used by step #7 to short-circuit on duplicates (`already slashed`); pass `SlashedSeenReader.neverSlashed` for S4a / pre-S4c
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    keyResolver: SlashingOperatorKeyResolver[F],
    sortition: CommitteeSortition[F],
    slashedReader: SlashedSeenReader[F]
  ): SlashableEvidenceValidator[F] = new SlashableEvidenceValidator[F] {

    def validate(
      evidence: SlashableEvidence,
      sigmaForEvidenceA: Ratio,
      sigmaForEvidenceB: Ratio,
      kTarget: Int,
      currentEpoch: Long
    ): F[SlashingValidationResult[SlashingRejection, SlashableEvidence]] = {

      val attA: MetagraphAttestation = evidence.evidenceA
      val attB: MetagraphAttestation = evidence.evidenceB

      // Step 1 — identity match. Same peer signed both.
      lazy val step1: Either[SlashingRejection, Unit] =
        if (attA.peerId === attB.peerId) Right(())
        else Left(SlashingRejection.IdentityMismatch(attA.peerId, attB.peerId))

      // Step 2 — subject match. Same metagraph address.
      lazy val step2: Either[SlashingRejection, Unit] =
        if (attA.metagraphAddress === attB.metagraphAddress) Right(())
        else Left(SlashingRejection.SubjectMismatch(attA.metagraphAddress, attB.metagraphAddress))

      // Step 3 — parent match. Historical resolution later proves eta-period equality; parent equality alone is insufficient.
      lazy val step3: Either[SlashingRejection, Unit] =
        if (attA.parentHash === attB.parentHash) Right(())
        else Left(SlashingRejection.ParentMismatch(attA.parentHash, attB.parentHash))

      // Step 4 — distinct binaries. Equal hash = duplicate retransmission, not equivocation.
      lazy val step4: Either[SlashingRejection, Unit] =
        if (attA.binaryHash =!= attB.binaryHash) Right(())
        else Left(SlashingRejection.DuplicateBinary(attA.binaryHash))

      def context(att: MetagraphAttestation): SlashingOffenceContext.MetagraphAdmission =
        SlashingOffenceContext.MetagraphAdmission(att.metagraphAddress, att.parentHash, att.binaryHash)

      def resolve(
        att: MetagraphAttestation
      ): F[Either[SlashingUnverifiableReason, SlashingOperatorKeyResolution.Resolved]] = {
        val offenceContext = context(att)
        keyResolver.resolve(att.peerId, offenceContext).attempt.map {
          case Left(_) =>
            Left(
              SlashingUnverifiableReason.HistoricalKeyStateUnavailable(
                offenceContext,
                HistoricalStateUnavailableReason.RegistryStateInvalid
              )
            )
          case Right(SlashingOperatorKeyResolution.HistoricalStateUnavailable(reason)) =>
            Left(SlashingUnverifiableReason.HistoricalKeyStateUnavailable(offenceContext, reason))
          case Right(resolved: SlashingOperatorKeyResolution.Resolved) =>
            ResolvedSlashingOperatorKeys.validate(att.peerId, resolved).map(_ => resolved)
        }
      }

      // The carried step is comparison evidence only. The historical pair offset and offence period derive the only accepted step.
      def verifyKes(
        att: MetagraphAttestation,
        resolved: SlashingOperatorKeyResolution.Resolved,
        expectedStep: Int,
        onFail: SlashingRejection
      ): F[Either[SlashingRejection, Unit]] = {
        val sigBytes = att.kesSignature.toBytes
        if (sigBytes.isEmpty || att.senderTreeStep != expectedStep) Async[F].pure(Left(onFail))
        else
          OperationalKeyMaker.decodeSignature(sigBytes) match {
            case Left(_) => Async[F].pure(Left(onFail))
            case Right(kSig) =>
              MetagraphCommitteeGate
                .messageBytes[F](att.peerId, att.metagraphAddress, att.parentHash, att.binaryHash)
                .map { msgBytes =>
                  val vkAtStep = resolved.keys.kes.vk.copy(step = expectedStep)
                  if (OperationalKeyMaker.verify(kSig, msgBytes, vkAtStep)) Right(()) else Left(onFail)
                }
                .handleError(_ => Left(onFail))
          }
      }

      // Step 6 — both committee VRF proofs verify under the exact historical pair. The evidence-carried key is comparison evidence only:
      // it must byte-match that pair, and only the resolved bytes reach the verifier.
      def verifyCommitteeVrf(
        att: MetagraphAttestation,
        resolved: SlashingOperatorKeyResolution.Resolved,
        sigma: Ratio,
        onFail: SlashingRejection
      ): F[Either[SlashingRejection, Unit]] = {
        val registeredVrfVk = resolved.keys.vrfPublicKey.toBytes
        if (
          att.vrfPublicKey.toBytes.length == io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.ExpectedLength &&
          java.security.MessageDigest.isEqual(registeredVrfVk, att.vrfPublicKey.toBytes)
        )
          sortition
            .verifyMembership(
              vrfVk = registeredVrfVk,
              eta = resolved.vrfEta,
              metagraphAddress = att.metagraphAddress,
              parentHash = att.parentHash,
              sigmaOperatorKey = sigma,
              // The committee-membership re-verify uses the DRAW target (`CommitteeSortition.verifyMembership`'s `kDraw`); this
              // validator's own `kTarget` param IS that draw value (it must match what was in effect when the attestation was produced).
              kDraw = kTarget,
              proof = att.committeeVrfProof.toBytes
            )
            .map(ok => if (ok) Right(()) else Left(onFail))
            .handleError(_ => Left(onFail))
        else Async[F].pure(Left(onFail))
      }

      // Step 7 — not already slashed (MPT lookup; stubbed in S4a). The reader is keyed by the
      // (peerId, metagraphAddress, parentHash) triple — same identity per `SLASHING-DESIGN.md` §4.1#7.
      def step7Check: F[Either[SlashingRejection, Unit]] =
        slashedReader.wasSlashed(attA.peerId, attA.metagraphAddress, attA.parentHash).map {
          case true  => Left(SlashingRejection.AlreadySlashed(attA.peerId, attA.metagraphAddress, attA.parentHash))
          case false => Right(())
        }

      // Step 9 — submitter signed the bounty preimage with their long-term Ed25519 key. The preimage
      // binds (evidenceA, evidenceB, submitterId) so the bounty can't be lifted by re-submitting
      // someone else's evidence.
      def step9Check: F[Either[SlashingRejection, Unit]] = {
        val preimage = BountyDigestPreimage(evidence.evidenceA, evidence.evidenceB, evidence.submitterId.value.value)
        for {
          digest <- Hasher[F].hash(preimage)
          submitterPk <- evidence.submitterId.value.toPublicKey[F]
          sigBytes = evidence.bountySignature.value.toBytes
          ok <- Signing.verifySignature[F](digest.getBytes, sigBytes)(submitterPk)
        } yield if (ok) Right(()) else Left(SlashingRejection.InvalidBountySignature)
      }

      // Short-circuit the cheap pure checks first (1-4 + 8) before the expensive crypto verifies
      // (5, 6, 9) and the I/O lookup (7). Per `SLASHING-DESIGN.md` §4.1 last sentence: steps 5-6 are
      // the expensive ones; cache-hot ordering puts them after the cheap rejections.
      val asyncUnit: F[Either[SlashingRejection, Unit]] = Async[F].pure(Right(()))
      def lift(e: Either[SlashingRejection, Unit]): F[Either[SlashingRejection, Unit]] = Async[F].pure(e)

      def chain(checks: List[F[Either[SlashingRejection, Unit]]]): F[Either[SlashingRejection, Unit]] =
        checks.foldLeft(asyncUnit) { (acc, next) =>
          acc.flatMap {
            case Left(r)  => Async[F].pure(Left(r))
            case Right(_) => next
          }
        }

      chain(List(lift(step1), lift(step2), lift(step3), lift(step4))).flatMap {
        case Left(reason) => Async[F].pure(SlashingValidationResult.Invalid(reason))
        case Right(()) =>
          (resolve(attA), resolve(attB)).tupled.flatMap {
            case (Left(reason), _) => Async[F].pure(SlashingValidationResult.Unverifiable(reason))
            case (_, Left(reason)) => Async[F].pure(SlashingValidationResult.Unverifiable(reason))
            case (Right(resolvedA), Right(resolvedB)) =>
              if (resolvedA.offencePeriod != resolvedB.offencePeriod)
                Async[F].pure(
                  SlashingValidationResult.Invalid(
                    SlashingRejection.AdmissionDrawPeriodMismatch(resolvedA.offencePeriod, resolvedB.offencePeriod)
                  )
                )
              else if (!java.security.MessageDigest.isEqual(resolvedA.vrfEta, resolvedB.vrfEta))
                Async[F].pure(SlashingValidationResult.Unverifiable(SlashingUnverifiableReason.ResolvedRandomnessMismatch))
              else if (!ResolvedSlashingOperatorKeys.sameAtomicPair(resolvedA, resolvedB))
                Async[F].pure(SlashingValidationResult.Unverifiable(SlashingUnverifiableReason.ResolvedAtomicPairMismatch))
              else
                (
                  ResolvedSlashingOperatorKeys.expectedKesStep(resolvedA),
                  ResolvedSlashingOperatorKeys.expectedKesStep(resolvedB)
                ) match {
                  case (Left(reason), _) => Async[F].pure(SlashingValidationResult.Unverifiable(reason))
                  case (_, Left(reason)) => Async[F].pure(SlashingValidationResult.Unverifiable(reason))
                  case (Right(expectedStepA), Right(expectedStepB)) =>
                    val eventEpoch = resolvedA.offencePeriod.value
                    val expired = BigInt(currentEpoch) > BigInt(eventEpoch) + BigInt(DefaultEvidenceWindowEpochs)
                    if (expired)
                      Async[F].pure(
                        SlashingValidationResult.Invalid(
                          SlashingRejection.EvidenceWindowExpired(currentEpoch, eventEpoch, DefaultEvidenceWindowEpochs)
                        )
                      )
                    else
                      chain(
                        List(
                          verifyKes(attA, resolvedA, expectedStepA, SlashingRejection.InvalidKesSignature.OnEvidenceA),
                          verifyKes(attB, resolvedB, expectedStepB, SlashingRejection.InvalidKesSignature.OnEvidenceB),
                          verifyCommitteeVrf(
                            attA,
                            resolvedA,
                            sigmaForEvidenceA,
                            SlashingRejection.InvalidCommitteeVrf.OnEvidenceA
                          ),
                          verifyCommitteeVrf(
                            attB,
                            resolvedB,
                            sigmaForEvidenceB,
                            SlashingRejection.InvalidCommitteeVrf.OnEvidenceB
                          ),
                          step7Check,
                          step9Check
                        )
                      ).map {
                        case Left(reason) => SlashingValidationResult.Invalid(reason)
                        case Right(())    => SlashingValidationResult.Valid(evidence)
                      }
                }
          }
      }
    }
  }

  /** Helper: produce the canonical bytes the submitter signs to claim the bounty. Exposed so detector (S4b) + tests can build the
    * `bountySignature` field without re-deriving the digest recipe.
    *
    * Takes the bare [[MetagraphAttestation]] bodies (not `Signed[_]` envelopes) — the detector strips the gossip-layer envelope via
    * `.value` before constructing evidence. See [[io.constellationnetwork.schema.slashing.SlashableEvidence]] scaladoc for why the outer
    * envelope is dropped (KES + VRF in the body carry the safety).
    */
  def bountyDigestBytes[F[_]: cats.Functor: Hasher](
    evidenceA: MetagraphAttestation,
    evidenceB: MetagraphAttestation,
    submitterId: PeerId
  ): F[Array[Byte]] = {
    val preimage = BountyDigestPreimage(evidenceA, evidenceB, submitterId.value.value)
    Hasher[F].hash(preimage).map(_.getBytes)
  }
}
