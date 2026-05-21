package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{CommitteeSortition, KesRegistry, MetagraphCommitteeGate}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.slashing.SlashableEvidence.BountyDigestPreimage
import io.constellationnetwork.schema.slashing.{MetagraphAttestation, SlashableEvidence, SlashingRejection}
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Slice S4a — accept-time validator for a [[SlashableEvidence]] L0 transaction.
  *
  * Each of the 9 steps from `SLASHING-DESIGN.md` §4.1 maps to a single `F[Either[SlashingRejection, ...]]` short-circuit. All steps are pure
  * derivations from the evidence bytes + a small amount of finalized chain state:
  *
  *   1. identity match (peerId equality on the two `Signed[MetagraphAttestation]`s)
  *   1. subject match (metagraph address equality)
  *   1. parent match (load-bearing — same VRF input ⇒ same committee draw)
  *   1. distinct binaries (different `binaryHash` ⇒ equivocation, equal ⇒ duplicate retransmission)
  *   1. both KES-signed by the same registered master VK (reuses `KesRegistry` via [[OperationalKeyMaker.verify]])
  *   1. both committee VRF proofs verify (reuses `CommitteeSortition.verifyMembership`)
  *   1. not already slashed (reads [[SlashedSeenReader]] — stubbed in S4a, MPT-backed in S4c)
  *   1. within evidence window (`currentEpoch ≤ eventEpoch + windowEpochs`)
  *   1. submitter bounty signature verifies under `submitterId`'s long-term Ed25519 key
  *
  * '''Safety bar.''' Every check is cryptographically verifiable from the evidence + chain state. No behavioral heuristics, no
  * consensus-state inferences. All honest nodes computing the validator over the same inputs produce byte-identical accept/reject results.
  * See `feedback_slashing_safety_bar`.
  *
  * '''Determinism.''' The validator is pure with respect to: KES registry contents, committee sortition (`CommitteeSortition.make`), the
  * eta/sigma inputs supplied by the caller (which themselves are read from N-2 frozen GSI state — see `project_consensus_epoch_staggering`),
  * and the `SlashedSeenReader` view (caller picks pending vs finalized branch view). No clock, no env reads, no I/O outside these.
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

  /** Validate a [[SlashableEvidence]] under the caller-supplied chain state (eta, sigmas, current epoch).
    *
    * The caller is responsible for sourcing these consistently with the rest of the consensus path:
    *   - `eta` MUST be the 32-byte epoch randomness for the relevant committee draw period (typically the parent's eta at the time of
    *     attestation),
    *   - `sigmaForEvidenceA` / `sigmaForEvidenceB` MUST be the operator's N-2 frozen stake fraction at the same period,
    *   - `kTarget` MUST match what was in effect when the attestations were produced,
    *   - `currentEpoch` MUST be the current chain epoch progress; `eventEpoch` is the epoch at which the attestations were produced
    *     (caller derives this from the same epoch the parent snapshot was created in).
    *
    * Different inputs across nodes ⇒ different accept/reject ⇒ consensus split. The validator does not re-source these — that's the
    * caller's contract, same as every other gl0 acceptance validator.
    */
  def validate(
    evidence: SlashableEvidence,
    eta: Array[Byte],
    sigmaForEvidenceA: Ratio,
    sigmaForEvidenceB: Ratio,
    kTarget: Int,
    currentEpoch: Long,
    eventEpoch: Long
  ): F[Either[SlashingRejection, SlashableEvidence]]
}

object SlashableEvidenceValidator {

  /** Default evidence window — matches `cooldown_epochs` per `SLASHING-DESIGN.md` §6 so the window expires exactly when re-staking becomes
    * possible. Production-grade values are downstream of public testnet experience.
    */
  val DefaultEvidenceWindowEpochs: Long =
    sys.env.get("NAKAMOTO_SLASH_EVIDENCE_WINDOW").flatMap(_.toLongOption).getOrElse(100L)

  /** Construct the validator. Dependencies are intentionally minimal: KES registry + committee sortition + the (stubbed in S4a, MPT-backed
    * in S4c) already-slashed reader. No GSAM, no MPT writer — this slice is purely about accept/reject.
    *
    * @param kesRegistry
    *   used by step #5 to look up the operator's master VK and verify both KES sigs under it
    * @param sortition
    *   used by step #6 to re-verify the committee VRF proofs
    * @param slashedReader
    *   used by step #7 to short-circuit on duplicates (`already slashed`); pass `SlashedSeenReader.neverSlashed` for S4a / pre-S4c
    * @param evidenceWindowEpochs
    *   step #8 lookback — defaults to [[DefaultEvidenceWindowEpochs]]
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    kesRegistry: KesRegistry[F],
    sortition: CommitteeSortition[F],
    slashedReader: SlashedSeenReader[F],
    evidenceWindowEpochs: Long = DefaultEvidenceWindowEpochs
  ): SlashableEvidenceValidator[F] = new SlashableEvidenceValidator[F] {

    def validate(
      evidence: SlashableEvidence,
      eta: Array[Byte],
      sigmaForEvidenceA: Ratio,
      sigmaForEvidenceB: Ratio,
      kTarget: Int,
      currentEpoch: Long,
      eventEpoch: Long
    ): F[Either[SlashingRejection, SlashableEvidence]] = {

      val attA: MetagraphAttestation = evidence.evidenceA.value
      val attB: MetagraphAttestation = evidence.evidenceB.value

      // Step 1 — identity match. Same peer signed both.
      lazy val step1: Either[SlashingRejection, Unit] =
        if (attA.peerId === attB.peerId) Right(())
        else Left(SlashingRejection.IdentityMismatch(attA.peerId, attB.peerId))

      // Step 2 — subject match. Same metagraph address.
      lazy val step2: Either[SlashingRejection, Unit] =
        if (attA.metagraphAddress === attB.metagraphAddress) Right(())
        else Left(SlashingRejection.SubjectMismatch(attA.metagraphAddress, attB.metagraphAddress))

      // Step 3 — parent match (the load-bearing identity).
      lazy val step3: Either[SlashingRejection, Unit] =
        if (attA.parentHash === attB.parentHash) Right(())
        else Left(SlashingRejection.ParentMismatch(attA.parentHash, attB.parentHash))

      // Step 4 — distinct binaries. Equal hash = duplicate retransmission, not equivocation.
      lazy val step4: Either[SlashingRejection, Unit] =
        if (attA.binaryHash =!= attB.binaryHash) Right(())
        else Left(SlashingRejection.DuplicateBinary(attA.binaryHash))

      // Step 5 — both KES sigs verify under the SAME registered master VK for `peerId`. Returns
      // `Left(Some(InvalidKesSignature.OnEvidenceX))` if a sig fails / decodes-fail / no registry
      // entry; the `OnEvidenceA`/`OnEvidenceB` distinction is preserved in the rejection. The no-registry
      // case fails CLOSED in slashing (unlike the lenient gossip-path verifier) — there's no
      // Slice-10 "newly-joined operator pre-registration" carve-out for accusing someone of
      // equivocation; if we can't verify the KES sig from a registered key, we can't establish
      // the evidence cryptographically.
      def verifyKes(att: MetagraphAttestation, onFail: SlashingRejection): F[Either[SlashingRejection, Unit]] =
        kesRegistry.getKesVk(att.peerId).flatMap {
          case None => Async[F].pure(Left(onFail))
          case Some(entry) =>
            val sigBytes = att.kesSignature.toBytes
            if (sigBytes.isEmpty) Async[F].pure(Left(onFail))
            else
              OperationalKeyMaker.decodeSignature(sigBytes) match {
                case Left(_) => Async[F].pure(Left(onFail))
                case Right(kSig) =>
                  val kesStep = att.senderTreeStep
                  if (kesStep < 0) Async[F].pure(Left(onFail))
                  else
                    MetagraphCommitteeGate
                      .messageBytes[F](att.peerId, att.metagraphAddress, att.parentHash, att.binaryHash)
                      .map { msgBytes =>
                        val vkAtStep = entry.vk.copy(step = kesStep)
                        if (OperationalKeyMaker.verify(kSig, msgBytes, vkAtStep)) Right(()) else Left(onFail)
                      }
              }
        }

      // Step 6 — both committee VRF proofs verify under the operator's published VRF VK.
      def verifyCommitteeVrf(
        att: MetagraphAttestation,
        sigma: Ratio,
        onFail: SlashingRejection
      ): F[Either[SlashingRejection, Unit]] =
        sortition
          .verifyMembership(
            vrfVk = att.vrfPublicKey.toBytes,
            eta = eta,
            metagraphAddress = att.metagraphAddress,
            parentHash = att.parentHash,
            sigmaOperatorKey = sigma,
            kTarget = kTarget,
            proof = att.committeeVrfProof.toBytes
          )
          .map(ok => if (ok) Right(()) else Left(onFail))

      // Step 7 — not already slashed (MPT lookup; stubbed in S4a). The reader is keyed by the
      // (peerId, metagraphAddress, parentHash) triple — same identity per `SLASHING-DESIGN.md` §4.1#7.
      def step7Check: F[Either[SlashingRejection, Unit]] =
        slashedReader.wasSlashed(attA.peerId, attA.metagraphAddress, attA.parentHash).map {
          case true  => Left(SlashingRejection.AlreadySlashed(attA.peerId, attA.metagraphAddress, attA.parentHash))
          case false => Right(())
        }

      // Step 8 — within evidence window. `currentEpoch ≤ eventEpoch + windowEpochs` ⇒ accept.
      // Comparing the inclusive sum (so e.g. window=100, event=0 accepts current ≤ 100, rejects 101+).
      lazy val step8: Either[SlashingRejection, Unit] =
        if (currentEpoch <= eventEpoch + evidenceWindowEpochs) Right(())
        else Left(SlashingRejection.EvidenceWindowExpired(currentEpoch, eventEpoch, evidenceWindowEpochs))

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

      val pipeline: F[Either[SlashingRejection, Unit]] = chain(
        List(
          lift(step1),
          lift(step2),
          lift(step3),
          lift(step4),
          lift(step8),
          verifyKes(attA, SlashingRejection.InvalidKesSignature.OnEvidenceA),
          verifyKes(attB, SlashingRejection.InvalidKesSignature.OnEvidenceB),
          verifyCommitteeVrf(attA, sigmaForEvidenceA, SlashingRejection.InvalidCommitteeVrf.OnEvidenceA),
          verifyCommitteeVrf(attB, sigmaForEvidenceB, SlashingRejection.InvalidCommitteeVrf.OnEvidenceB),
          step7Check,
          step9Check
        )
      )

      pipeline.map(_.map(_ => evidence))
    }
  }

  /** Helper: produce the canonical bytes the submitter signs to claim the bounty. Exposed so detector (S4b) + tests can build the
    * `bountySignature` field without re-deriving the digest recipe.
    */
  def bountyDigestBytes[F[_]: cats.Functor: Hasher](
    evidenceA: io.constellationnetwork.security.signature.Signed[MetagraphAttestation],
    evidenceB: io.constellationnetwork.security.signature.Signed[MetagraphAttestation],
    submitterId: PeerId
  ): F[Array[Byte]] = {
    val preimage = BountyDigestPreimage(evidenceA, evidenceB, submitterId.value.value)
    Hasher[F].hash(preimage).map(_.getBytes)
  }
}
