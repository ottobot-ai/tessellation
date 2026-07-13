package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.nio.ByteBuffer

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardCheckpoint}
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Slice 16 — accept-time validator for a [[ShardCheckpointEquivocationEvidence]].
  *
  * Mirrors [[SlashableEvidenceValidator]] (slice S4a) one layer up at the shard-checkpoint scope: same "cryptographically verifiable proof
  * of one signer producing two contradictory artefacts" algebra, applied to [[ShardCheckpoint]] envelopes per
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.1.
  *
  * '''Safety bar.''' Every check is cryptographically verifiable from the evidence plus an exact historical atomic KES+VRF pair. Missing or
  * ambiguous historical state is `Unverifiable`, never evidence of guilt.
  *
  * '''Seven steps:'''
  *   1. Parent-hash match (header agrees with both children) — pure field equality.
  *   1. Shard-id match (header agrees with both children) — pure field equality.
  *   1. Distinct canonical child hashes (via `Hasher[F].hash(child.signingPreimage)`) — equal ⇒ duplicate retransmission, not equivocation.
  *   1. Signer present in both children's `committeeSignatures` lists — extracts the offender's per-child signature contribution.
  *   1. Ed25519 signature on both children verifies under the signer's recovered long-term VK over the respective preimage-hash bytes.
  *   1. KES product signature on both children verifies under the exact historical pair at the derived KES step.
  *   1. VRF possession proof on both children verifies under that same pair over the historical shard eta and signed slot.
  *
  * '''Ordering rationale.''' Cheap pure equality checks (1–4) short-circuit before the expensive crypto verifies (5–6). Same cache-hot
  * ordering as [[SlashableEvidenceValidator]]: pure header equality + map lookup → recovered-public-key Ed25519 verify → KES Merkle-path
  * verify.
  *
  * '''Determinism.''' The resolver must prove the unique state governing each child's signed context. A receiver-current registry or an
  * ordinal-only branch guess is forbidden.
  *
  * '''Unresolved design blocker.''' Execution signatures attest locally reproduced state validity. The current protocol does not define a
  * consensus-safe exclusivity rule proving that an honest execution signer must refuse every second valid proposal at the same parent, nor
  * evidence for such a rule. Two valid state attestations therefore do not by themselves prove misconduct. After authenticating both legs
  * this validator returns `CheckpointSignerExclusivityNotSpecified`, not guilt. Do not replace this with a BFT lock or an implicit
  * one-signature rule; the owner must specify the intended Nakamoto/staircase behavior first.
  *
  * '''What this slice doesn't do.''' Following the [[ShardCheckpointEquivocationEvidence]] scaladoc — bounty/submitter logic lives on the
  * wrapping L0 tx type (handled by the GSAM accept-path agent, mirroring `SLASHING-DESIGN.md` §5). Already-slashed deduplication +
  * evidence-window checks live downstream too, keyed by `(equivocatingSigner, shardId, parentCheckpointHash)` — same triple shape as
  * [[io.constellationnetwork.schema.slashing.SlashingRejection.AlreadySlashed]] just with `shardId` substituted for `metagraphAddress`.
  * Slice 16 delivers the cryptographic-proof half; ledger effects (stake reduction, eviction, bounty, burn) compose downstream per
  * [[https://github.com/Constellation-Labs/tessellation-nakamoto/blob/main/docs/nakamoto/SLASHING-DESIGN.md SLASHING-DESIGN.md]] §5.
  */
trait ShardCheckpointEquivocationValidator[F[_]] {

  /** Validate equivocation evidence. Only `Valid(evidence)` proves guilt; `Unverifiable` is a defer/recovery outcome and must never slash.
    */
  def validate(
    evidence: ShardCheckpointEquivocationEvidence
  ): F[SlashingValidationResult[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]]
}

object ShardCheckpointEquivocationValidator {

  /** There is deliberately no constructor accepting a current or KES-only registry. */
  def make[F[_]: Async: Hasher: SecurityProvider](
    keyResolver: SlashingOperatorKeyResolver[F]
  ): ShardCheckpointEquivocationValidator[F] = new ShardCheckpointEquivocationValidator[F] {

    def validate(
      evidence: ShardCheckpointEquivocationEvidence
    ): F[SlashingValidationResult[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]] = {

      val childA: ShardCheckpoint = evidence.childA
      val childB: ShardCheckpoint = evidence.childB
      val signer: PeerId = evidence.equivocatingSigner

      // Step 1 — parent-hash match. Both children point to the same parent AND the evidence header agrees.
      lazy val step1: Either[ShardCheckpointEquivocationRejection, Unit] =
        if (
          childA.parentCheckpointHash === childB.parentCheckpointHash &&
          childA.parentCheckpointHash === evidence.parentCheckpointHash
        ) Right(())
        else
          Left(
            ShardCheckpointEquivocationRejection.ParentMismatch(
              parentA = childA.parentCheckpointHash,
              parentB = childB.parentCheckpointHash,
              evidenceParent = evidence.parentCheckpointHash
            )
          )

      // Step 2 — shard-id match. Both children share the same shard AND the evidence header agrees.
      lazy val step2: Either[ShardCheckpointEquivocationRejection, Unit] =
        if (childA.shardId === childB.shardId && childA.shardId === evidence.shardId) Right(())
        else
          Left(
            ShardCheckpointEquivocationRejection.ShardMismatch(
              shardA = childA.shardId,
              shardB = childB.shardId,
              evidenceShard = evidence.shardId
            )
          )

      // Competing children occupy one shard height and one execution committee period. A different ordinal is not the same chain
      // position; a different period is a different committee/key draw.
      lazy val step2b: Either[ShardCheckpointEquivocationRejection, Unit] =
        if (childA.shardOrdinal =!= childB.shardOrdinal)
          Left(ShardCheckpointEquivocationRejection.ShardOrdinalMismatch(childA.shardOrdinal, childB.shardOrdinal))
        else if (childA.epoch =!= childB.epoch)
          Left(ShardCheckpointEquivocationRejection.ExecutionPeriodMismatch(childA.epoch, childB.epoch))
        else Right(())

      // Step 3 — distinct canonical child hashes. Computed via Hasher[F].hash(signingPreimage) — identical canonical-bytes recipe to the
      // one the producer uses to derive the bytes the per-child signatures cover. Equal ⇒ duplicate retransmission, not equivocation.
      def step3Check: F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        for {
          hashA <- Hasher[F].hash(childA.signingPreimage)
          hashB <- Hasher[F].hash(childB.signingPreimage)
        } yield
          if (hashA =!= hashB) Right(())
          else Left(ShardCheckpointEquivocationRejection.SameChildHash(hashA))

      // Step 4 — signer present in both children's committeeSignatures lists. Returns the per-child signature contribution for use in
      // steps 5/6, or short-circuits with the rejection identifying which child lacked the signer.
      def lookupSignerSigs(): Either[ShardCheckpointEquivocationRejection, (CommitteeMemberSignature, CommitteeMemberSignature)] = {
        val sigAOpt = childA.committeeSignatures.find(_.peerId === signer)
        val sigBOpt = childB.committeeSignatures.find(_.peerId === signer)
        (sigAOpt, sigBOpt) match {
          case (None, _)          => Left(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildA(signer))
          case (_, None)          => Left(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildB(signer))
          case (Some(a), Some(b)) => Right((a, b))
        }
      }

      // Step 5 — Ed25519 signature verification. Recovers the signer's long-term VK from PeerId.value.toPublicKey and verifies the sig
      // bytes against the child's canonical preimage-hash bytes (same byte recipe the producer signs).
      def verifyEd25519(
        sig: CommitteeMemberSignature,
        preimageHashBytes: Array[Byte],
        onFail: ShardCheckpointEquivocationRejection
      ): F[Either[ShardCheckpointEquivocationRejection, Unit]] = {
        val verifyResult: F[Boolean] =
          for {
            pk <- signer.value.toPublicKey[F]
            sigBytes = Signature(sig.ed25519Sig).value.toBytes
            ok <- Signing.verifySignature[F](preimageHashBytes, sigBytes)(pk)
          } yield ok
        verifyResult
          .handleError(_ => false)
          .map(ok => if (ok) Right(()) else Left(onFail))
      }

      // The wire step is comparison evidence only. The active pair offset and signed offence period derive the accepted step.
      def verifyKes(
        sig: CommitteeMemberSignature,
        preimageHashBytes: Array[Byte],
        resolved: SlashingOperatorKeyResolution.Resolved,
        expectedStep: Int,
        onFail: ShardCheckpointEquivocationRejection
      ): F[Either[ShardCheckpointEquivocationRejection, Unit]] = {
        val sigBytes = sig.kesProductSig.toBytes
        if (sigBytes.isEmpty || sig.kesTreeStep != expectedStep) Async[F].pure(Left(onFail))
        else
          Async[F].delay {
            OperationalKeyMaker.decodeSignature(sigBytes) match {
              case Left(_) => Left(onFail)
              case Right(kSig) =>
                val vkAtStep = resolved.keys.kes.vk.copy(step = expectedStep)
                if (OperationalKeyMaker.verify(kSig, preimageHashBytes, vkAtStep)) Right(()) else Left(onFail)
            }
          }.handleError(_ => Left(onFail))
      }

      // CommitteeMemberSignature carries a possession proof, not a public key. The proof is verified only under the atomic pair selected
      // by historical state; there is no wire key that can become authority.
      def verifyVrf(
        child: ShardCheckpoint,
        sig: CommitteeMemberSignature,
        resolved: SlashingOperatorKeyResolution.Resolved,
        onFail: ShardCheckpointEquivocationRejection
      ): F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        if (sig.vrfProof.toBytes.isEmpty) Async[F].pure(Left(onFail))
        else
          Async[F].delay {
            val slotBytes = ByteBuffer.allocate(8).putLong(child.slot.value.value).array()
            val message = resolved.vrfEta ++ slotBytes
            EcVrf25519.default.vrfVerify(resolved.keys.vrfPublicKey.toBytes, message, sig.vrfProof.toBytes)
          }
            .handleError(_ => false)
            .map(ok => if (ok) Right(()) else Left(onFail))

      def resolve(
        child: ShardCheckpoint
      ): F[Either[SlashingUnverifiableReason, SlashingOperatorKeyResolution.Resolved]] = {
        val context = SlashingOffenceContext.ShardCheckpointExecution(
          child.shardId,
          child.parentCheckpointHash,
          child.shardOrdinal,
          child.gl0AnchorOrdinal,
          child.epoch
        )

        keyResolver.resolve(signer, context).attempt.map {
          case Left(_) =>
            Left(
              SlashingUnverifiableReason.HistoricalKeyStateUnavailable(
                context,
                HistoricalStateUnavailableReason.RegistryStateInvalid
              )
            )
          case Right(SlashingOperatorKeyResolution.HistoricalStateUnavailable(reason)) =>
            Left(SlashingUnverifiableReason.HistoricalKeyStateUnavailable(context, reason))
          case Right(resolved: SlashingOperatorKeyResolution.Resolved) =>
            if (resolved.offencePeriod != child.epoch)
              Left(SlashingUnverifiableReason.ResolvedPeriodMismatch(child.epoch, resolved.offencePeriod))
            else ResolvedSlashingOperatorKeys.validate(signer, resolved).map(_ => resolved)
        }
      }

      // Short-circuit chain: cheap pure checks (1, 2) → step-3 (one hasher call per child) → step-4 (in-memory list scan) → step-5 (two
      // Ed25519 verifies — recovers VK once, used for both) → step-6 (two KES verifies — registry lookup once, Merkle-path verify per
      // child). Each step pulls the per-child sig from step-4's lookup result when needed.
      val asyncUnit: F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        Async[F].pure(Right(()))
      def lift(e: Either[ShardCheckpointEquivocationRejection, Unit]): F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        Async[F].pure(e)

      def chain(
        checks: List[F[Either[ShardCheckpointEquivocationRejection, Unit]]]
      ): F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        checks.foldLeft(asyncUnit) { (acc, next) =>
          acc.flatMap {
            case Left(r)  => Async[F].pure(Left(r))
            case Right(_) => next
          }
        }

      // The crypto-verify steps need (a) the per-child preimage-hash bytes and (b) the per-child signer sig. We compute them once after
      // the cheap checks pass and thread them into the verify closures. If steps 1-4 fail the closures are never invoked.
      type ValidationResult =
        SlashingValidationResult[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]

      def pureValidationResult(result: ValidationResult): F[ValidationResult] =
        Async[F].pure(result)

      val pipeline: F[ValidationResult] =
        chain(List(lift(step1), lift(step2), lift(step2b), step3Check)).flatMap {
          (cheapResult: Either[ShardCheckpointEquivocationRejection, Unit]) =>
            cheapResult match {
              case Left(r) =>
                pureValidationResult(SlashingValidationResult.Invalid(r))
              case Right(_) =>
                lookupSignerSigs() match {
                  case Left(r) =>
                    pureValidationResult(SlashingValidationResult.Invalid(r))
                  case Right((sigA, sigB)) =>
                    for {
                      hashA <- Hasher[F].hash(childA.signingPreimage)
                      hashB <- Hasher[F].hash(childB.signingPreimage)
                      msgBytesA = hashA.getBytes
                      msgBytesB = hashB.getBytes
                      edResult <- chain(
                        List(
                          verifyEd25519(sigA, msgBytesA, ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildA),
                          verifyEd25519(sigB, msgBytesB, ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildB)
                        )
                      )
                      result <- edResult match {
                        case Left(reason) => pureValidationResult(SlashingValidationResult.Invalid(reason))
                        case Right(()) =>
                          (resolve(childA), resolve(childB)).tupled.flatMap {
                            case (Left(reason), _) => pureValidationResult(SlashingValidationResult.Unverifiable(reason))
                            case (_, Left(reason)) => pureValidationResult(SlashingValidationResult.Unverifiable(reason))
                            case (Right(resolvedA), Right(resolvedB)) =>
                              if (!java.security.MessageDigest.isEqual(resolvedA.vrfEta, resolvedB.vrfEta))
                                pureValidationResult(
                                  SlashingValidationResult.Unverifiable(SlashingUnverifiableReason.ResolvedRandomnessMismatch)
                                )
                              else if (!ResolvedSlashingOperatorKeys.sameAtomicPair(resolvedA, resolvedB))
                                pureValidationResult(
                                  SlashingValidationResult.Unverifiable(SlashingUnverifiableReason.ResolvedAtomicPairMismatch)
                                )
                              else
                                (
                                  ResolvedSlashingOperatorKeys.expectedKesStep(resolvedA),
                                  ResolvedSlashingOperatorKeys.expectedKesStep(resolvedB)
                                ) match {
                                  case (Left(reason), _) => pureValidationResult(SlashingValidationResult.Unverifiable(reason))
                                  case (_, Left(reason)) => pureValidationResult(SlashingValidationResult.Unverifiable(reason))
                                  case (Right(expectedStepA), Right(expectedStepB)) =>
                                    chain(
                                      List(
                                        verifyKes(
                                          sigA,
                                          msgBytesA,
                                          resolvedA,
                                          expectedStepA,
                                          ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA
                                        ),
                                        verifyKes(
                                          sigB,
                                          msgBytesB,
                                          resolvedB,
                                          expectedStepB,
                                          ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildB
                                        ),
                                        verifyVrf(
                                          childA,
                                          sigA,
                                          resolvedA,
                                          ShardCheckpointEquivocationRejection.InvalidVrfProof.OnChildA
                                        ),
                                        verifyVrf(
                                          childB,
                                          sigB,
                                          resolvedB,
                                          ShardCheckpointEquivocationRejection.InvalidVrfProof.OnChildB
                                        )
                                      )
                                    ).map[ValidationResult] {
                                      case Left(reason) => SlashingValidationResult.Invalid(reason)
                                      case Right(()) =>
                                        SlashingValidationResult.Unverifiable(
                                          SlashingUnverifiableReason.CheckpointSignerExclusivityNotSpecified
                                        )
                                    }
                                }
                          }
                      }
                    } yield result
                }
            }
        }

      pipeline
    }
  }
}
