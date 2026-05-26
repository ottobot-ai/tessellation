package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardCheckpoint}
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Slice 16 — accept-time validator for a [[ShardCheckpointEquivocationEvidence]].
  *
  * Mirrors [[SlashableEvidenceValidator]] (slice S4a) one layer up at the shard-checkpoint scope: same "cryptographically verifiable proof
  * of one signer producing two contradictory artefacts" algebra, applied to [[ShardCheckpoint]] envelopes per
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.1.
  *
  * '''Safety bar.''' Every check is cryptographically verifiable from the evidence + chain state (`KesRegistry`). No behavioral heuristics,
  * no consensus-state inferences. All honest nodes computing the validator over the same inputs produce byte-identical accept/reject
  * results. See `feedback_slashing_safety_bar`.
  *
  * '''Six steps:'''
  *   1. Parent-hash match (header agrees with both children) — pure field equality.
  *   1. Shard-id match (header agrees with both children) — pure field equality.
  *   1. Distinct canonical child hashes (via `Hasher[F].hash(child.signingPreimage)`) — equal ⇒ duplicate retransmission, not equivocation.
  *   1. Signer present in both children's `committeeSignatures` lists — extracts the offender's per-child signature contribution.
  *   1. Ed25519 signature on both children verifies under the signer's recovered long-term VK over the respective preimage-hash bytes.
  *   1. KES product signature on both children verifies under the signer's `KesRegistry` master VK at the wire-carried `kesTreeStep`.
  *
  * '''Ordering rationale.''' Cheap pure equality checks (1–4) short-circuit before the expensive crypto verifies (5–6). Same cache-hot
  * ordering as [[SlashableEvidenceValidator]]: pure header equality + map lookup → recovered-public-key Ed25519 verify → KES Merkle-path
  * verify.
  *
  * '''Determinism.''' The validator is pure with respect to: the evidence bytes (`shardId`, `parentCheckpointHash`, both checkpoint
  * envelopes, `equivocatingSigner`) and the `KesRegistry` contents. The implicit `Hasher[F]` is the project's canonical-JSON SHA-256
  * surface — byte-identical across all JVMs. No clock, no env reads, no I/O outside the `KesRegistry` lookup.
  *
  * '''What this slice doesn't do.''' Following the [[ShardCheckpointEquivocationEvidence]] scaladoc — bounty/submitter logic lives on the
  * wrapping L0 tx type (handled by the GSAM accept-path agent, mirroring `SLASHING-DESIGN.md` §5). Already-slashed deduplication +
  * evidence-window checks live downstream too, keyed by `(equivocatingSigner, shardId, parentCheckpointHash)` — same triple shape as
  * [[io.constellationnetwork.schema.slashing.SlashingRejection.AlreadySlashed]] just with `shardId` substituted for `metagraphAddress`.
  * Slice 16 delivers the cryptographic-proof half; ledger effects (stake reduction, eviction, bounty, burn) compose downstream per
  * [[https://github.com/Constellation-Labs/tessellation-nakamoto/blob/main/docs/nakamoto/SLASHING-DESIGN.md SLASHING-DESIGN.md]] §5.
  */
trait ShardCheckpointEquivocationValidator[F[_]] {

  /** Validate an equivocation evidence. Returns `Right(evidence)` if all six checks pass, `Left(rejection)` with the first failing step
    * otherwise.
    */
  def validate(
    evidence: ShardCheckpointEquivocationEvidence
  ): F[Either[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]]
}

object ShardCheckpointEquivocationValidator {

  /** Construct the validator. Dependencies are intentionally minimal: only the [[KesRegistry]] is required for the step-6 KES verify; the
    * Ed25519 step-5 verify is self-contained (recovers the signer's long-term VK from `equivocatingSigner.value.toPublicKey`).
    *
    * @param kesRegistry
    *   used by step 6 to look up the signer's KES master VK and verify both KES product signatures under it. Same fail-closed semantics as
    *   [[SlashableEvidenceValidator]]: no registry entry ⇒ reject — we can't establish the cryptographic equivocation evidence without the
    *   master VK.
    */
  def make[F[_]: Async: Hasher: SecurityProvider](
    kesRegistry: KesRegistry[F]
  ): ShardCheckpointEquivocationValidator[F] = new ShardCheckpointEquivocationValidator[F] {

    def validate(
      evidence: ShardCheckpointEquivocationEvidence
    ): F[Either[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]] = {

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

      // Step 6 — KES product signature verification. Mirrors SlashableEvidenceValidator's KES verify step exactly: look up the master VK
      // in the KesRegistry, decode the wire-carried sig bytes, copy the VK to the wire-carried tree-internal step, and run the pure
      // KesProduct.verify. Fail-closed on any of (no registry entry / empty sig / decode failure / negative step / cryptographic mismatch).
      def verifyKes(
        sig: CommitteeMemberSignature,
        preimageHashBytes: Array[Byte],
        onFail: ShardCheckpointEquivocationRejection
      ): F[Either[ShardCheckpointEquivocationRejection, Unit]] =
        kesRegistry.getKesVk(signer).map {
          case None => Left(onFail)
          case Some(entry) =>
            val sigBytes = sig.kesProductSig.toBytes
            if (sigBytes.isEmpty) Left(onFail)
            else
              OperationalKeyMaker.decodeSignature(sigBytes) match {
                case Left(_) => Left(onFail)
                case Right(kSig) =>
                  val kesStep = sig.kesTreeStep
                  if (kesStep < 0) Left(onFail)
                  else {
                    val vkAtStep = entry.vk.copy(step = kesStep)
                    if (OperationalKeyMaker.verify(kSig, preimageHashBytes, vkAtStep)) Right(()) else Left(onFail)
                  }
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
      val pipeline: F[Either[ShardCheckpointEquivocationRejection, ShardCheckpointEquivocationEvidence]] =
        chain(List(lift(step1), lift(step2), step3Check)).flatMap { (cheapResult: Either[ShardCheckpointEquivocationRejection, Unit]) =>
          cheapResult match {
            case Left(r) =>
              Async[F].pure(Left(r): Either[ShardCheckpointEquivocationRejection, Unit])
            case Right(_) =>
              lookupSignerSigs() match {
                case Left(r) =>
                  Async[F].pure(Left(r): Either[ShardCheckpointEquivocationRejection, Unit])
                case Right((sigA, sigB)) =>
                  for {
                    hashA <- Hasher[F].hash(childA.signingPreimage)
                    hashB <- Hasher[F].hash(childB.signingPreimage)
                    msgBytesA = hashA.getBytes
                    msgBytesB = hashB.getBytes
                    result <- chain(
                      List(
                        verifyEd25519(sigA, msgBytesA, ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildA),
                        verifyEd25519(sigB, msgBytesB, ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildB),
                        verifyKes(sigA, msgBytesA, ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA),
                        verifyKes(sigB, msgBytesB, ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildB)
                      )
                    )
                  } yield result
              }
          }
        }.map(_.map(_ => evidence))

      pipeline
    }
  }
}
