package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.{FraudProofSigPreimage, ShardCheckpoint}
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

/** WATCHTOWER invalid-state-proof DETERMINISTIC verdict — the accept-time validator for an [[InvalidStateProofEvidence]] (the
  * `InvalidStateProof` 100% slashing tier). Companion to [[SlashableEvidenceValidator]] / [[ShardCheckpointEquivocationValidator]].
  *
  * '''The single non-negotiable: the verdict is a pure, byte-identical function of the evidence + the canonical re-derivation, on every
  * honest node.''' Part 2 of the watchtower spec — "EVERY gl0 node independently re-runs the same deterministic re-derivation from the
  * disputed checkpoint's `includedSnapshots` and decides UPHELD iff attested ≠ honest-re-derived. Never trust the challenger's claimed
  * roots — recompute." This validator implements exactly that.
  *
  * '''The re-derivation primitive (`reDerivePerMgRoot`).''' Injected as the SAME `(metagraphAddress, includedChain, gl0AnchorOrdinal) =>
  * F[Hash]` closure the [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager]]
  * uses on its sub-quorum re-exec path (production: `ShardCheckpointWiring.reExecDerivationWithDiff(...)._1`, the PIN-1
  * `Hasher.hash((incrementalRoot, infoRoot))` encoding seeded from this node's FINALIZED base `GlobalStateReader.fromMptStore`). Two facts
  * make this deterministic cluster-wide:
  *   1. The closure reads NO live snapshot storage for the derivation itself (`noGlobalSnapshotLookup`) — only the finalized MPT base for
  *      the `S(N)` prior.
  *   1. The dispute is gated to land WITHIN the challenge window (`depth-k1`); below depth-k1 the finalized base `S(N)` is cluster-uniform
  *      (consensus has finalized it), so every honest node's PIN-1 re-derivation reads the IDENTICAL prior and computes the IDENTICAL root.
  *      Above the window the economic effect is already irreversible, so a late dispute is rejected by the GSAM gate before it reaches this
  *      validator (the validator itself stays pure given its inputs; the window gate is the caller's contract — same shape as
  *      [[SlashableEvidenceValidator]]'s `currentEpoch`/`eventEpoch` caller contract).
  *
  * '''Why re-derive from the checkpoint's OWN signed bytes, not the challenger's claim.''' The committee SIGNED `disputedCheckpoint`
  * (its `committeeSignatures` cover the `ShardCheckpointSigPreimage`, which includes `derivedStateDelta.includedSnapshots` and
  * `perMetagraphMptRoots`). So the inputs to the honest re-derivation (`includedSnapshots(mg)`, `gl0AnchorOrdinal`) AND the committee's
  * attested root (`perMetagraphMptRoots(mg)`) are both read off the cryptographically-bound envelope — the challenger cannot move them. The
  * [[io.constellationnetwork.schema.sharding.FraudProofEnvelope.challengerDerivation]] / `claimedDerivation` fields are HINTS only; this
  * validator never reads them for the verdict. A forged fraud proof carrying lies about the roots simply fails the UPHELD check (the honest
  * re-derivation reproduces the attested root) ⇒ [[InvalidStateProofRejection.DisputeNotUpheld]] ⇒ no slash. '''An honest committee can
  * never be slashed.'''
  *
  * '''Safety bar.''' Every check is cryptographically verifiable or a pure recomputation: header equality, the canonical checkpoint-hash
  * binding, the challenger Ed25519 signature, the double-slash MPT guard, and the load-bearing honest-re-derivation comparison. No
  * behavioral heuristics. See `feedback_slashing_safety_bar`.
  */
trait InvalidStateProofValidator[F[_]] {

  /** Validate an [[InvalidStateProofEvidence]] under the node's finalized chain state.
    *
    * @param evidence
    *   the candidate invalid-state-proof.
    * @return
    *   `Right(evidence)` iff the dispute is UPHELD (the committee signed a wrong derivation, every signer is a slash target) — the GSAM
    *   accept path then applies the 100% ledger effect. `Left(rejection)` names the exact step that failed; [[InvalidStateProofRejection.DisputeNotUpheld]]
    *   is the "honest committee" floor (re-derivation reproduced the attested root ⇒ no slash).
    */
  def validate(evidence: InvalidStateProofEvidence): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]]
}

object InvalidStateProofValidator {

  /** Construct the validator.
    *
    * @param reDerivePerMgRoot
    *   the canonical per-MG root re-derivation — MUST be the SAME closure the shard acceptance manager's sub-quorum re-exec uses
    *   (`ShardCheckpointWiring.reExecDerivationWithDiff(...)._1` seeded from the FINALIZED base reader), so the recomputed root is in the
    *   exact `perMetagraphMptRoots` (PIN-1) encoding and byte-comparable against the committee-attested value. Production wiring passes the
    *   identical instance constructed in `SharedServices`/`GlobalSnapshotConsensus`.
    * @param slashedReader
    *   the double-slash MPT guard, keyed on `(shardId, disputedCheckpointHash)`. `InvalidStateProofSlashedReader.neverSlashed` for tests /
    *   pre-MPT-partition wiring.
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    reDerivePerMgRoot: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash],
    slashedReader: InvalidStateProofSlashedReader[F]
  ): InvalidStateProofValidator[F] = new InvalidStateProofValidator[F] {

    def validate(evidence: InvalidStateProofEvidence): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]] = {
      val cp: ShardCheckpoint = evidence.disputedCheckpoint
      val fp = evidence.fraudProof
      val mg = evidence.metagraphAddress

      // Step 1 — header equality: shardId agrees across evidence, checkpoint, fraud proof.
      lazy val step1: Either[InvalidStateProofRejection, Unit] =
        if (evidence.shardId === cp.shardId && evidence.shardId === fp.shardId) Right(())
        else Left(InvalidStateProofRejection.ShardMismatch(evidence.shardId, cp.shardId, fp.shardId))

      // Step 2 — metagraph agreement between the evidence and the fraud proof.
      lazy val step2: Either[InvalidStateProofRejection, Unit] =
        if (mg === fp.metagraphAddress) Right(())
        else Left(InvalidStateProofRejection.MetagraphMismatch(mg, fp.metagraphAddress))

      // Step 3 — the disputed MG must actually have a derivation in the checkpoint (otherwise nothing to prove wrong).
      lazy val includedForMg: Option[NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
        cp.derivedStateDelta.includedSnapshots.get(mg)
      lazy val step3: Either[InvalidStateProofRejection, NonEmptyList[Signed[StateChannelSnapshotBinary]]] =
        includedForMg.toRight(InvalidStateProofRejection.MetagraphNotInCheckpoint(mg))

      // Step 4 — bind the carried checkpoint to the fraud proof: Hasher(signingPreimage) === fraudProof.disputedCheckpointHash.
      // Reads the committee's own signed identity; the challenger cannot pair an honest checkpoint with a fraud proof for another.
      def step4: F[Either[InvalidStateProofRejection, Unit]] =
        Hasher[F].hash(cp.signingPreimage).map { computed =>
          if (computed === fp.disputedCheckpointHash) Right(())
          else Left(InvalidStateProofRejection.CheckpointHashMismatch(computed, fp.disputedCheckpointHash))
        }

      // Step 5 — challenger Ed25519 signature over the canonical FraudProofSigPreimage, recovered from submitterId. Replay-bound spam guard.
      def step5: F[Either[InvalidStateProofRejection, Unit]] = {
        val preimage: FraudProofSigPreimage = fp.signingPreimage
        val sigBytes = fp.challengerSignature.toBytes
        if (sigBytes.isEmpty) Async[F].pure(Left(InvalidStateProofRejection.InvalidChallengerSignature))
        else
          (for {
            digest <- Hasher[F].hash(preimage)
            pubKey <- fp.submitterId.value.toPublicKey[F]
            ok <- Signing.verifySignature[F](digest.getBytes, sigBytes)(pubKey)
          } yield if (ok) Right[InvalidStateProofRejection, Unit](()) else Left(InvalidStateProofRejection.InvalidChallengerSignature))
            .handleError(_ => Left[InvalidStateProofRejection, Unit](InvalidStateProofRejection.InvalidChallengerSignature))
      }

      // Step 6 — double-slash guard: (shardId, disputedCheckpointHash) not already slashed.
      def step6: F[Either[InvalidStateProofRejection, Unit]] =
        slashedReader.wasSlashed(evidence.shardId, fp.disputedCheckpointHash).map {
          case true  => Left(InvalidStateProofRejection.AlreadySlashed(evidence.shardId, fp.disputedCheckpointHash))
          case false => Right(())
        }

      // Step 7 — THE VERDICT (load-bearing). Re-derive the honest per-MG root from the checkpoint's OWN signed binaries at its OWN
      // gl0AnchorOrdinal, using the SAME closure the sub-quorum re-exec uses (PIN-1 encoding, finalized base). Compare against the
      // committee-attested root read off the signed envelope. UPHELD iff they differ. Never trusts the challenger's carried roots.
      def step7(binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]]): F[Either[InvalidStateProofRejection, Unit]] = {
        val attested: Option[Hash] = cp.derivedStateDelta.perMetagraphMptRoots.get(mg)
        reDerivePerMgRoot(mg, binaries, cp.gl0AnchorOrdinal).map { honest =>
          attested match {
            case Some(attestedRoot) if attestedRoot === honest =>
              // Honest re-derivation reproduced the attested root ⇒ committee did NOT deviate ⇒ NOT upheld. The "honest committee" floor.
              Left(InvalidStateProofRejection.DisputeNotUpheld(honest, attestedRoot))
            case Some(_) =>
              // Divergence — the committee signed a root no honest re-execution of its own signed binaries produces. UPHELD.
              Right(())
            case None =>
              // The committee carried NO root for an MG it included binaries for — a structurally invalid derivation. UPHELD (the honest
              // derivation produced a root; the committee attested none).
              Right(())
          }
        }
      }

      // Short-circuit chain: cheap pure checks first (1-3), then the hash binding + crypto (4-5), the I/O guard (6), the verdict (7).
      val pureUnit: F[Either[InvalidStateProofRejection, Unit]] = Async[F].pure(Right(()))
      def lift(e: Either[InvalidStateProofRejection, Unit]): F[Either[InvalidStateProofRejection, Unit]] = Async[F].pure(e)

      (lift(step1), lift(step2)).tupled
        .flatMap {
          case (Left(r), _) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
          case (_, Left(r)) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
          case _            => pureUnit
        }
        .flatMap {
          case Left(r) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
          case Right(_) =>
            step3 match {
              case Left(r) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
              case Right(binaries) =>
                List(step4, step5, step6, step7(binaries))
                  .foldLeft(pureUnit) { (acc, next) =>
                    acc.flatMap {
                      case Left(r)  => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
                      case Right(_) => next
                    }
                  }
            }
        }
        .map(_.map(_ => evidence))
    }
  }
}
