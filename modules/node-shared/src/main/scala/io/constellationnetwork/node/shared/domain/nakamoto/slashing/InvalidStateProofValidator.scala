package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.sharding.{FraudProofSigPreimage, ShardCheckpoint}
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

sealed trait InvalidStateProofBatchReplay extends Product with Serializable

object InvalidStateProofBatchReplay {
  final case class Reproduced(perMetagraphRoots: SortedMap[Address, Hash]) extends InvalidStateProofBatchReplay
  case object ProvenInvalidTransition extends InvalidStateProofBatchReplay
  case object Unavailable extends InvalidStateProofBatchReplay
}

/** WATCHTOWER invalid-state-proof verdict function — the accept-time validator for an [[InvalidStateProofEvidence]] (the
  * `InvalidStateProof` slashing tier). Companion to [[SlashableEvidenceValidator]] / [[ShardCheckpointEquivocationValidator]].
  *
  * '''The single non-negotiable: the verdict must be a byte-identical function of portable evidence and a uniquely identified canonical
  * base on every honest node.''' The target exceptional path makes every GL0 replay the disputed signed inputs at the carried exact base
  * and uphold only when the checkpoint result differs. Current production does not yet satisfy that premise: replay-history availability
  * can differ, and unavailable maps to `CannotRederive`/no-slash. Never trust the challenger's claimed roots. Ordinary target adoption does
  * not universally replay: execution signers replay before signing, positive watchtower coverage is required pre-inclusion, and other GL0
  * nodes verify the certificate/coverage/base/namespace/diff/root.
  *
  * '''The re-derivation primitive (`replayCheckpoint`).''' Injected as the same `(includedChains, gl0AnchorOrdinal, executionBase) \=>
  * F[InvalidStateProofBatchReplay]` closure the
  * [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager]] uses for every
  * complete checkpoint replay (production: `ShardCheckpointWiring.reExecCheckpointAtPinnedBase`, the PIN-1 component-addressable
  * `currencySnapshotMgRoot` encoding, seeded from the reader `ShardCheckpointWiring.pinnedPriorReaderAt` resolves at the disputed
  * checkpoint's exact `executionBase` — Track-1 execution-base-pin, FINDING-B1). Two facts make this deterministic cluster-wide:
  *   1. The `S(N)` prior is read at the wire-carried, committee-signed exact execution base — NEVER this node's live base (a validator
  *      whose tip ran ahead of the checkpoint's base would otherwise recompute a different root and false-uphold against an honest
  *      committee). Any referenced historical GL0 snapshot that cannot be resolved marks the complete replay `Unavailable`.
  *   1. Live code gates disputes to a `depth-k1` window and expects the pinned base `S(N)` to resolve identically. This is a transitional
  *      resource policy, not an irreversibility claim: target Phase 2 remains density-reorgable and exact `(ordinal,hash,root)` identity is
  *      required. The caller owns the current window gate (same shape as [[SlashableEvidenceValidator]]'s `currentEpoch`/`eventEpoch`
  *      contract). A node that cannot RESOLVE the exact pinned base (retention miss / not reached) yields `Unavailable` and the verdict
  *      FAILS CLOSED ([[io.constellationnetwork.schema.slashing.InvalidStateProofRejection.CannotRederive]]) — an unverifiable dispute
  *      never slashes.
  *
  * '''Why re-derive from the checkpoint's OWN signed bytes, not the challenger's claim.''' The committee SIGNED `disputedCheckpoint` (its
  * `committeeSignatures` cover the `ShardCheckpointSigPreimage`, which includes `derivedStateDelta.includedSnapshots` and
  * `perMetagraphMptRoots`). So the inputs to the honest re-derivation (`includedSnapshots(mg)`, `gl0AnchorOrdinal`) AND the committee's
  * attested root (`perMetagraphMptRoots(mg)`) are both read off the cryptographically-bound envelope — the challenger cannot move them. The
  * [[io.constellationnetwork.schema.sharding.FraudProofEnvelope.challengerDerivation]] / `claimedDerivation` fields are HINTS only; this
  * validator never reads them for the verdict. A forged fraud proof carrying lies about the roots simply fails the UPHELD check (the honest
  * re-derivation reproduces the attested root) ⇒ [[InvalidStateProofRejection.DisputeNotUpheld]] ⇒ no slash. Under the exact signed state
  * claim and input identity supplied to this validator, a reproduced honest root is therefore not slashable. Authenticating that claim as
  * current Phase-2 authority with its historical eligibility context remains a separate consensus prerequisite.
  *
  * '''Safety bar.''' Every check is cryptographically verifiable or a pure recomputation: header equality, the canonical checkpoint-hash
  * binding, the challenger Ed25519 signature, the double-slash MPT guard, and the load-bearing honest-re-derivation comparison. No
  * behavioral heuristics. In particular, the disputed checkpoint's execution certificate is authenticated before its claimed signer IDs can
  * become slash targets. See `feedback_slashing_safety_bar`.
  */
trait InvalidStateProofValidator[F[_]] {

  /** Validate an [[InvalidStateProofEvidence]] against the reader installed at construction. This entry point is for non-authoritative
    * staging and isolated tests. Consensus acceptance must call [[validateAgainst]] with its exact proposal-parent reader.
    *
    * @param evidence
    *   the candidate invalid-state-proof.
    * @return
    *   `Right(evidence)` iff the dispute is UPHELD (the committee signed a wrong derivation, every signer is a slash target) — the GSAM
    *   accept path then applies the 100% ledger effect. `Left(rejection)` names the exact step that failed;
    *   [[InvalidStateProofRejection.DisputeNotUpheld]] is the "honest committee" floor (re-derivation reproduced the attested root ⇒ no
    *   slash).
    */
  def validate(evidence: InvalidStateProofEvidence): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]]

  /** Validate against an explicitly supplied slash-ledger view. Consensus acceptance must pass the immutable reader bound to the exact
    * proposal parent; this prevents a construction-time finalized reader (appropriate for daemon staging) from deciding an authoritative
    * branch transition.
    */
  def validateAgainst(
    evidence: InvalidStateProofEvidence,
    slashedReader: InvalidStateProofSlashedReader[F]
  ): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]]
}

object InvalidStateProofValidator {

  /** Construct the validator.
    *
    * @param replayCheckpoint
    *   the canonical complete-checkpoint replay — the SAME batch transition used by checkpoint validation, seeded from the Phase-2 base
    *   reader. The complete ordered multi-metagraph batch is replayed exactly once so shared fee-payer and other cross-metagraph
    *   dependencies cannot be erased by selecting one metagraph for the evidence. `Reproduced` roots use the exact `perMetagraphMptRoots`
    *   (PIN-1) encoding. `ProvenInvalidTransition` requires a typed deterministic rejection from the transition function; a generic partial
    *   replay is `Unavailable`, because legacy currency replay can also return a prefix after swallowing a local dependency failure.
    * @param slashedReader
    *   the default double-slash MPT guard, keyed on `(shardId, disputedCheckpointHash)`. Daemon staging may bind finalized state here;
    *   authoritative acceptance supplies its exact rooted proposal-parent view to [[InvalidStateProofValidator.validateAgainst]].
    *   `InvalidStateProofSlashedReader.neverSlashed` is only for isolated tests.
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    // Track-1 execution-base-pin: the 3rd arg is the disputed checkpoint's exact `executionBase`, so replay reads S(N) at the same pinned
    // base the committee executed over. Replaying the complete SortedMap once preserves the transition's canonical cross-MG order.
    replayCheckpoint: (
      SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
      SnapshotOrdinal,
      GlobalSnapshotStateRef
    ) => F[InvalidStateProofBatchReplay],
    slashedReader: InvalidStateProofSlashedReader[F],
    verifyExecutionCertificate: ShardCheckpoint => F[Either[String, Unit]]
  ): InvalidStateProofValidator[F] = new InvalidStateProofValidator[F] {

    def validate(evidence: InvalidStateProofEvidence): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]] =
      validateAgainst(evidence, slashedReader)

    def validateAgainst(
      evidence: InvalidStateProofEvidence,
      exactSlashedReader: InvalidStateProofSlashedReader[F]
    ): F[Either[InvalidStateProofRejection, InvalidStateProofEvidence]] = {
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

      // Step 6 — authenticate the exact disputed checkpoint before trusting any claimed slash target. The injected verifier runs
      // deterministic committee membership, distinct/quorum, Ed25519, registered KES, and registered VRF possession checks. Producer duty
      // is deliberately excluded: it is enforced on ordinary checkpoint intake, while a receiver-local retained parent must not alter the
      // culpability of execution signers who authenticated a wrong root. Missing historical key/eta/roster state fails closed here; a
      // receiver can defer, but it cannot slash an unauthenticated PeerId list.
      def step6: F[Either[InvalidStateProofRejection, Unit]] =
        verifyExecutionCertificate(cp)
          .map(_.leftMap[InvalidStateProofRejection](InvalidStateProofRejection.InvalidCheckpointCertificate(_)))
          .handleError(error =>
            Left[InvalidStateProofRejection, Unit](
              InvalidStateProofRejection.InvalidCheckpointCertificate(error.getClass.getSimpleName)
            )
          )

      // Step 7 — double-slash guard: (shardId, disputedCheckpointHash) not already slashed.
      def step7: F[Either[InvalidStateProofRejection, Unit]] =
        exactSlashedReader.wasSlashed(evidence.shardId, fp.disputedCheckpointHash).map {
          case true  => Left(InvalidStateProofRejection.AlreadySlashed(evidence.shardId, fp.disputedCheckpointHash))
          case false => Right(())
        }

      // Step 8 — THE VERDICT (load-bearing). Replay the checkpoint's COMPLETE ordered batch once at its signed base. Evidence selects the MG
      // whose attested root is compared, but it never narrows execution: shared fee-payer/dependency state is part of the signed transition.
      // A typed, proven deterministic batch rejection proves every execution signer vouched for an unexecutable checkpoint. Generic partial
      // replay and local inability to replay are Unavailable and never slash evidence. Never trusts the challenger's carried roots.
      def step8: F[Either[InvalidStateProofRejection, Unit]] = {
        val attested: Option[Hash] = cp.derivedStateDelta.perMetagraphMptRoots.get(mg)
        replayCheckpoint(cp.derivedStateDelta.includedSnapshots, cp.gl0AnchorOrdinal, cp.executionBase).map {
          case InvalidStateProofBatchReplay.Unavailable =>
            Left(InvalidStateProofRejection.CannotRederive(mg))
          case InvalidStateProofBatchReplay.ProvenInvalidTransition =>
            Right(())
          case InvalidStateProofBatchReplay.Reproduced(roots)
              if roots.keySet =!= cp.derivedStateDelta.includedSnapshots.keySet || roots.values.exists(_ === Hash.empty) =>
            Left(InvalidStateProofRejection.CannotRederive(mg))
          case InvalidStateProofBatchReplay.Reproduced(roots) =>
            roots.get(mg) match {
              case None => Left(InvalidStateProofRejection.CannotRederive(mg))
              case Some(honest) =>
                attested match {
                  case Some(attestedRoot) if attestedRoot === honest =>
                    Left(InvalidStateProofRejection.DisputeNotUpheld(honest, attestedRoot))
                  case Some(_) => Right(())
                  case None    => Right(())
                }
            }
        }
      }

      // Short-circuit chain: cheap pure checks first (1-3), hash/challenger crypto (4-5), checkpoint certificate authentication (6), the
      // I/O double-slash guard (7), then the replay verdict (8).
      val pureUnit: F[Either[InvalidStateProofRejection, Unit]] = Async[F].pure(Right(()))
      def lift(e: Either[InvalidStateProofRejection, Unit]): F[Either[InvalidStateProofRejection, Unit]] = Async[F].pure(e)

      (lift(step1), lift(step2)).tupled.flatMap {
        case (Left(r), _) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
        case (_, Left(r)) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
        case _            => pureUnit
      }.flatMap {
        case Left(r) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
        case Right(_) =>
          step3 match {
            case Left(r) => Async[F].pure(Left(r): Either[InvalidStateProofRejection, Unit])
            case Right(_) =>
              List(step4, step5, step6, step7, step8)
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
