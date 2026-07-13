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
  * honest node.''' Current exceptional adjudication makes every GL0 node independently re-run the disputed exact inputs/base and decides
  * UPHELD iff the checkpoint result differs from that reproduction. Never trust the challenger's claimed roots. Ordinary target adoption
  * does not universally replay: execution signers replay before signing, positive watchtower coverage is required pre-inclusion, and other
  * GL0 nodes verify the certificate/coverage/base/namespace/diff/root.
  *
  * '''The re-derivation primitive (`reDerivePerMgRoot`).''' Injected as the SAME `(metagraphAddress, includedChain, gl0AnchorOrdinal,
  * executionBaseOrdinal) => F[Hash]` closure the
  * [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager]] uses for every
  * checkpoint replay (production: `ShardCheckpointWiring.reExecDerivationAtPinnedBase`, the PIN-1 component-addressable
  * `currencySnapshotMgRoot` encoding, seeded from the reader `ShardCheckpointWiring.pinnedPriorReaderAt` resolves AT the disputed
  * checkpoint's own `executionBaseOrdinal` — Track-1 execution-base-pin, FINDING-B1). Two facts make this deterministic cluster-wide:
  *   1. The closure reads NO live snapshot storage for the derivation itself (`noGlobalSnapshotLookup`), and the `S(N)` prior is read at
  *      the wire-carried, committee-signed `executionBaseOrdinal` — NEVER this node's live base (a validator whose tip ran ahead of the
  *      checkpoint's base would otherwise recompute a different root and false-uphold against an honest committee).
  *   1. Live code gates disputes to a `depth-k1` window and expects the pinned base `S(N)` to resolve identically. This is a transitional
  *      resource policy, not an irreversibility claim: target Phase 2 remains density-reorgable and exact `(ordinal,hash,root)` identity is
  *      required. The caller owns the current window gate (same shape as [[SlashableEvidenceValidator]]'s `currentEpoch`/`eventEpoch`
  *      contract). A node that cannot RESOLVE the exact pinned base (retention miss / not reached) yields the `Hash.empty` sentinel and the
  *      verdict FAILS CLOSED ([[io.constellationnetwork.schema.slashing.InvalidStateProofRejection.CannotRederive]]) — an unverifiable
  *      dispute never slashes.
  *
  * '''Why re-derive from the checkpoint's OWN signed bytes, not the challenger's claim.''' The committee SIGNED `disputedCheckpoint` (its
  * `committeeSignatures` cover the `ShardCheckpointSigPreimage`, which includes `derivedStateDelta.includedSnapshots` and
  * `perMetagraphMptRoots`). So the inputs to the honest re-derivation (`includedSnapshots(mg)`, `gl0AnchorOrdinal`) AND the committee's
  * attested root (`perMetagraphMptRoots(mg)`) are both read off the cryptographically-bound envelope — the challenger cannot move them. The
  * [[io.constellationnetwork.schema.sharding.FraudProofEnvelope.challengerDerivation]] / `claimedDerivation` fields are HINTS only; this
  * validator never reads them for the verdict. A forged fraud proof carrying lies about the roots simply fails the UPHELD check (the honest
  * re-derivation reproduces the attested root) ⇒ [[InvalidStateProofRejection.DisputeNotUpheld]] ⇒ no slash. '''An honest committee can
  * never be slashed.'''
  *
  * '''Safety bar.''' Every check is cryptographically verifiable or a pure recomputation: header equality, the canonical checkpoint-hash
  * binding, the challenger Ed25519 signature, the double-slash MPT guard, and the load-bearing honest-re-derivation comparison. No
  * behavioral heuristics. In particular, the disputed checkpoint's execution certificate is authenticated before its claimed signer IDs can
  * become slash targets. See `feedback_slashing_safety_bar`.
  */
trait InvalidStateProofValidator[F[_]] {

  /** Validate an [[InvalidStateProofEvidence]] under the node's finalized chain state.
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
}

object InvalidStateProofValidator {

  /** Construct the validator.
    *
    * @param reDerivePerMgRoot
    *   the canonical per-MG root re-derivation — currently the SAME closure the shard acceptance manager's transitional universal replay
    *   uses (`ShardCheckpointWiring.reExecDerivationAtPinnedBase` seeded from the Phase-2 base reader), so the recomputed root is in the
    *   exact `perMetagraphMptRoots` (PIN-1) encoding and byte-comparable against the committee-attested value. Production wiring passes the
    *   identical instance constructed in `SharedServices`/`GlobalSnapshotConsensus`.
    * @param slashedReader
    *   the double-slash MPT guard, keyed on `(shardId, disputedCheckpointHash)`. `InvalidStateProofSlashedReader.neverSlashed` for tests /
    *   pre-MPT-partition wiring.
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    // Track-1 execution-base-pin: the 4th arg is the disputed checkpoint's `executionBaseOrdinal`, so the honest re-derivation reads S(N) at the
    // SAME pinned base the committee executed over (call site passes `cp.executionBaseOrdinal`) — a watchtower that read its own live base would
    // recompute a different root and false-slash an honest checkpoint whose base lags the watchtower's.
    reDerivePerMgRoot: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => F[Hash],
    slashedReader: InvalidStateProofSlashedReader[F],
    verifyExecutionCertificate: ShardCheckpoint => F[Either[String, Unit]]
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
        slashedReader.wasSlashed(evidence.shardId, fp.disputedCheckpointHash).map {
          case true  => Left(InvalidStateProofRejection.AlreadySlashed(evidence.shardId, fp.disputedCheckpointHash))
          case false => Right(())
        }

      // Step 8 — THE VERDICT (load-bearing). Re-derive the honest per-MG root from the checkpoint's OWN signed binaries at its OWN
      // gl0AnchorOrdinal over its OWN pinned executionBaseOrdinal, using the SAME closure the checkpoint replay uses (PIN-1 encoding,
      // execution-base-pinned reader). Compare against the committee-attested root read off the signed envelope. UPHELD iff the re-derivation
      // AFFIRMATIVELY differs. Never trusts the challenger's carried roots.
      def step8(binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]]): F[Either[InvalidStateProofRejection, Unit]] = {
        val attested: Option[Hash] = cp.derivedStateDelta.perMetagraphMptRoots.get(mg)
        reDerivePerMgRoot(mg, binaries, cp.gl0AnchorOrdinal, cp.executionBaseOrdinal).map { honest =>
          // FAIL-CLOSED (Track-1 execution-base-pin, FINDING-B1): `Hash.empty` is the wiring's "cannot re-derive" sentinel
          // (`reExecDerivationAtPinnedBase` returned None — the pinned executionBaseOrdinal is unresolvable below this node's retention / not
          // reached, or the derivation OMITted). An unverifiable dispute is NEVER upheld: "this node can't check" is not evidence of
          // committee deviation, and upholding here would 100%-slash an honest committee on a local retention miss. Checked FIRST so the
          // sentinel can neither "differ" from an attested root nor satisfy the None-attested branch below.
          if (honest === Hash.empty)
            Left(InvalidStateProofRejection.CannotRederive(mg))
          else
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
            case Right(binaries) =>
              List(step4, step5, step6, step7, step8(binaries))
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
