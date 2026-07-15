package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Parallel
import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptBranchImage, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.nakamoto.follow.{FollowVerifyCore, GlobalFollowProof}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaRangeProver
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaRangeProof

import scodec.bits.ByteVector

/** gl0-side prover for the gl1 follow path (Axis 2, Slice 1 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * For a `(branch, ordinal)` view of the global MPT, produces a [[GlobalFollowProof]] that lets a follower verify exactly the slice of
  * global state it consumes (balances + last-ref maps) without re-executing the snapshot. Mirrors [[HistoricalMptProofService]]'s access
  * pattern: one immutable `overlay.captureBranchImage(branch)` is projected through [[GlobalStateKey.consensusRootEntries]], then that one
  * projected image supplies both the trie and per-branch value bytes (the range proof's leaves carry only `dataDigest`, so the actual value
  * bytes travel alongside in the proof's `values` map — same split as
  * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProof.value]]). Root and values therefore cannot come from
  * two different overlay reads, and root-invisible field 32 cannot change the proof root. The caller-supplied ordinal remains unbound until
  * ROOT-005B provides an authenticated exact-parent generation; this service must remain unwired until then.
  *
  * '''Per-field full range.''' Each consumed field is proven over its '''entire''' key-range via the field's hypergraph prefix
  * ([[GlobalStateKey.hypergraphFieldPrefix]]). The range `[prefix + 0…, prefix + f…]` brackets every possible `userNamespace` encoding for
  * that field (the suffix is `01` + a 64-hex address hash, which sorts strictly between the all-`0` and all-`f` bounds), so the range
  * proof's inclusion + exclusion boundaries cover the whole field — completeness and absence are then cryptographic on the verifier side.
  * The delta-scoped optimization (re-prove only changed sub-ranges) is deferred (design "Open / deferred").
  *
  * '''Additive, no wiring.''' Slice 1 builds this service + the shared verify core + tests only. Nothing here is wired into Main / routes /
  * consensus / gl1 — that's Slice 2+.
  */
trait GlobalFollowProofService[F[_]] {

  /** Build a [[GlobalFollowProof]] over the consumed fields ([[FollowVerifyCore.consumedFields]]) against the MPT at `(branch, ordinal)`.
    * `committedRoot` in the result is that MPT's root — the value a follower must match against its trusted `attestedRoot`.
    */
  def proveConsumedFields(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[GlobalFollowProofService.ProofError, GlobalFollowProof]]
}

object GlobalFollowProofService {

  sealed trait ProofError extends Product with Serializable

  /** `overlay.captureBranchImage` failed to materialize the trie at `(branch, ordinal)` (e.g. empty base, build error). Mirrors
    * [[HistoricalMptProofService.TrieBuildFailed]].
    */
  final case class TrieBuildFailed(message: String) extends ProofError

  /** A field's range prover returned an error (e.g. malformed bounds). Carries the field and the underlying prover error's message — the
    * prover's [[io.constellationnetwork.security.mpt.prover.MerklePatriciaProofError]] is `extends Throwable` with no Eq/codec, so we keep
    * the field structured and the cause as its message string. Prover errors are not part of the follow protocol's wire surface.
    */
  final case class RangeProofGenerationFailed(field: GlobalStateFieldId, message: String) extends ProofError

  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](overlay: MptOverlay[F, GlobalStateKey]): GlobalFollowProofService[F] =
    new GlobalFollowProofService[F] {

      def proveConsumedFields(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[ProofError, GlobalFollowProof]] =
        overlay.captureBranchImage(branch).flatMap {
          case Left(err) =>
            (TrieBuildFailed(err.toString): ProofError).asLeft[GlobalFollowProof].pure[F]

          case Right(rawImage) =>
            val consensusBytes = GlobalStateKey.consensusRootEntries(rawImage.toByteMap)
            MptBranchImage.fromBytes[F](consensusBytes).flatMap {
              case Left(error) =>
                (TrieBuildFailed(error.toString): ProofError).asLeft[GlobalFollowProof].pure[F]
              case Right(image) =>
                val prover = MerklePatriciaRangeProver.make[F](image.trie)
                val committedRoot = image.trie.rootHash.value

                FollowVerifyCore.consumedFields
                  .traverse(field => EitherT(proveField(prover, image.entries, field)).tupleLeft(field))
                  .map { perField =>
                    val fields = SortedMap.from(perField.map { case (field, (rangeProof, _)) => field -> rangeProof })
                    val values = SortedMap.from(perField.map { case (field, (_, fieldValues)) => field -> fieldValues })
                    GlobalFollowProof(ordinal, committedRoot, fields, values)
                  }
                  .value
            }
        }

      /** Prove one field's full range and collect its `(keyHex → valueHex)` pairs from the branch entries. */
      private def proveField(
        prover: MerklePatriciaRangeProver[F],
        branchEntries: SortedMap[Hex, ByteVector],
        field: GlobalStateFieldId
      ): F[Either[ProofError, (MerklePatriciaRangeProof, SortedMap[Hex, Hex])]] =
        (for {
          prefix <- EitherT.liftF[F, ProofError, Hex](GlobalStateKey.hypergraphFieldPrefix[F](field))
          bounds <- EitherT.liftF[F, ProofError, (Hex, Hex)](FollowVerifyCore.consumedFieldBounds[F](field))
          rangeProof <- EitherT(prover.attestRange(bounds._1, bounds._2))
            .leftMap(e => RangeProofGenerationFailed(field, e.getMessage): ProofError)
          // Values for this field = every branch entry whose hex key carries the field prefix, carried as
          // hex. These are the exact bytes that produced each leaf's `dataDigest` (producer `createFromBytes`
          // → `Hasher.hashBytes`), so the verifier's binding `hashBytes(valueHex) == leaf.dataDigest` holds.
          fieldValues = SortedMap.from(
            branchEntries.collect {
              case (hex, bytes) if hex.value.startsWith(prefix.value) => hex -> Hex.fromBytes(bytes.toArray)
            }
          )
        } yield (rangeProof, fieldValues)).value

    }
}
