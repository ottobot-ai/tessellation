package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.EitherT
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.follow.{FollowVerifyCore, GlobalFollowProof}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaRangeProver
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaRangeProof

/** gl0-side prover for the gl1 follow path (Axis 2, Slice 1 — see `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * For a `(branch, ordinal)` view of the global MPT, produces a [[GlobalFollowProof]] that lets a follower verify exactly the slice of
  * global state it consumes (balances + last-ref maps) without re-executing the snapshot. Mirrors [[HistoricalMptProofService]]'s access
  * pattern: `overlay.buildRoot(branch, ordinal) → MerklePatriciaTrie → MerklePatriciaRangeProver`, and reads the per-branch value bytes via
  * `overlay.allEntriesAsBytes(branch)` (the range proof's leaves carry only `dataDigest`, so the actual value bytes travel alongside in the
  * proof's `values` map — same split as [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProof.value]]).
  *
  * '''Per-field full range.''' Each consumed field is proven over its '''entire''' key-range via the field's hypergraph prefix
  * ([[GlobalStateKey.hypergraphFieldPrefix]]). The range `[prefix + 0…, prefix + f…]` brackets every possible `userNamespace` encoding for
  * that field (the suffix is `01` + a 64-hex address hash, which sorts strictly between the all-`0` and all-`f` bounds), so the range proof's
  * inclusion + exclusion boundaries cover the whole field — completeness and absence are then cryptographic on the verifier side. The
  * delta-scoped optimization (re-prove only changed sub-ranges) is deferred (design "Open / deferred").
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

  /** `overlay.buildRoot` failed to materialize the trie at `(branch, ordinal)` (e.g. empty base, build error). Mirrors
    * [[HistoricalMptProofService.TrieBuildFailed]].
    */
  final case class TrieBuildFailed(message: String) extends ProofError

  /** A field's range prover returned an error (e.g. malformed bounds). Carries the field and the underlying prover error's message — the
    * prover's [[io.constellationnetwork.security.mpt.prover.MerklePatriciaProofError]] is `extends Throwable` with no Eq/codec, so we keep
    * the field structured and the cause as its message string. Prover errors are not part of the follow protocol's wire surface.
    */
  final case class RangeProofGenerationFailed(field: GlobalStateFieldId, message: String) extends ProofError

  def make[F[_]: Async: Hasher](
    store: MptStore[F, GlobalStateKey],
    overlay: MptOverlay[F, GlobalStateKey]
  ): GlobalFollowProofService[F] = new GlobalFollowProofService[F] {

    def proveConsumedFields(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[ProofError, GlobalFollowProof]] =
      // Same defensive lock as HistoricalMptProofService: `overlay.buildRoot` calls into the producer's
      // build which can race with concurrent `accept()` writes.
      store.withExclusiveLock {
        overlay.buildRoot(branch, ordinal).flatMap {
          case Left(err) =>
            (TrieBuildFailed(err.toString): ProofError).asLeft[GlobalFollowProof].pure[F]

          case Right(trie) =>
            val prover = MerklePatriciaRangeProver.make[F](trie)
            val committedRoot = trie.rootHash.value

            overlay.allEntriesAsBytes(branch).flatMap { branchEntries =>
              FollowVerifyCore.consumedFields
                .traverse(field => EitherT(proveField(prover, branchEntries, field)).tupleLeft(field))
                .map { perField =>
                  val fields = SortedMap.from(perField.map { case (field, (rangeProof, _)) => field -> rangeProof })
                  val values = SortedMap.from(perField.map { case (field, (_, fieldValues)) => field -> fieldValues })
                  GlobalFollowProof(ordinal, committedRoot, fields, values)
                }
                .value
            }
        }
      }

    /** Prove one field's full range and collect its `(keyHex → valueHex)` pairs from the branch entries. */
    private def proveField(
      prover: MerklePatriciaRangeProver[F],
      branchEntries: Map[Hex, Array[Byte]],
      field: GlobalStateFieldId
    ): F[Either[ProofError, (MerklePatriciaRangeProof, SortedMap[Hex, Hex])]] =
      (for {
        prefix <- EitherT.liftF[F, ProofError, Hex](GlobalStateKey.hypergraphFieldPrefix[F](field))
        bounds = fullRangeBounds(prefix)
        rangeProof <- EitherT(prover.attestRange(bounds._1, bounds._2))
          .leftMap(e => RangeProofGenerationFailed(field, e.getMessage): ProofError)
        // Values for this field = every branch entry whose hex key carries the field prefix, carried as
        // hex. These are the exact bytes that produced each leaf's `dataDigest` (producer `createFromBytes`
        // → `Hasher.hashBytes`), so the verifier's binding `hashBytes(valueHex) == leaf.dataDigest` holds.
        fieldValues = SortedMap.from(
          branchEntries.collect {
            case (hex, bytes) if hex.value.startsWith(prefix.value) => hex -> Hex.fromBytes(bytes)
          }
        )
      } yield (rangeProof, fieldValues)).value

    /** `[startPath, endPath]` covering a field's entire key-space: the field prefix followed by the all-`0` and all-`f` userNamespace
      * suffix. Every real key for the field (`prefix + 01 + <64-hex hash>`) sorts strictly inside, and any key outside the field has a
      * different prefix and falls outside the bracket — so the range proof's boundaries witness the field's full extent.
      *
      * `suffixLen` = `keyLen - prefixLen`, where a hypergraph field key is `00`(network) + 8(fieldId) + `00`(empty contract) +
      * `01`+64-hex(address-hashed user) = 78 hex chars, and the prefix is the first 12. We derive it from the prefix length so a future
      * key-layout change doesn't silently desync the bounds.
      */
    private def fullRangeBounds(prefix: Hex): (Hex, Hex) = {
      val keyHexLen = 78
      val suffixLen = math.max(keyHexLen - prefix.value.length, 0)
      (Hex(prefix.value + ("0" * suffixLen)), Hex(prefix.value + ("f" * suffixLen)))
    }
  }
}
