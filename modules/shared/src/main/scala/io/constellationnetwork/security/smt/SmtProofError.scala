package io.constellationnetwork.security.smt

import cats.Eq
import cats.syntax.eq._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

/** Failure modes of [[SmtProver.prove]] / [[SmtVerifier.verify]]. Sealed, exhaustive, no string-typed control flow — mirrors the structural
  * style of `FollowVerifyCore.FollowVerificationError` (carry the structural cause, never re-derive it from a message string).
  *
  *   - [[SmtProofError.ValueBindingFailed]] — an [[SmtProof.Inclusion]]'s `Hasher.hashBytes(value)` did not equal the leaf's committed
  *     value digest (the value bytes do not bind to the committed leaf). Value-binding is MANDATORY inside verify (the gl1 doc's contract
  *     bar #2); there is no verify variant that skips it.
  *   - [[SmtProofError.RootMismatch]] — the root recomputed by folding the proof's authentication path did not equal the trusted
  *     [[SmtRoot]]. Carries both `expected` (trusted) and `got` (recomputed).
  *   - [[SmtProofError.MalformedProof]] — the proof is structurally inconsistent before any root fold can be trusted: e.g. an
  *     [[AbsenceWitness.OtherLeaf]] whose recomputed position equals the queried position (so it is not a genuine OTHER leaf), or an
  *     authentication path deeper than the 256-bit position space. Carries the offending `key` and a structural `reason`.
  *   - [[SmtProofError.UnknownVersion]] — used only by the versioned layer ([[VersionedSmt.proveAt]] / [[VersionedSmt.rootAt]]): the
  *     requested `version` is not under retention (never committed, or already pruned). Distinct from a verification failure — it means
  *     there is no root to prove against.
  */
sealed trait SmtProofError extends Product with Serializable

object SmtProofError {

  final case class ValueBindingFailed(key: Hex) extends SmtProofError
  final case class RootMismatch(expected: Hash, got: Hash) extends SmtProofError
  final case class MalformedProof(key: Hex, reason: MalformedReason) extends SmtProofError
  final case class UnknownVersion(version: SnapshotOrdinal) extends SmtProofError

  /** Structural (non-string) reasons a proof is malformed. Closed set so control flow stays pattern-matched. */
  sealed trait MalformedReason extends Product with Serializable
  object MalformedReason {

    /** An [[AbsenceWitness.OtherLeaf]] whose recomputed occupying position equals the queried position — i.e. the key IS present, so the
      * proof cannot soundly claim absence.
      */
    case object OtherLeafCollidesWithKey extends MalformedReason

    /** The authentication path (siblings list) is longer than the 256-bit position space allows. */
    case object PathTooDeep extends MalformedReason

    implicit val eq: Eq[MalformedReason] = Eq.fromUniversalEquals
  }

  // GlobalStateFieldId-style: local Eq for test assertions / dedup, never control flow.
  implicit val eq: Eq[SmtProofError] = Eq.instance {
    case (ValueBindingFailed(k1), ValueBindingFailed(k2)) => k1 === k2
    case (RootMismatch(e1, g1), RootMismatch(e2, g2))     => e1 === e2 && g1 === g2
    case (MalformedProof(k1, r1), MalformedProof(k2, r2)) => k1 === k2 && r1 === r2
    case (UnknownVersion(v1), UnknownVersion(v2))         => v1 === v2
    case _                                                => false
  }
}
