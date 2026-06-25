package io.constellationnetwork.security.mpt.prover.attestation

import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.{MerklePatriciaCommitment, Nibble}

import io.circe._
import io.circe.syntax.EncoderOps

/** Non-membership (absence) proof for the hexary Merkle Patricia Trie — task #286.
  *
  * Proves that a key `path` is ABSENT from the trie committed to by a trusted `mptRoot`. The proof carries the authenticated path of node
  * commitments from the root TOWARD `path` that TERMINATES without reaching a leaf for `path`. The verifier recomputes the root from the
  * witness (exactly as for an inclusion proof) and confirms the terminal node witnesses non-existence in one of three structural ways
  * (see [[AbsenceTermination]]).
  *
  * '''Witness ordering''' mirrors [[MerklePatriciaInclusionProof]]: the `witness` list is stored '''terminal-first''' (the node where the
  * walk terminates is `witness.head`, the root commitment is `witness.last`). The verifier reverses it to walk root→terminal, identical to
  * [[io.constellationnetwork.security.mpt.verifier.MerklePatriciaInclusionVerifier]].
  *
  * '''Soundness.''' Because the chain root→terminal is authenticated by `mptRoot` (collision resistance of `Hasher[F]`), and the descent
  * follows `path`'s own nibbles, the terminal node is exactly the position where `path` would resolve in this trie. If that position is an
  * empty branch slot, a divergent extension, or a leaf bound to a different key, then `path` cannot be present. The `termination` tag is
  * carried for explicitness but the verifier '''independently re-derives''' the termination kind from the witness structure and rejects if
  * it disagrees — a forged tag cannot bypass verification.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`): fresh, additive proof type. It does not alter
  * [[MerklePatriciaInclusionProof]] or [[MerklePatriciaRangeProof]], so no existing consumer wire-format changes.
  */
final case class MerklePatriciaAbsenceProof(
  path: Hex,
  witness: List[MerklePatriciaCommitment],
  termination: AbsenceTermination
)

/** The structural reason the walk toward the absent key terminates without finding it. Sealed — exactly the three hexary-MPT termination
  * cases enumerated in the #286 task spec.
  */
sealed trait AbsenceTermination

object AbsenceTermination {

  /** The terminal node is a [[MerklePatriciaCommitment.Branch]] reached after consuming a prefix of `path`, and the branch has NO child for
    * `path`'s next nibble. `missingNibble` is that next nibble (carried for explicitness; the verifier re-derives and cross-checks it).
    */
  final case class BranchEmptySlot(missingNibble: Nibble) extends AbsenceTermination

  /** The terminal node is a [[MerklePatriciaCommitment.Extension]] whose `shared` segment DIVERGES from `path`'s remaining nibbles at this
    * position (`path` reached the extension but does not continue along the extension's shared segment). The verifier confirms the divergence
    * structurally.
    */
  case object ExtensionDivergence extends AbsenceTermination

  /** The terminal node is a [[MerklePatriciaCommitment.Leaf]] occupying the position `path` would resolve to, but bound to a DIFFERENT key
    * (the leaf's `remaining` does not equal `path`'s remaining nibbles at the terminal). The verifier confirms the mismatch structurally.
    */
  case object LeafMismatch extends AbsenceTermination

  implicit val absenceTerminationEncoder: Encoder[AbsenceTermination] = Encoder.instance {
    case bes: BranchEmptySlot =>
      Json.obj(
        "type" -> Json.fromString("BranchEmptySlot"),
        "missingNibble" -> bes.missingNibble.asJson
      )
    case ExtensionDivergence =>
      Json.obj("type" -> Json.fromString("ExtensionDivergence"))
    case LeafMismatch =>
      Json.obj("type" -> Json.fromString("LeafMismatch"))
  }

  implicit val absenceTerminationDecoder: Decoder[AbsenceTermination] = Decoder.instance { c =>
    c.downField("type").as[String].flatMap {
      case "BranchEmptySlot" =>
        c.downField("missingNibble").as[Nibble].map(BranchEmptySlot(_))
      case "ExtensionDivergence" => Right(ExtensionDivergence)
      case "LeafMismatch"        => Right(LeafMismatch)
      case other                 => Left(DecodingFailure(s"Unknown AbsenceTermination type: $other", c.history))
    }
  }
}

object MerklePatriciaAbsenceProof {

  implicit val mpAbsenceProofEncoder: Encoder[MerklePatriciaAbsenceProof] = (proof: MerklePatriciaAbsenceProof) =>
    Json.obj(
      "path" -> proof.path.asJson,
      "witness" -> proof.witness.asJson,
      "termination" -> proof.termination.asJson
    )

  implicit val mpAbsenceProofDecoder: Decoder[MerklePatriciaAbsenceProof] = (c: HCursor) =>
    for {
      path <- c.downField("path").as[Hex]
      witness <- c.downField("witness").as[List[MerklePatriciaCommitment]]
      termination <- c.downField("termination").as[AbsenceTermination]
    } yield MerklePatriciaAbsenceProof(path, witness, termination)
}
