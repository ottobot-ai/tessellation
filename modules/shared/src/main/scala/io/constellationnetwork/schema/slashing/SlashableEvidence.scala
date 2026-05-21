package io.constellationnetwork.schema.slashing

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.Signature

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Slashing evidence transaction (Slice S4a — see `docs/nakamoto/SLASHING-DESIGN.md` §4).
  *
  * Carries a pair of conflicting [[MetagraphAttestation]] signatures from a single committee member on the same `(metagraphAddress,
  * parentHash)` with distinct `binaryHash` — i.e., proof of equivocation. The submitter signs the evidence pair to claim the bounty; the
  * submitter-side signature is over a domain-separated digest of both attestations + the submitter's id, so a replay attacker can't lift
  * the bounty by re-submitting someone else's evidence.
  *
  * '''Threat model.''' All slashing operates from cryptographically verifiable evidence — no consensus-state heuristics, no behavioral
  * inference. The validator must decide accept/reject byte-identically on every honest node. See `feedback_slashing_safety_bar`.
  *
  * '''Why two `Signed[MetagraphAttestation]`s.''' The outer `Signed[_]` envelopes carry the operator's long-term Ed25519 signature over
  * each attestation. The KES product signature (carried in the body) is independently load-bearing — both must verify for the evidence
  * to be admissible. Distinct binaries on the same parent prove equivocation regardless of which sigs verified honestly: an honest operator
  * who deletes their old KES SK can never re-sign, so producing the second signature requires deliberate adversarial action.
  *
  * '''Why `bountySignature: Signature` (not `HashSignature`).''' The SLASHING-DESIGN.md draft references `HashSignature`, which is the
  * informal name for "signature over a Hash" — the Tessellation codebase encodes this directly as the [[Signature]] newtype (a `Hex` wrap
  * around the raw ECDSA bytes). The submitter signs over `Blake2b256(evidenceA.bytes ‖ evidenceB.bytes ‖ submitterId.bytes)` (routed
  * through `Hasher[F]` per `feedback_use_hasher_no_manual_serialize`), and the validator verifies the signature under the submitter's
  * long-term Ed25519 public key — same as the long-term signature path on any other L0 tx. The validator decides accept/reject; the GSAM
  * accept path (S4c, separate agent) credits the bounty.
  */
@derive(decoder, encoder, eqv, show)
final case class SlashableEvidence(
  evidenceA: Signed[MetagraphAttestation],
  evidenceB: Signed[MetagraphAttestation],
  submitterId: PeerId,
  bountySignature: Signature
)

object SlashableEvidence {

  /** The bytes the submitter signs to claim the bounty. Routed through `Hasher[F]` (Circe-encoded
    * [[BountyDigestPreimage]]) per `feedback_use_hasher_no_manual_serialize`. Producer and validator MUST agree on this case class's
    * encoding for the signature to round-trip — keep the field order frozen.
    *
    * The doc spec is `Blake2b256(evidenceA.bytes ‖ evidenceB.bytes ‖ submitterId.bytes)`; we encode the equivalent under the project's
    * unified Hasher surface (SHA-256 over canonical Circe JSON) instead of hand-rolling Blake2b. The semantic role is identical: a
    * domain-separated digest binding the submitter to the specific evidence pair, so the bounty can't be lifted by re-submitting a
    * neighbour's evidence.
    */
  @derive(encoder)
  final case class BountyDigestPreimage(
    evidenceA: Signed[MetagraphAttestation],
    evidenceB: Signed[MetagraphAttestation],
    submitterIdHex: String
  )
}

/** Rejection reasons for [[SlashableEvidence]] validation — one variant per step in `SLASHING-DESIGN.md` §4.1.
  *
  * Sealed ADT (per `feedback_no_string_matching`): the validator returns the exact step that fired so detection metrics + slasher
  * audit-log can name the failure mode without re-deriving from a string.
  */
@derive(eqv, show)
sealed trait SlashingRejection extends Product with Serializable

object SlashingRejection {

  /** §4.1 step 1 — `evidenceA.peerId != evidenceB.peerId`. Different signers ⇒ not equivocation. */
  @derive(eqv, show)
  final case class IdentityMismatch(peerA: PeerId, peerB: PeerId) extends SlashingRejection

  /** §4.1 step 2 — `evidenceA.metagraphAddress != evidenceB.metagraphAddress`. Different subjects ⇒ no equivocation. */
  @derive(eqv, show)
  final case class SubjectMismatch(metagraphA: Address, metagraphB: Address) extends SlashingRejection

  /** §4.1 step 3 — `evidenceA.parentHash != evidenceB.parentHash`. Different VRF inputs ⇒ two distinct committee draws ⇒ no equivocation.
    * This is the load-bearing identity per `COMMITTEE-SORTITION-DESIGN.md` §2.
    */
  @derive(eqv, show)
  final case class ParentMismatch(parentA: Hash, parentB: Hash) extends SlashingRejection

  /** §4.1 step 4 — `evidenceA.binaryHash == evidenceB.binaryHash`. Same binary ⇒ duplicate retransmission, not equivocation. */
  @derive(eqv, show)
  final case class DuplicateBinary(binaryHash: Hash) extends SlashingRejection

  /** §4.1 step 5 — KES signature on at least one attestation does not verify under the operator's master VK.
    *
    * Distinguishes evidenceA vs evidenceB failure for diagnostic precision. The reject-side metrics in S4b can label which leg failed.
    */
  @derive(eqv, show)
  sealed trait InvalidKesSignature extends SlashingRejection
  object InvalidKesSignature {
    case object OnEvidenceA extends InvalidKesSignature
    case object OnEvidenceB extends InvalidKesSignature
  }

  /** §4.1 step 6 — committee VRF proof does not verify on at least one attestation. Reuses `CommitteeSortition.verifyMembership`. */
  @derive(eqv, show)
  sealed trait InvalidCommitteeVrf extends SlashingRejection
  object InvalidCommitteeVrf {
    case object OnEvidenceA extends InvalidCommitteeVrf
    case object OnEvidenceB extends InvalidCommitteeVrf
  }

  /** §4.1 step 7 — the MPT key `slashings/<peer_id>/<metagraph_address>/<parent_hash>` already has an entry. Block double-slashing on the
    * same `(slashed_peer, metagraph, parent)` triple — only the first SlashableEvidence to land claims the bounty.
    */
  @derive(eqv, show)
  final case class AlreadySlashed(peerId: PeerId, metagraphAddress: Address, parentHash: Hash) extends SlashingRejection

  /** §4.1 step 8 — `currentEpoch > evidenceEpoch + evidence_window`. Stale evidence — the slashed stake has rolled over by now.
    *
    * Producer carries `eventEpoch` out-of-band (the validator's input). Window default is 100 epochs per `NAKAMOTO_SLASH_EVIDENCE_WINDOW`.
    */
  @derive(eqv, show)
  final case class EvidenceWindowExpired(currentEpoch: Long, eventEpoch: Long, windowEpochs: Long) extends SlashingRejection

  /** §4.1 step 9 — the submitter's signature over the bounty-digest preimage does not verify under the submitter's long-term Ed25519 key.
    * Prevents replay-style bounty hijack.
    */
  case object InvalidBountySignature extends SlashingRejection
}
