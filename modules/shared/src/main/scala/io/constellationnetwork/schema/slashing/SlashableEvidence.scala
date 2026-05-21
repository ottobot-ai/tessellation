package io.constellationnetwork.schema.slashing

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.signature.Signature

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** Slashing evidence transaction (Slice S4a — see `docs/nakamoto/SLASHING-DESIGN.md` §4).
  *
  * Carries a pair of conflicting [[MetagraphAttestation]] bodies from a single committee member on the same `(metagraphAddress,
  * parentHash)` with distinct `binaryHash` — i.e., proof of equivocation. The submitter signs the evidence pair to claim the bounty; the
  * submitter-side signature is over a domain-separated digest of both attestations + the submitter's id, so a replay attacker can't lift
  * the bounty by re-submitting someone else's evidence.
  *
  * '''Threat model.''' All slashing operates from cryptographically verifiable evidence — no consensus-state heuristics, no behavioral
  * inference. The validator must decide accept/reject byte-identically on every honest node. See `feedback_slashing_safety_bar`.
  *
  * '''Why no outer `Signed[_]` envelope on the evidence attestations.''' Each [[MetagraphAttestation]] body already carries the KES product
  * signature (verified at step 5 against the operator's master VK in [[io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry]])
  * and the committee VRF proof (verified at step 6 via `CommitteeSortition.verifyMembership`). KES + VRF together carry all the safety the
  * validator needs — the gossip-path Ed25519 envelope was never re-verified by `SlashableEvidenceValidator`, so wrapping each attestation
  * in `Signed[_]` here only inflated the wire bytes and the digest preimage with a redundant signature. Distinct binaries on the same
  * parent prove equivocation regardless: an honest operator who deletes their old KES SK can never re-sign the second binary, so producing
  * the second KES sig requires deliberate adversarial action. The detector (S4b) strips the gossip-layer `Signed[_]` envelope via `.value`
  * when constructing evidence; the gossip envelope continues to live on the gossip path where it does get verified.
  *
  * '''Wire-byte stability (hard-fork risk).''' The [[BountyDigestPreimage]] is hashed by `Hasher[F]` (canonical Circe JSON over the case
  * class). Adding optional fields, reordering fields, or wrapping a field in `Signed[_]` silently changes the digest bytes and breaks
  * cross-version evidence verification. If the schema ever has to evolve, bump the preimage case class explicitly
  * (`BountyDigestPreimageV2`) and version the validator branch — never silently mutate the field set.
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
  evidenceA: MetagraphAttestation,
  evidenceB: MetagraphAttestation,
  submitterId: PeerId,
  bountySignature: Signature
)

object SlashableEvidence {

  /** The bytes the submitter signs to claim the bounty. Routed through `Hasher[F]` (Circe-encoded [[BountyDigestPreimage]]) per
    * `feedback_use_hasher_no_manual_serialize`. Producer and validator MUST agree on this case class's encoding for the signature to
    * round-trip — keep the field order frozen.
    *
    * The doc spec is `Blake2b256(evidenceA.bytes ‖ evidenceB.bytes ‖ submitterId.bytes)`; we encode the equivalent under the project's
    * unified Hasher surface (SHA-256 over canonical Circe JSON) instead of hand-rolling Blake2b. The semantic role is identical: a
    * domain-separated digest binding the submitter to the specific evidence pair, so the bounty can't be lifted by re-submitting a
    * neighbour's evidence.
    *
    * '''Frozen wire shape.''' The fields here are [[MetagraphAttestation]] bodies (not `Signed[_]` envelopes) — see the
    * [[SlashableEvidence]] scaladoc for the design rationale. The exact field set and order is consensus-load-bearing: changing it silently
    * changes the digest and breaks cross-version evidence verification. Don't add optional fields; version the case class explicitly if it
    * has to evolve.
    */
  @derive(encoder)
  final case class BountyDigestPreimage(
    evidenceA: MetagraphAttestation,
    evidenceB: MetagraphAttestation,
    submitterIdHex: String
  )
}

/** Rejection reasons for [[SlashableEvidence]] validation — one variant per step in `SLASHING-DESIGN.md` §4.1.
  *
  * Sealed ADT (per `feedback_no_string_matching`): the validator returns the exact step that fired so detection metrics + slasher audit-log
  * can name the failure mode without re-deriving from a string.
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
