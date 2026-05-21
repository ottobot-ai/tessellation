package io.constellationnetwork.schema.slashing

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** JVM-side domain shape of a per-metagraph committee VRF attestation (Slice S2+).
  *
  * '''Why this lives here.''' The wire form is `pb.MetagraphAttestation` (proto, generated from `p2p/proto/sidecar.proto`) and the gate's
  * pre-parsed in-memory shape is [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.IncomingAttestation]]. Neither
  * is a stable, codec-friendly case class that can sit inside `Signed[_]` and round-trip through `Hasher[F]` for inclusion in an L0
  * transaction. Slashing evidence requires both: the validator (S4a) needs `Signed[MetagraphAttestation]` to identify the equivocator's
  * long-term Ed25519 signature, and the GSAM accept path (S4c) needs deterministic bytes to MPT-key by.
  *
  * '''Field set.''' Exactly what the slashing validator (`SLASHING-DESIGN.md` §4.1) reads:
  *   - `peerId` — committee member who signed
  *   - `metagraphAddress` — subject of the attestation
  *   - `parentHash` — load-bearing committee-VRF input (per `COMMITTEE-SORTITION-DESIGN.md` §2)
  *   - `binaryHash` — the distinguishing field (equal across the two evidence attestations = duplicate, distinct = equivocation)
  *   - `committeeVrfProof` — verified via `CommitteeSortition.verifyMembership` against the operator's VRF VK
  *   - `vrfPublicKey` — operator's VRF VK at attest time (carried on-wire; same Slice-S3 design as `IncomingAttestation`)
  *   - `kesSignature` — KES product signature reused by [[io.constellationnetwork.security.kes.OperationalKeyMaker.decodeSignature]]
  *   - `senderTreeStep` — KES tree-internal step at sign time, enables non-interactive receiver verify
  *
  * '''No long-term Ed25519 sig on the body.''' The body is what gets Ed25519-signed; the signature lives in the `Signed[_]` envelope.
  *
  * '''Byte storage choice.''' Raw VRF/KES material lives in [[Hex]] (newtype around `String`) rather than `Array[Byte]` so the case class
  * gets free Circe `decoder`/`encoder` instances and round-trips deterministically via the Hasher typeclass. The Hex form is exactly what
  * the wire-side `ByteString.copyFrom` ↔ JVM bytes path produces upstream — see `SidecarClient.mkMetagraphAttestation`.
  */
@derive(decoder, encoder, eqv, show)
final case class MetagraphAttestation(
  peerId: PeerId,
  metagraphAddress: Address,
  parentHash: Hash,
  binaryHash: Hash,
  committeeVrfProof: Hex,
  vrfPublicKey: Hex,
  kesSignature: Hex,
  senderTreeStep: Int
)
