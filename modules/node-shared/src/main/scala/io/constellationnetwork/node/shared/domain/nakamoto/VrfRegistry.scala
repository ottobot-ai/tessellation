package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.schema.peer.PeerId

/** Read-only VRF-key projection of an atomic [[OperatorConsensusKeyRegistry]].
  *
  * This algebra is a compatibility view for callers that need only `(operatorPeerId → vrfVk)`. It is not constructible through an
  * independent companion factory and is never a registration authority: production and consensus-path tests derive it from the same
  * [[OperatorConsensusKeyRegistry]] record that owns the operator's KES master key, activation period, and registration evidence.
  *
  * A key carried by a snapshot, attestation, checkpoint, proof, or transport envelope is comparison evidence only. Consensus consumers must
  * resolve the exact active atomic pair at the artifact's historical parent, require byte equality when a key is carried, and verify the
  * proof under the resolved key. Missing, pending, stale, duplicate, mismatched, or historically unavailable pairs fail closed.
  *
  * The projection exposes public verification material only. Local secret-key provisioning and its byte-equality check against the resolved
  * atomic registration are separate signing-side responsibilities.
  */
trait VrfRegistry[F[_]] {

  /** Look up the VRF verification key registered for `peerId`. `None` means the peer is not in the registry (didn't register at genesis, or
    * registered without a VRF VK).
    */
  def getVrfVk(peerId: PeerId): F[Option[Array[Byte]]]

  /** All currently-registered entries. Diagnostic / observability. */
  def list: F[Map[PeerId, Array[Byte]]]
}
