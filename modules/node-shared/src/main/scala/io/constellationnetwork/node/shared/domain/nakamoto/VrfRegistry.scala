package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync

import io.constellationnetwork.schema.peer.PeerId

/** Per-operator VRF verification-key registry.
  *
  * Holds the `(operatorPeerId → vrfVk)` map that lets a gl0 operator look up the VRF verification key bound to any peer, without trusting
  * the peer to ship its own VK in-band. The VRF VK is derived deterministically from the operator's long-term secp256k1 keypair via
  * `VrfKeyDeriver.deriveVrfKeyPair` — the SAME derivation the gl0 snapshot leader loop (`SnapshotLeaderLoop.deriveVrfKeys`) applies at
  * runtime — so a genesis-populated VK byte-matches the operator's live VRF identity.
  *
  * '''Why piggyback on KES registration.''' The registry is sourced from the SAME genesis + runtime-cert path the [[KesRegistry]] uses
  * (`L0GenesisData.operators[].vrfPublicKey` at genesis; `KesRegistrationCert.vrfVK` for mid-life joiners). One operator-registration event
  * carries both the KES master VK and the VRF VK, so they cannot drift out of sync.
  *
  * '''Consensus uses.''' The metagraph-binary admission gate requires an attester's wire key to byte-match this registry and verifies the
  * secret-sortition proof under the registered key. Execution-shard membership separately hashes each registered VK into the public,
  * deterministic committee draw, and checkpoint verification uses the same registration to validate key-possession proofs. Missing keys
  * fail those committee checks closed; sender-provided replacement keys are never authoritative.
  *
  * '''Read-only.''' This trait does not expose mutators — the v1 registry is loaded once from `L0GenesisData.operators` at startup and
  * frozen for the lifetime of the node. Runtime joiners (via `KesRegistrationCert.vrfVK`) are a follow-up; when wired they will overlay
  * this frozen base the same way [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] overlays the genesis KES
  * registry.
  *
  * '''Public material only.''' The registry stores ONLY the VRF verification key (a 32-byte Ed25519 public key). The VRF secret key is
  * derived on-demand inside the operator from its own keypair and never leaves the node; receivers only need the VK to verify a VRF proof.
  */
trait VrfRegistry[F[_]] {

  /** Look up the VRF verification key registered for `peerId`. `None` means the peer is not in the registry (didn't register at genesis, or
    * registered without a VRF VK).
    */
  def getVrfVk(peerId: PeerId): F[Option[Array[Byte]]]

  /** All currently-registered entries. Diagnostic / observability. */
  def list: F[Map[PeerId, Array[Byte]]]
}

object VrfRegistry {

  /** Construct a frozen registry from a pre-built `(PeerId → vrfVk)` map. Typically called from the L0 genesis loader with the parsed
    * `L0GenesisData.operators` hex-decoded VRF VKs.
    */
  def make[F[_]: Sync](registrations: Map[PeerId, Array[Byte]]): VrfRegistry[F] =
    new VrfRegistry[F] {
      override def getVrfVk(peerId: PeerId): F[Option[Array[Byte]]] =
        Sync[F].pure(registrations.get(peerId))
      override def list: F[Map[PeerId, Array[Byte]]] =
        Sync[F].pure(registrations)
    }

  /** Empty registry — used as the default when L0 genesis carries no operators with a `vrfPublicKey`. All `getVrfVk` lookups return `None`.
    */
  def empty[F[_]: Sync]: VrfRegistry[F] = make(Map.empty)
}
