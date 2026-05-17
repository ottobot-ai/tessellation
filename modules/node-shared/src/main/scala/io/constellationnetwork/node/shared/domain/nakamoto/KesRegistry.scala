package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Per-operator KES master-VK registry.
  *
  * Holds the `(operatorPeerId → kesMasterVk)` map that lets receivers verify incoming KES signatures from a peer without trusting the peer
  * to ship its own VK in-band. The binding `(operatorPeerId → kesMasterVk)` is established by the operator at registration time: the
  * operator signs `kesMasterVk.value` with its long-term Ed25519 key, and that signature is what gets persisted alongside the VK (in
  * genesis for v1, in a runtime registration tx for v2 per `project_consensus_epoch_staggering`).
  *
  * '''Read-only.''' This trait does not expose mutators — the v1 registry is loaded once from `L0GenesisData.kesRegistrations` at startup
  * and frozen for the lifetime of the node. v2 will add a separate runtime-mutable variant gated on a `KesRegistrationTx` with the N-2
  * eta-staggering rule.
  *
  * '''Forward security.''' The registry stores ONLY public material — `VerificationKeyKesProduct` (the period-0 root of the super × sub
  * Merkle tree). The corresponding KES secret key never leaves the operator; the receiver only needs the VK to verify signatures, and KES
  * forward security comes from the SK lifecycle inside `OperationalKeyMaker`, not from any property of this registry.
  *
  * See `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.2 and the `project_consensus_epoch_staggering` memory.
  */
trait KesRegistry[F[_]] {

  /** Look up the KES master VK registered for `peerId`. `None` means the peer is not in the registry (didn't register at genesis, or
    * registered in a not-yet-active eta period).
    */
  def getKesVk(peerId: PeerId): F[Option[VerificationKeyKesProduct]]

  /** All currently-registered (peer, kesVk) pairs. Diagnostic / observability. */
  def list: F[Map[PeerId, VerificationKeyKesProduct]]
}

object KesRegistry {

  /** Construct a frozen registry from a pre-built `(PeerId → VerificationKeyKesProduct)` map. Typically called from the L0 genesis loader
    * with the parsed `L0GenesisData.kesRegistrations` decoded into KES types.
    */
  def make[F[_]: Sync](registrations: Map[PeerId, VerificationKeyKesProduct]): KesRegistry[F] =
    new KesRegistry[F] {
      override def getKesVk(peerId: PeerId): F[Option[VerificationKeyKesProduct]] =
        Sync[F].pure(registrations.get(peerId))
      override def list: F[Map[PeerId, VerificationKeyKesProduct]] =
        Sync[F].pure(registrations)
    }

  /** Empty registry — used as the default when L0 genesis carries no `kesRegistrations` field (Slice 3 backward-compatibility for fixtures
    * predating the schema extension; behaves as "no peer registered"). All `getKesVk` lookups return `None`; verification logic should
    * treat this as a benign skip rather than an error during the Slice 5 warn-only verification phase.
    */
  def empty[F[_]: Sync]: KesRegistry[F] = make(Map.empty)
}
