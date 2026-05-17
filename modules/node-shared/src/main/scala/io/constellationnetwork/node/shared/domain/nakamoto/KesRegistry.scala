package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Per-operator KES master-VK registry.
  *
  * Holds the `(operatorPeerId → KesRegistryEntry)` map that lets receivers verify incoming KES signatures from a peer without trusting the
  * peer to ship its own VK in-band. The binding `(operatorPeerId → kesMasterVk, offset)` is established by the operator at registration
  * time: the operator signs `kesMasterVk.value || offset` with its long-term Ed25519 key, and that signature is what gets persisted
  * alongside the VK (in genesis for v1, in a runtime registration tx for v2 per `project_consensus_epoch_staggering`).
  *
  * '''Offset.''' Each operator's tree is rooted at a specific global eta period — its `offset`. At genesis every operator registers with
  * `offset = 0` so all KES trees are aligned. A mid-life joiner via Slice 10 (#179) registers with `offset = N` where N is the global eta
  * period at which their tree's internal step 0 becomes valid. Receivers compute `treeInternalStep = globalEtaPeriod - offset` and verify
  * with `vk.copy(step = treeInternalStep)`; a negative `treeInternalStep` means the operator wasn't yet active and the sig is rejected.
  *
  * '''Read-only.''' This trait does not expose mutators — the v1 registry is loaded once from `L0GenesisData.kesRegistrations` at startup
  * and frozen for the lifetime of the node. v2 will add a separate runtime-mutable variant gated on a `KesRegistrationTx` with the N-2
  * eta-staggering rule (#179).
  *
  * '''Forward security.''' The registry stores ONLY public material — `VerificationKeyKesProduct` (the period-0 root of the super × sub
  * Merkle tree) and the operator's eta-period offset. The corresponding KES secret key never leaves the operator; the receiver only needs
  * the VK + offset to verify signatures, and KES forward security comes from the SK lifecycle inside `OperationalKeyMaker`, not from any
  * property of this registry.
  *
  * See `docs/nakamoto/IMPLEMENTATION-PLAN-POST-VALIDATION.md` §1.2 and the `project_consensus_epoch_staggering` memory.
  */
trait KesRegistry[F[_]] {

  /** Look up the KES master VK + offset registered for `peerId`. `None` means the peer is not in the registry (didn't register at genesis,
    * or registered in a not-yet-active eta period).
    */
  def getKesVk(peerId: PeerId): F[Option[KesRegistryEntry]]

  /** All currently-registered entries. Diagnostic / observability. */
  def list: F[Map[PeerId, KesRegistryEntry]]
}

/** Per-operator registry entry: master KES verification key + the eta-period offset at which the operator's tree was rooted.
  *
  * For genesis operators, `offset = 0` (their tree's step 0 corresponds to global eta period 0). For mid-life joiners via Slice 10, `offset
  * \= etaPeriodAtRegistration` (their tree's step 0 corresponds to the period in which their registration tx finalized + the N-2
  * eta-staggering activation lag).
  */
final case class KesRegistryEntry(vk: VerificationKeyKesProduct, offset: Long)

object KesRegistry {

  /** Construct a frozen registry from a pre-built `(PeerId → KesRegistryEntry)` map. Typically called from the L0 genesis loader with the
    * parsed `L0GenesisData.kesRegistrations` decoded into KES types.
    */
  def make[F[_]: Sync](registrations: Map[PeerId, KesRegistryEntry]): KesRegistry[F] =
    new KesRegistry[F] {
      override def getKesVk(peerId: PeerId): F[Option[KesRegistryEntry]] =
        Sync[F].pure(registrations.get(peerId))
      override def list: F[Map[PeerId, KesRegistryEntry]] =
        Sync[F].pure(registrations)
    }

  /** Empty registry — used as the default when L0 genesis carries no `kesRegistrations` field. All `getKesVk` lookups return `None`. */
  def empty[F[_]: Sync]: KesRegistry[F] = make(Map.empty)
}
