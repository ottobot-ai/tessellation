package io.constellationnetwork.node.shared.domain.nakamoto

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.kes.VerificationKeyKesProduct

/** Read-only KES projection of an atomic operator consensus-key registry.
  *
  * This trait exists only for diagnostics and legacy non-consensus adapters that cannot yet consume [[OperatorConsensusKeyRegistry]]. It
  * has no standalone constructor: every implementation must be derived from the same atomic KES+VRF records. Consensus code resolves the
  * complete pair once and must never use this projection as registration authority.
  *
  * '''Offset.''' Each operator's tree is rooted at a specific global eta period — its `offset`. At genesis every operator registers with
  * `offset = 0` so all KES trees are aligned. Receivers are intended to compute `treeInternalStep = globalEtaPeriod - offset` and verify
  * with `vk.copy(step = treeInternalStep)`. A wire-carried step is evidence only.
  *
  * Runtime registration is resolved from exact candidate-parent history through [[HistoricalOperatorConsensusKeyRegistry]], never by
  * constructing a new projection from receiver-current state.
  *
  * '''Forward security.''' The registry stores ONLY public material — `VerificationKeyKesProduct` (the period-0 root of the super × sub
  * Merkle tree) and the operator's eta-period offset. The corresponding KES secret key never leaves the operator; the receiver only needs
  * the VK + offset to verify signatures, and KES forward security comes from the SK lifecycle inside `OperationalKeyMaker`, not from any
  * property of this registry.
  */
trait KesRegistry[F[_]] {

  /** Look up the KES master VK + offset registered for `peerId`. `None` means the peer is not in the registry (didn't register at genesis,
    * or registered in a not-yet-active eta period).
    */
  def getKesVk(peerId: PeerId): F[Option[KesRegistryEntry]]

  /** All currently-registered entries. Diagnostic / observability. */
  def list: F[Map[PeerId, KesRegistryEntry]]
}

/** Derived KES portion of one operator's atomic consensus-key pair.
  *
  * For genesis operators, `offset = 0` (their tree's step 0 corresponds to global eta period 0). A future runtime record will bind the KES
  * and VRF keys together and derives its offset/activation from canonical inclusion; this entry alone cannot represent that history.
  */
final case class KesRegistryEntry(vk: VerificationKeyKesProduct, offset: Long)
