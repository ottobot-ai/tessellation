package io.constellationnetwork.schema.kes

import cats.Order
import cats.Order._
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.util.Try

import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.ext.derevo.ordering
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.cats._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.refined._
import io.estatico.newtype.macros.newtype

/** Current unified operator consensus-key registration candidate.
  *
  * The signed body atomically binds one operator identity to both the KES master verification key and the VRF public key that consensus
  * consumers must use. A key carried by a later snapshot, checkpoint, attestation, or proof is evidence only; it is never registration
  * authority.
  *
  * The cert carries the operator's `peerId`, the new `kesMasterVK` (the period-0 root of the super × sub KES product tree, identified by
  * raw `value` bytes and tree-step `step`), an `offset` (the eta-period at which this tree's internal step 0 becomes valid), a 32-byte
  * `vrfPublicKey`, an `effectiveFromPeriod`, the exact GL0 `registrationParentHash`, and a monotonic per-operator `ordinal` and `parent`
  * sequence link. The two parent fields are intentionally distinct: the hash binds branch context, while the reference orders rotations.
  *
  * '''Binding signature.''' The `Signed[KesRegistrationCert]` envelope carries the operator's long-term Ed25519 signature over the cert
  * body. This is the binding that ties the new KES master VK to a peer that the network already trusts (the long-term key is the same one
  * the seedlist / on-chain operator records identify). Receivers verify (a) the envelope signature under the operator's known long-term
  * public key, (b) ordinal and parent continuity, (c) exact candidate-parent equality, (d) `effectiveFromPeriod >= canonical
  * inclusion/evaluation eta period + 2`, (e) the VRF public key is exactly 32 bytes, and (f) KES timing is bound to the same activation.
  * Cryptographic KES verification still occurs when a KES signature is presented.
  *
  * This type is the current registration candidate, not a complete runtime registry. GSAM persists accepted histories and latest pointers
  * in the candidate branch's rooted MPT; consensus consumers must still use the exact branch-historical resolver rather than a live view.
  */
object KesRegistrationCert {

  /** Per-operator monotonic ordinal. Increments by 1 across each successive registration cert from the same operator. The very first cert
    * uses `first` (ordinal=1); subsequent rotations use the checked `.next`, which returns `None` on overflow.
    */
  @derive(decoder, encoder, show, order, ordering)
  @newtype
  case class KesRegistrationOrdinal(value: NonNegLong) {
    def next: Option[KesRegistrationOrdinal] =
      Try(Math.addExact(value.value, 1L)).toOption.flatMap(NonNegLong.from(_).toOption).map(KesRegistrationOrdinal(_))
  }

  object KesRegistrationOrdinal {
    val empty: KesRegistrationOrdinal = KesRegistrationOrdinal(NonNegLong(0L))
    val first: KesRegistrationOrdinal = KesRegistrationOrdinal(NonNegLong(1L))
  }

  /** Reference into the chain of certs registered by a given operator. Replicates the `NodeCollateralReference` pattern. */
  @derive(eqv, show, encoder, decoder)
  case class KesRegistrationReference(ordinal: KesRegistrationOrdinal, hash: Hash)

  object KesRegistrationReference {
    val empty: KesRegistrationReference = KesRegistrationReference(KesRegistrationOrdinal.empty, Hash.empty)

    def of(hashedCert: Hashed[KesRegistrationCert]): KesRegistrationReference =
      KesRegistrationReference(hashedCert.ordinal, hashedCert.hash)

    def of[F[_]: Async: Hasher](signedCert: Signed[KesRegistrationCert]): F[KesRegistrationReference] =
      signedCert.value.hash.map(KesRegistrationReference(signedCert.value.ordinal, _))
  }

  /** Persisted record of an accepted runtime registration cert. Carries the snapshot ordinal at which it was accepted; lookups compare the
    * candidate's exact eta period with `effectiveFromPeriod` to distinguish pending from active registrations.
    */
  @derive(decoder, encoder, eqv, show)
  case class KesRegistrationRecord(event: Signed[KesRegistrationCert], acceptedAt: SnapshotOrdinal)

  object KesRegistrationRecord {
    implicit val ordering: Ordering[KesRegistrationRecord] =
      Ordering.by(r => (r.acceptedAt, r.event))
    implicit val order: Order[KesRegistrationRecord] = Order.fromOrdering(ordering)
  }
}

@derive(eqv, show, encoder, decoder, order, ordering)
case class KesRegistrationCert(
  operatorPeerId: PeerId,
  kesMasterVK: Hex,
  kesMasterVKStep: Int,
  offset: Long,
  vrfPublicKey: Hex,
  effectiveFromPeriod: EtaPeriod,
  registrationParentHash: Hash,
  ordinal: KesRegistrationCert.KesRegistrationOrdinal,
  parent: KesRegistrationCert.KesRegistrationReference = KesRegistrationCert.KesRegistrationReference.empty
)
