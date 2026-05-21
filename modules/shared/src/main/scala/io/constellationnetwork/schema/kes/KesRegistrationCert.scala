package io.constellationnetwork.schema.kes

import cats.Order
import cats.Order._
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.ext.derevo.ordering
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.auto.{autoRefineV, _}
import eu.timepit.refined.cats._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.refined._
import io.estatico.newtype.macros.newtype

/** §1.2 Slice 10 — runtime KES master-VK registration cert (v2).
  *
  * Genesis registration (Slice 3 / `L0GenesisKesRegistration`) handles operators present at network birth. This cert handles every other
  * case: an operator joining mid-life, or an existing operator rotating to a fresh master VK before their current KES tree expires.
  *
  * '''Cert content.''' The cert carries the operator's `peerId`, the new `kesMasterVK` (the period-0 root of the super × sub KES product
  * tree, identified by raw `value` bytes and tree-step `step`), an `offset` (the eta-period at which this tree's internal step 0 becomes
  * valid — same semantics as `KesRegistryEntry.offset`), an `effectiveFromEpoch` (the global epoch at which receivers begin honoring this
  * VK), and a monotonic `ordinal` (per-operator counter; replays / out-of-order cert delivery is rejected on `ordinal <= last seen`).
  *
  * '''Binding signature.''' The `Signed[KesRegistrationCert]` envelope carries the operator's long-term Ed25519 signature over the cert
  * body. This is the binding that ties the new KES master VK to a peer that the network already trusts (the long-term key is the same one
  * the seedlist / on-chain operator records identify). Receivers verify (a) the envelope signature under the operator's known long-term
  * public key, (b) `ordinal` monotonicity vs the last accepted cert for this `operatorPeerId`, (c) `effectiveFromEpoch > current epoch`,
  * and (d) the KES VK bytes are well-formed (non-empty and `kesVkStep >= 0`). Cryptographic VK well-formedness — that the bytes are a
  * genuine super × sub Merkle root, not garbage — is checked at the OperationalKeyMaker / verifier layer when a sig under the VK is
  * actually presented; the cert validator deliberately keeps that out of band since a malformed-VK cert is harmless until used.
  *
  * '''Activation lag.''' We forbid registering with `effectiveFromEpoch <= currentEpoch` to leave a small N-2-style staggering window so
  * that all honest nodes see the cert finalize before its VK becomes load-bearing for verification. The validator enforces the strict
  * inequality; the GSAM accept handler applies the cert (writes the new entry into the mutable registry overlay) only when the snapshot's
  * epoch crosses `effectiveFromEpoch`.
  */
object KesRegistrationCert {

  /** Per-operator monotonic ordinal. Increments by 1 across each successive registration cert from the same operator. The very first cert
    * uses `first` (ordinal=1); subsequent rotations call `.next`. Mirrors the `NodeCollateralOrdinal` pattern.
    */
  @derive(decoder, encoder, show, order, ordering)
  @newtype
  case class KesRegistrationOrdinal(value: NonNegLong) {
    def next: KesRegistrationOrdinal = KesRegistrationOrdinal(value |+| 1L)
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

  /** Persisted record of an accepted runtime registration cert. Carries the snapshot ordinal at which it was accepted; lookups can compare
    * against the snapshot's `epochProgress` to decide whether the cert is "pending" (acceptedAt epoch <= currentEpoch < effectiveFromEpoch)
    * vs "effective" (currentEpoch >= effectiveFromEpoch).
    */
  @derive(decoder, encoder, eqv, show)
  case class KesRegistrationRecord(event: Signed[KesRegistrationCert], acceptedAt: SnapshotOrdinal)

  object KesRegistrationRecord {
    implicit val order: Order[KesRegistrationRecord] = Order[SnapshotOrdinal].contramap(_.acceptedAt)
    implicit val ordering: Ordering[KesRegistrationRecord] =
      Ordering.by(r => (r.acceptedAt, r.event.value.ordinal))
  }
}

@derive(eqv, show, encoder, decoder, order)
case class KesRegistrationCert(
  operatorPeerId: PeerId,
  kesMasterVK: Hex,
  kesMasterVKStep: Int,
  offset: Long,
  effectiveFromEpoch: EpochProgress,
  ordinal: KesRegistrationCert.KesRegistrationOrdinal,
  parent: KesRegistrationCert.KesRegistrationReference = KesRegistrationCert.KesRegistrationReference.empty
)
