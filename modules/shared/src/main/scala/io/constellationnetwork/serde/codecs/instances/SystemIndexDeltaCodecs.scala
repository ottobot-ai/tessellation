package io.constellationnetwork.serde.codecs.instances

import cats.Order

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SetCodec.set
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Explicit scodec codecs for the system-index delta family carried by `StateChangesAccumulator`:
  * the three expiry-index key types and the `SystemIndexDelta` ADT.
  *
  * Hand-written (no auto-derivation) so the binary layout is a stable, audited spec — these bytes feed the per-ordinal state-diff wire
  * (currency-l0 adopt path). Field order matches the case-class declarations exactly.
  */
object SystemIndexDeltaCodecs {

  // ---- Expiry-index keys ----------------------------------------------------

  val allowSpendExpiryKeyCodec: Codec[AllowSpendExpiryKey] =
    (option(addressCodec) :: addressCodec :: hashCodec).xmap[AllowSpendExpiryKey](
      { case mid :: addr :: h :: HNil => AllowSpendExpiryKey(mid, addr, h) },
      k => k.metagraphId :: k.address :: k.hash :: HNil
    )

  val tokenLockExpiryKeyCodec: Codec[TokenLockExpiryKey] =
    (addressCodec :: hashCodec).xmap[TokenLockExpiryKey](
      { case addr :: h :: HNil => TokenLockExpiryKey(addr, h) },
      k => k.address :: k.hash :: HNil
    )

  val nodeCollateralWithdrawalExpiryKeyCodec: Codec[NodeCollateralWithdrawalExpiryKey] =
    (addressCodec :: hashCodec).xmap[NodeCollateralWithdrawalExpiryKey](
      { case addr :: h :: HNil => NodeCollateralWithdrawalExpiryKey(addr, h) },
      k => k.address :: k.hash :: HNil
    )

  // ---- SystemIndexDelta ADT -------------------------------------------------

  /** Codec for `SystemIndexDelta[K]`. Currently one variant (`EpochBucket`), encoded directly as `(adds, removes)`, each a
    * `SortedMap[EpochProgress, Set[K]]`. When a second variant is introduced it gets its own discriminator (a new era per the
    * `ImmutableCodec` doctrine) — not added prophylactically.
    */
  def systemIndexDeltaCodec[K: Order](kCodec: Codec[K]): Codec[SystemIndexDelta[K]] = {
    val bucketMapCodec: Codec[SortedMap[EpochProgress, Set[K]]] =
      sortedMap(Codec[EpochProgress], set(kCodec))

    (bucketMapCodec :: bucketMapCodec).xmap[SystemIndexDelta[K]](
      { case adds :: removes :: HNil => SystemIndexDelta.EpochBucket(adds, removes) },
      { case SystemIndexDelta.EpochBucket(adds, removes) => adds :: removes :: HNil }
    )
  }

  // Keep the unused Address/Hash type imports referenced for clarity of the key shapes above.
  private val _addrWitness: Codec[Address] = addressCodec
  private val _hashWitness: Codec[Hash] = hashCodec
  locally { val _ = (_addrWitness, _hashWitness) }
}
