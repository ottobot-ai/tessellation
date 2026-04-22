package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.transaction.{RewardTransaction, TransactionAmount}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `RewardTransaction` — `(destination: Address, amount: TransactionAmount)`.
  *
  * Wire layout:
  *   - destination : Address (uint8 length + ASCII, 41 or 52 bytes)
  *   - amount : TransactionAmount (8 bytes big-endian PosLong)
  *
  * Field order matches the case class declaration. Total size varies only with address length (41 or 52 bytes), so: 49 or 60 bytes total.
  *
  * Consensus contract: FROZEN. Any schema evolution requires a v2 era.
  */
object RewardTransactionCodec {

  private val addressCodec: Codec[Address] = AddressCodec.codec
  private val amountCodec: Codec[TransactionAmount] = Codec[TransactionAmount]

  implicit val codec: Codec[RewardTransaction] =
    (addressCodec :: amountCodec)
      .xmap[RewardTransaction](
        { case dst :: amt :: HNil => RewardTransaction(dst, amt) },
        rt => rt.destination :: rt.amount :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[RewardTransaction] = ImmutableCodec.fromScodecCodec(codec)
}
