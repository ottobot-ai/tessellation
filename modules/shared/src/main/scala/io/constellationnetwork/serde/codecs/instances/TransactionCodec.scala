package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{codec => transactionReferenceCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `Transaction` — the first "real" multi-field consensus type in the scodec era.
  *
  * Wire layout (ordered, fixed-width where possible):
  *   - source : Address (uint8 length + ASCII, 41 or 52 bytes)
  *   - destination : Address (uint8 length + ASCII, 41 or 52 bytes)
  *   - amount : TransactionAmount (8 bytes big-endian PosLong)
  *   - fee : TransactionFee (8 bytes big-endian NonNegLong)
  *   - parent : TransactionReference (40 bytes: 8 ordinal + 32 hash)
  *   - salt : TransactionSalt (8 bytes big-endian signed Long)
  *
  * Field order matches the case class declaration order exactly. Reordering the case class fields without updating this codec would be a
  * consensus break — caught by the golden-file test on the first CI run.
  *
  * Not parameterized; Transaction is a concrete type. Its `Signed[Transaction]` codec is derived automatically via
  * `SignedCodec.signedCodec` the moment a `Codec[Transaction]` implicit is in scope.
  *
  * Consensus contract: FROZEN. Six fields in declared order. Adding a field requires introducing `TransactionV2` and a new scodec era, not
  * mutating this codec.
  *
  * Goldens:
  *   - `Transaction-scodec-v1.hex` — a canonical sample with distinct values per field so any accidental field swap shows up at byte
  *     granularity.
  */
object TransactionCodec {

  // Explicit local bindings: the implicits we want are spread across several
  // companions. Rather than rely on wildcard imports + ambiguous-name
  // resolution (as we saw in TransactionReferenceCodec), we summon them
  // explicitly here.
  private val addressCodec: Codec[Address] = AddressCodec.codec
  private val amountCodec: Codec[TransactionAmount] = Codec[TransactionAmount]
  private val feeCodec: Codec[TransactionFee] = Codec[TransactionFee]
  private val refCodec: Codec[TransactionReference] = transactionReferenceCodec
  private val saltCodec: Codec[TransactionSalt] = Codec[TransactionSalt]

  implicit val codec: Codec[Transaction] =
    (addressCodec :: addressCodec :: amountCodec :: feeCodec :: refCodec :: saltCodec)
      .xmap[Transaction](
        {
          case src :: dst :: amt :: fee :: parent :: salt :: HNil =>
            Transaction(src, dst, amt, fee, parent, salt)
        },
        t => t.source :: t.destination :: t.amount :: t.fee :: t.parent :: t.salt :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[Transaction] = ImmutableCodec.fromScodecCodec(codec)
}
