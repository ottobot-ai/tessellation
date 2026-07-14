package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyIdCodec.{codec => currencyIdCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `ConsumedAllowSpend` — the value record of the cross-shard single-use SPENT-SET
  * (`GlobalStateFieldId.ConsumedAllowSpends`, fieldId 33). 8-field record.
  *
  * Wire layout (declared field order):
  *   - allowSpendHash : Hash (variable, length-prefixed)
  *   - source : Address (variable, length-prefixed)
  *   - destination : Address (variable, length-prefixed)
  *   - currencyId : Option[CurrencyId] (1 byte + body if Some)
  *   - amount : SwapAmount (8 bytes)
  *   - lastValidEpochProgress : EpochProgress (8 bytes)
  *   - consumedAtOrdinal : SnapshotOrdinal (8 bytes)
  *   - consumingSpendRef : Hash (variable, length-prefixed)
  *
  * The bytes are deterministic and reproducible from the record on every honest node (the slashing-safety determinism bar), exactly as
  * `AllowSpendCodec` and `InvalidStateProofSlashedReader.entryCodec` are for their partitions. GSAM writes this partition via these bytes
  * (`mpt.insert`) and reads it back via the SAME `immutableCodec`, so producer `postBytes` and any reader/replay agree byte-for-byte.
  *
  * Consensus contract: FROZEN.
  */
object ConsumedAllowSpendCodec {

  private val currencyIdOptCodec: Codec[Option[CurrencyId]] = option(currencyIdCodec)
  private val amountCodec: Codec[SwapAmount] = Codec[SwapAmount]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]

  implicit val codec: Codec[ConsumedAllowSpend] =
    (hashCodec :: addressCodec :: addressCodec :: currencyIdOptCodec ::
      amountCodec :: epochCodec :: ordinalCodec :: hashCodec)
      .xmap[ConsumedAllowSpend](
        {
          case h :: src :: dst :: cid :: amt :: lve :: ord :: spendRef :: HNil =>
            ConsumedAllowSpend(h, src, dst, cid, amt, lve, ord, spendRef)
        },
        c =>
          c.allowSpendHash ::
            c.source ::
            c.destination ::
            c.currencyId ::
            c.amount ::
            c.lastValidEpochProgress ::
            c.consumedAtOrdinal ::
            c.consumingSpendRef ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[ConsumedAllowSpend] = ImmutableCodec.fromScodecCodec(codec)
}
