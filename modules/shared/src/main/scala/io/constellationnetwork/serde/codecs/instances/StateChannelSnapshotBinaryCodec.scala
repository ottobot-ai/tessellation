package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ByteArrayCodec.{codec => byteArrayCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `StateChannelSnapshotBinary` — `(lastSnapshotHash, content, fee)`.
  *
  * Wire layout:
  *   - lastSnapshotHash : Hash         (32 bytes)
  *   - content          : Array[Byte]  (uint32 length + raw bytes)
  *   - fee              : SnapshotFee  (8 bytes NonNegLong)
  */
object StateChannelSnapshotBinaryCodec {

  private val feeCodec: Codec[SnapshotFee] = Codec[SnapshotFee]

  implicit val codec: Codec[StateChannelSnapshotBinary] =
    (hashCodec :: byteArrayCodec :: feeCodec)
      .xmap[StateChannelSnapshotBinary](
        { case lsh :: content :: fee :: HNil => StateChannelSnapshotBinary(lsh, content, fee) },
        s => s.lastSnapshotHash :: s.content :: s.fee :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[StateChannelSnapshotBinary] = ImmutableCodec.fromScodecCodec(codec)
}
