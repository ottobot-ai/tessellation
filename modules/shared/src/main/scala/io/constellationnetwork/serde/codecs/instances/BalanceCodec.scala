package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.Primitives._

import scodec.Codec

/** Canonical scodec codec for `Balance`, the first consensus-type pilot for the
  * scodec era.
  *
  * `Balance` is a newtype wrapper around `NonNegLong`, so its canonical bytes are
  * exactly the wrapped 8-byte big-endian non-negative int64. This establishes the
  * pattern for all newtype-around-refined consensus types (Amount, SnapshotOrdinal,
  * EpochProgress, …): `xmap` through the constructor/accessor of the newtype.
  *
  * Golden vector: `test/resources/golden/serde/Balance-scodec-v1.hex`.
  *
  * Consensus contract: FROZEN. Byte layout is 8 bytes big-endian non-negative long.
  * Changing this breaks every balance-hash that references a `Balance` in the MPT
  * or in a `GlobalSnapshotInfo` state proof.
  */
object BalanceCodec {

  implicit val codec: Codec[Balance] =
    nonNegLongCodec.xmap[Balance](Balance(_), _.value)

  implicit val immutableCodec: ImmutableCodec[Balance] = ImmutableCodec.fromScodecCodec(codec)
}
