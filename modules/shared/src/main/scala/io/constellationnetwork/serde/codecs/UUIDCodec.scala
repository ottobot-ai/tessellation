package io.constellationnetwork.serde.codecs

import java.util.UUID

import scodec.Codec
import scodec.codecs.int64
import shapeless.{::, HNil}

/** Canonical scodec codec for `java.util.UUID`.
  *
  * Wire format: 16 bytes = 8-byte big-endian mostSignificantBits + 8-byte big-endian leastSignificantBits.
  * Fixed-width, random-access friendly.
  *
  * Consensus contract: FROZEN.
  */
object UUIDCodec {

  implicit val codec: Codec[UUID] =
    (int64 :: int64)
      .xmap[UUID](
        { case msb :: lsb :: HNil => new UUID(msb, lsb) },
        (u: UUID) => u.getMostSignificantBits :: u.getLeastSignificantBits :: HNil
      )
}
