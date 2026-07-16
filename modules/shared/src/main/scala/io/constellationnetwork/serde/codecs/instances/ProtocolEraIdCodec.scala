package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.era.ProtocolEraId
import io.constellationnetwork.schema.era.ProtocolEraId.ScodecV1
import io.constellationnetwork.serde.ImmutableCodec

import scodec.Codec
import scodec.codecs.{discriminated, provide, uint8}

/** Canonical greenfield codec for [[ProtocolEraId]].
  *
  * `0x01` is `ScodecV1`. No legacy or speculative future-era decoder is present.
  */
object ProtocolEraIdCodec {
  implicit val codec: Codec[ProtocolEraId] =
    discriminated[ProtocolEraId]
      .by(uint8)
      .typecase(1, provide(ScodecV1))

  implicit val immutableCodec: ImmutableCodec[ProtocolEraId] =
    ImmutableCodec.fromScodecCodec(codec)
}
