package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.era.ProtocolEraId
import io.constellationnetwork.schema.era.ProtocolEraId.ScodecV1
import io.constellationnetwork.serde.ImmutableCodec

import scodec.codecs.uint8
import scodec.{Attempt, Codec, Err}

/** Canonical greenfield codec for [[ProtocolEraId]].
  *
  * `0x01` is `ScodecV1`. No legacy or speculative future-era decoder is present.
  */
object ProtocolEraIdCodec {
  implicit val codec: Codec[ProtocolEraId] =
    uint8.exmap(
      {
        case 1   => Attempt.successful(ScodecV1)
        case tag => Attempt.failure(Err(s"ProtocolEraId decode: unknown tag $tag"))
      },
      value =>
        if (ProtocolEraId.isRegistered(value)) Attempt.successful(1)
        else Attempt.failure(Err("ProtocolEraId encode: value is not the canonical registered ScodecV1 singleton"))
    )

  implicit val immutableCodec: ImmutableCodec[ProtocolEraId] =
    ImmutableCodec.fromScodecCodec(codec)
}
