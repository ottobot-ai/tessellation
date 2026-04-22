package io.constellationnetwork.serde.codecs

import java.nio.charset.StandardCharsets

import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector
import scodec.codecs.{uint16, variableSizeBytes}
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codec for `String`.
  *
  * Wire format: 2-byte UTF-8 byte-length prefix (uint16) + UTF-8 bytes.
  *
  * Byte-length (not code-point-length) prefix so the decoder can size the payload exactly. Caps a string at 65,535 UTF-8 bytes — ample for
  * node-metadata names, descriptions, labels. If a consensus string ever needs >64KB we'll add a dedicated `longString` alongside.
  *
  * Consensus contract: FROZEN. Any change to the prefix width or charset breaks every historical hash that covers a string field.
  */
object StringCodec {

  implicit val codec: Codec[String] =
    variableSizeBytes(uint16, scodec.codecs.bytes).exmap(
      (bv: ByteVector) => Attempt.successful(new String(bv.toArray, StandardCharsets.UTF_8)),
      (s: String) => {
        val bytes = s.getBytes(StandardCharsets.UTF_8)
        if (bytes.length > 0xffff)
          Attempt.failure(Err(s"String encode: UTF-8 length ${bytes.length} exceeds uint16 max"))
        else
          Attempt.successful(ByteVector.view(bytes))
      }
    )

  implicit val immutableCodec: ImmutableCodec[String] = ImmutableCodec.fromScodecCodec(codec)
}
