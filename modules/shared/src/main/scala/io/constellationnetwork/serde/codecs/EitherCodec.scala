package io.constellationnetwork.serde.codecs

import scodec.Codec
import scodec.codecs.{discriminated, uint8}

/** Generic scodec codec factory for `Either[L, R]`.
  *
  * Wire format: 1-byte discriminator (`0x00` for Left, `0x01` for Right) + body.
  *
  * Not marked implicit — call sites invoke `either(leftCodec, rightCodec)` explicitly.
  *
  * Consensus contract: FROZEN. Left before Right in the discriminator order (0x00 / 0x01). Any change breaks every hash that covers an
  * `Either` field.
  */
object EitherCodec {

  def either[L, R](leftCodec: Codec[L], rightCodec: Codec[R]): Codec[Either[L, R]] =
    discriminated[Either[L, R]]
      .by(uint8)
      .typecase(0, leftCodec.xmap[Left[L, R]](Left(_), _.value))
      .typecase(1, rightCodec.xmap[Right[L, R]](Right(_), _.value))
}
