package io.constellationnetwork.serde.codecs

import scodec.Codec
import scodec.codecs.{bool, optional}

/** Generic scodec codec factory for `Option[A]`.
  *
  * Wire format: 1-byte present/absent flag (`0x01`/`0x00`) + optional inner encoding.
  *   - `Some(a)` → `01 ++ encode(a)`
  *   - `None` → `00`
  *
  * The 1-byte discriminator is fixed-width and chosen over a raw bool (which `scodec.codecs.bool` defaults to 1 bit) because consensus
  * types want byte-aligned encodings for byte-offset random access into storage records.
  *
  * Not marked implicit — we register `Option[T]` codecs explicitly per call site, same as `NonEmptyListCodec.nonEmptyList`. That keeps
  * implicit-resolution surprises out of the hot path.
  *
  * Consensus contract: FROZEN. 1-byte discriminator + body. Any change breaks every historical signature that covers an `Option` field.
  */
object OptionCodec {

  def option[A](inner: Codec[A]): Codec[Option[A]] =
    optional(bool(8), inner)
}
