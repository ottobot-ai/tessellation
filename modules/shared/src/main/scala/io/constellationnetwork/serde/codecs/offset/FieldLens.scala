package io.constellationnetwork.serde.codecs.offset

import scodec.bits.ByteVector
import scodec.{Attempt, Codec, Err}

/** A read-only lens into a scodec-encoded byte slice for a specific field of `T`.
  *
  * Purpose: O(1) byte-offset access to a single field without decoding the whole record. Enables
  * "peek" operations like "read the `stateRoot` from a snapshot blob on disk" without materializing
  * the rest of the struct.
  *
  * Two regimes:
  *   - **Fixed**: both `offset` and `length` are compile-time known. `read` is a direct slice.
  *   - **Partial**: `offset` is known, but `length` depends on preceding Option discriminators or
  *     variable-length prefixes that must be scanned. Constructed via `FieldLens.atDynamic`.
  *
  * All FieldLenses are strictly-typed against the record `T` they project from — a lens for
  * `BlockReference.hash` cannot accidentally be used on a `TransactionReference` byte vector.
  */
sealed trait FieldLens[T, F] {

  /** Byte offset in the encoded-`T` representation where this field begins. */
  def offset: Long

  /** Decode just this field from a full-record byte vector. Fails with a `ScodecFailure` if the
    * input is too short or the field codec rejects the bytes.
    */
  def read(bytes: ByteVector): Attempt[F]
}

object FieldLens {

  /** A fixed-offset, fixed-length field. The byte slice `[offset, offset + length)` is handed to
    * `codec` for decoding. This is the common case for consensus types that are byte-aligned by
    * design.
    */
  final case class Fixed[T, F](
    offset: Long,
    length: Long,
    codec: Codec[F]
  ) extends FieldLens[T, F] {
    require(offset >= 0L, s"FieldLens.offset must be non-negative, got $offset")
    require(length > 0L, s"FieldLens.length must be positive, got $length")

    def read(bytes: ByteVector): Attempt[F] =
      if (bytes.length < offset + length)
        Attempt.failure(
          Err(s"FieldLens.read: bytes too short ($bytes.length < ${offset + length}) for field at offset=$offset length=$length")
        )
      else
        codec.decode(bytes.slice(offset, offset + length).bits).map(_.value)
  }

  /** A dynamic-offset field: a reader function must locate the field by scanning prefixes /
    * discriminators. The `locate` function returns `(offset, length)` — if the field is present.
    * `None` means the field isn't present in the encoded bytes (e.g. an Option field that's `None`).
    */
  final case class Dynamic[T, F](
    codec: Codec[F],
    locate: ByteVector => Option[(Long, Long)]
  ) extends FieldLens[T, F] {

    def offset: Long = -1L // dynamic; use `locate` to find

    def read(bytes: ByteVector): Attempt[F] =
      locate(bytes) match {
        case None =>
          Attempt.failure(Err("FieldLens.read (dynamic): field not present in the encoded bytes"))
        case Some((o, l)) =>
          if (bytes.length < o + l)
            Attempt.failure(
              Err(s"FieldLens.read (dynamic): bytes too short (${bytes.length} < ${o + l}) for field at offset=$o length=$l")
            )
          else
            codec.decode(bytes.slice(o, o + l).bits).map(_.value)
      }

    /** Variant that surfaces absence without throwing a scodec error — useful for lifting the
      * "field is optional and the byte stream says it's absent" case into ordinary Option-typed
      * control flow.
      */
    def readOption(bytes: ByteVector): Attempt[Option[F]] =
      locate(bytes) match {
        case None => Attempt.successful(None)
        case Some((o, l)) =>
          if (bytes.length < o + l)
            Attempt.failure(
              Err(s"FieldLens.readOption: bytes too short (${bytes.length} < ${o + l})")
            )
          else
            codec.decode(bytes.slice(o, o + l).bits).map(r => Some(r.value))
      }
  }

  /** Build a fixed-offset lens.  */
  def fixed[T, F](offset: Long, length: Long, codec: Codec[F]): FieldLens[T, F] =
    Fixed(offset, length, codec)

  /** Build a dynamic-offset lens. */
  def dynamic[T, F](codec: Codec[F])(locate: ByteVector => Option[(Long, Long)]): FieldLens[T, F] =
    Dynamic(codec, locate)
}
