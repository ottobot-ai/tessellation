package io.constellationnetwork.serde

import cats.syntax.either._

import scodec._
import scodec.bits.ByteVector

/** Canonical, round-trippable byte representation of `T` — the "immutable" encoding.
  *
  * These bytes serve two purposes interchangeably:
  *   1. They are the bytes fed into a signing primitive. `sign(ImmutableCodec[T].immutableBytes(t), key)` is the single path for producing
  *      a consensus signature. 2. They are the bytes hashed for content addressing.
  *      `Hash.fromBytes(ImmutableCodec[T].immutableBytes(t).toArray)` is the single path for producing a consensus hash.
  *
  * No separate `Signable` typeclass exists — there is no case in the codebase where we want different bytes for signing vs. hashing, and
  * merging the concepts keeps the mental model small. If a future type legitimately needs divergent signing bytes, introduce `Signable[T]`
  * at that point, not prophylactically.
  *
  * Laws:
  *   - Encoding is bit-exact and platform-independent.
  *   - `decode(encode(a)) == Right(a)` for all `a: T`.
  *   - `encode(decode(encode(a)).right.get) == encode(a)` (canonicality).
  *   - Once defined for a consensus type and era, the encoding MUST NOT change. New versions get a new type (e.g. `BlockV2`), or a new
  *     discriminator on a `discriminated` codec — never an in-place mutation.
  */
trait ImmutableCodec[T] {
  def immutableBytes(value: T): ByteVector
  def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, T]
}

object ImmutableCodec {
  def apply[T](implicit ev: ImmutableCodec[T]): ImmutableCodec[T] = ev

  /** Build from a scodec `Codec`. The scodec codec is the single source of truth for both encode and decode — same bits in both directions.
    */
  def fromScodecCodec[T](codec: Codec[T]): ImmutableCodec[T] = new ImmutableCodec[T] {
    def immutableBytes(value: T): ByteVector =
      codec.encode(value) match {
        case Attempt.Successful(bits) => bits.toByteVector
        case Attempt.Failure(cause) =>
          throw new IllegalArgumentException(s"ImmutableCodec encode failed: ${cause.messageWithContext}")
      }

    def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, T] =
      codec.complete
        .decodeValue(bytes.toBitVector)
        .toEither
        .leftMap(e => SerdeError.ScodecFailure(e.messageWithContext))
  }

  /** Summoner — picks up an implicit scodec `Codec` and builds the typeclass instance. */
  def derivedFromScodec[T](implicit codec: Codec[T]): ImmutableCodec[T] = fromScodecCodec[T](codec)

  /** Build asymmetric instance from separate encoder/decoder when you're bridging two existing scodec pieces (uncommon — prefer
    * `fromScodecCodec`).
    */
  def fromScodec[T](enc: Encoder[T], dec: Decoder[T]): ImmutableCodec[T] = new ImmutableCodec[T] {
    def immutableBytes(value: T): ByteVector =
      enc.encode(value) match {
        case Attempt.Successful(bits) => bits.toByteVector
        case Attempt.Failure(cause) =>
          throw new IllegalArgumentException(s"ImmutableCodec encode failed: ${cause.messageWithContext}")
      }
    def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, T] =
      dec.complete
        .decodeValue(bytes.toBitVector)
        .toEither
        .leftMap(e => SerdeError.ScodecFailure(e.messageWithContext))
  }

  /** Syntax: `value.immutableBytes` / `bytes.fromImmutableBytes[T]`. */
  trait ImmutableCodecSyntax {
    implicit class ImmutableCodecOps[T](private val self: T) {
      def immutableBytes(implicit ev: ImmutableCodec[T]): ByteVector = ev.immutableBytes(self)
    }
    implicit class ImmutableCodecBytesOps(private val self: ByteVector) {
      def fromImmutableBytes[T](implicit ev: ImmutableCodec[T]): Either[SerdeError, T] =
        ev.fromImmutableBytes(self)
    }
  }
}
