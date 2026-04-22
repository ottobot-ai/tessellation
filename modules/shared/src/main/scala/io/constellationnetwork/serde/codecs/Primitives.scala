package io.constellationnetwork.serde.codecs

import eu.timepit.refined.types.numeric._
import scodec.codecs.{int32, int64}
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codecs for the refined primitives that appear in consensus types.
  *
  * Codec policy — fixed-width big-endian integers:
  *   - `Long` → 8 bytes, `Int` → 4 bytes. Every value of a given type has the same wire size. Big-endian network byte order.
  *   - Chosen over protobuf varint (VLQ) because the MPT-as-primary workstream wants byte-offset random access into stored records; VLQ
  *     requires sequential parsing to locate the Nth field. The disk-size argument for VLQ is also weaker than it looks once brotli
  *     compresses the Persistable layer — fixed-width's redundant zero bytes brotli-compress extremely well.
  *   - Decoding validates the refinement — a byte stream claiming to be a `NonNegLong` but holding a negative value produces
  *     `Attempt.Failure`, not a silently-broken value.
  *   - No derivation. Each primitive codec is hand-written against the underlying scodec codec for its base type. Refactoring the type
  *     cannot silently change the wire bytes.
  *
  * Consensus contract: these codecs are FROZEN. A `NonNegLong` always serializes as an 8-byte big-endian non-negative int64. Changing the
  * encoding breaks every historical signature and hash under the scodec era.
  *
  * If we ever decide the compactness/wire-compat trade justifies VLQ on a specific type (e.g. a seq length in a highly-repeated record),
  * add a dedicated `varLong` codec alongside — don't overload these.
  */
object Primitives {

  /** 8 bytes, big-endian. Decode rejects negative values. */
  implicit val nonNegLongCodec: Codec[NonNegLong] =
    int64.exmap(
      l => NonNegLong.from(l).fold(err => Attempt.failure(Err(s"NonNegLong decode: $err")), Attempt.successful),
      (n: NonNegLong) => Attempt.successful(n.value)
    )

  /** 8 bytes, big-endian. Decode rejects non-positive values. */
  implicit val posLongCodec: Codec[PosLong] =
    int64.exmap(
      l => PosLong.from(l).fold(err => Attempt.failure(Err(s"PosLong decode: $err")), Attempt.successful),
      (n: PosLong) => Attempt.successful(n.value)
    )

  /** 4 bytes, big-endian. Decode rejects negative values. */
  implicit val nonNegIntCodec: Codec[NonNegInt] =
    int32.exmap(
      i => NonNegInt.from(i).fold(err => Attempt.failure(Err(s"NonNegInt decode: $err")), Attempt.successful),
      (n: NonNegInt) => Attempt.successful(n.value)
    )

  /** 4 bytes, big-endian. Decode rejects non-positive values. */
  implicit val posIntCodec: Codec[PosInt] =
    int32.exmap(
      i => PosInt.from(i).fold(err => Attempt.failure(Err(s"PosInt decode: $err")), Attempt.successful),
      (n: PosInt) => Attempt.successful(n.value)
    )
}
