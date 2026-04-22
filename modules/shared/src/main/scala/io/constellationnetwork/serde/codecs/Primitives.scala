package io.constellationnetwork.serde.codecs

import eu.timepit.refined.types.numeric._
import scodec.codecs.{int32, int64}
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codecs for the refined primitives that appear in consensus types.
  *
  * Codec policy:
  *   - Big-endian, fixed-width integers. `Long` → 8 bytes, `Int` → 4 bytes. Never
  *     variable-length here; variable-length is reserved for application types where
  *     size varies meaningfully (strings, seqs).
  *   - Decoding validates the refinement — a byte stream claiming to be a `NonNegLong`
  *     but holding a negative value produces `Attempt.Failure`, not a silently-broken
  *     value. Chain bytes are trusted at REST; they're still validated at DECODE.
  *   - No derivation. Each primitive codec is hand-written against the underlying
  *     scodec codec for its base type.
  *
  * Consensus contract: these codecs are FROZEN. A `NonNegLong` always serializes as
  * big-endian int64. Changing the byte width, endianness, or refinement semantics
  * breaks every historical signature and hash.
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
