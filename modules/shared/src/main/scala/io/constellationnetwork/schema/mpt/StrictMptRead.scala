package io.constellationnetwork.schema.mpt

import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector

/** Strict point-read result for consensus state. Unlike `Option`, this preserves the distinction between a key that is absent and a key
  * whose stored bytes exist but cannot be decoded as the requested type.
  */
sealed trait StrictMptRead[+V] extends Product with Serializable

object StrictMptRead {

  case object Absent extends StrictMptRead[Nothing]

  /** Successfully decoded value plus a defensive copy of the exact bytes committed by the MPT. */
  final case class Present[V](value: V, rawBytes: Array[Byte]) extends StrictMptRead[V]

  /** Stored bytes existed but were null, empty, or undecodable. Non-null bytes are retained as a defensive copy for diagnostics. */
  final case class Malformed(reason: String, rawBytes: Option[Array[Byte]]) extends StrictMptRead[Nothing]

  /** Decode one stored value without collapsing malformed storage into absence. Every non-null byte array returned to the caller is copied. */
  def fromStoredBytes[V: ImmutableCodec](bytes: Array[Byte]): StrictMptRead[V] =
    if (bytes eq null) Malformed("null stored bytes", None)
    else {
      val copied = bytes.clone()

      if (copied.isEmpty) Malformed("empty stored bytes", Some(copied))
      else
        ImmutableCodec[V].fromImmutableBytes(ByteVector.view(copied)) match {
          case Right(value) => Present(value, copied)
          case Left(error)  => Malformed(s"undecodable stored bytes: $error", Some(copied))
        }
    }
}
