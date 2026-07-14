package io.constellationnetwork.schema.mpt

import cats.MonadThrow
import cats.syntax.all._

import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector

/** Strict point-read result for consensus state. Unlike `Option`, this preserves the distinction between a key that is absent and a key
  * whose stored bytes exist but cannot be decoded as the requested type.
  */
sealed trait StrictMptRead[+V] extends Product with Serializable

object StrictMptRead {

  sealed abstract class ConsensusMptReadFailure(message: String) extends IllegalStateException(message) {
    def context: String
    def physicalKey: Hex
  }

  /** A consensus-state key existed, but its committed bytes could not be decoded as the value type required by the transition. Treating
    * this as absence would let a read-modify-write replace authenticated state with an identity value. Callers must fail/defer instead.
    */
  final case class MalformedConsensusMptValue(context: String, physicalKey: Hex, reason: String)
      extends ConsensusMptReadFailure(s"Malformed consensus MPT value at $context, key=${physicalKey.value}: $reason")

  /** A rooted consensus index referenced a physical target key that was absent from the same authenticated state view. */
  final case class MissingConsensusMptValue(context: String, physicalKey: Hex)
      extends ConsensusMptReadFailure(s"Missing consensus MPT value at $context, key=${physicalKey.value}")

  /** A rooted consensus index and its decoded target disagree (for example, an expiry bucket references a hash absent from the active
    * record set stored at `physicalKey`).
    */
  final case class InconsistentConsensusMptIndex(context: String, physicalKey: Hex, reason: String)
      extends ConsensusMptReadFailure(s"Inconsistent consensus MPT index at $context, key=${physicalKey.value}: $reason")

  case object Absent extends StrictMptRead[Nothing]

  /** Successfully decoded value plus a defensive copy of the exact bytes committed by the MPT. */
  final case class Present[V](value: V, rawBytes: Array[Byte]) extends StrictMptRead[V]

  /** Stored bytes existed but were null, empty, or undecodable. Non-null bytes are retained as a defensive copy for diagnostics. */
  final case class Malformed(reason: String, rawBytes: Option[Array[Byte]]) extends StrictMptRead[Nothing]

  /** Decode one stored value without collapsing malformed storage into absence. Every non-null byte array returned to the caller is copied.
    */
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

  /** Decode an optional raw entry without collapsing present-but-malformed bytes into absence. */
  def fromOptionalStoredBytes[V: ImmutableCodec](bytes: Option[Array[Byte]]): StrictMptRead[V] =
    bytes.fold[StrictMptRead[V]](Absent)(fromStoredBytes[V])

  /** Resolve an authenticated point read for a read-modify-write. Only true absence selects `ifAbsent`; malformed committed bytes are a
    * hard error so no mutation can be derived from a synthetic empty prior.
    */
  def valueOrElse[V](
    read: StrictMptRead[V],
    ifAbsent: => V,
    context: => String,
    physicalKey: Hex
  ): Either[MalformedConsensusMptValue, V] =
    read match {
      case Present(value, _)    => Right(value)
      case Absent               => Right(ifAbsent)
      case Malformed(reason, _) => Left(MalformedConsensusMptValue(context, physicalKey, reason))
    }

  def valueOrElseF[F[_]: MonadThrow, V](
    read: F[StrictMptRead[V]],
    ifAbsent: => V,
    context: => String,
    physicalKey: Hex
  ): F[V] =
    read.flatMap(valueOrElse(_, ifAbsent, context, physicalKey).liftTo[F])

  /** Convert a strict read to an optional value while preserving malformed bytes as an error. */
  def toOption[V](
    read: StrictMptRead[V],
    context: => String,
    physicalKey: Hex
  ): Either[MalformedConsensusMptValue, Option[V]] =
    read match {
      case Present(value, _)    => Right(Some(value))
      case Absent               => Right(None)
      case Malformed(reason, _) => Left(MalformedConsensusMptValue(context, physicalKey, reason))
    }

  def toOptionF[F[_]: MonadThrow, V](
    read: F[StrictMptRead[V]],
    context: => String,
    physicalKey: Hex
  ): F[Option[V]] =
    read.flatMap(toOption(_, context, physicalKey).liftTo[F])

  /** Resolve a target named by authenticated consensus state. Both malformed bytes and physical absence are errors; only a successfully
    * decoded present value may be consumed.
    */
  def requirePresent[V](
    read: StrictMptRead[V],
    context: => String,
    physicalKey: Hex
  ): Either[ConsensusMptReadFailure, V] =
    read match {
      case Present(value, _)    => Right(value)
      case Absent               => Left(MissingConsensusMptValue(context, physicalKey))
      case Malformed(reason, _) => Left(MalformedConsensusMptValue(context, physicalKey, reason))
    }

  def requirePresentF[F[_]: MonadThrow, V](
    read: F[StrictMptRead[V]],
    context: => String,
    physicalKey: Hex
  ): F[V] =
    read.flatMap(requirePresent(_, context, physicalKey).liftTo[F])
}
