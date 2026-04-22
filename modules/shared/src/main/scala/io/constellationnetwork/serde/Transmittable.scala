package io.constellationnetwork.serde

import scodec.bits.ByteVector

/** Byte representation for node-to-node wire transmission of `T`.
  *
  * Laws:
  *   - `fromTransmittableBytes(transmittableBytes(a)) == Right(a)` (round-trip).
  *   - Stable within a protocol version. A protocol version bump is a coordinated
  *     cluster upgrade; mid-version changes are forbidden.
  *   - Never compressed at this layer (HTTP/2, gRPC, and the sidecar's libp2p
  *     transport each handle compression independently).
  *
  * Policy note: these bytes are NEVER hashed, NEVER signed. The typeclass boundary
  * enforces that — no implicit conversion exists from `Transmittable[T]` to `Signable[T]`
  * or `ImmutableCodec[T]`. Code that needs to verify a signed wire payload must
  * `decode(wireBytes)` then `Signable[T].signableBytes(decoded)` and verify against that.
  *
  * Wire codec choice (protobuf vs scodec) is per type. For consensus messages that
  * round-trip through node-to-node wire we prefer scodec to keep bit-exactness
  * guarantees; for SDK / external integrations we use protobuf.
  */
trait Transmittable[T] {
  def transmittableBytes(value: T): ByteVector
  def fromTransmittableBytes(bytes: ByteVector): Either[SerdeError, T]
}

object Transmittable {
  def apply[T](implicit ev: Transmittable[T]): Transmittable[T] = ev

  /** Convenience: scodec-based Transmittable. A type can share its `ImmutableCodec`'s
    * scodec codec as its `Transmittable` codec — they're the same bytes, different
    * intent. Do NOT share the same Scala INSTANCE though; separate bindings keep
    * evolution paths independent (Transmittable can change at a protocol bump;
    * ImmutableCodec cannot).
    *
    * We deliberately do not provide an automatic implicit derivation from
    * `ImmutableCodec[T]` — the decision to expose a consensus type on the wire
    * must be explicit, per type.
    */
  def fromScodecCodec[T](codec: scodec.Codec[T]): Transmittable[T] = new Transmittable[T] {
    def transmittableBytes(value: T): ByteVector =
      codec.encode(value) match {
        case scodec.Attempt.Successful(bits) => bits.toByteVector
        case scodec.Attempt.Failure(cause) =>
          throw new IllegalArgumentException(s"Transmittable encode failed: ${cause.messageWithContext}")
      }
    def fromTransmittableBytes(bytes: ByteVector): Either[SerdeError, T] =
      codec
        .decodeValue(bytes.toBitVector)
        .toEither
        .left
        .map(e => SerdeError.ScodecFailure(e.messageWithContext))
  }

  /** Syntax. */
  trait TransmittableSyntax {
    implicit class TransmittableOps[T](private val self: T) {
      def transmittableBytes(implicit ev: Transmittable[T]): ByteVector = ev.transmittableBytes(self)
    }
    implicit class TransmittableBytesOps(private val self: ByteVector) {
      def fromTransmittableBytes[T](implicit ev: Transmittable[T]): Either[SerdeError, T] =
        ev.fromTransmittableBytes(self)
    }
  }
}
