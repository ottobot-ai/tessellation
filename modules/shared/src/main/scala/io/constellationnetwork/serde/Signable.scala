package io.constellationnetwork.serde

import scodec.bits.ByteVector
import scodec.{Attempt, Encoder}

/** Bytes to feed into a signing primitive for type `T`.
  *
  * Laws:
  *   - Once defined for a consensus type and ordinal era, `signableBytes` MUST NOT change.
  *     Changing it invalidates every historical signature.
  *   - Bit-exact, canonical output — two runs of the same `T` value must produce identical
  *     bytes regardless of JVM, platform default charset, or field-iteration order.
  *   - No compression, ever. The signing pipeline expects raw canonical bytes.
  *
  * Implementations for new types are hand-written scodec codecs under
  * `io.constellationnetwork.serde.scodec.instances`. Reflection-based derivation
  * (magnolia, shapeless, scodec-derivation) is banned for consensus types — a refactor
  * that silently reorders fields would silently change signing bytes.
  *
  * @tparam T
  *   the consensus type whose canonical bytes are needed
  */
trait Signable[T] {
  def signableBytes(value: T): ByteVector
}

object Signable {
  def apply[T](implicit ev: Signable[T]): Signable[T] = ev

  /** Adapter for a type that already has a scodec `Encoder`. Used as the default path
    * from hand-written consensus codecs.
    *
    * Raises `IllegalArgumentException` on encode failure — a scodec failure here means
    * the codec contract is broken (a bug), not a recoverable runtime condition.
    */
  def fromScodecEncoder[T](encoder: Encoder[T]): Signable[T] = new Signable[T] {
    def signableBytes(value: T): ByteVector =
      encoder.encode(value) match {
        case Attempt.Successful(bits) => bits.toByteVector
        case Attempt.Failure(cause) =>
          throw new IllegalArgumentException(s"Signable encode failed: ${cause.messageWithContext}")
      }
  }

  /** Summoner — `Signable.derivedFromScodec[T]` picks up an implicit scodec `Encoder`. */
  def derivedFromScodec[T](implicit encoder: Encoder[T]): Signable[T] = fromScodecEncoder[T](encoder)

  /** Every `ImmutableCodec[T]` induces a `Signable[T]` with the same bytes. Placed on
    * `Signable`'s companion so it's found by implicit search without needing an explicit
    * import chain at each call site. Signing = canonical-encoding-of-immutable — any
    * type with an `ImmutableCodec` gets a `Signable` for free. */
  implicit def fromImmutableCodec[T](implicit ev: ImmutableCodec[T]): Signable[T] = new Signable[T] {
    def signableBytes(value: T): ByteVector = ev.immutableBytes(value)
  }

  /** Syntax: `value.signableBytes` (requires `import io.constellationnetwork.serde.implicits._`). */
  trait SignableSyntax {
    implicit class SignableOps[T](private val self: T) {
      def signableBytes(implicit ev: Signable[T]): ByteVector = ev.signableBytes(self)
    }
  }
}
