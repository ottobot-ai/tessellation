package io.constellationnetwork.serde.legacy

import cats.syntax.either._

import scala.reflect.ClassTag

import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.serde.SerdeError
import io.constellationnetwork.serde.era.SerdeEra

import scodec.bits.ByteVector

/** Kryo-era bridge. Decodes historical kryo bytes into a modern type `T`.
  *
  * Requires a `KryoSerializer[F]` instance because the existing deserializer is
  * registration-aware (custom classes must be in the kryo registrar). The factory
  * takes `F[_]` only to satisfy the typeclass dependency — the actual decode is
  * synchronous (returns `Either`), matching `LegacyBridgeSerde`'s pure signature.
  *
  * Usage:
  * {{{
  *   implicit val kryo: KryoSerializer[F] = ... // resource-scoped at app start
  *   val bridge: LegacyBridgeSerde[MyType] = KryoBridge.fromKryo[F, MyType]
  * }}}
  */
object KryoBridge {

  def fromKryo[F[_], T: ClassTag](implicit kryo: KryoSerializer[F]): LegacyBridgeSerde[T] =
    new LegacyBridgeSerde[T] {
      val era: SerdeEra = SerdeEra.Kryo

      def fromLegacyBytes(bytes: ByteVector): Either[SerdeError, T] =
        kryo
          .deserialize[T](bytes.toArray)
          .leftMap(t => SerdeError.LegacyDecodeFailure(era.name, Option(t.getMessage).getOrElse(t.toString)))
    }
}
