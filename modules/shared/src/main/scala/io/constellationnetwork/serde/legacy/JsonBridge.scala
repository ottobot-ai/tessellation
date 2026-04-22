package io.constellationnetwork.serde.legacy

import cats.syntax.either._

import scala.util.control.NonFatal

import io.constellationnetwork.serde.SerdeError
import io.constellationnetwork.serde.era.SerdeEra

import io.circe.Decoder
import io.circe.parser.parse
import scodec.bits.ByteVector

/** JSON-era bridge. Decodes circe JSON bytes into a modern type `T` that has a
  * `circe.Decoder` in scope.
  *
  * This is intentionally thin: bytes → UTF-8 string → circe parse → `Decoder[T]`.
  * The original read path's behaviour is preserved (including lenient handling of
  * unknown fields, which circe does by default). If a migration needs strict
  * decoding — reject unknown fields — build a per-type bridge that uses a
  * stricter Decoder.
  */
object JsonBridge {

  def fromCirceDecoder[T](implicit dec: Decoder[T]): LegacyBridgeSerde[T] = new LegacyBridgeSerde[T] {
    val era: SerdeEra = SerdeEra.Json

    def fromLegacyBytes(bytes: ByteVector): Either[SerdeError, T] = {
      val attempt: Either[Throwable, T] =
        try parse(new String(bytes.toArray, java.nio.charset.StandardCharsets.UTF_8)).flatMap(_.as[T])
        catch { case NonFatal(e) => Left(e) }

      attempt.leftMap(t => SerdeError.LegacyDecodeFailure(era.name, Option(t.getMessage).getOrElse(t.toString)))
    }
  }
}
