package io.constellationnetwork.serde.codecs.instances

import java.nio.charset.StandardCharsets

import cats.syntax.either._

import io.constellationnetwork.serde.{ImmutableCodec, SerdeError}

import io.circe.{Json, Printer}
import scodec.bits.ByteVector

/** Compatibility codecs for interop between the scodec-typed `MptStore` and legacy storage paths
  * that carry `Json` / `Array[Byte]` values opaquely.
  *
  * These are INTENTIONALLY not canonical byte-exact encodings of domain types — they are the
  * minimum bridge to keep the type-erased storage callers compiling while typed migration
  * proceeds. Do NOT use these for new consensus-critical code — write a real `ImmutableCodec[T]`
  * for your type instead.
  *
  *   - `ImmutableCodec[Array[Byte]]` — raw byte passthrough. Storing an already-encoded blob.
  *   - `ImmutableCodec[Json]`         — UTF-8 of circe's `noSpaces` printer. Matches the bytes
  *     JsonSerializer produces for JSON values, so existing on-disk JSON blobs inside the MPT stay
  *     readable through this typeclass. Not consensus-canonical: two JSON formatters emit
  *     different bytes.
  */
object CompatCodecs {

  implicit val byteArrayImmutableCodec: ImmutableCodec[Array[Byte]] = new ImmutableCodec[Array[Byte]] {
    def immutableBytes(value: Array[Byte]): ByteVector = ByteVector.view(value)
    def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, Array[Byte]] = Right(bytes.toArray)
  }

  private val compactPrinter: Printer = Printer.noSpaces

  implicit val jsonImmutableCodec: ImmutableCodec[Json] = new ImmutableCodec[Json] {
    def immutableBytes(value: Json): ByteVector =
      ByteVector.view(compactPrinter.print(value).getBytes(StandardCharsets.UTF_8))

    def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, Json] =
      io.circe.parser
        .parse(new String(bytes.toArray, StandardCharsets.UTF_8))
        .leftMap(err => SerdeError.ScodecFailure(s"Json compat decode failed: ${err.message}"))
  }
}
