package io.constellationnetwork.json

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, IOException, OutputStream}
import java.nio.charset.StandardCharsets

import cats.effect.Async
import cats.syntax.all._

import scala.util.Using

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.decoder.BrotliInputStream
import com.aayushatharva.brotli4j.decoder.Decoder.{decompress => brotliDecompress}
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream
import com.aayushatharva.brotli4j.encoder.Encoder.Parameters
import io.circe.jawn.JawnParser
import io.circe.{Decoder, Encoder, Printer}

trait JsonBrotliBinarySerializer[F[_]] {
  def serialize[A: Encoder](content: A): F[Array[Byte]]
  def deserialize[A: Decoder](content: Array[Byte]): F[Either[Throwable, A]]
  def deserializeBounded[A: Decoder](
    content: Array[Byte],
    limits: JsonBrotliBinarySerializer.BrotliDecodeLimits
  ): F[Either[Throwable, A]]
}

object JsonBrotliBinarySerializer {
  final case class BrotliDecodeLimits(maxCompressedBytes: Long, maxDecompressedBytes: Long)

  final case class CompressedContentTooLarge(maximum: Long, actual: Long)
      extends IOException(s"Compressed content exceeds limit: actual=$actual maximum=$maximum")

  final case class DecompressedContentTooLarge(maximum: Long, actualAtLeast: Long)
      extends IOException(s"Decompressed content exceeds limit: actualAtLeast=$actualAtLeast maximum=$maximum")

  private val compressionLevel = 2
  private val parser = JawnParser(allowDuplicateKeys = false)
  private val UTF8 = StandardCharsets.UTF_8
  private val DecompressionBufferSize = 8192

  private class OutputStreamAppendable(os: OutputStream) extends Appendable {
    def append(csq: CharSequence): Appendable = { os.write(csq.toString.getBytes(UTF8)); this }
    def append(csq: CharSequence, start: Int, end: Int): Appendable = {
      os.write(csq.subSequence(start, end).toString.getBytes(UTF8)); this
    }
    def append(c: Char): Appendable = {
      if (c < 128) os.write(c.toInt) else os.write(c.toString.getBytes(UTF8))
      this
    }
  }

  private def streamPrintAndCompress[A](content: A, printer: Printer, params: Parameters)(implicit enc: Encoder[A]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val brotli = new BrotliOutputStream(baos, params)
    val appendable = new OutputStreamAppendable(brotli)

    try
      enc match {
        case sce: StreamingCollectionEncoder[A] @unchecked =>
          sce.streamEncode(content, printer, appendable)
        case _ =>
          printer.unsafePrintToAppendable(enc(content), appendable)
      }
    finally
      brotli.close()

    baos.toByteArray
  }

  private def validateLimit(name: String, value: Long): Unit =
    if (value <= 0L || value > Int.MaxValue.toLong)
      throw new IllegalArgumentException(s"$name must be between 1 and ${Int.MaxValue}: $value")

  private def decompressBounded(content: Array[Byte], maxCompressedBytes: Long, maxDecompressedBytes: Long): Array[Byte] = {
    validateLimit("maxCompressedBytes", maxCompressedBytes)
    validateLimit("maxDecompressedBytes", maxDecompressedBytes)

    if (content.length.toLong > maxCompressedBytes)
      throw CompressedContentTooLarge(maxCompressedBytes, content.length.toLong)

    Using.resource(new BrotliInputStream(new ByteArrayInputStream(content))) { input =>
      val output = new ByteArrayOutputStream(math.min(DecompressionBufferSize.toLong, maxDecompressedBytes).toInt)
      val buffer = new Array[Byte](DecompressionBufferSize)
      var total = 0L
      var complete = false

      while (!complete) {
        val remainingWithSentinel = maxDecompressedBytes - total + 1L
        val requested = math.min(buffer.length.toLong, remainingWithSentinel).toInt
        val read = input.read(buffer, 0, requested)

        if (read < 0) complete = true
        else {
          total += read.toLong
          if (total > maxDecompressedBytes)
            throw DecompressedContentTooLarge(maxDecompressedBytes, total)
          output.write(buffer, 0, read)
        }
      }

      output.toByteArray
    }
  }

  private def decode[A: Decoder](decompressed: Array[Byte]): Either[Throwable, A] =
    parser
      .parseByteBuffer(java.nio.ByteBuffer.wrap(decompressed))
      .flatMap[Throwable, A](_.as[A])

  def apply[F[_]: JsonBrotliBinarySerializer]: JsonBrotliBinarySerializer[F] = implicitly

  def forAsync[F[_]: Async](printer: Printer): F[JsonBrotliBinarySerializer[F]] =
    Async[F].delay(Brotli4jLoader.ensureAvailability()).map { _ =>
      new JsonBrotliBinarySerializer[F] {
        private val params = new Parameters().setQuality(compressionLevel)

        def serialize[A: Encoder](content: A): F[Array[Byte]] =
          Async[F].blocking {
            streamPrintAndCompress(content, printer, params)
          } <* Async[F].cede

        def deserialize[A: Decoder](content: Array[Byte]): F[Either[Throwable, A]] =
          Async[F].blocking {
            Either.catchNonFatal {
              val result = brotliDecompress(content)
              val decompressed = Option(result)
                .flatMap(value => Option(value.getDecompressedData))
                .getOrElse {
                  val status = Option(result).map(_.getResultStatus.toString).getOrElse("unavailable")
                  throw new IOException(s"Brotli decompression returned no data (status=$status)")
                }

              decode[A](decompressed)
            }.flatten
          } <* Async[F].cede

        def deserializeBounded[A: Decoder](content: Array[Byte], limits: BrotliDecodeLimits): F[Either[Throwable, A]] =
          Async[F].blocking {
            Either.catchNonFatal {
              decode[A](decompressBounded(content, limits.maxCompressedBytes, limits.maxDecompressedBytes))
            }.flatten
          } <* Async[F].cede
      }
    }
}
