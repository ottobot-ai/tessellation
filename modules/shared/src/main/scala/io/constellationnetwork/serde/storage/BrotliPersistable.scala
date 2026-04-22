package io.constellationnetwork.serde.storage

import scala.util.control.NonFatal

import io.constellationnetwork.serde.{ImmutableCodec, Persistable, SerdeError}

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.decoder.Decoder.{decompress => brotliDecompress}
import com.aayushatharva.brotli4j.encoder.{BrotliOutputStream, Encoder => BrotliEncoder}
import scodec.bits.ByteVector

/** `Persistable[T]` that wraps an `ImmutableCodec[T]` with brotli compression.
  *
  * The compressed bytes go to disk (MPT leaves, snapshot files, anything that
  * wants storage economy). The UNCOMPRESSED `ImmutableCodec` bytes are what
  * gets hashed — content-addressing never sees compressed bytes. That
  * separation is load-bearing: a brotli version bump or compression-level
  * change MUST NOT change any consensus-visible hash.
  *
  * Compression level is a tuning knob, not a contract. Default 2 (fast, good
  * ratio) matches the existing `JsonBrotliBinarySerializer`. Raising it trades
  * CPU for smaller on-disk footprint.
  *
  * Thread safety: brotli4j encoders/decoders are stateless after construction;
  * the streaming encoder is per-call (new `BrotliOutputStream`). The native
  * library is lazily loaded once via `Brotli4jLoader.ensureAvailability()`.
  */
object BrotliPersistable {

  /** Default compression level. Matches the historical choice for state-channel
    * JSON+brotli in `JsonBrotliBinarySerializer.scala:22`. */
  val DefaultCompressionLevel: Int = 2

  // Ensure the native library loads once per JVM. Safe to call repeatedly.
  locally { Brotli4jLoader.ensureAvailability() }

  /** Wrap an `ImmutableCodec[T]` into a brotli-compressed `Persistable[T]`. */
  def fromImmutableCodec[T](codec: ImmutableCodec[T], level: Int = DefaultCompressionLevel): Persistable[T] =
    new Persistable[T] {
      private val params = new BrotliEncoder.Parameters().setQuality(level)

      def persistedBytes(value: T): ByteVector = {
        val raw = codec.immutableBytes(value)
        val baos = new java.io.ByteArrayOutputStream()
        val out = new BrotliOutputStream(baos, params)
        try out.write(raw.toArray)
        finally out.close()
        ByteVector.view(baos.toByteArray)
      }

      def fromPersistedBytes(bytes: ByteVector): Either[SerdeError, T] = {
        val decompressed: Either[SerdeError, Array[Byte]] =
          try {
            val r = brotliDecompress(bytes.toArray)
            // brotli4j signals failure by returning a DirectDecompress with a non-ok status
            // AND null decompressed data — the getter doesn't throw on bad input. Guard both.
            val result = Option(r).flatMap(x => Option(x.getDecompressedData))
            result.toRight(
              SerdeError.BrotliFailure(
                s"brotli decompress returned no data (status=${Option(r).map(_.getResultStatus).orNull})"
              )
            )
          } catch {
            case NonFatal(e) =>
              Left(SerdeError.BrotliFailure(Option(e.getMessage).getOrElse(e.toString)))
          }
        decompressed.flatMap(arr => codec.fromImmutableBytes(ByteVector.view(arr)))
      }
    }
}
