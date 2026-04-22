package io.constellationnetwork.serde

import scala.util.control.NoStackTrace

/** Typed errors for serde operations. Sealed ADT — never matched by string.
  *
  * All four typeclasses (`Signable`, `ImmutableCodec`, `Persistable`, `Transmittable`)
  * return `Either[SerdeError, A]` on decode / `F[SerdeError \/ A]` on effectful decode.
  * Encode paths raise on failure by construction (scodec `Attempt.Failure` at encode
  * time means the codec contract was violated — a bug, not a recoverable error).
  */
sealed trait SerdeError extends NoStackTrace {
  def message: String
  override def getMessage: String = message
}

object SerdeError {

  /** scodec decode reported a structural problem with the bytes. */
  final case class ScodecFailure(message: String) extends SerdeError

  /** Brotli decompression failed — corrupt or non-brotli input. */
  final case class BrotliFailure(message: String) extends SerdeError

  /** Legacy bridge (JSON / Kryo) decode reported a structural problem. */
  final case class LegacyDecodeFailure(eraName: String, cause: String) extends SerdeError {
    def message: String = s"[$eraName] $cause"
  }

  /** No era codec is registered for the requested `SnapshotOrdinal`. Indicates a config
    * gap, not a data problem — every ordinal must fall inside some era's half-open range.
    */
  final case class NoEraCodec(ordinalValue: Long, typeName: String) extends SerdeError {
    def message: String =
      s"No codec era registered for type '$typeName' at ordinal $ordinalValue — check hash-eras config."
  }

  /** A value was successfully decoded under a legacy era but the consumer asked for
    * write access (Signable / Persistable encode). Legacy bridges are decode-only. */
  final case class LegacyWriteAttempt(eraName: String) extends SerdeError {
    def message: String =
      s"Attempted to encode via legacy era '$eraName'; legacy bridges are decode-only."
  }
}
