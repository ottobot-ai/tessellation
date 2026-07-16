package io.constellationnetwork.serde

import scala.util.control.NoStackTrace

/** Typed errors for serde operations. Sealed ADT — never matched by string.
  *
  * All three typeclasses (`ImmutableCodec`, `Persistable`, `Transmittable`) return `Either[SerdeError, A]` on decode / `F[SerdeError \/ A]`
  * on effectful decode. Encode paths raise on failure by construction (scodec `Attempt.Failure` at encode time means the codec contract was
  * violated — a bug, not a recoverable error).
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
}
