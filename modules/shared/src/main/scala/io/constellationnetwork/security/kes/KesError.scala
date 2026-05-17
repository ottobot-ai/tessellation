package io.constellationnetwork.security.kes

/** Errors raised by the KES crypto layer.
  *
  * Sealed so callers can pattern-match on the variant rather than string-matching error messages.
  */
sealed trait KesError extends Product with Serializable {
  def message: String
}

object KesError {

  /** Requested time step is past the maximum step expressible by the configured tree height. */
  final case class StepBeyondMax(currentStep: Int, requestedStep: Int, maxSteps: Int) extends KesError {
    val message: String = s"Step $requestedStep beyond max $maxSteps (current $currentStep)"
  }

  /** Requested time step is not strictly greater than the current step (KES keys evolve monotonically). */
  final case class StepNotMonotonic(currentStep: Int, requestedStep: Int) extends KesError {
    val message: String = s"Step $requestedStep is not greater than current step $currentStep"
  }

  /** Internal invariant violated while evolving a key. Should never happen for a well-formed `KesBinaryTree`. */
  final case class MalformedTree(reason: String) extends KesError {
    val message: String = s"Malformed key tree: $reason"
  }

  /** Secret-key bytes have already been consumed by the read-once store and cannot be replayed. */
  final case class KeyAlreadyConsumed(name: String) extends KesError {
    val message: String = s"Key '$name' has already been consumed"
  }

  /** The expected single key file is either missing or accompanied by other entries. */
  final case class StoreInvariantViolated(detail: String) extends KesError {
    val message: String = s"SecureStore invariant violated: $detail"
  }
}
