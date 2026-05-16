package io.constellationnetwork.security.kes

/** Stateful interpreter wrapper around [[KesProduct]] that manages the read-once key lifecycle.
  *
  * Caller-facing surface:
  *
  *   - [[currentPublicKey]] — read the verification key at the currently active period.
  *   - [[currentPeriod]] — read the currently active period (where "active" means "the next evolution from now should
  *     target a strictly greater period").
  *   - [[signAt]] — sign a message at the requested period; evolves the in-memory key first, persists the evolved
  *     state, then signs.
  *   - [[evolveTo]] — explicitly evolve to `period` without signing. The bytes corresponding to periods `< period` are
  *     destroyed.
  *
  * '''Period alignment''': per the consensus-epoch-staggering design notes, KES periods are aligned with eta rotation
  * cadence. This algebra takes an `etaPeriodLength` config parameter so callers can express that alignment at
  * construction; the alignment itself (slot -> period mapping) is the caller's responsibility and lives at the
  * SnapshotLeaderLoop / EtaRotation boundary, not here.
  *
  * '''Forward security''': all evolution operations are sequenced through a [[cats.effect.std.Semaphore]] (one
  * permit). Once `signAt(p, _)` has been observed, the underlying secret-key bytes for any period `<= p` are no longer
  * present in memory or in the secure store: they have been overwritten by the read-once consume + KES evolve sequence.
  */
trait OperationalKeyMakerAlgebra[F[_]] {

  /** The verification key associated with the secret key at its current step. */
  def currentPublicKey: F[VerificationKeyKesProduct]

  /** The KES product step currently held by the in-memory key (offset from the configured `activationPeriod`). */
  def currentPeriod: F[Int]

  /** Sign `message` at `period`. If the in-memory key is already at `period`, signs directly; otherwise evolves first
    * (destroying the bytes for periods `< period`) and persists.
    *
    *   - Returns `Left(StepNotMonotonic)` if `period` is in the past.
    *   - Returns `Left(StepBeyondMax)` if `period` exceeds the maximum step expressible by the configured tree height.
    */
  def signAt(period: Int, message: Array[Byte]): F[Either[KesError, SignatureKesProduct]]

  /** Evolve to `period` and persist. No signature is produced. Useful for proactively burning the past on a
    * fast-forward boundary (e.g. just after an eta rotation).
    */
  def evolveTo(period: Int): F[Either[KesError, Unit]]
}
