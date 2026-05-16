package io.constellationnetwork.security

/** Key-Evolving Signatures (KES) — forward-secure signatures.
  *
  * This package implements the MMM (Malkin–Micciancio–Miner, EUROCRYPT 2002) construction of forward-secure signatures
  * over Ed25519 + Blake2b-256.
  *
  * Forward security: an attacker who compromises the signing key at period `t` cannot forge signatures for any period
  * `t' < t`. This is achieved by evolving the secret key after each period; the bytes corresponding to past periods are
  * destroyed and cannot be reconstructed from the evolved state.
  *
  * == Public surface ==
  *
  * The CONSENSUS-FACING public API is the '''product+sum''' composition: [[kes.KesProduct]]. It composes two sum-based
  * trees (super and sub) and yields up to `2^(heightSup + heightSub)` periods.
  *
  * The plain sum composition ([[kes.KesSum]]) is exposed as a building block for completeness and tests; consumers that
  * need forward security at the consensus layer must use [[kes.KesProduct]].
  *
  * == Period alignment ==
  *
  * Per the consensus-epoch-staggering design, KES periods are aligned with eta rotation cadence. The
  * [[kes.OperationalKeyMaker]] takes an `etaPeriodLength` configuration parameter so callers can express that alignment
  * at construction; the alignment itself is the caller's responsibility (this package does not wire eta rotation).
  *
  * == Read-once secret store ==
  *
  * Per Bifrost's design (and the porting constraints recorded in `project_kes_port_constraints`), the secret-key bytes
  * corresponding to a given period are read-once: after they are loaded from the [[kes.SecureStore]] and used to sign,
  * the persisted representation is erased and the in-memory bytes are overwritten before the next evolve step. This
  * prevents an evolved key from being usable at an earlier period.
  *
  * == References ==
  *
  *   - Malkin, T., Micciancio, D., Miner, S. "Efficient generic forward-secure signatures with an unbounded number of
  *     time periods", EUROCRYPT 2002, LNCS 2332, pp. 400–417.
  *   - Bifrost ProductComposition / SumComposition (credit: Aaron Schutza).
  */
package object kes
