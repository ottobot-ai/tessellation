package io.constellationnetwork.security.smt

import io.constellationnetwork.security.hex.Hex

/** Produces a native [[SmtProof]] for a key against a [[SparseMerkleTree]] — INCLUSION when the key is present, ABSENCE when it is not (the
  * caller does not pre-decide which; absence is first-class).
  *
  * Mirrors the `MerklePatricia*Prover` split: the prover walks the tree and emits the authentication path; verification (and the mandatory
  * value-binding) is the [[SmtVerifier]]'s job.
  */
trait SmtProver[F[_]] {

  /** Prove `key`'s status against the tree this prover was built for. `Right` carries an [[SmtProof.Inclusion]] or [[SmtProof.Absence]];
    * `Left` carries a structural [[SmtProofError]] (the in-memory prover does not fail under normal operation, but the signature keeps the
    * error channel uniform with [[SmtVerifier]]).
    */
  def prove(key: Hex): F[Either[SmtProofError, SmtProof]]
}
