package io.constellationnetwork.security.smt

import io.constellationnetwork.security.hex.Hex

/** An immutable, additive sparse Merkle tree (the logical Jellyfish Merkle Tree core) over 256-bit key POSITIONS.
  *
  * This is a PARALLEL primitive — it does NOT replace or touch the consensus `MerklePatriciaTrie`. Hashing is Blake2b-via-`Hasher[F]` with
  * Circe-encoded, domain-separated commitments ([[SmtCommitment]]), matching `MerklePatriciaNode`'s discipline (ZK-friendly hashing a.k.a.
  * Poseidon is out of scope unless ZK becomes a stated goal).
  *
  * The position of a key is `Hasher.hash(key)` (a fixed, uniform 256-bit value), so leaf positions are uniformly distributed and the tree's
  * structure is a pure function of the live leaf SET. Combined with the Diem/JMT empty-subtree collapse (any subtree with 0 or 1 leaf
  * collapses to a default placeholder or to that single leaf), this gives:
  *   - node count proportional to the number of live leaves (NOT 2^256), and
  *   - INSERTION/REMOVAL ORDER INDEPENDENCE — the same key-set always yields the same [[root]].
  *
  * All mutators return a NEW tree with structural sharing; the receiver is unchanged.
  *
  * @tparam F
  *   the effect; concrete impls need `Async: Hasher` (positions and node digests are computed in `F`).
  */
trait SparseMerkleTree[F[_]] {

  /** The value bytes bound to `key`, or `None` if `key` is absent. `key` is hashed to its position internally. */
  def get(key: Hex): F[Option[Array[Byte]]]

  /** The root commitment (digest of the root node; [[SmtRoot.empty]] for the empty tree). */
  def root: F[SmtRoot]

  /** A new tree with `key` bound to `value` (upsert). Structural sharing with the receiver. */
  def insert(key: Hex, value: Array[Byte]): F[SparseMerkleTree[F]]

  /** A new tree with `key` removed (no-op if absent). Structural sharing with the receiver. */
  def remove(key: Hex): F[SparseMerkleTree[F]]

  /** A new tree with `removes` deleted then `upserts` applied (removals first, upsert-wins — matching `MerklePatriciaTrie.withChanges`).
    * Structural sharing with the receiver. The result is independent of the order entries appear in the inputs.
    */
  def withChanges(upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SparseMerkleTree[F]]
}
