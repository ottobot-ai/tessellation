package io.constellationnetwork.security.smt.node

import cats.effect.Sync
import cats.syntax.functor._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.SmtHashing

/** In-memory node of the binary sparse Merkle tree (the reference impl's materialized structure). Each node caches its digest, computed at
  * construction (no invalidation) — exactly like `MerklePatriciaNode`.
  *
  * Canonical, order-independent shape for a subtree holding leaf set `S` (all sharing the prefix down to this depth):
  *   - `|S| == 0` ⇒ [[SmtNode.Empty]] (digest = `Hash.empty`, never hashed),
  *   - `|S| == 1` ⇒ [[SmtNode.Leaf]] (digest binds the FULL position, so it is depth-independent),
  *   - `|S| >= 2` ⇒ [[SmtNode.Internal]] split by bit `depth` into `(left, right)`; either child MAY be `Empty` when all of `S` shares that
  *     bit (a "stem" of internal nodes with empty siblings until the leaves diverge — this is the Diem/JMT empty-subtree collapse).
  *
  * Because the shape is a pure function of `S` (and positions are key-hashes, hence uniform), the [[root]] digest is independent of the
  * order keys were inserted/removed.
  */
sealed trait SmtNode extends Serializable {
  def digest: Hash
}

object SmtNode {

  /** A collapsed empty (default) subtree. */
  case object Empty extends SmtNode {
    val digest: Hash = SmtHashing.empty
  }

  /** A single occupied position. The original `key` (pre-image of `position = Hasher.hash(key)`) is retained so an [[SmtNode.Leaf]] proving
    * the ABSENCE of a DIFFERENT key can hand the verifier the genuine occupying key (which the verifier re-hashes to the committed
    * position). `value` is retained so the in-memory tree can answer `get` and the prover can emit the value bytes.
    */
  final class Leaf private (
    val key: Hex,
    val position: Hash,
    val valueDigest: Hash,
    private val ownedValue: Array[Byte],
    val digest: Hash
  ) extends SmtNode {
    private[node] def valueCopy: Array[Byte] = ownedValue.clone()
  }

  /** An internal node split by one bit; `left`/`right` may be [[Empty]] (stem). */
  final case class Internal private (left: SmtNode, right: SmtNode, digest: Hash) extends SmtNode

  object Leaf {

    def make[F[_]: Sync: Hasher](key: Hex, position: Hash, valueDigest: Hash, value: Array[Byte]): F[Leaf] =
      SmtHashing.leafDigest[F](position, valueDigest).map(d => new Leaf(key, position, valueDigest, value.clone(), d))
  }

  object Internal {

    def make[F[_]: Sync: Hasher](left: SmtNode, right: SmtNode): F[Internal] =
      SmtHashing.internalDigest[F](left.digest, right.digest).map(d => new Internal(left, right, d))
  }
}
