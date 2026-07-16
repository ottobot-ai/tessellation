package io.constellationnetwork.security.smt.node

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt._

/** Pure structural operations on [[SmtNode]] trees: upsert, delete, lookup, and authentication-path proof generation. Every op preserves
  * the canonical collapse invariant documented on [[SmtNode]], so the resulting [[SmtNode.digest]] is a pure function of the live leaf set
  * (order-independent).
  *
  * These are the building blocks the in-memory [[SparseMerkleTree]] / [[SmtProver]] are written against; keeping them here (operating on
  * the node directly) keeps the `Ref`-backed wrapper thin.
  */
object SmtNodeOps {

  /** Upsert `(key → value)` (with precomputed `position = Hasher.hash(key)` and `valueDigest`) into the subtree rooted at `node`, which
    * sits at `depth`.
    */
  def insert[F[_]: Async: Hasher](
    node: SmtNode,
    key: Hex,
    position: Hash,
    valueDigest: Hash,
    value: Array[Byte],
    depth: Int
  ): F[SmtNode] =
    node match {
      case SmtNode.Empty =>
        SmtNode.Leaf.make[F](key, position, valueDigest, value).widen

      case leaf: SmtNode.Leaf =>
        if (leaf.position === position) SmtNode.Leaf.make[F](key, position, valueDigest, value).widen
        else mergeLeafWithNew[F](leaf, key, position, valueDigest, value, depth).widen

      case internal: SmtNode.Internal =>
        if (SmtHashing.bit(position, depth))
          insert[F](internal.right, key, position, valueDigest, value, depth + 1)
            .flatMap(r => SmtNode.Internal.make[F](internal.left, r).widen)
        else
          insert[F](internal.left, key, position, valueDigest, value, depth + 1)
            .flatMap(l => SmtNode.Internal.make[F](l, internal.right).widen)
    }

  /** Delete `position` from the subtree rooted at `node` (at `depth`), re-collapsing stems so the result stays canonical. No-op if absent.
    */
  def remove[F[_]: Async: Hasher](node: SmtNode, position: Hash, depth: Int): F[SmtNode] =
    node match {
      case SmtNode.Empty => (SmtNode.Empty: SmtNode).pure[F]

      case leaf: SmtNode.Leaf =>
        if (leaf.position === position) (SmtNode.Empty: SmtNode).pure[F]
        else (leaf: SmtNode).pure[F]

      case internal: SmtNode.Internal =>
        if (SmtHashing.bit(position, depth))
          remove[F](internal.right, position, depth + 1).flatMap(newR => collapse[F](internal.left, newR))
        else
          remove[F](internal.left, position, depth + 1).flatMap(newL => collapse[F](newL, internal.right))
    }

  /** The value bytes at `position`, or `None`. */
  def get(node: SmtNode, position: Hash, depth: Int): Option[Array[Byte]] =
    node match {
      case SmtNode.Empty => None
      case leaf: SmtNode.Leaf =>
        if (leaf.position === position) Some(leaf.valueCopy) else None
      case internal: SmtNode.Internal =>
        if (SmtHashing.bit(position, depth)) get(internal.right, position, depth + 1)
        else get(internal.left, position, depth + 1)
    }

  /** Build the authentication path for `key` (already hashed to `position`) against the subtree rooted at `node`, accumulating sibling
    * digests top-down. Returns the [[SmtProof]] the [[SmtVerifier]] can fold. The in-memory tree never produces a malformed proof, so this
    * is total in the `Right` channel; the `Either` keeps the type uniform with the algebra.
    */
  def prove[F[_]: Async](
    root: SmtNode,
    key: Hex,
    position: Hash
  ): F[Either[SmtProofError, SmtProof]] = {

    // Walk down accumulating siblings (root-first). Stops at Leaf / Empty.
    def loop(node: SmtNode, depth: Int, acc: List[SmtSibling]): Either[SmtProofError, SmtProof] =
      node match {
        case SmtNode.Empty =>
          (SmtProof.Absence(key, AbsenceWitness.Default, acc.reverse): SmtProof).asRight[SmtProofError]

        case leaf: SmtNode.Leaf =>
          if (leaf.position === position)
            (SmtProof.Inclusion(key, leaf.valueCopy, leaf.valueDigest, acc.reverse): SmtProof).asRight[SmtProofError]
          else
            (SmtProof.Absence(
              key,
              AbsenceWitness.OtherLeaf(leaf.key, leaf.valueDigest),
              acc.reverse
            ): SmtProof).asRight[SmtProofError]

        case internal: SmtNode.Internal =>
          if (SmtHashing.bit(position, depth))
            loop(internal.right, depth + 1, SmtSibling(internal.left.digest) :: acc)
          else
            loop(internal.left, depth + 1, SmtSibling(internal.right.digest) :: acc)
      }

    loop(root, 0, Nil).pure[F]
  }

  /** Place two distinct-position leaves (the existing `leaf` and a new one) under a fresh internal stem starting at `depth`. */
  private def mergeLeafWithNew[F[_]: Async: Hasher](
    leaf: SmtNode.Leaf,
    newKey: Hex,
    newPos: Hash,
    newVd: Hash,
    newValue: Array[Byte],
    depth: Int
  ): F[SmtNode.Internal] = {
    val existingBit = SmtHashing.bit(leaf.position, depth)
    val newBit = SmtHashing.bit(newPos, depth)
    if (existingBit === newBit)
      mergeLeafWithNew[F](leaf, newKey, newPos, newVd, newValue, depth + 1).flatMap { child =>
        if (newBit) SmtNode.Internal.make[F](SmtNode.Empty, child)
        else SmtNode.Internal.make[F](child, SmtNode.Empty)
      }
    else
      SmtNode.Leaf.make[F](newKey, newPos, newVd, newValue).flatMap { newLeaf =>
        if (newBit) SmtNode.Internal.make[F](leaf, newLeaf) // new goes right (newBit true), existing left
        else SmtNode.Internal.make[F](newLeaf, leaf) // new goes left, existing right
      }
  }

  /** Re-collapse an internal node after a child changed: if exactly one leaf remains in the subtree (one child a Leaf, the other Empty),
    * return that Leaf; otherwise keep an Internal. (A subtree that became fully Empty can only arise from removing the lone leaf, which the
    * Leaf case above already returns as Empty — so here at least one side is non-empty in the reachable cases; the `(Empty, Empty)` case is
    * handled defensively.)
    */
  private def collapse[F[_]: Async: Hasher](left: SmtNode, right: SmtNode): F[SmtNode] =
    (left, right) match {
      case (SmtNode.Empty, r: SmtNode.Leaf) => (r: SmtNode).pure[F]
      case (l: SmtNode.Leaf, SmtNode.Empty) => (l: SmtNode).pure[F]
      case (SmtNode.Empty, SmtNode.Empty)   => (SmtNode.Empty: SmtNode).pure[F]
      case _                                => SmtNode.Internal.make[F](left, right).widen
    }
}
