package io.constellationnetwork.security.smt

import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.node.{SmtNode, SmtNodeOps}

/** In-memory reference implementation of [[SparseMerkleTree]] — a binary sparse Merkle tree with the Diem/JMT empty-subtree collapse.
  *
  * `Ref[F, SmtNode]`-backed, mirroring `InMemoryMerklePatriciaProducer`'s idiom; the mutators ([[insert]] / [[remove]] / [[withChanges]])
  * return a NEW tree over a FRESH `Ref` seeded with the recomputed root node, while the unchanged subtrees are shared structurally (the
  * node ADT is immutable, so the new root reuses every untouched child). The receiver is never mutated.
  *
  * Positions are `Hasher.hash(key)` (uniform), so node count is proportional to live leaves and the [[root]] is order-independent.
  */
final class InMemorySparseMerkleTree[F[_]: Async: Hasher] private (rootRef: Ref[F, SmtNode]) extends SparseMerkleTree[F] {

  def get(key: Hex): F[Option[Array[Byte]]] =
    for {
      pos <- SmtHashing.position[F](key)
      node <- rootRef.get
    } yield SmtNodeOps.get(node, pos, 0)

  def root: F[SmtRoot] =
    rootRef.get.map(node => SmtRoot(node.digest))

  def insert(key: Hex, value: Array[Byte]): F[SparseMerkleTree[F]] =
    for {
      node <- rootRef.get
      pos <- SmtHashing.position[F](key)
      owned = value.clone()
      vd <- Hasher[F].hashBytes(owned)
      updated <- SmtNodeOps.insert[F](node, key, pos, vd, owned, 0)
      tree <- InMemorySparseMerkleTree.fromNode[F](updated)
    } yield tree

  def remove(key: Hex): F[SparseMerkleTree[F]] =
    for {
      node <- rootRef.get
      pos <- SmtHashing.position[F](key)
      updated <- SmtNodeOps.remove[F](node, pos, 0)
      tree <- InMemorySparseMerkleTree.fromNode[F](updated)
    } yield tree

  def withChanges(upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SparseMerkleTree[F]] =
    for {
      node <- rootRef.get
      // removals first (then upserts win), matching MerklePatriciaTrie.withChanges. The canonical collapse invariant makes the
      // result independent of the order within each phase.
      afterRemoves <- removes.toList.foldLeftM(node) { (acc, key) =>
        SmtHashing.position[F](key).flatMap(pos => SmtNodeOps.remove[F](acc, pos, 0))
      }
      afterUpserts <- upserts.toList.foldLeftM(afterRemoves) {
        case (acc, (key, value)) =>
          for {
            pos <- SmtHashing.position[F](key)
            owned = value.clone()
            vd <- Hasher[F].hashBytes(owned)
            next <- SmtNodeOps.insert[F](acc, key, pos, vd, owned, 0)
          } yield next
      }
      tree <- InMemorySparseMerkleTree.fromNode[F](afterUpserts)
    } yield tree

  /** A [[SmtProver]] bound to THIS tree's current root node (snapshot at call time). */
  def prover: F[SmtProver[F]] =
    rootRef.get.map { node =>
      new SmtProver[F] {
        def prove(key: Hex): F[Either[SmtProofError, SmtProof]] =
          SmtHashing.position[F](key).flatMap(pos => SmtNodeOps.prove[F](node, key, pos))
      }
    }
}

object InMemorySparseMerkleTree {

  /** An empty tree. */
  def empty[F[_]: Async: Hasher]: F[InMemorySparseMerkleTree[F]] =
    fromNode[F](SmtNode.Empty)

  /** A tree seeded with `initial` key→value bindings. Order-independent: any iteration order of `initial` yields the same root. */
  def make[F[_]: Async: Hasher](initial: Map[Hex, Array[Byte]] = Map.empty): F[InMemorySparseMerkleTree[F]] =
    initial.toList
      .foldLeftM(SmtNode.Empty: SmtNode) {
        case (acc, (key, value)) =>
          for {
            pos <- SmtHashing.position[F](key)
            owned = value.clone()
            vd <- Hasher[F].hashBytes(owned)
            next <- SmtNodeOps.insert[F](acc, key, pos, vd, owned, 0)
          } yield next
      }
      .flatMap(fromNode[F])

  private def fromNode[F[_]: Async: Hasher](node: SmtNode): F[InMemorySparseMerkleTree[F]] =
    Ref.of[F, SmtNode](node).map(new InMemorySparseMerkleTree[F](_))
}
