package io.constellationnetwork.security.mpt

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.annotation.tailrec

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{MerklePatriciaProducer, ParallelMerklePatriciaProducer}

import io.circe._
import io.circe.syntax._

/** Immutable Merkle Patricia Trie. Root hash is always available since nodes compute their digest at construction.
  */
final case class MerklePatriciaTrie(rootNode: MerklePatriciaNode) {

  /** Get the root hash - O(1) since digest is pre-computed.
    */
  def rootHash: MptRoot = MptRoot(rootNode.digest)

  /** Apply a delta of upserts and removals, returning a new trie that shares unchanged subtree references with this one.
    *
    * Implementation: thin wrapper over `IncrementalTrieOps.removeMultiple` then `insertMultiple`. Removals are applied first so a key
    * removed and re-upserted in the same delta ends up with the new value. Both operations sort by `CompactNibblePath` ordering to match
    * the deterministic trie structure produced by the parallel/file-system full-build paths.
    *
    * Cost: O(changed_keys × log N) new node allocations. Unchanged subtrees share refs with the receiver — branch-overlay friendly.
    *
    * Empty-state note: this operates at the trie level, so an empty result (e.g. all entries removed) is represented as an empty Branch.
    * That matches `ParallelMerklePatriciaProducer.createFromBytes(Map.empty)` but differs from `InMemoryMerklePatriciaProducer.build` which
    * errors on "no entries" — the latter is a producer-level invariant, not a trie-level one.
    */
  def withChanges[F[_]: Async: Hasher](
    upserts: Map[Hex, Array[Byte]],
    removes: Set[Hex]
  ): F[MerklePatriciaTrie] = {
    val sortedRemoves = removes.toList.sortBy(hex => CompactNibblePath.fromHexString(hex.value))

    for {
      afterRemoves <-
        if (sortedRemoves.isEmpty) rootNode.pure[F]
        else IncrementalTrieOps.removeMultiple[F](rootNode, sortedRemoves)
      hashedInserts <- upserts.toList.traverse {
        case (hex, bytes) => Hasher[F].hashBytes(bytes).map(hash => (hex, hash))
      }
      sortedInserts = hashedInserts.sortBy { case (hex, _) => CompactNibblePath.fromHexString(hex.value) }
      afterUpserts <-
        if (sortedInserts.isEmpty) afterRemoves.pure[F]
        else IncrementalTrieOps.insertMultiple[F](afterRemoves, sortedInserts)
    } yield MerklePatriciaTrie(afterUpserts)
  }
}

object MerklePatriciaTrie {

  implicit val merklePatriciaTrieEncoder: Encoder[MerklePatriciaTrie] =
    (tree: MerklePatriciaTrie) => Json.obj("rootNode" -> tree.rootNode.asJson)

  implicit val merklePatriciaTrieDecoder: Decoder[MerklePatriciaTrie] = (c: HCursor) =>
    c.downField("rootNode").as[MerklePatriciaNode].map(MerklePatriciaTrie(_))

  def make[F[_]: Hasher: Async, A: Encoder](data: Map[Hex, A]): F[MerklePatriciaTrie] =
    MerklePatriciaProducer
      .stateless[F]
      .create(data)

  def makeParallelFromBytes[F[_]: Hasher: Async: Parallel: JsonSerializer](data: Map[Hex, Array[Byte]]): F[MerklePatriciaTrie] =
    ParallelMerklePatriciaProducer[F].createFromBytes(data)

  def makeParallel[F[_]: Hasher: Async: Parallel: JsonSerializer, A: Encoder](data: Map[Hex, A]): F[MerklePatriciaTrie] =
    MerklePatriciaProducer
      .parallel[F]
      .create(data)

  def collectLeafNodes(trie: MerklePatriciaTrie): List[MerklePatriciaNode.Leaf] = {
    @tailrec
    def traverse(nodes: List[MerklePatriciaNode], acc: List[MerklePatriciaNode.Leaf]): List[MerklePatriciaNode.Leaf] =
      nodes match {
        case Nil => acc
        case (head: MerklePatriciaNode.Leaf) :: tail =>
          traverse(tail, head :: acc)
        case (branch: MerklePatriciaNode.Branch) :: tail =>
          // Sort by nibble value for deterministic traversal order
          traverse(branch.internalPaths.toList.sortBy(_._1).map(_._2) ++ tail, acc)
        case (ext: MerklePatriciaNode.Extension) :: tail =>
          traverse(ext.child :: tail, acc)
      }

    traverse(List(trie.rootNode), List()).reverse
  }

  def collectLeafNodesWithPaths(trie: MerklePatriciaTrie): List[(Hex, MerklePatriciaNode.Leaf)] = {
    case class NodeWithPath(node: MerklePatriciaNode, pathSoFar: CompactNibblePath)

    @tailrec
    def traverse(nodes: List[NodeWithPath], acc: List[(Hex, MerklePatriciaNode.Leaf)]): List[(Hex, MerklePatriciaNode.Leaf)] =
      nodes match {
        case Nil => acc
        case NodeWithPath(leaf: MerklePatriciaNode.Leaf, pathSoFar) :: tail =>
          val fullPath = pathSoFar ++ leaf.remainingPath
          traverse(tail, (fullPath.toHex, leaf) :: acc)
        case NodeWithPath(branch: MerklePatriciaNode.Branch, pathSoFar) :: tail =>
          // Sort by nibble value for deterministic traversal order
          val childNodes = branch.internalPaths.toList.sortBy(_._1).map {
            case (nibbleValue, child) =>
              NodeWithPath(child, pathSoFar ++ CompactNibblePath.single(nibbleValue))
          }
          traverse(childNodes ++ tail, acc)
        case NodeWithPath(ext: MerklePatriciaNode.Extension, pathSoFar) :: tail =>
          traverse(NodeWithPath(ext.child, pathSoFar ++ ext.sharedPath) :: tail, acc)
      }

    traverse(List(NodeWithPath(trie.rootNode, CompactNibblePath.empty)), List()).reverse
  }
}
