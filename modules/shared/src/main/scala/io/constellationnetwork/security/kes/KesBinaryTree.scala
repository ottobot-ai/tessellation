package io.constellationnetwork.security.kes

/** Binary tree used to represent the secret-key state of a KES sum-composition scheme.
  *
  * Ported from Bifrost's `co.topl.crypto.models.KesBinaryTree`. The tree has three node types:
  *
  *   - [[KesBinaryTree.MerkleNode]] — an interior node with a per-node seed, witnesses of its left/right subtrees, and
  *     the subtrees themselves. Either subtree may be [[KesBinaryTree.Empty]] once it has been collapsed during key
  *     evolution; this represents that the corresponding sub-key bytes have been destroyed.
  *   - [[KesBinaryTree.SigningLeaf]] — a leaf carrying the active Ed25519 keypair for the current period.
  *   - [[KesBinaryTree.Empty]] — a placeholder used to mark erased subtrees.
  *
  * The byte arrays stored here are mutable on purpose: the read-once / forward-security pattern requires that secret
  * bytes can be overwritten in place after they have been consumed. Callers must therefore treat instances as if they
  * carried embedded mutable state and avoid sharing references across phases.
  */
sealed trait KesBinaryTree extends Product with Serializable

object KesBinaryTree {

  /** Wire prefix used when serialising a [[MerkleNode]] entry. */
  val nodeTypePrefix: Byte = 0

  /** Wire prefix used when serialising a [[SigningLeaf]] entry. */
  val leafTypePrefix: Byte = 1

  /** Wire prefix used when serialising an [[Empty]] entry. */
  val emptyTypePrefix: Byte = 2

  final case class MerkleNode(
    seed: Array[Byte],
    witnessLeft: Array[Byte],
    witnessRight: Array[Byte],
    left: KesBinaryTree,
    right: KesBinaryTree
  ) extends KesBinaryTree

  final case class SigningLeaf(sk: Array[Byte], vk: Array[Byte]) extends KesBinaryTree

  final case class Empty() extends KesBinaryTree
}
