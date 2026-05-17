package io.constellationnetwork.security.kes

import java.security.SecureRandom

import scala.annotation.tailrec

import KesBinaryTree._

/** MMM sum composition of forward-secure signatures, instantiated over Ed25519 + Blake2b-256.
  *
  *   - Provides forward-secure signatures over `l = 2^h` time periods, where `h` is the tree height chosen at key generation.
  *   - Ported from Bifrost's `co.topl.crypto.signing.kes.SumComposition` (credit: Aaron Schutza).
  *
  * The sum scheme alone is a useful building block but is NOT the recommended consensus-facing API: it is bounded in the number of
  * supported periods. See [[ProductComposition]] / `KesProduct` for the production interface.
  *
  * This class is package-private; consumers should use `KesSum` (which wraps it and exposes a friendlier API surface).
  */
protected[kes] class SumComposition extends KesEd25519Blake2b256 {

  override type SIG = (Array[Byte], Array[Byte], Vector[Array[Byte]])
  override type VK = (Array[Byte], Int)
  override type SK = KesBinaryTree

  /** Source of entropy used to overwrite secret bytes during evolution. */
  protected val random: SecureRandom = new SecureRandom()

  /** Current time step encoded by the tree shape:
    *
    *   - A leaf-only tree is at step 0.
    *   - A right-recursive `MerkleNode(_, _, _, Empty, _)` has stepped past its left subtree by `2^h(right)` steps.
    *   - A left-recursive `MerkleNode(_, _, _, _, Empty)` is still in its left subtree.
    */
  protected[kes] def getKeyTime(keyTree: SK): Int =
    keyTree match {
      case MerkleNode(_, _, _, Empty(), _: SigningLeaf)    => 1
      case MerkleNode(_, _, _, Empty(), right: MerkleNode) => getKeyTime(right) + exp(getTreeHeight(right))
      case MerkleNode(_, _, _, left, Empty())              => getKeyTime(left)
      case _                                               => 0
    }

  /** Generate the public verification key associated with a secret-key tree at its current step. */
  protected[kes] def generateVerificationKey(keyTree: SK): VK =
    keyTree match {
      case node: MerkleNode  => (witness(node), getKeyTime(keyTree))
      case leaf: SigningLeaf => (witness(leaf), 0)
      case Empty()           => (new Array[Byte](hashBytes), 0)
    }

  /** Generate a secret-key tree at time-step 0 from the given seed and height.
    *
    *   - The recursion materialises the full binary tree using `prng` to derive child seeds;
    *   - then reduces (collapses) all right subtrees, leaving only the leftmost branch.
    *
    * The input `seed` is overwritten with random bytes on the way out.
    */
  protected[kes] def generateSecretKey(seed: Array[Byte], height: Int): SK = {

    def seedTree(seed: Array[Byte], height: Int): KesBinaryTree =
      if (height == 0) {
        val (sk, pk) = sGenKeypair(seed)
        SigningLeaf(sk, pk)
      } else {
        val (s1, s2) = prng(seed)
        val left = seedTree(s1, height - 1)
        val right = seedTree(s2, height - 1)
        MerkleNode(s2, witness(left), witness(right), left, right)
      }

    def reduceTree(fullTree: KesBinaryTree): KesBinaryTree =
      fullTree match {
        case MerkleNode(s, witL, witR, nodeL, nodeR) =>
          eraseOldNode(nodeR)
          MerkleNode(s, witL, witR, reduceTree(nodeL), Empty())
        case leaf: SigningLeaf => leaf
        case _                 => Empty()
      }

    val out = reduceTree(seedTree(seed, height))
    random.nextBytes(seed)
    out
  }

  /** Evolve the key to `step`. Idempotent at `step == 0` (returns the input unchanged).
    *
    * Returns `Left(KesError)` if `step` is past the maximum step expressible by the tree height, or if `step <= currentStep` and `step !=
    * 0`.
    */
  protected[kes] def updateKey(keyTree: SK, step: Int): Either[KesError, SK] = {
    val totalSteps = exp(getTreeHeight(keyTree))
    val keyTime = getKeyTime(keyTree)
    if (step == 0) Right(keyTree)
    else if (step >= totalSteps) Left(KesError.StepBeyondMax(keyTime, step, totalSteps))
    else if (keyTime >= step) Left(KesError.StepNotMonotonic(keyTime, step))
    else Right(evolveKey(keyTree, step))
  }

  /** Securely overwrite all secret bytes carried by a (potentially nested) tree node.
    *
    * Uses [[SecureRandom]] so the overwrite is not constant; this defeats heap-grep attacks that recognise the "all-zero" pattern that a
    * naive `Array.fill` would leave behind.
    */
  protected[kes] def eraseOldNode(node: KesBinaryTree): Unit =
    node match {
      case merkleNode: MerkleNode =>
        random.nextBytes(merkleNode.seed)
        random.nextBytes(merkleNode.witnessLeft)
        random.nextBytes(merkleNode.witnessRight)
        merkleNode.left match {
          case l: MerkleNode => eraseOldNode(l)
          case l: SigningLeaf =>
            random.nextBytes(l.sk)
            random.nextBytes(l.vk)
          case _ =>
        }
        merkleNode.right match {
          case r: MerkleNode => eraseOldNode(r)
          case r: SigningLeaf =>
            random.nextBytes(r.sk)
            random.nextBytes(r.vk)
          case _ =>
        }
      case leaf: SigningLeaf =>
        random.nextBytes(leaf.sk)
        random.nextBytes(leaf.vk)
      case _ =>
    }

  /** Internal: actually do the tree rotation/regeneration to reach `step`. Assumes preconditions checked by [[updateKey]]. */
  protected[kes] def evolveKey(input: KesBinaryTree, step: Int): KesBinaryTree = {
    val halfTotalSteps = exp(getTreeHeight(input) - 1)
    val shiftStep: Int => Int = (s: Int) => s % halfTotalSteps

    if (step >= halfTotalSteps) {
      input match {
        case MerkleNode(seed, witL, witR, oldLeaf: SigningLeaf, Empty()) =>
          val (sk, pk) = sGenKeypair(seed)
          val newNode = MerkleNode(
            new Array[Byte](seed.length),
            witL,
            witR,
            Empty(),
            SigningLeaf(sk, pk)
          )
          eraseOldNode(oldLeaf)
          random.nextBytes(seed)
          newNode
        case MerkleNode(seed, witL, witR, oldNode: MerkleNode, Empty()) =>
          val newNode = MerkleNode(
            new Array[Byte](seed.length),
            witL,
            witR,
            Empty(),
            evolveKey(generateSecretKey(seed, getTreeHeight(input) - 1), shiftStep(step))
          )
          eraseOldNode(oldNode)
          random.nextBytes(seed)
          newNode
        case MerkleNode(seed, witL, witR, Empty(), right) =>
          MerkleNode(seed, witL, witR, Empty(), evolveKey(right, shiftStep(step)))
        case leaf: SigningLeaf => leaf
        case _                 => Empty()
      }
    } else {
      input match {
        case MerkleNode(seed, witL, witR, left, Empty()) =>
          MerkleNode(seed, witL, witR, evolveKey(left, shiftStep(step)), Empty())
        case MerkleNode(seed, witL, witR, Empty(), right) =>
          MerkleNode(seed, witL, witR, Empty(), evolveKey(right, shiftStep(step)))
        case leaf: SigningLeaf => leaf
        case _                 => Empty()
      }
    }
  }

  /** Sign `m` with the active leaf of `keyTree`. The witness stack accumulates the sibling witnesses on the way down to the active leaf;
    * this is the Merkle authentication path the verifier will check against the [[VerificationKeyKesSum]] root.
    */
  protected[kes] def sign(keyTree: SK, m: Array[Byte]): SIG = {
    @tailrec
    def loop(tree: KesBinaryTree, W: Vector[Array[Byte]]): SIG =
      tree match {
        case MerkleNode(_, witL, _, Empty(), right) => loop(right, witL.clone() +: W)
        case MerkleNode(_, _, witR, left, _)        => loop(left, witR.clone() +: W)
        case leaf: SigningLeaf                      => (leaf.vk.clone(), sSign(m, leaf.sk).clone(), W)
        case _                                      => (new Array[Byte](pkBytes), new Array[Byte](sigBytes), Vector(Array.empty[Byte]))
      }
    loop(keyTree, Vector.empty)
  }

  /** Verify `(vkSign, sigSign, merkleProof)` against `(root, step)`. The verifier:
    *
    *   1. Walks the witness vector bottom-up, reconstructing the path hash;
    *   1. Checks the reconstructed root against the carried VK root;
    *   1. Verifies the underlying Ed25519 signature against the message.
    */
  protected[kes] def verify(kesSig: SIG, m: Array[Byte], kesVk: VK): Boolean = {
    val (vkSign, sigSign, merkleProof) = kesSig
    val (root, step) = kesVk

    val leftGoing: Int => Boolean = (level: Int) => ((step / exp(level)) % 2) == 0

    def concat(a: Array[Byte], b: Array[Byte]): Array[Byte] = {
      val out = new Array[Byte](a.length + b.length)
      System.arraycopy(a, 0, out, 0, a.length)
      System.arraycopy(b, 0, out, a.length, b.length)
      out
    }

    def verifyMerkle(W: Vector[Array[Byte]]): Boolean =
      if (W.isEmpty) emptyWitness
      else if (W.length == 1) singleWitness(W.head)
      else if (leftGoing(0)) multiWitness(W.tail, hash(vkSign), W.head, 1)
      else multiWitness(W.tail, W.head, hash(vkSign), 1)

    def emptyWitness: Boolean = root.sameElements(hash(vkSign))

    def singleWitness(w: Array[Byte]): Boolean =
      if (leftGoing(0)) root.sameElements(hash(concat(hash(vkSign), w)))
      else root.sameElements(hash(concat(w, hash(vkSign))))

    @tailrec
    def multiWitness(
      witnessList: Vector[Array[Byte]],
      witnessLeft: Array[Byte],
      witnessRight: Array[Byte],
      index: Int
    ): Boolean =
      if (witnessList.isEmpty) root.sameElements(hash(concat(witnessLeft, witnessRight)))
      else if (leftGoing(index))
        multiWitness(witnessList.tail, hash(concat(witnessLeft, witnessRight)), witnessList.head, index + 1)
      else multiWitness(witnessList.tail, witnessList.head, hash(concat(witnessLeft, witnessRight)), index + 1)

    val verifySign = sVerify(m, sigSign, vkSign)
    verifyMerkle(merkleProof) && verifySign
  }
}
