package io.constellationnetwork.security.kes

import java.security.SecureRandom

import KesBinaryTree._

/** MMM product (super × sub) composition of forward-secure signatures.
  *
  *   - The "super" (slow / hour-hand) tree commits to a sequence of "sub" (fast / minute-hand) trees.
  *   - Each super-step authorises a fresh sub-tree;
  *   - Each sub-step within the sub-tree signs a single message.
  *
  * Total expressible time steps: `2^(heightSup + heightSub)`. Bifrost notes this is theoretically unbounded for `log(l) / log(2) = 7` in
  * the asymmetric product composition (assuming integer time steps).
  *
  * This is the consensus-facing KES scheme. Ported from Bifrost's `co.topl.crypto.signing.kes.ProductComposition` (credit: Aaron Schutza).
  *
  * Package-private; consumers should use `KesProduct` (which wraps it and translates between the tuple-encoded internal representation and
  * the named-field model types in `models`).
  */
protected[kes] class ProductComposition extends KesEd25519Blake2b256 {

  protected val sumComposition: SumComposition = new SumComposition

  protected val random: SecureRandom = new SecureRandom()

  override type SIG = (sumComposition.SIG, sumComposition.SIG, Array[Byte])
  override type VK = (Array[Byte], Int)
  override type SK = (sumComposition.SK, sumComposition.SK, Array[Byte], sumComposition.SIG)

  /** Current step in the product (super × sub) lattice. */
  protected[kes] def getKeyTime(key: SK): Int = {
    val numSubSteps = exp(sumComposition.getTreeHeight(key._2))
    val tSup = sumComposition.getKeyTime(key._1)
    val tSub = sumComposition.getKeyTime(key._2)
    (tSup * numSubSteps) + tSub
  }

  /** Derive the product verification key from the secret-key tuple.
    *
    * The product VK is just the super-tree's witness + the current product step.
    */
  protected[kes] def generateVerificationKey(key: SK): VK = key._1 match {
    case node: MerkleNode  => (witness(node), getKeyTime(key))
    case leaf: SigningLeaf => (witness(leaf), 0)
    case Empty()           => (new Array[Byte](hashBytes), 0)
  }

  /** Generate a product secret key at step 0 from a seed and (heightSup, heightSub). */
  protected[kes] def generateSecretKey(seed: Array[Byte], heightSup: Int, heightSub: Int): SK = {
    val rSuper = prng(seed)
    val rSub = prng(rSuper._2)
    val superScheme = sumComposition.generateSecretKey(rSuper._1, heightSup)
    val subScheme = sumComposition.generateSecretKey(rSub._1, heightSub)
    val kesVkSub: sumComposition.VK = sumComposition.generateVerificationKey(subScheme)
    val kesSigSup: sumComposition.SIG = sumComposition.sign(superScheme, kesVkSub._1)
    random.nextBytes(rSuper._2)
    random.nextBytes(seed)
    (superScheme, subScheme, rSub._2, kesSigSup)
  }

  /** Erase the secret bytes carried by the active leaf inside `input` (the leaf hanging off the rightmost spine).
    *
    * Used after committing to a child VK so the parent secret key is no longer in a state where it can re-commit to a different child key
    * until the next super-step. Returns a new tree with the erased leaf substituted in.
    */
  protected[kes] def eraseLeafSecretKey(input: KesBinaryTree): Either[KesError, KesBinaryTree] =
    input match {
      case n: MerkleNode =>
        (n.left, n.right) match {
          case (Empty(), _) =>
            eraseLeafSecretKey(n.right).map(newR => MerkleNode(n.seed, n.witnessLeft, n.witnessRight, Empty(), newR))
          case (_, Empty()) =>
            eraseLeafSecretKey(n.left).map(newL => MerkleNode(n.seed, n.witnessLeft, n.witnessRight, newL, Empty()))
          case _ => Left(KesError.MalformedTree("Both children populated during eraseLeafSecretKey"))
        }
      case l: SigningLeaf =>
        random.nextBytes(l.sk)
        Right(SigningLeaf(new Array[Byte](skBytes), l.vk))
      case _ => Left(KesError.MalformedTree("eraseLeafSecretKey reached non-leaf, non-node tree element"))
    }

  /** Lift [[eraseLeafSecretKey]] to product SK. */
  protected[kes] def eraseProductLeafSk(key: SK): Either[KesError, SK] =
    eraseLeafSecretKey(key._2).map(newSub => (key._1, newSub, key._3, key._4))

  /** Evolve the product key to `step`.
    *
    *   - `step == 0` is a no-op.
    *   - When `step` crosses into a new super-period, the sub-tree is regenerated from `key._3` and the new sub-VK is signed by the
    *     (already-evolved) super-tree.
    *   - When `step` remains within the current super-period, only the sub-tree is evolved.
    */
  protected[kes] def updateKey(key: SK, step: Int): Either[KesError, SK] = {
    val keyTime = getKeyTime(key)
    val keyTimeSup = sumComposition.getKeyTime(key._1)
    val heightSup = sumComposition.getTreeHeight(key._1)
    val heightSub = sumComposition.getTreeHeight(key._2)
    val totalSteps = exp(heightSup + heightSub)
    val totalStepsSub = exp(heightSub)
    val newKeyTimeSup = step / totalStepsSub
    val newKeyTimeSub = step % totalStepsSub

    def getSeed(seeds: (Array[Byte], Array[Byte]), iter: Int): (Array[Byte], Array[Byte]) =
      if (iter < newKeyTimeSup) {
        val out = getSeed(prng(seeds._2), iter + 1)
        random.nextBytes(seeds._1)
        random.nextBytes(seeds._2)
        out
      } else seeds

    if (step == 0) Right(key)
    else if (step >= totalSteps) Left(KesError.StepBeyondMax(keyTime, step, totalSteps))
    else if (step <= keyTime) Left(KesError.StepNotMonotonic(keyTime, step))
    else if (keyTimeSup < newKeyTimeSup) {
      // Cross into a new super-period.
      sumComposition.eraseOldNode(key._2)
      val (s1, s2) = getSeed((Array.empty[Byte], key._3), keyTimeSup)
      for {
        evolvedSup <- sumComposition.updateKey(key._1, newKeyTimeSup)
        newSubScheme = sumComposition.generateSecretKey(s1, heightSub)
        _ = random.nextBytes(s1)
        kesVkSub = sumComposition.generateVerificationKey(newSubScheme)
        kesSigSuper = sumComposition.sign(evolvedSup, kesVkSub._1)
        forwardSecureSuperScheme <- eraseLeafSecretKey(evolvedSup)
        evolvedSubScheme <-
          if (newKeyTimeSub == 0) Right(newSubScheme)
          else sumComposition.updateKey(newSubScheme, newKeyTimeSub)
      } yield (forwardSecureSuperScheme, evolvedSubScheme, s2, kesSigSuper)
    } else {
      // Same super-period; only evolve the sub-tree.
      sumComposition.updateKey(key._2, newKeyTimeSub).map(newSub => (key._1, newSub, key._3, key._4))
    }
  }

  /** Sign `m` with the product key. The signature carries:
    *
    *   - the super-tree signature over the current sub-VK (already cached in `key._4`),
    *   - the sub-tree signature over `m`,
    *   - the sub-tree VK root (so the verifier can chain super → sub).
    */
  protected[kes] def sign(key: SK, m: Array[Byte]): SIG =
    (key._4, sumComposition.sign(key._2, m), sumComposition.generateVerificationKey(key._2)._1)

  /** Verify a product signature against a product VK. */
  protected[kes] def verify(kesSig: SIG, m: Array[Byte], kesVk: VK): Boolean = {
    val totalStepsSub = exp(kesSig._2._3.length)
    val keyTimeSup = kesVk._2 / totalStepsSub
    val keyTimeSub = kesVk._2 % totalStepsSub

    val verifySup = sumComposition.verify(kesSig._1, kesSig._3, (kesVk._1, keyTimeSup))
    val verifySub = sumComposition.verify(kesSig._2, m, (kesSig._3, keyTimeSub))

    verifySup && verifySub
  }
}
