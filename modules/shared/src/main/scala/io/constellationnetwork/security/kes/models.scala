package io.constellationnetwork.security.kes

import java.util.Arrays

/** Public key for the KES sum composition.
  *
  * @param value
  *   Root hash of the Merkle tree of leaf-keypair witnesses (32 bytes when using Blake2b-256).
  * @param step
  *   The time step the corresponding secret key was at when this VK was derived. A KES sum VK is implicitly bound to a
  *   specific step.
  */
final case class VerificationKeyKesSum(value: Array[Byte], step: Int) {

  override def hashCode(): Int = Arrays.hashCode(value) + step.hashCode

  override def equals(other: Any): Boolean = other match {
    case vk: VerificationKeyKesSum => value.sameElements(vk.value) && step == vk.step
    case _                         => false
  }
}

/** Public key for the KES product (super × sub) composition. */
final case class VerificationKeyKesProduct(value: Array[Byte], step: Int) {

  override def hashCode(): Int = Arrays.hashCode(value) + step.hashCode

  override def equals(other: Any): Boolean = other match {
    case vk: VerificationKeyKesProduct => value.sameElements(vk.value) && step == vk.step
    case _                             => false
  }
}

/** Signature of the KES sum composition.
  *
  * @param verificationKey
  *   Ed25519 leaf VK used for the underlying signature.
  * @param signature
  *   Raw Ed25519 signature (64 bytes).
  * @param witness
  *   Merkle authentication path from the leaf VK to the root. The verifier reconstructs the root and compares to the
  *   one carried in the [[VerificationKeyKesSum]].
  */
final case class SignatureKesSum(
  verificationKey: Array[Byte],
  signature: Array[Byte],
  witness: Seq[Array[Byte]]
) {

  override def hashCode(): Int =
    Arrays.hashCode(verificationKey) +
      Arrays.hashCode(signature) +
      witness.map(Arrays.hashCode(_: Array[Byte])).sum

  override def equals(other: Any): Boolean = other match {
    case s: SignatureKesSum =>
      verificationKey.sameElements(s.verificationKey) &&
        signature.sameElements(s.signature) &&
        witness.length == s.witness.length &&
        witness.zip(s.witness).forall { case (x, y) => x.sameElements(y) }
    case _ => false
  }
}

/** Signature of the KES product composition.
  *
  * @param superSignature
  *   Sum-scheme signature by the "super" (slow) tree over the sub-tree root.
  * @param subSignature
  *   Sum-scheme signature by the "sub" (fast) tree over the user message.
  * @param subRoot
  *   The sub-tree verification root that the super signature commits to. Carried explicitly so verifiers do not need
  *   the sub-tree secret state.
  */
final case class SignatureKesProduct(
  superSignature: SignatureKesSum,
  subSignature: SignatureKesSum,
  subRoot: Array[Byte]
) {

  override def hashCode(): Int =
    superSignature.hashCode() + subSignature.hashCode() + Arrays.hashCode(subRoot)

  override def equals(other: Any): Boolean = other match {
    case s: SignatureKesProduct =>
      superSignature == s.superSignature &&
        subSignature == s.subSignature &&
        subRoot.sameElements(s.subRoot)
    case _ => false
  }
}

/** Secret key for the KES sum composition. Wraps a [[KesBinaryTree]] and an offset (start time step).
  *
  * Note: mutable bytes are embedded in `tree`; instances must not be shared across phases of evolution.
  */
final case class SecretKeyKesSum(tree: KesBinaryTree, offset: Long)

/** Secret key for the KES product (super × sub) composition.
  *
  * @param superTree
  *   "Hour-hand" tree.
  * @param subTree
  *   "Minute-hand" tree.
  * @param nextSubSeed
  *   Seed material used to derive the next sub-tree once `subTree` is exhausted.
  * @param subSignature
  *   Super-tree signature over the current sub-tree's root; carried so we can re-issue product signatures without
  *   re-signing the super tree on every sign.
  * @param offset
  *   Start time step. The first usable step is `offset`.
  */
final case class SecretKeyKesProduct(
  superTree: KesBinaryTree,
  subTree: KesBinaryTree,
  nextSubSeed: Array[Byte],
  subSignature: SignatureKesSum,
  offset: Long
)
