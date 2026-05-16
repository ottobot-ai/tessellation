package io.constellationnetwork.security.kes

import weaver.SimpleIOSuite

/** Tests for the bespoke binary codec used to persist KES product secret keys.
  *
  *   - Encode + decode round-trip yields a structurally identical key.
  *   - The decoded key can be used to sign messages and produce signatures that verify against the original VK.
  */
object SecretKeyCodecSuite extends SimpleIOSuite {

  private def assertTreeEqual(t1: KesBinaryTree, t2: KesBinaryTree): weaver.Expectations = (t1, t2) match {
    case (KesBinaryTree.Empty(), KesBinaryTree.Empty()) => success
    case (l1: KesBinaryTree.SigningLeaf, l2: KesBinaryTree.SigningLeaf) =>
      expect.all(l1.sk.sameElements(l2.sk), l1.vk.sameElements(l2.vk))
    case (n1: KesBinaryTree.MerkleNode, n2: KesBinaryTree.MerkleNode) =>
      expect
        .all(
          n1.seed.sameElements(n2.seed),
          n1.witnessLeft.sameElements(n2.witnessLeft),
          n1.witnessRight.sameElements(n2.witnessRight)
        )
        .and(assertTreeEqual(n1.left, n2.left))
        .and(assertTreeEqual(n1.right, n2.right))
    case _ => failure(s"tree shapes differ: $t1 vs $t2")
  }

  pureTest("product SK encode + decode round-trip at step 0") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(51)
    val (sk, _) = kes.createKeyPair(seed, (2, 2), 7L)
    val encoded = SecretKeyCodec.encodeProductSk(sk)
    SecretKeyCodec.decodeProductSk(encoded) match {
      case Left(err) => failure(s"decode failed: $err")
      case Right(decoded) =>
        assertTreeEqual(sk.superTree, decoded.superTree)
          .and(assertTreeEqual(sk.subTree, decoded.subTree))
          .and(
            expect.all(
              sk.nextSubSeed.sameElements(decoded.nextSubSeed),
              sk.subSignature == decoded.subSignature,
              sk.offset == decoded.offset
            )
          )
    }
  }

  pureTest("decoded key produces signatures equivalent to the original") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(52)
    val (sk, vk) = kes.createKeyPair(seed, (2, 2), 0L)
    val evolved = kes.update(sk, 9).fold(e => sys.error(e.message), identity)

    val encoded = SecretKeyCodec.encodeProductSk(evolved)
    val decoded = SecretKeyCodec.decodeProductSk(encoded).fold(e => sys.error(e.message), identity)

    val msg = "round-trip".getBytes("UTF-8")
    val sigOrig = kes.sign(evolved, msg)
    val sigCopy = kes.sign(decoded, msg)
    val vkAfter = kes.getVerificationKey(decoded)

    expect.all(
      kes.verify(sigOrig, msg, vkAfter),
      kes.verify(sigCopy, msg, vkAfter),
      // Same VK before vs after roundtrip (modulo step).
      vkAfter.value.sameElements(kes.getVerificationKey(evolved).value),
      vkAfter.step == 9
    )
  }
}
