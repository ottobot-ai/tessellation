package io.constellationnetwork.security.kes

import io.constellationnetwork.security.kes.KesBinaryTree.SigningLeaf

import weaver.SimpleIOSuite

/** Tests that prior-period secret bytes are erased after evolution.
  *
  * '''What "erased" means in this test''':
  *
  *   - We capture a REFERENCE to the `sk` byte array held by the active [[SigningLeaf]] before evolving.
  *   - After evolution, we check the bytes at that same reference are no longer the original seed bytes (they have
  *     been scrambled by [[SumComposition.eraseOldNode]] / [[ProductComposition.eraseLeafSecretKey]]).
  *
  * '''Caveats / honest limits''':
  *
  *   1. This is REFERENCE EQUALITY + CONTENT CHANGE, not "the bytes are cryptographically unrecoverable". We are
  *      verifying that the ERASER ran and overwrote the bytes we held a handle to. We are NOT verifying that the JVM
  *      heap is free of stale copies left by, e.g., generational GC compaction, JIT-resident inlined constants, or
  *      thread-local buffers in the underlying Ed25519 implementation. The eraser uses
  *      [[java.security.SecureRandom]] (NOT zero-fill) so the post-erase content is unpredictable; a heap-grep attack
  *      that looks for the original seed pattern would not find it AT THAT REFERENCE — but heap survival in copying
  *      GC is a known and unmitigatable JVM property. This matches the limitation in Bifrost.
  *   2. The test only inspects the ACTIVE leaf's `sk` field. Erasure of evicted Merkle nodes' `seed` /
  *      `witnessLeft` / `witnessRight` fields is exercised indirectly: if those bytes were NOT scrambled, evolving
  *      from the evicted subtree would still succeed, which would mean we could re-sign at the prior period. The
  *      [[KesProductSuite]] test "evolve to a strictly past step returns StepNotMonotonic" rejects that at the
  *      validated API; but the underlying scrub happens regardless and is exercised here.
  *   3. Content inspection uses `sameElements` on byte arrays. Reference identity uses `eq`.
  */
object ReadOnceEnforcementSuite extends SimpleIOSuite {

  /** Walk down the leftmost spine of a [[KesBinaryTree]] (the active leaf in a sum-composition state). */
  private def findActiveLeaf(t: KesBinaryTree): Option[SigningLeaf] = t match {
    case l: SigningLeaf => Some(l)
    case KesBinaryTree.MerkleNode(_, _, _, KesBinaryTree.Empty(), right) => findActiveLeaf(right)
    case KesBinaryTree.MerkleNode(_, _, _, left, KesBinaryTree.Empty())  => findActiveLeaf(left)
    case KesBinaryTree.MerkleNode(_, _, _, left, _)                      => findActiveLeaf(left)
    case _                                                               => None
  }

  pureTest("SumComposition: evolving destroys the secret bytes of the previous active leaf") {
    val kes = KesSum.instance
    val seed = Array.fill[Byte](32)(7)
    val (sk0, _) = kes.createKeyPair(seed.clone(), 2, 0L)

    findActiveLeaf(sk0.tree) match {
      case None => failure("expected an active leaf at step 0")
      case Some(leaf) =>
        // Capture a reference to the seed bytes AND a snapshot of their content.
        val skRef: Array[Byte] = leaf.sk
        val skSnapshot: Array[Byte] = leaf.sk.clone()

        // Evolve to step 1 — should scramble the prior leaf's sk bytes.
        kes.update(sk0, 1) match {
          case Left(err) => failure(s"evolve failed: $err")
          case Right(_) =>
            // The original leaf object's sk reference is unchanged ...
            val identityHeld = skRef eq leaf.sk
            // ... but its CONTENT must no longer match the original snapshot (the eraser scrambled it).
            val contentScrambled = !(skRef sameElements skSnapshot)
            expect.all(
              identityHeld,
              contentScrambled
            )
        }
    }
  }

  pureTest("ProductComposition: evolving across a super-period destroys prior sub-tree leaf bytes") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(17)
    // heightSub = 2 ⇒ sub-period boundaries at every 4 product steps.
    val (sk0, _) = kes.createKeyPair(seed.clone(), (2, 2), 0L)

    findActiveLeaf(sk0.subTree) match {
      case None => failure("expected an active sub-leaf at step 0")
      case Some(leaf) =>
        val skRef: Array[Byte] = leaf.sk
        val skSnapshot: Array[Byte] = leaf.sk.clone()

        // Evolve to step 4: crosses a super-period boundary, regenerating the sub-tree.
        kes.update(sk0, 4) match {
          case Left(err) => failure(s"evolve failed: $err")
          case Right(_) =>
            val identityHeld = skRef eq leaf.sk
            val contentScrambled = !(skRef sameElements skSnapshot)
            expect.all(identityHeld, contentScrambled)
        }
    }
  }

  pureTest("ProductComposition: signing at a strictly past period is rejected once the key has evolved") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(23)
    val (sk0, _) = kes.createKeyPair(seed.clone(), (2, 2), 0L) // max product steps = 16

    val sk5 = kes.update(sk0, 5).fold(e => sys.error(e.message), identity)
    // Attempting to evolve the evolved key BACK to a smaller step must fail.
    matches(kes.update(sk5, 3)) { case Left(KesError.StepNotMonotonic(5, 3)) => success }
  }

  pureTest("ProductComposition: evolving past max returns StepBeyondMax (cannot exceed key lifetime)") {
    val kes = KesProduct.instance
    val seed = Array.fill[Byte](32)(31)
    val (sk0, _) = kes.createKeyPair(seed.clone(), (1, 2), 0L) // max = 8
    matches(kes.update(sk0, 8)) { case Left(KesError.StepBeyondMax(0, 8, 8)) => success }
  }
}
