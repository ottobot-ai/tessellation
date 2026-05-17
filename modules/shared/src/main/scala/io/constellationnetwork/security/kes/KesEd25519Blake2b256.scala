package io.constellationnetwork.security.kes

import org.bouncycastle.crypto.digests.Blake2bDigest
import org.bouncycastle.math.ec.rfc8032.Ed25519

import KesBinaryTree._

/** Base helpers for the Ed25519 + Blake2b-256 instantiation of the MMM forward-secure signature construction.
  *
  * Ports Bifrost's `co.topl.crypto.signing.kes.KesEd25519Blake2b256` to use:
  *
  *   - BouncyCastle's `org.bouncycastle.math.ec.rfc8032.Ed25519` instead of Bifrost's wrapper, which has the same
  *     `generatePublicKey`/`sign`/`verify` static signatures (`(sk, 0, pk, 0)`, `(sk, 0, msg, 0, msgLen, sig, 0)`, `(sig, 0, pk, 0, msg, 0,
  *     msgLen)`).
  *   - BouncyCastle's `Blake2bDigest(256)` for the hash function. The MMM construction uses a 32-byte digest, which is what Blake2b-256
  *     produces.
  *
  * Both choices keep the output bit-for-bit compatible with Bifrost so test vectors from `KesProductSpec` can be reused once we wire them.
  *
  * This class is the building block for [[SumComposition]] (and through it, [[ProductComposition]]); end-users should use `KesSum` /
  * `KesProduct` instead.
  */
protected[kes] trait KesEd25519Blake2b256 {

  type SIG
  type VK
  type SK

  protected val pkBytes: Int = Ed25519.PUBLIC_KEY_SIZE
  protected val skBytes: Int = Ed25519.SECRET_KEY_SIZE
  protected val sigBytes: Int = Ed25519.SIGNATURE_SIZE
  protected val hashBytes: Int = 32

  /** Blake2b-256 of `input`. The MMM construction uses a 32-byte digest. */
  protected val hash: Array[Byte] => Array[Byte] = { (input: Array[Byte]) =>
    val digest = new Blake2bDigest(256)
    val output = new Array[Byte](hashBytes)
    digest.update(input, 0, input.length)
    digest.doFinal(output, 0)
    output
  }

  /** 2^n. */
  protected def exp(n: Int): Int = 1 << n

  /** Pseudorandom function used to derive child seeds from a parent seed.
    *
    *   - `r1 = H(0x00 || seed)`
    *   - `r2 = H(0x01 || seed)`
    *
    * Each output is collision-resistant w.r.t. the input seed and independent of the other.
    */
  protected def prng(seed: Array[Byte]): (Array[Byte], Array[Byte]) = {
    val left = new Array[Byte](seed.length + 1)
    left(0) = 0x00.toByte
    System.arraycopy(seed, 0, left, 1, seed.length)

    val right = new Array[Byte](seed.length + 1)
    right(0) = 0x01.toByte
    System.arraycopy(seed, 0, right, 1, seed.length)

    (hash(left), hash(right))
  }

  /** Generate an Ed25519 keypair from a 32-byte seed. The first 32 bytes of `seed` (or all if shorter) are used; the returned `sk` is the
    * seed clone, the `pk` is derived.
    */
  protected def sGenKeypair(seed: Array[Byte]): (Array[Byte], Array[Byte]) = {
    val pk = new Array[Byte](pkBytes)
    val sk = seed.clone()
    Ed25519.generatePublicKey(sk, 0, pk, 0)
    (sk, pk)
  }

  /** Sign `m` with the Ed25519 secret key `sk`. */
  protected def sSign(m: Array[Byte], sk: Array[Byte]): Array[Byte] = {
    val signature = new Array[Byte](sigBytes)
    Ed25519.sign(sk, 0, m, 0, m.length, signature, 0)
    signature
  }

  /** Verify an Ed25519 signature. */
  protected def sVerify(m: Array[Byte], signature: Array[Byte], pk: Array[Byte]): Boolean =
    Ed25519.verify(signature, 0, pk, 0, m, 0, m.length)

  /** Tree height. A single leaf has height 0; a `MerkleNode(_, _, _, leaf, _)` has height 1; etc. The empty tree has height -1 (matches
    * Bifrost's behaviour: `loop(empty) = 0` then minus 1).
    */
  def getTreeHeight(tree: KesBinaryTree): Int = {
    def loop(t: KesBinaryTree): Int = t match {
      case n: MerkleNode  => Seq(loop(n.left), loop(n.right)).max + 1
      case _: SigningLeaf => 1
      case Empty()        => 0
    }
    loop(tree) - 1
  }

  /** Compute the witness (subtree-root commitment) for a node:
    *
    *   - `H(witnessLeft || witnessRight)` for an interior node,
    *   - `H(vk)` for a signing leaf,
    *   - all-zero bytes for an empty subtree.
    */
  def witness(tree: KesBinaryTree): Array[Byte] = tree match {
    case MerkleNode(_, witnessLeft, witnessRight, _, _) =>
      val concat = new Array[Byte](witnessLeft.length + witnessRight.length)
      System.arraycopy(witnessLeft, 0, concat, 0, witnessLeft.length)
      System.arraycopy(witnessRight, 0, concat, witnessLeft.length, witnessRight.length)
      hash(concat)
    case SigningLeaf(_, vk) => hash(vk)
    case Empty()            => new Array[Byte](hashBytes)
  }
}
