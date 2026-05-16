package io.constellationnetwork.security.kes

/** Public wrapper for the KES sum composition.
  *
  * Exposes the named model types ([[SecretKeyKesSum]] / [[VerificationKeyKesSum]] / [[SignatureKesSum]]) instead of the
  * internal tuple-encoded representation used by [[SumComposition]].
  *
  * NOTE: the sum composition is a building block. For consensus-layer use, prefer [[KesProduct]] which provides a
  * larger number of time periods via the product (super × sub) composition.
  *
  * Ported from Bifrost's `co.topl.crypto.signing.KesSum` (credit: Aaron Schutza).
  */
class KesSum private[kes] () extends SumComposition {

  /** Generate a KES sum keypair at step 0.
    *
    * @param seed
    *   Entropy used to derive the tree's leaf keypairs. The bytes are overwritten with random data on return.
    * @param height
    *   Tree height; the resulting key supports `2^height` periods.
    * @param offset
    *   Time-step offset embedded in the secret key (the secret key remembers when it started; useful for aligning with
    *   wall-clock period numbers).
    */
  def createKeyPair(seed: Array[Byte], height: Int, offset: Long): (SecretKeyKesSum, VerificationKeyKesSum) = {
    val sk = generateSecretKey(seed, height)
    val pk = generateVerificationKey(sk)
    (SecretKeyKesSum(sk, offset), VerificationKeyKesSum(pk._1, pk._2))
  }

  /** Sign `message` with the active period of `privateKey`. */
  def sign(privateKey: SecretKeyKesSum, message: Array[Byte]): SignatureKesSum = {
    val sumSig = sign(privateKey.tree, message)
    SignatureKesSum(sumSig._1, sumSig._2, sumSig._3)
  }

  /** Verify `signature` over `message` against `verifyKey`. */
  def verify(signature: SignatureKesSum, message: Array[Byte], verifyKey: VerificationKeyKesSum): Boolean = {
    val sumSig = (signature.verificationKey, signature.signature, signature.witness.toVector)
    val sumVk = (verifyKey.value, verifyKey.step)
    verify(sumSig, message, sumVk)
  }

  /** Evolve `privateKey` forward to time `steps`. Returns [[KesError]] if the step is past the maximum or not
    * monotonically increasing.
    */
  def update(privateKey: SecretKeyKesSum, steps: Int): Either[KesError, SecretKeyKesSum] =
    updateKey(privateKey.tree, steps).map(t => privateKey.copy(tree = t))

  /** Current step of `privateKey` (relative to its tree, not its offset). */
  def getCurrentStep(privateKey: SecretKeyKesSum): Int = getKeyTime(privateKey.tree)

  /** Total number of steps expressible by this key. */
  def getMaxStep(privateKey: SecretKeyKesSum): Int = exp(getTreeHeight(privateKey.tree))

  /** Verification key at the key's current step. */
  def getVerificationKey(privateKey: SecretKeyKesSum): VerificationKeyKesSum = {
    val vk = generateVerificationKey(privateKey.tree)
    VerificationKeyKesSum(vk._1, vk._2)
  }
}

object KesSum {

  /** A reusable instance. [[KesSum]] is stateless apart from a private [[java.security.SecureRandom]] used for
    * secret-byte overwrite during evolution.
    */
  val instance: KesSum = new KesSum
}
