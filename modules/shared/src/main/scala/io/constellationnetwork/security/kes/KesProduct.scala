package io.constellationnetwork.security.kes

/** Public wrapper for the KES product (super × sub) composition.
  *
  *   - This is the recommended consensus-facing KES API.
  *   - Forward security: signing at step `t` consumes the secret bytes specific to `t`; after evolution to `t' > t`, no signature for `t' <
  *     t` can be forged from the evolved state.
  *
  * Ported from Bifrost's `co.topl.crypto.signing.KesProduct` (credit: Aaron Schutza).
  *
  * Package-private: the consensus-facing entry point is [[OperationalKeyMakerAlgebra]] (F[_]-typed, lifecycle-managed, read-once
  * persistence). This raw imperative API is reachable only from within the `kes` package — primarily for tests and as the inner impl of
  * [[OperationalKeyMaker]]. Callers outside the package MUST use [[OperationalKeyMaker.make]].
  */
private[kes] class KesProduct extends ProductComposition {

  /** Generate a KES product keypair at step 0.
    *
    * @param seed
    *   Entropy; overwritten on return.
    * @param height
    *   `(heightSup, heightSub)` controlling the super × sub tree heights. Total supported steps: `2^(heightSup + heightSub)`.
    * @param offset
    *   Time-step offset embedded in the secret key.
    */
  def createKeyPair(
    seed: Array[Byte],
    height: (Int, Int),
    offset: Long
  ): (SecretKeyKesProduct, VerificationKeyKesProduct) = {
    val sk = generateSecretKey(seed, height._1, height._2)
    val pk = generateVerificationKey(sk)
    (
      SecretKeyKesProduct(
        sk._1,
        sk._2,
        sk._3,
        SignatureKesSum(sk._4._1, sk._4._2, sk._4._3),
        offset
      ),
      VerificationKeyKesProduct(pk._1, pk._2)
    )
  }

  /** Sign `message` with the active period of `privateKey`. */
  def sign(privateKey: SecretKeyKesProduct, message: Array[Byte]): SignatureKesProduct = {
    val prodSig = sign(unpackSecret(privateKey), message)
    SignatureKesProduct(
      SignatureKesSum(prodSig._1._1, prodSig._1._2, prodSig._1._3),
      SignatureKesSum(prodSig._2._1, prodSig._2._2, prodSig._2._3),
      prodSig._3
    )
  }

  /** Verify `signature` over `message` against `verifyKey`. */
  def verify(
    signature: SignatureKesProduct,
    message: Array[Byte],
    verifyKey: VerificationKeyKesProduct
  ): Boolean = {
    val prodSig = (
      (signature.superSignature.verificationKey, signature.superSignature.signature, signature.superSignature.witness.toVector),
      (signature.subSignature.verificationKey, signature.subSignature.signature, signature.subSignature.witness.toVector),
      signature.subRoot
    )
    val sumVk = (verifyKey.value, verifyKey.step)
    verify(prodSig, message, sumVk)
  }

  /** Evolve `privateKey` forward to time `steps`. Returns [[KesError]] if not strictly greater than the current step (and not 0), or if
    * past the maximum step expressible by the configured tree height.
    *
    * '''Read-once enforcement''': the implementation overwrites the bytes corresponding to past periods using a
    * [[java.security.SecureRandom]] before returning. See [[ProductComposition.eraseOldNode]] and
    * [[ProductComposition.eraseLeafSecretKey]].
    */
  def update(privateKey: SecretKeyKesProduct, steps: Int): Either[KesError, SecretKeyKesProduct] =
    updateKey(unpackSecret(privateKey), steps).map { sk =>
      SecretKeyKesProduct(
        sk._1,
        sk._2,
        sk._3,
        SignatureKesSum(sk._4._1, sk._4._2, sk._4._3),
        privateKey.offset
      )
    }

  /** Current step (in the product step space) of `privateKey`. */
  def getCurrentStep(privateKey: SecretKeyKesProduct): Int = getKeyTime(unpackSecret(privateKey))

  /** Maximum number of steps expressible by `privateKey`. */
  def getMaxStep(privateKey: SecretKeyKesProduct): Int = exp(
    getTreeHeight(privateKey.superTree) + getTreeHeight(privateKey.subTree)
  )

  /** Verification key at `privateKey`'s current step. */
  def getVerificationKey(privateKey: SecretKeyKesProduct): VerificationKeyKesProduct = {
    val vk = generateVerificationKey(unpackSecret(privateKey))
    VerificationKeyKesProduct(vk._1, vk._2)
  }

  private def unpackSecret(privateKey: SecretKeyKesProduct): SK =
    (
      privateKey.superTree,
      privateKey.subTree,
      privateKey.nextSubSeed,
      (privateKey.subSignature.verificationKey, privateKey.subSignature.signature, privateKey.subSignature.witness.toVector)
    )
}

private[kes] object KesProduct {

  /** A reusable instance. [[KesProduct]] is stateless apart from a private [[java.security.SecureRandom]] used for secret-byte overwrite
    * during evolution.
    */
  val instance: KesProduct = new KesProduct
}
