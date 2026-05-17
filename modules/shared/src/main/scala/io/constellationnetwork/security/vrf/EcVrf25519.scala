package io.constellationnetwork.security.vrf

import java.security.MessageDigest

import cafe.cryptography.curve25519._

/** ECVRF-ED25519-SHA512-TAI implementation per draft-irtf-cfrg-vrf-10.
  *
  * This implements the version that includes zero_string (0x00) suffix in:
  *   - hash_to_curve input
  *   - hash_points input
  *   - proof_to_hash input
  *
  * Parameters:
  *   - suite_string = 0x03
  *   - EC group G = Ed25519 (RFC 8032 Table 1)
  *   - 2n = qLen = 32, cofactor = 8, ptLen = 32, n = 16 (c is 16 bytes), hLen = 64
  *   - Hash = SHA-512
  *   - Key derivation per RFC 8032 §5.1.5
  *   - Nonce generation: ECVRF_nonce_generation_RFC8032 (§5.4.2.2)
  *   - Hash to curve: try_and_increment (§5.4.1.1)
  *
  * Proof format: 80 bytes = Gamma (32) || c (16) || s (32)
  */
class EcVrf25519 {

  import EcVrf25519._

  /** Generate VRF proof.
    *
    * @param secretKey
    *   32-byte Ed25519 seed
    * @param message
    *   message to prove
    * @return
    *   80-byte proof (Gamma || c || s)
    */
  def vrfProof(secretKey: Array[Byte], message: Array[Byte]): Array[Byte] = {
    require(secretKey.length == 32, "Secret key must be 32 bytes")

    // 1. Derive x (secret scalar) and Y (public key) per RFC 8032 §5.1.5
    val hashedSk = sha512(secretKey)
    val scalar = pruneScalar(hashedSk.slice(0, 32))
    val x = scalarFromBytes32(scalar)
    val yPoint = Constants.ED25519_BASEPOINT_TABLE.multiply(x)
    val yBytes = pointToBytes(yPoint)

    // 2. H = ECVRF_hash_to_curve(suite_string, Y, alpha_string)
    val hPoint = hashToCurve(yBytes, message)

    // 3. h_string = point_to_string(H)
    val hBytes = pointToBytes(hPoint)

    // 4. Gamma = [x]*H
    val gammaPoint = hPoint.multiply(x)
    val gammaBytes = pointToBytes(gammaPoint)

    // 5. k = ECVRF_nonce_generation_RFC8032(SK, h_string)
    val k = nonceGeneration(secretKey, hBytes)

    // 6. c = ECVRF_hash_points(H, Gamma, [k]*B, [k]*H)
    val kB = Constants.ED25519_BASEPOINT_TABLE.multiply(k)
    val kH = hPoint.multiply(k)
    val c = hashPoints(hPoint, gammaPoint, kB, kH)

    // 7. s = (k + c*x) mod q
    val s = k.add(c.multiply(x))

    // 8. pi = point_to_string(Gamma) || int_to_string(c, 16) || int_to_string(s, 32)
    val proof = new Array[Byte](ProofBytes)
    System.arraycopy(gammaBytes, 0, proof, 0, PointBytes)
    val cBytes = c.toByteArray
    System.arraycopy(cBytes, 0, proof, PointBytes, CBytes)
    System.arraycopy(s.toByteArray, 0, proof, PointBytes + CBytes, ScalarBytes)
    proof
  }

  /** Verify VRF proof.
    *
    * @param publicKey
    *   32-byte Ed25519 public key
    * @param message
    *   the message
    * @param proof
    *   80-byte proof
    * @return
    *   true if valid
    */
  def vrfVerify(publicKey: Array[Byte], message: Array[Byte], proof: Array[Byte]): Boolean = {
    if (publicKey.length != PointBytes || proof.length != ProofBytes)
      return false

    try {
      // Decode public key Y
      val yPoint = bytesToPoint(publicKey).getOrElse(return false)

      // 1. Decode proof → (Gamma, c, s)
      val (gammaPoint, c, s) = decodeProof(proof).getOrElse(return false)

      // 2. H = ECVRF_hash_to_curve(suite_string, Y, alpha_string)
      val hPoint = hashToCurve(publicKey, message)

      // 3. U = [s]*B - [c]*Y = [s]*B + [(-c)]*Y
      val cNeg = Scalar.ZERO.subtract(c)
      val sB = Constants.ED25519_BASEPOINT_TABLE.multiply(s)
      val cY = yPoint.multiply(cNeg)
      val uPoint = sB.add(cY)

      // 4. V = [s]*H - [c]*Gamma = [s]*H + [(-c)]*Gamma
      val sH = hPoint.multiply(s)
      val cGamma = gammaPoint.multiply(cNeg)
      val vPoint = sH.add(cGamma)

      // 5. c' = ECVRF_hash_points(H, Gamma, U, V)
      val cPrime = hashPoints(hPoint, gammaPoint, uPoint, vPoint)

      // 6. If c == c' → VALID
      cPrimeEqualsC(c, cPrime)
    } catch {
      case _: Exception => false
    }
  }

  /** Extract VRF output hash from proof.
    *
    * @param proof
    *   80-byte proof
    * @return
    *   64-byte hash (beta) or None if invalid proof format
    */
  def vrfProofToHash(proof: Array[Byte]): Option[Array[Byte]] = {
    if (proof.length != ProofBytes) return None

    try {
      // Decode Gamma from proof
      val gammaBytes = proof.slice(0, PointBytes)
      val gammaPoint = bytesToPoint(gammaBytes).getOrElse(return None)

      // beta = SHA-512(suite_string || 0x03 || point_to_string(cofactor * Gamma) || 0x00)
      val cofactorGamma = gammaPoint.multiply(Cofactor)
      val cofactorGammaBytes = pointToBytes(cofactorGamma)

      val md = MessageDigest.getInstance("SHA-512")
      md.update(SuiteString)
      md.update(0x03.toByte) // proof_to_hash_domain_separator_front
      md.update(cofactorGammaBytes)
      md.update(0x00.toByte) // zero_string suffix (draft-10)
      Some(md.digest())
    } catch {
      case _: Exception => None
    }
  }

  /** Derive Ed25519 public key from secret key (32-byte seed).
    */
  def getVerificationKey(secretKey: Array[Byte]): Array[Byte] = {
    require(secretKey.length == 32, "Secret key must be 32 bytes")
    val hashedSk = sha512(secretKey)
    val scalar = pruneScalar(hashedSk.slice(0, 32))
    val x = scalarFromBytes32(scalar)
    pointToBytes(Constants.ED25519_BASEPOINT_TABLE.multiply(x))
  }
}

object EcVrf25519 {

  /** Shared stateless singleton. `EcVrf25519` carries no per-instance mutable state — all methods read their inputs and write to fresh
    * arrays — so a single process-wide instance suffices. Callers (`EligibilityChecker`, `CommitteeSortition`, `NakamotoProposer`,
    * `NakamotoSyncDaemon`) should use this rather than `new EcVrf25519()` to avoid pointless allocation. Tests may still `new EcVrf25519()`
    * for isolation but typically don't need to.
    */
  val default: EcVrf25519 = new EcVrf25519

  val SuiteString: Byte = 0x03
  val PointBytes: Int = 32
  val ScalarBytes: Int = 32
  val CBytes: Int = 16 // n = 16 for Ed25519
  val ProofBytes: Int = PointBytes + CBytes + ScalarBytes // 80 bytes

  // Cofactor = 8 as a Scalar
  private val Cofactor: Scalar = {
    val bytes = new Array[Byte](32)
    bytes(0) = 8
    Scalar.fromBits(bytes)
  }

  /** ECVRF_hash_to_curve_try_and_increment (§5.4.1.1)
    *
    * Tries to decode each hash as a curve point until successful. Input format: suite_string || one_string || PK_string || alpha_string ||
    * ctr_string || zero_string
    */
  private def hashToCurve(publicKey: Array[Byte], alpha: Array[Byte]): EdwardsPoint = {
    var ctr = 0
    while (ctr < 256) {
      val md = MessageDigest.getInstance("SHA-512")
      md.update(SuiteString)
      md.update(0x01.toByte) // one_string
      md.update(publicKey)
      md.update(alpha)
      md.update(ctr.toByte) // ctr_string
      md.update(0x00.toByte) // zero_string (draft-10)
      val hashResult = md.digest()

      // Try to decode first 32 bytes as a curve point
      val candidateBytes = hashResult.slice(0, 32)
      bytesToPoint(candidateBytes) match {
        case Some(point) =>
          // Check for identity point (all zeros when compressed)
          val compressed = pointToBytes(point)
          if (!compressed.forall(_ == 0)) {
            // Apply cofactor multiplication: H = [8]*H
            return point.multiply(Cofactor)
          }
        case None => // Invalid point, try next counter
      }
      ctr += 1
    }
    throw new RuntimeException("Failed to hash to curve after 256 attempts")
  }

  /** ECVRF_nonce_generation_RFC8032 (§5.4.2.2)
    *
    * k = SHA-512(hashed_sk[32..63] || h_string) mod q
    */
  private def nonceGeneration(secretKey: Array[Byte], hBytes: Array[Byte]): Scalar = {
    val hashedSk = sha512(secretKey)
    val truncated = hashedSk.slice(32, 64) // upper 32 bytes

    val md = MessageDigest.getInstance("SHA-512")
    md.update(truncated)
    md.update(hBytes)
    val kString = md.digest()

    // k = string_to_int(k_string) mod q (little-endian)
    Scalar.fromBytesModOrderWide(kString)
  }

  /** ECVRF_hash_points (§5.4.4)
    *
    * c = SHA-512(suite_string || 0x02 || P1 || P2 || P3 || P4 || 0x00)[0..15] as LE integer
    */
  private def hashPoints(
    p1: EdwardsPoint,
    p2: EdwardsPoint,
    p3: EdwardsPoint,
    p4: EdwardsPoint
  ): Scalar = {
    val md = MessageDigest.getInstance("SHA-512")
    md.update(SuiteString)
    md.update(0x02.toByte) // two_string
    md.update(pointToBytes(p1))
    md.update(pointToBytes(p2))
    md.update(pointToBytes(p3))
    md.update(pointToBytes(p4))
    md.update(0x00.toByte) // zero_string (draft-10)
    val hashResult = md.digest()

    // Take first 16 bytes (n = 16), interpret as LE integer
    // Pad to 32 bytes with zeros for Scalar construction
    val truncated = hashResult.slice(0, CBytes)
    val padded = new Array[Byte](32)
    System.arraycopy(truncated, 0, padded, 0, CBytes)
    Scalar.fromBits(padded)
  }

  /** Compare two c values (16-byte integers stored in Scalars). Only the first 16 bytes matter.
    */
  private def cPrimeEqualsC(c: Scalar, cPrime: Scalar): Boolean = {
    val cBytes = c.toByteArray
    val cPrimeBytes = cPrime.toByteArray
    // Compare first 16 bytes
    var i = 0
    while (i < CBytes) {
      if (cBytes(i) != cPrimeBytes(i)) return false
      i += 1
    }
    true
  }

  /** Decode an 80-byte proof into (Gamma, c, s).
    */
  private def decodeProof(proof: Array[Byte]): Option[(EdwardsPoint, Scalar, Scalar)] = {
    if (proof.length != ProofBytes) return None

    // Gamma = first 32 bytes
    val gammaBytes = proof.slice(0, PointBytes)
    val gammaPoint = bytesToPoint(gammaBytes).getOrElse(return None)

    // c = next 16 bytes (LE integer, padded to 32 bytes)
    val cBytes = proof.slice(PointBytes, PointBytes + CBytes)
    val cPadded = new Array[Byte](32)
    System.arraycopy(cBytes, 0, cPadded, 0, CBytes)
    val c = Scalar.fromBits(cPadded)

    // s = last 32 bytes
    val sBytes = proof.slice(PointBytes + CBytes, ProofBytes)
    val s = Scalar.fromBits(sBytes)

    Some((gammaPoint, c, s))
  }

  /** Apply RFC 8032 scalar pruning (clamping).
    */
  private def pruneScalar(scalar: Array[Byte]): Array[Byte] = {
    val pruned = scalar.clone()
    pruned(0) = (pruned(0) & 0xf8).toByte // Clear bottom 3 bits
    pruned(31) = (pruned(31) & 0x7f).toByte // Clear top bit
    pruned(31) = (pruned(31) | 0x40).toByte // Set second-highest bit
    pruned
  }

  /** Load a 32-byte scalar without reducing (already clamped).
    */
  private def scalarFromBytes32(bytes: Array[Byte]): Scalar =
    Scalar.fromBits(bytes)

  /** Convert EdwardsPoint to 32-byte compressed form.
    */
  private def pointToBytes(point: EdwardsPoint): Array[Byte] =
    point.compress().toByteArray

  /** Try to decompress a 32-byte representation to EdwardsPoint.
    */
  private def bytesToPoint(bytes: Array[Byte]): Option[EdwardsPoint] = {
    if (bytes.length != PointBytes) return None
    try
      Some(new CompressedEdwardsY(bytes).decompress())
    catch {
      case _: Exception => None
    }
  }

  /** SHA-512 helper.
    */
  private def sha512(input: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance("SHA-512").digest(input)
}
