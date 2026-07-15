package io.constellationnetwork.security.kes

import java.io._

import scala.util.control.NonFatal

import org.bouncycastle.math.ec.rfc8032.Ed25519

/** Binary codec for [[SignatureKesProduct]] — the on-the-wire representation of a KES product signature.
  *
  *   - Streams via [[DataInputStream]] / [[DataOutputStream]] with 4-byte big-endian length prefixes for each byte array.
  *   - Matches the same low-level approach as [[SecretKeyCodec]] (which encodes the secret-key trees).
  *
  * No tag bytes are needed because the structure is fixed:
  *   - superSignature: [vk, sig, witness*] — same `SignatureKesSum` shape used inside the secret-key tree
  *   - subSignature: same
  *   - subRoot: a single byte array
  *
  * Package-private — the `SignatureKesSum` shape it encodes is internal. Cross-package callers (Slice 5/6 receivers in `dag-l0`) reach the
  * encode/decode via the [[OperationalKeyMaker]] forwarder methods, which keep the wire format scoped to this module.
  */
private[kes] object SignatureCodec {

  private[kes] val VerificationKeyBytes: Int = Ed25519.PUBLIC_KEY_SIZE
  private[kes] val SignatureBytes: Int = Ed25519.SIGNATURE_SIZE
  private[kes] val HashBytes: Int = 32

  private val MaxWitnessEntries: Int = 64
  private val EncodedSumBaseBytes: Int =
    Integer.BYTES + VerificationKeyBytes + Integer.BYTES + SignatureBytes + Integer.BYTES
  private val EncodedWitnessBytes: Int = Integer.BYTES + HashBytes
  private val MinEncodedSignatureBytes: Int = 2 * EncodedSumBaseBytes + Integer.BYTES + HashBytes
  private val MaxEncodedSignatureBytes: Int =
    MinEncodedSignatureBytes + 2 * MaxWitnessEntries * EncodedWitnessBytes

  /** Encode a product signature to its wire representation. The encoded length is small in practice: each `SignatureKesSum` is `vk (32B) +
    * sig (64B) + witness (~7 × 32B)` ≈ 320B for typical Bifrost-port heights; the product carries two of these plus a single root, so a
    * full product signature serializes to roughly 700 bytes. The 4-byte length prefixes add 4 × (2 + 1 + 2 × ⌈log₂(treeHeight)⌉) ≈ 60B of
    * overhead.
    */
  def encodeSignature(sig: SignatureKesProduct): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    encodeSumSig(dos, sig.superSignature)
    encodeSumSig(dos, sig.subSignature)
    encodeBytes(dos, sig.subRoot)
    dos.flush()
    baos.toByteArray
  }

  /** Decode a product signature from its wire representation. Returns `Left(KesError.MalformedTree)` on truncation, non-canonical field
    * sizes/counts, trailing bytes, or any other parse failure. The decoder checks the fixed Ed25519/Blake2b-256 shape before constructing a
    * signature, so length-prefix containers with empty keys/signatures can never reach Bouncy Castle.
    */
  def decodeSignature(bytes: Array[Byte]): Either[KesError, SignatureKesProduct] =
    if (bytes == null)
      malformed("input was null")
    else if (bytes.length < MinEncodedSignatureBytes || bytes.length > MaxEncodedSignatureBytes)
      malformed(
        s"encoded length ${bytes.length} outside canonical bounds [$MinEncodedSignatureBytes,$MaxEncodedSignatureBytes]"
      )
    else {
      val dis = new DataInputStream(new ByteArrayInputStream(bytes))
      try {
        val superSig = decodeSumSig(dis, "superSignature")
        val subSig = decodeSumSig(dis, "subSignature")
        val subRoot = decodeBytesExact(dis, HashBytes, "subRoot")
        if (dis.available() != 0)
          throw new IllegalStateException(s"trailing bytes=${dis.available()}")
        val signature = SignatureKesProduct(superSig, subSig, subRoot)
        validateSignatureOrThrow(signature)
        Right(signature)
      } catch {
        case NonFatal(error) => malformed(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
      }
    }

  /** Validate a decoded or directly constructed KES signature before invoking the pure cryptographic verifier. */
  private[kes] def validateSignature(signature: SignatureKesProduct): Either[KesError, Unit] =
    try {
      validateSignatureOrThrow(signature)
      Right(())
    } catch {
      case NonFatal(error) => malformed(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
    }

  private def encodeSumSig(dos: DataOutputStream, s: SignatureKesSum): Unit = {
    encodeBytes(dos, s.verificationKey)
    encodeBytes(dos, s.signature)
    dos.writeInt(s.witness.size)
    s.witness.foreach(encodeBytes(dos, _))
  }

  private def decodeSumSig(dis: DataInputStream, label: String): SignatureKesSum = {
    val vk = decodeBytesExact(dis, VerificationKeyBytes, s"$label.verificationKey")
    val sig = decodeBytesExact(dis, SignatureBytes, s"$label.signature")
    val n = dis.readInt()
    if (n < 0 || n > MaxWitnessEntries)
      throw new IllegalStateException(s"$label.witness count=$n outside [0,$MaxWitnessEntries]")
    val w = (0 until n).map(index => decodeBytesExact(dis, HashBytes, s"$label.witness[$index]")).toVector
    SignatureKesSum(vk, sig, w)
  }

  private def encodeBytes(dos: DataOutputStream, b: Array[Byte]): Unit = {
    dos.writeInt(b.length)
    dos.write(b)
  }

  private def decodeBytesExact(dis: DataInputStream, expected: Int, label: String): Array[Byte] = {
    val len = dis.readInt()
    if (len != expected)
      throw new IllegalStateException(s"$label length=$len expected=$expected")
    val buf = new Array[Byte](expected)
    dis.readFully(buf)
    buf
  }

  private def validateSignatureOrThrow(signature: SignatureKesProduct): Unit = {
    if (signature == null) throw new IllegalStateException("signature was null")
    validateSumSigOrThrow(signature.superSignature, "superSignature")
    validateSumSigOrThrow(signature.subSignature, "subSignature")
    requireExactLength(signature.subRoot, HashBytes, "subRoot")
  }

  private def validateSumSigOrThrow(signature: SignatureKesSum, label: String): Unit = {
    if (signature == null) throw new IllegalStateException(s"$label was null")
    requireExactLength(signature.verificationKey, VerificationKeyBytes, s"$label.verificationKey")
    requireExactLength(signature.signature, SignatureBytes, s"$label.signature")
    if (signature.witness == null)
      throw new IllegalStateException(s"$label.witness was null")
    if (signature.witness.size > MaxWitnessEntries)
      throw new IllegalStateException(s"$label.witness count=${signature.witness.size} exceeds $MaxWitnessEntries")
    signature.witness.zipWithIndex.foreach {
      case (witness, index) =>
        requireExactLength(witness, HashBytes, s"$label.witness[$index]")
    }
  }

  private def requireExactLength(bytes: Array[Byte], expected: Int, label: String): Unit =
    if (bytes == null) throw new IllegalStateException(s"$label was null")
    else if (bytes.length != expected)
      throw new IllegalStateException(s"$label length=${bytes.length} expected=$expected")

  private def malformed[A](detail: String): Either[KesError, A] =
    Left(KesError.MalformedTree(s"decodeSignature failed: $detail"))
}
