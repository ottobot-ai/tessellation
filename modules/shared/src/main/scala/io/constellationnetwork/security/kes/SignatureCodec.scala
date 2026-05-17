package io.constellationnetwork.security.kes

import java.io._

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
  * Package-private — the `SignatureKesSum` shape it encodes is internal. Cross-package callers (Slice 5/6 receivers in `dag-l0`)
  * reach the encode/decode via the [[OperationalKeyMaker]] forwarder methods, which keep the wire format scoped to this module.
  */
private[kes] object SignatureCodec {

  /** Encode a product signature to its wire representation. The encoded length is small in practice: each `SignatureKesSum` is
    * `vk (32B) + sig (64B) + witness (~7 × 32B)` ≈ 320B for typical Bifrost-port heights; the product carries two of these plus a
    * single root, so a full product signature serializes to roughly 700 bytes. The 4-byte length prefixes add 4 × (2 + 1 +
    * 2 × ⌈log₂(treeHeight)⌉) ≈ 60B of overhead.
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

  /** Decode a product signature from its wire representation. Returns `Left(KesError.MalformedTree)` on truncation, implausible
    * length fields, or any other parse failure. Implausibility bounds match [[SecretKeyCodec]] (16 MiB max per byte array, 64
    * max witness entries) to defend against length-prefix attacks; valid signatures sit far below both limits.
    */
  def decodeSignature(bytes: Array[Byte]): Either[KesError, SignatureKesProduct] = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val superSig = decodeSumSig(dis)
      val subSig = decodeSumSig(dis)
      val subRoot = decodeBytes(dis)
      Right(SignatureKesProduct(superSig, subSig, subRoot))
    } catch {
      case e: Exception => Left(KesError.MalformedTree(s"decodeSignature failed: ${e.getMessage}"))
    }
  }

  private def encodeSumSig(dos: DataOutputStream, s: SignatureKesSum): Unit = {
    encodeBytes(dos, s.verificationKey)
    encodeBytes(dos, s.signature)
    dos.writeInt(s.witness.size)
    s.witness.foreach(encodeBytes(dos, _))
  }

  private def decodeSumSig(dis: DataInputStream): SignatureKesSum = {
    val vk = decodeBytes(dis)
    val sig = decodeBytes(dis)
    val n = dis.readInt()
    if (n < 0 || n > 64) throw new IllegalStateException(s"decodeSumSig: implausible witness count $n")
    val w = (0 until n).map(_ => decodeBytes(dis)).toVector
    SignatureKesSum(vk, sig, w)
  }

  private def encodeBytes(dos: DataOutputStream, b: Array[Byte]): Unit = {
    dos.writeInt(b.length)
    dos.write(b)
  }

  private def decodeBytes(dis: DataInputStream): Array[Byte] = {
    val len = dis.readInt()
    if (len < 0 || len > (1 << 24)) {
      throw new IllegalStateException(s"decodeBytes: implausible length $len")
    }
    val buf = new Array[Byte](len)
    var off = 0
    while (off < len) {
      val n = dis.read(buf, off, len - off)
      if (n < 0) throw new IllegalStateException("Unexpected EOF")
      off += n
    }
    buf
  }
}
