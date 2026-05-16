package io.constellationnetwork.security.kes

import java.io._

import scala.annotation.tailrec

import KesBinaryTree._

/** Binary codec for [[SecretKeyKesProduct]] and friends.
  *
  *   - Tag-byte prefix per [[KesBinaryTree]] variant (matches Bifrost's `nodeTypePrefix` / `leafTypePrefix` /
  *     `emptyTypePrefix` constants).
  *   - 4-byte big-endian length prefix for each byte array.
  *   - Streams via [[DataInputStream]] / [[DataOutputStream]] to keep the code simple and obvious.
  *
  * No dependency on Tessellation's Kryo or Circe machinery — the KES bytes are short, the format is stable, and
  * doing it ourselves avoids dragging serde concerns into a security-critical primitive.
  *
  * Intentionally does NOT use `scodec` even though `shared` depends on it: the read-once pattern demands tight control
  * over which Array[Byte] instances we hold onto, and a streaming codec library is the wrong abstraction.
  */
private[kes] object SecretKeyCodec {

  def encodeProductSk(sk: SecretKeyKesProduct): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(baos)
    encodeTree(dos, sk.superTree)
    encodeTree(dos, sk.subTree)
    encodeBytes(dos, sk.nextSubSeed)
    encodeSumSig(dos, sk.subSignature)
    dos.writeLong(sk.offset)
    dos.flush()
    baos.toByteArray
  }

  def decodeProductSk(bytes: Array[Byte]): Either[KesError, SecretKeyKesProduct] = {
    val dis = new DataInputStream(new ByteArrayInputStream(bytes))
    try {
      val superTree = decodeTree(dis)
      val subTree = decodeTree(dis)
      val nextSubSeed = decodeBytes(dis)
      val subSignature = decodeSumSig(dis)
      val offset = dis.readLong()
      Right(SecretKeyKesProduct(superTree, subTree, nextSubSeed, subSignature, offset))
    } catch {
      case e: Exception => Left(KesError.MalformedTree(s"decodeProductSk failed: ${e.getMessage}"))
    }
  }

  private def encodeTree(dos: DataOutputStream, t: KesBinaryTree): Unit = t match {
    case m: MerkleNode =>
      dos.writeByte(nodeTypePrefix.toInt)
      encodeBytes(dos, m.seed)
      encodeBytes(dos, m.witnessLeft)
      encodeBytes(dos, m.witnessRight)
      encodeTree(dos, m.left)
      encodeTree(dos, m.right)
    case l: SigningLeaf =>
      dos.writeByte(leafTypePrefix.toInt)
      encodeBytes(dos, l.sk)
      encodeBytes(dos, l.vk)
    case Empty() =>
      dos.writeByte(emptyTypePrefix.toInt)
  }

  private def decodeTree(dis: DataInputStream): KesBinaryTree = {
    val tag = dis.readByte()
    if (tag == nodeTypePrefix) {
      val seed = decodeBytes(dis)
      val witL = decodeBytes(dis)
      val witR = decodeBytes(dis)
      val left = decodeTree(dis)
      val right = decodeTree(dis)
      MerkleNode(seed, witL, witR, left, right)
    } else if (tag == leafTypePrefix) {
      val sk = decodeBytes(dis)
      val vk = decodeBytes(dis)
      SigningLeaf(sk, vk)
    } else if (tag == emptyTypePrefix) {
      Empty()
    } else {
      throw new IllegalStateException(s"Unknown KesBinaryTree tag: $tag")
    }
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
    readFully(dis, len)
  }

  @tailrec
  private def readFully(dis: DataInputStream, len: Int, acc: Array[Byte] = null, off: Int = 0): Array[Byte] = {
    val buf = if (acc == null) new Array[Byte](len) else acc
    if (off == len) buf
    else {
      val n = dis.read(buf, off, len - off)
      if (n < 0) throw new IllegalStateException("Unexpected EOF")
      readFully(dis, len, buf, off + n)
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
}
