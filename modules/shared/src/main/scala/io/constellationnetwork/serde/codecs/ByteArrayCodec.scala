package io.constellationnetwork.serde.codecs

import scodec.Codec
import scodec.codecs.{bytes, uint32, variableSizeBytes}

/** Canonical scodec codec for `Array[Byte]`.
  *
  * Wire format: 4-byte unsigned length prefix (uint32) + raw bytes.
  *
  * 4-byte prefix because data-application blobs can legitimately be megabytes. If consensus ever needs byte arrays larger than 4GB we'll
  * file a follow-up — current DataApplicationPart payloads are well under that.
  *
  * Consensus contract: FROZEN. 4-byte length + raw bytes.
  */
object ByteArrayCodec {

  implicit val codec: Codec[Array[Byte]] =
    variableSizeBytes(uint32.xmap[Int](_.toInt, _.toLong), bytes)
      .xmap[Array[Byte]](_.toArray, bv => scodec.bits.ByteVector.view(bv))
}
