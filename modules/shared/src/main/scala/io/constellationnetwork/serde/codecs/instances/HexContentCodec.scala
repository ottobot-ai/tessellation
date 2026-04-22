package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector
import scodec.codecs.{bytes, uint16, variableSizeBytes}
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codec for the `Hex` newtype — a variable-length byte string that, at the Scala level, is stored as a lowercase hex
  * ASCII string.
  *
  * Wire format: 2-byte length prefix (uint16) + raw bytes (not ASCII hex).
  *   - For a 64-byte uncompressed pubkey hex string (128 ASCII chars on JVM), the wire is 2 + 64 = 66 bytes.
  *   - For a 64-byte signature, similarly 66 bytes on the wire.
  *
  * Why NOT store as ASCII hex on the wire: would double the byte count for no gain. Raw bytes round-trip back to the same hex string on
  * decode (via `ByteVector.toHex`).
  *
  * Length prefix width: uint16 (max 65535 bytes). Ample for any current cryptographic primitive. If we ever encode a >64KB blob as Hex —
  * e.g. a large membership proof — we'll need a wider prefix; file a follow-up then.
  *
  * Named `HexContentCodec` to avoid shadowing with `HashCodec.codec` (which is a fixed 32-byte encoding for content-address hashes —
  * similar concept, different byte layout). `Hex` is the flexible variant; `Hash` is the fixed-32 variant. Both exist for good reasons.
  *
  * Consensus contract: FROZEN. 2-byte length prefix + raw bytes.
  */
object HexContentCodec {

  /** 2-byte length prefix + raw bytes. */
  implicit val codec: Codec[Hex] =
    variableSizeBytes(uint16, bytes).exmap(
      (bv: ByteVector) => Attempt.successful(Hex(bv.toHex)),
      (h: Hex) =>
        ByteVector
          .fromHexDescriptive(h.value)
          .fold(err => Attempt.failure(Err(s"Hex encode: bad hex string: $err")), Attempt.successful)
    )

  implicit val immutableCodec: ImmutableCodec[Hex] = ImmutableCodec.fromScodecCodec(codec)
}
