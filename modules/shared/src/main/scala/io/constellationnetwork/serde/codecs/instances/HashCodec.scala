package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector
import scodec.codecs.bytes
import scodec.{Attempt, Codec, Err}

/** Canonical scodec codecs for content-address hash types (`Hash`, `ProofsHash`).
  *
  * Wire format: **32 raw bytes** — the underlying sha-256 digest. The existing Scala type `Hash(value: String)` stores the digest as a
  * 64-character lowercase hex string; the codec converts hex↔bytes at the boundary. That means:
  *   - Encode: take the 64-char hex string, parse to 32 bytes.
  *   - Decode: take 32 bytes, render as 64-char lowercase hex.
  *
  * This is a deliberate departure from legacy JSON / Kryo encodings, where hash bytes travelled as UTF-8-encoded ASCII hex (64 bytes per
  * hash). ScodecV1 halves hash storage cost and uses the same 32-byte representation as other content-address systems. Legacy source data
  * is outside this active codec and belongs in the isolated offline importer.
  *
  * Consensus contract: FROZEN. 32 bytes, length-prefixed? NO — we use a fixed-width 32 bytes; every `Hash` is exactly 32 bytes on the wire.
  * Any byte sequence of length != 32 is a decode failure.
  *
  * Goldens:
  *   - `Hash-scodec-v1.hex` — 32 zero bytes for `Hash.empty` (the empty-hash canary used throughout the codebase).
  */
object HashCodec {

  val HashByteLength: Int = 32
  private val HexCharLength: Int = HashByteLength * 2

  private val bytes32: Codec[ByteVector] = bytes(HashByteLength)

  /** Convert a 32-byte ByteVector → lowercase hex string of length 64. */
  private def toHex(bv: ByteVector): String = bv.toHex

  /** Parse a 64-char lowercase hex string → 32-byte ByteVector. Rejects malformed input (odd length, non-hex chars, or wrong length).
    */
  private def fromHex(s: String): Attempt[ByteVector] =
    if (s.length != HexCharLength)
      Attempt.failure(Err(s"Hash hex string must be exactly $HexCharLength chars, got ${s.length}"))
    else
      ByteVector
        .fromHexDescriptive(s)
        .fold(err => Attempt.failure(Err(s"Hash hex parse failed: $err")), Attempt.successful)

  /** 32 raw bytes on wire; `Hash(value: String)` where value is lowercase hex on JVM. */
  implicit val codec: Codec[Hash] =
    bytes32.exmap(
      (bv: ByteVector) => Attempt.successful(Hash(toHex(bv))),
      (h: Hash) => fromHex(h.value)
    )

  implicit val immutableCodec: ImmutableCodec[Hash] = ImmutableCodec.fromScodecCodec(codec)

  /** Same byte layout as `Hash` — `ProofsHash` is a parallel newtype around the same 64-hex-char representation.
    */
  implicit val proofsCodec: Codec[ProofsHash] =
    bytes32.exmap(
      (bv: ByteVector) => Attempt.successful(ProofsHash(toHex(bv))),
      (h: ProofsHash) => fromHex(h.value)
    )

  implicit val proofsImmutableCodec: ImmutableCodec[ProofsHash] = ImmutableCodec.fromScodecCodec(proofsCodec)
}
