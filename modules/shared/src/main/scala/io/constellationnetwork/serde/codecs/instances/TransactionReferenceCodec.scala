package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.NonNegLongNewtype

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `TransactionReference` — first compound type in
  * the scodec era.
  *
  * Wire layout (fixed-width, total 40 bytes):
  *   - bytes  0..7  : ordinal   (TransactionOrdinal → 8 bytes big-endian int64)
  *   - bytes  8..39 : hash      (Hash              → 32 raw bytes)
  *
  * Because both fields are fixed-width, byte-offset random access works on the
  * encoded record: jump to offset 8 to read the hash without touching the
  * ordinal. This is the property the MPT-as-primary workstream wants for leaf
  * records. Adding a variable-length field to `TransactionReference` would
  * break that and require this codec file to be rewritten.
  *
  * Compound codec pattern — no reflection / no derivation. The `::` HList
  * syntax from shapeless wires field codecs in an explicit order. A refactor
  * that reorders fields requires editing this file to match, making silent
  * byte drift impossible.
  *
  * Consensus contract: FROZEN. 40 bytes, ordinal-then-hash, fixed widths.
  *
  * Golden: `TransactionReference-scodec-v1.hex`.
  */
object TransactionReferenceCodec {

  // Explicit references — we pull Codec[TransactionOrdinal] via the
  // NonNegLongNewtype derivation and Codec[Hash] from HashCodec. Keeping
  // these bindings explicit (not relying on a wildcard import + implicit
  // search) avoids name-shadowing confusion when this file also defines
  // `implicit val codec: Codec[TransactionReference]`.
  private val ordinalCodec: Codec[TransactionOrdinal] =
    NonNegLongNewtype.derivedCodec[TransactionOrdinal](NewtypeLongShapes.transactionOrdinalShape)
  private val hashCodec: Codec[Hash] = HashCodec.codec

  implicit val codec: Codec[TransactionReference] =
    (ordinalCodec :: hashCodec)
      .xmap[TransactionReference](
        { case o :: h :: HNil => TransactionReference(o, h) },
        t => t.ordinal :: t.hash :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[TransactionReference] =
    ImmutableCodec.fromScodecCodec(codec)
}
