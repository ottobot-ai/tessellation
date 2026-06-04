package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.{AbsenceWitness, SmtProof, SmtSibling}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ListCodec.list
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}

import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs._
import shapeless.{::, HNil}

/** Canonical, EXPLICIT scodec codec for the native sparse-Merkle-tree proof [[SmtProof]] (Slice B — carried per-ordinal on the gl0 → ml0
  * changeset wire so the follower can verify the signed `smtRoot` by construction).
  *
  * Hand-written, NO auto-derivation (project HARD RULE for crypto wire — the byte layout is a frozen, audited spec). The layout mirrors the
  * `discriminated[..].by(uint8)` pattern used across the codec package (e.g. [[GlobalStateKeyCodec]], [[CurrencyAtomCodecs]]): a 1-byte
  * variant tag selects the body codec, re-emitted on encode.
  *
  * `SmtProof` discriminator:
  *   - 0x00 [[SmtProof.Inclusion]] — `key` ([[HexContentCodec]], uint16-prefixed bytes) :: `value` (uint16-prefixed RAW bytes — NOT the
  *     uint32 [[io.constellationnetwork.serde.codecs.ByteArrayCodec]]: an SMT leaf value is a 32-byte commitment digest, well under 64KB)
  *     :: `valueDigest` ([[HashCodec]], fixed 32 bytes) :: `siblings` (List via [[ListCodec]], each an [[SmtSibling]] = one `Hash`).
  *   - 0x01 [[SmtProof.Absence]] — `key` :: `witness` ([[AbsenceWitness]], 1-byte discriminator) :: `siblings`.
  *
  * `AbsenceWitness` discriminator:
  *   - 0x00 [[AbsenceWitness.Default]] — no body.
  *   - 0x01 [[AbsenceWitness.OtherLeaf]] — `occupyingKey` ([[HexContentCodec]]) :: `occupyingDataDigest` ([[HashCodec]]).
  *
  * `SmtSibling` is a single `Hash` (the sibling subtree digest). A `Hash.empty` sibling (collapsed default subtree) is encoded as 32 zero
  * bytes — the slot is kept explicit so the sibling index lines up with the position bit index (same contract as the JSON form).
  *
  * Consensus contract: this is a TRANSPORT codec, not a signing/hashing preimage — the cryptographic anchor is the SIGNED `smtRoot`,
  * against which the follower folds the decoded proof (the leaf-binding + `SmtVerifier` fold). The encoding is nonetheless frozen
  * byte-for-byte like every other scodec codec.
  */
object SmtProofCodec {

  /** One sibling = one 32-byte `Hash`. */
  private val siblingCodec: Codec[SmtSibling] =
    hashCodec.xmap[SmtSibling](SmtSibling(_), _.digest)

  /** Top-down authentication path. */
  private val siblingsCodec: Codec[List[SmtSibling]] = list(siblingCodec)

  /** uint16-length-prefixed RAW value bytes (Array[Byte]). Distinct from `ByteArrayCodec` (uint32): an SMT leaf value is a 32-byte
    * commitment digest, never multi-megabyte, so a 2-byte prefix is sufficient and byte-frugal.
    */
  private val valueBytesCodec: Codec[Array[Byte]] =
    variableSizeBytes(uint16, bytes).xmap[Array[Byte]](_.toArray, bv => ByteVector.view(bv))

  // ---- AbsenceWitness -------------------------------------------------------

  private val otherLeafCodec: Codec[AbsenceWitness.OtherLeaf] =
    (hexCodec :: hashCodec).xmap[AbsenceWitness.OtherLeaf](
      { case k :: d :: HNil => AbsenceWitness.OtherLeaf(k, d) },
      w => w.occupyingKey :: w.occupyingDataDigest :: HNil
    )

  val absenceWitnessCodec: Codec[AbsenceWitness] =
    discriminated[AbsenceWitness]
      .by(uint8)
      .typecase(0, provide(AbsenceWitness.Default))
      .typecase(1, otherLeafCodec)

  // ---- SmtProof -------------------------------------------------------------

  private val inclusionCodec: Codec[SmtProof.Inclusion] =
    (hexCodec :: valueBytesCodec :: hashCodec :: siblingsCodec).xmap[SmtProof.Inclusion](
      { case key :: value :: valueDigest :: siblings :: HNil => SmtProof.Inclusion(key, value, valueDigest, siblings) },
      p => p.key :: p.value :: p.valueDigest :: p.siblings :: HNil
    )

  private val absenceCodec: Codec[SmtProof.Absence] =
    (hexCodec :: absenceWitnessCodec :: siblingsCodec).xmap[SmtProof.Absence](
      { case key :: witness :: siblings :: HNil => SmtProof.Absence(key, witness, siblings) },
      p => p.key :: p.witness :: p.siblings :: HNil
    )

  implicit val codec: Codec[SmtProof] =
    discriminated[SmtProof]
      .by(uint8)
      .typecase(0, inclusionCodec)
      .typecase(1, absenceCodec)

  implicit val immutableCodec: ImmutableCodec[SmtProof] = ImmutableCodec.fromScodecCodec(codec)

  // Keep witness-type imports referenced for clarity of the body shapes above.
  private val _hashWitness: Codec[Hash] = hashCodec
  private val _hexWitness: Codec[Hex] = hexCodec
  locally { val _ = (_hashWitness, _hexWitness) }
}
