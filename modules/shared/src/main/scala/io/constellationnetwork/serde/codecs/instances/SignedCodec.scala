package io.constellationnetwork.serde.codecs.instances

import cats.Order
import cats.data.NonEmptySet

import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.NonEmptySetCodec
import io.constellationnetwork.serde.codecs.instances.SignatureCodecs._

import scodec.Codec
import scodec.bits.ByteVector
import shapeless.{::, HNil}

/** Parameterized scodec codec for `Signed[T]` — the universal signed-envelope wrapper around any consensus type.
  *
  * Wire format: value (via `Codec[T]`) followed by `NonEmptySet[SignatureProof]` (2-byte-count length prefix + sorted proofs).
  *
  * Generic derivation: given a `Codec[T]` in scope, `Codec[Signed[T]]` and `ImmutableCodec[Signed[T]]` resolve automatically via the
  * `derivedCodec` and `derivedImmutableCodec` implicits below. Usage pattern at a call site:
  *
  * {{{
  *   import SignedCodec._
  *   import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._  // e.g. to get Codec[Balance]
  *
  *   // Codec[Signed[Balance]] resolves automatically.
  *   val signedBalance: Codec[Signed[Balance]] = Codec[Signed[Balance]]
  * }}}
  *
  * Why is this the first parameterized codec in the library? The `@newtype`/refined-types family I've built so far is all concrete (one
  * codec per Scala type). Signed is where the typeclass dispatch earns its keep: every existing consensus type T that gets signed —
  * Transaction, Block, StateChannelSnapshotBinary, plus all the future ones — reuses this single codec without any per-type boilerplate
  * beyond its own `Codec[T]`.
  *
  * Consensus contract: FROZEN. Value first, then sorted proofs with 2-byte count prefix. Changing the order, the count prefix width, or the
  * proof sort order breaks every historical signature re-verification.
  *
  * Determinism: proof ordering is derived from the canonical decoded bytes of the signer id and signature, not from the source `Hex`
  * string. `Hex` accepts mixed-case input but its wire codec decodes to lowercase; ordering the source strings would let case change the
  * encoded order and make strict decode reject an otherwise valid envelope. Two nodes serializing byte-identical proofs therefore produce
  * bit-identical bytes regardless of input hex case.
  */
object SignedCodec {

  private def canonicalHexValue(hex: Hex): String =
    ByteVector.fromHexDescriptive(hex.value).fold(_ => hex.value, _.toHex)

  private val canonicalSignatureProofOrder: Order[SignatureProof] =
    Order.by { proof =>
      (
        canonicalHexValue(proof.id.hex),
        canonicalHexValue(proof.signature.value)
      )
    }

  private val proofsCodec: Codec[NonEmptySet[SignatureProof]] =
    NonEmptySetCodec.nonEmptySet(signatureProofCodec)(canonicalSignatureProofOrder)

  /** Build `Codec[Signed[T]]` from an explicit `Codec[T]`. Call sites that can't rely on implicit resolution (e.g. inside other codec
    * definitions where local implicits shadow) use this directly.
    */
  def codecFor[T](valueCodec: Codec[T]): Codec[Signed[T]] =
    (valueCodec :: proofsCodec)
      .xmap[Signed[T]](
        { case v :: p :: HNil => Signed(v, p) },
        s => s.value :: s.proofs :: HNil
      )

  /** Derived `Codec[Signed[T]]` for any `T` with a `Codec[T]` in scope. The implicit chain means `Codec[Signed[Balance]]` resolves through
    * `Balance`'s shape-derivation + this helper with no extra setup.
    *
    * Uniquely named (`signedCodec`, not `derivedCodec`) so a consumer that also imports a different family's derivation doesn't get an
    * ambiguous-implicit error.
    */
  implicit def signedCodec[T](implicit valueCodec: Codec[T]): Codec[Signed[T]] =
    codecFor(valueCodec)

  /** Derived `ImmutableCodec[Signed[T]]`. This is the typeclass instance that hashing / signing-of-signed-payloads (e.g.
    * `Signed[Signed[T]]`) reaches for.
    */
  implicit def signedImmutableCodec[T](implicit valueCodec: Codec[T]): ImmutableCodec[Signed[T]] =
    ImmutableCodec.fromScodecCodec(signedCodec[T])
}
