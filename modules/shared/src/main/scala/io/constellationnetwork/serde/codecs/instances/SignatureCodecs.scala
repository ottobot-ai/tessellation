package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec

import scodec.Codec
import shapeless.{::, HNil}

/** Codecs for the signature bundle types:
  *   - `Id` — newtype wrapping `Hex` (peer identity / public key as hex string).
  *   - `Signature` — newtype wrapping `Hex` (signature bytes as hex string).
  *   - `SignatureProof` — `(Id, Signature)` compound.
  *
  * Wire format for `Id` / `Signature`: delegates to the `Hex` codec's
  * length-prefixed raw-bytes encoding. Different pubkey/signature schemes
  * produce different byte lengths (secp256k1 uncompressed pubkey: 64 bytes;
  * ECDSA DER signature: up to ~72 bytes), which is why length-prefix rather
  * than fixed-width.
  *
  * No generic `HexNewtype[T]` shape typeclass here — unlike
  * `NonNegLongNewtype`, there are only two hex-based newtypes (Id, Signature)
  * and their codecs are one-line `xmap` calls anyway. Extract a shape helper
  * only when a third hex newtype appears.
  *
  * Consensus contract: FROZEN. `Id` and `Signature` both serialize via
  * `HexContentCodec`; any change to that helper's wire format is a consensus
  * break for these types.
  */
object SignatureCodecs {

  import HexContentCodec.{codec => hexCodec}

  implicit val idCodec: Codec[Id] =
    hexCodec.xmap[Id](h => Id(h), id => id.hex)

  implicit val idImmutableCodec: ImmutableCodec[Id] = ImmutableCodec.fromScodecCodec(idCodec)

  implicit val signatureCodec: Codec[Signature] =
    hexCodec.xmap[Signature](Signature(_), (s: Signature) => s.value)

  implicit val signatureImmutableCodec: ImmutableCodec[Signature] =
    ImmutableCodec.fromScodecCodec(signatureCodec)

  /** Compound: 2-byte-prefixed hex (Id) + 2-byte-prefixed hex (Signature). */
  implicit val signatureProofCodec: Codec[SignatureProof] =
    (idCodec :: signatureCodec)
      .xmap[SignatureProof](
        { case id :: sig :: HNil => SignatureProof(id, sig) },
        p => p.id :: p.signature :: HNil
      )

  implicit val signatureProofImmutableCodec: ImmutableCodec[SignatureProof] =
    ImmutableCodec.fromScodecCodec(signatureProofCodec)
}

/** Hex newtype unwrapper — needed because `Id` is `@newtype`, so extracting
  * `.hex` requires import of `newtype.ops`. Kept in the codec file for locality.
  */
private[instances] object SignatureCodecsNewtypeOps {
  import io.estatico.newtype.ops._
  def idHex(id: Id): Hex = id.coerce[Hex]
  def signatureHex(s: Signature): Hex = s.coerce[Hex]
}
