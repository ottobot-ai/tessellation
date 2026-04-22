package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import scodec.codecs.int32
import shapeless.{::, HNil}

/** Canonical scodec codecs for the Nakamoto slot-certificate family.
  *
  * Types (all hex newtypes + Slot + SlotCertificate):
  *   - `Slot` — NonNegLong wrapper (shape registered in `NewtypeLongShapes`).
  *   - `VrfProof`, `VrfOutput`, `VrfPublicKey` — Hex wrappers, length-prefixed raw bytes.
  *   - `SlotCertificate` — 8-field record.
  */
object NakamotoSlotCodecs {

  implicit val vrfProofCodec: Codec[VrfProof] =
    hexCodec.xmap[VrfProof](h => VrfProof(h), (p: VrfProof) => p.value: Hex)

  implicit val vrfProofImmutableCodec: ImmutableCodec[VrfProof] = ImmutableCodec.fromScodecCodec(vrfProofCodec)

  implicit val vrfOutputCodec: Codec[VrfOutput] =
    hexCodec.xmap[VrfOutput](h => VrfOutput(h), (o: VrfOutput) => o.value: Hex)

  implicit val vrfOutputImmutableCodec: ImmutableCodec[VrfOutput] = ImmutableCodec.fromScodecCodec(vrfOutputCodec)

  implicit val vrfPublicKeyCodec: Codec[VrfPublicKey] =
    hexCodec.xmap[VrfPublicKey](h => VrfPublicKey(h), (k: VrfPublicKey) => k.value: Hex)

  implicit val vrfPublicKeyImmutableCodec: ImmutableCodec[VrfPublicKey] = ImmutableCodec.fromScodecCodec(vrfPublicKeyCodec)

  private val slotCodec: Codec[Slot] = Codec[Slot]
  private val hashAliasCodec: Codec[Hash] = hashCodec

  implicit val slotCertificateCodec: Codec[SlotCertificate] =
    (slotCodec ::
      slotCodec ::
      vrfProofCodec ::
      vrfOutputCodec ::
      vrfPublicKeyCodec ::
      hashAliasCodec ::
      int32 ::
      hashAliasCodec)
      .xmap[SlotCertificate](
        {
          case s :: ps :: vp :: vo :: vpk :: eta :: aps :: aph :: HNil =>
            SlotCertificate(s, ps, vp, vo, vpk, eta, aps, aph)
        },
        c => c.slot :: c.parentSlot :: c.vrfProof :: c.vrfOutput :: c.vrfPublicKey :: c.eta :: c.activePoolSize :: c.activePoolHash :: HNil
      )

  implicit val slotCertificateImmutableCodec: ImmutableCodec[SlotCertificate] =
    ImmutableCodec.fromScodecCodec(slotCertificateCodec)
}
