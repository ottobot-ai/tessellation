package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import scodec.codecs.{int32, int64, vectorOfN}
import shapeless.{::, HNil}

/** Canonical scodec codecs for the Nakamoto slot-certificate family.
  *
  * Types (all hex newtypes + Slot + SlotCertificate):
  *   - `Slot` — NonNegLong wrapper (shape registered in `NewtypeLongShapes`).
  *   - `VrfProof`, `VrfOutput`, `VrfPublicKey` — Hex wrappers, length-prefixed raw bytes.
  *   - `SlotCertificate` — 9-field record (8 VRF/pool fields + `subchainLevelCounts` for §3 NIPoPoW header observation).
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

  /** Fixed-width 9-element vector of int64 for `SlotCertificate.subchainLevelCounts`. Size matches `SuperLevelParams.SuperLevelCount` in
    * `node-shared`; we keep the literal here (in `shared`) to avoid pulling a `node-shared` dependency into the codec layer. Any size
    * mismatch on encode will throw at scodec layer; decode of a longer/shorter wire payload is impossible because the width is constant.
    */
  private val subchainLevelCountsCodec: Codec[Vector[Long]] = vectorOfN(scodec.codecs.provide(9), int64)

  implicit val slotCertificateCodec: Codec[SlotCertificate] =
    (slotCodec ::
      slotCodec ::
      vrfProofCodec ::
      vrfOutputCodec ::
      vrfPublicKeyCodec ::
      hashAliasCodec ::
      int32 ::
      hashAliasCodec ::
      subchainLevelCountsCodec)
      .xmap[SlotCertificate](
        {
          case s :: ps :: vp :: vo :: vpk :: eta :: aps :: aph :: slc :: HNil =>
            SlotCertificate(s, ps, vp, vo, vpk, eta, aps, aph, slc)
        },
        c =>
          c.slot :: c.parentSlot :: c.vrfProof :: c.vrfOutput :: c.vrfPublicKey :: c.eta :: c.activePoolSize :: c.activePoolHash ::
            c.subchainLevelCounts :: HNil
      )

  implicit val slotCertificateImmutableCodec: ImmutableCodec[SlotCertificate] =
    ImmutableCodec.fromScodecCodec(slotCertificateCodec)
}
