package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.nakamoto.GenesisOperatorConsensusKey
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NakamotoSlotCodecs.vrfPublicKeyCodec
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignatureCodecs.signatureCodec

import scodec.Codec
import scodec.codecs._
import shapeless.{::, HNil}

/** Canonical encoding for the rooted, immutable genesis operator-key identity record. */
object GenesisOperatorConsensusKeyCodec {
  private val networkMagicCodec: Codec[String] = variableSizeBytes(uint16, utf8)

  implicit val codec: Codec[GenesisOperatorConsensusKey] =
    (networkMagicCodec ::
      int64 ::
      int64 ::
      peerIdCodec ::
      addressCodec ::
      hexCodec ::
      int32 ::
      int64 ::
      vrfPublicKeyCodec ::
      signatureCodec)
      .xmap[GenesisOperatorConsensusKey](
        {
          case magic :: activation :: start :: peer :: address :: kesVk :: kesStep :: offset :: vrfVk :: signature :: HNil =>
            GenesisOperatorConsensusKey(magic, activation, start, peer, address, kesVk, kesStep, offset, vrfVk, signature)
        },
        record =>
          record.networkMagic ::
            record.activationOrdinal ::
            record.startingEpochProgress ::
            record.operatorPeerId ::
            record.operatorAddress ::
            record.kesMasterVerificationKey ::
            record.kesMasterVerificationKeyStep ::
            record.kesPeriodOffset ::
            record.vrfPublicKey ::
            record.longTermSignature ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[GenesisOperatorConsensusKey] = ImmutableCodec.fromScodecCodec(codec)
}
