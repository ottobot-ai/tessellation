package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.consensus._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.ProtocolEraIdCodec.{codec => protocolEraIdCodec}

import scodec.codecs.bytes
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Explicit canonical Scodec composition for the dark S1 context candidates.
  *
  * These codecs do not register a `ConsensusHashSchema`, activate a runtime reader/writer, or grant signing authority. The bootstrap codec
  * remains noncircular by encoding only network identity and protocol era.
  */
object ConsensusArtifactContextV1Codec {
  val IdBytes: Int = 32
  val BootstrapContextBytes: Long = IdBytes.toLong + 1L
  val ArtifactContextBytes: Long = IdBytes.toLong * 3L + 1L

  implicit val networkIdCodec: Codec[NetworkIdV1] =
    bytes(IdBytes).exmap(
      value =>
        NetworkIdV1
          .fromByteVector(value)
          .fold(error => Attempt.failure(Err(error.message)), Attempt.successful),
      value =>
        if (value eq null) Attempt.failure(Err("NetworkIdV1 cannot be null"))
        else
          NetworkIdV1
            .fromByteVector(value.toByteVector)
            .fold(error => Attempt.failure(Err(error.message)), valid => Attempt.successful(valid.toByteVector))
    )

  implicit val genesisIdCodec: Codec[GenesisIdV1] =
    bytes(IdBytes).exmap(
      value =>
        GenesisIdV1
          .fromByteVector(value)
          .fold(error => Attempt.failure(Err(error.message)), Attempt.successful),
      value =>
        if (value eq null) Attempt.failure(Err("GenesisIdV1 cannot be null"))
        else
          GenesisIdV1
            .fromByteVector(value.toByteVector)
            .fold(error => Attempt.failure(Err(error.message)), valid => Attempt.successful(valid.toByteVector))
    )

  implicit val consensusParametersIdCodec: Codec[ConsensusParametersIdV1] =
    bytes(IdBytes).exmap(
      value =>
        ConsensusParametersIdV1
          .fromByteVector(value)
          .fold(error => Attempt.failure(Err(error.message)), Attempt.successful),
      value =>
        if (value eq null) Attempt.failure(Err("ConsensusParametersIdV1 cannot be null"))
        else
          ConsensusParametersIdV1
            .fromByteVector(value.toByteVector)
            .fold(error => Attempt.failure(Err(error.message)), valid => Attempt.successful(valid.toByteVector))
    )

  implicit val bootstrapContextCodec: Codec[ConsensusBootstrapContextV1] =
    (networkIdCodec :: protocolEraIdCodec).exmap[ConsensusBootstrapContextV1](
      {
        case networkId :: protocolEraId :: HNil =>
          ConsensusBootstrapContextV1
            .from(networkId, protocolEraId)
            .fold(error => Attempt.failure(Err(error.message)), Attempt.successful)
      },
      value =>
        if (value eq null) Attempt.failure(Err("ConsensusBootstrapContextV1 cannot be null"))
        else
          ConsensusBootstrapContextV1
            .from(value.networkId, value.protocolEraId)
            .fold(
              error => Attempt.failure(Err(error.message)),
              valid => Attempt.successful(valid.networkId :: valid.protocolEraId :: HNil)
            )
    )

  implicit val artifactContextCodec: Codec[ConsensusArtifactContextV1] =
    (networkIdCodec :: genesisIdCodec :: protocolEraIdCodec :: consensusParametersIdCodec)
      .exmap[ConsensusArtifactContextV1](
        {
          case networkId :: genesisId :: protocolEraId :: parametersId :: HNil =>
            ConsensusArtifactContextV1
              .from(networkId, genesisId, protocolEraId, parametersId)
              .fold(error => Attempt.failure(Err(error.message)), Attempt.successful)
        },
        value =>
          if (value eq null) Attempt.failure(Err("ConsensusArtifactContextV1 cannot be null"))
          else
            ConsensusArtifactContextV1
              .from(value.networkId, value.genesisId, value.protocolEraId, value.parametersId)
              .fold(
                error => Attempt.failure(Err(error.message)),
                valid =>
                  Attempt.successful(
                    valid.networkId ::
                      valid.genesisId ::
                      valid.protocolEraId ::
                      valid.parametersId ::
                      HNil
                  )
              )
      )

  implicit val networkIdImmutableCodec: ImmutableCodec[NetworkIdV1] =
    ImmutableCodec.fromScodecCodec(networkIdCodec)

  implicit val genesisIdImmutableCodec: ImmutableCodec[GenesisIdV1] =
    ImmutableCodec.fromScodecCodec(genesisIdCodec)

  implicit val consensusParametersIdImmutableCodec: ImmutableCodec[ConsensusParametersIdV1] =
    ImmutableCodec.fromScodecCodec(consensusParametersIdCodec)

  implicit val bootstrapContextImmutableCodec: ImmutableCodec[ConsensusBootstrapContextV1] =
    ImmutableCodec.fromScodecCodec(bootstrapContextCodec)

  implicit val artifactContextImmutableCodec: ImmutableCodec[ConsensusArtifactContextV1] =
    ImmutableCodec.fromScodecCodec(artifactContextCodec)
}
