package io.constellationnetwork.serde

import java.lang.reflect.{Constructor, InvocationTargetException}

import scala.util.Try

import io.constellationnetwork.schema.consensus._
import io.constellationnetwork.schema.era.ProtocolEraId
import io.constellationnetwork.schema.era.ProtocolEraId.ScodecV1
import io.constellationnetwork.security.ConsensusHashSchema
import io.constellationnetwork.serde.codecs.instances.ConsensusArtifactContextV1Codec._

import scodec.bits.{BitVector, ByteVector}
import weaver.FunSuite

object ConsensusArtifactContextV1CodecSuite extends FunSuite {

  private sealed trait NotGiven[A]
  private object NotGiven {
    implicit def absent[A]: NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent1[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent2[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
  }

  private def required[A](value: Either[Any, A]): A =
    value.fold(error => throw new Error(error.toString), identity)

  private def filled(length: Int, value: Int): ByteVector =
    ByteVector.fill(length.toLong)(value.toByte)

  private def invoke[A](constructor: Constructor[A], arguments: AnyRef*): Either[Throwable, A] =
    Try(constructor.newInstance(arguments: _*)).toEither.left.map {
      case invocation: InvocationTargetException => invocation.getCause
      case error                                 => error
    }

  private val networkIdConstructor =
    classOf[NetworkIdV1].getConstructor(classOf[Array[Byte]])
  private val genesisIdConstructor =
    classOf[GenesisIdV1].getConstructor(classOf[Array[Byte]])
  private val parametersIdConstructor =
    classOf[ConsensusParametersIdV1].getConstructor(classOf[Array[Byte]])
  private val bootstrapConstructor =
    classOf[ConsensusBootstrapContextV1].getConstructor(classOf[NetworkIdV1], classOf[ProtocolEraId])
  private val artifactConstructor =
    classOf[ConsensusArtifactContextV1].getConstructor(
      classOf[NetworkIdV1],
      classOf[GenesisIdV1],
      classOf[ProtocolEraId],
      classOf[ConsensusParametersIdV1]
    )

  private val networkBytes = ByteVector.fromValidHex("11" * IdBytes)
  private val genesisBytes = ByteVector.fromValidHex("22" * IdBytes)
  private val parametersBytes = ByteVector.fromValidHex("33" * IdBytes)

  private val networkId = required(NetworkIdV1.fromByteVector(networkBytes))
  private val genesisId = required(GenesisIdV1.fromByteVector(genesisBytes))
  private val parametersId = required(ConsensusParametersIdV1.fromByteVector(parametersBytes))
  private val bootstrap = required(ConsensusBootstrapContextV1.from(networkId, ScodecV1))
  private val artifact = required(ConsensusArtifactContextV1.from(networkId, genesisId, ScodecV1, parametersId))
  private val forgedScodecV1 =
    ScodecV1.getClass.getConstructor().newInstance().asInstanceOf[ProtocolEraId]

  private val bootstrapGolden = "11" * IdBytes + "01"
  private val artifactGolden = "11" * IdBytes + "22" * IdBytes + "01" + "33" * IdBytes

  test("records exact candidate bootstrap and complete artifact context vectors") {
    val bootstrapBytes = bootstrapContextCodec.encode(bootstrap).require.toByteVector
    val artifactBytes = artifactContextCodec.encode(artifact).require.toByteVector

    expect.eql(bootstrapBytes.toHex, bootstrapGolden) &&
    expect.eql(artifactBytes.toHex, artifactGolden) &&
    expect.eql(bootstrapBytes.length, BootstrapContextBytes) &&
    expect.eql(artifactBytes.length, ArtifactContextBytes) &&
    expect.eql(BootstrapContextBytes, 33L) &&
    expect.eql(ArtifactContextBytes, 97L)
  }

  test("every candidate identifier and context round-trips through its complete immutable codec") {
    expect(networkIdImmutableCodec.fromImmutableBytes(networkIdImmutableCodec.immutableBytes(networkId)) == Right(networkId)) &&
    expect(genesisIdImmutableCodec.fromImmutableBytes(genesisIdImmutableCodec.immutableBytes(genesisId)) == Right(genesisId)) &&
    expect(
      consensusParametersIdImmutableCodec.fromImmutableBytes(
        consensusParametersIdImmutableCodec.immutableBytes(parametersId)
      ) == Right(parametersId)
    ) &&
    expect(
      bootstrapContextImmutableCodec.fromImmutableBytes(bootstrapContextImmutableCodec.immutableBytes(bootstrap)) == Right(bootstrap)
    ) &&
    expect(
      artifactContextImmutableCodec.fromImmutableBytes(artifactContextImmutableCodec.immutableBytes(artifact)) == Right(artifact)
    )
  }

  test("identifier factories reject null, every wrong width, and all-zero identities") {
    val wrongWidths = List(0, 1, IdBytes - 1, IdBytes + 1, IdBytes * 2)
    val allZero = filled(IdBytes, 0)

    expect(NetworkIdV1.fromByteVector(null).isLeft) &&
    expect(NetworkIdV1.fromBytes(null).isLeft) &&
    expect(GenesisIdV1.fromByteVector(null).isLeft) &&
    expect(GenesisIdV1.fromBytes(null).isLeft) &&
    expect(ConsensusParametersIdV1.fromByteVector(null).isLeft) &&
    expect(ConsensusParametersIdV1.fromBytes(null).isLeft) &&
    expect(wrongWidths.forall(length => NetworkIdV1.fromByteVector(filled(length, 1)).isLeft)) &&
    expect(wrongWidths.forall(length => GenesisIdV1.fromByteVector(filled(length, 1)).isLeft)) &&
    expect(
      wrongWidths.forall(length =>
        ConsensusParametersIdV1.fromByteVector(filled(length, 1)).isLeft
      )
    ) &&
    expect(NetworkIdV1.fromByteVector(allZero).isLeft) &&
    expect(GenesisIdV1.fromByteVector(allZero).isLeft) &&
    expect(ConsensusParametersIdV1.fromByteVector(allZero).isLeft)
  }

  test("identifier codecs reject malformed values on decode and null on encode") {
    val wrongWidths = List(0, 1, IdBytes - 1, IdBytes + 1, IdBytes * 2)
    val allZero = BitVector.low(IdBytes.toLong * 8L)

    expect(wrongWidths.forall(length => networkIdCodec.complete.decodeValue(BitVector.low(length.toLong * 8L)).toEither.isLeft)) &&
    expect(wrongWidths.forall(length => genesisIdCodec.complete.decodeValue(BitVector.low(length.toLong * 8L)).toEither.isLeft)) &&
    expect(
      wrongWidths.forall(length =>
        consensusParametersIdCodec.complete.decodeValue(BitVector.low(length.toLong * 8L)).toEither.isLeft
      )
    ) &&
    expect(networkIdCodec.complete.decodeValue(allZero).toEither.isLeft) &&
    expect(genesisIdCodec.complete.decodeValue(allZero).toEither.isLeft) &&
    expect(consensusParametersIdCodec.complete.decodeValue(allZero).toEither.isLeft) &&
    expect(networkIdCodec.encode(null).toEither.isLeft) &&
    expect(genesisIdCodec.encode(null).toEither.isLeft) &&
    expect(consensusParametersIdCodec.encode(null).toEither.isLeft)
  }

  test("context factories and encoders reject every null component") {
    expect(ConsensusBootstrapContextV1.from(null, ScodecV1).isLeft) &&
    expect(ConsensusBootstrapContextV1.from(networkId, null).isLeft) &&
    expect(ConsensusArtifactContextV1.from(null, genesisId, ScodecV1, parametersId).isLeft) &&
    expect(ConsensusArtifactContextV1.from(networkId, null, ScodecV1, parametersId).isLeft) &&
    expect(ConsensusArtifactContextV1.from(networkId, genesisId, null, parametersId).isLeft) &&
    expect(ConsensusArtifactContextV1.from(networkId, genesisId, ScodecV1, null).isLeft) &&
    expect(bootstrapContextCodec.encode(null).toEither.isLeft) &&
    expect(artifactContextCodec.encode(null).toEither.isLeft)
  }

  test("complete context codecs reject truncation, trailing bytes, and every unknown era") {
    val bootstrapBits = ByteVector.fromValidHex(bootstrapGolden).bits
    val artifactBits = ByteVector.fromValidHex(artifactGolden).bits
    val bootstrapEraOffset = IdBytes.toLong * 8L
    val artifactEraOffset = IdBytes.toLong * 2L * 8L

    def replaceEra(bits: BitVector, offset: Long, tag: Int): BitVector =
      bits.take(offset) ++ ByteVector(tag).bits ++ bits.drop(offset + 8L)

    val unknownEraTags = (0 to 255).filterNot(_ == 1)

    expect(bootstrapContextCodec.complete.decodeValue(bootstrapBits.dropRight(8L)).toEither.isLeft) &&
    expect(bootstrapContextCodec.complete.decodeValue(bootstrapBits ++ BitVector.low(8L)).toEither.isLeft) &&
    expect(artifactContextCodec.complete.decodeValue(artifactBits.dropRight(8L)).toEither.isLeft) &&
    expect(artifactContextCodec.complete.decodeValue(artifactBits ++ BitVector.low(8L)).toEither.isLeft) &&
    expect(
      unknownEraTags.forall(tag =>
        bootstrapContextCodec.complete.decodeValue(replaceEra(bootstrapBits, bootstrapEraOffset, tag)).toEither.isLeft
      )
    ) &&
    expect(
      unknownEraTags.forall(tag =>
        artifactContextCodec.complete.decodeValue(replaceEra(artifactBits, artifactEraOffset, tag)).toEither.isLeft
      )
    )
  }

  test("every complete artifact-context field mutates canonical bytes") {
    val alternatives = List(
      required(
        ConsensusArtifactContextV1.from(
          required(NetworkIdV1.fromByteVector(filled(IdBytes, 0x44))),
          genesisId,
          ScodecV1,
          parametersId
        )
      ),
      required(
        ConsensusArtifactContextV1.from(
          networkId,
          required(GenesisIdV1.fromByteVector(filled(IdBytes, 0x55))),
          ScodecV1,
          parametersId
        )
      ),
      required(
        ConsensusArtifactContextV1.from(
          networkId,
          genesisId,
          ScodecV1,
          required(ConsensusParametersIdV1.fromByteVector(filled(IdBytes, 0x66)))
        )
      )
    )
    val original = artifactContextImmutableCodec.immutableBytes(artifact)
    val rawEraMutation = original.take(IdBytes.toLong * 2L) ++ ByteVector(0x02) ++ original.drop(IdBytes.toLong * 2L + 1L)
    val bootstrapOriginal = bootstrapContextImmutableCodec.immutableBytes(bootstrap)
    val bootstrapEraMutation = bootstrapOriginal.take(IdBytes.toLong) ++ ByteVector(0x02)

    expect(alternatives.forall(candidate => artifactContextImmutableCodec.immutableBytes(candidate) != original)) &&
    expect(rawEraMutation != original) &&
    expect(artifactContextImmutableCodec.fromImmutableBytes(rawEraMutation).isLeft) &&
    expect(
      bootstrapOriginal != bootstrapContextImmutableCodec.immutableBytes(
        required(
          ConsensusBootstrapContextV1.from(
            required(NetworkIdV1.fromByteVector(filled(IdBytes, 0x44))),
            ScodecV1
          )
        )
      )
    ) &&
    expect(bootstrapEraMutation != bootstrapOriginal) &&
    expect(bootstrapContextImmutableCodec.fromImmutableBytes(bootstrapEraMutation).isLeft)
  }

  test("distinct identifier types make field substitution unrepresentable") {
    val _ = implicitly[NotGiven[NetworkIdV1 =:= GenesisIdV1]]
    val _ = implicitly[NotGiven[NetworkIdV1 =:= ConsensusParametersIdV1]]
    val _ = implicitly[NotGiven[GenesisIdV1 =:= ConsensusParametersIdV1]]
    val typedFactory
      : (
          NetworkIdV1,
          GenesisIdV1,
          ProtocolEraId,
          ConsensusParametersIdV1
        ) => Either[ConsensusArtifactContextV1.ValidationError, ConsensusArtifactContextV1] =
      ConsensusArtifactContextV1.from

    expect(typedFactory(networkId, genesisId, ScodecV1, parametersId) == Right(artifact))
  }

  test("factory-constructed identifiers defend against input and output array mutation") {
    val source = Array.fill[Byte](IdBytes)(0x77.toByte)
    val value = required(NetworkIdV1.fromBytes(source))
    val expected = value.toByteVector
    source(0) = 0x00.toByte
    val output = value.toByteVector.toArray
    output(1) = 0x00.toByte

    expect(value.toByteVector == expected) &&
    expect(value.toByteVector(0) == 0x77.toByte) &&
    expect(value.toByteVector(1) == 0x77.toByte)
  }

  test("public JVM identifier constructors reject null, wrong-width, and all-zero values") {
    val invalid = List[Array[Byte]](
      null,
      Array.emptyByteArray,
      Array.fill[Byte](IdBytes - 1)(1.toByte),
      Array.fill[Byte](IdBytes + 1)(1.toByte),
      Array.fill[Byte](IdBytes)(0.toByte)
    )

    def rejects[A](constructor: Constructor[A]): Boolean =
      invalid.forall(bytes =>
        invoke(constructor, bytes).swap.exists(_.isInstanceOf[IllegalArgumentException])
      )

    expect(rejects(networkIdConstructor)) &&
    expect(rejects(genesisIdConstructor)) &&
    expect(rejects(parametersIdConstructor))
  }

  test("public JVM identifier constructors defensively clone valid input") {
    val networkSource = Array.fill[Byte](IdBytes)(0x11.toByte)
    val genesisSource = Array.fill[Byte](IdBytes)(0x22.toByte)
    val parametersSource = Array.fill[Byte](IdBytes)(0x33.toByte)
    val directNetwork = invoke(networkIdConstructor, networkSource).fold(throw _, identity)
    val directGenesis = invoke(genesisIdConstructor, genesisSource).fold(throw _, identity)
    val directParameters = invoke(parametersIdConstructor, parametersSource).fold(throw _, identity)

    networkSource(0) = 0x44.toByte
    genesisSource(0) = 0x55.toByte
    parametersSource(0) = 0x66.toByte

    expect(directNetwork.toByteVector == networkBytes) &&
    expect(directGenesis.toByteVector == genesisBytes) &&
    expect(directParameters.toByteVector == parametersBytes)
  }

  test("public JVM context constructors reject every null component") {
    def isIllegalArgument[A](result: Either[Throwable, A]): Boolean =
      result.swap.exists(_.isInstanceOf[IllegalArgumentException])

    expect(isIllegalArgument(invoke(bootstrapConstructor, null, ScodecV1))) &&
    expect(isIllegalArgument(invoke(bootstrapConstructor, networkId, null))) &&
    expect(isIllegalArgument(invoke(artifactConstructor, null, genesisId, ScodecV1, parametersId))) &&
    expect(isIllegalArgument(invoke(artifactConstructor, networkId, null, ScodecV1, parametersId))) &&
    expect(isIllegalArgument(invoke(artifactConstructor, networkId, genesisId, null, parametersId))) &&
    expect(isIllegalArgument(invoke(artifactConstructor, networkId, genesisId, ScodecV1, null))) &&
    expect(invoke(bootstrapConstructor, networkId, ScodecV1) == Right(bootstrap)) &&
    expect(invoke(artifactConstructor, networkId, genesisId, ScodecV1, parametersId) == Right(artifact))
  }

  test("factories and public JVM context constructors reject a forged era singleton") {
    def isIllegalArgument[A](result: Either[Throwable, A]): Boolean =
      result.swap.exists(_.isInstanceOf[IllegalArgumentException])

    expect(ConsensusBootstrapContextV1.from(networkId, forgedScodecV1).isLeft) &&
    expect(ConsensusArtifactContextV1.from(networkId, genesisId, forgedScodecV1, parametersId).isLeft) &&
    expect(isIllegalArgument(invoke(bootstrapConstructor, networkId, forgedScodecV1))) &&
    expect(isIllegalArgument(invoke(artifactConstructor, networkId, genesisId, forgedScodecV1, parametersId)))
  }

  test("candidate refined types expose no case-class copy or public array escape") {
    val candidateClasses = List(
      classOf[NetworkIdV1],
      classOf[GenesisIdV1],
      classOf[ConsensusParametersIdV1],
      classOf[ConsensusBootstrapContextV1],
      classOf[ConsensusArtifactContextV1]
    )

    expect(candidateClasses.forall(!_.getMethods.exists(_.getName == "copy"))) &&
    expect(candidateClasses.forall(!_.getMethods.exists(_.getReturnType == classOf[Array[Byte]])))
  }

  test("dark candidate values and contexts are not Java-serializable") {
    val candidateClasses = List(
      classOf[NetworkIdV1],
      classOf[GenesisIdV1],
      classOf[ConsensusParametersIdV1],
      classOf[ConsensusBootstrapContextV1],
      classOf[ConsensusArtifactContextV1]
    )

    expect(candidateClasses.forall(candidate => !classOf[java.io.Serializable].isAssignableFrom(candidate)))
  }

  test("dark candidates have no production hash schema") {
    val _ = implicitly[NotGiven[ConsensusHashSchema[NetworkIdV1]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[GenesisIdV1]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[ConsensusParametersIdV1]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[ConsensusBootstrapContextV1]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[ConsensusArtifactContextV1]]]

    expect(true)
  }
}
