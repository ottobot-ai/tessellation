package io.constellationnetwork.schema.consensus

import java.util.Arrays

import cats.{Eq, Show}

import io.constellationnetwork.schema.era.ProtocolEraId

import scodec.bits.ByteVector

/** Candidate network identity for the dark S1 consensus-artifact grammar.
  *
  * This type has no runtime authority. Its constructor repeats every refinement and clones the input because Scala source-private
  * constructors remain callable from JVM code.
  */
final class NetworkIdV1 private (sourceBytes: Array[Byte]) {
  private val bytes: Array[Byte] = {
    require(sourceBytes ne null, "network id bytes cannot be null")
    require(sourceBytes.length == NetworkIdV1.Length, s"network id must contain exactly ${NetworkIdV1.Length} bytes")
    require(sourceBytes.exists(_ != 0.toByte), "network id cannot be all zero")
    sourceBytes.clone()
  }

  def toByteVector: ByteVector = ByteVector.view(bytes.clone())

  override def equals(other: Any): Boolean =
    other match {
      case that: NetworkIdV1 => Arrays.equals(bytes, that.bytes)
      case _                 => false
    }

  override def hashCode(): Int = Arrays.hashCode(bytes)
  override def toString: String = s"NetworkIdV1(${toByteVector.toHex})"
}

object NetworkIdV1 {
  val Length: Int = 32

  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullBytes extends ValidationError("network id bytes cannot be null")
  final case class InvalidLength(actual: Long) extends ValidationError(s"network id must contain exactly $Length bytes, got $actual")
  case object AllZero extends ValidationError("network id cannot be all zero")

  def fromBytes(bytes: Array[Byte]): Either[ValidationError, NetworkIdV1] =
    if (bytes eq null) Left(NullBytes)
    else fromByteVector(ByteVector.view(bytes.clone()))

  def fromByteVector(bytes: ByteVector): Either[ValidationError, NetworkIdV1] =
    if (bytes eq null) Left(NullBytes)
    else if (bytes.length != Length.toLong) Left(InvalidLength(bytes.length))
    else if (bytes == ByteVector.fill(Length.toLong)(0.toByte)) Left(AllZero)
    else Right(new NetworkIdV1(bytes.toArray.clone()))

  implicit val eq: Eq[NetworkIdV1] = Eq.fromUniversalEquals
  implicit val show: Show[NetworkIdV1] = Show.fromToString
}

/** Candidate genesis identity for the dark S1 consensus-artifact grammar.
  *
  * It is deliberately distinct from network and parameter identities. Its JVM-callable constructor validates and clones its input; the type
  * has no runtime authority.
  */
final class GenesisIdV1 private (sourceBytes: Array[Byte]) {
  private val bytes: Array[Byte] = {
    require(sourceBytes ne null, "genesis id bytes cannot be null")
    require(sourceBytes.length == GenesisIdV1.Length, s"genesis id must contain exactly ${GenesisIdV1.Length} bytes")
    require(sourceBytes.exists(_ != 0.toByte), "genesis id cannot be all zero")
    sourceBytes.clone()
  }

  def toByteVector: ByteVector = ByteVector.view(bytes.clone())

  override def equals(other: Any): Boolean =
    other match {
      case that: GenesisIdV1 => Arrays.equals(bytes, that.bytes)
      case _                 => false
    }

  override def hashCode(): Int = Arrays.hashCode(bytes)
  override def toString: String = s"GenesisIdV1(${toByteVector.toHex})"
}

object GenesisIdV1 {
  val Length: Int = 32

  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullBytes extends ValidationError("genesis id bytes cannot be null")
  final case class InvalidLength(actual: Long) extends ValidationError(s"genesis id must contain exactly $Length bytes, got $actual")
  case object AllZero extends ValidationError("genesis id cannot be all zero")

  def fromBytes(bytes: Array[Byte]): Either[ValidationError, GenesisIdV1] =
    if (bytes eq null) Left(NullBytes)
    else fromByteVector(ByteVector.view(bytes.clone()))

  def fromByteVector(bytes: ByteVector): Either[ValidationError, GenesisIdV1] =
    if (bytes eq null) Left(NullBytes)
    else if (bytes.length != Length.toLong) Left(InvalidLength(bytes.length))
    else if (bytes == ByteVector.fill(Length.toLong)(0.toByte)) Left(AllZero)
    else Right(new GenesisIdV1(bytes.toArray.clone()))

  implicit val eq: Eq[GenesisIdV1] = Eq.fromUniversalEquals
  implicit val show: Show[GenesisIdV1] = Show.fromToString
}

/** Candidate consensus-parameter-set identity for the dark S1 consensus-artifact grammar.
  *
  * It is deliberately distinct from network and genesis identities. Its JVM-callable constructor validates and clones its input; the type
  * has no runtime authority.
  */
final class ConsensusParametersIdV1 private (sourceBytes: Array[Byte]) {
  private val bytes: Array[Byte] = {
    require(sourceBytes ne null, "consensus parameters id bytes cannot be null")
    require(
      sourceBytes.length == ConsensusParametersIdV1.Length,
      s"consensus parameters id must contain exactly ${ConsensusParametersIdV1.Length} bytes"
    )
    require(sourceBytes.exists(_ != 0.toByte), "consensus parameters id cannot be all zero")
    sourceBytes.clone()
  }

  def toByteVector: ByteVector = ByteVector.view(bytes.clone())

  override def equals(other: Any): Boolean =
    other match {
      case that: ConsensusParametersIdV1 => Arrays.equals(bytes, that.bytes)
      case _                             => false
    }

  override def hashCode(): Int = Arrays.hashCode(bytes)
  override def toString: String = s"ConsensusParametersIdV1(${toByteVector.toHex})"
}

object ConsensusParametersIdV1 {
  val Length: Int = 32

  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullBytes extends ValidationError("consensus parameters id bytes cannot be null")
  final case class InvalidLength(actual: Long)
      extends ValidationError(s"consensus parameters id must contain exactly $Length bytes, got $actual")
  case object AllZero extends ValidationError("consensus parameters id cannot be all zero")

  def fromBytes(bytes: Array[Byte]): Either[ValidationError, ConsensusParametersIdV1] =
    if (bytes eq null) Left(NullBytes)
    else fromByteVector(ByteVector.view(bytes.clone()))

  def fromByteVector(bytes: ByteVector): Either[ValidationError, ConsensusParametersIdV1] =
    if (bytes eq null) Left(NullBytes)
    else if (bytes.length != Length.toLong) Left(InvalidLength(bytes.length))
    else if (bytes == ByteVector.fill(Length.toLong)(0.toByte)) Left(AllZero)
    else Right(new ConsensusParametersIdV1(bytes.toArray.clone()))

  implicit val eq: Eq[ConsensusParametersIdV1] = Eq.fromUniversalEquals
  implicit val show: Show[ConsensusParametersIdV1] = Show.fromToString
}

/** Candidate noncircular bootstrap context for the dark S1 grammar.
  *
  * Genesis and consensus-parameter identities are intentionally absent so their own canonical bytes can be established from a network and
  * era without requiring either identifier to contain itself. Its JVM-callable constructor repeats null validation. This type has no
  * runtime authority.
  */
final class ConsensusBootstrapContextV1 private (
  val networkId: NetworkIdV1,
  val protocolEraId: ProtocolEraId
) {
  require(networkId ne null, "networkId cannot be null")
  require(protocolEraId ne null, "protocolEraId cannot be null")
  require(ProtocolEraId.isRegistered(protocolEraId), "protocolEraId must be the canonical registered era")

  override def equals(other: Any): Boolean =
    other match {
      case that: ConsensusBootstrapContextV1 =>
        networkId == that.networkId && protocolEraId == that.protocolEraId
      case _ => false
    }

  override def hashCode(): Int = (networkId, protocolEraId).hashCode()
  override def toString: String = s"ConsensusBootstrapContextV1($networkId,$protocolEraId)"
}

object ConsensusBootstrapContextV1 {
  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullNetworkId extends ValidationError("networkId cannot be null")
  case object NullProtocolEraId extends ValidationError("protocolEraId cannot be null")
  case object UnregisteredProtocolEraId extends ValidationError("protocolEraId must be the canonical registered era")

  def from(
    networkId: NetworkIdV1,
    protocolEraId: ProtocolEraId
  ): Either[ValidationError, ConsensusBootstrapContextV1] =
    if (networkId eq null) Left(NullNetworkId)
    else if (protocolEraId eq null) Left(NullProtocolEraId)
    else if (!ProtocolEraId.isRegistered(protocolEraId)) Left(UnregisteredProtocolEraId)
    else Right(new ConsensusBootstrapContextV1(networkId, protocolEraId))

  implicit val eq: Eq[ConsensusBootstrapContextV1] = Eq.fromUniversalEquals
  implicit val show: Show[ConsensusBootstrapContextV1] = Show.fromToString
}

/** Candidate complete artifact context for the dark S1 grammar.
  *
  * The four typed fields prevent cross-network, cross-genesis, cross-era, and cross-parameter replay once a separately reviewed artifact
  * schema binds this context. Its JVM-callable constructor repeats null validation. This type alone grants no hashing, signing, transport,
  * or runtime authority.
  */
final class ConsensusArtifactContextV1 private (
  val networkId: NetworkIdV1,
  val genesisId: GenesisIdV1,
  val protocolEraId: ProtocolEraId,
  val parametersId: ConsensusParametersIdV1
) {
  require(networkId ne null, "networkId cannot be null")
  require(genesisId ne null, "genesisId cannot be null")
  require(protocolEraId ne null, "protocolEraId cannot be null")
  require(ProtocolEraId.isRegistered(protocolEraId), "protocolEraId must be the canonical registered era")
  require(parametersId ne null, "parametersId cannot be null")

  override def equals(other: Any): Boolean =
    other match {
      case that: ConsensusArtifactContextV1 =>
        networkId == that.networkId &&
        genesisId == that.genesisId &&
        protocolEraId == that.protocolEraId &&
        parametersId == that.parametersId
      case _ => false
    }

  override def hashCode(): Int = (networkId, genesisId, protocolEraId, parametersId).hashCode()
  override def toString: String =
    s"ConsensusArtifactContextV1($networkId,$genesisId,$protocolEraId,$parametersId)"
}

object ConsensusArtifactContextV1 {
  sealed abstract class ValidationError(val message: String) extends Product with Serializable
  case object NullNetworkId extends ValidationError("networkId cannot be null")
  case object NullGenesisId extends ValidationError("genesisId cannot be null")
  case object NullProtocolEraId extends ValidationError("protocolEraId cannot be null")
  case object UnregisteredProtocolEraId extends ValidationError("protocolEraId must be the canonical registered era")
  case object NullParametersId extends ValidationError("parametersId cannot be null")

  def from(
    networkId: NetworkIdV1,
    genesisId: GenesisIdV1,
    protocolEraId: ProtocolEraId,
    parametersId: ConsensusParametersIdV1
  ): Either[ValidationError, ConsensusArtifactContextV1] =
    if (networkId eq null) Left(NullNetworkId)
    else if (genesisId eq null) Left(NullGenesisId)
    else if (protocolEraId eq null) Left(NullProtocolEraId)
    else if (!ProtocolEraId.isRegistered(protocolEraId)) Left(UnregisteredProtocolEraId)
    else if (parametersId eq null) Left(NullParametersId)
    else Right(new ConsensusArtifactContextV1(networkId, genesisId, protocolEraId, parametersId))

  implicit val eq: Eq[ConsensusArtifactContextV1] = Eq.fromUniversalEquals
  implicit val show: Show[ConsensusArtifactContextV1] = Show.fromToString
}
