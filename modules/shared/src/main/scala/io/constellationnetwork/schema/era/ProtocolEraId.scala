package io.constellationnetwork.schema.era

/** Canonical identifier for a protocol era.
  *
  * The greenfield protocol has exactly one era. This identifier is an artifact field, not an ordinal selector or runtime activation service.
  * A future era requires a separately ratified protocol transition and must not be added for speculative compatibility.
  */
sealed trait ProtocolEraId extends Product with Serializable

object ProtocolEraId {
  case object ScodecV1 extends ProtocolEraId

  val all: List[ProtocolEraId] = List(ScodecV1)
}
