package io.constellationnetwork.schema.consensus

/** Closed metadata-only inventory of the payload variants named by the live finality artifact pointer union.
  *
  * These labels are not wire tags or codec domains. They exist so the dark consensus-byte manifest cannot hide an undefined payload behind
  * the broader `FinalityDurabilityRecord` artifact family.
  */
sealed abstract class ConsensusFinalityPayloadKind(val semanticLabel: String) extends Product with Serializable

object ConsensusFinalityPayloadKind {
  case object CoreBatch extends ConsensusFinalityPayloadKind("gl0.finality.payload.core-batch")
  case object ReleasedCoreRecord extends ConsensusFinalityPayloadKind("gl0.finality.payload.released-core-record")
  case object PathManifest extends ConsensusFinalityPayloadKind("gl0.finality.payload.path-manifest")
  case object PathChunk extends ConsensusFinalityPayloadKind("gl0.finality.payload.path-chunk")
  case object DecidedAttestationEvidence
      extends ConsensusFinalityPayloadKind("gl0.finality.payload.decided-attestation-evidence")
  case object DepthK1Evidence extends ConsensusFinalityPayloadKind("gl0.finality.payload.depth-k1-evidence")
  case object ForkChoiceDecisionEvidence
      extends ConsensusFinalityPayloadKind("gl0.finality.payload.fork-choice-decision-evidence")
  case object PreparedSemanticState extends ConsensusFinalityPayloadKind("gl0.finality.payload.prepared-semantic-state")
  case object AuthenticatedTargetAnchor extends ConsensusFinalityPayloadKind("gl0.finality.payload.authenticated-target-anchor")
  case object AppliedSemanticStateReceipt
      extends ConsensusFinalityPayloadKind("gl0.finality.payload.applied-semantic-state-receipt")
  case object AuthenticatedAnchorReceipt
      extends ConsensusFinalityPayloadKind("gl0.finality.payload.authenticated-anchor-receipt")
  case object PriorSemanticStateReceipt
      extends ConsensusFinalityPayloadKind("gl0.finality.payload.prior-semantic-state-receipt")
  case object PriorAnchorReceipt extends ConsensusFinalityPayloadKind("gl0.finality.payload.prior-anchor-receipt")
  case object EffectPayload extends ConsensusFinalityPayloadKind("gl0.finality.payload.effect")

  val all: List[ConsensusFinalityPayloadKind] = List(
    CoreBatch,
    ReleasedCoreRecord,
    PathManifest,
    PathChunk,
    DecidedAttestationEvidence,
    DepthK1Evidence,
    ForkChoiceDecisionEvidence,
    PreparedSemanticState,
    AuthenticatedTargetAnchor,
    AppliedSemanticStateReceipt,
    AuthenticatedAnchorReceipt,
    PriorSemanticStateReceipt,
    PriorAnchorReceipt,
    EffectPayload
  )
}

/** Current payload-schema evidence. `OpaquePointerOnly` means the implementation commits externally supplied bytes by length and digest; it
  * does not mean those bytes have a canonical payload schema or semantic verifier.
  */
sealed trait FinalityPayloadShape extends Product with Serializable

object FinalityPayloadShape {
  final case class ConcreteSourceType(fullyQualifiedName: String) extends FinalityPayloadShape
  case object OpaquePointerOnly extends FinalityPayloadShape
}

sealed trait FinalityPayloadCodecStatus extends Product with Serializable

object FinalityPayloadCodecStatus {
  case object ConcreteCanonicalCodec extends FinalityPayloadCodecStatus
  case object NoCanonicalPayloadCodec extends FinalityPayloadCodecStatus
}

sealed trait FinalityPayloadVectorStatus extends Product with Serializable

object FinalityPayloadVectorStatus {
  case object ConcreteFrozenVector extends FinalityPayloadVectorStatus
  case object NoCanonicalPayloadVector extends FinalityPayloadVectorStatus
}
