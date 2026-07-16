package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.schema.consensus.{ConsensusFinalityPayloadKind => ManifestKind}
import io.constellationnetwork.serde.consensus.ConsensusArtifactRequirementsManifest.finalityPayloadEntries

import weaver.FunSuite

/** Keeps the dark payload requirements subtype-total with the runtime pointer union without making the metadata reachable from production. */
object FinalityArtifactRequirementsParitySuite extends FunSuite {

  private def manifestKind(kind: FinalityArtifactKind): ManifestKind =
    kind match {
      case FinalityArtifactKind.CoreBatch                  => ManifestKind.CoreBatch
      case FinalityArtifactKind.ReleasedCoreRecord         => ManifestKind.ReleasedCoreRecord
      case FinalityArtifactKind.PathManifest               => ManifestKind.PathManifest
      case FinalityArtifactKind.PathChunk                  => ManifestKind.PathChunk
      case FinalityArtifactKind.DecidedAttestationEvidence => ManifestKind.DecidedAttestationEvidence
      case FinalityArtifactKind.DepthK1Evidence             => ManifestKind.DepthK1Evidence
      case FinalityArtifactKind.ForkChoiceDecisionEvidence  => ManifestKind.ForkChoiceDecisionEvidence
      case FinalityArtifactKind.PreparedSemanticState       => ManifestKind.PreparedSemanticState
      case FinalityArtifactKind.AuthenticatedTargetAnchor   => ManifestKind.AuthenticatedTargetAnchor
      case FinalityArtifactKind.AppliedSemanticStateReceipt => ManifestKind.AppliedSemanticStateReceipt
      case FinalityArtifactKind.AuthenticatedAnchorReceipt  => ManifestKind.AuthenticatedAnchorReceipt
      case FinalityArtifactKind.PriorSemanticStateReceipt   => ManifestKind.PriorSemanticStateReceipt
      case FinalityArtifactKind.PriorAnchorReceipt          => ManifestKind.PriorAnchorReceipt
      case FinalityArtifactKind.EffectPayload               => ManifestKind.EffectPayload
    }

  test("every runtime finality artifact kind has exactly one dark payload contract") {
    val mappedRuntimeKinds = FinalityArtifactKind.all.map(manifestKind)
    val manifestKinds = finalityPayloadEntries.map(_.kind)

    expect.all(
      FinalityArtifactKind.all.size == 14,
      FinalityArtifactKind.all.distinct.size == FinalityArtifactKind.all.size,
      ManifestKind.all.size == 14,
      ManifestKind.all.distinct.size == ManifestKind.all.size,
      mappedRuntimeKinds.size == FinalityArtifactKind.all.size,
      mappedRuntimeKinds.toSet == ManifestKind.all.toSet,
      manifestKinds.size == ManifestKind.all.size,
      manifestKinds.toSet == mappedRuntimeKinds.toSet
    )
  }
}
