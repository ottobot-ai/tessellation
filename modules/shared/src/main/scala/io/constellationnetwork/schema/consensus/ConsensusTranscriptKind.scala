package io.constellationnetwork.schema.consensus

/** Declaration-only inventory of consensus-randomness and selection transcript subjects.
  *
  * These labels do not define transcript bytes, hashing, numeric tags, or runtime selection.
  */
sealed abstract class ConsensusTranscriptKind(val semanticLabel: String) extends Product with Serializable

object ConsensusTranscriptKind {
  case object Gl0LeaderVrf extends ConsensusTranscriptKind("gl0.leader-vrf")
  case object AdmissionVrf extends ConsensusTranscriptKind("gl0.admission-vrf")
  case object ExecutionCommitteeVkHashDraw extends ConsensusTranscriptKind("gl0.execution-committee-vk-hash-draw")
  case object ShardEtaDerivation extends ConsensusTranscriptKind("gl0.shard-eta-derivation")
  case object StaircaseRank extends ConsensusTranscriptKind("gl0.shard-staircase-rank")
  case object CheckpointVrfPossession extends ConsensusTranscriptKind("gl0.shard-checkpoint-vrf-possession")
  case object ExecutionSignatureVrfPossession extends ConsensusTranscriptKind("gl0.shard-execution-signature-vrf-possession")
  case object WatchtowerAssignment extends ConsensusTranscriptKind("gl0.watchtower-assignment")
  case object OptimisticSampling extends ConsensusTranscriptKind("gl0.finality-optimistic-sampling")
  case object TowerEligibility extends ConsensusTranscriptKind("gl0.tower-eligibility")

  val all: List[ConsensusTranscriptKind] = List(
    Gl0LeaderVrf,
    AdmissionVrf,
    ExecutionCommitteeVkHashDraw,
    ShardEtaDerivation,
    StaircaseRank,
    CheckpointVrfPossession,
    ExecutionSignatureVrfPossession,
    WatchtowerAssignment,
    OptimisticSampling,
    TowerEligibility
  )
}
