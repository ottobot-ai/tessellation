package io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto

import io.constellationnetwork.schema.nakamoto.slot._

/** Consensus-state carrier for the Nakamoto slot path.
  *
  * The former VRF slot-clock daemon (`run`) was legacy/dead — `SnapshotLeaderLoop` is the live producer driver and `SharedEpochState` is
  * the live eta accumulator (keyed on `etaRotationSnapshots` = R snapshots, NOT a slot-based epoch). Only the state case class remains,
  * threaded through the consensus state machinery (`GlobalSnapshotConsensusStateCreator` / `GlobalSnapshotConsensus`).
  */
object NakamotoTriggerDaemon {

  /** Mutable state tracked across slots. */
  final case class NakamotoTriggerState(
    genesisTimeMs: Long,
    lastProducedSlot: Option[Long],
    currentEta: Array[Byte],
    vrfAccumulator: List[Array[Byte]],
    totalProduced: Long,
    currentSlotCertificate: Option[SlotCertificate]
  )

  object NakamotoTriggerState {
    def initial(genesisTimeMs: Long, genesisEta: Array[Byte]): NakamotoTriggerState =
      NakamotoTriggerState(
        genesisTimeMs = genesisTimeMs,
        lastProducedSlot = None,
        currentEta = genesisEta,
        vrfAccumulator = Nil,
        totalProduced = 0L,
        currentSlotCertificate = None
      )
  }
}
