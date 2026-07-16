package io.constellationnetwork.schema.consensus

/** Declaration-only inventory of transport and coordination carriers.
  *
  * A carrier can move or correlate a nested artifact. It never inherits the nested artifact's authority, and these labels do not define
  * wire tags, canonical bytes, framing, chunking, or resource limits.
  */
sealed abstract class ConsensusCarrierKind(val semanticLabel: String) extends Product with Serializable

object ConsensusCarrierKind {
  case object PeerRumorEnvelope extends ConsensusCarrierKind("transport.rumor.peer-envelope")
  case object CommonRumorEnvelope extends ConsensusCarrierKind("transport.rumor.common-envelope")
  case object SidecarRumorEnvelope extends ConsensusCarrierKind("transport.rumor.sidecar-envelope")
  case object EventGossipEnvelope extends ConsensusCarrierKind("transport.event-gossip.envelope")
  case object EventGossipIHave extends ConsensusCarrierKind("transport.event-gossip.ihave")
  case object EventGossipIWantRequest extends ConsensusCarrierKind("transport.event-gossip.iwant-request")
  case object EventGossipIWantResponse extends ConsensusCarrierKind("transport.event-gossip.iwant-response")

  case object Ml0EventAnnouncementEnvelope extends ConsensusCarrierKind("transport.ml0.event-announcement-envelope")
  case object Ml0FacilityEnvelope extends ConsensusCarrierKind("transport.ml0.facility-envelope")
  case object Ml0ProposalEnvelope extends ConsensusCarrierKind("transport.ml0.proposal-envelope")
  case object Ml0MajoritySignatureEnvelope extends ConsensusCarrierKind("transport.ml0.majority-signature-envelope")
  case object Ml0BinarySignatureEnvelope extends ConsensusCarrierKind("transport.ml0.binary-signature-envelope")
  case object Ml0AckEnvelope extends ConsensusCarrierKind("transport.ml0.ack-envelope")
  case object Ml0WithdrawEnvelope extends ConsensusCarrierKind("transport.ml0.withdraw-envelope")
  case object Ml0ArtifactAnnouncementEnvelope extends ConsensusCarrierKind("transport.ml0.artifact-announcement-envelope")

  case object PeerRumorInquiryRequest extends ConsensusCarrierKind("transport.rumor.peer-inquiry-request")
  case object CommonRumorOfferResponse extends ConsensusCarrierKind("transport.rumor.common-offer-response")
  case object QueryCommonRumorsRequest extends ConsensusCarrierKind("transport.rumor.common-query-request")
  case object CommonRumorInitResponse extends ConsensusCarrierKind("transport.rumor.common-init-response")
  case object PeerRumorResponseStream extends ConsensusCarrierKind("transport.rumor.peer-response-stream")
  case object CommonRumorResponseStream extends ConsensusCarrierKind("transport.rumor.common-response-stream")

  case object SidecarSubscribeRequest extends ConsensusCarrierKind("transport.sidecar.subscribe-request")
  case object SidecarSubscribeStarted extends ConsensusCarrierKind("transport.sidecar.subscribe-started")

  case object ChainSyncSnapshotCarrier extends ConsensusCarrierKind("transport.chainsync.snapshot-carrier")
  case object ChainSyncMetagraphBinaryCarrier extends ConsensusCarrierKind("transport.chainsync.metagraph-binary-carrier")
  case object ChainSyncSelectionHint extends ConsensusCarrierKind("transport.chainsync.selection-hint")

  case object BootstrapMetadataHint extends ConsensusCarrierKind("transport.bootstrap.metadata-hint")
  case object BootstrapPhase2StateBundle extends ConsensusCarrierKind("transport.bootstrap.phase2-state-bundle")
  case object BootstrapGenesisBundle extends ConsensusCarrierKind("transport.bootstrap.genesis-bundle")

  val all: List[ConsensusCarrierKind] = List(
    PeerRumorEnvelope,
    CommonRumorEnvelope,
    SidecarRumorEnvelope,
    EventGossipEnvelope,
    EventGossipIHave,
    EventGossipIWantRequest,
    EventGossipIWantResponse,
    Ml0EventAnnouncementEnvelope,
    Ml0FacilityEnvelope,
    Ml0ProposalEnvelope,
    Ml0MajoritySignatureEnvelope,
    Ml0BinarySignatureEnvelope,
    Ml0AckEnvelope,
    Ml0WithdrawEnvelope,
    Ml0ArtifactAnnouncementEnvelope,
    PeerRumorInquiryRequest,
    CommonRumorOfferResponse,
    QueryCommonRumorsRequest,
    CommonRumorInitResponse,
    PeerRumorResponseStream,
    CommonRumorResponseStream,
    SidecarSubscribeRequest,
    SidecarSubscribeStarted,
    ChainSyncSnapshotCarrier,
    ChainSyncMetagraphBinaryCarrier,
    ChainSyncSelectionHint,
    BootstrapMetadataHint,
    BootstrapPhase2StateBundle,
    BootstrapGenesisBundle
  )
}
