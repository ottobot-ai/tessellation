package io.constellationnetwork.schema.consensus

/** Closed source-shape inventory beneath the three semantic ChainSync carrier families.
  *
  * These labels are not wire tags or canonical codecs. They make every protobuf request, response, and embedded chain-point shape used by
  * the current ChainSync services visible without pretending that a local gRPC message or libp2p frame has consensus authority.
  */
sealed abstract class ConsensusChainSyncWireKind(
  val semanticLabel: String,
  val parentCarrier: ConsensusCarrierKind
) extends Product
    with Serializable

object ConsensusChainSyncWireKind {
  import ConsensusCarrierKind._

  case object ChainPointComponent extends ConsensusChainSyncWireKind("transport.chainsync.wire.chain-point", ChainSyncSelectionHint)
  case object FetchSnapshotsRequest
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.fetch-snapshots-request", ChainSyncSnapshotCarrier)
  case object SnapshotResponse extends ConsensusChainSyncWireKind("transport.chainsync.wire.snapshot-response", ChainSyncSnapshotCarrier)
  case object FindIntersectionRequest
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.find-intersection-request", ChainSyncSelectionHint)
  case object FindIntersectionResponse
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.find-intersection-response", ChainSyncSelectionHint)
  case object GetPeerTipRequest extends ConsensusChainSyncWireKind("transport.chainsync.wire.get-peer-tip-request", ChainSyncSelectionHint)
  case object PeerTipResponse extends ConsensusChainSyncWireKind("transport.chainsync.wire.peer-tip-response", ChainSyncSelectionHint)
  case object ServeSnapshotsRequest
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.serve-snapshots-request", ChainSyncSnapshotCarrier)
  case object ServeChainPointsRequest
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.serve-chain-points-request", ChainSyncSelectionHint)
  case object ServeChainPointsResponse
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.serve-chain-points-response", ChainSyncSelectionHint)
  case object FetchMetagraphBinariesRequest
      extends ConsensusChainSyncWireKind(
        "transport.chainsync.wire.fetch-metagraph-binaries-request",
        ChainSyncMetagraphBinaryCarrier
      )
  case object MetagraphBinaryResponse
      extends ConsensusChainSyncWireKind("transport.chainsync.wire.metagraph-binary-response", ChainSyncMetagraphBinaryCarrier)

  val all: List[ConsensusChainSyncWireKind] = List(
    ChainPointComponent,
    FetchSnapshotsRequest,
    SnapshotResponse,
    FindIntersectionRequest,
    FindIntersectionResponse,
    GetPeerTipRequest,
    PeerTipResponse,
    ServeSnapshotsRequest,
    ServeChainPointsRequest,
    ServeChainPointsResponse,
    FetchMetagraphBinariesRequest,
    MetagraphBinaryResponse
  )
}
