package grpcserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/chainsync"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

// Server implements the SidecarService and ChainSyncOutbound gRPC interfaces.
type Server struct {
	pb.UnimplementedSidecarServiceServer
	pb.UnimplementedChainSyncOutboundServer

	node      *gossip.Node
	chainSync *chainsync.Handler
	startedAt time.Time
	grpcSrv   *grpc.Server
}

// New creates a gRPC server backed by the gossip node.
func New(node *gossip.Node, cs *chainsync.Handler) *Server {
	return &Server{
		node:      node,
		chainSync: cs,
		startedAt: time.Now(),
	}
}

// Start begins listening on the given address.
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	s.grpcSrv = grpc.NewServer(
		// Allow client keepalive pings every 20s (JVM client sends every 30s).
		// Without this, Go's default EnforcementPolicy rejects pings more frequent
		// than 5 minutes, preventing the JVM from detecting dead connections after
		// network partitions.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             20 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)
	pb.RegisterSidecarServiceServer(s.grpcSrv, s)
	pb.RegisterChainSyncOutboundServer(s.grpcSrv, s)

	fmt.Printf("gRPC server listening on %s\n", addr)
	return s.grpcSrv.Serve(lis)
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() {
	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
	}
}

// PublishSnapshot broadcasts a snapshot to the network.
func (s *Server) PublishSnapshot(ctx context.Context, snap *pb.Snapshot) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(snap)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishSnapshot(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishAttestation broadcasts an attestation to the network.
func (s *Server) PublishAttestation(ctx context.Context, att *pb.TipAttestation) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(att)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishAttestation(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishRumor broadcasts a generic rumor (event / BFT consensus message / etc).
func (s *Server) PublishRumor(ctx context.Context, ru *pb.Rumor) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(ru)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishRumor(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.PublishResponse{Ok: true}, nil
}

// Subscribe streams incoming gossip messages to the JVM.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.SidecarService_SubscribeServer) error {
	ctx := stream.Context()

	snCh := s.node.SnapshotMessages(ctx)
	atCh := s.node.AttestationMessages(ctx)
	ruCh := s.node.RumorMessages(ctx)
	reconnectCh := s.node.ReconnectCh()

	for {
		select {
		case <-reconnectCh:
			fmt.Println("gRPC Subscribe: mesh recovered, closing stream to force client reconnect")
			return fmt.Errorf("mesh recovered — reconnect required")

		case data, ok := <-snCh:
			if !ok {
				return nil
			}
			var snap pb.Snapshot
			if err := proto.Unmarshal(data, &snap); err != nil {
				continue // skip malformed
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_Snapshot{Snapshot: &snap},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-atCh:
			if !ok {
				return nil
			}
			var att pb.TipAttestation
			if err := proto.Unmarshal(data, &att); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_Attestation{Attestation: &att},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-ruCh:
			if !ok {
				return nil
			}
			var ru pb.Rumor
			if err := proto.Unmarshal(data, &ru); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_Rumor{Rumor: &ru},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PeerCount returns mesh membership stats.
func (s *Server) PeerCount(ctx context.Context, req *pb.PeerCountRequest) (*pb.PeerCountResponse, error) {
	snPeers, atPeers, ruPeers := s.node.MeshPeerCount()
	total := len(s.node.Host.Network().Peers())
	return &pb.PeerCountResponse{
		Total:            int32(total),
		MeshSnapshots:    int32(snPeers),
		MeshAttestations: int32(atPeers),
		MeshRumors:       int32(ruPeers),
	}, nil
}

// Health returns sidecar health info.
func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	uptime := int64(time.Since(s.startedAt).Seconds())
	peerCount := len(s.node.Host.Network().Peers())
	return &pb.HealthResponse{
		Healthy:       true,
		UptimeSeconds: uptime,
		PeerCount:     int32(peerCount),
	}, nil
}

// ─── ChainSyncOutbound: JVM requests chain data from the network ────

// FetchSnapshots fetches specific snapshots by hash from a peer.
func (s *Server) FetchSnapshots(req *pb.FetchSnapshotsRequest, stream pb.ChainSyncOutbound_FetchSnapshotsServer) error {
	if s.chainSync == nil {
		return fmt.Errorf("ChainSync not initialized")
	}
	snapshots, err := s.chainSync.FetchSnapshots(stream.Context(), req.Hashes)
	if err != nil {
		return err
	}
	for _, snap := range snapshots {
		if err := stream.Send(snap); err != nil {
			return err
		}
	}
	return nil
}

// FindIntersection finds the common ancestor between local and remote chains.
func (s *Server) FindIntersection(ctx context.Context, req *pb.FindIntersectionRequest) (*pb.FindIntersectionResponse, error) {
	if s.chainSync == nil {
		return nil, fmt.Errorf("ChainSync not initialized")
	}
	return s.chainSync.FindIntersection(ctx, req.Points)
}

// GetPeerTip returns a random peer's current best tip.
func (s *Server) GetPeerTip(ctx context.Context, req *pb.GetPeerTipRequest) (*pb.PeerTipResponse, error) {
	if s.chainSync == nil {
		return nil, fmt.Errorf("ChainSync not initialized")
	}
	return s.chainSync.GetPeerTip(ctx)
}

// FetchByRange fetches a contiguous range of snapshots from a peer.
func (s *Server) FetchByRange(req *pb.FetchByRangeRequest, stream pb.ChainSyncOutbound_FetchByRangeServer) error {
	if s.chainSync == nil {
		return fmt.Errorf("ChainSync not initialized")
	}
	snapshots, err := s.chainSync.FetchByRange(stream.Context(), req.StartOrdinal, req.EndOrdinal, req.TargetPeerId)
	if err != nil {
		return err
	}
	for _, snap := range snapshots {
		if err := stream.Send(snap); err != nil {
			return err
		}
	}
	return nil
}

// ListPeers returns IDs of all connected peers.
func (s *Server) ListPeers(ctx context.Context, req *pb.ListPeersRequest) (*pb.ListPeersResponse, error) {
	peers := s.chainSync.ListPeers()
	peerIDs := make([][]byte, len(peers))
	for i, p := range peers {
		peerIDs[i] = []byte(p)
	}
	return &pb.ListPeersResponse{PeerIds: peerIDs}, nil
}
