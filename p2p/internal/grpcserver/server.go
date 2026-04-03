package grpcserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

// Server implements the SidecarService gRPC interface.
type Server struct {
	pb.UnimplementedSidecarServiceServer

	node      *gossip.Node
	startedAt time.Time
	grpcSrv   *grpc.Server
}

// New creates a gRPC server backed by the gossip node.
func New(node *gossip.Node) *Server {
	return &Server{
		node:      node,
		startedAt: time.Now(),
	}
}

// Start begins listening on the given address.
func (s *Server) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	s.grpcSrv = grpc.NewServer()
	pb.RegisterSidecarServiceServer(s.grpcSrv, s)

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

// Subscribe streams incoming gossip messages to the JVM.
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.SidecarService_SubscribeServer) error {
	ctx := stream.Context()

	snCh := s.node.SnapshotMessages(ctx)
	atCh := s.node.AttestationMessages(ctx)

	for {
		select {
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

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// PeerCount returns mesh membership stats.
func (s *Server) PeerCount(ctx context.Context, req *pb.PeerCountRequest) (*pb.PeerCountResponse, error) {
	snPeers, atPeers := s.node.MeshPeerCount()
	total := len(s.node.Host.Network().Peers())
	return &pb.PeerCountResponse{
		Total:            int32(total),
		MeshSnapshots:    int32(snPeers),
		MeshAttestations: int32(atPeers),
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
