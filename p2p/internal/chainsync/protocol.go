// Package chainsync implements a libp2p request-response protocol for fetching
// missing chain segments from peers. This enables active parent resolution when
// GossipSub delivers snapshots out of order, and bootstrapping for new peers.
//
// Protocol: /nakamoto/chainsync/1.0.0
// Wire format: length-prefixed protobuf over libp2p streams
//
// The sidecar acts as a relay — it does not store snapshots itself. Incoming
// requests from peers are forwarded to the local JVM via ChainSyncInbound gRPC.
// Outgoing requests from the JVM are forwarded to a peer via libp2p streams.
package chainsync

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pb "github.com/scasplte2/tessellation/p2p/proto"
)

const (
	ProtocolID     = protocol.ID("/nakamoto/chainsync/1.0.0")
	MaxMessageSize = 16 * 1024 * 1024 // 16 MB max per message
	RequestTimeout = 30 * time.Second
	MaxHashesPerRequest = 64
	RateLimitPerPeer    = 10 // requests per minute
)

// Handler manages the ChainSync libp2p protocol. It registers a stream handler
// on the host for incoming requests and provides methods for outgoing requests.
type Handler struct {
	host      host.Host
	jvmAddr   string
	jvmConn   *grpc.ClientConn // gRPC connection to JVM's ChainSyncInbound server
	mu        sync.RWMutex
	peerFails map[peer.ID]time.Time // blacklist: peer -> unblock time
}

// New creates a ChainSync handler and registers the libp2p stream handler.
func New(h host.Host, jvmAddr string) (*Handler, error) {
	handler := &Handler{
		host:      h,
		jvmAddr:   jvmAddr,
		peerFails: make(map[peer.ID]time.Time),
	}

	// Register stream handler for incoming requests from peers
	h.SetStreamHandler(ProtocolID, handler.handleIncoming)

	return handler, nil
}

// ensureJVMConn lazily connects to the JVM's ChainSyncInbound gRPC server.
// The JVM server starts after the sidecar, so initial connections fail.
func (h *Handler) ensureJVMConn() *grpc.ClientConn {
	h.mu.RLock()
	if h.jvmConn != nil {
		defer h.mu.RUnlock()
		return h.jvmConn
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.jvmConn != nil {
		return h.jvmConn
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, h.jvmAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		fmt.Printf("[chainsync] JVM connection attempt to %s failed: %v\n", h.jvmAddr, err)
		return nil
	}
	fmt.Printf("[chainsync] Connected to JVM at %s\n", h.jvmAddr)
	h.jvmConn = conn
	return conn
}

// handleIncoming processes an incoming ChainSync request from a remote peer.
// It reads the request, calls the local JVM to get the data, and writes back.
func (h *Handler) handleIncoming(s network.Stream) {
	defer s.Close()

	remotePeer := s.Conn().RemotePeer()

	// Read request type (1 byte). Body is length-prefixed protobuf for most
	// types; GetPeerTip (0x03) is a bare ping with no body — reading a length
	// prefix for it would EOF (sender CloseWrite's after the type byte),
	// triggering spurious markFailed on the sender side and cascading to
	// "no available peers for ChainSync" after all peers get blocked.
	reqType := make([]byte, 1)
	if _, err := io.ReadFull(s, reqType); err != nil {
		fmt.Printf("[chainsync] Failed to read request type from %s: %v\n", remotePeer, err)
		return
	}

	switch reqType[0] {
	case 0x03: // GetPeerTip — no body
		h.serveGetPeerTip(s, remotePeer)
		return
	case 0x01, 0x02, 0x04, 0x05: // FetchSnapshots / FindIntersection / FetchByRange / FetchMetagraphBinaries — length-prefixed body
		data, err := readLengthPrefixed(s)
		if err != nil {
			fmt.Printf("[chainsync] Failed to read request from %s: %v\n", remotePeer, err)
			return
		}
		switch reqType[0] {
		case 0x01:
			h.serveFetchSnapshots(s, data, remotePeer)
		case 0x02:
			h.serveFindIntersection(s, data, remotePeer)
		case 0x04:
			h.serveFetchByRange(s, data, remotePeer)
		case 0x05:
			h.serveMetagraphBinaries(s, data, remotePeer)
		}
	default:
		fmt.Printf("[chainsync] Unknown request type 0x%02x from %s\n", reqType[0], remotePeer)
	}
}

func (h *Handler) serveFetchSnapshots(s network.Stream, data []byte, from peer.ID) {
	conn := h.ensureJVMConn()
	if conn == nil {
		fmt.Printf("[chainsync] No JVM connection, cannot serve snapshots to %s\n", from)
		return
	}

	var req pb.ServeSnapshotsRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		fmt.Printf("[chainsync] Failed to unmarshal FetchSnapshots from %s: %v\n", from, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	client := pb.NewChainSyncInboundClient(conn)
	stream, err := client.ServeSnapshots(ctx, &req)
	if err != nil {
		fmt.Printf("[chainsync] JVM ServeSnapshots failed: %v\n", err)
		return
	}

	// Relay snapshots back to the requesting peer
	for {
		snap, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("[chainsync] JVM ServeSnapshots stream error: %v\n", err)
			break
		}
		respBytes, _ := proto.Marshal(snap)
		if err := writeLengthPrefixed(s, respBytes); err != nil {
			break
		}
	}
}

func (h *Handler) serveFindIntersection(s network.Stream, data []byte, from peer.ID) {
	conn := h.ensureJVMConn()
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	client := pb.NewChainSyncInboundClient(conn)
	localPoints, err := client.ServeChainPoints(ctx, &pb.ServeChainPointsRequest{})
	if err != nil {
		fmt.Printf("[chainsync] JVM ServeChainPoints failed: %v\n", err)
		return
	}

	// Parse the incoming intersection request
	var req pb.FindIntersectionRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return
	}

	// Find intersection: check if any of the requester's points match our chain
	localPointSet := make(map[string]int64)
	for _, p := range localPoints.Points {
		localPointSet[string(p.Hash)] = p.Ordinal
	}

	resp := &pb.FindIntersectionResponse{Found: false}
	if localPoints.TipHash != nil {
		resp.TipHash = localPoints.TipHash
		resp.TipOrdinal = localPoints.TipOrdinal
	}

	for _, p := range req.Points {
		if _, ok := localPointSet[string(p.Hash)]; ok {
			resp.Found = true
			resp.IntersectionHash = p.Hash
			resp.IntersectionOrdinal = p.Ordinal
			break
		}
	}

	respBytes, _ := proto.Marshal(resp)
	writeLengthPrefixed(s, respBytes)
}

func (h *Handler) serveGetPeerTip(s network.Stream, from peer.ID) {
	conn := h.ensureJVMConn()
	if conn == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	client := pb.NewChainSyncInboundClient(conn)
	localPoints, err := client.ServeChainPoints(ctx, &pb.ServeChainPointsRequest{})
	if err != nil {
		return
	}

	resp := &pb.PeerTipResponse{
		Hash:    localPoints.TipHash,
		Ordinal: localPoints.TipOrdinal,
	}
	respBytes, _ := proto.Marshal(resp)
	writeLengthPrefixed(s, respBytes)
}

// FetchSnapshots sends a FetchSnapshots request to a random peer and returns
// the response snapshots. Called by the JVM via ChainSyncOutbound gRPC.
func (h *Handler) FetchSnapshots(ctx context.Context, hashes [][]byte) ([]*pb.Snapshot, error) {
	if len(hashes) > MaxHashesPerRequest {
		return nil, fmt.Errorf("too many hashes: %d > %d", len(hashes), MaxHashesPerRequest)
	}

	target, err := h.pickPeer()
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	s, err := h.host.NewStream(reqCtx, target, ProtocolID)
	if err != nil {
		h.markFailed(target)
		return nil, fmt.Errorf("stream to %s failed: %w", target, err)
	}
	defer s.Close()

	// Write request: type byte + length-prefixed protobuf
	req := &pb.FetchSnapshotsRequest{Hashes: hashes}
	reqBytes, _ := proto.Marshal(req)
	if _, err := s.Write([]byte{0x01}); err != nil {
		h.markFailed(target)
		return nil, err
	}
	if err := writeLengthPrefixed(s, reqBytes); err != nil {
		h.markFailed(target)
		return nil, err
	}
	// Signal we're done writing
	s.CloseWrite()

	// Read response snapshots
	var snapshots []*pb.Snapshot
	for {
		data, err := readLengthPrefixed(s)
		if err == io.EOF {
			break
		}
		if err != nil {
			h.markFailed(target)
			return snapshots, err
		}
		var snap pb.Snapshot
		if err := proto.Unmarshal(data, &snap); err != nil {
			continue
		}
		snapshots = append(snapshots, &snap)
	}

	return snapshots, nil
}

// FindIntersection sends a FindIntersection request to a random peer.
func (h *Handler) FindIntersection(ctx context.Context, points []*pb.ChainPoint) (*pb.FindIntersectionResponse, error) {
	target, err := h.pickPeer()
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	s, err := h.host.NewStream(reqCtx, target, ProtocolID)
	if err != nil {
		h.markFailed(target)
		return nil, fmt.Errorf("stream to %s failed: %w", target, err)
	}
	defer s.Close()

	req := &pb.FindIntersectionRequest{Points: points}
	reqBytes, _ := proto.Marshal(req)
	s.Write([]byte{0x02})
	writeLengthPrefixed(s, reqBytes)
	s.CloseWrite()

	data, err := readLengthPrefixed(s)
	if err != nil {
		h.markFailed(target)
		return nil, err
	}

	var resp pb.FindIntersectionResponse
	if err := proto.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetPeerTip asks a random peer for its current best tip.
func (h *Handler) GetPeerTip(ctx context.Context) (*pb.PeerTipResponse, error) {
	target, err := h.pickPeer()
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	s, err := h.host.NewStream(reqCtx, target, ProtocolID)
	if err != nil {
		h.markFailed(target)
		return nil, fmt.Errorf("stream to %s failed: %w", target, err)
	}
	defer s.Close()

	s.Write([]byte{0x03})
	s.CloseWrite()

	data, err := readLengthPrefixed(s)
	if err != nil {
		h.markFailed(target)
		return nil, err
	}

	var resp pb.PeerTipResponse
	if err := proto.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// serveFetchByRange handles incoming range requests from peers by relaying to JVM.
func (h *Handler) serveFetchByRange(s network.Stream, data []byte, from peer.ID) {
	conn := h.ensureJVMConn()
	if conn == nil {
		fmt.Printf("[chainsync] No JVM connection, cannot serve range to %s\n", from)
		return
	}

	var req pb.FetchByRangeRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		fmt.Printf("[chainsync] Failed to unmarshal FetchByRange from %s: %v\n", from, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	client := pb.NewChainSyncInboundClient(conn)
	stream, err := client.ServeByRange(ctx, &req)
	if err != nil {
		fmt.Printf("[chainsync] JVM ServeByRange failed: %v\n", err)
		return
	}

	for {
		snap, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("[chainsync] JVM ServeByRange stream error: %v\n", err)
			break
		}
		respBytes, _ := proto.Marshal(snap)
		if err := writeLengthPrefixed(s, respBytes); err != nil {
			break
		}
	}
}

// serveMetagraphBinaries handles incoming metagraph-binary fetch requests (#259)
// from peers by relaying to the JVM ServeMetagraphBinaries stream. The JVM looks
// up each requested value-hash among recent-finalized snapshots + a non-destructive
// orphan-buffer peek; the sidecar is a pure relay (mirrors serveFetchByRange).
func (h *Handler) serveMetagraphBinaries(s network.Stream, data []byte, from peer.ID) {
	conn := h.ensureJVMConn()
	if conn == nil {
		fmt.Printf("[chainsync] No JVM connection, cannot serve metagraph binaries to %s\n", from)
		return
	}

	var req pb.FetchMetagraphBinariesRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		fmt.Printf("[chainsync] Failed to unmarshal FetchMetagraphBinaries from %s: %v\n", from, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	client := pb.NewChainSyncInboundClient(conn)
	stream, err := client.ServeMetagraphBinaries(ctx, &req)
	if err != nil {
		fmt.Printf("[chainsync] JVM ServeMetagraphBinaries failed: %v\n", err)
		return
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("[chainsync] JVM ServeMetagraphBinaries stream error: %v\n", err)
			break
		}
		respBytes, _ := proto.Marshal(resp)
		if err := writeLengthPrefixed(s, respBytes); err != nil {
			break
		}
	}
}

// FetchMetagraphBinaries sends a metagraph-binary fetch request (#259) to a
// random peer and returns the matched binaries. Called by the JVM via
// ChainSyncOutbound gRPC (mirrors the FetchSnapshots outgoing path). The
// `binaryHashes` are value-hashes (UTF-8 of canonical Hash hex); the JVM
// re-feeds each returned binary through the committee gate.
func (h *Handler) FetchMetagraphBinaries(ctx context.Context, address string, hashes [][]byte) ([]*pb.MetagraphBinaryResponse, error) {
	if len(hashes) > MaxHashesPerRequest {
		return nil, fmt.Errorf("too many hashes: %d > %d", len(hashes), MaxHashesPerRequest)
	}

	target, err := h.pickPeer()
	if err != nil {
		return nil, err
	}

	reqCtx, cancel := context.WithTimeout(ctx, RequestTimeout)
	defer cancel()

	s, err := h.host.NewStream(reqCtx, target, ProtocolID)
	if err != nil {
		h.markFailed(target)
		return nil, fmt.Errorf("stream to %s failed: %w", target, err)
	}
	defer s.Close()

	req := &pb.FetchMetagraphBinariesRequest{MetagraphAddress: address, BinaryHashes: hashes}
	reqBytes, _ := proto.Marshal(req)
	if _, err := s.Write([]byte{0x05}); err != nil {
		h.markFailed(target)
		return nil, err
	}
	if err := writeLengthPrefixed(s, reqBytes); err != nil {
		h.markFailed(target)
		return nil, err
	}
	s.CloseWrite()

	var responses []*pb.MetagraphBinaryResponse
	for {
		data, err := readLengthPrefixed(s)
		if err == io.EOF {
			break
		}
		if err != nil {
			h.markFailed(target)
			return responses, err
		}
		var resp pb.MetagraphBinaryResponse
		if err := proto.Unmarshal(data, &resp); err != nil {
			continue
		}
		responses = append(responses, &resp)
	}

	return responses, nil
}

// FetchByRange sends a range request to a specific peer (or random if no target).
// Returns BackfillSnapshot messages for the requested ordinal range.
func (h *Handler) FetchByRange(ctx context.Context, startOrdinal, endOrdinal int64, targetPeerID []byte) ([]*pb.BackfillSnapshot, error) {
	var target peer.ID
	if len(targetPeerID) > 0 {
		target = peer.ID(targetPeerID)
	} else {
		var err error
		target, err = h.pickPeer()
		if err != nil {
			return nil, err
		}
	}

	reqCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	s, err := h.host.NewStream(reqCtx, target, ProtocolID)
	if err != nil {
		h.markFailed(target)
		return nil, fmt.Errorf("stream to %s failed: %w", target, err)
	}
	defer s.Close()

	req := &pb.FetchByRangeRequest{
		StartOrdinal: startOrdinal,
		EndOrdinal:   endOrdinal,
	}
	reqBytes, _ := proto.Marshal(req)
	if _, err := s.Write([]byte{0x04}); err != nil {
		h.markFailed(target)
		return nil, err
	}
	if err := writeLengthPrefixed(s, reqBytes); err != nil {
		h.markFailed(target)
		return nil, err
	}
	s.CloseWrite()

	var snapshots []*pb.BackfillSnapshot
	for {
		data, err := readLengthPrefixed(s)
		if err == io.EOF {
			break
		}
		if err != nil {
			h.markFailed(target)
			return snapshots, err
		}
		var snap pb.BackfillSnapshot
		if err := proto.Unmarshal(data, &snap); err != nil {
			continue
		}
		snapshots = append(snapshots, &snap)
	}

	return snapshots, nil
}

// ListPeers returns the IDs of all connected peers.
func (h *Handler) ListPeers() []peer.ID {
	return h.host.Network().Peers()
}

func (h *Handler) pickPeer() (peer.ID, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	now := time.Now()
	peers := h.host.Network().Peers()
	var candidates []peer.ID
	for _, p := range peers {
		if unblock, ok := h.peerFails[p]; ok && now.Before(unblock) {
			continue
		}
		candidates = append(candidates, p)
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no available peers for ChainSync")
	}
	return candidates[rand.Intn(len(candidates))], nil
}

func (h *Handler) markFailed(p peer.ID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.peerFails[p] = time.Now().Add(60 * time.Second)
}

// Wire helpers: length-prefixed protobuf framing

func readLengthPrefixed(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	if length > MaxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes", length)
	}
	data := make([]byte, length)
	_, err := io.ReadFull(r, data)
	return data, err
}

func writeLengthPrefixed(w io.Writer, data []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}
