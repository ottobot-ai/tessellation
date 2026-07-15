package grpcserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/chainsync"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	"github.com/scasplte2/tessellation/p2p/internal/metrics"
	"github.com/scasplte2/tessellation/p2p/internal/outbox"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

// Topic name labels used on the outbox + ConfirmFinalized wire. Kept in one
// place so the JVM caller can match by string literal — these are the
// authoritative names. Distinct from the GossipSub topic strings (which are
// versioned multistream paths); the outbox labels are short and stable.
const (
	TopicAllowSpendBlock      = "allow-spend-block"
	TopicMetagraphBinary      = "metagraph-binary"
	TopicMetagraphAttestation = "metagraph-attestation"
	TopicDAGBlock             = "dag-block"
	TopicTokenLockBlock       = "token-lock-block"

	// Slice 14: shard-checkpoint outbox labels. These are the stable
	// family-prefix labels the JVM addresses entries by (matching
	// SidecarClient.OutboxTopic.ShardCheckpoint /
	// .ShardCheckpointAttestation); the actual per-shard GossipSub topic
	// (`<topic-prefix><shardId>`) is derived from the message's shard_id at
	// publish time. Distinct from the GossipSub topic-path prefixes in
	// config.Config.
	TopicShardCheckpoint            = "shard-checkpoint"
	TopicShardCheckpointAttestation = "shard-checkpoint-attestation"

	// WATCHTOWER fraud-proof outbox label (EPIC-9-NET M4). gl0-wide topic —
	// no shard-id suffix. Matches SidecarClient.OutboxTopic.FraudProof.
	TopicFraudProof = "fraud-proof"

	// Subscribe-filter labels for the three families that have no outbox
	// label (they are never outbox-tracked). Together with the Topic*
	// constants above these form the complete SubscribeRequest.topics
	// vocabulary — one label per GossipMessage.body arm. The JVM mirror is
	// SidecarClient.SubscribeTopics; a rename without updating both sides
	// desyncs the subscribe filter.
	TopicSnapshot    = "snapshot"
	TopicAttestation = "attestation"
	TopicRumor       = "rumor"
)

// GossipNode is the narrow view of *gossip.Node the gRPC server depends on.
// An interface seam (consumer-side, Go-idiomatic) so Subscribe-routing
// behaviour is unit-testable against a fake node without a live libp2p host —
// the F1 dual-Subscribe race regression test needs exactly that.
//
// Channel semantics contract (load-bearing — see Subscribe):
//   - the *Messages(ctx) methods create a fresh per-call subscription each
//     call (fan-out: every caller sees every message);
//   - ShardCheckpointMessages / ShardCheckpointAttestationMessages return the
//     SINGLE node-lifetime shared fan-in channel (each message is delivered to
//     exactly ONE reader — concurrent readers race);
//   - acquisition methods return errors for inactive or failed subscriptions;
//     Subscribe must not emit Started after any such error.
type GossipNode interface {
	PublishSnapshot(ctx context.Context, data []byte) error
	PublishAttestation(ctx context.Context, data []byte) error
	PublishRumor(ctx context.Context, data []byte) error
	PublishMetagraphBinary(ctx context.Context, data []byte) error
	PublishMetagraphAttestation(ctx context.Context, data []byte) error
	PublishAllowSpendBlock(ctx context.Context, data []byte) error
	PublishDAGBlock(ctx context.Context, data []byte) error
	PublishTokenLockBlock(ctx context.Context, data []byte) error
	PublishShardCheckpoint(ctx context.Context, shardID uint32, data []byte) error
	PublishShardCheckpointAttestation(ctx context.Context, shardID uint32, data []byte) error
	PublishFraudProof(ctx context.Context, data []byte) error

	SnapshotMessages(ctx context.Context) (<-chan []byte, error)
	AttestationMessages(ctx context.Context) (<-chan []byte, error)
	RumorMessages(ctx context.Context) (<-chan []byte, error)
	MetagraphBinaryMessages(ctx context.Context) (<-chan []byte, error)
	MetagraphAttestationMessages(ctx context.Context) (<-chan []byte, error)
	AllowSpendBlockMessages(ctx context.Context) (<-chan []byte, error)
	DAGBlockMessages(ctx context.Context) (<-chan []byte, error)
	TokenLockBlockMessages(ctx context.Context) (<-chan []byte, error)
	FraudProofMessages(ctx context.Context) (<-chan []byte, error)
	ShardCheckpointMessages() (<-chan []byte, error)
	ShardCheckpointAttestationMessages() (<-chan []byte, error)
	ShardSubscriptionsActive() bool

	ReconnectCh() <-chan struct{}
	MeshPeerCount() (snapshots, attestations, rumors, metagraphBinaries, metagraphAttestations, allowSpendBlocks, dagBlocks, tokenLockBlocks int)
	ShardMeshPeerCount() (checkpoints, attestations int)
	ConnectedPeerCount() int
}

// Compile-time proof that the concrete node satisfies the seam.
var _ GossipNode = (*gossip.Node)(nil)

// Server implements the SidecarService and ChainSyncOutbound gRPC interfaces.
type Server struct {
	pb.UnimplementedSidecarServiceServer
	pb.UnimplementedChainSyncOutboundServer

	node      GossipNode
	chainSync *chainsync.Handler
	outbox    *outbox.Outbox
	startedAt time.Time
	grpcSrv   *grpc.Server

	// sessionID is regenerated on every sidecar process start. generation is
	// monotonic only within that session; together they let the JVM distinguish
	// a reconnect from a sidecar restart without claiming any network readiness.
	sessionID string

	// shardDrainStreams is an exclusive lease for the SHARED
	// shard fan-in channels. The channels' semantics tolerate exactly one
	// drainer (two would race and silently split deliveries — FINDING-F1). A
	// competing Subscribe fails before Started instead of sharing deliveries.
	shardDrainStreams atomic.Int32
}

var (
	sidecarProcessSessionID string
	processStreamGeneration atomic.Uint64
)

func init() {
	sidecarProcessSessionID = mustNewSessionID()
}

// New creates a gRPC server backed by the gossip node. The outbox is the
// shared durable-publish ledger (#196); the same instance is also held by
// the republish goroutine in main.go so confirmed entries disappear from
// the periodic resend immediately.
func New(node GossipNode, cs *chainsync.Handler, ob *outbox.Outbox) *Server {
	return &Server{
		node:      node,
		chainSync: cs,
		outbox:    ob,
		startedAt: time.Now(),
		sessionID: sidecarProcessSessionID,
	}
}

func mustNewSessionID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		panic(fmt.Sprintf("generate sidecar session id: %v", err))
	}
	return hex.EncodeToString(raw[:])
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

// PublishMetagraphBinary broadcasts a state channel snapshot binary to all
// GL0 nodes over the metagraph-binary topic. Opaque payload — sidecar only
// wraps and routes. Tracked in the outbox so the periodic ticker can resend
// if a transient mesh stall ate the first publish (#196).
func (s *Server) PublishMetagraphBinary(ctx context.Context, mb *pb.MetagraphBinary) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(mb)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishMetagraphBinary(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Outbox key = sha256 of the SIGNED PAYLOAD (mb.Binary), not the
	// proto-marshalled wrapper. The JVM addresses entries by the bytes it
	// serialized, not by the proto envelope it built around them. Same
	// rule for the other two topics below.
	s.outbox.Add(TopicMetagraphBinary, outbox.MsgIDFor(mb.Binary), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishMetagraphAttestation broadcasts a per-metagraph committee attestation
// (Slice S2) to all GL0 nodes over the metagraph-attestation topic. Opaque
// payload — sidecar only wraps and routes. Outbox-tracked (#196).
func (s *Server) PublishMetagraphAttestation(ctx context.Context, ma *pb.MetagraphAttestation) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(ma)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishMetagraphAttestation(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// MetagraphAttestation id = sha256 of the wire-level proto-marshalled
	// bytes (task brief §JVM hook). Unlike MetagraphBinary, the attestation
	// has no single "payload" field — the entire structured message IS the
	// payload. Both sender and receiver compute the id from these bytes.
	s.outbox.Add(TopicMetagraphAttestation, outbox.MsgIDFor(data), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishAllowSpendBlock broadcasts a Signed[AllowSpendBlock] to all GL0
// nodes over the allow-spend-block topic. Replaces the single-peer HTTP POST
// from Swap.sendBlockToL0 — the prior code's "pick a random peer" lottery
// dropped the block when the chosen peer was in reorg-recovery
// (iter-s3prep-baseline-mg4 diagnostic). Outbox-tracked so transient mesh
// stalls don't drop the block; the JVM acks via ConfirmFinalized after the
// block reaches Phase-3 finality on a global snapshot. (#196)
func (s *Server) PublishAllowSpendBlock(ctx context.Context, asb *pb.AllowSpendBlock) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(asb)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishAllowSpendBlock(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Outbox id = sha256 of the inner Signed[AllowSpendBlock] payload —
	// matches how the JVM addresses the block in ConfirmFinalized.
	s.outbox.Add(TopicAllowSpendBlock, outbox.MsgIDFor(asb.Payload), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishDAGBlock broadcasts a Signed[Block] to all GL0 nodes over the
// dag-block topic. Replaces the single-peer HTTP POST from
// StateChannel.sendBlockToL0; same #196 durable-outbox semantics.
func (s *Server) PublishDAGBlock(ctx context.Context, blk *pb.DAGBlock) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(blk)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishDAGBlock(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Outbox id = sha256 of the inner Signed[Block] payload — matches how the
	// JVM addresses the block in ConfirmFinalized.
	s.outbox.Add(TopicDAGBlock, outbox.MsgIDFor(blk.Payload), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishTokenLockBlock broadcasts a Signed[TokenLockBlock] to all GL0 nodes
// over the token-lock-block topic. Replaces the single-peer HTTP POST from
// TokenLock.sendBlockToL0; same #196 durable-outbox semantics.
func (s *Server) PublishTokenLockBlock(ctx context.Context, blk *pb.TokenLockBlock) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(blk)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishTokenLockBlock(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Outbox id = sha256 of the inner Signed[TokenLockBlock] payload — matches
	// how the JVM addresses the block in ConfirmFinalized.
	s.outbox.Add(TopicTokenLockBlock, outbox.MsgIDFor(blk.Payload), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishShardCheckpoint broadcasts a shard checkpoint envelope to the
// per-shard checkpoint topic (Slice 14). The sidecar derives the GossipSub
// topic from sc.ShardId — only operators in that shard subscribe, so non-shard
// gl0s shed the load (design doc §6.4). Opaque payload — sidecar only wraps and
// routes. Outbox-tracked; mirrors PublishMetagraphAttestation, with the shard
// id threaded through so the periodic republisher can re-derive the topic.
func (s *Server) PublishShardCheckpoint(ctx context.Context, sc *pb.ShardCheckpointWire) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(sc)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishShardCheckpoint(ctx, sc.ShardId, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Like MetagraphAttestation, the entire structured message IS the payload —
	// no single inner field — so the outbox id is sha256 of the wire-level
	// proto bytes. Both sender and receiver compute the id from these bytes.
	// The shard id needed to re-derive the per-shard topic on republish is read
	// back out of these same bytes in main.go's republisher.
	s.outbox.Add(TopicShardCheckpoint, outbox.MsgIDFor(data), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishShardCheckpointAttestation broadcasts a non-producing committee
// member's attestation to the per-shard checkpoint-attestation topic (Slice
// 14). Topic derived from sca.ShardId. Opaque payload — sidecar only wraps and
// routes. Outbox-tracked; mirrors PublishMetagraphAttestation.
func (s *Server) PublishShardCheckpointAttestation(ctx context.Context, sca *pb.ShardCheckpointAttestationWire) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(sca)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishShardCheckpointAttestation(ctx, sca.ShardId, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	s.outbox.Add(TopicShardCheckpointAttestation, outbox.MsgIDFor(data), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// PublishFraudProof broadcasts a watchtower fraud proof on the gl0-WIDE
// fraud-proof topic (EPIC-9-NET M4 — the transport leg of the watchtower
// slashing tooth). EVERY gl0 receives it and independently re-runs the
// deterministic dispute verdict over the disputed checkpoint's own bytes
// (WATCHTOWER-FRAUD-PROOF-DESIGN.md §10.2). Opaque payload — sidecar only
// wraps and routes. Outbox-tracked: a fraud proof is slashing EVIDENCE and
// must not be lossy, so the periodic republisher resends it until the JVM
// confirms or the TTL lapses (content-derived message IDs make republishes
// dedupe at peers' seen-caches). Fails ok=false when the fraud topic is not
// joined (numShards <= 1 — the watchtower is inert there and the JVM emitter
// is never constructed; an arriving call means JVM/sidecar config split).
func (s *Server) PublishFraudProof(ctx context.Context, fp *pb.FraudProofEnvelopeWire) (*pb.PublishResponse, error) {
	data, err := proto.Marshal(fp)
	if err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := s.node.PublishFraudProof(ctx, data); err != nil {
		return &pb.PublishResponse{Ok: false, Error: err.Error()}, nil
	}
	// Like MetagraphAttestation, the entire structured message IS the payload
	// — no single inner field — so the outbox id is sha256 of the wire-level
	// proto bytes. Both sender and receiver compute the id from these bytes.
	s.outbox.Add(TopicFraudProof, outbox.MsgIDFor(data), data)
	return &pb.PublishResponse{Ok: true}, nil
}

// ConfirmFinalized drops outbox entries for the named ids on the named
// topic. Called by the JVM Phase-3 finality hook after a global snapshot
// is fully finalized — at that point the entries it includes are durably
// committed network-wide and the sidecar can stop re-gossiping. Idempotent
// at every level: duplicate confirmations and unknown ids are silently
// accepted; the response carries the actual number of entries dropped so
// JVM-side logs can detect total-mismatch (a real bug worth surfacing).
func (s *Server) ConfirmFinalized(ctx context.Context, req *pb.ConfirmFinalizedRequest) (*pb.ConfirmFinalizedResponse, error) {
	dropped := s.outbox.Confirm(req.Topic, req.MessageIds)
	return &pb.ConfirmFinalizedResponse{Dropped: int32(dropped)}, nil
}

var canonicalSubscribeTopics = []string{
	TopicSnapshot,
	TopicAttestation,
	TopicRumor,
	TopicMetagraphBinary,
	TopicMetagraphAttestation,
	TopicAllowSpendBlock,
	TopicDAGBlock,
	TopicTokenLockBlock,
	TopicShardCheckpoint,
	TopicShardCheckpointAttestation,
	TopicFraudProof,
}

// normalizeSubscribeTopics validates an explicit request and returns both a
// membership set and a deduplicated list in protocol-canonical order. The
// canonical list is echoed in SubscribeStarted, so the client learns the exact
// local subscriptions acquired rather than inferring readiness from request
// receipt.
func normalizeSubscribeTopics(topics []string) (map[string]bool, []string, error) {
	if len(topics) == 0 {
		return nil, nil, status.Error(codes.InvalidArgument, "subscribe topics must be explicit and nonempty")
	}
	known := make(map[string]bool, len(canonicalSubscribeTopics))
	for _, topic := range canonicalSubscribeTopics {
		known[topic] = true
	}
	set := make(map[string]bool, len(topics))
	for _, tp := range topics {
		if !known[tp] {
			return nil, nil, status.Errorf(codes.InvalidArgument, "unknown subscribe topic label %q (vocabulary: snapshot, attestation, rumor, metagraph-binary, metagraph-attestation, allow-spend-block, dag-block, token-lock-block, shard-checkpoint, shard-checkpoint-attestation, fraud-proof)", tp)
		}
		set[tp] = true
	}
	normalized := make([]string, 0, len(set))
	for _, topic := range canonicalSubscribeTopics {
		if set[topic] {
			normalized = append(normalized, topic)
		}
	}
	return set, normalized, nil
}

func canonicalNakamotoTopics(shardSubscriptionsActive bool) []string {
	topics := make([]string, 0, len(canonicalSubscribeTopics)-1)
	for _, topic := range canonicalSubscribeTopics {
		if topic == TopicRumor {
			continue
		}
		shardingOnly := topic == TopicShardCheckpoint ||
			topic == TopicShardCheckpointAttestation ||
			topic == TopicFraudProof
		if shardingOnly && !shardSubscriptionsActive {
			continue
		}
		topics = append(topics, topic)
	}
	return topics
}

func matchesExactTopics(actual map[string]bool, expected []string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, topic := range expected {
		if !actual[topic] {
			return false
		}
	}
	return true
}

func validateSubscriberRole(role pb.SubscriberRole, topics map[string]bool, shardSubscriptionsActive bool) error {
	switch role {
	case pb.SubscriberRole_SUBSCRIBER_ROLE_RUMOR_BRIDGE:
		if len(topics) != 1 || !topics[TopicRumor] {
			return status.Errorf(codes.InvalidArgument, "rumor-bridge role must request exactly %q", TopicRumor)
		}
		return nil
	case pb.SubscriberRole_SUBSCRIBER_ROLE_NAKAMOTO_SYNC:
		expected := canonicalNakamotoTopics(shardSubscriptionsActive)
		if !matchesExactTopics(topics, expected) {
			return status.Errorf(codes.InvalidArgument, "nakamoto-sync role topics must exactly match the active sidecar profile %v", expected)
		}
		return nil
	case pb.SubscriberRole_SUBSCRIBER_ROLE_UNSPECIFIED:
		return status.Error(codes.InvalidArgument, "subscriber role must be specified")
	default:
		return status.Errorf(codes.InvalidArgument, "unknown subscriber role %d", role)
	}
}

// Subscribe streams incoming gossip messages to the JVM, filtered to the
// message families named in SubscribeRequest.topics.
//
// FINDING-F1 fix: the filter exists because the JVM holds TWO concurrent
// Subscribe streams (the SidecarRumorBridge and the NakamotoSyncDaemon) and
// the shard-checkpoint families are SHARED node-lifetime fan-in channels —
// each message is handed to exactly ONE drainer. Two subscribe-all streams
// race-drained them, and the bridge's `.collect isRumor` silently discarded
// the shard checkpoints it won (~half). With the filter, the bridge requests
// rumor-only and the daemon requests the non-rumor families, so the shared
// channels get exactly one drainer by construction. Universal topics are
// per-call fan-out subscriptions and were never at risk; unrequested families
// simply skip the subscription (a nil channel never fires in the select).
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.SidecarService_SubscribeServer) error {
	ctx := stream.Context()
	relayCtx, cancelRelays := context.WithCancel(ctx)
	defer cancelRelays()

	set, normalizedTopics, terr := normalizeSubscribeTopics(req.GetTopics())
	if terr != nil {
		return terr
	}
	if err := validateSubscriberRole(req.GetRole(), set, s.node.ShardSubscriptionsActive()); err != nil {
		return err
	}
	wants := func(label string) bool { return set[label] }

	// The shard channels are node-lifetime fan-in drains, not fan-out
	// subscriptions. Acquire their exclusive lease before opening any universal
	// subscriptions. A reconnect must wait for cancellation of the old stream;
	// overlap would split messages and cannot be acknowledged as Started.
	drainsShard := wants(TopicShardCheckpoint) || wants(TopicShardCheckpointAttestation)
	if drainsShard {
		if !s.shardDrainStreams.CompareAndSwap(0, 1) {
			return status.Error(codes.AlreadyExists, "a Subscribe stream already owns the shard-checkpoint drain lease")
		}
		metrics.ShardDrainStreams.Set(1)
		defer func() {
			s.shardDrainStreams.Store(0)
			metrics.ShardDrainStreams.Set(0)
		}()
	}

	// Universal topics: fresh per-call fan-out subscriptions, opened only for
	// requested families (unrequested = nil channel, never fires).
	var snCh, atCh, ruCh, mbCh, maCh, asbCh, dagCh, tlbCh, fpCh <-chan []byte
	var err error
	if wants(TopicSnapshot) {
		snCh, err = s.node.SnapshotMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire snapshot subscription: %v", err)
		}
	}
	if wants(TopicAttestation) {
		atCh, err = s.node.AttestationMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire attestation subscription: %v", err)
		}
	}
	if wants(TopicRumor) {
		ruCh, err = s.node.RumorMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire rumor subscription: %v", err)
		}
	}
	if wants(TopicMetagraphBinary) {
		mbCh, err = s.node.MetagraphBinaryMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire metagraph-binary subscription: %v", err)
		}
	}
	if wants(TopicMetagraphAttestation) {
		maCh, err = s.node.MetagraphAttestationMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire metagraph-attestation subscription: %v", err)
		}
	}
	if wants(TopicAllowSpendBlock) {
		asbCh, err = s.node.AllowSpendBlockMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire allow-spend-block subscription: %v", err)
		}
	}
	if wants(TopicDAGBlock) {
		dagCh, err = s.node.DAGBlockMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire dag-block subscription: %v", err)
		}
	}
	if wants(TopicTokenLockBlock) {
		tlbCh, err = s.node.TokenLockBlockMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire token-lock-block subscription: %v", err)
		}
	}
	// WATCHTOWER fraud proofs (EPIC-9-NET M4): universal-topic class — per-
	// call fan-out, immune to the shared-channel race by construction. A request
	// fails before Started when the topic is inactive at numShards <= 1.
	if wants(TopicFraudProof) {
		fpCh, err = s.node.FraudProofMessages(relayCtx)
		if err != nil {
			return status.Errorf(codes.Unavailable, "acquire fraud-proof subscription: %v", err)
		}
	}

	// Slice 14: shared per-shard fan-in channels. Unlike the universal topics
	// these are node-lifetime channels (not per-call subscriptions) populated
	// by relays started when each shard topic is first joined — so a single
	// Subscribe stream sees envelopes/attestations from every shard this node
	// has joined. Each message is delivered to exactly ONE drainer, so at
	// most one live stream may request these families. The topics filter
	// guarantees that for the two JVM consumers; the exclusive lease above
	// rejects a second concurrent drainer instead of silently splitting
	// deliveries — the exact FINDING-F1 failure shape.
	var scCh, scaCh <-chan []byte
	if drainsShard {
		if wants(TopicShardCheckpoint) {
			scCh, err = s.node.ShardCheckpointMessages()
			if err != nil {
				return status.Errorf(codes.Unavailable, "acquire shard-checkpoint drain: %v", err)
			}
		}
		if wants(TopicShardCheckpointAttestation) {
			scaCh, err = s.node.ShardCheckpointAttestationMessages()
			if err != nil {
				return status.Errorf(codes.Unavailable, "acquire shard-checkpoint-attestation drain: %v", err)
			}
		}
	}

	generation := processStreamGeneration.Add(1)
	started := &pb.GossipMessage{
		Body: &pb.GossipMessage_Started{Started: &pb.SubscribeStarted{
			Topics:           normalizedTopics,
			Role:             req.GetRole(),
			StreamGeneration: generation,
			SidecarSessionId: s.sessionID,
		}},
	}
	if err := stream.Send(started); err != nil {
		return err
	}

	reconnectCh := s.node.ReconnectCh()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

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

		case data, ok := <-mbCh:
			if !ok {
				return nil
			}
			var mb pb.MetagraphBinary
			if err := proto.Unmarshal(data, &mb); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_MetagraphBinary{MetagraphBinary: &mb},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-maCh:
			if !ok {
				return nil
			}
			var ma pb.MetagraphAttestation
			if err := proto.Unmarshal(data, &ma); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_MetagraphAttestation{MetagraphAttestation: &ma},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-asbCh:
			if !ok {
				return nil
			}
			var asb pb.AllowSpendBlock
			if err := proto.Unmarshal(data, &asb); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_AllowSpendBlock{AllowSpendBlock: &asb},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-dagCh:
			if !ok {
				return nil
			}
			var blk pb.DAGBlock
			if err := proto.Unmarshal(data, &blk); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_DagBlock{DagBlock: &blk},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-tlbCh:
			if !ok {
				return nil
			}
			var blk pb.TokenLockBlock
			if err := proto.Unmarshal(data, &blk); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_TokenLockBlock{TokenLockBlock: &blk},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-scCh:
			if !ok {
				return nil
			}
			var sc pb.ShardCheckpointWire
			if err := proto.Unmarshal(data, &sc); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_ShardCheckpoint{ShardCheckpoint: &sc},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-scaCh:
			if !ok {
				return nil
			}
			var sca pb.ShardCheckpointAttestationWire
			if err := proto.Unmarshal(data, &sca); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_ShardCheckpointAttestation{ShardCheckpointAttestation: &sca},
			}
			if err := stream.Send(msg); err != nil {
				return err
			}

		case data, ok := <-fpCh:
			if !ok {
				return nil
			}
			var fp pb.FraudProofEnvelopeWire
			if err := proto.Unmarshal(data, &fp); err != nil {
				continue
			}
			msg := &pb.GossipMessage{
				Body: &pb.GossipMessage_FraudProof{FraudProof: &fp},
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
	snPeers, atPeers, ruPeers, mbPeers, maPeers, asbPeers, dagPeers, tlbPeers := s.node.MeshPeerCount()
	// Per-shard topic families, summed across joined shards (see
	// Node.ShardMeshPeerCount for why sum, not max). 0 until the first shard
	// topic is joined AND has mesh peers — exactly the visibility task #40 needs.
	scPeers, scaPeers := s.node.ShardMeshPeerCount()
	total := s.node.ConnectedPeerCount()
	return &pb.PeerCountResponse{
		Total:                           int32(total),
		MeshSnapshots:                   int32(snPeers),
		MeshAttestations:                int32(atPeers),
		MeshRumors:                      int32(ruPeers),
		MeshMetagraphBinaries:           int32(mbPeers),
		MeshMetagraphAttestations:       int32(maPeers),
		MeshAllowSpendBlocks:            int32(asbPeers),
		MeshDagBlocks:                   int32(dagPeers),
		MeshTokenLockBlocks:             int32(tlbPeers),
		MeshShardCheckpoints:            int32(scPeers),
		MeshShardCheckpointAttestations: int32(scaPeers),
	}, nil
}

// Health returns sidecar health info.
func (s *Server) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	uptime := int64(time.Since(s.startedAt).Seconds())
	peerCount := s.node.ConnectedPeerCount()
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

// FetchMetagraphBinaries pulls missing metagraph binaries by value-hash from a
// peer (#259), relays to the chainsync handler, and streams each matched binary
// back to the JVM caller.
func (s *Server) FetchMetagraphBinaries(req *pb.FetchMetagraphBinariesRequest, stream pb.ChainSyncOutbound_FetchMetagraphBinariesServer) error {
	if s.chainSync == nil {
		return fmt.Errorf("ChainSync not initialized")
	}
	responses, err := s.chainSync.FetchMetagraphBinaries(stream.Context(), req.MetagraphAddress, req.BinaryHashes)
	if err != nil {
		return err
	}
	for _, resp := range responses {
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	return nil
}
