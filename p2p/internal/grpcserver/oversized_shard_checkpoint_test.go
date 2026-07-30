package grpcserver

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	"github.com/scasplte2/tessellation/p2p/internal/outbox"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

// TestTB03OversizedShardCheckpointFalseSuccess characterizes the open TB-03
// defect. It intentionally asserts the current broken behavior so O18 can
// replace it with closing tests once the descriptor/chunk and byte-budget
// contract is ratified.
//
// This is not an O18 size-policy test. DefaultMaxMessageSize is the active
// go-libp2p-pubsub v0.15.0 wire ceiling. Topic.Publish accepts the message into
// its local publish queue, but GossipSub later drops the indivisible oversized
// per-peer RPC. The sidecar therefore returns ok=true and retains an outbox
// entry for bytes that the subscribed peer never receives.
func TestTB03OversizedShardCheckpointFalseSuccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodeA := newTB03GossipNode(t, ctx)
	defer nodeA.Close()
	nodeB := newTB03GossipNode(t, ctx)
	defer nodeB.Close()

	if err := nodeA.Host.Connect(ctx, peer.AddrInfo{ID: nodeB.Host.ID(), Addrs: nodeB.Host.Addrs()}); err != nil {
		t.Fatalf("connect publisher to subscriber: %v", err)
	}

	received, err := nodeB.ShardCheckpointMessages()
	if err != nil {
		t.Fatalf("acquire shard-checkpoint receive channel: %v", err)
	}
	waitForTB03PeerSubscription(t, ctx, nodeA, "shard_checkpoint_0")

	ob := outbox.New(time.Millisecond, time.Hour)
	client := newTB03SidecarClient(t, New(nodeA, nil, ob))

	// Prove the exact peer/topic/relay path works before observing non-delivery.
	control := &pb.ShardCheckpointWire{
		ShardId:      0,
		ShardOrdinal: 1,
		DerivedStateDelta: &pb.ShardDerivedStateDeltaWire{
			PayloadJson: []byte("deliverable-control"),
		},
	}
	controlBytes, err := proto.Marshal(control)
	if err != nil {
		t.Fatalf("marshal control checkpoint: %v", err)
	}
	controlResponse, err := client.PublishShardCheckpoint(ctx, control)
	if err != nil {
		t.Fatalf("publish control checkpoint RPC: %v", err)
	}
	if !controlResponse.Ok {
		t.Fatalf("publish control checkpoint rejected: %s", controlResponse.Error)
	}
	requireTB03Delivery(t, received, controlBytes, nil, "pre-oversize control")
	confirmTB03OutboxEntry(t, ob, controlBytes, "pre-oversize control")

	oversized := &pb.ShardCheckpointWire{
		ShardId:      0,
		ShardOrdinal: 2,
		DerivedStateDelta: &pb.ShardDerivedStateDeltaWire{
			PayloadJson: make([]byte, pubsub.DefaultMaxMessageSize),
		},
	}
	oversizedBytes, err := proto.Marshal(oversized)
	if err != nil {
		t.Fatalf("marshal oversized checkpoint: %v", err)
	}
	if len(oversizedBytes) <= pubsub.DefaultMaxMessageSize {
		t.Fatalf(
			"test fixture is not above the active GossipSub ceiling: encoded=%d limit=%d",
			len(oversizedBytes),
			pubsub.DefaultMaxMessageSize,
		)
	}

	response, err := client.PublishShardCheckpoint(ctx, oversized)
	if err != nil {
		t.Fatalf("PublishShardCheckpoint RPC failed instead of exposing TB-03 false success: %v", err)
	}
	if !response.Ok {
		t.Fatalf("PublishShardCheckpoint rejected oversized gossip bytes: %s", response.Error)
	}
	if got := ob.Size(); got != 1 {
		t.Fatalf("outbox size after false-success publish = %d, want 1", got)
	}

	due := waitForTB03DueEntries(t, ob)
	if len(due) != 1 {
		t.Fatalf("due outbox entries after false-success publish = %d, want 1", len(due))
	}
	oversizedID := outbox.MsgIDFor(oversizedBytes)
	if due[0].Topic != TopicShardCheckpoint {
		t.Fatalf("oversized outbox topic = %q, want %q", due[0].Topic, TopicShardCheckpoint)
	}
	if !bytes.Equal(due[0].MsgID, oversizedID) {
		t.Fatal("oversized outbox message id differs from the exact wire artifact id")
	}
	if !bytes.Equal(due[0].Payload, oversizedBytes) {
		t.Fatalf(
			"oversized outbox payload differs from published wire bytes: got %d bytes, want %d",
			len(due[0].Payload),
			len(oversizedBytes),
		)
	}

	select {
	case got, ok := <-received:
		if !ok {
			t.Fatal("shard-checkpoint receive channel closed during oversized non-delivery window")
		}
		if bytes.Equal(got, oversizedBytes) {
			t.Fatalf("oversized checkpoint unexpectedly reached peer: got %d bytes", len(got))
		}
		t.Fatalf("unexpected checkpoint arrived during oversized non-delivery window: got %d bytes", len(got))
	case <-time.After(2 * time.Second):
		// Characterized defect: local success and retention, remote non-delivery.
	}

	if dropped := ob.Confirm(TopicShardCheckpoint, [][]byte{oversizedID}); dropped != 1 {
		t.Fatalf("outbox did not retain the exact oversized wire artifact: Confirm dropped %d, want 1", dropped)
	}
	if got := ob.Size(); got != 0 {
		t.Fatalf("outbox size after confirming oversized artifact = %d, want 0", got)
	}

	// A post-oversize sentinel proves the peer path remained live across the
	// absence window; non-delivery was caused by the oversized RPC, not a
	// connection, subscription, relay, or gRPC failure.
	sentinel := &pb.ShardCheckpointWire{
		ShardId:      0,
		ShardOrdinal: 3,
		DerivedStateDelta: &pb.ShardDerivedStateDeltaWire{
			PayloadJson: []byte("deliverable-post-oversize-sentinel"),
		},
	}
	sentinelBytes, err := proto.Marshal(sentinel)
	if err != nil {
		t.Fatalf("marshal post-oversize sentinel: %v", err)
	}
	sentinelResponse, err := client.PublishShardCheckpoint(ctx, sentinel)
	if err != nil {
		t.Fatalf("publish post-oversize sentinel RPC: %v", err)
	}
	if !sentinelResponse.Ok {
		t.Fatalf("publish post-oversize sentinel rejected: %s", sentinelResponse.Error)
	}
	requireTB03Delivery(t, received, sentinelBytes, oversizedBytes, "post-oversize sentinel")
	confirmTB03OutboxEntry(t, ob, sentinelBytes, "post-oversize sentinel")
}

func newTB03GossipNode(t *testing.T, ctx context.Context) *gossip.Node {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.ListenAddrs = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.NumShards = 2
	cfg.DisableMdns = true
	node, err := gossip.New(ctx, cfg)
	if err != nil {
		t.Fatalf("create gossip node: %v", err)
	}
	return node
}

func waitForTB03PeerSubscription(t *testing.T, ctx context.Context, node *gossip.Node, label string) {
	t.Helper()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(20 * time.Second)
	defer timeout.Stop()

	for {
		if node.ShardMeshPeersByTopic()[label] > 0 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for %s subscription: %v", label, ctx.Err())
		case <-timeout.C:
			t.Fatalf("peer subscription %s was not visible to publisher", label)
		case <-ticker.C:
		}
	}
}

func waitForTB03DueEntries(t *testing.T, ob *outbox.Outbox) []outbox.Entry {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if due := ob.DueForRepublish(); len(due) > 0 {
			return due
		}
		if time.Now().After(deadline) {
			t.Fatal("outbox entry never became due for byte-for-byte inspection")
		}
		time.Sleep(time.Millisecond)
	}
}

func requireTB03Delivery(
	t *testing.T,
	received <-chan []byte,
	expected []byte,
	forbidden []byte,
	label string,
) {
	t.Helper()

	timeout := time.NewTimer(10 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case got, ok := <-received:
			if !ok {
				t.Fatalf("%s receive channel closed", label)
			}
			if forbidden != nil && bytes.Equal(got, forbidden) {
				t.Fatalf("oversized checkpoint arrived while waiting for %s: got %d bytes", label, len(got))
			}
			if !bytes.Equal(got, expected) {
				t.Fatalf("%s delivery mismatch: got %d bytes, want %d", label, len(got), len(expected))
			}
			return
		case <-timeout.C:
			t.Fatalf("%s did not reach the subscribed peer", label)
		}
	}
}

func confirmTB03OutboxEntry(t *testing.T, ob *outbox.Outbox, wireBytes []byte, label string) {
	t.Helper()

	if dropped := ob.Confirm(TopicShardCheckpoint, [][]byte{outbox.MsgIDFor(wireBytes)}); dropped != 1 {
		t.Fatalf("%s outbox confirmation dropped %d entries, want 1", label, dropped)
	}
	if got := ob.Size(); got != 0 {
		t.Fatalf("outbox size after confirming %s = %d, want 0", label, got)
	}
}

func newTB03SidecarClient(t *testing.T, server *Server) pb.SidecarServiceClient {
	t.Helper()

	listener := bufconn.Listen(4 << 20)
	grpcServer := grpc.NewServer()
	pb.RegisterSidecarServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	connection, err := grpc.NewClient(
		"passthrough:///tb03-bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatalf("create TB-03 gRPC client: %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return pb.NewSidecarServiceClient(connection)
}
