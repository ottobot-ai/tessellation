package grpcserver

// FINDING-F1 / FINDING-F2 (EPIC-9-NET M1 + M4) regression tests for the
// Subscribe topic-routing contract and the fraud-proof transport.
//
// F1: the JVM holds TWO concurrent Subscribe streams (SidecarRumorBridge +
// NakamotoSyncDaemon). The shard-checkpoint families are node-lifetime SHARED
// fan-in channels — each message is delivered to exactly ONE drainer — so two
// subscribe-all streams race, and the rumor bridge silently discards the shard
// checkpoints it wins (~half). The fix implements SubscribeRequest.topics
// filtering: the bridge subscribes rumor-only, the daemon subscribes to the
// non-rumor families, so the shared shard channels get exactly one drainer.
//
// F2: PublishFraudProof had no sidecar implementation (the RPC fell through to
// gRPC's unknown-method handler → UNIMPLEMENTED) and the Subscribe loop had no
// fraud-proof arm — a fraud proof could neither leave nor reach a node.
//
// These tests encode the FIXED contract. Against the pre-fix server (topics
// ignored, no fraud handler) they fail:
//   - TestSubscribe_TopicFilter_DualStreamShardCheckpointDelivery: the
//     rumor-only stream steals ~half the checkpoints (daemon misses them).
//   - TestSubscribe_UnknownTopicLabelFailsLoud: no validation, no error.
//   - TestPublishFraudProof_PublishesAndOutboxTracks: rpc error = Unimplemented.
//   - TestSubscribe_FraudProofArmDelivers: no arm, nothing delivered.

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/outbox"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

const (
	rumorBridgeRole  = pb.SubscriberRole_SUBSCRIBER_ROLE_RUMOR_BRIDGE
	nakamotoSyncRole = pb.SubscriberRole_SUBSCRIBER_ROLE_NAKAMOTO_SYNC
)

// ─── fake GossipNode ─────────────────────────────────────────────────
//
// Mirrors the two channel classes of the real gossip.Node:
//   - universal topics: per-call fan-out (every subscriber channel receives
//     every emitted message);
//   - shard families: ONE shared node-lifetime channel (each message goes to
//     exactly one reader — the F1 hazard under test).

type fakeNode struct {
	mu sync.Mutex

	// per-call fan-out subscriber channels, keyed by family
	rumorSubs      []chan []byte
	snapshotSubs   []chan []byte
	fraudProofSubs []chan []byte

	// single shared fan-in channels (the real node's Slice-14 design)
	shardCheckpointCh    chan []byte
	shardCheckpointAttCh chan []byte

	// recorded publishes
	fraudPublished [][]byte

	reconnectCh chan struct{}

	// fraudJoined mirrors gossip.Node's numShards gate: when false,
	// FraudProofMessages and PublishFraudProof fail.
	fraudJoined bool

	// Optional acquisition failure used to prove Started is not emitted before
	// every requested local subscription succeeds.
	snapshotSubscribeErr error
	shardDrainErr        error
	activeSubscriptions  int
}

func newFakeNode(fraudJoined bool) *fakeNode {
	return &fakeNode{
		shardCheckpointCh:    make(chan []byte, 256),
		shardCheckpointAttCh: make(chan []byte, 256),
		reconnectCh:          make(chan struct{}),
		fraudJoined:          fraudJoined,
	}
}

func (f *fakeNode) register(ctx context.Context, subs *[]chan []byte) <-chan []byte {
	f.mu.Lock()
	ch := make(chan []byte, 256)
	*subs = append(*subs, ch)
	f.activeSubscriptions++
	f.mu.Unlock()
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		f.activeSubscriptions--
		f.mu.Unlock()
	}()
	return ch
}

func (f *fakeNode) emit(subs *[]chan []byte, data []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ch := range *subs {
		ch <- data
	}
}

func (f *fakeNode) subCount(subs *[]chan []byte) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(*subs)
}

func (f *fakeNode) activeSubCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.activeSubscriptions
}

// Publish side — only fraud is recorded (the rest are untested pass-throughs).
func (f *fakeNode) PublishSnapshot(ctx context.Context, data []byte) error             { return nil }
func (f *fakeNode) PublishAttestation(ctx context.Context, data []byte) error          { return nil }
func (f *fakeNode) PublishRumor(ctx context.Context, data []byte) error                { return nil }
func (f *fakeNode) PublishMetagraphBinary(ctx context.Context, data []byte) error      { return nil }
func (f *fakeNode) PublishMetagraphAttestation(ctx context.Context, data []byte) error { return nil }
func (f *fakeNode) PublishAllowSpendBlock(ctx context.Context, data []byte) error      { return nil }
func (f *fakeNode) PublishDAGBlock(ctx context.Context, data []byte) error             { return nil }
func (f *fakeNode) PublishTokenLockBlock(ctx context.Context, data []byte) error       { return nil }
func (f *fakeNode) PublishShardCheckpoint(ctx context.Context, shardID uint32, data []byte) error {
	return nil
}
func (f *fakeNode) PublishShardCheckpointAttestation(ctx context.Context, shardID uint32, data []byte) error {
	return nil
}
func (f *fakeNode) PublishFraudProof(ctx context.Context, data []byte) error {
	if !f.fraudJoined {
		return fmt.Errorf("fraud-proof topic not joined (numShards <= 1)")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fraudPublished = append(f.fraudPublished, data)
	return nil
}

// Subscribe side.
func (f *fakeNode) SnapshotMessages(ctx context.Context) (<-chan []byte, error) {
	if f.snapshotSubscribeErr != nil {
		return nil, f.snapshotSubscribeErr
	}
	return f.register(ctx, &f.snapshotSubs), nil
}
func (f *fakeNode) AttestationMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) RumorMessages(ctx context.Context) (<-chan []byte, error) {
	return f.register(ctx, &f.rumorSubs), nil
}
func (f *fakeNode) MetagraphBinaryMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) MetagraphAttestationMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) AllowSpendBlockMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) DAGBlockMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) TokenLockBlockMessages(ctx context.Context) (<-chan []byte, error) {
	return make(chan []byte), nil
}
func (f *fakeNode) FraudProofMessages(ctx context.Context) (<-chan []byte, error) {
	if !f.fraudJoined {
		return nil, fmt.Errorf("fraud-proof topic not joined")
	}
	return f.register(ctx, &f.fraudProofSubs), nil
}
func (f *fakeNode) ShardCheckpointMessages() (<-chan []byte, error) {
	if !f.fraudJoined {
		return nil, fmt.Errorf("shard-checkpoint topics not joined")
	}
	if f.shardDrainErr != nil {
		return nil, f.shardDrainErr
	}
	return f.shardCheckpointCh, nil
}
func (f *fakeNode) ShardCheckpointAttestationMessages() (<-chan []byte, error) {
	if !f.fraudJoined {
		return nil, fmt.Errorf("shard-checkpoint-attestation topics not joined")
	}
	return f.shardCheckpointAttCh, nil
}
func (f *fakeNode) ShardSubscriptionsActive() bool { return f.fraudJoined }
func (f *fakeNode) ReconnectCh() <-chan struct{}   { return f.reconnectCh }
func (f *fakeNode) MeshPeerCount() (int, int, int, int, int, int, int, int) {
	return 0, 0, 0, 0, 0, 0, 0, 0
}
func (f *fakeNode) ShardMeshPeerCount() (int, int) { return 0, 0 }
func (f *fakeNode) ConnectedPeerCount() int        { return 0 }

var _ GossipNode = (*fakeNode)(nil)

// ─── harness ─────────────────────────────────────────────────────────

type harness struct {
	fake   *fakeNode
	server *Server
	ob     *outbox.Outbox
	client pb.SidecarServiceClient
	conn   *grpc.ClientConn
	grpc   *grpc.Server
}

func startHarness(t *testing.T, fake *fakeNode) *harness {
	t.Helper()
	ob := outbox.New(30*time.Second, time.Hour)
	srv := New(fake, nil, ob)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterSidecarServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
	})
	return &harness{fake: fake, server: srv, ob: ob, client: pb.NewSidecarServiceClient(conn), conn: conn, grpc: gs}
}

// received accumulates the bodies a Subscribe stream delivered, by family.
type received struct {
	mu          sync.Mutex
	checkpoints map[uint64]bool // ShardOrdinal
	rumors      map[string]bool // ContentType
	frauds      map[string]bool // MetagraphAddress
	started     *pb.SubscribeStarted
	protocolErr error
	streamErr   error
}

func (r *received) counts() (cp, ru, fp int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.checkpoints), len(r.rumors), len(r.frauds)
}

func (r *received) startedEvent() (*pb.SubscribeStarted, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.started, r.protocolErr
}

// openStream starts a Subscribe stream with the given topic filter and drains
// it into a received accumulator until ctx is done or the stream errors.
func openStream(ctx context.Context, t *testing.T, client pb.SidecarServiceClient, topics []string, role pb.SubscriberRole) *received {
	t.Helper()
	r := &received{checkpoints: map[uint64]bool{}, rumors: map[string]bool{}, frauds: map[string]bool{}}
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{Topics: topics, Role: role})
	if err != nil {
		t.Fatalf("Subscribe(%v): %v", topics, err)
	}
	go func() {
		first := true
		for {
			msg, rerr := stream.Recv()
			if rerr != nil {
				r.mu.Lock()
				r.streamErr = rerr
				r.mu.Unlock()
				return
			}
			r.mu.Lock()
			switch b := msg.Body.(type) {
			case *pb.GossipMessage_Started:
				if !first {
					r.protocolErr = fmt.Errorf("SubscribeStarted was not the first event")
				} else {
					r.started = b.Started
				}
			case *pb.GossipMessage_ShardCheckpoint:
				if first {
					r.protocolErr = fmt.Errorf("first event was shard checkpoint, want SubscribeStarted")
				}
				r.checkpoints[b.ShardCheckpoint.GetShardOrdinal()] = true
			case *pb.GossipMessage_Rumor:
				if first {
					r.protocolErr = fmt.Errorf("first event was rumor, want SubscribeStarted")
				}
				r.rumors[b.Rumor.GetContentType()] = true
			case *pb.GossipMessage_FraudProof:
				if first {
					r.protocolErr = fmt.Errorf("first event was fraud proof, want SubscribeStarted")
				}
				r.frauds[b.FraudProof.GetMetagraphAddress()] = true
			}
			first = false
			r.mu.Unlock()
		}
	}()
	return r
}

func requireStarted(t *testing.T, stream pb.SidecarService_SubscribeClient) *pb.SubscribeStarted {
	t.Helper()
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("receive SubscribeStarted: %v", err)
	}
	started := msg.GetStarted()
	if started == nil {
		t.Fatalf("first Subscribe event was %T, want SubscribeStarted", msg.GetBody())
	}
	return started
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("waitFor timed out: %s", what)
	return false
}

func marshalCheckpoint(t *testing.T, ordinal uint64) []byte {
	t.Helper()
	data, err := proto.Marshal(&pb.ShardCheckpointWire{ShardId: 0, ShardOrdinal: ordinal})
	if err != nil {
		t.Fatalf("marshal checkpoint: %v", err)
	}
	return data
}

// The topic set the NakamotoSyncDaemon requests: every family EXCEPT rumor
// (rumors belong to the SidecarRumorBridge). Mirrors
// SidecarClient.SubscribeTopics.daemonTopics on the JVM side.
func daemonTopics() []string {
	return daemonTopicsForSharding(true)
}

func daemonTopicsForSharding(active bool) []string {
	topics := []string{
		TopicSnapshot, TopicAttestation,
		TopicMetagraphBinary, TopicMetagraphAttestation,
		TopicAllowSpendBlock, TopicDAGBlock, TopicTokenLockBlock,
	}
	if active {
		topics = append(topics, TopicShardCheckpoint, TopicShardCheckpointAttestation, TopicFraudProof)
	}
	return topics
}

// ─── F1: dual-Subscribe shard-checkpoint race ────────────────────────

func TestSubscribe_StartedIsFirstAndCarriesNormalizedLocalAcquisition(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	generationBefore := processStreamGeneration.Load()
	requestedTopics := daemonTopics()
	for left, right := 0, len(requestedTopics)-1; left < right; left, right = left+1, right-1 {
		requestedTopics[left], requestedTopics[right] = requestedTopics[right], requestedTopics[left]
	}
	requestedTopics = append(requestedTopics, TopicDAGBlock)
	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{
		Topics: requestedTopics,
		Role:   nakamotoSyncRole,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	started := requireStarted(t, stream)
	if got, want := fmt.Sprint(started.GetTopics()), fmt.Sprint(daemonTopics()); got != want {
		t.Errorf("Started topics = %s, want canonical deduplicated %s", got, want)
	}
	if started.GetRole() != nakamotoSyncRole {
		t.Errorf("Started role = %v, want %v", started.GetRole(), nakamotoSyncRole)
	}
	if started.GetStreamGeneration() != generationBefore+1 {
		t.Errorf("first stream generation = %d, want %d", started.GetStreamGeneration(), generationBefore+1)
	}
	if started.GetSidecarSessionId() == "" || started.GetSidecarSessionId() != h.server.sessionID {
		t.Errorf("Started session ID = %q, want this server session %q", started.GetSidecarSessionId(), h.server.sessionID)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	stream2, err := h.client.Subscribe(ctx2, &pb.SubscribeRequest{Topics: []string{TopicRumor}, Role: rumorBridgeRole})
	if err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	started2 := requireStarted(t, stream2)
	if started2.GetStreamGeneration() != started.GetStreamGeneration()+1 {
		t.Errorf("second stream generation = %d, want %d", started2.GetStreamGeneration(), started.GetStreamGeneration()+1)
	}
	if started2.GetSidecarSessionId() != started.GetSidecarSessionId() {
		t.Errorf("session changed within one server: first=%q second=%q", started.GetSidecarSessionId(), started2.GetSidecarSessionId())
	}
}

func TestSubscribe_NoStartedWhenRequestedSubscriptionAcquisitionFails(t *testing.T) {
	fake := newFakeNode(true)
	fake.snapshotSubscribeErr = fmt.Errorf("injected Subscribe failure")
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	generationBefore := processStreamGeneration.Load()
	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err != nil {
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("Subscribe acquisition failed with %v, want Unavailable", err)
		}
	} else if msg, recvErr := stream.Recv(); status.Code(recvErr) != codes.Unavailable {
		t.Fatalf("first response = %v err=%v, want no event and Unavailable", msg, recvErr)
	}
	if generation := processStreamGeneration.Load(); generation != generationBefore {
		t.Errorf("failed acquisition changed stream generation from %d to %d", generationBefore, generation)
	}
}

func TestSubscribe_DuplicateShardDrainerFailsAndCancellationReleasesLease(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx1, cancel1 := context.WithCancel(context.Background())
	stream1, err := h.client.Subscribe(ctx1, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}
	requireStarted(t, stream1)
	if got := h.server.shardDrainStreams.Load(); got != 1 {
		t.Fatalf("shard lease after Started = %d, want 1", got)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	stream2, err := h.client.Subscribe(ctx2, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err == nil {
		_, err = stream2.Recv()
	}
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate shard drainer failed with %v, want AlreadyExists", err)
	}

	cancel1()
	if !waitFor(t, 5*time.Second, "shard drain lease released after cancellation", func() bool {
		return h.server.shardDrainStreams.Load() == 0
	}) {
		t.Fatal("cancelled shard stream retained its exclusive drain lease")
	}

	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()
	stream3, err := h.client.Subscribe(ctx3, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err != nil {
		t.Fatalf("replacement Subscribe: %v", err)
	}
	requireStarted(t, stream3)
}

func TestSubscribe_CancellationStopsLocalUniversalSubscriptions(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: []string{TopicRumor}, Role: rumorBridgeRole})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	requireStarted(t, stream)
	if got := fake.activeSubCount(); got != 1 {
		t.Fatalf("active local subscriptions after Started = %d, want 1", got)
	}
	cancel()
	if !waitFor(t, 5*time.Second, "universal subscription cancelled", func() bool {
		return fake.activeSubCount() == 0
	}) {
		t.Fatal("Subscribe cancellation did not cancel the acquired local subscription")
	}
}

func TestSubscribe_RoleTopicCompatibilityFailsClosed(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)
	tests := []struct {
		name   string
		topics []string
		role   pb.SubscriberRole
	}{
		{name: "unspecified", topics: []string{TopicRumor}, role: pb.SubscriberRole_SUBSCRIBER_ROLE_UNSPECIFIED},
		{name: "rumor bridge extra family", topics: []string{TopicRumor, TopicShardCheckpoint}, role: rumorBridgeRole},
		{name: "nakamoto sync rumor", topics: []string{TopicRumor}, role: nakamotoSyncRole},
		{name: "nakamoto sync incomplete", topics: []string{TopicSnapshot}, role: nakamotoSyncRole},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: tc.topics, Role: tc.role})
			if err == nil {
				_, err = stream.Recv()
			}
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("role/topic mismatch failed with %v, want InvalidArgument", err)
			}
		})
	}
}

func TestSubscribe_InactiveShardFamiliesCannotBeAcknowledged(t *testing.T) {
	fake := newFakeNode(false)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("sharding-active topic profile on inactive sidecar failed with %v, want InvalidArgument", err)
	}
	if got := h.server.shardDrainStreams.Load(); got != 0 {
		t.Errorf("rejected inactive profile acquired shard lease: %d", got)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	stream2, err := h.client.Subscribe(ctx2, &pb.SubscribeRequest{Topics: daemonTopicsForSharding(false), Role: nakamotoSyncRole})
	if err != nil {
		t.Fatalf("inactive-profile Subscribe: %v", err)
	}
	started := requireStarted(t, stream2)
	if got, want := fmt.Sprint(started.GetTopics()), fmt.Sprint(daemonTopicsForSharding(false)); got != want {
		t.Errorf("inactive Started topics = %s, want %s", got, want)
	}
}

func TestSubscribe_SharedDrainFailurePreventsStartedAndReleasesLease(t *testing.T) {
	fake := newFakeNode(true)
	fake.shardDrainErr = fmt.Errorf("injected shared drain failure")
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: daemonTopics(), Role: nakamotoSyncRole})
	if err == nil {
		_, err = stream.Recv()
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("shared drain failure returned %v, want Unavailable before Started", err)
	}
	if got := h.server.shardDrainStreams.Load(); got != 0 {
		t.Errorf("failed shared drain acquisition leaked lease: %d", got)
	}
}

// With the rumor bridge subscribed rumor-only and the daemon subscribed to the
// non-rumor families, EVERY shard checkpoint must reach the daemon stream and
// NONE may leak to the rumor stream — and rumors must still reach the bridge.
//
// Pre-fix (server ignores SubscribeRequest.topics): both streams race-drain
// the single shared shard channel, so the daemon receives only ~half of the
// 200 checkpoints and the bridge stream receives the rest (which the JVM
// bridge would silently discard). This is FINDING-F1 reproduced at the
// transport layer.
func TestSubscribe_TopicFilter_DualStreamShardCheckpointDelivery(t *testing.T) {
	const nCheckpoints = 200
	const nRumors = 50

	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bridge := openStream(ctx, t, h.client, []string{TopicRumor}, rumorBridgeRole)
	daemon := openStream(ctx, t, h.client, daemonTopics(), nakamotoSyncRole)

	// Both Subscribe handlers are live once their per-call fan-out
	// subscriptions are registered on the fake (the bridge registers a rumor
	// sub; the daemon registers a snapshot sub). Pre-fix both handlers
	// register BOTH (topics ignored), so this gate opens in both worlds.
	if !waitFor(t, 5*time.Second, "streams registered", func() bool {
		return fake.subCount(&fake.rumorSubs) >= 1 && fake.subCount(&fake.snapshotSubs) >= 1
	}) {
		t.Fatal("Subscribe handlers did not register their subscriptions")
	}

	// Feed the SHARED shard channel (exactly-one-reader semantics) and the
	// rumor fan-out.
	for i := 1; i <= nCheckpoints; i++ {
		fake.shardCheckpointCh <- marshalCheckpoint(t, uint64(i))
	}
	for i := 1; i <= nRumors; i++ {
		data, err := proto.Marshal(&pb.Rumor{ContentType: fmt.Sprintf("rumor-%03d", i)})
		if err != nil {
			t.Fatalf("marshal rumor: %v", err)
		}
		fake.emit(&fake.rumorSubs, data)
	}

	waitFor(t, 10*time.Second, "daemon got all checkpoints + bridge got all rumors", func() bool {
		cp, _, _ := daemon.counts()
		_, ru, _ := bridge.counts()
		return cp == nCheckpoints && ru == nRumors
	})

	dcp, dru, _ := daemon.counts()
	bcp, bru, _ := bridge.counts()

	if dcp != nCheckpoints {
		t.Errorf("FINDING-F1: daemon stream received %d/%d shard checkpoints — the rest were race-drained by the rumor-bridge stream and would be silently discarded by its isRumor collect", dcp, nCheckpoints)
	}
	if bcp != 0 {
		t.Errorf("FINDING-F1: rumor-bridge stream received %d shard checkpoints (must be 0 — it only requested %q)", bcp, TopicRumor)
	}
	if bru != nRumors {
		t.Errorf("rumor bridge received %d/%d rumors", bru, nRumors)
	}
	if dru != 0 {
		t.Errorf("daemon stream received %d rumors (must be 0 — it did not request %q)", dru, TopicRumor)
	}
}

// Empty topics are ambiguous and cannot produce a truthful Started event.
func TestSubscribe_EmptyTopicsFailsLoud(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Role: rumorBridgeRole})
	if err != nil {
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Subscribe(empty topics) failed with %v, want InvalidArgument", err)
		}
		return
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Subscribe(empty topics) stream failed with %v, want InvalidArgument", err)
	}
}

// An unknown topic label must fail the stream loudly with InvalidArgument (a
// typo'd label silently subscribing to nothing would be a liveness hole worse
// than the race). NOTE: asserting the specific code matters — a plain
// "any error" assertion passes trivially via the test ctx deadline, which is
// exactly how the pre-fix server (no validation at all) would slip through.
func TestSubscribe_UnknownTopicLabelFailsLoud(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := h.client.Subscribe(ctx, &pb.SubscribeRequest{Topics: []string{"not-a-topic"}, Role: nakamotoSyncRole})
	if err != nil {
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Subscribe(unknown label) failed with %v, want InvalidArgument", status.Code(err))
		}
		return
	}
	_, rerr := stream.Recv()
	if rerr == nil {
		t.Fatal("Subscribe with unknown topic label 'not-a-topic' did not error — a typo would silently subscribe to nothing")
	}
	if status.Code(rerr) != codes.InvalidArgument {
		t.Errorf("Subscribe(unknown label) stream failed with %v (%v), want InvalidArgument", status.Code(rerr), rerr)
	}
}

// ─── F2: fraud-proof transport ───────────────────────────────────────

// PublishFraudProof must be implemented (pre-fix: UNIMPLEMENTED via gRPC's
// unknown-method fallthrough) and must both gossip the wire bytes and track
// them in the durable outbox under the "fraud-proof" label so a transient
// mesh stall cannot eat the only copy of slashing evidence.
func TestPublishFraudProof_PublishesAndOutboxTracks(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fp := &pb.FraudProofEnvelopeWire{
		ShardId:                0,
		DisputedCheckpointHash: []byte("aa11"),
		MetagraphAddress:       "DAG-mg-a",
		Gl0AnchorOrdinal:       100,
		ClaimedDerivation:      []byte("cafe"),
		ChallengerDerivation:   []byte("beef"),
		ReexecutionWitness:     []byte("w"),
		ChallengerSignature:    []byte("sig"),
		SubmitterId:            []byte("peer-1"),
	}
	resp, err := h.client.PublishFraudProof(ctx, fp)
	if err != nil {
		t.Fatalf("FINDING-F2: PublishFraudProof rpc failed (pre-fix this is codes.Unimplemented — the sidecar never implemented the watchtower transport): %v", err)
	}
	if !resp.Ok {
		t.Fatalf("PublishFraudProof returned ok=false: %s", resp.Error)
	}

	fake.mu.Lock()
	published := len(fake.fraudPublished)
	fake.mu.Unlock()
	if published != 1 {
		t.Errorf("fraud proof was not published to the gossip node (got %d publishes)", published)
	}

	wire, err := proto.Marshal(fp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if dropped := h.ob.Confirm(TopicFraudProof, [][]byte{outbox.MsgIDFor(wire)}); dropped != 1 {
		t.Errorf("fraud proof was not outbox-tracked under %q (Confirm dropped %d entries, want 1)", TopicFraudProof, dropped)
	}
}

// At numShards <= 1 the fraud topic is not joined; the publish must fail
// loudly (ok=false), never silently pretend delivery.
func TestPublishFraudProof_FailsClosedWhenShardingInactive(t *testing.T) {
	fake := newFakeNode(false)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := h.client.PublishFraudProof(ctx, &pb.FraudProofEnvelopeWire{MetagraphAddress: "DAG-x"})
	if err != nil {
		t.Fatalf("rpc-level failure (want ok=false response): %v", err)
	}
	if resp.Ok {
		t.Error("PublishFraudProof reported ok=true while the fraud topic is not joined (numShards<=1) — silent non-delivery")
	}
}

// A received fraud proof must be delivered to the stream that requested the
// fraud-proof family (the daemon) and NOT to the rumor-only bridge stream.
// Pre-fix the Subscribe loop has no fraud arm at all: nothing is delivered.
func TestSubscribe_FraudProofArmDelivers(t *testing.T) {
	fake := newFakeNode(true)
	h := startHarness(t, fake)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	bridge := openStream(ctx, t, h.client, []string{TopicRumor}, rumorBridgeRole)
	daemon := openStream(ctx, t, h.client, daemonTopics(), nakamotoSyncRole)

	if !waitFor(t, 5*time.Second, "fraud sub registered", func() bool {
		return fake.subCount(&fake.fraudProofSubs) >= 1 && fake.subCount(&fake.rumorSubs) >= 1
	}) {
		t.Fatal("FINDING-F2: the daemon-shaped Subscribe stream never opened a fraud-proof subscription (no fraud arm in the Subscribe loop)")
	}

	data, err := proto.Marshal(&pb.FraudProofEnvelopeWire{ShardId: 0, MetagraphAddress: "DAG-mg-b"})
	if err != nil {
		t.Fatalf("marshal fraud proof: %v", err)
	}
	fake.emit(&fake.fraudProofSubs, data)

	if !waitFor(t, 5*time.Second, "daemon received fraud proof", func() bool {
		_, _, fp := daemon.counts()
		return fp == 1
	}) {
		_, _, fp := daemon.counts()
		t.Errorf("FINDING-F2: fraud proof did not reach the daemon stream (got %d)", fp)
	}
	if _, _, fp := bridge.counts(); fp != 0 {
		t.Errorf("fraud proof leaked to the rumor-only bridge stream (got %d, want 0)", fp)
	}
}
