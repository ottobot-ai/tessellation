package gossip

// FINDING-F2 (EPIC-9-NET M4) — gossip-layer coverage for the watchtower
// fraud-proof topic: the gl0-wide topic is joined when sharding is active,
// a fraud proof published on node A reaches node B over a REAL libp2p mesh,
// and at the numShards <= 1 regression bar the topic is not joined at all
// (byte-identical single-shard sidecar; publish fails closed).

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/scasplte2/tessellation/p2p/internal/config"
)

func testConfig(numShards int) config.Config {
	cfg := config.DefaultConfig()
	cfg.ListenAddrs = []string{"/ip4/127.0.0.1/tcp/0"}
	cfg.NumShards = numShards
	// mDNS would advertise on the LAN and can cross-talk with unrelated
	// hosts/tests; the test wires the two nodes together explicitly.
	cfg.DisableMdns = true
	return cfg
}

// Two real in-process libp2p hosts: A publishes a fraud proof, B receives it
// via FraudProofMessages. Proves topic join (numShards > 1), the per-call
// fan-out subscription, and the relay — the full sidecar-side receive leg the
// gRPC Subscribe arm consumes.
func TestFraudProofTopic_TwoNodeRelay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodeA, err := New(ctx, testConfig(2))
	if err != nil {
		t.Fatalf("node A: %v", err)
	}
	defer nodeA.Close()
	nodeB, err := New(ctx, testConfig(2))
	if err != nil {
		t.Fatalf("node B: %v", err)
	}
	defer nodeB.Close()

	if nodeA.fraudProofTopic == nil || nodeB.fraudProofTopic == nil {
		t.Fatal("fraud-proof topic not joined at numShards=2")
	}

	// Wire A -> B explicitly (no discovery in this test).
	if err := nodeA.Host.Connect(ctx, peer.AddrInfo{ID: nodeB.Host.ID(), Addrs: nodeB.Host.Addrs()}); err != nil {
		t.Fatalf("connect A->B: %v", err)
	}

	// B subscribes BEFORE A publishes; then wait until A's topic sees B as a
	// subscriber (subscription propagation), else the publish precedes the
	// subscription exchange and is legitimately not delivered.
	recvCh, err := nodeB.FraudProofMessages(ctx)
	if err != nil {
		t.Fatalf("FraudProofMessages: %v", err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(nodeA.fraudProofTopic.ListPeers()) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(nodeA.fraudProofTopic.ListPeers()) < 1 {
		t.Fatal("node A never saw node B subscribed on the fraud-proof topic")
	}

	payload := []byte("fraud-proof-wire-bytes")
	if err := nodeA.PublishFraudProof(ctx, payload); err != nil {
		t.Fatalf("PublishFraudProof: %v", err)
	}

	select {
	case got := <-recvCh:
		if string(got) != string(payload) {
			t.Errorf("received %q, want %q", got, payload)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("FINDING-F2: fraud proof published on node A never reached node B's FraudProofMessages channel")
	}
}

// numShards <= 1 regression bar: no fraud topic join, publish fails closed,
// messages channel is nil (never delivers). The single-shard sidecar must be
// byte-identical to pre-sharding — zero extra topic joins.
func TestFraudProofTopic_NotJoinedAtSingleShard(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node, err := New(ctx, testConfig(1))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer node.Close()

	if node.fraudProofTopic != nil {
		t.Error("fraud-proof topic joined at numShards=1 — breaks the single-shard byte-identity regression bar")
	}
	if err := node.PublishFraudProof(ctx, []byte("x")); err == nil {
		t.Error("PublishFraudProof succeeded at numShards=1 — must fail closed (topic not joined)")
	}
	if ch, err := node.FraudProofMessages(ctx); err == nil || ch != nil {
		t.Error("FraudProofMessages acquired a subscription at numShards=1 — must fail closed")
	}
}
