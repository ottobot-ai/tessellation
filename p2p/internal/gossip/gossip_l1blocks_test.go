package gossip

// #186 token-lock replacement delivery gap (2026-07-09) — gossip-layer coverage
// for the cross-client l1-block topics (allow-spend / dag / token-lock blocks).
//
// The l1-block topics are CROSS-CLIENT: the publisher is the colocated gl1 JVM
// and the subscriber is the colocated gl0 JVM — two different gRPC clients of
// the same sidecar. The old relay skipped self-published messages on every
// topic, which combined fatally with content-derived message IDs when all L1
// validators co-sign one block and publish IDENTICAL bytes near-simultaneously:
// each sidecar's own publish poisoned its seen-cache before the remote copies
// arrived, the self-copy was skipped, and NO gl0 received the block until an
// outbox republish landed after the ~2min gossipsub seen-cache TTL (observed
// live: 28-ordinal delivery gap → testMultipleSequentialReplacements failure).

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

// A node's OWN publish on the token-lock-block topic must reach its LOCAL
// subscriber (the colocated gl0's Subscribe stream). RED under the old
// unconditional self-skip; GREEN with deliverSelf=true for l1-block topics.
func TestTokenLockBlock_SelfPublishReachesLocalSubscriber(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	node, err := New(ctx, testConfig(1))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer node.Close()

	recvCh := node.TokenLockBlockMessages(ctx)

	payload := []byte("token-lock-block-self-bytes")
	if err := node.PublishTokenLockBlock(ctx, payload); err != nil {
		t.Fatalf("PublishTokenLockBlock: %v", err)
	}

	select {
	case got := <-recvCh:
		if !bytes.Equal(got, payload) {
			t.Fatalf("received %q, want %q", got, payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("local subscriber never received the self-published token-lock block")
	}
}

// The live-failure shape: two sidecars publish IDENTICAL bytes at effectively
// the same instant (all gl1s co-sign one block; content-derived message IDs
// collapse the copies into one gossipsub message). EVERY node's local
// subscriber must still receive the block. Under the old self-skip each node's
// own publish was skipped locally and the remote copy was deduped by the
// seen-cache — zero deliveries network-wide.
func TestTokenLockBlock_SimultaneousIdenticalPublish_BothLocalsReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	nodeA, err := New(ctx, testConfig(1))
	if err != nil {
		t.Fatalf("node A: %v", err)
	}
	defer nodeA.Close()
	nodeB, err := New(ctx, testConfig(1))
	if err != nil {
		t.Fatalf("node B: %v", err)
	}
	defer nodeB.Close()

	if err := nodeA.Host.Connect(ctx, peer.AddrInfo{ID: nodeB.Host.ID(), Addrs: nodeB.Host.Addrs()}); err != nil {
		t.Fatalf("connect A->B: %v", err)
	}

	recvA := nodeA.TokenLockBlockMessages(ctx)
	recvB := nodeB.TokenLockBlockMessages(ctx)

	// Wait for subscription propagation both ways so the flood-publish can
	// even attempt remote delivery (mirrors the fraud-proof test).
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if len(nodeA.tokenLockBlockTopic.ListPeers()) >= 1 && len(nodeB.tokenLockBlockTopic.ListPeers()) >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	payload := []byte("token-lock-block-identical-bytes")
	// Publish on BOTH nodes back-to-back: each node's own publish lands in its
	// seen-cache before the remote copy arrives, so under the old semantics
	// neither local subscriber would ever see the message.
	if err := nodeA.PublishTokenLockBlock(ctx, payload); err != nil {
		t.Fatalf("A PublishTokenLockBlock: %v", err)
	}
	if err := nodeB.PublishTokenLockBlock(ctx, payload); err != nil {
		t.Fatalf("B PublishTokenLockBlock: %v", err)
	}

	for name, ch := range map[string]<-chan []byte{"A": recvA, "B": recvB} {
		select {
		case got := <-ch:
			if !bytes.Equal(got, payload) {
				t.Fatalf("node %s received %q, want %q", name, got, payload)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("node %s local subscriber never received the token-lock block", name)
		}
	}
}

// Same-client topics must keep the echo-prevention semantics: a rumor this
// sidecar publishes (the colocated JVM's own rumor) is NOT relayed back to the
// local subscriber. Guards the deliverSelf=false leg against regression.
func TestRumor_SelfPublishIsNotEchoedLocally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	node, err := New(ctx, testConfig(1))
	if err != nil {
		t.Fatalf("node: %v", err)
	}
	defer node.Close()

	recvCh := node.RumorMessages(ctx)

	payload := []byte("rumor-self-bytes")
	if err := node.PublishRumor(ctx, payload); err != nil {
		t.Fatalf("PublishRumor: %v", err)
	}

	select {
	case got := <-recvCh:
		t.Fatalf("rumor echoed back to local subscriber: %q", got)
	case <-time.After(3 * time.Second):
		// expected: no echo
	}
}
