package chainsync

import (
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestValidateRequestCount(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		count   int
		limit   int
		wantErr bool
	}{
		{name: "empty", count: 0, limit: MaxHashesPerRequest},
		{name: "at hash limit", count: MaxHashesPerRequest, limit: MaxHashesPerRequest},
		{name: "over hash limit", count: MaxHashesPerRequest + 1, limit: MaxHashesPerRequest, wantErr: true},
		{name: "at point limit", count: MaxPointsPerRequest, limit: MaxPointsPerRequest},
		{name: "over point limit", count: MaxPointsPerRequest + 1, limit: MaxPointsPerRequest, wantErr: true},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateRequestCount("items", tc.count, tc.limit)
			if tc.wantErr && err == nil {
				t.Fatalf("expected count %d over limit %d to fail", tc.count, tc.limit)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected count %d at limit %d to pass: %v", tc.count, tc.limit, err)
			}
		})
	}
}

func TestAllowIncomingRequest(t *testing.T) {
	t.Parallel()

	h := &Handler{}
	now := time.Unix(1_000, 0)
	peerA := peer.ID("peer-a")
	peerB := peer.ID("peer-b")

	for request := 1; request <= RateLimitPerPeer; request++ {
		if !h.allowIncomingRequest(peerA, now) {
			t.Fatalf("request %d at the limit was rejected", request)
		}
	}
	if h.allowIncomingRequest(peerA, now) {
		t.Fatal("request above the per-peer limit was accepted")
	}
	if h.allowIncomingRequest(peerA, now.Add(rateLimitWindow-time.Nanosecond)) {
		t.Fatal("peer quota reset before the sliding window elapsed")
	}
	if !h.allowIncomingRequest(peerB, now) {
		t.Fatal("one peer exhausting its quota affected another peer")
	}
	if !h.allowIncomingRequest(peerA, now.Add(rateLimitWindow)) {
		t.Fatal("peer quota did not reset in the next window")
	}
}
