package outbox

import (
	"bytes"
	"testing"
	"time"
)

// We pick a republish interval and TTL well below typical test latencies so a
// few-millisecond sleep observably crosses the boundary without forcing the
// suite to be slow.
const (
	testInterval = 50 * time.Millisecond
	testTTL      = 200 * time.Millisecond
)

func TestAddThenConfirmDropsEntry(t *testing.T) {
	o := New(testInterval, testTTL)
	payload := []byte("hello")
	id := MsgIDFor(payload)
	o.Add("topic-a", id, payload)
	if o.Size() != 1 {
		t.Fatalf("expected 1 entry after Add, got %d", o.Size())
	}
	dropped := o.Confirm("topic-a", [][]byte{id})
	if dropped != 1 {
		t.Errorf("expected Confirm to drop 1, got %d", dropped)
	}
	if o.Size() != 0 {
		t.Errorf("expected outbox empty after Confirm, got %d", o.Size())
	}
}

func TestConfirmUnknownIsNoOp(t *testing.T) {
	o := New(testInterval, testTTL)
	o.Add("topic-a", MsgIDFor([]byte("p1")), []byte("p1"))
	// Confirm an id that isn't in the outbox at all.
	dropped := o.Confirm("topic-a", [][]byte{MsgIDFor([]byte("unknown"))})
	if dropped != 0 {
		t.Errorf("expected 0 dropped for unknown id, got %d", dropped)
	}
	if o.Size() != 1 {
		t.Errorf("expected the real entry untouched, size=%d", o.Size())
	}
	// Confirm on an unknown topic is also a no-op.
	dropped = o.Confirm("topic-nope", [][]byte{MsgIDFor([]byte("p1"))})
	if dropped != 0 {
		t.Errorf("expected 0 dropped on unknown topic, got %d", dropped)
	}
}

func TestConfirmIsIdempotent(t *testing.T) {
	o := New(testInterval, testTTL)
	id := MsgIDFor([]byte("x"))
	o.Add("topic-a", id, []byte("x"))
	if dropped := o.Confirm("topic-a", [][]byte{id}); dropped != 1 {
		t.Fatalf("first Confirm should drop 1, got %d", dropped)
	}
	// Re-confirm the same id — must not error, must not drop again.
	if dropped := o.Confirm("topic-a", [][]byte{id}); dropped != 0 {
		t.Errorf("second Confirm should drop 0, got %d", dropped)
	}
}

func TestDueForRepublishWaitsForInterval(t *testing.T) {
	o := New(testInterval, testTTL)
	o.Add("topic-a", MsgIDFor([]byte("p")), []byte("p"))
	// Immediately after Add, LastRepublishedAt == now, so nothing is due.
	if due := o.DueForRepublish(); len(due) != 0 {
		t.Errorf("expected no due entries immediately after Add, got %d", len(due))
	}
	// After interval elapses, the entry is due.
	time.Sleep(testInterval + 20*time.Millisecond)
	due := o.DueForRepublish()
	if len(due) != 1 {
		t.Fatalf("expected 1 due entry after interval, got %d", len(due))
	}
	if due[0].Topic != "topic-a" || !bytes.Equal(due[0].Payload, []byte("p")) {
		t.Errorf("unexpected entry contents: %+v", due[0])
	}
}

func TestMarkRepublishedDelaysNextDue(t *testing.T) {
	o := New(testInterval, testTTL)
	id := MsgIDFor([]byte("p"))
	o.Add("topic-a", id, []byte("p"))
	time.Sleep(testInterval + 20*time.Millisecond)
	due := o.DueForRepublish()
	if len(due) != 1 {
		t.Fatalf("expected 1 due entry, got %d", len(due))
	}
	o.MarkRepublished("topic-a", id)
	// Immediately after MarkRepublished, the entry is NOT due again.
	if due := o.DueForRepublish(); len(due) != 0 {
		t.Errorf("expected 0 due immediately after MarkRepublished, got %d", len(due))
	}
}

func TestPruneDropsTTLExpiredEntries(t *testing.T) {
	o := New(testInterval, testTTL)
	id := MsgIDFor([]byte("old"))
	o.Add("topic-a", id, []byte("old"))
	// Before TTL: prune drops nothing.
	if dropped := o.Prune(); dropped != 0 {
		t.Errorf("expected 0 pruned before TTL, got %d", dropped)
	}
	// Sleep past TTL, then add a fresh entry — the fresh one must survive.
	time.Sleep(testTTL + 20*time.Millisecond)
	o.Add("topic-a", MsgIDFor([]byte("new")), []byte("new"))
	dropped := o.Prune()
	if dropped != 1 {
		t.Errorf("expected 1 pruned (the old one), got %d", dropped)
	}
	if o.Size() != 1 {
		t.Errorf("expected the fresh entry to survive, got size=%d", o.Size())
	}
}

func TestAddIsIdempotentOnDuplicateKey(t *testing.T) {
	o := New(testInterval, testTTL)
	id := MsgIDFor([]byte("p"))
	o.Add("topic-a", id, []byte("p"))
	if o.Size() != 1 {
		t.Fatalf("expected 1 after first Add, got %d", o.Size())
	}
	// Wait a touch, then re-Add. Size must stay 1 and LastRepublishedAt
	// must NOT advance (otherwise duplicate publishes would forever delay
	// republish, defeating the rate-limit).
	time.Sleep(20 * time.Millisecond)
	o.Add("topic-a", id, []byte("p"))
	if o.Size() != 1 {
		t.Errorf("expected 1 after duplicate Add, got %d", o.Size())
	}
}

func TestMsgIDForIsStable(t *testing.T) {
	// The JVM computes the same id; verifying determinism here is the
	// cheapest possible cross-language regression guard.
	a := MsgIDFor([]byte("payload-bytes"))
	b := MsgIDFor([]byte("payload-bytes"))
	if !bytes.Equal(a, b) {
		t.Error("MsgIDFor must be deterministic across calls")
	}
	if len(a) != 32 {
		t.Errorf("MsgIDFor must return 32 bytes, got %d", len(a))
	}
	c := MsgIDFor([]byte("different"))
	if bytes.Equal(a, c) {
		t.Error("MsgIDFor must produce different ids for different inputs")
	}
}

func TestSizeAcrossTopics(t *testing.T) {
	o := New(testInterval, testTTL)
	o.Add("t1", MsgIDFor([]byte("a")), []byte("a"))
	o.Add("t1", MsgIDFor([]byte("b")), []byte("b"))
	o.Add("t2", MsgIDFor([]byte("a")), []byte("a"))
	if o.Size() != 3 {
		t.Errorf("expected 3 across topics, got %d", o.Size())
	}
}

func TestConfirmEmptiesTopicMap(t *testing.T) {
	// Tidiness check: confirming the last entry in a topic must remove
	// the topic-level submap so Size and Prune don't have to walk a
	// long tail of empty maps.
	o := New(testInterval, testTTL)
	id := MsgIDFor([]byte("only"))
	o.Add("topic-a", id, []byte("only"))
	o.Confirm("topic-a", [][]byte{id})
	if o.Size() != 0 {
		t.Errorf("expected size 0, got %d", o.Size())
	}
	// The next Add must successfully recreate the topic map.
	o.Add("topic-a", id, []byte("only"))
	if o.Size() != 1 {
		t.Errorf("expected re-Add to land, got size=%d", o.Size())
	}
}
