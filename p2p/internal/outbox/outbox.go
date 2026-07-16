// Package outbox provides a small, in-memory republish ledger used by the
// sidecar to re-gossip messages until the JVM confirms them (task #196).
//
// Why this exists. GossipSub publish is fire-and-forget: a single Publish call
// hands the message to the local mesh once and never retries on the publisher's
// behalf. Under transient mesh degradation (peers in reorg-recovery, partition
// heal racing the publish, peer-score blowback) a single-shot publish can
// silently fail to reach a quorum of GL0s. The historical fix was per-protocol:
// AllowSpendBlock did a single HTTP POST to one random gl0 peer (broke when
// the peer was unhealthy — iter-s3prep-baseline-mg4); MetagraphBinary and
// MetagraphAttestation gossiped once and accepted the loss. The outbox
// generalises the retry: the sidecar holds each published message until
// either (a) the JVM acknowledges the message via ConfirmFinalized, or (b) the
// TTL expires. Between Add and Confirm, a periodic ticker re-publishes
// entries whose last-publish time is older than `republishInterval`.
//
// Scope (v1, intentional). In-memory only — sidecar crash means the JVM will
// resubmit on reconnect (its existing behaviour). Single ticker, single
// goroutine. No exponential backoff (flag for follow-up). No persistence (flag
// for follow-up). No metrics scaffolding beyond Size() for the gauge exporter.
package outbox

import (
	"encoding/hex"
	"sync"
	"time"
)

// Entry is a single message held by the outbox awaiting JVM confirmation.
// Fields are exported so the republish goroutine in main.go can read them
// without going through the outbox lock (the snapshot returned by
// DueForRepublish is a copy).
type Entry struct {
	// Topic the message belongs to (e.g. "allow-spend-block").
	Topic string
	// MsgID = first 32 bytes of sha256(payload). Stable across nodes; used as
	// the map key and as the wire id in ConfirmFinalized.
	MsgID []byte
	// Payload is the bytes handed to GossipSub. Opaque to the outbox.
	Payload []byte
	// FirstPublishedAt is the wall-clock time the entry was first Add()ed.
	// Compared against `now - ttl` in Prune() to drop ancient entries.
	FirstPublishedAt time.Time
	// LastRepublishedAt is the most recent time the republish ticker handed
	// this entry's payload back to the topic publisher. Compared against
	// `now - republishInterval` in DueForRepublish().
	LastRepublishedAt time.Time
}

// Outbox is a thread-safe ledger of unconfirmed published messages.
//
// Concurrency: a single sync.Mutex guards `entries`. All exported methods take
// the lock. The lock is held for O(N) work in DueForRepublish/Prune where N is
// the number of entries; at the expected sidecar scale (hundreds of
// in-flight outbox entries) this is fine. If profiling shows lock contention
// the natural next step is per-topic locks — but don't speculate ahead of the
// blocker (project note: feedback_dont_speculate_ahead_of_blockers).
type Outbox struct {
	mu sync.Mutex
	// entries is keyed by topic then by msgID-hex. Two-level map so Confirm
	// can short-circuit when an unknown topic is named — the typical case
	// during the rollout of a new topic.
	entries map[string]map[string]*Entry
	// republishInterval is the minimum age of an entry's LastRepublishedAt
	// before DueForRepublish returns it again. Defaults to 30s.
	republishInterval time.Duration
	// ttl is the maximum age of an entry from its FirstPublishedAt before
	// Prune drops it without confirmation. Defaults to 1h. Long enough that
	// in the expected confirmation cadence (k1=255 snapshots × ~7s = ~30min)
	// the JVM has ample time to ack; short enough that a leak on the JVM
	// side bounds memory.
	ttl time.Duration
}

// New returns an Outbox configured with the given republish interval and TTL.
// Both must be positive; values are not validated.
func New(republishInterval, ttl time.Duration) *Outbox {
	return &Outbox{
		entries:           make(map[string]map[string]*Entry),
		republishInterval: republishInterval,
		ttl:               ttl,
	}
}

// NewDefault returns an Outbox with `republishInterval=30s` and `ttl=1h`,
// matching the task #196 design defaults.
func NewDefault() *Outbox {
	return New(30*time.Second, 1*time.Hour)
}

// Add records that `payload` was just published to `topic` with the given
// `msgID`. Idempotent on (topic, msgID): a second Add for the same key does
// NOT reset timestamps — the existing entry's LastRepublishedAt is preserved
// so the next ticker sweep still rate-limits republish. This makes Add safe
// to call from both the initial Publish RPC handler and (defensively) from
// the republish loop without corrupting the rate-limit.
func (o *Outbox) Add(topic string, msgID, payload []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	byMsg, ok := o.entries[topic]
	if !ok {
		byMsg = make(map[string]*Entry)
		o.entries[topic] = byMsg
	}
	key := hex.EncodeToString(msgID)
	if _, exists := byMsg[key]; exists {
		// Don't reset timestamps on duplicate Add — the republish loop
		// owns LastRepublishedAt. Payload assumed identical (sha256
		// collision = bug elsewhere); we keep the original.
		return
	}
	// Copy slices defensively — caller may reuse the buffer.
	idCopy := make([]byte, len(msgID))
	copy(idCopy, msgID)
	payloadCopy := make([]byte, len(payload))
	copy(payloadCopy, payload)
	byMsg[key] = &Entry{
		Topic:             topic,
		MsgID:             idCopy,
		Payload:           payloadCopy,
		FirstPublishedAt:  now,
		LastRepublishedAt: now,
	}
}

// Confirm drops outbox entries for the listed message ids on `topic`. Returns
// the number actually dropped (≤ len(msgIDs)). Unknown ids are ignored —
// confirmation is idempotent so a duplicate JVM acknowledgement costs nothing.
func (o *Outbox) Confirm(topic string, msgIDs [][]byte) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	byMsg, ok := o.entries[topic]
	if !ok {
		return 0
	}
	dropped := 0
	for _, id := range msgIDs {
		key := hex.EncodeToString(id)
		if _, exists := byMsg[key]; exists {
			delete(byMsg, key)
			dropped++
		}
	}
	// Tidy empty topic-maps so Size()'s O(topics) cost stays bounded.
	if len(byMsg) == 0 {
		delete(o.entries, topic)
	}
	return dropped
}

// DueForRepublish returns a snapshot of entries whose LastRepublishedAt is
// older than `now - republishInterval`. Each returned Entry is a value copy
// (not a pointer into the outbox) so the caller may iterate and publish
// without holding any lock. The caller MUST call MarkRepublished for each
// entry it actually re-publishes; that's what updates LastRepublishedAt and
// rate-limits the next tick.
//
// Note that the entry might have been Confirm'd between this call and
// MarkRepublished — MarkRepublished is a no-op in that case (we look up by
// topic+msgID), so the republish goroutine doesn't need extra synchronization.
func (o *Outbox) DueForRepublish() []Entry {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	due := make([]Entry, 0)
	for _, byMsg := range o.entries {
		for _, e := range byMsg {
			if now.Sub(e.LastRepublishedAt) >= o.republishInterval {
				due = append(due, *e)
			}
		}
	}
	return due
}

// MarkRepublished updates LastRepublishedAt on the entry identified by
// (topic, msgID). Called by the republish loop AFTER the actual topic Publish
// returns (success or failure — we don't distinguish, the next tick retries
// either way). No-op if the entry was Confirm'd in the meantime.
func (o *Outbox) MarkRepublished(topic string, msgID []byte) {
	o.mu.Lock()
	defer o.mu.Unlock()
	byMsg, ok := o.entries[topic]
	if !ok {
		return
	}
	if e, exists := byMsg[hex.EncodeToString(msgID)]; exists {
		e.LastRepublishedAt = time.Now()
	}
}

// Prune drops entries whose FirstPublishedAt is older than `now - ttl`.
// Returns the count dropped — caller logs at WARN when non-zero (a non-zero
// rate means confirmation is taking longer than ttl, which is a real problem
// worth surfacing).
func (o *Outbox) Prune() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	now := time.Now()
	dropped := 0
	for topic, byMsg := range o.entries {
		for key, e := range byMsg {
			if now.Sub(e.FirstPublishedAt) >= o.ttl {
				delete(byMsg, key)
				dropped++
			}
		}
		if len(byMsg) == 0 {
			delete(o.entries, topic)
		}
	}
	return dropped
}

// Size returns the total number of entries across all topics. Cheap (O(topics)
// not O(entries)) so it's safe to call from a gauge updater.
func (o *Outbox) Size() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	total := 0
	for _, byMsg := range o.entries {
		total += len(byMsg)
	}
	return total
}

// MsgIDFor returns the first 32 bytes of sha256(payload). Stable across nodes
// (no salt, no node identity baked in) so the JVM and sidecar can independently
// compute the same id for the same wire bytes — that's how the JVM addresses
// outbox entries in ConfirmFinalized without needing the sidecar to surface
// them after Publish.
//
// Lives in this package (not crypto/sha256 inlined at the call site) so tests
// have a single canonical entry point — if the hash algorithm ever changes,
// it changes here and nowhere else. v1 uses a literal sha256.Sum256; if we
// ever want a domain prefix or shorter id, this is the seam.
func MsgIDFor(payload []byte) []byte {
	return msgIDFor(payload)
}
