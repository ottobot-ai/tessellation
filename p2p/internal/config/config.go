package config

import (
	"time"
)

// Config holds all sidecar configuration.
type Config struct {
	// ListenAddrs are libp2p multiaddrs to listen on.
	ListenAddrs []string

	// Seedlist of bootstrap peer multiaddrs.
	Seedlist []string

	// GRPCAddr is the localhost address for the JVM-facing gRPC server.
	GRPCAddr string

	// Topics
	SnapshotTopic             string
	AttestationTopic          string
	RumorTopic                string
	MetagraphBinaryTopic      string
	MetagraphAttestationTopic string
	AllowSpendBlockTopic      string
	DAGBlockTopic             string
	TokenLockBlockTopic       string

	// GossipSub parameters
	MeshD   int // target mesh degree (default 6)
	MeshDLo int // low watermark (default 4)
	MeshDHi int // high watermark (default 12)

	// Heartbeat interval for mesh maintenance
	HeartbeatInterval time.Duration

	// Per-topic relay channel buffer sizes. Each Subscribe() call on the sidecar
	// gets its own buffered Go channel sized from these; when full, inbound
	// messages on that topic are dropped and counted in sidecar_gossip_messages_dropped_total.
	// Tune upward if drops are observed during normal operation; tune downward
	// to fail-fast on JVM-consumer stalls.
	SnapshotBufferSize             int
	AttestationBufferSize          int
	RumorBufferSize                int
	MetagraphBinaryBufferSize      int
	MetagraphAttestationBufferSize int
	AllowSpendBlockBufferSize      int
	DAGBlockBufferSize             int
	TokenLockBlockBufferSize       int

	// Outbox parameters. The sidecar keeps an in-memory ledger of recently-
	// published AllowSpendBlock / MetagraphBinary / MetagraphAttestation
	// messages and re-publishes them on a ticker until the JVM acknowledges
	// Phase-3 finality via ConfirmFinalized. See task #196 / outbox package.
	//
	// OutboxRepublishInterval: minimum age of an entry's last publish before
	//   the ticker re-publishes it. Default 30s — balances "loud enough to
	//   recover from transient mesh degradation" against "not flooding the
	//   topic when finality is just slow".
	// OutboxTTL: maximum age of an unconfirmed entry. After this, the entry
	//   is dropped without confirmation; the JVM will eventually re-submit
	//   on reconnect (sidecar restart is the disaster fallback). Default 1h.
	OutboxRepublishInterval time.Duration
	OutboxTTL               time.Duration

	// PrivateKey is the libp2p identity key (Ed25519).
	// If empty, a new one is generated each run.
	PrivateKeyPath string

	// MetricsAddr enables Prometheus metrics if non-empty.
	MetricsAddr string

	// DisableMdns disables the mDNS local-network discovery service. Set this
	// when validating that the Kademlia DHT is the sole source of peer
	// discovery — useful for multi-host deployment validation where mDNS
	// cannot reach across subnets and the cluster MUST work via DHT alone.
	DisableMdns bool
}

// DefaultConfig returns sensible defaults for a Nakamoto sidecar.
func DefaultConfig() Config {
	return Config{
		ListenAddrs:               []string{"/ip4/0.0.0.0/tcp/9500"},
		GRPCAddr:                  "127.0.0.1:50051",
		SnapshotTopic:             "/nakamoto/snapshots/1.0.0",
		AttestationTopic:          "/nakamoto/attestations/1.0.0",
		RumorTopic:                "/tessellation/rumors/1.0.0",
		MetagraphBinaryTopic:      "/tessellation/metagraph-binaries/1.0.0",
		MetagraphAttestationTopic: "/tessellation/metagraph-attestations/1.0.0",
		AllowSpendBlockTopic:      "/tessellation/allow-spend-blocks/1.0.0",
		DAGBlockTopic:             "/tessellation/dag-blocks/1.0.0",
		TokenLockBlockTopic:       "/tessellation/token-lock-blocks/1.0.0",
		MeshD:                     6,
		MeshDLo:                   4,
		MeshDHi:                   12,
		HeartbeatInterval:         10 * time.Second,
		// Buffer sizes scale with expected burst rate per topic.
		// Slots tick at 1 Hz; snapshots arrive ~every 7s (winning slots are sparse).
		// Snapshot: ~1 per 7s → 64 tolerates ~7 min of backlog for a slow JVM consumer.
		// Attestation: N-of-N per snapshot → 256 tolerates attestation fan-in bursts.
		// Rumor: BFT gossip can burst at consensus transitions → 1024.
		// Metagraph binary: 500 KB/msg, ~8 concurrent metagraphs → 256.
		// Metagraph attestation: committee fan-in per binary → 256.
		SnapshotBufferSize:             64,
		AttestationBufferSize:          256,
		RumorBufferSize:                1024,
		MetagraphBinaryBufferSize:      256,
		MetagraphAttestationBufferSize: 256,
		// AllowSpendBlock: single fb produced per dl1 swap consensus round
		// (~5s cadence in prod), so burst rate is comparable to metagraph
		// binaries. Same 256-slot buffer.
		AllowSpendBlockBufferSize: 256,
		// DAGBlock + TokenLockBlock: same ~5s consensus cadence on dl1 as
		// AllowSpendBlock; reuse the 256-slot default.
		DAGBlockBufferSize:       256,
		TokenLockBlockBufferSize: 256,
		OutboxRepublishInterval:  30 * time.Second,
		OutboxTTL:                1 * time.Hour,
	}
}
