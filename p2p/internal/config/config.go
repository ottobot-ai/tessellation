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
	SnapshotTopic         string
	AttestationTopic      string
	RumorTopic            string
	MetagraphBinaryTopic  string

	// GossipSub parameters
	MeshD    int // target mesh degree (default 6)
	MeshDLo  int // low watermark (default 4)
	MeshDHi  int // high watermark (default 12)

	// Heartbeat interval for mesh maintenance
	HeartbeatInterval time.Duration

	// Per-topic relay channel buffer sizes. Each Subscribe() call on the sidecar
	// gets its own buffered Go channel sized from these; when full, inbound
	// messages on that topic are dropped and counted in sidecar_gossip_messages_dropped_total.
	// Tune upward if drops are observed during normal operation; tune downward
	// to fail-fast on JVM-consumer stalls.
	SnapshotBufferSize         int
	AttestationBufferSize      int
	RumorBufferSize            int
	MetagraphBinaryBufferSize  int

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
		ListenAddrs:       []string{"/ip4/0.0.0.0/tcp/9500"},
		GRPCAddr:          "127.0.0.1:50051",
		SnapshotTopic:        "/nakamoto/snapshots/1.0.0",
		AttestationTopic:     "/nakamoto/attestations/1.0.0",
		RumorTopic:           "/tessellation/rumors/1.0.0",
		MetagraphBinaryTopic: "/tessellation/metagraph-binaries/1.0.0",
		MeshD:             6,
		MeshDLo:           4,
		MeshDHi:           12,
		HeartbeatInterval: 10 * time.Second,
		// Buffer sizes scale with expected burst rate per topic.
		// Slots tick at 1 Hz; snapshots arrive ~every 7s (winning slots are sparse).
		// Snapshot: ~1 per 7s → 64 tolerates ~7 min of backlog for a slow JVM consumer.
		// Attestation: N-of-N per snapshot → 256 tolerates attestation fan-in bursts.
		// Rumor: BFT gossip can burst at consensus transitions → 1024.
		// Metagraph binary: 500 KB/msg, ~8 concurrent metagraphs → 256.
		SnapshotBufferSize:        64,
		AttestationBufferSize:     256,
		RumorBufferSize:           1024,
		MetagraphBinaryBufferSize: 256,
	}
}
