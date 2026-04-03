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
	SnapshotTopic     string
	AttestationTopic  string

	// GossipSub parameters
	MeshD    int // target mesh degree (default 6)
	MeshDLo  int // low watermark (default 4)
	MeshDHi  int // high watermark (default 12)

	// Heartbeat interval for mesh maintenance
	HeartbeatInterval time.Duration

	// PrivateKey is the libp2p identity key (Ed25519).
	// If empty, a new one is generated each run.
	PrivateKeyPath string

	// MetricsAddr enables Prometheus metrics if non-empty.
	MetricsAddr string
}

// DefaultConfig returns sensible defaults for a Nakamoto sidecar.
func DefaultConfig() Config {
	return Config{
		ListenAddrs:       []string{"/ip4/0.0.0.0/tcp/9500"},
		GRPCAddr:          "127.0.0.1:50051",
		SnapshotTopic:     "/nakamoto/snapshots/1.0.0",
		AttestationTopic:  "/nakamoto/attestations/1.0.0",
		MeshD:             6,
		MeshDLo:           4,
		MeshDHi:           12,
		HeartbeatInterval: 10 * time.Second,
	}
}
