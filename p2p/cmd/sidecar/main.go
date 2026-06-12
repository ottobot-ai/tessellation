package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"crypto/ed25519"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/chainsync"
	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	"github.com/scasplte2/tessellation/p2p/internal/grpcserver"
	"github.com/scasplte2/tessellation/p2p/internal/httpbridge"
	"github.com/scasplte2/tessellation/p2p/internal/metrics"
	"github.com/scasplte2/tessellation/p2p/internal/outbox"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

func main() {
	cfg := config.DefaultConfig()

	// CLI flags
	var (
		listenAddrs string
		seedlist    string
		httpAddr    string
		enableHTTP  bool
		jvmGRPCAddr string
	)
	flag.StringVar(&listenAddrs, "listen", "/ip4/0.0.0.0/tcp/9500", "comma-separated libp2p listen multiaddrs")
	flag.StringVar(&seedlist, "seedlist", "", "comma-separated bootstrap peer multiaddrs")
	flag.StringVar(&cfg.GRPCAddr, "grpc", cfg.GRPCAddr, "gRPC listen address for JVM")
	flag.StringVar(&httpAddr, "http", "127.0.0.1:50052", "HTTP bridge listen address (debug/fallback)")
	flag.BoolVar(&enableHTTP, "enable-http", false, "enable HTTP bridge (debug/fallback, gRPC is the primary interface)")
	flag.StringVar(&jvmGRPCAddr, "jvm-grpc", "127.0.0.1:50053", "JVM ChainSyncInbound gRPC address (sidecar calls JVM to serve peer requests)")
	flag.StringVar(&cfg.PrivateKeyPath, "key", "", "path to Ed25519 private key file")
	flag.StringVar(&cfg.MetricsAddr, "metrics", ":9501", "Prometheus metrics listen address (empty = disabled)")
	flag.BoolVar(&cfg.DisableMdns, "disable-mdns", false, "disable mDNS peer discovery (force DHT-only — for multi-host validation)")
	flag.IntVar(&cfg.SnapshotBufferSize, "snapshot-buffer", cfg.SnapshotBufferSize, "per-subscriber relay buffer size for snapshots (full = drop, counted in sidecar_gossip_messages_dropped_total)")
	flag.IntVar(&cfg.AttestationBufferSize, "attestation-buffer", cfg.AttestationBufferSize, "per-subscriber relay buffer size for attestations")
	flag.IntVar(&cfg.RumorBufferSize, "rumor-buffer", cfg.RumorBufferSize, "per-subscriber relay buffer size for rumors")
	flag.IntVar(&cfg.MetagraphBinaryBufferSize, "metagraph-binary-buffer", cfg.MetagraphBinaryBufferSize, "per-subscriber relay buffer size for metagraph binaries")
	flag.IntVar(&cfg.MetagraphAttestationBufferSize, "metagraph-attestation-buffer", cfg.MetagraphAttestationBufferSize, "per-subscriber relay buffer size for metagraph attestations")
	flag.IntVar(&cfg.AllowSpendBlockBufferSize, "allow-spend-block-buffer", cfg.AllowSpendBlockBufferSize, "per-subscriber relay buffer size for allow-spend blocks")
	flag.IntVar(&cfg.DAGBlockBufferSize, "dag-block-buffer", cfg.DAGBlockBufferSize, "per-subscriber relay buffer size for dag blocks")
	flag.IntVar(&cfg.TokenLockBlockBufferSize, "token-lock-block-buffer", cfg.TokenLockBlockBufferSize, "per-subscriber relay buffer size for token-lock blocks")
	flag.DurationVar(&cfg.OutboxRepublishInterval, "outbox-republish-interval", cfg.OutboxRepublishInterval, "outbox re-publish cadence (#196)")
	flag.DurationVar(&cfg.OutboxTTL, "outbox-ttl", cfg.OutboxTTL, "outbox entry max age before drop without confirmation (#196)")

	var generateKey bool
	var showPeerID bool
	var deriveFromECDSA string
	flag.BoolVar(&generateKey, "generate-key", false, "generate a new Ed25519 key, save to -key path, print peer ID, and exit")
	flag.BoolVar(&showPeerID, "show-peer-id", false, "load Ed25519 key from -key path, print its libp2p peer ID, and exit")
	flag.StringVar(&deriveFromECDSA, "derive-from-ecdsa", "", "derive Ed25519 key from ECDSA hex file, save to -key path, print peer ID, and exit")
	flag.Parse()

	// Key management modes: generate, derive, or inspect identity keys for compose-runner
	if deriveFromECDSA != "" {
		if cfg.PrivateKeyPath == "" {
			fmt.Fprintln(os.Stderr, "ERROR: -derive-from-ecdsa requires -key <path>")
			os.Exit(1)
		}
		ecdsaHex, err := os.ReadFile(deriveFromECDSA)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: read ECDSA hex file %s: %v\n", deriveFromECDSA, err)
			os.Exit(1)
		}
		ecdsaBytes, err := hex.DecodeString(strings.TrimSpace(string(ecdsaHex)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: decode ECDSA hex: %v\n", err)
			os.Exit(1)
		}
		// Derive Ed25519 seed: SHA-256(domain || ecdsaPrivKey)
		h := sha256.New()
		h.Write([]byte("tessellation-nakamoto-sidecar-identity:"))
		h.Write(ecdsaBytes)
		seed := h.Sum(nil) // 32 bytes
		ed25519Key := ed25519.NewKeyFromSeed(seed)
		// libp2p expects the full 64-byte Ed25519 private key (seed + public key)
		if err := os.WriteFile(cfg.PrivateKeyPath, ed25519Key, 0600); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: write key to %s: %v\n", cfg.PrivateKeyPath, err)
			os.Exit(1)
		}
		priv, err := libp2pcrypto.UnmarshalEd25519PrivateKey(ed25519Key)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: marshal libp2p key: %v\n", err)
			os.Exit(1)
		}
		id, _ := peer.IDFromPrivateKey(priv)
		fmt.Print(id.String())
		os.Exit(0)
	}
	if generateKey {
		if cfg.PrivateKeyPath == "" {
			fmt.Fprintln(os.Stderr, "ERROR: -generate-key requires -key <path>")
			os.Exit(1)
		}
		priv, _, err := libp2pcrypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: generate key: %v\n", err)
			os.Exit(1)
		}
		raw, err := priv.Raw()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: marshal key: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(cfg.PrivateKeyPath, raw, 0600); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: write key to %s: %v\n", cfg.PrivateKeyPath, err)
			os.Exit(1)
		}
		id, _ := peer.IDFromPrivateKey(priv)
		fmt.Print(id.String())
		os.Exit(0)
	}
	if showPeerID {
		if cfg.PrivateKeyPath == "" {
			fmt.Fprintln(os.Stderr, "ERROR: -show-peer-id requires -key <path>")
			os.Exit(1)
		}
		keyBytes, err := os.ReadFile(cfg.PrivateKeyPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: read key: %v\n", err)
			os.Exit(1)
		}
		priv, err := libp2pcrypto.UnmarshalEd25519PrivateKey(keyBytes)
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: parse key: %v\n", err)
			os.Exit(1)
		}
		id, _ := peer.IDFromPrivateKey(priv)
		fmt.Print(id.String())
		os.Exit(0)
	}

	if listenAddrs != "" {
		cfg.ListenAddrs = strings.Split(listenAddrs, ",")
	}
	if seedlist != "" {
		cfg.Seedlist = strings.Split(seedlist, ",")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		cancel()
	}()

	// Start libp2p + GossipSub
	node, err := gossip.New(ctx, cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}
	defer node.Close()

	fmt.Printf("Sidecar started\n")
	fmt.Printf("  PeerID: %s\n", node.Host.ID())
	for _, addr := range node.Host.Addrs() {
		fmt.Printf("  Listen: %s/p2p/%s\n", addr, node.Host.ID())
	}
	fmt.Printf("  Topics: %s, %s, %s, %s, %s, %s, %s, %s\n",
		cfg.SnapshotTopic,
		cfg.AttestationTopic,
		cfg.RumorTopic,
		cfg.MetagraphBinaryTopic,
		cfg.MetagraphAttestationTopic,
		cfg.AllowSpendBlockTopic,
		cfg.DAGBlockTopic,
		cfg.TokenLockBlockTopic,
	)
	if cfg.NumShards > 1 {
		// Shard-checkpoint topics are per-shard (`<prefix><shardId>`); the sidecar
		// eagerly joins shards 0 .. NumShards-1 in gossip.New (v1 full-set committee
		// membership). Surface them on startup so the join is observable — the
		// pre-fix sidecar's "Topics:" line never listed any shard-checkpoint topic.
		fmt.Printf("  Shard topics: %s{0..%d}, %s{0..%d} (numShards=%d)\n",
			cfg.ShardCheckpointTopicPrefix, cfg.NumShards-1,
			cfg.ShardCheckpointAttestationTopicPrefix, cfg.NumShards-1,
			cfg.NumShards,
		)
	}
	fmt.Printf("  gRPC:   %s\n", cfg.GRPCAddr)

	// Bootstrap the Kademlia DHT routing table and start the rendezvous
	// discovery loop. Once running, peer discovery is fully decentralized;
	// the seedlist is only used as the initial entry point.
	if err := node.BootstrapDHT(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "WARN: DHT bootstrap: %v\n", err)
	}

	// Connect to seedlist in background with retry — each dial can block up to
	// 10s on unreachable peers, and with N nodes starting sequentially most peers
	// aren't up yet. Retry every 10s until all seeds are connected or context is done.
	if len(cfg.Seedlist) > 0 {
		go func() {
			for attempt := 0; attempt < 12; attempt++ {
				if attempt > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(10 * time.Second):
					}
				}
				allConnected := true
				for _, addr := range cfg.Seedlist {
					ma, err := multiaddr.NewMultiaddr(addr)
					if err != nil {
						continue
					}
					pi, err := peer.AddrInfoFromP2pAddr(ma)
					if err != nil {
						continue
					}
					if pi.ID == node.Host.ID() {
						continue // skip self
					}
					if node.Host.Network().Connectedness(pi.ID) == libp2pnet.Connected {
						continue // already connected
					}
					allConnected = false
					dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					if err := node.Host.Connect(dialCtx, *pi); err != nil {
						fmt.Printf("WARN: failed to connect to seed %s: %v\n", addr, err)
					}
					cancel()
				}
				if allConnected {
					fmt.Printf("Seedlist: all peers connected (attempt %d)\n", attempt)
					return
				}
			}
		}()
	}

	// Mesh health monitor — detects GossipSub mesh degradation after network
	// partitions and forces seedlist reconnection to restore gossip flow.
	node.StartMeshHealthMonitor(ctx)

	// Prometheus metrics server
	if cfg.MetricsAddr != "" {
		metricsSrv := metrics.ListenAndServe(cfg.MetricsAddr)
		metrics.StartGaugeUpdater(ctx, node.Host, node.DHT, node.Topics())
		go func() {
			<-ctx.Done()
			metricsSrv.Close()
		}()
	}

	// HTTP bridge — opt-in debug/fallback (gRPC is the primary JVM interface)
	if enableHTTP {
		fmt.Printf("  HTTP:   %s (debug/fallback)\n", httpAddr)
		bridge := httpbridge.New(node)
		go func() {
			<-ctx.Done()
			bridge.Stop()
		}()
		go func() {
			if err := bridge.Start(httpAddr); err != nil && ctx.Err() == nil {
				fmt.Fprintf(os.Stderr, "WARN: HTTP bridge: %v\n", err)
			}
		}()
	}

	// Start ChainSync protocol handler (libp2p stream-based request-response)
	csHandler, err := chainsync.New(node.Host, jvmGRPCAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WARN: ChainSync init: %v (will operate without active parent fetching)\n", err)
	} else {
		fmt.Printf("  ChainSync: /nakamoto/chainsync/1.0.0 (JVM inbound: %s)\n", jvmGRPCAddr)
	}

	// Durable-publish outbox (#196). The same instance is wired into the gRPC
	// server (where Publish handlers Add and ConfirmFinalized Confirm) AND
	// into the republish goroutine below (which re-publishes due entries on
	// a ticker). In-memory only — if the sidecar crashes the JVM resubmits
	// on reconnect.
	ob := outbox.New(cfg.OutboxRepublishInterval, cfg.OutboxTTL)
	fmt.Printf("  Outbox: republish=%s ttl=%s\n", cfg.OutboxRepublishInterval, cfg.OutboxTTL)
	startOutboxRepublisher(ctx, node, ob, cfg.OutboxRepublishInterval)

	// Start gRPC server (blocks until shutdown)
	srv := grpcserver.New(node, csHandler, ob)
	go func() {
		<-ctx.Done()
		srv.Stop()
	}()

	if err := srv.Start(cfg.GRPCAddr); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "ERROR: gRPC server: %v\n", err)
		os.Exit(1)
	}
}

// startOutboxRepublisher launches a single goroutine that ticks every
// `interval` and (a) prunes TTL-expired entries, (b) re-publishes due
// entries. Routing per-topic to the correct gossip primitive keeps
// the outbox itself transport-agnostic. The goroutine exits when ctx is
// done. See task #196.
func startOutboxRepublisher(ctx context.Context, node *gossip.Node, ob *outbox.Outbox, interval time.Duration) {
	go func() {
		// Tick at interval/2, not interval: DueForRepublish requires
		// age >= interval and MarkRepublished stamps AFTER the publish, so a
		// ticker at exactly `interval` always finds entries a hair younger
		// than the threshold and they slip to the NEXT tick — observed as
		// 2×interval (60s for the 30s default) effective republish latency
		// (task #40). Half-interval ticks bound worst-case latency at ~1.5×
		// interval while DueForRepublish still rate-limits actual publishes.
		ticker := time.NewTicker(interval / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if dropped := ob.Prune(); dropped > 0 {
				fmt.Printf("outbox: pruned %d TTL-expired entries (Phase-3 finality stalled?)\n", dropped)
				metrics.OutboxPrunedTTL.Add(float64(dropped))
			}
			due := ob.DueForRepublish()
			for _, e := range due {
				// Route by topic label — single static switch, no
				// indirection. The published bytes are exactly the
				// proto-marshalled wrapper the Publish RPC produced.
				var perr error
				switch e.Topic {
				case grpcserver.TopicAllowSpendBlock:
					perr = node.PublishAllowSpendBlock(ctx, e.Payload)
				case grpcserver.TopicMetagraphBinary:
					perr = node.PublishMetagraphBinary(ctx, e.Payload)
				case grpcserver.TopicMetagraphAttestation:
					perr = node.PublishMetagraphAttestation(ctx, e.Payload)
				case grpcserver.TopicDAGBlock:
					perr = node.PublishDAGBlock(ctx, e.Payload)
				case grpcserver.TopicTokenLockBlock:
					perr = node.PublishTokenLockBlock(ctx, e.Payload)
				case grpcserver.TopicShardCheckpoint:
					// Slice 14: per-shard topic — re-derive the shard id from
					// the stored wire bytes (the payload IS a marshalled
					// ShardCheckpointWire) so the republish lands on the same
					// per-shard topic the original publish used.
					var sc pb.ShardCheckpointWire
					if uerr := proto.Unmarshal(e.Payload, &sc); uerr != nil {
						fmt.Printf("outbox: shard-checkpoint payload unmarshal failed, dropping entry: %v\n", uerr)
						ob.Confirm(e.Topic, [][]byte{e.MsgID})
						continue
					}
					perr = node.PublishShardCheckpoint(ctx, sc.ShardId, e.Payload)
				case grpcserver.TopicShardCheckpointAttestation:
					var sca pb.ShardCheckpointAttestationWire
					if uerr := proto.Unmarshal(e.Payload, &sca); uerr != nil {
						fmt.Printf("outbox: shard-checkpoint-attestation payload unmarshal failed, dropping entry: %v\n", uerr)
						ob.Confirm(e.Topic, [][]byte{e.MsgID})
						continue
					}
					perr = node.PublishShardCheckpointAttestation(ctx, sca.ShardId, e.Payload)
				default:
					// Unknown topic in outbox — bug elsewhere; drop the
					// entry so it doesn't loop forever.
					fmt.Printf("outbox: unknown topic %q, dropping entry\n", e.Topic)
					ob.Confirm(e.Topic, [][]byte{e.MsgID})
					continue
				}
				if perr != nil {
					// Don't drop the entry — next tick will try again.
					// Single log line per re-publish failure is loud
					// enough; volume goes nowhere unless the mesh is
					// actually broken.
					fmt.Printf("outbox: republish %s/%x failed: %v\n", e.Topic, e.MsgID[:8], perr)
				}
				// Update LastRepublishedAt regardless of publish error —
				// a publish that errored is still "we tried recently",
				// which is exactly the semantic the rate-limit wants.
				ob.MarkRepublished(e.Topic, e.MsgID)
				metrics.OutboxRepublished.Inc()
			}
			metrics.OutboxSize.Set(float64(ob.Size()))
		}
	}()
}
