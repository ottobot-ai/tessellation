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

	"crypto/ed25519"

	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/scasplte2/tessellation/p2p/internal/chainsync"
	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	"github.com/scasplte2/tessellation/p2p/internal/grpcserver"
	"github.com/scasplte2/tessellation/p2p/internal/httpbridge"
	"github.com/scasplte2/tessellation/p2p/internal/metrics"
)

func main() {
	cfg := config.DefaultConfig()

	// CLI flags
	var (
		listenAddrs    string
		seedlist       string
		httpAddr       string
		enableHTTP     bool
		jvmGRPCAddr    string
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
	fmt.Printf("  Topics: %s, %s, %s\n", cfg.SnapshotTopic, cfg.AttestationTopic, cfg.RumorTopic)
	fmt.Printf("  gRPC:   %s\n", cfg.GRPCAddr)

	// Bootstrap the Kademlia DHT routing table and start the rendezvous
	// discovery loop. Once running, peer discovery is fully decentralized;
	// the seedlist is only used as the initial entry point.
	if err := node.BootstrapDHT(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "WARN: DHT bootstrap: %v\n", err)
	}

	// Connect to seedlist in background — each dial can block up to 10s on
	// unreachable peers, and with N nodes starting sequentially most peers
	// aren't up yet. Running this after gRPC server setup ensures the
	// healthcheck (nc -z localhost 50051) passes immediately.
	if len(cfg.Seedlist) > 0 {
		go func() {
			if err := node.ConnectSeedlist(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "WARN: seedlist connect: %v\n", err)
			}
		}()
	}

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

	// Start gRPC server (blocks until shutdown)
	srv := grpcserver.New(node, csHandler)
	go func() {
		<-ctx.Done()
		srv.Stop()
	}()

	if err := srv.Start(cfg.GRPCAddr); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "ERROR: gRPC server: %v\n", err)
		os.Exit(1)
	}
}
