package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	"github.com/scasplte2/tessellation/p2p/internal/grpcserver"
	"github.com/scasplte2/tessellation/p2p/internal/httpbridge"
)

func main() {
	cfg := config.DefaultConfig()

	// CLI flags
	var (
		listenAddrs string
		seedlist    string
		httpAddr    string
	)
	flag.StringVar(&listenAddrs, "listen", "/ip4/0.0.0.0/tcp/9500", "comma-separated libp2p listen multiaddrs")
	flag.StringVar(&seedlist, "seedlist", "", "comma-separated bootstrap peer multiaddrs")
	flag.StringVar(&cfg.GRPCAddr, "grpc", cfg.GRPCAddr, "gRPC listen address for JVM")
	flag.StringVar(&httpAddr, "http", "127.0.0.1:50052", "HTTP bridge listen address for JVM")
	flag.StringVar(&cfg.PrivateKeyPath, "key", "", "path to Ed25519 private key file")
	flag.StringVar(&cfg.MetricsAddr, "metrics", "", "Prometheus metrics address (empty = disabled)")
	flag.Parse()

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
	fmt.Printf("  Topics: %s, %s\n", cfg.SnapshotTopic, cfg.AttestationTopic)
	fmt.Printf("  gRPC:   %s\n", cfg.GRPCAddr)
	fmt.Printf("  HTTP:   %s\n", httpAddr)

	// Connect to seedlist
	if len(cfg.Seedlist) > 0 {
		if err := node.ConnectSeedlist(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: seedlist connect: %v\n", err)
		}
	}

	// Start HTTP bridge
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

	// Start gRPC server (blocks until shutdown)
	srv := grpcserver.New(node)
	go func() {
		<-ctx.Done()
		srv.Stop()
	}()

	if err := srv.Start(cfg.GRPCAddr); err != nil && ctx.Err() == nil {
		fmt.Fprintf(os.Stderr, "ERROR: gRPC server: %v\n", err)
		os.Exit(1)
	}
}
