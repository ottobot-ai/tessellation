package gossip

import (
	"context"
	"fmt"
	"os"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2prouting "github.com/libp2p/go-libp2p/core/routing"
	routeddiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"

	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/metrics"
)

// Node manages libp2p host and GossipSub topics.
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub
	DHT    *dht.IpfsDHT

	snapshotTopic    *pubsub.Topic
	attestationTopic *pubsub.Topic
	rumorTopic       *pubsub.Topic

	cfg config.Config
}

// New creates a libp2p host with GossipSub and joins the Nakamoto topics.
func New(ctx context.Context, cfg config.Config) (*Node, error) {
	// Parse listen addresses
	listenAddrs := make([]multiaddr.Multiaddr, 0, len(cfg.ListenAddrs))
	for _, addr := range cfg.ListenAddrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid listen addr %q: %w", addr, err)
		}
		listenAddrs = append(listenAddrs, ma)
	}

	// Build libp2p host options. When a pre-generated Ed25519 key is provided
	// via -key, the host identity is deterministic — compose-runner uses this to
	// pre-compute the peer ID and include it in the seedlist multiaddrs so that
	// DHT-only discovery (no mDNS) works: /dns4/sidecar-N/tcp/9500/p2p/<id>.
	hostOpts := []libp2p.Option{
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.ForceReachabilityPrivate(),
		libp2p.DefaultSecurity,
	}
	if cfg.PrivateKeyPath != "" {
		keyBytes, err := os.ReadFile(cfg.PrivateKeyPath)
		if err != nil {
			return nil, fmt.Errorf("read identity key %q: %w", cfg.PrivateKeyPath, err)
		}
		privKey, err := crypto.UnmarshalEd25519PrivateKey(keyBytes)
		if err != nil {
			return nil, fmt.Errorf("parse Ed25519 key from %q: %w", cfg.PrivateKeyPath, err)
		}
		hostOpts = append(hostOpts, libp2p.Identity(privKey))
	}

	// Kademlia DHT in server mode so other peers can use this node as a
	// bootstrap target. The seedlist is the initial entry point; once connected,
	// peer discovery is fully decentralized via the DHT routing table.
	var kadDHT *dht.IpfsDHT
	hostOpts = append(hostOpts, libp2p.Routing(func(host host.Host) (libp2prouting.PeerRouting, error) {
		d, derr := dht.New(ctx, host, dht.Mode(dht.ModeServer))
		if derr != nil {
			return nil, derr
		}
		kadDHT = d
		return d, nil
	}))

	h, err := libp2p.New(hostOpts...)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	// Create GossipSub with custom parameters
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithGossipSubParams(pubsub.GossipSubParams{
			D:                 cfg.MeshD,
			Dlo:               cfg.MeshDLo,
			Dhi:               cfg.MeshDHi,
			HeartbeatInterval: cfg.HeartbeatInterval,
			// Use defaults for everything else
			Dout:                    pubsub.GossipSubDout,
			HistoryLength:           pubsub.GossipSubHistoryLength,
			HistoryGossip:           pubsub.GossipSubHistoryGossip,
			Dlazy:                   pubsub.GossipSubDlazy,
			GossipFactor:            pubsub.GossipSubGossipFactor,
			GossipRetransmission:    pubsub.GossipSubGossipRetransmission,
			HeartbeatInitialDelay:   pubsub.GossipSubHeartbeatInitialDelay,
			FanoutTTL:               pubsub.GossipSubFanoutTTL,
			PrunePeers:              pubsub.GossipSubPrunePeers,
			PruneBackoff:            pubsub.GossipSubPruneBackoff,
			UnsubscribeBackoff:      pubsub.GossipSubUnsubscribeBackoff,
			Connectors:              pubsub.GossipSubConnectors,
			MaxPendingConnections:   pubsub.GossipSubMaxPendingConnections,
			ConnectionTimeout:       pubsub.GossipSubConnectionTimeout,
			DirectConnectTicks:      pubsub.GossipSubDirectConnectTicks,
			DirectConnectInitialDelay: pubsub.GossipSubDirectConnectInitialDelay,
			OpportunisticGraftTicks: pubsub.GossipSubOpportunisticGraftTicks,
			OpportunisticGraftPeers: pubsub.GossipSubOpportunisticGraftPeers,
			MaxIHaveLength:          pubsub.GossipSubMaxIHaveLength,
			MaxIHaveMessages:        pubsub.GossipSubMaxIHaveMessages,
			IWantFollowupTime:       pubsub.GossipSubIWantFollowupTime,
		}),
	)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("create gossipsub: %w", err)
	}

	// Join topics
	snTopic, err := ps.Join(cfg.SnapshotTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join snapshot topic: %w", err)
	}

	atTopic, err := ps.Join(cfg.AttestationTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join attestation topic: %w", err)
	}

	ruTopic, err := ps.Join(cfg.RumorTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join rumor topic: %w", err)
	}

	node := &Node{
		Host:             h,
		PubSub:           ps,
		DHT:              kadDHT,
		snapshotTopic:    snTopic,
		attestationTopic: atTopic,
		rumorTopic:       ruTopic,
		cfg:              cfg,
	}

	// Start mDNS discovery for automatic peer finding on local network / Docker bridge.
	// Skipped when -disable-mdns is set, which forces all peer discovery through the
	// Kademlia DHT — useful for multi-host validation where mDNS cannot cross subnets.
	if cfg.DisableMdns {
		fmt.Println("mDNS: disabled (DHT-only peer discovery)")
	} else {
		mdnsService := mdns.NewMdnsService(h, "nakamoto-mesh", &mdnsNotifee{host: h, ctx: ctx})
		if err := mdnsService.Start(); err != nil {
			fmt.Printf("WARN: mDNS start failed: %v\n", err)
		} else {
			fmt.Println("mDNS: peer discovery active (service: nakamoto-mesh)")
		}
	}

	return node, nil
}

// mdnsNotifee handles mDNS peer discovery events.
type mdnsNotifee struct {
	host host.Host
	ctx  context.Context
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	if pi.ID == n.host.ID() {
		return // skip self
	}
	// Already connected? Skip.
	if n.host.Network().Connectedness(pi.ID) == libp2pnet.Connected {
		return
	}
	// Retry with backoff to handle simultaneous-connect race conditions
	go func() {
		for attempt := 0; attempt < 5; attempt++ {
			if attempt > 0 {
				// Stagger retries: 1s + random jitter up to 2s
				jitter := time.Duration(n.host.ID()[0]%20) * 100 * time.Millisecond
				time.Sleep(time.Duration(attempt)*time.Second + jitter)
			}
			if n.host.Network().Connectedness(pi.ID) == libp2pnet.Connected {
				return
			}
			ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
			err := n.host.Connect(ctx, pi)
			cancel()
			if err == nil {
				fmt.Printf("mDNS: connected to %s (attempt %d)\n", pi.ID.ShortString(), attempt+1)
				return
			}
			fmt.Printf("mDNS: connect attempt %d to %s failed: %v\n", attempt+1, pi.ID.ShortString(), err)
		}
		fmt.Printf("mDNS: giving up on %s after 5 attempts\n", pi.ID.ShortString())
	}()
}

// BootstrapDHT primes the Kademlia routing table from the seedlist (treated as
// bootstrap peers) and starts a background routing-table refresh, plus a
// rendezvous-based discovery loop that advertises this node and dials any peers
// found via DHT under the "tessellation-nakamoto" rendezvous string. Once
// running, peers added via -seedlist serve as DHT bootstrap entries; subsequent
// discovery is fully decentralized.
func (n *Node) BootstrapDHT(ctx context.Context) error {
	if n.DHT == nil {
		return fmt.Errorf("DHT not initialized")
	}
	if err := n.DHT.Bootstrap(ctx); err != nil {
		return fmt.Errorf("dht bootstrap: %w", err)
	}

	const rendezvous = "tessellation-nakamoto"
	routingDiscovery := routeddiscovery.NewRoutingDiscovery(n.DHT)

	// Advertise ourselves and periodically re-advertise so the routing table
	// stays warm even after partition heal.
	go func() {
		discutil.Advertise(ctx, routingDiscovery, rendezvous)
	}()

	// Discovery loop: periodically search for peers under the rendezvous and
	// dial any we don't already have a connection to.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		// Run one pass immediately, then on the ticker.
		for {
			peerCh, err := routingDiscovery.FindPeers(ctx, rendezvous)
			if err != nil {
				fmt.Printf("DHT: FindPeers error: %v\n", err)
			} else {
				for pi := range peerCh {
					if pi.ID == n.Host.ID() || pi.ID == "" {
						continue
					}
					if n.Host.Network().Connectedness(pi.ID) == libp2pnet.Connected {
						continue
					}
					dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
					if err := n.Host.Connect(dialCtx, pi); err != nil {
						fmt.Printf("DHT: connect to %s failed: %v\n", pi.ID.ShortString(), err)
					} else {
						fmt.Printf("DHT: connected to %s\n", pi.ID.ShortString())
					}
					cancel()
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	fmt.Println("DHT: kademlia routing active (server mode), rendezvous=tessellation-nakamoto")
	return nil
}

// ConnectSeedlist dials all seedlist peers.
func (n *Node) ConnectSeedlist(ctx context.Context) error {
	for _, addr := range n.cfg.Seedlist {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return fmt.Errorf("invalid seedlist addr %q: %w", addr, err)
		}
		// Try full p2p addr first (includes peer ID), fall back to addr-only discovery
		pi, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			// No peer ID in multiaddr — try connecting by address only.
			// This requires the remote peer to accept connections without prior ID knowledge.
			// We'll discover the peer ID during the handshake.
			fmt.Printf("INFO: seed %s has no peer ID, attempting direct dial\n", addr)
			pi = &peer.AddrInfo{Addrs: []multiaddr.Multiaddr{ma}}
		}
		if err := n.Host.Connect(ctx, *pi); err != nil {
			fmt.Printf("WARN: failed to connect to seed %s: %v\n", addr, err)
		}
	}
	return nil
}

// PublishSnapshot publishes raw bytes to the snapshot topic.
func (n *Node) PublishSnapshot(ctx context.Context, data []byte) error {
	err := n.snapshotTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("snapshot").Inc()
	}
	return err
}

// PublishAttestation publishes raw bytes to the attestation topic.
func (n *Node) PublishAttestation(ctx context.Context, data []byte) error {
	err := n.attestationTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("attestation").Inc()
	}
	return err
}

// PublishRumor publishes raw bytes to the rumor topic.
func (n *Node) PublishRumor(ctx context.Context, data []byte) error {
	err := n.rumorTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("rumor").Inc()
	}
	return err
}

// subscribeAndRelay creates a per-caller subscription on the given topic and
// relays incoming messages (excluding self-published) into the returned channel.
// The subscription is cancelled when ctx is done. The topicLabel is used for
// Prometheus metrics (e.g. "snapshot", "attestation", "rumor").
func (n *Node) subscribeAndRelay(ctx context.Context, topic *pubsub.Topic, bufSize int, topicLabel string) (<-chan []byte, error) {
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}
	ch := make(chan []byte, bufSize)
	go func() {
		defer close(ch)
		defer sub.Cancel()
		for {
			msg, err := sub.Next(ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == n.Host.ID() {
				continue
			}
			metrics.MessagesReceived.WithLabelValues(topicLabel).Inc()
			select {
			case ch <- msg.Data:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

// SnapshotMessages returns a channel of incoming snapshot messages.
// Each call creates its own GossipSub subscription so multiple consumers
// each receive every message independently.
func (n *Node) SnapshotMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.snapshotTopic, 64, "snapshot")
	if err != nil {
		fmt.Printf("ERROR: subscribe snapshot: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// AttestationMessages returns a channel of incoming attestation messages.
func (n *Node) AttestationMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.attestationTopic, 256, "attestation")
	if err != nil {
		fmt.Printf("ERROR: subscribe attestation: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// RumorMessages returns a channel of incoming rumor messages.
func (n *Node) RumorMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.rumorTopic, 1024, "rumor")
	if err != nil {
		fmt.Printf("ERROR: subscribe rumor: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// MeshPeerCount returns the number of peers in each topic mesh.
func (n *Node) MeshPeerCount() (snapshots, attestations, rumors int) {
	return len(n.snapshotTopic.ListPeers()),
		len(n.attestationTopic.ListPeers()),
		len(n.rumorTopic.ListPeers())
}

// Topics returns the GossipSub topic handles for metrics collection.
func (n *Node) Topics() metrics.TopicSet {
	return metrics.TopicSet{
		Snapshot:    n.snapshotTopic,
		Attestation: n.attestationTopic,
		Rumor:       n.rumorTopic,
	}
}

// Close shuts down the libp2p host. Per-caller subscriptions are cancelled
// by their own goroutines when the context is done.
func (n *Node) Close() error {
	return n.Host.Close()
}
