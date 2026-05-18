package gossip

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	libp2pnet "github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	libp2prouting "github.com/libp2p/go-libp2p/core/routing"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	routeddiscovery "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discutil "github.com/libp2p/go-libp2p/p2p/discovery/util"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/multiformats/go-multiaddr"

	"github.com/scasplte2/tessellation/p2p/internal/config"
	"github.com/scasplte2/tessellation/p2p/internal/metrics"
)

// Node manages libp2p host and GossipSub topics.
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub
	DHT    *dht.IpfsDHT

	snapshotTopic             *pubsub.Topic
	attestationTopic          *pubsub.Topic
	rumorTopic                *pubsub.Topic
	metagraphBinaryTopic      *pubsub.Topic
	metagraphAttestationTopic *pubsub.Topic
	allowSpendBlockTopic      *pubsub.Topic

	cfg config.Config

	// reconnectMu guards reconnectCh. When the mesh health monitor recovers
	// from degradation, it closes reconnectCh (broadcasting to all Subscribe
	// handlers) and creates a fresh channel.
	reconnectMu sync.Mutex
	reconnectCh chan struct{}
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

	// Resource manager: explicit ceilings on connections/streams/memory to
	// protect against adversarial peers at planetary scale. Start from libp2p's
	// default scaling limits (sized from host CPU/RAM), layer on service-level
	// defaults (GossipSub, Kad-DHT, identify, ping), then apply as a FixedLimiter.
	// Without this, the host uses InfiniteLimits which means no protection
	// against connection/stream/memory exhaustion.
	scalingLimits := rcmgr.DefaultLimits
	libp2p.SetDefaultServiceLimits(&scalingLimits)
	rcmgrLimits := scalingLimits.AutoScale()
	resourceManager, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(rcmgrLimits))
	if err != nil {
		return nil, fmt.Errorf("create resource manager: %w", err)
	}

	// Build libp2p host options. When a pre-generated Ed25519 key is provided
	// via -key, the host identity is deterministic — compose-runner uses this to
	// pre-compute the peer ID and include it in the seedlist multiaddrs so that
	// DHT-only discovery (no mDNS) works: /dns4/sidecar-N/tcp/9500/p2p/<id>.
	hostOpts := []libp2p.Option{
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.ForceReachabilityPrivate(),
		libp2p.DefaultSecurity,
		libp2p.ResourceManager(resourceManager),
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

	// Create GossipSub with custom parameters, peer scoring, and peer exchange.
	// Peer scoring lets the mesh shed misbehaving peers automatically (invalid
	// messages, IP colocation, unfulfilled IHave promises). At planetary scale
	// with unknown peers this is essential — without it, any peer can flood
	// the mesh freely. Peer exchange (PX) lets pruned peers hand off known-good
	// alternatives on PRUNE, which stabilises the mesh during churn.
	ps, err := pubsub.NewGossipSub(ctx, h,
		pubsub.WithGossipSubParams(pubsub.GossipSubParams{
			D:                 cfg.MeshD,
			Dlo:               cfg.MeshDLo,
			Dhi:               cfg.MeshDHi,
			HeartbeatInterval: cfg.HeartbeatInterval,
			// Use defaults for everything else
			Dout:                      pubsub.GossipSubDout,
			HistoryLength:             pubsub.GossipSubHistoryLength,
			HistoryGossip:             pubsub.GossipSubHistoryGossip,
			Dlazy:                     pubsub.GossipSubDlazy,
			GossipFactor:              pubsub.GossipSubGossipFactor,
			GossipRetransmission:      pubsub.GossipSubGossipRetransmission,
			HeartbeatInitialDelay:     pubsub.GossipSubHeartbeatInitialDelay,
			FanoutTTL:                 pubsub.GossipSubFanoutTTL,
			PrunePeers:                pubsub.GossipSubPrunePeers,
			PruneBackoff:              pubsub.GossipSubPruneBackoff,
			UnsubscribeBackoff:        pubsub.GossipSubUnsubscribeBackoff,
			Connectors:                pubsub.GossipSubConnectors,
			MaxPendingConnections:     pubsub.GossipSubMaxPendingConnections,
			ConnectionTimeout:         pubsub.GossipSubConnectionTimeout,
			DirectConnectTicks:        pubsub.GossipSubDirectConnectTicks,
			DirectConnectInitialDelay: pubsub.GossipSubDirectConnectInitialDelay,
			OpportunisticGraftTicks:   pubsub.GossipSubOpportunisticGraftTicks,
			OpportunisticGraftPeers:   pubsub.GossipSubOpportunisticGraftPeers,
			MaxIHaveLength:            pubsub.GossipSubMaxIHaveLength,
			MaxIHaveMessages:          pubsub.GossipSubMaxIHaveMessages,
			IWantFollowupTime:         pubsub.GossipSubIWantFollowupTime,
		}),
		pubsub.WithPeerScore(buildPeerScoreParams(cfg), buildPeerScoreThresholds()),
		pubsub.WithPeerExchange(true),
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

	mbTopic, err := ps.Join(cfg.MetagraphBinaryTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join metagraph-binary topic: %w", err)
	}

	maTopic, err := ps.Join(cfg.MetagraphAttestationTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join metagraph-attestation topic: %w", err)
	}

	asbTopic, err := ps.Join(cfg.AllowSpendBlockTopic)
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("join allow-spend-block topic: %w", err)
	}

	node := &Node{
		Host:                      h,
		PubSub:                    ps,
		DHT:                       kadDHT,
		snapshotTopic:             snTopic,
		attestationTopic:          atTopic,
		rumorTopic:                ruTopic,
		metagraphBinaryTopic:      mbTopic,
		metagraphAttestationTopic: maTopic,
		allowSpendBlockTopic:      asbTopic,
		cfg:                       cfg,
		reconnectCh:               make(chan struct{}),
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

// PublishMetagraphBinary publishes raw bytes to the metagraph-binary topic.
// Carries Signed[StateChannelSnapshotBinary] serialized by the JVM; the
// sidecar treats the payload as opaque.
func (n *Node) PublishMetagraphBinary(ctx context.Context, data []byte) error {
	err := n.metagraphBinaryTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("metagraph_binary").Inc()
	}
	return err
}

// PublishMetagraphAttestation publishes raw bytes to the metagraph-attestation topic.
// Carries the per-metagraph committee VRF attestation (Slice S2); the sidecar
// treats the payload as opaque.
func (n *Node) PublishMetagraphAttestation(ctx context.Context, data []byte) error {
	err := n.metagraphAttestationTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("metagraph_attestation").Inc()
	}
	return err
}

// PublishAllowSpendBlock publishes raw bytes to the allow-spend-block topic.
// Carries Signed[AllowSpendBlock] serialized by the JVM JsonSerializer; the
// sidecar treats the payload as opaque. Replaces the single-peer HTTP POST
// path from Swap.sendBlockToL0 (task #196).
func (n *Node) PublishAllowSpendBlock(ctx context.Context, data []byte) error {
	err := n.allowSpendBlockTopic.Publish(ctx, data)
	if err == nil {
		metrics.MessagesPublished.WithLabelValues("allow_spend_block").Inc()
	}
	return err
}

// subscribeAndRelay creates a per-caller subscription on the given topic and
// relays incoming messages (excluding self-published) into the returned channel.
// The subscription is cancelled when ctx is done. The topicLabel is used for
// Prometheus metrics (e.g. "snapshot", "attestation", "rumor").
//
// When the relay channel is full (slow JVM consumer), messages are dropped
// and counted in sidecar_gossip_messages_dropped_total rather than blocking
// the relay goroutine. Blocking would backpressure libp2p's internal gossipsub
// queue and stall the whole validator, so we prefer a loud, observable drop.
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
			default:
				metrics.MessagesDropped.WithLabelValues(topicLabel).Inc()
			}
		}
	}()
	return ch, nil
}

// SnapshotMessages returns a channel of incoming snapshot messages.
// Each call creates its own GossipSub subscription so multiple consumers
// each receive every message independently.
func (n *Node) SnapshotMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.snapshotTopic, n.cfg.SnapshotBufferSize, "snapshot")
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
	ch, err := n.subscribeAndRelay(ctx, n.attestationTopic, n.cfg.AttestationBufferSize, "attestation")
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
	ch, err := n.subscribeAndRelay(ctx, n.rumorTopic, n.cfg.RumorBufferSize, "rumor")
	if err != nil {
		fmt.Printf("ERROR: subscribe rumor: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// MetagraphBinaryMessages returns a channel of incoming metagraph-binary messages.
func (n *Node) MetagraphBinaryMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.metagraphBinaryTopic, n.cfg.MetagraphBinaryBufferSize, "metagraph_binary")
	if err != nil {
		fmt.Printf("ERROR: subscribe metagraph_binary: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// MetagraphAttestationMessages returns a channel of incoming metagraph-attestation messages.
func (n *Node) MetagraphAttestationMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.metagraphAttestationTopic, n.cfg.MetagraphAttestationBufferSize, "metagraph_attestation")
	if err != nil {
		fmt.Printf("ERROR: subscribe metagraph_attestation: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// AllowSpendBlockMessages returns a channel of incoming allow-spend-block messages.
func (n *Node) AllowSpendBlockMessages(ctx context.Context) <-chan []byte {
	ch, err := n.subscribeAndRelay(ctx, n.allowSpendBlockTopic, n.cfg.AllowSpendBlockBufferSize, "allow_spend_block")
	if err != nil {
		fmt.Printf("ERROR: subscribe allow_spend_block: %v\n", err)
		empty := make(chan []byte)
		close(empty)
		return empty
	}
	return ch
}

// MeshPeerCount returns the number of peers in each topic mesh.
func (n *Node) MeshPeerCount() (snapshots, attestations, rumors, metagraphBinaries, metagraphAttestations, allowSpendBlocks int) {
	return len(n.snapshotTopic.ListPeers()),
		len(n.attestationTopic.ListPeers()),
		len(n.rumorTopic.ListPeers()),
		len(n.metagraphBinaryTopic.ListPeers()),
		len(n.metagraphAttestationTopic.ListPeers()),
		len(n.allowSpendBlockTopic.ListPeers())
}

// TriggerSubscriberReconnect broadcasts to all active Subscribe handlers that
// they should terminate, forcing JVM clients to re-establish their gRPC streams.
// Called by the mesh health monitor after recovering from degradation.
func (n *Node) TriggerSubscriberReconnect() {
	n.reconnectMu.Lock()
	close(n.reconnectCh)
	n.reconnectCh = make(chan struct{})
	n.reconnectMu.Unlock()
}

// ReconnectCh returns a channel that is closed when subscribers should reconnect.
func (n *Node) ReconnectCh() <-chan struct{} {
	n.reconnectMu.Lock()
	defer n.reconnectMu.Unlock()
	return n.reconnectCh
}

// StartMeshHealthMonitor runs a background loop that periodically checks
// GossipSub mesh health and reconnects to seedlist peers when degraded.
//
// After a network partition, libp2p connections drop and GossipSub PRUNEs
// peers from the mesh. The DHT discovery loop may not recover quickly enough
// because the routing table can drain during extended partitions. This monitor
// provides a reliable recovery path by:
//  1. Checking mesh peer counts every 30s
//  2. If any topic has 0 mesh peers for 2+ consecutive checks, reconnecting
//     to all seedlist peers (forcing libp2p connection re-establishment)
//  3. Logging mesh health for observability
func (n *Node) StartMeshHealthMonitor(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		emptyMeshStreak := 0

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}

			sn, at, ru, mb, ma, asb := n.MeshPeerCount()
			connectedPeers := len(n.Host.Network().Peers())

			// Degraded = no topic subscribers OR no connected peers at all.
			// topic.ListPeers() returns subscribed peers (not mesh-specific),
			// so connectedPeers==0 is the more reliable partition signal.
			if sn == 0 || at == 0 || connectedPeers == 0 {
				emptyMeshStreak++
				fmt.Printf("mesh-health: DEGRADED streak=%d snapshots=%d attestations=%d rumors=%d metagraph_binaries=%d metagraph_attestations=%d allow_spend_blocks=%d connected=%d\n",
					emptyMeshStreak, sn, at, ru, mb, ma, asb, connectedPeers)

				// After 2 consecutive degraded checks (~60s), force seedlist reconnection.
				if emptyMeshStreak >= 2 && len(n.cfg.Seedlist) > 0 {
					fmt.Println("mesh-health: reconnecting to seedlist peers...")
					for _, addr := range n.cfg.Seedlist {
						ma, err := multiaddr.NewMultiaddr(addr)
						if err != nil {
							continue
						}
						pi, err := peer.AddrInfoFromP2pAddr(ma)
						if err != nil {
							continue
						}
						if pi.ID == n.Host.ID() {
							continue
						}
						// Disconnect first to clear stale connection state, then reconnect.
						// This forces a fresh protocol negotiation including GossipSub
						// subscription exchange.
						if n.Host.Network().Connectedness(pi.ID) == libp2pnet.Connected {
							_ = n.Host.Network().ClosePeer(pi.ID)
							time.Sleep(500 * time.Millisecond)
						}
						dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						if err := n.Host.Connect(dialCtx, *pi); err != nil {
							fmt.Printf("mesh-health: reconnect to %s failed: %v\n", pi.ID.ShortString(), err)
						} else {
							fmt.Printf("mesh-health: reconnected to %s\n", pi.ID.ShortString())
						}
						cancel()
					}
				}
			} else {
				if emptyMeshStreak > 0 {
					fmt.Printf("mesh-health: RECOVERED snapshots=%d attestations=%d rumors=%d metagraph_binaries=%d metagraph_attestations=%d allow_spend_blocks=%d connected=%d (was degraded for %d checks)\n",
						sn, at, ru, mb, ma, asb, connectedPeers, emptyMeshStreak)
					// Force all active Subscribe handlers to terminate so JVM clients
					// reconnect and receive gossip from the now-healthy mesh.
					n.TriggerSubscriberReconnect()
					fmt.Println("mesh-health: triggered subscriber reconnect")
				}
				emptyMeshStreak = 0
			}
		}
	}()
}

// Topics returns the GossipSub topic handles for metrics collection.
func (n *Node) Topics() metrics.TopicSet {
	return metrics.TopicSet{
		Snapshot:             n.snapshotTopic,
		Attestation:          n.attestationTopic,
		Rumor:                n.rumorTopic,
		MetagraphBinary:      n.metagraphBinaryTopic,
		MetagraphAttestation: n.metagraphAttestationTopic,
		AllowSpendBlock:      n.allowSpendBlockTopic,
	}
}

// Close shuts down the libp2p host. Per-caller subscriptions are cancelled
// by their own goroutines when the context is done.
func (n *Node) Close() error {
	return n.Host.Close()
}

// buildPeerScoreParams returns GossipSub peer scoring parameters tuned for
// planetary-scale deployment with unknown, potentially adversarial peers.
//
// The shape is modelled on Ethereum consensus-layer (Lighthouse/Prysm) and
// Filecoin mainnet practice: slow reputation decay so honest peers build and
// keep good standing over hours, steep penalty for invalid-message publishing,
// IP colocation weight to mitigate single-ASN Sybils.
//
// Starting-point values are intentionally conservative — MeshMessageDeliveries
// weights are 0 because they require knowing the expected per-topic traffic
// rate, which we don't yet have in production. Ramp those up once we have
// telemetry on typical arrival rates.
func buildPeerScoreParams(cfg config.Config) *pubsub.PeerScoreParams {
	topics := map[string]*pubsub.TopicScoreParams{
		cfg.SnapshotTopic:             buildTopicScoreParams(cfg.HeartbeatInterval),
		cfg.AttestationTopic:          buildTopicScoreParams(cfg.HeartbeatInterval),
		cfg.RumorTopic:                buildTopicScoreParams(cfg.HeartbeatInterval),
		cfg.MetagraphBinaryTopic:      buildTopicScoreParams(cfg.HeartbeatInterval),
		cfg.MetagraphAttestationTopic: buildTopicScoreParams(cfg.HeartbeatInterval),
		cfg.AllowSpendBlockTopic:      buildTopicScoreParams(cfg.HeartbeatInterval),
	}
	return &pubsub.PeerScoreParams{
		Topics:                      topics,
		TopicScoreCap:               32.0,
		AppSpecificScore:            func(peer.ID) float64 { return 0 },
		AppSpecificWeight:           1.0,
		IPColocationFactorWeight:    -35.11,
		IPColocationFactorThreshold: 10,
		BehaviourPenaltyWeight:      -15.92,
		BehaviourPenaltyThreshold:   6.0,
		BehaviourPenaltyDecay:       pubsub.ScoreParameterDecay(time.Hour),
		DecayInterval:               cfg.HeartbeatInterval,
		DecayToZero:                 0.01,
		RetainScore:                 100 * time.Minute,
	}
}

// buildTopicScoreParams returns per-topic scoring defaults. One set serves all
// four topics today; split into per-topic tuning only when telemetry shows one
// topic needs different weighting.
func buildTopicScoreParams(heartbeat time.Duration) *pubsub.TopicScoreParams {
	return &pubsub.TopicScoreParams{
		TopicWeight:                    0.25,
		TimeInMeshWeight:               0.0027,
		TimeInMeshQuantum:              heartbeat,
		TimeInMeshCap:                  3600,
		FirstMessageDeliveriesWeight:   1.0,
		FirstMessageDeliveriesDecay:    pubsub.ScoreParameterDecay(10 * time.Minute),
		FirstMessageDeliveriesCap:      1000,
		MeshMessageDeliveriesWeight:    0, // disabled until we have rate telemetry
		InvalidMessageDeliveriesWeight: -99.0,
		InvalidMessageDeliveriesDecay:  pubsub.ScoreParameterDecay(50 * heartbeat),
	}
}

// buildPeerScoreThresholds returns the score bands that gate gossip behaviour.
// Numbers match Ethereum CL and Filecoin mainnet; see libp2p/specs pubsub/gossipsub/gossipsub-v1.1.md.
func buildPeerScoreThresholds() *pubsub.PeerScoreThresholds {
	return &pubsub.PeerScoreThresholds{
		GossipThreshold:             -500,  // below this, no IHave/IWant is sent/honoured
		PublishThreshold:            -1000, // below this, we won't publish to this peer
		GraylistThreshold:           -2500, // below this, all RPCs from peer are ignored
		AcceptPXThreshold:           100,   // PX from peers below this is ignored
		OpportunisticGraftThreshold: 5,     // grafted-in opportunistic peers must score above this
	}
}
