package gossip

import (
	"context"
	"fmt"
	"sync"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/scasplte2/tessellation/p2p/internal/config"
)

// Node manages libp2p host and GossipSub topics.
type Node struct {
	Host   host.Host
	PubSub *pubsub.PubSub

	snapshotTopic    *pubsub.Topic
	attestationTopic *pubsub.Topic
	snapshotSub      *pubsub.Subscription
	attestationSub   *pubsub.Subscription

	cfg config.Config
	mu  sync.RWMutex
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

	// Create libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrs(listenAddrs...),
	)
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

	// Subscribe to receive messages
	snSub, err := snTopic.Subscribe()
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("subscribe snapshot: %w", err)
	}

	atSub, err := atTopic.Subscribe()
	if err != nil {
		h.Close()
		return nil, fmt.Errorf("subscribe attestation: %w", err)
	}

	return &Node{
		Host:             h,
		PubSub:           ps,
		snapshotTopic:    snTopic,
		attestationTopic: atTopic,
		snapshotSub:      snSub,
		attestationSub:   atSub,
		cfg:              cfg,
	}, nil
}

// ConnectSeedlist dials all seedlist peers.
func (n *Node) ConnectSeedlist(ctx context.Context) error {
	for _, addr := range n.cfg.Seedlist {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			return fmt.Errorf("invalid seedlist addr %q: %w", addr, err)
		}
		pi, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			return fmt.Errorf("parse peer info from %q: %w", addr, err)
		}
		if err := n.Host.Connect(ctx, *pi); err != nil {
			// Log but don't fail — some seeds may be down
			fmt.Printf("WARN: failed to connect to seed %s: %v\n", addr, err)
		}
	}
	return nil
}

// PublishSnapshot publishes raw bytes to the snapshot topic.
func (n *Node) PublishSnapshot(ctx context.Context, data []byte) error {
	return n.snapshotTopic.Publish(ctx, data)
}

// PublishAttestation publishes raw bytes to the attestation topic.
func (n *Node) PublishAttestation(ctx context.Context, data []byte) error {
	return n.attestationTopic.Publish(ctx, data)
}

// SnapshotMessages returns a channel of incoming snapshot messages.
func (n *Node) SnapshotMessages(ctx context.Context) <-chan []byte {
	ch := make(chan []byte, 64)
	go func() {
		defer close(ch)
		for {
			msg, err := n.snapshotSub.Next(ctx)
			if err != nil {
				return
			}
			// Skip our own messages
			if msg.ReceivedFrom == n.Host.ID() {
				continue
			}
			select {
			case ch <- msg.Data:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// AttestationMessages returns a channel of incoming attestation messages.
func (n *Node) AttestationMessages(ctx context.Context) <-chan []byte {
	ch := make(chan []byte, 256)
	go func() {
		defer close(ch)
		for {
			msg, err := n.attestationSub.Next(ctx)
			if err != nil {
				return
			}
			if msg.ReceivedFrom == n.Host.ID() {
				continue
			}
			select {
			case ch <- msg.Data:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}

// MeshPeerCount returns the number of peers in each topic mesh.
func (n *Node) MeshPeerCount() (snapshots, attestations int) {
	return len(n.snapshotTopic.ListPeers()), len(n.attestationTopic.ListPeers())
}

// Close shuts down the libp2p host.
func (n *Node) Close() error {
	n.snapshotSub.Cancel()
	n.attestationSub.Cancel()
	return n.Host.Close()
}
