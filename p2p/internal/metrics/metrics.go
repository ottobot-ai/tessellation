package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// MessagesPublished counts gossip messages published per topic.
	MessagesPublished = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sidecar_gossip_messages_published_total",
			Help: "Total gossip messages published by this sidecar.",
		},
		[]string{"topic"},
	)

	// MessagesReceived counts gossip messages received per topic.
	MessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sidecar_gossip_messages_received_total",
			Help: "Total gossip messages received by this sidecar.",
		},
		[]string{"topic"},
	)

	// MessagesDropped counts messages dropped because the per-subscriber relay
	// channel was full. A non-zero rate means the JVM consumer is slower than
	// inbound gossip; raise the corresponding buffer or investigate the slow
	// consumer.
	MessagesDropped = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sidecar_gossip_messages_dropped_total",
			Help: "Total gossip messages dropped due to full relay channel (slow JVM consumer).",
		},
		[]string{"topic"},
	)

	// MeshPeers tracks the number of peers in the GossipSub mesh per topic.
	MeshPeers = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sidecar_gossip_mesh_peers",
			Help: "Number of peers in the GossipSub mesh per topic.",
		},
		[]string{"topic"},
	)

	// ConnectedPeers tracks the total number of connected libp2p peers.
	ConnectedPeers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "sidecar_connected_peers_total",
			Help: "Total number of connected libp2p peers.",
		},
	)

	// DHTRoutingTableSize tracks the Kademlia DHT routing table size.
	DHTRoutingTableSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "sidecar_dht_routing_table_size",
			Help: "Number of peers in the Kademlia DHT routing table.",
		},
	)

	// OutboxSize tracks the number of entries the outbox is holding for
	// periodic re-gossip. Should drop to near-zero after each Phase-3
	// finality batch; a steady non-zero count means finality is stalled or
	// the JVM-side ConfirmFinalized ack path is broken. (#196)
	OutboxSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "sidecar_outbox_entries",
			Help: "Number of unconfirmed entries held in the outbox.",
		},
	)

	// OutboxRepublished counts entries the outbox re-published due to no
	// confirmation arriving within republish_interval. A sustained non-zero
	// rate indicates mesh degradation or JVM-side confirmation delay.
	OutboxRepublished = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sidecar_outbox_republished_total",
			Help: "Total outbox entries re-published by the periodic ticker.",
		},
	)

	// OutboxPrunedTTL counts entries the outbox dropped due to TTL expiry
	// without confirmation. Non-zero means finality didn't reach a topic's
	// message before its TTL — investigate the JVM finalize path.
	OutboxPrunedTTL = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "sidecar_outbox_pruned_ttl_total",
			Help: "Total outbox entries dropped without confirmation due to TTL expiry.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		MessagesPublished,
		MessagesReceived,
		MessagesDropped,
		MeshPeers,
		ConnectedPeers,
		DHTRoutingTableSize,
		OutboxSize,
		OutboxRepublished,
		OutboxPrunedTTL,
	)
}

// TopicSet holds the GossipSub topics for gauge collection.
type TopicSet struct {
	Snapshot             *pubsub.Topic
	Attestation          *pubsub.Topic
	Rumor                *pubsub.Topic
	MetagraphBinary      *pubsub.Topic
	MetagraphAttestation *pubsub.Topic
	AllowSpendBlock      *pubsub.Topic
	DAGBlock             *pubsub.Topic
	TokenLockBlock       *pubsub.Topic

	// ShardMeshPeers, when non-nil, returns a snapshot of per-shard topic
	// mesh sizes keyed by gauge label (e.g. "shard_checkpoint_3",
	// "shard_checkpoint_attestation_3"). A function rather than topic handles
	// because shard topics are joined lazily as shard ids become known
	// (task #40 instrumentation).
	ShardMeshPeers func() map[string]int
}

// StartGaugeUpdater launches a background goroutine that periodically updates
// the gauge metrics (mesh peers, connected peers, DHT routing table size).
// It stops when ctx is cancelled.
func StartGaugeUpdater(ctx context.Context, h host.Host, kadDHT *dht.IpfsDHT, topics TopicSet) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			// Mesh peers per topic
			MeshPeers.WithLabelValues("snapshot").Set(float64(len(topics.Snapshot.ListPeers())))
			MeshPeers.WithLabelValues("attestation").Set(float64(len(topics.Attestation.ListPeers())))
			MeshPeers.WithLabelValues("rumor").Set(float64(len(topics.Rumor.ListPeers())))
			if topics.MetagraphBinary != nil {
				MeshPeers.WithLabelValues("metagraph_binary").Set(float64(len(topics.MetagraphBinary.ListPeers())))
			}
			if topics.MetagraphAttestation != nil {
				MeshPeers.WithLabelValues("metagraph_attestation").Set(float64(len(topics.MetagraphAttestation.ListPeers())))
			}
			if topics.AllowSpendBlock != nil {
				MeshPeers.WithLabelValues("allow_spend_block").Set(float64(len(topics.AllowSpendBlock.ListPeers())))
			}
			if topics.DAGBlock != nil {
				MeshPeers.WithLabelValues("dag_block").Set(float64(len(topics.DAGBlock.ListPeers())))
			}
			if topics.TokenLockBlock != nil {
				MeshPeers.WithLabelValues("token_lock_block").Set(float64(len(topics.TokenLockBlock.ListPeers())))
			}
			// Per-shard checkpoint/attestation topic meshes (lazily joined,
			// hence the snapshot function). New shard labels appear as shards
			// are joined; a label persists at its last value if a shard topic
			// ever vanished, which cannot happen today (topics live for the
			// node lifetime).
			if topics.ShardMeshPeers != nil {
				for label, count := range topics.ShardMeshPeers() {
					MeshPeers.WithLabelValues(label).Set(float64(count))
				}
			}

			// Total connected peers
			ConnectedPeers.Set(float64(len(h.Network().Peers())))

			// DHT routing table size
			if kadDHT != nil {
				DHTRoutingTableSize.Set(float64(kadDHT.RoutingTable().Size()))
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
}

// ListenAndServe starts an HTTP server serving Prometheus metrics on addr.
// It blocks until the server is shut down. Use the returned *http.Server to
// shut down gracefully.
func ListenAndServe(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		fmt.Printf("Prometheus metrics: http://%s/metrics\n", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("WARN: metrics server: %v\n", err)
		}
	}()

	return srv
}
