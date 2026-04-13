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
)

func init() {
	prometheus.MustRegister(
		MessagesPublished,
		MessagesReceived,
		MeshPeers,
		ConnectedPeers,
		DHTRoutingTableSize,
	)
}

// TopicSet holds the GossipSub topics for gauge collection.
type TopicSet struct {
	Snapshot    *pubsub.Topic
	Attestation *pubsub.Topic
	Rumor       *pubsub.Topic
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
