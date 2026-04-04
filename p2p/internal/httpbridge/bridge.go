// Package httpbridge provides an HTTP/JSON fallback interface for the sidecar.
// This is NOT the primary JVM↔sidecar interface — gRPC is.
// Enable with -enable-http flag for debugging or when gRPC is unavailable.
package httpbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/scasplte2/tessellation/p2p/internal/gossip"
	pb "github.com/scasplte2/tessellation/p2p/proto"
)

// Bridge provides a simple HTTP API alongside the gRPC service.
// This avoids adding ScalaPB/protobuf dependencies to the JVM build
// for the PoC. Production can use pure gRPC.

type Bridge struct {
	node      *gossip.Node
	startedAt time.Time
	server    *http.Server
}

func New(node *gossip.Node) *Bridge {
	return &Bridge{
		node:      node,
		startedAt: time.Now(),
	}
}

type SnapshotMsg struct {
	Hash         string `json:"hash"`
	Slot         int64  `json:"slot"`
	Ordinal      int64  `json:"ordinal"`
	ParentHash   string `json:"parentHash"`
	VrfProof     string `json:"vrfProof"`
	VrfPublicKey string `json:"vrfPublicKey"`
	Eta          string `json:"eta"`
	Payload      string `json:"payload"`
	ProducerId   string `json:"producerId"`
}

type AttestationMsg struct {
	TipHash    string `json:"tipHash"`
	TipSlot    int64  `json:"tipSlot"`
	TipOrdinal int64  `json:"tipOrdinal"`
	AttestedAt int64  `json:"attestedAt"`
	AttesterId string `json:"attesterId"`
	Signature  string `json:"signature"`
}

type PublishResult struct {
	Ok    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

type PeerCountResult struct {
	Total            int `json:"total"`
	MeshSnapshots    int `json:"meshSnapshots"`
	MeshAttestations int `json:"meshAttestations"`
}

type HealthResult struct {
	Healthy       bool  `json:"healthy"`
	UptimeSeconds int64 `json:"uptimeSeconds"`
	PeerCount     int   `json:"peerCount"`
}

func (b *Bridge) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/publish/snapshot", b.handlePublishSnapshot)
	mux.HandleFunc("/publish/attestation", b.handlePublishAttestation)
	mux.HandleFunc("/health", b.handleHealth)
	mux.HandleFunc("/peers", b.handlePeers)

	b.server = &http.Server{Addr: addr, Handler: mux}
	fmt.Printf("HTTP bridge listening on %s\n", addr)
	return b.server.ListenAndServe()
}

func (b *Bridge) Stop() {
	if b.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		b.server.Shutdown(ctx)
	}
}

func (b *Bridge) handlePublishSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var msg SnapshotMsg
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}

	snap := &pb.Snapshot{
		Hash:         []byte(msg.Hash),
		Slot:         msg.Slot,
		Ordinal:      msg.Ordinal,
		ParentHash:   []byte(msg.ParentHash),
		VrfProof:     []byte(msg.VrfProof),
		VrfPublicKey: []byte(msg.VrfPublicKey),
		Eta:          []byte(msg.Eta),
		Payload:      []byte(msg.Payload),
		ProducerId:   []byte(msg.ProducerId),
	}

	data, err := proto.Marshal(snap)
	if err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}

	if err := b.node.PublishSnapshot(r.Context(), data); err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}
	writeJSON(w, PublishResult{Ok: true})
}

func (b *Bridge) handlePublishAttestation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var msg AttestationMsg
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}

	att := &pb.TipAttestation{
		TipHash:    []byte(msg.TipHash),
		TipSlot:    msg.TipSlot,
		TipOrdinal: msg.TipOrdinal,
		AttestedAt: msg.AttestedAt,
		AttesterId: []byte(msg.AttesterId),
		Signature:  []byte(msg.Signature),
	}

	data, err := proto.Marshal(att)
	if err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}

	if err := b.node.PublishAttestation(r.Context(), data); err != nil {
		writeJSON(w, PublishResult{Ok: false, Error: err.Error()})
		return
	}
	writeJSON(w, PublishResult{Ok: true})
}

func (b *Bridge) handleHealth(w http.ResponseWriter, r *http.Request) {
	uptime := int64(time.Since(b.startedAt).Seconds())
	peerCount := len(b.node.Host.Network().Peers())
	writeJSON(w, HealthResult{
		Healthy:       true,
		UptimeSeconds: uptime,
		PeerCount:     peerCount,
	})
}

func (b *Bridge) handlePeers(w http.ResponseWriter, r *http.Request) {
	snPeers, atPeers := b.node.MeshPeerCount()
	total := len(b.node.Host.Network().Peers())
	writeJSON(w, PeerCountResult{
		Total:            total,
		MeshSnapshots:    snPeers,
		MeshAttestations: atPeers,
	})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
