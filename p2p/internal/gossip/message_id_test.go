package gossip

import (
	"bytes"
	"testing"

	pb "github.com/scasplte2/tessellation/p2p/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestContentMessageIDDomainsByTopic(t *testing.T) {
	payload := []byte("same-protobuf-wire-payload")

	allowSpendID := contentMessageID("/allow-spend", payload)
	tokenLockID := contentMessageID("/token-lock", payload)
	if allowSpendID == tokenLockID {
		t.Fatal("identical payloads on distinct topics must not share a GossipSub message ID")
	}
}

func TestContentMessageIDDeduplicatesSameTopicPayload(t *testing.T) {
	topic := "/metagraph-binary"
	payload := []byte("same-republished-payload")

	if contentMessageID(topic, payload) != contentMessageID(topic, payload) {
		t.Fatal("same-topic outbox republish must retain a stable GossipSub message ID")
	}
}

func TestRumorMessageIDIgnoresUnsignedHints(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	signedBytes := []byte(`{"value":"signed-rumor"}`)

	first := mustMarshalRumor(t, &pb.Rumor{
		SignedRumorBytes: signedBytes,
		ContentType:      "type-a",
		OriginId:         []byte("origin-a"),
	})
	second := mustMarshalRumor(t, &pb.Rumor{
		SignedRumorBytes: signedBytes,
		ContentType:      "type-b",
		OriginId:         []byte("origin-b"),
	})
	want := gossipMessageID(rumorTopic, rumorTopic, first)
	if want == gossipMessageID(rumorTopic, rumorTopic, nil) {
		t.Fatal("honestly marshalled rumor with all current fields used the invalid-envelope domain")
	}
	if got := gossipMessageID(rumorTopic, rumorTopic, second); got != want {
		t.Fatal("mutated unsigned hints changed the signed-rumor message ID")
	}
}

func TestRumorMessageIDChangesWithSignedBytes(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	first := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte("signed-a")})
	second := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte("signed-b")})

	if gossipMessageID(rumorTopic, rumorTopic, first) == gossipMessageID(rumorTopic, rumorTopic, second) {
		t.Fatal("distinct signed rumor bytes must not share a GossipSub message ID")
	}
}

func TestRumorMessageIDCollapsesNoncanonicalEnvelopes(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	valid := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte("signed")})
	duplicateA := appendBytesField(valid, 1, []byte("duplicate-a"))
	duplicateB := appendBytesField(valid, 1, []byte("duplicate-b"))
	withUnknown := appendBytesField(valid, 100, []byte("unsigned-extension"))
	truncated := []byte{0x0a, 0x05, 0x01}
	wrongWire := protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), 1)
	reordered := appendBytesField(nil, 2, []byte("hint"))
	reordered = appendBytesField(reordered, 1, []byte("signed"))
	overlongTag := append([]byte{0x8a, 0x00, 0x06}, []byte("signed")...)
	overlongLength := append([]byte{0x0a, 0x86, 0x00}, []byte("signed")...)

	invalidID := gossipMessageID(rumorTopic, rumorTopic, nil)
	for name, data := range map[string][]byte{
		"missing field":     nil,
		"duplicate field a": duplicateA,
		"duplicate field b": duplicateB,
		"unknown field":     withUnknown,
		"truncated field":   truncated,
		"wrong wire type":   wrongWire,
		"reordered fields":  reordered,
		"overlong tag":      overlongTag,
		"overlong length":   overlongLength,
	} {
		if got := gossipMessageID(rumorTopic, rumorTopic, data); got != invalidID {
			t.Fatalf("%s minted a distinct invalid-envelope message ID", name)
		}
	}
	if gossipMessageID(rumorTopic, rumorTopic, valid) == invalidID {
		t.Fatal("valid signed rumor bytes collided with the invalid-envelope domain")
	}
}

func TestRumorMessageIDRejectsSeenCachePoisonEnvelopes(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	valid := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte("signed")})
	invalidUTF8 := appendBytesField(valid, 2, []byte{0xff})
	oversizedField := appendBytesField(valid, protowire.MaxValidNumber+1, []byte("invalid-field"))
	malformedGroup := append(append([]byte(nil), valid...), protowire.AppendTag(nil, 4, protowire.StartGroupType)...)
	nestedHighFieldGroup := append(append([]byte(nil), valid...), protowire.AppendTag(nil, 4, protowire.StartGroupType)...)
	nestedHighFieldGroup = appendBytesField(nestedHighFieldGroup, protowire.MaxValidNumber+1, []byte("invalid-field"))
	nestedHighFieldGroup = protowire.AppendTag(nestedHighFieldGroup, 4, protowire.EndGroupType)

	var goDecoded pb.Rumor
	if err := proto.Unmarshal(nestedHighFieldGroup, &goDecoded); err != nil {
		t.Fatalf("nested-high-field fixture must pass Go decoding so the strict ID parser contains it: %v", err)
	}

	validID := gossipMessageID(rumorTopic, rumorTopic, valid)
	invalidID := gossipMessageID(rumorTopic, rumorTopic, nil)
	for name, data := range map[string][]byte{
		"invalid utf8 content type": invalidUTF8,
		"oversized field number":    oversizedField,
		"malformed group":           malformedGroup,
		"nested high field group":   nestedHighFieldGroup,
	} {
		if name != "nested high field group" {
			var decoded pb.Rumor
			if err := proto.Unmarshal(data, &decoded); err == nil {
				t.Fatalf("%s unexpectedly passed the Go protobuf decoder", name)
			}
		}
		if got := gossipMessageID(rumorTopic, rumorTopic, data); got != invalidID {
			t.Fatalf("%s did not use the invalid-envelope message ID", name)
		}
		if got := gossipMessageID(rumorTopic, rumorTopic, data); got == validID {
			t.Fatalf("%s could poison the legitimate rumor's seen-cache entry", name)
		}
	}
}

func TestRumorMessageIDDoesNotCanonicalizeNestedSignedJSON(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	compact := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte(`{"value":1}`)})
	spaced := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: []byte(`{ "value": 1 }`)})

	if gossipMessageID(rumorTopic, rumorTopic, compact) == gossipMessageID(rumorTopic, rumorTopic, spaced) {
		t.Fatal("message ID containment must not pretend to canonicalize nested signed JSON")
	}
}

var (
	extractedRumorBytes []byte
	extractedRumorOK    bool
)

func TestRumorEnvelopeSignedBytesDoesNotAllocate(t *testing.T) {
	want := []byte("signed-rumor")
	data := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: want, ContentType: "hint"})

	allocations := testing.AllocsPerRun(1000, func() {
		extractedRumorBytes, extractedRumorOK = rumorEnvelopeSignedBytes(data)
	})
	if allocations != 0 {
		t.Fatalf("rumor envelope field extraction allocated %f objects per run", allocations)
	}
	if !extractedRumorOK || string(extractedRumorBytes) != string(want) {
		t.Fatal("rumor envelope field extraction returned the wrong signed bytes")
	}
}

func FuzzRumorEnvelopeSignedBytesImpliesGoDecoderAcceptance(f *testing.F) {
	valid := mustMarshalRumor(f, &pb.Rumor{
		SignedRumorBytes: []byte("signed"),
		ContentType:      "valid-type",
		OriginId:         []byte("origin"),
	})
	f.Add(valid)
	f.Add(appendBytesField(valid, 2, []byte{0xff}))
	f.Add(appendBytesField(valid, protowire.MaxValidNumber+1, []byte("invalid-field")))
	f.Add(append(append([]byte(nil), valid...), protowire.AppendTag(nil, 4, protowire.StartGroupType)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		signedBytes, ok := rumorEnvelopeSignedBytes(data)
		if !ok {
			return
		}

		var decoded pb.Rumor
		if err := proto.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("message-ID parser accepted an envelope rejected downstream: %v", err)
		}
		if !bytes.Equal(signedBytes, decoded.GetSignedRumorBytes()) {
			t.Fatal("message-ID parser and downstream decoder selected different signed rumor bytes")
		}
	})
}

func TestRumorMessageIDContainmentIsScopedToRumorTopic(t *testing.T) {
	rumorTopic := "/tessellation/rumors/1.0.0"
	otherTopic := "/tessellation/other/1.0.0"
	signedBytes := []byte("signed")
	first := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: signedBytes, ContentType: "type-a"})
	second := mustMarshalRumor(t, &pb.Rumor{SignedRumorBytes: signedBytes, ContentType: "type-b"})

	if gossipMessageID(otherTopic, rumorTopic, first) == gossipMessageID(otherTopic, rumorTopic, second) {
		t.Fatal("non-rumor topics must retain full-wire message identity")
	}
}

func mustMarshalRumor(t testing.TB, rumor *pb.Rumor) []byte {
	t.Helper()
	data, err := proto.Marshal(rumor)
	if err != nil {
		t.Fatalf("marshal rumor: %v", err)
	}
	return data
}

func appendBytesField(data []byte, number protowire.Number, value []byte) []byte {
	result := append([]byte(nil), data...)
	result = protowire.AppendTag(result, number, protowire.BytesType)
	return protowire.AppendBytes(result, value)
}
