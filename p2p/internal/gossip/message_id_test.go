package gossip

import "testing"

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
