package outbox

import "crypto/sha256"

// msgIDFor is the internal sha256 truncation. Exposed via the exported
// MsgIDFor for code-organisation; broken out into its own file so the hash
// implementation can evolve independently of the outbox lifecycle code.
func msgIDFor(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	// Use all 32 bytes — sha256 output is already the same size as the spec
	// calls for. Allocate a fresh slice so callers can keep / mutate it.
	out := make([]byte, 32)
	copy(out, sum[:])
	return out
}
