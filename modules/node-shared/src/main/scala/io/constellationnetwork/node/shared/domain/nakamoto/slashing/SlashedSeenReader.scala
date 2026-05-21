package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Typed read interface for the "already-slashed" MPT lookup used by [[SlashableEvidenceValidator]] step §4.1#7.
  *
  * '''Why a typed reader, not direct MPT access.''' S4a (this slice) defines the slashing schema + validator without the corresponding GSAM
  * ledger-effect write path. The actual MPT partition `slashings/<peer_id>/<metagraph_address>/<parent_hash>` will be added in S4c by the
  * accept-path agent (see `SLASHING-DESIGN.md` §5). Until then we expose this read-only surface so the validator can be wired against a
  * stub (no-op `false`) without rebuilding the MPT layer. S4c will land a concrete impl backed by `GlobalStateReader[F]` + a new
  * `slashings/...` key-prefix.
  *
  * '''Honest-node-byte-equivalence (slashing safety bar).''' Every honest node computes the same accept/reject decision from the same MPT
  * state. The `wasSlashed` predicate MUST be deterministic over the chosen branch view (caller picks pending vs finalized at construction
  * time, same convention as [[io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader]]).
  *
  * '''The MPT key shape''' (for S4c implementer reference, not load-bearing here): under the `slashings/` partition the composite key is
  * `keccak(peerId.value || metagraphAddress || parentHash.value)` — derived via the same `GlobalStateKey` helper pattern that
  * `historicalStakeSnapshotsKey` uses today, so the read-side MPT byte-equivalence proofs carry over.
  */
trait SlashedSeenReader[F[_]] {

  /** Returns `true` iff a slashing record exists for the `(peerId, metagraphAddress, parentHash)` triple. The triple is the doc's
    * load-bearing "double-slashing prevention" identity per `SLASHING-DESIGN.md` §4.1#7.
    */
  def wasSlashed(peerId: PeerId, metagraphAddress: Address, parentHash: Hash): F[Boolean]
}

object SlashedSeenReader {

  /** Always-false stub — useful for S4a tests and for nodes that haven't yet wired the S4c MPT partition. Returns the same answer on every
    * call, so byte-equivalence holds trivially.
    */
  def neverSlashed[F[_]](implicit F: cats.Applicative[F]): SlashedSeenReader[F] =
    (_: PeerId, _: Address, _: Hash) => F.pure(false)

  /** In-memory deterministic stub for negative-path tests (S4a). Honest-node-byte-equivalence holds because every node constructs the same
    * `seen` set from the same MPT bytes; this constructor takes the set directly so tests can pre-seed the "already slashed" triple.
    */
  def fromSet[F[_]](seen: Set[(PeerId, Address, Hash)])(implicit F: cats.Applicative[F]): SlashedSeenReader[F] =
    (peerId: PeerId, metagraphAddress: Address, parentHash: Hash) => F.pure(seen.contains((peerId, metagraphAddress, parentHash)))
}
