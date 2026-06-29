package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash

/** Double-slash MPT guard for the WATCHTOWER invalid-state-proof tier — the analog of [[SlashedSeenReader]] one layer up, keyed on the
  * checkpoint identity `(shardId, disputedCheckpointHash)` instead of the metagraph-equivocation triple.
  *
  * '''Why a typed reader, not direct MPT access.''' Keeps the verdict ([[InvalidStateProofValidator]]) testable against a stub before the
  * GSAM `slashings/` MPT partition is wired, exactly as `SlashedSeenReader` does for [[SlashableEvidenceValidator]]. The concrete impl reads
  * the `slashings` partition (fieldId 33) written by the GSAM accept path when an invalid-state-proof is upheld.
  *
  * '''Honest-node byte-equivalence (slashing safety bar).''' `wasSlashed` MUST be deterministic over the chosen branch view — every honest
  * node reading the same finalized MPT bytes returns the same answer, so the verdict is cluster-uniform.
  */
trait InvalidStateProofSlashedReader[F[_]] {

  /** Returns `true` iff a slash record already exists for `(shardId, disputedCheckpointHash)` — the load-bearing double-slash key (only the
    * first upheld invalid-state-proof for a given wrong checkpoint slashes; subsequent submissions are rejected).
    */
  def wasSlashed(shardId: ShardId, disputedCheckpointHash: Hash): F[Boolean]
}

object InvalidStateProofSlashedReader {

  /** Always-false stub — for the validator unit tests and for nodes that have not yet wired the MPT partition. Byte-equivalent trivially
    * (same answer on every call).
    */
  def neverSlashed[F[_]](implicit F: cats.Applicative[F]): InvalidStateProofSlashedReader[F] =
    (_: ShardId, _: Hash) => F.pure(false)

  /** In-memory deterministic stub for negative-path tests — every node builds the same `seen` set from the same MPT bytes. */
  def fromSet[F[_]](seen: Set[(ShardId, Hash)])(implicit F: cats.Applicative[F]): InvalidStateProofSlashedReader[F] =
    (shardId: ShardId, disputedCheckpointHash: Hash) => F.pure(seen.contains((shardId, disputedCheckpointHash)))
}
