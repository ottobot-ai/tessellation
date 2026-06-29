package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.security.hash.Hash

/** WATCHTOWER fraud-proof POOL (W3a) — the node-local staging area that bridges the gossip dispute consumer
  * ([[io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto]] daemon `handleFraudProof`) and the gl0 leader's snapshot
  * producer (`GlobalSnapshotConsensusFunctions`).
  *
  * '''Role.''' A validated (locally-UPHELD) [[InvalidStateProofEvidence]] arriving over the `fraud-proof` gossip topic is [[offer]]ed here.
  * The gl0 leader [[peekAll]]s the current contents when it produces a snapshot and embeds them in the snapshot's `fraudProofs` consensus
  * field. EVERY node (leader / follower / peer) then re-validates each carried evidence DETERMINISTICALLY inside
  * `GlobalSnapshotAcceptanceManager.accept` (recomputing the honest root from the disputed checkpoint's OWN signed bytes) and applies the
  * slash identically — so this pool is a pure LIVENESS/selection aid (which disputes a leader proposes), NEVER a trust input. The
  * authoritative UPHELD verdict + slash are the consensus fold, not this pool.
  *
  * '''Why peek (not drain) on produce.''' A produced snapshot may not finalize (fork). Removing on produce would lose the dispute on a
  * losing fork. Instead the producer peeks; an entry stays until its checkpoint is durably slashed, after which the accept-path validator's
  * double-slash guard (`InvalidStateProofSlashedReader.wasSlashed` over the `Slashings` MPT partition) returns `AlreadySlashed` ⇒ the carried
  * evidence is embedded-but-inert (no double slash). Bounded capacity ages out stale/slashed entries so the pool cannot grow unbounded.
  *
  * '''Canonical `SortedSet`''' ordered by `InvalidStateProofEvidence`'s `(shardId, disputedCheckpointHash)` `Order` — the SAME identity the
  * snapshot `fraudProofs` field uses, so the embedded set is byte-deterministic and re-offers of the same dispute coalesce.
  *
  * '''Determinism note.''' The pool's contents are node-local and gossip-timing-dependent (different leaders may hold different disputes),
  * exactly like the per-shard checkpoint candidate selection. That is fine: the LEADER's embedded set rides into the signed snapshot, and the
  * follower/peer thread THAT embedded set (never their own pool) into accept(), so the byte-exact `recreatedArtifact === artifact` round-trip
  * holds — mirroring the `shardCheckpoints` split-safety invariant.
  */
trait WatchtowerFraudProofPool[F[_]] {

  /** Stage a locally-validated (UPHELD) dispute. Idempotent: re-offering an evidence with the same `(shardId, disputedCheckpointHash)`
    * identity coalesces (the `SortedSet` keys on exactly that). Bounded — when over capacity the smallest-ordered entry is evicted
    * (deterministic, canonical).
    */
  def offer(evidence: InvalidStateProofEvidence): F[Unit]

  /** The current staged disputes as a canonical `SortedSet[InvalidStateProofEvidence]` — what the gl0 leader embeds in the produced
    * snapshot's `fraudProofs` field. Read-only peek (does not drain).
    */
  def peekAll: F[SortedSet[InvalidStateProofEvidence]]

  /** Drop disputes whose `disputedCheckpointHash` is in the given set (e.g. once durably slashed) — best-effort housekeeping; correctness
    * does not depend on it (the double-slash guard makes a re-embedded slashed proof inert).
    */
  def remove(checkpointHashes: Set[Hash]): F[Unit]
}

object WatchtowerFraudProofPool {

  /** In-memory bounded pool. `capacity` caps the number of staged disputes (default 256 — disputes are rare; one per genuinely-wrong
    * full-quorum checkpoint within the challenge window). Eviction on overflow drops the smallest-ordered entry (canonical, deterministic)
    * so the pool is a bounded sliding set rather than an unbounded leak.
    */
  def make[F[_]: Sync](capacity: Int = 256): F[WatchtowerFraudProofPool[F]] =
    Ref.of[F, SortedSet[InvalidStateProofEvidence]](SortedSet.empty).map { ref =>
      new WatchtowerFraudProofPool[F] {
        def offer(evidence: InvalidStateProofEvidence): F[Unit] =
          ref.update { s =>
            val updated = s + evidence
            if (updated.size <= capacity) updated else updated - updated.head // evict smallest (canonical) — bounded sliding set
          }

        def peekAll: F[SortedSet[InvalidStateProofEvidence]] = ref.get

        def remove(checkpointHashes: Set[Hash]): F[Unit] =
          ref.update(_.filterNot(e => checkpointHashes.contains(e.fraudProof.disputedCheckpointHash)))
      }
    }

  /** No-op pool for paths that never produce gl0 fraud-proof snapshots (cl0 / dl1 / tests / `numShards = 1`): `offer` discards, `peekAll`
    * is always empty ⇒ no fraud proofs are ever embedded ⇒ byte-identical to the pre-watchtower path.
    */
  def noop[F[_]: Sync]: WatchtowerFraudProofPool[F] =
    new WatchtowerFraudProofPool[F] {
      def offer(evidence: InvalidStateProofEvidence): F[Unit] = Sync[F].unit
      def peekAll: F[SortedSet[InvalidStateProofEvidence]] = Sync[F].pure(SortedSet.empty)
      def remove(checkpointHashes: Set[Hash]): F[Unit] = Sync[F].unit
    }
}
