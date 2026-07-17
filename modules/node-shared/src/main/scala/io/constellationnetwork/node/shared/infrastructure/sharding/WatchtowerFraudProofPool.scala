package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.snapshot.finality.CanonicalLineageRevision
import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.security.hash.Hash

/** WATCHTOWER fraud-proof POOL (W3a) — the node-local staging area that bridges the gossip dispute consumer
  * ([[io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto]] daemon `handleFraudProof`) and the gl0 leader's snapshot
  * producer (`GlobalSnapshotConsensusFunctions`).
  *
  * '''Role.''' A validated (locally-UPHELD) [[InvalidStateProofEvidence]] arriving over the `fraud-proof` gossip topic is [[offer]]ed here,
  * tagged with the process-local GL0 lineage generation that bracketed its lookup and replay. [[peekAll]] prunes any tag not equal to the
  * current generation before proposal selection. The tag is never serialized, signed, rooted, or used by the consensus validator. The GL0
  * leader [[peekAll]]s the current contents when it produces a snapshot and embeds them in the snapshot's `fraudProofs` consensus field.
  * The target requires every node to revalidate each carried proof from exact proposal-parent history before applying one identical rooted
  * result. That target is activation-blocked: unavailable retained history currently degrades to no-slash and local slash configuration can
  * affect rooted state. This pool is only a local selection aid and never a trust input or evidence that universal adjudication is sound.
  *
  * '''Why peek (not drain) on produce.''' A produced snapshot may lose a fork, so produce does not remove its disputes. While the local
  * lineage remains current, an entry persists until lineage pruning, explicit [[remove]], or deterministic capacity eviction; there is no
  * production durable-slash removal caller or time-based aging today. The accept-path double-slash guard makes an already-slashed proof
  * inert, but it can be proposed repeatedly until one of those local removal conditions occurs.
  *
  * '''Canonical `SortedSet`''' ordered by `InvalidStateProofEvidence`'s `(shardId, disputedCheckpointHash)` `Order` — the SAME identity the
  * snapshot `fraudProofs` field uses, so the embedded set is byte-deterministic and re-offers of the same dispute coalesce.
  *
  * '''Determinism note.''' The pool's contents are node-local and gossip-timing-dependent (different leaders may hold different disputes),
  * exactly like the per-shard checkpoint candidate selection. That is fine: the LEADER's embedded set rides into the signed snapshot, and
  * the follower/peer thread THAT embedded set (never their own pool) into accept(), so the byte-exact `recreatedArtifact === artifact`
  * round-trip holds — mirroring the `shardCheckpoints` split-safety invariant.
  */
trait WatchtowerFraudProofPool[F[_]] {

  /** Stage a locally-validated (UPHELD) dispute. Idempotent: re-offering an evidence with the same `(shardId, disputedCheckpointHash)`
    * identity coalesces (the `SortedSet` keys on exactly that). Bounded — when over capacity the smallest-ordered entry is evicted
    * (deterministic, canonical). Returns false when the current local lineage is absent or no longer equals `validatedAt`.
    *
    * The lineage effect read and subsequent Ref update are not one chain-store transaction. This is discard-only containment; exact Phase-2
    * lease `commitIfCurrent` remains required to close the final check-to-act race.
    */
  def offer(evidence: InvalidStateProofEvidence, validatedAt: CanonicalLineageRevision): F[Boolean]

  /** The current staged disputes as a canonical `SortedSet[InvalidStateProofEvidence]` — what the gl0 leader embeds in the produced
    * snapshot's `fraudProofs` field. Entries from a replaced or unavailable generation are atomically pruned before the set is returned;
    * matching entries are not drained. The returned set is not protected by a chain-store lease, so replacement can still race this read
    * and later proposal construction.
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
  def make[F[_]: Sync](
    localGlobalLineageRevision: F[Option[CanonicalLineageRevision]],
    capacity: Int = 256
  ): F[WatchtowerFraudProofPool[F]] =
    Ref.of[F, SortedMap[InvalidStateProofEvidence, CanonicalLineageRevision]](SortedMap.empty).map { ref =>
      new WatchtowerFraudProofPool[F] {

        private def readLineage: F[Option[CanonicalLineageRevision]] =
          localGlobalLineageRevision.handleError(_ => None)

        def offer(evidence: InvalidStateProofEvidence, validatedAt: CanonicalLineageRevision): F[Boolean] =
          readLineage.flatMap {
            case Some(current) if current == validatedAt =>
              ref.update { staged =>
                // Observing one current generation makes every older entry permanently inert, including after an A->B->A hash cycle.
                // `CanonicalLineageRevision` advances on replacement/reconstruction and remains stable on descendant extension.
                val currentOnly = staged.filter { case (_, revision) => revision == current }
                val updated = currentOnly.updated(evidence, current)
                if (updated.size <= capacity) updated else updated - updated.head._1
              }.as(true)
            case _ => Sync[F].pure(false)
          }

        def peekAll: F[SortedSet[InvalidStateProofEvidence]] =
          readLineage.flatMap { current =>
            ref.modify { staged =>
              // Prune, do not merely filter the returned view. Once a replacement/absence is observed, old entries cannot revive under ABA.
              val currentOnly = current.fold(SortedMap.empty[InvalidStateProofEvidence, CanonicalLineageRevision]) { revision =>
                staged.filter { case (_, stagedAt) => stagedAt == revision }
              }
              (currentOnly, SortedSet.from(currentOnly.keys))
            }
          }

        def remove(checkpointHashes: Set[Hash]): F[Unit] =
          ref.update(_.filterNot { case (evidence, _) => checkpointHashes.contains(evidence.fraudProof.disputedCheckpointHash) })
      }
    }

  /** No-op pool for paths that never produce gl0 fraud-proof snapshots (cl0 / dl1 / tests / `numShards = 1`): `offer` discards, `peekAll`
    * is always empty ⇒ no fraud proofs are ever embedded ⇒ byte-identical to the pre-watchtower path.
    */
  def noop[F[_]: Sync]: WatchtowerFraudProofPool[F] =
    new WatchtowerFraudProofPool[F] {
      def offer(evidence: InvalidStateProofEvidence, validatedAt: CanonicalLineageRevision): F[Boolean] = Sync[F].pure(false)
      def peekAll: F[SortedSet[InvalidStateProofEvidence]] = Sync[F].pure(SortedSet.empty)
      def remove(checkpointHashes: Set[Hash]): F[Unit] = Sync[F].unit
    }
}
