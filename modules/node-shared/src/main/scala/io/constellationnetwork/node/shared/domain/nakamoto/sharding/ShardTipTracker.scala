package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardMetrics
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Per-shard attestation tracker — Slice 6 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.5.
  *
  * Per-shard analog of [[io.constellationnetwork.node.shared.domain.nakamoto.TipTracker]] (`TipTracker.scala`). Where the gl0 `TipTracker`
  * accumulates [[io.constellationnetwork.schema.nakamoto.TipAttestation]] entries from the gl0 operator set keyed by `tipHash`, this
  * tracker accumulates committee attestations on `ShardCheckpoint` hashes from members of a single shard's committee.
  *
  * '''Why a separate tracker per shard.''' Two independent reasons:
  *   - '''Scope.''' A shard committee is a sortitioned subset of the gl0 operator set (per the `sharding-direction-clarified` memory note —
  *     execution sharded, not data sharded). Mixing attestations from different shards' committees in one tracker would lose the
  *     per-committee accounting used by `T_count_shard`. The cluster-uniform configured `kQuorum` is compared directly.
  *   - '''Pruning floor.''' Each shard advances its `lastFinalizedOrdinal` independently. Pruning needs to know "what's below the shard's
  *     own floor" — a single mixed tracker would have to track per-(shardId) floors anyway, which is just this per-shard tracker
  *     constructor-fed a `ShardId` for diagnostic logging plus a per-shard `Map[Hash, Map[PeerId, CommitteeMemberSignature]]`.
  *
  * '''Self-exclusion.''' `attestationCountFor` defaults to excluding the local node (`selfPeerId`) from the returned count. This mirrors
  * the [[io.constellationnetwork.node.shared.domain.nakamoto.TCountTrigger]] self-exclusion rule (#133 self-exclusion + P-11b small-cluster
  * deadlock prevention; see `TCountTrigger`'s scaladoc and the `[[project-117-path-b-fork-recovery-deadlock]]` memory entry). The mechanism
  * is identical at the shard layer: a single committee member must not count its own attestation toward its own finality threshold,
  * otherwise it can self-finalize a divergent shard fork and lock itself out of canonical recovery. Callers wire `excludeSelf = false` only
  * for diagnostic / aggregate accounting paths where the self-count is informational.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh per-shard tracker. The gl0 `TipTracker` stays as-is for the universal global chain.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - No env reads. `selfPeerId` and `shardId` are constructor params; `kQuorum` lives on [[ShardFinalityTriggers]].
  *
  * '''Why `Map[Hash, Map[PeerId, CommitteeMemberSignature]]`.''' The shard committee membership is bounded (`K_S` ≈ small) and each
  * committee member produces at most one signature per checkpoint, so the inverted shape — keyed by `Hash`, value a per-signer `Map[PeerId,
  * CommitteeMemberSignature]` — is simple for the direct `kQuorum` count comparison and for the `pruneBelow` predicate (drop hashes whose
  * shard-ord falls below the floor). Retaining the FULL signature (not just the `PeerId`, the original Slice-6 shape) is what slice 14
  * needs: the gl0 consensus leader reads [[signaturesFor]] and splices the ≥`kQuorum` collected signatures back into a candidate
  * checkpoint's `committeeSignatures` before the deterministic `verifyEmbedded` replay gate. Signature enrichment never bypasses replay.
  * (The gl0 `TipTracker` is keyed by `PeerId` instead because there each peer's latest attestation supersedes its previous — P-11b NID; the
  * shard's one-sig-per-checkpoint invariant makes the `Hash`-keyed shape sound here.)
  */
trait ShardTipTracker[F[_]] {

  /** Which shard this tracker is scoped to. Set at construction; immutable. */
  def shardId: ShardId

  /** Record an attestation from a committee member on a particular shard checkpoint, retaining the member's full
    * [[CommitteeMemberSignature]] (not just the `peerId`) so the gl0 consensus leader can splice ≥`kQuorum` collected signatures back into
    * the checkpoint's `committeeSignatures` before the deterministic `verifyEmbedded` adopt gate (slice 14). Idempotent — a peer attesting
    * the same `checkpointHash` twice keeps the FIRST signature (it is deterministic over the canonical preimage, so a re-gossiped copy is
    * byte-identical and re-recording is a no-op on the second call).
    *
    * Note this records the (`checkpointHash`, `peerId`) pair; the shard-ordinal floor for pruning is supplied separately by [[pruneBelow]]
    * (the caller looks up the ordinal from the [[ShardChainStore]]). This avoids forcing every recorder to know the ordinal at recording
    * time — they only know the hash they're attesting.
    */
  def recordAttestation(checkpointHash: Hash, peerId: PeerId, signature: CommitteeMemberSignature): F[Unit]

  /** Count distinct attesters for a given checkpoint hash.
    *
    * @param checkpointHash
    *   the canonical hash of the shard checkpoint (`Hasher[F]` over `ShardCheckpointSigPreimage` per design doc §3.3)
    * @param excludeSelf
    *   when `true` (the default), the local node's `selfPeerId` is excluded from the count — matching the [[TCountTrigger]] self-exclusion
    *   rule (#133 + P-11b). When `false`, returns the raw size including any self-attestation, intended for diagnostic logging and
    *   post-mortem analysis where the inflated count is the desired view.
    */
  def attestationCountFor(checkpointHash: Hash, excludeSelf: Boolean = true): F[Int]

  /** The full set of committee-member signatures collected for `checkpointHash`, keyed by signer `PeerId`. This is the slice-14 read the
    * gl0 consensus leader uses to enrich a candidate checkpoint's `committeeSignatures` to ≥`kQuorum` before the deterministic
    * `verifyEmbedded` adopt gate. NO self-exclusion here (unlike [[attestationCountFor]]): the leader needs every distinct signer it has
    * observed to reach quorum, and dedup-by-`peerId` in the merge keeps the producer's already-embedded signature from being
    * double-counted. Empty map means no additional attestations have been collected for this hash.
    */
  def signaturesFor(checkpointHash: Hash): F[Map[PeerId, CommitteeMemberSignature]]

  /** Drop attestations for checkpoints below the supplied shard-ordinal floor. The caller supplies a `Hash => Option[ShardOrdinal]` lookup
    * (typically `ShardChainStore.getByHash(_).map(_.signed.value.shardOrdinal)`); hashes the lookup can't resolve are retained
    * conservatively (we don't drop attestations whose host checkpoint may simply have been evicted from the chain store first; another tick
    * will catch them once the store sees the eviction).
    *
    * Idempotent + monotone: a `pruneBelow(ord)` call with `ord` at-or-below the current floor (which we don't store explicitly — the floor
    * is implicit in the surviving keys) becomes a no-op for already-removed entries.
    */
  def pruneBelow(shardOrdinal: ShardOrdinal, lookupOrdinal: Hash => F[Option[ShardOrdinal]]): F[Unit]

  /** Diagnostic / observability — read the full attestation map (signer sets per checkpoint hash). Production callers should NOT route
    * control flow through this; use [[attestationCountFor]] (self-exclusion-consistent counting) or [[signaturesFor]] (the slice-14 signer
    * signatures). The signature payloads are dropped here — observability only needs the signer identities.
    */
  def allAttestations: F[Map[Hash, Set[PeerId]]]
}

object ShardTipTracker {

  /** Construct a per-shard tip tracker.
    *
    * @param shardId
    *   which shard this tracker is scoped to. Used for diagnostic logging; the tracker does not enforce that recorded attestations
    *   originate from this shard's committee (membership verification lives in the gossip/admission layer — see design doc §6.3).
    * @param selfPeerId
    *   the local node's PeerId — captured here so `attestationCountFor(excludeSelf = true)` (the default) can apply the #133/P-11b
    *   self-exclusion without re-threading the identity through every call site.
    */
  def make[F[_]: Async: Metrics](
    shardId: ShardId,
    selfPeerId: PeerId
  ): F[ShardTipTracker[F]] = {
    val outerShardId = shardId
    val outerSelfId = selfPeerId
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardTipTracker[$shardId]")

    Ref.of[F, Map[Hash, Map[PeerId, CommitteeMemberSignature]]](Map.empty).map { attestationsRef =>
      new ShardTipTracker[F] {

        val shardId: ShardId = outerShardId

        def recordAttestation(checkpointHash: Hash, peerId: PeerId, signature: CommitteeMemberSignature): F[Unit] =
          attestationsRef.modify { current =>
            val priorSigs = current.getOrElse(checkpointHash, Map.empty[PeerId, CommitteeMemberSignature])
            // Per scaladoc: idempotent — keep the FIRST signature seen for a (hash, peer) pair. The signature is deterministic over the
            // canonical preimage, so a re-gossiped copy is byte-identical and re-recording is a no-op. We only bump the metric on the
            // FIRST attestation from this (peer, hash) pair so the counter tracks distinct attestation events, not gossip-replay events.
            val newSigs = if (priorSigs.contains(peerId)) priorSigs else priorSigs.updated(peerId, signature)
            val isFirst = priorSigs.size != newSigs.size
            (current.updated(checkpointHash, newSigs), isFirst)
          }.flatMap {
            case true  => ShardMetrics.incCommitteeAttestation[F](outerShardId)
            case false => Async[F].unit
          }

        def attestationCountFor(checkpointHash: Hash, excludeSelf: Boolean = true): F[Int] =
          attestationsRef.get.map { current =>
            current.get(checkpointHash) match {
              case None => 0
              case Some(sigs) =>
                if (excludeSelf) sigs.keysIterator.filter(_ =!= outerSelfId).size
                else sigs.size
            }
          }

        def signaturesFor(checkpointHash: Hash): F[Map[PeerId, CommitteeMemberSignature]] =
          attestationsRef.get.map(_.getOrElse(checkpointHash, Map.empty[PeerId, CommitteeMemberSignature]))

        def pruneBelow(shardOrdinal: ShardOrdinal, lookupOrdinal: Hash => F[Option[ShardOrdinal]]): F[Unit] =
          attestationsRef.get.flatMap { current =>
            // Resolve the shard-ord of each hash via the supplied lookup. Hashes the lookup can't
            // resolve (returns None — typically because the host checkpoint was already evicted from
            // the chain store) are retained: we don't drop attestations for orphan / evicted hashes
            // unconditionally because the eviction signal from the chain store could be lagging the
            // tracker by a tick. The next prune call will catch them once we have evidence either
            // way; conservative retention is bounded by the tracker's overall pruning cadence.
            current.toList.traverse {
              case (h, sigs) => lookupOrdinal(h).map(maybeOrd => (h, sigs, maybeOrd))
            }.flatMap { resolved =>
              val (toDrop, toKeep) = resolved.partition {
                case (_, _, Some(ord)) => ord.value < shardOrdinal.value
                case (_, _, None)      => false // unresolved → retain
              }
              if (toDrop.isEmpty) cats.effect.kernel.Sync[F].unit
              else {
                val newMap = toKeep.map { case (h, sigs, _) => h -> sigs }.toMap
                attestationsRef.set(newMap) >>
                  logger.debug(
                    s"pruneBelow: shardOrdinal=${shardOrdinal.value} dropped=${toDrop.size} remaining=${toKeep.size}"
                  )
              }
            }
          }

        def allAttestations: F[Map[Hash, Set[PeerId]]] =
          attestationsRef.get.map(_.view.mapValues(_.keySet).toMap)
      }
    }
  }
}
