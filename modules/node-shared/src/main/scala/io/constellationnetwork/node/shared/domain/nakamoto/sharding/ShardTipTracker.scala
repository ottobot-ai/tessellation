package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Per-shard attestation tracker — Slice 6 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.5.
  *
  * Per-shard analog of [[io.constellationnetwork.node.shared.domain.nakamoto.TipTracker]] (`TipTracker.scala`). Where the gl0 `TipTracker`
  * accumulates [[io.constellationnetwork.schema.nakamoto.TipAttestation]] entries from the gl0 operator set keyed by `tipHash`, this tracker
  * accumulates committee attestations on `ShardCheckpoint` hashes from members of a single shard's committee.
  *
  * '''Why a separate tracker per shard.''' Two independent reasons:
  *   - '''Scope.''' A shard committee is a sortitioned subset of the gl0 operator set (per the `sharding-direction-clarified` memory note —
  *     execution sharded, not data sharded). Mixing attestations from different shards' committees in one tracker would lose the
  *     per-committee threshold accounting that's load-bearing for `T_count_shard` (`design doc §5.4` row 2). Each shard's `K_S`
  *     committee-size value drives its own `⌈2·K_S/3⌉` threshold.
  *   - '''Pruning floor.''' Each shard advances its `lastFinalizedOrdinal` independently. Pruning needs to know "what's below the shard's
  *     own floor" — a single mixed tracker would have to track per-(shardId) floors anyway, which is just this per-shard tracker
  *     constructor-fed a `ShardId` for diagnostic logging plus a per-shard `Map[Hash, Set[PeerId]]`.
  *
  * '''Self-exclusion.''' `attestationCountFor` defaults to excluding the local node (`selfPeerId`) from the returned count. This mirrors
  * the [[io.constellationnetwork.node.shared.domain.nakamoto.TCountTrigger]] self-exclusion rule (#133 self-exclusion + P-11b small-cluster
  * deadlock prevention; see `TCountTrigger`'s scaladoc and the `[[project-117-path-b-fork-recovery-deadlock]]` memory entry). The
  * mechanism is identical at the shard layer: a single committee member must not count its own attestation toward its own finality
  * threshold, otherwise it can self-finalize a divergent shard fork and lock itself out of canonical recovery. Callers wire `excludeSelf =
  * false` only for diagnostic / aggregate accounting paths where the self-count is informational.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh per-shard tracker. The gl0 `TipTracker` stays as-is for the universal global chain.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - No env reads. `selfPeerId` and `shardId` are constructor params; `K_S` (committee size for threshold math) lives on
  *     [[ShardFinalityTriggers]] (the per-shard composite), not here.
  *
  * '''Why `Map[Hash, Set[PeerId]]` and not the gl0 `Map[PeerId, attestation]`.''' The gl0 tracker is keyed by `PeerId` because each
  * peer's latest attestation supersedes its previous (P-11b NID property: receiver-side observability). The shard committee membership
  * is bounded (`K_S` ≈ small) and each committee member produces at most one signature per checkpoint, so the inverted shape — keyed by
  * `Hash`, value `Set[PeerId]` — is simpler for the `⌈2·K_S/3⌉` count comparison and for the `pruneBelow` predicate (drop hashes whose
  * shard-ord falls below the floor).
  */
trait ShardTipTracker[F[_]] {

  /** Which shard this tracker is scoped to. Set at construction; immutable. */
  def shardId: ShardId

  /** Record an attestation from a committee member on a particular shard checkpoint. Idempotent — a peer attesting the same
    * `checkpointHash` twice is a no-op on the second call.
    *
    * Note this records the (`checkpointHash`, `peerId`) pair; the shard-ordinal floor for pruning is supplied separately by
    * [[pruneBelow]] (the caller looks up the ordinal from the [[ShardChainStore]]). This avoids forcing every recorder to know the
    * ordinal at recording time — they only know the hash they're attesting.
    */
  def recordAttestation(checkpointHash: Hash, peerId: PeerId): F[Unit]

  /** Count distinct attesters for a given checkpoint hash.
    *
    * @param checkpointHash
    *   the canonical hash of the shard checkpoint (`Hasher[F]` over `ShardCheckpointSigPreimage` per design doc §3.3)
    * @param excludeSelf
    *   when `true` (the default), the local node's `selfPeerId` is excluded from the count — matching the [[TCountTrigger]]
    *   self-exclusion rule (#133 + P-11b). When `false`, returns the raw size including any self-attestation, intended for diagnostic
    *   logging and post-mortem analysis where the inflated count is the desired view.
    */
  def attestationCountFor(checkpointHash: Hash, excludeSelf: Boolean = true): F[Int]

  /** Drop attestations for checkpoints below the supplied shard-ordinal floor. The caller supplies a `Hash => Option[ShardOrdinal]`
    * lookup (typically `ShardChainStore.getByHash(_).map(_.signed.value.shardOrdinal)`); hashes the lookup can't resolve are retained
    * conservatively (we don't drop attestations whose host checkpoint may simply have been evicted from the chain store first; another
    * tick will catch them once the store sees the eviction).
    *
    * Idempotent + monotone: a `pruneBelow(ord)` call with `ord` at-or-below the current floor (which we don't store explicitly — the
    * floor is implicit in the surviving keys) becomes a no-op for already-removed entries.
    */
  def pruneBelow(shardOrdinal: ShardOrdinal, lookupOrdinal: Hash => F[Option[ShardOrdinal]]): F[Unit]

  /** Diagnostic / observability — read the full attestation map. Production callers should NOT route control flow through this; use
    * [[attestationCountFor]] which applies the self-exclusion semantics consistently.
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
  def make[F[_]: Async](
    shardId: ShardId,
    selfPeerId: PeerId
  ): F[ShardTipTracker[F]] = {
    val outerShardId = shardId
    val outerSelfId = selfPeerId
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardTipTracker[$shardId]")

    Ref.of[F, Map[Hash, Set[PeerId]]](Map.empty).map { attestationsRef =>
      new ShardTipTracker[F] {

        val shardId: ShardId = outerShardId

        def recordAttestation(checkpointHash: Hash, peerId: PeerId): F[Unit] =
          attestationsRef.update { current =>
            val priorSet = current.getOrElse(checkpointHash, Set.empty[PeerId])
            current.updated(checkpointHash, priorSet + peerId)
          }

        def attestationCountFor(checkpointHash: Hash, excludeSelf: Boolean = true): F[Int] =
          attestationsRef.get.map { current =>
            current.get(checkpointHash) match {
              case None => 0
              case Some(peers) =>
                if (excludeSelf) peers.iterator.filter(_ =!= outerSelfId).size
                else peers.size
            }
          }

        def pruneBelow(shardOrdinal: ShardOrdinal, lookupOrdinal: Hash => F[Option[ShardOrdinal]]): F[Unit] =
          attestationsRef.get.flatMap { current =>
            // Resolve the shard-ord of each hash via the supplied lookup. Hashes the lookup can't
            // resolve (returns None — typically because the host checkpoint was already evicted from
            // the chain store) are retained: we don't drop attestations for orphan / evicted hashes
            // unconditionally because the eviction signal from the chain store could be lagging the
            // tracker by a tick. The next prune call will catch them once we have evidence either
            // way; conservative retention is bounded by the tracker's overall pruning cadence.
            current.toList
              .traverse {
                case (h, peers) => lookupOrdinal(h).map(maybeOrd => (h, peers, maybeOrd))
              }
              .flatMap { resolved =>
                val (toDrop, toKeep) = resolved.partition {
                  case (_, _, Some(ord)) => ord.value < shardOrdinal.value
                  case (_, _, None)      => false // unresolved → retain
                }
                if (toDrop.isEmpty) cats.effect.kernel.Sync[F].unit
                else {
                  val newMap = toKeep.map { case (h, peers, _) => h -> peers }.toMap
                  attestationsRef.set(newMap) >>
                    logger.debug(
                      s"pruneBelow: shardOrdinal=${shardOrdinal.value} dropped=${toDrop.size} remaining=${toKeep.size}"
                    )
                }
              }
          }

        def allAttestations: F[Map[Hash, Set[PeerId]]] =
          attestationsRef.get
      }
    }
  }
}
