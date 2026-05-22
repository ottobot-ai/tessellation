package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.HistoryLookup
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.Logger

/** Chain-store-backed implementation of `MetagraphParentOrdinalResolver.HistoryLookup`. Walks back through the gl0 chain store's
  * in-memory finalized snapshots, examining each one's `GlobalSnapshotInfo.lastStateChannelSnapshotHashes[mg]` for an entry matching the
  * incoming binary's `parentHash`.
  *
  * '''Why this exists (#202 v2).''' At 4-metagraph stress the gl0 GSI's `lastStateChannelSnapshotHashes` can lag the cluster-canonical
  * tip by ≥1 metagraph ordinal. The pre-v2 resolver fail-closed on every such mismatch — the forensic at HEAD f9073e474 measured
  * 6,654/6,654 attestation rejections + 9,772 buffered orphans + 0 admissions. The walk lets the resolver accept binaries whose parent
  * is any RECENTLY-OBSERVED metagraph tip (within a bounded depth), not strictly the current tip; the per-binary state-proof
  * verification downstream still catches divergence.
  *
  * '''Walk policy.'''
  *   - Start from `bestTip` and walk back through `parentHash` chain (same canonical-chain traversal `chainFromTip` uses).
  *   - At each `StoredSnapshot`, read `context.lastStateChannelSnapshotHashes(mg)`. If equal to the requested `scBinaryHash`, return
  *     `Some(mgOrdinal)` derived from `context.lastIncrementalCurrencySnapshots(mg)` (or `lastCurrencySnapshots(mg)` in the genesis-only
  *     window).
  *   - Cap the walk at `maxDepth` to bound CPU. First-match early-return so most calls return at depth 1 or below.
  *
  * '''Eviction survival.''' The chain store's in-memory `byHash` retention is bounded by `keepDepthBehindFinalized` (default k₁=255).
  * Beyond that, snapshots are evicted from memory and only their on-disk byte-encoded form remains. The disk path doesn't persist
  * `GlobalSnapshotInfo`, so a strict `keepDepthBehindFinalized`-bounded walk is the safe limit — the walk's `maxDepth` parameter should
  * be ≤ `keepDepthBehindFinalized`. The 200-default in HOCON sits comfortably inside the 255-default eviction window.
  *
  * '''Multi-metagraph isolation.''' We key on `(metagraphAddress, scBinaryHash)`. A binary from metagraph X whose `parentHash` happens
  * to equal a binary value-hash for metagraph Y (cryptographically improbable but theoretically possible under adversarial collisions)
  * will not be cross-resolved — every step explicitly checks `context.lastStateChannelSnapshotHashes(metagraphAddress)`, ignoring all
  * other metagraphs in the snapshot's GSI.
  */
object ChainStoreScHashHistoryLookup {

  /** Build a chain-store-backed history lookup.
    *
    *   - `chainStore` — provides `bestTip` + `get(hash)` for the in-memory parent walk.
    *   - `maxDepth` — hard cap on walk length. First-match early-returns; capped so pathological queries (unknown parent →
    *     walk-everything) can't burn unbounded CPU under gossip storm.
    */
  def make[F[_]: Async: Logger](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    maxDepth: Int
  ): HistoryLookup[F] = new HistoryLookup[F] {

    def lookup(metagraphAddress: Address, scBinaryHash: Hash): F[Option[Long]] =
      // Walk policy: start from bestTip, follow parentHash backward. Each hop checks if the snapshot's
      // GSI has `lastStateChannelSnapshotHashes(mg) == scBinaryHash`. First match wins; cap at maxDepth.
      chainStore.bestTip.flatMap {
        case None =>
          // No best tip yet — fresh / re-bootstrapping node. The resolver's `case None` branch already
          // calls this; return None to let the resolver fail-closed.
          Async[F].pure(Option.empty[Long])
        case Some(initialStored) =>
          walk(metagraphAddress, scBinaryHash, initialStored, maxDepth, 0)
      }

    /** Recursive backward walk. Tail-recursive in spirit (each hop is a flatMap), bounded by `maxDepth` to avoid pathological CPU
      * under gossip storms. Returns the first ordinal whose snapshot's GSI tip for `metagraphAddress` equals `scBinaryHash`.
      */
    private def walk(
      metagraphAddress: Address,
      scBinaryHash: Hash,
      current: NakamotoChainStore.StoredSnapshot,
      remaining: Int,
      depth: Int
    ): F[Option[Long]] =
      if (remaining <= 0) {
        Logger[F]
          .debug(
            s"📜 chain-store history walk: max depth $maxDepth reached for mg=$metagraphAddress " +
              s"parent=${scBinaryHash.value.take(12)}... — returning None"
          )
          .as(Option.empty[Long])
      } else {
        // Check this snapshot's GSI for a metagraph-tip equal to the requested parentHash.
        val gsi = current.context
        gsi.lastStateChannelSnapshotHashes.get(metagraphAddress) match {
          case Some(observedTip) if observedTip === scBinaryHash =>
            // Match. Recover the metagraph ordinal from the same GSI's currency-snapshot partitions.
            extractMetagraphOrdinal(metagraphAddress, gsi).flatMap {
              case Some(ord) => Async[F].pure(Some(ord))
              case None =>
                Logger[F]
                  .warn(
                    s"⚠️ chain-store history walk: matched mg=$metagraphAddress parent=${scBinaryHash.value.take(12)}... " +
                      s"in lastStateChannelSnapshotHashes but couldn't extract metagraph ordinal from GSI " +
                      s"at gl0-ord=${current.ordinal} — continuing walk"
                  )
                  .flatMap(_ => stepParent(metagraphAddress, scBinaryHash, current, remaining, depth))
            }
          case _ =>
            // No match (or this metagraph absent from the snapshot's GSI). Step to parent.
            stepParent(metagraphAddress, scBinaryHash, current, remaining, depth)
        }
      }

    /** Step to the parent snapshot via in-memory `chainStore.get`. Stop on parent-miss (parent evicted or genesis reached). */
    private def stepParent(
      metagraphAddress: Address,
      scBinaryHash: Hash,
      current: NakamotoChainStore.StoredSnapshot,
      remaining: Int,
      depth: Int
    ): F[Option[Long]] =
      chainStore.get(current.parentHash).flatMap {
        case Some(parent) => walk(metagraphAddress, scBinaryHash, parent, remaining - 1, depth + 1)
        case None         =>
          // Parent evicted from in-memory byHash (Fix B). The walk terminates; disk-backed GSI lookup
          // isn't available (disk doesn't persist GlobalSnapshotInfo — see NakamotoChainStore.scala
          // commit `3c009882a` placeholder GSI on getWithOrdinalFallback). Honest fail-closed; the
          // sender retries from the orphan buffer once the cluster admits an intermediate binary.
          Logger[F]
            .debug(
              s"📜 chain-store history walk: parent ${current.parentHash.value.take(12)}... evicted at depth $depth " +
                s"mg=$metagraphAddress parent-target=${scBinaryHash.value.take(12)}... — returning None"
            )
            .as(Option.empty[Long])
      }

    /** Recover the metagraph parent's ordinal from a GSI's currency-snapshot partitions. Same lookup the resolver's fast path does
      * via the overlay reader, just on a `GlobalSnapshotInfo` directly: try the Right side (incremental snapshots), fall back to the
      * Left side (genesis-only window). Returns `None` only when both partitions are empty for this metagraph — a state that
      * shouldn't appear post-genesis under normal operation; we WARN and fall through above.
      */
    private def extractMetagraphOrdinal(
      metagraphAddress: Address,
      gsi: io.constellationnetwork.schema.GlobalSnapshotInfo
    ): F[Option[Long]] =
      // `lastCurrencySnapshots` is a `SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]`.
      // Right = post-genesis incremental tip; Left = genesis-only window.
      Async[F].pure(
        gsi.lastCurrencySnapshots.get(metagraphAddress).flatMap {
          case Right((signedIncremental, _)) => Some(signedIncremental.value.ordinal.value.value)
          case Left(signedGenesis)           => Some(signedGenesis.value.ordinal.value.value)
        }
      )
  }
}
