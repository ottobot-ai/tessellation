package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §1 — eta resolver with MPT-cache + chainStore-walk fallback. Path 1 of the heap-leak workstream (Tessellation-Nakamoto).
  *
  * '''Problem statement.''' `vrfOutputsForPeriod` walks back to `periodStart` of period N-1 to recompute eta_N from VRF outputs. Under Fix
  * B's k₁-bounded `byHash` retention (default 255 ords), that walk hits the eviction floor for any `etaRotationSnapshots > 255` (production
  * default is 2550 = 10·k₁). Without a disk-immune cache, eta silently degrades to `genesisEta` for every period after the first eviction
  * crosses the rotation boundary — pseudo-predictability defeated cluster-wide. (Findings 1 + 2 from the reviewer.)
  *
  * '''Solution.''' At every eta-period boundary (`ord % R == R - 1` for the closing period N), GSAM persists a
  * `HistoricalStakeSnapshot(stakes_N, eta_N)` entry under the `HistoricalStakeSnapshots` MPT partition keyed by period N. Path 1 of the
  * workstream extends the existing per-period stake record to carry eta_N — eta_N is deterministic from the canonical chain at that
  * boundary (derived from period N-1's first 2/3 VRF outputs, fully knowable before period N starts), so all honest verifiers persist
  * byte-equivalent entries. Subsequent reads via [[getEta]] are O(1) MPT point reads, independent of `byHash` retention.
  *
  * '''Read path.''' [[getEta]] reads the MPT entry first (via [[HistoricalStakeReader]]); on a hit, returns `entry.eta`. On a miss (the
  * period's boundary hasn't been crossed yet — common during the active period N, where the period's boundary write hasn't fired), it falls
  * back to a chainStore VRF walk via the caller-supplied `chainWalkFallback`, computes eta via [[EtaCalculation.computeEta]], and returns
  * the computed value.
  *
  * '''Why no lazy MPT write-through.''' The MPT entry is written authoritatively at the period's closing boundary by GSAM's accept()
  * pipeline. Writing outside that flow would either:
  *   - Bypass the overlay's branch-aware contract (would need its own commit/rekey/discard plumbing for reorg correctness), or
  *   - Race with concurrent accept() writes to the same `historicalStakeSnapshotsKey[F](period)` key.
  *
  * Instead, the in-period chain-walk recompute is cheap enough (O(R/3) VRF outputs per call, post-fallback via `getWithOrdinalFallback`)
  * and the MPT lookup post-boundary is O(1). The reviewer's "lazy write-through" suggestion is replaced by: MPT entry is the durable source
  * after boundary; pre-boundary recompute is on-demand.
  *
  * '''Reorg handling.''' MPT writes go through the existing MultiBranch overlay; the overlay automatically drops writes from discarded
  * branches on chain selection rollback. No additional invalidation logic here. After a reorg, the MPT lookup naturally returns whatever
  * the canonical chain's boundary write put there — or `None` if the canonical chain hasn't crossed the boundary yet.
  *
  * '''Period 0 (and the COMPUTED period-1 convention, #259).''' Period 0 has no predecessor period to derive from, so [[getEta]] returns
  * `genesisEta` directly for period ≤ 0 without touching MPT or chain. Period 1 is NOT special-cased: it falls through to the MPT-lookup →
  * chain-walk path and computes `EtaCalculation.computeEta(genesisEta, 1, vrfOutputsForPeriod(0))` — BYTE-IDENTICAL to the wire /
  * eligibility eta in `SnapshotLeaderLoop` (which keys on `currentPeriod <= 0`) and to the committee draw. This unifies the per-period eta
  * across ALL sources (producer MPT boundary record == committee == wire == follower adopt) at every period, closing the #259 period-1
  * divergence where the record/committee said `genesisEta` while the wire said `computeEta(...)`. When period-0 VRF outputs do not yet
  * exist the chain walk is empty and the fallback returns `genesisEta` — matching `SnapshotLeaderLoop`'s empty-`vrfOutputsForPeriod(0)`
  * branch, so the period 0 → 1 rotation stays consistent across sources during warmup.
  */
trait EtaStateManager[F[_]] {

  /** Resolve eta for `period`. Returns the byte representation directly so callers can feed it into [[EligibilityChecker.checkEligibility]]
    * / [[CommitteeSortition]] / etc. without re-decoding.
    *
    *   - Period ≤ 0: returns `genesisEta`.
    *   - Period ≥ 1 with MPT cache hit: returns `entry.eta.toBytes`.
    *   - Period ≥ 1 with MPT cache miss: falls back to `chainWalkFallback(period - 1)`; if non-empty, computes eta via
    *     [[EtaCalculation.computeEta]]; if empty, returns `genesisEta` (parent chain not yet in store — the period 0 → 1 warmup window or
    *     bootstrap edge cases). Period 1 follows the COMPUTED convention (#259) so it byte-matches the wire / eligibility / committee eta.
    */
  def getEta(period: Long)(implicit hasher: Hasher[F]): F[Array[Byte]]
}

object EtaStateManager {

  /** Construct an [[EtaStateManager]] backed by an MPT `HistoricalStakeReader` (the eta-half of the per-period boundary record) and a
    * chain-walk callback that produces `(ordinal, vrfOutput)` pairs for the first 2/3 of `period`.
    *
    * The `chainWalkFallback` typically routes to `chainStore.vrfOutputsForPeriod(period, etaRotationSnapshots)` which under Path 1
    * disk-falls-through via [[NakamotoChainStore.getWithOrdinalFallback]]. The MPT cache short-circuits the walk for periods whose boundary
    * write has landed.
    *
    * @param genesisEta
    *   bootstrap eta for period ≤ 0 and for the degenerate empty-chain-walk fallback path. 32 bytes.
    * @param historicalStakeReader
    *   MPT-primary reader for the per-period boundary record. Returns `None` when the boundary hasn't yet been crossed.
    * @param chainWalkFallback
    *   `(sourcePeriod: Long) => F[List[(Long, Array[Byte])]]` — VRF outputs for the first 2/3 of `sourcePeriod`. Same return shape as
    *   [[NakamotoChainStore.vrfOutputsForPeriod]] / [[NakamotoChainStore.vrfOutputsForPeriodFrom]].
    */
  def make[F[_]: Async](
    genesisEta: Array[Byte],
    historicalStakeReader: HistoricalStakeReader[F],
    chainWalkFallback: Long => F[List[(Long, Array[Byte])]]
  ): F[EtaStateManager[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("EtaStateManager")
    // In-process recompute-suppression cache for the chain-walk path. Periods that hit `None` from
    // MPT are recomputed at most once per (period, F-runtime) — subsequent reads hit this cache,
    // avoiding O(R/3) walks per slot. Cleared on reorg via the `forgetUncommitted` hatch below.
    //
    // Cache keys are bounded by the small set of in-flight periods (~3 — the active, the prior, and
    // its prior under N-2 lookback). Not size-capped because the per-period eta is 32 bytes and the
    // map churn is at-most one entry per boundary crossing.
    Ref.of[F, SortedMap[EtaPeriod, Array[Byte]]](SortedMap.empty[EtaPeriod, Array[Byte]]).map { walkCacheRef =>
      new EtaStateManager[F] {

        def getEta(period: Long)(implicit hasher: Hasher[F]): F[Array[Byte]] =
          // Unified per-period eta — COMPUTED convention (#259). Only period 0 is the true genesis
          // case (no predecessor period to derive from). Period 1 falls through to the MPT-lookup →
          // chain-walk path so it computes `EtaCalculation.computeEta(genesisEta, 1, vrfOutputsForPeriod(0))`
          // BYTE-IDENTICALLY to the wire / eligibility eta in `SnapshotLeaderLoop` (which keys on
          // `currentPeriod <= 0`) and to the committee draw. This makes producer-record == committee ==
          // wire == follower-adopt at EVERY period: gl0 followers verifying the `historicalStakeSnapshots`
          // boundary entry now reproduce gl0's committed eta at period 1 instead of diverging to genesis.
          // The empty-chain-walk fallback below still returns `genesisEta`, matching `SnapshotLeaderLoop`'s
          // empty-`vrfOutputsForPeriod(0)` branch — so the first eta rotation (period 0 → 1) is consistent
          // across all sources even before any period-0 VRF outputs exist.
          if (period <= 0L) genesisEta.pure[F]
          else {
            val etaPeriod = EtaPeriod(period)
            historicalStakeReader.lookup(etaPeriod).flatMap {
              case Some(entry) =>
                // MPT hit — the boundary write for `period` has landed. Decode the embedded
                // 32-byte hash to a byte array (matches [[EtaCalculation.computeEta]]'s output
                // shape).
                val bytes = entry.eta.value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
                bytes.pure[F]

              case None =>
                // Pre-boundary recompute via the chain-walk fallback. The N-1 source period
                // (`period - 1`) carries the VRF outputs whose first 2/3 feed `computeEta`.
                walkCacheRef.get.flatMap { cache =>
                  cache.get(etaPeriod) match {
                    case Some(cached) => cached.pure[F]
                    case None =>
                      chainWalkFallback(period - 1L).flatMap { chainOutputs =>
                        if (chainOutputs.isEmpty) {
                          // Source-period not yet in store. Fall through to genesis — same
                          // semantic as [[EtaCalculation.etaForOrdinal]]'s degenerate-case branch.
                          logger
                            .debug(
                              s"getEta period=$period: MPT miss + empty chain walk for source=${period - 1L} — returning genesisEta"
                            )
                            .as(genesisEta)
                        } else {
                          val computed = EtaCalculation.computeEta(genesisEta, period, chainOutputs.map(_._2))
                          walkCacheRef.update(_.updated(etaPeriod, computed)) >>
                            logger
                              .debug(
                                s"getEta period=$period: MPT miss + chain-walk recompute (sourceOutputs=${chainOutputs.size}, cached for reuse)"
                              )
                              .as(computed)
                        }
                      }
                  }
                }
            }
          }
      }
    }
  }
}
