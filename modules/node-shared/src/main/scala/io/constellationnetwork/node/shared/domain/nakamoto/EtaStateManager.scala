package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §1 — eta resolver with MPT-cache + chainStore-walk fallback. Path 1 of the heap-leak workstream (Tessellation-Nakamoto).
  *
  * '''Problem statement.''' eta range collection walks back to `periodStart` of period N-1 to recompute eta_N from VRF outputs. Under Fix
  * B's k₁-bounded `byHash` retention (k₁ ords, per-env), that walk hits the eviction floor for any `etaRotationSnapshots > k₁` (always
  * true: R = round(3.1·k₁)). Without a disk-immune cache, eta silently degrades to `genesisEta` for every period after the first eviction
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
  * back to an exact chainStore VRF range via the caller-supplied `chainWalkFallback`. Only a range whose ancestry walk is proven complete
  * may feed [[EtaCalculation.computeEta]]. Missing ancestry and empty source ranges fail closed; neither is an eta value.
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
  * '''Bootstrap periods.''' Periods 0 and 1 have no settled predecessor range under the two-period lookback, so [[getEta]] derives them
  * only through [[EtaCalculation.bootstrapEta]] and never consults MPT or chain history. Every period N >= 2 is derived from a
  * proven-complete range for period N-1. This is the same split used by the GL0 producer and verifier.
  */
trait EtaStateManager[F[_]] {

  /** Resolve eta for `period`. Returns the byte representation directly so callers can feed it into [[EligibilityChecker.checkEligibility]]
    * / [[CommitteeSortition]] / etc. without re-decoding.
    *
    *   - Periods 0 and 1: return their period-specific bootstrap eta.
    *   - Period ≥ 2 with MPT cache hit: returns `entry.eta.toBytes`.
    *   - Period ≥ 2 with MPT cache miss: accepts only `EtaSourceRange.Complete(nonEmpty)` for period N-1. An incomplete or empty range
    *     raises [[EtaSourceUnavailable]] so callers defer instead of deriving a branch-dependent or attacker-chosen eta.
    */
  def getEta(period: Long)(implicit hasher: Hasher[F]): F[Array[Byte]]

  /** Resolve eta against the exact candidate parent. Unlike [[getEta]], this never consumes the receiver-current MPT entry: that entry may
    * belong to a sibling branch. The ancestry callback must prove a complete range from `parentHash`, and the memoization key includes the
    * hash so one branch's eta cannot be served while replaying another.
    */
  def getEtaAt(period: Long, parentHash: Hash)(implicit hasher: Hasher[F]): F[Array[Byte]]

  /** Drop the in-process chain-walk recompute-suppression cache (`walkCacheRef`). Track-3 S4: invoked on EVERY base-reverting path (the MPT
    * base revert `MptOverlay.revertToOrdinal` / finalize reorg-replace arms, and the follower resync-to-canonical) so a
    * subsequently-adopted CANONICAL branch does not read a stale eta that was computed by walking the REVERTED (pre-reorg) chain.
    *
    * There is deliberately NO parallel eta-revert: after the cache is dropped, [[getEta]] re-derives via the existing MPT-lookup →
    * `chainWalkFallback` path over the now-canonical chain — bootstrap-equivalent to a fresh peer that never held the stale entry. The
    * durable per-period eta record lives in the MPT (written at each period boundary) and is reverted WITH the base by the very same
    * `deleteAbove`/`readState`/re-fold machinery, so clearing only the in-process suppression cache is sufficient and safe. Idempotent:
    * clearing an already-empty cache is a no-op.
    */
  def forgetUncommitted: F[Unit]
}

object EtaStateManager {

  /** Completeness proof carried across the node-shared / dag-l0 module boundary. The range producer must prove it crossed the source
    * period's lower ordinal boundary while preserving every parent hash and ordinal link. A nonempty `Incomplete` prefix is not usable.
    */
  sealed trait EtaSourceRange {
    def outputs: List[(Long, Array[Byte])]
  }

  object EtaSourceRange {
    final case class Complete(outputs: List[(Long, Array[Byte])]) extends EtaSourceRange
    final case class Incomplete(outputs: List[(Long, Array[Byte])]) extends EtaSourceRange
  }

  final case class EtaSourceUnavailable(period: Long, sourcePeriod: Long, detail: String)
      extends RuntimeException(s"eta source unavailable for period=$period sourcePeriod=$sourcePeriod: $detail")

  /** Construct an [[EtaStateManager]] backed by an MPT `HistoricalStakeReader` (the eta-half of the per-period boundary record) and a
    * chain-walk callback that produces `(ordinal, vrfOutput)` pairs for the first 2/3 of `period`.
    *
    * The `chainWalkFallback` typically routes to `chainStore.vrfOutputRangeForPeriodFrom(period, etaRotationSnapshots, tipHash)` which
    * under Path 1 disk-falls-through via [[NakamotoChainStore.getWithOrdinalFallback]]. The MPT cache short-circuits the walk for periods
    * whose boundary write has landed.
    *
    * @param genesisEta
    *   bootstrap seed for periods 0 and 1. 32 bytes.
    * @param historicalStakeReader
    *   MPT-primary reader for the per-period boundary record. Returns `None` when the boundary hasn't yet been crossed.
    * @param chainWalkFallback
    *   `(sourcePeriod, optionalParentHash) => F[EtaSourceRange]` — ancestry result for the first 2/3 of `sourcePeriod`. `Some(hash)` must
    *   walk that exact parent; `None` is the explicitly ambient canonical lookup retained for unanchored callers.
    */
  def make[F[_]: Async](
    genesisEta: Array[Byte],
    historicalStakeReader: HistoricalStakeReader[F],
    chainWalkFallback: (Long, Option[Hash]) => F[EtaSourceRange]
  ): F[EtaStateManager[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("EtaStateManager")
    // In-process recompute-suppression cache for the chain-walk path. Periods that hit `None` from
    // MPT are recomputed at most once per (period, F-runtime) — subsequent reads hit this cache,
    // avoiding O(R/3) walks per slot. Cleared on reorg via the `forgetUncommitted` hatch below.
    //
    // Only the ambient canonical lookup is memoized. Exact-parent requests are deliberately not retained, so an attacker cannot turn
    // arbitrary candidate hashes into an unbounded cache; their correctness comes from the exact ancestry walk itself.
    Ref.of[F, Map[(EtaPeriod, Option[Hash]), Array[Byte]]](Map.empty).map { walkCacheRef =>
      new EtaStateManager[F] {

        // Track-3 S4: drop the recompute-suppression cache so post-revert `getEta` re-derives over the
        // canonical chain (bootstrap-equivalence). `:93` promised this hatch; here is the implementation.
        def forgetUncommitted: F[Unit] =
          walkCacheRef.set(Map.empty)

        private def resolveFromRange(period: Long, parentHash: Option[Hash]): F[Array[Byte]] = {
          val etaPeriod = EtaPeriod(period)
          val cacheKey = etaPeriod -> parentHash
          val cachedF = parentHash.fold(walkCacheRef.get.map(_.get(cacheKey)))(_ => none[Array[Byte]].pure[F])
          cachedF.flatMap {
            case Some(cached) => cached.pure[F]
            case None =>
              chainWalkFallback(period - 1L, parentHash).flatMap {
                case EtaSourceRange.Complete(chainOutputs) if chainOutputs.nonEmpty =>
                  val computed = EtaCalculation.computeEta(genesisEta, period, chainOutputs.map(_._2))
                  parentHash.fold(walkCacheRef.update(_.updated(cacheKey, computed)))(_ => Async[F].unit) >>
                    logger
                      .debug(
                        s"getEta period=$period parent=${parentHash.fold("best-tip")(_.value.take(12))}: " +
                          s"complete chain-walk recompute (sourceOutputs=${chainOutputs.size}, cached for reuse)"
                      )
                      .as(computed)

                case EtaSourceRange.Complete(_) =>
                  EtaSourceUnavailable(period, period - 1L, "complete range contained no VRF outputs")
                    .raiseError[F, Array[Byte]]

                case EtaSourceRange.Incomplete(chainOutputs) =>
                  EtaSourceUnavailable(
                    period,
                    period - 1L,
                    s"ancestry walk incomplete after ${chainOutputs.size} outputs"
                  ).raiseError[F, Array[Byte]]
              }
          }
        }

        private def resolve(period: Long, parentHash: Option[Hash])(implicit hasher: Hasher[F]): F[Array[Byte]] =
          // Cardano/Praos bootstrap (supersedes #259): periods 0 AND 1 are genesis-derivable via
          // `EtaCalculation.bootstrapEta` (= Blake2b(genesisEta ‖ period), no VRF-output dependency), so
          // every node computes them identically and the first eta rotation (period 0 → 1) cannot fork.
          // Period >= 2 folds period N-1's VRF outputs (MPT-lookup → chain-walk), BYTE-IDENTICALLY to the
          // wire / eligibility eta in `SnapshotLeaderLoop` / `NakamotoSyncDaemon` (which now key on
          // `currentPeriod <= 1`) and to the committee draw — producer-record == committee == wire ==
          // follower-adopt at EVERY period. The boundary writer materializes bootstrapEta for periods 0/1,
          // so the MPT-hit branch (reachable for period >= 2 only now) stays consistent.
          if (period <= 1L) EtaCalculation.bootstrapEta(genesisEta, period).pure[F]
          else
            parentHash match {
              case Some(_) =>
                // Exact-candidate replay must not read a receiver-current MPT entry that may belong to a sibling.
                resolveFromRange(period, parentHash)

              case None =>
                val etaPeriod = EtaPeriod(period)
                historicalStakeReader.lookup(etaPeriod).flatMap {
                  case Some(entry) =>
                    // MPT hit — the boundary write for `period` has landed. Decode the embedded
                    // 32-byte hash to a byte array (matches [[EtaCalculation.computeEta]]'s output
                    // shape).
                    val bytes = entry.eta.value.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
                    bytes.pure[F]

                  case None =>
                    // Receiver-current canonical lookup used only by callers that do not yet carry an exact anchor.
                    resolveFromRange(period, none)
                }
            }

        def getEta(period: Long)(implicit hasher: Hasher[F]): F[Array[Byte]] = resolve(period, none)

        def getEtaAt(period: Long, parentHash: Hash)(implicit hasher: Hasher[F]): F[Array[Byte]] =
          resolve(period, parentHash.some)
      }
    }
  }
}
