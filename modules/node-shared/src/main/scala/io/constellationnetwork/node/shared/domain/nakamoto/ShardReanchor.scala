package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptyList
import cats.syntax.eq._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

/**
 * SINGLE source of truth for "which binary in a committee checkpoint window continues gl0's per-metagraph SC tip".
 * Called byte-identically from BOTH the embed-selection (`GlobalSnapshotConsensusFunctions` `pick`) and the
 * adopt-guard (`GlobalSnapshotAcceptanceManager.adoptShardCheckpoints`) so the leader, every follower, and every
 * gl0 peer reach the same embed/adopt/defer decision — the split-safety invariant (no node-local reads).
 *
 * Background — the orphaned-tip freeze (2026-06-29): the trim-aware anchoring (09ace5227) heals a post-reorg window
 * that OVERLAPS gl0's adopted prefix (tip still ON the linear ml0 chain). But a SAME-ORDINAL slot-tiebreak shard reorg
 * can ORPHAN the very binary at gl0's committed SC tip — the tip then lies on a DEAD branch, no canonical window binary
 * ever has `lastSnapshotHash === tip`, the old guard returns `idx < 0` ⇒ DEFER FOREVER, and the per-MG mirror
 * (`lastCurrencySnapshots`) freezes while gl0 + ml0 keep advancing. [[Reanchor]] heals that: when the tip is orphaned
 * but the window is the genesis-rooted canonical lineage reaching past the tip's ordinal, adopt the canonical suffix
 * from the binary after the dead tip's ordinal. The committee already chain-link-validated the window; gl0's per-MG
 * root verify (`deriveAdoptedCurrencyState`, which commits the metagraph's authoritative CUMULATIVE state
 * base-independently and requires `recomputed === attestedRoot`) is the safety backstop, so a wrong reanchor index
 * cannot inject state — it DROPS. Pure function of (window bytes, prior GSI).
 */
object ShardReanchor {

  sealed trait Decision

  /** A window binary's parent === gl0's tip — adopt the suffix from `idx` (existing trim-aware path). */
  final case class Continue(idx: Int) extends Decision

  /** gl0's tip was orphaned by a reorg — adopt the canonical suffix from `idx` (the binary past the dead tip). */
  final case class Reanchor(idx: Int) extends Decision

  /** No continuation AND not a reanchorable orphan (a true chain hole) — defer; the leader re-offers an ancestor. */
  case object Defer extends Decision

  /**
   * gl0's committed mirror-tip metagraph ordinal for `mg`, recovered PURELY from the prior GSI currency mirror
   * (no opaque-`content` decode — `StateChannelSnapshotBinary` carries no ordinal). `None` for an unseeded mg.
   */
  def tipOrdinalFor(
    priorLastCurrencySnapshots: SortedMap[
      Address,
      Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]
    ],
    mg: Address
  ): Option[Long] =
    priorLastCurrencySnapshots
      .get(mg)
      .map(_.fold(_.value.ordinal.value.value, _._1.value.ordinal.value.value))

  /**
   * Classify a checkpoint window `nel` (oldest-first) for one metagraph against gl0's committed SC tip.
   *
   *  - `Continue(idx)`: a binary continues `tipHash` (parent match) — the trim-aware suffix from `idx`.
   *  - `Reanchor(idx)`: `tipHash` is continued by NO binary (orphaned), but the window is genesis-rooted (head
   *    parent === `Hash.empty`) and reaches at-or-past `tipOrdinal`. Because the committee chain-link-validated the
   *    window, a genesis-rooted window's binary index EQUALS its metagraph ordinal, so the canonical successor of the
   *    dead tip is at index `tipOrdinal + 1` (clamped to the window head's last index for the lateral-only case where
   *    the reorg winner exists at `tipOrdinal` but its successor has not landed yet).
   *  - `Defer`: otherwise — a true chain hole (window not genesis-rooted, or doesn't reach `tipOrdinal`). Never jump
   *    a gap.
   *
   * Genesis (`tipHash === Hash.empty`, `tipOrdinal == 0`) always classifies `Continue` (the genesis binary's
   * `lastSnapshotHash === Hash.empty === tipHash`), so an unseeded/genesis mg is never a `Reanchor`.
   */
  def classify(
    nel: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    tipHash: Hash,
    tipOrdinal: Option[Long]
  ): Decision = {
    val idx = nel.toList.indexWhere(_.value.lastSnapshotHash === tipHash)
    if (idx >= 0) Continue(idx)
    else {
      val genesisRooted = nel.head.value.lastSnapshotHash === Hash.empty
      val lastIdx = nel.length - 1
      tipOrdinal match {
        case Some(t) if genesisRooted && t >= 0L && t <= lastIdx.toLong =>
          Reanchor(math.min(t.toInt + 1, lastIdx))
        case _ => Defer
      }
    }
  }
}
