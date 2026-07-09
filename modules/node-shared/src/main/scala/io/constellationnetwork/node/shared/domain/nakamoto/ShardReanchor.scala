package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptyList
import cats.syntax.eq._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

/** SINGLE source of truth for "which binary in a committee checkpoint window continues gl0's per-metagraph SC tip". Called byte-identically
  * from BOTH the embed-selection (`GlobalSnapshotConsensusFunctions` `pick`) and the adopt-guard
  * (`GlobalSnapshotAcceptanceManager.adoptShardCheckpoints`) so the leader, every follower, and every gl0 peer reach the same
  * embed/adopt/defer decision — the split-safety invariant (no node-local reads).
  *
  * Background — the orphaned-tip freeze (2026-06-29): the trim-aware anchoring (09ace5227) heals a post-reorg window that OVERLAPS gl0's
  * adopted prefix (tip still ON the linear ml0 chain). But a SAME-ORDINAL slot-tiebreak shard reorg can ORPHAN the very binary at gl0's
  * committed SC tip — the tip then lies on a DEAD branch, no canonical window binary ever has `lastSnapshotHash === tip`, the old guard
  * returns `idx < 0` ⇒ DEFER FOREVER, and the per-MG mirror (`lastCurrencySnapshots`) freezes while gl0 + ml0 keep advancing. [[Reanchor]]
  * heals that: when the tip is orphaned but the window is the genesis-rooted canonical lineage reaching past the tip's ordinal, adopt the
  * canonical suffix from the binary after the dead tip's ordinal. The committee already chain-link-validated the window; gl0's per-MG root
  * verify (`deriveAdoptedCurrencyState`, which commits the metagraph's authoritative CUMULATIVE state base-independently and requires
  * `recomputed === attestedRoot`) is the safety backstop, so a wrong reanchor index cannot inject state — it DROPS. Pure function of
  * (window bytes, prior GSI).
  *
  * Background — the fully-adopted boundary wedge (2026-07-08, the 3gl0/2shard startup freeze): `classify` detected "tip orphaned" purely as
  * "no window binary's `lastSnapshotHash` === tip" — but that is ALSO true of a window gl0 has FULLY adopted (the tip IS the window's LAST
  * binary, and a chain-linked window never contains a child of its own tail). The genesis-rooted checkpoint therefore classified
  * `Reanchor(min(tipOrdinal + 1, lastIdx)) = Reanchor(lastIdx)` FOREVER once fully adopted: the oldest-first embed-selection re-picked it
  * every gl0 ordinal (shadowing the successor checkpoint), the adopt re-committed the tip binary as a no-op, the per-MG mirror froze at the
  * genesis window's tail ordinal, and the producer's pipeline gate (`awaiting-embed`) blocked all further checkpoint minting.
  * [[AlreadyAdopted]] disambiguates: the caller supplies `windowTipHash` — the window's last binary's own hash, computed with the SAME
  * value-hash (`Signed.toHashed.hash`, proofs excluded) the SC-tip setter records — and `windowTipHash === tipHash` means the window ends
  * EXACTLY at gl0's committed tip: nothing new to adopt, and the checkpoint must NOT qualify for (re-)embedding. A true same-ordinal
  * sibling (the lateral-reanchor heal) has `windowTipHash =!= tipHash` and still classifies [[Reanchor]].
  */
object ShardReanchor {

  sealed trait Decision

  /** A window binary's parent === gl0's tip — adopt the suffix from `idx` (existing trim-aware path). */
  final case class Continue(idx: Int) extends Decision

  /** gl0's tip was orphaned by a reorg — adopt the canonical suffix from `idx` (the binary past the dead tip). */
  final case class Reanchor(idx: Int) extends Decision

  /** The window's LAST binary IS gl0's committed tip (`windowTipHash === tipHash`) — the window is fully adopted. Nothing to adopt; the
    * embed-selection must SKIP this checkpoint (so its successor can be picked) instead of re-embedding it forever, and the adopt-guard
    * adopts nothing for this MG (a benign no-op, NOT a chain hole).
    */
  case object AlreadyAdopted extends Decision

  /** No continuation AND not a reanchorable orphan (a true chain hole) — defer; the leader re-offers an ancestor. */
  case object Defer extends Decision

  /** gl0's committed mirror-tip metagraph ordinal for `mg`, recovered PURELY from the prior GSI currency mirror (no opaque-`content` decode
    * — `StateChannelSnapshotBinary` carries no ordinal). `None` for an unseeded mg.
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

  /** The window's last binary's OWN hash — the disambiguator [[classify]] compares against gl0's committed SC tip. MUST be byte-identical
    * to how the SC-tip setter records the tip (`GlobalSnapshotAcceptanceManager`'s `sCSnapshotHashes = nel.last.toHashed.map(_.hash)` — the
    * VALUE hash, proofs excluded), so "window tail === tip" is an exact-identity check in the same hash space as the chain links
    * (`lastSnapshotHash`). Deterministic (Hasher over the wire-carried binary bytes, no node-local reads) ⇒ split-safe: the embed `pick`
    * and the adopt-guard compute the identical hash on every node.
    */
  def windowTipHashF[F[_]](
    nel: NonEmptyList[Signed[StateChannelSnapshotBinary]]
  )(implicit hasher: Hasher[F]): F[Hash] =
    hasher.hash(nel.last.value)

  /** Classify a checkpoint window `nel` (oldest-first) for one metagraph against gl0's committed SC tip.
    *
    *   - `Continue(idx)`: a binary continues `tipHash` (parent match) — the trim-aware suffix from `idx`.
    *   - `AlreadyAdopted`: no binary continues `tipHash`, but the window's last binary IS the tip (`windowTipHash === tipHash`) — the
    *     window is fully adopted. NOT an orphan: re-adopting would be a no-op, and re-embedding shadows the successor checkpoint forever
    *     (the 3gl0 startup freeze).
    *   - `Reanchor(idx)`: `tipHash` is continued by NO binary and is NOT the window's tail (a real orphan), but the window is
    *     genesis-rooted (head parent === `Hash.empty`) and reaches at-or-past `tipOrdinal`. Because the committee chain-link-validated the
    *     window, a genesis-rooted window's binary index EQUALS its metagraph ordinal, so the canonical successor of the dead tip is at
    *     index `tipOrdinal + 1` (clamped to the window head's last index for the lateral-only case where the reorg winner exists at
    *     `tipOrdinal` but its successor has not landed yet — reachable only when the winner DIFFERS from the tip, i.e. a true sibling; the
    *     identical-tail case is `AlreadyAdopted` above).
    *   - `Defer`: otherwise — a true chain hole (window not genesis-rooted, or doesn't reach `tipOrdinal`). Never jump a gap.
    *
    * Genesis (`tipHash === Hash.empty`, `tipOrdinal == 0`) always classifies `Continue` (the genesis binary's `lastSnapshotHash ===
    * Hash.empty === tipHash`), so an unseeded/genesis mg is never a `Reanchor`.
    *
    * @param windowTipHash
    *   the window's LAST binary's own hash, from [[windowTipHashF]] — same value-hash space as `tipHash` and the chain links.
    */
  def classify(
    nel: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    windowTipHash: Hash,
    tipHash: Hash,
    tipOrdinal: Option[Long]
  ): Decision = {
    val idx = nel.toList.indexWhere(_.value.lastSnapshotHash === tipHash)
    if (idx >= 0) Continue(idx)
    // FULLY-ADOPTED SKIP (2026-07-08): the window's tail IS gl0's committed tip — the window is fully adopted, NOT orphaned. Without
    // this check the branch below misclassified it as Reanchor(lastIdx) (a chain-linked window never contains a child of its own tail,
    // so idx < 0 here says nothing about orphaning) and the fully-adopted genesis checkpoint re-qualified for embedding every gl0
    // ordinal, shadowing its successor forever — the 3gl0/2shard startup freeze.
    else if (windowTipHash === tipHash) AlreadyAdopted
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
