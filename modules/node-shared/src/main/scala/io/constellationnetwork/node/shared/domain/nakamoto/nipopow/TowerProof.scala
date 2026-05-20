package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.security.hash.Hash

import derevo.cats.eqv
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** §3 NIPoPoW S4.1 — serializable header bundle for a single snapshot in a tower proof.
  *
  * Just enough of the `Signed[GlobalIncrementalSnapshot]` for verifier replays:
  *   - `ordinal`, `slot`, `parentSlot` — slot/ordinal positioning + slotGap reconstruction.
  *   - `vrfProof`, `vrfOutput`, `vrfPublicKey` — L0 eligibility re-verification via
  *     [[io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker.verifyEligibility]].
  *   - `eta` — proof builder copies the certificate's `eta` field; verifier independently re-derives an eta over the L0 suffix via
  *     [[io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation]] (against the level-0 suffix only — the only place the proof
  *     gives the verifier per-snapshot VRF outputs).
  *   - `subchainLevelCounts` — per-super-level cumulative counts at this ordinal, for level-density reconciliation.
  *   - `snapshotHash` — verifier cross-references `chain headers ↔ tower entries` (must match `TowerEntry.snapshotHash`).
  *   - `activePoolSize` — passed through unchanged; verifier doesn't re-derive the eligibility relativeStake (would require historical
  *     state). Proof v1 just records the certificate's claimed `activePoolSize` for diagnostic completeness.
  *
  * Size: 8 (ord) + 8 (slot) + 8 (parentSlot) + 80 (proof) + 64 (output) + 32 (pk) + 32 (eta) + 72 (9·int64 counts) + 32 (hash) + 4
  * (poolSize) = 340 bytes per header (raw); JSON-encoded ~600-800 bytes. Budget calculus: 250 headers cap → ~85 KB raw, ~200 KB JSON.
  * Suffix-trimmed proofs (k=5 L0 suffix + sparse upper levels) come in well under 50 KB.
  */
@derive(eqv, encoder, decoder)
final case class TowerProofHeader(
  ordinal: SnapshotOrdinal,
  slot: Slot,
  parentSlot: Slot,
  vrfProof: VrfProof,
  vrfOutput: VrfOutput,
  vrfPublicKey: VrfPublicKey,
  eta: Hash,
  activePoolSize: Int,
  subchainLevelCounts: Vector[Long],
  snapshotHash: Hash
) {

  /** Reconstruct slot-gap δ_S = slot - parentSlot. Defensive — should be ≥ 1 for non-genesis snapshots. */
  def deltaSlot: Long = slot.value.value - parentSlot.value.value

  /** Look up per-level cumulative count. Out-of-range → 0 (defensive). */
  def countAt(level: Int): Long =
    if (level >= 1 && level <= subchainLevelCounts.size) subchainLevelCounts(level - 1) else 0L
}

/** §3 NIPoPoW S4.1 — full proof structure produced by [[TowerProofBuilder]] and consumed by [[TowerVerifier]].
  *
  *   - `level0Suffix` — the k most-recent finalized snapshot headers (default k=5 per `T_depth2 = 5`). Verifier replays L0 trials over the
  *     suffix and re-derives `g_µ` to prime its own tower view.
  *   - `levelChains` — per-super-level (1..9) chain of headers, one entry per level-µ tower hit since `since`. Sparse: lower-frequency
  *     levels (L7-L9) typically have ≤ 1 entry per eta period.
  *   - `since` — anchor ordinal the proof is taken FROM (inclusive). Verifier uses this as the base for density-relative-error
  *     denominators.
  *   - `tipOrdinal` — the most-recent ordinal in the proof (== `level0Suffix.last.ordinal`). Verifier uses this as the upper-bound for the
  *     density window.
  *
  * '''Invariants''' (checked by verifier; producer should produce conformant proofs):
  *   - `level0Suffix.nonEmpty` AND ordered strictly-ascending by `ordinal`.
  *   - Every `header in levelChains(µ)` has ordinal ∈ [`since`, `tipOrdinal`].
  *   - Within each level, headers are strictly-ascending by ordinal.
  *
  * '''Size bound (proposal §5.3 / S6 acceptance)''': ≤ 50 KB. At default `targetDensity = f_0 · 2^(-µ)` over an eta period (≈ 100
  * snapshots), the total header count is bounded by `k + Σ_µ count(µ)` ≈ `5 + 50 + 25 + 12 + 6 + 3 + 2 + 1 + 1 + 1` ≈ 106 headers per eta
  * period; well below the 250-cap.
  */
@derive(eqv, encoder, decoder)
final case class TowerProof(
  since: SnapshotOrdinal,
  tipOrdinal: SnapshotOrdinal,
  level0Suffix: Vector[TowerProofHeader],
  levelChains: Map[Int, Vector[TowerProofHeader]]
) {

  /** Total header count across all levels — for size-cap enforcement at builder time. Headers at the L0 suffix are NOT double-counted in
    * the level-µ chains (they're separate carriers).
    */
  def totalHeaderCount: Int =
    level0Suffix.size + levelChains.valuesIterator.map(_.size).sum

  /** Headers ordered by ordinal across all levels (deduplicated). Used by the verifier's eta-chain reconstruction. */
  def allHeadersByOrdinal: Vector[TowerProofHeader] = {
    val all = level0Suffix.iterator ++ levelChains.valuesIterator.flatten
    all.toVector.distinctBy(_.ordinal).sortBy(_.ordinal.value.value)
  }
}

object TowerProof {

  /** Default suffix length k = `T_depth2` finality depth (= 5). Verifier replays L0 trials over this suffix. */
  val DefaultSuffixLength: Int = 5

  /** Empty proof at the genesis anchor — no L0 suffix yet, no level-µ hits. Verifier treats as a vacuous-OK base case. */
  val Empty: TowerProof =
    TowerProof(SnapshotOrdinal.MinValue, SnapshotOrdinal.MinValue, Vector.empty, Map.empty)
}
