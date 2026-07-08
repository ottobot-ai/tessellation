package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.Applicative
import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.SlashedRegistryEntry
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher

/** Per-operator COOLDOWN gate over the `Slashings` (fieldId 34) partition — the committee-EXCLUSION half of the watchtower slash tier
  * (FINDING-002 / EPIC-3.1). [[InvalidStateProofSlashedReader]] is the double-slash dedup over the SAME partition; this reader is the
  * "future per-operator cooldown gate" its scaladoc promised, consumed by
  * [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.committeeFor]] so a slashed operator is ABSENT
  * from the shard-committee draw (and therefore its checkpoint signature can never pass the committee-membership pre-check, i.e. can
  * never count toward `kQuorum`) until its cooldown elapses.
  *
  * ==Cluster-uniformity (the anti-fork contract — read this before touching the anchor rule)==
  *
  * `committeeFor` is CONSENSUS-LOAD-BEARING: two honest nodes computing different excluded sets compute different committees/quorum
  * verdicts for the same embedded checkpoint ⇒ divergent adopt decisions ⇒ fork. The committee is memoized per `(shardId, epoch)` and
  * MUST stay a pure function of that key. So the exclusion is pinned per EPOCH, Cardano-style (the settled epoch-staggering design:
  * "the signer set for epoch N is a function of state settled by the end of epoch N−2"):
  *
  *   - '''Anchor.''' [[slashAnchorOrdinal]]`(E) = (E−1)·R − 1` — the LAST gl0 ordinal of eta-period `E−2` (`R =
  *     nakamoto.etaRotationSnapshots`, the same period length the committee's eta resolver keys on). Only slash records written
  *     at-or-below the anchor participate in epoch `E`'s exclusion.
  *   - '''Why the reads are uniform from a live base store.''' The partition is APPEND-ONLY (entries are written once by the GSAM
  *     accept fold, never mutated, never pruned; the S01/S02 preservation keeps them across every GSI rebuild), and each entry's
  *     `eventOrdinal`/`cooldownUntilEpoch` are immutable consensus-written fields. Filtering `eventOrdinal <= anchor(E)` therefore
  *     reconstructs EXACTLY the set of records the canonical chain had written by the anchor, on ANY node whose finalized base has
  *     reached the anchor — the same per-key-immutability discipline that makes the committee's `etaForEpoch` read (the
  *     `HistoricalStakeSnapshot` lookup off the SAME `mptStore`) uniform. The anchor sits a full eta-period (+1 boundary) below any
  *     ordinal at which an epoch-`E` committee is legitimately evaluated — deeper than the k₁ write-freeze floor — so the filtered
  *     prefix is in the write-frozen common prefix of every honest node.
  *   - '''Margin.''' The earliest legitimate epoch-`E` draw is the pseudo-predictability point (~2/3 into period `E−1`, ordinal
  *     `≈ E·R − R/3`); a node evaluating there has finalized base `≥ E·R − R/3 − k₁ ≈ E·R − 2.06·k₁`, which exceeds
  *     `anchor(E) ≈ E·R − 3.03·k₁` by `≈ k₁` (R = round(3.03·k₁)).
  *   - '''anchorSettled.''' When this node's `lastPersistedOrdinal` has NOT reached the anchor (an early/adversarial draw for a future
  *     wire-carried epoch), the scan may under-report (records still to be written below the anchor). The result then carries
  *     `anchorSettled = false` and the caller MUST NOT memoize it (`ShardCheckpointWiring` only caches settled draws) — a fresh
  *     evaluation at real use time (anchor long settled) is exact. This mirrors `EtaStateManager`'s discipline of not caching the
  *     empty-chain-walk fallback.
  *
  * ==The cooldown test (epoch model — documented assumption)==
  *
  * A record excludes its operator for epoch `E` iff `eventOrdinal <= anchor(E) < cooldownExpiry`, where the expiry is the entry's own
  * consensus-written `cooldownUntilEpoch` compared against the anchor ORDINAL. `cooldownUntilEpoch` is an
  * [[io.constellationnetwork.schema.epoch.EpochProgress]] (`= epochProgress-at-slash + cooldownEpochs`, `InvalidStateProofSlashManager
  * .applySlash`), while the committee draw has only the eta-period/ordinal axis: under the Nakamoto overlay the two are the SAME axis —
  * the leader loop produces EVERY snapshot with `TimeTrigger` (`SnapshotLeaderLoop`), so `epochProgress` advances exactly 1 per ordinal
  * from genesis (both start at 0) and the consensus EpochProgress AT the anchor ordinal IS the anchor ordinal. Sharding (`numShards >
  * 1`) is a Nakamoto-overlay-only mechanism, so this equivalence holds wherever this reader is consulted; it is asserted by
  * `SlashCooldownReaderSuite` against `applySlash`-written entries. Every input (entry bytes, `R`, the epoch) is consensus-pinned and
  * cluster-uniform ⇒ every honest node computes the byte-identical excluded set.
  *
  * ==Degenerate-case floor (Polkadot `UpToLimitDisablingStrategy`, adapted — NOT a novel mechanism)==
  *
  * Exclusion must never make `kQuorum` structurally unmeetable (the committee is a subset of the post-exclusion pool). Polkadot
  * disables offenders only up to a limit and lets NEW offenders escape disabling once the limit is reached; [[effectiveExclusion]]
  * adapts exactly that: candidates are ordered oldest-slash-first (`(minEventOrdinal, peerId)` — deterministic), and at most
  * `max(0, |active| − kQuorum)` of them are excluded, so at least `kQuorum` eligible validators always remain (liveness floor =
  * the existing cluster-uniform `nakamoto.committee.kQuorum`; no new tunable). Everyone-slashed with `|active| <= kQuorum` ⇒ nobody
  * is excluded (liveness over exclusion, loudly visible in the slash ledger either way).
  */
trait SlashCooldownReader[F[_]] {

  /** The epoch-pinned exclusion view for `epoch`: the deterministic per-peer cooldown candidates (already filtered to records active at
    * the epoch's anchor) and whether this node's base has settled the anchor (only then may the caller memoize a committee drawn from
    * it).
    */
  def excludedForEpoch(epoch: EtaPeriod): F[SlashCooldownReader.EpochExclusion]
}

object SlashCooldownReader {

  /** Exclusion candidates for one epoch.
    *
    * @param candidates
    *   one `(peerId, minEventOrdinal)` per operator with at least one ACTIVE record at the epoch's anchor (`eventOrdinal <= anchor <
    *   cooldownUntilEpoch`), sorted oldest-slash-first then by `peerId` — the deterministic priority order [[effectiveExclusion]]
    *   consumes.
    * @param anchorSettled
    *   `true` iff this node's finalized base has reached the epoch's anchor ordinal (or the anchor is pre-genesis), i.e. the candidate
    *   list is FINAL for this epoch on every honest node. `false` ⇒ the caller must not cache any committee derived from it.
    */
  final case class EpochExclusion(candidates: List[(PeerId, Long)], anchorSettled: Boolean)

  object EpochExclusion {
    val empty: EpochExclusion = EpochExclusion(List.empty, anchorSettled = true)
  }

  /** No-op reader — no exclusion, always settled. The `None`-wiring fallback (`ShardCheckpointWiring.acceptanceDeps` default) and the
    * byte-identity stub for tests: with it the draw is BYTE-IDENTICAL to the pre-FINDING-002 code path (asserted by
    * `SlashCooldownExclusionSuite`). Production (`SharedServices`) always wires [[fromMptStore]].
    */
  def noExclusion[F[_]: Applicative]: SlashCooldownReader[F] =
    (_: EtaPeriod) => EpochExclusion.empty.pure[F]

  /** Deterministic in-memory reader for tests — every node building it from the same entries computes the same view. */
  def fromEntries[F[_]: Applicative](entries: List[SlashedRegistryEntry], etaRotationSnapshots: Long): SlashCooldownReader[F] =
    (epoch: EtaPeriod) => {
      val anchor = slashAnchorOrdinal(epoch, etaRotationSnapshots)
      EpochExclusion(activeSlashCandidates(entries, anchor), anchorSettled = true).pure[F]
    }

  /** The epoch's slash anchor: the last gl0 ordinal of eta-period `epoch − 2`, i.e. `(epoch − 1)·R − 1`. Negative for the bootstrap
    * epochs (`epoch <= 1`, where `R ≥ 1`) ⇒ no record can qualify ⇒ exclusion structurally empty — mirroring the periods-0/1
    * bootstrap-eta treatment.
    */
  def slashAnchorOrdinal(epoch: EtaPeriod, etaRotationSnapshots: Long): Long =
    (epoch.value - 1L) * etaRotationSnapshots - 1L

  /** PURE candidate derivation: records ACTIVE at `anchorOrdinal` (`eventOrdinal <= anchor < cooldownUntilEpoch`, both fields
    * consensus-written and immutable), collapsed to one candidate per operator at its EARLIEST active `eventOrdinal`, sorted
    * `(minEventOrdinal, peerId)` — the deterministic oldest-slash-first priority. Records younger than the anchor belong to a LATER
    * epoch's view; expired cooldowns (`cooldownUntilEpoch <= anchor`) have served their term.
    */
  def activeSlashCandidates(entries: Iterable[SlashedRegistryEntry], anchorOrdinal: Long): List[(PeerId, Long)] =
    entries.iterator
      .filter { e =>
        e.eventOrdinal.value.value <= anchorOrdinal && e.cooldownUntilEpoch.value.value > anchorOrdinal
      }
      .toList
      .groupMapReduce(_.peerId)(_.eventOrdinal.value.value)(math.min)
      .toList
      .sortBy { case (peerId, minOrd) => (minOrd, peerId.value.value) }

  /** PURE exclusion cap — the Polkadot `UpToLimit` adaptation (see class scaladoc). Excludes candidates (∩ `active`) in priority order,
    * never dropping the eligible pool below `minActiveFloor` (= `kQuorum` at the call site). Candidates beyond the cap stay ELIGIBLE
    * (newest offenders escape, exactly Polkadot's rule); `|active| <= minActiveFloor` ⇒ excludes nobody.
    */
  def effectiveExclusion(active: Set[PeerId], candidates: List[(PeerId, Long)], minActiveFloor: Int): Set[PeerId] = {
    val inActive = candidates.collect { case (peerId, _) if active.contains(peerId) => peerId }
    val maxExcludable = math.max(0, active.size - math.max(minActiveFloor, 0))
    inActive.take(maxExcludable).toSet
  }

  /** Production reader over the SAME finalized-base `MptStore` the committee draw's eta resolver reads (`SharedServices`
    * `storages.mptStore` — the store `InvalidStateProofSlashedReader.fromMptStore` scans and `GlobalSnapshotAcceptanceManager`'s
    * `Slashings` insert lands in once finalized). Prefix-scans fieldId 34 with the canonical
    * [[InvalidStateProofSlashedReader.entryCodec]] and reports `anchorSettled` off `lastPersistedOrdinal` (see class scaladoc for why
    * the anchor filter makes the live-store scan epoch-pure).
    *
    * The partition is tiny (≤ committee-size records per upheld dispute, slashes rare) — same cost argument as the double-slash scan.
    */
  def fromMptStore[F[_]: Sync: Hasher](
    mptStore: MptStore[F, GlobalStateKey],
    etaRotationSnapshots: Long
  ): SlashCooldownReader[F] =
    new SlashCooldownReader[F] {
      private implicit val codec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] =
        InvalidStateProofSlashedReader.entryCodec

      def excludedForEpoch(epoch: EtaPeriod): F[EpochExclusion] = {
        val anchor = slashAnchorOrdinal(epoch, etaRotationSnapshots)
        if (anchor < 0L)
          // Bootstrap epochs: nothing can precede genesis — structurally empty and final without touching the store.
          EpochExclusion.empty.pure[F]
        else
          for {
            prefix <- GlobalStateKey.slashingsFieldPrefix[F]
            entries <- mptStore.getAllForPrefix[SlashedRegistryEntry](prefix)
            lastPersisted <- mptStore.lastPersistedOrdinal
            settled = lastPersisted.exists(_.value.value >= anchor)
          } yield EpochExclusion(activeSlashCandidates(entries.values, anchor), anchorSettled = settled)
      }
    }
}
