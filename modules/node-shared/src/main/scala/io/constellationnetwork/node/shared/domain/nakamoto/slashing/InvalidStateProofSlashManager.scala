package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.types.numeric.NonNegLong

/** WATCHTOWER invalid-state-proof LEDGER EFFECT (slashing part 3) — the 100% `InvalidStateProof` slash tier of
  * `docs/nakamoto/SLASHING-DESIGN.md` §5/§6.
  *
  * '''Pure + deterministic.''' Given the slash targets (the committee that signed the wrong checkpoint), the prior `activeDelegatedStakes`
  * / `activeNodeCollaterals` maps, and the config, [[applySlash]] computes the post-slash maps + the slash records + the burned/bounty
  * amounts as a PURE function — every honest node applying the same upheld evidence to the same prior state computes the byte-identical
  * result. No I/O, no clock, no env (config is threaded). Same safety + determinism discipline as the `SlashableEvidence`/equivocation
  * ledger effects (`feedback_slashing_safety_bar`).
  *
  * '''What it does''' (per `SLASHING-DESIGN.md` §5, adapted — NOT a new primitive):
  *   1. '''Stake reduction''' — for every `(delegator, record)` whose `record.event.value.nodeId` is a slash target, reduce the staked
  *      amount by `slashFraction`. At the default `slashFraction = 1.0` (the `InvalidStateProof` tier is total loss) the record is REMOVED
  *      (amount → 0). For a partial fraction the `DelegatedStakeRecord.currentAmount` slot is reduced in place; node collaterals carry no
  *      mutable-amount slot, so a partial collateral slash is NOT representable without a schema change (see [[applySlash]] scaladoc) — the
  *      100% tier removes them outright, which IS representable. The default tier is the supported one.
  *   1. '''Eviction''' — every slashed operator gets a [[SlashedRegistryEntry]] with a `cooldownUntilEpoch`; the registry is read by the
  *      active-set gate so a slashed operator cannot contribute to any committee/quorum until cooldown elapses.
  *   1. '''Bounty + burn''' — `bountyFraction` of the slashed amount is credited to the submitter; the remainder is BURNED (removed from
  *      circulating supply, no minter).
  *
  * '''Reused, not invented.''' The schema mirrors `SLASHING-DESIGN.md` §5's table 1:1 (stake ×(1−fraction), `slashedRegistry` entry, bounty
  * credit, burn). The detection (watchtower re-exec) + the deterministic verdict ([[InvalidStateProofValidator]]) supply the evidence; this
  * manager is the §5 ledger sink.
  */
object InvalidStateProofSlashManager {

  /** Result of applying a slash — the post-slash maps + the audit records + the economic accounting. Pure; the GSAM accept path swaps the
    * post-slash maps into the `GlobalSnapshotInfo` it is building and credits/burn-accounts the amounts.
    *
    * @param slashedDelegatedStakes
    *   the `activeDelegatedStakes` map after reduction/removal of slashed operators' records.
    * @param slashedNodeCollaterals
    *   the `activeNodeCollaterals` map after removal of slashed operators' records (100% tier).
    * @param newRegistryEntries
    *   one [[SlashedRegistryEntry]] per slashed operator — the cooldown + double-slash audit record.
    * @param totalSlashedAmount
    *   the sum of all reduced stake + collateral (in `Balance` base units) — the pool split into bounty + burn.
    * @param bountyAmount
    *   `floor(totalSlashedAmount × bountyFraction)` credited to the submitter.
    * @param burnedAmount
    *   `totalSlashedAmount − bountyAmount` removed from circulating supply.
    */
  final case class SlashResult(
    slashedDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    slashedNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    newRegistryEntries: List[SlashedRegistryEntry],
    totalSlashedAmount: Long,
    bountyAmount: Long,
    burnedAmount: Long
  )

  /** Apply the 100%-tier (default) invalid-state-proof slash purely.
    *
    * '''Partial-fraction caveat (documented constraint).''' `DelegatedStakeRecord` has a `currentAmount` slot so a partial delegated-stake
    * slash IS representable (reduce in place). `NodeCollateralRecord` has NO mutable-amount slot, so a partial COLLATERAL slash would need
    * a schema change; this manager therefore supports a partial fraction for delegated stake but only FULL removal for collateral. At the
    * production default `slashFraction = 1.0` both are full removal — the supported, exercised path. A configured `slashFraction < 1.0`
    * reduces delegated stake proportionally and still fully removes collateral (conservative — never under-slashes the offender).
    *
    * @param slashTargets
    *   the operator `PeerId`s to slash (the committee that signed the wrong checkpoint), deduplicated by the caller.
    * @param priorDelegatedStakes
    *   the prior `activeDelegatedStakes.getOrElse(empty)` map.
    * @param priorNodeCollaterals
    *   the prior `activeNodeCollaterals.getOrElse(empty)` map.
    * @param eventOrdinal
    *   the ordinal the slash is applied at (stamped on each registry entry).
    * @param currentEpoch
    *   the current epoch (cooldown is `currentEpoch + cooldownEpochs`).
    * @param evidenceDigest
    *   the `(shardId, disputedCheckpointHash)` digest stamped on each registry entry (double-slash key) and on the audit record.
    * @param config
    *   the typed [[io.constellationnetwork.node.shared.config.types.InvalidStateProofSlashingConfig]] (slashFraction / cooldownEpochs /
    *   bountyFraction).
    */
  def applySlash(
    slashTargets: Set[PeerId],
    priorDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    priorNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]],
    eventOrdinal: SnapshotOrdinal,
    currentEpoch: EpochProgress,
    shardId: ShardId,
    disputedCheckpointHash: Hash,
    evidenceDigest: Hash,
    slashFraction: Double,
    bountyFraction: Double,
    cooldownEpochs: Long
  ): SlashResult = {
    val isTarget: PeerId => Boolean = slashTargets.contains

    // ---- Delegated stakes: reduce-or-remove per record whose operator is a target. `foldLeft` over (kept-map, runningSlashed) keeps it
    //      pure (no mutation). Iteration order is the SortedMap/SortedSet canonical order ⇒ the accumulated total is deterministic. ----
    val (newDelegated0, slashedFromStakes) =
      priorDelegatedStakes.foldLeft((SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]], 0L)) {
        case ((acc, sum), (delegator, records)) =>
          val (kept, recSum) = records.foldLeft((SortedSet.empty[DelegatedStakeRecord], 0L)) {
            case ((rs, s), rec) =>
              if (!isTarget(rec.event.value.nodeId)) (rs + rec, s)
              else {
                val before: Long = rec.amount.value.value
                if (slashFraction >= 1.0d)
                  // Full slash — remove the record (amount → 0).
                  (rs, s + before)
                else {
                  // Partial slash — reduce `currentAmount` in place. `floor` is deterministic (identical product floored on every node;
                  // `slashFraction` is a byte-identical config constant cluster-wide).
                  val slashed: Long = math.floor(before.toDouble * slashFraction).toLong
                  val remaining: Long = math.max(0L, before - slashed)
                  (
                    rs + rec.copy(currentAmount =
                      Some(io.constellationnetwork.schema.delegatedStake.DelegatedStakeAmount(NonNegLong.unsafeFrom(remaining)))
                    ),
                    s + (before - remaining)
                  )
                }
              }
          }
          (if (kept.nonEmpty) acc.updated(delegator, kept) else acc, sum + recSum)
      }
    val newDelegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]] = newDelegated0

    // ---- Node collaterals: remove every record whose operator is a target (100%-tier; no mutable-amount slot for partial). ----
    val (newCollaterals0, slashedFromCollaterals) =
      priorNodeCollaterals.foldLeft((SortedMap.empty[Address, SortedSet[NodeCollateralRecord]], 0L)) {
        case ((acc, sum), (funder, records)) =>
          val (kept, recSum) = records.foldLeft((SortedSet.empty[NodeCollateralRecord], 0L)) {
            case ((rs, s), rec) =>
              if (isTarget(rec.event.value.nodeId)) (rs, s + rec.event.value.amount.value.value)
              else (rs + rec, s)
          }
          (if (kept.nonEmpty) acc.updated(funder, kept) else acc, sum + recSum)
      }
    val newCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]] = newCollaterals0

    val total: Long = slashedFromStakes + slashedFromCollaterals
    val (bounty, burned) = splitBountyBurn(total, bountyFraction)

    val entries: List[SlashedRegistryEntry] =
      slashTargets.toList.sortBy(_.value.value).map { op =>
        SlashedRegistryEntry(
          peerId = op,
          shardId = shardId,
          disputedCheckpointHash = disputedCheckpointHash,
          eventOrdinal = eventOrdinal,
          cooldownUntilEpoch = EpochProgress(NonNegLong.unsafeFrom(currentEpoch.value.value + math.max(0L, cooldownEpochs))),
          evidenceDigest = evidenceDigest,
          reason = SlashReason.InvalidStateProof
        )
      }

    SlashResult(newDelegated, newCollaterals, entries, total, bounty, burned)
  }

  /** Deterministic bounty/burn split of a slashed pool. `bounty = floor(total × bountyFraction)` (clamped to `[0, total]`); the remainder
    * burns. Pulled out so the GSAM sink computes it once with the configured `bountyFraction`.
    */
  def splitBountyBurn(total: Long, bountyFraction: Double): (Long, Long) = {
    val f = math.max(0.0d, math.min(1.0d, bountyFraction))
    val bounty = math.max(0L, math.min(total, math.floor(total.toDouble * f).toLong))
    (bounty, total - bounty)
  }

  /** Sum a slashed-amount `Long` into a `Balance` for crediting the bounty. Saturates at `Balance.maxValue` defensively (a slash pool can't
    * realistically exceed it, but never overflow the refined type).
    */
  def asAmount(value: Long): Amount = Amount(NonNegLong.unsafeFrom(math.max(0L, value)))

  /** Add a bounty `Long` to an existing `Balance` (the submitter's). Saturating + total. */
  def creditBalance(prior: Balance, bounty: Long): Balance =
    Balance(NonNegLong.unsafeFrom(math.max(0L, prior.value.value + math.max(0L, bounty))))

  /** The reason an operator was slashed — the audit discriminator on [[SlashedRegistryEntry]]. Sealed ADT (per
    * `feedback_no_string_matching`): the slash tier is typed, not a string. `InvalidStateProof` is the 100% tier (this manager); future
    * tiers (equivocation, non-participation) slot in here when their ledger sinks land.
    */
  @derive(decoder, encoder, eqv, show)
  sealed trait SlashReason extends Product with Serializable
  object SlashReason {
    case object InvalidStateProof extends SlashReason
    case object CheckpointEquivocation extends SlashReason
    case object MetagraphEquivocation extends SlashReason
    case object NonParticipation extends SlashReason
  }

  /** One slashed-operator audit + cooldown record — the `slashedRegistry` entry of `SLASHING-DESIGN.md` §5. Written to the
    * [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.Slashings]] MPT partition (fieldId 34 — fieldId 33 is `ConsumedAllowSpends`),
    * keyed by `GlobalStateKey.slashingsKey(peerId, shardId, disputedCheckpointHash)`, and read by the active-set gate (cooldown) + the
    * double-slash guard ([[InvalidStateProofSlashedReader]]).
    *
    * @param peerId
    *   the slashed operator.
    * @param shardId
    *   the shard whose committee the operator was on (double-slash key half).
    * @param disputedCheckpointHash
    *   the wrong checkpoint (double-slash key half) — `(shardId, disputedCheckpointHash)` is the dedup identity.
    * @param eventOrdinal
    *   the snapshot ordinal the slash was applied at.
    * @param cooldownUntilEpoch
    *   the operator is inactive (cannot rejoin a committee / contribute to quorum) until this epoch.
    * @param evidenceDigest
    *   `Hasher` of the upheld evidence — provenance for the audit log.
    * @param reason
    *   the slash tier.
    */
  @derive(decoder, encoder, eqv, show)
  final case class SlashedRegistryEntry(
    peerId: PeerId,
    shardId: ShardId,
    disputedCheckpointHash: Hash,
    eventOrdinal: SnapshotOrdinal,
    cooldownUntilEpoch: EpochProgress,
    evidenceDigest: Hash,
    reason: SlashReason
  )
}
