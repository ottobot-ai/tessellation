package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nodeCollateral.NodeCollateralRecord
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** RED oracle for SLASH-07.
  *
  * A valid EventTrigger snapshot advances SnapshotOrdinal but retains EpochProgress.
  * InvalidStateProofSlashManager writes the cooldown expiry on the EpochProgress
  * axis, while SlashCooldownReader compares that value with an eta anchor expressed
  * as a snapshot ordinal. A burst of EventTrigger snapshots can therefore consume
  * an epoch-based cooldown without advancing one epoch.
  *
  * Keep this suite untracked until the cooldown record and reader use one
  * consensus-defined time axis.
  */
object SlashCooldownAxisMismatchRedSuite extends FunSuite {

  private val operator = PeerId(Hex("77" * 64))
  private val shard = ShardId.unsafeApply(0)
  private val checkpointHash = Hash("ab" * 32)

  test("SLASH-07 RED: the first delayed committee anchor cannot expire a cooldown while EpochProgress is unchanged") {
    val rotationSnapshots = 100L
    val slashOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(500L))
    val slashEpoch = EpochProgress(NonNegLong.unsafeFrom(42L))
    val cooldownEpochs = 100L

    val slash = InvalidStateProofSlashManager.applySlash(
      slashTargets = Set(operator),
      priorDelegatedStakes = SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]],
      priorNodeCollaterals = SortedMap.empty[Address, SortedSet[NodeCollateralRecord]],
      eventOrdinal = slashOrdinal,
      currentEpoch = slashEpoch,
      shardId = shard,
      disputedCheckpointHash = checkpointHash,
      evidenceDigest = checkpointHash,
      slashFraction = Ratio.One,
      bountyFraction = Ratio.Zero,
      cooldownEpochs = cooldownEpochs
    )

    // Epoch 7 resolves exclusions at ordinal 599, the first eta anchor after the
    // slash at ordinal 500. EventTrigger snapshots 501..599 may all retain epoch 42.
    val firstDelayedAnchorOrdinal =
      SlashCooldownReader.slashAnchorOrdinal(EtaPeriod(7L), rotationSnapshots)
    val epochProgressAtAnchor = slashEpoch

    val expectedWhileEpochCooldownIsLive =
      slash.newRegistryEntries.iterator
        .filter { entry =>
          entry.eventOrdinal.value.value <= firstDelayedAnchorOrdinal &&
          entry.cooldownUntilEpoch.value.value > epochProgressAtAnchor.value.value
        }
        .map(_.peerId)
        .toList

    val productionCandidates =
      SlashCooldownReader
        .activeSlashCandidates(slash.newRegistryEntries, firstDelayedAnchorOrdinal)
        .map(_._1)

    expect.all(
      firstDelayedAnchorOrdinal == 599L,
      slash.newRegistryEntries.head.cooldownUntilEpoch == EpochProgress(NonNegLong.unsafeFrom(142L)),
      expectedWhileEpochCooldownIsLive == List(operator),
      productionCandidates == expectedWhileEpochCooldownIsLive
    )
  }
}
