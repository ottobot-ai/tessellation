package io.constellationnetwork.schema

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.Async
import cats.syntax.functor._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.syntax.next.catsSyntaxNext
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.merkletree.syntax.SortedMapOpsImpl
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SharedArtifact, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.schema.snapshot.{FullSnapshot, IncrementalSnapshot}
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary
import io.constellationnetwork.syntax.sortedCollection._

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosInt

@derive(eqv, show, encoder, decoder)
case class GlobalIncrementalSnapshotV1(
  ordinal: SnapshotOrdinal,
  height: Height,
  subHeight: SubHeight,
  lastSnapshotHash: Hash,
  blocks: SortedSet[BlockAsActiveTip],
  stateChannelSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
  rewards: SortedSet[RewardTransaction],
  epochProgress: EpochProgress,
  nextFacilitators: NonEmptyList[PeerId],
  tips: SnapshotTips,
  stateProof: GlobalSnapshotStateProofV1,
  version: SnapshotVersion = SnapshotVersion("0.0.1")
) extends IncrementalSnapshot[GlobalSnapshotStateProofV1] {
  def toGlobalIncrementalSnapshot: GlobalIncrementalSnapshot =
    GlobalIncrementalSnapshot(
      ordinal,
      height,
      subHeight,
      lastSnapshotHash,
      blocks,
      stateChannelSnapshots,
      SortedMap.empty[ShardId, ShardCheckpoint], // shardCheckpoints — V1 pre-dates sharding; absence = empty map (§3.4)
      rewards,
      Some(SortedMap.empty),
      epochProgress,
      nextFacilitators,
      tips,
      stateProof.toGlobalSnapshotStateProof,
      Some(SortedSet.empty),
      Some(SortedSet.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedSet.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      version
    )
}

object GlobalIncrementalSnapshotV1 {
  def fromGlobalIncrementalSnapshot(snapshot: GlobalIncrementalSnapshot): GlobalIncrementalSnapshotV1 =
    GlobalIncrementalSnapshotV1(
      snapshot.ordinal,
      snapshot.height,
      snapshot.subHeight,
      snapshot.lastSnapshotHash,
      snapshot.blocks,
      snapshot.stateChannelSnapshots,
      snapshot.rewards,
      snapshot.epochProgress,
      snapshot.nextFacilitators,
      snapshot.tips,
      GlobalSnapshotStateProofV1.fromGlobalSnapshotStateProof(snapshot.stateProof),
      snapshot.version
    )
}

@derive(eqv, show, encoder)
case class GlobalIncrementalSnapshot(
  ordinal: SnapshotOrdinal,
  height: Height,
  subHeight: SubHeight,
  lastSnapshotHash: Hash,
  blocks: SortedSet[BlockAsActiveTip],
  stateChannelSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
  // Hierarchical shard checkpoints — one entry per shard per ord; absent shards mean "no checkpoint produced this ord"
  // (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.4). Empty map is the bootstrap-window / pre-sharding default
  // and is represented as `SortedMap.empty` (NOT `Option`) — absence is the empty map per design-doc convention.
  shardCheckpoints: SortedMap[ShardId, ShardCheckpoint],
  rewards: SortedSet[RewardTransaction],
  delegateRewards: Option[SortedMap[PeerId, Map[Address, Amount]]],
  epochProgress: EpochProgress,
  nextFacilitators: NonEmptyList[PeerId],
  tips: SnapshotTips,
  stateProof: GlobalSnapshotStateProof,
  allowSpendBlocks: Option[SortedSet[Signed[AllowSpendBlock]]],
  tokenLockBlocks: Option[SortedSet[Signed[TokenLockBlock]]],
  spendActions: Option[SortedMap[Address, List[SpendAction]]],
  updateNodeParameters: Option[SortedMap[Id, Signed[UpdateNodeParameters]]],
  artifacts: Option[SortedSet[SharedArtifact]],
  activeDelegatedStakes: Option[SortedMap[Address, List[Signed[UpdateDelegatedStake.Create]]]],
  delegatedStakesWithdrawals: Option[SortedMap[Address, List[Signed[UpdateDelegatedStake.Withdraw]]]],
  activeNodeCollaterals: Option[SortedMap[Address, List[Signed[UpdateNodeCollateral.Create]]]],
  nodeCollateralWithdrawals: Option[SortedMap[Address, List[Signed[UpdateNodeCollateral.Withdraw]]]],
  version: SnapshotVersion = SnapshotVersion("0.0.1"),
  slotCertificate: Option[io.constellationnetwork.schema.nakamoto.slot.SlotCertificate] = None,
  eta: Option[io.constellationnetwork.security.hash.Hash] = None,
  // WATCHTOWER fraud proofs carried as a CONSENSUS ARTIFACT (W3a) — a canonical `SortedSet` of fully-validated (UPHELD)
  // `InvalidStateProofEvidence`, each carrying the full disputed `ShardCheckpoint` + the challenger's `FraudProofEnvelope`. The set's
  // `Order` is keyed by the `(shardId, disputedCheckpointHash)` double-slash identity, so it serializes byte-deterministically and two
  // disputes over the SAME wrong checkpoint coalesce. Carried EXACTLY like `shardCheckpoints` (a dedicated field, NOT the `artifacts`
  // SharedArtifact set): the gl0 leader sources candidates from its node-local validated-fraud-proof pool, embeds the UPHELD subset here,
  // and the follower/peer threads `signedArtifact.fraudProofs` back through `accept()` (NOT re-sourcing node-local gossip) so the byte-exact
  // `recreatedArtifact === artifact` round-trip holds and EVERY node folds the SAME slash. Empty set = no disputes this ord; ALWAYS empty at
  // `numShards = 1` (no committees ⇒ no fraud proofs) ⇒ mptRoot byte-identical to the pre-watchtower path.
  fraudProofs: SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence] = SortedSet.empty
) extends IncrementalSnapshot[GlobalSnapshotStateProof]

object GlobalIncrementalSnapshot {

  /** Forgiving Circe decoder — defaults `shardCheckpoints` to [[SortedMap.empty]] when the field is absent.
    *
    * Replaces the derevo-derived decoder so that pre-Slice-4 (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.4) brotli
    * fixtures still round-trip through `JsonScodecParitySuite` (those fixtures were captured before this field existed). New encodings
    * always include the field via the derived encoder, so round-trips on current snapshots are unaffected. Mirrors the same pattern as
    * `SlotCertificate.decoder` in `schema.nakamoto.slot` for the `subchainLevelCounts` field.
    *
    * '''Why hand-rolled instead of `circe-magnolia.configured.withDefaults`.''' The rest of the schema package uses derevo's standard
    * (non-configured) magnolia derivation; introducing a per-type `Configuration` here would diverge from the project-wide convention.
    * Hand-rolling one decoder mirrors the existing precedent in `slot.scala:157` and keeps the customization narrow and explicit.
    */
  implicit val decoder: io.circe.Decoder[GlobalIncrementalSnapshot] = io.circe.Decoder.instance { c =>
    for {
      ordinal <- c.downField("ordinal").as[SnapshotOrdinal]
      height <- c.downField("height").as[Height]
      subHeight <- c.downField("subHeight").as[SubHeight]
      lastSnapshotHash <- c.downField("lastSnapshotHash").as[Hash]
      blocks <- c.downField("blocks").as[SortedSet[BlockAsActiveTip]]
      stateChannelSnapshots <- c
        .downField("stateChannelSnapshots")
        .as[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]]
      shardCheckpoints <- c
        .downField("shardCheckpoints")
        .as[Option[SortedMap[ShardId, ShardCheckpoint]]]
        .map(_.getOrElse(SortedMap.empty[ShardId, ShardCheckpoint]))
      rewards <- c.downField("rewards").as[SortedSet[RewardTransaction]]
      delegateRewards <- c.downField("delegateRewards").as[Option[SortedMap[PeerId, Map[Address, Amount]]]]
      epochProgress <- c.downField("epochProgress").as[EpochProgress]
      nextFacilitators <- c.downField("nextFacilitators").as[NonEmptyList[PeerId]]
      tips <- c.downField("tips").as[SnapshotTips]
      stateProof <- c.downField("stateProof").as[GlobalSnapshotStateProof]
      allowSpendBlocks <- c.downField("allowSpendBlocks").as[Option[SortedSet[Signed[AllowSpendBlock]]]]
      tokenLockBlocks <- c.downField("tokenLockBlocks").as[Option[SortedSet[Signed[TokenLockBlock]]]]
      spendActions <- c.downField("spendActions").as[Option[SortedMap[Address, List[SpendAction]]]]
      updateNodeParameters <- c.downField("updateNodeParameters").as[Option[SortedMap[Id, Signed[UpdateNodeParameters]]]]
      artifacts <- c.downField("artifacts").as[Option[SortedSet[SharedArtifact]]]
      activeDelegatedStakes <- c
        .downField("activeDelegatedStakes")
        .as[Option[SortedMap[Address, List[Signed[UpdateDelegatedStake.Create]]]]]
      delegatedStakesWithdrawals <- c
        .downField("delegatedStakesWithdrawals")
        .as[Option[SortedMap[Address, List[Signed[UpdateDelegatedStake.Withdraw]]]]]
      activeNodeCollaterals <- c
        .downField("activeNodeCollaterals")
        .as[Option[SortedMap[Address, List[Signed[UpdateNodeCollateral.Create]]]]]
      nodeCollateralWithdrawals <- c
        .downField("nodeCollateralWithdrawals")
        .as[Option[SortedMap[Address, List[Signed[UpdateNodeCollateral.Withdraw]]]]]
      version <- c.downField("version").as[Option[SnapshotVersion]].map(_.getOrElse(SnapshotVersion("0.0.1")))
      slotCertificate <- c.downField("slotCertificate").as[Option[io.constellationnetwork.schema.nakamoto.slot.SlotCertificate]]
      eta <- c.downField("eta").as[Option[io.constellationnetwork.security.hash.Hash]]
      // WATCHTOWER fraud proofs (W3a) — forgiving like `shardCheckpoints`: absent in pre-watchtower fixtures ⇒ empty set.
      fraudProofs <- c
        .downField("fraudProofs")
        .as[Option[SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]]]
        .map(_.getOrElse(SortedSet.empty[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]))
    } yield
      GlobalIncrementalSnapshot(
        ordinal,
        height,
        subHeight,
        lastSnapshotHash,
        blocks,
        stateChannelSnapshots,
        shardCheckpoints,
        rewards,
        delegateRewards,
        epochProgress,
        nextFacilitators,
        tips,
        stateProof,
        allowSpendBlocks,
        tokenLockBlocks,
        spendActions,
        updateNodeParameters,
        artifacts,
        activeDelegatedStakes,
        delegatedStakesWithdrawals,
        activeNodeCollaterals,
        nodeCollateralWithdrawals,
        version,
        slotCertificate,
        eta,
        fraudProofs
      )
  }

  def fromGlobalSnapshot[F[_]: Parallel: Async: Hasher: JsonSerializer](snapshot: GlobalSnapshot)(
    implicit stateProofSelector: StateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): F[GlobalIncrementalSnapshot] = {
    val gsi = snapshot.info.toGlobalSnapshotInfo
    gsi.stateProof[F](snapshot.ordinal).map { stateProof =>
      GlobalIncrementalSnapshot(
        snapshot.ordinal,
        snapshot.height,
        snapshot.subHeight,
        snapshot.lastSnapshotHash,
        snapshot.blocks,
        snapshot.stateChannelSnapshots,
        SortedMap.empty[ShardId, ShardCheckpoint], // shardCheckpoints — pre-sharding genesis path; absence = empty map (§3.4)
        snapshot.rewards,
        Some(SortedMap.empty),
        snapshot.epochProgress,
        snapshot.nextFacilitators,
        snapshot.tips,
        stateProof,
        Some(SortedSet.empty),
        Some(SortedSet.empty),
        Some(SortedMap.empty),
        gsi.updateNodeParameters.map(_.map { case (k, v) => (k, v._1) }),
        Some(SortedSet.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty),
        Some(SortedMap.empty)
      )
    }
  }
}
