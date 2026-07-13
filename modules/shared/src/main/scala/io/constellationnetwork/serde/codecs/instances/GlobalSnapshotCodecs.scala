package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptyList

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SharedArtifact, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.nakamoto.slot.SlotCertificate
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId}
import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ListCodec.list
import io.constellationnetwork.serde.codecs.MapCodec.{map => plainMap}
import io.constellationnetwork.serde.codecs.NonEmptyListCodec.nonEmptyList
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SortedMapCodec.{sortedMap, sortedMapCanonical}
import io.constellationnetwork.serde.codecs.SortedSetCodec.{sortedSet, sortedSetCanonical}
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs.snapshotVersionCodec
import io.constellationnetwork.serde.codecs.instances.CurrencyRecordCodecs.{allowSpendBlockCodec, tokenLockBlockCodec}
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs.updateDelegatedStakeCodec
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotInfoV1Codec.{codec => globalSnapshotInfoV1Codec}
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotStateProofCodec._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationCertCodec
import io.constellationnetwork.serde.codecs.instances.NakamotoSlotCodecs.slotCertificateCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs.updateNodeCollateralCodec
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.RewardTransactionCodec.{codec => rewardTransactionCodec}
import io.constellationnetwork.serde.codecs.instances.ShardingScodecCodecs.{
  invalidStateProofEvidenceCodec,
  shardCheckpointCodec,
  shardIdCodec
}
import io.constellationnetwork.serde.codecs.instances.SharedArtifactCodec.{sharedArtifactCodec, spendActionCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StateChannelSnapshotBinaryCodec.{codec => scsbCodec}
import io.constellationnetwork.serde.codecs.instances.UpdateNodeParametersCodec.{updateNodeParametersCodec => unpCodec}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codecs for the top-level snapshot capstones:
  *   - `GlobalSnapshot` (full, 11 fields, legacy pre-incremental).
  *   - `GlobalIncrementalSnapshotV1` (12 fields, legacy incremental shape).
  *   - `GlobalIncrementalSnapshot` (27 fields, current shape with Nakamoto sharding, fraud proofs, and operator-key registrations).
  *
  * Final commit of the scodec codec library for the snapshot data path.
  */
object GlobalSnapshotCodecs {

  // ---- Shared piece codecs -------------------------------------------------

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val heightCodec: Codec[Height] = Codec[Height]
  private val subHeightCodec: Codec[SubHeight] = Codec[SubHeight]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val versionCodec: Codec[SnapshotVersion] = snapshotVersionCodec
  private val tipsCodec: Codec[SnapshotTips] = CurrencyRecordCodecs.snapshotTipsCodec
  private val blocksCodec: Codec[SortedSet[BlockAsActiveTip]] =
    sortedSetCanonical(CurrencyRecordCodecs.blockAsActiveTipCodec)
  private val rewardsCodec: Codec[SortedSet[RewardTransaction]] = sortedSet(rewardTransactionCodec)

  private val signedStateChannelBinaryCodec: Codec[Signed[StateChannelSnapshotBinary]] = signedCodecFor(scsbCodec)
  private val stateChannelSnapshotsCodec: Codec[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] =
    sortedMap(addressCodec, nonEmptyList(signedStateChannelBinaryCodec))

  private val facilitatorsCodec: Codec[NonEmptyList[PeerId]] = nonEmptyList(peerIdCodec)

  // ---- GlobalSnapshot (full, 11 fields) ----------------------------------

  implicit val globalSnapshotCodec: Codec[GlobalSnapshot] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      stateChannelSnapshotsCodec ::
      rewardsCodec ::
      epochCodec ::
      facilitatorsCodec ::
      globalSnapshotInfoV1Codec ::
      tipsCodec)
      .xmap[GlobalSnapshot](
        {
          case ord :: h :: sh :: lsh :: blks :: scs :: rws :: ep :: nf :: info :: tips :: HNil =>
            GlobalSnapshot(ord, h, sh, lsh, blks, scs, rws, ep, nf, info, tips)
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.stateChannelSnapshots ::
            s.rewards ::
            s.epochProgress ::
            s.nextFacilitators ::
            s.info ::
            s.tips ::
            HNil
      )

  implicit val globalSnapshotImmutableCodec: ImmutableCodec[GlobalSnapshot] =
    ImmutableCodec.fromScodecCodec(globalSnapshotCodec)

  // ---- GlobalIncrementalSnapshotV1 (12 fields) ----------------------------

  implicit val globalIncrementalSnapshotV1Codec: Codec[GlobalIncrementalSnapshotV1] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      stateChannelSnapshotsCodec ::
      rewardsCodec ::
      epochCodec ::
      facilitatorsCodec ::
      tipsCodec ::
      v1Codec ::
      versionCodec)
      .xmap[GlobalIncrementalSnapshotV1](
        {
          case ord :: h :: sh :: lsh :: blks :: scs :: rws :: ep :: nf :: tips :: sp :: v :: HNil =>
            GlobalIncrementalSnapshotV1(ord, h, sh, lsh, blks, scs, rws, ep, nf, tips, sp, v)
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.stateChannelSnapshots ::
            s.rewards ::
            s.epochProgress ::
            s.nextFacilitators ::
            s.tips ::
            s.stateProof ::
            s.version ::
            HNil
      )

  implicit val globalIncrementalSnapshotV1ImmutableCodec: ImmutableCodec[GlobalIncrementalSnapshotV1] =
    ImmutableCodec.fromScodecCodec(globalIncrementalSnapshotV1Codec)

  // ---- GlobalIncrementalSnapshot (current, 27 fields) ---------------------

  // Field 7: every checkpoint carries its replayable CL1 inputs. This field is mandatory in the greenfield schema.
  private val shardCheckpointsCodec: Codec[SortedMap[ShardId, ShardCheckpoint]] =
    sortedMap(shardIdCodec, shardCheckpointCodec)

  // Field 8: Option[SortedMap[PeerId, Map[Address, Amount]]]
  private val amountCodec: Codec[Amount] = Codec[Amount]
  private val delegateRewardsInnerCodec: Codec[Map[Address, Amount]] = plainMap(addressCodec, amountCodec)
  private val delegateRewardsMapCodec: Codec[SortedMap[PeerId, Map[Address, Amount]]] =
    sortedMapCanonical(peerIdCodec, delegateRewardsInnerCodec)
  private val delegateRewardsOptCodec = option(delegateRewardsMapCodec)

  // Fields 13..14: Option[SortedSet[Signed[AllowSpendBlock / TokenLockBlock]]]
  private val signedAllowSpendBlockCodec: Codec[Signed[AllowSpendBlock]] = signedCodecFor(allowSpendBlockCodec)
  private val signedTokenLockBlockCodec: Codec[Signed[TokenLockBlock]] = signedCodecFor(tokenLockBlockCodec)
  private val allowSpendBlocksOptCodec =
    option(sortedSetCanonical(signedAllowSpendBlockCodec))
  private val tokenLockBlocksOptCodec =
    option(sortedSetCanonical(signedTokenLockBlockCodec))

  // Field 15: Option[SortedMap[Address, List[SpendAction]]]
  private val spendActionsMapCodec: Codec[SortedMap[Address, List[SpendAction]]] =
    sortedMap(addressCodec, list(spendActionCodec))
  private val spendActionsOptCodec = option(spendActionsMapCodec)

  // Field 16: Option[SortedMap[Id, Signed[UpdateNodeParameters]]]
  private val idCodec: Codec[Id] = SignatureCodecs.idCodec
  private val signedUnpCodec: Codec[Signed[UpdateNodeParameters]] = signedCodecFor(unpCodec)
  private val updateNodeParametersMapCodec: Codec[SortedMap[Id, Signed[UpdateNodeParameters]]] =
    sortedMapCanonical(idCodec, signedUnpCodec)
  private val updateNodeParametersOptCodec = option(updateNodeParametersMapCodec)

  // Field 17: Option[SortedSet[SharedArtifact]]
  private val artifactsOptCodec = option(sortedSetCanonical(sharedArtifactCodec))

  // Fields 18..21: four `Option[SortedMap[Address, List[Signed[UpdateDelegatedStake/NodeCollateral.Create/Withdraw]]]]`
  // Use the parent `UpdateDelegatedStake` / `UpdateNodeCollateral` codecs for polymorphic decode on
  // the concrete subtype (both Create and Withdraw serialize through the 1-byte discriminator).
  private val signedUdsCreateCodec: Codec[Signed[UpdateDelegatedStake.Create]] =
    signedCodecFor(io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs.createCodec)
  private val signedUdsWithdrawCodec: Codec[Signed[UpdateDelegatedStake.Withdraw]] =
    signedCodecFor(io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs.withdrawCodec)
  private val signedUncCreateCodec: Codec[Signed[UpdateNodeCollateral.Create]] =
    signedCodecFor(io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs.createCodec)
  private val signedUncWithdrawCodec: Codec[Signed[UpdateNodeCollateral.Withdraw]] =
    signedCodecFor(io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs.withdrawCodec)

  private val activeDelegatedStakesMapCodec: Codec[SortedMap[Address, List[Signed[UpdateDelegatedStake.Create]]]] =
    sortedMap(addressCodec, list(signedUdsCreateCodec))
  private val delegatedStakesWithdrawalsMapCodec: Codec[SortedMap[Address, List[Signed[UpdateDelegatedStake.Withdraw]]]] =
    sortedMap(addressCodec, list(signedUdsWithdrawCodec))
  private val activeNodeCollateralsMapCodec: Codec[SortedMap[Address, List[Signed[UpdateNodeCollateral.Create]]]] =
    sortedMap(addressCodec, list(signedUncCreateCodec))
  private val nodeCollateralWithdrawalsMapCodec: Codec[SortedMap[Address, List[Signed[UpdateNodeCollateral.Withdraw]]]] =
    sortedMap(addressCodec, list(signedUncWithdrawCodec))

  private val activeDelegatedStakesOptCodec = option(activeDelegatedStakesMapCodec)
  private val delegatedStakesWithdrawalsOptCodec = option(delegatedStakesWithdrawalsMapCodec)
  private val activeNodeCollateralsOptCodec = option(activeNodeCollateralsMapCodec)
  private val nodeCollateralWithdrawalsOptCodec = option(nodeCollateralWithdrawalsMapCodec)

  // Fields 23..25: version, slotCertificate, eta
  private val slotCertificateOptCodec: Codec[Option[SlotCertificate]] = option(slotCertificateCodec)
  private val etaOptCodec: Codec[Option[Hash]] = option(hashCodec)

  // Field 26: self-contained invalid-state-proof evidence. This field is mandatory in the greenfield schema.
  private val fraudProofsCodec: Codec[SortedSet[InvalidStateProofEvidence]] =
    sortedSetCanonical(invalidStateProofEvidenceCodec)
  // Field 27: exact-parent unified operator KES+VRF registrations accepted into this snapshot.
  private val operatorKeyRegistrationsCodec: Codec[SortedSet[Signed[KesRegistrationCert]]] =
    sortedSetCanonical(signedCodecFor(kesRegistrationCertCodec))

  // Keep the parent ADT codecs referenced so their imports survive scalafix.
  private val _udsADT = updateDelegatedStakeCodec
  private val _uncADT = updateNodeCollateralCodec
  locally { val _ = (_udsADT, _uncADT) }

  implicit val globalIncrementalSnapshotCodec: Codec[GlobalIncrementalSnapshot] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      stateChannelSnapshotsCodec ::
      shardCheckpointsCodec ::
      rewardsCodec ::
      delegateRewardsOptCodec ::
      epochCodec ::
      facilitatorsCodec ::
      tipsCodec ::
      codec ::
      allowSpendBlocksOptCodec ::
      tokenLockBlocksOptCodec ::
      spendActionsOptCodec ::
      updateNodeParametersOptCodec ::
      artifactsOptCodec ::
      activeDelegatedStakesOptCodec ::
      delegatedStakesWithdrawalsOptCodec ::
      activeNodeCollateralsOptCodec ::
      nodeCollateralWithdrawalsOptCodec ::
      versionCodec ::
      slotCertificateOptCodec ::
      etaOptCodec ::
      fraudProofsCodec ::
      operatorKeyRegistrationsCodec)
      .xmap[GlobalIncrementalSnapshot](
        {
          case ord :: h :: sh :: lsh :: blks :: scs :: shardCheckpoints :: rws :: dr ::
              ep :: nf :: tips :: sp :: asb :: tlb :: sa :: unp ::
              art :: ads :: dsw :: anc :: ncw :: v :: slot :: eta :: fraudProofs :: registrations :: HNil =>
            GlobalIncrementalSnapshot(
              ord,
              h,
              sh,
              lsh,
              blks,
              scs,
              shardCheckpoints,
              rws,
              dr,
              ep,
              nf,
              tips,
              sp,
              asb,
              tlb,
              sa,
              unp,
              art,
              ads,
              dsw,
              anc,
              ncw,
              v,
              slot,
              eta,
              fraudProofs,
              registrations
            )
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.stateChannelSnapshots ::
            s.shardCheckpoints ::
            s.rewards ::
            s.delegateRewards ::
            s.epochProgress ::
            s.nextFacilitators ::
            s.tips ::
            s.stateProof ::
            s.allowSpendBlocks ::
            s.tokenLockBlocks ::
            s.spendActions ::
            s.updateNodeParameters ::
            s.artifacts ::
            s.activeDelegatedStakes ::
            s.delegatedStakesWithdrawals ::
            s.activeNodeCollaterals ::
            s.nodeCollateralWithdrawals ::
            s.version ::
            s.slotCertificate ::
            s.eta ::
            s.fraudProofs ::
            s.operatorKeyRegistrations ::
            HNil
      )

  implicit val globalIncrementalSnapshotImmutableCodec: ImmutableCodec[GlobalIncrementalSnapshot] =
    ImmutableCodec.fromScodecCodec(globalIncrementalSnapshotCodec)
}
