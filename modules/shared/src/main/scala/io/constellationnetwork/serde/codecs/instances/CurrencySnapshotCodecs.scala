package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.FeeTransaction
import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSnapshotSync
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SharedArtifact
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.CurrencyMessage
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.swap.AllowSpendBlock
import io.constellationnetwork.schema.tokenLock.TokenLockBlock
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencyRecordCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.RewardTransactionCodec.{codec => rewardTransactionCodec}
import io.constellationnetwork.serde.codecs.instances.SharedArtifactCodec.sharedArtifactCodec
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codecs for the currency-snapshot family:
  *   - `CurrencyIncrementalSnapshotV1` (11 fields, legacy).
  *   - `CurrencyIncrementalSnapshot` (17 fields, current).
  *   - `CurrencySnapshot` (12 fields, full-snapshot variant).
  *
  * Closing the last big dependency before `GlobalSnapshotInfo`.
  */
object CurrencySnapshotCodecs {

  // ---- Shared field codecs ------------------------------------------------

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val heightCodec: Codec[Height] = Codec[Height]
  private val subHeightCodec: Codec[SubHeight] = Codec[SubHeight]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val versionCodec: Codec[SnapshotVersion] = snapshotVersionCodec
  private val blocksCodec: Codec[SortedSet[BlockAsActiveTip]] = sortedSet(blockAsActiveTipCodec)
  private val rewardsCodec: Codec[SortedSet[RewardTransaction]] = sortedSet(rewardTransactionCodec)
  private val tipsCodec: Codec[SnapshotTips] = snapshotTipsCodec
  private val optGlobalSyncViewCodec = option(globalSyncViewCodec)

  // ---- V1 (legacy) --------------------------------------------------------

  private val optDataApplicationV1Codec = option(dataApplicationPartV1Codec)

  implicit val currencyIncrementalSnapshotV1Codec: Codec[CurrencyIncrementalSnapshotV1] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      rewardsCodec ::
      tipsCodec ::
      currencySnapshotStateProofV1Codec ::
      epochCodec ::
      optDataApplicationV1Codec ::
      versionCodec)
      .xmap[CurrencyIncrementalSnapshotV1](
        {
          case o :: h :: sh :: lsh :: blks :: rws :: tp :: sp :: ep :: da :: v :: HNil =>
            CurrencyIncrementalSnapshotV1(o, h, sh, lsh, blks, rws, tp, sp, ep, da, v)
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.rewards ::
            s.tips ::
            s.stateProof ::
            s.epochProgress ::
            s.dataApplication ::
            s.version ::
            HNil
      )

  implicit val currencyIncrementalSnapshotV1ImmutableCodec: ImmutableCodec[CurrencyIncrementalSnapshotV1] =
    ImmutableCodec.fromScodecCodec(currencyIncrementalSnapshotV1Codec)

  // ---- Current CurrencyIncrementalSnapshot --------------------------------

  private val optDataApplicationCodec = option(dataApplicationPartCodec)
  private val signedCurrencyMessageCodec: Codec[Signed[CurrencyMessage]] = signedCodecFor(currencyMessageCodec)
  private val signedGlobalSyncCodec: Codec[Signed[GlobalSnapshotSync]] = signedCodecFor(globalSnapshotSyncCodec)
  private val signedFeeTransactionCodec: Codec[Signed[FeeTransaction]] = signedCodecFor(feeTransactionCodec)
  private val signedAllowSpendBlockCodec: Codec[Signed[AllowSpendBlock]] = signedCodecFor(allowSpendBlockCodec)
  private val signedTokenLockBlockCodec: Codec[Signed[TokenLockBlock]] = signedCodecFor(tokenLockBlockCodec)

  private val optMessagesCodec = option(sortedSet(signedCurrencyMessageCodec))
  private val optGlobalSyncsCodec = option(sortedSet(signedGlobalSyncCodec))
  private val optFeeTransactionsCodec = option(sortedSet(signedFeeTransactionCodec))
  private val optArtifactsCodec = option(sortedSet(sharedArtifactCodec))
  private val optAllowSpendBlocksCodec = option(sortedSet(signedAllowSpendBlockCodec))
  private val optTokenLockBlocksCodec = option(sortedSet(signedTokenLockBlockCodec))
  // The metagraph's authoritative cumulative balance map (the roots-only sharding security anchor — see
  // `CurrencyIncrementalSnapshot.authoritativeBalances`). Reuses the same `SortedMap[Address, Balance]` encoding as
  // `CurrencySnapshotInfo.balances` so the on-disk/scodec form stays byte-consistent with the Circe form.
  private val authoritativeBalancesMapCodec: Codec[SortedMap[Address, Balance]] = sortedMap(addressCodec, Codec[Balance])
  private val optAuthoritativeBalancesCodec = option(authoritativeBalancesMapCodec)

  implicit val currencyIncrementalSnapshotCodec: Codec[CurrencyIncrementalSnapshot] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      rewardsCodec ::
      tipsCodec ::
      currencySnapshotStateProofCodec ::
      epochCodec ::
      optDataApplicationCodec ::
      optMessagesCodec ::
      optGlobalSyncsCodec ::
      optFeeTransactionsCodec ::
      optArtifactsCodec ::
      optAllowSpendBlocksCodec ::
      optTokenLockBlocksCodec ::
      optGlobalSyncViewCodec ::
      optAuthoritativeBalancesCodec ::
      versionCodec)
      .xmap[CurrencyIncrementalSnapshot](
        {
          case o :: h :: sh :: lsh :: blks :: rws :: tp :: sp :: ep ::
              da :: msgs :: syncs :: fees :: artifacts :: asb :: tlb ::
              gsv :: authBal :: v :: HNil =>
            CurrencyIncrementalSnapshot(
              o,
              h,
              sh,
              lsh,
              blks,
              rws,
              tp,
              sp,
              ep,
              da,
              msgs,
              syncs,
              fees,
              artifacts,
              asb,
              tlb,
              gsv,
              authBal,
              v
            )
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.rewards ::
            s.tips ::
            s.stateProof ::
            s.epochProgress ::
            s.dataApplication ::
            s.messages ::
            s.globalSnapshotSyncs ::
            s.feeTransactions ::
            s.artifacts ::
            s.allowSpendBlocks ::
            s.tokenLockBlocks ::
            s.globalSyncView ::
            s.authoritativeBalances ::
            s.version ::
            HNil
      )

  implicit val currencyIncrementalSnapshotImmutableCodec: ImmutableCodec[CurrencyIncrementalSnapshot] =
    ImmutableCodec.fromScodecCodec(currencyIncrementalSnapshotCodec)

  // ---- CurrencySnapshot (full) --------------------------------------------

  implicit val currencySnapshotCodec: Codec[CurrencySnapshot] =
    (ordinalCodec ::
      heightCodec ::
      subHeightCodec ::
      hashCodec ::
      blocksCodec ::
      rewardsCodec ::
      tipsCodec ::
      currencySnapshotInfoV1Codec ::
      epochCodec ::
      optDataApplicationV1Codec ::
      optGlobalSyncViewCodec ::
      versionCodec)
      .xmap[CurrencySnapshot](
        {
          case o :: h :: sh :: lsh :: blks :: rws :: tp :: info :: ep :: da :: gsv :: v :: HNil =>
            CurrencySnapshot(o, h, sh, lsh, blks, rws, tp, info, ep, da, gsv, v)
        },
        s =>
          s.ordinal ::
            s.height ::
            s.subHeight ::
            s.lastSnapshotHash ::
            s.blocks ::
            s.rewards ::
            s.tips ::
            s.info ::
            s.epochProgress ::
            s.dataApplication ::
            s.globalSyncView ::
            s.version ::
            HNil
      )

  implicit val currencySnapshotImmutableCodec: ImmutableCodec[CurrencySnapshot] =
    ImmutableCodec.fromScodecCodec(currencySnapshotCodec)
}
