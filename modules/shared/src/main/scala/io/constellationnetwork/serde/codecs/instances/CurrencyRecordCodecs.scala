package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptySet

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSnapshotSyncOrdinal, GlobalSyncView}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ByteArrayCodec.{codec => byteArrayCodec}
import io.constellationnetwork.serde.codecs.ListCodec.list
import io.constellationnetwork.serde.codecs.NonEmptySetCodec.nonEmptySet
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.Primitives._
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.BlockCodec.{codec => blockCodec}
import io.constellationnetwork.serde.codecs.instances.BlockReferenceCodec.{codec => blockReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Round of simple currency-path record codecs — the structural middle layer between atoms and the big snapshot compounds.
  */
object CurrencyRecordCodecs {

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]

  // ---- GlobalSnapshotSync + GlobalSyncView ---------------------------------

  private val syncOrdinalCodec: Codec[GlobalSnapshotSyncOrdinal] = Codec[GlobalSnapshotSyncOrdinal]

  implicit val globalSnapshotSyncCodec: Codec[GlobalSnapshotSync] =
    (syncOrdinalCodec :: ordinalCodec :: hashCodec :: sessionTokenCodec)
      .xmap[GlobalSnapshotSync](
        { case p :: ord :: h :: s :: HNil => GlobalSnapshotSync(p, ord, h, s) },
        g => g.parentOrdinal :: g.globalSnapshotOrdinal :: g.globalSnapshotHash :: g.session :: HNil
      )

  implicit val globalSnapshotSyncImmutableCodec: ImmutableCodec[GlobalSnapshotSync] =
    ImmutableCodec.fromScodecCodec(globalSnapshotSyncCodec)

  implicit val globalSyncViewCodec: Codec[GlobalSyncView] =
    (ordinalCodec :: hashCodec :: epochCodec)
      .xmap[GlobalSyncView](
        { case o :: h :: e :: HNil => GlobalSyncView(o, h, e) },
        g => g.ordinal :: g.hash :: g.epochProgress :: HNil
      )

  implicit val globalSyncViewImmutableCodec: ImmutableCodec[GlobalSyncView] =
    ImmutableCodec.fromScodecCodec(globalSyncViewCodec)

  // ---- DataApplicationPart(V1) --------------------------------------------

  import io.constellationnetwork.currency.schema.currency.{DataApplicationPart, DataApplicationPartV1}

  private val blocksListCodec: Codec[List[Array[Byte]]] = list(byteArrayCodec)
  private val sortedHashSetCodec: Codec[SortedSet[Hash]] = sortedSet(hashCodec)
  private val updateHashesOptCodec: Codec[Option[SortedSet[Hash]]] = option(sortedHashSetCodec)

  implicit val dataApplicationPartV1Codec: Codec[DataApplicationPartV1] =
    (byteArrayCodec :: blocksListCodec :: hashCodec)
      .xmap[DataApplicationPartV1](
        { case st :: bs :: h :: HNil => DataApplicationPartV1(st, bs, h) },
        p => p.onChainState :: p.blocks :: p.calculatedStateProof :: HNil
      )

  implicit val dataApplicationPartV1ImmutableCodec: ImmutableCodec[DataApplicationPartV1] =
    ImmutableCodec.fromScodecCodec(dataApplicationPartV1Codec)

  implicit val dataApplicationPartCodec: Codec[DataApplicationPart] =
    (byteArrayCodec :: blocksListCodec :: hashCodec :: updateHashesOptCodec)
      .xmap[DataApplicationPart](
        { case st :: bs :: h :: uh :: HNil => DataApplicationPart(st, bs, h, uh) },
        p => p.onChainState :: p.blocks :: p.calculatedStateProof :: p.updateHashes :: HNil
      )

  implicit val dataApplicationPartImmutableCodec: ImmutableCodec[DataApplicationPart] =
    ImmutableCodec.fromScodecCodec(dataApplicationPartCodec)

  // ---- Tip types + SnapshotTips -------------------------------------------

  implicit val activeTipCodec: Codec[ActiveTip] =
    (blockReferenceCodec :: nonNegLongCodec :: ordinalCodec)
      .xmap[ActiveTip](
        { case br :: uc :: intro :: HNil => ActiveTip(br, uc, intro) },
        a => a.block :: a.usageCount :: a.introducedAt :: HNil
      )

  implicit val activeTipImmutableCodec: ImmutableCodec[ActiveTip] =
    ImmutableCodec.fromScodecCodec(activeTipCodec)

  implicit val deprecatedTipCodec: Codec[DeprecatedTip] =
    (blockReferenceCodec :: ordinalCodec)
      .xmap[DeprecatedTip](
        { case br :: dep :: HNil => DeprecatedTip(br, dep) },
        d => d.block :: d.deprecatedAt :: HNil
      )

  implicit val deprecatedTipImmutableCodec: ImmutableCodec[DeprecatedTip] =
    ImmutableCodec.fromScodecCodec(deprecatedTipCodec)

  private val deprecatedSetCodec: Codec[SortedSet[DeprecatedTip]] = sortedSet(deprecatedTipCodec)
  private val activeSetCodec: Codec[SortedSet[ActiveTip]] = sortedSet(activeTipCodec)

  implicit val snapshotTipsCodec: Codec[SnapshotTips] =
    (deprecatedSetCodec :: activeSetCodec)
      .xmap[SnapshotTips](
        { case d :: a :: HNil => SnapshotTips(d, a) },
        s => s.deprecated :: s.remainedActive :: HNil
      )

  implicit val snapshotTipsImmutableCodec: ImmutableCodec[SnapshotTips] =
    ImmutableCodec.fromScodecCodec(snapshotTipsCodec)

  // ---- BlockAsActiveTip ---------------------------------------------------

  private val signedBlockCodec: Codec[Signed[Block]] = signedCodecFor(blockCodec)

  implicit val blockAsActiveTipCodec: Codec[BlockAsActiveTip] =
    (signedBlockCodec :: nonNegLongCodec)
      .xmap[BlockAsActiveTip](
        { case b :: uc :: HNil => BlockAsActiveTip(b, uc) },
        t => t.block :: t.usageCount :: HNil
      )

  implicit val blockAsActiveTipImmutableCodec: ImmutableCodec[BlockAsActiveTip] =
    ImmutableCodec.fromScodecCodec(blockAsActiveTipCodec)

  // ---- AllowSpendBlock / TokenLockBlock -----------------------------------

  import io.constellationnetwork.schema.swap.AllowSpendBlock
  import io.constellationnetwork.schema.tokenLock.TokenLockBlock

  private val signedAllowSpendCodec: Codec[Signed[AllowSpend]] = signedCodecFor(allowSpendCodec)
  private val signedTokenLockCodec: Codec[Signed[TokenLock]] = signedCodecFor(tokenLockCodec)
  private val allowSpendNesCodec: Codec[NonEmptySet[Signed[AllowSpend]]] = nonEmptySet(signedAllowSpendCodec)
  private val tokenLockNesCodec: Codec[NonEmptySet[Signed[TokenLock]]] = nonEmptySet(signedTokenLockCodec)

  implicit val allowSpendBlockCodec: Codec[AllowSpendBlock] =
    (roundIdCodec :: allowSpendNesCodec)
      .xmap[AllowSpendBlock](
        { case r :: txs :: HNil => AllowSpendBlock(r, txs) },
        b => b.roundId :: b.transactions :: HNil
      )

  implicit val allowSpendBlockImmutableCodec: ImmutableCodec[AllowSpendBlock] =
    ImmutableCodec.fromScodecCodec(allowSpendBlockCodec)

  implicit val tokenLockBlockCodec: Codec[TokenLockBlock] =
    (roundIdCodec :: tokenLockNesCodec)
      .xmap[TokenLockBlock](
        { case r :: locks :: HNil => TokenLockBlock(r, locks) },
        b => b.roundId :: b.tokenLocks :: HNil
      )

  implicit val tokenLockBlockImmutableCodec: ImmutableCodec[TokenLockBlock] =
    ImmutableCodec.fromScodecCodec(tokenLockBlockCodec)

}
