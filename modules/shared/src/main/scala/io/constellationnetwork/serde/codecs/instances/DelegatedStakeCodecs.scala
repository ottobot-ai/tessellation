package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.{SnapshotOrdinal, balance}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}

import scodec.Codec
import scodec.codecs.{discriminated, uint8}
import shapeless.{::, HNil}

/** Canonical scodec codecs for the delegated-stake family:
  *   - `DelegatedStakeReference` — 40-byte ordinal+hash.
  *   - `UpdateDelegatedStake` — sealed ADT, currently one variant (`Create`).
  *   - `DelegatedStakeRecord` — 5-field record keyed on `Signed[UpdateDelegatedStake.Create]`.
  *   - `PendingDelegatedStakeWithdrawal` — 6-field record.
  *
  * UpdateDelegatedStake is encoded with a 1-byte discriminator today (0x00 = Create). The ADT is sealed but has only one variant; the
  * discriminator leaves 255 codepoints for future variants.
  */
object DelegatedStakeCodecs {

  private val ordinalRefCodec: Codec[DelegatedStakeOrdinal] = Codec[DelegatedStakeOrdinal]
  private val amountCodec: Codec[DelegatedStakeAmount] = Codec[DelegatedStakeAmount]
  private val feeCodec: Codec[DelegatedStakeFee] = Codec[DelegatedStakeFee]

  implicit val delegatedStakeReferenceCodec: Codec[DelegatedStakeReference] =
    (ordinalRefCodec :: hashCodec)
      .xmap[DelegatedStakeReference](
        { case o :: h :: HNil => DelegatedStakeReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val delegatedStakeReferenceImmutableCodec: ImmutableCodec[DelegatedStakeReference] =
    ImmutableCodec.fromScodecCodec(delegatedStakeReferenceCodec)

  // ---- UpdateDelegatedStake ADT -------------------------------------------

  val createCodec: Codec[UpdateDelegatedStake.Create] =
    (addressCodec :: peerIdCodec :: amountCodec :: feeCodec :: hashCodec :: delegatedStakeReferenceCodec)
      .xmap[UpdateDelegatedStake.Create](
        {
          case src :: nid :: amt :: fee :: tlRef :: parent :: HNil =>
            UpdateDelegatedStake.Create(src, nid, amt, fee, tlRef, parent)
        },
        c => c.source :: c.nodeId :: c.amount :: c.fee :: c.tokenLockRef :: c.parent :: HNil
      )

  val withdrawCodec: Codec[UpdateDelegatedStake.Withdraw] =
    (addressCodec :: hashCodec)
      .xmap[UpdateDelegatedStake.Withdraw](
        { case src :: ref :: HNil => UpdateDelegatedStake.Withdraw(src, ref) },
        w => w.source :: w.stakeRef :: HNil
      )

  implicit val updateDelegatedStakeCodec: Codec[UpdateDelegatedStake] =
    discriminated[UpdateDelegatedStake]
      .by(uint8)
      .typecase(0, createCodec)
      .typecase(1, withdrawCodec)

  implicit val updateDelegatedStakeImmutableCodec: ImmutableCodec[UpdateDelegatedStake] =
    ImmutableCodec.fromScodecCodec(updateDelegatedStakeCodec)

  private val signedCreateCodec: Codec[Signed[UpdateDelegatedStake.Create]] = signedCodecFor(createCodec)

  // ---- Record types --------------------------------------------------------

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val amountBalanceCodec: Codec[balance.Amount] = Codec[balance.Amount]
  private val tokenLockRefOptCodec: Codec[Option[Hash]] = option(hashCodec)
  private val currentAmountOptCodec: Codec[Option[DelegatedStakeAmount]] = option(amountCodec)

  implicit val delegatedStakeRecordCodec: Codec[DelegatedStakeRecord] =
    (signedCreateCodec :: ordinalCodec :: amountBalanceCodec :: tokenLockRefOptCodec :: currentAmountOptCodec)
      .xmap[DelegatedStakeRecord](
        {
          case evt :: createdAt :: rewards :: tlRef :: curAmt :: HNil =>
            DelegatedStakeRecord(evt, createdAt, rewards, tlRef, curAmt)
        },
        r => r.event :: r.createdAt :: r.rewards :: r.currentTokenLockRef :: r.currentAmount :: HNil
      )

  implicit val delegatedStakeRecordImmutableCodec: ImmutableCodec[DelegatedStakeRecord] =
    ImmutableCodec.fromScodecCodec(delegatedStakeRecordCodec)

  implicit val pendingDelegatedStakeWithdrawalCodec: Codec[PendingDelegatedStakeWithdrawal] =
    (signedCreateCodec :: amountBalanceCodec :: ordinalCodec :: epochCodec :: tokenLockRefOptCodec :: currentAmountOptCodec)
      .xmap[PendingDelegatedStakeWithdrawal](
        {
          case evt :: rewards :: accepted :: created :: tlRef :: curAmt :: HNil =>
            PendingDelegatedStakeWithdrawal(evt, rewards, accepted, created, tlRef, curAmt)
        },
        p =>
          p.event ::
            p.rewards ::
            p.acceptedOrdinal ::
            p.createdAt ::
            p.currentTokenLockRef ::
            p.currentAmount ::
            HNil
      )

  implicit val pendingDelegatedStakeWithdrawalImmutableCodec: ImmutableCodec[PendingDelegatedStakeWithdrawal] =
    ImmutableCodec.fromScodecCodec(pendingDelegatedStakeWithdrawalCodec)
}
