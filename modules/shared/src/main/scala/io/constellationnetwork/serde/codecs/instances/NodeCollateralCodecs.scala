package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}

import scodec.Codec
import scodec.codecs.{discriminated, uint8}
import shapeless.{::, HNil}

/** Canonical scodec codecs for the node-collateral family:
  *   - `NodeCollateralReference` — 40-byte ordinal+hash.
  *   - `UpdateNodeCollateral` — sealed ADT, currently one variant (`Create`).
  *   - `NodeCollateralRecord` — 2-field record.
  *   - `PendingNodeCollateralWithdrawal` — 3-field record.
  *
  * Parallels `DelegatedStakeCodecs` — same discriminator pattern, same `Signed[...]` composition.
  */
object NodeCollateralCodecs {

  private val ordinalRefCodec: Codec[NodeCollateralOrdinal] = Codec[NodeCollateralOrdinal]
  private val amountCodec: Codec[NodeCollateralAmount] = Codec[NodeCollateralAmount]
  private val feeCodec: Codec[NodeCollateralFee] = Codec[NodeCollateralFee]

  implicit val nodeCollateralReferenceCodec: Codec[NodeCollateralReference] =
    (ordinalRefCodec :: hashCodec)
      .xmap[NodeCollateralReference](
        { case o :: h :: HNil => NodeCollateralReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val nodeCollateralReferenceImmutableCodec: ImmutableCodec[NodeCollateralReference] =
    ImmutableCodec.fromScodecCodec(nodeCollateralReferenceCodec)

  // ---- UpdateNodeCollateral ADT -------------------------------------------

  val createCodec: Codec[UpdateNodeCollateral.Create] =
    (addressCodec :: peerIdCodec :: amountCodec :: feeCodec :: hashCodec :: nodeCollateralReferenceCodec)
      .xmap[UpdateNodeCollateral.Create](
        {
          case src :: nid :: amt :: fee :: tlRef :: parent :: HNil =>
            UpdateNodeCollateral.Create(src, nid, amt, fee, tlRef, parent)
        },
        c => c.source :: c.nodeId :: c.amount :: c.fee :: c.tokenLockRef :: c.parent :: HNil
      )

  val withdrawCodec: Codec[UpdateNodeCollateral.Withdraw] =
    (addressCodec :: hashCodec)
      .xmap[UpdateNodeCollateral.Withdraw](
        { case src :: ref :: HNil => UpdateNodeCollateral.Withdraw(src, ref) },
        w => w.source :: w.collateralRef :: HNil
      )

  implicit val updateNodeCollateralCodec: Codec[UpdateNodeCollateral] =
    discriminated[UpdateNodeCollateral]
      .by(uint8)
      .typecase(0, createCodec)
      .typecase(1, withdrawCodec)

  implicit val updateNodeCollateralImmutableCodec: ImmutableCodec[UpdateNodeCollateral] =
    ImmutableCodec.fromScodecCodec(updateNodeCollateralCodec)

  private val signedCreateCodec: Codec[Signed[UpdateNodeCollateral.Create]] = signedCodecFor(createCodec)

  // ---- Record types --------------------------------------------------------

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]

  implicit val nodeCollateralRecordCodec: Codec[NodeCollateralRecord] =
    (signedCreateCodec :: ordinalCodec)
      .xmap[NodeCollateralRecord](
        { case evt :: created :: HNil => NodeCollateralRecord(evt, created) },
        r => r.event :: r.createdAt :: HNil
      )

  implicit val nodeCollateralRecordImmutableCodec: ImmutableCodec[NodeCollateralRecord] =
    ImmutableCodec.fromScodecCodec(nodeCollateralRecordCodec)

  implicit val pendingNodeCollateralWithdrawalCodec: Codec[PendingNodeCollateralWithdrawal] =
    (signedCreateCodec :: ordinalCodec :: epochCodec)
      .xmap[PendingNodeCollateralWithdrawal](
        {
          case evt :: accepted :: created :: HNil =>
            PendingNodeCollateralWithdrawal(evt, accepted, created)
        },
        p => p.event :: p.acceptedOrdinal :: p.createdAt :: HNil
      )

  implicit val pendingNodeCollateralWithdrawalImmutableCodec: ImmutableCodec[PendingNodeCollateralWithdrawal] =
    ImmutableCodec.fromScodecCodec(pendingNodeCollateralWithdrawalCodec)
}
