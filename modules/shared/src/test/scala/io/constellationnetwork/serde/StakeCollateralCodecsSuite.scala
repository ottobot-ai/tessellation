package io.constellationnetwork.serde

import cats.data.NonEmptySet

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs.{
  delegatedStakeRecordImmutableCodec,
  pendingDelegatedStakeWithdrawalImmutableCodec,
  updateDelegatedStakeImmutableCodec
}
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs.{
  nodeCollateralRecordImmutableCodec,
  pendingNodeCollateralWithdrawalImmutableCodec,
  updateNodeCollateralImmutableCodec
}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite for the delegated-stake + node-collateral families.
  *
  * Exercises: 1-variant ADT discriminators, `Signed[_]` around an ADT, Option[Hash],
  * Option[DelegatedStakeAmount], and the composed record types.
  */
object StakeCollateralCodecsSuite extends FunSuite {

  private val src = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private val nodeId = PeerId(Hex("deadbeef"))

  private def proof: SignatureProof =
    SignatureProof(Id(Hex("cafe")), Signature(Hex("beef")))

  // ---- DelegatedStake ------------------------------------------------------

  test("UpdateDelegatedStake.Create round-trips through the sealed-ADT codec") {
    val c = UpdateDelegatedStake.Create(
      source = src,
      nodeId = nodeId,
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(100L)),
      fee = DelegatedStakeFee(NonNegLong.unsafeFrom(1L)),
      tokenLockRef = Hash("a" * 64),
      parent = DelegatedStakeReference.empty
    )
    val bytes = updateDelegatedStakeImmutableCodec.immutableBytes(c)
    expect(bytes.fromImmutableBytes[UpdateDelegatedStake](updateDelegatedStakeImmutableCodec) == Right(c))
  }

  test("DelegatedStakeRecord round-trips with both optional fields present") {
    val create = UpdateDelegatedStake.Create(
      src,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(100L)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
      Hash("b" * 64),
      DelegatedStakeReference.empty
    )
    val rec = DelegatedStakeRecord(
      event = Signed(create, NonEmptySet.of(proof)),
      createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(50L)),
      rewards = Amount(NonNegLong.unsafeFrom(42L)),
      currentTokenLockRef = Some(Hash("c" * 64)),
      currentAmount = Some(DelegatedStakeAmount(NonNegLong.unsafeFrom(99L)))
    )
    val bytes = delegatedStakeRecordImmutableCodec.immutableBytes(rec)
    expect(bytes.fromImmutableBytes[DelegatedStakeRecord](delegatedStakeRecordImmutableCodec) == Right(rec))
  }

  test("PendingDelegatedStakeWithdrawal round-trips with both optional fields absent") {
    val create = UpdateDelegatedStake.Create(
      src,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(7L)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
      Hash("d" * 64),
      DelegatedStakeReference.empty
    )
    val pending = PendingDelegatedStakeWithdrawal(
      event = Signed(create, NonEmptySet.of(proof)),
      rewards = Amount(NonNegLong.unsafeFrom(0L)),
      acceptedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(10L)),
      createdAt = EpochProgress(NonNegLong.unsafeFrom(100L)),
      currentTokenLockRef = None,
      currentAmount = None
    )
    val bytes = pendingDelegatedStakeWithdrawalImmutableCodec.immutableBytes(pending)
    expect(bytes.fromImmutableBytes[PendingDelegatedStakeWithdrawal](pendingDelegatedStakeWithdrawalImmutableCodec) == Right(pending))
  }

  // ---- NodeCollateral ------------------------------------------------------

  test("UpdateNodeCollateral.Create round-trips through the sealed-ADT codec") {
    val c = UpdateNodeCollateral.Create(
      source = src,
      nodeId = nodeId,
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(1_000L)),
      fee = NodeCollateralFee(NonNegLong.unsafeFrom(5L)),
      tokenLockRef = Hash("e" * 64),
      parent = NodeCollateralReference.empty
    )
    val bytes = updateNodeCollateralImmutableCodec.immutableBytes(c)
    expect(bytes.fromImmutableBytes[UpdateNodeCollateral](updateNodeCollateralImmutableCodec) == Right(c))
  }

  test("NodeCollateralRecord round-trips") {
    val create = UpdateNodeCollateral.Create(
      src,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(500L)),
      NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
      Hash("f" * 64),
      NodeCollateralReference.empty
    )
    val rec = NodeCollateralRecord(
      event = Signed(create, NonEmptySet.of(proof)),
      createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(77L))
    )
    val bytes = nodeCollateralRecordImmutableCodec.immutableBytes(rec)
    expect(bytes.fromImmutableBytes[NodeCollateralRecord](nodeCollateralRecordImmutableCodec) == Right(rec))
  }

  test("PendingNodeCollateralWithdrawal round-trips") {
    val create = UpdateNodeCollateral.Create(
      src,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(500L)),
      NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
      Hash("f" * 64),
      NodeCollateralReference.empty
    )
    val pending = PendingNodeCollateralWithdrawal(
      event = Signed(create, NonEmptySet.of(proof)),
      acceptedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(12L)),
      createdAt = EpochProgress(NonNegLong.unsafeFrom(200L))
    )
    val bytes = pendingNodeCollateralWithdrawalImmutableCodec.immutableBytes(pending)
    expect(bytes.fromImmutableBytes[PendingNodeCollateralWithdrawal](pendingNodeCollateralWithdrawalImmutableCodec) == Right(pending))
  }
}
