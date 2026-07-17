package io.constellationnetwork.serde

import cats.data.NonEmptySet

import scala.collection.immutable.SortedSet

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
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
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
  * Exercises: 1-variant ADT discriminators, `Signed[_]` around an ADT, Option[Hash], Option[DelegatedStakeAmount], and the composed record
  * types.
  */
object StakeCollateralCodecsSuite extends FunSuite {

  private val src = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
  private val nodeId = PeerId(Hex("deadbeef"))

  private def proof: SignatureProof =
    SignatureProof(Id(Hex("cafe")), Signature(Hex("beef")))

  private def alternateProof: SignatureProof =
    SignatureProof(Id(Hex("face")), Signature(Hex("feed")))

  private def verifySetContract[A: Ordering](
    codec: ImmutableCodec[SortedSet[A]],
    first: A,
    second: A,
    firstProofVariant: A,
    secondProofVariant: A
  ) = {
    val forward = SortedSet(first, second)
    val reverse = SortedSet(second, first)
    val proofVariants = SortedSet(firstProofVariant, secondProofVariant)
    val forwardBytes = codec.immutableBytes(forward)
    val reverseBytes = codec.immutableBytes(reverse)
    val proofVariantBytes = codec.immutableBytes(proofVariants)

    expect.all(
      forward.size == 2,
      reverse.size == 2,
      forwardBytes == reverseBytes,
      codec.fromImmutableBytes(forwardBytes) == Right(forward),
      proofVariants.size == 2,
      codec.fromImmutableBytes(proofVariantBytes) == Right(proofVariants)
    )
  }

  private def survivesSetRoundTrip[A: Ordering](codec: ImmutableCodec[SortedSet[A]], first: A, second: A): Boolean = {
    val records = SortedSet(first, second)
    records.size == 2 && codec.fromImmutableBytes(codec.immutableBytes(records)) == Right(records)
  }

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

  test("DelegatedStakeRecord sets preserve same-ordinal batching and alternate-proof identities") {
    val createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(51L))
    val firstCreate = UpdateDelegatedStake.Create(
      src,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(101L)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(1L)),
      Hash("1" * 64),
      DelegatedStakeReference.empty
    )
    val secondCreate = firstCreate.copy(
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(102L)),
      tokenLockRef = Hash("2" * 64)
    )
    val first = DelegatedStakeRecord(Signed(firstCreate, NonEmptySet.one(proof)), createdAt, Amount.empty)
    val second = DelegatedStakeRecord(Signed(secondCreate, NonEmptySet.one(proof)), createdAt, Amount.empty)
    val firstProofVariant = DelegatedStakeRecord(Signed(firstCreate, NonEmptySet.one(proof)), createdAt, Amount.empty)
    val secondProofVariant = DelegatedStakeRecord(Signed(firstCreate, NonEmptySet.one(alternateProof)), createdAt, Amount.empty)

    verifySetContract(delegatedStakeRecordSetCodec, first, second, firstProofVariant, secondProofVariant).and(
      expect.all(
        survivesSetRoundTrip(
          delegatedStakeRecordSetCodec,
          firstProofVariant,
          firstProofVariant.copy(currentTokenLockRef = Some(Hash("9" * 64)))
        ),
        survivesSetRoundTrip(
          delegatedStakeRecordSetCodec,
          firstProofVariant,
          firstProofVariant.copy(currentAmount = Some(DelegatedStakeAmount(NonNegLong.unsafeFrom(105L))))
        )
      )
    )
  }

  test("PendingDelegatedStakeWithdrawal sets preserve same-epoch batching and alternate-proof identities") {
    val createdAt = EpochProgress(NonNegLong.unsafeFrom(101L))
    val acceptedAt = SnapshotOrdinal(NonNegLong.unsafeFrom(52L))
    val firstCreate = UpdateDelegatedStake.Create(
      src,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(103L)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(1L)),
      Hash("3" * 64),
      DelegatedStakeReference.empty
    )
    val secondCreate = firstCreate.copy(
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(104L)),
      tokenLockRef = Hash("4" * 64)
    )
    val first = PendingDelegatedStakeWithdrawal(Signed(firstCreate, NonEmptySet.one(proof)), Amount.empty, acceptedAt, createdAt)
    val second = PendingDelegatedStakeWithdrawal(Signed(secondCreate, NonEmptySet.one(proof)), Amount.empty, acceptedAt, createdAt)
    val firstProofVariant =
      PendingDelegatedStakeWithdrawal(Signed(firstCreate, NonEmptySet.one(proof)), Amount.empty, acceptedAt, createdAt)
    val secondProofVariant =
      PendingDelegatedStakeWithdrawal(Signed(firstCreate, NonEmptySet.one(alternateProof)), Amount.empty, acceptedAt, createdAt)

    verifySetContract(pendingDelegatedStakeWithdrawalSetCodec, first, second, firstProofVariant, secondProofVariant).and(
      expect.all(
        survivesSetRoundTrip(
          pendingDelegatedStakeWithdrawalSetCodec,
          firstProofVariant,
          firstProofVariant.copy(acceptedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(53L)))
        ),
        survivesSetRoundTrip(
          pendingDelegatedStakeWithdrawalSetCodec,
          firstProofVariant,
          firstProofVariant.copy(currentTokenLockRef = Some(Hash("a" * 64)))
        ),
        survivesSetRoundTrip(
          pendingDelegatedStakeWithdrawalSetCodec,
          firstProofVariant,
          firstProofVariant.copy(currentAmount = Some(DelegatedStakeAmount(NonNegLong.unsafeFrom(106L))))
        )
      )
    )
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

  test("NodeCollateralRecord sets preserve same-ordinal batching and alternate-proof identities") {
    val createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(78L))
    val firstCreate = UpdateNodeCollateral.Create(
      src,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(501L)),
      NodeCollateralFee(NonNegLong.unsafeFrom(1L)),
      Hash("5" * 64),
      NodeCollateralReference.empty
    )
    val secondCreate = firstCreate.copy(
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(502L)),
      tokenLockRef = Hash("6" * 64)
    )
    val first = NodeCollateralRecord(Signed(firstCreate, NonEmptySet.one(proof)), createdAt)
    val second = NodeCollateralRecord(Signed(secondCreate, NonEmptySet.one(proof)), createdAt)
    val firstProofVariant = NodeCollateralRecord(Signed(firstCreate, NonEmptySet.one(proof)), createdAt)
    val secondProofVariant = NodeCollateralRecord(Signed(firstCreate, NonEmptySet.one(alternateProof)), createdAt)

    verifySetContract(
      nodeCollateralRecordSetCodec,
      first,
      second,
      firstProofVariant,
      secondProofVariant
    )
  }

  test("PendingNodeCollateralWithdrawal sets preserve same-epoch batching and alternate-proof identities") {
    val createdAt = EpochProgress(NonNegLong.unsafeFrom(201L))
    val acceptedAt = SnapshotOrdinal(NonNegLong.unsafeFrom(79L))
    val firstCreate = UpdateNodeCollateral.Create(
      src,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(503L)),
      NodeCollateralFee(NonNegLong.unsafeFrom(1L)),
      Hash("7" * 64),
      NodeCollateralReference.empty
    )
    val secondCreate = firstCreate.copy(
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(504L)),
      tokenLockRef = Hash("8" * 64)
    )
    val first = PendingNodeCollateralWithdrawal(Signed(firstCreate, NonEmptySet.one(proof)), acceptedAt, createdAt)
    val second = PendingNodeCollateralWithdrawal(Signed(secondCreate, NonEmptySet.one(proof)), acceptedAt, createdAt)
    val firstProofVariant = PendingNodeCollateralWithdrawal(Signed(firstCreate, NonEmptySet.one(proof)), acceptedAt, createdAt)
    val secondProofVariant = PendingNodeCollateralWithdrawal(Signed(firstCreate, NonEmptySet.one(alternateProof)), acceptedAt, createdAt)

    verifySetContract(pendingNodeCollateralWithdrawalSetCodec, first, second, firstProofVariant, secondProofVariant).and(
      expect(
        survivesSetRoundTrip(
          pendingNodeCollateralWithdrawalSetCodec,
          firstProofVariant,
          firstProofVariant.copy(acceptedOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(80L)))
        )
      )
    )
  }
}
