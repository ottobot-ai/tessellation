package io.constellationnetwork.node.shared.domain.economics

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object StakeBackingValidatorSuite extends MutableIOSuite {

  type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer
      .forAsync[IO]
      .asResource
      .map { implicit serializer =>
        Hasher.forJson[IO]
      }

  private val sourceA = Address.fromBytes("backing-source-a".getBytes("UTF-8"))
  private val sourceB = Address.fromBytes("backing-source-b".getBytes("UTF-8"))
  private val nodeId = PeerId(Hex("11" * 64))
  private val proof =
    SignatureProof(Id(Hex("22" * 64)), Signature(Hex("33" * 64)))
  private val proofs = NonEmptySet.one(proof)

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def signedLock(
    source: Address,
    amount: Long,
    unlockEpoch: Option[EpochProgress] = none,
    replaceRef: Option[Hash] = none
  ): Signed[TokenLock] =
    Signed(
      TokenLock(
        source,
        TokenLockAmount(PosLong.unsafeFrom(amount)),
        TokenLockFee(NonNegLong.unsafeFrom(0L)),
        TokenLockReference.empty,
        none,
        unlockEpoch,
        replaceRef
      ),
      proofs
    )

  private def pendingDelegated(
    source: Address,
    ref: Hash,
    amount: Long
  ): PendingDelegatedStakeWithdrawal = {
    val event = UpdateDelegatedStake.Create(
      source,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
      ref,
      DelegatedStakeReference.empty
    )
    PendingDelegatedStakeWithdrawal(Signed(event, proofs), Amount.empty, ordinal(2L), epoch(3L))
  }

  private def activeDelegated(
    source: Address,
    ref: Hash,
    amount: Long
  ): DelegatedStakeRecord = {
    val event = UpdateDelegatedStake.Create(
      source,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
      DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
      ref,
      DelegatedStakeReference.empty
    )
    DelegatedStakeRecord(Signed(event, proofs), ordinal(1L), Amount.empty)
  }

  private def pendingCollateral(
    source: Address,
    ref: Hash,
    amount: Long
  ): PendingNodeCollateralWithdrawal = {
    val event = UpdateNodeCollateral.Create(
      source,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(amount)),
      NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
      ref,
      NodeCollateralReference.empty
    )
    PendingNodeCollateralWithdrawal(Signed(event, proofs), ordinal(2L), epoch(3L))
  }

  private def refOf(lock: Signed[TokenLock])(implicit hasher: Hasher[IO]): IO[Hash] =
    TokenLockReference.of[IO](lock).map(_.hash)

  test("pending withdrawals are encumbered but carry zero active weight") { implicit hasher =>
    for {
      delegatedLock <- IO(signedLock(sourceA, 100L))
      collateralLock <- IO(signedLock(sourceB, 200L))
      delegatedRef <- refOf(delegatedLock)
      collateralRef <- refOf(collateralLock)
      state <- StakeBackingValidator.validateBackingState[IO](
        SortedMap(sourceA -> SortedSet(delegatedLock), sourceB -> SortedSet(collateralLock)),
        SortedMap.empty,
        SortedMap(sourceA -> SortedSet(pendingDelegated(sourceA, delegatedRef, 100L))),
        SortedMap.empty,
        SortedMap(sourceB -> SortedSet(pendingCollateral(sourceB, collateralRef, 200L)))
      )
    } yield
      expect.all(
        state.weightBearingBindings.isEmpty,
        state.encumbranceBindings.size == 2,
        state.encumbranceBindings.forall(!_.weightBearing),
        state.replacementRequirements.keySet == Set(delegatedRef, collateralRef)
      )
  }

  test("a pending delegated stake cannot move its principal into any replacement before maturity") { implicit hasher =>
    for {
      backing <- IO(signedLock(sourceA, 100L))
      backingRef <- refOf(backing)
      parent <- StakeBackingValidator.validateBackingState[IO](
        SortedMap(sourceA -> SortedSet(backing)),
        SortedMap.empty,
        SortedMap(sourceA -> SortedSet(pendingDelegated(sourceA, backingRef, 100L))),
        SortedMap.empty,
        SortedMap.empty
      )
      replacement = signedLock(sourceA, 100L, none, backingRef.some)
      result <- StakeBackingValidator.validateAcceptedReplacements[IO](parent, List(replacement)).attempt
    } yield expect(result.left.exists(_.isInstanceOf[StakeBackingValidator.PendingBackingReplacementUnsupported]))
  }

  test("pending collateral rejects replacement until collateral carries an effective reference") { implicit hasher =>
    for {
      backing <- IO(signedLock(sourceA, 100L))
      backingRef <- refOf(backing)
      parent <- StakeBackingValidator.validateBackingState[IO](
        SortedMap(sourceA -> SortedSet(backing)),
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(sourceA -> SortedSet(pendingCollateral(sourceA, backingRef, 100L)))
      )
      replacement = signedLock(sourceA, 100L, none, backingRef.some)
      result <- StakeBackingValidator.validateAcceptedReplacements[IO](parent, List(replacement)).attempt
    } yield expect(result.left.exists(_.isInstanceOf[StakeBackingValidator.PendingBackingReplacementUnsupported]))
  }

  test("one lock cannot be shared by active weight and a pending encumbrance") { implicit hasher =>
    for {
      backing <- IO(signedLock(sourceA, 100L))
      backingRef <- refOf(backing)
      result <- StakeBackingValidator
        .validateBackingState[IO](
          SortedMap(sourceA -> SortedSet(backing)),
          SortedMap(sourceA -> SortedSet(activeDelegated(sourceA, backingRef, 100L))),
          SortedMap.empty,
          SortedMap.empty,
          SortedMap(sourceA -> SortedSet(pendingCollateral(sourceA, backingRef, 100L)))
        )
        .attempt
    } yield expect(result.left.exists(_.isInstanceOf[StakeBackingValidator.DuplicateStakeBacking]))
  }

  test("maturity transition is valid only after both pending encumbrance and backing lock leave state") { implicit hasher =>
    for {
      backing <- IO(signedLock(sourceA, 100L))
      backingRef <- refOf(backing)
      parent <- StakeBackingValidator.validateBackingState[IO](
        SortedMap(sourceA -> SortedSet(backing)),
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap(sourceA -> SortedSet(pendingCollateral(sourceA, backingRef, 100L)))
      )
      missingLockResult <- StakeBackingValidator
        .validateTransition[IO](
          parent,
          SortedMap.empty,
          SortedMap.empty,
          SortedMap.empty,
          SortedMap.empty,
          SortedMap(sourceA -> SortedSet(pendingCollateral(sourceA, backingRef, 100L)))
        )
        .attempt
      released <- StakeBackingValidator.validateTransition[IO](
        parent,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty
      )
    } yield
      expect.all(
        missingLockResult.left.exists(_.isInstanceOf[StakeBackingValidator.MissingBackingTokenLock]),
        released.encumbranceBindings.isEmpty
      )
  }

  test("reward callbacks cannot drop or rewrite pending delegated withdrawals") { _ =>
    val pending = pendingDelegated(sourceA, Hash("44" * 32), 100L)
    val expected = SortedMap(sourceA -> SortedSet(pending))
    val rewritten = expected.updated(sourceA, SortedSet(pending.copy(createdAt = epoch(4L))))

    for {
      unchanged <- StakeBackingValidator.requirePendingStateUnchanged[IO](expected, expected).attempt
      dropped <- StakeBackingValidator.requirePendingStateUnchanged[IO](expected, SortedMap.empty).attempt
      changed <- StakeBackingValidator.requirePendingStateUnchanged[IO](expected, rewritten).attempt
    } yield
      expect(unchanged.isRight) &&
        expect(dropped.left.exists(_.isInstanceOf[StakeBackingValidator.RewardChangedPendingStakeBinding])) &&
        expect(changed.left.exists(_.isInstanceOf[StakeBackingValidator.RewardChangedPendingStakeBinding]))
  }
}
