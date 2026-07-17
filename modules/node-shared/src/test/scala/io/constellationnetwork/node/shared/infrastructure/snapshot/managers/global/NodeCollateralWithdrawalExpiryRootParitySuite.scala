package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceResult
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.statechannel.StateChannelValidationType

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Regression for the former construction-site flag split: every GSAM must emit the node-collateral-withdrawal expiry-index delta, and the
  * producer writer, independent accumulator replay, and from-GSI rebuild must commit the same bytes and root.
  */
object NodeCollateralWithdrawalExpiryRootParitySuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val withdrawalLimit = EpochProgress(NonNegLong(4L))
  private implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.some(withdrawalLimit)
  private implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)

  private def freshStore(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  private def consensusBytes(entries: Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]) =
    GlobalStateKey.consensusRootEntries(entries).view.mapValues(_.toVector).toMap

  test("GSAM producer, accumulator replay, and GSI rebuild commit the same collateral-withdrawal expiry bucket") { res =>
    implicit val (h, sp, js) = res
    val priorOrdinal = SnapshotOrdinal(NonNegLong(1L))
    val nextOrdinal = SnapshotOrdinal(NonNegLong(2L))
    val acceptedEpoch = EpochProgress(NonNegLong(10L))
    val expiryEpoch = acceptedEpoch |+| withdrawalLimit

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = keyPair.getPublic.toAddress
      nodeId = PeerId.fromPublic(keyPair.getPublic)
      backingLock <- Signed.forAsyncHasher[IO, TokenLock](
        TokenLock(
          source = source,
          amount = TokenLockAmount(PosLong.unsafeFrom(1_000_000L)),
          fee = TokenLockFee(NonNegLong.unsafeFrom(0L)),
          parent = TokenLockReference.empty,
          currencyId = none,
          unlockEpoch = none,
          replaceTokenLockRef = none
        ),
        keyPair
      )
      backingRef <- TokenLockReference.of(backingLock)
      create = UpdateNodeCollateral.Create(
        source = source,
        nodeId = nodeId,
        amount = NodeCollateralAmount(NonNegLong(1_000_000L)),
        tokenLockRef = backingRef.hash
      )
      signedCreate <- Signed.forAsyncHasher[IO, UpdateNodeCollateral.Create](create, keyPair)
      collateralRef <- NodeCollateralReference.of(signedCreate)
      signedWithdraw <- Signed.forAsyncHasher[IO, UpdateNodeCollateral.Withdraw](
        UpdateNodeCollateral.Withdraw(source, collateralRef.hash),
        keyPair
      )
      priorInfo = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(source -> Balance(NonNegLong(1_000_000L))),
        activeTokenLocks = SortedMap(source -> SortedSet(backingLock)).some,
        activeNodeCollaterals = SortedMap(source -> SortedSet(NodeCollateralRecord(signedCreate, priorOrdinal))).some
      )
      forcedAcceptance = UpdateNodeCollateralAcceptanceResult(
        acceptedCreates = SortedMap.empty,
        notAcceptedCreates = List.empty,
        acceptedWithdrawals = SortedMap(source -> List((signedWithdraw, acceptedEpoch))),
        notAcceptedWithdrawals = List.empty
      )

      manager <- Mocks.mkManager(
        initialSnapshotInfo = priorInfo.some,
        forcedNodeCollateralAcceptanceResult = forcedAcceptance.some,
        lastLegacyStateProofOrdinal = SnapshotOrdinal.MinValue
      )
      result <- manager.accept(
        ordinal = nextOrdinal,
        epochProgress = acceptedEpoch,
        previousEpochProgress = EpochProgress(NonNegLong(9L)),
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List(signedWithdraw),
        lastSnapshotContext = priorInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = Mocks.delegatedRewardsFunction(priorInfo),
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => none[Hashed[GlobalIncrementalSnapshot]].pure[IO],
        parentTip = BranchId.passthrough
      )
      producedInfo = result._9
      signedStateProof = result._10
      accumulator = result._16

      maturityManager <- Mocks.mkManager(
        initialSnapshotInfo = producedInfo.some,
        lastLegacyStateProofOrdinal = SnapshotOrdinal.MinValue
      )
      maturityResult <- maturityManager.accept(
        ordinal = SnapshotOrdinal(NonNegLong(3L)),
        epochProgress = expiryEpoch,
        previousEpochProgress = acceptedEpoch,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = producedInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = Mocks.delegatedRewardsFunction(producedInfo),
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => none[Hashed[GlobalIncrementalSnapshot]].pure[IO],
        parentTip = BranchId.passthrough
      )
      maturedInfo = maturityResult._9
      maturityAccumulator = maturityResult._16

      followUpResult <- maturityManager.accept(
        ordinal = SnapshotOrdinal(NonNegLong(4L)),
        epochProgress = EpochProgress(NonNegLong.unsafeFrom(expiryEpoch.value.value + 1L)),
        previousEpochProgress = expiryEpoch,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = maturedInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = Mocks.delegatedRewardsFunction(maturedInfo),
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => none[Hashed[GlobalIncrementalSnapshot]].pure[IO],
        parentTip = BranchId.passthrough
      )
      followUpInfo = followUpResult._9
      followUpAccumulator = followUpResult._16
      maturityExpiryDelta = maturityAccumulator.nodeCollateralWithdrawalExpiryIndex match {
        case delta: SystemIndexDelta.EpochBucket[NodeCollateralWithdrawalExpiryKey] => delta
      }
      followUpExpiryDelta = followUpAccumulator.nodeCollateralWithdrawalExpiryIndex match {
        case delta: SystemIndexDelta.EpochBucket[NodeCollateralWithdrawalExpiryKey] => delta
      }

      hashedCreate <- signedCreate.toHashed
      expiryKey = NodeCollateralWithdrawalExpiryKey(source, hashedCreate.hash)
      expectedIndexDelta =
        SystemIndexDelta.EpochBucket[NodeCollateralWithdrawalExpiryKey](adds = SortedMap(expiryEpoch -> Set(expiryKey)))

      producerStore <- freshStore
      _ <- producerStore.syncFromGlobalSnapshotInfo(priorInfo, priorOrdinal)
      preBytes <- producerStore.allEntriesAsBytes
      _ <- producerStore.syncFromStateChanges(accumulator, nextOrdinal)
      producerBytes <- producerStore.allEntriesAsBytes
      producerRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](producerBytes)
      producerBucket <- producerStore.getExpiryBucket[NodeCollateralWithdrawalExpiryKey](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        expiryEpoch
      )

      delta <- GlobalStateConverter.toAccumulatorHexDelta[IO](accumulator, preBytes)
      (replayUpserts, replayRemoves) = delta
      replayBytes = replayUpserts.foldLeft(preBytes -- replayRemoves) {
        case (entries, (key, value)) => entries.updated(key, value)
      }
      replayRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](replayBytes)

      rebuildStore <- freshStore
      _ <- rebuildStore.syncFromGlobalSnapshotInfo(producedInfo, nextOrdinal)
      rebuildBytes <- rebuildStore.allEntriesAsBytes
      rebuildRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](rebuildBytes)
      rebuildBucket <- rebuildStore.getExpiryBucket[NodeCollateralWithdrawalExpiryKey](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        expiryEpoch
      )

      maturedRebuildStore <- freshStore
      _ <- maturedRebuildStore.syncFromGlobalSnapshotInfo(maturedInfo, SnapshotOrdinal(NonNegLong(3L)))
      maturedRebuildBucket <- maturedRebuildStore.getExpiryBucket[NodeCollateralWithdrawalExpiryKey](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        expiryEpoch
      )
    } yield
      expect.all(
        clue(accumulator.nodeCollateralWithdrawalExpiryIndex) == clue(expectedIndexDelta),
        clue(producerBucket) == clue(SortedSet(expiryKey).some),
        clue(rebuildBucket) == clue(SortedSet(expiryKey).some),
        clue(signedStateProof.mptRoot) == clue(producerRoot.some),
        clue(replayRoot) == clue(producerRoot),
        clue(rebuildRoot) == clue(producerRoot),
        clue(consensusBytes(replayBytes)) == clue(consensusBytes(producerBytes)),
        clue(consensusBytes(rebuildBytes)) == clue(consensusBytes(producerBytes)),
        producedInfo.nodeCollateralWithdrawals.flatMap(_.get(source)).exists(_.nonEmpty),
        producedInfo.activeTokenLocks.flatMap(_.get(source)).contains(SortedSet(backingLock)),
        maturedInfo.nodeCollateralWithdrawals.flatMap(_.get(source)).forall(_.isEmpty),
        maturedInfo.activeTokenLocks.flatMap(_.get(source)).forall(_.isEmpty),
        maturedInfo.balances(source).value.value - producedInfo.balances(source).value.value == 1_000_000L,
        maturityExpiryDelta.removes
          .get(expiryEpoch)
          .exists(_.contains(expiryKey)),
        maturedRebuildBucket.isEmpty,
        followUpInfo.nodeCollateralWithdrawals.flatMap(_.get(source)).forall(_.isEmpty),
        followUpInfo.activeTokenLocks.flatMap(_.get(source)).forall(_.isEmpty),
        followUpInfo.balances(source) == maturedInfo.balances(source),
        followUpExpiryDelta.isEmpty
      )
  }
}
