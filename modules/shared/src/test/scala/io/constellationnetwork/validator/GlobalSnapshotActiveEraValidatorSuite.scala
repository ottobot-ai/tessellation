package io.constellationnetwork.validator

import cats.data.NonEmptyList
import cats.effect.IO

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.auto._
import weaver.SimpleIOSuite

object GlobalSnapshotActiveEraValidatorSuite extends SimpleIOSuite {

  private val forbiddenRoot = Hash("ab" * 32)

  private val proof = GlobalSnapshotStateProof(
    lastStateChannelSnapshotHashesProof = Hash("11" * 32),
    lastTxRefsProof = Hash("22" * 32),
    balancesProof = Hash("33" * 32),
    lastCurrencySnapshotsProof = None,
    activeAllowSpends = None,
    activeTokenLocks = None,
    tokenLockBalances = None,
    lastAllowSpendRefs = None,
    lastTokenLockRefs = None,
    updateNodeParameters = None,
    activeDelegatedStakes = None,
    delegatedStakesWithdrawals = None,
    activeNodeCollaterals = None,
    nodeCollateralWithdrawals = None,
    priceState = None,
    lastGlobalSnapshotsWithCurrency = None,
    mptRoot = Some(Hash("44" * 32)),
    historicalStakeSnapshots = None,
    smtRoot = None
  )

  private val snapshot = GlobalIncrementalSnapshot(
    ordinal = SnapshotOrdinal.unsafeApply(12L),
    height = Height(0L),
    subHeight = SubHeight.MinValue,
    lastSnapshotHash = Hash.empty,
    blocks = SortedSet.empty,
    stateChannelSnapshots = SortedMap.empty,
    shardCheckpoints = SortedMap.empty,
    rewards = SortedSet.empty,
    delegateRewards = Some(SortedMap.empty),
    epochProgress = EpochProgress.MinValue,
    nextFacilitators = NonEmptyList.one(PeerId(Hex("aa" * 64))),
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    stateProof = proof,
    allowSpendBlocks = Some(SortedSet.empty),
    tokenLockBlocks = Some(SortedSet.empty),
    spendActions = Some(SortedMap.empty),
    updateNodeParameters = Some(SortedMap.empty),
    artifacts = Some(SortedSet.empty),
    activeDelegatedStakes = Some(SortedMap.empty),
    delegatedStakesWithdrawals = Some(SortedMap.empty),
    activeNodeCollaterals = Some(SortedMap.empty),
    nodeCollateralWithdrawals = Some(SortedMap.empty),
    version = SnapshotVersion("0.0.1"),
    slotCertificate = None,
    eta = None
  )

  pureTest("accepts the active-era shape with no historical commitment SMT root") {
    expect.same(GlobalSnapshotActiveEraValidator.validate(snapshot), Right(()))
  }

  pureTest("rejects a historical commitment SMT root before activation") {
    val invalid = snapshot.copy(stateProof = proof.copy(smtRoot = Some(forbiddenRoot)))

    GlobalSnapshotActiveEraValidator.validate(invalid) match {
      case Left(GlobalSnapshotActiveEraValidator.HistoricalCommitmentSmtRootNotActive(ordinal, root)) =>
        expect.same(ordinal, invalid.ordinal).and(expect.same(root, forbiddenRoot))
      case other => failure(s"Expected HistoricalCommitmentSmtRootNotActive, got $other")
    }
  }

  test("effectful validation fails with the typed violation") {
    val invalid = snapshot.copy(stateProof = proof.copy(smtRoot = Some(forbiddenRoot)))

    GlobalSnapshotActiveEraValidator.requireValid[IO](invalid).attempt.map {
      case Left(_: GlobalSnapshotActiveEraValidator.HistoricalCommitmentSmtRootNotActive) => success
      case other => failure(s"Expected HistoricalCommitmentSmtRootNotActive, got $other")
    }
  }
}
