package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.Era
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, WithdrawalTimeLimit}
import io.constellationnetwork.schema.swap.AllowSpendReference
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.Hasher

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Optional state-proof fields are a canonical projection of authenticated MPT bytes, not the accompanying GSI's `Option` shape. */
object OptionShapeStateProofCanonicalizationSuite extends MutableIOSuite {

  override type Res = (JsonSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield (json, Hasher.forJson[IO])

  private implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal.MinValue)
  private implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  private val activationOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(10L))
  private val laterOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(20L))

  test("None and Some(empty) produce one canonical MPT proof shape") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2

    val absent = GlobalSnapshotInfo.empty.copy(lastAllowSpendRefs = None)
    val presentEmpty = absent.copy(lastAllowSpendRefs = Some(SortedMap.empty))

    for {
      absentProof <- absent.stateProof[IO](activationOrdinal)
      presentEmptyProof <- presentEmpty.stateProof[IO](activationOrdinal)
    } yield
      expect.all(
        absentProof.mptRoot.nonEmpty,
        absentProof == presentEmptyProof,
        absentProof.lastAllowSpendRefs.isEmpty,
        presentEmptyProof.lastAllowSpendRefs.isEmpty
      )
  }

  test("every optional per-field proof is absent when its authenticated partition is empty") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2

    val presentEmpty = GlobalSnapshotInfo.empty.copy(
      activeAllowSpends = Some(SortedMap.empty),
      activeTokenLocks = Some(SortedMap.empty),
      tokenLockBalances = Some(SortedMap.empty),
      lastAllowSpendRefs = Some(SortedMap.empty),
      lastTokenLockRefs = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty)
    )

    for {
      absentProof <- GlobalSnapshotInfo.empty.stateProof[IO](activationOrdinal)
      presentEmptyProof <- presentEmpty.stateProof[IO](activationOrdinal)
    } yield
      expect.all(
        absentProof == presentEmptyProof,
        presentEmptyProof.lastCurrencySnapshotsProof.isEmpty,
        presentEmptyProof.activeAllowSpends.isEmpty,
        presentEmptyProof.activeTokenLocks.isEmpty,
        presentEmptyProof.tokenLockBalances.isEmpty,
        presentEmptyProof.lastAllowSpendRefs.isEmpty,
        presentEmptyProof.lastTokenLockRefs.isEmpty,
        presentEmptyProof.updateNodeParameters.isEmpty,
        presentEmptyProof.activeDelegatedStakes.isEmpty,
        presentEmptyProof.delegatedStakesWithdrawals.isEmpty,
        presentEmptyProof.activeNodeCollaterals.isEmpty,
        presentEmptyProof.nodeCollateralWithdrawals.isEmpty,
        presentEmptyProof.priceState.isEmpty,
        presentEmptyProof.historicalStakeSnapshots.isEmpty
      )
  }

  test("era activation and ordinal-blind empty-delta replay produce one canonical proof") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2

    val era = Era(activationOrdinal, laterOrdinal, laterOrdinal)
    val prior = GlobalSnapshotInfo.empty.copy(lastAllowSpendRefs = None)

    val producer = prior.copy(
      lastAllowSpendRefs = era.postTess3(activationOrdinal)(SortedMap.empty[io.constellationnetwork.schema.address.Address, AllowSpendReference])
    )
    val replayed = GlobalStateConverter.applyAccumulatorToGSI(prior, StateChangesAccumulator())

    for {
      producerProof <- producer.stateProof[IO](activationOrdinal)
      replayedProof <- replayed.stateProof[IO](activationOrdinal)
    } yield
      expect.all(
        era.postTess3(activationOrdinal)(()).nonEmpty,
        producer.lastAllowSpendRefs.contains(
          SortedMap.empty[io.constellationnetwork.schema.address.Address, AllowSpendReference]
        ),
        replayed.lastAllowSpendRefs.isEmpty,
        producerProof == replayedProof,
        producerProof.lastAllowSpendRefs.isEmpty,
        replayedProof.lastAllowSpendRefs.isEmpty
      )
  }
}
