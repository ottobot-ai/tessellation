package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.InvalidStateProofSlashingConfig
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashManager.SlashedRegistryEntry
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotAcceptanceManager.{
  WatchtowerSlashRequest,
  applyWatchtowerSlashes
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.TokenLockStateManager
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** ECO-06 target regressions.
  *
  * These tests deliberately assert the required conservation semantics and are RED against the current active-only slash fold. Keep the
  * suite out of the green test aggregate until slash application atomically handles active and pending encumbrances, their exact backing
  * locks, and the corresponding expiry-index entries.
  */
object InvalidStateProofSlashPrincipalRedSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  private implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal.MinValue)
  private implicit val withdrawalTimeLimit: WithdrawalTimeLimit =
    WithdrawalTimeLimit.none

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(serializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, securityProvider, serializer)

  private val principal = 1_000L
  private val shard = ShardId(0).get
  private val checkpointHash = Hash("aa" * 32)
  private val eventOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L))
  private val eventEpoch = EpochProgress(NonNegLong.unsafeFrom(20L))
  private val withdrawalEpoch = EpochProgress(NonNegLong.unsafeFrom(21L))
  private val offender = PeerId(Hex("11" * 64))
  private val delegatedSource = Address.fromBytes("eco06-delegated".getBytes("UTF-8"))
  private val collateralSource = Address.fromBytes("eco06-collateral".getBytes("UTF-8"))
  private val submitter = Address.fromBytes("eco06-watchtower".getBytes("UTF-8"))

  private val proofs: NonEmptySet[SignatureProof] =
    NonEmptySet.one(SignatureProof(Id(Hex("22" * 64)), Signature(Hex("33" * 64))))

  private def delegatedCreate(backingRef: Hash): Signed[UpdateDelegatedStake.Create] =
    Signed(
      UpdateDelegatedStake.Create(
        source = delegatedSource,
        nodeId = offender,
        amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(principal)),
        fee = DelegatedStakeFee(NonNegLong.unsafeFrom(0L)),
        tokenLockRef = backingRef,
        parent = DelegatedStakeReference.empty
      ),
      proofs
    )

  private def collateralCreate(backingRef: Hash): Signed[UpdateNodeCollateral.Create] =
    Signed(
      UpdateNodeCollateral.Create(
        source = collateralSource,
        nodeId = offender,
        amount = NodeCollateralAmount(NonNegLong.unsafeFrom(principal)),
        fee = NodeCollateralFee(NonNegLong.unsafeFrom(0L)),
        tokenLockRef = backingRef,
        parent = NodeCollateralReference.empty
      ),
      proofs
    )

  private def activeDelegated(backingRef: Hash) =
    DelegatedStakeRecord(delegatedCreate(backingRef), eventOrdinal, Amount.empty)

  private def pendingDelegated(backingRef: Hash) =
    PendingDelegatedStakeWithdrawal(delegatedCreate(backingRef), Amount.empty, eventOrdinal, withdrawalEpoch)

  private def pendingCollateral(backingRef: Hash) =
    PendingNodeCollateralWithdrawal(collateralCreate(backingRef), eventOrdinal, withdrawalEpoch)

  private def backingLock(source: Address): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = source,
        amount = TokenLockAmount(PosLong.unsafeFrom(principal)),
        fee = TokenLockFee(NonNegLong.unsafeFrom(0L)),
        parent = TokenLockReference.empty,
        currencyId = none,
        unlockEpoch = none,
        replaceTokenLockRef = none
      ),
      proofs
    )

  private val delegatedBackingLock = backingLock(delegatedSource)
  private val collateralBackingLock = backingLock(collateralSource)

  private def refOf(lock: Signed[TokenLock])(implicit hasher: Hasher[IO]): IO[Hash] =
    TokenLockReference.of[IO](lock).map(_.hash)

  private val config =
    InvalidStateProofSlashingConfig(
      watchtowerEnabled = true,
      slashFraction = Ratio.One,
      bountyFraction = Ratio(1, 20),
      cooldownEpochs = 100L
    )

  private def apply(
    activeDelegatedStakes: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    activeNodeCollaterals: SortedMap[Address, SortedSet[NodeCollateralRecord]]
  ) =
    applyWatchtowerSlashes(
      requests = List(WatchtowerSlashRequest(shard, checkpointHash, List(offender), submitter)),
      priorDelegatedStakes = activeDelegatedStakes,
      priorNodeCollaterals = activeNodeCollaterals,
      postEconomicBalances = SortedMap.empty,
      eventOrdinal = eventOrdinal,
      currentEpoch = eventEpoch,
      config = config
    )

  test("RED ECO-06 active delegated slash debits the exact backing lock before paying a bounty") { res =>
    implicit val (hasher, _, _) = res

    refOf(delegatedBackingLock).map { backingRef =>
      val priorLocks = SortedMap(delegatedSource -> SortedSet(delegatedBackingLock))
      val slash =
        apply(SortedMap(delegatedSource -> SortedSet(activeDelegated(backingRef))), SortedMap.empty)

      // This is the current GSAM composition: the slash result replaces active stake records but has no token-lock delta.
      val postLocks = priorLocks
      val actualPrincipalDebit = principal - postLocks(delegatedSource).toList.map(_.amount.value.value).sum
      val bountyCredit = slash.bountyBalanceDelta.get(submitter).fold(0L)(_.value.value)
      val aggregateBefore = principal
      val aggregateAfter = postLocks(delegatedSource).toList.map(_.amount.value.value).sum + bountyCredit

      expect.all(
        slash.slashedDelegatedStakes.isEmpty,
        clue(actualPrincipalDebit) == clue(principal),
        clue(slash.totalBurned + bountyCredit) == clue(actualPrincipalDebit),
        clue(aggregateAfter) == clue(aggregateBefore)
      )
    }
  }

  test("RED ECO-06 pending delegated withdrawal remains slashable through unbond maturity") { res =>
    implicit val (hasher, _, _) = res

    refOf(delegatedBackingLock).map { backingRef =>
      val priorPending = SortedMap(delegatedSource -> SortedSet(pendingDelegated(backingRef)))
      val priorLocks = SortedMap(delegatedSource -> SortedSet(delegatedBackingLock))
      val slash = apply(SortedMap.empty, SortedMap.empty)

      // GSAM currently threads both maps around the active-only slash result unchanged.
      val postPending = priorPending
      val postLocks = priorLocks

      expect.all(
        slash.registryEntries.nonEmpty,
        clue(slash.totalBurned) == clue(principal - principal / 20L),
        clue(postPending) == clue(SortedMap.empty[Address, SortedSet[PendingDelegatedStakeWithdrawal]]),
        clue(postLocks) == clue(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])
      )
    }
  }

  test("RED ECO-06 pending node collateral remains slashable through unbond maturity") { res =>
    implicit val (hasher, _, _) = res

    refOf(collateralBackingLock).map { backingRef =>
      val priorPending = SortedMap(collateralSource -> SortedSet(pendingCollateral(backingRef)))
      val priorLocks = SortedMap(collateralSource -> SortedSet(collateralBackingLock))
      val slash = apply(SortedMap.empty, SortedMap.empty)

      val postPending = priorPending
      val postLocks = priorLocks

      expect.all(
        slash.registryEntries.nonEmpty,
        clue(slash.totalBurned) == clue(principal - principal / 20L),
        clue(postPending) == clue(SortedMap.empty[Address, SortedSet[PendingNodeCollateralWithdrawal]]),
        clue(postLocks) == clue(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])
      )
    }
  }

  test("RED ECO-06 durable double-slash dedup cannot finalize before pending principal is debited") { res =>
    implicit val (hasher, _, serializer) = res
    implicit val entryCodec: io.constellationnetwork.serde.ImmutableCodec[SlashedRegistryEntry] =
      InvalidStateProofSlashedReader.entryCodec
    for {
      backingRef <- refOf(delegatedBackingLock)
      slash = apply(SortedMap.empty, SortedMap.empty)
      pendingAfterSlash = SortedMap(delegatedSource -> SortedSet(pendingDelegated(backingRef)))
      locksAfterSlash = SortedMap(delegatedSource -> SortedSet(delegatedBackingLock))
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- slash.registryEntries.traverse_ { entry =>
        GlobalStateKey
          .slashingsKey[IO](entry.peerId, entry.shardId, entry.disputedCheckpointHash)
          .flatMap(store.insert[SlashedRegistryEntry](_, entry))
      }
      wasSlashed <- InvalidStateProofSlashedReader.fromMptStore[IO](store).wasSlashed(shard, checkpointHash)
    } yield
      expect.all(
        wasSlashed,
        !wasSlashed || pendingAfterSlash.isEmpty,
        !wasSlashed || locksAfterSlash.isEmpty,
        !wasSlashed || slash.totalBurned + slash.bountyBalanceDelta.values.map(_.value.value).sum == principal
      )
  }

  test("RED ECO-06 maturity cannot refund delegated principal after the offense is durably slashed") { res =>
    implicit val (hasher, _, serializer) = res
    val slash = apply(SortedMap.empty, SortedMap.empty)

    for {
      backingRef <- refOf(delegatedBackingLock)
      expired = SortedMap(delegatedSource -> SortedSet(pendingDelegated(backingRef)))
      byRef = Map(backingRef -> delegatedBackingLock)
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(
        GlobalSnapshotInfo.empty.copy(
          activeTokenLocks = SortedMap(delegatedSource -> SortedSet(delegatedBackingLock)).some
        ),
        eventOrdinal
      )
      manager = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))
      generated <- IO.fromEither(
        manager
          .generateTokenUnlocks(expired, SortedMap.empty, List.empty, byRef)
          .leftMap(new AssertionError(_))
      )
      lockState <- manager.acceptTokenLocksWithExpired(
        eventEpoch,
        SortedMap.empty,
        SortedMap(delegatedSource -> SortedSet(delegatedBackingLock)),
        generated,
        SortedMap.empty
      )
      balanceResult <- manager.updateGlobalBalancesByTokenLocksWithExpired(
        eventEpoch,
        SortedMap(delegatedSource -> Balance.empty),
        SortedMap.empty,
        generated,
        SortedMap.empty
      )
      balances <- IO.fromEither(balanceResult.leftMap(new AssertionError(_)))
    } yield
      expect.all(
        slash.registryEntries.nonEmpty,
        clue(generated) == clue(Map.empty[Address, List[io.constellationnetwork.schema.artifact.TokenUnlock]]),
        lockState.fullState.isEmpty,
        clue(balances._1.getOrElse(delegatedSource, Balance.empty)) == clue(Balance.empty)
      )
  }
}
