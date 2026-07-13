package io.constellationnetwork.node.shared.domain.swap.block

import java.util.UUID

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.AllowSpendStateManager
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.round.RoundId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object AllowSpendBlockAcceptanceLogicSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, sp)

  private def balance(value: Long): Balance = Balance(NonNegLong.unsafeFrom(value))
  private def amount(value: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(value))
  private def fee(value: Long): AllowSpendFee = AllowSpendFee(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def makeAllowSpend(
    sourceKeyPair: java.security.KeyPair,
    destination: Address,
    amountValue: Long,
    feeValue: Long
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    Signed.forAsyncHasher(
      AllowSpend(
        source = sourceKeyPair.getPublic.toAddress,
        destination = destination,
        currencyId = none,
        amount = amount(amountValue),
        fee = fee(feeValue),
        parent = AllowSpendReference.empty,
        lastValidEpochProgress = epoch(100L),
        approvers = List(destination)
      ),
      sourceKeyPair
    )

  private def makeBlock(
    tx: Signed[AllowSpend],
    id: Long
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpendBlock]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { blockKeyPair =>
      val block = AllowSpendBlock(
        RoundId(new UUID(0L, id)),
        NonEmptySet.fromSetUnsafe(SortedSet(tx))
      )
      Signed.forAsyncHasher(block, blockKeyPair)
    }

  private def accept(
    logic: AllowSpendBlockAcceptanceLogic[IO],
    block: Signed[AllowSpendBlock],
    context: AllowSpendBlockAcceptanceContext[IO],
    update: AllowSpendBlockAcceptanceContextUpdate
  )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] = {
    val tx = block.value.transactions.head
    logic
      .acceptBlock(
        block,
        Map(tx.source -> NonEmptyList.one(tx)),
        context,
        update,
        shouldPerformMetagraphSpecificValidations = false
      )
      .value
  }

  test("allow-spend creation reserves source funds and cannot fund its destination") { res =>
    implicit val (hasher, securityProvider) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      finalDestinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      finalDestination = finalDestinationKeyPair.getPublic.toAddress
      reservation <- makeAllowSpend(sourceKeyPair, destination, amountValue = 90L, feeValue = 10L)
      unfundedReservation <- makeAllowSpend(destinationKeyPair, finalDestination, amountValue = 1L, feeValue = 0L)
      reservationBlock <- makeBlock(reservation, 1L)
      unfundedBlock <- makeBlock(unfundedReservation, 2L)
      context = AllowSpendBlockAcceptanceContext.fromStaticData[IO](
        balances = Map(source -> balance(100L), destination -> Balance.empty),
        lastTxRefs = Map.empty,
        collateral = io.constellationnetwork.schema.balance.Amount.empty,
        initialTxRef = AllowSpendReference.empty
      )
      logic = AllowSpendBlockAcceptanceLogic.make[IO]
      first <- accept(logic, reservationBlock, context, AllowSpendBlockAcceptanceContextUpdate.empty)
      firstUpdate <- IO.fromEither(first.leftMap(new AssertionError(_)))
      second <- accept(logic, unfundedBlock, context, firstUpdate)

      stateManager = AllowSpendStateManager.make[IO](GlobalStateReader.empty[IO])
      finalBalances <- stateManager.updateGlobalBalancesByAllowSpendsWithExpired(
        epochProgress = epoch(1L),
        currentBalances = SortedMap(source -> balance(100L), destination -> Balance.empty),
        globalAllowSpends = SortedMap(source -> SortedSet(reservation)),
        expiredGlobalAllowSpends = SortedMap.empty
      )
      applied <- IO.fromEither(finalBalances.leftMap(new AssertionError(_)))
      (appliedBalances, _) = applied
    } yield expect.all(
      firstUpdate.balances.get(source).contains(Balance.empty),
      !firstUpdate.balances.contains(destination),
      second.left.exists {
        case AddressBalanceOutOfRange(address, _) => address === destination
        case _                                    => false
      },
      appliedBalances.get(source).contains(Balance.empty),
      appliedBalances.get(destination).contains(Balance.empty)
    )
  }

  test("allow-spend admission is permutation-stable when the only dependency is a destination credit") { res =>
    implicit val (hasher, securityProvider) = res

    def run(
      logic: AllowSpendBlockAcceptanceLogic[IO],
      context: AllowSpendBlockAcceptanceContext[IO],
      blocks: List[Signed[AllowSpendBlock]]
    ): IO[(AllowSpendBlockAcceptanceContextUpdate, List[Signed[AllowSpendBlock]])] =
      blocks.foldLeftM((AllowSpendBlockAcceptanceContextUpdate.empty, List.empty[Signed[AllowSpendBlock]])) {
        case ((update, accepted), block) =>
          accept(logic, block, context, update).map {
            case Right(next) => (next, accepted :+ block)
            case Left(_)     => (update, accepted)
          }
      }

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      finalDestinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      destination = destinationKeyPair.getPublic.toAddress
      reservation <- makeAllowSpend(sourceKeyPair, destination, amountValue = 100L, feeValue = 0L)
      unfundedReservation <- makeAllowSpend(destinationKeyPair, finalDestinationKeyPair.getPublic.toAddress, amountValue = 100L, feeValue = 0L)
      reservationBlock <- makeBlock(reservation, 11L)
      unfundedBlock <- makeBlock(unfundedReservation, 12L)
      context = AllowSpendBlockAcceptanceContext.fromStaticData[IO](
        balances = Map(source -> balance(100L), destination -> Balance.empty),
        lastTxRefs = Map.empty,
        collateral = io.constellationnetwork.schema.balance.Amount.empty,
        initialTxRef = AllowSpendReference.empty
      )
      logic = AllowSpendBlockAcceptanceLogic.make[IO]
      forward <- run(logic, context, List(reservationBlock, unfundedBlock))
      reverse <- run(logic, context, List(unfundedBlock, reservationBlock))
    } yield expect.all(
      forward._2 == List(reservationBlock),
      reverse._2 == List(reservationBlock),
      forward._1.balances == reverse._1.balances,
      forward._1.balances.get(source).contains(Balance.empty),
      !forward._1.balances.contains(destination)
    )
  }
}
