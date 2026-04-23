package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendTransaction
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap.SwapAmount
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object SpendTransactionBalanceManagerSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkMptStore(prior: SortedMap[Address, Balance])(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- prior.toList.traverse_ {
        case (addr, bal) =>
          mptStore.insert[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr), bal)
      }
    } yield mptStore

  test("both paths agree on source/destination credits when MPT state matches passed balances") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      priorState = SortedMap[Address, Balance](
        source -> Balance(NonNegLong(1000)),
        dest -> Balance(NonNegLong(50))
      )
      // A SpendTransaction without allowSpendRef: source pays dest directly.
      spendTx = SpendTransaction(
        source = source,
        destination = dest,
        amount = SwapAmount(PosLong(100)),
        currencyId = None,
        allowSpendRef = None
      )

      mptStore <- mkMptStore(priorState)
      legacy = SpendTransactionBalanceManager.make[IO](Some(mptStore), shouldUseMptStore = false)
      mptMgr = SpendTransactionBalanceManager.make[IO](Some(mptStore), shouldUseMptStore = true)

      legacyEither <- legacy.updateGlobalBalancesBySpendTransactions(priorState, SortedMap.empty, List(spendTx))
      mptEither <- mptMgr.updateGlobalBalancesBySpendTransactions(SortedMap.empty, SortedMap.empty, List(spendTx))

      legacyDelta = legacyEither.map(_._2).getOrElse(SortedMap.empty[Address, Balance])
      mptDelta = mptEither.map(_._2).getOrElse(SortedMap.empty[Address, Balance])
    } yield expect.all(
      legacyEither.isRight,
      mptEither.isRight,
      legacyDelta == mptDelta,
      legacyDelta(source) == Balance(NonNegLong(900)),
      legacyDelta(dest) == Balance(NonNegLong(150))
    )
  }

  test("mpt path: delta shadows MPT (in-ordinal updates take precedence)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      source = kp1.getPublic.toAddress
      dest = kp2.getPublic.toAddress

      // MPT says source had 0; delta says 500 (already credited this ordinal)
      priorInMpt = SortedMap[Address, Balance](source -> Balance(NonNegLong(0)))
      currentDelta = SortedMap[Address, Balance](source -> Balance(NonNegLong(500)))
      spendTx = SpendTransaction(
        source = source,
        destination = dest,
        amount = SwapAmount(PosLong(100)),
        currencyId = None,
        allowSpendRef = None
      )

      mptStore <- mkMptStore(priorInMpt)
      mgr = SpendTransactionBalanceManager.make[IO](Some(mptStore), shouldUseMptStore = true)

      result <- mgr.updateGlobalBalancesBySpendTransactions(currentDelta, SortedMap.empty, List(spendTx))
    } yield expect.all(
      result.isRight,
      result.map(_._2).getOrElse(SortedMap.empty[Address, Balance])(source) == Balance(NonNegLong(400))
    )
  }
}
