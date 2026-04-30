package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.{RewardTransaction, TransactionAmount}
import io.constellationnetwork.security._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

object RewardAcceptanceManagerSuite extends MutableIOSuite {
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

  test("recipient not in delta map: prior balance read from MPT") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      priorInMpt = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(priorInMpt)
      manager = RewardAcceptanceManager.make[IO](mptStore)

      (updated, accepted, delta) <- manager.acceptRewardTxs(SortedMap.empty, rewards)
    } yield
      expect.all(
        updated(recipient) == Balance(NonNegLong(150)),
        accepted.size == 1,
        delta == SortedMap(recipient -> Balance(NonNegLong(150)))
      )
  }

  test("recipient not in delta map and not in MPT: starts from Balance.empty") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(SortedMap.empty)
      manager = RewardAcceptanceManager.make[IO](mptStore)

      (updated, accepted, _) <- manager.acceptRewardTxs(SortedMap.empty, rewards)
    } yield
      expect.all(
        updated(recipient) == Balance(NonNegLong(50)),
        accepted.size == 1
      )
  }

  test("delta map shadows prior MPT balance (in-ordinal state)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      priorInMpt = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      // Already touched this ordinal — delta says 200 supersedes MPT's 100
      currentDelta = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(200)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(priorInMpt)
      manager = RewardAcceptanceManager.make[IO](mptStore)

      (updated, _, _) <- manager.acceptRewardTxs(currentDelta, rewards)
    } yield expect(updated(recipient) == Balance(NonNegLong(250)))
  }

  test("multiple rewards to the same recipient accumulate") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      rewards = SortedSet(
        RewardTransaction(recipient, TransactionAmount(PosLong(30))),
        RewardTransaction(recipient, TransactionAmount(PosLong(20)))
      )

      mptStore <- mkMptStore(SortedMap.empty)
      manager = RewardAcceptanceManager.make[IO](mptStore)

      (updated, accepted, _) <- manager.acceptRewardTxs(SortedMap.empty, rewards)
    } yield
      expect.all(
        updated(recipient) == Balance(NonNegLong(50)),
        accepted.size == 2
      )
  }

  test("empty rewards yields the input deltas unchanged and zero accepted") { res =>
    implicit val (h, sp, js) = res
    for {
      mptStore <- mkMptStore(SortedMap.empty)
      manager = RewardAcceptanceManager.make[IO](mptStore)

      (updated, accepted, delta) <- manager.acceptRewardTxs(SortedMap.empty, SortedSet.empty)
    } yield
      expect.all(
        updated.isEmpty,
        accepted.isEmpty,
        delta.isEmpty
      )
  }
}
