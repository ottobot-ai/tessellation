package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
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

  test("legacy path: rewards credit destinations from the full-map input") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      priorMap = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(SortedMap.empty)
      manager = RewardAcceptanceManager.make[IO](Some(mptStore), shouldUseMptStore = false)

      (updated, accepted, delta) <- manager.acceptRewardTxs(priorMap, rewards)
    } yield expect.all(
      updated(recipient) == Balance(NonNegLong(150)),
      accepted.size == 1,
      delta == SortedMap(recipient -> Balance(NonNegLong(150)))
    )
  }

  test("mpt path: rewards read prior balance from MPT when not in delta map") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      priorInMpt = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(priorInMpt)
      manager = RewardAcceptanceManager.make[IO](Some(mptStore), shouldUseMptStore = true)

      (updated, accepted, delta) <- manager.acceptRewardTxs(SortedMap.empty, rewards)
    } yield expect.all(
      updated(recipient) == Balance(NonNegLong(150)),
      accepted.size == 1,
      delta == SortedMap(recipient -> Balance(NonNegLong(150)))
    )
  }

  test("mpt path: delta map shadows prior MPT balance (in-ordinal state)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      priorInMpt = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      // Already touched this ordinal — delta says 200 supersedes MPT's 100
      currentDelta = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(200)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(priorInMpt)
      manager = RewardAcceptanceManager.make[IO](Some(mptStore), shouldUseMptStore = true)

      (updated, _, _) <- manager.acceptRewardTxs(currentDelta, rewards)
    } yield expect(updated(recipient) == Balance(NonNegLong(250)))
  }

  test("both paths agree when MPT prior matches the legacy balance map") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      recipient = kp.getPublic.toAddress

      state = SortedMap[Address, Balance](recipient -> Balance(NonNegLong(100)))
      rewards = SortedSet(RewardTransaction(recipient, TransactionAmount(PosLong(50))))

      mptStore <- mkMptStore(state)
      legacy = RewardAcceptanceManager.make[IO](Some(mptStore), shouldUseMptStore = false)
      mpt = RewardAcceptanceManager.make[IO](Some(mptStore), shouldUseMptStore = true)

      (legacyUpdated, legacyAccepted, legacyDelta) <- legacy.acceptRewardTxs(state, rewards)
      (mptUpdated, mptAccepted, mptDelta) <- mpt.acceptRewardTxs(SortedMap.empty, rewards)
    } yield expect.all(
      legacyUpdated(recipient) == mptUpdated(recipient),
      legacyAccepted == mptAccepted,
      legacyDelta == mptDelta
    )
  }
}
