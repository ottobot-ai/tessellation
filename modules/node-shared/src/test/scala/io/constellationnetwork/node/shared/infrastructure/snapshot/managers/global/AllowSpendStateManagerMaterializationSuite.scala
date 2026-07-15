package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.PosLong
import weaver.MutableIOSuite

object AllowSpendStateManagerMaterializationSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, sp, jsonSerializer)

  private def allowSpend(
    sourceKeyPair: KeyPair,
    destination: Address,
    amount: Long,
    currencyId: Option[CurrencyId]
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    Signed.forAsyncHasher(
      AllowSpend(
        source = sourceKeyPair.getPublic.toAddress,
        destination = destination,
        currencyId = currencyId,
        amount = SwapAmount(PosLong.unsafeFrom(amount)),
        fee = AllowSpendFee(0L),
        parent = AllowSpendReference.empty,
        lastValidEpochProgress = EpochProgress(100L),
        approvers = List(destination)
      ),
      sourceKeyPair
    )

  private def freshStore(
    implicit hasher: Hasher[IO],
    jsonSerializer: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  private def materialize(store: MptStore[IO, GlobalStateKey])(implicit hasher: Hasher[IO]) =
    AllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(store)).materializeActiveAllowSpendsFromMpt

  test("materializer reconstructs only entries whose values exactly match their MPT keys") { res =>
    implicit val (hasher, securityProvider, jsonSerializer) = res

    for {
      globalSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      globalSource = globalSourceKeyPair.getPublic.toAddress
      metagraphSource = metagraphSourceKeyPair.getPublic.toAddress
      metagraph = metagraphKeyPair.getPublic.toAddress
      global <- allowSpend(globalSourceKeyPair, destinationKeyPair.getPublic.toAddress, 10L, none)
      scoped <- allowSpend(metagraphSourceKeyPair, destinationKeyPair.getPublic.toAddress, 11L, CurrencyId(metagraph).some)
      store <- freshStore
      _ <- store.insert(GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, none, globalSource), SortedSet(global))
      _ <- store.insert(GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, metagraph.some, metagraphSource), SortedSet(scoped))
      result <- materialize(store)
      crossShardResult <- ConsumedAllowSpendStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .materializeActiveAllowSpendsFromMpt
      expected = SortedMap(
        Option.empty[Address] -> SortedMap(globalSource -> SortedSet(global)),
        metagraph.some -> SortedMap(metagraphSource -> SortedSet(scoped))
      )
    } yield
      expect.all(
        result == expected,
        crossShardResult == expected
      )
  }

  test("materializer rejects a homogeneous value stored under another source key") { res =>
    implicit val (hasher, securityProvider, jsonSerializer) = res

    for {
      valueSourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      keySourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      signed <- allowSpend(valueSourceKeyPair, destinationKeyPair.getPublic.toAddress, 10L, none)
      store <- freshStore
      wrongKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, none, keySourceKeyPair.getPublic.toAddress)
      _ <- store.insert(wrongKey, SortedSet(signed))
      result <- materialize(store).attempt
      crossShardResult <- ConsumedAllowSpendStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .materializeActiveAllowSpendsFromMpt
        .attempt
    } yield
      expect.all(
        result.left.exists(_.getMessage.contains("key/value mismatch")),
        crossShardResult.left.exists(_.getMessage.contains("key/value mismatch"))
      )
  }

  test("materializer rejects a set containing multiple embedded sources") { res =>
    implicit val (hasher, securityProvider, jsonSerializer) = res

    for {
      sourceAKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      sourceBKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      sourceA = sourceAKeyPair.getPublic.toAddress
      signedA <- allowSpend(sourceAKeyPair, destinationKeyPair.getPublic.toAddress, 10L, none)
      signedB <- allowSpend(sourceBKeyPair, destinationKeyPair.getPublic.toAddress, 11L, none)
      store <- freshStore
      key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, none, sourceA)
      _ <- store.insert(key, SortedSet(signedA, signedB))
      result <- materialize(store).attempt
    } yield expect(result.left.exists(_.getMessage.contains("mixed scope/source")))
  }

  test("materializer rejects a set containing multiple embedded scopes") { res =>
    implicit val (hasher, securityProvider, jsonSerializer) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      destinationKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      metagraphKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKeyPair.getPublic.toAddress
      native <- allowSpend(sourceKeyPair, destinationKeyPair.getPublic.toAddress, 10L, none)
      scoped <- allowSpend(
        sourceKeyPair,
        destinationKeyPair.getPublic.toAddress,
        11L,
        CurrencyId(metagraphKeyPair.getPublic.toAddress).some
      )
      store <- freshStore
      key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, none, source)
      _ <- store.insert(key, SortedSet(native, scoped))
      result <- materialize(store).attempt
    } yield expect(result.left.exists(_.getMessage.contains("mixed scope/source")))
  }

  test("materializer rejects an empty per-key set") { res =>
    implicit val (hasher, securityProvider, jsonSerializer) = res

    for {
      sourceKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      store <- freshStore
      key = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, none, sourceKeyPair.getPublic.toAddress)
      _ <- store.insert(key, SortedSet.empty[Signed[AllowSpend]])
      seeded <- store.get[SortedSet[Signed[AllowSpend]]](key)
      result <- materialize(store).attempt
    } yield
      expect.all(
        seeded.contains(SortedSet.empty[Signed[AllowSpend]]),
        result.left.exists(_.getMessage.contains("empty set"))
      )
  }
}
