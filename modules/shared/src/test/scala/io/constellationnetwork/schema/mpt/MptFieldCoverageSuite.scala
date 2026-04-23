package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.PricingUpdate
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.node._
import io.constellationnetwork.schema.priceOracle._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

object MptFieldCoverageSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def mkEmptyMptStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield mptStore

  private val testSignature = signature.Signature(Hex(""))
  private val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)
  private val testProofs = NonEmptySet.one(testSignatureProof)

  private def mkUnp(source: Address, label: String): Signed[UpdateNodeParameters] =
    Signed(
      UpdateNodeParameters(
        source = source,
        delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(10_000_000)),
        nodeMetadataParameters = NodeMetadataParameters(name = s"node-$label", description = s"desc-$label"),
        parent = UpdateNodeParametersReference(UpdateNodeParametersOrdinal(0L), testHash(s"p-$label"))
      ),
      testProofs
    )

  private def mkPriceRecord(value: Long): PriceRecord = {
    val pu = PricingUpdate(PriceFraction(TokenPair.DAG_USD, NonNegFraction.unsafeFrom(value, 1L)))
    PriceRecord(
      currentPrice = pu,
      upcomingPrice = pu,
      currentSum = pu,
      currentNumEvents = PosInt(1),
      nextWindowChange = EpochProgress(NonNegLong(1000L)),
      updatedAt = EpochProgress(NonNegLong(500L))
    )
  }

  test("syncFromGlobalSnapshotInfo + getUpdateNodeParameters round-trips via MPT") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      id1 = kp1.getPublic.toId
      id2 = kp2.getPublic.toId

      unp1 = mkUnp(addr1, "one")
      unp2 = mkUnp(addr2, "two")

      info = GlobalSnapshotInfo.empty.copy(
        updateNodeParameters = SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)](
          id1 -> (unp1, SnapshotOrdinal(NonNegLong(42L))),
          id2 -> (unp2, SnapshotOrdinal(NonNegLong(43L)))
        ).some
      )

      store <- mkEmptyMptStore
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(100L)))
      got1 <- store.getUpdateNodeParameters(id1)
      got2 <- store.getUpdateNodeParameters(id2)
    } yield expect.all(
      got1.contains((unp1, SnapshotOrdinal(NonNegLong(42L)))),
      got2.contains((unp2, SnapshotOrdinal(NonNegLong(43L))))
    )
  }

  test("syncFromGlobalSnapshotInfo + getPriceRecord round-trips via MPT") { res =>
    implicit val (h, sp, js) = res
    for {
      store <- mkEmptyMptStore
      record = mkPriceRecord(value = 123L)
      info = GlobalSnapshotInfo.empty.copy(
        priceState = SortedMap[TokenPair, PriceRecord](TokenPair.DAG_USD -> record).some
      )
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(100L)))
      got <- store.getPriceRecord(TokenPair.DAG_USD)
    } yield expect(got.contains(record))
  }

  test("syncFromStateChanges + getters round-trip both fields from an accumulator") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      id = kp.getPublic.toId
      unp = mkUnp(addr, "only")
      record = mkPriceRecord(value = 7L)

      acc = StateChangesAccumulator(
        updateNodeParameters = SortedMap(id -> ((unp, SnapshotOrdinal(NonNegLong(5L))))),
        priceState = SortedMap(TokenPair.DAG_USD -> record)
      )

      store <- mkEmptyMptStore
      _ <- store.syncFromStateChanges(acc, SnapshotOrdinal(NonNegLong(1L)))
      gotUnp <- store.getUpdateNodeParameters(id)
      gotPr <- store.getPriceRecord(TokenPair.DAG_USD)
    } yield expect.all(
      gotUnp.contains((unp, SnapshotOrdinal(NonNegLong(5L)))),
      gotPr.contains(record)
    )
  }

  test("mptRoot from stateChanges matches mptRoot rebuilt from the resulting GSI (no field drift)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      id = kp.getPublic.toId
      unp = mkUnp(addr, "root-check")
      record = mkPriceRecord(value = 77L)

      // Apply via accumulator (production delta path)
      acc = StateChangesAccumulator(
        updateNodeParameters = SortedMap(id -> ((unp, SnapshotOrdinal(NonNegLong(5L))))),
        priceState = SortedMap(TokenPair.DAG_USD -> record)
      )
      storeA <- mkEmptyMptStore
      _ <- storeA.syncFromStateChanges(acc, SnapshotOrdinal(NonNegLong(1L)))
      rootA <- storeA.underlying.getRootHashForOrdinal(SnapshotOrdinal(NonNegLong(1L)))

      // Apply via full-state seed (recovery / rollback path)
      info = GlobalSnapshotInfo.empty.copy(
        updateNodeParameters = SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)](
          id -> (unp, SnapshotOrdinal(NonNegLong(5L)))
        ).some,
        priceState = SortedMap[TokenPair, PriceRecord](TokenPair.DAG_USD -> record).some
      )
      storeB <- mkEmptyMptStore
      _ <- storeB.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
      rootB <- storeB.underlying.getRootHashForOrdinal(SnapshotOrdinal(NonNegLong(1L)))
    } yield expect.all(
      rootA.isDefined,
      rootB.isDefined,
      rootA == rootB
    )
  }
}
