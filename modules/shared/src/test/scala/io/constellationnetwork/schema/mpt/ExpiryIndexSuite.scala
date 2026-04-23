package io.constellationnetwork.schema.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object ExpiryIndexSuite extends MutableIOSuite {
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkEmptyMptStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      mptStore <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield mptStore

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private def mkAddress(implicit sp: SecurityProvider[IO]): IO[io.constellationnetwork.schema.address.Address] =
    KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)

  test("empty delta is a no-op (no bucket written)") { res =>
    implicit val (h, sp, js) = res
    for {
      store <- mkEmptyMptStore
      acc = StateChangesAccumulator()
      _ <- store.syncFromStateChanges(acc, SnapshotOrdinal(NonNegLong(1L)))
      bucket <- store.getExpiryBucket[TokenLockExpiryKey](
        SystemNamespaceLabel.ExpiryIndexTokenLocks,
        EpochProgress(NonNegLong(100))
      )
    } yield expect(bucket.isEmpty)
  }

  test("add via accumulator writes into the bucket; read via getExpiryBucket") { res =>
    implicit val (h, sp, js) = res
    for {
      addr <- mkAddress
      expiryEpoch = EpochProgress(NonNegLong(100))
      key = TokenLockExpiryKey(addr, testHash("tl-1"))

      acc = StateChangesAccumulator(
        tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(
          adds = SortedMap(expiryEpoch -> Set(key))
        )
      )
      store <- mkEmptyMptStore
      _ <- store.syncFromStateChanges(acc, SnapshotOrdinal(NonNegLong(1L)))
      got <- store.getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, expiryEpoch)
    } yield expect(got.contains(SortedSet(key)))
  }

  test("subsequent remove deletes the bucket entirely when it empties") { res =>
    implicit val (h, sp, js) = res
    for {
      addr <- mkAddress
      epoch = EpochProgress(NonNegLong(200))
      key = TokenLockExpiryKey(addr, testHash("tl-gc"))

      store <- mkEmptyMptStore
      // Seed
      _ <- store.syncFromStateChanges(
        StateChangesAccumulator(tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(key)))),
        SnapshotOrdinal(NonNegLong(1L))
      )
      before <- store.getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
      // Remove
      _ <- store.syncFromStateChanges(
        StateChangesAccumulator(tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(removes = SortedMap(epoch -> Set(key)))),
        SnapshotOrdinal(NonNegLong(2L))
      )
      after <- store.getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
    } yield expect.all(
      before.contains(SortedSet(key)),
      after.isEmpty
    )
  }

  test("three indices under SystemIndex fieldId are isolated (no key collisions)") { res =>
    implicit val (h, sp, js) = res
    for {
      addr <- mkAddress
      epoch = EpochProgress(NonNegLong(500))
      allowKey = AllowSpendExpiryKey(None, addr, testHash("as-shared-epoch"))
      lockKey = TokenLockExpiryKey(addr, testHash("tl-shared-epoch"))
      collKey = NodeCollateralWithdrawalExpiryKey(addr, testHash("nc-shared-epoch"))

      acc = StateChangesAccumulator(
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(allowKey))),
        tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(lockKey))),
        nodeCollateralWithdrawalExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(collKey)))
      )
      store <- mkEmptyMptStore
      _ <- store.syncFromStateChanges(acc, SnapshotOrdinal(NonNegLong(1L)))

      gotAllow <- store.getExpiryBucket[AllowSpendExpiryKey](SystemNamespaceLabel.ExpiryIndexAllowSpends, epoch)
      gotLock <- store.getExpiryBucket[TokenLockExpiryKey](SystemNamespaceLabel.ExpiryIndexTokenLocks, epoch)
      gotColl <- store.getExpiryBucket[NodeCollateralWithdrawalExpiryKey](
        SystemNamespaceLabel.ExpiryIndexNodeCollateralWithdrawals,
        epoch
      )
    } yield expect.all(
      gotAllow.contains(SortedSet(allowKey)),
      gotLock.contains(SortedSet(lockKey)),
      gotColl.contains(SortedSet(collKey))
    )
  }

  test("re-sync with empty delta leaves existing buckets intact") { res =>
    implicit val (h, sp, js) = res
    for {
      addr <- mkAddress
      epoch = EpochProgress(NonNegLong(50))
      key = AllowSpendExpiryKey(None, addr, testHash("as-persist"))

      store <- mkEmptyMptStore
      _ <- store.syncFromStateChanges(
        StateChangesAccumulator(
          allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(epoch -> Set(key)))
        ),
        SnapshotOrdinal(NonNegLong(1L))
      )
      _ <- store.syncFromStateChanges(StateChangesAccumulator(), SnapshotOrdinal(NonNegLong(2L)))
      after <- store.getExpiryBucket[AllowSpendExpiryKey](SystemNamespaceLabel.ExpiryIndexAllowSpends, epoch)
    } yield expect(after.contains(SortedSet(key)))
  }
}
