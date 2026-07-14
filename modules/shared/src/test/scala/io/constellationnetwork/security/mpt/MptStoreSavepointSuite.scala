package io.constellationnetwork.security.mpt

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore, MptTxAction}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{
  InMemoryMerklePatriciaProducer,
  MerklePatriciaError,
  ProducerSavepoint,
  StatefulMerklePatriciaProducer
}
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.serde.codecs.StringCodec._
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import io.circe.{Encoder, Json}
import weaver.MutableIOSuite

object MptStoreSavepointSuite extends MutableIOSuite {

  type Res = (HasherSelector[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer.forAsync[IO](sharedKryoRegistrar).flatMap { implicit kryo =>
      JsonSerializer.forAsync[IO].asResource.map { implicit json =>
        (
          HasherSelector.forSync[IO](
            Hasher.forJson[IO],
            Hasher.forKryo[IO],
            hashSelect = new HashSelect { def select(ordinal: SnapshotOrdinal): HashLogic = KryoHash }
          ),
          json
        )
      }
    }

  private final class BlockingClearProducer(
    delegate: StatefulMerklePatriciaProducer[IO],
    clearEntered: Deferred[IO, Unit]
  ) extends StatefulMerklePatriciaProducer[IO] {

    def entries: IO[Map[Hex, Array[Byte]]] = delegate.entries
    def physicalKeys: IO[Set[Hex]] = delegate.physicalKeys
    def entry(key: Hex): IO[Option[Array[Byte]]] = delegate.entry(key)
    def entriesForKeys(keys: Set[Hex]): IO[Map[Hex, Array[Byte]]] = delegate.entriesForKeys(keys)
    def entryCount: IO[Int] = delegate.entryCount
    def entriesWithPrefix(prefix: Hex): IO[Map[Hex, Array[Byte]]] = delegate.entriesWithPrefix(prefix)
    def build: IO[Either[MerklePatriciaError, MerklePatriciaTrie]] = delegate.build
    def buildForOrdinal(ordinal: SnapshotOrdinal): IO[Either[MerklePatriciaError, MerklePatriciaTrie]] =
      delegate.buildForOrdinal(ordinal)
    def getRootHashForOrdinal(ordinal: SnapshotOrdinal): IO[Option[MptRoot]] = delegate.getRootHashForOrdinal(ordinal)
    def getCurrentRootHash: IO[Option[MptRoot]] = delegate.getCurrentRootHash
    def getLastBuiltOrdinal: IO[Option[SnapshotOrdinal]] = delegate.getLastBuiltOrdinal
    def insert[A: Encoder](data: Map[Hex, A]): IO[Either[MerklePatriciaError, Unit]] = delegate.insert(data)
    def insertBytes(data: Map[Hex, Array[Byte]]): IO[Either[MerklePatriciaError, Unit]] = delegate.insertBytes(data)
    def replaceBytes(upserts: Map[Hex, Array[Byte]], removals: List[Hex]): IO[Either[MerklePatriciaError, Unit]] =
      delegate.replaceBytes(upserts, removals)
    def update[A: Encoder](key: Hex, value: A): IO[Either[MerklePatriciaError, Unit]] = delegate.update(key, value)
    def remove(keys: List[Hex]): IO[Either[MerklePatriciaError, Unit]] = delegate.remove(keys)
    def clear: IO[Unit] = delegate.clear >> clearEntered.complete(()).void >> IO.never[Unit]
    def getProver: IO[MerklePatriciaSingleInclusionProver[IO]] = delegate.getProver
    def buildHexMap(data: Map[GlobalStateKey, Json]): IO[Map[Hex, Array[Byte]]] = delegate.buildHexMap(data)
    def savepoint: IO[ProducerSavepoint[IO]] = delegate.savepoint
  }

  test("savepoint captures and restores producer state correctly") { implicit res =>
    implicit val (hs, js) = res
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()

        key1 <- hasher.hash("key1").map(hash => Hex(hash.value))
        key2 <- hasher.hash("key2").map(hash => Hex(hash.value))

        // Insert initial data
        _ <- producer.insert(Map(key1 -> "value1"))
        entriesBefore <- producer.entries

        // Take savepoint
        sp <- producer.savepoint

        // Insert more data (simulating validation mutation)
        _ <- producer.insert(Map(key2 -> "value2"))
        entriesAfterMutation <- producer.entries

        // Restore savepoint
        _ <- sp.restore
        entriesAfterRestore <- producer.entries

      } yield
        expect.all(
          entriesBefore.size == 1,
          entriesAfterMutation.size == 2,
          entriesAfterRestore.size == 1,
          entriesAfterRestore.contains(key1),
          !entriesAfterRestore.contains(key2)
        )
    }
  }

  test("savepoint restores trie and rootHash correctly") { implicit res =>
    implicit val (hs, js) = res
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()

        key1 <- hasher.hash("key1").map(hash => Hex(hash.value))
        key2 <- hasher.hash("key2").map(hash => Hex(hash.value))
        ordinal1 = SnapshotOrdinal.unsafeApply(100L)
        ordinal2 = SnapshotOrdinal.unsafeApply(101L)

        // Insert and build trie
        _ <- producer.insert(Map(key1 -> "value1"))
        trie1 <- producer.buildForOrdinal(ordinal1)
        rootHash1 = trie1.map(_.rootHash)

        // Take savepoint AFTER the build
        sp <- producer.savepoint

        // Mutate: insert more, build again
        _ <- producer.insert(Map(key2 -> "value2"))
        trie2 <- producer.buildForOrdinal(ordinal2)
        rootHash2 = trie2.map(_.rootHash)

        // Restore
        _ <- sp.restore
        trie3 <- producer.buildForOrdinal(ordinal1)
        rootHash3 = trie3.map(_.rootHash)

      } yield
        expect.all(
          rootHash1.isRight,
          rootHash2.isRight,
          rootHash3.isRight,
          // After restore, root hash should match the original
          rootHash1 == rootHash3,
          // The mutated root hash should be different
          rootHash1 != rootHash2
        )
    }
  }

  test("multiple savepoints are independent") { implicit res =>
    implicit val (hs, js) = res
    hs.withCurrent { implicit hasher =>
      for {
        producer <- InMemoryMerklePatriciaProducer.make[IO]()

        key1 <- hasher.hash("key1").map(hash => Hex(hash.value))
        key2 <- hasher.hash("key2").map(hash => Hex(hash.value))
        key3 <- hasher.hash("key3").map(hash => Hex(hash.value))

        // State: {key1}
        _ <- producer.insert(Map(key1 -> "value1"))
        sp1 <- producer.savepoint

        // State: {key1, key2}
        _ <- producer.insert(Map(key2 -> "value2"))
        sp2 <- producer.savepoint

        // State: {key1, key2, key3}
        _ <- producer.insert(Map(key3 -> "value3"))
        entriesFull <- producer.entries

        // Restore sp1 → should go back to {key1}
        _ <- sp1.restore
        entriesAfterSp1 <- producer.entries

      } yield
        expect.all(
          entriesFull.size == 3,
          entriesAfterSp1.size == 1,
          entriesAfterSp1.contains(key1),
          !entriesAfterSp1.contains(key2),
          !entriesAfterSp1.contains(key3)
        )
    }
  }

  test("MptStore savepoint captures and restores store-level state") { implicit res =>
    implicit val (hs, js) = res
    implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])

      key1 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, Address("DAG2AUdecqFwEGcgAcH1ac2wrsg8acrgGwrQojzw"))
      key2 = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebU"))

      // Insert initial data
      _ <- store.insert(key1, "value1")
      before <- store.get[String](key1)

      // Take savepoint
      sp <- store.savepoint

      // Insert more data (simulating validation mutation)
      _ <- store.insert(key2, "value2")
      afterMutation1 <- store.get[String](key1)
      afterMutation2 <- store.get[String](key2)

      // Restore savepoint
      _ <- sp.restore
      afterRestore1 <- store.get[String](key1)
      afterRestore2 <- store.get[String](key2)

    } yield
      expect.all(
        before.contains("value1"),
        afterMutation1.contains("value1"),
        afterMutation2.contains("value2"),
        afterRestore1.contains("value1"),
        afterRestore2.isEmpty // key2 should be gone after restore
      )
  }

  test("MptStore transaction restores its savepoint when canceled after mutation") { implicit res =>
    implicit val (_, js) = res
    implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
    val initial = Hex("aa")
    val transient = Hex("bb")
    val initialOrdinal = SnapshotOrdinal.unsafeApply(10L)
    val transientOrdinal = SnapshotOrdinal.unsafeApply(11L)

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](Map(initial -> "initial".getBytes("UTF-8")))
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      _ <- store.commit(initialOrdinal)
      rootBefore <- producer.getCurrentRootHash
      lastBuiltBefore <- producer.getLastBuiltOrdinal
      syncedBefore <- store.lastPersistedOrdinal
      mutationPublished <- Deferred[IO, Unit]
      fiber <- store
        .withTransaction[Unit](
          store.insert(transient, "transient") >>
            store.commit(transientOrdinal) >>
            mutationPublished.complete(()).void >>
            IO.never[(Unit, MptTxAction)]
        )
        .start
      _ <- mutationPublished.get
      during <- store.allEntriesAsBytes
      rootDuring <- producer.getCurrentRootHash
      syncedDuring <- store.lastPersistedOrdinal
      _ <- fiber.cancel
      after <- store.allEntriesAsBytes
      rootAfter <- producer.getCurrentRootHash
      transientCachedRootAfter <- producer.getRootHashForOrdinal(transientOrdinal)
      lastBuiltAfter <- producer.getLastBuiltOrdinal
      syncedAfter <- store.lastPersistedOrdinal
    } yield
      expect.all(
        rootBefore.nonEmpty,
        lastBuiltBefore.contains(initialOrdinal),
        syncedBefore.contains(initialOrdinal),
        during.contains(transient),
        rootDuring.exists(root => rootBefore.forall(_ != root)),
        syncedDuring.contains(transientOrdinal),
        after.keySet == Set(initial),
        new String(after(initial), "UTF-8") == "initial",
        rootAfter == rootBefore,
        transientCachedRootAfter.isEmpty,
        lastBuiltAfter == lastBuiltBefore,
        syncedAfter == syncedBefore
      )
  }

  test("MptStore empty load restores the complete prior state when canceled during clear") { implicit res =>
    implicit val (_, js) = res
    implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
    val initial = Hex("cc")
    val initialOrdinal = SnapshotOrdinal.unsafeApply(20L)
    val emptyOrdinal = SnapshotOrdinal.unsafeApply(21L)

    for {
      clearEntered <- Deferred[IO, Unit]
      delegate <- InMemoryMerklePatriciaProducer.make[IO](Map(initial -> "retained".getBytes("UTF-8")))
      producer = new BlockingClearProducer(delegate, clearEntered)
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      _ <- store.commit(initialOrdinal)
      rootBefore <- producer.getCurrentRootHash
      lastBuiltBefore <- producer.getLastBuiltOrdinal
      syncedBefore <- store.lastPersistedOrdinal

      loading <- store.loadBytes(Map.empty, emptyOrdinal).start
      _ <- clearEntered.get
      during <- store.allEntriesAsBytes
      _ <- loading.cancel

      after <- store.allEntriesAsBytes
      rootAfter <- producer.getCurrentRootHash
      lastBuiltAfter <- producer.getLastBuiltOrdinal
      syncedAfter <- store.lastPersistedOrdinal
    } yield
      expect.all(
        during.isEmpty,
        after.keySet == Set(initial),
        new String(after(initial), "UTF-8") == "retained",
        rootAfter == rootBefore,
        lastBuiltAfter == lastBuiltBefore,
        syncedAfter == syncedBefore
      )
  }
}
