package io.constellationnetwork.security.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateFieldId.Balances
import io.constellationnetwork.schema.mpt.PartitionNamespace.{AddressNamespace, EmptyNamespace, MetagraphNamespace}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer._
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import fs2.Stream
import fs2.io.file.Files
import weaver.MutableIOSuite

object PhysicalTrieKeyValidatorSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] = for {
    implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    hasher = Hasher.forJson[IO]
  } yield (json, hasher)

  private def sameEntries(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def awaitPersistedState(
    storage: MptStateStorage[IO],
    ordinal: SnapshotOrdinal,
    attempts: Int = 100
  ): IO[Option[Map[Hex, Array[Byte]]]] =
    storage.readState(ordinal).flatMap {
      case found @ Some(_)      => found.pure[IO]
      case None if attempts > 0 => IO.sleep(10.millis) >> awaitPersistedState(storage, ordinal, attempts - 1)
      case None                 => none[Map[Hex, Array[Byte]]].pure[IO]
    }

  test("generic grammar accepts canonical byte paths and a sole empty root terminal") { _ =>
    val canonical = Vector(Hex(""), Hex("00"), Hex("10abcdef"))

    IO.pure(
      expect.all(
        PhysicalTrieKeyValidator.validateKey(Hex("")) == Right(()),
        PhysicalTrieKeyValidator.validateKeys(Vector(Hex(""))) == Right(()),
        PhysicalTrieKeyValidator.validateKeys(canonical.tail) == Right(())
      )
    )
  }

  test("single-key validation rejects odd, invalid, and noncanonical physical spellings") { _ =>
    IO.pure(
      expect.all(
        PhysicalTrieKeyValidator.validateKey(Hex("abc")) == Left(OddLengthPhysicalTrieKey(Hex("abc"))),
        PhysicalTrieKeyValidator.validateKey(Hex("0g")) == Left(InvalidDigitPhysicalTrieKey(Hex("0g"), 1, 'g')),
        PhysicalTrieKeyValidator.validateKey(Hex("\uff100")) ==
          Left(InvalidDigitPhysicalTrieKey(Hex("\uff100"), 0, '\uff10')),
        PhysicalTrieKeyValidator.validateKey(Hex("AA")) == Left(NonCanonicalPhysicalTrieKey(Hex("AA"), Hex("aa")))
      )
    )
  }

  test("complete-set validation rejects nibble aliases deterministically before normalization") { _ =>
    val expected = Left(DuplicatePhysicalTriePath(Hex("aa03"), Hex("AA03"), Hex("aa03")))
    val forward = PhysicalTrieKeyValidator.validateKeys(Vector(Hex("aa03"), Hex("AA03")))
    val reverse = PhysicalTrieKeyValidator.validateKeys(Vector(Hex("AA03"), Hex("aa03")))

    IO.pure(expect.all(forward == expected, reverse == expected))
  }

  test("complete-set validation rejects terminal-prefix collisions, including the empty root terminal") { _ =>
    IO.pure(
      expect.all(
        PhysicalTrieKeyValidator.validateKeys(Vector(Hex("aa00"), Hex("aa"))) ==
          Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        PhysicalTrieKeyValidator.validateKeys(Vector(Hex("00"), Hex(""))) ==
          Left(TerminalPhysicalTrieKeyCollision(Hex(""), Hex("00")))
      )
    )
  }

  test("incremental validation permits replacement and rejects collisions in either prefix direction") { _ =>
    val current = Set(Hex("aa"), Hex("bb00"))

    IO.pure(
      expect.all(
        PhysicalTrieKeyValidator.validateInsertion(current, Vector(Hex("aa"))) == Right(()),
        PhysicalTrieKeyValidator.validateInsertion(current, Vector(Hex("aa00"))) ==
          Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        PhysicalTrieKeyValidator.validateInsertion(current, Vector(Hex("bb"))) ==
          Left(TerminalPhysicalTrieKeyCollision(Hex("bb"), Hex("bb00")))
      )
    )
  }

  test("parallel full build fails with typed errors within a deadline") { implicit res =>
    implicit val (json, hasher) = res
    val producer = ParallelMerklePatriciaProducer[IO]
    val value = Array[Byte](1)

    for {
      uppercaseResult <- producer.createFromBytes(Map(Hex("AA") -> value)).timeout(1.second).attempt
      oddResult <- producer.createFromBytes(Map(Hex("abc") -> value)).timeout(1.second).attempt
      invalidResult <- producer.createFromBytes(Map(Hex("0g") -> value)).timeout(1.second).attempt
      aliasResult <- producer
        .createFromBytes(Map(Hex("aa03") -> value, Hex("AA03") -> value))
        .timeout(1.second)
        .attempt
      prefixResult <- producer
        .createFromBytes(Map(Hex("aa") -> value, Hex("aa00") -> value))
        .timeout(1.second)
        .attempt
    } yield
      expect.all(
        uppercaseResult == Left(NonCanonicalPhysicalTrieKey(Hex("AA"), Hex("aa"))),
        oddResult == Left(OddLengthPhysicalTrieKey(Hex("abc"))),
        invalidResult == Left(InvalidDigitPhysicalTrieKey(Hex("0g"), 1, 'g')),
        aliasResult == Left(DuplicatePhysicalTriePath(Hex("aa03"), Hex("AA03"), Hex("aa03"))),
        prefixResult == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00")))
      )
  }

  test("parallel builder guard independently rejects a terminal in a multi-entry group") { _ =>
    val entries = Array(
      CompactNibblePath.fromHexString("aa") -> Array.emptyByteArray,
      CompactNibblePath.fromHexString("aa00") -> Array.emptyByteArray
    )
    val duplicateEntries = Array(
      CompactNibblePath.fromHexString("aa") -> Array.emptyByteArray,
      CompactNibblePath.fromHexString("aa") -> Array.emptyByteArray
    )

    val result = ParallelMerklePatriciaProducer.ensureGroupProgress(entries, 0, entries.length, depth = 2)
    val duplicateResult =
      ParallelMerklePatriciaProducer.ensureGroupProgress(duplicateEntries, 0, duplicateEntries.length, depth = 2)

    IO.pure(
      expect.all(
        result == Left(NonShrinkingPhysicalTrieGroup(2, Hex("aa"), Hex("aa00"))),
        duplicateResult == Left(NonShrinkingPhysicalTrieGroup(2, Hex("aa"), Hex("aa")))
      )
    )
  }

  test("parallel full build retains valid empty-terminal and canonical-key behavior") { implicit res =>
    implicit val (json, hasher) = res
    val producer = ParallelMerklePatriciaProducer[IO]

    for {
      emptyTerminal <- producer.createFromBytes(Map(Hex("") -> Array[Byte](1)))
      canonical <- producer.createFromBytes(Map(Hex("00") -> Array[Byte](1), Hex("10") -> Array[Byte](2)))
    } yield expect.all(emptyTerminal.rootHash.value.value.nonEmpty, canonical.rootHash.value.value.nonEmpty)
  }

  test("parallel incremental insertion preserves sole-empty-key parity and rejects a descendant") { implicit res =>
    implicit val (json, hasher) = res
    val producer = ParallelMerklePatriciaProducer[IO]
    val emptyEntry = Map(Hex("") -> Array[Byte](1))

    for {
      emptyTrie <- producer.createFromBytes(Map.empty)
      full <- producer.createFromBytes(emptyEntry)
      incremental <- producer.insertFromBytes(emptyTrie, emptyEntry).rethrow
      rejected <- producer.insertFromBytes(incremental, Map(Hex("00") -> Array[Byte](2)))
    } yield
      expect.all(
        incremental.rootHash == full.rootHash,
        rejected == Left(TerminalPhysicalTrieKeyCollision(Hex(""), Hex("00")))
      )
  }

  test("public producer paths sort inserts and removals to match canonical full builds") { implicit res =>
    implicit val (json, hasher) = res
    val parallel = ParallelMerklePatriciaProducer[IO]
    val stateless = new StatelessMerklePatriciaProducer[IO]
    val all = ListMap(
      Hex("00aa") -> Array[Byte](1),
      Hex("00bb") -> Array[Byte](2),
      Hex("10aa") -> Array[Byte](3),
      Hex("1faa") -> Array[Byte](4),
      Hex("ff00") -> Array[Byte](5)
    )
    val base = ListMap(all.head)
    val reversedTail = ListMap(all.tail.toList.reverse: _*)
    val typedAll = ListMap(all.keys.toList.zipWithIndex.map { case (key, index) => key -> Hash.fromBytes(Array(index.toByte)) }: _*)
    val typedBase = ListMap(typedAll.head)
    val typedReversedTail = ListMap(typedAll.tail.toList.reverse: _*)
    val removed = List(Hex("1faa"), Hex("00bb"))
    val remaining = all -- removed

    Files[IO].tempDirectory.use { directory =>
      for {
        full <- parallel.createFromBytes(all)
        expectedAfterRemove <- parallel.createFromBytes(remaining)
        parallelBase <- parallel.createFromBytes(base)
        parallelIncremental <- parallel.insertFromBytes(parallelBase, reversedTail).rethrow
        parallelRemoved <- parallel.remove(full, removed.reverse).rethrow
        statelessFull <- stateless.createFromBytes(all)
        statelessTypedFull <- stateless.create(typedAll)
        statelessBase <- stateless.create(typedBase)
        statelessIncremental <- stateless.insert(statelessBase, typedReversedTail).rethrow
        statelessRemoved <- stateless.remove(statelessFull, removed.reverse).rethrow
        memory <- InMemoryMerklePatriciaProducer.make[IO](base)
        _ <- memory.build.rethrow
        _ <- memory.insertBytes(reversedTail).rethrow
        memoryIncremental <- memory.build.rethrow
        _ <- memory.remove(removed.reverse).rethrow
        memoryRemoved <- memory.build.rethrow
        filesystem <- FileSystemMerklePatriciaProducer.make[IO](directory, base)
        _ <- filesystem.build.rethrow
        _ <- filesystem.insertBytes(reversedTail).rethrow
        filesystemIncremental <- filesystem.build.rethrow
        _ <- filesystem.remove(removed.reverse).rethrow
        filesystemRemoved <- filesystem.build.rethrow
      } yield
        expect.all(
          parallelIncremental.rootHash == full.rootHash,
          statelessFull.rootHash == full.rootHash,
          statelessIncremental.rootHash == statelessTypedFull.rootHash,
          memoryIncremental.rootHash == full.rootHash,
          filesystemIncremental.rootHash == full.rootHash,
          parallelRemoved.rootHash == expectedAfterRemove.rootHash,
          statelessRemoved.rootHash == expectedAfterRemove.rootHash,
          memoryRemoved.rootHash == expectedAfterRemove.rootHash,
          filesystemRemoved.rootHash == expectedAfterRemove.rootHash
        )
    }
  }

  test("in-memory producer rejects colliding insert and alias removal without changing entries or root") { implicit res =>
    implicit val (json, hasher) = res
    val initial = Map(Hex("aa") -> Array[Byte](1), Hex("bb") -> Array[Byte](2))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      before <- producer.entries
      beforeRoot <- producer.build.rethrow.map(_.rootHash)
      insertResult <- producer.insertBytes(Map(Hex("aa00") -> Array[Byte](3)))
      removeResult <- producer.remove(List(Hex("AA")))
      after <- producer.entries
      afterRoot <- producer.build.rethrow.map(_.rootHash)
    } yield
      expect.all(
        insertResult == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        removeResult == Left(NonCanonicalPhysicalTrieKey(Hex("AA"), Hex("aa"))),
        sameEntries(before, after),
        beforeRoot == afterRoot
      )
  }

  test("filesystem producer rejects a colliding insert without changing entries or root") { implicit res =>
    implicit val (json, hasher) = res
    val initial = Map(Hex("aa") -> Array[Byte](1), Hex("bb") -> Array[Byte](2))

    Files[IO].tempDirectory.use { directory =>
      for {
        producer <- FileSystemMerklePatriciaProducer.make[IO](directory, initial)
        before <- producer.entries
        beforeRoot <- producer.build.rethrow.map(_.rootHash)
        result <- producer.insertBytes(Map(Hex("aa00") -> Array[Byte](3)))
        after <- producer.entries
        afterRoot <- producer.build.rethrow.map(_.rootHash)
      } yield
        expect.all(
          result == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
          sameEntries(before, after),
          beforeRoot == afterRoot
        )
    }
  }

  test("concurrent terminal and descendant inserts publish exactly one valid state") { implicit res =>
    implicit val (json, hasher) = res
    val terminal = Map(Hex("aa") -> Array[Byte](1))
    val descendant = Map(Hex("aa00") -> Array[Byte](2))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      results <- (producer.insertBytes(terminal), producer.insertBytes(descendant)).parTupled
      entries <- producer.entries
      root <- producer.build.rethrow.map(_.rootHash)
      rebuilt <- ParallelMerklePatriciaProducer[IO].createFromBytes(entries)
    } yield
      expect.all(
        List(results._1, results._2).count(_.isRight) == 1,
        List(results._1, results._2).count(_.isLeft) == 1,
        entries.keySet == terminal.keySet || entries.keySet == descendant.keySet,
        root == rebuilt.rootHash
      )
  }

  test("raw MptStore replacement preflight rejects before clear and keeps its ordinal retryable") { implicit res =>
    implicit val (json, hasher) = res
    val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(7L))
    val initial = Map(Hex("aa") -> Array[Byte](1))
    val invalid = Map(Hex("bb") -> Array[Byte](2), Hex("bb00") -> Array[Byte](3))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      before <- store.allEntriesAsBytes
      beforeRoot <- store.build(ordinal).rethrow.map(_.rootHash)
      result <- store.loadBytes(invalid, ordinal).attempt
      after <- store.allEntriesAsBytes
      afterRoot <- store.build(ordinal).rethrow.map(_.rootHash)
      synced <- store.lastPersistedOrdinal
    } yield
      expect.all(
        result == Left(TerminalPhysicalTrieKeyCollision(Hex("bb"), Hex("bb00"))),
        sameEntries(before, after),
        beforeRoot == afterRoot,
        synced.isEmpty
      )
  }

  test("raw MptStore replacement restores its savepoint when canonical-key bytes fail to build") { implicit res =>
    implicit val (json, hasher) = res
    val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(8L))
    val initial = Map(Hex("aa") -> Array[Byte](1))
    val unbuildable = Map(Hex("bb") -> null.asInstanceOf[Array[Byte]])

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      before <- store.allEntriesAsBytes
      beforeRoot <- store.build(ordinal).rethrow.map(_.rootHash)
      result <- store.loadBytes(unbuildable, ordinal).attempt
      after <- store.allEntriesAsBytes
      afterRoot <- store.build(ordinal).rethrow.map(_.rootHash)
      synced <- store.lastPersistedOrdinal
    } yield
      expect.all(
        result.isLeft,
        sameEntries(before, after),
        beforeRoot == afterRoot,
        synced.isEmpty
      )
  }

  test("MptStore update validates the post-update image before applying removals") { implicit res =>
    implicit val (json, hasher) = res
    val initial = Map(Hex("aa") -> Array[Byte](1), Hex("bb") -> Array[Byte](2))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      before <- store.allEntriesAsBytes
      result <- store.update[Hash](Map(Hex("aa00") -> Hash.empty), Set(Hex("bb"))).attempt
      after <- store.allEntriesAsBytes
    } yield
      expect.all(
        result == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        sameEntries(before, after)
      )
  }

  test("MptStore rejects distinct logical keys that encode to one physical path before last-write merge") { implicit res =>
    implicit val (json, hasher) = res
    val initial = Map(Hex("bb") -> Array[Byte](1))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, String](producer, _ => Hex("aa").pure[IO])
      before <- store.allEntriesAsBytes
      beforeRoot <- producer.build.rethrow.map(_.rootHash)
      result <- store.insert[Hash](Map("first" -> Hash.empty, "second" -> Hash.empty)).attempt
      after <- store.allEntriesAsBytes
      afterRoot <- producer.build.rethrow.map(_.rootHash)
    } yield
      expect.all(
        result == Left(DuplicatePhysicalTriePath(Hex("aa"), Hex("aa"), Hex("aa"))),
        sameEntries(before, after),
        beforeRoot == afterRoot
      )
  }

  test("MptStore rejects distinct typed GlobalStateKeys that serialize to one physical path before map collapse") { implicit res =>
    implicit val (json, hasher) = res
    val address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
    val metagraphKey = GlobalStateKey(MetagraphNamespace(address), Balances, EmptyNamespace, EmptyNamespace)
    val addressKey = GlobalStateKey(AddressNamespace(address), Balances, EmptyNamespace, EmptyNamespace)
    val initial = Map(Hex("bb") -> Array[Byte](1))

    for {
      metagraphHex <- GlobalStateKey.toHex[IO](metagraphKey)
      addressHex <- GlobalStateKey.toHex[IO](addressKey)
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      before <- store.allEntriesAsBytes
      beforeRoot <- producer.build.rethrow.map(_.rootHash)
      result <- store.insert[Hash](Map(metagraphKey -> Hash.empty, addressKey -> Hash.empty)).attempt
      after <- store.allEntriesAsBytes
      afterRoot <- producer.build.rethrow.map(_.rootHash)
    } yield
      expect.all(
        metagraphKey != addressKey,
        metagraphHex == addressHex,
        result == Left(DuplicatePhysicalTriePath(metagraphHex, metagraphHex, addressHex)),
        sameEntries(before, after),
        beforeRoot == afterRoot
      )
  }

  test("persisted and wire MPT boundaries reject ambiguous physical-key sets") { implicit res =>
    implicit val (json, hasher) = res
    val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(11L))
    val aliasJson = "{\"aa\":[1],\"AA\":[2]}"
    val prefixJson = "{\"aa\":[1],\"aa00\":[2]}"
    val invalid = Map(Hex("aa") -> Array[Byte](1), Hex("aa00") -> Array[Byte](2))

    Files[IO].tempDirectory.use { directory =>
      for {
        storage <- MptStateStorage.make[IO](directory)
        aliasDecoded = io.circe.parser.decode[Map[Hex, Array[Byte]]](aliasJson)(MptStateStorage.mptEntriesDecoder)
        prefixDecoded = io.circe.parser.decode[Map[Hex, Array[Byte]]](prefixJson)(MptStateStorage.mptEntriesDecoder)
        writeResult <- storage.writeState(ordinal, invalid).attempt
        existsAfterRejectedWrite <- storage.exists(ordinal)
        _ <- Stream
          .emits(prefixJson.getBytes("UTF-8"))
          .covary[IO]
          .through(Files[IO].writeAll(directory / ordinal.value.value.toString))
          .compile
          .drain
        readResult <- storage.readState(ordinal).attempt
        producer <- FileSystemMerklePatriciaProducer.make[IO](directory / "producer", Map(Hex("bb") -> Array[Byte](3)))
        before <- producer.entries
        beforeRoot <- producer.build.rethrow.map(_.rootHash)
        producerPath = directory / "producer" / ordinal.value.value.toString
        _ <- Stream
          .emits(prefixJson.getBytes("UTF-8"))
          .covary[IO]
          .through(Files[IO].writeAll(producerPath))
          .compile
          .drain
        loadResult <- producer.load(ordinal).attempt
        loadOrBuildResult <- producer.loadOrBuild(ordinal, Map(Hex("cc") -> Array[Byte](4)).pure[IO]).attempt
        after <- producer.entries
        afterRoot <- producer.build.rethrow.map(_.rootHash)
      } yield
        expect.all(
          aliasDecoded.isLeft,
          prefixDecoded.isLeft,
          writeResult == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
          !existsAfterRejectedWrite,
          readResult.isLeft,
          loadResult.isLeft,
          loadOrBuildResult.isLeft,
          sameEntries(before, after),
          beforeRoot == afterRoot
        )
    }
  }

  test("failed syncFullIfNeeded does not poison the ordinal retry marker") { implicit res =>
    implicit val (json, hasher) = res
    val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(9L))
    val invalid = Map(Hex("aa") -> Hash.empty, Hex("aa00") -> Hash.empty)
    val valid = Map(Hex("cc") -> Hash.empty)

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](Map(Hex("bb") -> Array[Byte](1)))
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      rejected <- store.syncFullIfNeeded[Hash](invalid.pure[IO], ordinal).attempt
      afterRejected <- store.lastPersistedOrdinal
      _ <- store.syncFullIfNeeded[Hash](valid.pure[IO], ordinal)
      afterRetry <- store.lastPersistedOrdinal
      entries <- store.allEntriesAsBytes
    } yield
      expect.all(
        rejected == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        afterRejected.isEmpty,
        afterRetry.contains(ordinal),
        entries.keySet == valid.keySet
      )
  }

  test("empty replacement persists an explicit empty generation instead of resurrecting prior state") { implicit res =>
    implicit val (json, hasher) = res
    val initialOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(12L))
    val emptyOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(13L))
    val initial = Map(Hex("aa") -> Array[Byte](1))

    Files[IO].tempDirectory.use { directory =>
      for {
        producer <- FileSystemMerklePatriciaProducer.make[IO](directory, initial)
        store <- MptStore.make[IO, Hex](producer, _.pure[IO])
        storage <- MptStateStorage.make[IO](directory)
        _ <- producer.persist(initialOrdinal)
        before <- storage.readState(initialOrdinal)
        _ <- store.loadBytes(Map.empty, emptyOrdinal)
        persistedEmpty <- awaitPersistedState(storage, emptyOrdinal)
        restarted <- FileSystemMerklePatriciaProducer.make[IO](directory)
        restartedStore <- MptStore.make[IO, Hex](restarted, _.pure[IO])
        loaded <- restartedStore.loadPersisted(emptyOrdinal)
        afterRestart <- restartedStore.allEntriesAsBytes
        restartedOrdinal <- restartedStore.lastPersistedOrdinal
      } yield
        expect.all(
          before.exists(_.nonEmpty),
          persistedEmpty.exists(_.isEmpty),
          loaded,
          afterRestart.isEmpty,
          restartedOrdinal.contains(emptyOrdinal)
        )
    }
  }

  test("public producer factories bind Generic or a validated immutable fixed-width policy") { implicit res =>
    implicit val (json, hasher) = res

    Files[IO].tempDirectory.use { directory =>
      for {
        memoryGeneric <- InMemoryMerklePatriciaProducer.make[IO]()
        memoryFixed <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](2)
        invalidMemoryWidth <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](0).attempt
        filesystemGeneric <- FileSystemMerklePatriciaProducer.make[IO](directory / "generic")
        filesystemFixed <- FileSystemMerklePatriciaProducer.makeWithFixedWidthKeys[IO](directory / "fixed", 2)
        invalidFilesystemWidth <- FileSystemMerklePatriciaProducer
          .makeWithFixedWidthKeys[IO](directory / "invalid", 0)
          .attempt
      } yield
        expect.all(
          memoryGeneric.physicalKeyPolicy == PhysicalTrieKeyPolicy.Generic,
          memoryFixed.physicalKeyPolicy.exactWidthBytes.contains(2),
          invalidMemoryWidth == Left(PhysicalTrieKeyPolicy.InvalidFixedWidth(0)),
          filesystemGeneric.physicalKeyPolicy == PhysicalTrieKeyPolicy.Generic,
          filesystemFixed.physicalKeyPolicy.exactWidthBytes.contains(2),
          invalidFilesystemWidth == Left(PhysicalTrieKeyPolicy.InvalidFixedWidth(0))
        )
    }
  }

  test("in-memory fixed-width producer enforces its immutable policy across initial, mutation, clear, and savepoint restore") {
    implicit res =>
      implicit val (json, hasher) = res
      val initial = Map(Hex("aabb") -> Array[Byte](1))
      val short = Hex("aa")

      for {
        invalidInitial <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](2, Map(short -> Array[Byte](0))).attempt
        producer <- InMemoryMerklePatriciaProducer.makeWithFixedWidthKeys[IO](2, initial)
        savepoint <- producer.savepoint
        validInsert <- producer.insertBytes(Map(Hex("ccdd") -> Array[Byte](2)))
        invalidInsert <- producer.insertBytes(Map(short -> Array[Byte](3)))
        invalidRemove <- producer.remove(List(Hex("ff")))
        beforeClear <- producer.entries
        _ <- producer.clear
        empty <- producer.entries
        widthAfterClear = producer.physicalKeyPolicy.exactWidthBytes
        _ <- savepoint.restore
        afterRestore <- producer.entries
        widthAfterRestore = producer.physicalKeyPolicy.exactWidthBytes
      } yield
        expect.all(
          invalidInitial == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
          validInsert == Right(()),
          invalidInsert == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
          invalidRemove == Left(UnexpectedPhysicalTrieKeyWidth(Hex("ff"), 2, 1)),
          beforeClear.keySet == Set(Hex("aabb"), Hex("ccdd")),
          empty.isEmpty,
          widthAfterClear.contains(2),
          sameEntries(afterRestore, initial),
          widthAfterRestore.contains(2)
        )
  }

  test("filesystem fixed-width producer rejects a wrong-width disk generation without changing the loaded generation or policy") {
    implicit res =>
      implicit val (json, hasher) = res
      val validOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(20L))
      val invalidOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(21L))
      val valid = Map(Hex("aabb") -> Array[Byte](1), Hex("ccdd") -> Array[Byte](2))
      val short = Hex("aa")

      Files[IO].tempDirectory.use { directory =>
        for {
          invalidInitial <- FileSystemMerklePatriciaProducer
            .makeWithFixedWidthKeys[IO](directory / "invalid", 2, Map(short -> Array[Byte](0)))
            .attempt
          producer <- FileSystemMerklePatriciaProducer.makeWithFixedWidthKeys[IO](directory, 2, valid)
          _ <- producer.persist(validOrdinal)
          storage <- MptStateStorage.make[IO](directory)
          persisted <- awaitPersistedState(storage, validOrdinal)
          _ <- storage.writeState(invalidOrdinal, Map(short -> Array[Byte](9)))
          restarted <- FileSystemMerklePatriciaProducer.makeWithFixedWidthKeys[IO](directory, 2)
          loaded <- restarted.load(validOrdinal)
          before <- restarted.entries
          rejected <- restarted.load(invalidOrdinal).attempt
          after <- restarted.entries
          savepoint <- restarted.savepoint
          _ <- restarted.clear
          widthAfterClear = restarted.physicalKeyPolicy.exactWidthBytes
          _ <- savepoint.restore
          restored <- restarted.entries
          widthAfterRestore = restarted.physicalKeyPolicy.exactWidthBytes
        } yield
          expect.all(
            invalidInitial == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
            persisted.exists(image => sameEntries(image, valid)),
            loaded,
            rejected == Left(UnexpectedPhysicalTrieKeyWidth(short, 2, 1)),
            sameEntries(before, valid),
            sameEntries(after, valid),
            widthAfterClear.contains(2),
            sameEntries(restored, valid),
            widthAfterRestore.contains(2)
          )
      }
  }
}
