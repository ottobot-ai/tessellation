package io.constellationnetwork.security.mpt

import cats.effect.kernel.Resource
import cats.effect.{IO, Ref}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.FileSystemMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Path
import weaver.MutableIOSuite

object MptPersistenceFailureSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.map { jsonSerializer =>
      implicit val json: JsonSerializer[IO] = jsonSerializer
      (jsonSerializer, Hasher.forJson[IO])
    }

  private val ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(12L))
  private val initial = Map(Hex(Hash.empty.value) -> Array[Byte](1, 2, 3))

  private final class RecordingStorage(
    writeResult: IO[Unit],
    writes: Ref[IO, Int],
    cutoffs: Ref[IO, Int]
  )(implicit jsonSerializer: JsonSerializer[IO])
      extends MptStateStorage[IO](Path("/tmp/mpt-persistence-failure-suite-unused")) {

    override def writeState(ordinal: SnapshotOrdinal, state: Map[Hex, Array[Byte]]): IO[Unit] =
      writes.update(_ + 1) >> writeResult

    override def applyCutoff(currentOrdinal: SnapshotOrdinal): IO[Unit] =
      cutoffs.update(_ + 1)
  }

  private def producer(storage: MptStateStorage[IO])(implicit hasher: Hasher[IO], jsonSerializer: JsonSerializer[IO]) =
    for {
      state <- Ref.of[IO, Map[Hex, Array[Byte]]](initial)
      trie <- Ref.of[IO, Option[MerklePatriciaTrie]](None)
      pendingInserts <- Ref.of[IO, Map[Hex, Array[Byte]]](Map.empty)
      pendingRemoves <- Ref.of[IO, List[Hex]](List.empty)
      roots <- Ref.of[IO, Map[SnapshotOrdinal, MptRoot]](Map.empty)
      lastBuilt <- Ref.of[IO, Option[SnapshotOrdinal]](None)
    } yield
      new FileSystemMerklePatriciaProducer[IO](
        state,
        trie,
        pendingInserts,
        pendingRemoves,
        storage,
        roots,
        lastBuilt
      )

  test("failed state write propagates and never applies retention cutoff") { res =>
    implicit val (jsonSerializer, hasher) = res
    val failure = new RuntimeException("injected write failure")

    for {
      writes <- Ref.of[IO, Int](0)
      cutoffs <- Ref.of[IO, Int](0)
      storage = new RecordingStorage(IO.raiseError(failure), writes, cutoffs)
      p <- producer(storage)
      result <- p.persist(ordinal).attempt
      writeCount <- writes.get
      cutoffCount <- cutoffs.get
    } yield
      expect.all(
        result == Left(failure),
        writeCount == 1,
        cutoffCount == 0
      )
  }

  test("successful state write applies retention cutoff exactly once") { res =>
    implicit val (jsonSerializer, hasher) = res

    for {
      writes <- Ref.of[IO, Int](0)
      cutoffs <- Ref.of[IO, Int](0)
      storage = new RecordingStorage(IO.unit, writes, cutoffs)
      p <- producer(storage)
      result <- p.persist(ordinal).attempt
      writeCount <- writes.get
      cutoffCount <- cutoffs.get
    } yield
      expect.all(
        result == Right(()),
        writeCount == 1,
        cutoffCount == 1
      )
  }
}
