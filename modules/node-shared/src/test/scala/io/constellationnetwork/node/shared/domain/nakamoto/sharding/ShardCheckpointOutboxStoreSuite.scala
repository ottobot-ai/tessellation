package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.io.IOException
import java.nio.file.{Files, Path, StandardOpenOption}
import java.nio.{ByteBuffer, ByteOrder}
import java.security.MessageDigest

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointOutboxRecoveryRequired.{
  ConflictingOccupiedSlot,
  CorruptRecord
}
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointOutboxSnapshot.{Empty, Occupied}
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointOutboxStore.OutboxWriteArtifact
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointOutboxUnavailable.DurableWriteFailed
import io.constellationnetwork.storage.durable._

import weaver.SimpleIOSuite

object ShardCheckpointOutboxStoreSuite extends SimpleIOSuite {
  import DurableWriteBoundary._
  import DurableWriteStage._

  private val limits = ShardCheckpointOutboxLimits
    .from(maxOpaqueIdBytes = 32, maxArtifactBytes = 512L)
    .fold(throw _, identity)

  private type WriteEvent = DurableWriteEvent[OutboxWriteArtifact]

  private final class FailAt(
    stage: DurableWriteStage,
    boundary: DurableWriteBoundary,
    failure: Throwable
  ) extends DurableWriteHook[IO, OutboxWriteArtifact] {
    def onEvent(event: WriteEvent): IO[Unit] =
      if (event.stage == stage && event.boundary == boundary) IO.raiseError(failure)
      else IO.unit
  }

  private final class RecordEvents(events: Ref[IO, Vector[(DurableWriteStage, DurableWriteBoundary)]])
      extends DurableWriteHook[IO, OutboxWriteArtifact] {
    def onEvent(event: WriteEvent): IO[Unit] = events.update(_ :+ (event.stage -> event.boundary))
  }

  test("install captures exact opaque inputs and returns only a non-serializable custody witness") {
    tempDirectory.use { directory =>
      ShardCheckpointOutboxStore.resource[IO](directory, limits).use { store =>
        val suppliedId = Array[Byte](1, 2, 3)
        val suppliedArtifact = Array[Byte](9, 8, 7, 6)
        val expectedId = suppliedId.clone()
        val expectedArtifact = suppliedArtifact.clone()

        for {
          empty <- requireEmpty(store)
          custody <- store.install(empty.head, suppliedId, suppliedArtifact)
          _ <- IO.delay(java.util.Arrays.fill(suppliedId, 0.toByte))
          _ <- IO.delay(java.util.Arrays.fill(suppliedArtifact, 0.toByte))
          read <- store.readExact(custody)
          loaded <- store.load
        } yield
          loaded match {
            case occupied: Occupied =>
              expect.all(
                custody.byteLength == expectedArtifact.length.toLong,
                custody.copyOpaqueId.sameElements(expectedId),
                read.sameElements(expectedArtifact),
                occupied.custody.copyOpaqueId.sameElements(expectedId),
                !custody.asInstanceOf[AnyRef].isInstanceOf[java.io.Serializable],
                !occupied.head.asInstanceOf[AnyRef].isInstanceOf[java.io.Serializable]
              )
            case _: Empty => failure("installed record loaded as empty")
          }
      }
    }
  }

  test("restart loads verified custody but rejects a capability from the previous live store") {
    tempDirectory.use { directory =>
      val opaqueId = Array[Byte](4, 5, 6)
      val artifact = Array[Byte](11, 12, 13)

      ShardCheckpointOutboxStore.resource[IO](directory, limits).use { first =>
        requireEmpty(first).flatMap(empty => first.install(empty.head, opaqueId, artifact))
      }.flatMap { staleCustody =>
        ShardCheckpointOutboxStore.resource[IO](directory, limits).use { reopened =>
          for {
            snapshot <- reopened.load
            current <- snapshot match {
              case occupied: Occupied => reopened.readExact(occupied.custody)
              case _: Empty           => IO.raiseError[Array[Byte]](new AssertionError("durable record missing after restart"))
            }
            stale <- reopened.readExact(staleCustody).attempt
          } yield expect.all(current.sameElements(artifact), stale.isLeft)
        }
      }
    }
  }

  test("same expected head and exact bytes are idempotent while a different occupant requires recovery") {
    tempDirectory.use { directory =>
      ShardCheckpointOutboxStore.resource[IO](directory, limits).use { store =>
        val opaqueId = Array[Byte](1)
        val artifact = Array[Byte](2, 3, 4)

        for {
          empty <- requireEmpty(store)
          first <- store.install(empty.head, opaqueId, artifact)
          retry <- store.install(empty.head, opaqueId.clone(), artifact.clone())
          conflict <- store.install(empty.head, Array[Byte](9), artifact).attempt
          firstRead <- store.readExact(first)
          retryRead <- store.readExact(retry)
        } yield
          expect.all(
            firstRead.sameElements(artifact),
            retryRead.sameElements(artifact),
            conflict.swap.exists(_.isInstanceOf[ConflictingOccupiedSlot])
          )
      }
    }
  }

  test("concurrent expected-head CAS installs exactly one of two different opaque artifacts") {
    tempDirectory.use { directory =>
      ShardCheckpointOutboxStore.resource[IO](directory, limits).use { store =>
        val idA = Array[Byte](1)
        val bytesA = Array[Byte](10, 11)
        val idB = Array[Byte](2)
        val bytesB = Array[Byte](20, 21)

        for {
          empty <- requireEmpty(store)
          results <- (
            store.install(empty.head, idA, bytesA).attempt,
            store.install(empty.head, idB, bytesB).attempt
          ).parTupled
          snapshot <- store.load
          persisted <- snapshot match {
            case occupied: Occupied => store.readExact(occupied.custody)
            case _: Empty           => IO.raiseError[Array[Byte]](new AssertionError("CAS race persisted no winner"))
          }
          successes = List(results._1, results._2).count(_.isRight)
          conflicts = List(results._1, results._2).count(_.swap.exists(_.isInstanceOf[ConflictingOccupiedSlot]))
        } yield expect.all(successes == 1, conflicts == 1, persisted.sameElements(bytesA) || persisted.sameElements(bytesB))
      }
    }
  }

  test("successful install crosses write, file-force, atomic-move, directory-force, and exact-readback boundaries in order") {
    tempDirectory.use { directory =>
      Ref.of[IO, Vector[(DurableWriteStage, DurableWriteBoundary)]](Vector.empty).flatMap { events =>
        val hook = new RecordEvents(events)
        ShardCheckpointOutboxStore
          .resourceWith[IO](directory, limits, DurableFileOps.nio[IO], hook)
          .use { store =>
            for {
              empty <- requireEmpty(store)
              _ <- store.install(empty.head, Array[Byte](1), Array[Byte](2))
              observed <- events.get
              expected = DurableWriteStage.ordered.flatMap(stage => List(stage -> Before, stage -> After)).toVector
            } yield expect(observed == expected)
          }
      }
    }
  }

  test("failure before atomic move leaves the durable CAS head empty and retryable after restart") {
    tempDirectory.use { directory =>
      val hook = new FailAt(AtomicMove, Before, new IOException("simulated crash before move"))
      val failed = ShardCheckpointOutboxStore
        .resourceWith[IO](directory, limits, DurableFileOps.nio[IO], hook)
        .use { store =>
          requireEmpty(store).flatMap(empty => store.install(empty.head, Array[Byte](1), Array[Byte](2))).attempt
        }

      failed.flatMap { result =>
        ShardCheckpointOutboxStore.resource[IO](directory, limits).use { reopened =>
          reopened.load.map { snapshot =>
            expect.all(result.swap.exists(_.isInstanceOf[DurableWriteFailed]), snapshot.isInstanceOf[Empty])
          }
        }
      }
    }
  }

  test("failure after atomic move reopens the exact occupied record instead of permitting a different install") {
    tempDirectory.use { directory =>
      val opaqueId = Array[Byte](7)
      val artifact = Array[Byte](8, 9)
      val hook = new FailAt(AtomicMove, After, new IOException("simulated crash after move"))
      val failed = ShardCheckpointOutboxStore
        .resourceWith[IO](directory, limits, DurableFileOps.nio[IO], hook)
        .use { store =>
          requireEmpty(store).flatMap(empty => store.install(empty.head, opaqueId, artifact)).attempt
        }

      failed.flatMap { result =>
        ShardCheckpointOutboxStore.resource[IO](directory, limits).use { reopened =>
          for {
            snapshot <- reopened.load
            pair <- snapshot match {
              case occupied: Occupied =>
                reopened.readExact(occupied.custody).flatMap { exact =>
                  reopened.install(occupied.head, Array[Byte](99), artifact).attempt.map(exact -> _)
                }
              case _: Empty => IO.raiseError[(Array[Byte], Either[Throwable, DurablyCustodiedShardCheckpoint])](
                  new AssertionError("post-move record reopened as empty")
                )
            }
          } yield
            expect.all(
              result.swap.exists(_.isInstanceOf[DurableWriteFailed]),
              pair._1.sameElements(artifact),
              pair._2.swap.exists(_.isInstanceOf[ConflictingOccupiedSlot])
            )
        }
      }
    }
  }

  test("checksum corruption and oversized records fail resource acquisition with RecoveryRequired") {
    tempDirectory.use { corruptDirectory =>
      tempDirectory.use { oversizedDirectory =>
        val install = ShardCheckpointOutboxStore.resource[IO](corruptDirectory, limits).use { store =>
          requireEmpty(store).flatMap(empty => store.install(empty.head, Array[Byte](1), Array[Byte](2, 3))).void
        }

        for {
          _ <- install
          _ <- flipLastByte(corruptDirectory.resolve("held.local"))
          corrupt <- ShardCheckpointOutboxStore.resource[IO](corruptDirectory, limits).use_.attempt
          _ <- IO.blocking(
            Files.write(
              oversizedDirectory.resolve("held.local"),
              new Array[Byte](limits.maxRecordBytes + 1),
              StandardOpenOption.CREATE_NEW,
              StandardOpenOption.WRITE
            )
          )
          oversized <- ShardCheckpointOutboxStore.resource[IO](oversizedDirectory, limits).use_.attempt
        } yield
          expect.all(
            corrupt.swap.exists(_.isInstanceOf[CorruptRecord]),
            oversized.swap.exists(_.isInstanceOf[CorruptRecord])
          )
      }
    }
  }

  test("checksum-valid records cannot trade one oversized field against the other configured bound") {
    tempDirectory.use { oversizedIdDirectory =>
      tempDirectory.use { oversizedArtifactDirectory =>
        val oversizedId = new Array[Byte](limits.maxOpaqueIdBytes + 1)
        val oneByteArtifact = Array[Byte](1)
        val oneByteId = Array[Byte](2)
        val oversizedArtifact = new Array[Byte]((limits.maxArtifactBytes + 1L).toInt)

        for {
          _ <- writeLocalEnvelope(oversizedIdDirectory.resolve("held.local"), oversizedId, oneByteArtifact)
          idFailure <- ShardCheckpointOutboxStore.resource[IO](oversizedIdDirectory, limits).use_.attempt
          _ <- writeLocalEnvelope(oversizedArtifactDirectory.resolve("held.local"), oneByteId, oversizedArtifact)
          artifactFailure <- ShardCheckpointOutboxStore.resource[IO](oversizedArtifactDirectory, limits).use_.attempt
          idDetail = idFailure.swap.toOption.collect { case error: CorruptRecord => error.detail }
          artifactDetail = artifactFailure.swap.toOption.collect { case error: CorruptRecord => error.detail }
        } yield
          expect.all(
            idDetail.exists(_.contains("opaque id length")),
            artifactDetail.exists(_.contains("artifact length"))
          )
      }
    }
  }

  private def requireEmpty(store: ShardCheckpointOutboxStore[IO]): IO[Empty] =
    store.load.flatMap {
      case empty: Empty       => IO.pure(empty)
      case _: Occupied        => IO.raiseError(new AssertionError("expected an empty outbox slot"))
    }

  private def flipLastByte(path: Path): IO[Unit] =
    IO.blocking {
      val bytes = Files.readAllBytes(path)
      bytes(bytes.length - 1) = (bytes.last ^ 0x01).toByte
      Files.write(path, bytes, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)
    }.void

  private def writeLocalEnvelope(path: Path, opaqueId: Array[Byte], artifact: Array[Byte]): IO[Unit] =
    IO.blocking {
      val checksumBytes = 32
      val bodyBytes = java.lang.Long.BYTES + (2 * java.lang.Integer.BYTES) + java.lang.Long.BYTES + opaqueId.length + artifact.length
      val output = ByteBuffer.allocate(bodyBytes + checksumBytes).order(ByteOrder.BIG_ENDIAN)
      output.putLong(0x53434f5554425831L)
      output.putInt(1)
      output.putInt(opaqueId.length)
      output.putLong(artifact.length.toLong)
      output.put(opaqueId)
      output.put(artifact)
      output.put(MessageDigest.getInstance("SHA-256").digest(output.array().take(bodyBytes)))
      Files.write(path, output.array(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    }.void

  private def tempDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("shard-checkpoint-outbox")))(directory =>
      IO.blocking {
        val stream = Files.walk(directory)
        try stream.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
        finally stream.close()
      }.void
    )
}
