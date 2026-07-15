package io.constellationnetwork.node.shared.infrastructure.snapshot.storage

import java.io.{FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files => JFiles}

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.http.routes.SnapshotRoutesActiveEraSuite
import io.constellationnetwork.schema.height.Height
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.{Files, Path}
import weaver.MutableIOSuite

object CombinedSnapshotCheckpointFileSystemStorageSuite extends MutableIOSuite {

  type Res = JsonSerializer[IO]

  override def sharedResource: Resource[IO, Res] = JsonSerializer.forAsync[IO].asResource

  private val snapshot = SnapshotRoutesActiveEraSuite.signed(SnapshotRoutesActiveEraSuite.snapshot)
  private val replacement = SnapshotRoutesActiveEraSuite.signed(
    SnapshotRoutesActiveEraSuite.snapshot.copy(height = Height(NonNegLong.unsafeFrom(1L)))
  )
  private val checkpointInfo = GlobalSnapshotInfo.empty

  test("writer close failure retains the prior checkpoint and removes the temporary file") { _ =>
    Files[IO].tempDirectory.use { directory =>
      val checkpointDirectory = directory / "checkpoints"
      val checkpointPath = checkpointDirectory / snapshot.ordinal.value.value.toString
      val forcedFailure = new IOException("forced writer close failure")
      val failingWriter: Path => Resource[IO, OutputStreamWriter] = temporaryPath =>
        Resource.make(
          IO.blocking {
            val file = temporaryPath.toNioPath.toFile
            Option(file.getParentFile).foreach(_.mkdirs())
            new OutputStreamWriter(new FileOutputStream(file), UTF_8)
          }
        )(writer => IO.blocking(writer.close()) >> IO.raiseError(forcedFailure))

      for {
        storage <- CombinedSnapshotCheckpointFileSystemStorage.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
          checkpointDirectory
        )
        _ <- storage.tryWrite(snapshot.ordinal, snapshot, checkpointInfo, Hash.empty)
        before <- IO.blocking(JFiles.readAllBytes(checkpointPath.toNioPath))
        failingStorage <- CombinedSnapshotCheckpointFileSystemStorage
          .makeWithWriter[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](checkpointDirectory, failingWriter)
        attempted <- failingStorage.tryWrite(replacement.ordinal, replacement, checkpointInfo, Hash.empty).attempt
        after <- IO.blocking(JFiles.readAllBytes(checkpointPath.toNioPath))
        stored <- Files[IO].list(checkpointDirectory).compile.toList
        temporaryFiles = stored.filter(_.fileName.toString.endsWith(".tmp"))
      } yield
        expect(attempted.left.exists(_ eq forcedFailure)) &&
          expect(before.sameElements(after)) &&
          expect(temporaryFiles.isEmpty)
    }
  }

  test("releaseOnce runs the allocated finalizer once when cancellation overlaps a second release") { _ =>
    for {
      releases <- Ref.of[IO, Int](0)
      entered <- Deferred[IO, Unit]
      allowRelease <- Deferred[IO, Unit]
      secondCompleted <- Deferred[IO, Unit]
      release = releases.update(_ + 1) >> entered.complete(()).void >> allowRelease.get
      guarded <- CombinedSnapshotCheckpointFileSystemStorage.releaseOnce[IO](release)
      first <- guarded.start
      _ <- entered.get.timeout(3.seconds)
      cancellation <- first.cancel.start
      second <- (guarded >> secondCompleted.complete(()).void).start
      _ <- IO.sleep(100.millis)
      secondBeforeRelease <- secondCompleted.tryGet
      _ <- allowRelease.complete(())
      _ <- cancellation.joinWithNever.timeout(3.seconds)
      _ <- second.joinWithNever.timeout(3.seconds)
      count <- releases.get
    } yield expect(secondBeforeRelease.isEmpty) && expect(count === 1)
  }
}
