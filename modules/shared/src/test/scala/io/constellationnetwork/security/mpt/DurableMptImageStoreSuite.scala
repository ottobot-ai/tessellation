package io.constellationnetwork.security.mpt

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path}

import cats.effect.kernel.Resource
import cats.effect.testkit.TestControl
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import scala.jdk.CollectionConverters._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import scodec.bits.ByteVector
import weaver.MutableIOSuite

object DurableMptImageStoreSuite extends MutableIOSuite {
  import DurableMptImageError._
  import MptImageArtifact._
  import MptImageFaultPoint._

  type Res = (JsonSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.map(json => (json, Hasher.forJson[IO](implicitly, json)))

  private val ordinal0 = SnapshotOrdinal.unsafeApply(40L)
  private val ordinal1 = SnapshotOrdinal.unsafeApply(41L)

  private def key(label: String): Hex = Hex(Hash.fromBytes(label.getBytes(StandardCharsets.UTF_8)).value)

  private def entries(label: String): Map[Hex, ByteVector] =
    Map(
      key(s"$label-z") -> ByteVector.view(s"$label-value-z".getBytes(StandardCharsets.UTF_8)),
      key(s"$label-a") -> ByteVector.view(s"$label-value-a".getBytes(StandardCharsets.UTF_8)),
      key(s"$label-m") -> ByteVector.view(s"$label-value-m".getBytes(StandardCharsets.UTF_8))
    )

  private def rootOf(values: Map[Hex, ByteVector])(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[MptRoot] =
    MerklePatriciaTrie.makeParallelFromBytes[IO](values.iterator.map { case (k, v) => k -> v.toArray }.toMap).map(_.rootHash)

  private def prepare(
    store: DurableMptImageStore[IO],
    generation: Long,
    ordinal: SnapshotOrdinal,
    values: Map[Hex, ByteVector]
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[MptImageReceipt] =
    rootOf(values).flatMap(store.prepare(generation, ordinal, _, values))

  private def deleteRecursive(path: Path): Unit =
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try stream.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally stream.close()
    }

  private def tempDir: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("durable-mpt-image")))(path => IO.blocking(deleteRecursive(path)))

  private final class FailOnce(point: MptImageFaultPoint, fired: Ref[IO, Boolean], failure: Throwable)
      extends MptImageFaultInjector[IO] {
    def before(actual: MptImageFaultPoint): IO[Unit] =
      if (actual != point) IO.unit
      else
        fired
          .modify {
            case false => true -> IO.raiseError[Unit](failure)
            case true  => true -> IO.unit
          }
          .flatten
  }

  private final class BarrierOnce(
    point: MptImageFaultPoint,
    fired: Ref[IO, Boolean],
    entered: Deferred[IO, Unit],
    release: Deferred[IO, Unit],
    failure: Option[Throwable]
  ) extends MptImageFaultInjector[IO] {
    def before(actual: MptImageFaultPoint): IO[Unit] =
      if (actual != point) IO.unit
      else
        fired
          .modify {
            case false =>
              true -> (entered.complete(()).void >> release.get >> failure.fold(IO.unit)(IO.raiseError[Unit]))
            case true => true -> IO.unit
          }
          .flatten
  }

  private def storeWithFault(
    directory: Path,
    point: MptImageFaultPoint
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[DurableMptImageStore[IO]] =
    for {
      fired <- Ref.of[IO, Boolean](false)
      store <- DurableMptImageStore.makeWith[IO](
        directory,
        DurableMptImageFileOps.nio[IO],
        new FailOnce(point, fired, new IOException(s"injected $point")),
        MptImageRootBuilder.consensus[IO]
      )
    } yield store

  test("prepare is deterministic, copies aliased input, and never changes the active manifest") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val a = Array[Byte](1, 2, 3)
      val b = Array[Byte](4, 5, 6)
      val firstKey = key("alias-a")
      val secondKey = key("alias-b")
      val aliased = Map(firstKey -> ByteVector.view(a), secondKey -> ByteVector.view(b))
      val independent = aliased.iterator.map { case (k, v) => k -> ByteVector.view(v.toArray.clone()) }.toMap

      for {
        store <- DurableMptImageStore.make[IO](directory)
        expectedRoot <- rootOf(independent)
        first <- store.prepare(0L, ordinal0, expectedRoot, aliased)
        second <- store.prepare(0L, ordinal0, expectedRoot, independent.toList.reverse.toMap)
        beforePublish <- store.activeReceipt
        _ <- IO { a(0) = 99; b(2) = 88 }
        _ <- store.publish(first, None)
        loaded <- store.read(first)
        callerCopy = loaded(firstKey).toArray
        _ <- IO { callerCopy(0) = 77 }
        loadedAgain <- store.read(first)
      } yield
        expect.all(
          first == second,
          beforePublish.isEmpty,
          loaded.toMap == independent,
          loadedAgain.toMap == independent
        )
    }
  }

  test("published images are idempotent and boot through a fresh store instance") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val values = entries("restart")
      for {
        store <- DurableMptImageStore.make[IO](directory)
        receipt <- prepare(store, 0L, ordinal0, values)
        _ <- store.publish(receipt, None)
        _ <- store.publish(receipt, Some(MptImagePointer(MptImageId(Hash.empty), MptImageDigest(Hash.empty))))
        restarted <- DurableMptImageStore.make[IO](directory)
        active <- restarted.activeReceipt
        loaded <- restarted.read(receipt)
      } yield expect.all(active.contains(receipt), loaded.toMap == values)
    }
  }

  test("store creation requires a pre-existing root directory and does not create it implicitly") { res =>
    implicit val (json, hasher) = res

    tempDir.use { parent =>
      val missing = parent.resolve("missing-root")
      for {
        result <- DurableMptImageStore.make[IO](missing).attempt
        exists <- IO.blocking(Files.exists(missing))
      } yield expect.all(result.left.exists(_.isInstanceOf[RootDirectoryMustExist]), !exists)
    }
  }

  test("prepare rejects a wrong root and propagates an independent builder failure without publication") { res =>
    implicit val (json, hasher) = res

    tempDir.use { wrongRootDirectory =>
      tempDir.use { buildFailureDirectory =>
        val values = entries("root-failure")
        val buildFailure = new IOException("independent rebuild failed")
        val failingBuilder = new MptImageRootBuilder[IO] {
          def rebuild(entries: Vector[(Hex, ByteVector)]): IO[MptRoot] = IO.raiseError(buildFailure)
        }

        for {
          store <- DurableMptImageStore.make[IO](wrongRootDirectory)
          wrong <- store.prepare(0L, ordinal0, MptRoot(Hash.empty), values).attempt
          wrongActive <- store.activeReceipt
          failingStore <- DurableMptImageStore.makeWith[IO](
            buildFailureDirectory,
            DurableMptImageFileOps.nio[IO],
            MptImageFaultInjector.noop[IO],
            failingBuilder
          )
          failed <- failingStore.prepare(0L, ordinal0, MptRoot(Hash.empty), values).attempt
          failedActive <- failingStore.activeReceipt
        } yield
          expect.all(
            wrong.left.exists(_.isInstanceOf[RootMismatch]),
            wrongActive.isEmpty,
            failed == Left(buildFailure),
            failedActive.isEmpty
          )
      }
    }
  }

  test("publish enforces pointer CAS, consecutive generations, and typed generation conflicts") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        store <- DurableMptImageStore.make[IO](directory)
        invalidInitial <- prepare(store, 4L, SnapshotOrdinal.unsafeApply(44L), entries("cas-invalid-initial"))
        initialGenerationFailure <- store.publish(invalidInitial, None).attempt
        initial <- prepare(store, 0L, ordinal0, entries("cas-initial"))
        _ <- store.publish(initial, None)
        next <- prepare(store, 1L, ordinal1, entries("cas-next"))
        wrongCas <- store.publish(next, None).attempt
        stillInitial <- store.activeReceipt
        _ <- store.publish(next, Some(initial.pointer))
        conflicting <- prepare(store, 1L, ordinal1, entries("cas-conflict"))
        conflict <- store.publish(conflicting, Some(next.pointer)).attempt
        skipped <- prepare(store, 3L, SnapshotOrdinal.unsafeApply(43L), entries("cas-skipped"))
        gap <- store.publish(skipped, Some(next.pointer)).attempt
      } yield
        expect.all(
          initialGenerationFailure.left.exists(_.isInstanceOf[InitialGenerationMustBeZero]),
          wrongCas.left.exists(_.isInstanceOf[CompareAndSetConflict]),
          stillInitial.contains(initial),
          conflict.left.exists(_.isInstanceOf[GenerationConflict]),
          gap.left.exists(_.isInstanceOf[NonConsecutiveGeneration])
        )
    }
  }

  test("the publish mutex permits exactly one of two conflicting same-generation publications") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        store <- DurableMptImageStore.make[IO](directory)
        initial <- prepare(store, 0L, ordinal0, entries("race-initial"))
        _ <- store.publish(initial, None)
        left <- prepare(store, 1L, ordinal1, entries("race-left"))
        right <- prepare(store, 1L, ordinal1, entries("race-right"))
        results <- List(left, right).parTraverse(receipt => store.publish(receipt, Some(initial.pointer)).attempt)
        active <- store.activeReceipt
      } yield
        expect.all(
          results.count(_.isRight) == 1,
          results.count(_.left.exists(_.isInstanceOf[GenerationConflict])) == 1,
          active.exists(receipt => receipt == left || receipt == right)
        )
    }
  }

  test("every image write stage fails closed without changing the prior active generation") { res =>
    implicit val (json, hasher) = res

    val points = List(
      Write(Image),
      ForceFile(Image),
      AtomicMove(Image),
      ForceDirectory(Image),
      ReadBack(Image)
    )

    points.traverse { point =>
      tempDir.use { directory =>
        val initialValues = entries(s"image-prior-$point")
        val candidateValues = entries(s"image-candidate-$point")
        for {
          healthy <- DurableMptImageStore.make[IO](directory)
          initial <- prepare(healthy, 0L, ordinal0, initialValues)
          _ <- healthy.publish(initial, None)
          candidateRoot <- rootOf(candidateValues)
          failing <- storeWithFault(directory, point)
          result <- failing.prepare(1L, ordinal1, candidateRoot, candidateValues).attempt
          restarted <- DurableMptImageStore.make[IO](directory)
          active <- restarted.activeReceipt
          loaded <- restarted.read(initial)
        } yield result.isLeft && active.contains(initial) && loaded.toMap == initialValues
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("every manifest write stage restores the prior bootable generation on failure") { res =>
    implicit val (json, hasher) = res

    val points = List(
      Write(Manifest),
      ForceFile(Manifest),
      AtomicMove(Manifest),
      ForceDirectory(Manifest),
      ReadBack(Manifest)
    )

    points.traverse { point =>
      tempDir.use { directory =>
        val initialValues = entries(s"manifest-prior-$point")
        for {
          healthy <- DurableMptImageStore.make[IO](directory)
          initial <- prepare(healthy, 0L, ordinal0, initialValues)
          _ <- healthy.publish(initial, None)
          candidate <- prepare(healthy, 1L, ordinal1, entries(s"manifest-candidate-$point"))
          failing <- storeWithFault(directory, point)
          result <- failing.publish(candidate, Some(initial.pointer)).attempt
          restarted <- DurableMptImageStore.make[IO](directory)
          active <- restarted.activeReceipt
          loaded <- restarted.read(initial)
        } yield result.isLeft && active.contains(initial) && loaded.toMap == initialValues
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("a reader cannot observe a renamed candidate while verification and recovery are pending") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory => TestControl.executeEmbed {
      val injectedFailure = new IOException("post-rename read-back failed")
      val initialValues = entries("reader-barrier-initial")
      for {
        healthy <- DurableMptImageStore.make[IO](directory)
        initial <- prepare(healthy, 0L, ordinal0, initialValues)
        _ <- healthy.publish(initial, None)
        candidate <- prepare(healthy, 1L, ordinal1, entries("reader-barrier-candidate"))
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        blocked <- DurableMptImageStore.makeWith[IO](
          directory,
          DurableMptImageFileOps.nio[IO],
          new BarrierOnce(ReadBack(Manifest), fired, entered, release, Some(injectedFailure)),
          MptImageRootBuilder.consensus[IO]
        )
        publisher <- blocked.publish(candidate, Some(initial.pointer)).attempt.start
        _ <- entered.get
        readerStarted <- Deferred[IO, Unit]
        readerDone <- Deferred[IO, Unit]
        reader <- (readerStarted.complete(()).void >> blocked.activeReceipt)
          .guarantee(readerDone.complete(()).void)
          .start
        _ <- readerStarted.get
        dataReaderStarted <- Deferred[IO, Unit]
        dataReaderDone <- Deferred[IO, Unit]
        dataReader <- (dataReaderStarted.complete(()).void >> blocked.read(initial))
          .guarantee(dataReaderDone.complete(()).void)
          .start
        _ <- dataReaderStarted.get
        _ <- IO.cede
        readerBeforeRecovery <- readerDone.tryGet
        dataReaderBeforeRecovery <- dataReaderDone.tryGet
        _ <- release.complete(())
        publishResult <- publisher.joinWithNever
        readResult <- reader.joinWithNever
        dataReadResult <- dataReader.joinWithNever
      } yield
        expect.all(
          readerBeforeRecovery.isEmpty,
          dataReaderBeforeRecovery.isEmpty,
          publishResult == Left(injectedFailure),
          readResult.contains(initial),
          dataReadResult.toMap == initialValues
        )
    }}
  }

  test("cancellation after manifest rename cannot interrupt force and read-back publication") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory => TestControl.executeEmbed {
      for {
        healthy <- DurableMptImageStore.make[IO](directory)
        initial <- prepare(healthy, 0L, ordinal0, entries("cancel-initial"))
        _ <- healthy.publish(initial, None)
        candidate <- prepare(healthy, 1L, ordinal1, entries("cancel-candidate"))
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        blocked <- DurableMptImageStore.makeWith[IO](
          directory,
          DurableMptImageFileOps.nio[IO],
          new BarrierOnce(ForceDirectory(Manifest), fired, entered, release, None),
          MptImageRootBuilder.consensus[IO]
        )
        publisher <- blocked.publish(candidate, Some(initial.pointer)).start
        _ <- entered.get
        cancelStarted <- Deferred[IO, Unit]
        cancelDone <- Deferred[IO, Unit]
        canceler <- (cancelStarted.complete(()).void >> publisher.cancel)
          .guarantee(cancelDone.complete(()).void)
          .start
        _ <- cancelStarted.get
        _ <- IO.cede
        cancelBeforeRelease <- cancelDone.tryGet
        _ <- release.complete(())
        _ <- canceler.joinWithNever
        _ <- publisher.join
        active <- blocked.activeReceipt
        retry <- blocked.publish(candidate, Some(initial.pointer)).attempt
      } yield
        expect.all(
          cancelBeforeRelease.isEmpty,
          active.contains(candidate),
          retry == Right(())
        )
    }}
  }

  test("publish verifies the prior image before attempting any manifest mutation") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        healthy <- DurableMptImageStore.make[IO](directory)
        initial <- prepare(healthy, 0L, ordinal0, entries("prior-verification-initial"))
        _ <- healthy.publish(initial, None)
        candidate <- prepare(healthy, 1L, ordinal1, entries("prior-verification-candidate"))
        manifestPath = DurableMptImageLayout.activeManifest(directory)
        manifestBefore <- IO.blocking(Files.readAllBytes(manifestPath))
        priorImagePath = DurableMptImageLayout.image(directory, initial.imageId)
        _ <- IO.blocking(Files.write(priorImagePath, Array[Byte](1, 2, 3))).void
        failing <- storeWithFault(directory, Write(Manifest))
        result <- failing.publish(candidate, Some(initial.pointer)).attempt
        manifestAfter <- IO.blocking(Files.readAllBytes(manifestPath))
      } yield
        expect.all(
          result.left.exists(_.isInstanceOf[CorruptImage]),
          java.util.Arrays.equals(manifestBefore, manifestAfter)
        )
    }
  }

  test("atomic-move-unavailable has no fallback and leaves the active manifest unchanged") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val delegate = DurableMptImageFileOps.nio[IO]
      val unsupported = new DelegatingFileOps(delegate) {
        override def atomicMoveReplace(source: Path, target: Path): IO[Unit] =
          IO.raiseError(new AtomicMoveNotSupportedException(source.toString, target.toString, "injected"))
      }

      for {
        store <- DurableMptImageStore.makeWith[IO](
          directory,
          unsupported,
          MptImageFaultInjector.noop[IO],
          MptImageRootBuilder.consensus[IO]
        )
        values = entries("atomic-required")
        root <- rootOf(values)
        result <- store.prepare(0L, ordinal0, root, values).attempt
        active <- store.activeReceipt
      } yield expect.all(result.left.exists(_.isInstanceOf[AtomicMoveRequired]), active.isEmpty)
    }
  }

  test("same-length corruption and truncation of either image or manifest are rejected after restart") { res =>
    implicit val (json, hasher) = res

    tempDir.use { imageDirectory =>
      tempDir.use { manifestDirectory =>
        for {
          imageStore <- DurableMptImageStore.make[IO](imageDirectory)
          imageReceipt <- prepare(imageStore, 0L, ordinal0, entries("corrupt-image"))
          _ <- imageStore.publish(imageReceipt, None)
          imagePath = DurableMptImageLayout.image(imageDirectory, imageReceipt.imageId)
          originalImage <- IO.blocking(Files.readAllBytes(imagePath))
          corruptImageBytes = originalImage.clone()
          _ = corruptImageBytes(corruptImageBytes.length - 1) = (corruptImageBytes.last ^ 1).toByte
          _ <- IO.blocking(Files.write(imagePath, corruptImageBytes)).void
          corruptImageRestart <- DurableMptImageStore.make[IO](imageDirectory)
          corruptImage <- corruptImageRestart.activeReceipt.attempt
          _ <- IO.blocking(Files.write(imagePath, originalImage.take(3))).void
          truncatedImageRestart <- DurableMptImageStore.make[IO](imageDirectory)
          truncatedImage <- truncatedImageRestart.activeReceipt.attempt
          manifestStore <- DurableMptImageStore.make[IO](manifestDirectory)
          manifestReceipt <- prepare(manifestStore, 0L, ordinal0, entries("corrupt-manifest"))
          _ <- manifestStore.publish(manifestReceipt, None)
          manifestPath = DurableMptImageLayout.activeManifest(manifestDirectory)
          originalManifest <- IO.blocking(Files.readAllBytes(manifestPath))
          corruptManifestBytes = originalManifest.clone()
          _ = corruptManifestBytes(12) = (corruptManifestBytes(12) ^ 1).toByte
          _ <- IO.blocking(Files.write(manifestPath, corruptManifestBytes)).void
          corruptManifestRestart <- DurableMptImageStore.make[IO](manifestDirectory)
          corruptManifest <- corruptManifestRestart.activeReceipt.attempt
          _ <- IO.blocking(Files.write(manifestPath, originalManifest.take(3))).void
          truncatedManifestRestart <- DurableMptImageStore.make[IO](manifestDirectory)
          truncatedManifest <- truncatedManifestRestart.activeReceipt.attempt
        } yield
          expect.all(
            corruptImage.left.exists(_.isInstanceOf[CorruptImage]),
            truncatedImage.left.exists(_.isInstanceOf[CorruptImage]),
            corruptManifest.left.exists(_.isInstanceOf[CorruptManifest]),
            truncatedManifest.left.exists(_.isInstanceOf[CorruptManifest])
          )
      }
    }
  }

  private class DelegatingFileOps(delegate: DurableMptImageFileOps[IO]) extends DurableMptImageFileOps[IO] {
    def createDirectories(path: Path): IO[Unit] = delegate.createDirectories(path)
    def createTempFile(directory: Path, prefix: String, suffix: String): IO[Path] = delegate.createTempFile(directory, prefix, suffix)
    def write(path: Path, bytes: Array[Byte]): IO[Unit] = delegate.write(path, bytes)
    def forceFile(path: Path): IO[Unit] = delegate.forceFile(path)
    def atomicMoveReplace(source: Path, target: Path): IO[Unit] = delegate.atomicMoveReplace(source, target)
    def forceDirectory(path: Path): IO[Unit] = delegate.forceDirectory(path)
    def readAllBytes(path: Path): IO[Array[Byte]] = delegate.readAllBytes(path)
    def exists(path: Path): IO[Boolean] = delegate.exists(path)
    def isDirectory(path: Path): IO[Boolean] = delegate.isDirectory(path)
    def deleteIfExists(path: Path): IO[Unit] = delegate.deleteIfExists(path)
  }
}
