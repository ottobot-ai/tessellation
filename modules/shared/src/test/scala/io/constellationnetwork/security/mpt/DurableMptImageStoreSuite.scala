package io.constellationnetwork.security.mpt

import java.io.{IOException, InputStream}
import java.nio.ByteBuffer
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
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.storage.durable.{
  DurableFileOps,
  DurableWriteBoundary,
  DurableWriteEvent,
  DurableWriteHook,
  DurableWriteStage
}

import scodec.bits.ByteVector
import weaver.MutableIOSuite

object DurableMptImageStoreSuite extends MutableIOSuite {
  import DurableMptImageError._
  import MptImageArtifact._
  import DurableWriteBoundary._
  import DurableWriteStage._

  type Res = (JsonSerializer[IO], Hasher[IO])

  override def sharedResource: Resource[IO, Res] =
    JsonSerializer.forAsync[IO].asResource.map(json => (json, Hasher.forJson[IO](implicitly, json)))

  private val ordinal0 = SnapshotOrdinal.unsafeApply(40L)
  private val ordinal1 = SnapshotOrdinal.unsafeApply(41L)
  private val codecEra = MptImageCodecEra(Hash.fromBytes("scodec-v1".getBytes(StandardCharsets.UTF_8)))
  private val rootEra = MptImageRootEra(Hash.fromBytes("test-mpt-root-v1".getBytes(StandardCharsets.UTF_8)))
  private val limits = MptImageReadLimits
    .from(maxImageBytes = 1024L * 1024L, maxEntries = 1000, maxKeyBytes = 1024, maxValueBytes = 256 * 1024)
    .fold(throw _, identity)

  private def key(label: String): Hex = Hex(Hash.fromBytes(label.getBytes(StandardCharsets.UTF_8)).value)

  private def entries(label: String): Map[Hex, ByteVector] =
    Map(
      key(s"$label-z") -> ByteVector.view(s"$label-value-z".getBytes(StandardCharsets.UTF_8)),
      key(s"$label-a") -> ByteVector.view(s"$label-value-a".getBytes(StandardCharsets.UTF_8)),
      key(s"$label-m") -> ByteVector.view(s"$label-value-m".getBytes(StandardCharsets.UTF_8))
    )

  private def rootOf(values: Map[Hex, ByteVector])(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[MptRoot] =
    MerklePatriciaTrie.makeParallelFromBytes[IO](values.iterator.map { case (k, v) => k -> v.toArray }.toMap).map(_.rootHash)

  private def snapshotHash(ordinal: SnapshotOrdinal): Hash =
    Hash.fromBytes(s"snapshot-${ordinal.value.value}".getBytes(StandardCharsets.UTF_8))

  private def parentHash(ordinal: SnapshotOrdinal): Hash =
    Hash.fromBytes(s"parent-${ordinal.value.value}".getBytes(StandardCharsets.UTF_8))

  private def rootVerifier(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): MptImageRootVerifier[IO] =
    MptImageRootVerifier.consensus[IO](rootEra)

  private def makeStore(directory: Path)(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): Resource[IO, DurableMptImageStore[IO]] =
    DurableMptImageStore.resource[IO](directory, codecEra, rootVerifier, limits)

  private def makeStoreWithLimits(
    directory: Path,
    readLimits: MptImageReadLimits
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): Resource[IO, DurableMptImageStore[IO]] =
    DurableMptImageStore.resource[IO](directory, codecEra, rootVerifier, readLimits)

  private def readLimits(maxImageBytes: Long, maxEntries: Int, maxKeyBytes: Int, maxValueBytes: Int): MptImageReadLimits =
    MptImageReadLimits.from(maxImageBytes, maxEntries, maxKeyBytes, maxValueBytes).fold(throw _, identity)

  private def prepare(
    store: DurableMptImageStore[IO],
    generation: Long,
    ordinal: SnapshotOrdinal,
    values: Map[Hex, ByteVector]
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[MptImageReceipt] =
    rootOf(values).flatMap { root =>
      store.prepare(generation, GlobalSnapshotStateRef(ordinal, snapshotHash(ordinal), parentHash(ordinal), root), values)
    }

  private def deleteRecursive(path: Path): Unit =
    if (Files.exists(path)) {
      val stream = Files.walk(path)
      try stream.iterator().asScala.toList.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally stream.close()
    }

  private def tempDir: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("durable-mpt-image")))(path => IO.blocking(deleteRecursive(path)))

  private def temporaryArtifacts(directory: Path): IO[List[Path]] =
    IO.blocking {
      val stream = Files.walk(directory)
      try stream.iterator().asScala.filter(_.getFileName.toString.endsWith(".tmp")).toList
      finally stream.close()
    }

  private type FaultPoint = DurableWriteEvent[MptImageArtifact]

  private def before(stage: DurableWriteStage, artifact: MptImageArtifact): FaultPoint =
    DurableWriteEvent(artifact, stage, Before)

  private def after(stage: DurableWriteStage, artifact: MptImageArtifact): FaultPoint =
    DurableWriteEvent(artifact, stage, After)

  private final class FailOnce(point: FaultPoint, fired: Ref[IO, Boolean], failure: Throwable)
      extends DurableWriteHook[IO, MptImageArtifact] {
    def onEvent(actual: FaultPoint): IO[Unit] =
      if (actual != point) IO.unit
      else
        fired.modify {
          case false => true -> IO.raiseError[Unit](failure)
          case true  => true -> IO.unit
        }.flatten
  }

  private final class BarrierOnce(
    point: FaultPoint,
    fired: Ref[IO, Boolean],
    entered: Deferred[IO, Unit],
    release: Deferred[IO, Unit],
    failure: Option[Throwable]
  ) extends DurableWriteHook[IO, MptImageArtifact] {
    def onEvent(actual: FaultPoint): IO[Unit] =
      if (actual != point) IO.unit
      else
        fired.modify {
          case false =>
            true -> (entered.complete(()).void >> release.get >> failure.fold(IO.unit)(IO.raiseError[Unit]))
          case true => true -> IO.unit
        }.flatten
  }

  private final class BlockingRootVerifier(
    delegate: MptImageRootVerifier[IO],
    entered: Deferred[IO, Unit],
    release: Deferred[IO, Unit]
  ) extends MptImageRootVerifier[IO] {
    val rootEra: MptImageRootEra = delegate.rootEra

    def rebuild(entries: Vector[(Hex, ByteVector)]): IO[MptRoot] =
      entered.complete(()).void >> release.get >> delegate.rebuild(entries)
  }

  private def storeWithFault(
    directory: Path,
    point: FaultPoint
  )(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): Resource[IO, DurableMptImageStore[IO]] =
    Resource.eval(Ref.of[IO, Boolean](false)).flatMap { fired =>
      DurableMptImageStore.resourceWith[IO](
        directory,
        DurableFileOps.nio[IO],
        new FailOnce(point, fired, new IOException(s"injected $point")),
        codecEra,
        rootVerifier,
        limits
      )
    }

  test("prepare is deterministic, copies aliased input, and never changes the active manifest") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val a = Array[Byte](1, 2, 3)
      val b = Array[Byte](4, 5, 6)
      val firstKey = key("alias-a")
      val secondKey = key("alias-b")
      val aliased = Map(firstKey -> ByteVector.view(a), secondKey -> ByteVector.view(b))
      val independent = aliased.iterator.map { case (k, v) => k -> ByteVector.view(v.toArray.clone()) }.toMap

      makeStore(directory).use { store =>
        for {
          expectedRoot <- rootOf(independent)
          anchor = GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), expectedRoot)
          first <- store.prepare(0L, anchor, aliased)
          second <- store.prepare(0L, anchor, independent.toList.reverse.toMap)
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
  }

  test("prepare captures aliased bytes before the independent root verifier runs") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val aliasedBytes = Array[Byte](10, 20, 30)
      val entryKey = key("capture-before-verify")
      val aliased = Map(entryKey -> ByteVector.view(aliasedBytes))
      val expected = Map(entryKey -> ByteVector.view(aliasedBytes.clone()))

      val testResource = for {
        expectedRoot <- rootOf(expected)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        verifier = new BlockingRootVerifier(rootVerifier, entered, release)
        result <- DurableMptImageStore.resource[IO](directory, codecEra, verifier, limits).use { store =>
          val anchor = GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), expectedRoot)
          for {
            preparing <- store.prepare(0L, anchor, aliased).start
            _ <- entered.get
            _ <- IO { aliasedBytes(0) = 99 }
            _ <- release.complete(())
            receipt <- preparing.joinWithNever
            _ <- store.publish(receipt, None)
            loaded <- store.read(receipt)
          } yield expect(loaded.toMap == expected)
        }
      } yield result

      testResource
    }
  }

  test("image identity binds snapshot, parent, ordinal, codec era, and root era") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val values = entries("anchor-binding")
      val alternateHash = Hash.fromBytes("alternate-anchor".getBytes(StandardCharsets.UTF_8))

      makeStore(directory).use { store =>
        for {
          root <- rootOf(values)
          anchor = GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), root)
          receipt <- store.prepare(0L, anchor, values)
          sibling <- store.prepare(0L, anchor.copy(hash = alternateHash), values)
          childOfOtherParent <- store.prepare(0L, anchor.copy(parentHash = alternateHash), values)
          sameHashesOtherOrdinal <- store.prepare(0L, anchor.copy(ordinal = ordinal1), values)
          reservedSnapshot <- store.publish(receipt.copy(anchor = anchor.copy(hash = Hash.empty)), None).attempt
          wrongSnapshot <- store.publish(receipt.copy(anchor = anchor.copy(hash = alternateHash)), None).attempt
          wrongParent <- store.publish(receipt.copy(anchor = anchor.copy(parentHash = alternateHash)), None).attempt
          wrongOrdinal <- store.publish(receipt.copy(anchor = anchor.copy(ordinal = ordinal1)), None).attempt
          wrongCodec <- store
            .publish(receipt.copy(codecEra = MptImageCodecEra(alternateHash)), None)
            .attempt
          wrongRootEra <- store
            .publish(receipt.copy(rootEra = MptImageRootEra(alternateHash)), None)
            .attempt
          active <- store.activeReceipt
        } yield
          expect.all(
            receipt.anchor == anchor,
            receipt.codecEra == codecEra,
            receipt.rootEra == rootEra,
            receipt.imageId != sibling.imageId,
            receipt.digest != sibling.digest,
            receipt.imageId != childOfOtherParent.imageId,
            receipt.digest != childOfOtherParent.digest,
            receipt.imageId != sameHashesOtherOrdinal.imageId,
            receipt.digest != sameHashesOtherOrdinal.digest,
            reservedSnapshot.left.exists(_.isInstanceOf[CorruptManifest]),
            wrongSnapshot.left.exists(_.isInstanceOf[CorruptImage]),
            wrongParent.left.exists(_.isInstanceOf[CorruptImage]),
            wrongOrdinal.left.exists(_.isInstanceOf[CorruptImage]),
            wrongCodec.left.exists(_.isInstanceOf[CorruptImage]),
            wrongRootEra.left.exists(_.isInstanceOf[CorruptImage]),
            active.isEmpty
          )
      }
    }
  }

  test("restart rejects a manifest under a different configured codec or root verifier era") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val otherCodecEra = MptImageCodecEra(Hash.fromBytes("other-codec-era".getBytes(StandardCharsets.UTF_8)))
      val otherRootEra = MptImageRootEra(Hash.fromBytes("other-root-era".getBytes(StandardCharsets.UTF_8)))

      for {
        _ <- makeStore(directory).use { store =>
          prepare(store, 0L, ordinal0, entries("configured-era")).flatMap(store.publish(_, None))
        }
        wrongCodec <- DurableMptImageStore.resource[IO](directory, otherCodecEra, rootVerifier, limits).use(_.activeReceipt).attempt
        wrongRoot <- DurableMptImageStore
          .resource[IO](directory, codecEra, MptImageRootVerifier.consensus[IO](otherRootEra), limits)
          .use(_.activeReceipt)
          .attempt
      } yield
        expect.all(
          wrongCodec.left.exists(_.isInstanceOf[CorruptImage]),
          wrongRoot.left.exists(_.isInstanceOf[CorruptImage])
        )
    }
  }

  test("published images are idempotent and boot through a fresh store instance") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val values = entries("restart")
      for {
        receipt <- makeStore(directory).use { store =>
          prepare(store, 0L, ordinal0, values).flatTap { prepared =>
            store.publish(prepared, None) >>
              store.publish(prepared, Some(MptImagePointer(MptImageId(Hash.empty), MptImageDigest(Hash.empty))))
          }
        }
        result <- makeStore(directory).use { restarted =>
          (restarted.activeReceipt, restarted.read(receipt)).mapN { (active, loaded) =>
            expect.all(active.contains(receipt), loaded.toMap == values)
          }
        }
      } yield result
    }
  }

  test("a published store never treats a missing active manifest as pristine") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        initial <- makeStore(directory).use { store =>
          prepare(store, 0L, ordinal0, entries("initialized-history")).flatTap(store.publish(_, None))
        }
        markerExists <- IO.blocking(Files.isRegularFile(DurableMptImageLayout.initializationMarker(directory)))
        _ <- IO.blocking(Files.delete(DurableMptImageLayout.activeManifest(directory)))
        results <- makeStore(directory).use { store =>
          for {
            active <- store.activeReceipt.attempt
            replacement <- prepare(store, 0L, ordinal0, entries("forged-new-history"))
            reset <- store.publish(replacement, None).attempt
          } yield active -> reset
        }
        (active, reset) = results
      } yield {
        def isMissingActiveManifest(result: Either[Throwable, _]): Boolean =
          result.left.exists {
            case MissingArtifact(MptImageArtifactRef.ActiveManifest, path, _) =>
              path == DurableMptImageLayout.activeManifest(directory)
            case _ => false
          }

        expect.all(markerExists, isMissingActiveManifest(active), isMissingActiveManifest(reset), initial.generation == 0L)
      }
    }
  }

  test("store creation requires a pre-existing root directory and does not create it implicitly") { res =>
    implicit val (json, hasher) = res

    tempDir.use { parent =>
      val missing = parent.resolve("missing-root")
      for {
        result <- makeStore(missing).use(_ => IO.unit).attempt
        exists <- IO.blocking(Files.exists(missing))
      } yield expect.all(result.left.exists(_.isInstanceOf[RootDirectoryMustExist]), !exists)
    }
  }

  test("the store owns its directory for the Resource lifetime and leaves a reusable lock file") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        contended <- makeStore(directory).use { _ =>
          makeStore(directory).use(_ => IO.unit).attempt
        }
        lockPath = DurableMptImageLayout.ownerLock(directory)
        lockExistsAfterRelease <- IO.blocking(Files.isRegularFile(lockPath))
        reacquired <- makeStore(directory).use(_ => IO.unit).attempt
        lockStillExists <- IO.blocking(Files.isRegularFile(lockPath))
      } yield
        expect.all(
          contended.left.exists(_.isInstanceOf[DirectoryAlreadyOwned]),
          lockExistsAfterRelease,
          reacquired == Right(()),
          lockStillExists
        )
    }
  }

  test("constructor failure and cancellation release directory ownership") { res =>
    implicit val (json, hasher) = res

    tempDir.use { failureDirectory =>
      tempDir.use { cancellationDirectory =>
        val delegate = DurableFileOps.nio[IO]
        val injected = new IOException("injected constructor failure")
        val failingOps = new DelegatingFileOps(delegate) {
          override def createDirectories(path: Path): IO[Unit] = IO.raiseError(injected)
        }

        for {
          failed <- DurableMptImageStore
            .resourceWith[IO](
              failureDirectory,
              failingOps,
              DurableWriteHook.noop[IO, MptImageArtifact],
              codecEra,
              rootVerifier,
              limits
            )
            .use(_ => IO.unit)
            .attempt
          reopenedAfterFailure <- makeStore(failureDirectory).use(_ => IO.unit).attempt
          entered <- Deferred[IO, Unit]
          blockingOps = new DelegatingFileOps(delegate) {
            override def createDirectories(path: Path): IO[Unit] = entered.complete(()).void >> IO.never
          }
          allocating <- DurableMptImageStore
            .resourceWith[IO](
              cancellationDirectory,
              blockingOps,
              DurableWriteHook.noop[IO, MptImageArtifact],
              codecEra,
              rootVerifier,
              limits
            )
            .use(_ => IO.unit)
            .start
          _ <- entered.get
          _ <- allocating.cancel
          reopenedAfterCancellation <- makeStore(cancellationDirectory).use(_ => IO.unit).attempt
        } yield
          expect.all(
            failed == Left(injected),
            reopenedAfterFailure == Right(()),
            reopenedAfterCancellation == Right(())
          )
      }
    }
  }

  test("prepare enforces entry, key, value, and aggregate limits before publication") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val oneKey = key("bounded-entry")
      val oneEntry = Map(oneKey -> ByteVector.view(Array[Byte](1, 2, 3)))
      val exactImageBytes = MptImageEncoding.ImageHeaderBytes + 8L + oneKey.value.length.toLong + 3L
      val exactLimits = readLimits(exactImageBytes, maxEntries = 1, maxKeyBytes = oneKey.value.length, maxValueBytes = 3)
      val aggregateTooSmall = readLimits(exactImageBytes - 1L, maxEntries = 1, maxKeyBytes = oneKey.value.length, maxValueBytes = 3)
      val countLimits = readLimits(1024L * 1024L, maxEntries = 1, maxKeyBytes = 1024, maxValueBytes = 1024)
      val keyLimits = readLimits(1024L * 1024L, maxEntries = 10, maxKeyBytes = 2, maxValueBytes = 1024)
      val valueLimits = readLimits(1024L * 1024L, maxEntries = 10, maxKeyBytes = 1024, maxValueBytes = 2)

      def attemptPrepare(readLimits0: MptImageReadLimits, values: Map[Hex, ByteVector]): IO[Either[Throwable, MptImageReceipt]] =
        makeStoreWithLimits(directory, readLimits0).use { store =>
          rootOf(values).flatMap { root =>
            store.prepare(0L, GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), root), values).attempt
          }
        }

      for {
        exact <- attemptPrepare(exactLimits, oneEntry)
        aggregate <- attemptPrepare(aggregateTooSmall, oneEntry)
        count <- attemptPrepare(countLimits, entries("limit-count"))
        keyFailure <- attemptPrepare(keyLimits, oneEntry)
        valueFailure <- attemptPrepare(valueLimits, oneEntry)
        active <- makeStore(directory).use(_.activeReceipt)
      } yield
        expect.all(
          exact.isRight,
          aggregate.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          count.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          keyFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          valueFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          active.isEmpty
        )
    }
  }

  test("prepare rejects a wrong root and propagates an independent builder failure without publication") { res =>
    implicit val (json, hasher) = res

    tempDir.use { wrongRootDirectory =>
      tempDir.use { buildFailureDirectory =>
        val values = entries("root-failure")
        val buildFailure = new IOException("independent rebuild failed")
        val failingBuilder = new MptImageRootVerifier[IO] {
          val rootEra: MptImageRootEra = DurableMptImageStoreSuite.rootEra

          def rebuild(entries: Vector[(Hex, ByteVector)]): IO[MptRoot] = IO.raiseError(buildFailure)
        }

        val wrongAnchor = GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), MptRoot(Hash.empty))
        val reservedAnchor = wrongAnchor.copy(hash = Hash.empty)

        for {
          wrongResult <- makeStore(wrongRootDirectory).use { store =>
            (store.prepare(0L, wrongAnchor, values).attempt, store.prepare(0L, reservedAnchor, values).attempt, store.activeReceipt).tupled
          }
          (wrong, reserved, wrongActive) = wrongResult
          failedResult <- DurableMptImageStore
            .resourceWith[IO](
              buildFailureDirectory,
              DurableFileOps.nio[IO],
              DurableWriteHook.noop[IO, MptImageArtifact],
              codecEra,
              failingBuilder,
              limits
            )
            .use { failingStore =>
              (failingStore.prepare(0L, wrongAnchor, values).attempt, failingStore.activeReceipt).tupled
            }
          (failed, failedActive) = failedResult
        } yield
          expect.all(
            wrong.left.exists(_.isInstanceOf[RootMismatch]),
            reserved.left.exists(_.isInstanceOf[InvalidAnchor]),
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
      makeStore(directory).use { store =>
        for {
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
  }

  test("an old generation cannot overwrite or delete the newer active generation") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val nextValues = entries("stale-next")

      makeStore(directory).use { store =>
        for {
          initial <- prepare(store, 0L, ordinal0, entries("stale-initial"))
          _ <- store.publish(initial, None)
          next <- prepare(store, 1L, ordinal1, nextValues)
          _ <- store.publish(next, Some(initial.pointer))
          stale <- store.publish(initial, Some(next.pointer)).attempt
          active <- store.activeReceipt
          loaded <- store.read(next)
          initialStillExists <- IO.blocking(Files.exists(DurableMptImageLayout.image(directory, initial.imageId)))
          nextStillExists <- IO.blocking(Files.exists(DurableMptImageLayout.image(directory, next.imageId)))
        } yield
          expect.all(
            stale.left.exists(_.isInstanceOf[NonConsecutiveGeneration]),
            active.contains(next),
            loaded.toMap == nextValues,
            initialStillExists,
            nextStillExists
          )
      }
    }
  }

  test("publication-journal initialization requires the exact legacy receipt and then owns active selection") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          before <- store.activePublication
          initial <- prepare(store, 0L, ordinal0, entries("publication-init"))
          _ <- store.publish(initial, None)
          manifestPath = DurableMptImageLayout.activeManifest(directory)
          manifestBefore <- IO.blocking(Files.readAllBytes(manifestPath))
          wrongExpected <- store.initializeActivePublication(None).attempt
          manifestAfterFailure <- IO.blocking(Files.readAllBytes(manifestPath))
          initialized <- store.initializeActivePublication(initial.some)
          repeated <- store.initializeActivePublication(initial.some)
          publication <- store.activePublication
          active <- store.activeReceipt
          legacyManifestExists <- IO.blocking(Files.exists(manifestPath))
          legacyMarkerExists <- IO.blocking(Files.exists(DurableMptImageLayout.initializationMarker(directory)))
          publicationExists <- IO.blocking(Files.isRegularFile(DurableMptImageLayout.activePublication(directory)))
          publicationMarkerExists <- IO.blocking(
            Files.isRegularFile(DurableMptImageLayout.publicationInitializationMarker(directory))
          )
          next <- prepare(store, 1L, ordinal1, entries("publication-legacy-rejected"))
          legacyPublish <- store.publish(next, initial.pointer.some).attempt
        } yield
          expect.all(
            before.isEmpty,
            wrongExpected.left.exists(_.isInstanceOf[PublicationInitializationConflict]),
            java.util.Arrays.equals(manifestBefore, manifestAfterFailure),
            initialized == MptActivePublication(MptPublicationRevision(0L), initial.some),
            repeated == initialized,
            publication.contains(initialized),
            active.contains(initial),
            !legacyManifestExists,
            !legacyMarkerExists,
            publicationExists,
            publicationMarkerExists,
            legacyPublish.left.exists(_.isInstanceOf[PublicationJournalOwnsActiveState])
          )
      }.flatMap { liveResult =>
        makeStore(directory).use { restarted =>
          (restarted.activePublication, restarted.activeReceipt).mapN { (publication, active) =>
            liveResult && expect.all(publication.exists(_.revision == MptPublicationRevision(0L)), active.nonEmpty)
          }
        }
      }
    }
  }

  test("verified publication lease rejects an uninitialized journal without invoking the callback") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          invoked <- Ref.of[IO, Boolean](false)
          result <- store.withVerifiedActivePublication(_ => invoked.set(true)).attempt
          callbackInvoked <- invoked.get
        } yield
          expect.all(
            result.left.exists(_ == PublicationJournalNotInitialized),
            !callbackInvoked
          )
      }
    }
  }

  test("verified publication lease exposes exact pristine and image publication readback") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          pristine <- store.initializeActivePublication(None)
          pristineReadback <- store.withVerifiedActivePublication(_.read)
          receipt <- prepare(store, 12L, ordinal0, entries("verified-publication-image"))
          image = MptActivePublication(MptPublicationRevision(1L), receipt.some)
          _ <- store.transitionActive(pristine, image)
          imageReadback <- store.withVerifiedActivePublication(_.read)
        } yield expect.all(pristineReadback == pristine, imageReadback == image)
      }
    }
  }

  test("verified publication lease rejects effectful readback after the callback returns, fails, or is canceled") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        var synchronouslyCaptured = Option.empty[VerifiedActiveMptPublication[IO]]

        for {
          _ <- store.initializeActivePublication(None)
          returnedToken <- store.withVerifiedActivePublication(token => IO.pure(token))
          returnedResult <- returnedToken.read.attempt
          synchronousResult <- store
            .withVerifiedActivePublication[Unit] { token =>
              synchronouslyCaptured = token.some
              throw new IllegalStateException("lease callback threw synchronously")
            }
            .attempt
          synchronousToken <- synchronouslyCaptured.liftTo[IO](
            new IllegalStateException("The synchronously failed verified-publication callback did not capture its lease")
          )
          synchronousRead <- synchronousToken.read.attempt
          failedCapture <- Ref.of[IO, Option[VerifiedActiveMptPublication[IO]]](None)
          failed <- store
            .withVerifiedActivePublication { token =>
              failedCapture.set(token.some) >> IO.raiseError[Unit](new IllegalStateException("lease callback failed"))
            }
            .attempt
          failedToken <- failedCapture.get.flatMap(
            _.liftTo[IO](new IllegalStateException("The failed verified-publication callback did not capture its lease"))
          )
          failedResult <- failedToken.read.attempt
          captured <- Ref.of[IO, Option[VerifiedActiveMptPublication[IO]]](None)
          entered <- Deferred[IO, Unit]
          callback <- store
            .withVerifiedActivePublication { token =>
              captured.set(token.some) >> entered.complete(()).void >> IO.never[Unit]
            }
            .start
          _ <- entered.get
          _ <- callback.cancel
          canceledToken <- captured.get.flatMap(
            _.liftTo[IO](new IllegalStateException("The canceled verified-publication callback did not capture its lease"))
          )
          canceledResult <- canceledToken.read.attempt
        } yield
          expect.all(
            returnedResult.left.exists(_ == LeaseExpired),
            synchronousResult.left.exists(_.getMessage == "lease callback threw synchronously"),
            synchronousRead.left.exists(_ == LeaseExpired),
            failed.left.exists(_.getMessage == "lease callback failed"),
            failedResult.left.exists(_ == LeaseExpired),
            canceledResult.left.exists(_ == LeaseExpired)
          )
      }
    }
  }

  test("verified publication lease fails closed on a corrupt referenced image without invoking the callback") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          pristine <- store.initializeActivePublication(None)
          receipt <- prepare(store, 13L, ordinal0, entries("verified-publication-corrupt-image"))
          image = MptActivePublication(MptPublicationRevision(1L), receipt.some)
          _ <- store.transitionActive(pristine, image)
          imagePath = DurableMptImageLayout.image(directory, receipt.imageId)
          imageBytes <- IO.blocking(Files.readAllBytes(imagePath))
          corruptImageBytes = imageBytes.clone()
          _ = corruptImageBytes(corruptImageBytes.length - 1) = (corruptImageBytes.last ^ 1).toByte
          _ <- IO.blocking(Files.write(imagePath, corruptImageBytes)).void
          invoked <- Ref.of[IO, Boolean](false)
          result <- store.withVerifiedActivePublication(_ => invoked.set(true)).attempt
          callbackInvoked <- invoked.get
        } yield
          expect.all(
            result.left.exists(_.isInstanceOf[CorruptImage]),
            !callbackInvoked
          )
      }
    }
  }

  test("verified publication lease blocks a publication transition until its callback completes") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      TestControl.executeEmbed {
        makeStore(directory).use { store =>
          for {
            pristine <- store.initializeActivePublication(None)
            receipt <- prepare(store, 14L, ordinal0, entries("verified-publication-lease"))
            image = MptActivePublication(MptPublicationRevision(1L), receipt.some)
            leaseEntered <- Deferred[IO, Unit]
            releaseLease <- Deferred[IO, Unit]
            lease <- store
              .withVerifiedActivePublication { token =>
                leaseEntered.complete(()).void >> token.read.flatTap(_ => releaseLease.get)
              }
              .start
            _ <- leaseEntered.get
            transitionStarted <- Deferred[IO, Unit]
            transitionDone <- Deferred[IO, Unit]
            transition <- (transitionStarted.complete(()).void >> store.transitionActive(pristine, image))
              .guarantee(transitionDone.complete(()).void)
              .start
            _ <- transitionStarted.get
            _ <- IO.cede
            transitionBeforeRelease <- transitionDone.tryGet
            leasedPublication <- releaseLease.complete(()) >> lease.joinWithNever
            _ <- transition.joinWithNever
            active <- store.activePublication
          } yield
            expect.all(
              transitionBeforeRelease.isEmpty,
              leasedPublication == pristine,
              active.contains(image)
            )
        }
      }
    }
  }

  test("publication revision permits exact lower-generation rollback and durable explicit pristine restoration") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val lowValues = entries("publication-low")
      val highValues = entries("publication-high")

      makeStore(directory).use { store =>
        for {
          initial <- store.initializeActivePublication(None)
          low <- prepare(store, 2L, ordinal0, lowValues)
          high <- prepare(store, 10L, ordinal1, highValues)
          highState = MptActivePublication(MptPublicationRevision(1L), high.some)
          _ <- store.transitionActive(initial, highState)
          lowState = MptActivePublication(MptPublicationRevision(2L), low.some)
          _ <- store.transitionActive(highState, lowState)
          _ <- store.transitionActive(highState, lowState)
          loadedLow <- store.read(low)
          pristine = MptActivePublication(MptPublicationRevision(3L), None)
          _ <- store.transitionActive(lowState, pristine)
          activeAfterPristine <- store.activeReceipt
          publicationAfterPristine <- store.activePublication
        } yield
          expect.all(
            high.generation > low.generation,
            loadedLow.toMap == lowValues,
            activeAfterPristine.isEmpty,
            publicationAfterPristine.contains(pristine)
          ) -> (pristine, high, highValues)
      }.flatMap {
        case (beforeRestart, (pristine, high, highValues0)) =>
          makeStore(directory).use { restarted =>
            val restored = MptActivePublication(MptPublicationRevision(4L), high.some)
            for {
              booted <- restarted.activePublication
              _ <- restarted.transitionActive(pristine, restored)
              loaded <- restarted.read(high)
            } yield beforeRestart && expect.all(booted.contains(pristine), loaded.toMap == highValues0)
          }
      }
    }
  }

  test("publication transitions reject nonconsecutive revisions, stale exact state, and exhausted revision space") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          initial <- store.initializeActivePublication(None)
          left <- prepare(store, 7L, ordinal0, entries("publication-cas-left"))
          right <- prepare(store, 8L, ordinal1, entries("publication-cas-right"))
          gap = MptActivePublication(MptPublicationRevision(2L), left.some)
          gapFailure <- store.transitionActive(initial, gap).attempt
          selected = MptActivePublication(MptPublicationRevision(1L), left.some)
          _ <- store.transitionActive(initial, selected)
          staleTarget = MptActivePublication(MptPublicationRevision(1L), right.some)
          staleFailure <- store.transitionActive(initial, staleTarget).attempt
          exhausted = MptActivePublication(MptPublicationRevision(Long.MaxValue), left.some)
          exhaustedFailure <- store.transitionActive(exhausted, exhausted).attempt
          active <- store.activePublication
        } yield
          expect.all(
            gapFailure.left.exists(_.isInstanceOf[NonConsecutivePublicationRevision]),
            staleFailure.left.exists(_.isInstanceOf[PublicationCompareAndSetConflict]),
            exhaustedFailure.left.exists(_.isInstanceOf[PublicationRevisionExhausted]),
            active.contains(selected)
          )
      }
    }
  }

  test("the publication mutex permits exactly one conflicting transition from the same exact state") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
          initial <- store.initializeActivePublication(None)
          left <- prepare(store, 30L, ordinal0, entries("publication-race-left"))
          right <- prepare(store, 31L, ordinal1, entries("publication-race-right"))
          candidates = List(left, right).map(receipt => MptActivePublication(MptPublicationRevision(1L), receipt.some))
          results <- candidates.parTraverse(target => store.transitionActive(initial, target).attempt)
          active <- store.activePublication
        } yield
          expect.all(
            results.count(_.isRight) == 1,
            results.count(_.left.exists(_.isInstanceOf[PublicationCompareAndSetConflict])) == 1,
            active.exists(candidates.contains)
          )
      }
    }
  }

  test("publication initialization never infers state from missing legacy data or a missing initialized journal") { res =>
    implicit val (json, hasher) = res

    tempDir.use { missingLegacyDirectory =>
      tempDir.use { missingPublicationDirectory =>
        tempDir.use { disagreementDirectory =>
          for {
            legacyReceipt <- makeStore(missingLegacyDirectory).use { store =>
              prepare(store, 0L, ordinal0, entries("publication-missing-legacy")).flatTap(store.publish(_, None))
            }
            _ <- IO.blocking(Files.delete(DurableMptImageLayout.activeManifest(missingLegacyDirectory)))
            missingLegacy <- makeStore(missingLegacyDirectory)
              .use(_.initializeActivePublication(legacyReceipt.some))
              .attempt
            _ <- makeStore(missingPublicationDirectory).use(_.initializeActivePublication(None))
            _ <- IO.blocking(Files.delete(DurableMptImageLayout.activePublication(missingPublicationDirectory)))
            missingPublication <- makeStore(missingPublicationDirectory).use(_.activeReceipt).attempt
            rogue <- makeStore(disagreementDirectory).use { store =>
              for {
                _ <- store.initializeActivePublication(None)
                receipt <- prepare(store, 0L, ordinal0, entries("publication-disagreement"))
              } yield receipt
            }
            _ <- IO
              .blocking(
                Files.write(DurableMptImageLayout.activeManifest(disagreementDirectory), MptImageEncoding.encodeManifest(rogue))
              )
              .void
            _ <- IO
              .blocking(
                Files.write(
                  DurableMptImageLayout.initializationMarker(disagreementDirectory),
                  MptImageEncoding.encodeInitializationMarker
                )
              )
              .void
            disagreement <- makeStore(disagreementDirectory).use(_.activeReceipt).attempt
          } yield {
            val missingLegacyMatches = missingLegacy.left.exists {
              case MissingArtifact(MptImageArtifactRef.ActiveManifest, path, _) =>
                path == DurableMptImageLayout.activeManifest(missingLegacyDirectory)
              case _ => false
            }
            val missingPublicationMatches = missingPublication.left.exists {
              case MissingArtifact(MptImageArtifactRef.ActivePublication, path, _) =>
                path == DurableMptImageLayout.activePublication(missingPublicationDirectory)
              case _ => false
            }

            expect.all(
              missingLegacyMatches,
              missingPublicationMatches,
              disagreement.left.exists(_.isInstanceOf[LegacyPublicationDisagreement])
            )
          }
        }
      }
    }
  }

  test("publication compensation crash prefixes resume only from an exact journal or clean legacy ownership") { res =>
    implicit val (json, hasher) = res

    tempDir.use { journalOnlyDirectory =>
      tempDir.use { cleanLegacyDirectory =>
        tempDir.use { mismatchDirectory =>
          tempDir.use { corruptDirectory =>
            for {
              journalOnlyLegacy <- makeStore(journalOnlyDirectory).use { store =>
                prepare(store, 0L, ordinal0, entries("compensation-journal-only")).flatTap(store.publish(_, None))
              }
              journalOnly = MptActivePublication(MptPublicationRevision(0L), journalOnlyLegacy.some)
              _ <- IO.blocking(
                Files.write(
                  DurableMptImageLayout.activePublication(journalOnlyDirectory),
                  MptImageEncoding.encodePublication(journalOnly)
                )
              ).void
              missingMarker <- makeStore(journalOnlyDirectory).use(_.activeReceipt).attempt
              resumed <- makeStore(journalOnlyDirectory).use { store =>
                for {
                  initialized <- store.initializeActivePublication(journalOnlyLegacy.some)
                  active <- store.activeReceipt
                } yield initialized == journalOnly && active.contains(journalOnlyLegacy)
              }
              cleanLegacy <- makeStore(cleanLegacyDirectory).use { store =>
                prepare(store, 0L, ordinal0, entries("compensation-clean-legacy")).flatTap(store.publish(_, None))
              }
              cleanInitialized <- makeStore(cleanLegacyDirectory).use(_.initializeActivePublication(cleanLegacy.some))
              mismatch <- makeStore(mismatchDirectory).use { store =>
                for {
                  legacy <- prepare(store, 0L, ordinal0, entries("compensation-mismatch-legacy"))
                  _ <- store.publish(legacy, None)
                  other <- prepare(store, 9L, ordinal1, entries("compensation-mismatch-other"))
                } yield legacy -> other
              }
              (mismatchLegacy, mismatchOther) = mismatch
              _ <- IO.blocking(
                Files.write(
                  DurableMptImageLayout.activePublication(mismatchDirectory),
                  MptImageEncoding.encodePublication(
                    MptActivePublication(MptPublicationRevision(0L), mismatchOther.some)
                  )
                )
              ).void
              mismatchResult <- makeStore(mismatchDirectory)
                .use(_.initializeActivePublication(mismatchLegacy.some))
                .attempt
              mismatchLegacyIntact <- IO.blocking(
                Files.isRegularFile(DurableMptImageLayout.activeManifest(mismatchDirectory)) &&
                  Files.isRegularFile(DurableMptImageLayout.initializationMarker(mismatchDirectory))
              )
              corruptLegacy <- makeStore(corruptDirectory).use { store =>
                prepare(store, 0L, ordinal0, entries("compensation-corrupt")).flatTap(store.publish(_, None))
              }
              corruptBytes = MptImageEncoding
                .encodePublication(MptActivePublication(MptPublicationRevision(0L), corruptLegacy.some))
                .clone()
              _ = corruptBytes(corruptBytes.length - 1) = (corruptBytes.last ^ 1).toByte
              _ <- IO.blocking(
                Files.write(DurableMptImageLayout.activePublication(corruptDirectory), corruptBytes)
              ).void
              corruptResult <- makeStore(corruptDirectory)
                .use(_.initializeActivePublication(corruptLegacy.some))
                .attempt
            } yield {
              val missingMarkerMatches = missingMarker.left.exists {
                case MissingArtifact(MptImageArtifactRef.PublicationInitializationMarker, path, _) =>
                  path == DurableMptImageLayout.publicationInitializationMarker(journalOnlyDirectory)
                case _ => false
              }

              expect.all(
                missingMarkerMatches,
                resumed,
                cleanInitialized.image.contains(cleanLegacy),
                mismatchResult.left.exists(_.isInstanceOf[PublicationInitializationConflict]),
                mismatchLegacyIntact,
                corruptResult.left.exists(_.isInstanceOf[CorruptPublication])
              )
            }
          }
        }
      }
    }
  }

  test("cancellation after publication-journal rename cannot expose an unverified intermediate state") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      TestControl.executeEmbed {
        for {
          prepared <- makeStore(directory).use { healthy =>
            for {
              initial <- healthy.initializeActivePublication(None)
              receipt <- prepare(healthy, 40L, ordinal0, entries("publication-cancel"))
            } yield initial -> MptActivePublication(MptPublicationRevision(1L), receipt.some)
          }
          (initial, target) = prepared
          fired <- Ref.of[IO, Boolean](false)
          entered <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          result <- DurableMptImageStore
            .resourceWith[IO](
              directory,
              DurableFileOps.nio[IO],
              new BarrierOnce(after(AtomicMove, Publication), fired, entered, release, None),
              codecEra,
              rootVerifier,
              limits
            )
            .use { blocked =>
              for {
                publisher <- blocked.transitionActive(initial, target).start
                _ <- entered.get
                cancelDone <- Deferred[IO, Unit]
                canceler <- publisher.cancel.guarantee(cancelDone.complete(()).void).start
                _ <- IO.cede
                cancelBeforeRelease <- cancelDone.tryGet
                _ <- release.complete(())
                _ <- canceler.joinWithNever
                _ <- publisher.join
                active <- blocked.activePublication
                retry <- blocked.transitionActive(initial, target).attempt
              } yield
                expect.all(
                  cancelBeforeRelease.isEmpty,
                  active.contains(target),
                  retry == Right(())
                )
            }
        } yield result
      }
    }
  }

  test("every publication-journal write stage restores the exact prior publication") { res =>
    implicit val (json, hasher) = res

    val points = DurableWriteStage.ordered.flatMap(stage => List(before(stage, Publication), after(stage, Publication)))

    points.traverse { point =>
      tempDir.use { directory =>
        for {
          prepared <- makeStore(directory).use { healthy =>
            for {
              initial <- healthy.initializeActivePublication(None)
              receipt <- prepare(healthy, 20L, ordinal0, entries(s"publication-write-$point"))
            } yield initial -> MptActivePublication(MptPublicationRevision(1L), receipt.some)
          }
          (initial, target) = prepared
          failed <- storeWithFault(directory, point).use(_.transitionActive(initial, target).attempt)
          recovered <- makeStore(directory).use { healthy =>
            (healthy.activePublication, healthy.activeReceipt).mapN { (publication, active) =>
              publication.contains(initial) && active.isEmpty
            }
          }
          temporary <- temporaryArtifacts(directory)
        } yield failed.isLeft && recovered && temporary.isEmpty
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("every publication-initialization-marker write stage restores the exact legacy publication") { res =>
    implicit val (json, hasher) = res

    val points = DurableWriteStage.ordered.flatMap { stage =>
      List(before(stage, PublicationInitializationMarker), after(stage, PublicationInitializationMarker))
    }

    points.traverse { point =>
      tempDir.use { directory =>
        for {
          legacy <- makeStore(directory).use { healthy =>
            prepare(healthy, 0L, ordinal0, entries(s"publication-marker-$point")).flatTap(healthy.publish(_, None))
          }
          failed <- storeWithFault(directory, point).use(_.initializeActivePublication(legacy.some).attempt)
          recovered <- makeStore(directory).use { healthy =>
            (healthy.activePublication, healthy.activeReceipt).mapN { (publication, active) =>
              publication.isEmpty && active.contains(legacy)
            }
          }
          temporary <- temporaryArtifacts(directory)
        } yield failed.isLeft && recovered && temporary.isEmpty
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("publication journal and marker reject checksummed invalid tags and corruption") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        _ <- makeStore(directory).use(_.initializeActivePublication(None))
        publicationPath = DurableMptImageLayout.activePublication(directory)
        markerPath = DurableMptImageLayout.publicationInitializationMarker(directory)
        originalPublication <- IO.blocking(Files.readAllBytes(publicationPath))
        invalidTagPayload = originalPublication.dropRight(32)
        _ = invalidTagPayload(20) = 2.toByte
        invalidTag = invalidTagPayload ++ Hash.sha256DigestFromBytes(invalidTagPayload).toByteArray
        _ <- IO.blocking(Files.write(publicationPath, invalidTag)).void
        invalidTagResult <- makeStore(directory).use(_.activePublication).attempt
        _ <- IO.blocking(Files.write(publicationPath, originalPublication)).void
        originalMarker <- IO.blocking(Files.readAllBytes(markerPath))
        corruptMarker = originalMarker.clone()
        _ = corruptMarker(corruptMarker.length - 1) = (corruptMarker.last ^ 1).toByte
        _ <- IO.blocking(Files.write(markerPath, corruptMarker)).void
        corruptMarkerResult <- makeStore(directory).use(_.activePublication).attempt
      } yield
        expect.all(
          invalidTagResult.left.exists(_.isInstanceOf[CorruptPublication]),
          corruptMarkerResult.left.exists(_.isInstanceOf[CorruptPublicationInitializationMarker])
        )
    }
  }

  test("the publish mutex permits exactly one of two conflicting same-generation publications") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      makeStore(directory).use { store =>
        for {
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
  }

  test("every image write stage fails closed without changing the prior active generation") { res =>
    implicit val (json, hasher) = res

    val points = DurableWriteStage.ordered.flatMap(stage => List(before(stage, Image), after(stage, Image)))

    points.traverse { point =>
      tempDir.use { directory =>
        val initialValues = entries(s"image-prior-$point")
        val candidateValues = entries(s"image-candidate-$point")
        for {
          initial <- makeStore(directory).use { healthy =>
            prepare(healthy, 0L, ordinal0, initialValues).flatTap(healthy.publish(_, None))
          }
          candidateRoot <- rootOf(candidateValues)
          result <- storeWithFault(directory, point).use { failing =>
            failing
              .prepare(1L, GlobalSnapshotStateRef(ordinal1, snapshotHash(ordinal1), parentHash(ordinal1), candidateRoot), candidateValues)
              .attempt
          }
          validPrior <- makeStore(directory).use { restarted =>
            (restarted.activeReceipt, restarted.read(initial)).mapN { (active, loaded) =>
              active.contains(initial) && loaded.toMap == initialValues
            }
          }
          temporary <- temporaryArtifacts(directory)
        } yield result.isLeft && validPrior && temporary.isEmpty
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("every manifest write stage restores the prior bootable generation on failure") { res =>
    implicit val (json, hasher) = res

    val points = DurableWriteStage.ordered.flatMap(stage => List(before(stage, Manifest), after(stage, Manifest)))

    points.traverse { point =>
      tempDir.use { directory =>
        val initialValues = entries(s"manifest-prior-$point")
        for {
          prepared <- makeStore(directory).use { healthy =>
            for {
              initial <- prepare(healthy, 0L, ordinal0, initialValues)
              _ <- healthy.publish(initial, None)
              candidate <- prepare(healthy, 1L, ordinal1, entries(s"manifest-candidate-$point"))
            } yield initial -> candidate
          }
          (initial, candidate) = prepared
          result <- storeWithFault(directory, point).use(_.publish(candidate, Some(initial.pointer)).attempt)
          validPrior <- makeStore(directory).use { restarted =>
            (restarted.activeReceipt, restarted.read(initial)).mapN { (active, loaded) =>
              active.contains(initial) && loaded.toMap == initialValues
            }
          }
        } yield result.isLeft && validPrior
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("every initialization-marker write stage rolls back the first publication") { res =>
    implicit val (json, hasher) = res

    val points = DurableWriteStage.ordered.flatMap(stage => List(before(stage, InitializationMarker), after(stage, InitializationMarker)))

    points.traverse { point =>
      tempDir.use { directory =>
        for {
          receipt <- makeStore(directory).use(prepare(_, 0L, ordinal0, entries(s"marker-$point")))
          failed <- storeWithFault(directory, point).use(_.publish(receipt, None).attempt)
          recovered <- makeStore(directory).use { store =>
            for {
              beforeRetry <- store.activeReceipt
              _ <- store.publish(receipt, None)
              afterRetry <- store.activeReceipt
            } yield beforeRetry.isEmpty && afterRetry.contains(receipt)
          }
        } yield failed.isLeft && recovered
      }
    }.map(results => expect(results.forall(identity)))
  }

  test("a reader cannot observe a renamed candidate while verification and recovery are pending") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      TestControl.executeEmbed {
        val injectedFailure = new IOException("post-rename read-back failed")
        val initialValues = entries("reader-barrier-initial")
        for {
          prepared <- makeStore(directory).use { healthy =>
            for {
              initial <- prepare(healthy, 0L, ordinal0, initialValues)
              _ <- healthy.publish(initial, None)
              candidate <- prepare(healthy, 1L, ordinal1, entries("reader-barrier-candidate"))
            } yield initial -> candidate
          }
          (initial, candidate) = prepared
          fired <- Ref.of[IO, Boolean](false)
          entered <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          result <- DurableMptImageStore
            .resourceWith[IO](
              directory,
              DurableFileOps.nio[IO],
              new BarrierOnce(before(ReadBack, Manifest), fired, entered, release, Some(injectedFailure)),
              codecEra,
              rootVerifier,
              limits
            )
            .use { blocked =>
              for {
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
            }
        } yield result
      }
    }
  }

  test("cancellation after manifest rename cannot interrupt force and read-back publication") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      TestControl.executeEmbed {
        for {
          prepared <- makeStore(directory).use { healthy =>
            for {
              initial <- prepare(healthy, 0L, ordinal0, entries("cancel-initial"))
              _ <- healthy.publish(initial, None)
              candidate <- prepare(healthy, 1L, ordinal1, entries("cancel-candidate"))
            } yield initial -> candidate
          }
          (initial, candidate) = prepared
          fired <- Ref.of[IO, Boolean](false)
          entered <- Deferred[IO, Unit]
          release <- Deferred[IO, Unit]
          result <- DurableMptImageStore
            .resourceWith[IO](
              directory,
              DurableFileOps.nio[IO],
              new BarrierOnce(after(AtomicMove, Manifest), fired, entered, release, None),
              codecEra,
              rootVerifier,
              limits
            )
            .use { blocked =>
              for {
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
            }
        } yield result
      }
    }
  }

  test("publish verifies the prior image before attempting any manifest mutation") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      for {
        prepared <- makeStore(directory).use { healthy =>
          for {
            initial <- prepare(healthy, 0L, ordinal0, entries("prior-verification-initial"))
            _ <- healthy.publish(initial, None)
            candidate <- prepare(healthy, 1L, ordinal1, entries("prior-verification-candidate"))
          } yield initial -> candidate
        }
        (initial, candidate) = prepared
        manifestPath = DurableMptImageLayout.activeManifest(directory)
        manifestBefore <- IO.blocking(Files.readAllBytes(manifestPath))
        priorImagePath = DurableMptImageLayout.image(directory, initial.imageId)
        _ <- IO.blocking(Files.write(priorImagePath, Array[Byte](1, 2, 3))).void
        result <- storeWithFault(directory, before(Write, Manifest)).use(_.publish(candidate, Some(initial.pointer)).attempt)
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
      val delegate = DurableFileOps.nio[IO]
      val unsupported = new DelegatingFileOps(delegate) {
        override def atomicMoveReplace(source: Path, target: Path): IO[Unit] =
          IO.raiseError(new AtomicMoveNotSupportedException(source.toString, target.toString, "injected"))
      }

      DurableMptImageStore
        .resourceWith[IO](
          directory,
          unsupported,
          DurableWriteHook.noop[IO, MptImageArtifact],
          codecEra,
          rootVerifier,
          limits
        )
        .use { store =>
          val values = entries("atomic-required")
          for {
            root <- rootOf(values)
            result <- store
              .prepare(0L, GlobalSnapshotStateRef(ordinal0, snapshotHash(ordinal0), parentHash(ordinal0), root), values)
              .attempt
            active <- store.activeReceipt
          } yield expect.all(result.left.exists(_.isInstanceOf[AtomicMoveRequired]), active.isEmpty)
        }
    }
  }

  test("streaming decode rejects forged allocation lengths and bytes beyond configured bounds") { res =>
    implicit val (json, hasher) = res

    tempDir.use { directory =>
      val values = entries("stream-limits")

      def replacingInt(bytes: Array[Byte], offset: Int, value: Int): Array[Byte] = {
        val result = bytes.clone()
        ByteBuffer.wrap(result).putInt(offset, value)
        result
      }

      for {
        receipt <- makeStore(directory).use { store =>
          prepare(store, 0L, ordinal0, values).flatTap(store.publish(_, None))
        }
        imagePath = DurableMptImageLayout.image(directory, receipt.imageId)
        manifestPath = DurableMptImageLayout.activeManifest(directory)
        originalImage <- IO.blocking(Files.readAllBytes(imagePath))
        originalManifest <- IO.blocking(Files.readAllBytes(manifestPath))
        countOffset = MptImageEncoding.ImageHeaderBytes.toInt - 4
        keyLengthOffset = MptImageEncoding.ImageHeaderBytes.toInt
        firstKeyLength = ByteBuffer.wrap(originalImage).getInt(keyLengthOffset)
        valueLengthOffset = keyLengthOffset + 4 + firstKeyLength
        exactFileLimits = readLimits(
          originalImage.length.toLong,
          maxEntries = 100,
          maxKeyBytes = 128,
          maxValueBytes = originalImage.length
        )
        _ <- IO.blocking(Files.write(imagePath, replacingInt(originalImage, countOffset, Int.MaxValue))).void
        countFailure <- makeStore(directory).use(_.activeReceipt).attempt
        _ <- IO.blocking(Files.write(imagePath, replacingInt(originalImage, keyLengthOffset, Int.MaxValue))).void
        keyFailure <- makeStore(directory).use(_.activeReceipt).attempt
        _ <- IO.blocking(Files.write(imagePath, replacingInt(originalImage, valueLengthOffset, Int.MaxValue))).void
        valueFailure <- makeStore(directory).use(_.activeReceipt).attempt
        remainingAfterValueLength = originalImage.length - valueLengthOffset - 4
        _ <- IO
          .blocking(
            Files.write(imagePath, replacingInt(originalImage, valueLengthOffset, remainingAfterValueLength + 1))
          )
          .void
        aggregateDeclaredLengthFailure <- makeStoreWithLimits(directory, exactFileLimits).use(_.activeReceipt).attempt
        _ <- IO.blocking(Files.write(imagePath, originalImage ++ Array[Byte](1))).void
        trailingImageFailure <- makeStoreWithLimits(directory, exactFileLimits).use(_.activeReceipt).attempt
        _ <- IO.blocking(Files.write(imagePath, originalImage)).void
        _ <- IO.blocking(Files.write(manifestPath, originalManifest ++ Array[Byte](1))).void
        trailingManifestFailure <- makeStore(directory).use(_.activeReceipt).attempt
      } yield
        expect.all(
          countFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          keyFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          valueFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          aggregateDeclaredLengthFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          trailingImageFailure.left.exists(_.isInstanceOf[ImageLimitExceeded]),
          trailingManifestFailure.left.exists(_.isInstanceOf[ImageLimitExceeded])
        )
    }
  }

  test("artifact reads distinguish pristine absence, missing required artifacts, and other I/O failures") { res =>
    implicit val (json, hasher) = res

    tempDir.use { virginDirectory =>
      tempDir.use { missingImageDirectory =>
        tempDir.use { missingManifestDirectory =>
          tempDir.use { failedReadDirectory =>
            val readFailure = new IOException("injected artifact read failure")
            val manifestPath = DurableMptImageLayout.activeManifest(failedReadDirectory)
            val failingFileOps = new DelegatingFileOps(DurableFileOps.nio[IO]) {
              override def openInput(path: Path): Resource[IO, InputStream] =
                if (path == manifestPath) Resource.eval(IO.raiseError(readFailure))
                else super.openInput(path)
            }

            for {
              virgin <- makeStore(virginDirectory).use(_.activeReceipt)
              missingImageReceipt <- makeStore(missingImageDirectory).use { store =>
                prepare(store, 0L, ordinal0, entries("missing-image")).flatTap(store.publish(_, None))
              }
              missingImagePath = DurableMptImageLayout.image(missingImageDirectory, missingImageReceipt.imageId)
              _ <- IO.blocking(Files.delete(missingImagePath))
              missingImage <- makeStore(missingImageDirectory).use(_.activeReceipt).attempt
              missingManifestReceipt <- makeStore(missingManifestDirectory).use { store =>
                prepare(store, 0L, ordinal0, entries("missing-manifest")).flatTap(store.publish(_, None))
              }
              _ <- IO.blocking(Files.delete(DurableMptImageLayout.activeManifest(missingManifestDirectory)))
              missingManifest <- makeStore(missingManifestDirectory).use(_.read(missingManifestReceipt)).attempt
              failedRead <- DurableMptImageStore
                .resourceWith[IO](
                  failedReadDirectory,
                  failingFileOps,
                  DurableWriteHook.noop[IO, MptImageArtifact],
                  codecEra,
                  rootVerifier,
                  limits
                )
                .use(_.activeReceipt)
                .attempt
            } yield {
              val imageErrorMatches = missingImage.left.exists {
                case MissingArtifact(MptImageArtifactRef.Image(imageId), path, _) =>
                  imageId == missingImageReceipt.imageId && path == missingImagePath
                case _ => false
              }
              val manifestErrorMatches = missingManifest.left.exists {
                case MissingArtifact(MptImageArtifactRef.ActiveManifest, path, _) =>
                  path == DurableMptImageLayout.activeManifest(missingManifestDirectory)
                case _ => false
              }
              val readErrorMatches = failedRead.left.exists {
                case ArtifactReadFailed(MptImageArtifactRef.ActiveManifest, path, cause) =>
                  path == manifestPath && cause == readFailure
                case _ => false
              }

              expect.all(virgin.isEmpty, imageErrorMatches, manifestErrorMatches, readErrorMatches)
            }
          }
        }
      }
    }
  }

  test("same-length corruption and truncation of either image or manifest are rejected after restart") { res =>
    implicit val (json, hasher) = res

    tempDir.use { imageDirectory =>
      tempDir.use { manifestDirectory =>
        for {
          imageReceipt <- makeStore(imageDirectory).use { imageStore =>
            prepare(imageStore, 0L, ordinal0, entries("corrupt-image")).flatTap(imageStore.publish(_, None))
          }
          imagePath = DurableMptImageLayout.image(imageDirectory, imageReceipt.imageId)
          originalImage <- IO.blocking(Files.readAllBytes(imagePath))
          corruptImageBytes = originalImage.clone()
          _ = corruptImageBytes(corruptImageBytes.length - 1) = (corruptImageBytes.last ^ 1).toByte
          _ <- IO.blocking(Files.write(imagePath, corruptImageBytes)).void
          corruptImage <- makeStore(imageDirectory).use(_.activeReceipt).attempt
          _ <- IO.blocking(Files.write(imagePath, originalImage.take(3))).void
          truncatedImage <- makeStore(imageDirectory).use(_.activeReceipt).attempt
          _ <- makeStore(manifestDirectory).use { manifestStore =>
            prepare(manifestStore, 0L, ordinal0, entries("corrupt-manifest")).flatTap(manifestStore.publish(_, None))
          }
          manifestPath = DurableMptImageLayout.activeManifest(manifestDirectory)
          originalManifest <- IO.blocking(Files.readAllBytes(manifestPath))
          corruptManifestBytes = originalManifest.clone()
          _ = corruptManifestBytes(12) = (corruptManifestBytes(12) ^ 1).toByte
          _ <- IO.blocking(Files.write(manifestPath, corruptManifestBytes)).void
          corruptManifest <- makeStore(manifestDirectory).use(_.activeReceipt).attempt
          _ <- IO.blocking(Files.write(manifestPath, originalManifest.take(3))).void
          truncatedManifest <- makeStore(manifestDirectory).use(_.activeReceipt).attempt
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

  private class DelegatingFileOps(delegate: DurableFileOps[IO]) extends DurableFileOps[IO] {
    def acquireExclusiveLock(path: Path): Resource[IO, Unit] = delegate.acquireExclusiveLock(path)
    def createDirectories(path: Path): IO[Unit] = delegate.createDirectories(path)
    def createTempFile(directory: Path, prefix: String, suffix: String): IO[Path] = delegate.createTempFile(directory, prefix, suffix)
    def write(path: Path, bytes: Array[Byte]): IO[Unit] = delegate.write(path, bytes)
    def forceFile(path: Path): IO[Unit] = delegate.forceFile(path)
    def atomicMoveReplace(source: Path, target: Path): IO[Unit] = delegate.atomicMoveReplace(source, target)
    def forceDirectory(path: Path): IO[Unit] = delegate.forceDirectory(path)
    def openInput(path: Path): Resource[IO, InputStream] = delegate.openInput(path)
    def isDirectory(path: Path): IO[Boolean] = delegate.isDirectory(path)
    def deleteIfExists(path: Path): IO[Unit] = delegate.deleteIfExists(path)
  }
}
