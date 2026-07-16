package io.constellationnetwork.node.shared.domain.nakamoto.custody

import java.io.IOException
import java.nio.file._

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.constellationnetwork.storage.durable._

import weaver.SimpleIOSuite

object MetagraphBinaryCustodyStoreSuite extends SimpleIOSuite {
  import DurableWriteBoundary._
  import DurableWriteStage._
  import MetagraphBinaryCustodyRecoveryRequired._
  import MetagraphBinaryCustodyRejected._
  import MetagraphBinaryCustodyStore._
  import MetagraphBinaryCustodyUnavailable._

  private val limits = MetagraphBinaryCustodyLimits
    .from(maxItems = 4, maxTotalBytes = 1024L, maxArtifactBytes = 512L)
    .fold(throw _, identity)

  private type FaultEvent = DurableWriteEvent[BinaryArtifact]

  private final class FailOnce(
    stage: DurableWriteStage,
    boundary: DurableWriteBoundary,
    fired: Ref[IO, Boolean],
    failure: Throwable
  ) extends DurableWriteHook[IO, BinaryArtifact] {
    def onEvent(event: FaultEvent): IO[Unit] =
      if (event.stage != stage || event.boundary != boundary) IO.unit
      else
        fired.modify {
          case false => true -> IO.raiseError[Unit](failure)
          case true  => true -> IO.unit
        }.flatten
  }

  private final class BarrierOnce(
    stage: DurableWriteStage,
    boundary: DurableWriteBoundary,
    fired: Ref[IO, Boolean],
    entered: Deferred[IO, Unit],
    release: Deferred[IO, Unit]
  ) extends DurableWriteHook[IO, BinaryArtifact] {
    def onEvent(event: FaultEvent): IO[Unit] =
      if (event.stage != stage || event.boundary != boundary) IO.unit
      else
        fired.modify {
          case false => true -> (entered.complete(()).void >> release.get)
          case true  => true -> IO.unit
        }.flatten
  }

  private final class BarrierThenFailOnce(
    stage: DurableWriteStage,
    boundary: DurableWriteBoundary,
    fired: Ref[IO, Boolean],
    entered: Deferred[IO, Unit],
    release: Deferred[IO, Unit],
    failure: Throwable
  ) extends DurableWriteHook[IO, BinaryArtifact] {
    def onEvent(event: FaultEvent): IO[Unit] =
      if (event.stage != stage || event.boundary != boundary) IO.unit
      else
        fired.modify {
          case false => true -> (entered.complete(()).void >> release.get >> IO.raiseError[Unit](failure))
          case true  => true -> IO.unit
        }.flatten
  }

  private final class CorruptBeforeReadBack extends DurableWriteHook[IO, BinaryArtifact] {
    def onEvent(event: FaultEvent): IO[Unit] =
      if (event.stage != ReadBack || event.boundary != Before) IO.unit
      else
        event.artifact match {
          case artifact: ExactBinaryArtifact =>
            IO.blocking(
              Files.write(
                artifactPath(artifact.contentAddress),
                Array[Byte](99, 98, 97),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
              )
            ).void
        }

    private var directory: Path = _

    def in(value: Path): CorruptBeforeReadBack = {
      directory = value
      this
    }

    private def artifactPath(contentAddress: LocalCustodyAddress): Path =
      directory.resolve(contentAddress.hex + ".binary")
  }

  test("put stores exact captured bytes and mints only a non-serializable local capability") {
    tempDirectory.use { directory =>
      MetagraphBinaryCustodyStore.resource[IO](directory, limits).use { store =>
        val supplied = Array[Byte](1, 2, 3, 4)
        val expected = supplied.clone()

        for {
          stored <- store.putExact(supplied)
          _ <- IO.delay(java.util.Arrays.fill(supplied, 0.toByte))
          read <- store.readExact(stored)
          usage <- store.usage
          serializable = stored.asInstanceOf[AnyRef].isInstanceOf[java.io.Serializable]
          addressSerializable = stored.contentAddress.asInstanceOf[AnyRef].isInstanceOf[java.io.Serializable]
        } yield
          expect.all(
            stored.contentAddress == LocalCustodyAddress.fromExactBytes(expected),
            stored.byteLength == expected.length.toLong,
            read.sameElements(expected),
            usage.items == 1,
            usage.totalBytes == expected.length.toLong,
            !serializable,
            !addressSerializable
          )
      }
    }
  }

  test("idempotent put consumes capacity once and verified open survives restart") {
    tempDirectory.use { directory =>
      val bytes = Array[Byte](8, 7, 6, 5)
      val contentAddress = LocalCustodyAddress.fromExactBytes(bytes)

      MetagraphBinaryCustodyStore.resource[IO](directory, limits).use { store =>
        for {
          first <- store.putExact(bytes)
          second <- store.putExact(bytes.clone())
          usage <- store.usage
        } yield
          expect.all(
            first.contentAddress == second.contentAddress,
            usage.items == 1,
            usage.totalBytes == bytes.length.toLong
          )
      } >> MetagraphBinaryCustodyStore.resource[IO](directory, limits).use { reopened =>
        for {
          usage <- reopened.usage
          stored <- reopened.openExact(contentAddress)
          read <- reopened.readExact(stored)
        } yield expect.all(usage.items == 1, usage.totalBytes == bytes.length.toLong, read.sameElements(bytes))
      }
    }
  }

  test("resource release closes every leaked operation before lock release and reopen") {
    val oneItem = MetagraphBinaryCustodyLimits.from(1, 16L, 8L).fold(throw _, identity)
    val bytes = Array[Byte](8, 6, 4, 2)

    tempDirectory.use { directory =>
      for {
        allocated <- MetagraphBinaryCustodyStore.resource[IO](directory, oneItem).allocated
        (leaked, release) = allocated
        stored <- leaked.putExact(bytes)
        _ <- release
        putAfterClose <- leaked.putExact(null).attempt
        openAfterClose <- leaked.openExact(stored.contentAddress).attempt
        readAfterClose <- leaked.readExact(stored).attempt
        usageAfterClose <- leaked.usage.attempt
        reopened <- MetagraphBinaryCustodyStore.resource[IO](directory, oneItem).use { store =>
          for {
            stale <- store.readExact(stored).attempt
            reopenedStored <- store.openExact(stored.contentAddress)
            read <- store.readExact(reopenedStored)
            full <- store.putExact(Array[Byte](9)).attempt
            usage <- store.usage
          } yield (stale, read, full, usage)
        }
        (stale, read, full, usage) = reopened
      } yield
        expect.all(
          List(putAfterClose, openAfterClose, readAfterClose, usageAfterClose).forall(
            _.left.exists(_.isInstanceOf[StoreClosed])
          ),
          stale.left.exists(_.isInstanceOf[ForeignCapability]),
          read.sameElements(bytes),
          full.left.exists(_.isInstanceOf[CapacityExceeded]),
          usage.items == 1,
          usage.totalBytes == bytes.length.toLong
        )
    }
  }

  test("resource finalizer waits for the operation semaphore before closing") {
    tempDirectory.use { directory =>
      for {
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        continue <- Deferred[IO, Unit]
        released <- Deferred[IO, Unit]
        hook = new BarrierOnce(ForceFile, After, fired, entered, continue)
        allocated <- resourceWith[IO](directory, limits, hook).allocated
        (store, release) = allocated
        put <- store.putExact(Array[Byte](7, 7, 7)).start
        _ <- entered.get.timeout(3.seconds)
        releasing <- (release >> released.complete(()).void).start
        _ <- IO.sleep(100.millis)
        releasedWhileBusy <- released.tryGet
        _ <- continue.complete(())
        _ <- put.joinWithNever.timeout(3.seconds)
        _ <- releasing.joinWithNever.timeout(3.seconds)
        afterClose <- store.usage.attempt
      } yield
        expect.all(
          releasedWhileBusy.isEmpty,
          afterClose.left.exists(_.isInstanceOf[StoreClosed])
        )
    }
  }

  test("concurrent distinct puts cannot overbook one-item capacity") {
    val oneItem = MetagraphBinaryCustodyLimits.from(1, 16L, 8L).fold(throw _, identity)

    tempDirectory.use { directory =>
      MetagraphBinaryCustodyStore.resource[IO](directory, oneItem).use { store =>
        for {
          attempts <- (store.putExact(Array[Byte](1)).attempt, store.putExact(Array[Byte](2)).attempt).parTupled
          usage <- store.usage
          successes = List(attempts._1, attempts._2).count(_.isRight)
          capacityFailures = List(attempts._1, attempts._2).count(_.left.exists(_.isInstanceOf[CapacityExceeded]))
        } yield expect.all(successes == 1, capacityFailures == 1, usage.items == 1, usage.totalBytes == 1L)
      }
    }
  }

  test("caller limits reject per-artifact, item, total, and invalid configurations") {
    val oneItem = MetagraphBinaryCustodyLimits.from(1, 10L, 10L).fold(throw _, identity)
    val fourBytes = MetagraphBinaryCustodyLimits.from(3, 4L, 4L).fold(throw _, identity)

    val invalid = MetagraphBinaryCustodyLimits.from(0, 0L, Int.MaxValue.toLong + 1L)

    tempDirectory.use { itemDirectory =>
      tempDirectory.use { byteDirectory =>
        for {
          oversized <- MetagraphBinaryCustodyStore
            .resource[IO](itemDirectory, oneItem)
            .use(
              _.putExact(new Array[Byte](11)).attempt
            )
          itemFull <- MetagraphBinaryCustodyStore.resource[IO](itemDirectory, oneItem).use { store =>
            store.putExact(Array[Byte](1)) >> store.putExact(Array[Byte](2)).attempt
          }
          byteFull <- MetagraphBinaryCustodyStore.resource[IO](byteDirectory, fourBytes).use { store =>
            store.putExact(Array[Byte](1, 2, 3)) >> store.putExact(Array[Byte](4, 5)).attempt
          }
        } yield
          expect.all(
            oversized.left.exists(error =>
              error.isInstanceOf[ArtifactTooLarge] &&
                error.isInstanceOf[MetagraphBinaryCustodyUnavailable] &&
                !error.isInstanceOf[MetagraphBinaryCustodyRejected]
            ),
            itemFull.left.exists(error =>
              error.isInstanceOf[CapacityExceeded] &&
                error.isInstanceOf[MetagraphBinaryCustodyUnavailable] &&
                !error.isInstanceOf[MetagraphBinaryCustodyRejected]
            ),
            byteFull.left.exists(error =>
              error.isInstanceOf[CapacityExceeded] &&
                error.isInstanceOf[MetagraphBinaryCustodyUnavailable] &&
                !error.isInstanceOf[MetagraphBinaryCustodyRejected]
            ),
            invalid.left.exists(error => error.detail.contains("maxItems") && error.detail.contains("maxTotalBytes"))
          )
      }
    }
  }

  test("startup inventory is bounded by caller item and total limits") {
    val oneItem = MetagraphBinaryCustodyLimits.from(1, 16L, 8L).fold(throw _, identity)
    val twoBytes = MetagraphBinaryCustodyLimits.from(4, 2L, 8L).fold(throw _, identity)

    tempDirectory.use { itemDirectory =>
      tempDirectory.use { byteDirectory =>
        for {
          _ <- writeArtifact(itemDirectory, Array[Byte](1))
          _ <- writeArtifact(itemDirectory, Array[Byte](2))
          tooMany <- MetagraphBinaryCustodyStore.resource[IO](itemDirectory, oneItem).use(_ => IO.unit).attempt
          _ <- writeArtifact(byteDirectory, Array[Byte](1, 2, 3))
          tooLarge <- MetagraphBinaryCustodyStore.resource[IO](byteDirectory, twoBytes).use(_ => IO.unit).attempt
        } yield
          expect.all(
            tooMany.left.exists(_.isInstanceOf[InventoryEnumerationLimitExceeded]),
            tooLarge.left.exists(_.isInstanceOf[ExistingInventoryExceedsLimits])
          )
      }
    }
  }

  test("startup artifact above a lowered local limit is unavailable, not corrupt") {
    val initialLimits = MetagraphBinaryCustodyLimits.from(2, 16L, 8L).fold(throw _, identity)
    val loweredLimits = MetagraphBinaryCustodyLimits.from(2, 16L, 2L).fold(throw _, identity)
    val bytes = Array[Byte](1, 2, 3, 4)

    tempDirectory.use { directory =>
      MetagraphBinaryCustodyStore.resource[IO](directory, initialLimits).use(_.putExact(bytes).void) >>
        MetagraphBinaryCustodyStore.resource[IO](directory, loweredLimits).use(_ => IO.unit).attempt.map { result =>
          expect(
            result.left.exists(error =>
              error.isInstanceOf[ExistingArtifactExceedsLimit] &&
                error.isInstanceOf[MetagraphBinaryCustodyUnavailable] &&
                !error.isInstanceOf[MetagraphBinaryCustodyRecoveryRequired]
            )
          )
        }
    }
  }

  test("resource requires a caller-created directory and never creates one implicitly") {
    tempDirectory.use { parent =>
      val missing = parent.resolve("not-created")

      for {
        result <- MetagraphBinaryCustodyStore.resource[IO](missing, limits).use(_ => IO.unit).attempt
        exists <- IO.blocking(Files.exists(missing))
      } yield expect.all(result.left.exists(_.isInstanceOf[DirectoryPreconditionFailed]), !exists)
    }
  }

  test("symbolic-link root components and lock files fail closed") {
    tempDirectory.use { sandbox =>
      val realRoot = sandbox.resolve("real-root")
      val linkedRoot = sandbox.resolve("linked-root")
      val lockRoot = sandbox.resolve("lock-root")
      val lockTarget = sandbox.resolve("lock-target")
      val lockPath = lockRoot.resolve(".metagraph-binary-custody.lock")

      for {
        _ <- IO.blocking(Files.createDirectory(realRoot))
        _ <- IO.blocking(Files.createSymbolicLink(linkedRoot, realRoot))
        _ <- IO.blocking(Files.createDirectory(lockRoot))
        _ <- IO.blocking(Files.write(lockTarget, Array[Byte](1)))
        _ <- IO.blocking(Files.createSymbolicLink(lockPath, lockTarget))
        rootResult <- MetagraphBinaryCustodyStore.resource[IO](linkedRoot, limits).use(_ => IO.unit).attempt
        lockResult <- MetagraphBinaryCustodyStore.resource[IO](lockRoot, limits).use(_ => IO.unit).attempt
      } yield
        expect.all(
          rootResult.left.exists {
            case DirectoryPreconditionFailed(path, detail) => path.isAbsolute && detail.contains("symbolic-link")
            case _                                         => false
          },
          lockResult.left.exists(_.isInstanceOf[LockPreconditionFailed])
        )
    }
  }

  test("live root and ancestor replacement fail closed without writing through replacement paths") {
    tempDirectory.use { sandbox =>
      val root = sandbox.resolve("root")
      val movedRoot = sandbox.resolve("moved-root")
      val ancestor = sandbox.resolve("ancestor")
      val ancestorRoot = ancestor.resolve("root")
      val movedAncestor = sandbox.resolve("moved-ancestor")

      for {
        _ <- IO.blocking(Files.createDirectory(root))
        rootAllocated <- MetagraphBinaryCustodyStore.resource[IO](root, limits).allocated
        (rootStore, releaseRoot) = rootAllocated
        _ <- IO.blocking(Files.move(root, movedRoot, StandardCopyOption.ATOMIC_MOVE))
        _ <- IO.blocking(Files.createDirectory(root))
        rootOperation <- rootStore.putExact(Array[Byte](1, 2, 3)).attempt
        rootReplacementEntries <- directoryEntries(root)
        rootRelease <- releaseRoot.attempt
        _ <- IO.blocking(Files.createDirectories(ancestorRoot))
        ancestorAllocated <- MetagraphBinaryCustodyStore.resource[IO](ancestorRoot, limits).allocated
        (ancestorStore, releaseAncestor) = ancestorAllocated
        _ <- IO.blocking(Files.move(ancestor, movedAncestor, StandardCopyOption.ATOMIC_MOVE))
        _ <- IO.blocking(Files.createDirectories(ancestorRoot))
        ancestorOperation <- ancestorStore.usage.attempt
        ancestorReplacementEntries <- directoryEntries(ancestorRoot)
        ancestorRelease <- releaseAncestor.attempt
      } yield
        expect.all(
          rootOperation.left.exists(_.isInstanceOf[OwnershipIdentityChanged]),
          rootReplacementEntries.isEmpty,
          rootRelease.left.exists(containsOwnershipChange),
          ancestorOperation.left.exists(_.isInstanceOf[OwnershipIdentityChanged]),
          ancestorReplacementEntries.isEmpty,
          ancestorRelease.left.exists(containsOwnershipChange)
        )
    }
  }

  test("live lock unlink permits a second inode owner but permanently disables the original store") {
    tempDirectory.use { directory =>
      val lockPath = directory.resolve(".metagraph-binary-custody.lock")

      for {
        firstAllocated <- MetagraphBinaryCustodyStore.resource[IO](directory, limits).allocated
        (first, releaseFirst) = firstAllocated
        _ <- IO.blocking(Files.delete(lockPath))
        firstOperation <- first.usage.attempt
        secondAllocated <- MetagraphBinaryCustodyStore.resource[IO](directory, limits).allocated
        (second, releaseSecond) = secondAllocated
        secondUsage <- second.usage
        firstRelease <- releaseFirst.attempt
        secondRelease <- releaseSecond.attempt
      } yield
        expect.all(
          firstOperation.left.exists(_.isInstanceOf[OwnershipIdentityChanged]),
          secondUsage.items == 0,
          firstRelease.left.exists(containsOwnershipChange),
          secondRelease.isRight
        )
    }
  }

  test("providers without SecureDirectoryStream fail unavailable before taking storage authority") {
    tempDirectory.use { directory =>
      val nonSecureOpen: Path => DirectoryStream[Path] = path => {
        val delegate = Files.newDirectoryStream(path)
        new DirectoryStream[Path] {
          def iterator(): java.util.Iterator[Path] = delegate.iterator()
          def close(): Unit = delegate.close()
        }
      }
      val hook = DurableWriteHook.noop[IO, BinaryArtifact]

      for {
        result <- resourceWith[IO](directory, limits, hook, nonSecureOpen).use(_ => IO.unit).attempt
        entries <- directoryEntries(directory)
      } yield
        expect.all(
          result.left.exists(_.isInstanceOf[SecureStorageUnsupported]),
          entries.isEmpty
        )
    }
  }

  test("symbolic-link artifacts reject at startup and through a live capability even when target bytes match") {
    tempDirectory.use { sandbox =>
      val startupRoot = sandbox.resolve("startup")
      val runtimeRoot = sandbox.resolve("runtime")
      val external = sandbox.resolve("external.binary")
      val bytes = Array[Byte](3, 1, 4, 1)
      val address = LocalCustodyAddress.fromExactBytes(bytes)

      for {
        _ <- IO.blocking(Files.createDirectory(startupRoot))
        _ <- IO.blocking(Files.createDirectory(runtimeRoot))
        _ <- IO.blocking(Files.write(external, bytes))
        _ <- IO.blocking(Files.createSymbolicLink(artifactPath(startupRoot, address), external))
        startup <- MetagraphBinaryCustodyStore.resource[IO](startupRoot, limits).use(_ => IO.unit).attempt
        runtime <- MetagraphBinaryCustodyStore.resource[IO](runtimeRoot, limits).use { store =>
          for {
            stored <- store.putExact(bytes)
            path = artifactPath(runtimeRoot, stored.contentAddress)
            _ <- IO.blocking(Files.delete(path))
            _ <- IO.blocking(Files.createSymbolicLink(path, external))
            read <- store.readExact(stored).attempt
          } yield read
        }
      } yield
        expect.all(
          startup.left.exists(_.isInstanceOf[CorruptArtifact]),
          runtime.left.exists(_.isInstanceOf[CorruptArtifact])
        )
    }
  }

  test("artifact replacement between identity check and no-follow open is detected even when exact bytes match") {
    tempDirectory.use { directory =>
      val bytes = Array[Byte](5, 4, 3, 2, 1)
      val writeHook = DurableWriteHook.noop[IO, BinaryArtifact]
      val openDirectory: Path => DirectoryStream[Path] = path => Files.newDirectoryStream(path)

      for {
        armed <- Ref.of[IO, Boolean](false)
        fired <- Ref.of[IO, Boolean](false)
        openHook = new ArtifactOpenHook[IO] {
          def beforeOpen(path: Path): IO[Unit] =
            armed.get.ifM(
              fired.modify {
                case false =>
                  true -> IO.blocking {
                    Files.delete(path)
                    Files.write(path, bytes)
                    ()
                  }
                case true => true -> IO.unit
              }.flatten,
              IO.unit
            )
        }
        result <- resourceWith[IO](directory, limits, writeHook, openDirectory, openHook).use { store =>
          for {
            stored <- store.putExact(bytes)
            _ <- armed.set(true)
            replacedDuringOpen <- store.readExact(stored).attempt
            stableRead <- store.readExact(stored)
          } yield replacedDuringOpen -> stableRead
        }
        (replacedDuringOpen, stableRead) = result
      } yield
        expect.all(
          replacedDuringOpen.left.exists(_.isInstanceOf[OwnershipIdentityChanged]),
          stableRead.sameElements(bytes)
        )
    }
  }

  test("hostile stale-temp population fails at the bounded directory-entry prefix") {
    val twoItems = MetagraphBinaryCustodyLimits.from(2, 16L, 8L).fold(throw _, identity)

    tempDirectory.use { directory =>
      for {
        _ <- (0 until 64).toList.traverse_(index => writeStaleTemporary(directory, index))
        result <- MetagraphBinaryCustodyStore.resource[IO](directory, twoItems).use(_ => IO.unit).attempt
      } yield
        expect(result.left.exists {
          case DirectoryEnumerationLimitExceeded(observed, maximum) => observed == 4L && maximum == 3L
          case _                                                    => false
        })
    }
  }

  test("missing and corrupt artifacts fail with typed RecoveryRequired") {
    tempDirectory.use { missingDirectory =>
      tempDirectory.use { corruptDirectory =>
        val bytes = Array[Byte](1, 3, 5, 7)

        for {
          missing <- MetagraphBinaryCustodyStore.resource[IO](missingDirectory, limits).use { store =>
            for {
              stored <- store.putExact(bytes)
              _ <- IO.blocking(Files.delete(artifactPath(missingDirectory, stored.contentAddress)))
              result <- store.readExact(stored).attempt
            } yield result
          }
          corrupt <- MetagraphBinaryCustodyStore.resource[IO](corruptDirectory, limits).use { store =>
            for {
              stored <- store.putExact(bytes)
              _ <- IO.blocking(
                Files.write(
                  artifactPath(corruptDirectory, stored.contentAddress),
                  Array[Byte](7, 5, 3, 1),
                  StandardOpenOption.TRUNCATE_EXISTING
                )
              )
              result <- store.readExact(stored).attempt
            } yield result
          }
        } yield
          expect.all(
            missing.left.exists(_.isInstanceOf[MissingArtifact]),
            corrupt.left.exists(_.isInstanceOf[CorruptArtifact])
          )
      }
    }
  }

  test("readback corruption prevents capability minting and enters RecoveryRequired") {
    tempDirectory.use { directory =>
      val hook = new CorruptBeforeReadBack().in(directory)

      resourceWith[IO](directory, limits, hook).use { store =>
        store.putExact(Array[Byte](1, 2, 3)).attempt.map { result =>
          expect(result.left.exists(_.isInstanceOf[CorruptArtifact]))
        }
      }
    }
  }

  test("uncertain post-move recovery poisons the live store until a repaired full reopen scan") {
    val oneItem = MetagraphBinaryCustodyLimits.from(1, 32L, 16L).fold(throw _, identity)
    val bytesA = Array[Byte](1, 2, 3, 4)
    val bytesB = Array[Byte](5, 6, 7)
    val addressA = LocalCustodyAddress.fromExactBytes(bytesA)
    val addressB = LocalCustodyAddress.fromExactBytes(bytesB)

    tempDirectory.use { directory =>
      val hook = new CorruptBeforeReadBack().in(directory)

      for {
        allocated <- resourceWith[IO](directory, oneItem, hook).allocated
        (store, release) = allocated
        failedA <- store.putExact(bytesA).attempt
        latched <- failedA match {
          case Left(error: MetagraphBinaryCustodyRecoveryRequired) => IO.pure(error)
          case other => IO.raiseError(new AssertionError(s"post-move corruption was not recovery-required: $other"))
        }
        poisonedUsage <- store.usage.attempt
        poisonedOpen <- store.openExact(addressA).attempt
        poisonedRead <- store.readExact(null).attempt
        poisonedPutB <- store.putExact(bytesB).attempt
        aExists <- IO.blocking(Files.exists(artifactPath(directory, addressA)))
        bExists <- IO.blocking(Files.exists(artifactPath(directory, addressB)))
        releaseResult <- release.attempt
        _ <- IO.blocking(Files.delete(artifactPath(directory, addressA)))
        reopened <- MetagraphBinaryCustodyStore.resource[IO](directory, oneItem).use { repaired =>
          for {
            storedB <- repaired.putExact(bytesB)
            readB <- repaired.readExact(storedB)
            usage <- repaired.usage
          } yield (readB, usage)
        }
        (readB, reopenedUsage) = reopened
      } yield
        expect.all(
          poisonedUsage.left.exists(_ eq latched),
          poisonedOpen.left.exists(_ eq latched),
          poisonedRead.left.exists(_ eq latched),
          poisonedPutB.left.exists(_ eq latched),
          aExists,
          !bExists,
          releaseResult.isRight,
          readB.sameElements(bytesB),
          reopenedUsage.items == 1,
          reopenedUsage.totalBytes == bytesB.length.toLong
        )
    }
  }

  test("every durable-write boundary fails locally and supports an idempotent verified retry") {
    val points = DurableWriteStage.ordered.flatMap(stage => List(Before, After).map(boundary => stage -> boundary))
    val bytes = Array[Byte](6, 2, 6, 4)

    points.traverse_ {
      case (stage, boundary) =>
        tempDirectory.use { directory =>
          for {
            fired <- Ref.of[IO, Boolean](false)
            failure = new IOException(s"injected $boundary $stage")
            hook = new FailOnce(stage, boundary, fired, failure)
            _ <- resourceWith[IO](directory, limits, hook).use { store =>
              for {
                first <- store.putExact(bytes).attempt
                retried <- store.putExact(bytes)
                read <- store.readExact(retried)
                usage <- store.usage
                _ <- IO.raiseUnless(
                  first.left.exists {
                    case error: DurableWriteFailed => error.getCause eq failure
                    case _                         => false
                  } && read.sameElements(bytes) && usage.items == 1 && usage.totalBytes == bytes.length.toLong
                )(
                  new AssertionError(
                    s"durable boundary did not reconcile: stage=$stage boundary=$boundary first=$first " +
                      s"usage=${usage.items}/${usage.totalBytes}"
                  )
                )
              } yield ()
            }
          } yield ()
        }
    }.as(success)
  }

  test("failure before atomic install publishes nothing and removes its temporary file") {
    tempDirectory.use { directory =>
      for {
        fired <- Ref.of[IO, Boolean](false)
        failure = new IOException("injected before atomic move")
        hook = new FailOnce(AtomicMove, Before, fired, failure)
        result <- resourceWith[IO](directory, limits, hook).use { store =>
          store.putExact(Array[Byte](4, 3, 2, 1)).attempt
        }
        entries <- directoryEntries(directory)
      } yield
        expect.all(
          result.left.exists {
            case error: DurableWriteFailed => error.getCause eq failure
            case _                         => false
          },
          entries.forall(path => path.getFileName.toString == ".metagraph-binary-custody.lock")
        )
    }
  }

  test("swapping the opened temporary leaf for a symlink never truncates the external target") {
    tempDirectory.use { sandbox =>
      val directory = sandbox.resolve("custody")
      val external = sandbox.resolve("external")
      val externalBytes = Array[Byte](9, 9, 9, 9, 9)

      for {
        _ <- IO.blocking(Files.createDirectory(directory))
        _ <- IO.blocking(Files.write(external, externalBytes))
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        continue <- Deferred[IO, Unit]
        hook = new BarrierOnce(Write, Before, fired, entered, continue)
        outcome <- resourceWith[IO](directory, limits, hook).use { store =>
          for {
            put <- store.putExact(Array[Byte](1, 2, 3, 4)).attempt.start
            _ <- entered.get.timeout(3.seconds)
            entries <- directoryEntries(directory)
            temporary <- IO.fromOption(entries.find(_.getFileName.toString.endsWith(".tmp")))(
              new AssertionError(s"opened temporary file not found in $entries")
            )
            _ <- IO.blocking(Files.delete(temporary))
            _ <- IO.blocking(Files.createSymbolicLink(temporary, external))
            _ <- continue.complete(())
            result <- put.joinWithNever.timeout(3.seconds)
            usage <- store.usage
            remaining <- directoryEntries(directory)
          } yield (result, usage, remaining)
        }
        (result, usage, remaining) = outcome
        externalAfter <- IO.blocking(Files.readAllBytes(external))
      } yield
        expect.all(
          result.left.exists(_.isInstanceOf[DurableWriteFailed]),
          externalAfter.sameElements(externalBytes),
          usage.items == 0,
          usage.totalBytes == 0L,
          remaining.forall(_.getFileName.toString == ".metagraph-binary-custody.lock")
        )
    }
  }

  test("cancellation during preparation publishes nothing and removes its temporary file") {
    tempDirectory.use { directory =>
      for {
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        hook = new BarrierOnce(ForceFile, After, fired, entered, release)
        _ <- resourceWith[IO](directory, limits, hook).use { store =>
          for {
            fiber <- store.putExact(Array[Byte](9, 8, 7)).start
            _ <- entered.get.timeout(3.seconds)
            _ <- fiber.cancel.timeout(3.seconds)
            usage <- store.usage
            entries <- directoryEntries(directory)
            _ <- IO.raiseUnless(usage.items == 0 && entries.forall(_.getFileName.toString == ".metagraph-binary-custody.lock"))(
              new AssertionError(s"canceled preparation leaked custody state: items=${usage.items} entries=$entries")
            )
          } yield ()
        }
      } yield success
    }
  }

  test("post-install failure is durably reconciled and retry is idempotent") {
    tempDirectory.use { directory =>
      val bytes = Array[Byte](5, 10, 15)

      for {
        fired <- Ref.of[IO, Boolean](false)
        failure = new IOException("injected before directory force")
        hook = new FailOnce(ForceDirectory, Before, fired, failure)
        result <- resourceWith[IO](directory, limits, hook).use { store =>
          for {
            failed <- store.putExact(bytes).attempt
            retried <- store.putExact(bytes)
            read <- store.readExact(retried)
            usage <- store.usage
          } yield (failed, read, usage)
        }
        (failed, read, usage) = result
      } yield
        expect.all(
          failed.left.exists {
            case error: DurableWriteFailed => error.getCause eq failure
            case _                         => false
          },
          read.sameElements(bytes),
          usage.items == 1,
          usage.totalBytes == bytes.length.toLong
        )
    }
  }

  test("pending cancellation after atomic install leaves a verified idempotent artifact") {
    tempDirectory.use { directory =>
      val bytes = Array[Byte](2, 4, 6, 8)

      for {
        fired <- Ref.of[IO, Boolean](false)
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        hook = new BarrierOnce(ReadBack, Before, fired, entered, release)
        result <- resourceWith[IO](directory, limits, hook).use { store =>
          for {
            fiber <- store.putExact(bytes).start
            _ <- entered.get.timeout(3.seconds)
            cancellation <- fiber.cancel.start
            _ <- release.complete(())
            _ <- cancellation.joinWithNever.timeout(3.seconds)
            retried <- store.putExact(bytes)
            read <- store.readExact(retried)
            usage <- store.usage
          } yield (read, usage)
        }
        (read, usage) = result
      } yield expect.all(read.sameElements(bytes), usage.items == 1, usage.totalBytes == bytes.length.toLong)
    }
  }

  test("pending cancellation cannot interrupt reconciliation after a post-move failure") {
    val bytes = Array[Byte](4, 8, 15, 16, 23, 42)
    val points = List(ForceDirectory -> After, ReadBack -> Before)

    points.traverse_ {
      case (stage, boundary) =>
        tempDirectory.use { directory =>
          for {
            fired <- Ref.of[IO, Boolean](false)
            entered <- Deferred[IO, Unit]
            continue <- Deferred[IO, Unit]
            failure = new IOException(s"post-move failure at $boundary $stage")
            hook = new BarrierThenFailOnce(stage, boundary, fired, entered, continue, failure)
            _ <- resourceWith[IO](directory, limits, hook).use { store =>
              for {
                put <- store.putExact(bytes).start
                _ <- entered.get.timeout(3.seconds)
                cancellation <- put.cancel.start
                _ <- IO.sleep(50.millis)
                _ <- continue.complete(())
                _ <- cancellation.joinWithNever.timeout(3.seconds)
                outcome <- put.join.timeout(3.seconds)
                retried <- store.putExact(bytes)
                read <- store.readExact(retried)
                usage <- store.usage
                expectedTerminal = outcome.isCanceled || (outcome match {
                  case Outcome.Errored(error: DurableWriteFailed) => error.getCause eq failure
                  case _                                          => false
                })
                _ <- IO.raiseUnless(
                  expectedTerminal &&
                    read.sameElements(bytes) &&
                    usage.items == 1 &&
                    usage.totalBytes == bytes.length.toLong
                )(
                  new AssertionError(
                    s"pending cancellation escaped reconciliation: stage=$stage boundary=$boundary outcome=$outcome " +
                      s"usage=${usage.items}/${usage.totalBytes}"
                  )
                )
              } yield ()
            }
          } yield ()
        }
    }.as(success)
  }

  test("reopen observes reconciled state after injected move, directory-force, and readback failures") {
    val bytes = Array[Byte](2, 7, 1, 8)
    val address = LocalCustodyAddress.fromExactBytes(bytes)
    val points = List(
      (AtomicMove, Before, false),
      (AtomicMove, After, true),
      (ForceDirectory, Before, true),
      (ForceDirectory, After, true),
      (ReadBack, Before, true),
      (ReadBack, After, true)
    )

    points.traverse_ {
      case (stage, boundary, installed) =>
        tempDirectory.use { directory =>
          for {
            fired <- Ref.of[IO, Boolean](false)
            failure = new IOException(s"reopen failure at $boundary $stage")
            hook = new FailOnce(stage, boundary, fired, failure)
            first <- resourceWith[IO](directory, limits, hook).use(_.putExact(bytes).attempt)
            _ <- IO.raiseUnless(first.left.exists(_.isInstanceOf[DurableWriteFailed]))(
              new AssertionError(s"fault was not surfaced at $boundary $stage: $first")
            )
            _ <- MetagraphBinaryCustodyStore.resource[IO](directory, limits).use { reopened =>
              for {
                usage <- reopened.usage
                read <-
                  if (installed) reopened.openExact(address).flatMap(reopened.readExact).map(_.some)
                  else IO.pure(none[Array[Byte]])
                _ <- IO.raiseUnless(
                  usage.items == (if (installed) 1 else 0) &&
                    usage.totalBytes == (if (installed) bytes.length.toLong else 0L) &&
                    read.forall(_.sameElements(bytes))
                )(
                  new AssertionError(
                    s"reopen state mismatch: stage=$stage boundary=$boundary installed=$installed " +
                      s"usage=${usage.items}/${usage.totalBytes} read=$read"
                  )
                )
              } yield ()
            }
          } yield ()
        }
    }.as(success)
  }

  private def artifactPath(directory: Path, contentAddress: LocalCustodyAddress): Path =
    directory.resolve(contentAddress.hex + ".binary")

  private def writeArtifact(directory: Path, bytes: Array[Byte]): IO[Unit] = {
    val contentAddress = LocalCustodyAddress.fromExactBytes(bytes)
    IO.blocking(Files.write(artifactPath(directory, contentAddress), bytes)).void
  }

  private def writeStaleTemporary(directory: Path, index: Int): IO[Unit] = {
    val contentAddress = LocalCustodyAddress.fromExactBytes(BigInt(index).toByteArray)
    val path = directory.resolve(s".${contentAddress.hex}.binary.crash-$index.tmp")
    IO.blocking(Files.write(path, Array(index.toByte))).void
  }

  private def directoryEntries(directory: Path): IO[Vector[Path]] =
    IO.blocking {
      val stream = Files.list(directory)
      try stream.iterator().asScala.toVector
      finally stream.close()
    }

  private def containsOwnershipChange(error: Throwable): Boolean = {
    val nested = Option(error.getCause).toList ++ error.getSuppressed.toList
    error.isInstanceOf[OwnershipIdentityChanged] || nested.exists(containsOwnershipChange)
  }

  private def tempDirectory: Resource[IO, Path] =
    Resource.make(IO.blocking(Files.createTempDirectory("metagraph-binary-custody")))(directory =>
      IO.blocking {
        val stream = Files.walk(directory)
        try stream.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
        finally stream.close()
      }.void
    )
}
