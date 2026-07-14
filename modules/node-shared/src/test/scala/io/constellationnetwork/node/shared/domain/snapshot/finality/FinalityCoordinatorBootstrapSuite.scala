package io.constellationnetwork.node.shared.domain.snapshot.finality

import java.nio.file.{Files => NioFiles, Path}

import cats.effect.{Deferred, IO, Resource}

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCodecFixtures.{finalityDomain, hash}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoordinatorBootstrapStatus.{
  LocalPublicationBound,
  RecoveryRequired => BootstrapRecoveryRequired
}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.storage.durable._

import fs2.io.file.Files
import scodec.bits.ByteVector
import weaver.SimpleIOSuite

object FinalityCoordinatorBootstrapSuite extends SimpleIOSuite {

  private val codecEra = MptImageCodecEra(hash(960))
  private val rootEra = MptImageRootEra(hash(961))
  private val limits = MptImageReadLimits
    .from(maxImageBytes = 1024L * 1024L, maxEntries = 1000, maxKeyBytes = 1024, maxValueBytes = 256 * 1024)
    .fold(throw _, identity)
  private val rootVerifier = new MptImageRootVerifier[IO] {
    val rootEra: MptImageRootEra = FinalityCoordinatorBootstrapSuite.rootEra
    def rebuild(entries: Vector[(Hex, ByteVector)]): IO[MptRoot] = IO.pure(MptRoot(hash(962)))
  }

  private final case class Stores(
    mpt: DurableMptImageStore[IO],
    finality: FinalityDurableStore[IO],
    finalityDirectory: Path
  )

  private def stores(
    root: Path,
    hook: DurableWriteHook[IO, FinalityDurableWriteArtifact] =
      DurableWriteHook.noop[IO, FinalityDurableWriteArtifact]
  ): Resource[IO, Stores] = {
    val mptDirectory = root.resolve("mpt")
    val finalityDirectory = root.resolve("finality")

    for {
      _ <- Resource.eval(IO.blocking(NioFiles.createDirectories(mptDirectory)))
      mpt <- DurableMptImageStore.resource[IO](mptDirectory, codecEra, rootVerifier, limits)
      finality <- FinalityDurableStore.make[IO](
        finalityDirectory,
        finalityDomain,
        FinalityDurableStoreLimits.default,
        DurableFileOps.nio[IO],
        hook
      )
    } yield Stores(mpt, finality, finalityDirectory)
  }

  test("an uninitialized or corrupt MPT publication performs no finality write") {
    Files[IO].tempDirectory.use { temporary =>
      val root = temporary.toNioPath
      stores(root).use { stores =>
        for {
          uninitialized <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality).attempt
          afterUninitialized <- stores.finality.coordinatorHead
          _ <- stores.mpt.initializeActivePublication(None)
          _ <- IO.blocking(NioFiles.write(root.resolve("mpt").resolve("active.publication"), Array[Byte](1, 2, 3))).void
          corrupt <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality).attempt
          afterCorrupt <- stores.finality.coordinatorHead
        } yield
          expect.all(
            uninitialized.isLeft,
            corrupt.isLeft,
            afterUninitialized.isEmpty,
            afterCorrupt.isEmpty,
            !NioFiles.exists(stores.finalityDirectory.resolve("coordinator.head"))
          )
      }
    }
  }

  test("fresh bootstrap exact-initializes once and an aligned restart stays locally publication-bound without an outbox") {
    Files[IO].tempDirectory.use { temporary =>
      stores(temporary.toNioPath).use { stores =>
        for {
          active <- stores.mpt.initializeActivePublication(None)
          first <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          firstHead <- stores.finality.coordinatorHead.map(_.get)
          firstOutbox <- stores.finality.effectOutboxHead(firstHead)
          second <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          secondHead <- stores.finality.coordinatorHead.map(_.get)
        } yield
          expect.all(
            first == LocalPublicationBound,
            second == LocalPublicationBound,
            firstHead == secondHead,
            firstHead.value.mode == CoordinatorMode.Running,
            firstHead.value.publication == active,
            firstHead.audit.mutation == CoordinatorMutationKind.Initialized,
            firstOutbox.isEmpty,
            !NioFiles.exists(stores.finalityDirectory.resolve("effects.head"))
          )
      }
    }
  }

  test("a full publication mismatch durably enters typed absorbing recovery and a later startup preserves it unchanged") {
    Files[IO].tempDirectory.use { temporary =>
      stores(temporary.toNioPath).use { stores =>
        for {
          initial <- stores.mpt.initializeActivePublication(None)
          _ <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          observed = MptActivePublication(MptPublicationRevision(initial.revision.value + 1L), None)
          _ <- stores.mpt.transitionActive(initial, observed)
          mismatch <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          recovered <- stores.finality.coordinatorHead.map(_.get)
          digest <- IO.fromEither(FinalityIdentity.publicationMismatchDigest(initial, observed))
          nextObserved = MptActivePublication(MptPublicationRevision(observed.revision.value + 1L), None)
          _ <- stores.mpt.transitionActive(observed, nextObserved)
          repeated <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          unchanged <- stores.finality.coordinatorHead.map(_.get)
        } yield {
          val pointer = recovered.value.mode match {
            case CoordinatorMode.RecoveryRequired(value) => Some(value)
            case CoordinatorMode.Running                 => None
          }
          val expectedStatus = pointer.map(BootstrapRecoveryRequired)

          expect.all(
            expectedStatus.contains(mismatch),
            expectedStatus.contains(repeated),
            recovered == unchanged,
            recovered.value.publication == initial,
            recovered.recovery.exists(
              _.reason == RecoveryReason.PublicationMismatch(observed, digest)
            ),
            !NioFiles.exists(stores.finalityDirectory.resolve("effects.head"))
          )
        }
      }
    }
  }

  test("the MPT publication lease blocks a concurrent transition through the complete finality-head initialization") {
    Files[IO].tempDirectory.use { temporary =>
      for {
        entered <- Deferred[IO, Unit]
        release <- Deferred[IO, Unit]
        attempted <- Deferred[IO, Unit]
        transitionDone <- Deferred[IO, Unit]
        hook = new DurableWriteHook[IO, FinalityDurableWriteArtifact] {
          def onEvent(event: DurableWriteEvent[FinalityDurableWriteArtifact]): IO[Unit] =
            event match {
              case DurableWriteEvent(
                    FinalityDurableWriteArtifact.CoordinatorHead(revision),
                    DurableWriteStage.Write,
                    DurableWriteBoundary.Before
                  ) if revision.value.value == 0L =>
                entered.complete(()).void >> release.get
              case _ => IO.unit
            }
        }
        result <- stores(temporary.toNioPath, hook).use { stores =>
          for {
            initial <- stores.mpt.initializeActivePublication(None)
            bootstrapFiber <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality).start
            _ <- entered.get
            target = MptActivePublication(MptPublicationRevision(initial.revision.value + 1L), None)
            transitionFiber <- (attempted.complete(()).void >> stores.mpt.transitionActive(initial, target))
              .guarantee(transitionDone.complete(()).void)
              .start
            _ <- attempted.get
            _ <- IO.cede
            completedWhileLeaseHeld <- transitionDone.tryGet
            _ <- release.complete(())
            bootstrap <- bootstrapFiber.joinWithNever
            _ <- transitionFiber.joinWithNever
            active <- stores.mpt.activePublication
            mismatch <- FinalityCoordinatorBootstrap.bootstrap(stores.mpt, stores.finality)
          } yield
            expect.all(
              completedWhileLeaseHeld.isEmpty,
              bootstrap == LocalPublicationBound,
              active.contains(target),
              mismatch match {
                case _: BootstrapRecoveryRequired => true
                case _                            => false
              }
            )
        }
      } yield result
    }
  }
}
