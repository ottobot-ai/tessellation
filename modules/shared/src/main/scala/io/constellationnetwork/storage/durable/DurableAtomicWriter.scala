package io.constellationnetwork.storage.durable

import java.nio.file.{AtomicMoveNotSupportedException, Path}

import cats.effect.{Async, Resource}
import cats.syntax.all._

sealed trait DurableWriteBoundary

object DurableWriteBoundary {
  case object Before extends DurableWriteBoundary
  case object After extends DurableWriteBoundary
}

sealed trait DurableWriteStage

object DurableWriteStage {
  case object Write extends DurableWriteStage
  case object ForceFile extends DurableWriteStage
  case object AtomicMove extends DurableWriteStage
  case object ForceDirectory extends DurableWriteStage
  case object ReadBack extends DurableWriteStage

  val ordered: List[DurableWriteStage] = List(Write, ForceFile, AtomicMove, ForceDirectory, ReadBack)
}

final case class DurableWriteEvent[Artifact](artifact: Artifact, stage: DurableWriteStage, boundary: DurableWriteBoundary)

/** Test and crash-oracle seam around every durability boundary.
  *
  * Production uses [[DurableWriteHook.noop]]. Tests may fail, pause, cancel, or terminate a process at either side of a stage without
  * replacing the filesystem implementation.
  */
trait DurableWriteHook[F[_], Artifact] {
  def onEvent(event: DurableWriteEvent[Artifact]): F[Unit]
}

object DurableWriteHook {
  def noop[F[_]: Async, Artifact]: DurableWriteHook[F, Artifact] = new DurableWriteHook[F, Artifact] {
    def onEvent(event: DurableWriteEvent[Artifact]): F[Unit] = Async[F].unit
  }
}

/** Forced atomic replacement with verified readback.
  *
  * The target becomes visible only through an atomic replace. The temporary file is always offered for deletion and no non-atomic move is
  * attempted. Successful return means temp bytes and the containing directory were forced and the caller's target readback completed.
  * Preparation is cancelable and bracketed cleanup removes the temporary file. Once atomic replacement starts, replacement through verified
  * readback is uncancelable, so cancellation cannot be observed after the target changed but before its durability was checked.
  * This is intentionally not an immutable create-or-verify operation: callers which require write-once content addressing must use a
  * separate create-new primitive which never replaces an existing target.
  */
final class DurableAtomicWriter[F[_]: Async](fileOps: DurableFileOps[F]) {
  import DurableWriteBoundary._
  import DurableWriteStage._

  def replace[A, Artifact](
    target: Path,
    bytes: Array[Byte],
    artifact: Artifact,
    hook: DurableWriteHook[F, Artifact]
  )(readBack: Path => F[A]): F[A] =
    Async[F]
      .fromOption(Option(target.getParent), DurableFileError.TargetHasNoParent(target))
      .flatMap { parent =>
        Resource
          .make(fileOps.createTempFile(parent, s".${target.getFileName.toString}.", ".tmp"))(fileOps.deleteIfExists)
          .use { temporary =>
            val prepare =
              at(artifact, Write, hook)(fileOps.write(temporary, bytes)) >>
                at(artifact, ForceFile, hook)(fileOps.forceFile(temporary))

            val commit = Async[F].uncancelable { _ =>
              for {
                _ <- at(artifact, AtomicMove, hook)(
                  fileOps.atomicMoveReplace(temporary, target).adaptError {
                    case error: AtomicMoveNotSupportedException => DurableFileError.AtomicMoveRequired(target, error)
                  }
                )
                _ <- at(artifact, ForceDirectory, hook)(fileOps.forceDirectory(parent))
                verified <- at(artifact, ReadBack, hook)(readBack(target))
              } yield verified
            }

            prepare >> commit
          }
      }

  private def at[A, Artifact](
    artifact: Artifact,
    stage: DurableWriteStage,
    hook: DurableWriteHook[F, Artifact]
  )(operation: F[A]): F[A] =
    hook.onEvent(DurableWriteEvent(artifact, stage, Before)) >>
      operation.flatTap(_ => hook.onEvent(DurableWriteEvent(artifact, stage, After)))
}
