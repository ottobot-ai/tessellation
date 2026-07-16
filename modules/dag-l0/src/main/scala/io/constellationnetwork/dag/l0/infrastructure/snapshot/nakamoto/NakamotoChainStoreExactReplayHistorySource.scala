package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{
  ExactReplayHistoryFailure,
  ExactReplayHistoryPosition,
  ExactReplayHistorySource
}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

/** Dark structural adapter from the hash-aware GL0 chain store to exact replay-history bytes.
  *
  * This source proves only content identity and exact parent linkage. It deliberately ignores `StoredSnapshot.context`: disk fallback
  * synthesizes that value, and neither a stored context nor this adapter authenticates execution, an MPT image, Phase 2, or economic state.
  * No live caller is wired to this source.
  */
object NakamotoChainStoreExactReplayHistorySource {

  def make[F[_]: Async: HasherSelector](chainStore: NakamotoChainStoreAlgebra[F]): ExactReplayHistorySource[F] =
    new ExactReplayHistorySource[F] {

      import ExactReplayHistoryFailure._

      def fetchExact(
        position: ExactReplayHistoryPosition
      ): F[Either[ExactReplayHistoryFailure, Hashed[io.constellationnetwork.schema.GlobalIncrementalSnapshot]]] = {
        val walkPosition = ExactWalkPosition(position.hash, position.ordinal)

        chainStore
          .walkBackExact(walkPosition, position.ordinal, maxSteps = 1)
          .attempt
          .flatMap {
            case Left(error) =>
              Async[F].pure(Left(SourceReadFailed(position, s"exact-walk: ${renderThrowable(error)}")))
            case Right(Left(error: ExactWalkError.StorageReadFailed)) =>
              Async[F].pure(Left(SourceReadFailed(position, s"${error.lookup}: ${error.cause}")))
            case Right(Left(ExactWalkError.HashEraUnavailable(_, current, ordinal))) =>
              Async[F].pure(Left(SourceHashEraUnavailable(position, s"current=$current ordinal=$ordinal")))
            case Right(Left(ExactWalkError.ContentHashFailed(_, cause))) =>
              Async[F].pure(Left(SourceVerificationUnavailable(position, s"content-rehash-failed: $cause")))
            case Right(Left(error)) =>
              Async[F].pure(Left(classifyWalkError(position, error)))
            case Right(Right(ExactWalkResult.Incomplete(_, _, ExactWalkIncompleteReason.NotFound))) =>
              Async[F].pure(Left(ArtifactUnavailable(position)))
            case Right(Right(ExactWalkResult.Incomplete(_, _, ExactWalkIncompleteReason.SiblingAtOrdinal(foundHash)))) =>
              Async[F].pure(Left(SameOrdinalSibling(position, foundHash)))
            case Right(Right(ExactWalkResult.Complete(path))) =>
              path match {
                case Vector(link) if link.position == walkPosition =>
                  readAndRehash(chainStore, position, link)
                case _ =>
                  Async[F].pure(
                    Left(
                      SourceVerificationUnavailable(
                        position,
                        s"one-step exact walk returned ${path.size} links or a different position"
                      )
                    )
                  )
              }
          }
      }
    }

  private def readAndRehash[F[_]: Async: HasherSelector](
    chainStore: NakamotoChainStoreAlgebra[F],
    position: ExactReplayHistoryPosition,
    walked: ExactWalkLink
  ): F[Either[ExactReplayHistoryFailure, Hashed[io.constellationnetwork.schema.GlobalIncrementalSnapshot]]] = {
    import ExactReplayHistoryFailure._

    chainStore.getWithOrdinalFallback(position.hash, position.ordinal.value.value).attempt.flatMap {
      case Left(error) =>
        Async[F].pure(Left(SourceReadFailed(position, s"artifact-read: ${renderThrowable(error)}")))
      case Right(None) =>
        Async[F].pure(Left(ArtifactUnavailable(position)))
      case Right(Some(stored)) =>
        validateSecondRead(position, walked, stored) match {
          case Left(detail) => Async[F].pure(Left(SourceCorrupt(position, detail)))
          case Right(_)     => rehash(position, stored)
        }
    }
  }

  private def validateSecondRead(
    position: ExactReplayHistoryPosition,
    walked: ExactWalkLink,
    stored: StoredSnapshot
  ): Either[String, Unit] = {
    val signed = stored.signedSnapshot.value
    val root = signed.stateProof.mptRoot

    if (stored.hash =!= position.hash)
      Left(s"stored hash ${stored.hash.value} differs from requested hash ${position.hash.value}")
    else if (stored.ordinal =!= position.ordinal.value.value)
      Left(s"stored ordinal ${stored.ordinal} differs from requested ordinal ${position.ordinal.value.value}")
    else if (signed.ordinal =!= position.ordinal)
      Left(s"signed ordinal ${signed.ordinal.value.value} differs from requested ordinal ${position.ordinal.value.value}")
    else if (stored.parentHash =!= signed.lastSnapshotHash)
      Left(s"stored parent ${stored.parentHash.value} differs from signed parent ${signed.lastSnapshotHash.value}")
    else if (signed.lastSnapshotHash =!= walked.parentHash)
      Left(s"second-read parent ${signed.lastSnapshotHash.value} differs from walked parent ${walked.parentHash.value}")
    else if (!isCanonicalParent(position.ordinal, signed.lastSnapshotHash))
      Left(s"signed parent is not a canonical parent identity at ordinal ${position.ordinal.value.value}")
    else
      root match {
        case None => Right(())
        case Some(value) if !isCanonicalNonEmptyHash(value) =>
          Left("signed stateProof.mptRoot is reserved or non-canonical")
        case Some(_) => Right(())
      }
  }

  private def rehash[F[_]: Async: HasherSelector](
    position: ExactReplayHistoryPosition,
    stored: StoredSnapshot
  ): F[Either[ExactReplayHistoryFailure, Hashed[io.constellationnetwork.schema.GlobalIncrementalSnapshot]]] = {
    import ExactReplayHistoryFailure._

    Async[F].delay {
      val selector = HasherSelector[F]
      val current = selector.getCurrent
      val currentLogic = current.getLogic(position.ordinal)
      val ordinalLogic = selector.getForOrdinal(position.ordinal).getLogic(position.ordinal)
      (current, currentLogic, ordinalLogic)
    }.attempt.flatMap {
      case Left(error) =>
        Async[F].pure(Left(SourceVerificationUnavailable(position, s"hash-era-selection-failed: ${renderThrowable(error)}")))
      case Right((_, currentLogic, ordinalLogic)) if currentLogic != ordinalLogic =>
        Async[F].pure(
          Left(SourceHashEraUnavailable(position, s"current=$currentLogic ordinal=$ordinalLogic"))
        )
      case Right((current, _, _)) =>
        implicit val hasher: Hasher[F] = current
        stored.signedSnapshot.toHashed[F].attempt.map {
          case Left(error) =>
            Left(SourceVerificationUnavailable(position, s"content-rehash-failed: ${renderThrowable(error)}"))
          case Right(hashed) if !isCanonicalNonEmptyHash(hashed.hash) =>
            Left(SourceVerificationUnavailable(position, "content rehash produced a reserved or non-canonical hash"))
          case Right(hashed) if hashed.hash =!= position.hash =>
            Left(SourceCorrupt(position, s"content rehash ${hashed.hash.value} differs from requested ${position.hash.value}"))
          case Right(hashed) => Right(hashed)
        }
    }
  }

  private def isCanonicalNonEmptyHash(hash: Hash): Boolean = {
    val value = hash.value
    hash =!= Hash.empty && value.length == 64 && value.forall(c => (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))
  }

  private def isCanonicalParent(ordinal: SnapshotOrdinal, hash: Hash): Boolean =
    isCanonicalNonEmptyHash(hash) || (ordinal == SnapshotOrdinal.MinValue && hash === Hash.empty)

  private def renderWalkError(error: ExactWalkError): String =
    error.toString

  private def classifyWalkError(position: ExactReplayHistoryPosition, error: ExactWalkError): ExactReplayHistoryFailure = {
    import ExactReplayHistoryFailure._
    import ExactWalkError._
    import ExactWalkHashRole._

    error match {
      case InvalidMaxSteps(_) | TargetAboveStart(_, _) | RequiredStepsExceedLimit(_, _) | StepLimitExceeded(_, _) | ReservedSnapshotHash(
            _
          ) | ExactWalkError.PrematureChainRoot(_, _) | CycleDetected(_, _) =>
        SourceVerificationUnavailable(position, s"unexpected one-link exact-walk result: ${renderWalkError(error)}")
      case NonCanonicalSnapshotHash(_, Requested | Rehashed, _) =>
        SourceVerificationUnavailable(position, renderWalkError(error))
      case NonCanonicalSnapshotHash(_, Stored | StoredParent | SignedParent, _) | StoredHashMismatch(_, _) | StoredOrdinalMismatch(_, _) |
          SignedOrdinalMismatch(_, _) | StoredParentMismatch(_, _, _) | ExactHashContentMismatch(_, _) =>
        SourceCorrupt(position, renderWalkError(error))
      case HashEraUnavailable(_, current, ordinal) =>
        SourceHashEraUnavailable(position, s"current=$current ordinal=$ordinal")
      case ContentHashFailed(_, cause) =>
        SourceVerificationUnavailable(position, s"content-rehash-failed: $cause")
      case StorageReadFailed(_, lookup, cause) =>
        SourceReadFailed(position, s"$lookup: $cause")
    }
  }

  private def renderThrowable(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getName)
}
