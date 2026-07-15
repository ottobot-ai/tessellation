package io.constellationnetwork.validator

import cats.ApplicativeThrow

import scala.util.control.NoStackTrace

import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash

/** Active-protocol-era shape validation for global incremental snapshots.
  *
  * Historical-commitment SMT activation is dark. The field remains in the schema for a future explicit activation, but no snapshot in the
  * current era may claim an SMT root. This guard is intentionally independent of state-proof recreation so every ingress and egress path
  * can reject the forbidden shape before trusting or exposing it.
  */
object GlobalSnapshotActiveEraValidator {

  sealed abstract class Violation(message: String) extends RuntimeException(message) with NoStackTrace

  final case class HistoricalCommitmentSmtRootNotActive(ordinal: SnapshotOrdinal, smtRoot: Hash)
      extends Violation(s"Historical commitment SMT root is not active at ordinal=${ordinal.value.value}, root=${smtRoot.value}")

  def validate(snapshot: GlobalIncrementalSnapshot): Either[Violation, Unit] =
    snapshot.stateProof.smtRoot match {
      case None          => Right(())
      case Some(smtRoot) => Left(HistoricalCommitmentSmtRootNotActive(snapshot.ordinal, smtRoot))
    }

  def requireValid[F[_]: ApplicativeThrow](snapshot: GlobalIncrementalSnapshot): F[Unit] =
    ApplicativeThrow[F].fromEither(validate(snapshot))
}
