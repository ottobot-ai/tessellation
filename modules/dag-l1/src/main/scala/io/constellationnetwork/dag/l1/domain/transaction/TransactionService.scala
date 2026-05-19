package io.constellationnetwork.dag.l1.domain.transaction

import cats.Parallel
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l1.domain.transaction.ContextualTransactionValidator.NonContextualValidationError
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.transaction.TransactionValidator
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.transaction.Transaction
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hashed, Hasher}

import fs2.Stream
import io.circe.Json

import ContextualTransactionValidator.ContextualTransactionValidationError

trait TransactionService[F[_]] {
  def offer(transaction: Hashed[Transaction])(
    implicit hasher: Hasher[F]
  ): F[Either[NonEmptyList[ContextualTransactionValidationError], Hash]]
}

object TransactionService {

  def make[
    F[_]: Async: Parallel: JsonSerializer,
    P <: StateProof,
    S <: Snapshot,
    SI <: SnapshotInfo[P]
  ](
    transactionStorage: TransactionStorage[F],
    lastSnapshotStorage: LastSnapshotStorage[F, S, SI] with LatestBalances[F],
    transactionValidator: TransactionValidator[F]
  ): TransactionService[F] = new TransactionService[F] {

    def offer(
      transaction: Hashed[Transaction]
    )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualTransactionValidationError], Hash]] =
      transactionValidator
        .validate(transaction.signed)
        .map(_.errorMap(NonContextualValidationError))
        .flatMap {
          case Valid(_) =>
            // Wait for the first real snapshot rather than defaulting to
            // (MinValue, Balance.empty) — before the MPT-primary migration, node
            // startup was fast enough that the first stream emission was usually
            // Some(...) by the time transactions arrived; post-migration the bigger
            // accept() work pushes currency-snapshot bootstrap past fast clients
            // (e.g. spend/currency tests that submit immediately) so None leaks
            // through and every address reads balance=0 → InsufficientBalance for
            // genesis-funded accounts. Drop the None prefix and take the first
            // Some(...) — same semantics as before once any snapshot is available.
            //
            // Bound the wait with a timeout: when an upstream issue prevents currency
            // snapshots from ever being processed at this node (e.g. gl0 chain-link
            // rejection on all metagraph SC binaries due to a forcedGlobalSyncView
            // mismatch — see 2026-05-18 4-mg e2e diagnosis), `lastSnapshotStorage`
            // stays empty forever and `head` blocks forever. That manifests as a
            // hung HTTP POST /transactions request that holds the test client open
            // until the test framework's outer timeout, masking the real cluster
            // bug. Surface it as a `503 Service Unavailable`-style error after a
            // bounded wait so the test fails loudly with the actual root-cause
            // rather than silently hanging the e2e for hours.
            val firstSnapshotTimeout = 30.seconds
            val waitForFirstSnapshot: F[Option[Either[NonEmptyList[ContextualTransactionValidationError], Hash]]] =
              lastSnapshotStorage.getCombinedStream.collect {
                case Some((s, si)) => (s.ordinal, si.balances.getOrElse(transaction.source, Balance.empty))
              }.changes.switchMap {
                case (latestOrdinal, balance) =>
                  Stream.eval(transactionStorage.tryPut(transaction, latestOrdinal, balance))
              }.head.compile.last
            Async[F]
              .timeoutTo(
                waitForFirstSnapshot,
                firstSnapshotTimeout,
                new Exception(
                  s"Timed out after ${firstSnapshotTimeout.toSeconds}s waiting for the first currency snapshot — " +
                    s"the node has no currency snapshot to validate against (lastSnapshotStorage empty). " +
                    s"Upstream cause is typically gl0 rejecting all metagraph SC binaries (chain-link rejection or " +
                    s"forcedGlobalSyncView mismatch); inspect gl0 logs for [SCAcceptance] warnings or " +
                    s"GlobalSnapshotStateChannelEventsProcessor errors."
                ).raiseError[F, Option[Either[NonEmptyList[ContextualTransactionValidationError], Hash]]]
              )
              .flatMap {
                case Some(value) => value.pure[F]
                case None =>
                  new Exception(s"Unexpected state, stream should always emit the first snapshot")
                    .raiseError[F, Either[NonEmptyList[ContextualTransactionValidationError], Hash]]
              }
          case Invalid(e) =>
            e.toNonEmptyList.asLeft[Hash].leftWiden[NonEmptyList[ContextualTransactionValidationError]].pure[F]
        }
  }
}
