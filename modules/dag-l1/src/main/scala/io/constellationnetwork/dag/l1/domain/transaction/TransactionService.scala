package io.constellationnetwork.dag.l1.domain.transaction

import cats.Parallel
import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l1.domain.transaction.ContextualTransactionValidator.NonContextualValidationError
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.transaction.TransactionValidator
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
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
    transactionValidator: TransactionValidator[F],
    mptStore: MptStore[F, GlobalStateKey],
    shouldUseMptStore: Boolean
  ): TransactionService[F] = new TransactionService[F] {

    def useMptStore(
      transaction: Hashed[Transaction]
    )(implicit hasher: Hasher[F]) =
      // Mirror of the wait pattern in useGlobalSnapshotInfo (see comment there).
      // The MPT is populated by SnapshotProcessor's updateMptStorage step on
      // every download/align, but until the first snapshot lands, getBalance
      // returns None → Balance.empty → InsufficientBalance for genesis-funded
      // accounts that submit immediately after cluster-ready (currency.js,
      // token-locks). Wait for the first Some(snapshot) before reading.
      lastSnapshotStorage.getCombinedStream.collect {
        case Some((s, _)) => s.ordinal
      }.changes.switchMap { latestOrdinal =>
        Stream.eval(
          mptStore.getBalance(transaction.source).map(_.getOrElse(Balance.empty)).flatMap { balance =>
            transactionStorage.tryPut(transaction, latestOrdinal, balance)
          }
        )
      }.head.compile.last.flatMap {
        case Some(value) => value.pure[F]
        case None =>
          new Exception(s"Unexpected state, stream should always emit the first snapshot")
            .raiseError[F, Either[NonEmptyList[ContextualTransactionValidationError], Hash]]
      }

    def useGlobalSnapshotInfo(
      transaction: Hashed[Transaction]
    ) =
      // Wait for the first real snapshot rather than defaulting to
      // (MinValue, Balance.empty) — before the MPT-primary migration, node
      // startup was fast enough that the first stream emission was usually
      // Some(...) by the time transactions arrived; post-migration the bigger
      // accept() work pushes currency-snapshot bootstrap past fast clients
      // (e.g. spend/currency tests that submit immediately) so None leaks
      // through and every address reads balance=0 → InsufficientBalance for
      // genesis-funded accounts. Drop the None prefix and take the first
      // Some(...) — same semantics as before once any snapshot is available.
      lastSnapshotStorage.getCombinedStream.collect {
        case Some((s, si)) => (s.ordinal, si.balances.getOrElse(transaction.source, Balance.empty))
      }.changes.switchMap {
        case (latestOrdinal, balance) => Stream.eval(transactionStorage.tryPut(transaction, latestOrdinal, balance))
      }.head.compile.last.flatMap {
        case Some(value) => value.pure[F]
        case None =>
          new Exception(s"Unexpected state, stream should always emit the first snapshot")
            .raiseError[F, Either[NonEmptyList[ContextualTransactionValidationError], Hash]]
      }

    def offer(
      transaction: Hashed[Transaction]
    )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualTransactionValidationError], Hash]] =
      transactionValidator
        .validate(transaction.signed)
        .map(_.errorMap(NonContextualValidationError))
        .flatMap {
          case Valid(_) =>
            if (shouldUseMptStore) {
              useMptStore(transaction)
            } else {
              useGlobalSnapshotInfo(transaction)
            }
          case Invalid(e) =>
            e.toNonEmptyList.asLeft[Hash].leftWiden[NonEmptyList[ContextualTransactionValidationError]].pure[F]
        }
  }
}
