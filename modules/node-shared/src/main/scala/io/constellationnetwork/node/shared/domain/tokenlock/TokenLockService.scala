package io.constellationnetwork.node.shared.domain.tokenlock

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedSet
import scala.concurrent.duration._

import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.tokenlock.ContextualTokenLockValidator.{
  ContextualTokenLockValidationError,
  NonContextualValidationError
}
import io.constellationnetwork.schema.GlobalIncrementalSnapshot
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

trait TokenLockService[F[_]] {
  def offer(tokenLock: Hashed[TokenLock])(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualTokenLockValidationError], Hash]]
}

object TokenLockService {
  def make[F[_]: Async, P <: StateProof, S <: Snapshot, SI <: SnapshotInfo[P]](
    tokenLockStorage: TokenLockStorage[F],
    lastSnapshotStorage: LastSnapshotStorage[F, S, SI] with LatestBalances[F],
    tokenLockValidator: TokenLockValidator[F]
  ): TokenLockService[F] = new TokenLockService[F] {

    private def getBalanceAndTokenLocks(
      si: SI,
      address: io.constellationnetwork.schema.address.Address
    ): (Balance, SortedSet[Signed[TokenLock]]) =
      (
        si.balances.getOrElse(address, Balance.empty),
        si.getActiveTokenLocks.getOrElse(address, SortedSet.empty[Signed[TokenLock]])
      )

    def offer(
      tokenLock: Hashed[TokenLock]
    )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualTokenLockValidationError], Hash]] =
      for {
        lastGlobalEpochProgress <- lastSnapshotStorage.get.map {
          case Some(snapshot) =>
            snapshot.signed.value match {
              case cis: CurrencyIncrementalSnapshot =>
                cis.globalSyncView.map(_.epochProgress).getOrElse(EpochProgress.MinValue)
              case gis: GlobalIncrementalSnapshot =>
                gis.epochProgress
              case _ =>
                EpochProgress.MinValue
            }
          case None =>
            EpochProgress.MinValue
        }
        result <- tokenLockValidator
          .validate(tokenLock.signed, lastGlobalEpochProgress.some)
          .map(_.errorMap(NonContextualValidationError))
          .flatMap {
            case Valid(_) =>
              // Take the FIRST real (Some) combined snapshot, then run `tryPut` exactly ONCE so it
              // completes uninterrupted. The prior `.changes.switchMap(tryPut).head` re-ran tryPut on
              // every combined-snapshot emission (one per adopted gl0 ordinal) and CANCELLED the
              // in-flight tryPut when the next emission arrived; if tryPut couldn't finish within one
              // inter-snapshot interval it never resolved and `POST /token-locks` hung forever (observed:
              // the 2nd token-lock submission wedged cl1 indefinitely). Dropping the None prefix via
              // `collect` keeps the original e7a8daa8 fix (don't validate against a None→Balance.empty
              // substitution that yields InsufficientBalance{balance:0} for genesis-funded addresses).
              //
              // Bound the first-snapshot wait with a timeout (mirror of dag-l1 TransactionService.offer):
              // if an upstream issue keeps `lastSnapshotStorage` empty forever (no currency snapshot to
              // validate against), `.head` blocks forever and a hung POST masks the real cluster bug until
              // the test's outer timeout — surface it loudly after a bounded wait instead.
              val firstSnapshotTimeout = 90.seconds
              val waitForFirstSnapshot: F[Option[Either[NonEmptyList[ContextualTokenLockValidationError], Hash]]] =
                lastSnapshotStorage.getCombinedStream.collect {
                  case Some(value) => value
                }.head.compile.last.flatMap {
                  case Some((s, si)) =>
                    val (balance, activeTokenLocks) = getBalanceAndTokenLocks(si, tokenLock.source)
                    tokenLockStorage
                      .tryPut(tokenLock, s.ordinal, lastGlobalEpochProgress, balance, activeTokenLocks)
                      .map(_.some)
                  case None =>
                    none[Either[NonEmptyList[ContextualTokenLockValidationError], Hash]].pure[F]
                }
              Async[F]
                .timeoutTo(
                  waitForFirstSnapshot,
                  firstSnapshotTimeout,
                  new Exception(
                    s"Timed out after ${firstSnapshotTimeout.toSeconds}s waiting for the first currency snapshot — " +
                      s"the node has no currency snapshot to validate the token lock against (lastSnapshotStorage empty). " +
                      s"Upstream cause is typically gl0 rejecting all metagraph SC binaries (chain-link rejection or " +
                      s"pinnedGlobalSyncView mismatch); inspect gl0 logs for [SCAcceptance] warnings or " +
                      s"GlobalSnapshotStateChannelEventsProcessor errors."
                  ).raiseError[F, Option[Either[NonEmptyList[ContextualTokenLockValidationError], Hash]]]
                )
                .flatMap {
                  case Some(value) => value.pure[F]
                  case None =>
                    new Exception(s"Unexpected state, stream should always emit the first snapshot")
                      .raiseError[F, Either[NonEmptyList[ContextualTokenLockValidationError], Hash]]
                }

            case Invalid(e) =>
              e.toNonEmptyList.asLeft[Hash].leftWiden[NonEmptyList[ContextualTokenLockValidationError]].pure[F]
          }
      } yield result
  }
}
