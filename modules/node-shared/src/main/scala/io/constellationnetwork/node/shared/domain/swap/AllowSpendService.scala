package io.constellationnetwork.node.shared.domain.swap

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.node.shared.domain.collateral.LatestBalances
import io.constellationnetwork.node.shared.domain.snapshot.storage.LastSnapshotStorage
import io.constellationnetwork.node.shared.domain.swap.ContextualAllowSpendValidator.{
  ContextualAllowSpendValidationError,
  NonContextualValidationError
}
import io.constellationnetwork.schema.GlobalIncrementalSnapshot
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo, StateProof}
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hashed, Hasher}

import fs2.Stream

trait AllowSpendService[F[_]] {
  def offer(allowSpend: Hashed[AllowSpend])(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualAllowSpendValidationError], Hash]]
}

object AllowSpendService {
  def make[F[_]: Async, P <: StateProof, S <: Snapshot, SI <: SnapshotInfo[P]](
    allowSpendStorage: AllowSpendStorage[F],
    lastSnapshotStorage: LastSnapshotStorage[F, S, SI] with LatestBalances[F],
    allowSpendValidator: AllowSpendValidator[F],
    maybeMptStore: Option[MptStore[F, GlobalStateKey]] = None
  ): AllowSpendService[F] = new AllowSpendService[F] {

    import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._

    private def getBalance(si: SI, address: io.constellationnetwork.schema.address.Address): F[Balance] =
      maybeMptStore match {
        case Some(mptStore) => mptStore.getBalance(address).map(_.getOrElse(Balance.empty))
        case None           => si.balances.getOrElse(address, Balance.empty).pure[F]
      }

    def offer(
      allowSpend: Hashed[AllowSpend]
    )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[ContextualAllowSpendValidationError], Hash]] =
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
        result <- allowSpendValidator
          .validate(allowSpend.signed, lastGlobalEpochProgress.some)
          .map(_.errorMap(NonContextualValidationError))
          .flatMap {
            case Valid(_) =>
              // Wait for the first real snapshot rather than defaulting to
              // (MinValue, Balance.empty). Pre-MPT-primary migration node startup
              // was fast enough that the stream's first emission was usually
              // Some(...) by the time submissions arrived; post-migration the
              // bigger accept() pipeline lands cl1's first currency snapshot
              // later than fast clients (e.g. allow-spends test posting right
              // after cluster-ready), so a None first emission would be
              // substituted to Balance.empty and `.head` would consume that as
              // the answer — producing InsufficientBalance{balance:0} for
              // genesis-funded addresses. Mirror of the e7a8daa8 fix in
              // dag-l1 TransactionService: drop None emissions via `collect`,
              // then run getBalance on the first Some.
              lastSnapshotStorage.getCombinedStream.collect {
                case Some(value) => value
              }.evalMap {
                case (s, si) =>
                  getBalance(si, allowSpend.source).map(balance => (s.ordinal, balance))
              }.changes.switchMap {
                case (latestOrdinal, balance) =>
                  Stream.eval(allowSpendStorage.tryPut(allowSpend, latestOrdinal, lastGlobalEpochProgress, balance))
              }.head.compile.last.flatMap {
                case Some(value) => value.pure[F]
                case None =>
                  new Exception(s"Unexpected state, stream should always emit the first snapshot")
                    .raiseError[F, Either[NonEmptyList[ContextualAllowSpendValidationError], Hash]]
              }

            case Invalid(e) =>
              e.toNonEmptyList.asLeft[Hash].leftWiden[NonEmptyList[ContextualAllowSpendValidationError]].pure[F]
          }
      } yield result
  }
}
