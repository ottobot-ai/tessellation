package io.constellationnetwork.node.shared.infrastructure.snapshot.services

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.node.shared.config.types.AddressesConfig
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.node.shared.domain.snapshot.services.AddressService
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import io.estatico.newtype.ops._

object AddressService {

  /** Only `getBalance` is migrated to MptStore for per-address O(log n) lookup. Aggregate methods (`getTotalSupply`, `getWalletCount`,
    * `getFilteredOutTotalSupply`, `getCirculatedSupply`, `getFilteredOutCirculatedSupply`) intentionally remain on `SnapshotInfo` because
    * they iterate all balances/token-locks/delegated-stakes and cannot benefit from key-based MptStore access.
    */
  def make[F[_]: Async, S <: Snapshot, C <: SnapshotInfo[_]](
    addressCfg: AddressesConfig,
    snapshotStorage: SnapshotStorage[F, S, C],
    maybeMptStore: Option[MptStore[F, GlobalStateKey]] = None,
    maybeMptOverlay: Option[MptOverlay[F, GlobalStateKey]] = None,
    bestTipBranchF: F[Option[BranchId]] = null.asInstanceOf[F[Option[BranchId]]]
  ): AddressService[F, S] = {
    val effectiveBestTipBranchF: F[Option[BranchId]] =
      Option(bestTipBranchF).getOrElse(Async[F].pure(Option.empty[BranchId]))
    new AddressService[F, S] {

      // #117 read path: when an overlay is wired, query at the canonical bestTip so HTTP reads
      // reflect the chain's current view (pending → base fallthrough). Without overlay-aware
      // reads, balances lag by `finalizeBranch.foldIntoBase` cycles — bounded by attestation
      // finality (~5s healthy) but unbounded when finality stalls (#117 saw 2m15s gaps under
      // multi-metagraph load).
      def getBalance(address: Address): F[Option[(Balance, SnapshotOrdinal)]] =
        (maybeMptOverlay, maybeMptStore) match {
          case (Some(overlay), _) =>
            snapshotStorage.head.flatMap {
              case Some((snapshot, _)) =>
                effectiveBestTipBranchF.flatMap { maybeBranch =>
                  val branch = maybeBranch.getOrElse(BranchId.base)
                  overlay
                    .get[Balance](branch, GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))
                    .map { maybeBalance =>
                      Some((maybeBalance.getOrElse(Balance.empty), snapshot.value.ordinal))
                    }
                }
              case None => Async[F].pure(None)
            }
          case (None, Some(mptStore)) =>
            snapshotStorage.head.flatMap {
              case Some((snapshot, _)) =>
                mptStore.getBalance(address).map { maybeBalance =>
                  Some((maybeBalance.getOrElse(Balance.empty), snapshot.value.ordinal))
                }
              case None => Async[F].pure(None)
            }
          case (None, None) =>
            snapshotStorage.head.map(_.map {
              case (snapshot, state) =>
                val balance = state.balances.getOrElse(address, Balance.empty)
                val ordinal = snapshot.value.ordinal
                (balance, ordinal)
            })
        }

      def getTotalSupply: F[Option[(BigInt, SnapshotOrdinal)]] =
        snapshotStorage.head.map(_.map {
          case (snapshot, state) =>
            calculateTotalSupply(
              state.balances.values,
              state.getActiveTokenLocks.values.flatten,
              state.getActiveDelegatedStakes.values.flatten,
              snapshot.value.ordinal
            )
        })

      def getWalletCount: F[Option[(Int, SnapshotOrdinal)]] =
        snapshotStorage.head.map(_.map {
          case (snapshot, state) =>
            val balance = state.balances.size
            val ordinal = snapshot.value.ordinal

            (balance, ordinal)
        })

      def getFilteredOutTotalSupply: F[Option[(BigInt, SnapshotOrdinal)]] =
        snapshotStorage.head.map(_.map {
          case (snapshot, state) =>
            calculateTotalSupply(
              state.balances.filterNot { case (a, _) => addressCfg.locked.contains(a) }.values,
              state.getActiveTokenLocks.filterNot { case (a, _) => addressCfg.locked.contains(a) }.values.flatten,
              state.getActiveDelegatedStakes.filterNot { case (a, _) => addressCfg.locked.contains(a) }.values.flatten,
              snapshot.value.ordinal
            )
        })

      private def calculateTotalSupply(
        balances: Iterable[Balance],
        tokenLocks: Iterable[Signed[TokenLock]],
        delegatedStakes: Iterable[DelegatedStakeRecord],
        ordinal: SnapshotOrdinal
      ): (BigInt, SnapshotOrdinal) = {
        val empty = BigInt(Balance.empty.coerce.value)
        val supply = balances.foldLeft(empty) { (acc, b) =>
          acc + BigInt(b.coerce.value)
        } + tokenLocks.foldLeft(empty) { (acc, t) =>
          acc + BigInt(t.amount.coerce.value)
        } + delegatedStakes.foldLeft(empty) { (acc, s) =>
          acc + BigInt(s.rewards.coerce.value)
        }

        (supply, ordinal)
      }

      def getCirculatedSupply: F[Option[(BigInt, SnapshotOrdinal)]] =
        snapshotStorage.head.map(_.map {
          case (snapshot, state) =>
            calculateTotalSupply(
              state.balances.values,
              List.empty,
              List.empty,
              snapshot.value.ordinal
            )
        })

      def getFilteredOutCirculatedSupply: F[Option[(BigInt, SnapshotOrdinal)]] =
        snapshotStorage.head.map(_.map {
          case (snapshot, state) =>
            calculateTotalSupply(
              state.balances.filterNot { case (a, _) => addressCfg.locked.contains(a) }.values,
              List.empty,
              List.empty,
              snapshot.value.ordinal
            )
        })

    }
  }
}
