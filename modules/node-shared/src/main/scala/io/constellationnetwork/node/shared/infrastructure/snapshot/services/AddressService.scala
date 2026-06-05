package io.constellationnetwork.node.shared.infrastructure.snapshot.services

import cats.effect.Async
import cats.syntax.flatMap._
import cats.syntax.functor._

import io.constellationnetwork.node.shared.config.types.AddressesConfig
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.snapshot.services.{AddressService, BalanceProof}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeRecord
import io.constellationnetwork.schema.snapshot.{Snapshot, SnapshotInfo}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed

import io.estatico.newtype.ops._

object AddressService {

  /** `getBalance` is overlay-aware on gl0 (`OverlayReader.pending`) — under `OverlayMode.MultiBranch` the chain's pending writes are
    * invisible to the underlying `MptStore` base until `finalizeBranch.foldIntoBase` lands, so HTTP reads via base lag by attestation
    * finality (~5s healthy) and unboundedly under finality stalls (#117 saw 2m15s gaps under multi-metagraph load). Routing through the
    * overlay at the chain's bestTip walks pending → falls through to base.
    *
    * Aggregate methods (`getTotalSupply`, `getWalletCount`, `getFilteredOutTotalSupply`, `getCirculatedSupply`,
    * `getFilteredOutCirculatedSupply`) intentionally remain on `SnapshotInfo` because they iterate all
    * balances/token-locks/delegated-stakes and cannot benefit from per-key MptStore access.
    */
  def make[F[_]: Async, S <: Snapshot, C <: SnapshotInfo[_]](
    addressCfg: AddressesConfig,
    snapshotStorage: SnapshotStorage[F, S, C],
    maybeReader: Option[GlobalStateReader[F]] = None
  ): AddressService[F, S] =
    new AddressService[F, S] {

      def getBalance(address: Address): F[Option[(Balance, SnapshotOrdinal)]] =
        maybeReader match {
          case Some(reader) =>
            snapshotStorage.head.flatMap {
              case Some((snapshot, _)) =>
                reader.getBalance(address).map { maybeBalance =>
                  Some((maybeBalance.getOrElse(Balance.empty), snapshot.value.ordinal))
                }
              case None => Async[F].pure(None)
            }
          case None =>
            snapshotStorage.head.map(_.map {
              case (snapshot, state) =>
                val balance = state.balances.getOrElse(address, Balance.empty)
                val ordinal = snapshot.value.ordinal
                (balance, ordinal)
            })
        }

      /** Builds the per-address balance MPT from the committed snapshot state (`state.balances`, NOT the overlay) and proves `address`'s
        * inclusion against its root — so the returned `root`/`proof` are self-consistent and TS-verifiable for the same `ordinal`. `None`
        * when there is no snapshot yet or `address` has no balance entry (no inclusion proof exists for an absent key → 404 at the route).
        */
      def getBalanceProof(address: Address): F[Option[BalanceProof]] =
        snapshotStorage.head.flatMap {
          case Some((snapshot, state)) =>
            BalanceMpt.buildProof[F](state.balances, address, snapshot.value.ordinal)
          case None => Async[F].pure(None)
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
