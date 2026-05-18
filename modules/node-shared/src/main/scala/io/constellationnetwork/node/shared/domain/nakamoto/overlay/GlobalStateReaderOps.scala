package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

/** Typed read accessors over `GlobalStateReader[F]`, mirroring the `MptStoreReadOps` extension defined in `GlobalStateConverter` for
  * `MptStore[F, GlobalStateKey]`. Lets call sites that already write `mptStore.getBalance(addr)` switch to `reader.getBalance(addr)` with
  * no change to body code — the only difference is that `reader` resolves via the branch-aware path (under `OverlayMode.MultiBranch`
  * `pending` reader picks up the chain's pending writes, `finalized` reader reads base directly).
  *
  * Only the accessors actually used by the Phase 2 migrated sites are mirrored here — `getBalance`, `getDelegatedStakes`,
  * `getDelegatedStakeWithdrawals`, `getNodeCollaterals`, `getNodeCollateralWithdrawals`, `getCurrencySnapshotInfo`. Adding more on demand
  * is a one-line follow.
  */
object GlobalStateReaderOps {

  implicit class GlobalStateReaderTypedOps[F[_]: Async](val reader: GlobalStateReader[F]) {

    def getBalance(address: Address): F[Option[Balance]] =
      reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, address))

    def getDelegatedStakes(address: Address): F[Option[SortedSet[DelegatedStakeRecord]]] =
      reader.get[SortedSet[DelegatedStakeRecord]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveDelegatedStakes, address))

    def getDelegatedStakeWithdrawals(address: Address): F[Option[SortedSet[PendingDelegatedStakeWithdrawal]]] =
      reader.get[SortedSet[PendingDelegatedStakeWithdrawal]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.DelegatedStakesWithdrawals, address)
      )

    def getNodeCollaterals(address: Address): F[Option[SortedSet[NodeCollateralRecord]]] =
      reader.get[SortedSet[NodeCollateralRecord]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveNodeCollaterals, address))

    def getNodeCollateralWithdrawals(address: Address): F[Option[SortedSet[PendingNodeCollateralWithdrawal]]] =
      reader.get[SortedSet[PendingNodeCollateralWithdrawal]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.NodeCollateralWithdrawals, address)
      )

    def getCurrencySnapshotInfo(metagraphAddress: Address): F[Option[CurrencySnapshotInfo]] =
      reader.get[CurrencySnapshotInfo](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshotInfo))
  }
}
