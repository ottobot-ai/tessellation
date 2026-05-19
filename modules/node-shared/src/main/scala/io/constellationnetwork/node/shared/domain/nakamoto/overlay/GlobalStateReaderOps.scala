package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutableCodec}
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

    def getActiveTokenLocks(address: Address): F[Option[SortedSet[Signed[TokenLock]]]] =
      reader.get[SortedSet[Signed[TokenLock]]](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, address))

    def getCurrencySnapshotInfo(metagraphAddress: Address): F[Option[CurrencySnapshotInfo]] =
      reader.get[CurrencySnapshotInfo](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshotInfo))

    /** Hash of the most-recently-accepted state-channel binary for a metagraph — the "binary chain" tip used by
      * `Signed[StateChannelSnapshotBinary].lastSnapshotHash` for parent-chain validation. A new incoming binary's `parentHash` MUST equal
      * this value when its parent is the metagraph's current tip (v1: no metagraph reorgs).
      */
    def getLastStateChannelSnapshotHash(metagraphAddress: Address): F[Option[Hash]] =
      reader.get[Hash](GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastStateChannelSnapshotHashes))

    /** The signed incremental snapshot at the metagraph's current tip — `.value.ordinal` gives the metagraph parent's ordinal that the
      * committee gate's KES period and eta derivation need. None when the metagraph is at its genesis (legacy `LastCurrencySnapshots`
      * partition) or the partition is unwritten (pre-bootstrap).
      */
    def getLastIncrementalCurrencySnapshot(metagraphAddress: Address): F[Option[Signed[CurrencyIncrementalSnapshot]]] =
      reader.get[Signed[CurrencyIncrementalSnapshot]](
        GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
      )

    /** The signed genesis (full) snapshot at the metagraph's current tip — populated only at the post-genesis pre-first-incremental window.
      * After the first incremental binary is accepted, the metagraph moves to the `LastIncrementalCurrencySnapshots` partition and this
      * returns `None`. Used by `MetagraphParentOrdinalResolver` to recover the genesis ordinal so the very-first-incremental binary's
      * parent-ordinal can be resolved against the genesis Left-side state (otherwise the gate would fail-close on every cluster with >1
      * metagraph the moment ml0 sends its post-genesis incremental binary).
      */
    def getLastCurrencySnapshot(metagraphAddress: Address): F[Option[Signed[CurrencySnapshot]]] =
      reader.get[Signed[CurrencySnapshot]](
        GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastCurrencySnapshots)
      )
  }
}
