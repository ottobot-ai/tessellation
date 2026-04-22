package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot}
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs._
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs._
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}

/** Aggregator for the composite value-type `ImmutableCodec` instances stored in the global-state
  * MPT via `MptStore[F, GlobalStateKey]`.
  *
  * Leaf value types (Balance, TransactionReference, Hash, AllowSpendReference,
  * TokenLockReference, MetagraphSyncDataInfo, Proof, CurrencySnapshotInfo) already have
  * `ImmutableCodec` instances directly importable from their own codec modules — those are not
  * re-exported here.
  *
  * Composite types (`SortedSet[Signed[X]]`, `SortedSet[Y]`, `Signed[Z]`) are built from the
  * leaf codecs + collection / wrapper helpers. This file exposes them as implicits for the
  * call-site ergonomics of `store.get[SortedSet[Signed[AllowSpend]]](...)`.
  */
object GlobalStateMptCodecs {

  private val signedAllowSpendCodec = signedCodecFor(allowSpendCodec)
  private val signedTokenLockCodec = signedCodecFor(tokenLockCodec)
  private val signedCurrencySnapshotCodec = signedCodecFor(currencySnapshotCodec)
  private val signedCurrencyIncrementalSnapshotCodec = signedCodecFor(currencyIncrementalSnapshotCodec)

  implicit val signedAllowSpendSetCodec: ImmutableCodec[SortedSet[Signed[AllowSpend]]] =
    ImmutableCodec.fromScodecCodec(sortedSet(signedAllowSpendCodec))

  implicit val signedTokenLockSetCodec: ImmutableCodec[SortedSet[Signed[TokenLock]]] =
    ImmutableCodec.fromScodecCodec(sortedSet(signedTokenLockCodec))

  implicit val delegatedStakeRecordSetCodec: ImmutableCodec[SortedSet[DelegatedStakeRecord]] =
    ImmutableCodec.fromScodecCodec(sortedSet(delegatedStakeRecordCodec))

  implicit val pendingDelegatedStakeWithdrawalSetCodec: ImmutableCodec[SortedSet[PendingDelegatedStakeWithdrawal]] =
    ImmutableCodec.fromScodecCodec(sortedSet(pendingDelegatedStakeWithdrawalCodec))

  implicit val nodeCollateralRecordSetCodec: ImmutableCodec[SortedSet[NodeCollateralRecord]] =
    ImmutableCodec.fromScodecCodec(sortedSet(nodeCollateralRecordCodec))

  implicit val pendingNodeCollateralWithdrawalSetCodec: ImmutableCodec[SortedSet[PendingNodeCollateralWithdrawal]] =
    ImmutableCodec.fromScodecCodec(sortedSet(pendingNodeCollateralWithdrawalCodec))

  implicit val signedCurrencySnapshotImmutableCodec: ImmutableCodec[Signed[CurrencySnapshot]] =
    ImmutableCodec.fromScodecCodec(signedCurrencySnapshotCodec)

  implicit val signedCurrencyIncrementalSnapshotImmutableCodec: ImmutableCodec[Signed[CurrencyIncrementalSnapshot]] =
    ImmutableCodec.fromScodecCodec(signedCurrencyIncrementalSnapshotCodec)
}
