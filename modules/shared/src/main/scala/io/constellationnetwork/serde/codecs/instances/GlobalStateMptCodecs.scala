package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedSet

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.kes.KesRegistrationCert.KesRegistrationRecord
import io.constellationnetwork.schema.mpt.{AllowSpendExpiryKey, NodeCollateralWithdrawalExpiryKey, TokenLockExpiryKey}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.sharding.{ShardId, ShardNonParticipationCounter}
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{etaPeriodCodec => etaPeriodScodec}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}
import io.constellationnetwork.serde.codecs.instances.UpdateNodeParametersCodec.updateNodeParametersCodec

import scodec.Codec
import scodec.codecs._
import shapeless.{::, HNil}

/** Aggregator for the composite value-type `ImmutableCodec` instances stored in the global-state MPT via `MptStore[F, GlobalStateKey]`.
  *
  * Leaf value types (Balance, TransactionReference, Hash, AllowSpendReference, TokenLockReference, MetagraphSyncDataInfo, Proof,
  * CurrencySnapshotInfo) already have `ImmutableCodec` instances directly importable from their own codec modules — those are not
  * re-exported here.
  *
  * Composite types (`SortedSet[Signed[X]]`, `SortedSet[Y]`, `Signed[Z]`) are built from the leaf codecs + collection / wrapper helpers.
  * This file exposes them as implicits for the call-site ergonomics of `store.get[SortedSet[Signed[AllowSpend]]](...)`.
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

  /** Per-operator history-set encoding for the `KesRegistrationCerts` MPT partition (§1.2 Slice 10 / #179). One MPT entry per `peerId`
    * carrying the `SortedSet[KesRegistrationRecord]` of every accepted runtime cert for that operator. Ordering inside the set is
    * `(acceptedAt, ordinal)` from `KesRegistrationRecord.ordering`, so the head is the earliest cert; lookups for "latest" iterate in
    * reverse, which is fine for the bounded chains expected here (kept short by §1.2 rotation cadence).
    */
  implicit val kesRegistrationRecordSetCodec: ImmutableCodec[SortedSet[KesRegistrationRecord]] =
    ImmutableCodec.fromScodecCodec(sortedSet(kesRegistrationRecordCodec))

  implicit val signedCurrencySnapshotImmutableCodec: ImmutableCodec[Signed[CurrencySnapshot]] =
    ImmutableCodec.fromScodecCodec(signedCurrencySnapshotCodec)

  implicit val signedCurrencyIncrementalSnapshotImmutableCodec: ImmutableCodec[Signed[CurrencyIncrementalSnapshot]] =
    ImmutableCodec.fromScodecCodec(signedCurrencyIncrementalSnapshotCodec)

  private val signedUpdateNodeParametersCodec = signedCodecFor(updateNodeParametersCodec)
  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]

  private val unpRecordCodec: Codec[(Signed[UpdateNodeParameters], SnapshotOrdinal)] =
    (signedUpdateNodeParametersCodec :: snapshotOrdinalCodec).xmap[(Signed[UpdateNodeParameters], SnapshotOrdinal)](
      { case s :: o :: HNil => (s, o) },
      { case (s, o) => s :: o :: HNil }
    )

  implicit val unpRecordImmutableCodec: ImmutableCodec[(Signed[UpdateNodeParameters], SnapshotOrdinal)] =
    ImmutableCodec.fromScodecCodec(unpRecordCodec)

  // ---- System-index key codecs ---------------------------------------------

  private val allowSpendExpiryKeyCodec: Codec[AllowSpendExpiryKey] =
    (option(addressCodec) :: addressCodec :: hashCodec).xmap[AllowSpendExpiryKey](
      { case mid :: addr :: h :: HNil => AllowSpendExpiryKey(mid, addr, h) },
      k => k.metagraphId :: k.address :: k.hash :: HNil
    )

  implicit val allowSpendExpiryKeySetImmutableCodec: ImmutableCodec[SortedSet[AllowSpendExpiryKey]] =
    ImmutableCodec.fromScodecCodec(sortedSet(allowSpendExpiryKeyCodec))

  private val tokenLockExpiryKeyCodec: Codec[TokenLockExpiryKey] =
    (addressCodec :: hashCodec).xmap[TokenLockExpiryKey](
      { case addr :: h :: HNil => TokenLockExpiryKey(addr, h) },
      k => k.address :: k.hash :: HNil
    )

  implicit val tokenLockExpiryKeySetImmutableCodec: ImmutableCodec[SortedSet[TokenLockExpiryKey]] =
    ImmutableCodec.fromScodecCodec(sortedSet(tokenLockExpiryKeyCodec))

  private val nodeCollateralWithdrawalExpiryKeyCodec: Codec[NodeCollateralWithdrawalExpiryKey] =
    (addressCodec :: hashCodec).xmap[NodeCollateralWithdrawalExpiryKey](
      { case addr :: h :: HNil => NodeCollateralWithdrawalExpiryKey(addr, h) },
      k => k.address :: k.hash :: HNil
    )

  implicit val nodeCollateralWithdrawalExpiryKeySetImmutableCodec: ImmutableCodec[SortedSet[NodeCollateralWithdrawalExpiryKey]] =
    ImmutableCodec.fromScodecCodec(sortedSet(nodeCollateralWithdrawalExpiryKeyCodec))

  // ---- ActiveAddressIndex value codec --------------------------------------

  /** Codec for the `ActiveAddressIndex` partition's value type. One MPT entry per indexed `GlobalStateFieldId` holds the full
    * `SortedSet[Address]` of every address with an active record in that field — used by manager `materializeXFromMpt` paths to recover
    * keys when the underlying value type doesn't carry the address (refs, balances).
    */
  implicit val addressSetImmutableCodec: ImmutableCodec[SortedSet[Address]] =
    ImmutableCodec.fromScodecCodec(sortedSet(addressCodec))

  /** Codec for the address-pair index partition (used by `tokenLockBalances` whose key is `(metagraphAddr, holderAddr)` and whose `Balance`
    * value carries no source addresses). The set ordering uses `Address`'s lexicographic ordering on the pair.
    */
  private val addressPairCodec: Codec[(Address, Address)] =
    (addressCodec :: addressCodec).xmap[(Address, Address)](
      { case a :: b :: HNil => (a, b) },
      { case (a, b) => a :: b :: HNil }
    )

  implicit val addressPairSetImmutableCodec: ImmutableCodec[SortedSet[(Address, Address)]] =
    ImmutableCodec.fromScodecCodec(sortedSet(addressPairCodec))

  // ---- Slice 17 — Shard non-participation counter --------------------------

  /** Wire shape for `ShardId` — `int32` of the underlying refined `NonNegInt`. The refinement is enforced on decode through
    * `ShardId.unsafeApply` because the codec already constrains the source to a non-negative producer (`ShardId.value.value`). Production
    * decodes always come from prior encodes, so the round-trip is the discipline; should a corrupt byte stream surface a negative value
    * `unsafeApply` will throw at decode time, which is the desired loud failure for consensus-bytes.
    */
  private val shardIdCodec: Codec[ShardId] =
    int32.xmap[ShardId](ShardId.unsafeApply, _.value.value)

  /** Scodec for the `ShardNonParticipationCounter` record. Field order mirrors the case-class declaration order; changing the order would
    * shift the on-disk bytes and is a hard-fork move. Per-(shard, peer, epoch) tuples plus four `Long` tallies — total wire size is ~58
    * bytes for typical PeerId hex lengths, well inside the MPT entry budget.
    */
  private val shardNonParticipationCounterCodec: Codec[ShardNonParticipationCounter] = {
    val _ = (etaPeriodScodec, peerIdCodec) // bind context-bound implicits
    (shardIdCodec :: peerIdCodec :: etaPeriodScodec :: int64 :: int64 :: int64 :: int64).xmap[ShardNonParticipationCounter](
      {
        case sid :: pid :: ep :: missedSlots :: missedAtts :: totalSlots :: totalCps :: HNil =>
          ShardNonParticipationCounter(sid, pid, ep, missedSlots, missedAtts, totalSlots, totalCps)
      },
      c =>
        c.shardId :: c.peerId :: c.epoch :: c.missedSlotsAsLeader :: c.missedAttestationWindows ::
          c.totalSlotsAsLeader :: c.totalCheckpointsReceived :: HNil
    )
  }

  /** Bound here so MPT reads/writes for the shard non-participation partition (`GlobalStateFieldId.ShardNonParticipation`) resolve
    * `store.get[ShardNonParticipationCounter](...)` and `store.insert[ShardNonParticipationCounter](...)` without each call site importing
    * `shardNonParticipationCounterCodec` explicitly.
    */
  implicit val shardNonParticipationCounterImmutableCodec: ImmutableCodec[ShardNonParticipationCounter] =
    ImmutableCodec.fromScodecCodec(shardNonParticipationCounterCodec)

  /** `ShardId` codec re-export so `EpochBucket`-style consumers that need a typed key codec (e.g. cross-shard sortition tests) can pick up
    * the same wire bytes the counter partition writes use. Public-but-explicit avoids implicit pollution at unrelated call sites.
    */
  val shardIdScodec: Codec[ShardId] = shardIdCodec
  locally { val _ = shardIdScodec }
}
