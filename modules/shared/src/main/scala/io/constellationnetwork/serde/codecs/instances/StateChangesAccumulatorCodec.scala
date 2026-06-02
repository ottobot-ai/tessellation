package io.constellationnetwork.serde.codecs.instances

import cats.Order

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.swap.AllowSpend
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.EitherCodec.either
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SetCodec.set
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{codec => allowSpendRefCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.{proofCodec => merkleProofCodec}
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{codec => metagraphSyncCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.{priceRecordCodec, tokenPairCodec}
import io.constellationnetwork.serde.codecs.instances.SignatureCodecs.idCodec
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{etaPeriodCodec, historicalCodec => historicalStakeSnapshotCodec}
import io.constellationnetwork.serde.codecs.instances.SystemIndexDeltaCodecs._
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{codec => tokenLockRefCodec}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{codec => transactionReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.UpdateNodeParametersCodec.{updateNodeParametersCodec => unpCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `GlobalStateConverter.StateChangesAccumulator` — the typed per-ordinal global-state delta.
  *
  * This is the wire form of the gl0→currency-l0 state-diff (the adopt-and-verify follow path that replaces full re-execution). It is
  * NOT a signing/hashing preimage — the cryptographic anchor is the snapshot's `mptRoot`, recomputed independently by applying this
  * delta. The encoding is nonetheless an explicit, hand-written spec (no auto-derivation): the wire layout follows the case-class field
  * order exactly (1..17 the GSI-shaped partition deltas, 18..20 the system expiry-index deltas, 21..27 the removed-key sets, 28..29 the
  * historical-stake delta + removed periods). `Set` fields are sorted on encode for determinism (see [[SetCodec]]).
  */
object StateChangesAccumulatorCodec {

  // ---- field 4: lastCurrencySnapshots Either (byte-identical to GlobalSnapshotInfoCodec) ----
  private val signedCurrencySnapshotCodec: Codec[Signed[CurrencySnapshot]] =
    signedCodecFor(currencySnapshotCodec)
  private val signedCurrencyIncrementalSnapshotCodec: Codec[Signed[CurrencyIncrementalSnapshot]] =
    signedCodecFor(currencyIncrementalSnapshotCodec)
  private val incrementalWithInfoTupleCodec: Codec[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)] =
    (signedCurrencyIncrementalSnapshotCodec :: currencySnapshotInfoCodec).xmap(
      { case sig :: info :: HNil => (sig, info) },
      t => t._1 :: t._2 :: HNil
    )
  private val currencySnapshotEitherCodec
    : Codec[Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
    either(signedCurrencySnapshotCodec, incrementalWithInfoTupleCodec)

  // ---- explicit Orders for the tuple-keyed removed-key sets (no auto-derivation) ----
  private implicit val optionAddressOrder: Order[Option[Address]] = Order.from {
    case (None, None)       => 0
    case (None, _)          => -1
    case (_, None)          => 1
    case (Some(a), Some(b)) => Order[Address].compare(a, b)
  }
  private implicit val optAddrAddrOrder: Order[(Option[Address], Address)] =
    Order.whenEqual(Order.by(_._1), Order.by(_._2))
  private implicit val addrAddrOrder: Order[(Address, Address)] =
    Order.whenEqual(Order.by(_._1), Order.by(_._2))

  private val optAddrAddrTupleCodec: Codec[(Option[Address], Address)] =
    (option(addressCodec) :: addressCodec).xmap({ case a :: b :: HNil => (a, b) }, t => t._1 :: t._2 :: HNil)
  private val addrAddrTupleCodec: Codec[(Address, Address)] =
    (addressCodec :: addressCodec).xmap({ case a :: b :: HNil => (a, b) }, t => t._1 :: t._2 :: HNil)

  // ---- signed inner codecs ----
  private val signedAllowSpendCodec: Codec[Signed[AllowSpend]] = signedCodecFor(allowSpendCodec)
  private val signedTokenLockCodec: Codec[Signed[TokenLock]] = signedCodecFor(tokenLockCodec)
  private val signedUnpCodec: Codec[Signed[UpdateNodeParameters]] = signedCodecFor(unpCodec)
  private val unpWithOrdinalTupleCodec: Codec[(Signed[UpdateNodeParameters], SnapshotOrdinal)] =
    (signedUnpCodec :: Codec[SnapshotOrdinal]).xmap({ case s :: o :: HNil => (s, o) }, t => t._1 :: t._2 :: HNil)

  // ---- per-field codecs (order matches StateChangesAccumulator declaration) ----
  private val f1 = sortedMap(addressCodec, hashCodec)
  private val f2 = sortedMap(addressCodec, transactionReferenceCodec)
  private val f3 = sortedMap(addressCodec, Codec[Balance])
  private val f4 = sortedMap(addressCodec, currencySnapshotEitherCodec)
  private val f5 = sortedMap(addressCodec, merkleProofCodec)
  private val f6 = sortedMap(option(addressCodec), sortedMap(addressCodec, sortedSet(signedAllowSpendCodec)))
  private val f7 = sortedMap(addressCodec, sortedSet(signedTokenLockCodec))
  private val f8 = sortedMap(addressCodec, sortedMap(addressCodec, Codec[Balance]))
  private val f9 = sortedMap(addressCodec, allowSpendRefCodec)
  private val f10 = sortedMap(addressCodec, tokenLockRefCodec)
  private val f11 = sortedMap(addressCodec, sortedSet(delegatedStakeRecordCodec))
  private val f12 = sortedMap(addressCodec, sortedSet(pendingDelegatedStakeWithdrawalCodec))
  private val f13 = sortedMap(addressCodec, sortedSet(nodeCollateralRecordCodec))
  private val f14 = sortedMap(addressCodec, sortedSet(pendingNodeCollateralWithdrawalCodec))
  private val f15 = sortedMap(addressCodec, metagraphSyncCodec)
  private val f16 = sortedMap(idCodec, unpWithOrdinalTupleCodec)
  private val f17 = sortedMap(tokenPairCodec, priceRecordCodec)
  private val f18 = systemIndexDeltaCodec(allowSpendExpiryKeyCodec)
  private val f19 = systemIndexDeltaCodec(tokenLockExpiryKeyCodec)
  private val f20 = systemIndexDeltaCodec(nodeCollateralWithdrawalExpiryKeyCodec)
  private val f21 = set(optAddrAddrTupleCodec)
  private val f22 = set(addressCodec)
  private val f23 = set(addrAddrTupleCodec)
  private val f24 = set(addressCodec)
  private val f25 = set(addressCodec)
  private val f26 = set(addressCodec)
  private val f27 = set(addressCodec)
  private val f28 = sortedMap(etaPeriodCodec, historicalStakeSnapshotCodec)
  private val f29 = set(etaPeriodCodec)

  implicit val codec: Codec[StateChangesAccumulator] =
    (f1 :: f2 :: f3 :: f4 :: f5 :: f6 :: f7 :: f8 :: f9 :: f10 ::
      f11 :: f12 :: f13 :: f14 :: f15 :: f16 :: f17 :: f18 :: f19 :: f20 ::
      f21 :: f22 :: f23 :: f24 :: f25 :: f26 :: f27 :: f28 :: f29)
      .xmap[StateChangesAccumulator](
        {
          case lsch :: ltx :: bal :: lcs :: lcsp :: aas :: atl :: tlb :: lasr :: ltlr ::
              ads :: dsw :: anc :: ncw :: msd :: unp :: ps :: asei :: tlei :: ncwei ::
              rask :: rtlk :: rtlbk :: rdsk :: rdswk :: rnck :: rncwk :: hss :: rhssk :: HNil =>
            StateChangesAccumulator(
              lsch, ltx, bal, lcs, lcsp, aas, atl, tlb, lasr, ltlr,
              ads, dsw, anc, ncw, msd, unp, ps, asei, tlei, ncwei,
              rask, rtlk, rtlbk, rdsk, rdswk, rnck, rncwk, hss, rhssk
            )
        },
        a =>
          a.lastStateChannelSnapshotHashes :: a.lastTxRefs :: a.balances :: a.lastCurrencySnapshots :: a.lastCurrencySnapshotsProofs ::
            a.activeAllowSpends :: a.activeTokenLocks :: a.tokenLockBalances :: a.lastAllowSpendRefs :: a.lastTokenLockRefs ::
            a.activeDelegatedStakes :: a.delegatedStakesWithdrawals :: a.activeNodeCollaterals :: a.nodeCollateralWithdrawals :: a.metagraphSyncData ::
            a.updateNodeParameters :: a.priceState :: a.allowSpendExpiryIndex :: a.tokenLockExpiryIndex :: a.nodeCollateralWithdrawalExpiryIndex ::
            a.removedAllowSpendKeys :: a.removedTokenLockKeys :: a.removedTokenLockBalanceKeys :: a.removedDelegatedStakeKeys :: a.removedDelegatedStakeWithdrawalKeys ::
            a.removedNodeCollateralKeys :: a.removedNodeCollateralWithdrawalKeys :: a.historicalStakeSnapshots :: a.removedHistoricalStakeSnapshotKeys :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[StateChangesAccumulator] = ImmutableCodec.fromScodecCodec(codec)
}
