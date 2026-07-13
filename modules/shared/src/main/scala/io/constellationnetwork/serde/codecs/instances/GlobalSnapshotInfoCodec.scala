package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.merkletree.Proof
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GenesisOperatorConsensusKey, HistoricalStakeSnapshot}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.{PriceRecord, TokenPair}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.EitherCodec.either
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SortedMapCodec.{sortedMap, sortedMapCanonical}
import io.constellationnetwork.serde.codecs.SortedSetCodec.{sortedSet, sortedSetCanonical}
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{codec => allowSpendRefCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.DelegatedStakeCodecs._
import io.constellationnetwork.serde.codecs.instances.GenesisOperatorConsensusKeyCodec.{codec => genesisOperatorConsensusKeyCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.{kesRegistrationRecordCodec, kesRegistrationReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.{proofCodec => merkleProofCodec}
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{codec => metagraphSyncCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.NodeCollateralCodecs._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.{priceRecordCodec, tokenPairCodec}
import io.constellationnetwork.serde.codecs.instances.SignatureCodecs.{idCodec, signatureProofCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{
  etaPeriodCodec,
  historicalCodec => historicalStakeSnapshotCodec
}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{codec => tokenLockRefCodec}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{codec => transactionReferenceCodec}
import io.constellationnetwork.serde.codecs.instances.UpdateNodeParametersCodec.{updateNodeParametersCodec => unpCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `GlobalSnapshotInfo` — the 21-field current info record.
  *
  * Capstone codec for the info layer. Composes every inner-type codec built to this point.
  *
  * Wire layout matches the declared field order exactly:
  * 1..3 — required maps (last-state-channel-hashes, last-tx-refs, balances) 4 — lastCurrencySnapshots: SortedMap[Address,
  * Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] 5 — lastCurrencySnapshotsProofs:
  * SortedMap[Address, Proof] 6..17 — 12 Option[SortedMap[_, _]] fields for post-V1 feature activations 18 — historicalStakeSnapshots:
  * SortedMap[EtaPeriod, StakeDistribution] (NIPoPoW S0 N-2 epoch staggering — required, not Option, since it has a sensible empty default
  * the genesis loader / V1+V2 upgrade paths supply) 19..20 — rooted unified operator-key histories and latest references. 21 — immutable
  * signed period-zero operator-key identities committed at genesis.
  */
object GlobalSnapshotInfoCodec {

  // ---- Required field-4 internal: Either + Tuple2 of currency snapshot payloads --------

  private val signedCurrencySnapshotCodec: Codec[Signed[CurrencySnapshot]] =
    signedCodecFor(currencySnapshotCodec)

  private val signedCurrencyIncrementalSnapshotCodec: Codec[Signed[CurrencyIncrementalSnapshot]] =
    signedCodecFor(currencyIncrementalSnapshotCodec)

  private val incrementalWithInfoTupleCodec: Codec[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)] =
    (signedCurrencyIncrementalSnapshotCodec :: currencySnapshotInfoCodec)
      .xmap[(Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)](
        { case sig :: info :: HNil => (sig, info) },
        t => t._1 :: t._2 :: HNil
      )

  private val currencySnapshotEitherCodec
    : Codec[Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
    either(signedCurrencySnapshotCodec, incrementalWithInfoTupleCodec)

  // ---- All 20 field codecs ------------------------------------------------

  private val stateChannelHashesMapCodec: Codec[SortedMap[Address, Hash]] =
    sortedMap(addressCodec, hashCodec)

  private val lastTxRefsMapCodec: Codec[SortedMap[Address, TransactionReference]] =
    sortedMap(addressCodec, transactionReferenceCodec)

  private val balancesMapCodec: Codec[SortedMap[Address, Balance]] =
    sortedMap(addressCodec, Codec[Balance])

  private val lastCurrencySnapshotsMapCodec
    : Codec[SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]] =
    sortedMap(addressCodec, currencySnapshotEitherCodec)

  private val currencySnapshotProofsMapCodec: Codec[SortedMap[Address, Proof]] =
    sortedMap(addressCodec, merkleProofCodec)

  // Field 6: Option[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]]
  private val signedAllowSpendCodec: Codec[Signed[AllowSpend]] = signedCodecFor(allowSpendCodec)
  private val optionalAddressCodec: Codec[Option[Address]] = option(addressCodec)

  private val activeAllowSpendsInnerMapCodec: Codec[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
    sortedMap(addressCodec, sortedSetCanonical(signedAllowSpendCodec))

  private val activeAllowSpendsMapCodec: Codec[SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]] =
    sortedMap(optionalAddressCodec, activeAllowSpendsInnerMapCodec)

  private val activeAllowSpendsOptCodec = option(activeAllowSpendsMapCodec)

  // Field 7
  private val signedTokenLockCodec: Codec[Signed[TokenLock]] = signedCodecFor(tokenLockCodec)
  private val activeTokenLocksMapCodec: Codec[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
    sortedMap(addressCodec, sortedSetCanonical(signedTokenLockCodec))
  private val activeTokenLocksOptCodec = option(activeTokenLocksMapCodec)

  // Field 8: SortedMap[Address, SortedMap[Address, Balance]]
  private val tokenLockBalancesMapCodec: Codec[SortedMap[Address, SortedMap[Address, Balance]]] =
    sortedMap(addressCodec, balancesMapCodec)
  private val tokenLockBalancesOptCodec = option(tokenLockBalancesMapCodec)

  // Fields 9, 10
  private val lastAllowSpendRefsMapCodec: Codec[SortedMap[Address, AllowSpendReference]] =
    sortedMap(addressCodec, allowSpendRefCodec)
  private val lastAllowSpendRefsOptCodec = option(lastAllowSpendRefsMapCodec)

  private val lastTokenLockRefsMapCodec: Codec[SortedMap[Address, TokenLockReference]] =
    sortedMap(addressCodec, tokenLockRefCodec)
  private val lastTokenLockRefsOptCodec = option(lastTokenLockRefsMapCodec)

  // Field 11: SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]
  private val signedUnpCodec: Codec[Signed[UpdateNodeParameters]] = signedCodecFor(unpCodec)
  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val unpWithOrdinalTupleCodec: Codec[(Signed[UpdateNodeParameters], SnapshotOrdinal)] =
    (signedUnpCodec :: ordinalCodec)
      .xmap[(Signed[UpdateNodeParameters], SnapshotOrdinal)](
        { case s :: o :: HNil => (s, o) },
        t => t._1 :: t._2 :: HNil
      )
  private val updateNodeParametersMapCodec: Codec[SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
    sortedMapCanonical(idCodec, unpWithOrdinalTupleCodec)
  private val updateNodeParametersOptCodec = option(updateNodeParametersMapCodec)

  // Fields 12, 13
  private val activeDelegatedStakesMapCodec: Codec[SortedMap[Address, SortedSet[DelegatedStakeRecord]]] =
    sortedMap(addressCodec, sortedSet(delegatedStakeRecordCodec))
  private val activeDelegatedStakesOptCodec = option(activeDelegatedStakesMapCodec)

  private val delegatedStakesWithdrawalsMapCodec: Codec[SortedMap[Address, SortedSet[PendingDelegatedStakeWithdrawal]]] =
    sortedMap(addressCodec, sortedSet(pendingDelegatedStakeWithdrawalCodec))
  private val delegatedStakesWithdrawalsOptCodec = option(delegatedStakesWithdrawalsMapCodec)

  // Fields 14, 15
  private val activeNodeCollateralsMapCodec: Codec[SortedMap[Address, SortedSet[NodeCollateralRecord]]] =
    sortedMap(addressCodec, sortedSet(nodeCollateralRecordCodec))
  private val activeNodeCollateralsOptCodec = option(activeNodeCollateralsMapCodec)

  private val nodeCollateralWithdrawalsMapCodec: Codec[SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]] =
    sortedMap(addressCodec, sortedSet(pendingNodeCollateralWithdrawalCodec))
  private val nodeCollateralWithdrawalsOptCodec = option(nodeCollateralWithdrawalsMapCodec)

  // Fields 16, 17
  private val priceStateMapCodec: Codec[SortedMap[TokenPair, PriceRecord]] =
    sortedMap(tokenPairCodec, priceRecordCodec)
  private val priceStateOptCodec = option(priceStateMapCodec)

  private val metagraphSyncDataMapCodec: Codec[SortedMap[Address, MetagraphSyncDataInfo]] =
    sortedMap(addressCodec, metagraphSyncCodec)
  private val metagraphSyncDataOptCodec = option(metagraphSyncDataMapCodec)

  // Field 18: NIPoPoW S0 historical stake — SortedMap[EtaPeriod, HistoricalStakeSnapshot]. Codecs for the leaf types live in
  // [[StakeDistributionCodec]] because they're also used by the MPT projection (`toAllStateKeyValueBytes`) that authenticates
  // the per-period partition under [[GlobalStateFieldId.HistoricalStakeSnapshots]]. Reusing the same
  // `ImmutableCodec[HistoricalStakeSnapshot]` there means the bytes the MPT hashes equal the bytes this codec emits — required for
  // cross-path determinism (parity gate #107). Path 1 (heap-leak workstream): the per-period entry now bundles `(stakes, eta)` so
  // `EtaStateManager.getEta` has a disk-immune cache against chainStore eviction.
  private val historicalStakeSnapshotsMapCodec: Codec[SortedMap[EtaPeriod, HistoricalStakeSnapshot]] =
    sortedMap(etaPeriodCodec, historicalStakeSnapshotCodec)

  private val kesRegistrationCertsMapCodec: Codec[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]] =
    sortedMapCanonical(peerIdCodec, sortedSetCanonical(kesRegistrationRecordCodec))
  private val lastKesRegistrationRefsMapCodec: Codec[SortedMap[PeerId, KesRegistrationReference]] =
    sortedMapCanonical(peerIdCodec, kesRegistrationReferenceCodec)
  private val genesisOperatorKeysMapCodec: Codec[SortedMap[PeerId, GenesisOperatorConsensusKey]] =
    sortedMapCanonical(peerIdCodec, genesisOperatorConsensusKeyCodec)

  // Witness to keep SignatureProof import referenced.
  private val _spWitness: Codec[SignatureProof] = signatureProofCodec
  locally { val _ = _spWitness }

  // ---- Assembly -----------------------------------------------------------

  implicit val codec: Codec[GlobalSnapshotInfo] =
    (stateChannelHashesMapCodec ::
      lastTxRefsMapCodec ::
      balancesMapCodec ::
      lastCurrencySnapshotsMapCodec ::
      currencySnapshotProofsMapCodec ::
      activeAllowSpendsOptCodec ::
      activeTokenLocksOptCodec ::
      tokenLockBalancesOptCodec ::
      lastAllowSpendRefsOptCodec ::
      lastTokenLockRefsOptCodec ::
      updateNodeParametersOptCodec ::
      activeDelegatedStakesOptCodec ::
      delegatedStakesWithdrawalsOptCodec ::
      activeNodeCollateralsOptCodec ::
      nodeCollateralWithdrawalsOptCodec ::
      priceStateOptCodec ::
      metagraphSyncDataOptCodec ::
      historicalStakeSnapshotsMapCodec ::
      kesRegistrationCertsMapCodec ::
      lastKesRegistrationRefsMapCodec ::
      genesisOperatorKeysMapCodec)
      .xmap[GlobalSnapshotInfo](
        {
          case sch :: tx :: bal :: lcs :: lcsp ::
              aas :: atl :: tlb :: lasr :: ltlr ::
              unp :: ads :: dsw :: anc :: ncw ::
              ps :: msd :: hss :: kesCerts :: kesRefs :: genesisKeys :: HNil =>
            GlobalSnapshotInfo(
              sch,
              tx,
              bal,
              lcs,
              lcsp,
              aas,
              atl,
              tlb,
              lasr,
              ltlr,
              unp,
              ads,
              dsw,
              anc,
              ncw,
              ps,
              msd,
              hss,
              kesCerts,
              kesRefs,
              genesisKeys
            )
        },
        i =>
          i.lastStateChannelSnapshotHashes ::
            i.lastTxRefs ::
            i.balances ::
            i.lastCurrencySnapshots ::
            i.lastCurrencySnapshotsProofs ::
            i.activeAllowSpends ::
            i.activeTokenLocks ::
            i.tokenLockBalances ::
            i.lastAllowSpendRefs ::
            i.lastTokenLockRefs ::
            i.updateNodeParameters ::
            i.activeDelegatedStakes ::
            i.delegatedStakesWithdrawals ::
            i.activeNodeCollaterals ::
            i.nodeCollateralWithdrawals ::
            i.priceState ::
            i.metagraphSyncData ::
            i.historicalStakeSnapshots ::
            i.kesRegistrationCerts ::
            i.lastKesRegistrationRefs ::
            i.genesisOperatorKeys ::
            HNil
      )

  implicit val immutableCodec: ImmutableCodec[GlobalSnapshotInfo] = ImmutableCodec.fromScodecCodec(codec)
}
