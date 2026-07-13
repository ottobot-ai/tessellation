package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSnapshotSync
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageType}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock.{TokenLock, TokenLockReference}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.SortedMapCodec.{sortedMap, sortedMapCanonical}
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSetCanonical
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendCodec.{codec => allowSpendCodec}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{codec => allowSpendRefCodec}
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs._
import io.constellationnetwork.serde.codecs.instances.CurrencyRecordCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.TokenLockCodec.{codec => tokenLockCodec}
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{codec => tokenLockRefCodec}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{codec => transactionReferenceCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codecs for the currency-snapshot state proof and info records.
  *
  * State proofs:
  *   - `CurrencySnapshotStateProofV1` (2 fields, legacy) / `CurrencySnapshotStateProof` (9 fields, current).
  *
  * Info:
  *   - `CurrencySnapshotInfoV1` (2 SortedMaps, legacy) / `CurrencySnapshotInfo` (9 fields, current).
  */
object CurrencySnapshotInfoCodecs {

  private val optionalHashCodec: Codec[Option[Hash]] = option(hashCodec)

  // ---- CurrencySnapshotStateProof (V1 + current) --------------------------

  implicit val currencySnapshotStateProofV1Codec: Codec[CurrencySnapshotStateProofV1] =
    (hashCodec :: hashCodec)
      .xmap[CurrencySnapshotStateProofV1](
        { case tx :: bal :: HNil => CurrencySnapshotStateProofV1(tx, bal) },
        p => p.lastTxRefsProof :: p.balancesProof :: HNil
      )

  implicit val currencySnapshotStateProofV1ImmutableCodec: ImmutableCodec[CurrencySnapshotStateProofV1] =
    ImmutableCodec.fromScodecCodec(currencySnapshotStateProofV1Codec)

  implicit val currencySnapshotStateProofCodec: Codec[CurrencySnapshotStateProof] =
    (hashCodec :: hashCodec ::
      optionalHashCodec :: optionalHashCodec :: optionalHashCodec ::
      optionalHashCodec :: optionalHashCodec :: optionalHashCodec :: optionalHashCodec)
      .xmap[CurrencySnapshotStateProof](
        {
          case tx :: bal :: msgs :: fee :: allowSp :: activeAllow :: globSync :: tokLock :: activeLocks :: HNil =>
            CurrencySnapshotStateProof(tx, bal, msgs, fee, allowSp, activeAllow, globSync, tokLock, activeLocks)
        },
        p =>
          p.lastTxRefsProof ::
            p.balancesProof ::
            p.lastMessagesProof ::
            p.lastFeeTxRefsProof ::
            p.lastAllowSpendRefsProof ::
            p.activeAllowSpends ::
            p.globalSnapshotSync ::
            p.lastTokenLockRefsProof ::
            p.activeTokenLocks ::
            HNil
      )

  implicit val currencySnapshotStateProofImmutableCodec: ImmutableCodec[CurrencySnapshotStateProof] =
    ImmutableCodec.fromScodecCodec(currencySnapshotStateProofCodec)

  // ---- CurrencySnapshotInfo (V1 + current) --------------------------------

  private val lastTxRefsMapCodec: Codec[SortedMap[Address, TransactionReference]] =
    sortedMap(addressCodec, transactionReferenceCodec)
  private val balancesMapCodec: Codec[SortedMap[Address, Balance]] =
    sortedMap(addressCodec, Codec[Balance])

  implicit val currencySnapshotInfoV1Codec: Codec[CurrencySnapshotInfoV1] =
    (lastTxRefsMapCodec :: balancesMapCodec)
      .xmap[CurrencySnapshotInfoV1](
        { case tx :: bal :: HNil => CurrencySnapshotInfoV1(tx, bal) },
        i => i.lastTxRefs :: i.balances :: HNil
      )

  implicit val currencySnapshotInfoV1ImmutableCodec: ImmutableCodec[CurrencySnapshotInfoV1] =
    ImmutableCodec.fromScodecCodec(currencySnapshotInfoV1Codec)

  // Signed wrappers for the maps inside the current info.
  private val signedCurrencyMessageCodec: Codec[Signed[CurrencyMessage]] = signedCodecFor(currencyMessageCodec)
  private val signedGlobalSyncCodec: Codec[Signed[GlobalSnapshotSync]] = signedCodecFor(globalSnapshotSyncCodec)
  private val signedAllowSpendCodec: Codec[Signed[AllowSpend]] = signedCodecFor(allowSpendCodec)
  private val signedTokenLockCodec: Codec[Signed[TokenLock]] = signedCodecFor(tokenLockCodec)

  private val lastMessagesMapCodec: Codec[SortedMap[MessageType, Signed[CurrencyMessage]]] =
    sortedMap(messageTypeCodec, signedCurrencyMessageCodec)

  private val lastFeeTxRefsMapCodec: Codec[SortedMap[Address, TransactionReference]] = lastTxRefsMapCodec
  private val lastAllowSpendRefsMapCodec: Codec[SortedMap[Address, AllowSpendReference]] =
    sortedMap(addressCodec, allowSpendRefCodec)
  private val activeAllowSpendsMapCodec: Codec[SortedMap[Address, SortedSet[Signed[AllowSpend]]]] =
    sortedMap(addressCodec, sortedSetCanonical(signedAllowSpendCodec))
  private val globalSyncViewMapCodec: Codec[SortedMap[PeerId, Signed[GlobalSnapshotSync]]] =
    sortedMapCanonical(peerIdCodec, signedGlobalSyncCodec)
  private val lastTokenLockRefsMapCodec: Codec[SortedMap[Address, TokenLockReference]] =
    sortedMap(addressCodec, tokenLockRefCodec)
  private val activeTokenLocksMapCodec: Codec[SortedMap[Address, SortedSet[Signed[TokenLock]]]] =
    sortedMap(addressCodec, sortedSetCanonical(signedTokenLockCodec))

  private val optLastMessagesCodec = option(lastMessagesMapCodec)
  private val optLastFeeTxRefsCodec = option(lastFeeTxRefsMapCodec)
  private val optLastAllowSpendRefsCodec = option(lastAllowSpendRefsMapCodec)
  private val optActiveAllowSpendsCodec = option(activeAllowSpendsMapCodec)
  private val optGlobalSyncViewCodec = option(globalSyncViewMapCodec)
  private val optLastTokenLockRefsCodec = option(lastTokenLockRefsMapCodec)
  private val optActiveTokenLocksCodec = option(activeTokenLocksMapCodec)

  // Witness so NonEmptySet import survives lint (it's imported for scodec.Codec
  // types that reference it transitively via Signed).
  private val _nesWitness: Codec[NonEmptySet[Signed[AllowSpend]]] =
    io.constellationnetwork.serde.codecs.NonEmptySetCodec.nonEmptySetCanonical(signedAllowSpendCodec)
  locally { val _ = _nesWitness }

  implicit val currencySnapshotInfoCodec: Codec[CurrencySnapshotInfo] =
    (lastTxRefsMapCodec ::
      balancesMapCodec ::
      optLastMessagesCodec ::
      optLastFeeTxRefsCodec ::
      optLastAllowSpendRefsCodec ::
      optActiveAllowSpendsCodec ::
      optGlobalSyncViewCodec ::
      optLastTokenLockRefsCodec ::
      optActiveTokenLocksCodec)
      .xmap[CurrencySnapshotInfo](
        {
          case tx :: bal :: msgs :: fee :: allowSpR :: activeAllow :: gsync :: lockR :: activeLock :: HNil =>
            CurrencySnapshotInfo(tx, bal, msgs, fee, allowSpR, activeAllow, gsync, lockR, activeLock)
        },
        i =>
          i.lastTxRefs ::
            i.balances ::
            i.lastMessages ::
            i.lastFeeTxRefs ::
            i.lastAllowSpendRefs ::
            i.activeAllowSpends ::
            i.globalSnapshotSyncView ::
            i.lastTokenLockRefs ::
            i.activeTokenLocks ::
            HNil
      )

  implicit val currencySnapshotInfoImmutableCodec: ImmutableCodec[CurrencySnapshotInfo] =
    ImmutableCodec.fromScodecCodec(currencySnapshotInfoCodec)

  // ---- UNROLLED per-metagraph CurrencySnapshotInfo sub-field value codecs ---------------------------------------
  // (docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md) Each unrolled `Mg*` partition stores one entry per
  // account/holder/messageType/peer. The MPT key hashes that entry key (lossy — `toHex` over `AddressNamespace`), so the VALUE carries the
  // typed entry key as `(EntryKey, FieldValue)`; reconstruction recovers the logical key from the value via a prefix scan (the
  // value-carries-key pattern, cf. `getAllUpdateNodeParameters`). `activeAllowSpends` is NOT unrolled here — it stays in fieldId-7.

  private def entryTupleCodec[A, B](ca: Codec[A], cb: Codec[B]): Codec[(A, B)] =
    (ca :: cb).xmap[(A, B)]({ case a :: b :: HNil => (a, b) }, { case (a, b) => a :: b :: HNil })

  /** `MgBalances` (fieldId 25) value: `(account, balance)`. */
  implicit val mgBalanceEntryImmutableCodec: ImmutableCodec[(Address, Balance)] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(addressCodec, Codec[Balance]))

  /** `MgLastTxRefs` (26) AND `MgLastFeeTxRefs` (27) value: `(account, ref)` — same shape, one codec serves both partitions. */
  implicit val mgTxRefEntryImmutableCodec: ImmutableCodec[(Address, TransactionReference)] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(addressCodec, transactionReferenceCodec))

  /** `MgLastAllowSpendRefs` (28) value: `(account, ref)`. */
  implicit val mgAllowSpendRefEntryImmutableCodec: ImmutableCodec[(Address, AllowSpendReference)] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(addressCodec, allowSpendRefCodec))

  /** `MgLastTokenLockRefs` (29) value: `(account, ref)`. */
  implicit val mgTokenLockRefEntryImmutableCodec: ImmutableCodec[(Address, TokenLockReference)] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(addressCodec, tokenLockRefCodec))

  /** `MgActiveTokenLocks` (30) value: `(holder, locks)`. */
  implicit val mgActiveTokenLocksEntryImmutableCodec: ImmutableCodec[(Address, SortedSet[Signed[TokenLock]])] =
    ImmutableCodec.fromScodecCodec(
      entryTupleCodec(addressCodec, sortedSetCanonical(signedTokenLockCodec))
    )

  /** `MgLastMessages` (31) value: `(messageType, signedMessage)`. */
  implicit val mgLastMessagesEntryImmutableCodec: ImmutableCodec[(MessageType, Signed[CurrencyMessage])] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(messageTypeCodec, signedCurrencyMessageCodec))

  /** `MgGlobalSnapshotSyncView` (32) value: `(peerId, signedSync)`. */
  implicit val mgGlobalSyncEntryImmutableCodec: ImmutableCodec[(PeerId, Signed[GlobalSnapshotSync])] =
    ImmutableCodec.fromScodecCodec(entryTupleCodec(peerIdCodec, signedGlobalSyncCodec))
}
