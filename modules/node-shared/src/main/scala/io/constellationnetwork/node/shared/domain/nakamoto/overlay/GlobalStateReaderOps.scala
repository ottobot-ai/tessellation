package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.delegatedStake.{DelegatedStakeRecord, PendingDelegatedStakeWithdrawal}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral.{NodeCollateralRecord, PendingNodeCollateralWithdrawal}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutableCodec}
import io.constellationnetwork.serde.codecs.instances.MetagraphSyncDataInfoCodec.{immutableCodec => metagraphSyncDataImmutableCodec}
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

    def getMetagraphSyncData(metagraphAddress: Address): F[Option[MetagraphSyncDataInfo]] =
      reader.get[MetagraphSyncDataInfo](GlobalStateKey.hypergraph(GlobalStateFieldId.MetagraphSyncData, metagraphAddress))

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

    def getActiveTokenLocks(address: Address)(implicit hasher: Hasher[F]): F[Option[SortedSet[Signed[TokenLock]]]] =
      ActiveTokenLockMptReader.readNative(reader, address)

    /** Reconstruct a metagraph's `CurrencySnapshotInfo` from the UNROLLED per-entry `Mg*` partitions (+ fieldId-7 allow-spends), gated on
      * the presence of the fieldId-5 incremental so a metagraph still at its genesis (`LastCurrencySnapshots` Left partition, no
      * incremental written yet) reads `None` — byte-for-byte the same `Some`/`None` distinction the old direct blob read produced. NEVER
      * reads the legacy `LastCurrencySnapshotInfo` blob (it is no longer written; see GlobalStateConverter unroll,
      * UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md).
      */
    def getCurrencySnapshotInfo(metagraphAddress: Address)(implicit hasher: Hasher[F]): F[Option[CurrencySnapshotInfo]] =
      reader
        .get[Signed[CurrencyIncrementalSnapshot]](
          GlobalStateKey.metagraph(metagraphAddress, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
        )
        .map(_.isDefined)
        .ifM(
          GlobalStateConverter
            .reconstructCurrencyInfoFrom[F](metagraphAddress, CurrencyInfoMptAdapters.readerFor(reader))
            .map(_.some),
          none[CurrencySnapshotInfo].pure[F]
        )

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

    /** Materialize the rooted state-channel-tip map. Every address named by the index must have one decodable target in this exact reader
      * view; callers must fail/defer on an index/target gap instead of healing it from a GSI or another branch.
      */
    def materializeLastStateChannelSnapshotHashes(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, Hash]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastStateChannelSnapshotHashes)
        indexHex <- GlobalStateKey.toHex[F](indexKey)
        addresses <- StrictMptRead.valueOrElseF(
          reader.getStrict[SortedSet[Address]](indexKey),
          SortedSet.empty[Address],
          "materialize LastStateChannelSnapshotHashes ActiveAddressIndex",
          indexHex
        )
        entries <- addresses.toList.traverse { address =>
          val targetKey = GlobalStateKey.metagraph(address, GlobalStateFieldId.LastStateChannelSnapshotHashes)
          for {
            targetHex <- GlobalStateKey.toHex[F](targetKey)
            value <- StrictMptRead.requirePresentF(
              reader.getStrict[Hash](targetKey),
              s"materialize LastStateChannelSnapshotHashes indexed target(address=$address)",
              targetHex
            )
          } yield address -> value
        }
      } yield SortedMap.from(entries)

    /** Materialize the rooted metagraph-sync acknowledgement state. The index and every target are read from the same branch view, so a
      * stale `GlobalSnapshotInfo` cannot become an alternate authority for pending economic acknowledgements.
      */
    def materializeMetagraphSyncData(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, MetagraphSyncDataInfo]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.MetagraphSyncData)
        indexHex <- GlobalStateKey.toHex[F](indexKey)
        addresses <- StrictMptRead.valueOrElseF(
          reader.getStrict[SortedSet[Address]](indexKey),
          SortedSet.empty[Address],
          "materialize MetagraphSyncData ActiveAddressIndex",
          indexHex
        )
        entries <- addresses.toList.traverse { address =>
          val targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.MetagraphSyncData, address)
          for {
            targetHex <- GlobalStateKey.toHex[F](targetKey)
            value <- StrictMptRead.requirePresentF(
              reader.getStrict[MetagraphSyncDataInfo](targetKey),
              s"materialize MetagraphSyncData indexed target(address=$address)",
              targetHex
            )
          } yield address -> value
        }
      } yield SortedMap.from(entries)

    /** Materialize the rooted currency-snapshot union. Both physical arms are decoded strictly so malformed bytes in either partition
      * cannot be hidden by the other arm. Exactly one arm must be present: a legacy Left arm or the canonical incremental Right arm. Dual
      * presence and an indexed address with neither arm are authenticated index/target inconsistencies and fail closed.
      */
    def materializeLastCurrencySnapshots(
      implicit hasher: Hasher[F]
    ): F[SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]] =
      for {
        indexKey <- GlobalStateKey.activeAddressIndexKey[F](GlobalStateFieldId.LastCurrencySnapshots)
        indexHex <- GlobalStateKey.toHex[F](indexKey)
        addresses <- StrictMptRead.valueOrElseF(
          reader.getStrict[SortedSet[Address]](indexKey),
          SortedSet.empty[Address],
          "materialize LastCurrencySnapshots ActiveAddressIndex",
          indexHex
        )
        entries <- addresses.toList.traverse { address =>
          val leftKey = GlobalStateKey.metagraph(address, GlobalStateFieldId.LastCurrencySnapshots)
          val incrementalKey = GlobalStateKey.metagraph(address, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
          for {
            leftHex <- GlobalStateKey.toHex[F](leftKey)
            incrementalHex <- GlobalStateKey.toHex[F](incrementalKey)
            left <- StrictMptRead.toOptionF(
              reader.getStrict[Signed[CurrencySnapshot]](leftKey),
              s"materialize LastCurrencySnapshots left arm(address=$address)",
              leftHex
            )
            incremental <- StrictMptRead.toOptionF(
              reader.getStrict[Signed[CurrencyIncrementalSnapshot]](incrementalKey),
              s"materialize LastCurrencySnapshots right arm(address=$address)",
              incrementalHex
            )
            entry <- (left, incremental) match {
              case (Some(_), Some(_)) =>
                Async[F].raiseError[
                  (
                    Address,
                    Either[
                      Signed[CurrencySnapshot],
                      (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
                    ]
                  )
                ](
                  StrictMptRead.InconsistentConsensusMptIndex(
                    s"materialize LastCurrencySnapshots dual arms(address=$address)",
                    incrementalHex,
                    s"both legacy full-snapshot key=$leftHex and incremental key=$incrementalHex are present"
                  )
                )
              case (Some(snapshot), None) =>
                (address -> (Left(snapshot): Either[
                  Signed[CurrencySnapshot],
                  (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
                ])).pure[F]
              case (None, Some(inc)) =>
                GlobalStateConverter
                  .reconstructCurrencyInfoFrom[F](address, CurrencyInfoMptAdapters.readerFor(reader))
                  .map(info =>
                    address -> (Right((inc, info)): Either[
                      Signed[CurrencySnapshot],
                      (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
                    ])
                  )
              case (None, None) =>
                Async[F].raiseError[
                  (
                    Address,
                    Either[
                      Signed[CurrencySnapshot],
                      (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
                    ]
                  )
                ](
                  StrictMptRead.MissingConsensusMptValue(
                    s"materialize LastCurrencySnapshots neither arm(address=$address)",
                    incrementalHex
                  )
                )
            }
          } yield entry
        }
      } yield SortedMap.from(entries)
  }
}
