package io.constellationnetwork.schema.mpt

import cats.Show
import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.priceOracle.TokenPair
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import derevo.cats.{eqv, order, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive
import io.circe._

sealed trait PartitionKeyType {
  def toByte: Byte
}

object PartitionKeyType {
  case object PKTHypergraph extends PartitionKeyType { val toByte: Byte = 0x00 }
  case object PKTAddress extends PartitionKeyType { val toByte: Byte = 0x01 }
  case object PKTHash extends PartitionKeyType { val toByte: Byte = 0x02 }
  case object PKTSystem extends PartitionKeyType { val toByte: Byte = 0x03 }

  implicit val ordering: Ordering[PartitionKeyType] = Ordering.by(_.toByte)
  implicit val show: Show[PartitionKeyType] = Show.show(_.toByte.toString)

  implicit val encoder: Encoder[PartitionKeyType] = Encoder[Byte].contramap(_.toByte)
  implicit val decoder: Decoder[PartitionKeyType] = Decoder[Byte].emap { b =>
    fromByte(b).toRight(s"Invalid PartitionKeyType byte: $b")
  }

  def fromByte(b: Byte): Option[PartitionKeyType] = b match {
    case 0x00 => Some(PKTHypergraph)
    case 0x01 => Some(PKTAddress)
    case 0x02 => Some(PKTHash)
    case 0x03 => Some(PKTSystem)
    case _    => None
  }
}

/** Discriminator for MPT partitions reserved for internal/derived state that isn't user-addressable: expiry indices, GC markers,
  * reverse-lookup tables, etc. Adding a new system-scoped partition = adding a new sealed case object here (plus its value codec and the
  * sync wiring). Keeps `GlobalStateFieldId` bounded to user-visible state.
  */
sealed trait SystemNamespaceLabel {
  def canonicalName: String
}
object SystemNamespaceLabel {
  case object ExpiryIndexAllowSpends extends SystemNamespaceLabel { val canonicalName = "expiry-index-allowspends" }
  case object ExpiryIndexTokenLocks extends SystemNamespaceLabel { val canonicalName = "expiry-index-tokenlocks" }
  case object ExpiryIndexNodeCollateralWithdrawals extends SystemNamespaceLabel {
    val canonicalName = "expiry-index-nodecollateralwithdrawals"
  }

  /** Reverse-lookup partition: per `GlobalStateFieldId`, holds a `SortedSet[Address]` of every address with an active entry in that field.
    * Lets consumers materialize a `Map[Address, V]` for fields whose value type doesn't carry the address (refs, balances) — the
    * prefix-scan primitive only recovers addresses from values that embed a `source: Address`, so address-keyed maps with reference-only
    * values need this sidecar to be reverse-lookable.
    */
  case object ActiveAddressIndex extends SystemNamespaceLabel { val canonicalName = "active-address-index" }

  val all: List[SystemNamespaceLabel] = List(
    ExpiryIndexAllowSpends,
    ExpiryIndexTokenLocks,
    ExpiryIndexNodeCollateralWithdrawals,
    ActiveAddressIndex
  )

  def fromCanonicalName(name: String): Option[SystemNamespaceLabel] = all.find(_.canonicalName === name)

  implicit val ordering: Ordering[SystemNamespaceLabel] = Ordering.by(_.canonicalName)
  implicit val show: Show[SystemNamespaceLabel] = Show.show(_.canonicalName)

  implicit val encoder: Encoder[SystemNamespaceLabel] = Encoder[String].contramap(_.canonicalName)
  implicit val decoder: Decoder[SystemNamespaceLabel] =
    Decoder[String].emap(n => fromCanonicalName(n).toRight(s"Unknown SystemNamespaceLabel: $n"))
}

sealed trait PartitionNamespace {
  def keyType: PartitionKeyType
}

object PartitionNamespace {
  import PartitionKeyType._

  case object HypergraphNamespace extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTHypergraph
  }

  case class MetagraphNamespace(address: Address) extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTAddress
  }

  case class AddressNamespace(address: Address) extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTAddress
  }

  case class HashNamespace(hash: Hash) extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTHash
  }

  /** Reserved namespace for system-derived partitions (expiry indices, GC markers, reverse-lookup tables, ...). Hashed to fixed width
    * during `toHex` like the address-bearing namespaces. Adding a new subsystem = adding a new `SystemNamespaceLabel` case object.
    */
  case class SystemNamespace(label: SystemNamespaceLabel) extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTSystem
  }

  case object EmptyNamespace extends PartitionNamespace {
    val keyType: PartitionKeyType = PKTHypergraph
  }

  implicit val ordering: Ordering[PartitionNamespace] = Ordering.by {
    case EmptyNamespace           => (0, "", "")
    case HypergraphNamespace      => (0, "", "")
    case MetagraphNamespace(addr) => (1, addr.value.value, "")
    case AddressNamespace(addr)   => (1, addr.value.value, "")
    case HashNamespace(hash)      => (2, hash.value, "")
    case SystemNamespace(label)   => (3, label.canonicalName, "")
  }

  implicit val show: Show[PartitionNamespace] = Show.show {
    case EmptyNamespace           => "Empty"
    case HypergraphNamespace      => "Hypergraph"
    case MetagraphNamespace(addr) => s"Metagraph(${addr.value.value})"
    case AddressNamespace(addr)   => s"Address(${addr.value.value})"
    case HashNamespace(hash)      => s"Hash(${hash.value})"
    case SystemNamespace(label)   => s"System(${label.canonicalName})"
  }

  implicit val encoder: Encoder[PartitionNamespace] = Encoder.instance {
    case HypergraphNamespace => Json.obj("type" -> Json.fromString("hypergraph"))
    case EmptyNamespace      => Json.obj("type" -> Json.fromString("empty"))
    case MetagraphNamespace(addr) =>
      Json.obj("type" -> Json.fromString("metagraph"), "address" -> Json.fromString(addr.value.value))
    case AddressNamespace(addr) =>
      Json.obj("type" -> Json.fromString("address"), "address" -> Json.fromString(addr.value.value))
    case HashNamespace(hash) =>
      Json.obj("type" -> Json.fromString("hash"), "hash" -> Json.fromString(hash.value))
    case SystemNamespace(label) =>
      Json.obj("type" -> Json.fromString("system"), "label" -> Json.fromString(label.canonicalName))
  }

  implicit val decoder: Decoder[PartitionNamespace] = Decoder.instance { cursor =>
    cursor.downField("type").as[String].flatMap {
      case "empty"      => Right(EmptyNamespace)
      case "hypergraph" => Right(HypergraphNamespace)
      // Decode via the canonical Decoder[Address] — the exact inverse of the encoder above
      // (`Json.fromString(addr.value.value)`). The previous `Address.fromBytes(s.getBytes)` was a
      // BUG: `fromBytes` SHA-256-hashes its input (address.scala:30-40), so it produced the address
      // OF THE HASH OF the address string, not the original — making address/metagraph-keyed
      // GlobalStateKeys non-round-trippable over JSON (blocked inclusion-proof fetch for balances).
      case "metagraph" => cursor.downField("address").as[Address].map(MetagraphNamespace(_))
      case "address"   => cursor.downField("address").as[Address].map(AddressNamespace(_))
      case "hash"      => cursor.downField("hash").as[String].map(s => HashNamespace(Hash(s)))
      case "system" =>
        cursor
          .downField("label")
          .as[String]
          .flatMap { l =>
            SystemNamespaceLabel.fromCanonicalName(l).toRight(DecodingFailure(s"Unknown SystemNamespaceLabel: $l", cursor.history))
          }
          .map(SystemNamespace(_))
      case other => Left(DecodingFailure(s"Unknown PartitionNamespace type: $other", cursor.history))
    }
  }
}

sealed trait GlobalStateFieldId {
  def toInt: Int
}

object GlobalStateFieldId {
  case object LastStateChannelSnapshotHashes extends GlobalStateFieldId { def toInt: Int = 0 }
  case object LastTxRefs extends GlobalStateFieldId { def toInt: Int = 1 }
  case object Balances extends GlobalStateFieldId { def toInt: Int = 2 }
  case object LastCurrencySnapshots extends GlobalStateFieldId { def toInt: Int = 3 }
  case object LastCurrencySnapshotsProofs extends GlobalStateFieldId { def toInt: Int = 4 }
  case object LastIncrementalCurrencySnapshots extends GlobalStateFieldId { def toInt: Int = 5 }
  case object LastCurrencySnapshotInfo extends GlobalStateFieldId { def toInt: Int = 6 }
  case object ActiveAllowSpends extends GlobalStateFieldId { def toInt: Int = 7 }
  case object ActiveTokenLocks extends GlobalStateFieldId { def toInt: Int = 8 }
  case object TokenLockBalances extends GlobalStateFieldId { def toInt: Int = 9 }
  case object LastAllowSpendRefs extends GlobalStateFieldId { def toInt: Int = 10 }
  case object LastTokenLockRefs extends GlobalStateFieldId { def toInt: Int = 11 }
  case object UpdateNodeParameters extends GlobalStateFieldId { def toInt: Int = 12 }
  case object ActiveDelegatedStakes extends GlobalStateFieldId { def toInt: Int = 13 }
  case object DelegatedStakesWithdrawals extends GlobalStateFieldId { def toInt: Int = 14 }
  case object ActiveNodeCollaterals extends GlobalStateFieldId { def toInt: Int = 15 }
  case object NodeCollateralWithdrawals extends GlobalStateFieldId { def toInt: Int = 16 }
  case object PriceState extends GlobalStateFieldId { def toInt: Int = 17 }
  case object MetagraphSyncData extends GlobalStateFieldId { def toInt: Int = 18 }

  /** Shared fieldId for all system-namespaced partitions (expiry indices, GC markers, etc). Discriminated by `SystemNamespaceLabel` in the
    * `networkNamespace` slot, not by a distinct fieldId.
    */
  case object SystemIndex extends GlobalStateFieldId { def toInt: Int = 19 }

  /** §3 NIPoPoW S0 stake-distribution snapshots, indexed by closing eta-period. One MPT entry per period under retention (last 4). Covered
    * by the global mptRoot — light clients verifying NIPoPoW level-µ chain superblock proofs read the historical distribution from this
    * partition and verify it against the snapshot's mptRoot.
    */
  case object HistoricalStakeSnapshots extends GlobalStateFieldId { def toInt: Int = 20 }

  /** §3 NIPoPoW S3 superblock-tower entries, indexed by `(level, ordinal)`. Used by the local per-node `MptTowerStore` — '''NOT''' part of
    * the global stateProof / `mptRoot`. The tower is a derived persistent index (see proposal §4.5): each node maintains its own copy by
    * replaying [[LevelTrialComputer.runAll]] on every finalized snapshot, so byte-equivalent tower bytes are not a producer/verifier
    * requirement and corruption recovery is a chain replay (not a state-proof rollback). The FieldId slot is reserved here for taxonomy /
    * `fieldIdFromHex` classification only; the tower lives in a dedicated MPT producer to keep its bytes out of consensus state.
    */
  case object TowerEntries extends GlobalStateFieldId { def toInt: Int = 21 }

  /** §1.2 Slice 10 KES runtime registration certs (#179). Per-operator full history of accepted certs, keyed by `peerId`, value
    * `SortedSet[KesRegistrationRecord]`. This is the durable source of truth that `MutableKesRegistry` materializes its overlay from across
    * node restarts and that newly-joining cluster peers replay from finalized state.
    *
    * Mirrors the `ActiveNodeCollaterals` shape: per-operator SortedSet of records, with cert chain ordering enforced by `parent`/`ordinal`
    * inside each record. Paired with `LastKesRegistrationRefs` for O(1) lookup of the latest accepted cert per peer.
    */
  case object KesRegistrationCerts extends GlobalStateFieldId { def toInt: Int = 22 }

  /** §1.2 Slice 10 last accepted `KesRegistrationReference` per operator (#179). Keyed by `peerId`, value `KesRegistrationReference`.
    * Provides O(1) chain-link lookup for the validator (`parent` matching) and identifies the head of the per-peer cert history without
    * needing to scan the full `KesRegistrationCerts` set. Mirrors the `LastTxRefs` / `LastAllowSpendRefs` pattern.
    */
  case object LastKesRegistrationRefs extends GlobalStateFieldId { def toInt: Int = 23 }

  /** Slice 17 — per-(shard, peer, epoch) non-participation accumulator (see `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md`
    * §10.3). One MPT entry per `(shardId, peerId, epoch)` triple carrying a
    * [[io.constellationnetwork.schema.sharding.ShardNonParticipationCounter]] — the running tally of missed slot-leader and missed
    * attestation duties. Written by `ShardNonParticipationStateManager` during the shard's `accept`/attestation paths; read at gl0 epoch
    * boundary by `ShardNonParticipationSlasher` to produce the slash list for the just-closed epoch.
    *
    * '''Why a hypergraph-namespaced field, not system-namespaced.''' The counter is user-addressable in the sense that operator tooling
    * needs to query it (operators want to know "am I close to being slashed for non-participation?"). System-namespaced partitions are
    * reserved for derived/internal indices; a per-peer ledger-relevant counter is closer to `ActiveDelegatedStakes` (per-peer state) than
    * to `ExpiryIndexTokenLocks` (derived index). Following the pattern lets the standard hypergraph key constructors apply.
    */
  case object ShardNonParticipation extends GlobalStateFieldId { def toInt: Int = 24 }

  /** Per-metagraph UNROLLED `CurrencySnapshotInfo` sub-fields (`docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md`). These REPLACE the
    * monolithic `LastCurrencySnapshotInfo` blob (fieldId 6): instead of one `metagraph(mgAddr, LastCurrencySnapshotInfo) ->
    * CurrencySnapshotInfo` key per MG (O(N) rewrite on any change), each `CurrencySnapshotInfo` field becomes per-ENTRY keys under the MG's
    * own `MetagraphNamespace` — `metagraphEntry(mgAddr, MgXxx, entryKey) -> value` — so the per-ordinal MPT diff and the committee
    * state-diff are O(changed entries), not O(N) (scalability is the primary driver — the blob is a state-diff dead-end). The 8 sub-fields
    * cover all of `CurrencySnapshotInfo` EXCEPT `activeAllowSpends`, which stays in the existing `ActiveAllowSpends` (fieldId 7)
    * metagraph-scope partition (already per-MG unrolled, already read cross-shard by `SpendActionValidator`). `infoRoot` (in
    * `CurrencySnapshotMptRoots`) is the single MPT root over the UNION of these 8 sub-field partitions (producer + follower compute it
    * identically via `currencySnapshotEntryBytes`).
    */
  case object MgBalances extends GlobalStateFieldId { def toInt: Int = 25 }
  case object MgLastTxRefs extends GlobalStateFieldId { def toInt: Int = 26 }
  case object MgLastFeeTxRefs extends GlobalStateFieldId { def toInt: Int = 27 }
  case object MgLastAllowSpendRefs extends GlobalStateFieldId { def toInt: Int = 28 }
  case object MgLastTokenLockRefs extends GlobalStateFieldId { def toInt: Int = 29 }
  case object MgActiveTokenLocks extends GlobalStateFieldId { def toInt: Int = 30 }
  case object MgLastMessages extends GlobalStateFieldId { def toInt: Int = 31 }
  case object MgGlobalSnapshotSyncView extends GlobalStateFieldId { def toInt: Int = 32 }

  /** Cross-shard single-use SPENT-SET for consumed allow-spends — the global, hypergraph-namespaced "nullifier" partition keyed by the
    * allow-spend '''hash'''. Where [[ActiveAllowSpends]] (fieldId 7) holds the still-spendable allow-spends keyed by source `Address`
    * (`SortedSet[Signed[AllowSpend]]` per `(Option[metagraph], source)`), this partition records that a specific allow-spend has ALREADY
    * been consumed, so a cross-shard SPEND cannot replay it on a second shard. The marker is keyed by the allow-spend's content `Hash` (the
    * single-use identity that is stable across shards), '''not''' by the holder address — hence the dedicated
    * [[GlobalStateKey.consumedAllowSpendKey]] hash-in-user-slot constructor rather than the `Address`-keyed `hypergraph` builders that
    * `ActiveAllowSpends` uses.
    *
    * '''Value: a minimal presence/consuming-reference record.''' Membership alone is the single-use evidence; the value carries the minimal
    * consuming reference (e.g. the consuming spend/snapshot reference) for auditability, mirroring how `LastAllowSpendRefs` stores a small
    * `AllowSpendReference` rather than the full event. Append-only in spirit (a consumed hash never un-consumes), so the partition is a
    * monotonically growing set under retention by the eventual producer.
    *
    * '''Why hypergraph-namespaced (DAG-scoped), not system-namespaced.''' This is consensus-load-bearing single-use state read cross-shard
    * by the spend path — the exact role `ActiveAllowSpends` already plays — so it belongs in the consensus global `mptRoot`
    * ([[consensusRootEntries]] keeps it; it is neither a path-dependent `SystemNamespace` sidecar nor an observation-dependent `Mg*`
    * sub-field). SCHEMA + KEY PLUMBING ONLY: no producer writes and no consumer reads this partition yet — the acceptance-fold wiring that
    * populates/checks the spent-set is a separate later task.
    */
  case object ConsumedAllowSpends extends GlobalStateFieldId { def toInt: Int = 33 }

  /** The unrolled per-metagraph `CurrencySnapshotInfo` sub-fields whose UNION is committed by `CurrencySnapshotMptRoots.infoRoot`. Used by
    * `GlobalStateConverter.currencySnapshotFieldRoots` / `GlobalSnapshotInfo.mptStateProofFromBytes` to group these entries into the single
    * `infoRoot` (replacing the `fieldId == LastCurrencySnapshotInfo` filter). MUST stay in sync with the `Mg*` case objects above.
    *
    * '''`MgGlobalSnapshotSyncView` (field 32) is DELIBERATELY EXCLUDED''' (cause-2, the sharded-mirror m1 freeze). The per-peer
    * `globalSnapshotSyncView` is OBSERVATION-DEPENDENT: the metagraph producer accumulates it under the FULL consensus committee, a
    * re-deriving gl0 verifier (and the currency-layer follower) under only the 2/3 signers (#259), so honest nodes hold DIFFERENT per-peer
    * maps. In the shard-checkpoint diff/apply that drift is UNRECONCILABLE — a removals-free minimal diff cannot evict a peer present only
    * in the follower's prior, so the verifier's recomputed `infoRoot` mismatches the committee-attested root EVERY ordinal and the MG
    * freezes out of gl0 adoption forever (run-2x: gl0 ord 465+, `diff(upserts=1,removals=0)`, attested≠recomputed). gl0 does NOT consume a
    * metagraph's view of gl0-syncs, so the field is excluded from the consensus root (the #116 pattern: path-dependent state stays STORED +
    * diffed + reconstructed, but leaves the root). The metagraph's OWN `CurrencySnapshotInfo.stateProof` still commits to it independently.
    * See `CurrencyDiffRoundTripSuite` (freeze-repro + fix guard) and `ShardCheckpointWiring.reExecDerivationWithDiff`.
    */
  val infoSubFields: Set[GlobalStateFieldId] =
    Set(
      MgBalances,
      MgLastTxRefs,
      MgLastFeeTxRefs,
      MgLastAllowSpendRefs,
      MgLastTokenLockRefs,
      MgActiveTokenLocks,
      MgLastMessages
    )

  implicit val ordering: Ordering[GlobalStateFieldId] = Ordering.by(_.toInt)
  implicit val show: Show[GlobalStateFieldId] = Show.show(_.toInt.toString)

  implicit val encoder: Encoder[GlobalStateFieldId] = Encoder[Int].contramap(_.toInt)
  implicit val decoder: Decoder[GlobalStateFieldId] = Decoder[Int].emap { i =>
    fromInt(i).toRight(s"Invalid GlobalStateFieldId int: $i")
  }

  def fromInt(i: Int): Option[GlobalStateFieldId] = i match {
    case 0  => Some(LastStateChannelSnapshotHashes)
    case 1  => Some(LastTxRefs)
    case 2  => Some(Balances)
    case 3  => Some(LastCurrencySnapshots)
    case 4  => Some(LastCurrencySnapshotsProofs)
    case 5  => Some(LastIncrementalCurrencySnapshots)
    case 6  => Some(LastCurrencySnapshotInfo)
    case 7  => Some(ActiveAllowSpends)
    case 8  => Some(ActiveTokenLocks)
    case 9  => Some(TokenLockBalances)
    case 10 => Some(LastAllowSpendRefs)
    case 11 => Some(LastTokenLockRefs)
    case 12 => Some(UpdateNodeParameters)
    case 13 => Some(ActiveDelegatedStakes)
    case 14 => Some(DelegatedStakesWithdrawals)
    case 15 => Some(ActiveNodeCollaterals)
    case 16 => Some(NodeCollateralWithdrawals)
    case 17 => Some(PriceState)
    case 18 => Some(MetagraphSyncData)
    case 19 => Some(SystemIndex)
    case 20 => Some(HistoricalStakeSnapshots)
    case 21 => Some(TowerEntries)
    case 22 => Some(KesRegistrationCerts)
    case 23 => Some(LastKesRegistrationRefs)
    case 24 => Some(ShardNonParticipation)
    case 25 => Some(MgBalances)
    case 26 => Some(MgLastTxRefs)
    case 27 => Some(MgLastFeeTxRefs)
    case 28 => Some(MgLastAllowSpendRefs)
    case 29 => Some(MgLastTokenLockRefs)
    case 30 => Some(MgActiveTokenLocks)
    case 31 => Some(MgLastMessages)
    case 32 => Some(MgGlobalSnapshotSyncView)
    case 33 => Some(ConsumedAllowSpends)
    case _  => None
  }
}

@derive(encoder, decoder, eqv, show, order)
case class GlobalStateKey(
  networkNamespace: PartitionNamespace,
  fieldId: GlobalStateFieldId,
  contractNamespace: PartitionNamespace,
  userNamespace: PartitionNamespace
)

object GlobalStateKey {

  def metagraph(addr: Address, fieldId: GlobalStateFieldId): GlobalStateKey =
    GlobalStateKey(MetagraphNamespace(addr), fieldId, EmptyNamespace, EmptyNamespace)

  /** Per-entry key into an UNROLLED per-metagraph `CurrencySnapshotInfo` sub-field (`GlobalStateFieldId.infoSubFields`, see
    * `docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md`). The MG owns the partition (`MetagraphNamespace` in the network slot); the
    * per-entry account/holder goes in the user slot — mirrors `hypergraph(field, contract, user)` but MG-scoped so it never collides with
    * the DAG-scoped global partitions. One MPT entry per `(mgAddr, subField, account)`.
    *
    * '''Value carries the entry key.''' `toHex` HASHES `AddressNamespace(account)` (it is lossy — you cannot recover the account from the
    * key), so the stored VALUE is `(Address, V)` and reconstruction recovers the logical key from the value via a prefix scan (the same
    * value-carries-key pattern as `getAllUpdateNodeParameters`). The user-slot hash only provides per-entry uniqueness.
    */
  def metagraphEntry(mgAddr: Address, subField: GlobalStateFieldId, account: Address): GlobalStateKey =
    GlobalStateKey(MetagraphNamespace(mgAddr), subField, EmptyNamespace, AddressNamespace(account))

  /** As [[metagraphEntry]] for an unrolled sub-field whose entry key is NOT an `Address` (`MgLastMessages` → `MessageType`,
    * `MgGlobalSnapshotSyncView` → `PeerId`): the canonical-string entry key is hashed into the user-namespace slot for uniqueness (same as
    * `updateNodeParametersKey`/`priceStateKey`). The stored value still carries the typed key for reconstruction.
    */
  def metagraphEntryHashed[F[_]: Sync: Hasher](mgAddr: Address, subField: GlobalStateFieldId, entryKey: String): F[GlobalStateKey] =
    Hasher[F].hash(entryKey).map { h =>
      GlobalStateKey(MetagraphNamespace(mgAddr), subField, EmptyNamespace, HashNamespace(h))
    }

  /** Hex prefix matching every per-entry key under one unrolled per-metagraph sub-field for `mgAddr` (pairs with `MptStore.getAllForPrefix`
    * to reconstruct that field's full map). Layout: `<metagraph keyType+addrHash> + <subField 8 hex> + <empty contract (00)>` — stops short
    * of the user-namespace component, so it matches every `(account)` entry under that `(mgAddr, subField)` pair.
    */
  def metagraphFieldPrefix[F[_]: Sync: Hasher](mgAddr: Address, subField: GlobalStateFieldId): F[Hex] =
    for {
      networkPart <- serializeNamespace[F](MetagraphNamespace(mgAddr))
      fieldPart = f"${subField.toInt}%08x"
      contractPart <- serializeNamespace[F](EmptyNamespace)
    } yield Hex(networkPart + fieldPart + contractPart)

  def hypergraph(fieldId: GlobalStateFieldId, user: Address): GlobalStateKey =
    GlobalStateKey(HypergraphNamespace, fieldId, EmptyNamespace, AddressNamespace(user))

  def hypergraph(fieldId: GlobalStateFieldId, contract: Address, user: Address): GlobalStateKey =
    GlobalStateKey(HypergraphNamespace, fieldId, AddressNamespace(contract), AddressNamespace(user))

  def hypergraph(fieldId: GlobalStateFieldId, contract: Option[Address], user: Address): GlobalStateKey =
    GlobalStateKey(
      HypergraphNamespace,
      fieldId,
      contract.map(MetagraphNamespace(_)).getOrElse(EmptyNamespace),
      AddressNamespace(user)
    )

  /** Hypergraph key whose user-namespace component carries a pre-computed hash of an `Id` (hex of a public key). Used for the
    * `UpdateNodeParameters` partition which is keyed by `Id`, not `Address`.
    */
  def updateNodeParametersKey[F[_]: Sync: Hasher](id: Id): F[GlobalStateKey] =
    Hasher[F].hash(id.hex.value).map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.UpdateNodeParameters, EmptyNamespace, HashNamespace(h))
    }

  /** Hypergraph key into the `KesRegistrationCerts` partition. Keyed by `peerId` (hex of operator's long-term pubkey) hashed into the
    * user-namespace slot. One MPT entry per operator carrying the full `SortedSet[KesRegistrationRecord]` history (latest at head by
    * `acceptedAt + ordinal`).
    */
  def kesRegistrationCertsKey[F[_]: Sync: Hasher](peerId: PeerId): F[GlobalStateKey] =
    Hasher[F].hash(peerId.value.value).map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.KesRegistrationCerts, EmptyNamespace, HashNamespace(h))
    }

  /** Hypergraph key into the `LastKesRegistrationRefs` partition. Keyed by `peerId`. Value is the latest accepted
    * `KesRegistrationReference` for the operator — provides O(1) chain-link lookup for the validator and the runtime registry without
    * scanning the full cert history.
    */
  def lastKesRegistrationRefsKey[F[_]: Sync: Hasher](peerId: PeerId): F[GlobalStateKey] =
    Hasher[F].hash(peerId.value.value).map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.LastKesRegistrationRefs, EmptyNamespace, HashNamespace(h))
    }

  /** Hypergraph key whose user-namespace component carries a pre-computed hash of a `TokenPair`. Used for the `PriceState` partition which
    * is keyed by `TokenPair`, not `Address`.
    */
  def priceStateKey[F[_]: Sync: Hasher](tokenPair: TokenPair): F[GlobalStateKey] =
    Hasher[F].hash(s"${tokenPair.base}/${tokenPair.quote}").map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.PriceState, EmptyNamespace, HashNamespace(h))
    }

  /** Key into a system-namespaced epoch-bucketed index. `userNamespace` carries a hash of the epoch's canonical string form. */
  def expiryIndexKey[F[_]: Sync: Hasher](label: SystemNamespaceLabel, epoch: EpochProgress): F[GlobalStateKey] =
    Hasher[F].hash(epoch.show).map { h =>
      GlobalStateKey(SystemNamespace(label), GlobalStateFieldId.SystemIndex, EmptyNamespace, HashNamespace(h))
    }

  /** Hex prefix that every `SystemNamespace` (sidecar) entry carries: the `PKTSystem` keyType byte `0x03` serialized as `"03"` at offset 0
    * (see `PartitionKeyType.PKTSystem` and `toHex`). Sidecar partitions — `ActiveAddressIndex`, the AllowSpend / TokenLock / NodeCollateral
    * expiry buckets — are the ONLY partitions under this prefix; all user-field partitions are `00`/`01`/`02`.
    */
  val systemNamespaceHexPrefix: String = "03"

  /** True iff `hex` is a `SystemNamespace` (sidecar) entry. O(1) prefix check — no fieldId parse.
    *
    * '''Consensus contract''': sidecar entries are local read-acceleration indices, NOT consensus state. The `ActiveAddressIndex` partition
    * in particular is maintained '''append-only''' on the incremental accept path (`applyActiveAddressIndexDelta` is always called with
    * `removed = Set.empty`), so its contents are a function of the per-ordinal delta '''history''', not of the current KV state — two
    * honest nodes that processed different (but equivalent-final) ordinal streams accumulate different index sets, and a rebuild from
    * current keysets yields yet another value. Folding such a path-dependent partition into the consensus global `mptRoot` makes the root
    * non-deterministic across nodes (surfaces as `stateProof[mptRoot]`-only divergence: every per-field proof matches because sidecars have
    * no per-field proof slot, but the global root differs). The global root MUST therefore exclude every `SystemNamespace` entry — use
    * `nonSystemNamespaceEntries` at every consensus-root computation site.
    */
  def isSystemNamespaceHex(hex: Hex): Boolean =
    hex.value.startsWith(systemNamespaceHexPrefix)

  /** Drop every `SystemNamespace` (sidecar) entry from a hex-keyed byte map. The surviving entries are exactly the user-field partitions
    * that constitute the consensus state. Apply this immediately before any global-`mptRoot` `makeParallelFromBytes` so the root is a pure
    * function of the user-field KV set (see `isSystemNamespaceHex`).
    */
  def nonSystemNamespaceEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    entries.filterNot { case (hex, _) => isSystemNamespaceHex(hex) }

  /** The entry set that constitutes the CONSENSUS global `mptRoot`: [[nonSystemNamespaceEntries]] (drop path-dependent SystemNamespace
    * sidecars) MINUS the observation-dependent per-metagraph `MgGlobalSnapshotSyncView` (fieldId 32).
    *
    * '''Why also drop field 32.''' `globalSnapshotSyncView` is a per-peer `Signed[GlobalSnapshotSync]` map the metagraph producer
    * accumulates under the FULL consensus committee, whereas a re-deriving gl0 node sees only the 2/3 signers (#259 / cause-2). Honest
    * nodes therefore hold DIFFERENT field-32 byte sets, and — unlike every other `Mg*` field — it has NO per-field proof slot
    * (`GlobalStateFieldId.infoSubFields` excludes it), so a divergence surfaces ONLY in the global `mptRoot` (every per-field root
    * matches). Folding it into the consensus root makes the root non-deterministic across nodes: it froze the sharded per-MG adoption (the
    * cause-2 ADOPT-VERIFY freeze, since fixed by excluding it from `infoSubfields`) AND — because the producer commits `mptRoot` from the
    * overlay `postBytes` while the Tier-3 catch-up gate re-encodes from `info.allStateEntriesAsBytes` — it makes a lagging node's catch-up
    * stateProof check ALWAYS mismatch on `mptRoot`, wedging recovery forever. gl0 never consumes a metagraph's view of gl0-syncs, so the
    * field stays STORED + diffed + reconstructed (the mirror is unchanged) but leaves the consensus root. The metagraph's OWN
    * `CurrencySnapshotInfo.stateProof` still commits to it. Apply at EVERY global-`mptRoot` site (producer + follower + catch-up + the #107
    * self-check) so producer and verifier compute the byte-identical root. See `infoSubFields`.
    */
  def consensusRootEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    nonSystemNamespaceEntries(entries).filterNot {
      case (hex, _) => fieldIdFromHex(hex).contains(GlobalStateFieldId.MgGlobalSnapshotSyncView)
    }

  /** Slice 17 — key into the per-(shard, peer, epoch) [[GlobalStateFieldId.ShardNonParticipation]] partition. The composite tuple is folded
    * into a single hash so each `(shardId, peerId, epoch)` triple maps to one MPT entry under the hypergraph namespace.
    *
    * '''Why one composite hash rather than three nested namespaces.''' `GlobalStateKey` has exactly four slots (network, field, contract,
    * user); the natural layout for this partition would be three keyed slots (shard, peer, epoch) plus the field. Folding to one composite
    * hash keeps the field's MPT root scannable with a single prefix (`hypergraphFieldPrefix(ShardNonParticipation)`) for the slasher's
    * `materializeAllForEpoch` and avoids forcing structural changes to `GlobalStateKey`.
    *
    * '''Per-epoch filtering at materialize-time.''' Because all `(shardId, peerId, epoch)` triples share one prefix, the slasher's
    * per-epoch scan filters the prefix-scan results by `counter.epoch === closedEpoch` rather than narrowing the prefix. This is fine for
    * the expected partition size (at most `numShards * |operators| * retentionEpochs` entries ≈ 4 × 100 × 4 = 1600 in v1).
    */
  def shardNonParticipationKey[F[_]: Sync: Hasher](
    shardId: ShardId,
    peerId: PeerId,
    epoch: EtaPeriod
  ): F[GlobalStateKey] =
    Hasher[F].hash(s"${shardId.value.value}|${peerId.value.value}|${epoch.value}").map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.ShardNonParticipation, EmptyNamespace, HashNamespace(h))
    }

  /** Key into the §3 NIPoPoW historical-stake-snapshots partition. `userNamespace` carries a hash of the eta-period's canonical string
    * form. One MPT entry per stored period (last 4 under retention).
    */
  def historicalStakeSnapshotsKey[F[_]: Sync: Hasher](period: EtaPeriod): F[GlobalStateKey] =
    Hasher[F].hash(period.value.toString).map { h =>
      GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.HistoricalStakeSnapshots, EmptyNamespace, HashNamespace(h))
    }

  /** Key into the [[GlobalStateFieldId.ConsumedAllowSpends]] cross-shard single-use spent-set, keyed by the allow-spend's content `Hash`.
    * The hash IS the single-use identity (stable across shards), so it goes DIRECTLY into the user-namespace `HashNamespace` slot — unlike
    * `priceStateKey` / `kesRegistrationCertsKey`, which `Hasher[F].hash(...)` a canonical-string identity first. `HashNamespace` serializes
    * its hash verbatim in `serializeNamespace` (no re-hash), so the marker for a given allow-spend is reproducible from its hash alone,
    * with no `F[_]`/`Hasher` needed. Mirrors the `ActiveAllowSpends` (fieldId 7) hypergraph scope but addressed by hash rather than by
    * source `Address`. SCHEMA/KEY PLUMBING ONLY — no producer/consumer is wired yet (separate later task).
    */
  def consumedAllowSpendKey(allowSpendHash: Hash): GlobalStateKey =
    GlobalStateKey(HypergraphNamespace, GlobalStateFieldId.ConsumedAllowSpends, EmptyNamespace, HashNamespace(allowSpendHash))

  /** Key into the `ActiveAddressIndex` partition for a given user-visible field. `userNamespace` carries a hash of the fieldId's integer
    * code, so each field gets its own MPT entry under the same partition.
    */
  def activeAddressIndexKey[F[_]: Sync: Hasher](fieldId: GlobalStateFieldId): F[GlobalStateKey] =
    Hasher[F].hash(fieldId.toInt.toString).map { h =>
      GlobalStateKey(
        SystemNamespace(SystemNamespaceLabel.ActiveAddressIndex),
        GlobalStateFieldId.SystemIndex,
        EmptyNamespace,
        HashNamespace(h)
      )
    }

  def toHex[F[_]: Sync: Hasher](key: GlobalStateKey): F[Hex] =
    for {
      networkPart <- serializeNamespace[F](key.networkNamespace)
      fieldPart <- f"${key.fieldId.toInt}%08x".pure[F]
      contractPart <- serializeNamespace[F](key.contractNamespace)
      userPart <- serializeNamespace[F](key.userNamespace)
      serialized = networkPart + fieldPart + contractPart + userPart
    } yield Hex(serialized)

  /** Inverse of `toHex` over the network-namespace + fieldPart prefix only. Reads the keyType byte at offset 0, skips the namespace's hash
    * payload (none for `PKTHypergraph`, 64 hex chars for the hashed types) and parses the next 8 hex chars as a fieldId integer. Returns
    * `None` if the hex is malformed or the integer doesn't resolve to a `GlobalStateFieldId`. Used by overlay-derived per-field root
    * construction (#56.10 Phase J), which has hex-keyed bytes instead of typed `GlobalStateKey`s and needs to group by `fieldId` without
    * round-tripping through `GlobalStateKey`.
    */
  def fieldIdFromHex(hex: Hex): Option[GlobalStateFieldId] = {
    val s = hex.value
    if (s.length < 2) None
    else {
      val keyTypeOffset = 2
      // PKTHypergraph (0x00) carries no hash — fieldPart is at offset 2.
      // PKTAddress / PKTHash / PKTSystem all carry a 32-byte hash → 64 hex chars → fieldPart at offset 2 + 64.
      val fieldStart = s.substring(0, keyTypeOffset) match {
        case "00" => keyTypeOffset
        case _    => keyTypeOffset + 64
      }
      val fieldEnd = fieldStart + 8
      if (s.length < fieldEnd) None
      else
        scala.util
          .Try(java.lang.Integer.parseInt(s.substring(fieldStart, fieldEnd), 16))
          .toOption
          .flatMap(GlobalStateFieldId.fromInt)
    }
  }

  /** Hex prefix matching every key in the hypergraph network for the given `fieldId`, optionally scoped to a specific contract address.
    * Pair with `MptStore.getAllForPrefix` to materialize a per-field view without an external address set.
    *
    * Layout: `<hypergraph keyType byte (00)> + <fieldId 8 hex> [+ <contract namespace>]`. Stops short of the user-namespace component, so
    * the prefix matches every (user-keyed) entry under that field/contract pair.
    */
  def hypergraphFieldPrefix[F[_]: Sync: Hasher](
    fieldId: GlobalStateFieldId,
    contract: Option[Address] = None
  ): F[Hex] =
    for {
      networkPart <- serializeNamespace[F](HypergraphNamespace)
      fieldPart = f"${fieldId.toInt}%08x"
      contractPart <- serializeNamespace[F](contract.map(MetagraphNamespace(_)).getOrElse(EmptyNamespace))
    } yield Hex(networkPart + fieldPart + contractPart)

  /** Hex prefix matching every entry under `fieldId` regardless of contract scope (global + per-metagraph). Useful for fields like
    * `ActiveAllowSpends` whose runtime view is `SortedMap[Option[Address], ...]` collapsing all contract scopes into a single scan. Layout:
    * `<hypergraph (00)> + <fieldId 8 hex>`.
    */
  def hypergraphFieldPrefixAcrossContracts[F[_]: Sync: Hasher](
    fieldId: GlobalStateFieldId
  ): F[Hex] =
    for {
      networkPart <- serializeNamespace[F](HypergraphNamespace)
      fieldPart = f"${fieldId.toInt}%08x"
    } yield Hex(networkPart + fieldPart)

  private def serializeNamespace[F[_]: Sync: Hasher](ns: PartitionNamespace): F[String] =
    ns match {
      case HypergraphNamespace =>
        f"${ns.keyType.toByte}%02x".pure[F]
      case EmptyNamespace =>
        f"${ns.keyType.toByte}%02x".pure[F]
      case MetagraphNamespace(addr) =>
        Hasher[F].hash(addr.value.value).map(h => f"${ns.keyType.toByte}%02x" + h.value)
      case AddressNamespace(addr) =>
        Hasher[F].hash(addr.value.value).map(h => f"${ns.keyType.toByte}%02x" + h.value)
      case HashNamespace(hash) =>
        (f"${ns.keyType.toByte}%02x" + hash.value).pure[F]
      case SystemNamespace(label) =>
        Hasher[F].hash(label.canonicalName).map(h => f"${ns.keyType.toByte}%02x" + h.value)
    }
}
