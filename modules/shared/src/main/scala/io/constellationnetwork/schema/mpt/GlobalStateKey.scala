package io.constellationnetwork.schema.mpt

import cats.Show
import cats.effect.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.priceOracle.TokenPair
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
      case "metagraph"  => cursor.downField("address").as[String].map(s => MetagraphNamespace(Address.fromBytes(s.getBytes)))
      case "address"    => cursor.downField("address").as[String].map(s => AddressNamespace(Address.fromBytes(s.getBytes)))
      case "hash"       => cursor.downField("hash").as[String].map(s => HashNamespace(Hash(s)))
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
