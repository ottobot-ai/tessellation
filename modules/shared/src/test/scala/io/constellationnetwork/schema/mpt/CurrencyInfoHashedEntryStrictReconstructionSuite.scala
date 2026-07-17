package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.globalSnapshotSync.{GlobalSnapshotSync, GlobalSnapshotSyncOrdinal}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.cluster.SessionToken
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.mpt.GlobalStateConverter.CurrencyInfoReader
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import scodec.bits.ByteVector
import weaver.MutableIOSuite

object CurrencyInfoHashedEntryStrictReconstructionSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], Address, Address, Address)

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(sp: SecurityProvider[IO]) = securityProvider
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      metagraphA <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      metagraphB <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      account <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
    } yield (hasher, securityProvider, jsonSerializer, metagraphA, metagraphB, account)

  private type MessageEntry = (MessageType, Signed[CurrencyMessage])
  private type SyncEntry = (PeerId, Signed[GlobalSnapshotSync])

  private val peerA = PeerId(Hex("11" * 64))
  private val peerB = PeerId(Hex("22" * 64))

  private def proofs(peerId: PeerId, signatureByte: String): NonEmptySet[SignatureProof] =
    NonEmptySet.one(SignatureProof(Id(peerId.value), Signature(Hex(signatureByte * 64))))

  private def message(
    messageType: MessageType,
    account: Address,
    metagraph: Address,
    signer: PeerId = peerA
  ): Signed[CurrencyMessage] =
    Signed(
      CurrencyMessage(messageType, account, metagraph, MessageOrdinal(NonNegLong.unsafeFrom(0L))),
      proofs(signer, "ab")
    )

  private def sync(signer: PeerId): Signed[GlobalSnapshotSync] =
    Signed(
      GlobalSnapshotSync(
        parentOrdinal = GlobalSnapshotSyncOrdinal(NonNegLong.unsafeFrom(0L)),
        globalSnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(10L)),
        globalSnapshotHash = io.constellationnetwork.security.hash.Hash("cd" * 32),
        session = SessionToken(Generation(PosLong.unsafeFrom(1L)))
      ),
      proofs(signer, "ef")
    )

  private def encoded[V: ImmutableCodec](value: V): Array[Byte] =
    ImmutableCodec[V].immutableBytes(value).toArray

  private def rawReader(entries: List[(Hex, Array[Byte])]): CurrencyInfoReader[IO] = new CurrencyInfoReader[IO] {
    private val physicalEntries = entries.toMap

    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)

    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      IO.pure(
        StrictMptRead.decodeEntries[V](physicalEntries.filter { case (key, _) => key.value.startsWith(prefix.value) })
      )
  }

  private def strictReader(entries: List[StrictMptEntry[Any]]): CurrencyInfoReader[IO] = new CurrencyInfoReader[IO] {
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)

    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      IO.pure(
        entries
          .filter(_.physicalKey.value.startsWith(prefix.value))
          .map(_.asInstanceOf[StrictMptEntry[V]])
      )
  }

  private def anyEntry[V](physicalKey: Hex, read: StrictMptRead[V]): StrictMptEntry[Any] =
    StrictMptEntry(physicalKey, read)

  private def reconstruct(
    metagraph: Address,
    reader: CurrencyInfoReader[IO]
  )(implicit hasher: Hasher[IO]) =
    GlobalStateConverter.reconstructCurrencyInfoFrom[IO](metagraph, reader)

  private def messageKey(metagraph: Address, messageType: MessageType)(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey
      .metagraphEntryHashed[IO](metagraph, GlobalStateFieldId.MgLastMessages, messageType.value)
      .flatMap(GlobalStateKey.toHex[IO])

  private def syncKey(metagraph: Address, peerId: PeerId)(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey
      .metagraphEntryHashed[IO](metagraph, GlobalStateFieldId.MgGlobalSnapshotSyncView, peerId.value.value)
      .flatMap(GlobalStateKey.toHex[IO])

  private def wrongContractKey(
    metagraph: Address,
    contract: Address,
    field: GlobalStateFieldId,
    semanticKey: String
  )(implicit hasher: Hasher[IO]): IO[Hex] =
    Hasher[IO].hash(semanticKey).flatMap { digest =>
      GlobalStateKey.toHex[IO](
        GlobalStateKey(MetagraphNamespace(metagraph), field, AddressNamespace(contract), HashNamespace(digest))
      )
    }

  private def inconsistentAt(result: Either[Throwable, Any], physicalKey: Hex, clue: String): Boolean =
    result.left.exists {
      case error: StrictMptRead.InconsistentConsensusMptIndex =>
        error.physicalKey == physicalKey && error.getMessage.contains(clue)
      case _ => false
    }

  test("reconstructs canonical nonempty MgLastMessages and MgGlobalSnapshotSyncView entries") { res =>
    implicit val (hasher, _, _, metagraph, _, account) = res
    val messageEntry: MessageEntry = MessageType.Owner -> message(MessageType.Owner, account, metagraph)
    val syncEntry: SyncEntry = peerA -> sync(peerA)

    for {
      messageHex <- messageKey(metagraph, messageEntry._1)
      syncHex <- syncKey(metagraph, syncEntry._1)
      result <- reconstruct(
        metagraph,
        rawReader(List(messageHex -> encoded(messageEntry), syncHex -> encoded(syncEntry)))
      )
    } yield
      expect.all(
        result.lastMessages.contains(SortedMap(messageEntry)),
        result.globalSnapshotSyncView.contains(SortedMap(syncEntry))
      )
  }

  test("rejects A-key/B-value mismatches for both hashed fields") { res =>
    implicit val (hasher, _, _, metagraph, _, account) = res
    val messageB: MessageEntry = MessageType.Staking -> message(MessageType.Staking, account, metagraph)
    val syncB: SyncEntry = peerB -> sync(peerB)

    for {
      messageKeyA <- messageKey(metagraph, MessageType.Owner)
      syncKeyA <- syncKey(metagraph, peerA)
      messageResult <- reconstruct(metagraph, rawReader(List(messageKeyA -> encoded(messageB)))).attempt
      syncResult <- reconstruct(metagraph, rawReader(List(syncKeyA -> encoded(syncB)))).attempt
    } yield
      expect.all(
        inconsistentAt(messageResult, messageKeyA, "key/value mismatch"),
        inconsistentAt(syncResult, syncKeyA, "key/value mismatch")
      )
  }

  test("rejects wrong-contract and suffix placements for both hashed fields") { res =>
    implicit val (hasher, _, _, metagraph, contract, account) = res
    val messageEntry: MessageEntry = MessageType.Owner -> message(MessageType.Owner, account, metagraph)
    val syncEntry: SyncEntry = peerA -> sync(peerA)

    for {
      canonicalMessage <- messageKey(metagraph, messageEntry._1)
      canonicalSync <- syncKey(metagraph, syncEntry._1)
      wrongContractMessage <- wrongContractKey(
        metagraph,
        contract,
        GlobalStateFieldId.MgLastMessages,
        messageEntry._1.value
      )
      wrongContractSync <- wrongContractKey(
        metagraph,
        contract,
        GlobalStateFieldId.MgGlobalSnapshotSyncView,
        syncEntry._1.value.value
      )
      suffixMessage = Hex(canonicalMessage.value + "00")
      suffixSync = Hex(canonicalSync.value + "00")
      wrongContractMessageResult <- reconstruct(
        metagraph,
        rawReader(List(wrongContractMessage -> encoded(messageEntry)))
      ).attempt
      wrongContractSyncResult <- reconstruct(metagraph, rawReader(List(wrongContractSync -> encoded(syncEntry)))).attempt
      suffixMessageResult <- reconstruct(metagraph, rawReader(List(suffixMessage -> encoded(messageEntry)))).attempt
      suffixSyncResult <- reconstruct(metagraph, rawReader(List(suffixSync -> encoded(syncEntry)))).attempt
    } yield
      expect.all(
        inconsistentAt(wrongContractMessageResult, wrongContractMessage, "key/value mismatch"),
        inconsistentAt(wrongContractSyncResult, wrongContractSync, "key/value mismatch"),
        inconsistentAt(suffixMessageResult, suffixMessage, "key/value mismatch"),
        inconsistentAt(suffixSyncResult, suffixSync, "key/value mismatch")
      )
  }

  test("fails closed on strict Absent, Malformed, and noncanonical retained bytes") { res =>
    implicit val (hasher, _, _, metagraph, _, account) = res
    val messageEntry: MessageEntry = MessageType.Owner -> message(MessageType.Owner, account, metagraph)
    val syncEntry: SyncEntry = peerA -> sync(peerA)

    for {
      messageHex <- messageKey(metagraph, messageEntry._1)
      syncHex <- syncKey(metagraph, syncEntry._1)
      absentMessage <- reconstruct(
        metagraph,
        strictReader(List(anyEntry(messageHex, StrictMptRead.Absent)))
      ).attempt
      absentSync <- reconstruct(metagraph, strictReader(List(anyEntry(syncHex, StrictMptRead.Absent)))).attempt
      malformedMessage <- reconstruct(
        metagraph,
        strictReader(List(anyEntry(messageHex, StrictMptRead.Malformed("test malformed message bytes", None))))
      ).attempt
      malformedSync <- reconstruct(
        metagraph,
        strictReader(List(anyEntry(syncHex, StrictMptRead.Malformed("test malformed sync bytes", None))))
      ).attempt
      noncanonicalMessage <- reconstruct(
        metagraph,
        strictReader(List(anyEntry(messageHex, StrictMptRead.Present(messageEntry, ByteVector(0.toByte)))))
      ).attempt
      noncanonicalSync <- reconstruct(
        metagraph,
        strictReader(List(anyEntry(syncHex, StrictMptRead.Present(syncEntry, ByteVector(0.toByte)))))
      ).attempt
    } yield
      expect.all(
        absentMessage.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
        absentSync.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
        malformedMessage.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        malformedSync.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        noncanonicalMessage.left.exists(_.getMessage.contains("non-canonical value encoding")),
        noncanonicalSync.left.exists(_.getMessage.contains("non-canonical value encoding"))
      )
  }

  test("rejects duplicate logical hashed keys deterministically before map construction") { res =>
    implicit val (hasher, _, _, metagraph, _, account) = res
    val messageEntry: MessageEntry = MessageType.Owner -> message(MessageType.Owner, account, metagraph)
    val syncEntry: SyncEntry = peerA -> sync(peerA)

    for {
      messageHex <- messageKey(metagraph, messageEntry._1)
      syncHex <- syncKey(metagraph, syncEntry._1)
      duplicateMessageHex = Hex(messageHex.value + "00")
      duplicateSyncHex = Hex(syncHex.value + "00")
      messageRaw = mgLastMessagesEntryImmutableCodec.immutableBytes(messageEntry)
      syncRaw = mgGlobalSyncEntryImmutableCodec.immutableBytes(syncEntry)
      messageCanonical = anyEntry(messageHex, StrictMptRead.Present(messageEntry, messageRaw))
      messageDuplicate = anyEntry(duplicateMessageHex, StrictMptRead.Present(messageEntry, messageRaw))
      syncCanonical = anyEntry(syncHex, StrictMptRead.Present(syncEntry, syncRaw))
      syncDuplicate = anyEntry(duplicateSyncHex, StrictMptRead.Present(syncEntry, syncRaw))
      messageForward <- reconstruct(metagraph, strictReader(List(messageCanonical, messageDuplicate))).attempt
      messageReverse <- reconstruct(metagraph, strictReader(List(messageDuplicate, messageCanonical))).attempt
      syncForward <- reconstruct(metagraph, strictReader(List(syncCanonical, syncDuplicate))).attempt
      syncReverse <- reconstruct(metagraph, strictReader(List(syncDuplicate, syncCanonical))).attempt
    } yield
      expect.all(
        inconsistentAt(messageForward, duplicateMessageHex, "duplicate logical key"),
        messageForward.leftMap(_.getMessage) == messageReverse.leftMap(_.getMessage),
        inconsistentAt(syncForward, duplicateSyncHex, "duplicate logical key"),
        syncForward.leftMap(_.getMessage) == syncReverse.leftMap(_.getMessage)
      )
  }

  test("rejects MgLastMessages tuple message-type and embedded-metagraph mismatches") { res =>
    implicit val (hasher, _, _, metagraph, otherMetagraph, account) = res
    val wrongTypeEntry: MessageEntry = MessageType.Owner -> message(MessageType.Staking, account, metagraph)
    val wrongMetagraphEntry: MessageEntry = MessageType.Owner -> message(MessageType.Owner, account, otherMetagraph)

    for {
      messageHex <- messageKey(metagraph, MessageType.Owner)
      wrongType <- reconstruct(metagraph, rawReader(List(messageHex -> encoded(wrongTypeEntry)))).attempt
      wrongMetagraph <- reconstruct(metagraph, rawReader(List(messageHex -> encoded(wrongMetagraphEntry)))).attempt
    } yield
      expect.all(
        inconsistentAt(wrongType, messageHex, "message type mismatch"),
        inconsistentAt(wrongMetagraph, messageHex, "message metagraph mismatch")
      )
  }
}
