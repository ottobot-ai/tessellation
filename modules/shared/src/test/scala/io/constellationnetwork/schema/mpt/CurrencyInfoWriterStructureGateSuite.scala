package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.CurrencyInfoMpt
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.{Expectations, MutableIOSuite}

object CurrencyInfoWriterStructureGateSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], Address, Address, Address)

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(sp: SecurityProvider[IO]) = securityProvider
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      metagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      otherMetagraph <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
    } yield (hasher, securityProvider, jsonSerializer, metagraph, otherMetagraph, holder)

  private val peerA = PeerId(Hex("31" * 64))
  private val peerB = PeerId(Hex("42" * 64))

  private case class InvalidCase(name: String, info: CurrencySnapshotInfo, expectedError: String)

  private case class InvalidInputs(cases: List[InvalidCase], duplicateUnsignedSetSize: Int)

  private case class MutationLog(
    insertBatches: Vector[Set[GlobalStateKey]],
    removeBatches: Vector[List[GlobalStateKey]]
  )

  private object MutationLog {
    val empty: MutationLog = MutationLog(Vector.empty, Vector.empty)
  }

  private def proofs(peerId: PeerId, signatureByte: String): NonEmptySet[SignatureProof] =
    NonEmptySet.one(SignatureProof(Id(peerId.value), Signature(Hex(signatureByte * 64))))

  private def signedTokenLock(value: TokenLock, signer: PeerId, signatureByte: String): Signed[TokenLock] =
    Signed(value, proofs(signer, signatureByte))

  private def tokenLock(source: Address, currencyId: Option[CurrencyId]): TokenLock =
    TokenLock(
      source = source,
      amount = TokenLockAmount(PosLong.unsafeFrom(10L)),
      fee = TokenLockFee(NonNegLong.unsafeFrom(1L)),
      parent = TokenLockReference.empty,
      currencyId = currencyId,
      unlockEpoch = None,
      replaceTokenLockRef = None
    )

  private def signedMessage(
    messageType: MessageType,
    holder: Address,
    metagraph: Address
  ): Signed[CurrencyMessage] =
    Signed(
      CurrencyMessage(messageType, holder, metagraph, MessageOrdinal(NonNegLong.unsafeFrom(0L))),
      proofs(peerA, "53")
    )

  private val emptyInfo: CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  private def canonicalInfo(metagraph: Address, holder: Address): CurrencySnapshotInfo = {
    val lock = signedTokenLock(tokenLock(holder, CurrencyId(metagraph).some), peerA, "64")
    val message = signedMessage(MessageType.Owner, holder, metagraph)

    emptyInfo.copy(
      lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](MessageType.Owner -> message).some,
      activeTokenLocks = SortedMap(holder -> SortedSet(lock)).some
    )
  }

  private def invalidInputs(metagraph: Address, otherMetagraph: Address, holder: Address): InvalidInputs = {
    val validCurrency = CurrencyId(metagraph).some
    val duplicateValue = tokenLock(holder, validCurrency)
    val duplicateUnsignedSet = SortedSet(
      signedTokenLock(duplicateValue, peerA, "75"),
      signedTokenLock(duplicateValue, peerB, "86")
    )

    val cases = List(
      InvalidCase(
        "empty field-30 set",
        emptyInfo.copy(activeTokenLocks = SortedMap(holder -> SortedSet.empty[Signed[TokenLock]]).some),
        "empty token-lock set"
      ),
      InvalidCase(
        "field-30 source mismatch",
        emptyInfo.copy(
          activeTokenLocks = SortedMap(
            holder -> SortedSet(signedTokenLock(tokenLock(otherMetagraph, validCurrency), peerA, "97"))
          ).some
        ),
        "token-lock source/holder mismatch"
      ),
      InvalidCase(
        "field-30 native currency scope",
        emptyInfo.copy(
          activeTokenLocks = SortedMap(holder -> SortedSet(signedTokenLock(tokenLock(holder, None), peerA, "a8"))).some
        ),
        "token-lock currency scope mismatch"
      ),
      InvalidCase(
        "field-30 other-metagraph currency scope",
        emptyInfo.copy(
          activeTokenLocks = SortedMap(
            holder -> SortedSet(
              signedTokenLock(tokenLock(holder, CurrencyId(otherMetagraph).some), peerA, "b9")
            )
          ).some
        ),
        "token-lock currency scope mismatch"
      ),
      InvalidCase(
        "field-30 duplicate unsigned identity",
        emptyInfo.copy(activeTokenLocks = SortedMap(holder -> duplicateUnsignedSet).some),
        "duplicate unsigned token-lock identity"
      ),
      InvalidCase(
        "field-31 tuple message type mismatch",
        emptyInfo.copy(
          lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](
            MessageType.Owner -> signedMessage(MessageType.Staking, holder, metagraph)
          ).some
        ),
        "message type mismatch"
      ),
      InvalidCase(
        "field-31 embedded metagraph mismatch",
        emptyInfo.copy(
          lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](
            MessageType.Owner -> signedMessage(MessageType.Owner, holder, otherMetagraph)
          ).some
        ),
        "message metagraph mismatch"
      )
    )

    InvalidInputs(cases, duplicateUnsignedSet.size)
  }

  private def rejectedWith[A](result: Either[Throwable, A], expectedError: String): Boolean =
    result.left.exists {
      case error: StrictMptRead.InconsistentConsensusMptIndex => error.getMessage.contains(expectedError)
      case _                                                  => false
    }

  private def recordingMpt(ref: Ref[IO, MutationLog]): CurrencyInfoMpt[IO] = new CurrencyInfoMpt[IO] {
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)

    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] = IO.pure(List.empty)

    def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): IO[Unit] =
      ref.update(current => current.copy(insertBatches = current.insertBatches :+ entries.keySet))

    def remove(keys: List[GlobalStateKey]): IO[Unit] =
      ref.update(current => current.copy(removeBatches = current.removeBatches :+ keys))
  }

  private def foldExpectations[A](values: List[A])(run: A => IO[Expectations]): IO[Expectations] =
    values.foldLeft(IO.pure(Expectations.Helpers.success)) { (accIO, value) =>
      for {
        acc <- accIO
        next <- run(value)
      } yield acc.and(next)
    }

  test("infoEntryBytes and infoEntryJson reject every malformed field-30 and field-31 structure") { res =>
    implicit val (hasher, _, _, metagraph, otherMetagraph, holder) = res
    val inputs = invalidInputs(metagraph, otherMetagraph, holder)

    foldExpectations(inputs.cases) { invalid =>
      for {
        bytes <- GlobalStateConverter.infoEntryBytes[IO](metagraph, invalid.info).attempt
        json <- GlobalStateConverter.infoEntryJson[IO](metagraph, invalid.info).attempt
      } yield
        expect.all(
          clue(invalid.name -> rejectedWith(bytes, invalid.expectedError))._2,
          clue(invalid.name -> rejectedWith(json, invalid.expectedError))._2
        )
    }.map(expectation => expect(inputs.duplicateUnsignedSetSize == 2).and(expectation))
  }

  test("writeCurrencyInfo rejects malformed structure before any insert or remove mutation") { res =>
    implicit val (hasher, _, _, metagraph, otherMetagraph, holder) = res
    val inputs = invalidInputs(metagraph, otherMetagraph, holder)

    foldExpectations(inputs.cases) { invalid =>
      for {
        mutations <- Ref.of[IO, MutationLog](MutationLog.empty)
        result <- GlobalStateConverter
          .writeCurrencyInfo[IO](metagraph, invalid.info, emptyInfo, recordingMpt(mutations))
          .attempt
        observed <- mutations.get
      } yield
        expect.all(
          clue(invalid.name -> rejectedWith(result, invalid.expectedError))._2,
          clue(invalid.name -> observed)._2 == MutationLog.empty
        )
    }
  }

  test("canonical field-30 and field-31 structure is emitted and written") { res =>
    implicit val (hasher, _, _, metagraph, _, holder) = res
    val info = canonicalInfo(metagraph, holder)

    for {
      bytes <- GlobalStateConverter.infoEntryBytes[IO](metagraph, info)
      json <- GlobalStateConverter.infoEntryJson[IO](metagraph, info)
      mutations <- Ref.of[IO, MutationLog](MutationLog.empty)
      _ <- GlobalStateConverter.writeCurrencyInfo[IO](metagraph, info, emptyInfo, recordingMpt(mutations))
      observed <- mutations.get
      byteFields = bytes.iterator.map(_._1.fieldId).toSet
      jsonFields = json.iterator.map(_._1.fieldId).toSet
      insertedFields = observed.insertBatches.iterator.flatMap(_.iterator).map(_.fieldId).toSet
    } yield
      expect.all(
        byteFields == Set(GlobalStateFieldId.MgActiveTokenLocks, GlobalStateFieldId.MgLastMessages),
        jsonFields == byteFields,
        observed.insertBatches.size == 8,
        observed.removeBatches.isEmpty,
        insertedFields.contains(GlobalStateFieldId.MgActiveTokenLocks),
        insertedFields.contains(GlobalStateFieldId.MgLastMessages)
      )
  }
}
