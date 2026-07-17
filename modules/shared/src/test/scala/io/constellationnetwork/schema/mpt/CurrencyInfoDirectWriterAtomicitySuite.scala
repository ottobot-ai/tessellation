package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{CurrencyMessage, MessageOrdinal, MessageType}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.tokenLock.TokenLock
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{HashLogic, Hasher}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder
import weaver.{Expectations, MutableIOSuite}

object CurrencyInfoDirectWriterAtomicitySuite extends MutableIOSuite {

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, json)

  private type CurrencyArm = Either[
    Signed[CurrencySnapshot],
    (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)
  ]

  private case class InvalidCase(name: String, info: CurrencySnapshotInfo, expectedError: String)

  private val ordinal = SnapshotOrdinal(NonNegLong(11L))
  private val orderedMetagraphs =
    List(address("direct-writer-mg-a"), address("direct-writer-mg-b")).sorted
  private val firstMetagraph = orderedMetagraphs.head
  private val secondMetagraph = orderedMetagraphs.last
  private val holder = address("direct-writer-holder")
  private val existingOwner = address("direct-writer-existing-owner")
  private val unrelatedOwner = address("direct-writer-unrelated-owner")
  private val proof = SignatureProof(Id(Hex("31" * 64)), Signature(Hex("42" * 70)))

  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  private def incremental(snapshotOrdinal: Long): Signed[CurrencyIncrementalSnapshot] =
    Signed(
      CurrencyIncrementalSnapshot(
        ordinal = SnapshotOrdinal.unsafeApply(snapshotOrdinal),
        height = Height.MinValue,
        subHeight = SubHeight.MinValue,
        lastSnapshotHash = Hash.empty,
        blocks = SortedSet.empty,
        rewards = SortedSet.empty,
        tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
        stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
        epochProgress = EpochProgress.MinValue,
        dataApplication = None,
        messages = None,
        globalSnapshotSyncs = None,
        feeTransactions = None,
        artifacts = None,
        allowSpendBlocks = None,
        tokenLockBlocks = None,
        globalSyncView = None
      ),
      NonEmptySet.one(proof)
    )

  private val emptyInfo = CurrencySnapshotInfo(
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

  private val validFirstInfo =
    emptyInfo.copy(balances = SortedMap(holder -> Balance(NonNegLong(5L))))

  private def signedMessage(messageType: MessageType): Signed[CurrencyMessage] =
    Signed(
      CurrencyMessage(messageType, holder, secondMetagraph, MessageOrdinal(NonNegLong(0L))),
      NonEmptySet.one(proof)
    )

  private val invalidCases = List(
    InvalidCase(
      "field-30 empty token-lock set",
      emptyInfo.copy(activeTokenLocks = SortedMap(holder -> SortedSet.empty[Signed[TokenLock]]).some),
      "empty token-lock set"
    ),
    InvalidCase(
      "field-31 tuple message-type mismatch",
      emptyInfo.copy(
        lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](
          MessageType.Owner -> signedMessage(MessageType.Staking)
        ).some
      ),
      "message type mismatch"
    )
  )

  private val validSecondMessageInfo =
    emptyInfo.copy(
      lastMessages = SortedMap[MessageType, Signed[CurrencyMessage]](
        MessageType.Owner -> signedMessage(MessageType.Owner)
      ).some
    )

  private def currencySnapshots(secondInfo: CurrencySnapshotInfo): SortedMap[Address, CurrencyArm] =
    SortedMap[Address, CurrencyArm](
      firstMetagraph -> Right((incremental(1L), validFirstInfo)),
      secondMetagraph -> Right((incremental(2L), secondInfo))
    )

  private def freshStore(
    implicit hasher: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[MptStore[IO, GlobalStateKey]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(MptStore.make[IO, GlobalStateKey](_, GlobalStateKey.toHex[IO]))

  private def snapshotBytes(store: MptStore[IO, GlobalStateKey]): IO[Map[Hex, Array[Byte]]] =
    store.allEntriesAsBytes.map(_.map { case (key, bytes) => key -> bytes.clone() })

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

  private def rejectedStructure(result: Either[Throwable, Unit], expectedError: String): Boolean =
    result.left.exists {
      case error: StrictMptRead.InconsistentConsensusMptIndex => error.getMessage.contains(expectedError)
      case _                                                  => false
    }

  private def seed(store: MptStore[IO, GlobalStateKey]): IO[Unit] =
    store.insert[Balance](
      GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, existingOwner),
      Balance(NonNegLong(13L))
    )

  private def runFullRebuild(
    secondInfo: CurrencySnapshotInfo
  )(
    implicit hasher: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[(Either[Throwable, Unit], Map[Hex, Array[Byte]], Map[Hex, Array[Byte]])] = {
    val info = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap(unrelatedOwner -> Balance(NonNegLong(17L))),
      lastCurrencySnapshots = currencySnapshots(secondInfo)
    )

    for {
      store <- freshStore
      _ <- seed(store)
      before <- snapshotBytes(store)
      result <- store.syncFromGlobalSnapshotInfo(info, ordinal).attempt
      after <- snapshotBytes(store)
    } yield (result, before, after)
  }

  private def runIncremental(
    secondInfo: CurrencySnapshotInfo
  )(
    implicit hasher: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[(Either[Throwable, Unit], Map[Hex, Array[Byte]], Map[Hex, Array[Byte]])] = {
    val acc = StateChangesAccumulator(
      balances = SortedMap(unrelatedOwner -> Balance(NonNegLong(17L))),
      lastCurrencySnapshots = currencySnapshots(secondInfo)
    )

    for {
      store <- freshStore
      _ <- seed(store)
      before <- snapshotBytes(store)
      result <- store.syncFromStateChanges(acc, ordinal).attempt
      after <- snapshotBytes(store)
    } yield (result, before, after)
  }

  private def foldExpectations[A](values: List[A])(run: A => IO[Expectations]): IO[Expectations] =
    values.foldLeft(IO.pure(Expectations.Helpers.success)) { (accIO, value) =>
      (accIO, run(value)).mapN(_.and(_))
    }

  private def failHashingString(delegate: Hasher[IO], target: String): Hasher[IO] = new Hasher[IO] {
    private val injectedError = s"injected key materialization failure for $target"

    def hash[A: Encoder](data: A): IO[Hash] =
      data match {
        case value: String if value == target => IO.raiseError(new IllegalStateException(injectedError))
        case _                                => delegate.hash(data)
      }

    def hashBytes(bytes: Array[Byte]): IO[Hash] = delegate.hashBytes(bytes)

    def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] =
      hash(data).map(_ === expectedHash)

    def getLogic(snapshotOrdinal: SnapshotOrdinal): HashLogic = delegate.getLogic(snapshotOrdinal)

    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] = delegate.prefixedHash(data, prefix)
  }

  test("plain full rebuild rejects a malformed later field-30/31 metagraph without changing any bytes") { res =>
    implicit val (hasher, json) = res

    foldExpectations(invalidCases) { invalid =>
      runFullRebuild(invalid.info).map {
        case (result, before, after) =>
          expect.all(
            clue(invalid.name -> rejectedStructure(result, invalid.expectedError))._2,
            clue(invalid.name -> sameBytes(before, after))._2
          )
      }
    }
  }

  test("direct incremental writer rejects a malformed later field-30/31 metagraph without changing any bytes") { res =>
    implicit val (hasher, json) = res

    foldExpectations(invalidCases) { invalid =>
      runIncremental(invalid.info).map {
        case (result, before, after) =>
          expect.all(
            clue(invalid.name -> rejectedStructure(result, invalid.expectedError))._2,
            clue(invalid.name -> sameBytes(before, after))._2
          )
      }
    }
  }

  test("later metagraph key-materialization failure leaves full and incremental writer bytes unchanged") { res =>
    val (delegate, jsonResource) = res
    implicit val json: JsonSerializer[IO] = jsonResource
    implicit val hasher: Hasher[IO] = failHashingString(delegate, MessageType.Owner.value)
    val expectedError = s"injected key materialization failure for ${MessageType.Owner.value}"

    for {
      full <- runFullRebuild(validSecondMessageInfo)
      incremental <- runIncremental(validSecondMessageInfo)
      (fullResult, fullBefore, fullAfter) = full
      (incrementalResult, incrementalBefore, incrementalAfter) = incremental
    } yield
      expect.all(
        fullResult.left.exists(_.getMessage.contains(expectedError)),
        sameBytes(fullBefore, fullAfter),
        incrementalResult.left.exists(_.getMessage.contains(expectedError)),
        sameBytes(incrementalBefore, incrementalAfter)
      )
  }
}
