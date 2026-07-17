package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import java.security.KeyPair

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec

import eu.timepit.refined.auto._
import scodec.bits.ByteVector
import weaver.MutableIOSuite

object ActiveTokenLockMptReaderSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], KeyPair, KeyPair, KeyPair)

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(sp: SecurityProvider[IO]) = securityProvider
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      keyPairA <- KeyPairGenerator.makeKeyPair[IO].asResource
      keyPairB <- KeyPairGenerator.makeKeyPair[IO].asResource
      currencyKeyPair <- KeyPairGenerator.makeKeyPair[IO].asResource
    } yield (hasher, securityProvider, jsonSerializer, keyPairA, keyPairB, currencyKeyPair)

  private def value(
    sourceKeyPair: KeyPair,
    currencyId: Option[CurrencyId] = none,
    amount: TokenLockAmount = TokenLockAmount(10L)
  ): TokenLock =
    TokenLock(
      source = sourceKeyPair.getPublic.toAddress,
      amount = amount,
      fee = TokenLockFee(1L),
      parent = TokenLockReference.empty,
      currencyId = currencyId,
      unlockEpoch = none,
      replaceTokenLockRef = none
    )

  private def sign(
    tokenLock: TokenLock,
    signer: KeyPair
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[Signed[TokenLock]] =
    Signed.forAsyncHasher(tokenLock, signer)

  private def encoded(value: SortedSet[Signed[TokenLock]]): Array[Byte] =
    signedTokenLockSetCodec.immutableBytes(value).toArray

  private def keyHex(source: Address)(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey.toHex[IO](GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source))

  private def rawReader(entries: List[(Hex, Array[Byte])]): GlobalStateReader[IO] = new GlobalStateReader[IO] {
    private val physicalEntries = entries.toMap

    def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = IO.pure(None)
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] = IO.pure(StrictMptRead.Absent)
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      IO.pure(
        StrictMptRead.decodeEntries[V](physicalEntries.filter { case (key, _) => key.value.startsWith(prefix.value) })
      )
  }

  private def strictReader(
    entries: List[StrictMptEntry[SortedSet[Signed[TokenLock]]]],
    pointRead: StrictMptRead[SortedSet[Signed[TokenLock]]] = StrictMptRead.Absent
  ): GlobalStateReader[IO] = new GlobalStateReader[IO] {
    def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = IO.pure(None)
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] =
      IO.pure(pointRead.asInstanceOf[StrictMptRead[V]])
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      IO.pure(
        entries
          .filter(_.physicalKey.value.startsWith(prefix.value))
          .map(_.asInstanceOf[StrictMptEntry[V]])
      )
  }

  private def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[ActiveTokenLockMptReader.ActiveTokenLocks] =
    ActiveTokenLockMptReader.materializeNative(reader)

  test("materializes canonical native token-lock entries in source order") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, keyPairB, _) = res
    val lockA = value(keyPairA)
    val secondLockA = value(keyPairA, amount = TokenLockAmount(11L))
    val lockB = value(keyPairB)

    for {
      signedA <- sign(lockA, keyPairA)
      secondSignedA <- sign(secondLockA, keyPairA)
      signedB <- sign(lockB, keyPairB)
      setA = SortedSet(signedA, secondSignedA)
      setB = SortedSet(signedB)
      keyA <- keyHex(lockA.source)
      keyB <- keyHex(lockB.source)
      result <- materialize(rawReader(List(keyB -> encoded(setB), keyA -> encoded(setA))))
    } yield expect.same(SortedMap(lockA.source -> setA, lockB.source -> setB), result)
  }

  test("rejects wrong-key, suffix, and wrong-contract placements") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, keyPairB, currencyKeyPair) = res
    val lockA = value(keyPairA)
    val lockB = value(keyPairB)
    val currency = currencyKeyPair.getPublic.toAddress

    for {
      signedB <- sign(lockB, keyPairB)
      setB = SortedSet(signedB)
      keyA <- keyHex(lockA.source)
      keyB <- keyHex(lockB.source)
      suffixKey = Hex(keyB.value + "00")
      wrongContractKey <- GlobalStateKey.toHex[IO](
        GlobalStateKey(
          HypergraphNamespace,
          GlobalStateFieldId.ActiveTokenLocks,
          AddressNamespace(currency),
          AddressNamespace(lockB.source)
        )
      )
      wrongKey <- materialize(rawReader(List(keyA -> encoded(setB)))).attempt
      suffix <- materialize(rawReader(List(suffixKey -> encoded(setB)))).attempt
      wrongContract <- materialize(rawReader(List(wrongContractKey -> encoded(setB)))).attempt
    } yield
      expect.all(
        wrongKey.left.exists(_.getMessage.contains("key/value mismatch")),
        suffix.left.exists(_.getMessage.contains("key/value mismatch")),
        wrongContract.left.exists(_.getMessage.contains("key/value mismatch"))
      )
  }

  test("rejects empty, mixed-source, and currency-scoped field-8 sets") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, keyPairB, currencyKeyPair) = res
    val lockA = value(keyPairA)
    val lockB = value(keyPairB)
    val scoped = value(keyPairA, CurrencyId(currencyKeyPair.getPublic.toAddress).some)

    for {
      signedA <- sign(lockA, keyPairA)
      signedB <- sign(lockB, keyPairB)
      signedScoped <- sign(scoped, keyPairA)
      keyA <- keyHex(lockA.source)
      empty <- materialize(rawReader(List(keyA -> encoded(SortedSet.empty[Signed[TokenLock]])))).attempt
      mixed <- materialize(rawReader(List(keyA -> encoded(SortedSet(signedA, signedB))))).attempt
      wrongScope <- materialize(rawReader(List(keyA -> encoded(SortedSet(signedScoped))))).attempt
    } yield
      expect.all(
        empty.left.exists(_.getMessage.contains("empty token-lock set")),
        mixed.left.exists(_.getMessage.contains("mixed token-lock sources")),
        wrongScope.left.exists(_.getMessage.contains("currencyId=None"))
      )
  }

  test("rejects null, malformed, empty, trailing, and noncanonical retained bytes") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, _, _) = res
    val lock = value(keyPairA)

    for {
      signed <- sign(lock, keyPairA)
      locks = SortedSet(signed)
      key <- keyHex(lock.source)
      nullBytes <- materialize(rawReader(List(key -> null.asInstanceOf[Array[Byte]]))).attempt
      malformed <- materialize(rawReader(List(key -> Array(0xff.toByte)))).attempt
      empty <- materialize(rawReader(List(key -> Array.emptyByteArray))).attempt
      trailing <- materialize(rawReader(List(key -> (encoded(locks) ++ Array(0.toByte))))).attempt
      noncanonical <- materialize(
        strictReader(List(StrictMptEntry(key, StrictMptRead.Present(locks, ByteVector(0.toByte)))))
      ).attempt
    } yield
      expect.all(
        nullBytes.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        empty.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        trailing.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        noncanonical.left.exists(_.getMessage.contains("non-canonical value encoding"))
      )
  }

  test("rejects impossible absent prefix entries and duplicate logical sources deterministically") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, _, _) = res
    val lock = value(keyPairA)

    for {
      signed <- sign(lock, keyPairA)
      locks = SortedSet(signed)
      key <- keyHex(lock.source)
      duplicateKey = Hex(key.value + "00")
      rawBytes = signedTokenLockSetCodec.immutableBytes(locks)
      absent <- materialize(strictReader(List(StrictMptEntry(key, StrictMptRead.Absent)))).attempt
      forward <- materialize(
        strictReader(
          List(
            StrictMptEntry(key, StrictMptRead.Present(locks, rawBytes)),
            StrictMptEntry(duplicateKey, StrictMptRead.Present(locks, rawBytes))
          )
        )
      ).attempt
      reverse <- materialize(
        strictReader(
          List(
            StrictMptEntry(duplicateKey, StrictMptRead.Present(locks, rawBytes)),
            StrictMptEntry(key, StrictMptRead.Present(locks, rawBytes))
          )
        )
      ).attempt
    } yield
      expect.all(
        absent.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
        forward.left.exists(_.getMessage.contains("duplicate logical token-lock source")),
        forward.leftMap(_.getMessage) == reverse.leftMap(_.getMessage)
      )
  }

  test("rejects duplicate unsigned token-lock identities with distinct proof sets") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, keyPairB, _) = res
    val lock = value(keyPairA)

    for {
      signedByA <- sign(lock, keyPairA)
      signedByB <- sign(lock, keyPairB)
      locks = SortedSet(signedByA, signedByB)
      key <- keyHex(lock.source)
      result <- materialize(rawReader(List(key -> encoded(locks)))).attempt
    } yield
      expect.all(
        locks.size == 2,
        result.left.exists(_.getMessage.contains("duplicate unsigned token-lock identity"))
      )
  }

  test("strict point reads preserve absence and reject malformed or wrong-source state") { res =>
    implicit val (hasher, securityProvider, _, keyPairA, keyPairB, _) = res
    val lockA = value(keyPairA)
    val lockB = value(keyPairB)

    for {
      signedA <- sign(lockA, keyPairA)
      signedB <- sign(lockB, keyPairB)
      setA = SortedSet(signedA)
      setB = SortedSet(signedB)
      absent <- ActiveTokenLockMptReader.readNative(strictReader(Nil), lockA.source)
      present <- ActiveTokenLockMptReader.readNative(
        strictReader(Nil, StrictMptRead.Present(setA, signedTokenLockSetCodec.immutableBytes(setA))),
        lockA.source
      )
      malformed <- ActiveTokenLockMptReader
        .readNative(strictReader(Nil, StrictMptRead.Malformed("bad bytes", none)), lockA.source)
        .attempt
      wrongSource <- ActiveTokenLockMptReader
        .readNative(
          strictReader(Nil, StrictMptRead.Present(setB, signedTokenLockSetCodec.immutableBytes(setB))),
          lockA.source
        )
        .attempt
    } yield
      expect.all(
        absent.isEmpty,
        present.contains(setA),
        malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        wrongSource.left.exists(_.getMessage.contains("key/value mismatch"))
      )
  }
}
