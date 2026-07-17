package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap.{ConsumedAllowSpend, CurrencyId, SwapAmount}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec.{immutableCodec => consumedAllowSpendImmutableCodec}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import scodec.bits.ByteVector
import weaver.MutableIOSuite

object ConsumedAllowSpendStateManagerMaterializationSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], Address, Address, Address)

  override def sharedResource: Resource[IO, Res] =
    for {
      securityProvider <- SecurityProvider.forAsync[IO]
      implicit0(sp: SecurityProvider[IO]) = securityProvider
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      destination <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
      currency <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress).asResource
    } yield (hasher, securityProvider, jsonSerializer, source, destination, currency)

  private def hash(digit: Char): Hash = Hash(List.fill(64)(digit).mkString)

  private def marker(
    allowSpendHash: Hash,
    source: Address,
    destination: Address,
    currency: Address
  ): ConsumedAllowSpend =
    ConsumedAllowSpend(
      allowSpendHash = allowSpendHash,
      source = source,
      destination = destination,
      currencyId = CurrencyId(currency).some,
      amount = SwapAmount(PosLong.unsafeFrom(10L)),
      lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(20L)),
      consumedAtOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(30L)),
      consumingSpendRef = hash('f')
    )

  private def encoded(value: ConsumedAllowSpend): Array[Byte] =
    consumedAllowSpendImmutableCodec.immutableBytes(value).toArray

  private def keyHex(value: Hash)(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey.toHex[IO](GlobalStateKey.consumedAllowSpendKey(value))

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

  /** Injects strict-reader contract violations which cannot be expressed as raw bytes, such as `Absent` in a returned prefix entry or a
    * decoded value paired with different retained bytes.
    */
  private def strictReader(entries: List[StrictMptEntry[ConsumedAllowSpend]]): GlobalStateReader[IO] = new GlobalStateReader[IO] {
    def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = IO.pure(None)
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] = IO.pure(StrictMptRead.Absent)
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] =
      IO.pure(
        entries
          .filter(_.physicalKey.value.startsWith(prefix.value))
          .map(_.asInstanceOf[StrictMptEntry[V]])
      )
  }

  private def materialize(
    reader: GlobalStateReader[IO]
  )(implicit hasher: Hasher[IO]): IO[SortedMap[Hash, ConsumedAllowSpend]] =
    ConsumedAllowSpendStateManager.make[IO](reader).materializeConsumedAllowSpendsFromMpt

  test("materializes canonical field-33 entries in semantic-hash order") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val hashA = hash('a')
    val hashB = hash('b')
    val markerA = marker(hashA, source, destination, currency)
    val markerB = marker(hashB, source, destination, currency)

    for {
      keyA <- keyHex(hashA)
      keyB <- keyHex(hashB)
      result <- materialize(rawReader(List(keyB -> encoded(markerB), keyA -> encoded(markerA))))
    } yield expect.same(SortedMap(hashA -> markerA, hashB -> markerB), result)
  }

  test("rejects an A-key carrying a B-value") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val hashA = hash('a')
    val hashB = hash('b')
    val markerB = marker(hashB, source, destination, currency)

    for {
      keyA <- keyHex(hashA)
      result <- materialize(rawReader(List(keyA -> encoded(markerB)))).attempt
    } yield
      expect(
        result.left.exists {
          case error: StrictMptRead.InconsistentConsensusMptIndex =>
            error.physicalKey == keyA && error.getMessage.contains("key/value mismatch")
          case _ => false
        }
      )
  }

  test("rejects suffix and wrong-contract placements even when their values are canonical") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val allowSpendHash = hash('c')
    val value = marker(allowSpendHash, source, destination, currency)

    for {
      canonicalKey <- keyHex(allowSpendHash)
      suffixKey = Hex(canonicalKey.value + "00")
      wrongContractKey <- GlobalStateKey.toHex[IO](
        GlobalStateKey(
          HypergraphNamespace,
          GlobalStateFieldId.ConsumedAllowSpends,
          AddressNamespace(currency),
          HashNamespace(allowSpendHash)
        )
      )
      suffixResult <- materialize(rawReader(List(suffixKey -> encoded(value)))).attempt
      wrongContractResult <- materialize(rawReader(List(wrongContractKey -> encoded(value)))).attempt
    } yield
      expect.all(
        suffixResult.left.exists(_.getMessage.contains("key/value mismatch")),
        wrongContractResult.left.exists(_.getMessage.contains("key/value mismatch"))
      )
  }

  test("rejects null, malformed, empty, and trailing field-33 bytes") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val allowSpendHash = hash('d')
    val value = marker(allowSpendHash, source, destination, currency)

    for {
      key <- keyHex(allowSpendHash)
      nullBytes <- materialize(rawReader(List(key -> null.asInstanceOf[Array[Byte]]))).attempt
      malformed <- materialize(rawReader(List(key -> Array(0xff.toByte)))).attempt
      empty <- materialize(rawReader(List(key -> Array.emptyByteArray))).attempt
      trailing <- materialize(rawReader(List(key -> (encoded(value) ++ Array(0.toByte))))).attempt
    } yield
      expect.all(
        nullBytes.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        empty.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        trailing.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue])
      )
  }

  test("rejects impossible absent prefix entries and decoded values with noncanonical retained bytes") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val allowSpendHash = hash('e')
    val value = marker(allowSpendHash, source, destination, currency)

    for {
      key <- keyHex(allowSpendHash)
      absent <- materialize(strictReader(List(StrictMptEntry(key, StrictMptRead.Absent)))).attempt
      noncanonical <- materialize(
        strictReader(List(StrictMptEntry(key, StrictMptRead.Present(value, ByteVector(0.toByte)))))
      ).attempt
    } yield
      expect.all(
        absent.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
        noncanonical.left.exists {
          case error: StrictMptRead.MalformedConsensusMptValue => error.getMessage.contains("non-canonical value encoding")
          case _                                               => false
        }
      )
  }

  test("rejects duplicate semantic hashes deterministically before map construction") { res =>
    implicit val (hasher, _, _, source, destination, currency) = res
    val allowSpendHash = hash('1')
    val value = marker(allowSpendHash, source, destination, currency)
    val rawBytes = consumedAllowSpendImmutableCodec.immutableBytes(value)

    for {
      canonicalKey <- keyHex(allowSpendHash)
      duplicateKey = Hex(canonicalKey.value + "00")
      canonicalEntry = StrictMptEntry(canonicalKey, StrictMptRead.Present(value, rawBytes))
      duplicateEntry = StrictMptEntry(duplicateKey, StrictMptRead.Present(value, rawBytes))
      forward <- materialize(strictReader(List(canonicalEntry, duplicateEntry))).attempt
      reverse <- materialize(strictReader(List(duplicateEntry, canonicalEntry))).attempt
      forwardMessage = forward.leftMap(_.getMessage)
      reverseMessage = reverse.leftMap(_.getMessage)
    } yield
      expect.all(
        forward.left.exists {
          case error: StrictMptRead.InconsistentConsensusMptIndex =>
            error.physicalKey == duplicateKey && error.getMessage.contains("duplicate semantic allow-spend hash")
          case _ => false
        },
        forwardMessage == reverseMessage
      )
  }
}
