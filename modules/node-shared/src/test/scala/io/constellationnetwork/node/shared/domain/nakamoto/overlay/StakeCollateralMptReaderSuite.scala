package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{SnapshotOrdinal, balance}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._

import eu.timepit.refined.types.numeric.NonNegLong
import scodec.bits.ByteVector
import weaver.{Expectations, MutableIOSuite}

object StakeCollateralMptReaderSuite extends MutableIOSuite {

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, jsonSerializer)

  private val sourceA = Address.fromBytes("stake-source-a".getBytes("UTF-8"))
  private val sourceB = Address.fromBytes("stake-source-b".getBytes("UTF-8"))
  private val contract = Address.fromBytes("wrong-contract".getBytes("UTF-8"))
  private val nodeId = PeerId(Hex("11" * 64))

  private def proof(byte: String): SignatureProof =
    SignatureProof(Id(Hex(byte * 64)), Signature(Hex(byte.reverse * 64)))

  private val proofsA = NonEmptySet.one(proof("12"))
  private val proofsB = NonEmptySet.one(proof("34"))

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))
  private def epoch(value: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(value))

  private def delegatedCreate(source: Address, amount: Long): UpdateDelegatedStake.Create =
    UpdateDelegatedStake.Create(
      source,
      nodeId,
      DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)),
      DelegatedStakeFee(NonNegLong(1L)),
      Hash("55" * 32),
      DelegatedStakeReference.empty
    )

  private def collateralCreate(source: Address, amount: Long): UpdateNodeCollateral.Create =
    UpdateNodeCollateral.Create(
      source,
      nodeId,
      NodeCollateralAmount(NonNegLong.unsafeFrom(amount)),
      NodeCollateralFee(NonNegLong(1L)),
      Hash("66" * 32),
      NodeCollateralReference.empty
    )

  private sealed trait FieldFixture {
    type Entry

    def name: String
    def fieldId: GlobalStateFieldId
    def codec: ImmutableCodec[SortedSet[Entry]]
    def canonicalA: SortedSet[Entry]
    def canonicalB: SortedSet[Entry]
    def empty: SortedSet[Entry]
    def mixed: SortedSet[Entry]
    def duplicateProofIdentity: SortedSet[Entry]
    def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[SortedMap[Address, SortedSet[Entry]]]
    def read(reader: GlobalStateReader[IO], address: Address)(implicit hasher: Hasher[IO]): IO[Option[SortedSet[Entry]]]

    final def encoded(value: SortedSet[Entry]): Array[Byte] = codec.immutableBytes(value).toArray
  }

  private object ActiveDelegatedFixture extends FieldFixture {
    type Entry = DelegatedStakeRecord

    val name = "ActiveDelegatedStakes"
    val fieldId: GlobalStateFieldId = GlobalStateFieldId.ActiveDelegatedStakes
    val codec: ImmutableCodec[SortedSet[Entry]] = delegatedStakeRecordSetCodec

    private val createA = delegatedCreate(sourceA, 10L)
    private val createB = delegatedCreate(sourceB, 20L)
    val canonicalA = SortedSet(DelegatedStakeRecord(Signed(createA, proofsA), ordinal(1L), balance.Amount.empty))
    val canonicalB = SortedSet(DelegatedStakeRecord(Signed(createB, proofsA), ordinal(2L), balance.Amount.empty))
    val empty = SortedSet.empty[Entry]
    val mixed = canonicalA ++ canonicalB
    val duplicateProofIdentity = SortedSet(
      DelegatedStakeRecord(Signed(createA, proofsA), ordinal(1L), balance.Amount.empty),
      DelegatedStakeRecord(Signed(createA, proofsB), ordinal(2L), balance.Amount.empty)
    )

    def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[SortedMap[Address, SortedSet[Entry]]] =
      StakeCollateralMptReader.materializeActiveDelegatedStakes(reader)

    def read(reader: GlobalStateReader[IO], address: Address)(implicit hasher: Hasher[IO]): IO[Option[SortedSet[Entry]]] =
      StakeCollateralMptReader.readActiveDelegatedStakes(reader, address)
  }

  private object DelegatedWithdrawalFixture extends FieldFixture {
    type Entry = PendingDelegatedStakeWithdrawal

    val name = "DelegatedStakesWithdrawals"
    val fieldId: GlobalStateFieldId = GlobalStateFieldId.DelegatedStakesWithdrawals
    val codec: ImmutableCodec[SortedSet[Entry]] = pendingDelegatedStakeWithdrawalSetCodec

    private val createA = delegatedCreate(sourceA, 30L)
    private val createB = delegatedCreate(sourceB, 40L)
    val canonicalA = SortedSet(
      PendingDelegatedStakeWithdrawal(
        Signed(createA, proofsA),
        balance.Amount.empty,
        ordinal(1L),
        epoch(1L)
      )
    )
    val canonicalB = SortedSet(
      PendingDelegatedStakeWithdrawal(
        Signed(createB, proofsA),
        balance.Amount.empty,
        ordinal(2L),
        epoch(2L)
      )
    )
    val empty = SortedSet.empty[Entry]
    val mixed = canonicalA ++ canonicalB
    val duplicateProofIdentity = SortedSet(
      PendingDelegatedStakeWithdrawal(
        Signed(createA, proofsA),
        balance.Amount.empty,
        ordinal(1L),
        epoch(1L)
      ),
      PendingDelegatedStakeWithdrawal(
        Signed(createA, proofsB),
        balance.Amount.empty,
        ordinal(1L),
        epoch(2L)
      )
    )

    def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[SortedMap[Address, SortedSet[Entry]]] =
      StakeCollateralMptReader.materializeDelegatedStakeWithdrawals(reader)

    def read(reader: GlobalStateReader[IO], address: Address)(implicit hasher: Hasher[IO]): IO[Option[SortedSet[Entry]]] =
      StakeCollateralMptReader.readDelegatedStakeWithdrawals(reader, address)
  }

  private object ActiveCollateralFixture extends FieldFixture {
    type Entry = NodeCollateralRecord

    val name = "ActiveNodeCollaterals"
    val fieldId: GlobalStateFieldId = GlobalStateFieldId.ActiveNodeCollaterals
    val codec: ImmutableCodec[SortedSet[Entry]] = nodeCollateralRecordSetCodec

    private val createA = collateralCreate(sourceA, 50L)
    private val createB = collateralCreate(sourceB, 60L)
    val canonicalA = SortedSet(NodeCollateralRecord(Signed(createA, proofsA), ordinal(1L)))
    val canonicalB = SortedSet(NodeCollateralRecord(Signed(createB, proofsA), ordinal(2L)))
    val empty = SortedSet.empty[Entry]
    val mixed = canonicalA ++ canonicalB
    val duplicateProofIdentity = SortedSet(
      NodeCollateralRecord(Signed(createA, proofsA), ordinal(1L)),
      NodeCollateralRecord(Signed(createA, proofsB), ordinal(2L))
    )

    def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[SortedMap[Address, SortedSet[Entry]]] =
      StakeCollateralMptReader.materializeActiveNodeCollaterals(reader)

    def read(reader: GlobalStateReader[IO], address: Address)(implicit hasher: Hasher[IO]): IO[Option[SortedSet[Entry]]] =
      StakeCollateralMptReader.readActiveNodeCollaterals(reader, address)
  }

  private object CollateralWithdrawalFixture extends FieldFixture {
    type Entry = PendingNodeCollateralWithdrawal

    val name = "NodeCollateralWithdrawals"
    val fieldId: GlobalStateFieldId = GlobalStateFieldId.NodeCollateralWithdrawals
    val codec: ImmutableCodec[SortedSet[Entry]] = pendingNodeCollateralWithdrawalSetCodec

    private val createA = collateralCreate(sourceA, 70L)
    private val createB = collateralCreate(sourceB, 80L)
    val canonicalA = SortedSet(
      PendingNodeCollateralWithdrawal(Signed(createA, proofsA), ordinal(1L), epoch(1L))
    )
    val canonicalB = SortedSet(
      PendingNodeCollateralWithdrawal(Signed(createB, proofsA), ordinal(2L), epoch(2L))
    )
    val empty = SortedSet.empty[Entry]
    val mixed = canonicalA ++ canonicalB
    val duplicateProofIdentity = SortedSet(
      PendingNodeCollateralWithdrawal(Signed(createA, proofsA), ordinal(1L), epoch(1L)),
      PendingNodeCollateralWithdrawal(Signed(createA, proofsB), ordinal(1L), epoch(2L))
    )

    def materialize(reader: GlobalStateReader[IO])(implicit hasher: Hasher[IO]): IO[SortedMap[Address, SortedSet[Entry]]] =
      StakeCollateralMptReader.materializeNodeCollateralWithdrawals(reader)

    def read(reader: GlobalStateReader[IO], address: Address)(implicit hasher: Hasher[IO]): IO[Option[SortedSet[Entry]]] =
      StakeCollateralMptReader.readNodeCollateralWithdrawals(reader, address)
  }

  private val fixtures: List[FieldFixture] = List(
    ActiveDelegatedFixture,
    DelegatedWithdrawalFixture,
    ActiveCollateralFixture,
    CollateralWithdrawalFixture
  )

  private def keyHex(fieldId: GlobalStateFieldId, source: Address)(implicit hasher: Hasher[IO]): IO[Hex] =
    GlobalStateKey.toHex[IO](GlobalStateKey.hypergraph(fieldId, source))

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

  private def strictReader[V](
    entries: List[StrictMptEntry[V]],
    pointRead: StrictMptRead[V] = StrictMptRead.Absent
  ): GlobalStateReader[IO] = new GlobalStateReader[IO] {
    def get[A: ImmutableCodec](key: GlobalStateKey): IO[Option[A]] = IO.pure(None)
    def getStrict[A: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[A]] =
      IO.pure(pointRead.asInstanceOf[StrictMptRead[A]])
    def getMany[A: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, A]] = IO.pure(Map.empty)
    def getAllForPrefix[A: ImmutableCodec](prefix: Hex): IO[Map[Hex, A]] = IO.pure(Map.empty)
    def getAllForPrefixStrict[A: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[A]]] =
      IO.pure(
        entries
          .filter(_.physicalKey.value.startsWith(prefix.value))
          .map(_.asInstanceOf[StrictMptEntry[A]])
      )
  }

  private def forEveryFixture(check: FieldFixture => IO[Expectations]): IO[Expectations] =
    fixtures.traverse(check).map(_.reduce(_.and(_)))

  test("materializes canonical fields 13-16 entries in logical source order") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        keyA <- keyHex(fixture.fieldId, sourceA)
        keyB <- keyHex(fixture.fieldId, sourceB)
        result <- fixture.materialize(
          rawReader(List(keyB -> fixture.encoded(fixture.canonicalB), keyA -> fixture.encoded(fixture.canonicalA)))
        )
      } yield
        expect.same(
          SortedMap(sourceA -> fixture.canonicalA, sourceB -> fixture.canonicalB),
          result
        )
    }
  }

  test("rejects A-key/B-value, suffix, and nonempty-contract placements for fields 13-16") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        keyA <- keyHex(fixture.fieldId, sourceA)
        keyB <- keyHex(fixture.fieldId, sourceB)
        suffixKey = Hex(keyB.value + "00")
        wrongContractKey <- GlobalStateKey.toHex[IO](
          GlobalStateKey(HypergraphNamespace, fixture.fieldId, AddressNamespace(contract), AddressNamespace(sourceB))
        )
        wrongKey <- fixture.materialize(rawReader(List(keyA -> fixture.encoded(fixture.canonicalB)))).attempt
        suffix <- fixture.materialize(rawReader(List(suffixKey -> fixture.encoded(fixture.canonicalB)))).attempt
        wrongContract <- fixture.materialize(rawReader(List(wrongContractKey -> fixture.encoded(fixture.canonicalB)))).attempt
      } yield
        expect.all(
          wrongKey.left.exists(_.getMessage.contains("key/value mismatch")),
          suffix.left.exists(_.getMessage.contains("key/value mismatch")),
          wrongContract.left.exists(_.getMessage.contains("key/value mismatch"))
        )
    }
  }

  test("rejects null, empty, malformed, trailing, and noncanonical retained bytes for fields 13-16") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        key <- keyHex(fixture.fieldId, sourceA)
        nullBytes <- fixture.materialize(rawReader(List(key -> null.asInstanceOf[Array[Byte]]))).attempt
        emptyBytes <- fixture.materialize(rawReader(List(key -> Array.emptyByteArray))).attempt
        malformed <- fixture.materialize(rawReader(List(key -> Array(0xff.toByte)))).attempt
        trailing <- fixture
          .materialize(rawReader(List(key -> (fixture.encoded(fixture.canonicalA) ++ Array(0.toByte)))))
          .attempt
        noncanonical <- fixture
          .materialize(
            strictReader(
              List(StrictMptEntry(key, StrictMptRead.Present(fixture.canonicalA, ByteVector(0.toByte))))
            )
          )
          .attempt
      } yield
        expect.all(
          nullBytes.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
          emptyBytes.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
          malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
          trailing.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
          noncanonical.left.exists(_.getMessage.contains("non-canonical value encoding"))
        )
    }
  }

  test("rejects empty and mixed-source sets for fields 13-16") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        key <- keyHex(fixture.fieldId, sourceA)
        empty <- fixture.materialize(rawReader(List(key -> fixture.encoded(fixture.empty)))).attempt
        mixed <- fixture.materialize(rawReader(List(key -> fixture.encoded(fixture.mixed)))).attempt
      } yield
        expect.all(
          empty.left.exists(_.getMessage.contains("empty record set")),
          mixed.left.exists(_.getMessage.contains("mixed embedded create sources"))
        )
    }
  }

  test("rejects duplicate proof variants of one unsigned create identity for fields 13-16") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        key <- keyHex(fixture.fieldId, sourceA)
        result <- fixture.materialize(rawReader(List(key -> fixture.encoded(fixture.duplicateProofIdentity)))).attempt
      } yield
        expect.all(
          fixture.duplicateProofIdentity.size == 2,
          result.left.exists(_.getMessage.contains("duplicate unsigned create identity"))
        )
    }
  }

  test("rejects impossible absent prefix entries and duplicate logical sources in physical-key order") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        key <- keyHex(fixture.fieldId, sourceA)
        duplicateKey = Hex(key.value + "00")
        rawBytes = fixture.codec.immutableBytes(fixture.canonicalA)
        present = StrictMptRead.Present(fixture.canonicalA, rawBytes)
        absent <- fixture.materialize(strictReader(List(StrictMptEntry(key, StrictMptRead.Absent)))).attempt
        forward <- fixture
          .materialize(strictReader(List(StrictMptEntry(key, present), StrictMptEntry(duplicateKey, present))))
          .attempt
        reverse <- fixture
          .materialize(strictReader(List(StrictMptEntry(duplicateKey, present), StrictMptEntry(key, present))))
          .attempt
      } yield
        expect.all(
          absent.left.exists(_.isInstanceOf[StrictMptRead.MissingConsensusMptValue]),
          forward.left.exists(_.getMessage.contains("duplicate logical source")),
          forward.leftMap(_.getMessage) == reverse.leftMap(_.getMessage)
        )
    }
  }

  test("strict point reads preserve absence and reject malformed, noncanonical, or wrong-source state") { res =>
    implicit val (hasher, _) = res

    forEveryFixture { fixture =>
      for {
        absent <- fixture.read(strictReader[SortedSet[fixture.Entry]](Nil), sourceA)
        present <- fixture.read(
          strictReader(
            Nil,
            StrictMptRead.Present(fixture.canonicalA, fixture.codec.immutableBytes(fixture.canonicalA))
          ),
          sourceA
        )
        malformed <- fixture
          .read(strictReader(Nil, StrictMptRead.Malformed("bad bytes", none)), sourceA)
          .attempt
        noncanonical <- fixture
          .read(strictReader(Nil, StrictMptRead.Present(fixture.canonicalA, ByteVector(0.toByte))), sourceA)
          .attempt
        wrongSource <- fixture
          .read(
            strictReader(
              Nil,
              StrictMptRead.Present(fixture.canonicalB, fixture.codec.immutableBytes(fixture.canonicalB))
            ),
            sourceA
          )
          .attempt
      } yield
        expect.all(
          absent.isEmpty,
          present.contains(fixture.canonicalA),
          malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
          noncanonical.left.exists(_.getMessage.contains("non-canonical value encoding")),
          wrongSource.left.exists(_.getMessage.contains("key/value mismatch"))
        )
    }
  }
}
