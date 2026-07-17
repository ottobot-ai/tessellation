package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.{GlobalStateKey, StrictMptEntry, StrictMptRead}
import io.constellationnetwork.schema.node._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.unpRecordImmutableCodec

import scodec.bits.ByteVector
import weaver.MutableIOSuite

object UpdateNodeParametersMptReaderSuite extends MutableIOSuite {

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(jsonSerializer: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
    } yield (hasher, jsonSerializer)

  private val idA = Id(Hex("11" * 64))
  private val idB = Id(Hex("22" * 64))
  private val source = Address.fromBytes("update-node-parameters-source".getBytes("UTF-8"))

  private def proof(id: Id, byte: String): SignatureProof =
    SignatureProof(id, Signature(Hex(byte * 64)))

  private def record(proofs: NonEmptySet[SignatureProof]): UpdateNodeParametersMptReader.Record =
    Signed(
      UpdateNodeParameters(
        source,
        DelegatedStakeRewardParameters(RewardFraction.unsafeFrom(80_000_000)),
        NodeMetadataParameters("name", "description"),
        UpdateNodeParametersReference.empty
      ),
      proofs
    ) -> SnapshotOrdinal.MinValue

  private def readerAt(
    target: GlobalStateKey,
    read: StrictMptRead[UpdateNodeParametersMptReader.Record]
  ): GlobalStateReader[IO] =
    new GlobalStateReader[IO] {
      def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] = IO.pure(None)

      def getStrict[V: ImmutableCodec](key: GlobalStateKey): IO[StrictMptRead[V]] =
        IO.pure(if (key == target) read.asInstanceOf[StrictMptRead[V]] else StrictMptRead.Absent)

      def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] = IO.pure(Map.empty)

      def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] = IO.pure(Map.empty)

      def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): IO[List[StrictMptEntry[V]]] = IO.pure(List.empty)
    }

  test("returns canonical field-12 state and preserves true absence") { res =>
    implicit val (hasher, _) = res
    val canonical = record(NonEmptySet.one(proof(idA, "33")))

    for {
      key <- GlobalStateKey.updateNodeParametersKey[IO](idA)
      present <- UpdateNodeParametersMptReader.read(
        readerAt(key, StrictMptRead.Present(canonical, unpRecordImmutableCodec.immutableBytes(canonical))),
        idA
      )
      absent <- UpdateNodeParametersMptReader.read(GlobalStateReader.empty[IO], idA)
    } yield expect.all(present.contains(canonical), absent.isEmpty)
  }

  test("fails closed on malformed and non-canonical field-12 bytes") { res =>
    implicit val (hasher, _) = res
    val canonical = record(NonEmptySet.one(proof(idA, "33")))
    val canonicalBytes = unpRecordImmutableCodec.immutableBytes(canonical)

    for {
      key <- GlobalStateKey.updateNodeParametersKey[IO](idA)
      malformed <- UpdateNodeParametersMptReader
        .read(readerAt(key, StrictMptRead.Malformed("undecodable", Some(ByteVector(1.toByte)))), idA)
        .attempt
      nonCanonical <- UpdateNodeParametersMptReader
        .read(readerAt(key, StrictMptRead.Present(canonical, canonicalBytes ++ ByteVector(0.toByte))), idA)
        .attempt
    } yield
      expect.all(
        malformed.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue]),
        nonCanonical.left.exists(_.isInstanceOf[StrictMptRead.MalformedConsensusMptValue])
      )
  }

  test("fails closed when proof cardinality or proof identity does not reproduce the field-12 key") { res =>
    implicit val (hasher, _) = res
    val wrongId = record(NonEmptySet.one(proof(idB, "44")))
    val multipleProofs = record(NonEmptySet.of(proof(idA, "33"), proof(idB, "44")))

    for {
      key <- GlobalStateKey.updateNodeParametersKey[IO](idA)
      wrong <- UpdateNodeParametersMptReader
        .read(readerAt(key, StrictMptRead.Present(wrongId, unpRecordImmutableCodec.immutableBytes(wrongId))), idA)
        .attempt
      multiple <- UpdateNodeParametersMptReader
        .read(readerAt(key, StrictMptRead.Present(multipleProofs, unpRecordImmutableCodec.immutableBytes(multipleProofs))), idA)
        .attempt
    } yield
      expect.all(
        wrong.left.exists(_.isInstanceOf[StrictMptRead.InconsistentConsensusMptIndex]),
        multiple.left.exists(_.isInstanceOf[StrictMptRead.InconsistentConsensusMptIndex])
      )
  }
}
