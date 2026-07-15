package io.constellationnetwork.schema.mpt

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{FileSystemMerklePatriciaProducer, InMemoryMerklePatriciaProducer}
import io.constellationnetwork.serde.codecs.StringCodec._
import io.constellationnetwork.serde.{ImmutableCodec, SerdeError}

import fs2.io.file.Files
import scodec.bits.ByteVector
import weaver.MutableIOSuite

object StrictMptEnumerationSuite extends MutableIOSuite {

  type Res = (Hasher[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      hasher = Hasher.forJson[IO]
    } yield (hasher, json)

  private def encoded(value: String): Array[Byte] =
    ImmutableCodec[String].immutableBytes(value).toArray

  private def freshStore(
    implicit hasher: Hasher[IO],
    json: JsonSerializer[IO]
  ): IO[MptStore[IO, Hex]] =
    InMemoryMerklePatriciaProducer.make[IO]().flatMap(MptStore.make[IO, Hex](_, _.pure[IO]))

  test("strict prefix scan retains malformed, empty, and null entries in physical-key order") { res =>
    implicit val (hasher, json) = res
    val validKey = Hex("aa03")
    val caseAliasKey = Hex("AA03")
    val malformedKey = Hex("aa02")
    val emptyKey = Hex("aa01")
    val nullKey = Hex("aa00")
    val outsideKey = Hex("bb00")

    for {
      store <- freshStore
      _ <- store.underlying
        .insertBytes(
          Map(
            validKey -> encoded("valid"),
            caseAliasKey -> encoded("case-alias"),
            malformedKey -> Array[Byte](0x7f),
            emptyKey -> Array.emptyByteArray,
            nullKey -> null,
            outsideKey -> encoded("outside")
          )
        )
        .flatMap(_.liftTo[IO])
      strict <- store.getAllForPrefixStrict[String](Hex("aa"))
      legacy <- store.getAllForPrefix[String](Hex("aa")).attempt
    } yield {
      val reads = strict.map(entry => entry.physicalKey -> entry.read).toMap
      expect.all(
        strict.map(_.physicalKey) == List(caseAliasKey, nullKey, emptyKey, malformedKey, validKey),
        reads.get(caseAliasKey).exists {
          case StrictMptRead.Present("case-alias", _) => true
          case _                                      => false
        },
        reads.get(validKey).exists {
          case StrictMptRead.Present("valid", raw) => raw == ByteVector.view(encoded("valid"))
          case _                                   => false
        },
        reads.get(malformedKey).exists {
          case StrictMptRead.Malformed(_, Some(raw)) => raw == ByteVector(0x7f)
          case _                                     => false
        },
        reads.get(emptyKey).exists {
          case StrictMptRead.Malformed(reason, Some(raw)) => reason == "empty stored bytes" && raw.isEmpty
          case _                                          => false
        },
        reads.get(nullKey).contains(StrictMptRead.Malformed("null stored bytes", None)),
        !reads.contains(outsideKey),
        legacy.isLeft
      )
    }
  }

  test("strict decoded and raw enumerations own their bytes") { res =>
    implicit val (hasher, json) = res
    val validKey = Hex("aa10")
    val malformedKey = Hex("aa11")
    val nullKey = Hex("aa12")
    val valid = encoded("owned")
    val malformed = Array[Byte](0x7f)
    val originalValid = valid.clone()
    val originalMalformed = malformed.clone()

    for {
      store <- freshStore
      _ <- store.underlying
        .insertBytes(Map(validKey -> valid, malformedKey -> malformed, nullKey -> null))
        .flatMap(_.liftTo[IO])
      decoded <- store.getAllForPrefixStrict[String](Hex("aa"))
      decodedAgain <- store.getAllForPrefixStrict[String](Hex("aa"))
      raw <- store.allEntriesStrict
      _ = valid.indices.foreach(index => valid(index) = 0.toByte)
      _ = malformed(0) = 0.toByte
      afterInputMutation <- store.allEntriesStrict
      legacy <- store.allEntriesAsBytes
      _ = legacy(validKey)(0) = 0.toByte
      _ = legacy(malformedKey)(0) = 0.toByte
      afterLegacyOutputMutation <- store.allEntriesStrict
      producerEntries <- store.underlying.entries
      _ = producerEntries(validKey)(0) = 0.toByte
      producerPoint <- store.underlying.entry(validKey)
      _ = producerPoint.foreach(bytes => bytes(0) = 0.toByte)
      producerSelected <- store.underlying.entriesForKeys(Set(validKey, malformedKey))
      _ = producerSelected.values.foreach(bytes => bytes(0) = 0.toByte)
      afterProducerOutputMutation <- store.allEntriesStrict
    } yield {
      val decodedByKey = decoded.map(entry => entry.physicalKey -> entry.read).toMap
      val rawByKey = raw.map(entry => entry.physicalKey -> entry.rawBytes).toMap
      val afterInputByKey = afterInputMutation.map(entry => entry.physicalKey -> entry.rawBytes).toMap
      val afterLegacyByKey = afterLegacyOutputMutation.map(entry => entry.physicalKey -> entry.rawBytes).toMap
      val afterProducerByKey = afterProducerOutputMutation.map(entry => entry.physicalKey -> entry.rawBytes).toMap

      expect.all(
        decoded == decodedAgain,
        decodedByKey.get(validKey).exists {
          case StrictMptRead.Present("owned", bytes) => bytes == ByteVector.view(originalValid)
          case _                                     => false
        },
        decodedByKey.get(malformedKey).exists {
          case StrictMptRead.Malformed(_, Some(bytes)) => bytes == ByteVector.view(originalMalformed)
          case _                                       => false
        },
        raw.map(_.physicalKey) == List(validKey, malformedKey, nullKey),
        rawByKey.get(validKey).contains(Some(ByteVector.view(originalValid.clone()))),
        rawByKey.get(malformedKey).contains(Some(ByteVector.view(originalMalformed.clone()))),
        rawByKey.get(nullKey).contains(None),
        afterInputByKey.get(validKey).contains(Some(ByteVector.view(originalValid.clone()))),
        afterInputByKey.get(malformedKey).contains(Some(ByteVector.view(originalMalformed.clone()))),
        afterLegacyByKey.get(validKey).contains(Some(ByteVector.view(originalValid.clone()))),
        afterLegacyByKey.get(malformedKey).contains(Some(ByteVector.view(originalMalformed.clone()))),
        afterProducerByKey.get(validKey).contains(Some(ByteVector.view(originalValid.clone()))),
        afterProducerByKey.get(malformedKey).contains(Some(ByteVector.view(originalMalformed.clone())))
      )
    }
  }

  test("filesystem producer owns inserted bytes and every raw read result") { res =>
    implicit val (hasher, json) = res
    val key = Hex("DD00")
    val original = encoded("filesystem-owned")
    val input = original.clone()

    Files[IO].tempDirectory.use { directory =>
      for {
        producer <- FileSystemMerklePatriciaProducer.make[IO](directory / "mpt")
        store <- MptStore.make[IO, Hex](producer, _.pure[IO])
        _ <- producer.insertBytes(Map(key -> input)).flatMap(_.liftTo[IO])
        full <- producer.entries
        point <- producer.entry(key)
        selected <- producer.entriesForKeys(Set(key))
        prefixed <- producer.entriesWithPrefix(Hex("dd"))
        legacy <- store.allEntriesAsBytes
        _ = input(0) = 0.toByte
        _ = full(key)(0) = 0.toByte
        _ = point.foreach(bytes => bytes(0) = 0.toByte)
        _ = selected(key)(0) = 0.toByte
        _ = prefixed(key)(0) = 0.toByte
        _ = legacy(key)(0) = 0.toByte
        retained <- store.allEntriesStrict
      } yield
        expect(
          retained == List(StrictMptRawEntry(key, Some(ByteVector.view(original.clone()))))
        )
    }
  }

  test("strict decoding retains bytes when a codec throws") { _ =>
    final case class ThrowingValue(value: String)
    implicit val throwingCodec: ImmutableCodec[ThrowingValue] = new ImmutableCodec[ThrowingValue] {
      def immutableBytes(value: ThrowingValue): ByteVector = ByteVector.encodeUtf8(value.value).getOrElse(ByteVector.empty)
      def fromImmutableBytes(bytes: ByteVector): Either[SerdeError, ThrowingValue] =
        throw new IllegalStateException("test decoder failure")
    }

    val key = Hex("cc00")
    val bytes = Array[Byte](1, 2, 3)
    val decoded = StrictMptRead.decodeEntries[ThrowingValue](Map(key -> bytes))

    IO.pure(
      expect(decoded match {
        case List(StrictMptEntry(`key`, StrictMptRead.Malformed(reason, Some(retained)))) =>
          reason.contains("decoder threw") && reason.contains("test decoder failure") && retained == ByteVector.view(bytes)
        case _ => false
      })
    )
  }

  test("pure strict helpers are deterministic for arbitrary input iteration order") { _ =>
    val entriesA = List(
      Hex("ff02") -> encoded("two"),
      Hex("ff00") -> null,
      Hex("ff01") -> Array[Byte](0x7f)
    )
    val entriesB = entriesA.reverse

    val decodedA = StrictMptRead.decodeEntries[String](entriesA.toMap)
    val decodedB = StrictMptRead.decodeEntries[String](entriesB.toMap)
    val rawA = StrictMptRead.captureRawEntries(entriesA.toMap)
    val rawB = StrictMptRead.captureRawEntries(entriesB.toMap)
    def decodedShape(entries: List[StrictMptEntry[String]]): List[(Hex, String, Option[List[Byte]])] =
      entries.map {
        case StrictMptEntry(key, StrictMptRead.Present(value, bytes)) =>
          (key, s"present:$value", Some(bytes.toArray.toList))
        case StrictMptEntry(key, StrictMptRead.Malformed(reason, bytes)) =>
          (key, s"malformed:$reason", bytes.map(_.toArray.toList))
        case StrictMptEntry(key, StrictMptRead.Absent) =>
          (key, "absent", None)
      }

    IO.pure(
      expect.all(
        decodedA.map(_.physicalKey) == List(Hex("ff00"), Hex("ff01"), Hex("ff02")),
        decodedShape(decodedA) == decodedShape(decodedB),
        rawA == rawB
      )
    )
  }
}
