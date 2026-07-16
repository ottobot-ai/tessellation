package io.constellationnetwork.json

import cats.effect.{IO, Resource}
import cats.syntax.traverse._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import io.circe.{Decoder, HCursor, Printer}
import weaver.MutableIOSuite

object JsonBrotliBinarySerializerSuite extends MutableIOSuite {

  private final case class ThrowingDecode(value: String)

  private implicit val throwingDecodeDecoder: Decoder[ThrowingDecode] = new Decoder[ThrowingDecode] {
    def apply(cursor: HCursor): Decoder.Result[ThrowingDecode] =
      throw new IllegalStateException("hostile decoder")
  }

  type Res = (Hasher[IO], JsonSerializer[IO], JsonBrotliBinarySerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    KryoSerializer
      .forAsync[IO](sharedKryoRegistrar)
      .flatMap { implicit res =>
        JsonSerializer.forAsync[IO].asResource.map { implicit json =>
          (Hasher.forJson[IO], json)
        }
      }
      .flatMap { kp =>
        JsonBrotliBinarySerializer
          .forAsync[IO](Printer(dropNullValues = true, indent = "", sortKeys = true))
          .asResource
          .map((kp._1, kp._2, _))
      }

  test("should deserialize properly serialized object") {
    case (hasher, js, serializer) =>
      implicit val h = hasher
      implicit val j = js

      for {
        signedSnapshot <- JsonBinarySerializerSuite
          .currencyIncrementalSnapshot[IO](
            Hash.empty,
            CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)
          )
        serialized <- serializer.serialize(signedSnapshot)
        deserialized <- serializer.deserialize[Signed[CurrencyIncrementalSnapshot]](serialized)
      } yield expect.same(Right(signedSnapshot), deserialized)
  }

  test("should not deserialize different serialized object") {
    case (hasher, js, serializer) =>
      implicit val h = hasher
      implicit val j = js

      for {
        signedSnapshot <- JsonBinarySerializerSuite
          .currencyIncrementalSnapshot[IO](
            Hash.empty,
            CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)
          )
        serialized <- serializer.serialize(signedSnapshot)
        deserialized <- serializer.deserialize[CurrencySnapshot](serialized)
      } yield expect.same(true, deserialized.isLeft)
  }

  test("invalid Brotli bytes should return Left without failing the effect") {
    case (_, _, serializer) =>
      serializer.deserialize[String](Array[Byte](0x00, 0x01, 0x02, 0x03)).attempt.map {
        case Right(Left(_))      => success
        case Right(Right(value)) => failure(s"expected malformed input to be rejected, decoded '$value'")
        case Left(error)         => failure(s"deserialize failed the effect instead of returning Left: $error")
      }
  }

  test("bounded deserialize rejects compressed input before decompression") {
    case (_, _, serializer) =>
      for {
        serialized <- serializer.serialize("bounded")
        limits = JsonBrotliBinarySerializer.BrotliDecodeLimits(serialized.length.toLong - 1L, 1024L)
        result <- serializer.deserializeBounded[String](serialized, limits).attempt
      } yield
        result match {
          case Right(Left(JsonBrotliBinarySerializer.CompressedContentTooLarge(maximum, actual))) =>
            expect.same(serialized.length.toLong - 1L, maximum).and(expect.same(serialized.length.toLong, actual))
          case Right(other) => failure(s"expected compressed-size rejection, got $other")
          case Left(error)  => failure(s"bounded deserialize failed the effect instead of returning Left: $error")
        }
  }

  test("bounded deserialize stops a highly-compressible expansion at the decompressed limit") {
    case (_, _, serializer) =>
      val value = "a" * 100000

      for {
        serialized <- serializer.serialize(value)
        limits = JsonBrotliBinarySerializer.BrotliDecodeLimits(serialized.length.toLong, 1024L)
        result <- serializer.deserializeBounded[String](serialized, limits).attempt
      } yield
        result match {
          case Right(Left(JsonBrotliBinarySerializer.DecompressedContentTooLarge(maximum, actualAtLeast))) =>
            expect(serialized.length < maximum)
              .and(expect.same(1024L, maximum))
              .and(expect.same(maximum + 1L, actualAtLeast))
          case Right(other) => failure(s"expected decompressed-size rejection, got $other")
          case Left(error)  => failure(s"bounded deserialize failed the effect instead of returning Left: $error")
        }
  }

  test("bounded deserialize accepts the exact decompressed limit and rejects one byte less") {
    case (_, _, serializer) =>
      val value = "bounded"
      val exactJsonBytes = value.length.toLong + 2L

      for {
        serialized <- serializer.serialize(value)
        exactLimits = JsonBrotliBinarySerializer.BrotliDecodeLimits(serialized.length.toLong, exactJsonBytes)
        belowLimits = exactLimits.copy(maxDecompressedBytes = exactJsonBytes - 1L)
        exact <- serializer.deserializeBounded[String](serialized, exactLimits)
        below <- serializer.deserializeBounded[String](serialized, belowLimits)
      } yield
        expect.same(Right(value), exact).and {
          below match {
            case Left(JsonBrotliBinarySerializer.DecompressedContentTooLarge(maximum, actualAtLeast)) =>
              expect.same(exactJsonBytes - 1L, maximum).and(expect.same(exactJsonBytes, actualAtLeast))
            case other => failure(s"expected one-byte-below decompressed rejection, got $other")
          }
        }
  }

  test("bounded deserialize contains malformed and truncated Brotli input without failing the effect") {
    case (_, _, serializer) =>
      val limits = JsonBrotliBinarySerializer.BrotliDecodeLimits(1024L, 1024L)

      for {
        serialized <- serializer.serialize("truncated")
        malformed <- serializer.deserializeBounded[String](Array[Byte](0x00, 0x01, 0x02, 0x03), limits).attempt
        truncated <- serializer.deserializeBounded[String](serialized.dropRight(1), limits).attempt
      } yield expect(malformed.exists(_.isLeft)).and(expect(truncated.exists(_.isLeft)))
  }

  test("bounded deserialize contains a throwing domain decoder without failing the effect") {
    case (_, _, serializer) =>
      for {
        serialized <- serializer.serialize("valid-json")
        limits = JsonBrotliBinarySerializer.BrotliDecodeLimits(serialized.length.toLong, 1024L)
        result <- serializer.deserializeBounded[ThrowingDecode](serialized, limits).attempt
      } yield
        result match {
          case Right(Left(error: IllegalStateException)) => expect.same("hostile decoder", error.getMessage)
          case Right(other)                              => failure(s"expected contained decoder failure, got $other")
          case Left(error)                               => failure(s"bounded deserialize failed the effect: $error")
        }
  }

  test("bounded deserialize rejects invalid limits without failing the effect") {
    case (_, _, serializer) =>
      val content = Array[Byte](0x00)
      val invalid = List(
        JsonBrotliBinarySerializer.BrotliDecodeLimits(0L, 1L),
        JsonBrotliBinarySerializer.BrotliDecodeLimits(1L, 0L),
        JsonBrotliBinarySerializer.BrotliDecodeLimits(Int.MaxValue.toLong + 1L, 1L),
        JsonBrotliBinarySerializer.BrotliDecodeLimits(1L, Int.MaxValue.toLong + 1L)
      )

      invalid.traverse(serializer.deserializeBounded[String](content, _).attempt).map { results =>
        expect(results.forall(_.exists(_.left.exists(_.isInstanceOf[IllegalArgumentException]))))
      }
  }

}
