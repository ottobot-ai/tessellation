package io.constellationnetwork.json

import cats.effect.{IO, Resource}

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
import io.circe.Printer
import weaver.MutableIOSuite

object JsonBrotliBinarySerializerSuite extends MutableIOSuite {

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

}
