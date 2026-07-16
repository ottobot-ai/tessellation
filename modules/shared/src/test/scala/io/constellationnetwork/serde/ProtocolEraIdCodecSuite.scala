package io.constellationnetwork.serde

import io.constellationnetwork.schema.era.ProtocolEraId
import io.constellationnetwork.schema.era.ProtocolEraId.ScodecV1
import io.constellationnetwork.serde.codecs.instances.ProtocolEraIdCodec._
import io.constellationnetwork.serde.implicits._

import scodec.bits.ByteVector
import weaver.FunSuite

object ProtocolEraIdCodecSuite extends FunSuite {

  private val scodecV1Bytes = GoldenVectors.load("ProtocolEraId-scodec-v1")

  test("ScodecV1 has the frozen one-byte era identity") {
    val value: ProtocolEraId = ScodecV1

    expect(value.immutableBytes == scodecV1Bytes)
  }

  test("ScodecV1 round-trips through the complete immutable codec") {
    expect(scodecV1Bytes.fromImmutableBytes[ProtocolEraId] == Right(ScodecV1))
  }

  test("empty input rejects") {
    expect(ByteVector.empty.fromImmutableBytes[ProtocolEraId].isLeft)
  }

  test("every one-byte tag other than ScodecV1 rejects") {
    val rejected = (0 to 255).filterNot(_ == 0x01).forall { tag =>
      ByteVector(tag).fromImmutableBytes[ProtocolEraId].isLeft
    }

    expect(rejected)
  }

  test("trailing bytes reject") {
    expect((scodecV1Bytes ++ ByteVector(0x00)).fromImmutableBytes[ProtocolEraId].isLeft)
  }

  test("the greenfield build exposes exactly one protocol era") {
    expect(ProtocolEraId.all == List(ScodecV1))
  }
}
