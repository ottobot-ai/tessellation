package io.constellationnetwork.serde

import scala.util.Try

import scodec.codecs.bool
import weaver.SimpleIOSuite

object ImmutableCodecSuite extends SimpleIOSuite {

  pureTest("fromScodecCodec rejects non-byte-aligned immutable encodings") {
    val codec = ImmutableCodec.fromScodecCodec(bool)
    val result = Try(codec.immutableBytes(true)).toEither

    result match {
      case Left(error: IllegalArgumentException) =>
        expect.eql(
          error.getMessage,
          "ImmutableCodec encode produced a non-byte-aligned payload of 1 bits"
        )
      case other =>
        failure(s"Expected non-byte-aligned encoding failure, got $other")
    }
  }

  pureTest("fromScodec rejects non-byte-aligned immutable encodings") {
    val codec = ImmutableCodec.fromScodec(bool, bool)
    val result = Try(codec.immutableBytes(false)).toEither

    result match {
      case Left(error: IllegalArgumentException) =>
        expect.eql(
          error.getMessage,
          "ImmutableCodec encode produced a non-byte-aligned payload of 1 bits"
        )
      case other =>
        failure(s"Expected non-byte-aligned encoding failure, got $other")
    }
  }
}
