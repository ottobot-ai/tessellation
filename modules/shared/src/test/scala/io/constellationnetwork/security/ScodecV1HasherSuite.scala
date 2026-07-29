package io.constellationnetwork.security

import cats.effect.IO
import cats.syntax.all._

import io.constellationnetwork.serde.ImmutableCodec

import scodec.bits.ByteVector
import scodec.codecs.int32
import weaver.SimpleIOSuite

object ScodecV1HasherSuite extends SimpleIOSuite {

  private sealed trait NotGiven[A]

  private object NotGiven {
    implicit def absent[A]: NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent1[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
    implicit def ambiguousWhenPresent2[A](implicit present: A): NotGiven[A] = new NotGiven[A] {}
  }

  private def schema[A](
    codec: ImmutableCodec[A],
    domain: String,
    maxEncodedBytes: Long
  ): ConsensusHashSchema[A] =
    ConsensusHashSchema
      .make(codec, domain, maxEncodedBytes)
      .fold(error => throw new IllegalArgumentException(error.message), identity)

  private final case class Alpha(value: Int)

  private object Alpha {
    val immutableCodec: ImmutableCodec[Alpha] =
      ImmutableCodec.fromScodecCodec(int32.xmap(Alpha(_), _.value))

    implicit val consensusHashSchema: ConsensusHashSchema[Alpha] =
      schema(immutableCodec, "tessellation/test/alpha/v1", maxEncodedBytes = 4L)
  }

  private final case class Beta(value: Int)

  private object Beta {
    val immutableCodec: ImmutableCodec[Beta] =
      ImmutableCodec.fromScodecCodec(int32.xmap(Beta(_), _.value))

    implicit val consensusHashSchema: ConsensusHashSchema[Beta] =
      schema(immutableCodec, "tessellation/test/beta/v1", maxEncodedBytes = 4L)
  }

  private final case class Unencodable()

  private object Unencodable {
    val immutableCodec: ImmutableCodec[Unencodable] = new ImmutableCodec[Unencodable] {
      def immutableBytes(value: Unencodable): ByteVector =
        throw new IllegalArgumentException("deliberate immutable encoding failure")

      def fromImmutableBytes(bytes: ByteVector): Either[io.constellationnetwork.serde.SerdeError, Unencodable] =
        Right(Unencodable())
    }

    implicit val consensusHashSchema: ConsensusHashSchema[Unencodable] =
      schema(immutableCodec, "tessellation/test/unencodable/v1", maxEncodedBytes = 4L)
  }

  private final case class Oversized(value: Int)

  private object Oversized {
    val immutableCodec: ImmutableCodec[Oversized] =
      ImmutableCodec.fromScodecCodec(int32.xmap(Oversized(_), _.value))

    implicit val consensusHashSchema: ConsensusHashSchema[Oversized] =
      schema(immutableCodec, "tessellation/test/oversized/v1", maxEncodedBytes = 3L)
  }

  private val hasher = Hasher.forScodec[IO]

  test("matches the frozen SHA-256 known-answer vector") {
    hasher.digest(Alpha(0x01020304)).map { digest =>
      expect.eql(
        digest.toHexString,
        "9b59433cdaab140924210bb4f95694347f66121dbac62c1063588a5be7c5ee5c"
      ) &&
      expect.eql(digest.toByteVector.length, ConsensusDigest.Length)
    }
  }

  test("separates equal immutable bytes by their static type domain") {
    (hasher.digest(Alpha(0x01020304)), hasher.digest(Beta(0x01020304))).mapN { (alpha, beta) =>
      expect(alpha != beta) &&
      expect.eql(beta.toHexString, "8ec2bf606c2fc94b9e859ddb70e71458f720fbd119b4a7bded91833fd0967b0a")
    }
  }

  test("changes the digest when immutable value bytes change") {
    (hasher.digest(Alpha(0x01020304)), hasher.digest(Alpha(0x01020305))).mapN { (first, second) =>
      expect(first != second) &&
      expect.eql(second.toHexString, "42ae8307ad93580a7f533e82ba33924a9ae37289238862e96592d460b206d00f")
    }
  }

  test("independent equal digests have value equality and set identity") {
    (hasher.digest(Alpha(0x01020304)), hasher.digest(Alpha(0x01020304))).mapN { (first, second) =>
      expect(first == second) &&
      expect(first === second) &&
      expect.eql(first.hashCode(), second.hashCode()) &&
      expect.eql(Set(first, second).size, 1) &&
      expect.eql(first.show, s"ConsensusDigest(${first.toHexString})")
    }
  }

  test("captures immutable encoding failure without a serializer fallback") {
    hasher.digest(Unencodable()).attempt.map {
      case Left(error: IllegalArgumentException) =>
        expect(error.getMessage == "deliberate immutable encoding failure")
      case result =>
        failure(s"Expected immutable encoding failure, got $result")
    }
  }

  test("rejects an immutable encoding larger than the schema bound") {
    hasher.digest(Oversized(1)).attempt.map {
      case Left(ScodecV1Hasher.EncodedValueTooLarge(actual, maximum)) =>
        expect.eql(actual, 4L) && expect.eql(maximum, 3L)
      case result =>
        failure(s"Expected encoded-value bound failure, got $result")
    }
  }

  pureTest("schema construction validates codec, domain, and encoded-size bound") {
    val maximum =
      "tessellation/" + ("a" * (ConsensusHashSchema.MaxDomainLength - "tessellation/".length - "/v1".length)) + "/v1"

    expect(ConsensusHashSchema.make[Alpha](null, "valid", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, null, 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "line\nbreak", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "\u007f", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "non-ascii-\u00e9", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, maximum + "a", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "tessellation/test/v1", 0L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "Tessellation/test/v1", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "tessellation.test.v1", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "tessellation/test/v0", 4L).isLeft) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, "tessellation/test/v1", 4L).isRight) &&
    expect(ConsensusHashSchema.make(Alpha.immutableCodec, maximum, 4L).isRight)
  }

  pureTest("domain framing is one unsigned length byte followed by exact ASCII") {
    val short = ConsensusHashSchema.make(Alpha.immutableCodec, "tessellation/a/v1", 4L)
    val maximumDomain =
      "tessellation/" + ("a" * (ConsensusHashSchema.MaxDomainLength - "tessellation/".length - "/v1".length)) + "/v1"
    val maximum =
      ConsensusHashSchema.make(Alpha.immutableCodec, maximumDomain, 4L)

    expect(short.exists(_.lengthDelimitedDomain.toHex == "1174657373656c6c6174696f6e2f612f7631")) &&
    expect(maximum.exists(_.lengthDelimitedDomain.headOption.contains(0xff.toByte))) &&
    expect(maximum.exists(_.lengthDelimitedDomain.length == 1L + ConsensusHashSchema.MaxDomainLength))
  }

  pureTest("raw and projection types have no ambient consensus hash schema") {
    val _ = implicitly[NotGiven[ConsensusHashSchema[String]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[Array[Byte]]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[ByteVector]]]
    val _ = implicitly[NotGiven[ConsensusHashSchema[io.circe.Json]]]

    success
  }

  pureTest("ConsensusDigest validates length and owns its bytes") {
    val source = Array.tabulate[Byte](ConsensusDigest.Length.toInt)(_.toByte)
    val result = ConsensusDigest.fromByteArray(source)

    result match {
      case Right(digest) =>
        val expected = digest.toHexString
        source(0) = 99.toByte
        val duplicate = ConsensusDigest.fromByteVector(ByteVector.fromValidHex(expected))

        expect.eql(digest.toHexString, expected) &&
        expect.eql(digest.toByteVector.length, ConsensusDigest.Length) &&
        expect(duplicate.contains(digest)) &&
        expect(duplicate.exists(_.hashCode() == digest.hashCode())) &&
        expect(duplicate.exists(ConsensusDigest.eq.eqv(_, digest))) &&
        expect(ConsensusDigest.fromByteVector(ByteVector.fill(31)(0)).isLeft) &&
        expect(ConsensusDigest.fromByteVector(ByteVector.fill(33)(0)).isLeft)
      case Left(error) =>
        failure(error.message)
    }
  }
}
