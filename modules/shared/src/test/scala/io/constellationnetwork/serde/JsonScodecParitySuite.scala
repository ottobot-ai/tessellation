package io.constellationnetwork.serde

import java.io.ByteArrayInputStream

import cats.effect.{IO, Resource}

import scala.io.Source
import scala.util.Using

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshot}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs._
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.SignedCodec._
import io.constellationnetwork.serde.implicits._

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.decoder.BrotliInputStream
import io.circe.Decoder
import io.circe.parser.decode
import weaver.MutableIOSuite

/** Parity suite validating our scodec codecs against real Brotli-JSON snapshots from an 8-node sim (copied into test resources).
  *
  * The pipeline for each fixture:
  *   1. Read Brotli-compressed JSON bytes from `test/resources/serde/real/`. 2. Decompress → UTF-8 JSON string. 3. Decode via the existing
  *      circe `Decoder[T]` (the live read path). 4. Encode that Scala value via our scodec codec. 5. Decode the scodec bytes back. 6.
  *      Assert equal to the circe-decoded value.
  *
  * A mismatch means our scodec codec for `T` doesn't faithfully round-trip the Scala representation of live on-disk data. This is the
  * validation layer that catches field-order / Option / sum-type mistakes the unit-level round-trip tests would miss.
  *
  * The upstream-v4 incremental fixtures predate this greenfield fork's mandatory shard fields. They are rejection fixtures, not a reason to
  * default absent fork-only fields; current non-empty incremental coverage lives in `ShardingScodecCodecsSuite`.
  */
object JsonScodecParitySuite extends MutableIOSuite {

  type Res = Unit
  override def sharedResource: Resource[IO, Unit] =
    Resource.eval(IO(Brotli4jLoader.ensureAvailability()))

  private def readFixture(name: String): Array[Byte] = {
    val path = s"/serde/real/$name"
    val stream = Option(getClass.getResourceAsStream(path))
      .getOrElse(throw new IllegalStateException(s"Missing fixture: $path"))
    Using.resource(stream)(_.readAllBytes())
  }

  private def brotliToJson(compressed: Array[Byte]): String = {
    val bis = new BrotliInputStream(new ByteArrayInputStream(compressed))
    try Source.fromInputStream(bis).mkString
    finally bis.close()
  }

  private def parityCheck[T: Decoder](
    fixtureName: String,
    scodecImmutable: ImmutableCodec[T]
  ): Boolean = {
    val json = brotliToJson(readFixture(fixtureName))
    val circeDecoded = decode[T](json).fold(err => throw new AssertionError(s"circe decode failed: $err"), identity)
    val scodecBytes = scodecImmutable.immutableBytes(circeDecoded)
    val scodecDecoded = scodecBytes
      .fromImmutableBytes[T](scodecImmutable)
      .fold(err => throw new AssertionError(s"scodec decode failed: $err"), identity)
    scodecDecoded == circeDecoded
  }

  private def currentSchemaRejects[T: Decoder](fixtureName: String): Boolean = {
    val json = brotliToJson(readFixture(fixtureName))
    decode[T](json).isLeft
  }

  // ---- Tests --------------------------------------------------------------

  test("upstream-v4 incremental fixtures are rejected by the strict greenfield schema") { _ =>
    val preGreenfieldFixtures = List(
      "incremental_snapshot_ordinal_1.brotli",
      "incremental_snapshot_ordinal_10.brotli",
      "incremental_snapshot_ordinal_100.brotli",
      "incremental_snapshot_ordinal_500.brotli",
      "incremental_snapshot_ordinal_700.brotli"
    )

    IO(expect(preGreenfieldFixtures.forall(currentSchemaRejects[Signed[GlobalIncrementalSnapshot]])))
  }

  test("full_snapshot genesis round-trips through scodec") { _ =>
    IO(
      expect(
        parityCheck[Signed[GlobalSnapshot]](
          "full_snapshot_ordinal_0.brotli",
          signedImmutableCodec(globalSnapshotCodec)
        )
      )
    )
  }

  // NOTE on mpt_snapshot_info fixtures:
  //   The `mpt_snapshot_info/<ordinal>` files on disk are NOT `GlobalSnapshotInfo` JSON — they are
  //   the serialized MPT key→value store at that ordinal (`Map[hex-GlobalStateKey, Array[Byte]]`).
  //   This is a structural confirmation that the existing tessellation storage is already
  //   key-addressed by `GlobalStateKey`, which dovetails directly with our `GlobalStateKeyCodec`
  //   work. A parity test for this shape belongs with the MPT integration layer (Phase 3) rather
  //   than here. Fixtures retained in test resources for that work.

  // Witness — keep CurrencySnapshotInfo import referenced even if no fixture uses it today.
  locally { val _ = implicitly[ImmutableCodec[CurrencySnapshotInfo]] }
}
