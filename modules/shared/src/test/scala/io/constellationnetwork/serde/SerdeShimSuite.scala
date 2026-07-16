package io.constellationnetwork.serde

import io.constellationnetwork.serde.implicits._
import io.constellationnetwork.serde.storage.BrotliPersistable

import scodec.Codec
import scodec.bits.ByteVector
import scodec.codecs.{int64, utf8_32}
import shapeless.{::, HNil}
import weaver.FunSuite

/** Pilot type: an opaque struct that exercises the full shim. Not a consensus type — the shim tests only the plumbing. Real consensus
  * codecs ship one per PR under `serde/scodec/instances/...`.
  */
final case class PilotValue(counter: Long, label: String)

object PilotValue {
  // Hand-written scodec codec. No derivation; adding a field here would require
  // also updating this codec (and the golden file, once one exists).
  implicit val codec: Codec[PilotValue] =
    (int64 :: utf8_32)
      .xmap[PilotValue](
        { case c :: l :: HNil => PilotValue(c, l) },
        p => p.counter :: p.label :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[PilotValue] = ImmutableCodec.derivedFromScodec[PilotValue]

  // Brotli Persistable — opt-in. Shadows the default implicit Persistable.
  val brotliPersistable: Persistable[PilotValue] = BrotliPersistable.fromImmutableCodec(immutableCodec)
}

object SerdeShimSuite extends FunSuite {

  private val sample = PilotValue(counter = 42L, label = "hello")

  test("ImmutableCodec round-trips and is canonical") {
    val bytes = sample.immutableBytes
    val decoded = bytes.fromImmutableBytes[PilotValue]
    val reEncoded = decoded.map(_.immutableBytes)
    expect(decoded == Right(sample)).and(expect(reEncoded == Right(bytes)))
  }

  test("ImmutableCodec rejects a valid value with trailing bytes") {
    val bytes = sample.immutableBytes ++ ByteVector(0x7f)

    expect(bytes.fromImmutableBytes[PilotValue].isLeft)
  }

  test("Transmittable rejects a valid value with trailing bytes") {
    val transmittable = Transmittable.fromScodecCodec(PilotValue.codec)
    val bytes = transmittable.transmittableBytes(sample) ++ ByteVector(0x7f)

    expect(transmittable.fromTransmittableBytes(bytes).isLeft)
  }

  test("Default Persistable matches ImmutableCodec (no compression)") {
    val persist = sample.persistedBytes
    val imm = sample.immutableBytes
    expect(persist == imm)
  }

  test("BrotliPersistable round-trips; hashed bytes differ from disk bytes") {
    val persisted = PilotValue.brotliPersistable.persistedBytes(sample)
    val decoded = PilotValue.brotliPersistable.fromPersistedBytes(persisted)
    val immutable = sample.immutableBytes
    expect(decoded == Right(sample)).and(
      // Critical invariant: compressed disk bytes are NOT the hashable bytes.
      expect(persisted != immutable)
    )
  }

  test("BrotliPersistable rejects non-brotli input as SerdeError.BrotliFailure") {
    val garbage = ByteVector(Array[Byte](0x00, 0x01, 0x02, 0x03))
    val result = PilotValue.brotliPersistable.fromPersistedBytes(garbage)
    result match {
      case Left(_: SerdeError.BrotliFailure) => success
      case other                             => failure(s"expected BrotliFailure, got $other")
    }
  }

}
