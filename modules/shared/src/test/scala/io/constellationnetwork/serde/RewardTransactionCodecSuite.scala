package io.constellationnetwork.serde

import io.constellationnetwork.schema.transaction.{RewardTransaction, TransactionAmount}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.RewardTransactionCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.PosLong
import scodec.bits.ByteVector
import weaver.FunSuite

/** Golden + round-trip suite for `RewardTransaction` — `(Address, TransactionAmount)`.
  *
  * A minimal 2-field compound: length-prefixed address (41 or 52 bytes) followed by
  * an 8-byte PosLong amount. The golden uses a 40-char address so the total is 49 bytes.
  */
object RewardTransactionCodecSuite extends FunSuite {

  private val dstAddr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  private def sample: RewardTransaction =
    RewardTransaction(
      destination = dstAddr,
      amount = TransactionAmount(PosLong.unsafeFrom(1234567890L))
    )

  private def golden = GoldenVectors.load("RewardTransaction-scodec-v1")

  test("RewardTransaction encodes to golden bytes") {
    expect(sample.immutableBytes == golden)
  }

  test("RewardTransaction decodes golden bytes back to sample") {
    expect(golden.fromImmutableBytes[RewardTransaction] == Right(sample))
  }

  test("RewardTransaction is 49 bytes for a 40-char address (1 len + 40 ascii + 8 amount)") {
    expect(sample.immutableBytes.length == 49L)
  }

  test("amount occupies the final 8 bytes") {
    val bytes = sample.immutableBytes
    val amountSlice = bytes.drop(bytes.length - 8)
    // 1234567890 as big-endian int64
    expect(amountSlice == ByteVector.fromValidHex("00000000499602d2"))
  }

  test("round-trip preserves value") {
    val bytes = sample.immutableBytes
    expect(bytes.fromImmutableBytes[RewardTransaction] == Right(sample))
  }

  test("wrong-length encoded bytes fail decode") {
    val truncated = sample.immutableBytes.dropRight(1)
    truncated.fromImmutableBytes[RewardTransaction] match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }
}
