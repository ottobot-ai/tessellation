package io.constellationnetwork.serde

import io.constellationnetwork.schema.node._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.UpdateNodeParametersCodec.{
  rewardFractionImmutableCodec,
  updateNodeParametersImmutableCodec
}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip + boundary suite for the `UpdateNodeParameters` codec family. */
object UpdateNodeParametersCodecSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  test("RewardFraction round-trips at min (0), middle, and max (100_000_000)") {
    val samples = Seq(RewardFraction(0), RewardFraction(50_000_000), RewardFraction(100_000_000))
    val decoded = samples.map { rf =>
      val bytes = rewardFractionImmutableCodec.immutableBytes(rf)
      bytes.fromImmutableBytes[RewardFraction](rewardFractionImmutableCodec)
    }
    expect(decoded == samples.map(Right(_)))
  }

  test("RewardFraction encode is 4 bytes (Int)") {
    val bytes = rewardFractionImmutableCodec.immutableBytes(RewardFraction(100_000_000))
    expect(bytes.length == 4L)
  }

  test("Out-of-range bytes fail RewardFraction decode") {
    val badBytes = scodec.bits.ByteVector.fromValidHex("7fffffff") // max Int, way above 100M
    badBytes.fromImmutableBytes[RewardFraction](rewardFractionImmutableCodec) match {
      case Left(_: SerdeError.ScodecFailure) => success
      case other                             => failure(s"expected ScodecFailure, got $other")
    }
  }

  test("UpdateNodeParameters round-trips end-to-end") {
    val sample = UpdateNodeParameters(
      source = addr,
      delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction(5_000_000)),
      nodeMetadataParameters = NodeMetadataParameters("my-node", "a short description"),
      parent = UpdateNodeParametersReference(
        UpdateNodeParametersOrdinal(NonNegLong.unsafeFrom(7L)),
        Hash("c" * 64)
      )
    )
    val bytes = updateNodeParametersImmutableCodec.immutableBytes(sample)
    expect(bytes.fromImmutableBytes[UpdateNodeParameters](updateNodeParametersImmutableCodec) == Right(sample))
  }

  test("UpdateNodeParameters round-trips with unicode metadata") {
    val sample = UpdateNodeParameters(
      source = addr,
      delegatedStakeRewardParameters = DelegatedStakeRewardParameters(RewardFraction(0)),
      nodeMetadataParameters = NodeMetadataParameters("✓ node", "Test: αβγ δεζ"),
      parent = UpdateNodeParametersReference.empty
    )
    val bytes = updateNodeParametersImmutableCodec.immutableBytes(sample)
    expect(bytes.fromImmutableBytes[UpdateNodeParameters](updateNodeParametersImmutableCodec) == Right(sample))
  }
}
