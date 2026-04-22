package io.constellationnetwork.serde

import cats.data.NonEmptyList

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.priceOracle._
import io.constellationnetwork.schema.swap.SwapAmount
import io.constellationnetwork.schema.tokenLock.TokenLockAmount
import io.constellationnetwork.schema.{NonNegFraction => NNF, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.SharedArtifactCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.numeric.{NonNegative, Positive}
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.FunSuite

/** Round-trip suite for `SharedArtifact` sum type, exercising every variant.
  *
  * Byte-distinct encodings for each variant verify the discriminator wiring.
  */
object SharedArtifactCodecSuite extends FunSuite {

  private val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")

  private def fraction = NNF(
    Refined.unsafeApply[Long, NonNegative](1L),
    Refined.unsafeApply[Long, Positive](2L)
  )

  private val spendAction: SharedArtifact = SpendAction(
    NonEmptyList.of(
      SpendTransaction(Some(Hash("a" * 64)), None, SwapAmount(PosLong.unsafeFrom(1L)), addr, addr)
    )
  )
  private val tokenUnlock: SharedArtifact = TokenUnlock(Hash("b" * 64), TokenLockAmount(PosLong.unsafeFrom(5L)), None, addr)
  private val allowExp: SharedArtifact = AllowSpendExpiration(Hash("c" * 64))
  private val pricing: SharedArtifact = PricingUpdate(PriceFraction(TokenPair.DAG_USD, fraction))
  private val balanceAdj: SharedArtifact = BalanceAdjustment(
    addr,
    SpendTransactionNotApplied,
    SortedSet(Hash("d" * 64)),
    Some(Amount(NonNegLong.unsafeFrom(100L))),
    None
  )
  private val globalProc: SharedArtifact = GlobalSnapshotsProcessed(
    SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(10L)), SnapshotOrdinal(NonNegLong.unsafeFrom(11L)))
  )

  private val all: List[SharedArtifact] = List(spendAction, tokenUnlock, allowExp, pricing, balanceAdj, globalProc)

  test("Every SharedArtifact variant round-trips through the sum-type codec") {
    val decoded = all.map(_.immutableBytes.fromImmutableBytes[SharedArtifact])
    expect(decoded == all.map(Right(_)))
  }

  test("Each variant's discriminator byte is distinct (6 distinct prefixes)") {
    val prefixes = all.map(_.immutableBytes.take(1)).toSet
    expect(prefixes.size == 6)
  }

  test("Variant discriminator bytes match the FROZEN spec (0x00..0x05)") {
    val prefixes = all.map(_.immutableBytes.head)
    expect(prefixes == List[Byte](0x00, 0x01, 0x02, 0x03, 0x04, 0x05))
  }
}
