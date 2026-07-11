package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.eq._

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object ShardWindowContinuationSuite extends SimpleIOSuite {

  private val proof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  private def bin(parent: Hash): Signed[StateChannelSnapshotBinary] =
    Signed(
      StateChannelSnapshotBinary(parent, Array[Byte](0), SnapshotFee(NonNegLong.unsafeFrom(0L))),
      NonEmptySet.of(proof)
    )

  private def h(c: String): Hash = Hash(c * 64)
  private val A = h("a")
  private val B = h("b")
  private val C = h("c")
  private val X = h("e")
  private val W = h("f")

  pureTest("Continue: a window binary continues GL0's tip at the head") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B)), W, A) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("Continue: a window binary continues GL0's tip after an already-adopted prefix") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B), bin(C)), W, B) ==
        ShardWindowContinuation.Continue(1)
    )
  }

  pureTest("unseeded metagraph: a genesis window continues Hash.empty") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(Hash.empty), bin(A)), W, Hash.empty) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("sibling lineage: genesis-rooted window is deferred when it does not continue the committed tip") {
    val sibling = NonEmptyList.of(bin(Hash.empty), bin(A), bin(B), bin(C))
    expect(ShardWindowContinuation.classify(sibling, W, X) == ShardWindowContinuation.Defer)
  }

  pureTest("AlreadyAdopted: a window whose tail is GL0's tip is a no-op") {
    val T = h("d")
    val fullyAdopted = NonEmptyList.of(bin(Hash.empty), bin(A))
    expect(ShardWindowContinuation.classify(fullyAdopted, T, T) == ShardWindowContinuation.AlreadyAdopted)
  }

  pureTest("Continue takes precedence over AlreadyAdopted") {
    val T = h("d")
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(T), bin(A)), T, T) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("successor window continues the tip at which its predecessor ended") {
    val T = h("d")
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(T), bin(A)), W, T) ==
        ShardWindowContinuation.Continue(0)
    )
  }

  pureTest("missing ancestor: a mid-rooted window that does not continue the tip is deferred") {
    expect(
      ShardWindowContinuation.classify(NonEmptyList.of(bin(A), bin(B)), W, X) == ShardWindowContinuation.Defer
    )
  }

  test("windowTipHashF uses the same value-hash as GL0's state-channel tip setter") {
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val nel = NonEmptyList.of(bin(Hash.empty), bin(A))
      for {
        viaHelper <- ShardWindowContinuation.windowTipHashF[IO](nel)
        viaTipSetter <- nel.last.toHashed[IO].map(_.hash)
      } yield expect(viaHelper === viaTipSetter)
    }
  }
}
