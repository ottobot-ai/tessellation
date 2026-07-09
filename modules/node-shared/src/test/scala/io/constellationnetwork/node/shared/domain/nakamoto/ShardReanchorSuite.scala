package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.eq._

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Edge-case matrix for the orphaned-tip reanchor classifier (the 2026-06-29 sharded-mirror freeze fix, extended 2026-07-08 with the
  * fully-adopted `AlreadyAdopted` disambiguation — the 3gl0/2shard startup-freeze fix). `classify` is a pure function of (window, window
  * tail hash, tip hash, tip metagraph-ordinal) — the SAME logic the GSAM adopt-guard and the GSCF embed-selection call, so these cases pin
  * the cluster-wide (split-safe) behavior. Genesis-rooted ⇒ binary index == ordinal.
  */
object ShardReanchorSuite extends SimpleIOSuite {

  private val proof = SignatureProof(ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  /** A binary carrying only the parent hash the classifier reads — content is irrelevant (never decoded). */
  private def bin(parent: Hash): Signed[StateChannelSnapshotBinary] =
    Signed(
      StateChannelSnapshotBinary(parent, Array[Byte](0), SnapshotFee(NonNegLong.unsafeFrom(0L))),
      NonEmptySet.of(proof)
    )

  private def h(c: String): Hash = Hash(c * 64)
  private val A = h("a")
  private val B = h("b")
  private val C = h("c")
  private val X = h("e") // an orphaned tip hash present in NO window binary's parent
  private val W = h("f") // a window-tail hash that does NOT match the tip (the "tail is not the tip" placeholder)

  // A genesis-rooted contiguous window: index k ⇒ metagraph ordinal k (head parent === Hash.empty).
  private val genesisWindow = NonEmptyList.of(bin(Hash.empty), bin(A), bin(B), bin(C)) // ordinals 0,1,2,3

  pureTest("Continue: a window binary continues gl0's tip (idx 0)") {
    expect(ShardReanchor.classify(NonEmptyList.of(bin(A), bin(B)), W, A, Some(0L)) == ShardReanchor.Continue(0))
  }

  pureTest("Continue: tip continued mid-window") {
    expect(ShardReanchor.classify(NonEmptyList.of(bin(A), bin(B), bin(C)), W, B, Some(1L)) == ShardReanchor.Continue(1))
  }

  pureTest("Genesis/unseeded: tip == Hash.empty is a Continue at idx 0, NEVER a Reanchor") {
    expect(ShardReanchor.classify(genesisWindow, W, Hash.empty, Some(0L)) == ShardReanchor.Continue(0))
  }

  pureTest("Reanchor: tip orphaned, genesis-rooted window reaches past tipOrdinal ⇒ adopt from tipOrdinal+1") {
    // tip X not continued; genesis-rooted len 4 (ord 0..3); tipOrdinal 1 ⇒ reIdx = min(2, 3) = 2
    expect(ShardReanchor.classify(genesisWindow, W, X, Some(1L)) == ShardReanchor.Reanchor(2))
  }

  pureTest("Reanchor (lateral SIBLING): tipOrdinal == last ordinal AND the tail is a true sibling (hash differs) ⇒ clamp to last index") {
    // tipOrdinal 3 == len-1, window tail hash W =!= tip X ⇒ a same-ordinal reorg winner exists at ord 3; reIdx = min(4, 3) = 3
    expect(ShardReanchor.classify(genesisWindow, W, X, Some(3L)) == ShardReanchor.Reanchor(3))
  }

  pureTest("AlreadyAdopted (the 3gl0 startup-freeze regression): window tail IS gl0's tip ⇒ NOT an orphan, nothing to adopt") {
    // The live wedge shape (m0, shard 0): genesis window [b0, b1] fully adopted at gl0 ord 10; tip = hash(b1); tipOrdinal 1 == lastIdx.
    // Pre-fix this classified Reanchor(min(2, 1)) = Reanchor(1): the oldest-first embed-selection re-picked the fully-adopted genesis
    // checkpoint every gl0 ordinal (1314 REANCHOR lines observed live), shadowing shardOrd=2 forever — the mirror froze at currency ord 1.
    val T = h("d") // stands for hash(window.last) — the caller-computed windowTipHash
    val fullyAdopted = NonEmptyList.of(bin(Hash.empty), bin(A)) // ordinals 0,1; tail's own hash = T
    expect(ShardReanchor.classify(fullyAdopted, T, T, Some(1L)) == ShardReanchor.AlreadyAdopted)
  }

  pureTest("AlreadyAdopted beats the lateral clamp for the longer window shape too (m1, shard 1: len 3, tipOrd 2)") {
    val T = h("d")
    val fullyAdopted = NonEmptyList.of(bin(Hash.empty), bin(A), bin(B)) // ordinals 0,1,2
    expect(ShardReanchor.classify(fullyAdopted, T, T, Some(2L)) == ShardReanchor.AlreadyAdopted)
  }

  pureTest("Continue wins over AlreadyAdopted (defensive precedence — a chain-linked window cannot contain a child of its own tail)") {
    // If a window somehow contained a continuation of the tip AND its tail hash equaled the tip, adopt the suffix (progress over no-op).
    val T = h("d")
    expect(ShardReanchor.classify(NonEmptyList.of(bin(T), bin(A)), T, T, Some(0L)) == ShardReanchor.Continue(0))
  }

  pureTest("Groundhog escape (the live heal path): successor checkpoint's window continues the tip the ancestor ends at") {
    // After the fully-adopted genesis checkpoint classifies AlreadyAdopted (skipped by the embed pick), the oldest-first walk reaches the
    // successor checkpoint, whose window head's parent IS gl0's tip ⇒ Continue(0) — the mirror advances.
    val T = h("d")
    val successorWindow = NonEmptyList.of(bin(T), bin(A)) // shardOrd=2 window: the ord-2 binary chains off the tip
    expect(ShardReanchor.classify(successorWindow, W, T, Some(1L)) == ShardReanchor.Continue(0))
  }

  pureTest("Defer: tip orphaned but window is NOT genesis-rooted (true chain hole — re-offer ancestor)") {
    val midRooted = NonEmptyList.of(bin(A), bin(B)) // head parent A != Hash.empty
    expect(ShardReanchor.classify(midRooted, W, X, Some(0L)) == ShardReanchor.Defer)
  }

  pureTest("Defer: genesis-rooted but window does NOT reach tipOrdinal (must not jump a gap)") {
    val shortWindow = NonEmptyList.of(bin(Hash.empty), bin(A)) // ordinals 0,1
    expect(ShardReanchor.classify(shortWindow, W, X, Some(5L)) == ShardReanchor.Defer)
  }

  pureTest("Defer: no tip ordinal known (unseeded mirror) ⇒ never reanchor") {
    expect(ShardReanchor.classify(genesisWindow, W, X, None) == ShardReanchor.Defer)
  }

  test("windowTipHashF === the SC-tip setter's hash space (Signed value-hash, proofs excluded)") {
    // The AlreadyAdopted check compares windowTipHashF(nel) against gl0's committed tip, which GSAM records as `nel.last.toHashed.hash`
    // (the VALUE hash). If these ever diverged, fully-adopted windows would silently classify Reanchor again — re-freezing startup.
    JsonSerializer.forAsync[IO].flatMap { implicit json =>
      implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
      val nel = NonEmptyList.of(bin(Hash.empty), bin(A))
      for {
        viaHelper <- ShardReanchor.windowTipHashF[IO](nel)
        viaTipSetter <- nel.last.toHashed[IO].map(_.hash)
      } yield expect(viaHelper === viaTipSetter)
    }
  }
}
