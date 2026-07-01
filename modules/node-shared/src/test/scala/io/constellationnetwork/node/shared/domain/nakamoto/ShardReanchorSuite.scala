package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.schema._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

/** Edge-case matrix for the orphaned-tip reanchor classifier (the 2026-06-29 sharded-mirror freeze fix). `classify` is a pure function of
  * (window, tip hash, tip metagraph-ordinal) — the SAME logic the GSAM adopt-guard and the GSCF embed- selection call, so these cases pin
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

  // A genesis-rooted contiguous window: index k ⇒ metagraph ordinal k (head parent === Hash.empty).
  private val genesisWindow = NonEmptyList.of(bin(Hash.empty), bin(A), bin(B), bin(C)) // ordinals 0,1,2,3

  pureTest("Continue: a window binary continues gl0's tip (idx 0)") {
    expect(ShardReanchor.classify(NonEmptyList.of(bin(A), bin(B)), A, Some(0L)) == ShardReanchor.Continue(0))
  }

  pureTest("Continue: tip continued mid-window") {
    expect(ShardReanchor.classify(NonEmptyList.of(bin(A), bin(B), bin(C)), B, Some(1L)) == ShardReanchor.Continue(1))
  }

  pureTest("Genesis/unseeded: tip == Hash.empty is a Continue at idx 0, NEVER a Reanchor") {
    expect(ShardReanchor.classify(genesisWindow, Hash.empty, Some(0L)) == ShardReanchor.Continue(0))
  }

  pureTest("Reanchor: tip orphaned, genesis-rooted window reaches past tipOrdinal ⇒ adopt from tipOrdinal+1") {
    // tip X not continued; genesis-rooted len 4 (ord 0..3); tipOrdinal 1 ⇒ reIdx = min(2, 3) = 2
    expect(ShardReanchor.classify(genesisWindow, X, Some(1L)) == ShardReanchor.Reanchor(2))
  }

  pureTest("Reanchor (lateral): tipOrdinal == window's last ordinal ⇒ clamp reIdx to the last index") {
    // tipOrdinal 3 == len-1 ⇒ reIdx = min(4, 3) = 3 (the reorg winner at ord 3; successor not yet landed)
    expect(ShardReanchor.classify(genesisWindow, X, Some(3L)) == ShardReanchor.Reanchor(3))
  }

  pureTest("Defer: tip orphaned but window is NOT genesis-rooted (true chain hole — re-offer ancestor)") {
    val midRooted = NonEmptyList.of(bin(A), bin(B)) // head parent A != Hash.empty
    expect(ShardReanchor.classify(midRooted, X, Some(0L)) == ShardReanchor.Defer)
  }

  pureTest("Defer: genesis-rooted but window does NOT reach tipOrdinal (must not jump a gap)") {
    val shortWindow = NonEmptyList.of(bin(Hash.empty), bin(A)) // ordinals 0,1
    expect(ShardReanchor.classify(shortWindow, X, Some(5L)) == ShardReanchor.Defer)
  }

  pureTest("Defer: no tip ordinal known (unseeded mirror) ⇒ never reanchor") {
    expect(ShardReanchor.classify(genesisWindow, X, None) == ShardReanchor.Defer)
  }
}
