package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.SecureRandom

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher

import weaver.MutableIOSuite

/** Tests for the EXECUTION-SHARDING shard-committee sortition primitive on [[CommitteeSortition]] —
  * `shardDrawValue` / `isInShardCommittee` (the VK-seeded deterministic draw `ShardCheckpointWiring.committeeFor` enumerates over).
  *
  * Unlike the per-metagraph committee VRF (covered by [[CommitteeSortitionSuite]]), this draw is a deterministic PSEUDO-RANDOM
  * function of the operator's PUBLIC VRF VK (so a non-member can enumerate the whole committee — see the primitive's scaladoc). The
  * load-bearing property is byte-determinism cluster-wide: every gl0 node MUST compute the identical committee for the same
  * `(shardId, epoch, VK set, eta)`, or the shard-checkpoint adopt decision diverges and the cluster splits (#261).
  *
  * Coverage:
  *   1. Determinism across two INDEPENDENT `Hasher` instances — same `(shardId, epoch, vrfVk, eta)` ⇒ identical draw value + membership.
  *   2. Per-shard independence — different shardId (same VK/epoch/eta) generally yields a different draw value.
  *   3. Threshold edges — `kTarget·σ ≥ 1` ⇒ always in; σ=0 ⇒ always out.
  *   4. Enumerated committee ⊆ the candidate set, with mean size ≈ kTarget at uniform σ = 1/N (the sortition denominator is K_S ≈ kTarget,
  *      NOT N).
  */
object CommitteeShardSortitionSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], Hasher[IO])

  // Two INDEPENDENT Hasher instances (each over its OWN JsonSerializer) — the determinism tests run the draw under each and assert
  // byte-equality, proving the result is a pure function of the inputs and not of any per-instance state. Built in separate scopes so
  // only one `JsonSerializer` is ever in implicit scope at a time (two in one for-comprehension would be ambiguous).
  private def freshHasher: Resource[IO, Hasher[IO]] =
    JsonSerializer.forAsync[IO].asResource.map { implicit j => Hasher.forJson[IO] }

  override def sharedResource: Resource[IO, Res] =
    for {
      h1 <- freshHasher
      h2 <- freshHasher
    } yield (h1, h2)

  // Deterministic PRNG so the statistical size test is reproducible.
  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_41_52_44_56_4bL) // ASCII "SHARDVK"
    r
  }

  private def randomVk(): Array[Byte] = {
    val vk = new Array[Byte](32)
    random.nextBytes(vk)
    vk
  }

  private val eta: Array[Byte] = Array.tabulate[Byte](32)(i => (i * 7 + 1).toByte)

  test("shardDrawValue: deterministic across two independent Hasher instances") { res =>
    val (h1, h2) = res
    val vk = randomVk()
    (
      CommitteeSortition.shardDrawValue[IO](eta, ShardId.unsafeApply(3), EtaPeriod(11L), vk)(implicitly, h1),
      CommitteeSortition.shardDrawValue[IO](eta, ShardId.unsafeApply(3), EtaPeriod(11L), vk)(implicitly, h2)
    ).tupled.map { case (a, b) => expect(a == b) }
  }

  test("isInShardCommittee: deterministic across two independent Hasher instances") { res =>
    val (h1, h2) = res
    val vk = randomVk()
    val sigma = Ratio(1, 8)
    (
      CommitteeSortition.isInShardCommittee[IO](vk, eta, ShardId.unsafeApply(1), EtaPeriod(5L), sigma, kTarget = 4)(implicitly, h1),
      CommitteeSortition.isInShardCommittee[IO](vk, eta, ShardId.unsafeApply(1), EtaPeriod(5L), sigma, kTarget = 4)(implicitly, h2)
    ).tupled.map { case (a, b) => expect(a == b) }
  }

  test("shardDrawValue: different shardId (same VK/epoch/eta) generally yields a different draw value") { res =>
    val (h1, _) = res
    implicit val h: Hasher[IO] = h1
    // Sample many VKs; assert the per-shard draws differ for the overwhelming majority (a hash collision across two shards for the same
    // VK is cryptographically negligible). Use > 90% as a robust lower bound that still catches a "shardId not in the preimage" bug.
    val vks = List.fill(64)(randomVk())
    vks
      .traverse { vk =>
        (
          CommitteeSortition.shardDrawValue[IO](eta, ShardId.unsafeApply(0), EtaPeriod(9L), vk),
          CommitteeSortition.shardDrawValue[IO](eta, ShardId.unsafeApply(1), EtaPeriod(9L), vk)
        ).tupled.map { case (a, b) => a != b }
      }
      .map(diffs => expect(diffs.count(identity) >= 60))
  }

  test("isInShardCommittee: kTarget·σ >= 1 ⇒ always a member; σ = 0 ⇒ never a member") { res =>
    val (h1, _) = res
    implicit val h: Hasher[IO] = h1
    val vk = randomVk()
    (
      // σ = 1/2, kTarget = 4 ⇒ kTarget·σ = 2 ≥ 1 ⇒ saturates ⇒ always in.
      CommitteeSortition.isInShardCommittee[IO](vk, eta, ShardId.unsafeApply(0), EtaPeriod(1L), Ratio(1, 2), kTarget = 4),
      // σ = 0 ⇒ threshold 0 ⇒ draw value (in [0,1)) is never < 0 ⇒ always out.
      CommitteeSortition.isInShardCommittee[IO](vk, eta, ShardId.unsafeApply(0), EtaPeriod(1L), Ratio(0, 1), kTarget = 4)
    ).tupled.map { case (saturated, zero) => expect.all(saturated, !zero) }
  }

  test("enumerated committee ⊆ candidate set with mean size ≈ kTarget at σ = 1/N") { res =>
    val (h1, _) = res
    implicit val h: Hasher[IO] = h1
    // N candidates, uniform σ = 1/N, kTarget = K ⇒ per-operator inclusion prob = K/N, so E[|committee|] = K. Average over many shards to
    // smooth binomial noise, then assert the mean is within a generous band of K and every committee is a subset of the candidates.
    val n = 40
    val kTarget = 8
    val sigma = Ratio(1, n)
    val candidateVks: Vector[(Int, Array[Byte])] = Vector.tabulate(n)(i => i -> randomVk())
    val shards = (0 until 50).toList

    shards
      .traverse { s =>
        candidateVks.toList
          .traverse {
            case (i, vk) =>
              CommitteeSortition
                .isInShardCommittee[IO](vk, eta, ShardId.unsafeApply(s), EtaPeriod(2L), sigma, kTarget)
                .map(in => if (in) Some(i) else None)
          }
          .map(_.flatten.toSet)
      }
      .map { committees =>
        val allSubsets = committees.forall(_.subsetOf(candidateVks.map(_._1).toSet))
        val totalSize = committees.foldLeft(0)((acc, c) => acc + c.size)
        val meanSize = totalSize.toDouble / committees.size
        expect.all(
          allSubsets,
          // Generous band around K=8 (binomial mean over 40 trials, averaged across 50 shards): well inside ±50%.
          meanSize >= 4.0,
          meanSize <= 12.0
        )
      }
  }
}
