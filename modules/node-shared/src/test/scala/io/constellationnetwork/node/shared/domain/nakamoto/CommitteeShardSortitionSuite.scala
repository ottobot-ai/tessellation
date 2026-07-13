package io.constellationnetwork.node.shared.domain.nakamoto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash

import weaver.MutableIOSuite

/** Tests for the EXECUTION-SHARDING shard-committee sortition primitive on [[CommitteeSortition]] — `shardDrawValue` / `isInShardCommittee`
  * (the VK-seeded deterministic draw `ShardCheckpointWiring.committeeFor` enumerates over).
  *
  * Unlike the per-metagraph committee VRF (covered by [[CommitteeSortitionSuite]]), this draw is a deterministic PSEUDO-RANDOM function of
  * the operator's PUBLIC VRF VK (so a non-member can enumerate the whole committee — see the primitive's scaladoc). The load-bearing
  * property is byte-determinism cluster-wide: every gl0 node MUST compute the identical committee for the same `(shardId, epoch, VK set,
  * eta)`, or the shard-checkpoint adopt decision diverges and the cluster splits (#261).
  *
  * Coverage:
  *   1. Determinism across two INDEPENDENT `Hasher` instances — same `(shardId, epoch, vrfVk, eta)` ⇒ identical draw value + membership. 2.
  *      Per-shard independence — different shardId (same VK/epoch/eta) generally yields a different draw value. 3. Threshold edges —
  *      `kTarget·σ ≥ 1` ⇒ always in; σ=0 ⇒ always out. 4. Enumerated committee ⊆ the candidate set, with mean size ≈ kTarget at uniform σ =
  *      1/N (the sortition denominator is K_S ≈ kTarget, NOT N).
  */
object CommitteeShardSortitionSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], Hasher[IO], CanonicalOperatorConsensusPopulation)

  // Two INDEPENDENT Hasher instances (each over its OWN JsonSerializer) — the determinism tests run the draw under each and assert
  // byte-equality, proving the result is a pure function of the inputs and not of any per-instance state. Built in separate scopes so
  // only one `JsonSerializer` is ever in implicit scope at a time (two in one for-comprehension would be ambiguous).
  private def freshHasher: Resource[IO, Hasher[IO]] =
    JsonSerializer.forAsync[IO].asResource.map(implicit j => Hasher.forJson[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      h1 <- freshHasher
      h2 <- freshHasher
      operators <- CanonicalOperatorConsensusFixture.makePopulation(8)
    } yield (h1, h2, operators)

  private def eta(tag: String): Array[Byte] =
    MessageDigest.getInstance("SHA-256").digest(tag.getBytes(StandardCharsets.UTF_8))

  private val fixedEta: Array[Byte] = eta("committee-shard-fixed-eta")

  test("hashDigestAsRatio: decodes the hex rendering to the actual 32 SHA-256 bytes") { _ =>
    val denominator = BigInt(2).pow(256)
    val zero = CommitteeSortition.hashDigestAsRatio(Hash("00" * 32))
    val midpoint = CommitteeSortition.hashDigestAsRatio(Hash("80" + "00" * 31))
    val maximum = CommitteeSortition.hashDigestAsRatio(Hash("ff" * 32))

    IO.pure(
      expect.all(
        zero == Ratio(0, 1),
        midpoint == Ratio(1, 2),
        maximum == Ratio(denominator - 1, denominator)
      )
    )
  }

  test("shardDrawValue: deterministic across two independent Hasher instances") { res =>
    val (h1, h2, operators) = res
    val vk = operators.operators.head.resolvedPair.vrfPublicKey.toBytes
    (
      CommitteeSortition.shardDrawValue[IO](fixedEta, ShardId.unsafeApply(3), EtaPeriod(11L), vk)(implicitly, h1),
      CommitteeSortition.shardDrawValue[IO](fixedEta, ShardId.unsafeApply(3), EtaPeriod(11L), vk)(implicitly, h2)
    ).tupled.map { case (a, b) => expect(a == b) }
  }

  test("isInShardCommittee: deterministic across two independent Hasher instances") { res =>
    val (h1, h2, operators) = res
    val vk = operators.operators.head.resolvedPair.vrfPublicKey.toBytes
    val sigma = Ratio(1, 8)
    (
      CommitteeSortition.isInShardCommittee[IO](vk, fixedEta, ShardId.unsafeApply(1), EtaPeriod(5L), sigma, kDraw = 4)(implicitly, h1),
      CommitteeSortition.isInShardCommittee[IO](vk, fixedEta, ShardId.unsafeApply(1), EtaPeriod(5L), sigma, kDraw = 4)(implicitly, h2)
    ).tupled.map { case (a, b) => expect(a == b) }
  }

  test("shardDrawValue: different shardId (same VK/epoch/eta) generally yields a different draw value") { res =>
    val (h1, _, operators) = res
    implicit val h: Hasher[IO] = h1
    // Repeat independent eta draws over the registered population; a hash collision across two
    // shards for the same registered VK and eta is cryptographically negligible.
    (0 until 64).toList.traverse { draw =>
      val vk = operators.operators(draw % operators.operators.size).resolvedPair.vrfPublicKey.toBytes
      val drawEta = eta(s"per-shard-independence-$draw")
      (
        CommitteeSortition.shardDrawValue[IO](drawEta, ShardId.unsafeApply(0), EtaPeriod(9L), vk),
        CommitteeSortition.shardDrawValue[IO](drawEta, ShardId.unsafeApply(1), EtaPeriod(9L), vk)
      ).tupled.map { case (a, b) => a != b }
    }
      .map(diffs => expect(diffs.count(identity) >= 60))
  }

  test("isInShardCommittee: kTarget·σ >= 1 ⇒ always a member; σ = 0 ⇒ never a member") { res =>
    val (h1, _, operators) = res
    implicit val h: Hasher[IO] = h1
    val vk = operators.operators.head.resolvedPair.vrfPublicKey.toBytes
    (
      // σ = 1/2, kDraw = 4 ⇒ kDraw·σ = 2 ≥ 1 ⇒ saturates ⇒ always in.
      CommitteeSortition.isInShardCommittee[IO](vk, fixedEta, ShardId.unsafeApply(0), EtaPeriod(1L), Ratio(1, 2), kDraw = 4),
      // σ = 0 ⇒ threshold 0 ⇒ draw value (in [0,1)) is never < 0 ⇒ always out.
      CommitteeSortition.isInShardCommittee[IO](vk, fixedEta, ShardId.unsafeApply(0), EtaPeriod(1L), Ratio(0, 1), kDraw = 4)
    ).tupled.map { case (saturated, zero) => expect.all(saturated, !zero) }
  }

  test("enumerated committee ⊆ candidate set with mean size ≈ kDraw at σ = 1/N") { res =>
    val (h1, _, operators) = res
    implicit val h: Hasher[IO] = h1
    // The candidate set is the loader-validated period-zero population. Vary eta, shard, and epoch
    // over independent contexts; no random public key can enter the enumerated population.
    val n = operators.operators.size
    val kDraw = 2
    val sigma = Ratio(1, n)
    val candidateVks = operators.operators.zipWithIndex.map { case (operator, i) => i -> operator.resolvedPair.vrfPublicKey.toBytes }
    val draws = (0 until 125).toList

    draws.traverse { draw =>
      candidateVks.traverse {
        case (i, vk) =>
          CommitteeSortition
            .isInShardCommittee[IO](
              vk,
              eta(s"committee-size-$draw"),
              ShardId.unsafeApply(draw % 16),
              EtaPeriod(2L + draw.toLong),
              sigma,
              kDraw
            )
            .map(in => if (in) Some(i) else None)
      }
        .map(_.flatten.toSet)
    }.map { committees =>
      val allSubsets = committees.forall(_.subsetOf(candidateVks.map(_._1).toSet))
      val totalSize = committees.foldLeft(0)((acc, c) => acc + c.size)
      val meanSize = totalSize.toDouble / committees.size
      expect.all(
        allSubsets,
        // Generous band around kDraw=2 across 125 independent registered-population draws.
        meanSize >= 1.0,
        meanSize <= 3.0
      )
    }
  }
}
