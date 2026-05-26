package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hex.Hex

import weaver.MutableIOSuite

/** Wiring smoke test for [[ShardCheckpointWiring.acceptanceDeps]] — the priority-1 acceptance-side production wiring of
  * hierarchical-shard-checkpoints v1.
  *
  * The full `accept()`-path behavior (gate firing, binary supplementation, reject/pending handling) is already covered by
  * [[io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.GlobalSnapshotAcceptanceManagerShardingSuite]]. This suite
  * targets the NARROW contract the production wiring adds: "does the helper return `None` at the `numShards = 1` regression bar, and `Some`
  * with a correctly-shaped per-shard registry when `numShards > 1`?" — the literal activation gate the two GSAM call sites depend on.
  *
  * '''Required coverage''':
  *   1. `numShards = 1` ⇒ `None` (regression bar — call sites pass all-`None`, behavior byte-identical to pre-wiring).
  *   1. `numShards = 0` ⇒ `None` (degenerate guard — `<= 1` not just `== 1`).
  *   1. `numShards = 4` ⇒ `Some` with `shardingConfig.numShards == 4`, a non-null acceptance manager + shard assignment, and a registry
  *      keyed by exactly shards `{0,1,2,3}`.
  *   1. `buildRegistry` produces one entry per shard with the matching `ShardId` stamped on each per-shard store/tracker/triggers.
  */
object ShardCheckpointWiringSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  private val selfPeerId: PeerId = PeerId(Hex("ab" * 64))
  private val otherPeerId: PeerId = PeerId(Hex("cd" * 64))
  private val validators: Set[PeerId] = Set(selfPeerId, otherPeerId)

  /** Build a [[ShardingConfig]] with the supplied `numShards`. Other fields use representative defaults — only `numShards`,
    * `committeeKTarget`, and `finality.k1Shard` are consulted by the wiring helper.
    */
  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      committeeKTarget = 4,
      finality = ShardFinalityConfig(k1Shard = 8L),
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100),
      observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
      slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
    )

  private def runAcceptanceDeps(
    numShards: Int
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Option[ShardCheckpointWiring.AcceptanceDeps[IO]]] =
    ShardCheckpointWiring.acceptanceDeps[IO](
      cfg = mkShardingConfig(numShards),
      selfPeerId = selfPeerId,
      kesRegistry = KesRegistry.empty[IO],
      activeValidators = IO.pure(validators)
    )(implicitly, h, sp, implicitly)

  test("numShards=1 ⇒ None (regression bar — nothing constructed)") { res =>
    implicit val (h, sp) = res
    runAcceptanceDeps(numShards = 1).map(deps => expect(deps.isEmpty))
  }

  test("numShards=0 ⇒ None (degenerate guard, <= 1 not just == 1)") { res =>
    implicit val (h, sp) = res
    runAcceptanceDeps(numShards = 0).map(deps => expect(deps.isEmpty))
  }

  test("numShards=4 ⇒ Some with config + manager + assignment + 4-shard registry keyed {0,1,2,3}") { res =>
    implicit val (h, sp) = res
    runAcceptanceDeps(numShards = 4).map {
      case None => failure("expected Some(acceptanceDeps) at numShards=4, got None")
      case Some(deps) =>
        val expectedShardIds = (0 until 4).map(ShardId.unsafeApply).toSet
        expect.all(
          deps.shardingConfig.numShards == 4,
          deps.shardingConfig.committeeKTarget == 4,
          // acceptanceManager + shardAssignment are non-null references (constructed)
          deps.acceptanceManager != null,
          deps.shardAssignment != null,
          // registry is keyed by exactly shards 0..3
          deps.registry.size == 4,
          deps.registry.keySet == expectedShardIds
        )
    }
  }

  test("buildRegistry: one entry per shard with the matching ShardId stamped on store + tracker + triggers") { res =>
    implicit val (h, sp) = res
    ShardCheckpointWiring.buildRegistry[IO](mkShardingConfig(numShards = 3), selfPeerId).map { registry =>
      val perShardIdConsistent = registry.toList.forall {
        case (sid, entry) =>
          entry.chainStore.shardId == sid &&
          entry.tipTracker.shardId == sid &&
          entry.finalityTriggers.shardId == sid
      }
      expect.all(
        registry.size == 3,
        registry.keySet == (0 until 3).map(ShardId.unsafeApply).toSet,
        perShardIdConsistent
      )
    }
  }

  test("committeeFor (v1): returns the full active validator set regardless of shardId/epoch") { res =>
    implicit val (_h, _sp) = res
    val s0 = ShardCheckpointWiring.committeeFor[IO](ShardId.unsafeApply(0), io.constellationnetwork.schema.nakamoto.EtaPeriod(7L), IO.pure(validators))
    val s2 = ShardCheckpointWiring.committeeFor[IO](ShardId.unsafeApply(2), io.constellationnetwork.schema.nakamoto.EtaPeriod(99L), IO.pure(validators))
    (s0, s2).tupled.map { case (c0, c2) => expect.all(c0 == validators, c2 == validators) }
  }
}
