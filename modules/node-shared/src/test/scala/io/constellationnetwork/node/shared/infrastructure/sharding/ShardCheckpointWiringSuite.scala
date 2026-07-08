package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot}
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

  /** Build a [[ShardingConfig]] with the supplied `numShards`. Other fields use representative defaults — only `numShards` and
    * `finality.k1Shard` are consulted by the wiring helper. The committee draw/quorum (`kDraw`/`kQuorum`) are now separate params, not on
    * `ShardingConfig` (the test passes them directly to [[ShardCheckpointWiring.acceptanceDeps]]).
    */
  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      finality = ShardFinalityConfig(k1Shard = 8L),
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100, binaryBufferCap = 4096),
      observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
      slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
    )

  // Fixed 32-byte eta resolver for the committee draw (deterministic across calls — what `committeeFor` consumes).
  private val fixedEta: Array[Byte] = Array.fill[Byte](32)(7.toByte)
  private val etaForEpoch: io.constellationnetwork.schema.nakamoto.EtaPeriod => IO[Array[Byte]] = _ => IO.pure(fixedEta)

  private def runAcceptanceDeps(
    numShards: Int
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Option[ShardCheckpointWiring.AcceptanceDeps[IO]]] =
    ShardCheckpointWiring.acceptanceDeps[IO](
      cfg = mkShardingConfig(numShards),
      kDraw = 4,
      kQuorum = 3,
      selfPeerId = selfPeerId,
      kesRegistry = KesRegistry.empty[IO],
      vrfRegistry = VrfRegistry.empty[IO],
      activeValidators = IO.pure(validators),
      etaForEpoch = etaForEpoch
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
    ShardCheckpointWiring.buildRegistry[IO](mkShardingConfig(numShards = 3), kQuorum = 3, selfPeerId).map { registry =>
      val perShardIdConsistent = registry.toList.forall {
        case (sid, entry) =>
          entry.chainStore.shardId == sid &&
          entry.tipTracker.shardId == sid &&
          entry.finalityTriggers.shardId == sid &&
          entry.binaryBuffer.shardId == sid
      }
      expect.all(
        registry.size == 3,
        registry.keySet == (0 until 3).map(ShardId.unsafeApply).toSet,
        perShardIdConsistent
      )
    }
  }

  // VRF-VK registry: each validator → a distinct 32-byte VK (the per-operator seed for the draw). Bytes are arbitrary; the
  // draw is deterministic in (eta, shardId, epoch, vrfVk), so distinct VKs give the per-operator independence the sortition needs.
  private val vkSelf: Array[Byte] = Array.fill[Byte](32)(0x11.toByte)
  private val vkOther: Array[Byte] = Array.fill[Byte](32)(0x22.toByte)
  private val vrfReg: VrfRegistry[IO] = VrfRegistry.make[IO](Map(selfPeerId -> vkSelf, otherPeerId -> vkOther))

  private def committee(shardId: Int, epoch: Long, kDraw: Int, reg: VrfRegistry[IO] = vrfReg)(
    implicit h: Hasher[IO]
  ): IO[Set[PeerId]] =
    ShardCheckpointWiring.committeeFor[IO](
      ShardId.unsafeApply(shardId),
      io.constellationnetwork.schema.nakamoto.EtaPeriod(epoch),
      IO.pure(validators),
      reg,
      etaForEpoch,
      kDraw,
      kQuorum = 1,
      slashCooldown = io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader.noExclusion[IO]
    )

  test("committeeFor: result is always a SUBSET of the active validator set") { res =>
    implicit val (h, _sp) = res
    (committee(0, 7L, 2), committee(2, 99L, 1)).tupled.map {
      case (c0, c2) => expect.all(c0.subsetOf(validators), c2.subsetOf(validators))
    }
  }

  test("committeeFor: deterministic — same (shardId, epoch, VKs, eta) ⇒ identical committee") { res =>
    implicit val (h, _sp) = res
    (committee(1, 5L, 1), committee(1, 5L, 1)).tupled.map { case (a, b) => expect(a == b) }
  }

  test("committeeFor: an operator with NO registered VRF VK is never sortitioned in") { res =>
    implicit val (h, _sp) = res
    // Registry missing `otherPeerId` ⇒ it can never be a committee member regardless of kTarget.
    val regSelfOnly = VrfRegistry.make[IO](Map(selfPeerId -> vkSelf))
    committee(0, 7L, kDraw = 4, reg = regSelfOnly).map(c => expect(!c.contains(otherPeerId)))
  }

  test("committeeFor: kDraw >= N saturates threshold to 1 ⇒ all registered operators are members") { res =>
    implicit val (h, _sp) = res
    // threshold = min(kDraw/N, 1); kDraw=8, N=2 ⇒ kDraw·σ = 8·(1/2) = 4 ≥ 1 ⇒ everyone in.
    committee(0, 7L, kDraw = 8).map(c => expect(c == validators))
  }

  test("committeeFor: empty active set ⇒ empty committee") { res =>
    implicit val (h, _sp) = res
    ShardCheckpointWiring
      .committeeFor[IO](
        ShardId.unsafeApply(0),
        io.constellationnetwork.schema.nakamoto.EtaPeriod(7L),
        IO.pure(Set.empty[PeerId]),
        vrfReg,
        etaForEpoch,
        kDraw = 4,
        kQuorum = 1,
        slashCooldown = io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader.noExclusion[IO]
      )
      .map(c => expect(c.isEmpty))
  }

  // ====================================================================================================================
  // #261 (eta axis) — follower vs leader committee-eta SYMMETRY.
  //
  // The split vector: `committeeFor(shardId, epoch)` is deterministic GIVEN eta, but eta is resolved through TWO
  // `EtaStateManager` instances with DIFFERENT chain-walk fallbacks. The leader/validator path (GlobalSnapshotConsensus)
  // walks the REAL `chainStore.vrfOutputsForPeriod`; the follower / `createContext` path (SharedServices) previously used
  // a no-op walk. During the ACTIVE window of period P (before the boundary MPT write at `ord % R == R-1`), the MPT lookup
  // MISSES on both paths, so:
  //   - leader  getEta(P≥2) = computeEta(genesis, P, realVrfOutputs)   (non-genesis)
  //   - buggy follower getEta(P≥2) = bootstrapEta(genesis, P)          (empty walk → per-period bootstrap)
  // ⇒ different eta ⇒ different `shardDrawValue` ⇒ different committee SET ⇒ a follower (gl0 Download / RollbackLoader /
  // fork-recovery rebuild) ADOPTS / REJECTS checkpoints differently than the leader ⇒ StateProofMismatch split.
  //
  // The fix threads the SAME real chain walk into the follower's `EtaStateManager`. These tests model the production seam
  // directly at the `EtaStateManager` → `committeeFor` level: they FAIL on the old (no-op-walk) follower and PASS on the
  // fixed (real-walk) follower. Epochs 0/1 (genesis) stay identical on every path.

  // 20 validators with distinct VRF VKs. `committeeFor` sets σ = 1/N = 1/20, so threshold = min(kTarget/N, 1) = 6/20 = 0.30.
  // The draw (`CommitteeSortition.shardDrawValue`) interprets the SHA-256 hash's *hex-string* bytes as the ratio numerator,
  // so the high byte is an ASCII hex char (∈ [0x30,0x66]) ⇒ every draw lands in ≈[0.19, 0.40). A threshold of 0.30 therefore
  // sits inside that band: membership genuinely depends on the lower hash bytes — i.e. on `eta`. Two distinct etas (leader's
  // chain-walk eta vs the buggy follower's genesis eta) thus draw DIFFERENT committee sets — the observable split. (A 0.5
  // threshold would put every operator in for ANY eta — the high-byte band tops out below 0.40 — making the test eta-blind.)
  private val symmetryValidators: Set[PeerId] =
    (0 until 20).map(i => PeerId(Hex(f"$i%02x" * 64))).toSet
  private val symmetryVrfReg: VrfRegistry[IO] =
    VrfRegistry.make[IO](symmetryValidators.toList.zipWithIndex.map {
      case (p, i) => p -> Array.fill[Byte](32)((0xa0 + i).toByte)
    }.toMap)
  private val symmetryKTarget = 6
  private val symmetryNumShards = 4

  // The cluster-wide genesis eta (Blake2b of the fixed domain string). Both GlobalSnapshotConsensus.nakamotoGenesisEta and
  // SharedServices.DefaultNakamotoGenesisEta produce these exact bytes; we reuse the latter so the test pins the SAME value
  // both production resolvers fall through to for periods ≤ 1.
  private val symmetryGenesisEta: Array[Byte] =
    io.constellationnetwork.node.shared.modules.SharedServices.DefaultNakamotoGenesisEta

  // A non-genesis set of VRF outputs the REAL chain walk returns for the source period (period P-1's first 2/3). Distinct
  // enough that `computeEta(genesisEta, P, these)` ≠ genesisEta. Models `chainStore.vrfOutputsForPeriod(P-1, R)`.
  private val realVrfOutputs: List[(Long, Array[Byte])] =
    List(
      (0L, Array.fill[Byte](32)(0x5a.toByte)),
      (1L, Array.fill[Byte](32)(0x3c.toByte)),
      (2L, Array.fill[Byte](32)(0x71.toByte))
    )

  // Always-empty MPT reader — models the ACTIVE-window pre-boundary lookup miss (the boundary write for the current period
  // hasn't fired yet), which is exactly when the chain-walk-fallback asymmetry decides the committee.
  private val emptyMptReader: HistoricalStakeReader[IO] = new HistoricalStakeReader[IO] {
    def lookup(period: EtaPeriod)(implicit hasher: Hasher[IO]): IO[Option[HistoricalStakeSnapshot]] =
      IO.pure(None)
  }

  // The leader's chain-walk fallback: returns the REAL VRF outputs for ANY source period ≥ 1 (so periods ≥ 2 compute a
  // non-genesis eta), and empty for source period 0. Periods 0 AND 1 are genesis-derivable bootstrapEta (Cardano/Praos
  // bootstrap — they short-circuit before the walk and fold NO VRF outputs), so the chain walk only matters for period ≥ 2.
  private val realChainWalk: Long => IO[List[(Long, Array[Byte])]] = (sourcePeriod: Long) =>
    if (sourcePeriod >= 1L) IO.pure(realVrfOutputs) else IO.pure(List.empty[(Long, Array[Byte])])

  // The BUGGY follower's chain-walk fallback: the no-op walk (`SharedServices.noopEtaChainWalk` analog) — empty for every
  // source period ⇒ `getEta(P≥2)` falls through to genesisEta.
  private val noopChainWalk: Long => IO[List[(Long, Array[Byte])]] = (_: Long) => IO.pure(List.empty[(Long, Array[Byte])])

  // Build a `committeeFor`-shaped `EtaPeriod => IO[Array[Byte]]` resolver from an `EtaStateManager` with the given chain
  // walk — exactly the production shape (`etaForEpoch = epoch => mgr.getEta(epoch.value)`).
  private def etaResolver(chainWalk: Long => IO[List[(Long, Array[Byte])]])(
    implicit h: Hasher[IO]
  ): IO[EtaPeriod => IO[Array[Byte]]] =
    EtaStateManager
      .make[IO](symmetryGenesisEta, emptyMptReader, chainWalk)
      .map(mgr => (epoch: EtaPeriod) => mgr.getEta(epoch.value))

  private def committeeForResolver(resolver: EtaPeriod => IO[Array[Byte]], shardId: Int, epoch: Long)(
    implicit h: Hasher[IO]
  ): IO[Set[PeerId]] =
    ShardCheckpointWiring.committeeFor[IO](
      ShardId.unsafeApply(shardId),
      EtaPeriod(epoch),
      IO.pure(symmetryValidators),
      symmetryVrfReg,
      resolver,
      symmetryKTarget,
      kQuorum = 1,
      slashCooldown = io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader.noExclusion[IO]
    )

  test("#261 eta axis — ROOT CAUSE: leader getEta(epoch≥2) ≠ buggy-follower getEta (real walk vs no-op→genesis)") { res =>
    implicit val (h, _sp) = res
    for {
      leader <- etaResolver(realChainWalk)
      buggy <- etaResolver(noopChainWalk)
      leaderEta2 <- leader(EtaPeriod(2L))
      buggyEta2 <- buggy(EtaPeriod(2L))
      // The leader's period-2 eta IS the chain-walk recompute; the buggy follower's empty walk falls back
      // to the per-period bootstrapEta(genesis, 2) (the N>=2 degenerate fallback — NOT raw genesis).
      expectedLeaderEta2 = EtaCalculation.computeEta(symmetryGenesisEta, 2L, realVrfOutputs.map(_._2))
      expectedBuggyEta2 = EtaCalculation.bootstrapEta(symmetryGenesisEta, 2L)
    } yield
      expect.all(
        leaderEta2.sameElements(expectedLeaderEta2),
        buggyEta2.sameElements(expectedBuggyEta2),
        !leaderEta2.sameElements(buggyEta2) // the asymmetry that drives the committee split
      )
  }

  test("#261 eta axis — FAIL-BEFORE: buggy follower draws a DIFFERENT committee than the leader for some shard at epoch≥2") { res =>
    implicit val (h, _sp) = res
    for {
      leader <- etaResolver(realChainWalk)
      buggy <- etaResolver(noopChainWalk)
      epoch = 2L
      leaderSets <- (0 until symmetryNumShards).toList.traverse(committeeForResolver(leader, _, epoch))
      buggySets <- (0 until symmetryNumShards).toList.traverse(committeeForResolver(buggy, _, epoch))
    } yield
      // At least one shard's committee differs between the leader and the buggy follower — this is the cluster split the
      // fix eliminates. (Pre-fix this holds; it is the failure the fix prevents from ever mattering, since post-fix the
      // follower uses the SAME walk and the next test shows the sets become identical.)
      expect(
        leaderSets.zip(buggySets).exists { case (l, b) => l != b },
        s"expected the buggy follower to draw a different committee than the leader on some shard; " +
          s"leaderSets=$leaderSets buggySets=$buggySets"
      )
  }

  test("#261 eta axis — PASS-AFTER: fixed follower (SAME real walk) getEta byte-equals leader for every epoch incl. ≥2") { res =>
    implicit val (h, _sp) = res
    for {
      leader <- etaResolver(realChainWalk)
      fixed <- etaResolver(realChainWalk) // the fix: follower threads the IDENTICAL chain walk the leader uses
      // Periods 0/1 (genesis-safe) and a representative ≥2 set, including the cross-period-ride boundary epoch.
      epochs = List(0L, 1L, 2L, 3L, 5L)
      results <- epochs.traverse { p =>
        (leader(EtaPeriod(p)), fixed(EtaPeriod(p))).tupled.map { case (le, fe) => (p, le, fe) }
      }
    } yield
      results.foldLeft(success) {
        case (acc, (p, le, fe)) =>
          acc.and(expect(le.sameElements(fe), s"follower getEta($p) must byte-equal leader getEta($p)"))
      }
  }

  test("#261 eta axis — PASS-AFTER: fixed follower draws the IDENTICAL committee as the leader on every shard at epoch≥2") { res =>
    implicit val (h, _sp) = res
    for {
      leader <- etaResolver(realChainWalk)
      fixed <- etaResolver(realChainWalk)
      epoch = 2L
      leaderSets <- (0 until symmetryNumShards).toList.traverse(committeeForResolver(leader, _, epoch))
      fixedSets <- (0 until symmetryNumShards).toList.traverse(committeeForResolver(fixed, _, epoch))
    } yield expect(leaderSets == fixedSets, s"committee sets must match per shard: leader=$leaderSets fixed=$fixedSets")
  }

  test("#261 eta axis — BOOTSTRAP-SAFE: leader, buggy follower, and fixed follower all agree at epochs 0 and 1") { res =>
    implicit val (h, _sp) = res
    // Cardano/Praos bootstrap: periods 0 AND 1 are genesis-derivable bootstrapEta(genesis, p) (fold NO VRF
    // outputs, bypass the chain walk), so all three resolvers — regardless of their walk — agree byte-for-byte
    // and draw identical committees. This is what makes the first eta rotation (0 → 1) fork-proof.
    for {
      leader <- etaResolver(realChainWalk)
      buggy <- etaResolver(noopChainWalk)
      fixed <- etaResolver(realChainWalk)
      checks <- List(0L, 1L).traverse { p =>
        for {
          le <- leader(EtaPeriod(p))
          be <- buggy(EtaPeriod(p))
          fe <- fixed(EtaPeriod(p))
          lc <- committeeForResolver(leader, 0, p)
          bc <- committeeForResolver(buggy, 0, p)
          fc <- committeeForResolver(fixed, 0, p)
        } yield (p, le, be, fe, lc, bc, fc)
      }
    } yield
      checks.foldLeft(success) {
        case (acc, (p, le, be, fe, lc, bc, fc)) =>
          val expectedBootstrap = EtaCalculation.bootstrapEta(symmetryGenesisEta, p)
          acc
            .and(expect(le.sameElements(expectedBootstrap), s"leader eta at genesis period $p must be bootstrapEta(genesis, $p)"))
            .and(expect(be.sameElements(expectedBootstrap), s"buggy follower eta at genesis period $p must be bootstrapEta(genesis, $p)"))
            .and(expect(fe.sameElements(expectedBootstrap), s"fixed follower eta at genesis period $p must be bootstrapEta(genesis, $p)"))
            .and(expect(lc == bc && bc == fc, s"all committees must agree at genesis period $p: $lc / $bc / $fc"))
      }
  }
}
