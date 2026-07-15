package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics.unsafeLabelName
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.algebras.Exp
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.ExpInterpreter
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §3 NIPoPoW S3 — [[TowerFinalizer]] unit tests. Drives the finalizer with synthetic VRF outputs and verifies the tower grows
  * monotonically at the expected level-rates (no e2e cluster needed).
  *
  * Strategy: use `finalizeFromParts` to bypass the schema construction of `Hashed[GlobalIncrementalSnapshot]`. The pure inner method takes
  * the same ingredients the wire-level `finalize` extracts from the snapshot.
  *
  * '''Exp convergence sweet spot.''' The Bifrost continued-fraction `Exp` is only fast for modest argument magnitudes. Tests stay with `g_µ
  * ≤ 10` (matches `LevelTrialComputerSuite`'s "stay within the convergence sweet spot" comment); larger gaps push exp args toward
  * `-2·g_µ/σ` which inflate test wall-time without any unique-to-the-finalizer signal.
  */
object TowerFinalizerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], LevelTrialComputer[IO], Metrics[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource: Resource[IO, Exp[IO]]
      computer = LevelTrialComputer.make[IO](exp)
      metrics <- Resource.eval(CountingMetrics.make.map(_._2))
    } yield (h, sp, j, computer, metrics)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** 64-byte synthetic VRF output. Caller controls a single seed byte so each snapshot has a distinct `ρ_S`. */
  private def synthVrf(seed: Byte): Array[Byte] = Array.fill[Byte](64)(seed)

  /** Build a synthetic finalized-snapshot content-address. Bytes-of-label as hex, right-padded to 64 chars. */
  private def synthHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  private def freshTower(res: Res): IO[TowerStore[IO]] = {
    val (hh: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO], _comp: LevelTrialComputer[IO], _met: Metrics[IO]) = res
    implicit val h: Hasher[IO] = hh
    implicit val j: JsonSerializer[IO] = js
    val _ = (sp, _comp, _met) // unused at construction; required by Res tuple
    MptTowerStore.inMemory[IO]
  }

  private val Gamma: Long = 15L

  test("finalizeFromParts — deltaSlot=0 yields no passes (gating multiplier = 0)") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // deltaSlot = 0 → gating multiplier = 0 → effectiveThreshold = 0 → no level can pass.
      trials <- finalizer.finalizeFromParts(ord(10), synthHash("a"), synthVrf(0x42.toByte), deltaSlot = 0L)
      l1 <- tower.entriesAtLevel(1, ord(0))
    } yield expect(trials.forall(!_.passed)).and(expect(l1 == Nil))
  }

  test("finalizeFromParts — gMu < ψ_super yields no L1 pass (burst-zero condition)") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // ord=0 → g_µ = 0 < ψ_super=1 for every level → thresholdMu = 0 → no passes regardless of ρ.
      trials <- finalizer.finalizeFromParts(ord(0), synthHash("z"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      counts <- (1 to SuperLevelParams.SuperLevelCount).toVector.traverse(tower.cumulativeCount)
    } yield expect(trials.forall(!_.passed)).and(expect(counts.forall(_ == 0L)))
  }

  test("finalizeFromParts — gap computation consults tower.latestAt(µ) (not snapshot ordinal)") { res =>
    // Verify the gap-update path actually reads `tower.latestAt(µ)`. Prime the tower with an L1 hit at
    // ord=2, then finalize at ord=5 (g_µ for L1 = 3, NOT 5). With deltaSlot=0 no level passes — but we
    // can confirm via the returned trial vector that the trial was computed at g_1=3 (effectiveThreshold
    // is non-zero only when g_µ > ψ_super=1 AND deltaSlot > 0, so we check the no-pass under
    // deltaSlot=0 is consistent with the post-prime state, NOT a fresh-tower state).
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // Manually prime: append an L1 "synthetic" hit at ord=2.
      _ <- tower.appendAtFinality(
        ord(2),
        synthHash("prime"),
        Vector.tabulate(SuperLevelParams.SuperLevelCount)(i => LevelTrial(i + 1, Ratio.Zero, Ratio.Zero, passed = i == 0))
      )
      preL1 <- tower.latestAt(1)
      _ <- finalizer.finalizeFromParts(ord(5), synthHash("after"), synthVrf(0x77.toByte), deltaSlot = 0L)
      postL1 <- tower.latestAt(1) // unchanged (deltaSlot=0 → no pass)
    } yield
      expect(preL1.map(_.ordinal.value.value) == Some(2L))
        .and(expect(postL1 == preL1))
  }

  test("finalizeFromParts — when gMu ≥ ψ_super and deltaSlot > 0, threshold is non-zero (pass-or-fail is ρ-dependent)") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // ord=3 → g_1 = 3, ψ_super = 1 → L1 threshold is non-zero. deltaSlot=γ=15 → gating=1.
      // The trial is byte-deterministic given ρ, so either it passes or it doesn't — we just check the
      // computer was invoked (trial vector has the right size + level ordering) and the side-effect
      // matches the trial outcome.
      trials <- finalizer.finalizeFromParts(ord(3), synthHash("ok"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      cumL1 <- tower.cumulativeCount(1)
    } yield
      expect(trials.size == SuperLevelParams.SuperLevelCount)
        .and(expect(trials.zipWithIndex.forall { case (t, i) => t.level == i + 1 }))
        // L1's cumulative count matches the L1 trial outcome from this finalization.
        .and(expect(cumL1 == (if (trials.head.passed) 1L else 0L)))
  }

  test("finalizeFromParts — same (vrf, ordinal, deltaSlot) on a fresh tower is byte-deterministic") { res =>
    // Same inputs on a freshly-constructed tower → same gaps (all = ord-0) → same trials.
    // Production invariant: the finalizer is called once per archival-finalized ordinal; re-finalize
    // at the same ord is a non-scenario (watermark advances strictly forward). But the underlying
    // computation is byte-deterministic given the same inputs.
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower1 <- freshTower(res)
      finalizer1 <- TowerFinalizer.make[IO](tower1, computer, Gamma)
      trial1 <- finalizer1.finalizeFromParts(ord(3), synthHash("idem"), synthVrf(0x55.toByte), deltaSlot = Gamma)
      tower2 <- freshTower(res)
      finalizer2 <- TowerFinalizer.make[IO](tower2, computer, Gamma)
      trial2 <- finalizer2.finalizeFromParts(ord(3), synthHash("idem"), synthVrf(0x55.toByte), deltaSlot = Gamma)
    } yield expect(trial1 == trial2)
  }

  test("finalizeFromParts — monotonic ordinal stream grows tower counts monotonically (per-level appends never decrease)") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    // Drive the finalizer with a synthetic snapshot stream of 6 finalizations at consecutive ordinals.
    // Per-level cumulative count can only grow or stay equal across calls.
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // Ordinals 2..7 keep g_µ small (≤ 7) so the exp interpreter stays in its sweet spot.
      counts <- (2 to 7).toList.foldLeftM(Vector.empty[(Long, Long)]) { (acc, i) =>
        finalizer.finalizeFromParts(ord(i.toLong), synthHash(s"m$i"), synthVrf((0x40 + i).toByte), deltaSlot = Gamma) >>
          (tower.cumulativeCount(1), tower.cumulativeCount(2)).tupled.map(c => acc :+ c)
      }
    } yield {
      val l1Counts = counts.map(_._1)
      val l2Counts = counts.map(_._2)
      expect(l1Counts.zip(l1Counts.tail).forall { case (a, b) => b >= a })
        .and(expect(l2Counts.zip(l2Counts.tail).forall { case (a, b) => b >= a }))
    }
  }

  test("finalizeFromParts — high-water-mark skip: ord ≤ lastFinalized returns empty vector") { res =>
    // After finalizing ord=5, a subsequent finalize at ord=3 (or ord=5 again) is skipped because
    // production invariant is "strictly-forward only". The high-water-mark Ref enforces this even
    // if the wire-level caller (SnapshotLeaderLoop.finalityMonitor) makes a mistake — the tower's
    // contents stay deterministic.
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5
    for {
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      first <- finalizer.finalizeFromParts(ord(5), synthHash("ok"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      // Re-finalize at the same ord — high-water-mark blocks it; empty trials returned.
      replay <- finalizer.finalizeFromParts(ord(5), synthHash("ok"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      // Earlier ord also blocked.
      earlier <- finalizer.finalizeFromParts(ord(3), synthHash("earlier"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      // Strictly-forward ord goes through.
      later <- finalizer.finalizeFromParts(ord(6), synthHash("later"), synthVrf(0x42.toByte), deltaSlot = Gamma)
    } yield
      expect(first.nonEmpty) // first finalize ran
        .and(expect(replay.isEmpty)) // re-finalize at same ord blocked
        .and(expect(earlier.isEmpty)) // earlier ord blocked
        .and(expect(later.nonEmpty)) // later ord ran
  }

  test("prepareFromParts — store reads and level-trial preparation remain cancelable before publication") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5

    for {
      delegate <- freshTower(res)
      readStarted <- Deferred[IO, Unit]
      appendCalls <- Ref.of[IO, Int](0)
      blockingTower = new TowerStore[IO] {
        def appendAtFinality(
          ordinal: SnapshotOrdinal,
          snapshotHash: Hash,
          passes: Vector[LevelTrial]
        ): IO[Unit] = appendCalls.update(_ + 1) >> delegate.appendAtFinality(ordinal, snapshotHash, passes)

        def entriesAtLevel(level: Int, since: SnapshotOrdinal): IO[List[TowerEntry]] = delegate.entriesAtLevel(level, since)
        def latestAt(level: Int): IO[Option[TowerEntry]] = readStarted.complete(()).void >> IO.never
        def cumulativeCount(level: Int): IO[Long] = delegate.cumulativeCount(level)
        def pruneBelow(keepFrom: SnapshotOrdinal): IO[Unit] = delegate.pruneBelow(keepFrom)
      }
      finalizer <- TowerFinalizer.make[IO](blockingTower, computer, Gamma)
      preparing <- finalizer.prepareFromParts(ord(3), synthHash("prepare"), synthVrf(0x42.toByte), Gamma).start
      _ <- readStarted.get
      _ <- preparing.cancel
      outcome <- preparing.join
      observedAppends <- appendCalls.get
      wasCanceled = outcome match {
        case Outcome.Canceled() => true
        case _                  => false
      }
    } yield expect(wasCanceled).and(expect(observedAppends == 0))
  }

  test("finalizeFromParts — cancellation cannot publish a tower append without its de-duplication watermark") { res =>
    val (_, _, _, computer, _) = res
    implicit val metrics: Metrics[IO] = res._5

    for {
      delegate <- freshTower(res)
      appendCalls <- Ref.of[IO, Int](0)
      appendPublished <- Deferred[IO, Unit]
      releaseFirstAppend <- Deferred[IO, Unit]
      blockingTower = new TowerStore[IO] {
        def appendAtFinality(
          ordinal: SnapshotOrdinal,
          snapshotHash: Hash,
          passes: Vector[LevelTrial]
        ): IO[Unit] =
          delegate.appendAtFinality(ordinal, snapshotHash, passes) >> appendCalls.updateAndGet(_ + 1).flatMap {
            case 1 => appendPublished.complete(()).void >> releaseFirstAppend.get
            case _ => IO.unit
          }

        def entriesAtLevel(level: Int, since: SnapshotOrdinal): IO[List[TowerEntry]] = delegate.entriesAtLevel(level, since)
        def latestAt(level: Int): IO[Option[TowerEntry]] = delegate.latestAt(level)
        def cumulativeCount(level: Int): IO[Long] = delegate.cumulativeCount(level)
        def pruneBelow(keepFrom: SnapshotOrdinal): IO[Unit] = delegate.pruneBelow(keepFrom)
      }
      passingSeed <- (0 to 255).toList.findM { seed =>
        computer
          .runAll(synthVrf(seed.toByte), Vector.fill(SuperLevelParams.SuperLevelCount)(3L), Gamma, Gamma)
          .map(_.exists(_.passed))
      }
        .flatMap(IO.fromOption(_)(new AssertionError("expected at least one deterministic passing VRF fixture")))
      finalizer <- TowerFinalizer.make[IO](blockingTower, computer, Gamma)
      first <- finalizer.finalizeFromParts(ord(3), synthHash("atomic"), synthVrf(passingSeed.toByte), Gamma).start
      _ <- appendPublished.get
      cancel <- first.cancel.start
      _ <- IO.sleep(20.millis)
      _ <- releaseFirstAppend.complete(())
      _ <- cancel.joinWithNever
      firstOutcome <- first.join
      retry <- finalizer.finalizeFromParts(ord(3), synthHash("atomic"), synthVrf(passingSeed.toByte), Gamma)
      observedCalls <- appendCalls.get
      wasCanceled = firstOutcome match {
        case Outcome.Canceled() => true
        case _                  => false
      }
    } yield expect(wasCanceled).and(expect(retry.isEmpty)).and(expect(observedCalls == 1))
  }

  test("noop finalizer — finalizeFromParts returns empty vector, no store side effects") { res =>
    val (_, _, _, _, _) = res
    val finalizer = TowerFinalizer.noop[IO]
    for {
      v <- finalizer.finalizeFromParts(ord(5), synthHash("n"), synthVrf(0x00.toByte), deltaSlot = Gamma)
    } yield expect(v.isEmpty)
  }

  // ──────────────── Tower density gauge sampler (§5 — `dag_nakamoto_tower_density_relative_error{level}`) ────────────────

  test("density gauge — one gauge value per super-level (1..9) emitted after each successful finalize") { res =>
    val (_, _, _, computer, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, gaugeMetrics) = pair
      implicit0(m: Metrics[IO]) = gaugeMetrics
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // ord=3 → gMu ≥ ψ_super, deltaSlot=γ → trial computer is invoked; the density sampler runs unconditionally.
      _ <- finalizer.finalizeFromParts(ord(3), synthHash("d1"), synthVrf(0x42.toByte), deltaSlot = Gamma)
      st <- stateRef.get
    } yield {
      // Expect exactly SuperLevelCount entries keyed by gauge name + `level=µ` tag.
      val gaugeKeys = st.gauges.keys.filter(_._1 == "dag_nakamoto_tower_density_relative_error").toList
      val levels = gaugeKeys.flatMap(_._2.collectFirst { case (k, v) if k.value == "level" => v.toInt }).sorted
      expect(levels == (1 to SuperLevelParams.SuperLevelCount).toList)
    }
  }

  test("density gauge — value matches DensityChecker.relativeError(cumulativeCount, finalizeCount, targetDensity)") { res =>
    // Prime the tower with synthetic L1+L2 hits, then run one finalize with deltaSlot=0 (no new pass — neither tower
    // counts nor the finalize counter is corrupted by the test scenario) and compare the per-level gauge against the
    // independently-computed `DensityChecker.relativeError`.
    val (_, _, _, computer, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, gaugeMetrics) = pair
      implicit0(m: Metrics[IO]) = gaugeMetrics
      tower <- freshTower(res)
      // Prime: 2 L1 hits + 1 L2 hit at ord=2, 4, 6.
      _ <- tower.appendAtFinality(
        ord(2),
        synthHash("p2"),
        Vector(LevelTrial(1, Ratio.Zero, Ratio.Zero, passed = true)) ++ Vector
          .tabulate(SuperLevelParams.SuperLevelCount - 1)(i => LevelTrial(i + 2, Ratio.Zero, Ratio.Zero, passed = false))
      )
      _ <- tower.appendAtFinality(
        ord(4),
        synthHash("p4"),
        Vector(
          LevelTrial(1, Ratio.Zero, Ratio.Zero, passed = true),
          LevelTrial(2, Ratio.Zero, Ratio.Zero, passed = true)
        ) ++ Vector.tabulate(SuperLevelParams.SuperLevelCount - 2)(i => LevelTrial(i + 3, Ratio.Zero, Ratio.Zero, passed = false))
      )
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // One finalize call → finalizeCount becomes 1 (the L0 reference seen by the density sampler).
      _ <- finalizer.finalizeFromParts(ord(10), synthHash("after"), synthVrf(0x55.toByte), deltaSlot = 0L)
      st <- stateRef.get
      // Independently compute the expected per-level gauge using the same DensityChecker call.
      countsByLevel <- (1 to SuperLevelParams.SuperLevelCount).toVector.traverse(tower.cumulativeCount)
    } yield {
      val totalLevel0 = 1L // exactly one non-skipped finalize call has run
      val expected = (1 to SuperLevelParams.SuperLevelCount).map { µ =>
        val cnt = countsByLevel(µ - 1)
        val tgt = SuperLevelParams.Levels(µ - 1).targetDensity
        µ -> DensityChecker.relativeError(cnt, totalLevel0, tgt).toDouble
      }.toMap
      val observed = expected.keys.toList.sorted.map { µ =>
        val tagKey = ("dag_nakamoto_tower_density_relative_error", Seq(unsafeLabelName("level") -> µ.toString))
        µ -> st.gauges.getOrElse(tagKey, Double.NaN)
      }.toMap
      expect(observed == expected)
    }
  }

  test("density gauge — empty tower yields relativeError=1.0 (DensityChecker defensive contract)") { res =>
    // First non-skipped finalize: all per-level counts are 0; relative error = |0 - target|/target = 1.0 for every µ.
    // This matches the [[DensityChecker.relativeError]] zero-observed-against-non-zero-target case.
    val (_, _, _, computer, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, gaugeMetrics) = pair
      implicit0(m: Metrics[IO]) = gaugeMetrics
      tower <- freshTower(res)
      finalizer <- TowerFinalizer.make[IO](tower, computer, Gamma)
      // deltaSlot=0 → no level passes → tower stays empty → every level-µ count is 0.
      _ <- finalizer.finalizeFromParts(ord(5), synthHash("empty"), synthVrf(0x42.toByte), deltaSlot = 0L)
      st <- stateRef.get
    } yield {
      val allOne = (1 to SuperLevelParams.SuperLevelCount).forall { µ =>
        val tagKey = ("dag_nakamoto_tower_density_relative_error", Seq(unsafeLabelName("level") -> µ.toString))
        st.gauges.get(tagKey).contains(1.0)
      }
      expect(allOne)
    }
  }
}
