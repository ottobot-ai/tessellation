package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.FiniteDuration

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics.{MetricKey, TagSeq}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardMetrics
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardBinaryBuffer]] — the per-shard raw-binary accumulator (EXECUTION-SHARDING R-1, the inversion intake), incl. the S3
  * finalize-keyed pruning + occupancy gauge + evict-finalized-first contract (`docs/nakamoto/SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md` §S3).
  *
  * Coverage:
  *   - buffer then snapshot round-trips per MG in insertion order.
  *   - dedup by binary hash (idempotent re-buffer, even across MGs).
  *   - per-MG grouping with multiple MGs.
  *   - S3 (1) pruneFinalized drops strict ancestors of the finalized tip; idempotent on re-run.
  *   - S3 (2) pruneFinalized KEEPS the tip + its descendants (the re-inclusion set between finalized tip and bestTip).
  *   - S3 (3) pruneFinalized with a floorHash not in the buffer ⇒ no-op.
  *   - S3 (4) at cap: a buffer full of UN-finalized binaries fires the overflow counter; reclaim frees a finalized binary + admits the new
  *     one.
  *   - S3 (5) MIN(finalizedTip,bestTip) guard: a backward reorg (caller passes the bestTip floor below finalized) retains binaries above
  *     bestTip.
  *   - S3 (6) gauge reflects occupancy after buffer + prune.
  *   - empty buffer ⇒ empty snapshot.
  */
object ShardBinaryBufferSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private def hashFromString(s: String): Hash =
    Hash.fromBytes(s.getBytes("UTF-8"))

  private def hashOf(b: Signed[StateChannelSnapshotBinary])(implicit h: Hasher[IO]): IO[Hash] = {
    import io.constellationnetwork.security.signature.Signed.SignedOps
    SignedOps(b).toHashed[IO].map(_.hash)
  }

  // A signed binary whose parent pointer (`lastSnapshotHash`) is `parent`. Distinct (label, idx) ⇒ distinct content ⇒ distinct hash.
  private def mkSignedBinaryWithParent(label: String, idx: Int, parent: Hash): Signed[StateChannelSnapshotBinary] = {
    import cats.data.NonEmptySet
    import io.constellationnetwork.schema.ID.Id
    import io.constellationnetwork.security.hex.Hex
    val body = StateChannelSnapshotBinary(
      lastSnapshotHash = parent,
      content = s"$label-content-$idx".getBytes("UTF-8"),
      fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
    )
    val sentinelProof = SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(body, NonEmptySet.of(sentinelProof))
  }

  // `lastSnapshotHash` is irrelevant to the non-prune cases (buffer dedups + groups), so vary it for realism only.
  private def mkSignedBinary(label: String, idx: Int): Signed[StateChannelSnapshotBinary] =
    mkSignedBinaryWithParent(label, idx, hashFromString(s"$label-parent-$idx"))

  /** Build a REAL chain of `n` binaries for `label`, each linked by `lastSnapshotHash == previous binary's actual canonical hash`. Returns
    * the binaries in chain order alongside their hashes so a test can pick a finalize floor at any depth and exercise the ancestor-walk.
    */
  private def mkChain(label: String, n: Int, genesisParent: Hash)(
    implicit h: Hasher[IO]
  ): IO[List[(Signed[StateChannelSnapshotBinary], Hash)]] =
    (0 until n).toList
      .foldLeftM(List.empty[(Signed[StateChannelSnapshotBinary], Hash)]) { (acc, idx) =>
        val parent = acc.headOption.map(_._2).getOrElse(genesisParent)
        val b = mkSignedBinaryWithParent(label, idx, parent)
        hashOf(b).map(hb => (b, hb) :: acc)
      }
      .map(_.reverse)

  /** Recording `Metrics[IO]`: captures the latest gauge value + counter increments per (key, tags) so the S3 gauge/overflow assertions can
    * read back the production emit path. Delegates every other method to [[NoOpMetrics]].
    */
  private final class RecordingMetrics(
    gauges: Ref[IO, Map[(MetricKey, TagSeq), Long]],
    counters: Ref[IO, Map[(MetricKey, TagSeq), Long]]
  ) extends Metrics[IO] {
    private val del = NoOpMetrics.make

    override def updateGauge(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] =
      gauges.update(_.updated((key, tags), value.toLong))
    override def updateGauge(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] =
      gauges.update(_.updated((key, tags), value))
    override def incrementCounter(key: MetricKey, tags: TagSeq): IO[Unit] =
      counters.update(m => m.updated((key, tags), m.getOrElse((key, tags), 0L) + 1L))

    override def updateGauge(key: MetricKey, value: Int): IO[Unit] = del.updateGauge(key, value)
    override def updateGauge(key: MetricKey, value: Long): IO[Unit] = del.updateGauge(key, value)
    override def updateGauge(key: MetricKey, value: Float): IO[Unit] = del.updateGauge(key, value)
    override def updateGauge(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = del.updateGauge(key, value, tags)
    override def updateGauge(key: MetricKey, value: Double): IO[Unit] = del.updateGauge(key, value)
    override def updateGauge(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = del.updateGauge(key, value, tags)
    override def incrementCounterBy(key: MetricKey, value: Int): IO[Unit] = del.incrementCounterBy(key, value)
    override def incrementCounterBy(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = del.incrementCounterBy(key, value, tags)
    override def incrementCounterBy(key: MetricKey, value: Long): IO[Unit] = del.incrementCounterBy(key, value)
    override def incrementCounterBy(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = del.incrementCounterBy(key, value, tags)
    override def incrementCounterBy(key: MetricKey, value: Float): IO[Unit] = del.incrementCounterBy(key, value)
    override def incrementCounterBy(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = del.incrementCounterBy(key, value, tags)
    override def incrementCounterBy(key: MetricKey, value: Double): IO[Unit] = del.incrementCounterBy(key, value)
    override def incrementCounterBy(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = del.incrementCounterBy(key, value, tags)
    override def recordTime(key: MetricKey, duration: FiniteDuration, tags: TagSeq): IO[Unit] = del.recordTime(key, duration, tags)
    override def recordDistribution(key: MetricKey, value: Int): IO[Unit] = del.recordDistribution(key, value)
    override def recordDistribution(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = del.recordDistribution(key, value, tags)
    override def recordDistribution(key: MetricKey, value: Long): IO[Unit] = del.recordDistribution(key, value)
    override def recordDistribution(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = del.recordDistribution(key, value, tags)
    override def recordDistribution(key: MetricKey, value: Float): IO[Unit] = del.recordDistribution(key, value)
    override def recordDistribution(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = del.recordDistribution(key, value, tags)
    override def recordDistribution(key: MetricKey, value: Double): IO[Unit] = del.recordDistribution(key, value)
    override def recordDistribution(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = del.recordDistribution(key, value, tags)
    override private[shared] def getAllAsText: IO[String] = del.getAllAsText
    override def timedMetric[A](operation: IO[A], metricKey: MetricKey, tags: TagSeq): IO[A] = operation
    override def genericRecordDistributionWithTimeBuckets[A: Numeric](
      key: MetricKey,
      value: A,
      timeSeconds: Float,
      tags: TagSeq
    ): IO[Unit] = del.genericRecordDistributionWithTimeBuckets(key, value, timeSeconds, tags)
    override def recordTimeHistogram(key: MetricKey, duration: FiniteDuration, tags: TagSeq, buckets: Array[Double]): IO[Unit] =
      del.recordTimeHistogram(key, duration, tags, buckets)
    override def recordSizeHistogram(key: MetricKey, sizeBytes: Long, tags: TagSeq, buckets: Array[Double]): IO[Unit] =
      del.recordSizeHistogram(key, sizeBytes, tags, buckets)
  }

  private def mkRecordingMetrics: IO[(RecordingMetrics, Ref[IO, Map[(MetricKey, TagSeq), Long]], Ref[IO, Map[(MetricKey, TagSeq), Long]])] =
    for {
      gauges <- Ref.of[IO, Map[(MetricKey, TagSeq), Long]](Map.empty)
      counters <- Ref.of[IO, Map[(MetricKey, TagSeq), Long]](Map.empty)
    } yield (new RecordingMetrics(gauges, counters), gauges, counters)

  private val noopMetrics: Metrics[IO] = NoOpMetrics.make

  // ---- Existing intake coverage (no-op metrics) ----------------------------

  test("empty buffer ⇒ empty snapshotPending") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val _ = h
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      pending <- buffer.snapshotPending
    } yield expect(pending.isEmpty)
  }

  test("buffer one binary ⇒ snapshotPending returns it under its MG") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    val b = mkSignedBinary("mg-a", 0)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b)
      pending <- buffer.snapshotPending
    } yield
      expect.all(
        pending.size == 1,
        pending.get(mg).exists(_.toList.size == 1),
        pending.get(mg).exists(_.head == b)
      )
  }

  test("multiple binaries for one MG preserve insertion order") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    val b0 = mkSignedBinary("mg-a", 0)
    val b1 = mkSignedBinary("mg-a", 1)
    val b2 = mkSignedBinary("mg-a", 2)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b0)
      _ <- buffer.bufferBinary(mg, b1)
      _ <- buffer.bufferBinary(mg, b2)
      pending <- buffer.snapshotPending
    } yield expect(pending.get(mg).map(_.toList) == Some(List(b0, b1, b2)))
  }

  test("dedup by binary hash: re-buffering the same binary is a no-op") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    val b = mkSignedBinary("mg-a", 0)
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mg, b)
      _ <- buffer.bufferBinary(mg, b)
      _ <- buffer.bufferBinary(mg, b)
      pending <- buffer.snapshotPending
    } yield expect(pending.get(mg).map(_.toList.size) == Some(1))
  }

  test("multiple MGs are grouped separately") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mgA = mkAddress("mg-a")
    val mgB = mkAddress("mg-b")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 16)
      _ <- buffer.bufferBinary(mgA, mkSignedBinary("mg-a", 0))
      _ <- buffer.bufferBinary(mgB, mkSignedBinary("mg-b", 0))
      _ <- buffer.bufferBinary(mgA, mkSignedBinary("mg-a", 1))
      pending <- buffer.snapshotPending
    } yield
      expect.all(
        pending.size == 2,
        pending.get(mgA).map(_.toList.size) == Some(2),
        pending.get(mgB).map(_.toList.size) == Some(1)
      )
  }

  // ---- S3 (1) drop strict ancestors of the finalized tip; idempotent ----

  test("S3: pruneFinalized drops strict ancestors of the finalized tip; idempotent on re-run") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 64)
      chain <- mkChain("mg-a", 5, hashFromString("genesis-a")) // b0 <- b1 <- b2 <- b3 <- b4
      _ <- chain.zipWithIndex.traverse_ { case ((b, _), _) => buffer.bufferBinary(mg, b) }
      floorHash = chain(3)._2 // finalize tip at b3 (depth 3); strict ancestors b0,b1,b2 should drop, b3,b4 kept
      _ <- buffer.pruneFinalized(SortedMap(mg -> floorHash))
      pending1 <- buffer.snapshotPending
      kept1 <- pending1.get(mg).toList.flatMap(_.toList).traverse(hashOf)
      // Idempotent re-run with the SAME floor drops nothing new.
      _ <- buffer.pruneFinalized(SortedMap(mg -> floorHash))
      pending2 <- buffer.snapshotPending
      kept2 <- pending2.get(mg).toList.flatMap(_.toList).traverse(hashOf)
    } yield
      expect.all(
        kept1.toSet == Set(chain(3)._2, chain(4)._2),
        !kept1.toSet.contains(chain(0)._2),
        !kept1.toSet.contains(chain(1)._2),
        !kept1.toSet.contains(chain(2)._2),
        kept2.toSet == kept1.toSet // idempotent
      )
  }

  // ---- S3 (2) KEEP tip + descendants (the re-inclusion set) ----------------

  test("S3: pruneFinalized keeps the finalized tip AND its descendants (the re-inclusion set)") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 64)
      chain <- mkChain("mg-a", 6, hashFromString("genesis-a")) // b0..b5; finalize at b2 (bestTip = b5)
      _ <- chain.traverse_ { case (b, _) => buffer.bufferBinary(mg, b) }
      floorHash = chain(2)._2
      _ <- buffer.pruneFinalized(SortedMap(mg -> floorHash))
      pending <- buffer.snapshotPending
      kept <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
    } yield
      expect.all(
        // tip b2 + descendants b3,b4,b5 retained (S2's base->latest re-inclusion set); ancestors b0,b1 dropped.
        kept == List(chain(2)._2, chain(3)._2, chain(4)._2, chain(5)._2),
        !kept.contains(chain(0)._2),
        !kept.contains(chain(1)._2)
      )
  }

  // ---- S3 (3) floorHash not in buffer ⇒ no-op -------------------------------

  test("S3: pruneFinalized with a floorHash not in the buffer ⇒ no-op for that MG") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 64)
      chain <- mkChain("mg-a", 4, hashFromString("genesis-a"))
      _ <- chain.traverse_ { case (b, _) => buffer.bufferBinary(mg, b) }
      _ <- buffer.pruneFinalized(SortedMap(mg -> hashFromString("not-a-buffered-binary")))
      pending <- buffer.snapshotPending
      kept <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
    } yield expect(kept == chain.map(_._2)) // unchanged — conservative no-op
  }

  // ---- S3 (4) at cap: overflow counter + reclaim admits ---------------------

  test("S3: at cap with only UN-finalized binaries, a new binary is rejected and the overflow counter fires") { res =>
    implicit val (h, _) = res
    for {
      mm <- mkRecordingMetrics
      (metrics, _, counters) = mm
      result <- {
        implicit val m: Metrics[IO] = metrics
        val mg = mkAddress("mg-a")
        for {
          buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(3), cap = 2)
          chain <- mkChain("mg-a", 3, hashFromString("genesis-a")) // b0 <- b1 <- (b2 beyond cap)
          _ <- buffer.bufferBinary(mg, chain(0)._1)
          _ <- buffer.bufferBinary(mg, chain(1)._1)
          // No pruneFinalized ran ⇒ retainedFloor empty ⇒ nothing finalized to evict ⇒ reject + overflow counter.
          _ <- buffer.bufferBinary(mg, chain(2)._1)
          pending <- buffer.snapshotPending
          kept <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
        } yield (kept, chain(0)._2, chain(1)._2, chain(2)._2)
      }
      (kept, b0Hash, b1Hash, b2Hash) = result
      cs <- counters.get
      overflow = cs.get((ShardMetrics.BufferOverflowTotal, ShardMetrics.shardIdTag(ShardId.unsafeApply(3))))
    } yield
      expect.all(
        kept.toSet == Set(b0Hash, b1Hash), // un-finalized binaries remain
        !kept.toSet.contains(b2Hash), // the new one is rejected
        overflow == Some(1L) // LOUD overflow counter fired
      )
  }

  test("S3: at cap, reclaim evicts a finalized binary and admits the new one (no overflow)") { res =>
    implicit val (h, _) = res
    for {
      mm <- mkRecordingMetrics
      (metrics, _, counters) = mm
      result <- {
        implicit val m: Metrics[IO] = metrics
        val mg = mkAddress("mg-a")
        for {
          buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 2)
          chain <- mkChain("mg-a", 3, hashFromString("genesis-a")) // b0 <- b1 <- b2
          _ <- buffer.bufferBinary(mg, chain(0)._1) // {b0}
          _ <- buffer.bufferBinary(mg, chain(1)._1) // {b0,b1}
          // Finalize tip at b1 ⇒ b0 is a strict (finalized) ancestor. pruneFinalized records retainedFloor={mg->b1} and drops b0 ⇒ {b1}.
          _ <- buffer.pruneFinalized(SortedMap(mg -> chain(1)._2))
          // Refill to cap WITHOUT a new floor: b0 re-admitted (1 < cap) ⇒ {b1,b0} at cap=2.
          _ <- buffer.bufferBinary(mg, chain(0)._1)
          // Admitting b2 forces the at-cap reclaim: prune ancestors of retainedFloor (b1) ⇒ evicts finalized b0 ⇒ admits b2 ⇒ {b1,b2}.
          _ <- buffer.bufferBinary(mg, chain(2)._1)
          pending <- buffer.snapshotPending
          ks <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
        } yield (ks, chain(1)._2, chain(2)._2, chain(0)._2)
      }
      (kept, b1Hash, b2Hash, b0Hash) = result
      cs <- counters.get
      overflow = cs.get((ShardMetrics.BufferOverflowTotal, ShardMetrics.shardIdTag(ShardId.unsafeApply(0))))
    } yield
      expect.all(
        kept.toSet == Set(b1Hash, b2Hash), // finalized b0 evicted, new b2 admitted
        !kept.toSet.contains(b0Hash),
        overflow.isEmpty // reclaim succeeded ⇒ NO overflow counter
      )
  }

  // ---- S3 (5) MIN(finalizedTip,bestTip) backward-reorg guard ----------------

  test("S3: MIN-guard — a backward-reorg floor (below bestTip) retains binaries above the bestTip floor") { res =>
    implicit val (h, _) = res
    implicit val m: Metrics[IO] = noopMetrics
    val mg = mkAddress("mg-a")
    for {
      buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(0), cap = 64)
      chain <- mkChain("mg-a", 6, hashFromString("genesis-a")) // b0..b5
      _ <- chain.traverse_ { case (b, _) => buffer.bufferBinary(mg, b) }
      // Simulate the §5 guard: finalizedTip = b4, but a backward noteAnchor reorg pushed bestTip to b2, so the CALLER passes
      // MIN = the bestTip floor (b2). The buffer must then retain b2..b5 — NOT prune up to b4 — so the post-reorg window finds them.
      bestTipFloor = chain(2)._2
      _ <- buffer.pruneFinalized(SortedMap(mg -> bestTipFloor))
      pending <- buffer.snapshotPending
      kept <- pending.get(mg).toList.flatMap(_.toList).traverse(hashOf)
    } yield
      expect.all(
        kept == List(chain(2)._2, chain(3)._2, chain(4)._2, chain(5)._2), // b3,b4 (above bestTip) RETAINED
        kept.contains(chain(3)._2),
        kept.contains(chain(4)._2),
        !kept.contains(chain(0)._2),
        !kept.contains(chain(1)._2)
      )
  }

  // ---- S3 (6) gauge reflects occupancy --------------------------------------

  test("S3: occupancy gauge reflects buffer size after buffer + prune") { res =>
    implicit val (h, _) = res
    for {
      mm <- mkRecordingMetrics
      (metrics, gauges, _) = mm
      sizes <- {
        implicit val m: Metrics[IO] = metrics
        val mg = mkAddress("mg-a")
        for {
          buffer <- ShardBinaryBuffer.make[IO](ShardId.unsafeApply(7), cap = 64)
          chain <- mkChain("mg-a", 5, hashFromString("genesis-a"))
          _ <- chain.traverse_ { case (b, _) => buffer.bufferBinary(mg, b) }
          afterBuffer <- gauges.get.map(_.get((ShardMetrics.BufferSize, ShardMetrics.shardIdTag(ShardId.unsafeApply(7)))))
          _ <- buffer.pruneFinalized(SortedMap(mg -> chain(3)._2)) // drops b0,b1,b2 ⇒ keep b3,b4 ⇒ size 2
          afterPrune <- gauges.get.map(_.get((ShardMetrics.BufferSize, ShardMetrics.shardIdTag(ShardId.unsafeApply(7)))))
        } yield (afterBuffer, afterPrune)
      }
    } yield
      expect.all(
        sizes._1 == Some(5L), // gauge tracked the 5 buffered binaries
        sizes._2 == Some(2L) // gauge tracked the post-prune occupancy
      )
  }
}
