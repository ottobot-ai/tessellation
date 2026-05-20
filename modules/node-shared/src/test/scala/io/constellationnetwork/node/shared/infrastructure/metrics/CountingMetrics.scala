package io.constellationnetwork.node.shared.infrastructure.metrics

import cats.effect.IO
import cats.effect.kernel.Ref

import scala.concurrent.duration.FiniteDuration

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics.{MetricKey, TagSeq}

/** Test-only Metrics interpreter that records `incrementCounter` calls into a `Ref[Map[String, Int]]` so suites can assert against
  * per-counter call counts. Also captures the **latest** gauge value per `(metricName, tags)` so suites can assert against per-label
  * `updateGauge` emissions (gauges replace, so we keep most-recent semantics).
  *
  * Lives under the `node.shared.infrastructure.metrics` package so it can satisfy the `private[shared] getAllAsText` member of [[Metrics]]
  * without subclass-visibility tricks at the call site.
  */
object CountingMetrics {

  /** Snapshot of all captured state. Counters increment per call; gauges record the most-recent value per `(name, tags)` key. */
  final case class State(
    counters: Map[String, Int] = Map.empty,
    gauges: Map[(String, TagSeq), Double] = Map.empty
  )

  /** Build a `(stateRef, metricsInstance)` pair. Call sites pass `metricsInstance` into the system under test, then snapshot `stateRef` at
    * the end of the test. Counters retain `Map[String, Int]` semantics through the legacy `make` helper.
    */
  def makeWithState: IO[(Ref[IO, State], Metrics[IO])] =
    Ref.of[IO, State](State()).map { ref =>
      (ref, instanceWithState(ref))
    }

  /** Legacy helper preserved for existing suites that only need counter counts (8 callers). Wraps `makeWithState` and exposes a
    * counter-only projection — `Ref[IO, Map[String, Int]]` reads remain valid because the inner state is observed through a `.map` view.
    */
  def make: IO[(Ref[IO, Map[String, Int]], Metrics[IO])] =
    Ref.of[IO, Map[String, Int]](Map.empty).map { ref =>
      (ref, instance(ref))
    }

  /** Wrap an existing Ref. Useful if a suite wants several metrics instances sharing the same store. */
  def instance(ref: Ref[IO, Map[String, Int]]): Metrics[IO] =
    new Metrics[IO] {
      override def updateGauge(key: MetricKey, value: Int): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Long): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Float): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Double): IO[Unit] = IO.unit
      override def updateGauge(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = IO.unit

      override def incrementCounter(key: MetricKey, tags: TagSeq): IO[Unit] =
        ref.update(m => m.updated(key.value, m.getOrElse(key.value, 0) + 1))

      override def incrementCounterBy(key: MetricKey, value: Int): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Long): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Float): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Double): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = IO.unit

      override def recordTime(key: MetricKey, duration: FiniteDuration, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Int): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Long): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Float): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Double): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = IO.unit

      override private[shared] def getAllAsText: IO[String] = IO("Metrics.counting-test")

      override def timedMetric[A](operation: IO[A], metricKey: MetricKey, tags: TagSeq): IO[A] = operation

      override def genericRecordDistributionWithTimeBuckets[A: Numeric](
        key: MetricKey,
        value: A,
        timeSeconds: Float,
        tags: TagSeq
      ): IO[Unit] = IO.unit

      override def recordTimeHistogram(
        key: MetricKey,
        duration: FiniteDuration,
        tags: TagSeq,
        buckets: Array[Double]
      ): IO[Unit] = IO.unit

      override def recordSizeHistogram(
        key: MetricKey,
        sizeBytes: Long,
        tags: TagSeq,
        buckets: Array[Double]
      ): IO[Unit] = IO.unit
    }

  /** Build a Metrics[IO] backed by a [[State]] Ref — captures both counters and per-tag gauge updates. Distribution / time / size
    * histograms remain no-ops; tests that need them should add them under the same pattern.
    */
  def instanceWithState(ref: Ref[IO, State]): Metrics[IO] = {
    def setGauge(key: MetricKey, tags: TagSeq, value: Double): IO[Unit] =
      ref.update(s => s.copy(gauges = s.gauges.updated((key.value, tags), value)))

    new Metrics[IO] {
      override def updateGauge(key: MetricKey, value: Int): IO[Unit] = setGauge(key, Seq.empty, value.toDouble)
      override def updateGauge(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = setGauge(key, tags, value.toDouble)
      override def updateGauge(key: MetricKey, value: Long): IO[Unit] = setGauge(key, Seq.empty, value.toDouble)
      override def updateGauge(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = setGauge(key, tags, value.toDouble)
      override def updateGauge(key: MetricKey, value: Float): IO[Unit] = setGauge(key, Seq.empty, value.toDouble)
      override def updateGauge(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = setGauge(key, tags, value.toDouble)
      override def updateGauge(key: MetricKey, value: Double): IO[Unit] = setGauge(key, Seq.empty, value)
      override def updateGauge(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = setGauge(key, tags, value)

      override def incrementCounter(key: MetricKey, tags: TagSeq): IO[Unit] =
        ref.update(s => s.copy(counters = s.counters.updated(key.value, s.counters.getOrElse(key.value, 0) + 1)))

      override def incrementCounterBy(key: MetricKey, value: Int): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Long): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Float): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Double): IO[Unit] = IO.unit
      override def incrementCounterBy(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = IO.unit

      override def recordTime(key: MetricKey, duration: FiniteDuration, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Int): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Int, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Long): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Long, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Float): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Float, tags: TagSeq): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Double): IO[Unit] = IO.unit
      override def recordDistribution(key: MetricKey, value: Double, tags: TagSeq): IO[Unit] = IO.unit

      override private[shared] def getAllAsText: IO[String] = IO("Metrics.counting-test")

      override def timedMetric[A](operation: IO[A], metricKey: MetricKey, tags: TagSeq): IO[A] = operation

      override def genericRecordDistributionWithTimeBuckets[A: Numeric](
        key: MetricKey,
        value: A,
        timeSeconds: Float,
        tags: TagSeq
      ): IO[Unit] = IO.unit

      override def recordTimeHistogram(
        key: MetricKey,
        duration: FiniteDuration,
        tags: TagSeq,
        buckets: Array[Double]
      ): IO[Unit] = IO.unit

      override def recordSizeHistogram(
        key: MetricKey,
        sizeBytes: Long,
        tags: TagSeq,
        buckets: Array[Double]
      ): IO[Unit] = IO.unit
    }
  }
}
