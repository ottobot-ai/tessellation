package io.constellationnetwork.node.shared.infrastructure.metrics

import cats.effect.IO
import cats.effect.kernel.Ref

import scala.concurrent.duration.FiniteDuration

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics.{MetricKey, TagSeq}

/** Test-only Metrics interpreter that records `incrementCounter` calls into a `Ref[Map[String, Int]]`
  * so suites can assert against per-counter call counts. All other methods are no-ops.
  *
  * Lives under the `node.shared.infrastructure.metrics` package so it can satisfy the
  * `private[shared] getAllAsText` member of [[Metrics]] without subclass-visibility tricks at the
  * call site.
  */
object CountingMetrics {

  /** Build a `(counterRef, metricsInstance)` pair. Call sites usually start with `setup` and pass
    * the metrics into the system under test, then snapshot the ref at the end of the test.
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
}
