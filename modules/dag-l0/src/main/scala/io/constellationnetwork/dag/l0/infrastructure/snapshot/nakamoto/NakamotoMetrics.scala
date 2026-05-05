package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics._
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._

import eu.timepit.refined.auto._
import fs2.Stream

/** Periodically publishes Nakamoto chain-state gauges to Prometheus.
  *
  * Point-of-action counters (slots_won, snapshots_produced/received/rejected, finalized, catchups) are emitted directly in
  * SnapshotLeaderLoop and NakamotoSyncDaemon via Metrics[F].
  *
  * This stream handles gauges that require periodic sampling from chain store state:
  *   - dag_nakamoto_chain_length: snapshots in chain store
  *   - dag_nakamoto_fork_count: active fork branches
  *   - dag_nakamoto_fill_rate: snapshots / slots
  *   - dag_nakamoto_attestation_weight: best tip attestation weight
  */
object NakamotoMetrics {

  def run[F[_]: Async: Metrics](
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    tipTracker: TipTracker[F],
    genesisTimeMs: Long
  ): Stream[F, Unit] = {
    val m = Metrics[F]

    Stream.awakeEvery[F](5.seconds).evalMap { _ =>
      for {
        bestTip <- chainStore.bestTip
        chainLen <- chainStore.chainLength
        forkCnt <- chainStore.forkCount
        finalOrd <- chainStore.lastFinalizedOrdinal

        now = System.currentTimeMillis()
        currentSlot = if (genesisTimeMs > 0 && now > genesisTimeMs) (now - genesisTimeMs) / 1000L else 0L

        tipOrd = bestTip.map(_.ordinal).getOrElse(0L)
        fillRate = if (currentSlot > 0) tipOrd.toDouble / currentSlot.toDouble else 0.0

        weight <- bestTip.map(_.hash).traverse(tipTracker.attestationWeight)
        // Lossy Ratio→Double for metric gauge only — never fed back into consensus arithmetic.
        attWeight = weight.getOrElse(Ratio.Zero).toDouble

        _ <- m.updateGauge("dag_nakamoto_chain_length", chainLen.toLong)
        _ <- m.updateGauge("dag_nakamoto_fork_count", forkCnt.toLong)
        _ <- m.updateGauge("dag_nakamoto_fill_rate", fillRate)
        _ <- m.updateGauge("dag_nakamoto_attestation_weight", attWeight)
      } yield ()
    }
  }
}
