package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object ExecutionShardEpochSuite extends SimpleIOSuite {

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(value))

  pureTest("execution-shard membership rotates only when the finalized anchor crosses the eta boundary") {
    val periodLength = 99L

    expect.same(EtaCalculation.executionShardEpoch(ordinal(98L), periodLength), EtaPeriod(0L)) &&
    expect.same(EtaCalculation.executionShardEpoch(ordinal(99L), periodLength), EtaPeriod(1L)) &&
    expect.same(EtaCalculation.executionShardEpoch(ordinal(197L), periodLength), EtaPeriod(1L)) &&
    expect.same(EtaCalculation.executionShardEpoch(ordinal(198L), periodLength), EtaPeriod(2L))
  }
}
