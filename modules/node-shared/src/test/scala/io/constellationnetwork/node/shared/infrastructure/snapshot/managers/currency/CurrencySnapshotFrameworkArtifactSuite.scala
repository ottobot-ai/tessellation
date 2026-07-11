package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, PricingUpdate, SharedArtifact}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

object CurrencySnapshotFrameworkArtifactSuite extends FunSuite {

  test("supplied GlobalSnapshotsProcessed is discarded and cannot forge framework acknowledgement") {
    val claimed = GlobalSnapshotsProcessed(SortedSet(SnapshotOrdinal(NonNegLong(17L))))
    val applicationArtifact = PricingUpdate.zero
    val supplied = SortedSet[SharedArtifact](claimed, applicationArtifact)

    val filtered = CurrencySnapshotAcceptanceManager.filterFrameworkGeneratedArtifacts(supplied)

    expect.all(
      !filtered.exists(_.isInstanceOf[GlobalSnapshotsProcessed]),
      filtered.contains(applicationArtifact)
    )
  }
}
