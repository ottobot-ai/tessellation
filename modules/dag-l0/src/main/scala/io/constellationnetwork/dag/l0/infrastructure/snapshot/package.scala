package io.constellationnetwork.dag.l0.infrastructure

import io.constellationnetwork.schema._

package object snapshot {

  type GlobalSnapshotKey = SnapshotOrdinal

  type GlobalSnapshotArtifact = GlobalIncrementalSnapshot

  type GlobalSnapshotContext = GlobalSnapshotInfo

}
