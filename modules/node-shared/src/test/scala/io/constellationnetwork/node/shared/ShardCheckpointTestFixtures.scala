package io.constellationnetwork.node.shared

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

object ShardCheckpointTestFixtures {

  val defaultExecutionBase: GlobalSnapshotStateRef =
    executionBaseAt(SnapshotOrdinal.MinValue)

  def executionBaseAt(ordinal: SnapshotOrdinal): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      ordinal = ordinal,
      hash = Hash("a" * 64),
      parentHash = if (ordinal == SnapshotOrdinal.MinValue) Hash.empty else Hash("b" * 64),
      mptRoot = MptRoot(Hash("c" * 64))
    )
}
