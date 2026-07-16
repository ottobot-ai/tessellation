package io.constellationnetwork.tools.migration.v4

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.schema.SnapshotOrdinal

sealed trait V4SourceEncoding extends Product with Serializable

object V4SourceEncoding {
  case object V4KryoV1 extends V4SourceEncoding
  case object V4BrotliJson extends V4SourceEncoding
}

/** Exact upstream-v4 source era selected for an offline migration input.
  *
  * This type lives in `tools`, whose build depends on node projects while no node project depends on `tools`. The one-way dependency keeps
  * this historical selector structurally unavailable to active runtime hashing, validation, and storage paths.
  */
sealed abstract case class V4SourceContext private (
  environment: AppEnvironment,
  ordinal: SnapshotOrdinal
) {
  final val encoding: V4SourceEncoding = V4SourceContext.resolveEncoding(environment, ordinal)
}

object V4SourceContext {
  import V4SourceEncoding.{V4BrotliJson, V4KryoV1}

  def apply(environment: AppEnvironment, ordinal: SnapshotOrdinal): V4SourceContext =
    new V4SourceContext(environment, ordinal) {}

  private def resolveEncoding(environment: AppEnvironment, ordinal: SnapshotOrdinal): V4SourceEncoding = {
    // Frozen from the v4.0.0 application.conf. Live fork configuration is never historical-source authority.
    val inclusiveLastKryoOrdinal = environment match {
      case AppEnvironment.Mainnet        => 2572384L
      case AppEnvironment.Testnet        => 1933590L
      case AppEnvironment.Integrationnet => 1527434L
      case AppEnvironment.Dev            => 0L
    }

    if (ordinal.value.value <= inclusiveLastKryoOrdinal) V4KryoV1 else V4BrotliJson
  }
}
