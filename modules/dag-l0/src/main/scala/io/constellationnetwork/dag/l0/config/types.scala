package io.constellationnetwork.dag.l0.config

import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.schema.SnapshotOrdinal

import ciris.Secret
import eu.timepit.refined.types.numeric._
import eu.timepit.refined.types.string.NonEmptyString
import fs2.io.file.Path

object types {

  /** GL0 snapshot storage and deterministic proposal-size limits.
    *
    * GL0 does not consume the shared `ConsensusConfig`: its proposal lifecycle is Nakamoto/Taktikos, not the inherited
    * facility/proposal/signature BFT round engine retained by CurrencyL0.
    */
  case class GlobalSnapshotConfig(
    eventCutter: EventCutterConfig,
    inMemoryCapacity: NonNegLong,
    snapshotPath: Path,
    snapshotInfoPath: Path,
    incrementalTmpSnapshotPath: Path,
    incrementalPersistedSnapshotPath: Path,
    calculatedStatePath: Path,
    globalSnapshotsWithStatePath: Path,
    globalSnapshotsWithStateDeltasPath: Path,
    maxGlobalSnapshotsWithStateStored: PosLong,
    maxGlobalSnapshotsWithStateDeltasStored: PosLong,
    combinedSnapshotCheckpointPath: Path
  )

  case class AppConfigReader(
    snapshot: GlobalSnapshotConfig,
    stateChannel: StateChannelConfig,
    peerDiscovery: PeerDiscoveryConfig,
    incremental: IncrementalConfig
  )

  case class AppConfig(
    snapshot: GlobalSnapshotConfig,
    rewards: ClassicRewardsConfig,
    stateChannel: StateChannelConfig,
    peerDiscovery: PeerDiscoveryConfig,
    incremental: IncrementalConfig,
    shared: SharedConfig
  ) {
    val environment = shared.environment
    val gossip = shared.gossip
    val http = shared.http
    val snapshotSize = shared.snapshotSize
    val collateral = shared.collateral
  }

  case class IncrementalConfig(
    lastFullGlobalSnapshotOrdinal: Map[AppEnvironment, SnapshotOrdinal]
  )

  case class StateChannelConfig(
    pullDelay: NonNegLong,
    purgeDelay: NonNegLong
  )

  case class PeerDiscoveryConfig(
    delay: PeerDiscoveryDelay
  )

  case class DBConfig(
    driver: NonEmptyString,
    url: NonEmptyString,
    user: NonEmptyString,
    password: Secret[String]
  )
}
