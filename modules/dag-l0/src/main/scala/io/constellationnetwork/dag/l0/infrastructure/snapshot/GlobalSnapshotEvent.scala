package io.constellationnetwork.dag.l0.infrastructure.snapshot

/** Re-exports GlobalSnapshotEvent types from node-shared.
  *
  * These types were moved to `io.constellationnetwork.node.shared.snapshot.global` so they can be referenced by the generic event mempool
  * and gossip infrastructure in node-shared. This object preserves backward compatibility for existing dag-l0 code.
  */
object event {
  type GlobalSnapshotEvent = io.constellationnetwork.node.shared.snapshot.global.GlobalSnapshotEvent

  type DAGEvent = io.constellationnetwork.node.shared.snapshot.global.DAGEvent
  val DAGEvent = io.constellationnetwork.node.shared.snapshot.global.DAGEvent

  type StateChannelEvent = io.constellationnetwork.node.shared.snapshot.global.StateChannelEvent
  val StateChannelEvent = io.constellationnetwork.node.shared.snapshot.global.StateChannelEvent

  type AllowSpendEvent = io.constellationnetwork.node.shared.snapshot.global.AllowSpendEvent
  val AllowSpendEvent = io.constellationnetwork.node.shared.snapshot.global.AllowSpendEvent

  type TokenLockEvent = io.constellationnetwork.node.shared.snapshot.global.TokenLockEvent
  val TokenLockEvent = io.constellationnetwork.node.shared.snapshot.global.TokenLockEvent

  type UpdateNodeParametersEvent = io.constellationnetwork.node.shared.snapshot.global.UpdateNodeParametersEvent
  val UpdateNodeParametersEvent = io.constellationnetwork.node.shared.snapshot.global.UpdateNodeParametersEvent

  type UpdateDelegatedStakeEvent = io.constellationnetwork.node.shared.snapshot.global.UpdateDelegatedStakeEvent

  type CreateDelegatedStakeEvent = io.constellationnetwork.node.shared.snapshot.global.CreateDelegatedStakeEvent
  val CreateDelegatedStakeEvent = io.constellationnetwork.node.shared.snapshot.global.CreateDelegatedStakeEvent

  type WithdrawDelegatedStakeEvent = io.constellationnetwork.node.shared.snapshot.global.WithdrawDelegatedStakeEvent
  val WithdrawDelegatedStakeEvent = io.constellationnetwork.node.shared.snapshot.global.WithdrawDelegatedStakeEvent

  type UpdateNodeCollateralEvent = io.constellationnetwork.node.shared.snapshot.global.UpdateNodeCollateralEvent

  type CreateNodeCollateralEvent = io.constellationnetwork.node.shared.snapshot.global.CreateNodeCollateralEvent
  val CreateNodeCollateralEvent = io.constellationnetwork.node.shared.snapshot.global.CreateNodeCollateralEvent

  type WithdrawNodeCollateralEvent = io.constellationnetwork.node.shared.snapshot.global.WithdrawNodeCollateralEvent
  val WithdrawNodeCollateralEvent = io.constellationnetwork.node.shared.snapshot.global.WithdrawNodeCollateralEvent
}
