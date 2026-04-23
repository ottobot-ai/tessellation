package io.constellationnetwork.schema.mpt

import io.constellationnetwork.schema.epoch.EpochProgress

/** Node-wide MPT configuration: the epoch offset applied to node-collateral withdrawals to compute their expiry epoch. Supplied
  * implicitly alongside `StateProofSelector` because it shapes MPT write-path output (the node-collateral-withdrawal expiry index). When
  * `None`, the expiry index is not maintained — used at rebuild sites that pre-date the index and in tests that don't exercise it.
  *
  * Both `accept()` (delta path) and `syncFromGlobalSnapshotInfo` / `toAllStateKeyValueBytes` (rebuild path) MUST observe the same value;
  * otherwise the two paths produce divergent mptRoots. Enforce one implicit-provider line per process/test-suite.
  */
final case class WithdrawalTimeLimit(value: Option[EpochProgress]) extends AnyVal

object WithdrawalTimeLimit {
  val none: WithdrawalTimeLimit = WithdrawalTimeLimit(None)
  def some(ep: EpochProgress): WithdrawalTimeLimit = WithdrawalTimeLimit(Some(ep))
}
