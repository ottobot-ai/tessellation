package io.constellationnetwork.node.shared.domain.genesis

import io.constellationnetwork.node.shared.domain.genesis.types.{GenesisAccount, L0GenesisData}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.snapshot.FullSnapshot
import io.constellationnetwork.security.signature.Signed

import fs2.io.file.Path

trait GenesisFS[F[_], S <: FullSnapshot[_, _]] {
  def write(genesis: Signed[S], identifier: Address, path: Path): F[Unit]

  def loadBalances(path: Path): F[Set[GenesisAccount]]

  def loadSignedGenesis(path: Path): F[Signed[S]]

  /** Load a Tier-1 `l0-genesis.json` test-vector file. Returns the parsed `L0GenesisData` carrying operator set, delegated-stake records,
    * node-collateral records, protocol params, and initial balances. The on-disk schema is documented in
    * `modules/node-shared/.../genesis/types.scala::L0GenesisData`.
    *
    * Callers use `L0GenesisData.initialBalanceMap` to build the `GlobalSnapshot.mkGenesis` balance map, then apply the `delegatedStakes`
    * and `nodeCollaterals` fields by overlay onto the in-memory `GlobalSnapshotInfo` AFTER `toGlobalSnapshotInfo` (Option (ii) in the plan
    * — the on-disk `Signed[GlobalSnapshot]` stays V1).
    */
  def loadL0Genesis(path: Path): F[L0GenesisData]
}
