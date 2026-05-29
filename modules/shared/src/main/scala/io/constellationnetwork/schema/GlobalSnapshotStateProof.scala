package io.constellationnetwork.schema

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.schema.snapshot.StateProof
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotStateProofV1(
  lastStateChannelSnapshotHashesProof: Hash,
  lastTxRefsProof: Hash,
  balancesProof: Hash,
  lastCurrencySnapshotsProof: Option[MerkleRoot]
) extends StateProof {
  def toGlobalSnapshotStateProof: GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      lastStateChannelSnapshotHashesProof,
      lastTxRefsProof,
      balancesProof,
      lastCurrencySnapshotsProof,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
}

object GlobalSnapshotStateProofV1 {
  def apply: ((Hash, Hash, Hash, Option[MerkleRoot])) => GlobalSnapshotStateProofV1 = {
    case (x1, x2, x3, x4) => GlobalSnapshotStateProofV1.apply(x1, x2, x3, x4)
  }

  def fromGlobalSnapshotStateProof(proof: GlobalSnapshotStateProof): GlobalSnapshotStateProofV1 =
    GlobalSnapshotStateProofV1(
      proof.lastStateChannelSnapshotHashesProof,
      proof.lastTxRefsProof,
      proof.balancesProof,
      proof.lastCurrencySnapshotsProof
    )
}

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotStateProof(
  lastStateChannelSnapshotHashesProof: Hash,
  lastTxRefsProof: Hash,
  balancesProof: Hash,
  lastCurrencySnapshotsProof: Option[MerkleRoot],
  activeAllowSpends: Option[Hash],
  activeTokenLocks: Option[Hash],
  tokenLockBalances: Option[Hash],
  lastAllowSpendRefs: Option[Hash],
  lastTokenLockRefs: Option[Hash],
  updateNodeParameters: Option[Hash],
  activeDelegatedStakes: Option[Hash],
  delegatedStakesWithdrawals: Option[Hash],
  activeNodeCollaterals: Option[Hash],
  nodeCollateralWithdrawals: Option[Hash],
  priceState: Option[Hash],
  lastGlobalSnapshotsWithCurrency: Option[Hash],
  mptRoot: Option[Hash],
  /** §3 NIPoPoW S0: per-field MPT subtree root covering historicalStakeSnapshots entries. `Some` once any boundary has fired and the
    * partition has at least one entry; `None` during the warmup window before the first eta-period closes. Transitively covered by
    * `mptRoot` (which hashes over the full byte map including this partition's entries) — the per-field hash is supplied for efficient
    * single-period Merkle proofs by NIPoPoW light clients.
    */
  historicalStakeSnapshots: Option[Hash],
  /** §3 NIPoPoW historical-commitment SMT: the SINGLE root of the unbounded, on-disk SMT keyed by snapshot ordinal whose leaves are
    * `PerOrdinalCommitment(hypergraphRoot, incrementalSnapshotHash, towerEligibility)` (see
    * `node.shared.domain.nakamoto.nipopow.HistoricalCommitmentSmtStore`). `smtRoot(N) = SMT({ (i, commitment_i) : i ≤ N−k }).root`, `k` =
    * the confirmation depth, so it commits ONLY finalized ordinals (circularity-free: snapshot N's own incremental hash first appears in
    * `smtRoot(N+k)`). `None` in the genesis/warmup window (`N ≤ k`) and on any proof-build path that lacks the maintained store (those are
    * EXCLUDED from the `StateProofValidator` `===` via `StateProofComparison`; the field is populated + cross-checked on the
    * producer/follower-symmetric GSAM accept path). NOT transitively covered by `mptRoot` — the SMT is a SEPARATE on-disk store, so the
    * hypergraph (ledger) root stays independent of `smtRoot`.
    */
  smtRoot: Option[Hash]
) extends StateProof

object GlobalSnapshotStateProof {
  def apply: (
    (
      Hash,
      Hash,
      Hash,
      Option[MerkleRoot],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash],
      Option[Hash]
    )
  ) => GlobalSnapshotStateProof = {
    case (x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19) =>
      GlobalSnapshotStateProof.apply(x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16, x17, x18, x19)
  }
}
