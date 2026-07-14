package io.constellationnetwork.schema

import io.constellationnetwork.merkletree.MerkleRoot
import io.constellationnetwork.schema.snapshot.StateProof
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import derevo.cats.{eqv, show}
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** The two SIGNED MPT subtree roots for the `lastCurrencySnapshots` GSI field, carried in [[GlobalSnapshotStateProof]] as
  * `lastCurrencySnapshotsProof`. In gl0's MPT the currency-snapshots slot SPLITS per metagraph `Address` into TWO `metagraph`-namespaced
  * sub-keys — `LastIncrementalCurrencySnapshots` (`GlobalStateFieldId` 5, the `Signed[CurrencyIncrementalSnapshot]`) and
  * `LastCurrencySnapshotInfo` (`GlobalStateFieldId` 6, the `CurrencySnapshotInfo`) — so the field has TWO per-fieldId subtree roots, not
  * one. Both are computed by the SAME `GlobalStateConverter.fieldRootFromBytes` path that backs every other per-field root (byte-identical
  * producer-vs-follower), and are transitively also covered by the global `mptRoot`. Their purpose here is to give the cl1/dl1-consumed
  * `lastCurrencySnapshots` field a TRUE signed per-field anchor (symmetric with the five uniform-`Hash` consumed fields), so cl1/dl1
  * followers verify their recomputed roots against a SIGNED value rather than against the producer's served (unsigned) claimed map.
  *
  * This occupies the `lastCurrencySnapshotsProof` slot (field 4) on the V2 [[GlobalSnapshotStateProof]] — the same slot that held the
  * legacy pre-MPT `Option[MerkleRoot]` (a separate-Merkle-tree currency root) in [[GlobalSnapshotStateProofV1]], now repurposed for the
  * live MPT currency partition roots. V1 keeps the legacy `Option[MerkleRoot]` shape unchanged; the V1→V2 conversion drops it (no MPT
  * equivalent).
  */
@derive(encoder, decoder, eqv, show)
case class CurrencySnapshotMptRoots(
  incrementalRoot: Hash,
  infoRoot: Hash
)

object CurrencySnapshotMptRoots {
  def apply: ((Hash, Hash)) => CurrencySnapshotMptRoots = {
    case (x1, x2) => CurrencySnapshotMptRoots.apply(x1, x2)
  }
}

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
      // V1's legacy separate-Merkle-tree currency root has no MPT-partition representation; field 4 on V2 is now the SIGNED
      // currency MPT partition roots, so V1 → V2 drops it to `None` (same as every other V2-only field below).
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
      // V2's field 4 is now the SIGNED currency MPT partition roots, which have no legacy single-MerkleRoot representation, so the
      // V2 → V1 downgrade drops it to `None` (V1's `Option[MerkleRoot]` only ever carried the pre-MPT separate-tree root).
      None
    )
}

@derive(encoder, decoder, eqv, show)
case class GlobalSnapshotStateProof(
  lastStateChannelSnapshotHashesProof: Hash,
  lastTxRefsProof: Hash,
  balancesProof: Hash,
  /** SIGNED per-field MPT root(s) for the `lastCurrencySnapshots` GSI field (the cl1/dl1-consumed 6th follow field). `Some` iff the
    * authenticated MPT byte map contains a currency-snapshot partition entry; `None` on empty-state / legacy-V1 paths. Holds BOTH currency
    * MPT partition roots — `incrementalRoot` (`GlobalStateFieldId.LastIncrementalCurrencySnapshots`, fieldId 5) and `infoRoot`
    * (`GlobalStateFieldId.LastCurrencySnapshotInfo`, fieldId 6) — computed from the SAME byte map (and thus the same `fieldRootFromBytes`
    * path) as every other per-field root, so it is byte-identical producer-vs-follower and ALSO transitively covered by `mptRoot`. Unlike
    * `smtRoot`, this IS reproducible on the GSI-derived rebuild paths (it derives from `info.lastCurrencySnapshots` alone), so it is
    * INCLUDED in the `StateProofValidator` `===` (not excluded via `StateProofComparison`). It gives the cl1/dl1 `lastCurrencySnapshots`
    * follow-verify a TRUE signed anchor (recompute-vs-signed, symmetric with the five uniform-`Hash` fields) instead of the prior
    * recompute-vs-producer-claimed-map guard. This reuses the slot that held the legacy pre-MPT `Option[MerkleRoot]` (a
    * separate-Merkle-tree currency root, still present on [[GlobalSnapshotStateProofV1]]); on V2 it is now the live signed currency-field
    * root.
    */
  lastCurrencySnapshotsProof: Option[CurrencySnapshotMptRoots],
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
      Option[CurrencySnapshotMptRoots],
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
