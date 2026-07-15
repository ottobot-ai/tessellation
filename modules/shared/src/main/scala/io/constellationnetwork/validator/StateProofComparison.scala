package io.constellationnetwork.validator

import cats.Eq
import cats.syntax.all._

import io.constellationnetwork.schema.snapshot.StateProof
import io.constellationnetwork.schema.{CurrencySnapshotMptRoots, GlobalSnapshotStateProof}
import io.constellationnetwork.security.hash.Hash

/** How [[StateProofValidator.validateProof]] decides a recomputed state proof matches the one carried in a signed snapshot. */
trait StateProofComparison[P <: StateProof] {

  /** True iff `recomputed` matches `claimed` for the purposes of state-proof validation. */
  def equivalent(recomputed: P, claimed: P): Boolean

  /** Human-readable per-field breakdown of where `recomputed` (the locally-rebuilt proof, labelled `c=`) differs from `claimed` (the value
    * the signed snapshot commits to, labelled `l=`). Empty when there is no field-level difference to report. Diagnostic only — it is
    * logged by [[StateProofValidator.validateProof]] on a mismatch so the offending field is NAMED instead of having to parse a positional
    * `Expected: … Found: …` dump. It is only ever consulted on the `!equivalent` branch.
    */
  def fieldDiffs(recomputed: P, claimed: P): List[String]
}

object StateProofComparison extends StateProofComparisonLowPriority {

  def apply[P <: StateProof](implicit ev: StateProofComparison[P]): StateProofComparison[P] = ev

  /** `GlobalSnapshotStateProof` comparison is exact structural equality. Historical commitment activation is dark, so honest snapshots
    * carry `smtRoot = None`; a supplied `Some` must not bypass proof validation.
    */
  implicit val globalSnapshotStateProof: StateProofComparison[GlobalSnapshotStateProof] =
    new StateProofComparison[GlobalSnapshotStateProof] {
      def equivalent(recomputed: GlobalSnapshotStateProof, claimed: GlobalSnapshotStateProof): Boolean =
        recomputed === claimed

      def fieldDiffs(recomputed: GlobalSnapshotStateProof, claimed: GlobalSnapshotStateProof): List[String] =
        globalSnapshotStateProofFieldDiffs(recomputed, claimed)
    }

  /** Per-field diff for [[GlobalSnapshotStateProof]] — names every diverging proof slot as `field(c=<computed>,l=<claimed>)` with `c` = the
    * locally-rebuilt root and `l` = the signed/committed root. Shared single source of truth for the catch-up validator
    * ([[StateProofValidator.validateProof]]) and the live-follower mismatch log (`GlobalSnapshotContextFunctions.perFieldRootDiffs`) so
    * both surfaces report identical field labels.
    */
  def globalSnapshotStateProofFieldDiffs(
    computed: GlobalSnapshotStateProof,
    claimed: GlobalSnapshotStateProof
  ): List[String] = {
    def shortHash(h: Hash): String = h.show.take(12)
    def shortCurrencyRoots(r: CurrencySnapshotMptRoots): String = r.show.take(12)
    def diffHash(label: String, a: Hash, b: Hash): Option[String] =
      if (a === b) None else Some(s"$label(c=${shortHash(a)},l=${shortHash(b)})")
    def diffOptHash(label: String, a: Option[Hash], b: Option[Hash]): Option[String] =
      if (a === b) None else Some(s"$label(c=${a.map(shortHash).getOrElse("none")},l=${b.map(shortHash).getOrElse("none")})")
    def diffOptCurrencyRoots(label: String, a: Option[CurrencySnapshotMptRoots], b: Option[CurrencySnapshotMptRoots]): Option[String] =
      if (a === b) None
      else Some(s"$label(c=${a.map(shortCurrencyRoots).getOrElse("none")},l=${b.map(shortCurrencyRoots).getOrElse("none")})")
    List(
      diffHash("lastStateChannelSnapshotHashes", computed.lastStateChannelSnapshotHashesProof, claimed.lastStateChannelSnapshotHashesProof),
      diffHash("lastTxRefs", computed.lastTxRefsProof, claimed.lastTxRefsProof),
      diffHash("balances", computed.balancesProof, claimed.balancesProof),
      diffOptCurrencyRoots("lastCurrencySnapshots", computed.lastCurrencySnapshotsProof, claimed.lastCurrencySnapshotsProof),
      diffOptHash("activeAllowSpends", computed.activeAllowSpends, claimed.activeAllowSpends),
      diffOptHash("activeTokenLocks", computed.activeTokenLocks, claimed.activeTokenLocks),
      diffOptHash("tokenLockBalances", computed.tokenLockBalances, claimed.tokenLockBalances),
      diffOptHash("lastAllowSpendRefs", computed.lastAllowSpendRefs, claimed.lastAllowSpendRefs),
      diffOptHash("lastTokenLockRefs", computed.lastTokenLockRefs, claimed.lastTokenLockRefs),
      diffOptHash("updateNodeParameters", computed.updateNodeParameters, claimed.updateNodeParameters),
      diffOptHash("activeDelegatedStakes", computed.activeDelegatedStakes, claimed.activeDelegatedStakes),
      diffOptHash("delegatedStakesWithdrawals", computed.delegatedStakesWithdrawals, claimed.delegatedStakesWithdrawals),
      diffOptHash("activeNodeCollaterals", computed.activeNodeCollaterals, claimed.activeNodeCollaterals),
      diffOptHash("nodeCollateralWithdrawals", computed.nodeCollateralWithdrawals, claimed.nodeCollateralWithdrawals),
      diffOptHash("priceState", computed.priceState, claimed.priceState),
      diffOptHash("lastGlobalSnapshotsWithCurrency", computed.lastGlobalSnapshotsWithCurrency, claimed.lastGlobalSnapshotsWithCurrency),
      diffOptHash("mptRoot", computed.mptRoot, claimed.mptRoot),
      diffOptHash("historicalStakeSnapshots", computed.historicalStakeSnapshots, claimed.historicalStakeSnapshots),
      diffOptHash("smtRoot", computed.smtRoot, claimed.smtRoot)
    ).flatten
  }
}

private[validator] trait StateProofComparisonLowPriority {

  /** Default for any state-proof type: plain structural equality. */
  implicit def fromEq[P <: StateProof: Eq]: StateProofComparison[P] =
    new StateProofComparison[P] {
      def equivalent(recomputed: P, claimed: P): Boolean = recomputed === claimed

      def fieldDiffs(recomputed: P, claimed: P): List[String] =
        if (recomputed === claimed) Nil
        else List("<proof differs; no per-field breakdown for this proof type>")
    }
}
