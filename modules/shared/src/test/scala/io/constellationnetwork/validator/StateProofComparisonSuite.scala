package io.constellationnetwork.validator

import io.constellationnetwork.schema.{CurrencySnapshotMptRoots, GlobalSnapshotStateProof}
import io.constellationnetwork.security.hash.Hash

import weaver.SimpleIOSuite

/** Locks in the per-field state-proof diff that [[StateProofValidator.validateProof]] logs on a mismatch (and that the live-follower
  * `GlobalSnapshotContextFunctions.perFieldRootDiffs` delegates to). The contract: name the diverging field as
  * `field(c=<rebuilt>,l=<committed>)` — `c` = the proof THIS node recomputed, `l` = the value the signed snapshot commits to — so a
  * `StateProof Broken` line is read directly instead of positionally diffing two 19-field proof dumps (the exact pain the catch-up wedge
  * investigation hit).
  */
object StateProofComparisonSuite extends SimpleIOSuite {

  private def h(s: String): Hash = Hash(s.padTo(64, '0').take(64))

  private val cmp = StateProofComparison[GlobalSnapshotStateProof]

  private val base = GlobalSnapshotStateProof(
    lastStateChannelSnapshotHashesProof = h("aa"),
    lastTxRefsProof = h("bb"),
    balancesProof = h("cc"),
    lastCurrencySnapshotsProof = None,
    activeAllowSpends = None,
    activeTokenLocks = None,
    tokenLockBalances = None,
    lastAllowSpendRefs = None,
    lastTokenLockRefs = None,
    updateNodeParameters = None,
    activeDelegatedStakes = None,
    delegatedStakesWithdrawals = None,
    activeNodeCollaterals = None,
    nodeCollateralWithdrawals = None,
    priceState = None,
    lastGlobalSnapshotsWithCurrency = None,
    mptRoot = Some(h("d0")),
    historicalStakeSnapshots = None,
    smtRoot = None
  )

  pureTest("identical proofs → no field diffs and equivalent") {
    expect(cmp.fieldDiffs(base, base).isEmpty).and(expect(cmp.equivalent(base, base)))
  }

  pureTest("historicalStakeSnapshots divergence is named, with c=/l= values, and breaks equivalence") {
    val rebuilt = base.copy(historicalStakeSnapshots = Some(h("11"))) // c
    val committed = base.copy(historicalStakeSnapshots = Some(h("22"))) // l
    val diffs = cmp.fieldDiffs(rebuilt, committed)
    expect(diffs.size == 1)
      .and(expect(diffs.head.startsWith("historicalStakeSnapshots(c=")))
      .and(expect(diffs.head.contains("l=")))
      .and(expect(!cmp.equivalent(rebuilt, committed)))
  }

  pureTest("currency infoRoot divergence is named via lastCurrencySnapshots") {
    val rebuilt = base.copy(lastCurrencySnapshotsProof = Some(CurrencySnapshotMptRoots(h("55"), h("66"))))
    val committed = base.copy(lastCurrencySnapshotsProof = Some(CurrencySnapshotMptRoots(h("55"), h("77"))))
    val diffs = cmp.fieldDiffs(rebuilt, committed)
    expect(diffs.size == 1).and(expect(diffs.head.startsWith("lastCurrencySnapshots(c=")))
  }

  pureTest("multiple diverging fields are all named") {
    val rebuilt = base.copy(balancesProof = h("99"), mptRoot = Some(h("e0")))
    val diffs = cmp.fieldDiffs(rebuilt, base)
    expect(diffs.exists(_.startsWith("balances(c="))).and(expect(diffs.exists(_.startsWith("mptRoot(c=")))).and(expect(diffs.size == 2))
  }

  pureTest("smtRoot-only difference: surfaced by fieldDiffs (diagnostic) but ignored by equivalent") {
    val a = base.copy(smtRoot = Some(h("33")))
    val b = base.copy(smtRoot = None)
    // `equivalent` normalizes smtRoot away on both sides, so an smtRoot-only delta is still equivalent — yet the diagnostic still shows it.
    expect(cmp.equivalent(a, b)).and(expect(cmp.fieldDiffs(a, b).exists(_.startsWith("smtRoot(c="))))
  }
}
