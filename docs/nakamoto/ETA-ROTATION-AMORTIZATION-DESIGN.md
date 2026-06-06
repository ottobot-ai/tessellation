# Eta-Rotation Amortization (#31)

**Status:** RFC / S0 — design for sign-off.
**Author:** OttoBot, 2026-06-05.
**Prereq (done):** #29 verify-on-attach (`a196c735e`) cleared the committee-gate CPU sink, which had masked this.

## 1. Problem — a deterministic scheduled event forks

The eta rotation is the single most predictable moment in the protocol: every node knows it
arrives at `ordinal % R == 0`. Yet at the boundary the cluster reliably forks. v22 e2e (8gl0+4mg+4shards)
proved it with the committee-gate already fixed: at the period-1→2 boundary, MptOverlay pending
branches went 10→16, finality stalled past depth-k, tips diverged (215↔256), nodes went silent under
sustained host load ~210, and **the forks never resolved** — torn down as unrecoverable.

A correct consensus must not fork on a deterministic schedule. The fork is an artifact of *how* the
rotation does its work, in two coupled defects:

### Defect A — O(epoch) synchronized compute spike (the verifier walk)
For snapshots near the boundary, the receiver recomputes the eta by **walking the whole prior period**:

```
// NakamotoSyncDaemon.scala:1042-1072  (verifier / sync path)
currentPeriod = rotationPeriod(snap.ordinal - 1, R)
eta = if (currentPeriod <= 0) genesisEta
      else chainStore.vrfOutputsForPeriodFrom(currentPeriod - 1, R, parentHash)   // ← O(R) walk
              .map(outs => EtaCalculation.computeEta(genesisEta, currentPeriod, outs.map(_._2)))
              .orElse( snapshot.embeddedEta )                                       // ← fork fallback
```

All 8 nodes do this O(R≈100) walk simultaneously at the boundary. On a contended host the walk stalls
each node unevenly → production/propagation desync → forks form → fork recovery (MptOverlay multi-branch
re-execution) needs CPU the walk is consuming → **death spiral**.

### Defect B — the read is bestTip-coupled, not finalized
`vrfOutputsForPeriodFrom(..., parentHash)` walks from the *received snapshot's parent* — i.e. a bestTip
that may be on a losing fork. When that parent chain isn't in the local store, the verifier can't
independently derive the eta and **falls back to the eta embedded in the snapshot** (the "Using embedded
eta (parent chain not in store)" log that floods during the storm). So under forks the eta is effectively
taken on trust, per-branch, instead of being a single deterministic value.

The producer path is *already* half-amortized — `EpochState.rotateEpoch` folds an in-memory
`vrfAccRef` accumulator. The fix generalizes that to a **durable, finalized-sourced, single-source-of-truth
accumulator** that both producer and verifier read in O(1).

## 2. Key enabling fact

`EpochState.computeNextEta` is a **streaming hash** — already an incremental fold by construction:

```scala
def computeNextEta(prevEta, epochNumber, vrfOutputs): Array[Byte] = {
  val d = SHA-256
  d.update(prevEta); d.update(int64(epochNumber))
  vrfOutputs.foreach(d.update)   // fold each VRF output in order
  d.digest()
}
```

So eta(N) = `SHA-256( eta(N-1) || N || vrf(o₀) || vrf(o₁) || … )` over the first ⅔ of period N-1's
snapshots, in ordinal order. We can `update()` per snapshot as it **finalizes** and `digest()` once at
the ⅔-mark. No new crypto, no re-architecture of the hash.

## 3. Design — `EtaAccumulator` (durable, finalized, O(1) read)

A single component, fed off the **finalize hook**, owning the eta for every period.

### 3.1 State
```
EtaAccumulator:
  frozen:      Map[Period, HistoricalStakeSnapshot]   // durable output = (stakes, eta) per period
  inProgress:  Option[InProgress]                     // the period currently folding (in-memory)
InProgress:
  period:      Long                      // = N-1, producing the snapshot period N consumes
  digest:      MessageDigest             // running SHA-256 for the eta, seeded eta(N-1) || N
  lastOrdinal: Long                      // last finalized ordinal folded (contiguity guard)
  twoThirdsAt: Long                      // freeze ordinal = periodStart(N-1) + floor(2R/3)
```
The frozen artifact is the whole `HistoricalStakeSnapshot(stakes, eta)`: the eta from the streaming fold,
and `stakes` read from the **finalized** state at `twoThirdsAt`. Both halves are captured at the same
finalized ordinal (§3.2 step 4) — never on adoption.

### 3.2 Fold trigger (on snapshot FINALIZE, ordinal O)
1. `O` belongs to period P. If `inProgress` is for an earlier period and `O` opens a new one, **finalize
   the prior `inProgress`** first (it must already be frozen — see invariant) and open the new one seeded
   with `frozen(P-1) || (P+1)`.
2. **Contiguity guard:** require `O == lastOrdinal + 1` (no gaps; finalize is monotone). Otherwise reseed
   from the finalized chain (restart/recovery path, §3.4).
3. If `O <= twoThirdsAt`: `digest.update(vrf(O)); lastOrdinal = O`.
4. If `O == twoThirdsAt`: `frozen(period) = (digest.digest(), stakeAt(O))` → **the next period's
   `HistoricalStakeSnapshot(stakes, eta)` is ready**. Persist (§3.3).

**The freeze fires on FINALIZATION, never adoption — and finalization can lag k1.** `O` is always a
*finalized* ordinal. We must never freeze on the 2/3 snapshot being merely *adopted* (a bestTip is
reorg-able and per-fork — using it would reintroduce the split this whole design removes). In a healthy
cluster optimistic/attestation finality is ~1 ord deep, so `twoThirdsAt` finalizes soon after it is
produced and there is ≈ R/3 ≈ k1 of margin. **But in the worst case — depth-k finality only, no optimistic
finalization — `twoThirdsAt` finalizes k1 ordinals AFTER it is produced:**

```
twoThirdsAt produced   = periodStart + (2/3)·R = periodStart + 2.067·k1
twoThirdsAt FINALIZED  = + k1 (worst case)     = periodStart + 3.067·k1  ← freeze happens HERE
epoch ends (boundary)  = periodStart + R       = periodStart + 3.1·k1
                                        margin = 0.033·k1  (≈34 ords / ~4 min @ prod k1=1024, 7s)
```

So the freeze can land *just before* the boundary, not ~k1 before. The design must tolerate a just-in-time
freeze and, if finality is degraded past k1, must **stall rather than fall back to an un-finalized value**
(§7).

### 3.3 Persistence (the durable side-file — chosen design)
- File: `<gl0 dataDir>/nakamoto/eta-accumulator.json` (a gl0 data-dir file, **not** the Go sidecar).
- Stores **`frozen: Map[Period, Eta]`** only — the small, immutable outputs (prune periods < currentPeriod − keepDepth).
- Written atomically (tmp+rename) on each freeze. Read once at startup.
- `inProgress` (the live `MessageDigest`) is **not** serialized — `MessageDigest` has no portable state.
  It is reconstructed on restart by §3.4.

### 3.4 Restart / recovery (the recompute fallback)
On startup: load `frozen` from the file. For the in-progress period, **recompute** by folding the
finalized chain's VRF outputs from `periodStart` up to the latest finalized ordinal (a one-time O(≤R) walk
— the only place the old walk survives, and only on the rare restart path). If the file is missing or a
frozen entry fails a cross-check against the finalized chain, recompute that period too. This is exactly
the "file for speed + recompute for correctness" hybrid: the file is the steady-state fast path, the chain
is always the ground truth.

### 3.5 The O(1) read (both call sites collapse to this)
- **Verifier** (`NakamotoSyncDaemon:1047`): replace the `vrfOutputsForPeriodFrom` walk + `computeEta` +
  embedded-eta fallback with `etaAccumulator.etaFor(currentPeriod)` → `frozen(currentPeriod-1)`. No walk,
  no bestTip, no per-branch trust.
- **Producer** (`EpochState.rotateEpoch`): read the same `frozen` value instead of folding `vrfAccRef` at
  the boundary. `vrfAccRef` and the in-memory boundary fold are removed.

## 4. Determinism contract (the safety bar — S3 test gate)
The eta is consensus-critical; a 1-byte divergence splits the cluster. The accumulator is deterministic
because every input is canonical:
1. It folds **finalized** snapshots only — all honest nodes share the identical finalized prefix.
2. In strict **ordinal order**, contiguous (the guard).
3. Via the existing byte-exact `computeNextEta` streaming hash.
4. Seeded from `frozen(P-1)` (itself deterministic, inductively from genesis).

**S3 must assert all N nodes produce byte-identical `frozen(P)` for every P** across a multi-node run —
this is the gate before cutover lands.

**Reconciliation item:** today the producer chains from `prevEta` (`computeNextEta`) while the verifier
folds from `genesisEta` (`EtaCalculation.computeEta`). S1 must confirm these already agree (or fix to the
prev-chained form) and make the accumulator the **single** definition, deleting the other path.

## 5. Slice plan
- **S0** — this RFC.
- **S1** — `EtaAccumulator` (in-memory): seed/fold/freeze off the finalize hook; reconcile the producer vs
  verifier eta definitions onto it; unit tests (incremental fold == batch `computeNextEta`; freeze at ⅔).
- **S2** — durable file (§3.3) + restart recompute (§3.4); round-trip + stale-file-recompute tests.
- **S3** — cutover: swap the two read sites (§3.5), delete `vrfAccRef` boundary fold + the verifier walk +
  embedded-eta fallback; **multi-node byte-identical-eta determinism test** (the gate).
- **S4** — e2e re-run. **Success = zero new MptOverlay branches at the boundary, finality never lags the
  tip through ≥3 rotations.**

(The historicalStakeSnapshots write is no longer a separate "measure/maybe-amortize" slice — moving its
freeze from the `ord%R==R-1` adoption-write onto the finalized ⅔-mark, paired with the eta, is part of the
core, S1+S3.)

## 6. Out of scope / non-blockers
- **SMT (historical-commitment):** already incremental (`HistoricalCommitmentSmtStore`, per-ordinal,
  `smtRoot(N)=SMT(i≤N-k)`). No work.
- **#22 participating set:** kept out — accumulator uses current `historicalStakeSnapshots`; integrate later.
- **Host capacity:** 43 JVMs / 24 cores oversubscription amplifies the spike but is not its cause; #31
  removes the spike, capacity is a separate (test-infra) lever.

## 7. Risks
- **Determinism** (§4) — the one that splits the cluster; gated by the S3 multi-node test.
- **Freeze-margin (the load-bearing risk).** `twoThirdsAt` must FINALIZE before the boundary consumes the
  eta. Worst case (depth-k finality, no optimistic) it finalizes at ~3.067·k1 vs the boundary at 3.1·k1 — a
  **~0.033·k1 margin** (≈34 ords / ~4 min @ prod k1=1024; was 0.01·k1 at the old R=3.03). If finality
  degrades **past k1**, the freeze lands *after* the boundary and `eta(N)` isn't ready when period N opens.
  The protocol then **STALLS the rotation (waits for the freeze) — it MUST NOT fall back to a bestTip/adopted
  value** (that per-fork eta is exactly the split this design removes). A liveness stall is the correct trade
  vs a safety fork, and it only triggers when finality is already broken (a separate failure). Removing the
  spike is self-reinforcing: it restores finality, which restores the margin. **R was widened to 3.1·k₁ (from
  3.03) specifically to grow this worst-case headroom.** Settled prod family: **k₁=1024, R=3.1·k₁=3174,
  k₂=100·k₁=102400** — a deliberately deep, slow, stable consensus epoch (≈6 h @ 7s snapshots).
- **First-rotation / genesis** — period ≤ 0 keeps `genesisEta` (unchanged).
