# Currency-App Token Enforcement — the L2 layer model (CANONICAL)

**Status:** load-bearing direction (user, 2026-06-30). Supersedes any framing that treats the
authoritative-balance push as the primary adoption path for currency-app token state.
**One-line:** *a state channel is an arbitrary binary we adopt on trust; a currency app's token model is
framework-defined and must be RE-EXECUTED and enforced by the hypergraph. Different objects, different
guarantees.*

---

## 1. Two objects, two guarantees (do not conflate)

| Object | What it is | gl0's guarantee |
|---|---|---|
| **State channel** | An arbitrary signed binary (`StateChannelSnapshotBinary.content: Array[Byte]`). | **Adopt AUTHORITATIVELY** on the signature (I-AUTH). gl0 never re-derives it. Correct — gl0 cannot run arbitrary code. |
| **Currency application** | The *only* state-channel type we run today. Its `content` brotli-JSON-decodes to a `Signed[CurrencyIncrementalSnapshot]`. Data-app metagraphs are currency apps **+** a `dataApplication` part. | **Split** (below). |

A **currency application** has two sub-classes with *different* guarantees:

| Sub-class | Examples | Guarantee | Mechanism |
|---|---|---|---|
| **Framework token model** | transfer, fee, allow-spend, token-lock, balance, supply, and the cumulative ref-maps | **HYPERGRAPH-ENFORCED — RE-EXECUTED** | the framework defines these identically for every metagraph, so gl0 re-executes the token transitions and enforces `reExecRoot === snapshot.stateProof` |
| **Data-app custom state** | `dataApplication` (`onChainState`, `calculatedStateProof`, app records) | **METAGRAPH-AUTHORITATIVE (labeled)** | arbitrary metagraph code; gl0 can't run it → adopt the metagraph-signed value, labeled as metagraph-sourced |

**The boundary is "framework-defined vs metagraph-defined," not "economic vs data."** Token primitives are
framework-defined even inside a data app, so they are re-executed; only the genuinely-custom data-app state
is authoritative.

---

## 2. Dispatch — "is this a currency app?" = "does it decode?"

The binary content is brotli-compressed JSON (`JsonBrotliBinarySerializer`). The dispatch is the decode itself:

```
JsonSerializer[F].deserialize[Signed[CurrencyIncrementalSnapshot]](binary.content).toOption
  Some(snap) ⇒ CURRENCY APP  → re-exec the token model from snap.blocks; adopt snap.dataApplication authoritatively
  None       ⇒ STATE CHANNEL → adopt binary.content authoritatively (trust the I-AUTH signature)
```

This already exists: `GlobalSnapshotStateChannelEventsProcessor.deserialize` (`:199-200`, used at `:214`)
returns `None` on undecodable content. We formalize that `Option` as the branch point. (A cheap header/magic
peek can gate the full decompress, but a currency app is decoded anyway to re-exec, so the decode is not waste.)

---

## 3. Adoption roles

- **Shard committee** (VRF-sortitioned, per metagraph) — the **SOLE re-executor**. Re-executes the currency
  token transitions from the metagraph's blocks at the **finalized base**, with global/cross-shard reads
  **pinned** to the carried `globalSyncView` (deterministic, no live reads, no stall). Emits the byte-diff +
  per-MG root + the `Signed[CurrencyIncrementalSnapshot]` it executed (data availability).
- **Non-committee gl0** — **adopt-from-signed-committee**: apply the byte-diff onto the **same finalized base**
  and check `resultRoot === snapshot.stateProof`. Cheap and synchronous. Catches a committee that ships a diff
  inconsistent with the metagraph's signed proof. *(This is the "standard, not revolutionary" check: the node
  has the stateProof inside the currency incremental snapshot and verifies the MPT root from the same base.)*
- **Watchtowers** — **VRF-sampled, per metagraph, rotating INTRA-EPOCH** (unlike the committee, which is
  predictable-within-epoch by design). Each independently RE-EXECUTES its assigned metagraph from the
  committee-shipped snapshot and disputes (`FraudProofEnvelope`) if `reExecRoot ≠ stateProof`. Catches a
  committee that rubber-stamps a lying metagraph. **Challenge window = k1** (mainnet k1=1024 ≈ 2 h @ 7 s/ord;
  testnet 256 ≈ 30 min; dev 32 ≈ 4 min). Per-metagraph sampling keeps any single node from re-executing the
  whole shard; intra-epoch rotation keeps watcher identity unpredictable to a corrupt committee
  (Polkadot approval-checking).

Trust chain: committee re-exec catches a lying **metagraph**; non-committee root-check catches a committee
that lies about the **diff** (synchronous); watchtower re-exec catches a committee that **rubber-stamps**
(async, within k1). I-AUTH (env-dependent operator-majority signature) authenticates the metagraph source.

---

## 4. The deviation to REMOVE (and why it exists)

Today (`numShards > 1`, `AdoptFromSignedFields` path) the token state is **NOT** enforced by re-execution. At
`GlobalSnapshotAcceptanceManager:1064-1073` gl0 **unconditionally overrides** the re-executed token fields
(balances, active allow-spends/token-locks, the 5 ref-maps) with the metagraph's pushed `authoritative*` maps,
and only checks that the pushed maps **hash to** the signed `stateProof` (`:1113-1144`). That is a
*consistency* check (pushed === signed), not an independent re-derivation → **the token model becomes
trusted-to-the-metagraph.** This is the bug.

**The re-execution already exists and is substantially complete.** `deriveAdoptedCurrencyInfo`
(`GlobalSnapshotStateChannelEventsProcessor.scala`, `AdoptFromSignedFields`) replays the accepted events to
reconstruct balances, `lastTxRefs` / `lastTokenLockRefs` / `lastAllowSpendRefs`, `activeTokenLocks`,
`activeAllowSpends`, token-unlocks, and messages, and **verifies the derived root against the committed
`stateProof`** (`:973-975`). Its output is then discarded by the override above.

**Why the override was added (two walls, neither solved):**
1. **Base lag.** To apply the diff / replay correctly, gl0 must start from the **same finalized base** the
   committee diffed against. gl0's per-MG mirror base `S(N)` lags at multi-mg / cross-shard; applying the delta
   onto the stale base reconstructs the wrong value for any non-empty field (e.g. balances) → root mismatch →
   the MG freezes (`GSAM:1003-1014`, `:1041-1055`). The override is base-independent, so it sidesteps the lag.
2. **Global-lookup stall.** The complete re-exec (`Recreate` / `createContext`) does **live** global-snapshot
   lookups, which stalled on the sharded path (31 s retries against genesis). `AdoptFromSignedFields` avoids
   them by not reading global state at all.

**The fix is base/mirror consistency + pinned reads, not new machinery:** make gl0 re-execute from the
committee-matched **finalized** base with global reads **pinned** to the carried `globalSyncView`, make
`reExecRoot === stateProof` the **primary** adoption gate, and **remove/demote the authoritative override**.
The authoritative path stays only for (a) arbitrary non-currency state channels and (b) the data-app sub-state.

---

## 5. NOT this

- ❌ The `authoritative*` push as the **primary** adoption path for the currency token model.
- ❌ gl0 **recreating the binary** or re-validating block admissibility. We *trust the binary* (the blocks are
  accepted); we re-execute only the **currency portions** (the framework token effects of those blocks).
- ❌ Treating a currency app's token state as "metagraph-authoritative." Only its `dataApplication` is.

---

## 6. Status (what to build for production-readiness)

| Piece | State |
|---|---|
| Re-exec machinery (`deriveAdoptedCurrencyInfo` + verify vs `stateProof`) | EXISTS — but **overridden** |
| Authoritative override removal + base/mirror sync so re-exec is primary | **TO BUILD** (the core task) |
| Committee = real re-exec at finalized base, pinned reads | partial (uses `AdoptFromSignedFields` + override) |
| Pinned global reads in re-exec (no live lookup, no stall) | R1/I-PIN partly done; complete it |
| Watchtower = VRF-sampled per-metagraph, intra-epoch rotation | **TO BUILD** (today every node re-execs every MG, unsampled) |
| Challenge-window gating: effects not spendable until depth-k1 + dispute→**revert** path (not just slash) | **TO BUILD** (currently punitive-only, no revert) |
| Value-at-risk cap | **TO BUILD** |
| I-AUTH env-dependent operator-majority signature | **TO BUILD** (intended; today ≥1 sig) |

Greenfield rule (user): build the full re-exec model **before** the next sharded e2e — no more deferring.
