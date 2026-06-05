# Roots-Only Sharding — Light-Client-Verifiable Metagraph State (Option ii)

**Status:** DRAFT — 2026-06-04
**Decision:** chosen over Option (i) (gl0-holds-full-state). Rationale below.
**Supersedes:** the Slice-1 framing in [`UNIFIED-STATE-PROPAGATION.md`](./UNIFIED-STATE-PROPAGATION.md) — this is the concrete realization of that doc's (ii) branch.

---

## 0. Why (ii), and why now

The 8gl0+4mg+4shard e2e is blocked by a **gl0 currency-fold freeze** (`lastCurrencySnapshots` stuck at genesis for all metagraphs). Proven root cause: at `numShards>1`, sharded metagraphs are excluded from the chain-link path and reach gl0 **only** via committee-adopt, which hands gl0 a **shard-tip-relative delta window** that it folds against **gl0's own full-state prior** via `createContext`. The two tips diverge at the first checkpoint and never reconcile; the genesis→ord-1 bridge binary is orphan-buffered (78k events, 0 drains). A node-shared-only fix can't synthesize the missing binary (split-safety forbids node-local reconstruction).

**The freeze is intrinsic to "gl0 reconstructs full per-MG currency state by folding deltas."** Option (ii) deletes that reconstruction: gl0 holds **roots only**; metagraph balances are read from the metagraph's own ml0 plus an **inclusion proof** verified against gl0's committed root. No fold, no `createContext`, no genesis-bridge, no shard-tip-vs-gl0-tip divergence. The blocker becomes structurally impossible, and we land the architecture we've been driving toward ([[project_cross_shard_message_passing_direction]]).

---

## 1. The model: two-layer proof (mirrors DED)

A light client proving "address `A` has balance `B` in metagraph `M` as of global ordinal `N`":

```
Layer 1 — balance ∈ ml0's balance tree
   ml0 serves { balance B, smtProof, balanceRoot R_M, ordinal }
   client verifies:  verifyProof(smtProof against R_M)  ⇒  "A=B is in M's committed balance tree R_M"

Layer 2 — R_M ∈ gl0's committed metagraph commitment
   gl0 serves { metagraphProof, globalRoot, snapshot signature }
   client verifies:  verifyProof(metagraphProof against globalRoot)  ⇒  "M's root R_M is in gl0 snapshot N"
                  +  committee signature over snapshot N (metakit ECDSA verifier)

Together: "A=B in M, and M's state is committed on the Constellation DAG at ordinal N."
```

This is **exactly the DED audit model** (`ded-smt-service` README §8: "SMT proof" + "DED proof"), so the client-side TS already exists in pattern and largely in code.

---

## 2. The load-bearing constraint: cross-language hash determinism

A TS client verifying a Scala-produced root requires **byte-identical** leaf/node hashing. Today they are incompatible:

| | leaf hash | node hash | key→path |
|---|---|---|---|
| **Tessellation** (`Hasher.forJson` / MPT / `smtRoot` SMT) | `SHA-256(0x00 ++ Brotli(circeJSON(commitment)))` | `SHA-256(0x01 ++ Brotli(circeJSON({l,r})))` | one-way `SHA-256(Brotli(JSON(addr)))` |
| **`@zk-kit/smt`** (the importable verifier) | `SHA-256(ascii(key+value+"1"))` | `SHA-256(ascii(String(l)+String(r)))` | hex→bits, pad 256, **reverse** |

The **Brotli-compression-inside-the-preimage** is the killer — no off-the-shelf TS verifier reproduces it, and reproducing circe's printer + Brotli byte-for-byte in TS is brittle.

**Decision: introduce a purpose-built, `@zk-kit/smt`-byte-compatible "Light-Client SMT" (LC-SMT) for the commitments light clients verify — parallel to the core MPT/state-proof stack, which is unchanged.** The TS verifier is then `@zk-kit/smt`'s `verifyProof` imported with **zero custom code** (lift DED's 4-line `sha256Hash` config). This is greenfield (no wire-compat obligation, [[feedback_greenfield_no_wire_compat]]); the existing `smtRoot` (a hand-rolled SMT) is precedent for a parallel SMT commitment.

LC-SMT spec (must match `@zk-kit/smt` exactly — KAT-tested):
- hash = **plain SHA-256**, hex output; **no Brotli, no domain prefix**
- node = `SHA-256(utf8(hexL + hexR))`; leaf = `SHA-256(utf8(key + value + "1"))`
- key = `SHA-256(addressBytes)` hex (64 chars); value = balance as fixed-width hex
- path = `hexToBin(key)`, left-pad 256, **reverse** (LSB-first); zero node = `"0"`
- absence proofs supported (zero-node + matching-leaf), per `@zk-kit/smt`

> The existing `SparseMerkleTree[F]` (`shared/.../security/smt/`) is the structural starting point, but its hashing (`SHA-256(prefix ++ Brotli(JSON))`) must be swapped to the LC-SMT convention. Cleanest as a sibling `LightClientSmt` sharing the tree logic but not the `Hasher` seam.

---

## 3. Component changes

### 3.1 gl0 — roots-only
- **Delete the currency-delta fold for sharded MGs.** Stop calling `createContext`/`applyCurrencySnapshot` to reconstruct `CurrencySnapshotInfo` from adopted binaries (`GlobalSnapshotStateChannelEventsProcessor`). This removes the freeze.
- **Commit per-MG roots.** The committee-attested shard checkpoint already carries + byte-verifies a per-MG commitment (`perMetagraphMptRoots`, via `verifyEmbedded`). Redefine the per-MG committed value as the MG's **`balanceRoot R_M`** (the LC-SMT root the metagraph signs into its `CurrencySnapshotStateProof`) rather than an opaque `hash((address,fullState))`. gl0 commits the set `{M → R_M}` into a gl0-level LC-SMT → **`metagraphBalancesRoot`**, anchored in `GlobalSnapshotStateProof` (new field, parallel to `smtRoot`).
- **Keep a thin per-MG config summary, not full balances.** The genuine cross-MG consensus dependencies — fee addresses (`getFeeAddresses`, `MessageValidationOpsManager`) and SC-binary validation (`StateChannelValidator`) — need per-MG *config* (owner/staking/fee addresses, latest accepted ordinal), **not** the balance map. Carry a compact `MetagraphConfigSummary` per MG instead of `lastCurrencySnapshots` full state.
- **gl0 no longer stores per-MG balances in the MPT** (`AcceptanceMptStateChanges` fieldIds 5/6 shrink to the root + config summary).

### 3.2 ml0 — balance + proof serving (NET-NEW; this is the bulk of the work)
ml0 today has a balance endpoint but **no provable tree** (`CurrencySnapshotInfo.balancesProof = balances.hash`, flat; zero MPT/overlay/prover infra on the metagraph side). Add:
- **`LightClientSmt` over balances** — maintained in the currency accept path (`CurrencySnapshotAcceptanceManager`), keyed `SHA-256(address)`, value = balance hex. Recompute/update incrementally per accepted snapshot.
- **Anchor `balanceRoot R_M`** in the signed `CurrencySnapshotStateProof` (new field; currency-wire change, greenfield-OK). This is what gl0 commits and the client trusts.
- **New route** `GET /currency/{address}/balance/proof` → `{ balance, smtProof, balanceRoot, ordinal, snapshotSignature }`. Extend `AddressService`/`WalletRoutes`.

### 3.3 Light client (TS) — import, don't write
- **Import `@zk-kit/smt`'s `verifyProof`** standalone (`new SMT(sha256Hash, false).verifyProof(proof)`; zero runtime deps; lift DED's `sha256Hash`). Verifies BOTH layers (ml0 balance proof + gl0 metagraph proof) since both are LC-SMT.
- **Reuse the DED two-layer wrapper** (`ded-smt-service` is the working reference) + the **metakit ECDSA `verify`** for the snapshot/committee signature (Layer 2's "signed by committee" half).
- Ship as a small `@tessellation/light-client` TS package (or fold into the e2e harness first).

### 3.4 Reshape surface — the 12 `lastCurrencySnapshots` readers
| Class | Sites | Action |
|---|---|---|
| gl0 producer (builds the roots) | GSAM, GSCEP | **keep** — builds `{M→R_M}` instead of full map |
| own-entry / ordinal-only | StateChannel, BinaryTracker, Rollback, MetagraphParentOrdinalResolver, CurrencySnapshotAcceptanceManager | **keep** — satisfied by a "latest accepted ordinal per MG" summary |
| cl1/dl1 genesis bootstrap (own entry) | `CurrencySnapshotProcessor:358` | **thin** — fetch own genesis from own ml0 + verify vs gl0 root |
| gl0 serves full map to followers | `GlobalFollowSliceService:131` | **replace** — serve `{M→R_M}` + config summary, not full state |
| cross-MG consensus | `getFeeAddresses` (#7), `StateChannelValidator` (#8) | **keep via config summary** (not balances) |
| gl0 MPT storage of full per-MG state | `AcceptanceMptStateChanges` | **shrink** to root + summary |

No gl0 HTTP balance route reads currency state (currency balances are already ml0-served), so client-facing read APIs are mostly unaffected except the new proof endpoint.

---

## 4. Slice plan

**Slice 0 — LC-SMT + cross-language KAT.** Build `LightClientSmt` in Scala (byte-match `@zk-kit/smt`). Golden test: Scala emits root+proof for a fixed key/value set; a TS harness importing `@zk-kit/smt` verifies them; assert byte-identical roots + accepted proofs. *This de-risks everything; do it first.*

**Slice 1 — ml0 balance tree + proof route.** Maintain `LightClientSmt` over balances; anchor `balanceRoot` in `CurrencySnapshotStateProof`; serve `/currency/{address}/balance/proof`. Unit + KAT.

**Slice 2 — gl0 roots-only (the freeze fix).** Delete the currency-delta fold for sharded MGs; commit `{M→R_M}` (gl0 LC-SMT → `metagraphBalancesRoot` in the state proof); add `MetagraphConfigSummary`. Preserve `numShards=1` behavior + split-safety (the producer-embeds/follower-re-adopts contract). **This greens the e2e.**

**Slice 3 — light-client e2e.** Harness imports `@zk-kit/smt`, fetches `{balance, smtProof, balanceRoot}` from ml0 + `{metagraphProof, globalRoot}` from gl0, verifies both layers + the committee signature against a real running cluster. Add as a `just test` assertion.

**Slice 4 — reshape the readers.** Genesis bootstrap → proof-verified fetch; `getFeeAddresses`/`StateChannelValidator` → config summary; shrink the MPT partitions.

**Slice 5 — wire/replace the dark infra.** `ShardSubtreeProofService` + `ShardProofRoutes` are built but **unwired** (not in `HttpApi`) and internally inconsistent (verify against an opaque leaf hash, not a tree root). Either retarget them to the LC-SMT model or delete and replace with the Slice-1/2 routes.

---

## 5. Open decisions

1. **gl0 metagraph-commitment tree:** commit `{M→R_M}` in a **gl0 LC-SMT** (clean, TS-verifiable Layer 2, recommended) vs keep the existing binary `MerkleTree` (Brotli) and reproduce it in TS (brittle). → recommend LC-SMT for both layers so the client uses one verifier.
2. **Cross-MG fee/config dependency (#7,#8):** `MetagraphConfigSummary` carried in gl0 state (simple, recommended) vs proof-carrying fee config per validation. → start with the summary.
3. **Absence proofs:** LC-SMT gives them free (`@zk-kit/smt`). Use for "address has no balance" / non-existence queries the flat-hash model can't answer.
4. **Reuse `SparseMerkleTree[F]` vs new `LightClientSmt`:** new sibling (different hash seam) — the existing one is Brotli-bound.

---

## 6. Risks

- **Cross-language byte-determinism** — the whole edifice rests on it. Mitigate with mandatory KAT golden tests (Slice 0) before any consensus change. The single easiest mistake: `@zk-kit/smt` hashes the **ASCII concatenation of hex strings**, not raw 32-byte buffers, and appends the literal `"1"` entry-mark.
- **Split-safety in Slice 2** — gl0's roots-only commitment must remain a deterministic function of `(wire-carried checkpoint, prior)`; no node-local shard-store reads (the contract the freeze investigation flagged).
- **`numShards=1` regression bar** — the production default path must stay byte-identical.
- **Cross-MG consensus deps** — confirm the config summary fully covers `getFeeAddresses` + `StateChannelValidator`; if a validation genuinely needs another MG's *balance*, it must arrive as a proof-carrying input ([[project_cross_shard_message_passing_direction]]), not a gl0 full-state read.

---

## 7. What we import vs build

| Piece | Source |
|---|---|
| TS inclusion verifier | **import** `@zk-kit/smt` (zero-dep) + DED's `sha256Hash` 4-liner; DED `ded-smt-service` is the working two-layer reference |
| TS signature verifier (committee attestation) | **import** `metakit-sdk` `verify.ts` (ECDSA, RFC-8785) |
| Scala LC-SMT | **build** (sibling of `SparseMerkleTree[F]`, `@zk-kit/smt`-byte-compatible) |
| ml0 balance tree + proof route | **build** (net-new metagraph-side prover) |
| gl0 roots-only commit + config summary | **build** (delete fold; commit `{M→R_M}`) |
| gl0→MG "root ∈ gl0" proof | existing `MerkleTree.findPath` (Layer-2 today) → migrate to gl0 LC-SMT |

---

## 8. Related
[[project_cross_shard_message_passing_direction]] · [[project_ml0_diff_adopt_design]] · [[reference_metakit_sdk_ts_inclusion_verifier]] (correct the note: metakit is ECDSA-only) · [[reference_ded_smt_digital_evidence]] · `UNIFIED-STATE-PROPAGATION.md` · DED `~/repos/ded-smt-service` · `@zk-kit/smt`
