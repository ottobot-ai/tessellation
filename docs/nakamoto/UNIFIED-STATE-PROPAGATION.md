# Unified State Propagation — Byte-Diff + Inclusion-Proof Adopt

**Status:** DRAFT — 2026-06-04
**Motivation:** the gl0 metagraph-currency-fold **freeze** blocking the 8gl0+4mg+4shard e2e (balance-assertion failure), plus the standing goal to stop re-executing state we can instead *adopt-and-verify*.

> **CORRECTION (2026-06-30):** the "doesn't yet commit the state behind the verified root" / "built, zero L1 callers / promote into use" status below is STALE. The checkpoint now carries the executed snapshots (`ShardDerivedStateDelta.includedSnapshots`) and gl0 adopts-and-verifies the per-MG state (not just the root); the inclusion-proof transport (`ShardSubtreeProofService` + routes) is BUILT AND WIRED, backed by PIN-1 component-addressable per-MG roots (`8ac7ce04f..0b7902d69`). The framework token model is RE-EXECUTED and enforced (`reExecRoot === stateProof`). See CURRENCY-APP-TOKEN-ENFORCEMENT.md + SHARDING-PRODUCTION-READINESS-PLAN.md.

---

## 1. Problem

Across the hierarchy, a node that needs state another layer already committed has two ways to get it:

- **Re-execute** — re-run the state transition (blocks → rewards → balances) and require byte-equality with the signed artifact.
- **Adopt-and-verify** — take the layer's *signed committed root*, get the state (as a typed delta or full snapshot), and verify the recomputed root equals the signed one (holder) or verify a Merkle inclusion proof against it (stateless).

Re-execution is **structurally fragile**: it diverges whenever the re-executor's prior state, global-sync view, reward implementation, data-application state, or message set differs from the producer's. The L1→gl0 follow path already moved off it (field-root equality, `DAGSnapshotProcessor` / `CurrencySnapshotProcessor`); the **metagraph→gl0 currency fold did not**, and it is currently the e2e blocker.

### 1.1 The live evidence (v12, 8gl0+4mg+4shards, senders OFF)

- gl0's `lastCurrencySnapshots` is **frozen at ordinal 0 for all 4 metagraphs** — even idle, post-test — while each metagraph's own ml0 reached ordinal 62 with correct balances. So the metagraphs are healthy; gl0's *view* of them never advances past genesis.
- `GlobalSnapshotStateChannelEventsProcessor.applyCurrencySnapshot` → `CurrencySnapshotContextFunctions.createContext` → `CurrencySnapshotValidator.validateRecreateContent` **re-creates** the whole currency artifact and requires `artifact === expected` (`SnapshotDifferentThanExpected` otherwise). 478 such failures on gl0-0 alone.
- Decisive: across every failing ordinal (expected = 2 … 36), the re-created `actual` is **always ordinal 1** ⇒ gl0's prior is **always genesis (ord 0)**. The fold **never advances** — a total freeze, not a partial lag.
- Meanwhile the SC-binary chain (`lastStateChannelSnapshotHashes`) *does* advance and the committee gate admits binaries (≈13k) — so binaries flow, but the **derived currency state never commits forward**.
- The global chain is healthy throughout (finalized ord ~124, lockstep, 2–4s finality). The freeze is isolated to the metagraph-currency derivation side-channel.

The freeze is almost certainly a **recent regression** (v23 / `d619c30a5` advanced metagraph ordinals fine through token-lock tests). Suspect set: the createContext-adopt (#24), cl1/dl1 slice-follow (#8), and committee-gate (#290) changes landed since.

**Root cause is under trace** (Slice 1). Candidates, all consistent with "prior always genesis":
1. gl0's genesis currency prior is represented as `Right((ord0, info))` instead of `Left(fullSnapshot)`, so the first incremental takes the re-execution path instead of the adopt path.
2. The per-ordinal acceptance window systematically excludes ord-1's binary, so ord-2+ fold against genesis.
3. The fold *succeeds* per window but its result is **not persisted** to the canonical `lastCurrencySnapshots` MPT field (each window restarts from genesis).

---

## 2. The unifying primitive

> **Adopt = verify a typed state delta against a signed committed root; never re-execute the producer's transition.**

Two composable concerns, one trust model:

| Concern | Mechanism | Notes |
|---|---|---|
| **Transport** | typed **byte-diff / delta** (removals + upserts since last tip) | how the state moves; cheap; already `ConsumedFieldDelta` (#287) on L1, `StateChangesAccumulator` (#14–19) for ml0 |
| **Trust (holder)** | **field-root equality** — recompute the field's MPT subtree root from local content, assert `== signed stateProof root` | for consumers that hold the field content |
| **Trust (stateless)** | **Merkle inclusion proof** against the signed committed root | for consumers that hold *no* content (cross-shard, light client, metakit TS verifier) |

The byte-diff is *transport*; the proof/root-equality is *trust*. They compose: ship the delta, verify the resulting root against the signed commitment — or, if you hold no content, carry an inclusion proof. **No layer re-runs the producer's reward/state computation.**

This is already proven on the L1→gl0 path and seeded by the `SyncedField` registry (`shared/.../follow/SyncedField.scala`, CI-enforced by `RegistryConsistencySuite`). The work is to **generalize it to every committed-state edge** and **delete the surviving re-execution sites**.

---

## 3. Current-state map

| Edge | Mechanism today | Re-exec? | Target |
|---|---|---|---|
| gl1 (dag-l1) → gl0 | field-root equality (5 uniform fields) + delta transport | no | unchanged ✓ |
| cl1/dl1 (currency-l1) → gl0 | field-root equality (5 + `lastCurrencySnapshots`) + delta transport | no | unchanged ✓ |
| cl1/dl1 → its **own** currency chain (cl0/ml0) | `createContext` **re-execute** | **yes** | adopt-against-signed-root |
| **metagraph (ml0) → gl0** currency fold | `createContext` **re-execute** (frozen) | **yes** | **adopt-against-signed-root (Slice 1/3)** |
| shard checkpoint → gl0 (`ShardCheckpointGl0AcceptanceManager.verifyEmbedded`) | per-MG root **byte-compare** vs committee-signed `perMetagraphMptRoots` (adopt, verbatim binaries) | no | extend to commit *state*, not just verify root |
| cross-shard / light client | inclusion proof (`FollowVerifyCore.verifyConsumedFields`, `ShardSubtreeProofService`) — **built, zero L1 callers** | no | promote into use |

**The two surviving re-execution sites are the same disease** and the metagraph→gl0 one is the live freeze. The shard checkpoint already *verifies* the per-MG root via the byte-diff/attestation path — ~~it just doesn't yet *commit the state behind that verified root*; it hands the binaries to the same re-executing `createContext`.~~ **[STALE (2026-06-30): the checkpoint now carries the executed snapshots (`ShardDerivedStateDelta.includedSnapshots`) and gl0 adopts-and-verifies the committed per-MG state via the `deriveAdoptedCurrencyInfo` re-derive (verified vs `stateProof`) at `numShards>1` — not the live-global `createContext`.]**

---

## 4. Slice plan

**Slice 1 — Unblock e2e: trace + fix the gl0 currency-fold freeze.** *(blocking; smallest surface)*
Trace the three candidates in §1.1 to the exact line. Fix is either a direct bug-fix (genesis representation / window assembly / persist) **or** the first adopt cutover (commit `lastCurrencySnapshots` from the already-verified signed snapshot instead of re-creating it). Acceptance: `lastCurrencySnapshots` advances past genesis for all metagraphs; the v12 balance assertion passes; full e2e green.

**Slice 2 — Generalize `SyncedField` to the currency/metagraph dimension.**
Extend the registry (or a sibling) to enumerate `lastCurrencySnapshots` (already a bespoke `currencySnapshotsCheck`) and the per-MG `perMetagraphMptRoots`, so adopt/verify is registry-driven and drift is a CI failure — same correct-by-construction guarantee the 5 uniform fields have.

**Slice 3 — Cut the two re-execution sites over to adopt-against-signed-root.**
metagraph→gl0 fold and cl1/dl1→own-chain: replace `createContext` re-creation with "derive state from the signed snapshot's accepted content (or adopt the committee-verified per-MG state), assert resulting root == signed stateProof / `perMetagraphMptRoots`." Delete the byte-equality recreation. This makes the Slice-1 freeze class **structurally impossible**.

**Slice 4 — Promote the stateless inclusion-proof path (mechanism c) into use.**
Wire `verifyConsumedFields` / `ShardSubtreeProofService` for the genuinely stateless consumers (cross-shard reads per [[cross-shard-message-passing-direction]], light client, metakit TS verifier). Decide the holder-vs-stateless boundary per consumer.

**Slice 5 — Unify the sidecar message + migrate the seam to HOCON.**
One typed `StateDeltaProof` envelope (delta + signed root + optional inclusion proof) on the appropriate topic; migrate the `SIDECAR_HOST`/`SIDECAR_GRPC_PORT` `sys.env.get` reads (`dag-l0 Services.scala`, `dag-l1 Main.scala`) to HOCON per the project rule.

---

## 5. Go sidecar integration

The sidecar (`p2p/`, libp2p + GossipSub, gRPC seam to the JVM on `:50051`/`:50053`) is **already the transport and already carries byte-diff payloads**: the shard checkpoint rides it as `ShardCheckpointWire → ShardDerivedStateDeltaWire{ bytes payload_json }` — `perMetagraphMptRoots` + included binaries as **opaque bytes** (the sidecar never inspects payloads). That is a byte-diff+root message already in flight.

Facts that shape the design:

- **Two transports coexist.** Sidecar GossipSub = the **gl0-to-gl0 mesh** (snapshots, binaries, attestations, blocks, shard checkpoints). L1/ml0 follow-slices are **HTTP pull** (`/global-follow` routes). cl0/ml0 run **no** sidecar — they dial a colocated gl0's sidecar to publish, but pull global state over HTTP. Slice 5 must decide: keep the split (push-gossip for the gl0 mesh, pull-HTTP for followers) with one payload schema, or converge.
- **Adding a typed message** = proto `oneof` arm + topic + Go publish/subscribe handler + Scala client method + `NakamotoSyncDaemon` dispatch branch (~6–8 touch points, a well-trodden template — every recent addition followed it). Or ride the generic opaque `Rumor` carrier for near-zero plumbing.
- The opaque-`bytes` payload convention (carry the serialized delta + proof; keep only routing/crypto fields typed) is the established idiom — so the wire surface is minimal; the real work is the JVM-side verify/apply handler.

---

## 6. Open decisions

1. **Does gl0 hold full metagraph state, or roots-only?**
   - *(i) full state* — gl0 keeps `CurrencySnapshotInfo`, adopts the verified delta, serves balances as today. Smaller; completes the existing design.
   - *(ii) roots-only* — gl0 stores only `perMetagraphMptRoots`; balances served by ml0 + inclusion proof. The "true" sharded end-state; larger, reshapes read APIs + clients.
2. **Transport unification** — one push-gossip path through the sidecar, or keep pull-HTTP follow-slices for followers with a shared payload schema (§5).

Slice 1 is independent of both and unblocks e2e; (i)/(ii) is the Slice-3/4 fork.

---

## 7. Related

[[project_cross_shard_message_passing_direction]] · [[project_ml0_diff_adopt_design]] · [[project_259_shard_checkpoint_fix_plan]] · [[reference_metakit_sdk_ts_inclusion_verifier]] · `GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md` · `SHARD-SORTITION-WORKSTREAM-PLAN.md`
