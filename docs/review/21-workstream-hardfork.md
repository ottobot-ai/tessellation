# Workstream: "The Hardfork" — grounded state for Fable review

> **HISTORICAL PRE-FIX REVIEW, NOT A RETENTION REQUIREMENT.** This file records
> the 2026-07-07 tree and may describe inherited or fork-only migration machinery
> that is no longer present. This repository is a greenfield fork of upstream
> Tessellation v4.0.0; abandoned fork-only schemas and paths have never been
> deployed and must not be kept merely because this review mentions them. Verify
> current behavior in source and follow [`../../AGENTS.md`](../../AGENTS.md).
> Deleted authority-design paths cited below are intentionally absent and remain
> available only through git history at commit `725b25b`.
>
> **Current migration target (owner clarification 2026-07-11):** a future fork of
> an existing network is an offline, deterministic transform from one exact
> finalized upstream snapshot to a new-network ScodecV1 ordinal-0 genesis. The
> exporter/importer and migration manifest must audit supply and every retained or
> discarded state namespace. This does not justify legacy runtime codecs or any
> `authoritative*`/roots-only state import path. Future upgrades after the new
> network launches use the finalized hash/ordinal-bound `ProtocolEra` mechanism.

> **Purpose.** Ground the *hardfork* workstream so a Fable review can be pointed at it without
> re-deriving from stale memory. Sibling of `docs/review/HANDOFF.md` (Track-1/Track-3 sharding).
> One of several parallel workstreams (others: execution sharding, serde migration).
> **Every claim here is a `file:line` / commit / doc citation, or is tagged `UNVERIFIED`.**
> **Verified against the working tree 2026-07-07**, branch `feature/committee-state-diff`,
> HEAD `21933559c`. Where a fact came only from a memory note, it was re-checked against source;
> memory notes are treated as STALE until confirmed.

---

## 0. TL;DR — what "the hardfork" actually is in THIS project

There is **no single artifact named "the hardfork."** The word maps to **two distinct things**,
and conflating them is the first way to lose the thread:

1. **Inherited-mainnet era machinery** (`Era`, `EraCodecRegistry`, `HashSelect`,
   `StateProofSelector`, `last-kryo-hash-ordinal`, `last-legacy-state-proof-ordinal`,
   `fields-added-ordinals`). This is a **classic activation-ordinal** apparatus for *reading old
   chain state* (Kryo→JSON→scodec, tess3 schema migrations, legacy vs MPT state-proof format). It
   is **real and partly wired**, but on the Nakamoto/greenfield chain its boundaries are all
   `dev = 0` — i.e. "everything is the latest era from genesis." It exists as *pattern + legacy
   read-path*, not as an active fork on this chain.

2. **The sharding economic-trust cutover** — the item prior notes call **"A4 = remove gl0 currency
   re-validation (HARD FORK)"** and the design docs call the **roots-only fold deletion**. This is
   the actual backward-incompatible change the team means by "getting the hardfork working." It is
   the change from *gl0 re-executes every metagraph's currency logic* to *gl0 adopts a
   committee-attested byte-diff and verifies a state proof* (Polkadot shared-security; see
   `docs/review/HANDOFF.md §0`).

**The load-bearing finding:** change (2) is currently **gated on `numShards > 1` cutover, NOT on
an activation ordinal**. The regression bar is "`numShards = 1` byte-identical to the pre-sharding
path" (`SHARDING-PRODUCTION-READINESS-PLAN.md:44`,
`GlobalSnapshotConsensusFunctions.scala:369,393,571,812`). The *fully* roots-only step (gl0 stops
holding per-MG state at all, same-shard reads go proof-only) is the piece that is described as an
**ordinal-gated hard fork** — and that piece is **DESIGN-ONLY / FUTURE**
(`SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:209-223` S8; `ECONOMIC-TRUST-…:194-195` W4c).

**Design-of-record docs:**
- Era machinery: `docs/nakamoto/ERA-REGISTRY-DESIGN.md` (research/design, 2026-05-29, *no
  implementation in the doc* — its own status line, `:3`).
- The A4/roots-only hard fork: `docs/nakamoto/CROSS-SHARD-MITIGATION-PROPOSAL.md:711-720` (A4
  definition), `docs/nakamoto/ROOTS-ONLY-SHARDING-ARCHITECTURE.md`,
  `docs/nakamoto/SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:153-241` (Phase 2 / S8),
  `docs/nakamoto/ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md:180-195` (W4c),
  `docs/nakamoto/COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md` (the adopt-and-verify replacement for
  re-derivation), `docs/nakamoto/SHARDING-PRODUCTION-READINESS-PLAN.md` (the *current* directive).

---

## 1. The crux: reconcile "greenfield, no wire compat" with "hardfork"

The memory rule `feedback_greenfield_no_wire_compat` (HARD RULE) says: *no network wire-format
backward compatibility; the only thing that must stay readable is **prior on-disk state + already
finalized chain-store snapshots**.* If it is greenfield, why is there a "hardfork" at all?

**Resolution — "hardfork" here means a consensus-root-changing behavior flip, and it comes in two
deployment shapes, neither of which is the classic multi-party coordinated wire-negotiation fork:**

- **Shape A — greenfield cutover (what change (2) is TODAY).** Flip a HOCON knob
  (`nakamoto.sharding.num-shards`, `application.conf:485`, `${?NAKAMOTO_NUM_SHARDS}` `:486`), build
  new JARs, restart — possibly re-genesis. Safety is bought by the **`numShards = 1` byte-identity
  regression bar**: a `numShards = 1` chain is *unaffected*, so no coordinated activation ordinal is
  needed. This is exactly the greenfield rule applied: "migration = flip the flag, rebuild, restart"
  (`feedback_greenfield_no_wire_compat`, "How to apply"). **No `protocolVersion`, no gossip-version
  negotiation, no capability handshake** — verified: `grep -rn "protocolVersion|protocol-version"
  modules/ = 0 hits`.

- **Shape B — ordinal-gated era boundary (what the *fully* roots-only step WOULD be, future).** Once
  a chain has history you will not throw away, you cannot re-genesis. To change what bytes get hashed
  into the consensus root (e.g. delete gl0's per-MG fold, or migrate the MPT node digest
  Brotli-JSON→scodec) you gate the new behavior at an **activation ordinal `B`** and keep pre-`B`
  ordinals **re-derivable the old way**. `SYNC-PROTOCOL.md:345-346`: *"hard fork at activation
  ordinal means everyone switches simultaneously."* This is the *only* sense in which the classic
  "activation-height fork" appears — and it is precisely the **on-disk + chain-store readability**
  that the greenfield rule says you MUST keep. The Era registry (`ERA-REGISTRY-DESIGN.md`) exists to
  make this boundary clean.

**So the two rules are consistent, not in tension:** the greenfield rule drops *wire-protocol
versioning ceremony*; it keeps *on-disk / chain-store read-path readability*. The Era machinery
governs exactly the readability the rule requires (`ERA-REGISTRY-DESIGN.md:196`, B.5: *"the registry
governs on-disk + chain-store readability (the only back-compat that matters per the greenfield
rule)"*). A "hardfork" in this project = **a consensus-root-changing flip**, deployed either as a
greenfield cutover (Shape A, when history can be re-genesised or a `numShards=1` chain is untouched)
or as an ordinal-gated Era boundary (Shape B, when live history must stay readable). What is
explicitly *dropped* is the multi-version wire-negotiation / mixed-mode-cluster coordination
(`SYNC-PROTOCOL.md:345-346` answers its own "can BFT and Nakamoto coexist?" with "Probably not").

---

## 2. State board — BUILT vs DESIGN-ONLY vs ABSENT

### 2.1 Era / activation machinery (inherited-mainnet read-path apparatus)

| Mechanism | State | `file:line` |
|---|---|---|
| `config.types.Era` (schema-migration predicate: `atOrAfterTess3` / `postTess3` / `postTess301` / `postMetagraphSync`) | **BUILT + WIRED** | `config/types.scala:43-69`; consumed `GlobalSnapshotAcceptanceManager.scala:1907` (`Era.fromConfig`), `:1638-1655`, `:2863`. Landed by `53d267f53` ("collapse 14 inline tess3 ordinal gates into Era predicate"). |
| `FieldsAddedOrdinals` (per-env schema gates) | **BUILT + WIRED** | `config/types.scala:27-38`; HOCON `application.conf:165-224`. Values are **mainnet ordinals** (e.g. tess3 mainnet 2 572 384), `dev = 0`. |
| `HashSelect` / `HashLogic` (crypto-hash era: Kryo vs JSON) | **BUILT, ad-hoc inline** (not the registry) | per `ERA-REGISTRY-DESIGN.md:46-54` at `security/Hasher.scala:15-21` + inline wiring `TessellationIOApp.scala:135-138`. HOCON `last-kryo-hash-ordinal` `application.conf:57-62` (mainnet 2 572 384, `dev 0`). *(Cited from design doc + HOCON; `Hasher.scala` line range UNVERIFIED against current source — check `Hasher.scala:15-21`.)* |
| `StateProofSelector` / `SnapshotFormat` (legacy-16-field vs MPT root) | **BUILT, ad-hoc inline** | per `ERA-REGISTRY-DESIGN.md:74-80` at `schema/StateProofSelector.scala:8-39`; HOCON `last-legacy-state-proof-ordinal` `application.conf:64-69` (mainnet 5 960 000, `dev 0`). *(Line ranges cited from design doc; UNVERIFIED against current source.)* |
| `EraCodecRegistry` (serde: the ONE validated, sealed-ADT registry) | **BUILT but UNWIRED** — scaffold/seed | `serde/era/EraCodecRegistry.scala:17-91` (VERIFIED, read in full). `fromRanges` validation `:45-83`; `defaultScodec` `:88-90`. Consumers are **only** the serde package + legacy bridges (`serde/legacy/{KryoBridge,JsonBridge,LegacyBridgeSerde}.scala`, `serde/package.scala`) — **no production dispatch site**. Confirms `ERA-REGISTRY-DESIGN.md:40` ("built and tested … not yet wired into production dispatch"). |
| **Unified `EraRegistry`** (the `ERA-REGISTRY-DESIGN.md` proposal: bundle serde+crypto+schema on one range list) | **ABSENT / DESIGN-ONLY** | `grep "class EraRegistry\|object EraRegistry\|io.constellationnetwork.era" modules/ = 0 hits`. The doc's own status: *"research / design … no implementation in this doc"* (`:3`). No S1–S6 slice from Part C is built. |
| `protocolVersion` field anywhere | **ABSENT** | `grep -rn "protocolVersion\|protocol_version" modules/ = 0 hits`. |
| Genesis `activationOrdinal` | **BUILT — but NOT a protocol fork** | `L0GenesisData.activationOrdinal = 0L` / `Cl1GenesisData.activationOrdinal = 0L` (`tools/genesis/GenesisGenerator.scala:301,315`; also test fixtures `…:79,98`, KES/VRF loaders). This is the *genesis activation ordinal* (from which genesis state is live), not a hardfork/era boundary — do not mistake it for one. |

**Net:** the era apparatus is a genuine, partly-live **read-path** for historical mainnet formats.
On the greenfield Nakamoto chain every boundary is `dev = 0`, so it is dormant-except-as-template.
The *unified* registry the design doc proposes (and its S4 "Brotli-JSON → scodec MPT node hash"
consensus flip, `ERA-REGISTRY-DESIGN.md:204-213`) is **entirely design-only**.

### 2.2 The sharding economic-trust cutover ("A4" / roots-only)

| Piece | State | Evidence |
|---|---|---|
| `numShards` config + cluster-wide passthrough | **BUILT** | `ShardingConfig.numShards` `config/types.scala:292-298`; HOCON `application.conf:481-486` (`num-shards = 1`, `${?NAKAMOTO_NUM_SHARDS}`); e2e flag `--num-shards` (`c3487f68a`). |
| Economic-trust substrate at `numShards > 1` (committee re-exec byte-diff, PIN-1 component roots, watchtower fraud-proof, VRF committee verify, `Slashings`/`ConsumedAllowSpends` partitions) | **BUILT (landed `8ac7ce04f..025a58688`, refined this session)** | `ECONOMIC-TRUST-…:180` correction ("much of Waves 2-3 is now LANDED"); `HANDOFF.md §2` (Track-1 landed). Gated `numShards > 1` throughout `GlobalSnapshotConsensusFunctions.scala:369,393,571,735,812,875,897`. |
| Byte-diff-adopt as the **primary** token-model path (delete authoritative override) | **BUILT this session** | `dc0dbe1c7` ("delete authoritative overrides — byteDiff-adopt primary, GAP-1 re-grounded"); see `HANDOFF.md §3` for the precise "removed the override *layering*, not the functions" correction. |
| Gating model for the above | **`numShards`-cutover, NOT ordinal** | `SHARDING-PRODUCTION-READINESS-PLAN.md:44` (invariant #2: "numShards=1 stays byte-identical"); no ordinal gate on the sharding path (`grep "activationOrdinal\|forkOrdinal" ⇒` only genesis + `ChainSelection` fork-ancestor + `MptOverlay.revertToOrdinal`, none a protocol fork). |
| Same-shard **reads** flipped to proof-only + gl0's per-MG fold **deleted** (the *actual* roots-only hard fork) | **DESIGN-ONLY / FUTURE, explicitly ordinal-gated** | `SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:209-223` (S8 "[HARD FORK — gated, separately scheduled]"); `ECONOMIC-TRUST-…:194-195` (W4c "[HARD FORK] … ordinal-gated"). Marked OPTIONAL for the current campaign (`…ENDGAME-PLAN.md:223`). |
| Cross-shard read-side **wiring** for roots-only | **ABSENT (hardcoded `numShards = 1`)** | `SpendActionValidator.scala:78-84` — the default `make[F]` hardcodes `ShardAssignment.make[F](numShards = 1)`, so *"the cross-shard branch is unreachable because every"* metagraph maps to shard 0 (`:78`). S8 must thread cluster `numShards` here (`SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:211`). Contrast: the checkpoint **producer** already threads real `cfg.numShards` (`ShardCheckpointWiring.scala:488`). |
| A2 "shard-eligibility gate" (`validateShardEligibility`, `NAKAMOTO_SHARD_VRF_ELIGIBILITY_GATE`) from `project_sharding_direction_clarified` | **UNVERIFIED — appears ABSENT under that name** | `grep "validateShardEligibility\|SHARD_VRF_ELIGIBILITY" = 0 hits`. Either never built under that name or superseded by the committee-VRF verify (`025a58688`). Check whether SC-binary admission is VRF-shard-gated before trusting the A2 memory item. |

---

## 3. The backward-incompatibility surface

What actually breaks compat, and what the greenfield rule says must stay readable:

1. **The consensus behavior of gl0 currency acceptance** (the real break). Pre-cutover: gl0
   re-executes each metagraph's currency `accept()` (mainnet full re-exec via `createContext`) — the
   `SpendActionValidator priorBalances` merge inside GSAM
   (`project_sharding_direction_clarified`, "gl0 re-executes today"). Post-cutover: gl0 **adopts a
   committee byte-diff and verifies the state proof**, never re-executing metagraph logic
   (`COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md:9`). This changes **what gl0 commits and how the global
   snapshot is validated** — two binaries on opposite sides of the flip will not agree on acceptance.
   Today this break is contained by the `numShards = 1` byte-identity bar (Shape A); the *roots-only*
   version (gl0 no longer even holds per-MG state) is the ordinal-gated Shape-B break (S8).

2. **On-disk / chain-store state SHAPE, if roots-only lands.** Roots-only changes gl0 from holding
   full per-MG `CurrencySnapshotInfo` to holding only `{metagraph → per-MG root}` + a thin summary
   (`SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:211`, `ECONOMIC-TRUST-…:195`). That is a
   consensus-root-changing schema change ⇒ classic hard fork.

3. **The MPT node digest (Brotli-JSON → scodec), if the Era S4 flip lands.** `ERA-REGISTRY-DESIGN.md`
   Part A.2/C-S4 (`:57-64,204-213`): the MPT node hash is currently
   `Blake2b(prefix ++ brotli(json(commitment)))`; migrating to a scodec-`immutable`-bytes hash is
   *consensus-root-changing ⇒ greenfield re-derivation ⇒ full e2e* (`:14`, `:209`). **Design-only.**
   This is the precondition for cross-language (metakit) proof verification (`:189`).

**What MUST stay readable (per the greenfield rule):**
- **Prior on-disk state** — a node must not fail to start because its disk state was written by an
  older binary (`feedback_greenfield_no_wire_compat`, "What we DO need to preserve").
- **Already-finalized chain-store snapshot bytes** — must be parseable by any version shipped
  (ibid.). This is precisely what `EraCodecRegistry` + `LegacyBridgeSerde`'s *read-only-historical*
  contract exist to guarantee (`serde/package.scala:18`, `ERA-REGISTRY-DESIGN.md:32`), and why a
  Shape-B ordinal boundary keeps pre-`B` ordinals re-derivable the old way rather than re-genesising.

**What is explicitly NOT preserved:** network wire-format compat across versions; protocol-version
negotiation; mixed-mode BFT/Nakamoto clusters during migration (`SYNC-PROTOCOL.md:345-346`); dual
"old-wire/new-wire" codec paths.

---

## 4. Dependency / sequencing — is the hardfork gated LAST?

**Yes, by design, in every doc — but note the two-step structure.**

- The user-approved phase order (`project_post_nipopow_phase_order`) puts **"Hard fork migration —
  Deferred to last"** and *"Hard-fork last to keep options open on greenfield vs fork-in-place
  launch."*
- A4's own hard-prerequisites (`CROSS-SHARD-MITIGATION-PROPOSAL.md:711-720`): A1+A2 VRF-eligibility
  load-bearing, A3 subtree-stub deployed, C1+C2 slashing live. *"This is the final step of the
  sharding rollout and lands at the hard fork boundary."*
- The **current** directive (`SHARDING-PRODUCTION-READINESS-PLAN.md`, APPROVED 2026-06-30) reframes
  the near-term work as **Track-1 (re-exec primary, delete authoritative override)** +
  **Track-2 (optimistic spendability)**, with the roots-only fold deletion still a **separately
  scheduled hard fork** (`ECONOMIC-TRUST-…:194-195` W4c; `…ENDGAME-PLAN.md:209` S8, "OPTIONAL for
  this campaign").

**What must be TRUE before the (roots-only) hardfork can activate** — the preconditions are largely
about *evidence*, not code:
1. **`reExecRoot === stateProof` with the override gone** — the Track-1 gate
   (`SHARDING-PRODUCTION-READINESS-PLAN.md:102`). Landed in unit; **not e2e-validated**
   (`HANDOFF.md §4`, "E2E cluster sanity pass — NOT RUN").
2. **Committee re-exec-before-attest actually gives the model teeth** — `HANDOFF.md §1.1` flags this
   as **⚠ UNVERIFIED / APPEARS ABSENT**: the gl0 happy path accepts on quorum-signature, not
   independent re-derivation. If attesters don't re-execute before signing, a Byzantine producer's
   false root is checked by nobody on the quorum path. **This is the top blocker for any "safe to
   fork" conclusion.**
3. **Genesis-window fallback** for a just-onboarded sharded MG that has no `Mg*` partition entry to
   prove against yet (`SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:211-223` hole (1)) — else universal
   rejection of every new metagraph's genesis-window spends.
4. **Thread cluster `numShards` into the read-side validator** (`SpendActionValidator.scala:84`
   hardcodes 1) — else the cross-shard read path is unreachable in production
   (`…ENDGAME-PLAN.md:211` hole (2)).
5. **Slashing consequence path live** (C1+C2) — `HANDOFF.md §4` marks it SPLIT: invalid-state-proof
   tier built; equivocation accumulator is shelf-ware; VaR cap un-enforced.
6. **Multi-run same-ordinal parity gate green** across full-topology e2e (`…ENDGAME-PLAN.md:223`).

Ordering constraint (`project_trust_model_audit_roots_only_greenlight` + `…ENDGAME-PLAN.md:223`):
the validator proof-carrying redesign must land **atomically with** the fold deletion in one
ordinal-gated change — you cannot delete gl0's fold before the proof-read path that replaces it is
load-bearing.

---

## 5. Fable-grade questions vs mechanical work

### 5.1 Frontier questions (spend Fable budget here)

- **F1 — Is "hardfork" even the right frame, or is it a pure greenfield cutover?** The deepest
  question for a reviewer: given `feedback_greenfield_no_wire_compat`, does the roots-only change
  *need* an ordinal-gated activation (Shape B) at all, or can it be a re-genesis cutover (Shape A)
  with the `numShards = 1` byte-identity bar? This is `ERA-REGISTRY-DESIGN.md` Open Question Q1
  (`:219`) unresolved: *"is there any historical chain state that must remain readable … or is this
  a fresh-chain cutover where the legacy cases could even be dropped?"* The answer determines whether
  the entire Era apparatus is load-bearing for this workstream or vestigial. **Requires judgment
  about launch strategy, not a grep.**

- **F2 — Do the era axes share one boundary or move independently?** `ERA-REGISTRY-DESIGN.md` Q2
  (`:221`) argues *independent* from real evidence (`last-kryo-hash-ordinal` 2 572 384 ≠
  `last-legacy-state-proof-ordinal` 5 960 000 on mainnet; `StateProofSelector` exists *specifically*
  because hash-scheme and format transitions happened at different ordinals). If the unified
  `EraRegistry` is ever built, is a single bundled `Era` per range the right model, or does it
  over-couple axes that must move separately? Load-bearing for any future Shape-B fork design.

- **F3 — Enforcement teeth on the cutover (subsumes `HANDOFF.md §1.1`/§5.1).** The economic-trust
  cutover only *is* a security upgrade if a quorum of committee members independently re-derived
  before signing. Verify whether that holds; if not, the "hardfork" ships a *weaker* trust model
  than the re-execution it replaces. This is the single highest-severity question across this
  workstream and the sharding workstream.

- **F4 — Atomicity of the roots-only flip vs the read-path redesign.** Can the fold-deletion and the
  proof-only read path (`SpendActionValidator`, `TokenLockStateManager`, allow-spend acceptance) be
  proven to activate at the *same* ordinal on every honest node, with the genesis-window fallback,
  without any node reading proof-only against an MG whose first incremental hasn't landed
  (`…ENDGAME-PLAN.md:211-223`)? A same-ordinal split here is a chain fork.

### 5.2 Mechanical / lower-leverage (fallback tasks for Opus/Sonnet)

- **M1** — Scrub stale line-number citations in this doc's §2.1 "UNVERIFIED" rows: confirm
  `Hasher.scala:15-21` (HashSelect/HashLogic), `StateProofSelector.scala:8-39`, `SerdeEra.scala`
  line ranges against current source (the design doc's citations predate this branch).
- **M2** — Confirm/deny the A2 shard-eligibility gate exists under any name (SC-binary admission
  VRF-shard-gating); reconcile `project_sharding_direction_clarified` A2 with source.
- **M3** — Enumerate every reader of gl0's per-MG `CurrencySnapshotInfo` /
  `GlobalStateFieldId.LastCurrencySnapshotInfo` (fieldId-6) and classify each dead / decoder-only /
  live — the S8 audit precondition (`SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:159`). Pure grep +
  classification; no judgment.
- **M4** — Grep-audit that no `numShards > 1`-gated code path leaks into the `numShards = 1`
  byte-identity path (the regression bar); list every site keyed on `numShards`/`shardAcceptanceDeps
  = None` and confirm each collapses to the pre-sharding bytes.
- **M5** — If the unified `EraRegistry` is greenlit, S1 (promote+widen `EraCodecRegistry`, add
  `HashKind`/`SchemaShape`, no behavior change) is a mechanical, additive refactor
  (`ERA-REGISTRY-DESIGN.md:206`) — Sonnet-appropriate once the design decision (F1/F2) is made.

---

## 6. Evidence appendix (commands run 2026-07-07, branch `feature/committee-state-diff`, HEAD `21933559c`)

- `git rev-parse HEAD` → `21933559c`. No local `main`.
- `grep -rn "protocolVersion|protocol_version" modules/ --include=*.scala` → **0 hits**.
- `grep -rn "class EraRegistry|object EraRegistry|io.constellationnetwork.era\b" modules/` → **0
  hits** (unified registry ABSENT).
- `EraCodecRegistry.scala` read in full (91 lines): `fromRanges` validation `:45-83`, `defaultScodec`
  `:88-90`. Consumers = serde package + legacy bridges only (no prod dispatch).
- `config/types.scala:43-69` = `Era` + `Era.fromConfig` (VERIFIED); `:27-38` = `FieldsAddedOrdinals`;
  `:292-298` = `ShardingConfig.numShards`.
- `GlobalSnapshotAcceptanceManager.scala:1907` `Era.fromConfig`; `:1638-1655` `era.postTess3/301/…`;
  `:2863` era passed to `buildGlobalSnapshotInfo` (VERIFIED via grep).
- `application.conf`: `last-kryo-hash-ordinal` `:57-62` (mainnet 2 572 384 / dev 0);
  `last-legacy-state-proof-ordinal` `:64-69` (mainnet 5 960 000 / dev 0); `sharding.num-shards`
  `:481-486` (`= 1`, `${?NAKAMOTO_NUM_SHARDS}`).
- `tools/genesis/GenesisGenerator.scala:301,315` `activationOrdinal = 0L` (genesis, NOT a fork).
- `SpendActionValidator.scala:78-84` default `make[F]` hardcodes `ShardAssignment.make[F](numShards =
  1)` — cross-shard branch unreachable in prod. Contrast `ShardCheckpointWiring.scala:488`
  (`cfg.numShards`, real cluster count).
- `git show --stat 53d267f53` → `refactor(global): collapse 14 inline tess3 ordinal gates into Era
  predicate` (the schema-`Era` origin).
- Docs read: `ERA-REGISTRY-DESIGN.md` (full), `SHARDING-PRODUCTION-READINESS-PLAN.md` (full),
  `COMMITTEE-STATE-DIFF-ADOPTION-DESIGN.md` (full), `docs/review/HANDOFF.md` (full); grep-cited
  `CROSS-SHARD-MITIGATION-PROPOSAL.md:123,381,711-720`, `SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md:5,
  35,153-241`, `ECONOMIC-TRUST-…:180-195`, `SYNC-PROTOCOL.md:345-346`.

---

*Scope note:* this file grounds **the hardfork workstream only**. For the sharding
adopt≡re-exec / finality-band review, `docs/review/HANDOFF.md` is the master. The two overlap at the
economic-trust cutover — this file frames it as *the backward-incompatible change*; HANDOFF frames
it as *the safety invariant*. Read both before concluding "safe to fork."
