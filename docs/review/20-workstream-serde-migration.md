# Workstream: Serde Migration / MPT-as-Primary (portfolio grounding)

> **Purpose.** Ground the STATE of the "serde migration" workstream (MPT-as-primary +
> `GlobalSnapshotInfo` elimination + kryo→circe/scodec codec migration) so a Fable review can be
> pointed at it, alongside the sharding (`HANDOFF.md`) and hardfork workstreams.
> **Verified against the working tree 2026-07-07**, branch `feature/committee-state-diff`,
> HEAD `21933559c`. Every factual sentence carries a `file:line` / commit / doc anchor.
> Unverifiable statements are marked `UNVERIFIED`. Memory notes were treated as **hypotheses** and
> re-checked against source; where they were stale, that is called out.

> **Current root-contract correction (2026-07-13):**
> `GlobalSnapshotInfo.consensusMptRoot` includes every `SystemNamespace` economic
> index and excludes only field 32. The latter remains a HIGH confirmed open
> defect because checkpoint replay consumes the prior sync view without an exact
> signed/root-bound replay witness; it is not safe non-consensus metadata.

---

## 1. What the workstream is

**MPT-as-primary** makes the producer's hex-keyed Merkle-Patricia-Trie byte map the single
authoritative state substrate, demoting `GlobalSnapshotInfo` (GSI — the ~100 MB in-memory
materialized full-state case class) from *source of truth* to *derived projection*, and eventually
deleting it. The signed consensus anchor is `stateProof.mptRoot`, derived from the producer's own
byte map (`GlobalSnapshotInfo.scala:250,269`, `mptStateProofFromBytes`), not from a re-materialized
case class. Bundled with it is a **codec migration** off the mainnet-lineage kryo binary
serialization onto (a) circe/JSON for snapshot consensus hashing (`Hasher.forJson`,
`Hasher.scala:111`) and (b) a scodec-backed `ImmutableCodec[T]` typeclass — the "serde-typeclass-shim"
(`modules/shared/src/main/scala/io/constellationnetwork/serde/ImmutableCodec.scala:26`) — for
byte-exact, consensus-stable MPT leaf values and wire diffs.

**Design-of-record docs (both present, owned by this workstream):**
- `docs/serde/MPT-AS-PRIMARY-PLAN.md` — the master plan (phases 3a–3e).
- `docs/serde/FINISH-3C-EXECUTION-PLAN.md` — decomposes phase 3c ("remove GSI materialization on
  the accept path") into ordered slices **3c-A … 3c-E**. This is the live execution plan and the
  spine of §2 below.

Historical context (memory, confirmed): the user directive is standing — *"I want to get rid of
the GSI eventually"* (`project_mpt_primary_migration_pattern.md`), reaffirmed 2026-06-17 as
"finish Phase 3c." The branch lineage is `feature/serde-typeclass-shim → feature/committee-state-diff`
(the current branch descends from the shim branch; see the stash note in §2.4).

---

## 2. State: DONE / REMAINING / BLOCKED

### 2.1 The headline answer

**MPT is primary on the byte-transfer / verify / catch-up / resync paths (3c-A, landed
end-to-end). GSI is NOT eliminated:** it is still **materialized in `accept()`**, still **served**
as the `/latest/combined` side-state, still **the read source for ~dozens of consensus sites**, and
still **persisted** as `GlobalSnapshotWithState`. So the workstream's *fork-healing* half is done;
its *GSI-deletion* half (3c-B/C/D/E) is largely not started. The **codec migration is partial by
design** — MPT values are fully on scodec/`ImmutableCodec`; snapshot hashing carries a live dual
circe/kryo path gated by ordinal, with kryo as legacy backward-read.

### 2.2 Finish-3c slice board

| Slice | What it is | State | Evidence |
|---|---|---|---|
| **3c-A** | Serve the signed MPT byte map; followers `loadBytes` verbatim + verify `consensusMptRoot === signed mptRoot` by construction; delete the `syncFromGlobalSnapshotInfo` re-encode on follow/resync/catch-up | **DONE — end-to-end incl. the gl0↔gl0 catch-up gate** | see §2.3 |
| **3c-B** | Stop `accept()` materializing GSI as authoritative; emit a delta, make the returned GSI a `from(mptStore, ordinal)` projection | **NOT DONE** | `accept()` still calls `buildGlobalSnapshotInfo` (`GlobalSnapshotAcceptanceManager.scala:1606`, constructor `:1636`, call site `:2861`). No `GlobalSnapshotInfo.from(mptStore, ordinal)` projection exists (grep: only `fromGlobalSnapshotInfo` V1/V2 converter at `GlobalSnapshotInfo.scala:124`). |
| **3c-C** | Complete branch-aware `GlobalStateReaderOps` accessors + repoint `info.<field>` read-sites onto MPT getters | **PARTIAL** | `GlobalStateReaderOps.scala:35-98` still has only the same ~10 getters the plan listed (`getBalance`, `getDelegatedStakes`, `getNodeCollaterals`, `getActiveTokenLocks`, `getCurrencySnapshotInfo`, `getLast*` …). The plan's gap list (`getTxRef`, `getActiveAllowSpends`, `getTokenLockRef`, `getPriceRecord`, `getUpdateNodeParameters`, `getMetagraphSyncData`, `historicalStakeSnapshots`) is still **absent**. ~98 raw `info.<field>` read-site hits remain in main (incl. schema definitions) across `RewardsInfoCalculator`, `ShardCheckpointWiring`, `StateChannelValidator`, `ChangeSet`, `GlobalSnapshotAcceptanceManager`, `CurrencySnapshotCreator`, … (grep, non-test). |
| **3c-D** | MPT bytes authoritative on disk; GSI side-files (`GlobalSnapshotWithState`) derived, with on-disk read-compat | **PARTIAL** | Signed byte store IS persisted + retained (`mpt_snapshot_info_signed`, `ContiguousOrdinalCutoff` depth k₂ — `GlobalSnapshotConsensus.scala:401-404`). But `/latest/combined` still serves the `[snapshot, GSI]` pair (`SnapshotRoutes.scala:135,140`) and `GlobalSnapshotWithState` is still a live persisted type (`GlobalSnapshotWithStateDeltas.scala:20`, `GlobalSnapshotsWithStateLocalFileSystemStorage.scala:38`). The re-derive-on-load / drop-legacy migration is not done. |
| **3c-E** | Cleanup + the ~132-file `SnapshotInfo[_]` type-param threading | **NOT STARTED (deferred, non-load-bearing)** | plan §7; mechanical. |

### 2.3 3c-A — the landed part, in detail (this is real and end-to-end)

3c-A was reverted mid-June (`f42dedaba` reverted `MptStore.loadBytes`), then the **enabler landed
and it was un-reverted**:
- `0b3358a4c` — seal-time signed-postBytes persistence (the MultiBranch enabler: persist the exact
  bytes `accept()` signed over, keyed for finalize-promote).
- `f1bc34a79` — unit gate for the enabler (`SignedPostBytesPromotionSuite`, 3/3).
- `082ed24e7` — **un-revert 3c-A serve+rebuild + repoint the serve route at the signed store**.
- `324174883` — signed-store retention + serve-side walk-down (stop the fast-path 404→legacy drift).
- `e310f6ccd` — **gl0 deep-catch-up adopts signed MPT bytes byte-faithful, not GSI re-encode**
  (this closes the `NakamotoSyncDaemon` gl0↔gl0 scope gap the June memory flagged as open).

As-built wiring (all main, non-test):
- Primitive: `MptStore.loadBytes(entries, ordinal)` stores signed bytes verbatim, no re-encode
  (`MptStore.scala:91,295-304`).
- Serve: `GET /latest/combined/mpt-entries` (`SnapshotRoutes.scala:156`) →
  `FinalizedSnapshotReader.latestMptEntriesResponse` (`FinalizedSnapshotReader.scala:51,90,187`),
  serving the `[snapshot, GSI, Map[Hex,Array[Byte]]]` triple from the signed store; 404s on layers
  with no MPT store (BFT/currency).
- Client: `SnapshotClient.getLatestMptEntries` (`SnapshotClient.scala:96`);
  `GlobalL0Service.pullLatestMptEntries` (`GlobalL0Service.scala:59,180`) with **graceful 404→legacy
  fallback**.
- Consumers store verbatim + keep the verify gate: ml0 `StateChannel.ensureMptFromSignedBytes`
  (`currency-l0/.../StateChannel.scala:196`), cl1 `CurrencySnapshotProcessor` (`:200,209,448,461`),
  dl1 `GlobalSnapshotAlignment` (`GlobalSnapshotAlignment.scala:220,232`), and gl0↔gl0
  `NakamotoSyncDaemon.pullLatestMptEntriesFromPeer` → `mptStore.loadBytes` (`NakamotoSyncDaemon.scala:232,282`).
- The legacy `syncFromGlobalSnapshotInfo` re-encode still exists but is now a **fallback / one-time
  bootstrap path**, not the steady-state path (`dag-l0/Main.scala:430,486,593`; still referenced in
  `MptOverlay.scala:489`).

Codec anchor for the wire bytes: `MptStateStorage` encodes/decodes the signed entries with a shared
codec so producer↔follower bytes are identical, `loadBytes` then reproduces the signed root by
construction (`MptStateStorage.scala:91-93`).

### 2.4 Codec migration state (kryo → circe / scodec-`ImmutableCodec`)

Three distinct serialization surfaces — do not conflate them:

1. **MPT leaf value encoding — MIGRATED (DONE, consensus-critical).** Values are encoded via
   `ImmutableCodec[V]` (scodec-backed, byte-exact); "this replaces the earlier circe-based encoding
   and locks in consensus-stable byte layouts" (`MptStore.scala:36-37,44-73`). Landed as a chain of
   commits after a real fork: `e3f61f37f` (migrate MptStore value encoding circe→scodec),
   `65c3e8f5f` (migrate JSON→scodec for all MPT writes, full green), `72d43bc7f` (bootstrap paths
   must write scodec not JSON), `24c4ae69a` (format-gated consistency check). Motivating bug:
   `project_mpt_json_scodec_divergence_20260423` (cross-node `mptRoot` divergence from JSON
   non-canonicality) — confirmed the migration was safety-driven, not cosmetic.

2. **`ImmutableCodec` typeclass shim ("serde-typeclass-shim") — BUILT, in active use.** 579 total
   usages (61 main in `shared`, 23 main in `node-shared`, 3 `dag-l0`, 1 `currency-l0`). scodec is
   the single source of truth for both directions; laws demand `decode(encode(a)) == Right(a)` and
   "once defined for a consensus type and era, the encoding MUST NOT change" (`ImmutableCodec.scala:19-24`).
   A full snapshot-family scodec codec set was built (commits `ee8f7d7cb` GlobalSnapshot,
   `39987868c` GlobalSnapshotInfo 17-field capstone, `e9594a913` CurrencySnapshot family, …) and the
   changeset wire transport is explicit scodec (`cc67b5a43`, `c3449e4fe`). **Note:** the snapshot-family
   codecs exist but signing/hashing does **not** yet route through them (see surface 3) — they back
   MPT values + wire diffs today.

3. **Snapshot consensus hashing — DUAL circe/kryo, ordinal-gated (NOT fully migrated; kryo is
   legacy backward-read).** `Hasher` has two impls: `forJson` (circe/`JsonSerializer`,
   `Hasher.scala:111`) and `forKryo` (`Hasher.scala:69`, routes `Transaction` via
   `CustomKryoSerializer.hash` at `:85`). Selection is per-ordinal:
   `if (ordinal <= lastKryoHashOrdinal(env)) KryoHash else JsonHash`
   (`TessellationIOApp.scala:135-137`), wired `HasherSelector.forSync(Hasher.forJson, Hasher.forKryo, hashSelect)`
   (`:167`). `getCurrent = hasherJson` (`Hasher.scala:56`) — the **new/current path is circe/JSON**;
   kryo only fires for ordinals ≤ the configured legacy boundary. For a greenfield Nakamoto cluster
   with `lastKryoHashOrdinal` at `MinValue`, kryo is unreachable, but the code + registrars remain
   (`dag-l0/package.scala:18-21`, `node-shared/package.scala:24`; 265 total `KryoSerializer` refs,
   ~31 main across modules). **Removing kryo is a not-yet-scheduled cleanup, gated on greenfield
   "no wire compat" being confirmed for every path.** `UNVERIFIED` — whether any live Nakamoto
   config sets `lastKryoHashOrdinal` above `MinValue` (would keep kryo hot); check the per-env
   `application.conf` block for `lastKryoHashOrdinal`.

**Stash note (do NOT apply).** `git stash list` shows entries `stash@{0..4}` and `stash@{6}` are
`On feature/serde-typeclass-shim` — WIP salvage from the shim branch (roots-only balance-proof MPT
+ light-client SMT `stash@{0}`; dead top-K sortition `stash@{1}`; `phase-d-prep-stash` `stash@{6}`;
etc.). There is no single stash literally *named* "serde-typeclass-shim" — the name is the parent
**branch** these stashes descend from. They are stale WIP; **do not apply them** as part of any
grounding or review.

### 2.5 What is BLOCKED / at-risk (not merely "remaining")

- **3c-B (accept emits delta, GSI derived) — highest-surface, blocked on a faithful
  `from(mptStore, ordinal)`.** Plan §9 risk 2: any field the reader reconstructs imperfectly
  (Some/None present-only lifting, empty-vs-absent maps) silently corrupts a downstream consumer.
  Needs the field-by-field parity suite (the #107 byte-equivalence contract,
  `GlobalSnapshotInfo.scala:285-286`) before it can flip. Not started.
- **3c-C `historicalStakeSnapshots` migration — cross-workstream coupled.** Plan §8: 259-follower-trust
  explicitly *removes* it from the follower obligation (its empty `c=000000` recompute was the
  original #259 fork); 3c-C must not introduce a follower read-site that re-derives it. Sequence
  after 259-slice-3 + 3c-A. It has **no getter on either reader** today (read directly off GSI,
  field id 20).
- **On-disk migration (3c-D) — boot-risk.** Plan §9 risk 3: a botched
  `GlobalSnapshotWithState → mpt_snapshot_info` migration bricks boot; needs dual-write-for-one-release
  + re-derive-on-load fallback. Not started.

---

## 3. Interaction with the sharding workstream

**3c-A is a landed, load-bearing dependency of the sharding byte-diff/state-proof contract — but
finishing GSI elimination (3c-B/C/D) is NOT a precondition for it.**

The sharding Track-1 contract (`docs/review/10-reexec-byte-contract.md`) verifies the per-MG
currency state, **not** the global GSI. Its pinned-base reader and its verify gate ride directly on
3c-A's infrastructure:

- **Shared store.** The Track-1 pinned base reader reads over the **same** 3c-A signed-bytes store:
  `PinnedCurrencyInfoReader.make(signedBytesStore, …)` where `signedBytesStore` is
  `<mptSnapshotInfoPath>_signed`, contiguous, retained to k₂ (`GlobalSnapshotConsensus.scala:401-404,566-576`).
- **Shared verify primitive.** Track-1's PIN-2 gate is 3c-A's gate verbatim:
  `GlobalSnapshotInfo.consensusMptRoot(bytes) === snap.stateProof.mptRoot`
  (`PinnedCurrencyInfoReader.scala:207-213`; byte-contract §1 line 133).
- **Shared deep-revert source.** Track-3's k₂ deep revert (`MptOverlay.revertToOrdinal`) reads the
  same store via `setDeepStateReader(signedBytesStore.readState)` (`GlobalSnapshotConsensus.scala:415`).

So the per-MG root does **not** depend on MPT-primary being *finished*; it depends on 3c-A's
serve/store/verify primitive, which is *landed*. The byte-diff adopter reconstructs a
`CurrencySnapshotInfo` from `reconstructInfoFromDiff` (`ChangeSet.scala:151-172`) over a pinned base
hydrated from signed bytes — the global GSI is not on that path.

**Thin residual coupling (real, flagged in HANDOFF §4 as #23).** Because GSI is still materialized in
`accept()` (3c-B not done), some message-path global reads (`balances`, `lastCurrencySnapshots`) and
the ml0 producer are still **head-sourced** rather than read at the pinned anchor — HANDOFF's open
task #23 ("I-PIN residual purity … need a global-state-at-anchor reader"). The re-exec path already
uses the MPT-backed `GlobalStateReader.fromMptStore(mptStore)` reader
(`GlobalSnapshotConsensus.scala:470,540,581,1641`), but the adopter's `deriveAdoptedCurrencyState`
and the residual `info.<field>` reads (3c-C not done) mean the two sources can still disagree on
exactly the global fields #23 names. **This is the one place where the serde workstream's unfinished
half can bite the sharding safety claim** — completing 3c-B/C for `balances`/`lastCurrencySnapshots`
would retire #23 structurally.

---

## 4. The Fable-grade question(s)

Most of this workstream is **mechanical, not frontier**. 3c-C (fill reader getters, repoint
`info.<field>` sites), 3c-E (type-param threading), and the kryo removal are Sonnet-grade rename/
migration work behind parity suites. But three questions genuinely need frontier reasoning:

1. **Does a byte-verbatim `loadBytes` + `consensusMptRoot === signed` gate actually make the
   follower verify tautological on *honest* input across *every* path — and fail-closed on dishonest
   input — including under MultiBranch/reorg?** The enabler (`0b3358a4c`) persists accept-time
   `postBytes` so the served file reproduces the signed root under MultiBranch; the memory
   (`project_mpt_as_primary_finish_3c.md`) documented that the *finalize-time* `persist()` base and
   the *accept-time* signed `postBytes` are on different clocks and could diverge on the reorg path
   (`MptOverlay.finalizeBranch` orphan-fold, `MptOverlay.scala` folding a never-locally-committed
   canonical). Is the seal-time persistence provably sufficient across every reorg/replace shape, or
   is there a reorg schedule where the served signed bytes ≠ the signed root and the fast path
   silently falls back to the drift-prone legacy re-encode? Anchor: `GlobalSnapshotConsensus.scala:908-913`
   (staged postBytes), `SignedPostBytesPromotionSuite`, `MptStore.loadBytes`.

2. **`from(mptStore, ordinal)` faithfulness (the 3c-B crux, if/when built).** MPT-primary's whole
   payoff is that GSI becomes a pure projection of the byte map. That is only safe if the reader
   reproduces the legacy hand-built GSI **byte-for-byte** on the present-only Option lifting,
   empty-vs-absent map, and expiry-index dimensions (`GlobalSnapshotInfo.scala:50-70` V1/V2 Option
   dance). This is the same "version disease" class that caused the data-with-fee fork
   (`reference_sharded_mirror_version_model`). Proving there is *no* field where the projection and
   the case class disagree is a genuine adversarial-parity reasoning task, not a rename.

3. **Does eliminating GSI as a read source close, or merely relocate, the sharding #23 residual?**
   (See §3.) If `balances`/`lastCurrencySnapshots` move to a pinned MPT reader, does the adopt≡re-exec
   byte-equivalence claim (HANDOFF §5.1) become *provable*, or does the pinned-vs-head distinction
   resurface elsewhere? This is the intersection where the two workstreams' safety arguments meet.

Everything else here is honest mechanical migration. If a Fable pass is scoped to this workstream
alone, questions 1 and 2 are the only ones worth frontier budget; question 3 is better handled
inside the sharding review (`10-reexec-byte-contract.md` §4.7, HANDOFF §5.1).

---

## 5. Fallback tasks (an Opus/Sonnet agent can pick these up cold)

Ordered easiest-first; each names an entry point and a done-check.

1. **Scrub the kryo-vs-JSON reachability question (30 min, low risk).** Read the `lastKryoHashOrdinal`
   block in `application.conf` per env; confirm whether kryo hashing is dead on the Nakamoto path.
   Entry: `TessellationIOApp.scala:135-137`, `config/types.scala:399,427`. Done: a one-paragraph
   "kryo is/ isn't reachable in mainnet/testnet/dev" note appended here.
2. **3c-C accessor fill (mechanical, per-getter, behind parity).** Add the missing branch-aware
   getters to `GlobalStateReaderOps` — `getTxRef`, `getActiveAllowSpends`, `getTokenLockRef`,
   `getPriceRecord`, `getUpdateNodeParameters`, `getMetagraphSyncData` — each a one-line mirror of
   the corresponding `MptStoreReadOps` body (`GlobalStateConverter.scala:1572+`). Entry:
   `GlobalStateReaderOps.scala:35-98`. Done: getter + a parity test asserting it equals the
   `info.<field>` read for a corpus of ordinals. Do `historicalStakeSnapshots` **last** and coordinate
   with 259-follower-trust (§2.5).
3. **3c-C read-site migration, one file at a time.** Repoint `info.<field>` → `reader.get<Field>`
   in a single manager (start with `RewardsInfoCalculator` or `TokenLockStateManager` — scoped reads,
   existing unit suites). Entry: the ~98 grep hits (§2.2). Done: file's existing suite re-runs green
   against the reader-backed source; commit per file.
4. **Build the 3c-B `from(mptStore, ordinal)` projection + field-by-field parity suite (blocks the
   accept-delta flip).** This is the enabler for demoting GSI in `accept()`. Entry: plan §4;
   `buildGlobalSnapshotInfo` (`GlobalSnapshotAcceptanceManager.scala:1606`) is the reference shape to
   reproduce from the MPT. Done: parity suite green over accepted ordinals; then (separately) flip
   `accept()` behind an era gate.
5. **3c-D dual-write + re-derive-on-load migration.** Make `mpt_snapshot_info_signed` the
   authoritative on-disk artifact; keep writing `GlobalSnapshotWithState` for one release; add a
   boot-time re-derive if only the legacy file exists. Entry: `GlobalSnapshotConsensus.scala:401-404`
   (signed store), `GlobalSnapshotsWithStateLocalFileSystemStorage.scala`. Done: a restart-mid-run
   fixture test proving the migrated bytes reproduce the signed `mptRoot`.

**Do not** start the ~132-file type-param threading (3c-E) — it is non-load-bearing and best done as
one scripted rename after 3c-B/C settle (plan §7).

---

*Grounding note:* the June memory notes (`project_mpt_as_primary_finish_3c.md`) were point-in-time
and are now stale on two counts confirmed against source: (a) 3c-A is **landed and un-reverted**
(the memory captured it mid-revert), and (b) the gl0↔gl0 catch-up scope gap it flagged is **closed**
(`e310f6ccd`, `NakamotoSyncDaemon.pullLatestMptEntriesFromPeer`). The GSI-still-materialized claims
(accept/serve/persist) remain accurate.
