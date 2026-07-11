# Finish 3c — MPT as the single source of truth (execution plan)

**Status:** PLAN (2026-06-17). Decomposes `docs/serde/MPT-AS-PRIMARY-PLAN.md` Phase 3c
("remove `GlobalSnapshotInfo` materialization on accept path") into ordered, independently
shippable slices.
**Branch:** `feature/serde-typeclass-shim`.
**Greenfield:** NO network wire-compat is required (`[[feedback-greenfield-no-wire-compat]]`).
On-disk state DOES need read-compat — called out explicitly in the persistence slice (3c-D).
**Scope companion docs (owned elsewhere — do not edit):** `docs/serde/MPT-AS-PRIMARY-PLAN.md`,
`docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md`.

---

## 0. Where 3c actually stands (verified)

3c is **half done**. The signed global root is already MPT-authoritative, but `GlobalSnapshotInfo`
(GSI) is still materialized in accept, served over the wire as the side-state, persisted, and
**re-encoded by followers**. That last re-encode is the live fork.

| Concern | State today | Citation |
|---|---|---|
| Sign the root from MPT bytes | **DONE.** Proof's `mptRoot` derives from `p.entries` (producer's own byte map), sidecar-free. | `GlobalSnapshotInfo.scala:255-257` → `mptStateProofFromBytes:288-377`, `userEntries` line 314, global root 322-323; recompute helper `sidecarFreeMptRoot:385-390` |
| `accept()` builds the GSI case class | **NOT done** — still constructs and returns the full `GlobalSnapshotInfo`. | `GlobalSnapshotAcceptanceManager.scala` (GSAM); read-sites grep below |
| Serve | **GSI is the side-state.** `/latest/combined[/stream]` returns the JSON pair `[snapshot, GSI]`. | route `SnapshotRoutes.scala:135,146`; reader `FinalizedSnapshotReader.scala:62-67,87-97`; client decode `SnapshotClient.scala:62-84` |
| Rebuild on followers (**the drift site**) | **NOT done** — follower re-encodes the served GSI via `syncFromGlobalSnapshotInfo`, then verifies a *recomputed* root against the signed root. Re-encode can diverge ⇒ `recomputed ≠ signed`, deterministic per ordinal. | `StateChannel.scala:172-173, 252-261`; converter `GlobalStateConverter.scala:1791`; other rebuild sites grep below |
| Read accessors over MPT | **Half built.** Full typed getter set exists on `MptStore` (`MptStoreReadOps`, `GlobalStateConverter.scala:1576-1780`); the **branch-aware** mirror `GlobalStateReaderOps` has only ~10 of them. | `GlobalStateReaderOps.scala:33-102` |
| Persist | GSI persisted twice (`GlobalSnapshotWithState`, `GlobalSnapshotWithStateDeltas`). MPT bytes ALSO persisted, flat-KV `mpt_snapshot_info/<ordinal>`. | `StateChannel.scala:175-181`; `MptStateStorage.scala:19-48` |

**Coupling check (load-bearing for scope) — CONFIRMED.** `GlobalIncrementalSnapshot`
(`GlobalIncrementalSnapshot.scala:104+`) carries only `stateProof: GlobalSnapshotStateProof`
(roots) — **not** the GSI. So reducing/removing the GSI does **not** change the snapshot wire
format. Only the *side-served* combined-state payload and the on-disk side-files change. This is
what keeps the whole effort off the snapshot hardfork surface.

---

## 1. Goal + invariant

- **MPT = single source of truth.** Authoritative state is the producer's hex-keyed byte map
  (`MptStore.underlying.entries`, `FileSystemMerklePatriciaProducer.scala:54`).
- **GSI = derived projection.** `GlobalSnapshotInfo.from(mptStore, ordinal)` materializes the old
  case-class shape ON DEMAND for read APIs and the legacy persistence file; hot paths never go
  through it. (Helper to add — master plan 3c, `MPT-AS-PRIMARY-PLAN.md:91`.)
- **Single trust anchor = signed `stateProof.mptRoot`.** Already true (the produce/sign path);
  this plan stops *every other path* from re-deriving an independent root that can disagree with it.
- **Determinism rule (do not regress):** the global root is `makeParallelFromBytes(nonSystemNamespaceEntries(entries))` — the `03…` SystemNamespace sidecars are excluded (`GlobalSnapshotInfo.scala:307-314, 379-390`). Any new "store the bytes" path MUST round-trip those same `entries` so the sidecar-free recompute is unchanged.

---

## 2. Ordered slices (one line each)

- **3c-A — DO FIRST (heals the fork).** Serve the signed MPT entry bytes; followers store them
  directly (`clear` + `insertBytes`) and verify against signed `mptRoot`. The verify gate matches
  *by construction* because the served bytes ARE what was signed. Deletes the
  `syncFromGlobalSnapshotInfo` re-encode on the follow/resync paths.
- **3c-B — Stop GSI being authoritative in `accept()`.** GSAM emits a delta → `MptStore.update`/`commit`;
  the returned GSI becomes a thin `from(mptStore, ordinal)` projection, not the source of state.
- **3c-C — Complete `GlobalStateReader` accessors + migrate remaining `info.<field>` read-sites.**
  Fill the `GlobalStateReaderOps` gaps; repoint the ~12 consensus read-sites off `info.<field>`.
- **3c-D — Persistence: MPT bytes authoritative, GSI side-files derived (on-disk read-compat).**
  `mpt_snapshot_info/<ordinal>` is the authoritative on-disk artifact; `GlobalSnapshotWithState`
  becomes a derived/serve-time projection with a re-derive-on-load (or one-time migration) for old files.
- **3c-E — Cleanup + the long tail (defer).** Remove GSI from remaining hot paths; the ~132-file
  type-param threading is a mechanical, separately-scheduled follow.

```
3c-A ──▶ 3c-B ──▶ 3c-C ──▶ 3c-D ──▶ 3c-E
 (fork    (accept   (reads    (persist   (long tail,
  heal)    delta)    via MPT)  derived)    deferred)
```

3c-A is independently shippable and unblocks the sharded e2e on its own. 3c-B…D each ship behind a
typed-HOCON era gate (same machinery as `259-FOLLOWER-TRUST` slice migration, `FieldsAddedOrdinals.*`)
so they can A/B and roll back. 3c-E is non-load-bearing.

---

## 3. DO FIRST — Slice 3c-A: serve + rebuild from the signed MPT bytes

### 3c-A.1 Why this is the minimal fork fix

The follow/resync verify gate recomputes a root from the **rebuilt** store and compares it to the
signed root:

```
StateChannel.scala:252-261   (ml0 resyncToCanonical)
  ensureMptInitialized(ord, GSI)            // = mptStore.syncFromGlobalSnapshotInfo(GSI, ord)   :172-173
  afterBytes      = mptStore.underlying.entries
  recomputedRoot  = GlobalSnapshotInfo.sidecarFreeMptRoot(afterBytes)
  signedRoot      = canonicalSnapshot.signed.value.stateProof.mptRoot
  if (recomputedRoot === signedRoot) adopt  else re-pull / idle
```

The recompute is over bytes produced by **re-encoding the GSI** (`syncFromGlobalSnapshotInfo`,
`GlobalStateConverter.scala:1791`). That re-encode is a *different* byte path than the producer's
own `p.entries` that was actually signed — any field whose serialized shape, sidecar maintenance,
expiry-index, or present-only `Some/None` lifting differs by a byte makes
`recomputed ≠ signed`, deterministically, at the same ordinal (the data-with-fee fork; the same
"version disease" as `[[reference_sharded_mirror_version_model]]`).

**Fix:** serve the bytes that were signed and store them verbatim. Then `afterBytes` == the signed
byte map, and `sidecarFreeMptRoot(afterBytes) === signedRoot` holds **by construction** — no
re-encode, no drift.

### 3c-A.2 The two halves already exist

- **Serve side has the bytes:** the producer's signed byte map is `MptStore.underlying.entries`
  (`FileSystemMerklePatriciaProducer.scala:54`), and it is ALSO already on disk per ordinal in
  exactly this shape: `MptStateStorage` writes `Map[Hex, Array[Byte]]` to `mpt_snapshot_info/<ordinal>`
  (`MptStateStorage.scala:26-48`, `writeState`/`readState`). So gl0 can serve the signed bytes with
  no new computation — read the same map it signed.
- **Rebuild side has the bulk-load primitive:** the producer exposes
  `insertBytes(data: Map[Hex, Array[Byte]])` (`FileSystemMerklePatriciaProducer.scala:217-225`)
  and `loadOrBuild(ordinal, buildData)` which does `stateRef.set(data)` from a provided byte map
  (lines 317-329). `MptStore.clear` (`MptStore.scala:58`) + a thin
  `loadBytes(entries, ordinal)` (wraps `clear` then `insertBytes` then `commit(ordinal)`) replaces
  the typed re-encode with a verbatim store.

### 3c-A.3 Concrete diff sketch

**(a) New gl0 serve endpoint — return the signed MPT byte map at an ordinal.**
Add `GET -> Root / "latest" / "combined" / "mpt-entries"` (and a by-ordinal sibling) to
`SnapshotRoutes.scala` (next to `:135`). Body = the finalized snapshot's `Map[Hex, Array[Byte]]`
read from `MptStateStorage.readState(finalizedOrdinal)` (already JSON-encodable, `MptStateStorage.scala:26-30`),
gated through `FinalizedSnapshotReader` finality exactly like `latestCombinedResponse`
(`FinalizedSnapshotReader.scala:87-97`). No new serialization — it serves the persisted signed bytes.
*(Greenfield: a brand-new route, no compat concern.)*

**(b) Client — fetch the byte map.**
`SnapshotClient` (`SnapshotClient.scala`) gains
`def getLatestMptEntries: PeerResponse[F, (Signed[S], Map[Hex, Array[Byte]])]`
hitting the new route. The existing `getLatest` (`:62-84`) stays for callers not yet migrated.

**(c) Rebuild — store verbatim instead of re-encode.**
Replace `ensureMptInitialized` (`StateChannel.scala:172-173`):

```scala
// BEFORE
def ensureMptInitialized(ordinal: SnapshotOrdinal, state: GlobalSnapshotInfo): F[Unit] =
  sharedStorages.mptStore.syncFromGlobalSnapshotInfo(state, ordinal)

// AFTER — store the signed bytes verbatim (no GSI re-encode)
def ensureMptFromSignedBytes(ordinal: SnapshotOrdinal, entries: Map[Hex, Array[Byte]]): F[Unit] =
  sharedStorages.mptStore.clear >>
    sharedStorages.mptStore.underlying.insertBytes(entries).rethrow >>
    sharedStorages.mptStore.commit(ordinal)
```

and in `resyncToCanonical` (`StateChannel.scala:248-261`) pull
`(canonicalSnapshot, canonicalBytes)` via the new client, call `ensureMptFromSignedBytes`, then keep
the EXACT SAME verify gate:

```scala
afterBytes     <- sharedStorages.mptStore.underlying.entries
recomputedRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[F](afterBytes).map(_.some)
signedRoot      = canonicalSnapshot.signed.value.stateProof.mptRoot
// now recomputedRoot === signedRoot holds by construction
```

The verify gate **stays** — it is now a cheap, always-passing integrity check (and still catches a
genuinely corrupt/truncated transfer, which is the only failure mode left). The GSI value the
storage refs still need (`setForRecovery(snapshot, GSI)`) is obtained ONCE via
`GlobalSnapshotInfo.from(mptStore, ordinal)` (the 3c-B helper; until it lands, keep serving the GSI
**alongside** the bytes and pass it straight through — additive, no re-encode on the verify path).

**(d) Same swap at the other rebuild+verify sites** (identical shape, all currently re-encode):
- `currency-l1/.../CurrencySnapshotProcessor.scala:190-194` (cl1 NotNext resync; note this one uses
  `getRootHashForOrdinal` not `sidecarFreeMptRoot` — fold both onto `sidecarFreeMptRoot` for the
  apples-to-apples compare while here).
- `dag-l1/.../GlobalSnapshotAlignment.scala:224` and `dag-l1/.../SnapshotProcessor.scala:131,178,218,271` (gl1/dl1 follow).
- `node-shared/.../GlobalL0Service.scala:352` (`stateProofValidation` — this one rebuilds to *validate*; once bytes are the input it validates the served bytes directly).

### 3c-A.4 Scope / test / risk

- **Files:** `SnapshotRoutes.scala`, `FinalizedSnapshotReader.scala` (+ a `latestMptEntriesResponse`),
  `SnapshotClient.scala`, `L0GlobalSnapshotClient.scala`, `StateChannel.scala`,
  `CurrencySnapshotProcessor.scala`, `GlobalSnapshotAlignment.scala`,
  `dag-l1/SnapshotProcessor.scala`, `GlobalL0Service.scala`. A thin `MptStore.loadBytes` on
  `MptStore.scala` (wrapping `clear`+`insertBytes`+`commit`).
- **Unit tests:** (i) round-trip — sign bytes via `mptStateProofFromBytes`, store via `loadBytes`,
  assert `sidecarFreeMptRoot(entries) === proof.mptRoot` (extends the existing
  `BalanceProofCrossLangSuite` / parity-suite pattern in `node-shared` test). (ii) negative — a
  one-byte-mutated served map ⇒ gate fails ⇒ re-pull (assert no adopt). (iii) the existing GSAM /
  producer parity suites must stay green (no change to the sign path).
- **e2e validation:** the sharded run
  (`currency→rewards→token-locks→allow-spends→spend→data-without-fee→**data-with-fee**→multi-mg`).
  The fork is at **data-with-fee**; 3c-A is the slice that should carry the run past it. Flags per
  `[[feedback_e2e_run_command]]`: `just test --skip-streaming --num-gl0=8 --metagraphs=4
  --num-shards=4 --shards=4 --stake-dist=harmonic`.
- **Risk / rollback:** the new route + client + `ensureMptFromSignedBytes` are additive; if the
  byte-serve path misbehaves, revert the call sites to `ensureMptInitialized` (keep
  `syncFromGlobalSnapshotInfo`) — one-line per site. The verify gate is unchanged, so a bad path
  fails safe (re-pull/idle, never adopts unverified state). Era-gate the swap on
  `FieldsAddedOrdinals.serveSignedMptBytes` (typed HOCON, `[[feedback_prefer_hocon_over_sysenv]]`)
  for A/B at ordinal 0 in test, current behavior below the gate.

> **Net:** 3c-A makes the follower verify gate tautologically true on honest input, which is exactly
> the property the migration always intended (the bytes are the state). It removes a redundant,
> drift-prone re-encode rather than adding a trust surface — same framing as `259-FOLLOWER-TRUST`'s
> "removes a redundant recompute".

---

## 4. Slice 3c-B — stop materializing GSI as authoritative in `accept()`

### Scope
GSAM (`GlobalSnapshotAcceptanceManager.scala`) currently builds the `GlobalSnapshotInfo` case class
and computes the proof over it. Target (master plan 3c, `MPT-AS-PRIMARY-PLAN.md:88-92`): accept emits a
delta `(toUpsert: Map[GlobalStateKey, V], toRemove: Set[GlobalStateKey])`, applies it via
`MptStore.update` (`MptStore.scala:75`) / `commit(ordinal)` (`:66`), and the proof builds from
`p.entries` (already the path — `GlobalSnapshotInfo.scala:255-257`). The returned `GlobalSnapshotInfo`
becomes a derived projection via a NEW `GlobalSnapshotInfo.from(mptStore, ordinal)` that reads the
typed partitions back through `MptStoreReadOps` (`GlobalStateConverter.scala:1576-1780`,
`getAllLastCurrencySnapshots:1710`, `getAllUpdateNodeParameters:1761`, etc. — the readers already exist).

### Behavior change
`accept()` no longer threads a hand-built case class as the state of record. Downstream callers that
take the returned GSI still get one — produced by `from(...)` — but it is a view, not the source.
The accept path already maintains the overlay byte map under `OverlayMode.MultiBranch`
(`mptStateProofFromBytes`'s "post-write overlay+handle view" note, `GlobalSnapshotInfo.scala:279-287`),
so the delta-emit is wiring the existing overlay writer (`AcceptanceMptStateChanges` /
`ChangeSet.withChanges`, referenced by `COMMITTEE-STATE-DIFF` §3) as the primary, with GSI derived after.

### Test / e2e / risk
- **Unit:** parity — for a corpus of accepted ordinals assert `from(mptStore, ord)` equals the
  legacy hand-built GSI field-by-field (the #107 byte-equivalence contract, `GlobalSnapshotInfo.scala:285-286`).
- **e2e:** full sharded run must stay green end-to-end (this slice is invisible if `from` is faithful).
- **Risk:** highest-surface change in 3c (this is the master plan's "long pole",
  `MPT-AS-PRIMARY-PLAN.md:94`). **Rollback:** era-gate (`FieldsAddedOrdinals.acceptEmitsDelta`); below the gate keeps the hand-built GSI path verbatim. Land AFTER 3c-A so the cluster is already
  fork-free when this goes in.

---

## 5. Slice 3c-C — complete `GlobalStateReader` accessors + migrate `info.<field>` read-sites

### Accessor gaps (verified)
`GlobalStateReaderOps` (`GlobalStateReaderOps.scala:33-102`) has branch-aware getters for:
`getBalance, getDelegatedStakes, getDelegatedStakeWithdrawals, getNodeCollaterals,
getNodeCollateralWithdrawals, getActiveTokenLocks, getCurrencySnapshotInfo,
getLastStateChannelSnapshotHash, getLastIncrementalCurrencySnapshot, getLastCurrencySnapshot`.

Present on `MptStoreReadOps` but **missing** from the branch-aware reader (the real gap list):
`getTxRef` (lastTxRefs), `getAllowSpendRef` + `getActiveAllowSpends` (lastAllowSpendRefs,
activeAllowSpends), `getTokenLockRef` (lastTokenLockRefs), `getTokenLockBalance` (tokenLockBalances),
`getPriceRecord` (priceState), `getUpdateNodeParameters` (updateNodeParameters),
`getMetagraphSyncData` (metagraphSyncData). And `historicalStakeSnapshots` has **no getter on either**
(it is read directly off the GSI; field id 20, `GlobalStateKey.scala:213`). Each is a one-line mirror
of the corresponding `MptStoreReadOps` body (the task's stated gap list is accurate).

### Read-sites to migrate (non-test grep — `\binfo\.(balances|lastTxRefs|…)`)
~12 files: `GlobalDelegatedRewardsDistributor.scala`, `UpdateNodeParametersStateReader.scala`,
`NodeCollateralStateManager.scala`, `DelegatedStakeStateManager.scala`, `RewardsInfoCalculator.scala`,
`GlobalSnapshotAcceptanceManager.scala`, `ShardCheckpointWiring.scala`, `StateChannelValidator.scala`,
`StakeRegistry.scala`, `NodeStakeAggregator.scala`, `ChangeSet.scala`, `TokenLockStateManager.scala`
(plus pure-schema occurrences in `currency.scala` / `GlobalSnapshotStateProof.scala` /
`StakeDistribution.scala` which are definitions, not read-sites). Repoint each `info.<field>` to the
branch-aware `reader.get<Field>` (or `mptStore.get<Field>` where the site has no branch context).

### Behavior change / test / risk
- No semantic change — same values, different source (MPT vs case-class field).
- **Unit:** each migrated manager's existing suite (e.g. `TokenLockStateManager` suite) re-run with
  the reader-backed source asserting identical outputs.
- **e2e:** the phase that exercises each subsystem — token-locks phase for `TokenLockStateManager`,
  allow-spends/spend for the GSAM spend path + `getActiveAllowSpends`, rewards phase for
  `RewardsInfoCalculator` / `GlobalDelegatedRewardsDistributor`, multi-mg for `ShardCheckpointWiring`.
- **Risk:** medium, but mechanical and per-file shippable. **Rollback:** per-site revert (each site is independent). Do `historicalStakeSnapshots` LAST — it is the field that `259-FOLLOWER-TRUST`
  explicitly removes from the *follower* obligation, so coordinate (see §7).

---

## 6. Slice 3c-D — persistence: MPT bytes authoritative, GSI side-files derived

### Scope
`persistGlobalSnapshot` (`StateChannel.scala:175-181`) writes two GSI files
(`GlobalSnapshotWithState`, `GlobalSnapshotWithStateDeltas`). MPT bytes are ALSO already persisted
flat-KV at `mpt_snapshot_info/<ordinal>` (`MptStateStorage.scala`). Target: the MPT byte file is the
**authoritative** on-disk artifact; the `GlobalSnapshotWithState` file becomes either (i) derived at
serve/persist time from `from(mptStore, ordinal)`, or (ii) dropped in favor of serving the byte file
(3c-A's `mpt-entries` route makes the combined `GlobalSnapshotWithState` file redundant for the
follow path).

### On-disk READ-COMPAT (the one place compat matters)
Per `[[feedback_greenfield_no_wire_compat]]`, network has no compat needs but on-disk state DOES.
Two safe options, pick one:
- **Re-derive on load (preferred):** on boot, if only the legacy `GlobalSnapshotWithState` file
  exists for an ordinal (old node upgrading), load it and run the 3c-A `loadBytes`-from-GSI once
  (i.e. keep `syncFromGlobalSnapshotInfo` strictly as a **one-time boot migration** path, not a
  steady-state path) to materialize the `mpt_snapshot_info/<ordinal>` file, then never read the GSI
  file again. `FileSystemMerklePatriciaProducer.load`/`loadOrBuild` (`:297-329`) already implements
  load-or-rebuild.
- **One-time migration tool:** a `tools` CLI pass that walks existing `GlobalSnapshotWithState` files
  and emits `mpt_snapshot_info/<ordinal>` (same `loadBytes`-from-GSI conversion). Then delete the old
  files.

### Test / e2e / risk
- **Unit:** load an old-format `GlobalSnapshotWithState` fixture, assert the migrated
  `mpt_snapshot_info/<ordinal>` reproduces the signed `mptRoot` for that ordinal.
- **e2e:** a restart-mid-run scenario (node stops after data-with-fee, restarts, resumes) to exercise
  the load path on real on-disk files.
- **Risk:** medium — get the migration wrong and a node can't boot. **Rollback:** keep writing BOTH
  files for one release (authoritative byte file + legacy GSI file) so a rollback still finds the old
  format; drop the legacy write only once the migration is proven. Era-gate the *read* preference
  (`FieldsAddedOrdinals.mptBytesAuthoritativeOnDisk`).

---

## 7. Slice 3c-E — cleanup + long tail (defer)

- Remove GSI construction from any remaining hot path once 3c-B/C/D have removed all authoritative
  reads. The `GlobalSnapshotInfo.from(...)` projection stays for external read APIs and rosetta.
- The ~132-file `SnapshotInfo[_]` / `GlobalSnapshotInfo` **type-param threading** (storages,
  consensus state creators, processors carrying `SI` purely as a type parameter) is mechanical and
  **deferred** — it does not gate correctness and is best done as one scripted rename after the
  authoritative-read migration settles. Not load-bearing for the e2e.

---

## 8. Dependencies / conflicts with adjacent in-flight work

### Execution-shard checkpoints

The abandoned committee state-diff adoption design has been removed. A checkpoint carries root claims and complete replay inputs, and
every GL0 adopter recreates all framework-economic CL1 transitions at the signed finalized execution base before use. MPT-primary storage
work may optimize how canonical bytes are persisted or served downstream, but it must never reintroduce committee-provided economic bytes
as an adoption authority.

### `docs/nakamoto/259-FOLLOWER-TRUST-REDESIGN.md` (slices 1–2 built)
Follower trusts the finalized `stateProof.mptRoot` and cross-checks only `followerConsumedFieldIds`
via per-field inclusion proofs, instead of recomputing the global root. Slice 1 (`TrustedGlobalStateReader`,
`4421bd506`) + slice 2 (`GlobalStateProofRoutes/Service/Client`, `3aef072ac`) built on worktrees;
slice 3 (the gated trust-branch in `createContext`) is prep, awaiting sign-off.

- **Same direction, complementary, mild overlap.**
  - 3c-A removes the re-encode on the **bootstrap/resync/follow** path (store-the-signed-bytes); the
    259 redesign removes the recompute on the **incremental verify-replay** path (trust-the-root +
    per-field cross-check). 3c-A makes the verify gate pass by construction; 259 changes *which*
    obligation the follower carries. They are not mutually exclusive — 3c-A is the stronger, simpler
    primitive where the follower can get the full signed byte map; 259's per-field cold-cache proof
    is the fallback where it cannot.
  - **Conflict to watch — `historicalStakeSnapshots`.** 259 explicitly **removes** it from the
    follower obligation (it is gl0 leader-election state the follower cannot reproduce; its empty
    `c=000000` recompute was the original #259 SPM). 3c-C must therefore NOT introduce a follower
    read-site that re-derives `historicalStakeSnapshots` from a recompute — followers read it from the
    served/stored signed bytes (3c-A) only. Order: **259 slice-3 sign-off and 3c-A both precede** any
    3c-C touch of `historicalStakeSnapshots`/`StakeRegistry`/`StakeDistribution`.
  - **Shared infra:** 259's `GlobalStateProofRoutes` is finalized-anchored (`FinalityGate.finalizedOrdinal`);
    3c-A's `mpt-entries` route is the same finality anchor (`FinalizedSnapshotReader`,
    `FinalizedSnapshotReader.scala:87-97`). Put both on the same anchor callback to avoid two
    finality-gate notions of "latest servable."
  - **Migration gates:** both use typed-HOCON `FieldsAddedOrdinals.*` era gates
    (`[[feedback_prefer_hocon_over_sysenv]]`). Keep the gate names distinct
    (`serveSignedMptBytes` vs `followerTrustFinalizedRoot`) so they A/B independently.

---

## 9. Top 3 risks

1. **Determinism of the served byte map (3c-A).** If gl0 serves bytes that include or exclude the
   `03…` SystemNamespace sidecars differently from what `sidecarFreeMptRoot` strips, the gate breaks
   the other way. Mitigation: serve `MptStateStorage.readState` (the exact persisted signed map) and
   keep the recompute on `nonSystemNamespaceEntries` (`GlobalSnapshotInfo.scala:314, 385-390`); add the
   round-trip unit test (§3c-A.4-i) as a CI gate. The sidecars are append-only/path-dependent
   (`GlobalSnapshotInfo.scala:307-313`) — never let them into the *signed-root* comparison.
2. **`accept()` delta parity (3c-B).** A faithful `GlobalSnapshotInfo.from(mptStore, ordinal)` is the
   crux; any field the readers reconstruct imperfectly (Some/None present-only lifting, empty-vs-absent
   maps — cf. the V1/V2 `toGlobalSnapshotInfo` Option dance, `GlobalSnapshotInfo.scala:50-70`) silently
   corrupts a downstream consumer. Mitigation: field-by-field parity suite against the legacy
   hand-built GSI before flipping the gate; land behind `FieldsAddedOrdinals.acceptEmitsDelta` after
   3c-A.
3. **On-disk migration (3c-D) + cross-slice file collisions.** A botched `GlobalSnapshotWithState` →
   `mpt_snapshot_info` migration bricks boot; and 3c-C vs COMMITTEE-STATE-DIFF both edit
   `ShardCheckpointWiring`, 3c-C vs 259 both touch the stake/historical-stake path. Mitigation:
   dual-write both on-disk formats for one release with re-derive-on-load fallback
   (`FileSystemMerklePatriciaProducer.load`, `:297-315`); sequence the shared-file edits per §8
   (one PR for `ShardCheckpointWiring`; 259-slice-3 + 3c-A before any `historicalStakeSnapshots` read
   migration).
