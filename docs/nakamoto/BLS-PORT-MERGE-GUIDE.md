# BLS Aggregate-Signature Port — Merge Guide

**For the agent/engineer merging the BLS work into the main repo.** Date: 2026-06-02.
User has approved using **BC 1.85 beta in main for now** (so the DO-NOT-MERGE markers are
intentionally relaxable — see the BC section).

## What this is

The BLS12-381 aggregate-signature port, on branch **`feature/bls-aggregate-sigs`** at
`/home/euler/repos/tessellation-bls`. Four commits, all **build-green + unit-green**,
**NOT yet e2e-validated**.

## Branch lineage — READ FIRST

`feature/bls-aggregate-sigs` is based on **`e1a143a0c`**, the tip of
**`feature/serde-typeclass-shim`** (which is the K8s-docs cleanup commit). Consequences:
- The BLS codecs (`AggregateSignedCodec`, the cert scodecs) **depend on the `ImmutableCodec`
  serde shim** from `feature/serde-typeclass-shim`. They will NOT compile on a base lacking it.
- **Merge order:** land `feature/serde-typeclass-shim` first (or merge `feature/bls-aggregate-sigs`
  in a way that carries the serde work). Do **not** cherry-pick the BLS commits onto plain `main`.
- The base also carries the stale-K8s-docs cleanup (`kubernetes/` removal + doc fixes).

## The commits (in order)

| Commit | Slice | Summary |
|---|---|---|
| `84355c817` | S1+S2b-1 | `BlsSigner`/`BlsKeyDeriver` (`security/bls/`); unified **`ValidatorKeyRegistry`** *replaces* `VrfRegistry`+`KesRegistry`; genesis BLS derivation, PoP-gated at load |
| `c4968993f` | S2b-2 | `KesRegistrationCert`→**`OperatorKeyRegistrationCert`** rename (family + `MutableKesRegistry`→`MutableValidatorKeyRegistry`; field-id 22 preserved) + `vrfVK`/`blsVK`/`blsPoP` fields + hand-written scodec extension + validator **PoP gate** (`BlsProofOfPossessionInvalid`) + rotation overlay |
| `e0be13ff6` | S3 | detached **`AggregateSigned(hash, signers, sig)`** + concrete scodec + fail-closed `verify` |
| `94c5a3b98` | S4 | **gl0 finality cert**: `TipAttestation` += `bls_signature` (tag 8) + `SnapshotBlsAggregator` + assemble/self-verify/attach at the 2/3 finalize sink + `StoredSnapshot.aggregateCert` sidecar |

Note S2b-1 **deleted** `VrfRegistry.scala`/`KesRegistry.scala` and relocated `KesRegistryEntry`
into `ValidatorKeyRegistry.scala`; ~16 source + ~12 test files migrated to the unified registry.
`Signed[A]` and `SignatureProof` are **untouched** across all four commits.

## The one real merge blocker: BC 1.85

The BLS API (`org.bouncycastle.crypto.bls.*`) exists **only in BC 1.85**, currently a **beta**
(raw `-SNAPSHOT` jars, NOT on Maven Central). The branch vendors them worktree-locally:
- `modules/{shared,node-shared,dag-l0}/lib/*.jar` — the two beta jars (`bcprov-jdk18on`,
  `bcpkix-jdk18on`), currently **untracked**.
- `build.sbt`: `shared` de-manages `Libraries.bc`/`bcExtensions`; `nodeShared` + `dagL0` add
  `excludeDependencies` for `bcprov-jdk18on`+`bcpkix-jdk18on` (they pull 1.83 transitively via
  `keytool`). All marked `BLS-WORKTREE-LOCAL ... DO NOT MERGE AS-IS`.

**To merge with the 1.85 beta now (user-approved), pick one — in preference order:**
- **(B) Internal artifact repo (cleanest):** publish the two 1.85-beta jars to your team's
  internal Maven/artifact repository, add the resolver, depend by coordinate, drop the
  `excludeDependencies` + vendored `lib/` jars. No git bloat, normal dependency resolution.
- **(A) Vendor in-repo:** commit the beta jars. **Avoid 3× duplication** (~34 MB) — point all
  three modules at a SINGLE shared `unmanagedBase`/`unmanagedJars` path (one copy of the two
  jars, ~11 MB) rather than per-module `lib/`. Keep the `excludeDependencies`. File a tracking
  issue to remove at 1.85-stable.
- **(C) Wait for 1.85 stable** and skip vendoring entirely (the clean migration below).

## Clean migration when BC 1.85 goes stable (mechanical, ~3 edits)

1. `project/Dependencies.scala`: `val bouncyCastle = "1.83"` → `"1.85"`.
2. `build.sbt`: restore `Libraries.bc` + `Libraries.bcExtensions` in `shared`; drop the
   `excludeDependencies` blocks on `nodeShared` + `dagL0`.
3. `rm` the vendored jars (`modules/*/lib/*.jar` or the shared unmanaged path).

Nothing else changes — the source is already written against the stable API surface.

## Gotchas for the merging agent

- **scalafmt churn:** `scalafmtOnCompile` re-formats two committed-non-canonical serde files
  (`StateChangesAccumulatorCodec.scala`, `SystemIndexDeltaCodecs.scala`) on every build. If they
  appear in your diff and you didn't touch them, `git checkout HEAD --` them. **Better fix:**
  reformat + commit those two canonically once on `main` to kill the recurring churn permanently.
- **Worktree scalafmt errors:** `scalafmt: ... git rev-parse ... not a git repository` lines are a
  git-*worktree* artifact (the `.git` commondir points outside the build mount); they're
  **non-fatal** (build still `[success]`) and do NOT occur in a normal `main` checkout.
- **Go sidecar:** the `TipAttestation` proto gained `bls_signature` (tag 8). The Go p2p sidecar
  relays it untouched via **proto3 unknown-field retention** (protobuf-go APIv2) — **no Go change /
  sidecar rebuild needed**. If you ever regenerate the Go proto or change the sidecar's
  attestation parse/reserialize, preserve unknown-field retention.

## Deferred (NOT in these commits — tracked follow-ups)

- **S5:** light-client cert **serve + verify HTTP route** — S4 only assembles, self-verifies, and
  stores the cert on `StoredSnapshot`; nothing external serves or re-checks it yet.
- **HOCON:** migrate `NAKAMOTO_ATTESTATION_THRESHOLD` (`TipTracker` static `val` @ ~35 sites) to
  `SharedConfig.nakamoto.*` per the project HARD RULE. **S4 added zero new env reads.**
- **currency-l0 BFT co-signing** aggregation (the Option-a path, compresses N facilitator
  `MajoritySignature` proofs) — not built.

## Validation status

- **Build-green:** `shared *Bls*` 16/16, `nodeShared SnapshotBlsAggregator` 5/5, `nodeShared *Kes*/*Vrf*`
  52/52, `dagL0/compile`. Each independently re-built in ephemeral docker.
- **NOT e2e-validated.** Recommend a `just test --grafana` run on this branch before relying on
  the gl0 aggregate cert end-to-end (the cert assembly is non-gating, so a failure there is
  logged-and-skipped, not consensus-fatal — but validate the happy path produces a verifying cert).
