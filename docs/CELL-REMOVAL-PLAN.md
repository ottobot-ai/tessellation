# Cell-Removal Plan — staged, behavior-preserving teardown of the kernel `Cell`

**Status:** ready to execute. Date: 2026-06-04. Companion to `docs/CELL-CONSTRUCT-ASSESSMENT.md`
(the *why*). This doc is the *how* — an ordered, mechanical removal plan grounded in an exhaustive
wiring sweep. Every stage is a no-runtime-behavior change.

## Goal & principles

Remove the inherited `modules/kernel` `Cell`/Topos apparatus and all residual wiring. It is pure
in-memory plumbing (no wire/MPT/state/codec impact → greenfield-clean, no migration). The two
`L0Cell`s are `queue.offer` pass-throughs; `BlockConsensusCell` is a static dispatch table (`F = Id`,
`scheme.hyloM` never called). Removing all of it changes no runtime behavior.

- **One stage = one PR-sized, behavior-preserving change.** Compile + touched-module tests after each;
  one e2e after Stage C and after Stage D.
- **Bottom-up dependency order:** the kernel symbols `Ω`, `CellError`, `StackF`/`More`/`Done` are
  used *only as the cells' own input/output/error ADTs*, so they can only be deleted **after** the
  cells that reference them. Order the stages so each deletes only what no longer has references.
- **Builds run in ephemeral docker** (host `sbt` is broken in this environment —
  `createTempFile: Permission denied`). Pattern: `sbtscala/scala-sbt:eclipse-temurin-21.0.8_9_1.12.11_2.13.18`
  with `-Dsbt.offline=true`, mounting the worktree + coursier cache + `~/.sbt`.

## Inventory (what exists, from the wiring sweep)

**Kernel module** `modules/kernel/src/main/scala/io/constellationnetwork/kernel/`:
| File | Symbols | Used outside kernel? |
|---|---|---|
| `Cell.scala` | `Cell[M,F,A,B,S]`, `Cell.NullTerminal`, `cellMonoid`, `unapply` | `Cell`/`NullTerminal` yes; `cellMonoid` **no** |
| `Hom.scala` | `Poset`, `Ω`, `Hom[+A,+B]` | `Ω` yes; `Poset`/`Hom` no (only `Ω`/`Cell` extend them) |
| `Stack.scala` | `CellError`, `StackF`, `More`, `Done`, instances | `CellError`/`StackF`/`More`/`Done` yes |
| `ΩList.scala` | `ΩList`, `::`, `ΩNil` | **no** (only `cellMonoid` + tests) |
| `PipeArrow.scala` | `PipeArrow`, `arrowIOInstance` | **no** (zero importers) |

Kernel-only tests: `CellMonoidLawsSuite.scala`, `CellMonoidManualTestSuite.scala`, `ΩListSuite.scala`.
**Kernel exports nothing non-Cell that is used elsewhere** → the module can be deleted wholesale once
its three consumers are removed.

**Three production cells** (the only real consumers):
1. **dag-l0 `L0Cell`** — `modules/dag-l0/.../domain/cell/L0Cell.scala` (+ `L0CellInput`, `AlgebraCommand`, `CoalgebraCommand`).
2. **currency-l0 `L0Cell`** — `modules/currency-l0/.../cell/L0Cell.scala` (+ same companions).
3. **dag-l1 `BlockConsensusCell`** — `modules/dag-l1/.../domain/consensus/block/BlockConsensusCell.scala`.

**Build wiring** (`build.sbt`): `kernel` aggregated (L162), module def (L164–181, deps `drosteCore`
+ `fs2Core`), and `.dependsOn(kernel)` at `nodeShared` (L375), `rosetta` (L441), `dagL1` (L467),
`dagL0` (L551), `currencyL0` (L622). **`nodeShared` and `rosetta` have ZERO kernel references** — those
two dep edges are already vestigial. `droste` in `project/Dependencies.scala` (V L17; `droste()` L59;
`drosteCore`/`drosteLaws`/`drosteMacros` L111–113).

**Pre-existing breakage to fold in:** `examples/` has dangling kernel imports (`L0TokenDef.scala`,
`types.scala` import a non-existent `kernel.StateChannelSnapshot`; `simple-snapshot-publisher` imports
`Ω`/`Cell.NullTerminal`). Handle in Stage E.

---

## Stage 0 — prune the two vestigial dep edges (zero code change)

`nodeShared` and `rosetta` declare `.dependsOn(kernel)` but reference nothing in it. Remove `kernel`
from those two `dependsOn` lists in `build.sbt` (L375, L441). Compile both modules. This shrinks the
blast radius and proves the "no non-Cell consumers" claim early. **Pre-check:** grep `nodeShared` +
`rosetta` sources for `io.constellationnetwork.kernel` → expect zero hits (sweep confirms).

## Stage A — delete the provably-dead apparatus (zero risk)

Nothing outside kernel tests references these. Delete:
- `PipeArrow.scala` (0 importers anywhere).
- `ΩList.scala` and the `cellMonoid` def + its `ΩList` use in `Cell.scala` (used only by `cellMonoid`).
- The three kernel-only test suites (`CellMonoidLawsSuite`, `CellMonoidManualTestSuite`, `ΩListSuite`).
- The `Topos`/sheaf `TODO` stubs / dead comments in `Hom.scala` (keep `Ω`/`Poset`/`Hom` for now —
  `Ω` is still referenced by the cells; `Poset`/`Hom` are its supertypes).

Compile `kernel` + run remaining kernel tests (should be none, or trivial). No other module touched.

## Stage B — collapse the two pass-through `L0Cell`s to direct enqueue

Both cells reduce to `queue.offer(event)`. Replace the `mkCell: A => Cell[F, StackF, _, Either[CellError, Ω], _]`
indirection with a plain enqueue function `A => F[Unit]` (or inline the `queue.offer` at call sites —
one already coexists at `Services.scala:251` / `NakamotoSyncDaemon.scala:1938`).

**dag-l0 edits:**
- Maker: `Services.scala:206` (`L0Cell.mkL0Cell()`); `HttpApi.scala:134-184` (the 4 `mk*Cell` makers) + `:246` (`mkKesRegistrationCertCell(cert).run().void`).
- Route param types + run sites: `NodeParametersRoutes.scala:38,197`; `DelegatedStakesRoutes.scala:40,152,174`; `NodeCollateralRoutes.scala:37,112,136`.
- Delete: `domain/cell/L0Cell.scala`, `L0CellInput.scala`, `AlgebraCommand.scala`, `CoalgebraCommand.scala`.

**currency-l0 edits:**
- Maker: `CurrencyL0App.scala:129` + run sites `CurrencyL0App.scala:515`, `CurrencyMessageRoutes.scala:73`.
- Param types: `StateChannel.scala:74,121`; `Services.scala:79`; `HttpApi.scala:51,119`; `CurrencyBlockRoutes.scala:18`; `CurrencyMessageRoutes.scala:35`; `DataBlockRoutes.scala:21`.
- Delete: `cell/L0Cell.scala`, `L0CellInput.scala`, `AlgebraCommand.scala`, `CoalgebraCommand.scala`.

Each deleted `AlgebraCommand` was `extends Ω` — those are the only `Ω` users besides dag-l1. Compile
dag-l0 + currency-l0 + their tests.

## Stage C — de-Cell `BlockConsensusCell` (the only non-trivial stage)

`BlockConsensusCell[F]` uses `F = Id`, `convert = identity`; its "hylo" is a 5-way pattern match to
static handlers — `scheme.hyloM` is never invoked. Convert to a plain function; **preserve the 5-way
dispatch and all companion-object logic byte-for-byte** (this is the one cell carrying real consensus
logic — review carefully, no semantic change).

- Replace `new BlockConsensusCell[F](input, ctx, ordinal).run()` at `dag-l1/StateChannel.scala:138`
  with `runBlockConsensusStep(input, ctx, ordinal)` (the logic already lives in the companion `def`s).
- Drop `import io.constellationnetwork.kernel.CellError` at `StateChannel.scala:28`; replace the
  `Either[CellError, BlockConsensusOutput]` return with a dag-l1-local error type (or keep a tiny local
  `BlockConsensusError`). Remove `extends Ω` / kernel import from `BlockConsensusInput.scala` +
  `BlockConsensusOutput.scala`.
- Delete `BlockConsensusCell.scala`.

Compile dag-l1 + tests. **Run one e2e after Stage C** (dag-l1 block-consensus path is live).

## Stage D — delete the kernel module + droste dependency

After A–C, nothing references kernel. Verify, then remove:
- `build.sbt`: drop `kernel` from `.aggregate(...)` (L162); delete the `lazy val kernel = ...` def
  (L164–181); remove `kernel` from `dagL1` (L467), `dagL0` (L551), `currencyL0` (L622) `dependsOn`.
- Delete `modules/kernel/`.
- `project/Dependencies.scala`: remove `droste` (L17, L59, L111–113) **iff** no other consumer.
  **Pre-check:** grep repo-wide for `higherkindness` / `droste` / `qq.droste` / `scheme.` / `hyloM` /
  `Coalgebra`/`Algebra` (droste ones) → expect kernel-only. Keep `fs2Core` (used everywhere).

Compile the whole build + full test. **Run one e2e after Stage D.**

## Stage E — clean up the dead `examples/` imports (pre-existing breakage)

`examples/l0-token/{L0TokenDef,types}.scala` and `examples/simple-snapshot-publisher/*` reference
kernel (incl. an already-broken `kernel.StateChannelSnapshot`). Determine whether `examples/` is in the
build graph: if it compiles, fix the imports (point at the real `StateChannelSnapshot`, drop `Cell`
usage); if it's dead/un-built sample code, delete the stale files. Don't let pre-existing dangling
imports block Stage D.

---

## Validation matrix

| Stage | Compile | Tests | e2e |
|---|---|---|---|
| 0 | nodeShared, rosetta | — | — |
| A | kernel | kernel | — |
| B | dag-l0, currency-l0 | dag-l0, currency-l0 | — |
| C | dag-l1 | dag-l1 | ✅ one e2e |
| D | full | full | ✅ one e2e |
| E | examples (or delete) | — | — |

## Risk notes

- **Stage C is the only one with real logic.** A–B–D–E are mechanical pass-through/dep deletions. Keep
  the `BlockConsensusCell` dispatch + handler bodies identical; this is a rename/unwrap, not a rewrite.
- **No wire/MPT/state/codec impact** — `Cell` never touches serialized or consensus state. No greenfield
  migration concern.
- **Pre-execution greps (run all first):** (1) `io.constellationnetwork.kernel` across all modules to
  confirm the consumer set is exactly {dag-l0, currency-l0, dag-l1} (+ examples); (2) `droste`/`scheme.`/
  `hyloM` to confirm kernel is droste's only consumer; (3) `nodeShared`/`rosetta` kernel refs = 0.
- Sequenceable as 5–6 small PRs (0, A, B, C, D, E) or two (A+0+B, then C+D+E with the two e2es).
