# The `Cell` Construct — Assessment, Intent, and Recommendation

**Status:** investigation complete (read-only). Date: 2026-06-04. **Audience:** a fresh agent or
engineer picking up the "clean up vs. use appropriately" decision on the kernel `Cell`.

## TL;DR

The `Cell` (in `modules/kernel`) is **inherited categorical-FRP boilerplate from the upstream
Constellation `org.tessellation` codebase** (introduced 2021, **frozen since 2023**) that the
Nakamoto team **built *around*, not *on*.** In production it is reduced to:
- two **pass-through** cells (`dag-l0 L0Cell`, `currency-l0 L0Cell`) that wrap a single
  `queue.offer(event)` in a hylomorphism **that never recurses**, and
- one consensus cell (`dag-l1 BlockConsensusCell`) that is a `Cell` **in name only** (`F = Id`,
  `convert = identity` → a flat 5-way dispatch table; `scheme.hyloM` is never called).

The recursion-scheme / Topos apparatus (`Ω`, `StackF`, `cellMonoid`, `ΩList`, `PipeArrow`, the
`Topos` ambition) is **inert outside kernel unit tests**. Removing the whole thing would **not
change runtime behavior**. The team's own code comments call the Cell path a *"pure pass-through …
no-op layer"* they deliberately skip.

**Recommendation: clean it up** (remove the dead apparatus; collapse the cells to plain
functions / direct `queue.offer`). **Do *not* "use it appropriately"** — that would mean
re-architecting working FS2 / typed-FSM code to route through a hylomorphism for theoretical
elegance, with no demonstrated benefit and real risk to consensus paths.

## 1. What the `Cell` is — and how it was *meant* to be used (the design intent)

```scala
// modules/kernel/.../kernel/Cell.scala:10
class Cell[M[_], F[_], A, B, S](val data: A, val hylo: S => M[B], val convert: A => S) extends Hom[A, B] {
  def run(): M[B] = hylo(convert(data))
}
```

- **M** = effect monad (prod: `IO`); **F** = stack functor (`StackF`); **A** = input event;
  **B** = output (`Either[CellError, Ω]`); **S** = coalgebra seed.
- `run()` = `hylo(convert(data))`. The intended `hylo` is droste's `scheme.hyloM(algebra, coalgebra)`:
  **unfold** the seed into an `F`-shaped stack machine (coalgebra / anamorphism), then **fold** it
  back to the output (algebra / catamorphism), **stack-safely** (trampolined).
- Supporting cast: `Ω` (`Hom.scala`) = the *terminal object* every value inhabits; `StackF` =
  `More`(recurse) / `Done`(halt) stack-machine carrier; `CellError`/`Either[CellError, Ω]` = uniform
  error/output channel; `cellMonoid` + `ΩList` = compose many cells into one (`x |+| y`); `PipeArrow`
  = make FS2 `Pipe` a cats `Arrow` so cells compose as stream arrows.
- **Provenance:** original Constellation Topos/categorical-FRP idiom (`Hom` = "Terminal object" `Ω`,
  `Poset` = "Characteristic Sheaf"; `Cell` was meant to `extend Topos` — now `// TODO: was Topos but
  we aren't using it yet`).

**Intended DAG/ledger mapping:** a `Cell` is the **per-event processing unit at the boundary between
ingress (network/HTTP) and the snapshot/consensus pipelines.** Input `A` = a block / state-channel
snapshot / currency event / consensus message; the unfold→fold `S→F→B` = the processing/routing
logic; output `B` = a terminal effect (event enqueued) or a produced `FinalBlock`. **Lifecycle: one
short-lived cell per inbound event, constructed fresh, `run()` once, discarded** — the cell is the
*stateless transition/routing function*; durable state lives outside it (`consensusStorage`, queues).
The monoid + `ΩList` were meant to let independent event-cells be folded and run together; `PipeArrow`
to let them compose as FS2 arrows.

## 2. How it's *actually* used (the three production cells)

| Cell | Input → Output | What it actually does |
|---|---|---|
| `dag-l0 L0Cell` | `L0CellInput` (6 cases) → `Either[CellError, Ω]` | Coalgebra emits `Done(EnqueueX)` on step 1; algebra does `queue.offer(x)`. **Net effect = enqueue.** The hylomorphism never recurses. |
| `currency-l0 L0Cell` | single `HandleCurrencySnapshotEvent` | Identical, thinner: `l1OutputQueue.offer(event)`. |
| `dag-l1 BlockConsensusCell` | `BlockConsensusInput` (5 cases) → `BlockConsensusOutput` | **Real ~580-line consensus logic** — but `F = Id`, `convert = identity`, "hylo" = a bare 5-way pattern match to static `object` handlers. **`scheme.hyloM` is never called.** Cell-in-name-only. |

- The recursion scheme is **never meaningfully exercised** (every coalgebra returns `Done` on the
  first step → a hylomorphism that never recurses = one `flatMap`).
- `cellMonoid` / `ΩList` / `PipeArrow`: **zero production call sites** — exercised only by kernel
  law-suites. `PipeArrow` has **no importers at all**.
- The L0Cell layer is **bypassed**: DAG-L1 blocks reach the same `l1Output` queue via direct
  `queue.offer` (no cell) at `Services.scala:251` and `NakamotoSyncDaemon.scala:1938`.
- The **live Nakamoto consensus FSM** (`node-shared`: consensus functions / event loop / manager /
  state advancer) has **zero** `Cell` references — the entire rework was built outside it.
- Real validation/consensus decisions live in `*Validator` / `*ConsensusFunctions`; the cell `run()`s
  *after*, purely for the `queue.offer` side-effect (its output is discarded with `.as(...)`).

## 3. Is it useful? — verdict: vestigial ceremony, worked-around

Confirmed by the team's own artifacts:
- `NakamotoSyncDaemon.scala:404-409` / `:1938-1942`: *"The Cell pipeline for DAGBlock was a pure
  pass-through in the L0Cell … the new gossip path skips that no-op layer."* — they bypassed it on
  purpose when adding the gossip path.
- Kernel `Cell.scala` **frozen since 2023** (last substantive edit 2023-08-04); `BlockConsensusCell`
  untouched since 2024-04; the only recent Cell commits are *additive enqueue arms* slotted into the
  existing pass-through (KES cert, delegated stake, …) — nobody refactored the abstraction.
- `// TODO: was Topos but we aren't using it yet`; a `CellMonoidLawsSuite` `WARN` admitting the laws
  only hold by comparing `.run()` results.

**It is not load-bearing.** The one nominally-live instance (`BlockConsensusCell`) is a dispatch
table that happens to extend `Cell`; its real logic is free-standing `def`s.

## 4. Why NOT "use it appropriately"

The intended model — stack-safe recursion-scheme *folding* — pays off on **recursive structures**
(DAG segments, MPT tries, fork/tine trees, NIPoPoW towers). Tessellation *has* those — but they are
**already handled well without the Cell** (the MPT code, `ChainSelection`/maxvalid-tk, `TowerVerifier`).
Nakamoto's actual event processing is **flat** ("validate, then enqueue" / "dispatch one consensus
step"), which gains nothing from a hylomorphism. "Using it appropriately" would mean re-routing
working, hot, typed-FSM/FS2 code through `Cell` for elegance — a speculative rewrite with no
demonstrated benefit and real risk to consensus paths. That's the opposite of the team's (correct)
trajectory of building around it with plain Cats-Effect patterns.

## 5. Recommendation: clean up — staged, behavior-preserving

Every stage is a no-behavior-change refactor (the cells are pass-throughs / static dispatch):

- **Stage A — delete the provably-dead apparatus (zero risk):** `PipeArrow` (0 importers),
  `cellMonoid` + `ΩList` (0 prod uses), the `Topos`/sheaf `TODO` stubs in `Hom.scala`; drop the
  `CellMonoidLawsSuite`/`ΩListSuite`.
- **Stage B — collapse the pass-through L0Cells:** replace `dag-l0 L0Cell` + `currency-l0 L0Cell`
  with a thin enqueue helper (or direct `queue.offer` at call sites — one already coexists). Touches
  `Services`/`HttpApi`/the validator-state routes + `StateChannelService` + the currency app wiring.
- **Stage C — de-Cell consensus:** convert `BlockConsensusCell.run()` to a plain
  `def runBlockConsensusStep(input, ctx, ordinal): F[BlockConsensusOutput]` (logic already in the
  companion `def`s); `dag-l1/StateChannel.scala` `runConsensus` calls it directly in `evalMap`.
- **Stage D — drop the module + dependency:** kernel appears to contain *only* the Cell apparatus —
  **verify no non-Cell consumers**, then delete `modules/kernel` and the `droste` dependency
  (`project/Dependencies.scala`), and remove `kernel` from the `dagL0`/`dagL1` deps.

Validate each stage with compile + the touched modules' tests + one e2e (the L0Cell/BlockConsensus
paths are live, even if trivial). **No wire/MPT/state impact** — `Cell` is pure in-memory plumbing,
so this is greenfield-clean (no codec/migration concerns).

**Estimated blast radius:** ~the L0Cell makers in `Services`/`HttpApi` + ~4 validator-state routes +
`StateChannelService` + the currency-l0 app wiring + the one `BlockConsensusCell` run-site; plus the
`modules/kernel` deletion. All mechanical; no consensus-logic change.

## 6. The honest alternative (only if you *want* a recursion-scheme layer)

If the team decides it *wants* a uniform recursion-scheme processing layer as a deliberate principle,
the place it would actually earn its keep is the **recursive structures** (fork/tine selection, MPT
folds, NIPoPoW tower verification) — not the flat ingress events. That's a fresh from-scratch design
proposal, **not** salvage of the current `Cell`. Treat it as a separate RFC, not this cleanup.

## Key files
- `modules/kernel/src/main/scala/io/constellationnetwork/kernel/{Cell,Hom,Stack,ΩList,PipeArrow}.scala`
  (+ `src/test/.../CellMonoidLawsSuite.scala`, `ΩListSuite`)
- `modules/dag-l0/.../domain/cell/L0Cell.scala` (+ `Coalgebra`/`Algebra`/`*Command`)
- `modules/currency-l0/.../cell/L0Cell.scala`
- `modules/dag-l1/.../domain/consensus/block/BlockConsensusCell.scala` (richest; still hollow)
- bypass/no-op evidence: `dag-l0/.../nakamoto/NakamotoSyncDaemon.scala:404-409,1938-1942`
- run-sites: `dag-l1/StateChannel.scala:131-142`; `dag-l0/.../statechannel/StateChannelService.scala:62-67`;
  `Services.scala` / `HttpApi.scala` (L0Cell makers); validator-state routes
- dep: `project/Dependencies.scala` (`droste 0.10.0`)
