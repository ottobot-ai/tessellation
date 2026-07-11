# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Tessellation is the Constellation Network Node Software - a DAG (Directed Acyclic Graph) based distributed ledger with Layer 0 (L0) and Layer 1 (L1) validators. Written in Scala 2.13. Runs as bare JARs in production; test clusters are brought up via `just` recipes and Docker images.

## Build Commands

```bash
sbt compile              # Compile the project
sbt test                 # Run all tests
sbt runLinter            # Format code (scalafmt + scalafix)

# Run tests in specific module
sbt "dagL0/test"
sbt "nodeShared/test"

# Assembly (create JARs)
sbt dagL0/assembly
sbt dagL1/assembly
```

## Docker-based Development

```bash
just test                 # Full test suite with Docker
just test --skip-assembly # Skip compilation, reuse JARs
just up                   # Start test environment
just down                 # Teardown environment
just check                # Lint + format check + tests
```

## Code Quality

Before committing, run:
```bash
sbt runLinter   # Auto-format with scalafmt and scalafix
```

CI runs: `sbt --error 'scalafixAll --check --rules OrganizeImports;scalafmtCheckAll;test'`

Conventional commits required (enforced by commitlint). Format: `type: description`
- Types: feat, fix, refactor, test, docs, chore, build, ci, perf, style, revert

## Architecture

`AGENTS.md` is the normative layer/topology and economic-authority map. In
particular, `dag-l1` is GL1, not a metagraph layer, and all CL1 framework economics
must be re-executed on the way into canonical GL0 state.

Both metagraph-binary admission and execution-shard committees are GL0 operator
committees. Admission uses a real per-binary VRF with uniform `1/N` weight;
execution membership uses a separate public, enumerable VK-hash draw with uniform
`1/N` weight. ML0 signs the binary but is not either committee.

```
modules/
├── shared        # Core data structures, crypto, serialization
├── kernel        # Recursion schemes (Droste), core abstractions
├── keytool       # Key management and cryptography
├── wallet        # Wallet operations
├── node-shared   # P2P networking, consensus, metrics, gossip
├── dag-l0        # GL0: global consensus, settlement, canonical MPT, finality
├── dag-l1        # GL1: native DAG-token edge application; sends directly to GL0
├── currency-l0   # ML0/CL0: metagraph snapshot consensus; submits binaries to GL0
├── currency-l1   # CL1 or DL1 runtime: framework economics and optional custom data app
├── sdk           # SDK for custom metagraph extensions
├── rosetta       # Blockchain data standardization
├── tools         # CLI utilities
└── test-shared   # Test utilities and generators
```

**Module Dependencies:**
- `dagL0` depends on: kernel, shared, keytool, nodeShared
- `dagL1` depends on: kernel, shared, nodeShared
- `currencyL0/L1` depend on: their respective dag layers + nodeShared
- `sdk` depends on all core modules (provided scope)

## Tech Stack

- **Scala 2.13.18** with **Java 21** (enforced at build time)
- **Cats-Effect 3** for async/IO
- **FS2** for streaming
- **HTTP4s** (Ember) for HTTP server/client
- **Circe** for JSON serialization
- **Weaver** for testing (cats-effect based)
- **BouncyCastle** for cryptography
- **Refined types** for validation

## Testing

Tests use Weaver framework with `MutableIOSuite` base class:

```scala
object MySuite extends MutableIOSuite with Checkers {
  override type Res = (Dependency1, Dependency2)

  override def sharedResource: Resource[IO, Res] =
    for {
      dep1 <- createDep1
      dep2 <- createDep2
    } yield (dep1, dep2)

  test("my test") { case (dep1, dep2) =>
    // test implementation
  }
}
```

Test utilities are in `modules/test-shared/`.

## Key Patterns

- Resource-based dependency management with `Resource[IO, _]`
- Refined types for compile-time validation
- Monocle optics for data transformation
- Droste for recursion schemes
- Newtype for zero-cost type wrappers

## Config Conventions

**Always prefer HOCON typed config over `sys.env.get(...)` reads in production code.** Scattered env reads risk cluster-split (different operators run with different defaults for consensus-critical values).

When adding a new tunable, follow this pattern:

1. Add the key to `modules/node-shared/src/main/resources/application.conf` under the `nakamoto { ... }` block (or appropriate namespace)
2. Add a typed field on `NakamotoConfig` (or equivalent) at `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/config/types.scala`
3. Thread the typed config through `SharedConfig.nakamoto.<field>` at use sites — NEVER `sys.env.get(...)`
4. For env override, use HOCON's `${?ENV_VAR_NAME}` substitution **in `application.conf`** — concentrated config-level, not scattered code reads

**If you encounter an existing `sys.env.get("NAKAMOTO_*")` call in code you're modifying, migrate it to HOCON as part of your change** unless the user explicitly says otherwise.

This is a project-wide rule. Sub-agents and humans alike — encapsulate config, don't sprinkle env reads through the codebase.

## Codebase Overview

Tessellation implements a hierarchical DAG consensus with L0 (global) aggregating L1 (metagraph) blocks. The largest module is `node-shared` (419k tokens) providing consensus FSM, anti-entropy gossip, and cluster management. Core data structures (transactions, blocks, snapshots, Merkle Patricia Tries) live in `shared`. Currency modules extend dag-l0/l1 with metagraph-specific logic and extension points for custom data applications.

**Native DAG flow**: client -> GL1 -> GL0.

**Metagraph flow**: CL1 framework-economic blocks and DL1 custom-data blocks ->
ML0 snapshot consensus -> state-channel binary -> GL0. Finalized GL0 state then
flows back to GL1, ML0, CL1, and DL1 as the canonical follower state.

For detailed architecture, file purposes, and navigation guides, see [docs/CODEBASE_MAP.md](docs/CODEBASE_MAP.md).
