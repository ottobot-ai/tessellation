# Tessellation v4.0.0 Economic Grammar Audit

**Status:** Current-source inventory guarded; policy/oracle and activation gates open

**Date:** 2026-07-15

**Scope:** Upstream-v4 framework grammar, current-fork constructors and wire
carriers, codec/decoder reachability, validation and writer boundaries,
configuration/genesis/migration issuance, and authority provenance. The
`TokenUnlock` and no-reference `SpendTransaction` investigations remain the
deep source case studies.

This document is not a protocol specification, owner decision, implementation
claim, or production-readiness finding. O-06, O-07, and O-09 are owner-ratified
in `CONSENSUS-OWNER-DECISIONS-ANSWERS.md`; this source audit neither supersedes
those directions nor closes their remaining engineering grammar/schema gates.
It records source evidence that E2.1/E2.9 and their RED/oracle suites must cover
before an operation is enabled.

## 1. Verified baseline and limits

The audited upstream baseline is Constellation Labs Tessellation `v4.0.0`:

| Identity | Verified value |
|---|---|
| Upstream remote | `https://github.com/Constellation-Labs/tessellation.git` |
| Annotated tag object | `refs/tags/v4.0.0` = `bae49bf03db49bb19933e7f86d3242e5abb08a34` |
| Peeled commit | `v4.0.0^{commit}` = `22953a1ee835d4d93fb6a0b193599d18c82a284c` |
| Commit subject | `chore: align develop config with release/mainnet (#1464)` |

The current-fork comparison packet was refreshed from tranche-start HEAD
`4b09a795e131e328cce165d0e106bea35ba2d487`. The relevant fork hardening
commit is `c610a0740c34833e563f8a94c2ab820186a75897`,
`refactor(consensus): enforce GL0 economic replay`.

The upstream comparison was a source and history audit; no upstream test was
executed for that historical packet. An upstream test is evidence of intended
and tested behavior, but this packet does not claim that the test currently
passes on either tree. The current-fork completeness tripwire described below
is a separate executable suite and was run against the current worktree.

### 1.1 Current-fork source tripwire

E2.9 now has a production-dark structured manifest at
`modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/economics/V4EconomicGrammarManifest.scala`
and a CI guard at
`modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/economics/V4EconomicGrammarCompletenessGuardSuite.scala`.
At the frozen baseline they classify 39 operation identities, 12 economic wire
carriers, and 182 reviewed current-fork sources. V4 feature identity is kept
separate from current activation, so retaining a v4 capability does not approve
its current authority path. Every row records intended and current authority as
separate mandatory fields; an unsafe or missing implementation cannot inherit
the intended authority by default.

The guard resolves operation and carrier declarations, exhaustively checks the
current sealed `SharedArtifact`, `GlobalSnapshotEvent`, delegated-stake,
node-collateral, and slash-reason variants, and scans production sources for new
economic constructors, codecs, decoder ingress, validators, writer-shaped
authority, reward construction, configuration, genesis, and migration gates.
Each classified source has a SHA-256 fingerprint over comment/whitespace-free
lexical tokens. A semantic branch change or a newly discovered unclassified
source fails CI; comment and scalafmt churn does not.

Discovery treats compound `validate*`/`verify*`/`accept*` methods and snapshot
`ConsensusFunctions` as validation boundaries. This is required to cover the
actual `CurrencySnapshotValidator`, `CurrencySnapshotConsensusFunctions`, and
generic `SnapshotConsensusFunctions`; an exact-token search for a method named
only `validate` previously missed them. Fee decision sources are independently
discovered and classified, including `FeeCalculator`,
`SnapshotBinaryFeeCalculator`, and the ML0 `StateChannelSnapshotService` that
constructs the signed outer fee claim.
The broadened pass surfaced and froze eight additional existing boundaries that
the old exact-token/path predicates missed: the DL1 data-consensus engine, block
acceptance manager, generic consensus contract, delegated-stake validator,
node-collateral validator, shared data-transaction validator, shared fee
validator, and fee-prioritized global event cutter.

This is a source tripwire, not the economic reference interpreter, conservation
oracle, authorization proof, or activation gate. In particular, the manifest
marks offline upstream-v4-to-new-chain migration issuance `MissingFailClosed`;
there is no runtime source row claiming that missing transform exists. Passing
this suite does not make any `ActiveNeedsOracle`, `ActiveSourceProvenUnsafe`, or
`ActiveWithOpenConservationGate` operation production-safe.

The suite pins these current unsafe behaviors as RED evidence:

- manual `TokenUnlock` has no owner-signed framework intent or permanent replay
  identity;
- no-reference metagraph-source spend has no rooted ML0 operator threshold and
  writes no permanent nullifier/replay identity;
- `GlobalSnapshotsProcessed` replay memory is reconstructed from caller-supplied
  bounded history instead of an exact consensus-pinned GL0 replay cursor;
- framework data fees have a source signature but no parent/reference head and
  do not advance `lastFeeTxRefs`; the current effect therefore does not claim a
  `ReferenceAdvance` that source never performs;
- a state-channel binary may claim any fee at or above the deterministic
  recommendation; GL0 debits that full claim from the owner-message address but
  credits no recipient and adjusts no supply field, making the current branch
  an overchargeable burn/unaccounted sink rather than a transfer;
- genesis/full and first-incremental state-channel binaries pass the same
  outer-signature and minimum-fee validation without requiring an owner message,
  but take a distinct processor branch that performs no fee debit at all; only
  subsequent incrementals require an owner-message payer for the debit/burn
  branch;
- native/currency transfer, allow-spend, and token-lock fees are debited without
  a matching recipient credit or supply-accounting write. The inventory records
  those current sinks as `Burn`, not `FeeTransfer`; only framework data-fee
  transactions currently move the fee from a source to a destination;
- delegated-stake and node-collateral create requests bind an already-funded
  `tokenLockRef`; their acceptance managers do not debit the balance or consume
  their request `fee` field again. Withdrawal requests update backing records,
  not spendable balances. Delegated-stake expiry later generates a canonical
  `TokenUnlock`; node-collateral expiry currently has no analogous release path,
  so `ECO-NODE-COLLATERAL-RELEASE` is explicitly `MissingFailClosed`;
- an absent pricing allowlist authorizes every metagraph;
- total supply is not a rooted field or writable accumulator. The API derives it
  from balances, active token locks, and delegated-stake rewards. Manifest rows
  therefore record actual balance mint/burn effects and do not claim a
  `SupplyDeclaration`/`SupplyWriter` that source does not contain;
- invalid-state slashing reduces or removes stake/collateral records, leaves the
  backing token locks in place, uses its computed principal `burned` value only
  in a log, and credits the submitter bounty into balances. A full delegated-
  stake slash also removes the record's accumulated rewards, which the derived
  supply calculation counts, so the current effect union contains both the
  bounty `Mint` and that reward `Burn`. Neither effect makes the claimed
  principal burn real;
- current GL0 wiring supplies no deterministic currency-reward implementation.
  Re-execution substitutes an empty reward set, so exact artifact comparison
  rejects a non-empty metagraph reward claim. `ECO-REWARD-CURRENCY` is
  `MissingFailClosed`, not active;
- the L0 genesis loader signs arbitrary delegated-stake and node-collateral
  fixture events and installs them as active records while token-lock state is
  empty; a one-unit address stipend is not backing for the event amount. The
  delegated-stake fixture also installs arbitrary accumulated rewards counted by
  derived supply, so its genesis-only row records a `Mint`. The two genesis-only
  backing operations are separate unsafe rows and are excluded from ordinary
  incremental/event carrier sets;
- standalone opaque state-channel binaries are retained only before fee
  activation or while fees are waived. Once fees are required the processor has
  no authenticated opaque fee-payer lane and drops/stops the opaque chain, so a
  distinct fee-era row is `MissingFailClosed`;
- framework-with-data `DataApplicationBlock` carriage and standalone opaque
  state-channel carriage are distinct rows and carrier sets. The full
  `CurrencySnapshot` carrier is limited to fields it actually contains rather
  than inheriting the full incremental framework grammar;
- allow-spend-backed settlement, whether local or cross-metagraph, is authorized
  by an unsigned `SpendAction` / `SpendTransaction` checked against the
  referenced rooted signed `AllowSpend`. `ConsumedAllowSpend` is the resulting
  cross-metagraph permanent spent-set record, not the constructor or source of
  authority; and
- custom data execution can synthesize framework effects. Specifically,
  `DataApplicationSnapshotAcceptanceManager.scala` accepts
  `newDataState.sharedArtifacts`, calls
  `service.getTokenUnlocks(newDataState)`, and unions both into the framework
  acceptance input. Arbitrary DL1 application output can therefore currently
  reach `SpendAction`, allow-spend expiry, pricing, and unsigned token-unlock
  framework transitions. The exact live site is
  `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/DataApplicationSnapshotAcceptanceManager.scala:245-251`;
  `ECON-LANE-001` remains open and the opaque lane is carriage-only by target
  policy. `GlobalSnapshotsProcessed` is not injectable through this path:
  `CurrencySnapshotAcceptanceManager.filterFrameworkGeneratedArtifacts` removes
  any supplied acknowledgement and the manager regenerates it from the exact
  GL0 ordinals actually processed at
  `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:82-87,296-300,631-645`.

## 2. Exact upstream path

The two operations examined here are not ordinary CL1 transaction block types.
Upstream keeps normal CL1 blocks, allow-spend blocks, token-lock blocks, and data
application blocks as distinct ML0 events:
`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/snapshot/currency.scala:20-52`.

Their demonstrated v4 path is:

```text
Signed[DL1 DataUpdate]
  -> Signed[DataApplicationBlock]
  -> ML0 data-application validation and combine
  -> DataState.sharedArtifacts
  -> CurrencySnapshotCreator framework acceptance
  -> Signed[CurrencyIncrementalSnapshot]
  -> Signed[StateChannelSnapshotBinary]
  -> GL0 state-channel and inner-snapshot validation
  -> GL0 framework artifact validation and balance/state application
```

The enforcing and non-enforcing sites are:

1. DL1 checks that a submitted data transaction has a cryptographically valid
   signature, but does not generically bind the proof ID to an economic source
   field:
   `v4.0.0@22953a1e:modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/http/DataApplicationRoutes.scala:103-140`.
2. DL1 consensus validates through application callbacks, constructs a
   `DataApplicationBlock`, and signs it:
   `v4.0.0@22953a1e:modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/domain/dataApplication/consensus/Engine.scala:323-373`.
   The finalized block is sent to ML0 at
   `v4.0.0@22953a1e:modules/currency-l1/src/main/scala/io/constellationnetwork/currency/l1/DataApplication.scala:130-145`.
3. ML0 validates data transactions through the metagraph's L0 callback, runs
   `combine`, and takes the resulting shared artifacts:
   `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/DataApplicationSnapshotAcceptanceManager.scala:162-205,231-271`.
4. `CurrencySnapshotCreator` supplies those artifacts to framework acceptance:
   `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotCreator.scala:170-197`.
5. ML0 validators recreate the proposed currency artifact before the ML0
   majority artifact is signed:
   `v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusFunctions.scala:56-77` and
   `v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusStateAdvancer.scala:220-265`.
6. ML0 serializes the signed incremental into, and separately signs, a state
   channel binary:
   `v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/StateChannelSnapshotService.scala:101-137`.
7. GL0 validates the outer binary signature and requires at least one seedlist
   signer and, when configured, one allowance-list signer:
   `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala:92-134,164-199`.
8. GL0 cryptographically verifies the inner incremental and recreates its
   framework transition:
   `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala:59-116,158-188`.
   In v4, a GL0 instance without the custom data application passes
   `expected.artifacts` into recreation and then replaces recreated artifacts
   with the expected artifacts at lines 171-188. It therefore checks resulting
   framework behavior, not custom-origin authority.
9. GL0 extracts `SpendAction` from accepted currency incrementals, validates it,
   and applies its balance effects:
   `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:736-805,1001-1016`.

Current commit `c610a0740` removes the wholesale artifact replacement, but it
still supplies the claimed artifacts as framework inputs at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala:156-180`.
GL0 constructs that validator without a data application at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/modules/SharedServices.scala:211-223`.
Re-execution consequently checks deterministic transition behavior, but cannot
by itself prove that unknown DL1 logic had authority to construct the framework
input.

## 3. ECO-03: unsigned `TokenUnlock`

### 3.1 Adjudication

**Confirmed:** Manual token unlock is intentional upstream v4 functionality,
not an abandoned fork-only schema. Its v4 authorization is defective.

The wire value is an unsigned `SharedArtifact`:
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:35-41`.
The current fork retains the same shape at
`modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:35-40`.

The official v4 project template deliberately translates a custom update into
that framework artifact:

- Update schema:
  `v4.0.0@22953a1e:.github/templates/metagraphs/project_template/modules/shared_data/src/main/scala/com/my/project_template/shared_data/types/Types.scala:62-68`.
- Artifact construction:
  `v4.0.0@22953a1e:.github/templates/metagraphs/project_template/modules/shared_data/src/main/scala/com/my/project_template/shared_data/LifecycleSharedFunctions.scala:15-32` and
  `v4.0.0@22953a1e:.github/templates/metagraphs/project_template/modules/shared_data/src/main/scala/com/my/project_template/shared_data/combiners/Combiners.scala:43-55`.
- The template's L1 and L0 semantic validators accept every update:
  `v4.0.0@22953a1e:.github/templates/metagraphs/project_template/modules/data_l1/src/main/scala/com/my/project_template/data_l1/Main.scala:40-45` and
  `v4.0.0@22953a1e:.github/templates/metagraphs/project_template/modules/l0/src/main/scala/com/my/project_template/l0/Main.scala:54-68`.

The upstream integration workflow signs the manual-unlock update with the
source user's private key and verifies pre-expiry lock removal and refund:
`v4.0.0@22953a1e:.github/action_scripts/send_transactions/token-locks.js:56-65,235-258,304-339`.
That is strong evidence that the intended rule is owner-initiated manual unlock,
not arbitrary metagraph authority.

### 3.2 Proven defect and current delta

V4 accepts an incoming unlock when its reference names any active, unexpired
lock:
`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala:73-81`.
It removes the referenced lock and refunds the artifact's caller-supplied amount
at lines 38-57 and 83-130.

The upstream test demonstrates a conservation failure: a live lock contains
100, the unsigned unlock claims 200, and the test expects
`500 - 100 + 200 = 600`:
`v4.0.0@22953a1e:modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManagerSuite.scala:285-325`.

Current `c610a0740` partially repairs this by building the canonical lock map at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:515-540`
and requiring exact amount, currency, and source equality at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala:161-173`.

That is not authorization. A Byzantine ML0 can copy those public fields from a
victim's active lock, pass the equality predicate, remove the lock at lines
38-57, and trigger the refund at lines 117-124 before the signed unlock epoch.
The amount-mismatch mint is repaired; vesting, escrow, or backing conditions can
still be defeated.

### 3.3 Required target property

Preserve two distinct operations:

1. **Deterministic expiry:** derive the unlock from the signed canonical lock and
   consensus epoch. Upstream's default derivation is at
   `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala:367-395`.
2. **Manual owner unlock:** accept only an explicit framework-lane
   `Signed[TokenUnlockIntent]`. The signer-derived address must equal the
   canonical lock source. Amount, currency, and refund source are derived from
   the canonical referenced lock, not supplied as authoritative intent fields.
   The intent is domain separated and permanently nullified.

For `FrameworkCurrencyWithData`, the owner-signed framework intent may bind the
exact custom-data commitment. Custom `combine` output cannot synthesize it.

## 4. ECO-04: no-reference `SpendTransaction`

### 4.1 Adjudication

**Confirmed:** The no-reference branch is intentional upstream v4
metagraph-source spend functionality, not a fork regression. Deleting the branch
would regress v4.

The unsigned shape is defined at
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:23-33`.
The upstream test explicitly says
`should validate spend action with both metagraph-issued transactions` and uses
no references with `source == currencyId`:
`v4.0.0@22953a1e:modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidatorSuite.scala:203-225`.
Mixed user allow-spend and metagraph-source examples are at lines 42-114 and
298-342.

The v4 integration workflow constructs `allowSpendRef: null`, sets the source to
`CURRENCY_TOKEN_ID`, and carries it in a custom update signed by an unrelated
test key:
`v4.0.0@22953a1e:.github/action_scripts/send_transactions/allow-spends-and-spend-transactions.js:1085-1152`.
**Inference from the named upstream test and workflow:** the intended authority
is the metagraph's control of its own source balance, not the custom-update
signer pretending to own that address. The source does not define a globally
anchored operator threshold that safely realizes that intent.

V4 validates only that the amount fits the emitting metagraph's balance and
that `source == currencyId`:
`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala:242-256`.
Current code preserves this rule at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala:319-328,372-382,517-531`.

The branch moves value. V4 applies it in global state at
`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/SpendTransactionBalanceManager.scala:28-81`
and in metagraph currency state at
`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/AllowSpendOpsManager.scala:159-209`.
Current global application is at
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/SpendTransactionBalanceManager.scala:43-99`.

### 4.2 Proven authority gap

ML0 does recreate and sign its majority artifact in the normal upstream path,
but GL0 does not anchor the inner signer population and threshold to a canonical
metagraph registry. Current GL0 verifies the inner signatures, then derives the
facilitator set from those same artifact proofs:
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala:56-74,96-105`.

The outer state-channel check requires one seedlist signature and, if an
allowance list exists, one member of it:
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala:165-200`.
Neither check proves that the registered ML0 operator threshold authorized this
specific metagraph-source spend.

The defect should therefore be stated precisely: v4 defines metagraph-source
authority, but the current GL0 transition does not canonically bind that
authority to a registered operator set, threshold, domain, sequence, and
single-use intent.

### 4.3 Required target property

Preserve the feature as an explicit framework `MetagraphSpendIntent`:

- derive `source` from the metagraph ID;
- bind exact currency, destination, amount, sequence or semantic ID, and
  optional custom-data commitment;
- verify a rooted registration of metagraph ID to ML0 operator set and threshold;
- spend only the canonical balance at the metagraph's own address;
- execute cumulatively against one exact Phase-2 base with checked arithmetic;
- consume a permanent nullifier; and
- route cross-metagraph effects through the GL0-owned settlement kernel.

The execution committee verifies this framework authority and transition before
signing. It need not understand the custom application's business logic. An
arbitrary custom payload, arbitrary data-update signature, or single outer
binary signer cannot satisfy the framework authorization.

## 5. Partial v4 grammar-gap inventory

This is a minimum inventory, not proof that every v4 operation has been found.
The E2 reference interpreter needs distinct rows where authority, ordering,
replay, source/sink, or state transition differs.

| Operation or variant | Upstream evidence | Why a separate grammar row is required |
|---|---|---|
| Native and currency transfers | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/transaction.scala:43-83,97-117` | Separate currency namespace, parent reference, salt, fee, and replay rules. |
| Allow-spend create, consume, expiry/refund | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/swap.scala:43-97`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:43-46` | Reservation, approver, expiry, refund, and single-use consumption are different transitions. |
| Allow-spend-backed vs metagraph-source spend | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:23-33` | Owner authorization and referenced reservation differ from rooted ML0 threshold authority. DAG, same-MG, and cross-MG targets also read different state. |
| Token-lock create, replacement, expiry, manual unlock | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/tokenLock.scala:82-93`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:35-41` | Signed creation/replacement, deterministic expiry, and owner-triggered early release have different authority and backing effects. |
| Distinct fee lanes | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/transaction.scala:43-49`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/swap.scala:43-49`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/tokenLock.scala:56`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala:53`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/nodeCollateral.scala:40`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala:102-119`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:166-174` | Signer, destination, binding, replay key, debit time, and source/sink differ. |
| `PricingUpdate` | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:48-56`; validation at `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/priceOracle/PricingUpdateValidator.scala:39-110`; application at `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala:1051-1055` | This is an existing named v4 operation affecting consensus price state. Its allowed-source and frequency policy must be explicit; do not invent a differently named authority. |
| Protocol balance correction | V4 `BalanceAdjustment` at `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:58-72`; v4 application at `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:490-515` | Preserve the capability, not the upward ML0 authority. The target is a separately typed, root-covered GL0 protocol correction under locked decision L-18 and O-06's deferred engineering schema gate; no owner answer remains pending. |
| Node parameter update | `v4.0.0@22953a1e:modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotEvent.scala:33-34`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/node.scala:162-181` | Changes reward fraction and requires source, reference, replay, and activation rules. |
| Delegated stake create/withdraw/release | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala:80-100` | Create and withdrawal requests mutate records backed by a pre-funded token lock; they do not debit or refund principal. Deterministic withdrawal expiry separately generates the `TokenUnlock` that releases the lock. |
| Node collateral create/withdraw/missing release | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/nodeCollateral.scala:67-87` | Create and withdrawal requests mutate collateral records backed by a pre-funded token lock. The current expiry path drops pending withdrawals but does not generate the analogous token unlock, so release is a named missing operation rather than a false request-side balance credit. |
| Genesis stake and collateral backing | Current loader at `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisLoader.scala:167-235`; fixture balances at `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala:189-221` | Genesis installs active stake/collateral records independently of empty token-lock state. These genesis-only operations cannot be conflated with runtime creates that validate a pre-funded lock. |
| Reward subtypes | Base value at `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/transaction.scala:144-152` | Operator, delegator, reserved-address, withdrawal, and configured one-time rewards need named mint source, cap, and order. Current GL0 wiring rejects non-empty currency rewards because no deterministic implementation is registered, so retained wire syntax is not current activation. |
| Currency owner/staking messages | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/currencyMessage.scala:23-32,60-73,87-97` | They select fee and staking addresses, so signature, sequence, and activation are economically relevant. |
| Global sync and delivery acknowledgement | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/globalSnapshotSync.scala:27-42,52-74`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:74-98` | Exact hash-bound cursor, application acknowledgement, and permanent replay state must not be collapsed to bounded history. |
| State-channel inclusion and snapshot fee | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:166-174`; binary creation at `v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/StateChannelSnapshotService.scala:68-137` | Full genesis and first incremental accept a fee claim without debiting it; subsequent incrementals debit the owner-message address and credit nobody. Those are distinct grammar rows. Framework-with-data and opaque DA carriage also remain distinct. Current standalone opaque carriage stops when fees activate because it has no fee-payer lane; decoder success cannot select authority. |

The current fork deleted v4's `BalanceAdjustment` in `c610a0740`. The target
must not restore that metagraph-originated schema. A replacement GL0 correction
needs exact target metagraph, lineage, pre-root/version, checked correction,
post-root/version, activation, replay protection, full root coverage, and
downstream rebase behavior.

### 5.1 Landed nonactivating reference slice

As of 2026-07-16, the test-only reference interpreter supports exactly four
typed operation IDs: zero-fee native transfer, zero-fee currency transfer,
zero-fee allow-spend creation, and zero-fee nonreplacement token-lock creation.
Allow-spend creation requires a complete
source-bound preimage, exact domain and lane, exact per-source parent, and an
explicit supplied epoch window. It uses checked `BigInt` arithmetic, moves the
amount from spendable balance into an active reservation without crediting the
destination, advances a separate allow-spend reference, retains a permanent
semantic identity, and rejects invalid/replayed prefixes atomically.

Token-lock creation requires the corresponding complete source/preimage,
domain/lane/parent, and explicit supplied epoch policy. It moves principal from
spendable balance into the active-lock ledger, advances a separate token-lock
reference, retains permanent replay identity, and shares the ordered conserved
balance accumulator with transfers and allow-spends.

The supported-ID set is closed: each supported ID has exactly one positive
constructor mapping, and the unsupported sentinel cannot represent a supported
ID. Every remaining manifest row is discovered dynamically and fails closed.
The interpreter, allow-spend, token-lock, and manifest-coverage suites pass 41
focused tests.

A bounded test-only production adapter exercises the real lower native
acceptance managers and currency ML0 wrappers for zero-fee transfer and
allow-spend creation. It checks production signature/source ownership,
canonical lane genesis, exact parents, accepted-block payload binding, exact
exposed balance/reference state, single-operation active allow-spends, and real
one-call mixed-outcome batches against the reference model. Raw reference inputs
are not exposed. The 13 focused characterization tests pass.

This is not production-kernel implementation, the complete GL0 acceptance path,
E2.8 completion, canonical Scodec/hash/signature binding, MPT/root integration,
or runtime activation. Production batch insufficiency awaits while the reference
row rejects; live GL0's allow-spend epoch rule differs from the target window;
legacy signatures omit domain/lane; production exposes no independent replay-ID
or write-order evidence and no batch allow-spend active-record delta; and
single-result observers accept caller-supplied production results. Nonzero fees
remain fail-closed because their disposition is not frozen. Live snapshot
acceptance does not yet enforce the reference row's contextual token-lock
minimum-duration rule. Token-lock replacement, expiry/refund, and manual unlock
remain open. Allow-spend consume, expiry, and refund remain blocked on O-13
terminal ordering. Every other grammar row remains open.

## 6. Required RED and oracle tests

These names are proposed test obligations, not ratified identifiers. Every test
must compare acceptance/rejection, ordered semantic IDs, exact canonical diff,
post-root, extracted global intents, and resource units between the small
reference interpreter and production kernel.

| Test | Required assertion |
|---|---|
| `ECON-AUTH-001 forged-manual-unlock` | Copy the exact public ref/amount/currency/source of another owner's unexpired lock. No artifact, lock removal, refund, extracted intent, or diff is accepted. |
| `ECON-AUTH-001 owner-manual-unlock` | The lock owner signs the domain-separated intent. The kernel derives fields from the canonical lock, refunds exactly once, removes the exact lock, and writes the permanent nullifier. |
| `ECON-AUTH-001 expiry-vs-manual` | Expiry derives from consensus epoch without an owner intent; manual release before expiry requires one. Both routes produce the same conservation result when applied at their valid boundary. |
| `ECON-AUTH-001 tampered-custom-binding` | Change one byte of custom data after signing a bound framework intent. The framework and custom portions reject atomically with no partial effect. |
| `ECON-AUTH-002 unregistered-or-subthreshold-MG-spend` | A no-ref spend signed by arbitrary inner keys or fewer than the registered ML0 threshold rejects even if source equals MG and balance is sufficient. One allowed outer signer cannot upgrade it. |
| `ECON-AUTH-002 registered-threshold-MG-spend` | A registered ML0 threshold authorizes an explicit metagraph-source intent; committee replay accepts it and debits only the MG-address balance. This is the v4 functionality-preservation vector. |
| `ECON-REPLAY-MGSPEND-001` | Re-include the same metagraph-source semantic ID in another binary, checkpoint, shard, and GL0 branch. At most one canonical effect occurs. |
| `ECON-RESERVE-001 cumulative-no-ref-spends` | Two spends of 60 from 100 are evaluated in canonical order against one accumulator. No exception occurs; accepted/rejected prefix and exact balance are deterministic under every input permutation normalization. |
| `ECON-LANE-001 custom-cannot-construct-framework` | Arbitrary DL1 output containing bytes that decode to `TokenUnlock`, `SpendAction`, fee, reward, or correction cannot enter the framework input ADT without its separately valid framework authorization. |
| `ECON-F-004 exact-snapshot-fee-and-sink` | A subsequent incremental accepts only the exact deterministic fee under the target rule. The current minimum-only overcharge and debit-without-recipient behavior stays RED until the fee sink and supply equation are ratified. |
| `ECON-F-004A first-incremental-no-debit` | Full/genesis and first-incremental fee claims take their explicitly classified no-debit path. They cannot be mistaken for a paid fee or satisfy a conservation assertion that assumes the subsequent-incremental debit. |
| `ECON-F-005 fee-source-sink` | For every fee-bearing operation, the oracle names the debited source, credited recipient, and supply delta. Current debit-only transaction, allow-spend, and token-lock fees are burn/unaccounted sinks; a `FeeTransfer` classification requires a matching destination credit. |
| `ECON-F-006 pre-funded-backing-no-double-debit` | Delegated-stake and node-collateral create/withdraw requests bind the canonical pre-funded token lock and perform no second principal/fee debit or immediate refund. Delegated expiry releases exactly that lock once. |
| `ECON-COLLATERAL-RELEASE-001` | An expired node-collateral withdrawal must generate and apply the canonical token-lock release exactly once. Until that path exists, the release operation remains `MissingFailClosed` and the request path cannot claim a balance credit. |
| `ECON-SUPPLY-001 derived-supply-and-slash` | Every accepted transition preserves the explicit equation over balances, active token locks, and delegated-stake rewards. A slash cannot reduce only a backing record, retain the backing lock, log a fictitious burn, and mint a bounty balance. |
| `ECON-REWARD-CURRENCY-001 deterministic-registration` | A non-empty currency reward set rejects while no deterministic active-era reward implementation is registered. Once registered, every execution signer recreates the exact reward set and GL0 adoption verifies the same diff/root. |
| `ECON-OPAQUE-FEE-001 fee-era-carriage` | Fee activation cannot silently drop a previously supported opaque chain. Either a separately authenticated fee-payer mechanism accepts it as carriage-only or the fee-era lane remains explicitly unavailable. |
| `ECON-GENESIS-BACKING-001` | Every genesis stake/collateral amount is backed by an exact genesis token lock or an explicit genesis issuance/conservation rule. An arbitrary fixture event plus one-unit signer stipend cannot create unbacked eligibility weight. |
| `ECON-CORRECTION-001 trust-direction` | ML0/CL1/DL1 attempts to construct a correction reject. The active-era GL0 protocol rule applies the same root-covered correction on every GL0 node and downstream nodes rebase from the exact containing Phase-2 hash. |
| `V4-GRAMMAR-PARITY-*` | Golden v4 fixtures for every retained operation reproduce intended valid functionality. Fixtures that encode upstream authorization, inflation, replay, or ordering defects must reject under a named new rule rather than silently disappear. |
| `V4-GRAMMAR-UNSUPPORTED-001` | Every v4/fork economic constructor absent from the frozen grammar fails closed before diff construction or signing. |
| `V4-GRAMMAR-DOMAIN-001` | A signature valid for data update, outer binary, custody, execution, optimistic finality, or another metagraph cannot be replayed as a framework authorization. |

The oracle corpus must include same-MG, DAG-target, cross-MG, multi-binary,
multi-metagraph, reorg/replay, restart, arithmetic-boundary, invalid-signature,
wrong-domain, stale-base, and duplicate-ID cases. At least the production and
reference kernels must be tested after every ordered input prefix, not only on
the final root.

## 7. Residual unknowns

1. O-07's direction is owner-ratified. The current ADT/carrier/codec/decoder/
   validator/writer/configuration/genesis/migration source inventory is now
   mechanically guarded at its frozen worktree baseline, but scanner discovery
   plus human source classification is only a completeness tripwire. The
   operation semantics, conservation equations, authorization domains, exact
   ordering, replay identities, and v4 golden vectors still require the E2
   reference interpreter and differential oracle before activation.
2. The canonical ML0 operator registry, threshold, rotation, and emergency
   replacement policy for metagraph-source authority is not frozen here.
3. O-06's direction is owner-ratified, but its engineering schema and activation
   mechanism for a GL0 protocol correction remain unresolved.
   Metagraph-originated authority is excluded by locked decision L-18.
4. Offline v4 state export/transform into new-chain genesis is explicitly
   `MissingFailClosed`. The current genesis loaders and ordinal reward-migration
   switch are classified, but they are not that hard-fork migration pipeline.
5. The live DL1 shared-artifact injection, unsigned manual unlock, no-reference
   metagraph spend, bounded global-processing acknowledgement, replayable data
   fee, debit-only fee sinks, overchargeable/unaccounted state-channel fee sink,
   first-incremental no-debit fee branch, missing node-collateral release,
   optional-empty pricing allowlist, disabled currency rewards, fee-era opaque
   rejection, unbacked genesis stake/collateral, derived-supply accounting, and
   inflationary slash-conservation gates remain open defects. The tripwire
   prevents silent drift; it does not repair them.
6. `PricingUpdate` exists and mutates v4 consensus state. Current source treats
   an absent allowlist as permission for every metagraph; the intended rooted
   active-era allowlist and activation policy remain open.
7. O-09's pure opaque/data-only lane remains an engineering/schema gate. Current
   source carries it only before fee activation/while fee-waived; the fee-era
   row is `MissingFailClosed`. This is distinct from the locked
   `FrameworkCurrency` and `FrameworkCurrencyWithData` lanes.
8. No runtime economic oracle, differential production/reference-kernel,
   cross-JVM, or full adversarial corpus was run for this source inventory. The
   20-test tripwire suite is structural and RED evidence only.

Until these items and the required oracle vectors are closed, this document is
evidence for planning E2.1/E2.2/E2.9 only. It is not evidence that the current
economic transition is secure.
