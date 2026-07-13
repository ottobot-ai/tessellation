# Tessellation v4.0.0 Economic Grammar Audit

**Status:** Partial, source-only, non-ratifying audit input

**Date:** 2026-07-12

**Scope:** Upstream framework grammar and authority provenance for `TokenUnlock`,
no-reference `SpendTransaction`, and operations that the current E2 grammar
collapses or omits.

This document is not a protocol specification, owner decision, implementation
claim, or production-readiness finding. It does not resolve
`CONSENSUS-OWNER-DECISIONS.md` O-06, O-07, or O-09. It records source evidence
that E2.1/E2.9 and their RED/oracle suites must cover before an operation is
enabled.

## 1. Verified baseline and limits

The audited upstream baseline is Constellation Labs Tessellation `v4.0.0`:

| Identity | Verified value |
|---|---|
| Upstream remote | `https://github.com/Constellation-Labs/tessellation.git` |
| Annotated tag object | `refs/tags/v4.0.0` = `bae49bf03db49bb19933e7f86d3242e5abb08a34` |
| Peeled commit | `v4.0.0^{commit}` = `22953a1ee835d4d93fb6a0b193599d18c82a284c` |
| Commit subject | `chore: align develop config with release/mainnet (#1464)` |

The current-fork comparison was performed at HEAD
`184f7863df212a38b6eea380258c81870a218d89`. The relevant fork hardening
commit is `c610a0740c34833e563f8a94c2ab820186a75897`,
`refactor(consensus): enforce GL0 economic replay`.

This was a source and history audit. No test was executed for this packet. An
upstream test is evidence of intended and tested behavior, but this packet does
not claim that the test currently passes on either tree.

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
| Protocol balance correction | V4 `BalanceAdjustment` at `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:58-72`; v4 application at `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:490-515` | Preserve the capability, not the upward ML0 authority. The target is a separately typed, root-covered GL0 protocol correction under locked decision L-18 and open gate O-06. |
| Node parameter update | `v4.0.0@22953a1e:modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotEvent.scala:33-34`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/node.scala:162-181` | Changes reward fraction and requires source, reference, replay, and activation rules. |
| Delegated stake create/withdraw | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/delegatedStake.scala:80-100` | Backing lock, amount, fee, replacement, withdrawal delay, and reward payout cannot be one generic `stake` rule. |
| Node collateral create/withdraw | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/nodeCollateral.scala:67-87` | Backing and eligibility lifecycle differ from delegated stake. |
| Reward subtypes | Base value at `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/transaction.scala:144-152` | Operator, delegator, reserved-address, withdrawal, and configured one-time rewards need named mint source, cap, and order. |
| Currency owner/staking messages | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/currencyMessage.scala:23-32,60-73,87-97` | They select fee and staking addresses, so signature, sequence, and activation are economically relevant. |
| Global sync and delivery acknowledgement | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/globalSnapshotSync.scala:27-42,52-74`; `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:74-98` | Exact hash-bound cursor, application acknowledgement, and permanent replay state must not be collapsed to bounded history. |
| State-channel inclusion and snapshot fee | `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:166-174`; binary creation at `v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/services/StateChannelSnapshotService.scala:68-137` | Full genesis, incremental, framework-with-data, DA, and fee-debit semantics differ. Decoder success cannot select authority. |

The current fork deleted v4's `BalanceAdjustment` in `c610a0740`. The target
must not restore that metagraph-originated schema. A replacement GL0 correction
needs exact target metagraph, lineage, pre-root/version, checked correction,
post-root/version, activation, replay protection, full root coverage, and
downstream rebase behavior.

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

1. O-07 remains open: this packet is not a mechanically exhaustive inventory of
   every v4 economic constructor, configuration-driven issuance path, migration
   repair, or route. Freeze requires an automated ADT/codec/event/acceptance
   reachability inventory and human classification.
2. The canonical ML0 operator registry, threshold, rotation, and emergency
   replacement policy for metagraph-source authority is not frozen here.
3. O-06 remains open: the permissionless authorization and activation mechanism
   for a GL0 protocol correction is unresolved. Metagraph-originated authority
   is excluded by locked decision L-18.
4. `PricingUpdate` exists and mutates v4 consensus state, but this packet does
   not ratify whether it ships unchanged or its exact allowed-source policy.
5. O-09 remains open for a pure opaque/data-only lane. This does not affect the
   locked `FrameworkCurrency` and `FrameworkCurrencyWithData` lanes.
6. No runtime, differential, cross-JVM, or adversarial test was run for this
   source-only packet.

Until these items and the required oracle vectors are closed, this document is
evidence for planning E2.1/E2.2/E2.9 only. It is not evidence that the current
economic transition is secure.
