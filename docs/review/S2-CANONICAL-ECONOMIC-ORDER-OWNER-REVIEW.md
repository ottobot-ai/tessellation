# S2 canonical economic order owner review

**Status:** OWNER REVIEW REQUIRED. This bounded creation-order proposal neither
freezes the complete economic schedule nor authorizes production activation.

## 1. Decision boundary

The first reference slice contains only zero-fee transfer, zero-fee allow-spend
creation, and zero-fee nonreplacement token-lock creation. Native and currency
transfer are distinct typed IDs; every other grammar row fails closed
(`V4-ECONOMIC-GRAMMAR-AUDIT.md:430-450`). Its mixed adapter uses
`Transfer < AllowSpendCreate < TokenLockCreate` only as evidence scaffolding,
not protocol law (`V4EconomicProductionProjection.scala:365-388`).

Approval freezes only that relation and the bounded funding/retry rules below.
It does **not** freeze the within-family comparator/Scodec identity; fees,
rewards, SpendActions, terminal/replacement/expiry operations; stake,
collateral, parameters, slashing, corrections; or the relative rank of
maintenance, issuance, burn, and settlement. Those remain fail-closed gates.

## 2. What is already ratified

There are only two ratified order edges relevant here:

- exact Phase-2 GL0 inbox deliveries apply before ML0-local spend
  (`CONSENSUS-OWNER-DECISIONS-ANSWERS.md:377-396`);
- slash adjudication applies before release in the same candidate
  (`CONSENSUS-OWNER-DECISIONS-ANSWERS.md:679-690`).

The **target invariant**, not another ratified edge, requires atomic, conserved,
replay-protected operations over one accumulator with no partial rejection
writes (`CONSENSUS-ECONOMIC-SECURITY-ROADMAP.md:657-668`). None of this ranks the
three creation families. O-13 terminal order and token-lock expiry remain open
(`O13-ALLOW-SPEND-TERMINAL-ORDER-OWNER-REVIEW.md:246-261`;
`TOKEN-LOCK-EXPIRY-OWNER-REVIEW.md:190-224`).

The layer architecture is also not an ordering rule:

- **GL1 native path:** every GL0 validator authenticates and executes every
  native DAG-token operation at the exact proposal parent.
- **CL1 sharded path:** the checkpoint producer and every execution signer
  replay the exact ordered CL1 inputs before signing; assigned watchtowers
  independently replay as the collusion backstop.
- **Ordinary CL1 adoption:** a noncommittee GL0 validator does **not** replay
  ordinary CL1. It verifies committee identity/certificate, exact base, scope,
  continuity, namespace-bounded diff, and recomputed root, then applies the
  certified diff.
- **Global settlement:** every GL0 validator executes the small deterministic
  conflict/nullifier/settlement kernel over checkpoint-extracted global intents.
  That universal kernel is not universal replay of the CL1 transition.

## 3. Confirmed divergence

The live layers do not share one heterogeneous accumulator:

- GL0 accepts transfers first
  (`GlobalSnapshotAcceptanceManager.scala:698-719`), independently accepts
  allow-spend and token-lock blocks from the parent state
  (`GlobalSnapshotAcceptanceManager.scala:1235-1264,1888-1921`), then applies
  allow-spend balance changes before token-lock balance changes
  (`GlobalSnapshotAcceptanceManager.scala:2521-2549,2586-2595`).
- ML0 accepts transfers and derives their balance update first
  (`CurrencySnapshotAcceptanceManager.scala:259-287`), independently accepts
  token locks and allow-spends in parallel from the old snapshot context
  (`CurrencySnapshotAcceptanceManager.scala:415-434`), then applies token-lock
  debits before allow-spend debits
  (`CurrencySnapshotAcceptanceManager.scala:517-565`).

The differential reproduces this: balance `100` admits allow-spend `60` and
token lock `60`; GL0 applies allow-spend first and underflows the lock, while
ML0 does the reverse (`V4EconomicProductionDifferentialSuite.scala:3388-3620`).
Transfer `60` plus lock `60` double-admits and underflows in both layers
(`V4EconomicProductionDifferentialSuite.scala:3154-3385`).

### Concrete failure

```text
balance(scope, A) = 100
lastAllowSpendRef(A) = genesis
lastTokenLockRef(A) = genesis
allowSpendCreate(A, amount = 60, fee = 0, parent = genesis)
tokenLockCreate(A, amount = 60, fee = 0, parent = genesis)
```

Both family managers see the same parent balance and mark their input accepted.
Combined demand is `120`; the later fold reaches `AmountUnderflow` after
opposite first debits. A producer can repeatedly waste its slots with jointly
unfunded families. Different accidental call orders derive different accepted
IDs/diffs/roots: fail-closed comparison blocks the checkpoint, while accepting
one finalizes a result another honest replayer rejects.

This is **CONFIRMED** as double-admission and candidate failure. Finalization of
the divergent result is not reproduced; that consequence is an activation risk
until the unified kernel is load-bearing.

## 4. S2-ORDER-01: bounded family rank

**Recommendation:** freeze this rank for the first supported slice:

```text
Transfer < AllowSpendCreate < TokenLockCreate
```

The rank determines attempt order only. S2-ORDER-03 separately decides retry,
disposition, and candidate atomicity. GL0 native execution, ML0 execution,
execution-checkpoint producer/every execution signer/assigned watchtower replay,
and the differential oracle must return identical ordered decisions and writes.
Ordinary noncommittee GL0 adoption verifies the resulting
certificate/diff/root contract.

Consequences:

- a transfer has priority over a new reservation when one source cannot fund
  both;
- an allow-spend reservation has priority over a new token lock under the same
  conflict;
- this aligns the bounded slice with current GL0 application order and the
  existing reference scaffold, but intentionally replaces ML0's opposite
  allow-spend/token-lock order; and
- it does not decide any terminal or maintenance precedence.

Alternatives:

1. Introduce one domain-bound, signed framework sequence/reference shared by
   transfer, allow-spend, and token-lock operations. It gives owner-chosen
   same-source cross-family order and rejects conflicting successors, but is a
   larger schema change and still needs a deterministic cross-source rule.
2. `TokenLockCreate < AllowSpendCreate < Transfer`: prioritizes locking over
   payment and reverses existing GL0/reference behavior.
3. Sort all families by a user-influenced operation hash: salts or otherwise
   mutable signed fields grind economic priority unless a separate
   precommitment proof is designed.
4. Make any aggregate funding conflict invalidate the whole candidate. This
   gives strong all-or-nothing artifact semantics and charges the proposer for
   bad packing, but sacrifices salvage/throughput and requires admission to stop
   one bad input invalidating unrelated work.
5. Preserve layer-specific order: cannot produce portable committee replay or
   one canonical root.

## 5. S2-ORDER-02: same-prefix funding

**Recommendation:** execute against the accumulator left by the prior accepted
operation in the canonical schedule.

- An ordinary credit from an earlier accepted operation is spendable by a later
  operation.
- Credits created by the operation being checked cannot satisfy that same
  operation's gross outgoing check. The dark ledger already enforces gross
  debit before applying ordinary credits
  (`BalanceReservationLedger.scala:475-496`), with an atomic self-credit
  regression (`BalanceReservationLedgerSuite.scala:96-112`).
- A rejected operation contributes no balance, reference, reservation, replay,
  intent, diff, or root write.
- An authenticated release may fund only the exact operation to which that
  release is bound (`BalanceReservationLedger.scala:521-539`).
- Ratified Phase-2 inbox effects are earlier state and may fund ML0-local work.
  Unratified expiry/refund or other maintenance credits do not become
  same-candidate funding by implication.

This preserves deterministic causal batching without whole-batch netting.
Choosing proposal-parent-only funding instead is safer to reason about but
rejects a later signed operation that is fully funded by an earlier accepted
transfer or mandatory inbox credit. Choosing whole-batch netting is rejected:
it creates circular funding and permits an operation's own or a later operation's
credit to mask an unfunded debit.

The exact within-family total order is still required before activation. Parent
references constrain same-source order, but arrival order, collection order,
map/set iteration, signature bytes, and JSON bytes are forbidden tie-breakers.
This packet does not invent the remaining comparator or its Scodec identity.

## 6. S2-ORDER-03: retry, disposition, and candidate scope

The rank and funding rule do not answer three separate questions:

1. Is each operation attempted once, or are funding/parent dependencies retried?
2. Is an unresolved input permanently `Rejected` or `Awaiting` reinsertion?
3. Does one failed input become a candidate-local no-op or invalidate the whole
   candidate?

Concrete dependency: start with `A=0`, `B=100`. Canonical transfer order places
`T1: A -> C, 60` before `T2: B -> A, 60`. A single pass leaves `T1` unfunded,
then accepts `T2`, ending with `A=60`. A retry accepts `T1` in the next round,
ending with `A=0`, `B=40`, `C=60`. Both are deterministic, but they are different
protocols.

**Recommendation:** use bounded deterministic retry-to-fixed-point for only
same-candidate funding and operation-parent dependencies:

- scan the frozen total order; accepted operations update the accumulator and
  retryable inputs become `Awaiting`;
- if the pass made progress, rescan only `Awaiting` inputs in the same order;
  at most `inputCount` productive rounds are possible;
- at no progress, unresolved funding/parent dependencies remain `Awaiting`, make
  no writes, and are eligible for upstream reinsertion;
- canonically decoded but unauthorized, replayed, equivocal, wrong-domain, or
  out-of-range operations are `Rejected`, make no writes, and are not reinserted;
- correctly classified `Awaiting`/`Rejected` inputs are candidate-local no-ops,
  so unrelated accepted work is retained; and
- malformed/ambiguous envelopes, resource-bound violations, or any mismatch in
  committed order, decisions, diff, root, or execution signatures invalidate
  the whole artifact.

Every execution-checkpoint producer, execution signer, and assigned watchtower
reproduces the complete decision vector. Noncommittee GL0 validators do not
replay it; they verify that the execution certificate commits the bounded
decision/diff/root contract.
Single-attempt execution is simpler and makes priority entirely comparator
driven. Whole-candidate invalidity gives stronger proposer-cost/atomicity but
less throughput and salvage. Neither alternative may be selected accidentally
by reusing current `Awaiting` types or exception behavior.

## 7. Required RED and integration gates

1. In native-execution and CL1-replay vectors, base `100` with allow-spend `60`
   plus lock `60` selects only allow-spend; transfer `60` plus lock `60` selects
   only transfer. Decisions/writes match their lane oracle, with no partial write.
2. All six input permutations of the three supported families produce identical
   ordered IDs, writes, balances, reservations, references, diff, and root.
3. The `A=0, B=100` dependency takes exactly two deterministic rounds; a
   zero-funded dependency cycle terminates at no progress as `Awaiting`.
4. Same-operation self-credit, rejected-operation credit, and unresolved
   `Awaiting` inputs never fund or write; exact reinsertion classification holds.
5. Permanent invalidity is candidate-local `Rejected`, while malformed
   envelopes, bounds, or committed-result mismatches invalidate the artifact.
6. A failed multi-address operation is atomic; unrelated accepted work survives.
7. Native producer/follower execution, ML0 execution, execution-checkpoint
   producer/every execution signer/assigned watchtower replay, noncommittee GL0
   certificate/diff/root adoption, and universal GL0 settlement execution
   respect their distinct contracts.
8. Shard counts `1`, `2`, and `K`, restart, replay, Phase-2 reorg/refold, and
   child-before-parent arrival preserve the same result.
9. Unsupported operation IDs, unfrozen fees/replacements/terminal effects, or a
   missing exact comparator fail closed before signing or adoption.
10. Frozen Scodec bytes and hashes have cross-JVM/platform golden vectors; no JSON
   or signature-container ordering reaches the schedule.

## 8. Owner response

```text
S2-ORDER-01: ACCEPT Transfer < AllowSpendCreate < TokenLockCreate / REVISE
S2-ORDER-02: ACCEPT accepted-prefix funding rule / SELECT parent-only / REVISE
S2-ORDER-03: ACCEPT bounded retry + Awaiting/reinsert + candidate-local skip /
             SELECT single-attempt / SELECT whole-candidate invalidity / REVISE

Notes:
```

After approval, a dark packet encodes these rules and the RED corpus, then
freezes the within-family comparator and Scodec identity. Production wiring
still waits for complete grammar, root, and execution-certificate replay gates.
