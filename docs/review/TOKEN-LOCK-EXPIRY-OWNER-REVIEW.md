# Token-lock expiry owner review

**Status:** OWNER REVIEW REQUIRED. None of the four semantic choices in section 3
is ratified by this packet. `ECO-TOKEN-LOCK-EXPIRY` must remain outside the
supported reference-interpreter operation set until all four are resolved.

## 1. Scope and invariants that do not change

This packet separates deterministic expiry from owner-triggered manual unlock.
It does not select manual-unlock authority, token-lock replacement policy, fee
disposition, or production activation.

Regardless of the four open choices:

1. Expiry is a protocol-derived transition. It needs no signature and cannot be
   authorized by a caller-supplied `TokenUnlock`, custom-data output, ML0 vote,
   checkpoint signature count, or receipt.
2. Eligibility is derived from the canonical active lock and a single
   branch-authenticated consensus epoch. Receiver wall clock, live head, and
   local synchronization position are not inputs.
3. The refund source, currency, amount, and lock reference are derived from the
   canonical active lock. The transition removes that exact active lock and its
   expiry-index membership and credits exactly its principal once. It does not
   refund the creation fee or advance the owner's token-lock creation reference.
4. A lock with `unlockEpoch = None` has no deterministic expiry candidate.
5. Removal, refund, checked balance arithmetic, diff construction, and root
   reproduction are atomic and identical for native GL1 execution and replayed
   CL1 framework execution in their respective balance scopes.

These properties follow the economic-authority direction in
`docs/review/CONSENSUS-OWNER-DECISIONS.md:166-182` and the canonical-lock-derived
manual/expiry split in
`docs/review/V4-ECONOMIC-GRAMMAR-AUDIT.md:287-301`. They do not resolve the
boundary and ordering choices below.

## 2. Why owner review is required

Current source retains two incompatible v4 expiry boundaries.

The core currency manager treats a lock as expired only when
`unlockEpoch < currentEpoch`:

- current:
  `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala:62-71`;
- upstream v4:
  `v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/TokenLockOpsManager.scala:62-71`.

The native GL0 MPT path uses the same strict boundary and sweeps expiry buckets
through `currentEpoch - 1`:
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/TokenLockStateManager.scala:377-405`.
It verifies every indexed hash and epoch against the rooted active lock before
using it at `TokenLockStateManager.scala:408-454`.

In contrast, the default data-application service emits a `TokenUnlock` when
`unlockEpoch <= lastSynchronizedGlobalEpoch`:

- current:
  `modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala:367-394`;
- upstream v4:
  `v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/dataApplication/package.scala:367-394`.

At equality, currency acceptance does not classify the lock as expired because
its hash set uses strict `<`, but it can accept the generated artifact through
the manual-unlock path:
`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/currency/CurrencySnapshotAcceptanceManager.scala:483-527`.
The current fork verifies the artifact's public fields against the active lock
at `TokenLockOpsManager.scala:161-173`; that is conservation hardening, not a
resolution of whether equality is deterministic expiry or manual release.

This is consensus-relevant. Two implementations choosing different boundaries
can disagree on the active-lock set, spendable balance, replacement validity,
canonical diff, and state root.

## 3. Open semantic choices

### TL-EXP-1: expiry boundary

Choose exactly one rule:

- **Strict:** a lock is active while `currentEpoch <= unlockEpoch` and expires
  only when `currentEpoch > unlockEpoch` (`unlockEpoch < currentEpoch`).
- **Inclusive:** a lock expires when `currentEpoch >= unlockEpoch`
  (`unlockEpoch <= currentEpoch`).

**Recommendation:** strict `<`. It matches both core token-lock managers, the
rooted GL0 expiry-index window, and replacement validation, which rejects a
target only when `unlockEpoch < currentEpoch`
(`TokenLockBlockAcceptanceLogic.scala:125-159`). Under this interpretation,
`unlockEpoch = E` means principal remains locked through epoch `E` and becomes
refundable in the first later epoch.

**Consequence of inclusive `<=`:** release moves one epoch earlier. The core
expiry-index sweep, replacement predicate, currency artifact path, tests, and
all historical fixture expectations must change together. Merely changing the
data-application helper would leave native and currency framework semantics
different.

**Owner response:** UNRESOLVED - ACCEPT STRICT / SELECT INCLUSIVE / REVISE.

### TL-EXP-2: authoritative epoch source

The selected boundary still needs one exact epoch input for each execution
lane. Current data-application derivation reads the node's last synchronized
global snapshot (`currency/dataApplication/package.scala:367-382`), while
currency acceptance receives `lastGlobalSnapshotEpochProgress`
(`CurrencySnapshotAcceptanceManager.scala:498-527`). Those values can refer to
different global states during catch-up, reorg, or asymmetric synchronization.

**Recommendation:** define one branch-authenticated `executionEpoch` in each
signed execution context:

- native GL1/DAG-token expiry uses the epoch deterministically validated for the
  exact GL0 proposal execution context and its exact parent; and
- CL1 currency expiry uses the epoch from the exact Phase-2 GL0 state reference
  signed into the metagraph input/checkpoint execution base. Producer, every
  execution signer, watchtower, and adopter resolve that same `(ordinal, hash,
  mptRoot, epoch)` capability.

The default data-application `getLastSynchronizedGlobalSnapshot` result may help
construct a proposal, but it cannot be execution authority. A receiver-live GL0
head, wall clock, or locally latest ML0 snapshot is never a substitute.

**Consequence:** choosing a live or ordinal-only source permits honest replayers
at different tips to expire different locks, producing a fork or a false
invalid-state-proof accusation. Choosing an exact Phase-2 reference can delay a
currency expiry until the metagraph advances its signed base, but makes replay
portable and reorg behavior well defined.

**Owner response:** UNRESOLVED - ACCEPT RECOMMENDATION / REVISE.

### TL-EXP-3: terminal precedence

At one execution epoch, the same active lock may be named by deterministic
expiry, an owner manual-unlock intent, or a replacement transaction. Exactly one
terminal effect may remove and refund the lock.

Current source partially implies an order but does not state a complete protocol
rule:

- replacement rejects an already-expired target under the strict boundary at
  `TokenLockBlockAcceptanceLogic.scala:125-159`;
- currency acceptance excludes expired hashes from incoming manual unlocks at
  `CurrencySnapshotAcceptanceManager.scala:498-508`; and
- the GL0 state fold derives expiry payload fields from the canonical lock and
  rejects conflicting generated payloads at
  `TokenLockStateManager.scala:603-635`.

**Recommendation:** once expiry is eligible under TL-EXP-1, expiry is the sole
terminal winner for that lock. Replacement and manual unlock reject as targeting
an already-expired lock. Before expiry is eligible, a valid replacement or
owner-authorized manual unlock may win according to its separately frozen rule.
Duplicates of the same derived expiry coalesce; different terminal effects do
not stack credits.

**Consequence of replacement/manual precedence after expiry eligibility:** the
result can depend on input packaging or call order and can change the new lock,
fee, reference, or refund path. It also expands the replacement/manual security
surface after the protocol already has enough rooted information to perform a
deterministic release.

**Owner response:** UNRESOLVED - ACCEPT EXPIRY PRECEDENCE / REVISE.

### TL-EXP-4: same-proposal refund availability

Choose whether principal released by expiry can fund another input in the same
snapshot/checkpoint transition.

Current token-lock block acceptance checks each new lock against the accumulated
pre-expiry balance plus only an explicit replacement credit
(`TokenLockBlockAcceptanceLogic.scala:165-183`). The later GL0 state fold adds
expired principal and debits accepted new locks when computing the post-state
balance (`TokenLockStateManager.scala:641-674`). Thus current admission does not
rely on an expiry refund to make an otherwise-insufficient new lock valid.

**Recommendation:** expiry refund is not available to user-input admission in
the same proposal. Validate ordered user inputs against the proposal base and
explicitly permitted in-round effects, then run the deterministic expiry sweep
and expose its balance credit in the resulting state for descendants. This
avoids a circular dependency between input validity and terminal maintenance
ordering.

**Consequence of same-proposal availability:** an account can fund a transfer,
new lock, allow-spend, stake, or collateral operation from expiring principal in
that proposal. The protocol must then freeze an exhaustive cross-operation order;
otherwise different folds accept different prefixes and roots. The production
managers and differential fixtures would require coordinated changes.

**Owner response:** UNRESOLVED - ACCEPT NEXT-STATE-ONLY / SELECT SAME-PROPOSAL / REVISE.

## 4. Implementation gate after review

Until TL-EXP-1 through TL-EXP-4 are ratified:

- `ECO-TOKEN-LOCK-EXPIRY` remains outside
  `SupportedReferenceOperationId`; the currently supported set contains only
  transfer, allow-spend creation, and token-lock creation
  (`modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/economics/V4EconomicReferenceInterpreter.scala:243-260`);
- no reference model may silently import either `<` or `<=` as target policy;
- the unsigned `TokenUnlock` shape at
  `modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:35-40`
  remains evidence/output only and cannot become expiry authority; and
- no production ordering or activation change may land from this packet.

After approval, implement a protocol-derived reference transition rather than a
caller input. It enumerates eligible canonical active locks in canonical order,
removes each exact lock, credits its exact principal with checked arithmetic,
preserves the creation reference, emits an explicit deterministic expiry write,
and rechecks conservation over spendable balances plus active locks.

The minimum differential/RED corpus is:

1. equality boundary and first eligible epoch;
2. multi-epoch jump and empty/malformed/wrong-epoch index buckets;
3. duplicate expiry and restart/replay exact-once behavior;
4. expiry versus manual unlock and replacement in every input permutation;
5. insufficient same-proposal spend with and without the expiry credit;
6. balance overflow with atomic rejection;
7. native and metagraph-currency parity at the same authenticated epoch; and
8. Phase-2 reorg undo/refold against the replacement canonical hash.

Passing the reference tests does not activate production expiry. Production
activation still depends on the complete O-07 economic grammar, O-17 physical
identity/root contract, execution-certificate replay, and downstream reorg
qualification.
