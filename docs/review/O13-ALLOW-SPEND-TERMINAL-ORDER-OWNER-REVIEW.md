# O-13 allow-spend terminal order owner review

**Status:** OWNER REVIEW REQUIRED. The durable-delivery direction is locked;
the terminal ordering decisions below are not. No production implementation may
infer them from current call order.

## 1. Locked architecture

The already-ratified O-13 direction remains:

1. GL0 owns global conflict, nullifier, and settlement validity.
2. A destination metagraph receives a hash-linked, per-destination delivery
   sequence with rooted pending leaves and a permanent economic nullifier.
3. ML0 applies one contiguous range from one exact Phase-2 GL0 reference, stores
   `(sequence, deliveryId)`, and acknowledges by compare-and-set.
4. Missing delivery bytes defer. They never authorize a peer claim or permit a
   sequence skip.
5. An already-canonical GL0 inbox applies before ML0-local spend.

This yields two distinct order sites:

- **GL0 global settlement order:** combines universally re-executed native
  GL1/DAG-lane effects, replay-certified checkpoint-extracted CL1 intents, and
  proposal-parent-rooted expiry candidates; chooses at most one terminal effect
  for a reservation; and emits the canonical delivery/refund effects.
- **ML0 application order:** applies those already-canonical deliveries before
  validating local metagraph economics.

ML0 must not independently choose a different global allow-spend winner.

## 2. Why review is required

Current source does not implement one coherent rule:

- `GlobalSnapshotsProcessed` is explicitly temporary and unretained
  (`modules/shared/src/main/scala/io/constellationnetwork/schema/artifact.scala:57-81`).
  ML0 reconstructs processed ordinals from a bounded snapshot window
  (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotCreator.scala:353-383`),
  so an old pending ordinal can become executable again.
- ML0 currently computes local transfer/reward/fee balances before loading the
  GL0 inbox (`CurrencySnapshotAcceptanceManager.scala:259-294,446-481,533-578`).
  That contradicts the one locked precedence rule.
- Duplicate cross-shard consumes currently reject every candidate
  (`ConsumedAllowSpendStateManager.scala:239-266`), and the regression freezes
  reject-all behavior (`ConsumedAllowSpendSettlementSuite.scala:405-456`). An
  attacker can copy a public consume into the same candidate and censor it.
- Native GL0 rejects consume/expiry overlap after filtering expired
  reservations (`GlobalSnapshotAcceptanceManager.scala:306-335,2318-2327,2391-2397`).
  ML0 removes an inbound-consumed reservation from expiry first, so its selected
  consume can win (`AllowSpendOpsManager.scala:61-80,83-224`).
- There is no v4/fork allow-spend cancel constructor. The manifest contains
  create, consume/metagraph-source spend, and expiry only
  (`V4EconomicGrammarManifest.scala:624-630`).

Arrival order, shard arrival, map iteration, and current accidental call order
are all forbidden consensus inputs.

## 3. Decisions

### O13-A1: complete GL0 candidate universe

**Recommendation:** the terminal fold has exactly three authoritative input
classes:

1. native GL1/DAG-lane framework inputs and effects that every GL0 validator
   independently authenticates and executes at the exact proposal parent;
2. CL1 framework intents extracted from a checkpoint only after producer,
   execution-signature quorum, and required watchtower replay have bound the
   complete ordered inputs, exact Phase-2 base, diff, roots, and extracted
   intents; and
3. expiry candidates exhaustively derived by GL0 from the exact proposal-parent
   rooted expiry index.

A checkpoint certificate never authorizes the native lane. Admission, ML0
signature count, data availability, receipt, or checkpoint depth never creates
an economic candidate. If rooted expiry enumeration is incomplete or malformed,
the proposal is invalid; receiver-local scans cannot fill the gap.

**Owner response:** ACCEPT / REVISE.

### O13-A2: candidate identity, duplicates, and conflicts

**Recommendation:** before ordering, GL0 resolves every candidate against the
same exact proposal-parent reservation record and retains a normalized semantic
record containing the reservation hash, owner scope, source, destination,
amount, expiry, approver set, terminal kind, and authenticated source locator.
The existing currency, destination, approver, source, and amount checks remain
prerequisites (`SpendActionValidator.scala:543-573`).

An exact duplicate means the same semantic consumption identity and the same
fully bound economic effect. It is coalesced and has no second write. Signature
bytes, artifact bytes, arrival position, and transport copies do not create a
distinct consumption identity. Duplicate copies observed at multiple
authenticated locators retain a sorted evidence set or the minimum locator under
the frozen O13-A3 order; arrival never chooses the retained locator.

The conflict classes are distinct:

- one reservation hash resolving to different reservation bodies invalidates
  the whole proposal;
- one semantic consumption identity resolving to different normalized effects
  invalidates the whole proposal; and
- different semantic consumption identities against the same valid reservation
  are competing candidates ordered by O13-A3, not malformed bindings.

The winning terminal effect atomically writes one permanent nullifier, so no
later proposal can replay it.

**Owner response:** ACCEPT / REVISE.

### O13-A3: exact order among simultaneous valid consumes

This decision is still open and blocks production implementation. The previous
phrase "for example" did not freeze a consensus rule. The selected rule must
specify all of the following:

- an explicit source discriminator and category priority for native versus
  checkpoint-extracted candidates;
- the exact normalized locator fields for each source class;
- the exact comparison of multiple distinct, otherwise-valid consumes of one
  reservation; and
- the domain-separated semantic identifier and byte ordering used as the final
  objective tie-break.

**Recommendation:** derive the locator only from authenticated, signed canonical
proposal contents: native block/transaction coordinates for the native lane and
`(shardId, checkpointHash, metagraphId, binaryIndex, operationIndex)` for the
checkpoint lane. Compare a frozen tagged union of those locators, then a
domain-separated hash of the normalized semantic consumption record. Do not use
arrival time, receiver head, map/set traversal, local store order, signature
bytes, committee arrival, shard arrival, or an unconstrained producer-supplied
tie value. The source tags, native locator schema, hash domain, and category
priority require an explicit owner/schema freeze; `ACCEPT` here is not approval
to let implementation invent them.

**Owner response:** ACCEPT RECOMMENDATION AND REQUEST THE EXACT SCHEMA / REVISE.

### O13-B: consume versus expiry deadline

**Recommendation:** `lastValidEpochProgress == candidateEpoch` remains active;
expiry becomes eligible only when the consensus candidate epoch is greater than
`lastValidEpochProgress`. Once expiry is eligible, it wins over a newly included
consume. A historical Phase-2 reference proves the consume's reads; it does not
extend the reservation deadline. Receiver wall clock is never consulted.

An inbound delivery already canonicalized before the deadline is no longer a
competing consume and must still apply at ML0 before local economics.

**Owner response:** ACCEPT / REVISE.

### O13-C: partial consume

**Recommendation:** preserve the current v4 economic behavior. A valid consume
of `x <= reserved` credits exactly `x` to the canonical destination, refunds
`reserved - x` to the source, retires the entire reservation, and consumes one
permanent nullifier. No remainder stays reusable. All writes are atomic.

Authentication and all structural reservation checks remain prerequisites; this
section specifies the result only after those checks pass. Expiry refunds the
entire reserved amount because no consume amount was applied.

**Owner response:** ACCEPT / REVISE.

### O13-D: explicit cancel

**Recommendation:** V1 has no cancel operation. Keep it absent and fail closed.
Owner-signed early cancellation would be a new protocol operation with its own
domain, reference, deadline interaction, and era activation; it must not be
inferred from expiry or custom data.

**Owner response:** ACCEPT / REVISE.

### O13-E: ML0 application order

**Recommendation:** for one ML0 snapshot transition:

1. verify a contiguous range of exact GL0-decided deliveries against its exact
   signed Phase-2 reference; ML0 does not derive a winner or independently
   convert a reservation into expiry;
2. apply the exact GL0-decided expiry/refund/credit effects in delivery-sequence
   order;
3. validate and execute ML0-local framework inputs against that resulting
   accumulator; and
4. emit a compare-and-set acknowledgement only as part of the complete ML0
   transition replayed by every execution-committee signer and subsequently
   checked by GL0 against the proposal-parent mirror root/version, the expected
   prior rooted delivery/outbox cursor and head, and the exact new contiguous
   `(sequence, deliveryId)` cursor.

Failure is atomic. Local activity cannot starve a mandatory delivery, and an
acknowledgement cannot precede the economic effect it represents. Admission or
an ML0 signature without complete replay is never acknowledgement authority.

**Owner response:** ACCEPT / REVISE.

### O13-F: same-proposal reservation creation

**Recommendation:** a reservation created in the current GL0 proposal is not
eligible for a terminal consume or expiry in that proposal. Direct native
GL1/DAG-lane candidates are universally executed against the exact GL0 proposal
parent. Checkpoint/cross-metagraph consumes additionally bind their signed
historical exact Phase-2 origin/read base. Both lanes require the reservation to
exist in their authoritative origin state, so neither can consume a
same-proposal creation. The creation can become eligible in a descendant after
it is canonical and referenceable. This removes source-class/call-order
dependence without treating a checkpoint certificate as native-lane authority.

**Owner response:** ACCEPT / REVISE.

## 4. Implementation gate after review

Until O13-A1 through O13-F are frozen, including an exact O13-A3 schema:

- `ECO-ALLOW-CONSUME` and `ECO-ALLOW-EXPIRY` remain outside the supported
  reference-operation set;
- the terminal-conflict model may classify current-v4 eligibility and return
  typed `UnresolvedTerminalOrder`, but may not choose a winner;
- no production ordering change may land; and
- the existing reject-all behavior remains a confirmed liveness/censorship
  defect, not target semantics.

After approval, implementation proceeds in this order: pure total-order model,
reference interpreter rows, production/reference differential corpus, GL0
settlement kernel, rooted delivery sequence, ML0 inbox-first fold, restart/reorg
tests, and independent adversarial review.
