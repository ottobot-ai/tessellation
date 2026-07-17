# O-23 Slash Liability and Bond Tranche Owner Review

**Status:** OWNER RESPONSE REQUIRED. ECO-06 cannot be closed by passing pending
withdrawal maps and token locks into the existing slash manager. The current
state does not identify which principal was liable when a checkpoint signer
committed the offense.

**Runtime authority:** None. This packet does not activate fraud proofs,
slashing, field 34, or any current-era snapshot field.

**Primary gates:** O-03, O-11, O-20, O-22, ECO-02, ECO-06, WT-002,
WT-005, SLASH-03..06, ROOT-008-F34, and PARAM-001

**Updated:** 2026-07-16

## 1. Why another owner decision is required

The confirmed ECO-06 exploit has two parts.

First, the live slash fold sees only active delegated-stake and collateral maps,
removes nominal records, and calculates bounty and burn from those nominal
amounts. It does not receive pending withdrawals or active backing locks
(`InvalidStateProofSlashManager.scala:96-172`). GSAM applies that fold after
ordinary stake transitions and carries the pending maps and token locks forward
unchanged (`GlobalSnapshotAcceptanceManager.scala:2673-2713`). A signer can move
principal to pending before adjudication, receive a cooldown record with zero
principal debit, and later receive the full lock at maturity
(`DelegatedRewardsDistributor.scala:168-180,219-255`;
`NodeCollateralStateManager.scala:218-251`;
`TokenLockStateManager.scala:694-711,870-918`). Even without withdrawal, the
nominal stake record disappears while the backing lock remains spendable, so a
bounty can be credited without a matching debit.

Second, changing the fold to scan current active and pending records by
`PeerId` is still wrong. It applies liability at proof-inclusion time rather than
offense time. Offense-time principal can withdraw or change lineage, while an
unrelated delegation created after the bad signature can be destroyed. Current
delegated successors replace the signed event and effective lock/amount, while
collateral records expose no stable effective lineage
(`delegatedStake.scala:123-151`; `nodeCollateral.scala:89-103`;
`DelegatedRewardsDistributor.scala:134-166`). The historical N-2 stake snapshot
contains only aggregate `PeerId -> amount`, not the exact bonded positions or
lock references (`StakeDistribution.scala:18-40,100-130`).

Therefore a production-correct implementation needs a stable bond/tranche
identity and a frozen liability interval, or an equally strong rooted construct.
A current-map-only patch may be retained only as a RED demonstration; it cannot
be activated.

## 2. Recommended V1 model

### O23-01 - infraction-time bond liability

**Recommendation:** liability attaches to the exact bonded tranches in the
delayed canonical population that made the signer eligible for the disputed
checkpoint. For checkpoint period `E`, the normal source is the exact rooted
`E-2` eligibility population selected under O-11.

Each tranche has a domain-separated stable `BondId` preserved across active,
successor, replacement, and pending-withdrawal state. The rooted eligibility
boundary commits the exact `(BondId, operator, source, family, amount)` liability
set or an equivalent authenticated index. A proof cannot choose that set.
This does not restore the abandoned stake-weighted shard draw: execution members
remain a uniform public draw over the eligible operator population, while the
liability index records the exact slashable backing that made each operator
eligible.

Consequences:

- delegation created after the offense is not charged for the earlier offense;
- withdrawal, replacement, or redelegation cannot erase an already-open
  liability interval;
- the same bond cannot be charged twice for the same signer/checkpoint identity;
- current aggregate `HistoricalStakeSnapshot` is insufficient by itself and must
  be extended or paired with a rooted liability index.

The alternative is slash-time operator-wide liability: every active or pending
bond currently pointing at the `PeerId` bears every outstanding offense. That is
simpler, but it can seize later innocent delegation and is not recommended.

### O23-02 - full InvalidStateProof V1 only

**Recommendation:** freeze InvalidStateProof V1 at exactly `1/1`. Any active
policy carrying another fraction is invalid.

`TokenLock` amount and reference are immutable signed data, delegated records
can change only metadata amount, and collateral has no partial-amount slot. The
current fractional path therefore cannot debit the exact principal it reports.
Partial slashing requires a separately designed protocol residual-claim/bond
transition, exact surplus handling, and new conservation vectors. It is not a
configuration change.

### O23-03 - exit and evidence horizon

**Recommendation:** a tranche remains locked and slashable through the last
period in which its N-2 population entry can authorize an artifact, plus the
complete rooted evidence publication, retrieval, adjudication, inclusion, and
Phase-2 replacement horizon. The unbond/release delay must be strictly no shorter
than that liability horizon.

Pending withdrawal is a staged exit, not a release of liability. Maturity,
amount-reducing replacement, and any other principal release are blocked until
the tranche's rooted liability deadline has passed. Missing exact history defers
adjudication and cannot shorten the deadline or authorize release.

The exact network constants remain a PARAM-001 derivation, but their ordering is
protocol law and cannot come from local HOCON.

### O23-04 - same-candidate order

**Recommendation:** all ordinary events and proof envelopes validate against the
same exact proposal parent. Withdrawals may be staged, but no maturity or
replacement release is paid before adjudication. The deterministic order is:

1. validate ordinary economic inputs and stage their proposed state;
2. authenticate and adjudicate every carried proof atomically;
3. apply each newly culpable signer's offense-time slash to active or pending
   tranches and consume the exact backing locks;
4. remove affected expiry-index entries and write per-signer field-34 records;
5. process maturity/replacement only for surviving, released tranches; and
6. compute checked bounty, burn, balances, accumulator delta, and final root.

Slash wins every collision with withdrawal maturity or backing replacement.
There is no filter-failed-proof-and-continue branch.

### O23-05 - actual-debit funding and rewards

**Recommendation:** `actualDebit` is the sum of exact backing principal removed
without a balance credit after the complete backing join succeeds. Bounty is
`floor(actualDebit * rootedBountyFraction)` using checked arithmetic; burn is the
remainder. Saturation is forbidden. A culpable signer with no debit-capable
principal may receive the protocol record/cooldown but creates no bounty.

Accrued delegated rewards on a slashed tranche are forfeited as a separately
accounted burn and never increase the bounty pool. This keeps the bounty tied to
security principal rather than reward-accounting timing.

### O23-06 - density reorg behavior

**Recommendation:** a signature over an objectively invalid execution result
remains culpable when its exact base was authenticated Phase 2 at signing time,
even if a later density reorg orphans that base. The slash effect itself is
ordinary branch state: it rolls back when its containing snapshot is orphaned
and may land on the replacement branch only after the exact historical base,
signer context, liability set, evidence, and newly-culpable status are
revalidated there. No local upheld cache or orphaned field-34 record carries
authority across the reorg.

A signature issued against a base that was never valid Phase 2 is outside this
InvalidStateProof rule and requires its own typed offense; it cannot be smuggled
into this tier by treating an unauthenticated state reference as canonical.

## 3. Required transition shape

The target ledger sink is one pure, atomic transition over an exact parent:

```scala
applySlash(
  exactParent: ValidatedSlashBackingState,
  liabilities: SortedMap[PeerId, SortedSet[BondLiability]],
  newlyCulpable: SortedSet[SlashIdentity],
  rootedPolicy: InvalidStateProofPolicy
): Either[SlashTransitionError, SlashDelta]
```

`ValidatedSlashBackingState` contains the active token locks, active and pending
delegated stake, active and pending collateral, and affected expiry indices from
one immutable parent capture. `SlashDelta` contains consumed lock refs, all four
post-state lifecycle maps, expiry removals, per-signer actual debits, checked
bounty credits, burns, cooldowns, and typed field-34 records. The same delta must
flow through `StateChangesAccumulator`; direct field-34 writes outside the
accumulator are forbidden.

Per-signer adjudication identity is
`(peerId, shardId, disputedCheckpointHash)`. The checkpoint signature collection
is excluded from the signed preimage, so checkpoint-wide deduplication can let
later authenticated signers escape. O22-03 owns claimant ordering; O23 requires
that only newly culpable signers produce debit or reward.

## 4. Required tests

1. Active and pending delegated stake and collateral consume the exact backing
   locks before bounty or burn is recorded.
2. Same-round and prior-round withdrawal cannot hide principal; maturity cannot
   refund a consumed lock.
3. A post-offense delegation to the same operator is not charged.
4. Successor, replacement, redelegation, and pending state preserve one stable
   `BondId` and cannot reduce open liability.
5. Overlapping signer sets `A/B/C` then `A/D/E` debit each signer once and reward
   only the first canonical claimant for each new debit.
6. Partial-fraction policy, surplus/missing/duplicate backing, malformed lineage,
   overflow, saturation, and key/value mismatch reject atomically.
7. Expiry indices, balances, supply, field 34, accumulator replay, restart, and
   density reorg reproduce the identical root.
8. Unavailable historical base, roster, key, eta, liability, or lock bytes defer
   and cannot slash or release the challenged tranche.

The focused `InvalidStateProofSlashPrincipalRedSuite` currently compiles and
fails all five intended cases: zero active-lock debit with synthetic bounty,
delegated pending escape, collateral pending escape, durable dedup before debit,
and full maturity refund after the slash record. Its SHA-256 is
`96f568291a4eee8e636debd814b8d8e4a3d5eebc6c3c2e06a5a752f4b3c04ca7`.

## 5. Implementation sequence after ratification

1. **Contain current authority under O-22.** Remove `fraudProofs` from the
   current greenfield snapshot schema and disconnect proposal, follower, GSAM,
   field-34 writer, and cooldown authority. Keep transport/replay only as dark,
   bounded test components.
2. **Freeze schemas under O-20/O-23.** Define `BondId`, bond family, liability
   interval/index, full-only rooted InvalidStateProof policy, per-signer slash
   identity, and the invalid-state-proof-only Scodec field-34 record/key. Add
   independent golden and rejection vectors before runtime wiring.
3. **Propagate stable liability through the lifecycle.** Creation assigns the
   stable tranche identity; successor, token-lock replacement, withdrawal, and
   pending state preserve it. Amount-reducing release cannot cross an open
   liability deadline. The N-2 boundary commits the exact eligible population
   and liability index from one post-transition state.
4. **Separate signer authentication from artifact validity.** Authenticate every
   historical execution signature over the exact bytes even when the signed
   checkpoint is structurally invalid. Ordinary adoption still rejects the
   malformed checkpoint. Adjudication emits only newly culpable per-signer
   identities.
5. **Land the pure atomic economic sink.** Capture locks, four lifecycle maps,
   and expiry state from one exact parent; consume exact liable locks; remove or
   update records and indices; calculate checked actual debit, reward forfeiture,
   bounty, and burn; emit one accumulator-owned delta and root.
6. **Qualify history, recovery, and reorg.** Resolve exact Phase-2 base,
   historical roster/KES/VRF/eta/parameters/liabilities, bounded inputs, and
   signer-specific prior records. Prove restart, catch-up, deep retrieval,
   candidate defer, branch rollback, and density-replacement revalidation.
7. **Activate in a protocol era only after the RED corpus is green.** The
   activation parent commits every schema and parameter. Dark pool results are
   discarded/revalidated and no pre-activation bytes are grandfathered.

The first four stages can be delegated in parallel only at their model/vector
boundaries. Runtime integration remains ordered because the economic sink cannot
be wired before the liability and record schemas are frozen, and activation
cannot precede exact historical/recovery qualification.

## 6. Owner response format

Please answer all six:

```text
O23-01: accept infraction-time BondId/tranche liability
O23-02: accept full InvalidStateProof V1 only
O23-03: accept pending slashability through the complete liability horizon
O23-04: accept slash-before-release same-candidate ordering
O23-05: accept actual-debit funding and reward forfeiture as separate burn
O23-06: accept revalidated culpability after a later density reorg
```

An answer selects design only. It does not activate fraud proofs or close O-20,
O-22, exact historical context, resource, recovery, serde, or adversarial-test
gates.
