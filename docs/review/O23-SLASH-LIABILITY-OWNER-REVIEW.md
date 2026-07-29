# O-23 Slash Liability and Bond Tranche Owner Review

**Status:** OWNER-RATIFIED DIRECTION; ENGINEERING OPEN. On 2026-07-29 the owner
accepted `O23-01` through `O23-06`. ECO-06 still cannot be closed by passing
pending withdrawal maps and token locks into the existing slash manager. The
current state does not identify which principal was liable when a checkpoint
signer committed the offense.

**Current runtime authority:** Unsafe and unqualified. This packet does not
activate or bless the provisional fraud-proof, slashing, field-34, or snapshot
paths.

**Primary gates:** O-03, O-11, O-20, O-22, ECO-02, ECO-06, WT-002,
WT-005, SLASH-03..06, ROOT-008-F34, and PARAM-001

**Updated:** 2026-07-29

**Adversarial correction:** the earlier shorthand was not schema-freezeable. A
single amount-changing replacement lock cannot preserve two exact liability
tranches; ambient create/reference hashes are not stable identities across hash
eras; unexplained missing backing is not equivalent to a previously consumed
bond; and a reorg-surviving verdict needs portable historical Phase-2 evidence.
The selected rules below incorporate those corrections.

## 1. Ratified liability problem

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

## 2. Ratified V1 model

### O23-01 - infraction-time bond liability

**Selected:** liability attaches to every exact security-bond tranche
assigned to the signer in the delayed canonical population used for the disputed
checkpoint. For checkpoint period `E`, the normal source is the exact rooted
`E-2` eligibility population selected under O-11.

Each tranche has a domain-separated stable `BondId` preserved across active,
same-operator/same-family successor, exact-amount replacement, and pending-
withdrawal state. V1 freezes the
identity preimage and algorithm independently of the ordinal-selected ambient
`Hasher`: fixed protocol domain, rooted chain/genesis domain, bond-family tag,
and canonical ScodecV1 unsigned-create bytes, hashed with fixed SHA-256. The ID is
stored at initial acceptance and never rederived from JSON/Kryo-era references or
successor bytes.

Because the shard draw is uniform over eligible operators, no individual stake
tranche probabilistically "made" the operator win. The liable set is therefore
**all** active security-bond tranches assigned to that operator in the exact
rooted `E-2` eligibility view. The boundary commits the complete ordered
`(BondId, operator, source, family, amount)` set or an equivalent authenticated
index; neither the proof nor the verifier chooses a qualifying subset. This does
not restore the abandoned stake-weighted shard draw: amounts and `BondId`s record
liability only and never feed committee probability.

Consequences:

- delegation created after the offense is not charged for the earlier offense;
- withdrawal or exact-amount replacement cannot erase an already-open liability
  interval;
- operator or bond-family retargeting never preserves a `BondId`; V1 requires
  the old tranche to finish every liability interval before a separate create
  may establish a new operator/family tranche;
- the same bond cannot be charged twice for the same signer/checkpoint identity;
- current aggregate `HistoricalStakeSnapshot` is insufficient by itself and must
  be extended or paired with a rooted liability index.

The alternative is slash-time operator-wide liability: every active or pending
bond currently pointing at the `PeerId` bears every outstanding offense. That is
simpler, but it can seize later innocent delegation and is not recommended.

### O23-02 - full InvalidStateProof V1 only

**Selected:** freeze InvalidStateProof V1 at exactly `1/1`. Any active
policy carrying another fraction is invalid.

Each slashable V1 tranche owns one unique exact-amount backing lock. An
amount-preserving replacement may carry the same `BondId`. The current one-lock
replacement transaction cannot represent `old principal + new principal` as two
exact tranches, so V1 rejects every amount-changing replacement of an attached
bonded lock. An increase is a separate new exact lock and new `BondId`; a decrease
stages the complete old tranche unchanged until its liability deadline and may
create a smaller position only through a separate exact lock. An attached
slashable tranche cannot silently mutate into a larger or smaller single lock. A
full slash therefore consumes the complete exact lock for each selected liable
tranche without creating a residual lock, losing old liability, or overcharging
later principal.

`TokenLock` amount and reference are immutable signed data, delegated records
can change only metadata amount, and collateral has no partial-amount slot. The
current fractional path therefore cannot debit the exact principal it reports.
Partial slashing requires a separately designed protocol residual-claim/bond
transition, exact surplus handling, and new conservation vectors. It is not a
configuration change.

### O23-03 - exit and evidence horizon

**Selected:** a tranche remains locked and slashable through the last
period in which its N-2 population entry can authorize an artifact, plus the
complete rooted evidence publication, retrieval, adjudication, inclusion, and
Phase-2 replacement horizon. The unbond/release delay must be strictly no shorter
than that liability horizon.

Pending withdrawal is a staged exit, not a release of liability. Maturity and any
other principal release are blocked until the tranche's rooted liability
deadline has passed. The protocol must root a maximum artifact signing/anchor
age, evidence-publication deadline, adjudication bound, Phase-2 replacement
horizon, and release grace before this schema freezes. Artifacts and evidence
outside those bounds are invalid.

V1 forbids operator/family retargeting while any prior liability interval is
open. The same exact principal cannot concurrently secure operator A and operator
B under one `BondId`. After the old tranche is fully released, a new signed create
with a new `BondId` may establish the new operator/family position.

An allegation cannot freeze an honest exit indefinitely. Only a timely,
authenticated, resource-valid evidence notice carrying the exact bounded data
and passing the cheap signer/structure gates can open a rooted hold. Missing
remote history or an unauthenticated/incomplete claim cannot create or extend a
hold. Once a valid hold exists, unavailable adjudication data defers slash and
release through the bounded recovery rule; it never becomes no-slash acceptance.

The exact network constants remain a PARAM-001 derivation, but the above bounds
and ordering are protocol law and cannot come from local HOCON.

### O23-04 - same-candidate order

**Selected:** all ordinary events and proof envelopes validate against the
same exact proposal parent. Withdrawals may be staged, but no maturity,
exact-amount successor, or staged release is applied before adjudication. The
deterministic order is:

1. reject every amount-changing bonded replacement or operator/family retarget,
   then validate and stage the remaining ordinary economic inputs;
2. authenticate and adjudicate every carried proof atomically;
3. apply each newly culpable signer's offense-time slash to active or pending
   tranches and consume the exact backing locks;
4. remove affected release-index entries and write per-signer field-34 records;
5. process exact-amount successors and liability-expired staged release only for
   surviving tranches; and
6. compute checked bounty, principal/reward burns, balances, accumulator delta,
   and final root.

Slash wins every collision with withdrawal maturity, exact-amount successor, or
staged release.
There is no filter-failed-proof-and-continue branch.

### O23-05 - actual-debit funding and rewards

**Selected:** `actualDebit` is the sum of exact backing principal removed
without a balance credit after the complete backing join succeeds. Bounty is
`floor(actualDebit * rootedBountyFraction)` using checked arithmetic; principal
burn is the remainder. Saturation is forbidden. A culpable signer with no
debit-capable principal may receive the protocol record/cooldown but creates no
bounty only when a rooted consumed-bond tombstone proves that the same `BondId`
was already canonically debited, or O23-06 portable old-branch evidence proves
that the offense-time bond is genuinely absent on the replacement branch.
Unexplained missing or mismatched backing is an atomic failure/defer, not a
zero-debit slash.

The tombstone binds `BondId`, consumed amount, consuming slash identity, and
consuming snapshot. This permits two distinct invalid checkpoints signed by one
bond to record both offenses while debiting the principal once. Same-identity
duplicates create neither another record nor another economic effect.

Accrued delegated rewards on a slashed tranche are removed from the active or
pending claim and forfeited as a separately accounted burn; they never increase
the bounty pool. The conservation equations are
`principalDebit = bounty + principalBurn` and
`rewardClaimDebit = rewardBurn`, with supply delta equal to
`-(principalBurn + rewardBurn)`. Every term uses checked arithmetic and one
accumulator-owned transition.

### O23-06 - density reorg behavior

**Selected:** a signature over an objectively invalid execution result
remains culpable when its exact base was authenticated Phase 2 at signing time,
even if a later density reorg orphans that base. The slash effect itself is
ordinary branch state: it rolls back when its containing snapshot is orphaned
and may land on the replacement branch only after portable evidence revalidates
the exact historical base, signer context, liability set, evidence, and
newly-culpable status there. The historical bundle includes the exact signed
snapshot/root, one permitted Phase-2 qualification for that exact hash
(`decided-attestation T_weight` or authenticated canonical `k1` depth), and the
historical roster/KES/VRF/eta/liability proofs needed for the signing period. A
local old-branch cache, upheld result, or orphaned field-34 record carries no
authority across the reorg.

Replacement-branch economic outcomes are exact:

- byte-identical live `BondId`/liability/backing applies the original debit;
- a canonical consumed-bond tombstone records the additional distinct offense
  with zero debit and no bounty;
- a portable old-branch liability proof with no replacement-branch `BondId`
  permits culpability/cooldown only, with zero debit, no bounty, and no hold; and
- a present but divergent same-`BondId` lineage enters `RecoveryRequired` and
  cannot be coerced into either debit or no-debit acceptance.

The orphaned branch's hold rolls back with that branch. Re-inclusion may recreate
only the unexpired remainder of the original rooted liability/evidence deadline;
density replacement never restarts or extends the clock.

A signature issued against a base that was never valid Phase 2 is outside this
InvalidStateProof rule and requires its own typed offense; it cannot be smuggled
into this tier by treating an unauthenticated state reference as canonical.

### Non-normative Polkadot comparison

The broad security pattern is similar to
[Polkadot staking](https://paritytech.github.io/polkadot-sdk/master/pallet_staking/index.html),
not copied from its consensus or dispute protocol. Polkadot records era-specific
validator/nominator exposure, permits after-the-fact offenses, delays withdrawal,
and supports reporter rewards. Its
[slashing-span model](https://paritytech.github.io/substrate/master/pallet_staking/slashing/index.html)
addresses reused stake and offenses discovered after the fact. Those mechanisms
preserve historical slashability across unbonding.

This V1 is deliberately different in its accounting identity and consensus
context: one fixed `BondId` names an exact tranche; full-only
invalid-state-proof slashing consumes exact backing; a consumed-bond tombstone
prevents duplicate debit; each signer is adjudicated independently; and evidence
must survive Phase-2 density replacement through portable revalidation.
Polkadot's maximum-per-slashing-span accounting and parachain dispute voting are
not imported. GL0 remains Nakamoto/Taktikos/LDD, and deterministic exceptional
replay decides an invalid-state proof.

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
delegated stake, active and pending collateral, affected release indices, and
consumed-bond tombstones from one immutable parent capture. `SlashDelta` contains
consumed lock refs, tombstone upserts, all four post-state lifecycle maps, release
removals, per-signer actual debits, checked bounty credits, principal/reward
burns, cooldowns, and typed field-34 records. The same delta must flow through
`StateChangesAccumulator`; direct field-34 or tombstone writes outside the
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
4. Same-operator/same-family successor, exact-amount replacement, and pending
   state preserve one stable `BondId`; every amount-changing replacement and
   operator/family retarget rejects. A separate exact lock/create establishes a
   distinct increased, reduced, or retargeted tranche only after the old
   liability interval permits release.
5. Overlapping signer sets `A/B/C` then `A/D/E` debit each signer once and reward
   only the first canonical claimant for each new debit.
6. One bond signing two distinct invalid checkpoints debits once, records both
   offenses, and relies on a rooted consumed-bond tombstone for the later
   zero-debit result. Unexplained missing backing rejects/defer atomically.
7. Fixed-algorithm `BondId` golden vectors remain identical across ambient hash
   eras and reject cross-family, cross-chain, malformed, and trailing bytes.
8. Partial-fraction policy, surplus/duplicate backing, malformed lineage,
   overflow, saturation, and key/value mismatch reject atomically.
9. Release indices, balances, supply, field 34, tombstones, accumulator replay,
   restart, and density reorg reproduce the identical root and conservation
   equations.
10. Phase-2 qualification, historical base, roster, key, eta, liability, or lock
    evidence is portable and exact. Missing data defers only under the bounded
    authenticated-hold rule and cannot slash or silently release the tranche.
11. Density replacement covers byte-identical backing, canonical tombstone,
    genuinely absent bond, and divergent same-ID outcomes. No reorg resets a
    hold/release deadline or converts unexplained absence into no-debit guilt.

The focused `InvalidStateProofSlashPrincipalRedSuite` currently compiles and
fails all five intended cases: zero active-lock debit with synthetic bounty,
delegated pending escape, collateral pending escape, durable dedup before debit,
and full maturity refund after the slash record. Its SHA-256 is
`96f568291a4eee8e636debd814b8d8e4a3d5eebc6c3c2e06a5a752f4b3c04ca7`.

## 5. Implementation sequence after ratification

1. **Interlock provisional authority under O-22.** Disconnect provisional pool,
   proposal, follower, GSAM, field-34, and cooldown authority while building the
   direct final replacement. This is source-level development containment, not a
   target wire era or field removal/re-add sequence.
2. **Freeze schemas under O-20/O-23.** Define the fixed SHA-256 `BondId` preimage,
   bond family, complete E-2 liability set, liability interval/index, bounded
   evidence hold, consumed-bond tombstone, full-only rooted InvalidStateProof
   policy, per-signer slash identity, and invalid-state-proof-only Scodec field-34
   record/key. Add independent golden and rejection vectors before runtime wiring.
3. **Propagate stable liability through the lifecycle.** Creation assigns the
   stable tranche identity; same-operator/same-family successor, exact-amount
   token-lock replacement, withdrawal, and pending state preserve it. Amount-
   changing replacement and operator/family retarget reject, and release cannot
   cross an open liability deadline. The N-2 boundary commits the exact eligible
   population and complete liability index from one post-transition state.
4. **Separate signer authentication from artifact validity.** Authenticate every
   historical execution signature over the exact bytes even when the signed
   checkpoint is structurally invalid. Ordinary adoption still rejects the
   malformed checkpoint. Adjudication emits only newly culpable per-signer
   identities.
5. **Land the pure atomic economic sink.** Capture locks, four lifecycle maps,
   release state, and tombstones from one exact parent; consume exact liable
   locks; remove or update records and indices; calculate checked actual debit,
   reward-claim debit, bounty, and burns; emit one accumulator-owned delta and
   root.
6. **Qualify history, recovery, and reorg.** Resolve exact Phase-2 base,
   historical roster/KES/VRF/eta/parameters/liabilities, bounded inputs, and
   signer-specific prior records. Prove restart, catch-up, deep retrieval,
   candidate defer, branch rollback, and density-replacement revalidation.
7. **Integrate the final launch path only after the RED corpus is green.** The
   sole ordinal-zero ScodecV1 launch grammar commits every schema and parameter.
   Provisional pool results and bytes are never accepted or grandfathered.

The first four stages can be delegated in parallel only at their model/vector
boundaries. Runtime integration remains ordered because the economic sink cannot
be wired before the liability and record schemas are frozen, and final launch
integration cannot precede exact historical/recovery qualification.

## 6. Owner disposition

```text
O23-01: accept fixed-hash BondId and complete E-2 operator-tranche liability
O23-02: accept full-only V1 and reject amount-changing/retargeting replacements
O23-03: accept rooted bounded liability/hold/release horizons
O23-04: accept slash-before-release same-candidate ordering
O23-05: accept actual-debit funding, consumed-bond tombstones, and reward burn
O23-06: accept portable revalidation, exact backing outcomes, and no deadline reset
```

All six directions were accepted on 2026-07-29. The answer selects design only.
It does not activate fraud proofs or close O-20/O-22 implementation, exact
historical context, resource, recovery, serde, or adversarial-test gates.
