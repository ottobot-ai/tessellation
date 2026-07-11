# Consensus Economic Security Roadmap

**Status:** Active planning baseline  
**Source baseline:** `c610a0740c34833e563f8a94c2ab820186a75897`  
**Security baseline:** `CORRECTNESS-SECURITY-AUDIT-2026-07-11.md`  
**Scope:** Remaining consensus-level work required before this greenfield network can carry economic value

This document supersedes the execution order in `audit/01-epics-and-tasks.md`.
That packet remains useful historical analysis, but it was written on 2026-07-07 at
`21933559c`, before universal GL0 replay replaced committee-root/diff adoption and
before the 2026-07-11 audit expanded the production blockers. No task is complete
because this document says so. Completion requires the acceptance evidence named
below and a source re-audit against the then-current commit.

## 1. Decision

There is not yet a safe incremental path that starts by enabling sharding or
optimistic finality. The dependency head is:

1. specify and prove one finality rule;
2. make every economic transition authorized, deterministic, conservative, and
   replay-safe in a pure GL0 transition kernel;
3. make inter-metagraph settlement an exact-once GL0 state machine whose result is
   unusable until GL0 finality;
4. make shard scheduling, recovery, networking, and slashing preserve those rules;
5. pass adversarial model, differential, restart, reorg, partition, and cluster
   gates before any economic testnet.

Slashing is deterrence and recovery. It is never a substitute for rejecting an
invalid transition before value is usable.

## 2. Target invariants

| ID | Invariant | Required enforcement point |
|---|---|---|
| I-FINAL-1 | Two honest nodes never finalize conflicting hashes at one ordinal. | One fixed-set quorum-certificate protocol with locking/intersection; depth alone cannot release value. |
| I-FINAL-2 | A finalized `(ordinal, hash)` is durable, atomic with its state commitment, and cannot be erased by restart, rebootstrap, or peer input. | Finality WAL/store transaction used by fork choice, MPT promotion, serving, and downstream release. |
| I-FINAL-3 | An honest signer never forgets a vote/lock and signs a conflicting branch after crash or KES rotation. | Persist epoch/set, highest voted round, locked QC, and KES period atomically before signing. |
| I-ECON-1 | ML0, DL1, a committee, or a proof can propose an economic operation but cannot authorize it. | GL0 independently verifies the framework signature/condition and re-executes the transition. |
| I-ECON-2 | Every accepted transition conserves supply except a named, bounded, consensus-defined mint/burn rule. | One checked GL0 reservation/delta accumulator over all operation classes. |
| I-ECON-3 | Validation is a total deterministic function of the pinned parent state and ordered inputs. | No wall clock, local head, peer choice, unordered iteration, `Double`, saturation, wraparound, or exception-as-control-flow. |
| I-XMG-1 | Every inter-metagraph authorization and consumption has one domain-separated canonical identity. A one-shot authorization is consumed at most once; any reusable policy has an explicit nonce/sequence and remaining allowance. | Permanent GL0 replay/status state written atomically with its economic effect. |
| I-XMG-2 | Shard count and committee outcome affect scheduling only, never authorization or economic semantics. | The same inter-metagraph state machine runs at `numShards=1` and `numShards=K`. |
| I-XMG-3 | A source authorization is usable only from a finalized GL0 state, and a settlement is externally usable only after the containing GL0 state finalizes. | Finalized `(ordinal, hash, stateRoot, certificate)` reads and finality-gated APIs/followers. |
| I-XMG-4 | ML0 acknowledgement records mirror progress only. It cannot create, alter, duplicate, cancel, refund, or make a GL0 settlement valid. | GL0 recomputes acknowledgement against its pending queue; acknowledgement only permits deterministic queue compaction. |
| I-XMG-5 | Every byte needed to replay or deliver a finalized settlement remains available by exact hash. | GL0 validates full replay inputs before voting and exact-byte storage retains finalized settlement batches through acknowledgement/recovery horizons. |
| I-REC-1 | Restart, rollback, catch-up, and sibling recovery reproduce exact canonical bytes or stop without mutation. | Hash-addressed bytes/full replay, verify-before-write, and no local reconstruction fallback. |
| I-SLASH-1 | A slash is deterministic evidence over finalized/pinned inputs, debits real bonded principal, and affects the next eligible set. | Evidence verifier plus atomic lock debit, reward cap, registry update, and epoch-boundary activation. |

## 3. Target cross-metagraph protocol

The global snapshot chain is the shared sequencer, escrow, replay-protection
registry, and finality authority. Metagraphs do not trust one another and shard
committees do not settle value.

### 3.1 Canonical identity

Every framework-economic interaction has three distinct identities:

```text
authorizationId = H(canonical authorized intent/body bytes)
consumptionId   = H(canonical consumption intent/body bytes)
semanticReplayKey = type-specific one-shot or nonce/sequence key

signed preimage domain = (
  networkId,
  genesisHash,
  protocolVersion,
  operationType,
  sourceOrOwnerMetagraph,
  targetOrConsumerDomain,
  assetId,
  amountAndPolicy,
  nonceOrSequence,
  expiry
)
```

The exact byte encoding must be an ADR and golden-vector contract. Network,
genesis, protocol/type version, and the complete economic policy must be inside
the bytes the owner signs. Adding those fields only outside the signature would
create different nullifier keys without preventing cross-network or
cross-version signature replay. The current `AllowSpend` schema does not carry a
network/genesis domain (`swap.scala:85-95`), so this is a greenfield signed-schema
change, not merely a new hash wrapper.

Identity hashes exclude the signature/proof container: proof ordering, additional
valid signatures, or signature encoding must not create a second identity for one
semantic intent. Signatures are verified separately against the canonical body.
The body includes the authorizing source, nonce/sequence or parent reference, and
all fields needed to distinguish an intentional replacement from replay.

A transport hash, ML0 snapshot hash, shard, committee, or local ordinal is not an
operation identity. A consume records both `consumptionId` and the finalized
`authorizationId`. For the v1 one-shot allow-spend, the permanent replay key is
the authorization ID even when the consume uses less than the authorized amount.
Any future reusable policy needs a signed sequence/nonce and canonical remaining
allowance; it cannot reuse the one-shot rule implicitly.

### 3.2 State machine

```text
Economic authorization:
Absent -> Authorized/Reserved -> Consumed/Settled
                              -> Expired/Cancelled

Delivery progress (created atomically with Consumed/Settled):
Absent -> PendingDelivery -> Acknowledged
```

- `Authorized/Reserved`: GL0 recreated and verified the owner operation and its
  reservation against the pinned parent MPT.
- `Consumed/Settled`: GL0 verified the consuming operation against a finalized
  authorization, checked all balances cumulatively, moved the authorized
  reservation according to the operation rules, and atomically wrote the
  nullifier plus the exact conservation deltas.
- `Expired/Cancelled`: GL0 derives the timeout or verifies the owner-authorized
  cancellation. It is mutually exclusive with `Consumed/Settled`.
- `PendingDelivery/Acknowledged`: ML0 proved only that its next currency snapshot
  incorporated a finalized settlement. Acknowledgement can compact delivery
  metadata, not the permanent `Consumed/Settled` status, nullifier, or economic
  result.

There is no transition from receipt of a committee root, ML0 assertion, DL1
artifact, watchtower promise, or local cache.

`CanonicalEconomicState` and the latest `Ml0AttestedState` must be separate
concepts. GL0 can be economically ahead while an ML0 is offline. A pending
settlement is already part of GL0's canonical effective state; acknowledgement
only changes its representation after ML0 catches up. The effective balances
immediately before and after a valid acknowledgement must be identical.

Validation deliberately uses two coherent state views:

```text
finalizedOriginRef -> proves the authorization was created on a certified ancestor
proposalParentState -> proves its current Active/Consumed/Expired status and supplies evolving balances
```

The finalized-origin proof may not come from a pending branch. Status, nullifier,
and balance checks must use the exact proposal parent so an earlier unfinalized
descendant consume cannot be forgotten. Same-batch operations then thread one
ordered in-memory accumulator, so every later operation observes earlier writes.
The current code already has separate finalized and branch-aware reader roles in
`GlobalSnapshotAcceptanceManager.scala:514-530,2203-2217`; the new kernel must
make that split explicit rather than accidentally mixing views.

Descendants may speculatively build on unfinalized effects along the exact same
branch. Such effects are tentative: they cannot be used as a new cross-metagraph
origin proof, returned by a finalized API, adopted downstream, withdrawn, or
otherwise leave the system before their containing certificate. A reorg discards
the entire dependent branch. This preserves pipelining without pretending that a
pending state is economically final.

For one-shot allow-spend consumption, the reference transition is per asset:

```text
reservation -= authorizedAmount
destination += consumedAmount
source      += authorizedAmount - consumedAmount
fee remains in its authorization-time sink
```

Those deltas, the `Consumed` status, permanent nullifier, and delivery append are
one checked write set. A partial consume is still one-shot: the unconsumed
remainder returns to the source and no remaining allowance survives. SEC-0 must
also fix fee/refund semantics, the certified GL0 epoch/ordinal used for expiry,
and deterministic priority when consume, cancel, and expiry are eligible in the
same batch; ML0 progress and wall clock are not valid time sources.

### 3.3 Delivery and acknowledgement

GL0 maintains a lossless, deterministically ordered pending-delivery queue per
owner metagraph. Each pre-seal entry binds at least `(ownerSequence,
authorizationId, consumptionId, settlementBatchId, payloadHash, deltaHash)`, where
`settlementBatchId = H(proposalParentRef, finalizedOriginRef, owner,
sequenceRange, orderedConsumptionIds, payloadHash, deltaHash)`. The exact proposal
parent may itself be tentative; `finalizedOriginRef` separately pins authorization
provenance. The ID must not contain the hash of the snapshot whose MPT root
contains the entry; that would be self-referential. The durable finality record
later binds the batch ID to `(ordinal, snapshotHash, mptRoot, validatorSetId,
certificate)` outside the self-committing payload.

A full queue defers the entire containing ML0 binary before any partial economic
effect in v1; GL0 must never adopt the binary and silently prune one of its
framework effects. A future partial-acceptance design would need the recreated ML0
framework result to encode the identical deterministic rejection. The queue never
evicts an unacknowledged item.

Every ML0 snapshot declares the exact certified GL0 base and prior settlement
cursor. ML0 applies mandatory inbox entries first, then local CL1 operations that
may spend the resulting balances; GL0 replays the same order. ML0 follows a
finalized GL0 `(ordinal, hash, certificate)`, applies messages idempotently in
sequence, and emits a contiguous cursor or explicit message IDs in its next
snapshot. GL0 recreates that snapshot and accepts only acknowledgements for
messages actually applied in order.

The current bounded reconstruction of processed GL0 ordinals is not sufficient:
`CurrencySnapshotCreator.scala:353-383` explicitly loses history after a configured
depth, while `GlobalSnapshotOpsManager.scala:69-77,176-180` states that a still
pending ordinal can then be reapplied. `artifact.scala:57-65` also describes
`GlobalSnapshotsProcessed` as temporary and discarded. Replace the bounded `P`
history with canonical monotone progress and permanent operation nullifiers; do
not tune the window from 1 to 50 or add keep-alives as a correctness mechanism.
Ordinal alone is also not a branch pin: `MetagraphSyncDataInfo` stores only pending
ordinals (`snapshot.scala:75-81`), and `GlobalSnapshotOpsManager.scala:117-134`
fetches them without an expected hash/certificate. Every delivery batch and cursor
must bind the exact finalized GL0 reference and batch hash.

### 3.4 Current seam to retain and correct

The following shapes are useful but not a safety proof:

- Allow-spend creation verifies the source signature and exclusive source
  ownership in `AllowSpendValidator.scala:40-59`.
- Consume validation matches currency, approver, source, and destination and
  bounds the consumed amount by the authorization in
  `SpendActionValidator.scala:485-515`.
- GL0 reads the owner state from its finalized base in
  `GlobalSnapshotAcceptanceManager.scala:2203-2217`.
- Allow-spend consumption writes a canonical marker through the same MPT writer
  as other consensus state in `GlobalSnapshotAcceptanceManager.scala:2868-2873`.
- ML0 derives applied actions from pinned pending ordinals and regenerates
  `GlobalSnapshotsProcessed` in
  `CurrencySnapshotAcceptanceManager.scala:478-505,672-676`.
- A supplied `GlobalSnapshotsProcessed` is stripped before regeneration in
  `CurrencySnapshotAcceptanceManager.scala:83-88,305-308`.

The corrected protocol must remove shard-count gating from economic identity and
replay protection. `CrossShardMessageHandler.scala:70-72,99-100` and
`GlobalSnapshotAcceptanceManager.scala:2284-2294` currently make the permanent
marker conditional on `numShards > 1`, while
`ConsumedAllowSpendStateManager.scala:179-220` classifies by physical shard
difference. It must also replace saturating effective balance arithmetic
(`ConsumedAllowSpendStateManager.scala:289-339`) with exact, checked transition
arithmetic. Sharding may choose where work is prepared; it must not select a
different validity function.

Only allow-spend consumption has a registered handler today
(`CrossShardMessageHandler.scala:61-68`). Token-lock, generic transfer, and custom
message support remain disabled until each has its own explicit authorization,
conservation, identity, timeout, and acknowledgement specification.

## 4. Evaluation of the 2026-07-07 epics

The old board covered 2 CRITICAL and 10 HIGH findings. The current audit has 20
CRITICAL and 20 HIGH findings, of which 16 CRITICAL and 18 HIGH remain open after
the fixes already landed. Reconciliation against the old acceptance criteria
found zero fully owned open findings: 10 are only partially covered, 21 are
absent, and three are contradicted by an old task or sequencing rule. Six fixed
findings also lacked explicit regression ownership.

| Old epic | Disposition | Reason |
|---|---|---|
| EPIC-1, GSI rebuild durability | Re-scope | Exact-byte recovery and verify-before-write remain required, but the old six/17-site GSI migration assumptions predate replay-only recovery. Re-inventory current paths under SEC-7. |
| EPIC-2, ML0 operator threshold | Demote from economic gate | Source authenticity can be admission/DoS policy. An ML0 threshold can never authorize a framework balance transition under I-ECON-1. |
| EPIC-3, slash/exclusion | Replace | Cooldown exclusion is partly present, non-participation state was removed, and ECO-06 proves the principal is not debited. Rebuild under SEC-3/SEC-6 after pre-adoption validity is sound. |
| EPIC-4, pinned re-derivation | Retain residual only | Much of execution-base pinning landed. ECO-15 still reads a node-local GL0 head; every current read must be re-inventoried and pinned under SEC-4. |
| EPIC-5, bounded P/U windows | Reject design | A 50-deep window, keep-alive, or cache is not replay protection. ECO-05 requires permanent canonical identity/progress. Replace with SEC-5. |
| EPIC-6, denominator/config fix | Reject as finality plan | Fixing local renormalization alone leaves unlocked votes, unsafe depth finality, fork-choice violations, volatile/nonatomic finality, implicit votes, and branch-stale stake. Replace with SEC-1/SEC-2. |
| EPIC-7, band reverts | Keep disabled | Do not spend effort enabling deeper reorgs before a single safe finality/fork-choice protocol and exact replay exist. Re-evaluate only after SEC-2/SEC-7. |
| EPIC-8, invariant tests | Retain method, replace assertions | Differential and adversarial gates are required. The old 8.2 enshrines `smtRootBlind`, while SMT-01 requires recomputation or removal. |
| EPIC-9-SERDE | Re-scope | Byte-faithful persistence is useful; roots-only/GSI-era migration goals are not automatically current requirements. Keep only source-proven recovery obligations. |
| EPIC-9-HARDFORK roots-only | Retire | Greenfield scope needs no fork-only migration, and roots-only authority contradicts universal GL0 economic replay. |
| EPIC-9-NET | Rebase | Preserve boundedness/durability intent, but map work to the current NET-01..NET-10 findings and current sidecar source. |

The old DoD reference to mainnet `v3.5.12` is not a compatibility requirement for
this greenfield v4 fork. Its economic scenarios remain useful regression inputs.
The replacement gate is semantic and byte-level equivalence of canonical economic
state for the same ordered inputs at `numShards=1` and `numShards=K`.

## 5. Ordered epics

### SEC-0 - Executable protocol contract

**Blocks:** every consensus implementation epic.

| Task | Acceptance evidence |
|---|---|
| SEC-0.1 | ADR fixes the economic operation grammar, canonical byte encoding, domain-separated IDs, deterministic total order, rejection semantics, mint/burn authority, and cross-metagraph state machine. Framework-generated artifacts are a closed type boundary that DL1/ML0 cannot manufacture as authority. |
| SEC-0.2 | Small pure reference interpreter over an abstract key/value state implements checked transitions for transfer, fee, allow-spend, spend, token lock/unlock, stake/collateral, slash, rewards, and acknowledgement. |
| SEC-0.3 | Machine-readable finding ledger maps every open audit finding to one owner task, regression test, status, and closing commit. CI rejects an unowned open CRITICAL/HIGH. |
| SEC-0.4 | Public/economic deployment remains disabled until SEC-9; zero-value development remains possible, but no configuration can silently enable an unsafe finality rail. |
| SEC-0.5 | Inventory and ratify the key schema plus atomic storage/WAL contract for tentative branches, signed-vote safety state, certificates, MPT promotion, operation status/nullifiers, pending delivery, and exact replay bytes. This contract blocks SEC-2, SEC-4, and SEC-5 implementation; recovery integration remains SEC-7. |

### SEC-1 - One finality protocol

**Owns:** FIN-01, FIN-02, FIN-03, FIN-12.  
**Depends on:** SEC-0. Stake-weighted activation also depends on SEC-3.1.

| Task | Acceptance evidence |
|---|---|
| SEC-1.1 | Replace "first wins" with one specified certificate rule and fault model. Name `n`, `f`, `q`, weighted versus unweighted intersection, network synchrony assumptions, adaptive-corruption bound, and the exact safety/liveness claims. |
| SEC-1.2 | The certificate binds network, protocol version, epoch, round, ordinal, exact body hash, parent hash, state root, and validator-set hash. Each set is derived from a previously finalized state and activates after a specified non-self-referential delay. No observed-active renormalization exists; if stake weights are used, every unit joins to live bonded principal. |
| SEC-1.3 | Honest signing rules are round/sequence-monotone, prevent conflicting votes, and lock descendants according to the intersection proof; Byzantine signers may equivocate and duplicate signatures count once. Receipt of a proposal never invents a vote. The required `(epoch,setHash,highestVotedRound,lockedQcHash,KES period)` safety state is durable before signing. |
| SEC-1.4 | Depth/chain quality may drive proposal preference and availability alarms but cannot independently expose irreversible economic state. |
| SEC-1.5 | Model checker or exhaustive small-N state-machine test covers partitions, delayed/reordered votes, crash/restart, Byzantine equivocation, validator-set transition, and `>k` competing chains; no conflicting certificate is reachable within the stated fault bound. |
| SEC-1.6 | A validator votes only after fully replaying the exact economic body and durably retaining the body/batch bytes needed for finalized recovery. A hash-only certificate over unavailable data is invalid. |
| SEC-1.7 | FIN-11 regression: missing, empty, mismatched, or invalid registered KES material rejects every received snapshot/attestation path, including epoch/set transitions. |
| SEC-1.8 | Specify the full KES lifecycle: finalized registry/PoP, activation delay, durable period evolution, rotation/revocation, crash recovery, and evidence verification across a rotation. No local key state can silently change certificate validity. |
| SEC-1.9 | Pin global proposer eligibility to the certified epoch/set/stake snapshot and validate the actual Taktikos/LDD process under its stated delay, partition, and adversarial stake assumptions. Do not transfer a Praos/common-prefix bound. Stake-weighted activation waits for SEC-3.1; execution-shard membership remains the separately specified uniform draw. |

### SEC-2 - Durable finality, fork choice, and release gate

**Owns:** FIN-04 through FIN-10.  
**Depends on:** SEC-0.5, SEC-1, and SEC-3.1 before any live stake/leader-weighted activation.

| Task | Acceptance evidence |
|---|---|
| SEC-2.1 | Persist one atomic record `(ordinal, hash, parent, stateRoot, setHash, certificate)` through a WAL/transaction before any finalized API or downstream release advances. |
| SEC-2.2 | Fork choice accepts only descendants of the exact finalized hash. Remove unordered same-height lookup and every ordinal-only finality/stake cache. |
| SEC-2.3 | Atomically persist `(epoch,setHash,highestVotedRound,lockedQcHash,KES period)` before signing; then durably enqueue signed vote bytes. Count only signatures contained in a verifiable QC, retry publication until superseded by protocol rules, and remove receiver-generated producer votes. Crash/restart cannot forget a lock and sign a conflict. |
| SEC-2.4 | Restart and rebootstrap reload the certificate and can never erase/downgrade it from peer input or retry counters. MPT promotion, outbox state, finality ref, and serving resume idempotently. |
| SEC-2.5 | Finalized-only APIs, GL1/ML0/CL1/DL1 followers, cross-metagraph reads, reward release, unlocks, and withdrawals all consume the same durable record. |
| SEC-2.6 | Crash-point tests at every write boundary plus two-partition and descendant-only cluster tests prove no conflicting release or reopening of finalized history. |

### SEC-3 - Authorization, backing, and slash conservation

**Owns:** ECO-02, ECO-03, ECO-04, ECO-06, ECO-18.  
**Depends on:** SEC-0.

| Task | Acceptance evidence |
|---|---|
| SEC-3.1 | Stake/collateral record has a unique MPT join to one live eligible backing lock. Replacement, expiry, unlock, and slash update lock, indexes, record, voting weight, and reward weight atomically. |
| SEC-3.2 | Remove unsigned external `TokenUnlock`; GL0 either derives expiry from pinned state or verifies a domain-separated owner/framework authorization. |
| SEC-3.3 | Remove no-ref spend authority or define and verify a signed, bounded treasury policy. ML0 inclusion alone is rejected. |
| SEC-3.4 | Fee acceptance verifies its carried data-update binding and writes a permanent `(source,dataUpdateRef)` or signed-fee-hash nullifier before debit. |
| SEC-3.5 | Define canonical slash evidence bytes and identity. Verify offense signature(s), exact epoch/set/anchor/checkpoint, reporter authorization, deduplication, and a deterministic pinned-state verdict; malformed, ambiguous, stale, or split-view evidence cannot slash an honest signer. |
| SEC-3.6 | Slash execution debits actual bonded principal, removes every backing/index entry, caps bounty at the debit, and changes only a subsequent pinned eligibility set. |
| SEC-3.7 | Adversarial tests cover forged ML0 artifacts, replay, lock replacement/expiry, duplicate identities, split slash evidence, false-slash rejection, and slash/reward conservation. |
| SEC-3.8 | Regression ownership: ECO-01 same-round duplicate collateral/stake creates remain rejected, and ECO-08's `BalanceAdjustment` variant, decoder, loader, resource, and mutation path remain absent. |

### SEC-4 - Deterministic conservative economic kernel

**Owns:** ECO-10 through ECO-17, including MEDIUM ECO-14/ECO-16.  
**Depends on:** SEC-0/SEC-0.5; authorization schemas from SEC-3.

| Task | Acceptance evidence |
|---|---|
| SEC-4.1 | One pure ordered transition kernel consumes the pinned parent MPT plus sorted GL1/framework CL1 inputs and returns accepted/rejected operations plus one write set. GL0 production and validation call the same function. |
| SEC-4.2 | One reservation accumulator spans transfers, fees, allow-spends, spends, token locks/unlocks, collateral, rewards, and cross-metagraph effects. Validation threads the evolving state rather than validating classes independently. |
| SEC-4.3 | All balance/supply math uses checked integer/fixed-point arithmetic. Overflow, underflow, saturation, `Double`, `Math.pow/exp`, and thrown arithmetic failures are forbidden in consensus paths. Invalid operations are deterministically rejected without poisoning the snapshot. |
| SEC-4.4 | Multi-metagraph maps are globally ordered and folded through the evolving registry, eliminating right-biased address/fee collisions. CL1 message validation reads the exact pinned `(ordinal,hash)` state, never a local GL0 head. |
| SEC-4.5 | Reward withdrawals group and checked-sum by address, retain unpaid records, and weight the current stake record. Pricing input has a bounded, explicit global authority or remains disabled. |
| SEC-4.6 | After every accepted operation and for every currency, a test oracle proves `sum(spendable)+sum(locked)+sum(reserved)=declaredSupply` and `declaredSupply'=declaredSupply+authorizedMint-authorizedBurn`, with explicit fee sinks, no duplicated reference, and no negative component. |
| SEC-4.7 | ECO-07 regression: adversarial aggregate fee vectors around `Long.MaxValue` use checked arithmetic and cannot wrap, saturate, mint, or abort the containing snapshot. |

### SEC-5 - Exact-once inter-metagraph settlement

**Owns:** ECO-05 and the cross-metagraph portions of ECO-03, ECO-04, ECO-12,
ECO-18.  
**Depends on:** SEC-2, SEC-3, SEC-4.

| Task | Acceptance evidence |
|---|---|
| SEC-5.1 | Implement the Section 3 state machine and domain-separated operation IDs in canonical GL0 MPT state for every enabled inter-metagraph operation, independent of shard assignment. Unregistered operation types reject rather than falling through to carried authority. |
| SEC-5.2 | Prove authorization origin against `finalizedOriginRef`, but read Active/Consumed/Expired status and evolving balances from the exact proposal parent; thread same-batch writes in canonical order. Move the reservation under checked rules, write nullifier/status, and enqueue delivery in one write set. No ad hoc or saturating read-side overlay can create spendable value. |
| SEC-5.3 | Replace bounded `P/U` reconstruction with a canonical lossless pending queue plus permanent nullifier and monotone ML0 progress. Queue compaction requires a finalized, GL0-recreated acknowledgement. |
| SEC-5.4 | Every ML0 snapshot declares exact certified GL0 base and prior settlement cursor. ML0 applies mandatory inbox entries before local CL1 operations; GL0 replays that same order and rejects gaps/skips/stale bases. Recovery is either contiguous message replay or adoption of a certificate-bound canonical slice/root/cursor, never an ML0-claimed slice. CL1 only adopts the finalized GL0 result. |
| SEC-5.5 | Timeout/cancel/refund is an exclusive GL0 transition from unconsumed authorization. The ADR fixes its certified GL0 time source, boundary predicate, and same-batch priority against consume/cancel. A consumed intent can never refund, and a refunded intent can never consume. |
| SEC-5.6 | Deterministic backpressure defers the entire containing ML0 binary before partial effects when a queue/envelope is full; GL0 never adopts it and silently omits one framework effect. Restart, long partition, acknowledgement loss, and replay cannot duplicate or erase settlement. |
| SEC-5.7 | First ship only one-shot allow-spend consume; a partial consume refunds the remainder and consumes the whole authorization. Each later operation type needs explicit authorization/replay/remainder semantics plus a RED adversarial suite before registration. |
| SEC-5.8 | Pre-ack, post-ack, and recovered effective balances are byte-identical; one authorization concurrently consumed across metagraphs, shards, ordinals, and forks produces at most one finalized effect. Permanent logical nullifiers may be physically compacted only behind a proven commitment/absence-proof scheme. |
| SEC-5.9 | Bound permanent state growth with protocol fees/rent and explicit storage limits or a proven accumulator/absence-proof compaction. Age alone never expires replay protection. |
| SEC-5.10 | Race suite covers consume vs cancel/expiry in one batch, two unfinalized descendants, inbound settlement plus local spend, acknowledgement plus new settlement, fast-forward then replay of the boundary message, and protocol upgrade with an old live authorization. |
| SEC-5.11 | ECO-09 regression: incremental replay uses one coherent exact parent reader for state, removals, status, and balances; a consumed lock/allow-spend cannot resurrect across a child. |

### SEC-6 - Shard lifecycle without shard authority

**Owns:** SHARD-01, SHARD-02, SHARD-03.  
**Depends on:** SEC-2, SEC-3.6, and SEC-5.

| Task | Acceptance evidence |
|---|---|
| SEC-6.1 | Replace ordinal-only anchor/base fields with signed certified references `(ordinal,hash,mptRoot,setHash/certificateHash)`, derive execution epoch from that anchor with one pure function, and reject any mismatch before committee lookup or replay. `ShardCheckpoint.scala:53-67` and `PinnedCurrencyInfoReader.scala:90-96` currently rely on ordinal-only resolution. Adversarial branch ambiguity and epoch-grinding tests are RED then GREEN. |
| SEC-6.2 | Advance checkpoint/adoption watermarks only from the finalized GL0 hook, keyed by exact checkpoint and global hashes. Reorged candidates never suppress replay. |
| SEC-6.3 | Implement sibling rollback to a common canonical parent and full ordered replay. Missing ancestry or bytes defer without mutation; no ordinal relabel/reanchor exists. |
| SEC-6.4 | Committee replay/attestation remains an optimization and early fault signal. Every GL0 adopter still runs SEC-4/SEC-5; quorum never bypasses them. |
| SEC-6.5 | Define deterministic bounded censorship fallbacks for both metagraph-binary admission withholding and checkpoint/window withholding. Every fallback enters universal GL0 replay and cannot bypass ordering, authorization, or envelope limits. Commit `numShards`, assignment rule, admission/execution draw parameters, and rotation rules in network/genesis consensus configuration. |
| SEC-6.6 | Differential tests run identical inputs at one and many shards, adversarial committee compositions, rotation/cooldown boundaries, withheld windows, and sibling forks. Canonical economic writes and outcomes are identical. |
| SEC-6.7 | Reconcile mempools after certified fork switches: reinsert every orphaned still-valid DAG/framework input exactly once, discard finalized/conflicting inputs, and prove restart/reorg idempotence. Mempool state never enters the consensus root. |
| SEC-6.8 | Close the metagraph committee-gate parent-resolution/orphan loop using exact certified references and bounded ancestry recovery; demonstrate the 2-metagraph/2-shard token-lock flow without weakening replay or admission checks. |

### SEC-7 - State commitment and exact recovery

**Owns:** SMT-01 and recovery/root-durability residuals.  
**Depends on:** SEC-0.5 for the storage contract; integration follows SEC-2,
SEC-4, and SEC-5.

| Task | Acceptance evidence |
|---|---|
| SEC-7.1 | Either recompute and compare `smtRoot` at consensus/finality or remove it from the signed schema until reproducible. No blind comparison remains. |
| SEC-7.2 | Inventory every restart, catch-up, rollback, rebootstrap, download, and follower adoption write. Each names an exact byte source and signed-root gate; verification occurs before mutation. Define maximum accepted execution-base age, exact-byte/evidence retention horizons, and the certificate-bound canonical-slice fallback after each horizon; unavailable older references defer/reject without false slash. |
| SEC-7.3 | Persist/replay every economic status, nullifier, pending queue, slash, backing join, and finality record. No `GlobalSnapshotInfo` projection or node-local cache may reconstruct missing consensus state. |
| SEC-7.4 | Corrupt/missing-byte and crash-point tests prove fail-closed behavior and byte-identical recovery at one/many shards. |

### SEC-8 - Bounded authenticated consensus transport

**Owns:** NET-01, NET-02A, NET-03 through NET-10. FIN-11 and NET-02 remain
regression invariants.  
**Depends on:** SEC-0 envelope definitions; mechanical bounds can run in
parallel. Finality/evidence outbox acknowledgement depends on SEC-2 and SEC-3.5.

| Task | Acceptance evidence |
|---|---|
| SEC-8.1 | Register structural/size topic validators, report application validity, enforce fair per-peer/topic queues and quotas, and score every dynamic critical topic. |
| SEC-8.2 | Define one protocol-wide envelope maximum above a proved worst valid message. Reject an oversize proposal before local store, mempool clearing, or branch advancement. |
| SEC-8.3 | Authenticate/localize gRPC (Unix socket or mTLS), permit one subscribed consumer/demux, validate shard ranges, and cap RPC concurrency, queue memory, and outbox storage. |
| SEC-8.4 | Make every consensus/evidence outbox durable until canonical finalized-inclusion acknowledgement. Every invalid-root rejection of a checkpoint carrying slash-eligible registered signatures emits SEC-3.5 canonical evidence before dropping it; unsigned/malformed junk cannot create slash work. Crash/restart recovers and re-emits valid evidence. Retry semantics must survive GossipSub seen-cache suppression without changing signed bytes. |
| SEC-8.5 | Enforce ChainSync deadlines, rate/concurrency/range limits before allocation; select eligible scored validators, penalize empty/malformed responses, and require multi-peer agreement where recovery data is not self-authenticating. Health/reconnect requires intersection across every protocol-required topic; one partially subscribed peer cannot mask missing seed/validator connectivity. |
| SEC-8.6 | Namespace discovery, topics, and protocols by consensus-pinned network/genesis identity; harden metrics/management endpoints. |
| SEC-8.7 | Flood, slow-read, eclipse, sidecar-crash, message-loss, oversize, and partition tests show bounded memory/disk and eventual recovery without accepting invalid state. |
| SEC-8.8 | NET-02 regression: message IDs remain domain-separated by exact topic/network domain, with wrong-topic prepublication and same-topic dedup vectors in CI. |

### SEC-9 - Economic release qualification

**Owns:** closure evidence for every finding and invariant.  
**Depends on:** SEC-1 through SEC-8.

| Gate | Required evidence |
|---|---|
| G1 Reference differential | Production kernel and pure interpreter agree on accepted/rejected IDs and exact writes for generated/adversarial traces. |
| G2 Conservation | Supply/lock/reservation invariant holds after every prefix; replay, input permutation before canonical sorting, and implementation chunking/parallelism within one ordered batch cannot change the result. |
| G3 Shard equivalence | Same ordered economic trace at `numShards=1`, 2, and K yields identical canonical economic leaves/root and operation statuses. |
| G4 Byzantine roles | Malicious GL1, ML0, CL1, DL1, producer, admission committee, execution committee, watchtower, and peer cannot authorize an invalid effect. |
| G5 Finality | Model proof plus cluster partitions/restarts/reordering show no conflicting certificate or finalized economic release within the documented adversary bound. |
| G6 Recovery | Crash/restart/reorg/catch-up at every write boundary reproduces exact bytes or stops before mutation; nullifiers and finality never disappear. |
| G7 Platform determinism | Linux/macOS and supported JVM/CPU matrix produce identical roots for boundary vectors; no floating-point consensus arithmetic exists. |
| G8 Network adversity | Sustained junk, slow peers, lost first publication, sidecar restart, eclipse attempts, and maximum valid envelopes remain bounded and live. |
| G9 Finding closure | Every open CRITICAL/HIGH row has a merged fix, RED/GREEN exploit regression, source re-audit, and no compensating-control-only waiver. |

## 6. Dependency graph and work waves

```text
SEC-0 protocol + SEC-0.5 storage contract
  |-- SEC-1 finality + SEC-3.1 backed stake --> SEC-2 durable finality/release --+
  |-- SEC-3 authorization/backing ------------> SEC-4 deterministic kernel -----+--> SEC-5 exact-once inter-MG
  |-- SEC-8 mechanical transport (parallel) -------------------------------------+          |
                                                                                           +--> SEC-6 shard lifecycle
SEC-0.5 + SEC-2 + SEC-4 + SEC-5 --> SEC-7 commitment/recovery integration ----------------+
SEC-1..SEC-8 ----------------------------------------------------------------------------> SEC-9 release gates
```

Recommended execution waves:

| Wave | Work | Parallelism | Exit condition |
|---|---|---|---|
| 0 | SEC-0/SEC-0.5 plus RED exploit tests for FIN-01/02/04/06, ECO-02/03/04/05/18, SHARD-03 | One protocol/storage contract owner; test work parallel | ADR, interpreter, key/WAL contract, fail-closed deployment gate, and finding ledger approved. |
| 1 | SEC-1 finality design/model; SEC-3 authorization/backing; SEC-8 envelope/auth/rate-limit foundations | Three parallel teams | Finality proof/model passes; unsigned/replayed authority paths have specified replacements; transport envelopes frozen. |
| 2 | SEC-2 durable finality; SEC-4 transition kernel | Two parallel teams with shared state API review | One finalized release gate and one checked kernel used by producer/verifier. |
| 3 | SEC-5 allow-spend-only exact-once protocol; SEC-7 commitment/recovery integration | Parallel after shared key schema is frozen | Permanent IDs/nullifiers and idempotent finalized delivery survive restart/reorg. |
| 4 | SEC-6 shard lifecycle; remaining SEC-8 durability/recovery | Parallel | Committee epoch, watermark, sibling recovery, and transport tests pass. |
| 5 | SEC-9 adversarial qualification | Independent red team owns verdict | Every CRITICAL/HIGH is closed; no economic release waiver. |

## 7. Finding ownership

| Owner | Findings |
|---|---|
| SEC-1 | FIN-01, FIN-02, FIN-03, FIN-12 |
| SEC-2 | FIN-04, FIN-05, FIN-06, FIN-07, FIN-08, FIN-09, FIN-10 |
| SEC-3 | ECO-02, ECO-03, ECO-04, ECO-06, ECO-18 |
| SEC-4 | ECO-10, ECO-11, ECO-12, ECO-13, ECO-14, ECO-15, ECO-16, ECO-17 |
| SEC-5 | ECO-05 plus cross-metagraph closure for ECO-03/04/12/18 |
| SEC-6 | SHARD-01, SHARD-02, SHARD-03 |
| SEC-7 | SMT-01 |
| SEC-8 | NET-01, NET-02A, NET-03, NET-04, NET-05, NET-06, NET-07, NET-08, NET-09, NET-10 |
| SEC-1 regression | FIN-11 |
| SEC-3 regression | ECO-01, ECO-08 |
| SEC-4 regression | ECO-07 |
| SEC-5 regression | ECO-09 |
| SEC-8 regression | NET-02 |

The ownership table covers every CRITICAL/HIGH finding open in the 2026-07-11
audit. MEDIUM ECO-14/ECO-16 and NET-08/09/10 are included because they touch
consensus economics or the production security boundary, not deferred as hygiene.

## 8. First implementation packets

Do not begin with shard expansion. The first mergeable packets are:

1. **Packet A - SEC-0 contract, storage contract, and reference interpreter.**
   The only initial runtime change is a fail-closed public deployment/economic
   release guard. Freeze operation IDs, ordering, checked arithmetic, settlement
   states, key/WAL atomicity, and finding ownership; add RED vectors for the known
   exploits.
2. **Packet B - SEC-3 replay/authorization closures.** Fee nullifier first, then
   unsigned unlock removal, no-ref spend authority removal/policy, and backing-lock
   join. These are narrow, independently testable, and unblock the kernel.
3. **Packet C - SEC-1 finality model and ADR.** Do not patch only the denominator.
   Select and prove the certificate/lock rule before rewriting live finality.
4. **Packet D - SEC-4 reservation kernel skeleton.** Route a small operation class
   through the pure checked accumulator without changing wire schemas, then migrate
   all economic classes behind differential tests.

Only after A-D establish stable contracts should SEC-5 replace the current
cross-metagraph `P/U` and effective-balance overlay. Otherwise the interaction
protocol would be built twice on changing finality, identity, and arithmetic rules.

## 9. Other consensus backlog disposition

These existing roadmap items are not silently dropped:

| Item | Disposition and dependency |
|---|---|
| Global stake-weighted leadership | Owned by SEC-1.9 and blocked by live backing in SEC-3.1. It is not execution-shard membership. |
| KES on-chain rotation | Owned by SEC-1.8 and required before a long-running KES-secured release. |
| Mempool reinsertion | Owned by SEC-6.7 after certified fork choice/recovery exists. |
| Metagraph parent-resolution wedge | Owned by SEC-6.8 after exact certified anchor schemas are fixed. It is an e2e liveness blocker, not the security dependency head. |
| Content-addressed MPT | Adopt only as needed to satisfy SEC-0.5/SEC-7 exact-byte and atomic recovery contracts; storage representation is not economic authority. |
| Separate two-level economic finality | Not a second release rail. ML0 consensus proposes a candidate; only the GL0 certificate makes framework economics usable. A future independent rail needs a new intersection proof and ADR. |
| Hard-fork/dual-mode migration | Retired for the greenfield v4 fork unless migration from an actually deployed network becomes a new product requirement. |
| Roots-only GL0 economics | Retired. It contradicts universal GL0 framework execution. |
| Secret stake-weighted execution-shard VRF | Retired. The current execution draw is uniform/public; changing it needs a new threat model and does not replace GL0 replay. |
| NIPoPoW, BLS aggregation, global attestation committees | Post-SEC-9 features/optimizations. None may change the validity or finality rule without reopening SEC-0/SEC-1 proofs and adversarial gates. |
| Replacing ML0 BFT consensus | Separate product work after SEC-9. It must preserve ML0-as-proposer and GL0-as-economic-authority. |
