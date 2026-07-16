# Operator Consensus-Key Registry

Status: ratified target contract. Current implementation is partial; the gaps are
listed below and tracked by `NAKAMOTO-PLAN.md` E2/E2K.

## 1. Invariant

No consensus role may derive, prove, verify, sign, attest, store, select, or
adjudicate VRF eligibility under a key learned from the candidate or its
transport.

For an operator `O`, eta period `N`, and exact candidate parent `P`, every
consumer must resolve the same active `(KES master VK, VRF VK)` pair from the
canonical registry view proved by `P`. A carried key is comparison evidence
only. Missing, malformed, mismatched, duplicated, pending, orphaned, or
historically unavailable registration state never falls back to a live map,
peer list, seed list, receiver tip, or carried key.

## 2. Timing

Eligibility in eta period `N` uses:

- the stake and eligible-operator/key view committed by the candidate branch at
  `N-2`;
- the eta/nonce evidence derived by that branch from `N-1`; and
- the active protocol parameters for `N` proved by the same branch.

A runtime key pair used in period `N` must already be present in the exact
canonical registry/roster/stake view selected from `N-2`; eta comes from `N-1`.
This is the preregistration rule for every VRF consumer and its associated KES
key. Current source evaluates a record included in period `I` as follows:

```text
require signedEffectivePeriod >= inclusionPeriod + 2
activationPeriod = signedEffectivePeriod
```

The addition is checked and overflow rejects. The historical resolver requires
the registration to be in the exact branch prefix selected for `N-2` before its
key can be used in `N`; disclosure of eta for `N` cannot be followed by a new key
registration for that same period. This is a two-period index lookback, not a
claim that two full period durations elapsed after an intra-period inclusion.
The ratified V1 rule is this period-index delay; implementations must not silently
strengthen or weaken it to an elapsed-duration `I+3` rule. Wall time is never the
source of activation.

Genesis records are the only exception to runtime inclusion delay. They are
active from period zero because their complete unique KES+VRF pairs are part of
the canonical rooted genesis state. Missing or ambiguous genesis pairs make
startup fail; restart materializes the root-authenticated records and requires
the local signed genesis data and key material to match. This commitment fixes
identity only. The separately authorized immutable genesis operator/stake
population is not yet a rooted consensus input, so the current genesis cut does
not establish a production eligibility roster.

Registration fixes key ownership; it does not authorize an operator or grant
stake. The population for every draw is the intersection of the active paired
registry view and the independently authorized delayed canonical operator/stake
roster. A self-signed pair outside that roster has zero weight and cannot enter
an admission, execution, watchtower, finality, or tower population.

### 2.1 O-11 population authority packet

**Current gap.** The rooted state contains stake amounts and paired-key state, but
not the permissionless operator population. `StakeRegistry` still filters through
a startup-updated validator set and its historical MPT path may fall back to a
live aggregate/equal-weight result (`StakeRegistry.scala:32-45,68-85,425-438,
480-503`). `SharedServices` currently derives that set from the local seedlist
(`SharedServices.scala:275,300-304`). The isolated historical resolver models the
missing authority as a separate `CanonicalOperatorRosterSource` and returns
unavailable without it. This containment is correct; the local seedlist must not
be promoted into the missing source. The model's additional `stakeOf > 0` filter
can enforce a ratified backing condition after roster resolution, but positive
stake cannot become the roster rule by itself.

**OWNER-RATIFIED DIRECTION; SCHEMA NOT FROZEN.** The smallest common boundary model is conceptually:

```scala
CanonicalOperatorPopulation(
  members: SortedMap[PeerId, BigInt]
)

HistoricalOperatorBoundary(
  population: CanonicalOperatorPopulation,
  eta: Hash
)
```

The names and representation are illustrative, not a frozen schema. The sorted
map keys are the exact authorized roster and the values are its raw consensus
stake/weight. At the closing GL0 snapshot of period `P`, producer and verifier run
the same pure, era-selected O-11 rule over the exact post-transition state and
commit the period-`P` population atomically with the historical boundary. Keeping
roster and weight in one authenticated value prevents mixed-branch denominators.
Keeping `eta_P` in the same period entry is compatible with the current one-read
historical cache, but does not change the lookup lag below. No field number, codec,
or membership predicate is selected by this proposal.

The key registry remains separate. Registration may occur before an operator is
authorized, and an authorized operator may temporarily lack an active pair. In
both cases the operator has zero eligibility because every consumer takes the
intersection. A registration endpoint's resource controls likewise cannot become
a membership rule.

For eligibility period `N`, the exact-parent resolver must produce one capability
containing:

```text
population/stake = boundary[N-2].population
active key pair  = paired registry prefix present at end of N-2
eta              = boundary[N-1].eta
parameters       = active branch-bound parameters for N
eligible         = population keys intersect active paired records
```

The input authority is an authenticated candidate parent `(ordinal, hash,
parentHash, mptRoot)`, not just an ordinal. The output binds the source periods, MPT/root
witnesses, exact operator map, weights, active records, and derived KES steps.
Missing history returns unavailable/recovery. A same-ordinal sibling, current
seedlist/stake, observed peers, receiver head, or candidate-carried key is never a
fallback. A shard checkpoint uses this API only through its exact Phase-2 GL0
anchor.

Portable evidence should carry a separate inclusion witness containing the
accepted record, the containing snapshot header/root, and its MPT path. The
registration body cannot safely embed the hash of the snapshot that contains it
without creating a circular commitment.

Tessellation v4.0.0's signed chained node profile and token-lock-backed delegated
stake/collateral create, withdrawal, and pending-withdrawal state can be reused as
identity/profile and bonded-principal inputs. Their validators use the seedlist to
authorize target nodes, and v4 GSI contains no permissionless roster. Therefore
the event/accounting machinery is reusable, but its membership authority is not.
The minimum-bond/self-pledge structure, proportional delegation slashing, and
`bond >= extractable value in one fraud window` are ratified. Exact values,
activation/exit/unbond, slash/cooldown, genesis-population, lifecycle-event, and
state-growth rules remain engineering and parameter freeze gates under O-11 in
`docs/review/CONSENSUS-OWNER-DECISIONS.md`.

## 3. Signed Record

The target canonical runtime record must bind, in one explicit signature domain:

- network/genesis/protocol-era identity;
- operator `PeerId` and its established long-term identity signer;
- KES master verification key, step-zero marker, and activation offset;
- 32-byte VRF verification key;
- signed effective eta period;
- per-operator registration ordinal and previous registration reference; and
- the exact GL0 candidate parent hash against which the record was submitted.

For a fresh KES tree, the registered master step is zero and its offset equals
the effective period. KES and VRF are registered and activated as one atomic
pair: no consumer may select or activate one key from a different record. This
atomicity rule does not itself require both key byte strings to change between
successive complete records. Current acceptance rejects conflicting same-operator
candidates in one batch, enforces sequence/effective-period monotonicity, and
rejects cross-operator KES/VRF ownership collisions. It does not compare a new
record's keys with that operator's prior record, so same-owner full-key reuse and
a record in which only the KES or only the VRF key changes are currently
accepted. The locked safety boundary is one complete, uniquely active,
pre-registered pair selected from the exact N-2 branch prefix. Engineering must
freeze canonical duplicate and partial-rotation semantics and prove
`KEYREG-004`, `KEYREG-005`, and `KEYREG-014`; runtime rotation remains fail-closed
until those schema and proof gates pass.

The current `KesRegistrationCert` binds the operator, both public keys, timing,
sequence link, and registration-parent hash, but it does **not** carry an explicit
network/genesis/protocol-era domain. `KesRegistrationRecord` adds only
`acceptedAt: SnapshotOrdinal`; it does not retain the containing GL0 hash,
inclusion period, state root, or registry-root witness. The MPT commits the
history, but a portable inclusion record/witness with those identities is still
an open target.

## 4. Historical API

Consensus code uses one branch-aware algebra. `KesRegistry` and `VrfRegistry`
may exist only as read-only projections of the same atomic record. They cannot
be independently constructed, updated, or consulted as separate authority maps.
Runtime history is resolved through the branch-aware atomic algebra.

```scala
activeKeysAt(
  operator: PeerId,
  candidateParent: Hash,
  period: EtaPeriod
): F[Either[HistoricalOperatorRegistryError, Option[OperatorConsensusKeys]]]

eligibleOperatorsAt(
  candidateParent: Hash,
  period: EtaPeriod
): F[Either[HistoricalOperatorRegistryError, CanonicalEligibleOperatorSet]]
```

For a runtime entry, `OperatorConsensusKeys` carries the pair, signed registration,
effective period, and accepted ordinal. It does not carry the containing snapshot
hash/state root or a registry-root witness. `CanonicalEligibleOperatorSet` adds
the requested parent hash/ordinal and the pair/roster/stake intersection, but it
also lacks an explicit state-root witness. The algebra is therefore a useful
in-process model over an already authenticated `GlobalSnapshotInfo`, not yet the
portable exact `(ordinal,hash,parentHash,mptRoot)` capability required by the target.
Key-registry membership alone, a receiver-local peer list, or a live stake map is
never a substitute.

Historical data absence is not cryptographic invalidity. A validator that
cannot reconstruct the exact branch view defers or enters authenticated
recovery before signing/accepting. It cannot slash an operator based on a
different branch's current key.

## 5. Consumers

The same lookup is mandatory at every producer and receiver boundary:

| Consumer | Registered-key use |
|---|---|
| GL0 snapshot leader | Secret VRF trial and receiver eligibility verification. |
| Metagraph-binary admission | Per-input secret VRF self-sortition and admission evidence verification. |
| Execution shard | Public deterministic registered-VK hash membership, shuffled staircase duty identity, and VRF key-possession proof. This is not a stake-weighted secret-VRF shard draw. |
| Execution signer | Resolve the active pair before replay, KES signing, VRF possession proof, local record, or publish. |
| Watchtower | Derive the noncommittee assignment population and authenticate any assignment/proof from the same historical view. A deterministic non-VRF sample still uses the canonical registered population. |
| Optimistic finality | Resolve every signer/sample identity used by the K/alpha/beta attestation cascade. Registration does not make an attestation economically valid. |
| NiPoPoW tower | Verify producer identity, KES/VRF eligibility, activation, and registry membership against the proved historical root. A carried tower key without membership evidence is insufficient. |
| Slashing/evidence | Resolve keys and expected KES step at the offence parent/period. Wire steps and keys are evidence only; missing history defers and cannot slash. |

Raw cryptographic primitive tests may generate arbitrary keypairs. Any test that
claims consensus acceptance, signing, committee membership, tower validity, or
slash validity must first construct the applicable registered historical pair.

## 6. KES Verification

The expected KES tree step is derived from the artifact's proved eta period and
the active record's offset:

```text
expectedStep = artifactPeriod - registeredOffset
```

A negative or out-of-range result rejects. The wire-carried step must equal the
derived step and cannot select a different verification key or period.

Registration does not excuse replay-before-sign. An execution signature means
the signer independently reproduced the framework-economic result. Admission,
DA custody, and state-validity signatures remain different domains and cannot
satisfy one another's thresholds.

### 6.1 Local secret bundle

Runtime rotation requires more than swapping the historical public-key resolver.
Before submitting a registration, the operator must atomically provision one
local bundle containing the fresh KES secret tree and corresponding VRF secret,
derive its public pair, and bind that pair to a stable registration/bundle ID.
The public record must satisfy the exact N-2 historical-view preregistration rule
before use. At every production/signing boundary, an
exact-parent historical eligibility capability selects the active record; only
then may the local store
open the bundle with that record ID and byte-compare both derived public keys.
Missing, partial, mismatched, prematurely selected, or independently rotated
secret material means no draw, proof, or signature.

The current runtime has no such scheduled bundle selector. GL0 and shard VRF
secrets are derived from the long-term identity key
(`SnapshotLeaderLoop.scala:250-261,644`;
`GlobalSnapshotConsensus.scala:1260,1777-1789,1911-1922`), and
`LocalOperatorKeyPairGate` repeats that derivation when matching the registered
record (`LocalOperatorKeyPairGate.scala:110-123,174-185`).
`OperationalKeyMaker` consumes exactly one named KES entry, keeps one evolving
secret state, and destructively erases/replaces prior material
(`OperationalKeyMaker.scala:9-20,37-67,72-90`). Thus the isolated historical
resolver can select a runtime public record that the node has no matching local
VRF/KES secret bundle to open. Production deliberately does not promote that
result to eligibility today. The resolver alone cannot enable rotation.

## 7. Branches, Restart, and Proofs

Current runtime histories/pointers and genesis pairs are rooted MPT state. The
remaining bullets are target behavior, not landed production guarantees:

- A density reorg removes or reinstates registrations with the branch. A
  receiver validating a sibling never consults its current canonical latest
  record.
- Restart and catch-up reconstruct the same active pair from authenticated exact
  history. Current startup fail-closed checks cover the immutable genesis pair;
  runtime consumer reconstruction is not wired.
- Portable proofs carry membership and activation witnesses tied to the exact
  historical registry root. Current proof shapes do not carry those witnesses.
  One honest peer may eventually supply the data, but that peer must not choose
  the key or root.
- Registry pruning obeys the proof, evidence, reorg, challenge, and recovery
  horizons. Missing retained history triggers recovery rather than fallback.
- KES secret evolution is not ordinary Phase-2 state rollback. Runtime activation
  assumes the `N-2` registration/roster prefix is common-prefix stable under the
  ratified security parameters. If objective density comparison crosses an
  activation boundary whose prior KES secret has already been erased, the node
  enters `RecoveryRequired`, emits no consensus signatures, and requires explicit
  operator realignment to the winning chain. Retaining old KES masters through
  `k2` would permit automatic rollback only by weakening KES forward security and
  is not an implicit implementation option. The no-`k2`-secret-retention and
  `RecoveryRequired`/rejoin baseline is owner-ratified; quantifying the stability
  assumption and implementing the deletion and recovery boundaries remain O-12
  engineering gates.

## 8. Current State

The worktree's immediate safety cut commits the complete signed genesis KES+VRF
pair registry into the canonical MPT and binds existing GL0 producer/receiver,
admission, execution-checkpoint, slashing, and local tower verification paths to
an atomic pair. GL0 snapshot construction selects signed unified registration
candidates, embeds only accepted candidates, and roots complete per-operator
histories plus latest pointers. Separate constructible KES-only and VRF-only
registries have been removed.

The exact historical resolver enforces the N-2 cutoff and current
`inclusionPeriod + 2` period-index rule in isolation. It is not wired as runtime
authority for all production consumers. Those consumers remain
frozen-genesis or unavailable because the branch-bound operator roster is not
rooted. The live population/weight services still admit receiver-local seedlist
and current-state inputs, so they cannot be substituted for that resolver.
Checkpoint artifacts do not commit the exact Phase-2 GL0 hash/root needed to
select historical state.
The exact-hash hot-chain view adapter distinguishes same-ordinal siblings by
requested hash and rejects a mismatched, malformed, or missing chain-store
result, but production does not construct it as the registry authority.
Production also has no activation-scheduled local KES+VRF secret bundle: live VRF
secrets derive from the long-term identity and `OperationalKeyMaker` holds one
destructively evolving KES key. Resolver wiring without atomic local provisioning
would therefore turn a canonically active rotation into a signing halt.

The live GL0 leader/receiver, `EtaStateManager`, both GSAM boundary/follower
construction sites, and the admission-anchor callback now require a typed proved-
complete, nonempty N-1 VRF-output interval for eta period N>=2. Partial or empty
history defers and neither producer-carried nor bootstrap eta is substituted.
`EtaStateManager.getEtaAt(period,parentHash)` bypasses receiver-current MPT state
and ambient memoization; GSAM supplies the exact parent `BranchId`, and admission
walks the exact Phase-2 anchor. The remaining exact-branch defect is the shard
artifact boundary: `ShardCheckpoint` carries an anchor ordinal and epoch but no
exact GL0 hash/root, so SharedServices committee selection and shard
producer/attester proof eta still use ambient `getEta(period)`. The signed
checkpoint must bind exact Phase-2 `(ordinal,hash,mptRoot)` and every shard VRF
consumer must use that parent. A separate engineering/reference-model gate must
derive and freeze one portable canonical result for a genuinely complete empty
source period while preserving the owner-ratified N-2/N-1 and fail-closed rules.
Until that gate closes, complete-empty remains typed unavailable/defer; it cannot
reuse an older eta, substitute producer/bootstrap randomness, sign, mutate, or
slash. This is an unresolved liveness construction, not an unanswered owner
choice and not permission to conflate complete-empty with incomplete history.

The currently inventoried operative consensus fixtures use a canonical committed
genesis pair. Remaining direct pair construction is registry algebra or an
explicit negative/runtime-unavailable case. A static source-inventory suite now
fails on split-registry factories, unreviewed raw VRF production consumers, and
new or count-changed direct pair fixtures. It is deliberately a reviewed textual
allowlist, not proof that an allowlisted path resolves the correct branch history.
It scans split factories, raw `EcVrf25519`/`VrfKeyDeriver` references, and direct
`OperatorConsensusKeys` constructors. A separate checked semantic manifest now
inventories reviewed lexical spellings for current higher-level sortition,
eligibility, duty, signing, proof, tower, and evidence calls at file/count
granularity, and requires uncommented/unquoted negative-vector test-shaped
declarations. Same-file line motion or same-kind substitution is not detected,
and the lexer does not resolve Weaver symbols. It also records the
intentionally absent watchtower and optimistic-sampler consumers. K7a now adds
a generated two-operator frozen-view matrix: A is authority, while separately
signed and loader-validated B is absent from A's view. The real genesis-only
resolver rejects B, B's complete pair substituted for A, an unrooted runtime-
shaped record with a dummy proof, and a malformed carried VRF key. That runtime-
shaped negative is not loader- or cryptographically validated. Every present
semantic row also names a concrete adapter-negative test and required zero-
authority-effect obligations. Anchor existence does not prove that test
instruments every listed effect. K7b-1 now qualifies the frozen execution-
attester boundary only: for the fixture's one included metagraph, one invocation
of the concrete acceptance manager's injected re-execution hook returns the
fixture-selected matching root and produces the private capability. Distinct
missing, wrong-VRF, wrong-long-term-key, unrooted dummy runtime-shaped, and
malformed local identity cases then directly produce zero eta lookup, possession
proof, KES signature, tracker record, and publication effects. Ed25519 signing is
source-order dominated by the observed zero KES call. A positive A control proves
the emitter probes are live, and staged missing-KES and local-verification failures
stop record/publication. This exercises the production capability-before-sign
order, but the injected hook is a stub and proves neither CL1 replay semantics nor
economic correctness. It does not claim zero draw, store, adopt, slash, or
historical qualification. Neither inventory nor
that lexical anchor mapping is an AST/call-graph proof, and the generated matrix
does not prove branch-historical qualification, so K7 remains open.

The tower verifier resolves every suffix and upper-level occurrence through the
current atomic period-zero pair, verifies its VRF proof over the header's exact
carried `eta || slot` bytes,
derives and constant-time compares the carried output, rejects malformed or
empty nonempty-proof shapes, and never reads sender `activePoolSize` as stake.
It then returns historical eligibility unavailable for every otherwise-valid
nonempty proof. This is a fail-closed safety cut, not a portable historical
membership proof: branch-bound roster/stake/eta witnesses remain absent.
The old ordinal-range archival backfill did not carry complete registered
KES/VRF/eta authority context and has been removed. Any replacement must carry
that evidence and re-enter the normal parent-first validator. The slashing path
still does not have a production exact-offence-parent historical resolver.
Unavailable history therefore cannot be treated as guilt.

This is not runtime completion. The following remain merge gates:

1. an explicit network, genesis, and protocol-era domain in the signed runtime
   certificate body and validator, plus a containing-hash/state-root/registry-root
   witness type;
2. a rooted, Sybil-resistant canonical operator roster independent of key
   registration, plus exact N-2 roster/stake population derivation;
3. exact Phase-2 GL0 `(ordinal, hash, mptRoot)` anchors on shard checkpoints and
   hash-bound historical lookup at every producer, signer, and verifier;
4. one typed complete/incomplete exact-parent eta-source API at the GL0 leader,
   receiver, shared boundary, admission, checkpoint, finality, tower, and
   evidence consumers, with one reference-model-derived canonical complete-empty
   rule and no unavailable-history fallback; complete-empty remains fail-closed
   until that engineering gate passes;
5. portable inclusion, activation, and registry-root proofs, including tower
   and backfill/recovery evidence;
6. migration of every producer, verifier, selected watchtower, optimistic-
   finality sampler, tower, and slashing verifier to the historical resolver;
7. atomic provisioning, durable selection, and public-key matching of future
   local KES+VRF secret bundles before registration, plus implementation of
   O-12's ratified common-prefix/deletion/recovery baseline;
8. canonical duplicate and partial-rotation semantics consistent with the
   ratified atomic N-2 preregistration boundary, plus completion of
   duplicate/rotation/reorg/restart proofs and `KEYREG-001..015`; and
9. cross-consumer semantic qualification behind the landed raw source/fixture
   tripwire and checked higher-level consumer manifest. `CommitteeSortitionSuite`, `CommitteeShardSortitionSuite`,
   and `EligibilityCheckerSuite` use loader-validated period-zero paired identities;
   wrong-key cases use another registered identity and repeated statistical trials
   vary canonical draw inputs instead of minting disposable keys. The K7a matrix
   qualifies the frozen resolver and the manifest pins concrete adapter-negative
   anchors plus the complete zero-effect obligation vocabulary. K7b-1 adds
   reviewed direct/source-dominated evidence only for `KSEM-EXEC-008..011` at
   the frozen execution-attester boundary. Per-adapter instrumentation of the
   remaining obligations remains incomplete. The manifest fails on new
   matched spellings in unmanifested files, file/count changes, and stale negative
   or qualification test-shaped declarations; it is not proof of historical
   semantics. `KEYREG-006`, `KEYREG-010`, and `KEYREG-011` remain open until every
   allowlisted path passes the generated historical-branch and exact-parent
   no-side-effect matrix, including positive historical runtime consumers.
