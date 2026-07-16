# O-19 V4 Snapshot Migration Policy Owner Review

**Status:** OWNER RESPONSE REQUIRED. This packet does not ratify a migration
policy, authorize an importer, or close any `MIG-*` gate. O19-01 through O19-08
are deliberately unanswered.

**Runtime authority:** None. The repository has no upstream-v4 snapshot-to-new-chain
transform. The existing JSON/CSV genesis loaders are fresh-test-genesis
loaders and must not be promoted into a migration authority.

**Upstream source baseline:** Constellation Labs Tessellation `v4.0.0`, annotated
tag `bae49bf03db49bb19933e7f86d3242e5abb08a34`, peeled commit
`22953a1ee835d4d93fb6a0b193599d18c82a284c`.

**Current-source audit baseline:** `d9268886a7de91a3dd849090010f7b2b8a4b9707`.

**Primary gates:** `MIG-001`, `MIG-001A`, `MIG-002`, `MIG-003`, `MIG-004`,
`MIG-004A`, `MIG-005`, `MIG-005A`, `MIG-005B`, `MIG-005C`, `MIG-006`,
`ECON-C-001`, `ECON-B-001`, `ECON-R-001`, `ECON-GENESIS-BACKING-001`

**Updated:** 2026-07-16

## 1. Purpose and stop line

The future hard fork may select one exact upstream-v4 GL0 snapshot and use a
deterministic transform of its state as the genesis state of this protocol. That
is not equivalent to loading balances from a CSV or copying one
`GlobalSnapshotInfo` object.

The migration transform must satisfy all of these constraints:

1. Every one of the 17 upstream-v4 `GlobalSnapshotInfo` fields has an explicit,
   typed disposition. There is no optional/default disposition and no silent
   empty-map substitution.
2. Source signatures and roots authenticate only what the source protocol
   actually committed. They are migration evidence, never target-chain
   transaction, operator, registry, or state-transition authority.
3. A field not committed by the selected source snapshot is accepted only after
   exact source-history replay from an authenticated base, or after an explicit
   owner-ratified reset recorded in the manifest. A peer-supplied value is not a
   substitute.
4. The transform does not create stipends, synthesize/re-sign owner operations,
   right-bias duplicate keys, discard malformed entries, or silently repair
   source state.
5. Every asset has an exact source-to-target conservation report covering
   spendable balances, reservations, locks, stake/collateral backing, pending
   withdrawals, and pending rewards. Every difference has one explicit
   protocol-level issuance, burn, settlement, or correction reason.
6. Old-domain transactions, references, binaries, fees, registrations, and
   signatures cannot be replayed as new-domain authority after target genesis.

The source schema really has 17 fields at
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:140-157`.
The current economic manifest correctly marks the offline transform
`MissingFailClosed` at
`modules/node-shared/src/test/scala/io/constellationnetwork/node/shared/domain/economics/V4EconomicGrammarManifest.scala:551-561`.

## 2. Source-authentication limits

### 2.1 What a signed source snapshot commits

The v4 state-proof format changes at an ordinal boundary: ordinals at or below
the configured boundary use the legacy field proof, and later ordinals use one
MPT root
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/StateProofSelector.scala:21-29`).
The importer must select the source-era codec and proof algorithm from the exact
source network and ordinal. It cannot choose the more convenient verifier.

For a legacy-proof snapshot, `legacyStateProof` hashes the major maps, including
`updateNodeParameters` and `priceState`, and commits the currency-snapshot
Merkle root
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:289-309`).
The source `lastCurrencySnapshotsProofs` map is supporting proof material rather
than a separately hashed legacy field; it must be recomputed and checked against
the committed currency root and exact heads.

For a post-MPT snapshot, `mptStateProof` clears the legacy proof members and
commits only the MPT root built from `allStateEntries`
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:264-286`).
The v4 `toAllStateKeyValuePairs` list commits 15 semantic field families but
omits both `updateNodeParameters` and `priceState`
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:197-220`).
Those two names still have numeric field IDs 12 and 17
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:126-132`),
but an unused ID does not put a value under the root.

Therefore, for a selected post-MPT source tip:

- mutating `updateNodeParameters` or `priceState` in the accompanying
  `GlobalSnapshotInfo` does not change the signed MPT root;
- those values cannot be authenticated by checking only the selected signed
  snapshot plus its supplied state object;
- exact source replay from an earlier authenticated state can derive them, or a
  ratified manifest can reset them; and
- accepting either value from one peer, archive, local database, or operator is
  an authoritative override and is forbidden.

MPT authentication is also authentication of materialized entries, not of an
arbitrary Scala/JSON wrapper shape. In particular, an absent optional map and a
present empty map both produce zero entries in the v4 converter. The current
fork records the same general limitation explicitly at
`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:138-147`.
The v4 converter also normalizes a full currency snapshot and its corresponding
incremental-plus-info representation into the same two MPT leaves
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:82-118`).
The migration plan must preserve every semantically relevant distinction and
must not claim byte-authentication for a shape the source root did not encode.

### 2.2 Terminal signatures do not prove the historical ML0 population

The v4 currency receiver verifies the signatures on the inner incremental, then
defines `facilitators` from those same proof IDs and uses that set for recreation
(`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/CurrencySnapshotValidator.scala:59-77,99-123`).
It does not independently resolve a complete metagraph operator population or
threshold at the historical parent. The inner currency snapshot wire shape also
contains no complete facilitator set or facilitator-set hash
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/currency/schema/currency.scala:221-241`).
Consequently, valid terminal inner signatures prove possession and recreation
under the proof-derived set; they do not prove that the signers were the complete
authorized ML0 population for that source ordinal.

Normal ML0 round advancement waited for a declaration from every member of its
current in-memory active-facilitator set
(`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/consensus/state/ConsensusStateAdvancer.scala:75-102`).
That set was derived from the prior outcome, mutable candidate/seedlist and
collateral filtering, deterministic subsetting, withdrawals, and removal state
(`v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusStateCreator.scala:65-125`).
The finalization path filters invalid proofs and requires only a nonempty
remaining set after the all-declarations barrier
(`v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/CurrencySnapshotConsensusStateAdvancer.scala:353-407`).
The full facilitator state/hash lives in the ML0 consensus outcome, not the
`StateChannelSnapshotBinary`, whose wire value contains only parent hash,
content, and fee
(`v4.0.0@22953a1e:modules/currency-l0/src/main/scala/io/constellationnetwork/currency/l0/snapshot/schema.scala:74-104`;
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/statechannel/StateChannelSnapshotBinary.scala:16-21`).
Thus an archived terminal binary does not carry the population needed to prove
the production-time all-declarations rule.

GL0 outer admission was also an intersection check, not an ML0 quorum proof. It
cryptographically validated the outer signatures and, when configured, required
at least one signer in the GL0 seedlist and at least one signer in the per-address
allowance entry
(`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/statechannel/StateChannelValidator.scala:92-134,164-199`).
An absent seedlist passes the intersection helper
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/security/signature/SignedValidator.scala:152-171`),
an absent allowance map accepts the signed binary, and v4 configured the
allowance map as absent for dev, testnet, and integrationnet, with entries only on mainnet
(`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/statechannel/StateChannelAllowanceLists.scala:14-28`).
Neither check proves a complete metagraph operator population or threshold.

The same portability limit exists at v4 GL0. `GlobalConsensusOutcome` carries
facilitators and removals beside `Finished`, while the terminal signed snapshot
contains no complete population witness
(`v4.0.0@22953a1e:modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/schema.scala:59-79`).
The active set came from prior outcomes, candidates, seedlist filtering,
collateral filtering, withdrawals, and deterministic subsetting
(`v4.0.0@22953a1e:modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusStateCreator.scala:55-120`).
The advancer waited for the then-active declarations, filtered invalid proofs,
and emitted any nonempty valid proof set
(`v4.0.0@22953a1e:modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/GlobalSnapshotConsensusStateAdvancer.scala:281-341`).
A terminal signed GL0 snapshot therefore proves cryptographic possession, not by
itself the historical facilitator population or source-protocol finality.

This limitation does not create a second migration authority above the selected
v4 GL0 state. Once the hard-fork release pins one exact v4 GL0 checkpoint and
the importer reproduces that checkpoint's authenticated state root, a currency
head committed by that root is source state under the v4 protocol's actual
rules. Historical metagraph operators do not get a new discretionary approval
over that GL0-rooted state.

Migration validation instead keeps three claims separate:

1. source-root authenticity proves that the exact currency head/state was
   included in the release-pinned GL0 checkpoint;
2. exact-head continuity additionally proves the metagraph genesis/address,
   retained inner and outer ancestry/data, source hash domains, and the first
   target successor against the new rooted target operator registry; and
3. historical signer-population reconstruction is optional audit evidence. It
   is mandatory only if a report separately claims that every terminal signer
   belonged to the actual historical ML0 round population.

Seedlists, allowance lists, collateral inputs, facilitator outcomes, and removal
transcripts therefore cannot override or veto an authenticated GL0 source root.
If they are absent, the importer reports the narrower historical admission fact
instead of inventing a signer-authorization claim. Any malformed or weakly
authorized state committed by the release-selected v4 GL0 checkpoint is handled only by the
owner's O19-07 preserve-versus-accounted-correction disposition.

### 2.3 Required source anchor

The hard-fork artifact must pin one exact source anchor, not "latest" or an
ordinal alone. Verification must cover:

- source network/genesis identity and exact source codec/proof eras;
- exact signed snapshot bytes, snapshot hash, ordinal, parent hash, epoch
  progress, and state proof;
- authenticated ancestry sufficient to establish the selected source chain and
  replay every field whose selected-tip proof is incomplete;
- the exact accompanying state bundle and a per-field authentication result;
- exact metagraph head binaries, currency state, and opaque data retained by
  the selected transform; and
- for each exact-head continuation, the retained metagraph ancestry and data
  needed to prove the genesis/address binding, terminal head, and first target
  successor; historical operator/configuration provenance is retained when a
  separate signer-population audit claim is made;
- a content-addressed copy of every replay, proof, and transform input needed to
  reproduce the result offline.

The exact source checkpoint is selected by the already-ratified binary/social,
ordinal/hash-bound hard-fork authority in O-06. Peer majority, terminal signature
count, or a new historical-operator approval cannot select or replace it.
If the migration report additionally calls that checkpoint finalized under the
historical v4 consensus, it must reconstruct and authenticate the relevant GL0
outcomes, facilitator populations, and ancestry. Otherwise it must use the
narrower and accurate term `release-selected source checkpoint`.

The source state proof must be rebuilt and compared with the signed proof; the
v4 validator's corresponding invariant is exact equality between the rebuilt
and snapshot-carried proof
(`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/validator/StateProofValidator.scala:63-93`).
That check is necessary but, because of the omissions above, is not sufficient
to authenticate all 17 fields at a post-MPT tip.

### 2.4 Source evidence is not target authority

The manifest's eventual wire schema and whether it carries a redundant release
signature are not frozen by this packet. Its authority comes from the
owner-ratified binary/social hard-fork release in O-06, which must bind the exact
source anchor, manifest hash, complete target root, and target genesis identity.
A detached signature or operator statement not bound by that release is not a
substitute. Target consensus starts from the release-bound target state under
the target protocol's keys and domains.

An old signed allow-spend, token lock, stake event, collateral event, metagraph
binary, node-parameter update, or operator record may be retained as source
evidence. It cannot be transplanted into a target signature preimage, re-signed
by the importer, counted toward a target threshold, or accepted as a fresh
target operation. A policy that requires target reauthorization must carry a
new target-domain intent from the authorized principal; absence means the item
stays quarantined rather than being synthesized.

## 3. Current loader is not a migration transform

The fresh JSON genesis schema carries operators, delegated-stake fixtures,
node-collateral fixtures, and initial balances only
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala:170-187`).
It does not accept an authenticated v4 snapshot or a 17-field transform plan.

Its balance helper converts a list with `.toMap`, gives stake/collateral signer
addresses a one-unit stipend when absent, and silently excludes values that fail
address/balance refinement
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/genesis/types.scala:189-221`).
The augmenter signs synthetic stake/collateral creates, suppresses signing errors
by dropping the entry, then installs those records independently of token-lock
backing
(`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/genesis/L0GenesisLoader.scala:160-188,195-235`).
The fresh-start path constructs a new genesis and applies that augmenter; it does
not authenticate or transform a source snapshot
(`modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/Main.scala:506-585`).

If this shape were misused as a v4 migration loader, only balances would be
approximately carried and stake/collateral would be recreated lossily; the other
14 v4 field families would have no source-driven disposition. Duplicate
right-bias, invalid-entry omission, stipends, synthetic signatures, arbitrary
rewards, and unbacked stake/collateral would all violate the stop line. The
current loader can remain a test-genesis tool, but it is not reusable as the
migration transform.

## 4. Required 17-field disposition matrix

`V4TransformPlan` must be a fixed product with exactly these 17 required members.
The disposition column is a requirement on the future plan, not a choice made by
this packet.

| # | Upstream-v4 field | Source commitment | Required explicit disposition |
|---:|---|---|---|
| 1 | `lastStateChannelSnapshotHashes` | Legacy field hash; post-MPT entries | Preserve exact heads with all retained data needed to continue them, or declare the affected metagraph epoch restart. |
| 2 | `lastTxRefs` | Legacy field hash; post-MPT entries | Preserve every exact reference. A reset would reopen old-domain transaction replay and is not a default. |
| 3 | `balances` | Legacy field hash; post-MPT entries | Preserve exact balances; reject duplicate, malformed, negative, or out-of-range inputs. Do not right-bias, skip, or add stipends. |
| 4 | `lastCurrencySnapshots` | Legacy committed currency Merkle root; post-MPT incremental/info entries | Preserve each metagraph's exact head and retained replay data, or explicitly rebase/restart that metagraph under O19-02. |
| 5 | `lastCurrencySnapshotsProofs` | Legacy supporting proof material; post-MPT entries | Recompute from exact currency heads and require consistency. Never copy a stale proof map as authority. |
| 6 | `activeAllowSpends` | Legacy field hash; post-MPT entries | Explicitly choose inert carry, deterministic expiry/refund, or target-domain reauthorization under O19-01. Account for every reservation. |
| 7 | `activeTokenLocks` | Legacy field hash; post-MPT entries | Preserve exact locks or apply one deterministic unlock/settlement with complete accounting under O19-01. |
| 8 | `tokenLockBalances` | Legacy field hash; post-MPT entries | Recompute from exact retained locks and compare with source; mismatch is a typed migration failure or O19-07 correction, never overwrite-by-claim. |
| 9 | `lastAllowSpendRefs` | Legacy field hash; post-MPT entries | Preserve every exact reference so pre-fork allow-spend creates cannot replay. |
| 10 | `lastTokenLockRefs` | Legacy field hash; post-MPT entries | Preserve every exact reference so pre-fork token-lock creates cannot replay. |
| 11 | `updateNodeParameters` | Legacy field hash; **not in the v4 post-MPT root** | Derive by authenticated source replay or apply the explicit reset selected in O19-04. Never trust the supplied tip object alone. |
| 12 | `activeDelegatedStakes` | Legacy field hash; post-MPT entries | Preserve semantic principal/reward/backing state or explicitly settle it. Old signatures are evidence only and cannot become target eligibility authority. |
| 13 | `delegatedStakesWithdrawals` | Legacy field hash; post-MPT entries | Preserve exact pending schedule or deterministically settle and account for every pending principal/reward. |
| 14 | `activeNodeCollaterals` | Legacy field hash; post-MPT entries | Preserve semantic collateral and exact backing, or deterministically settle it. Do not synthesize a target create. |
| 15 | `nodeCollateralWithdrawals` | Legacy field hash; post-MPT entries | Preserve exact pending schedule or deterministically settle and account for it, including the source protocol's missing-release edge. |
| 16 | `priceState` | Legacy field hash; **not in the v4 post-MPT root** | Derive by authenticated source replay or apply the explicit reset selected in O19-04. Never trust the supplied tip object alone. |
| 17 | `metagraphSyncData` | Legacy field hash; post-MPT entries | Preserve exact per-metagraph sync state or bind its removal to the same declared metagraph epoch restart selected in O19-02. |

The post-MPT commitment description above is sourced from the exact v4
converter list at
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:197-220`.
The legacy description is sourced from
`v4.0.0@22953a1e:modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:289-309`.
For post-MPT sources, `post-MPT entries` does not authenticate a unique wrapper
representation. Every `None` versus `Some(empty)` collision and every equivalent
full versus incremental-plus-info currency representation must be characterized.
The transform then either derives the exact source semantics by authenticated
replay or records one explicit deterministic normalization; it cannot claim the
root selected the wrapper shape.

The source node-collateral expiry fold removes expired pending withdrawals from
the pending set but emits no token-lock release in that manager
(`v4.0.0@22953a1e:modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/NodeCollateralStateManager.scala:59-77`).
The resulting missing-release operation is tracked at
`docs/review/V4-ECONOMIC-GRAMMAR-AUDIT.md:405,498`; row 15 therefore requires an
explicit preserve/settle disposition rather than assuming the source released it.

## 5. Illustrative minimum manifest content

The eventual artifact needs at least the canonical, schema-versioned content
below. This sketch is non-normative: names describe required information and do
not freeze a Scala representation, codec, signing envelope, or commitment
ordering.

```text
V4MigrationManifest(
  manifestVersion,
  source: V4SourceAnchor(
    sourceNetworkAndGenesis,
    sourceCodecAndProofEra,
    selectedOrdinal,
    selectedSnapshotHash,
    selectedParentHash,
    selectedEpochProgress,
    signedSnapshotBundleHash,
    authenticatedAncestryBundleHash,
    sourceStateBundleHash,
    releaseBindingHash,
    continuedMetagraphAncestryBundleHash,
    optionalHistoricalSignerAuditBundleHash,
    fieldAuthenticationReport
  ),
  target: TargetGenesisContext(
    targetNetworkAndGenesisDomain,
    targetProtocolAndCodecEra,
    targetGenesisOrdinal,
    epochProgressMapping,
    operatorRegistryRoot,
    metagraphRegistryRoot,
    initialStakeHistoryAndEtaRoot
  ),
  transform: V4TransformPlan(
    lastStateChannelSnapshotHashes,
    lastTxRefs,
    balances,
    lastCurrencySnapshots,
    lastCurrencySnapshotsProofs,
    activeAllowSpends,
    activeTokenLocks,
    tokenLockBalances,
    lastAllowSpendRefs,
    lastTokenLockRefs,
    updateNodeParameters,
    activeDelegatedStakes,
    delegatedStakesWithdrawals,
    activeNodeCollaterals,
    nodeCollateralWithdrawals,
    priceState,
    metagraphSyncData
  ),
  targetState: TargetStateDescriptor(
    canonicalStateBundleHash,
    completeTargetMptRoot,
    targetGenesisBodyHash,
    perFieldCommitments
  ),
  conservation: ConservationReport(
    perAssetSourceBuckets,
    perAssetTargetBuckets,
    explicitIssuanceBurnSettlementAndCorrectionEntries,
    backingAndReplayReports
  )
)
```

Each `V4TransformPlan` member contains a disposition tag, exact input
commitments, exact output commitments, and a deterministic witness. The product
has no defaults and no catch-all map: adding/removing a source field must break
the schema and its exhaustiveness test.

Target-only state is not an excuse for an implicit empty default. The complete
target descriptor must also commit every target genesis partition, including
protocol parameters, operator identities/registries, metagraph registries,
stake-history/eta seeds, and any replay/nullifier or settlement state derived
from source history. Its derivation belongs to the corresponding owner decision
and conservation/replay witness even though it is not one of the 17 v4 fields.

One possible nonrecursive construction gives the manifest a
`targetGenesisBodyHash` whose frozen domain excludes the manifest-commitment
slot, then has the O-06 release bind both the manifest hash and ordinary final
target-genesis hash. That ordering is illustrative, not ratified here. The
E1.12/MIG-003/MIG-005C migration codec work must freeze one exact acyclic
construction before implementation; a schema that places each full hash inside
the other's preimage is invalid.

## 6. Required executable tests

### 6.1 Source authentication and exhaustiveness

| Gate | Required test |
|---|---|
| `MIG-001` | Verify the exact O-06 release-pinned upstream network/genesis/checkpoint, snapshot hash/root, and every retained metagraph head's inclusion. Wrong-network, peer-selected, signature-count-selected, mismatched, or unrooted sources fail closed. A separate claim of historical v4 GL0 finality requires authenticated outcomes, facilitator populations, and ancestry; terminal proofs alone cannot supply it. |
| `MIG-001A` | Decode frozen pre-MPT and post-MPT fixtures using exact source-era codecs. Verify snapshot bytes/hash/proofs, ordinal/parent continuity, rebuilt state proof, and required replay. Wrong era, boundary, bytes, hash, proof, parent, or missing replay input fails closed. Historical ML0 population evidence is checked only when the output claims that stronger audit fact. |
| `MIG-002` | Compile/decode guards prove all 17 source fields have mandatory typed dispositions and no default. Mutate each field independently: legacy committed fields change/fail their proof; post-MPT `updateNodeParameters`/`priceState` reproduce their omission; `None`/empty and full/incremental representation-equivalence vectors reproduce every materialization collision. Each unauthenticated distinction requires replay or an explicit ratified normalization/reset. Missing, duplicate, unknown, reordered, or unrecognized disposition data fails before target-state construction. |
| `MIG-003` | Two implementations and randomized input iteration orders produce byte-identical ScodecV1 manifest bytes, state bundle, complete MPT root, and target ordinal-zero genesis. Every target leaf decodes and re-encodes under the single target era. |

### 6.2 Conservation and backing

The conservation oracle uses `BigInt` accounting and a protocol-defined asset
identity. It partitions source and target value into mutually exclusive buckets
before comparing totals; the same principal cannot be counted as both spendable
and reserved.

| Gate | Required test |
|---|---|
| `MIG-004` | For every asset, prove `source total + explicit issuance = target total + explicit burn`, with deterministic separate entries for settlements and corrections. Cover balances, allow-spend reservations/refunds, active locks, `tokenLockBalances`, delegated-stake principal/rewards, collateral principal, and both pending-withdrawal families. Zero, maximum, overflow, duplicate, malformed, and mismatched-derived-state vectors fail closed. |
| `MIG-004A` | Every retained stake/collateral record resolves to exact retained backing and every pending withdrawal resolves to one backing/release path. Synthetic signatures, one-unit stipends, unbacked eligibility, reward insertion, double release, missing release, and principal counted in two buckets are rejected. Recomputed `tokenLockBalances` and currency proofs equal their authenticated source semantics or an O19-07 correction entry. |

The current grammar audit already identifies the unsafe test-genesis behavior:
stake/collateral records are installed while token-lock state is empty and
arbitrary delegated rewards affect derived supply
(`docs/review/V4-ECONOMIC-GRAMMAR-AUDIT.md:131-136`). That behavior is a RED
fixture for migration, not a model to preserve.

### 6.3 Replay and authority separation

| Gate | Required test |
|---|---|
| `MIG-005` | After target genesis, replay every retained pre-fork transaction, allow-spend create/consume, token-lock create/unlock, framework fee, metagraph binary, withdrawal, node-parameter update, registry record, and signature. Each fails through preserved references/nullifiers, a declared epoch/domain break, or explicit inert-state policy. No source proof satisfies a target operator, metagraph, execution, optimistic-finality, or state-validity threshold. |
| `MIG-005A` | Every source-signed live authorization, order, delegation, lock, collateral, and withdrawal follows its O19-01 expire/refund/inert/reauthorize rule with exact backing. No old-domain signature creates a target spend or eligibility fact. |
| `MIG-005B` | For each exact-head-resume metagraph, the first target successor verifies against the migrated head, retained binary/state data, recomputed proof, and rooted target registry. For each restarted metagraph, the old head and all old-domain successors reject and opaque DL1 retention follows O19-02. |
| `MIG-005C` | Mutating one manifest byte, source bundle, target field, conservation entry, registry root, epoch mapping, release binding, or genesis commitment prevents startup. Re-running the importer is idempotent and cannot apply issuance, refunds, or corrections twice. |
| `MIG-006` | A full cutover rehearsal covers source freeze, export, public verification, launch, restart/bootstrap, abort, and post-launch comparison with the exact release-bound artifacts. |

## 7. Owner decisions required

These are the only owner decisions introduced by O-19. Safety constraints in
sections 1-6 apply to every choice and are not alternative policy answers.

### O19-01 Old-domain live economic state

**Question:** For each live source class - active allow-spends, active token
locks, active delegated stake, active node collateral, and both pending-
withdrawal families - does the target genesis retain it as inert accounted
state, deterministically settle/refund it, or require a new target-domain
reauthorization?

The answer may differ by class but must be exhaustive. No class may be silently
dropped, defaulted empty, re-signed by the importer, or made spendable/eligible
without exact backing and replay protection.

### O19-02 Metagraph continuity and opaque retention

**Question:** For each currency metagraph, must the target resume the exact
source head, or may the hard fork declare a new ML0 epoch with a deterministic
rebase? Separately, is opaque DL1 data semantically preserved, quarantined, or
discarded when the target cannot execute it?

An exact-head resume requires all currency replay inputs and proofs. An epoch
restart must bind old-head invalidation, new domain/epoch identity, framework
state mapping, `lastStateChannelSnapshotHashes`, `lastCurrencySnapshots`, and
`metagraphSyncData` as one disposition.

O19-02 does not select bytes, compression, descriptors, chunks, size limits,
delivery, or retention duration. Those transport/DA mechanics remain frozen by
pending O-18 and must be applied consistently to the semantic choice made here.

### O19-03 Epoch-progress mapping

**Question:** Does target genesis preserve the selected source
`epochProgress`, or apply one fixed deterministic translation into the target
epoch domain?

The selected mapping must be committed in `TargetGenesisContext` and applied
consistently to expiries, pending withdrawals, era activation, stake history,
eta, and replay domains. Receiver wall clock or operator choice is forbidden.

### O19-04 Source-unrooted node parameters and prices

**Question:** For a post-MPT source anchor, must migration replay authenticated
source history to derive `updateNodeParameters` and `priceState`, or deliberately
reset either field under an explicit hard-fork rule?

The selected-tip MPT root cannot choose the answer. Directly copying either map
from a supplied `GlobalSnapshotInfo` is forbidden.

### O19-05 Target operator and metagraph registries

**Question:** What exact rooted target-genesis records define the eligible GL0
operator roster and each metagraph's ML0 operator population/threshold?

The answer must define unique identities, authorization provenance, threshold
and key domains, and the relationship to target KES/VRF registration. A source
signature, peerlist, seedlist, or importer claim cannot be the live target
registry by itself.

### O19-06 Initial stake history, eta, and fresh consensus keys

**Question:** What exact target-genesis stake-history and eta records seed the
`N-2`/`N-1` eligibility model, and can migrated source stake confer eligibility
before the owner has a fresh target-domain preregistered KES+VRF pair?

The answer must bind stake, roster membership, atomic KES+VRF identity,
activation period, eta, and genesis exception in rooted target state. Migrated
stake and old keys must not independently mint eligibility.

### O19-07 Malformed release-selected source state

**Question:** When the O-06 release-selected v4 checkpoint contains internally
inconsistent, unbacked, malformed, or inflationary state, does migration preserve
it exactly as historical state, or apply an explicit protocol-level accounted
correction?

There is no silent cleanup option. Every correction needs an exact precondition,
target, asset delta, reason, and conservation entry committed by the manifest
and target genesis.

### O19-08 Per-asset conservation policy

**Question:** What exact per-asset conservation equation and permitted
hard-fork issuance/burn categories govern migration, including pending rewards,
allow-spend reservations, token locks, stake/collateral backing, and pending
withdrawals?

The answer must specify mutually exclusive accounting buckets and treatment of
source defects so the oracle cannot double-count backing or hide a mint/burn in
a dropped field.

## 8. Response template

The owner may answer each item with a precise disposition. This packet does not
pre-populate an answer:

```text
O19-01:
O19-02:
O19-03:
O19-04:
O19-05:
O19-06:
O19-07:
O19-08:
```

Until all eight are dispositioned, implementation may add only read-only source
fixtures, characterization tests, and unsigned/dark schema experiments that
cannot construct or install target economic state. No migration importer may be
wired into fresh genesis, restart, download, state sync, MPT publication, or
consensus startup.
