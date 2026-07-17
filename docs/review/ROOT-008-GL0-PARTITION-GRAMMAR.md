# ROOT-008 GL0 Partition Grammar

Status: **DESIGN PACKET - NOT IMPLEMENTED, NOT AN ACTIVATION CLAIM**

Date: 2026-07-14

This packet defines the target structural grammar for every physical leaf that may
appear in the GL0 consensus MPT. It is the design input for `ROOT-008`; it is not
evidence that current readers, recovery, diff adoption, or snapshot acceptance
enforce the contract. The current implementation still contains value-only
reconstruction and root-excluded writable field 32. Those paths remain release
blockers.

The contract has two independent layers:

1. `ROOT-011` proves that a candidate image has one canonical physical-key
   representation and can be built without alias, prefix-collision, or
   nontermination behavior.
2. `ROOT-008` proves that every physical key and complete raw value encode the
   one structural record permitted at that position, and that the complete image
   satisfies the index, identity, namespace, population, and retention relations
   owned by this grammar.

A matching MPT root proves only byte inclusion. It does not prove that a balance
was placed in the balance partition, that a nullifier key matches the value, or
that two physical keys do not claim the same logical identity. The semantic
grammar is therefore part of consensus validation, not an optional recovery
lint.

This packet does **not** prove that a decoded economic record was authorized, that
balances/supply are conserved, that stake or collateral is backed, that a replay
or nullifier transition is valid, or that a slash/correction is justified. Those
are O-07/ECON-G and P2 economic-oracle obligations over the exact parent and
ordered transition. Canonical economic use requires both the ROOT-008 structural
receipt and the independently reproduced economic transition receipt. Neither can
substitute for the other.

## 1. Source facts and current defects

The four namespace type bytes are `00` hypergraph, `01` address, `02` hash, and
`03` system. `MetagraphNamespace` and `AddressNamespace` both serialize as type
`01`, so their semantic roles cannot be recovered from the type byte alone
(`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:24-48,89-129,666-680`).

`GlobalStateFieldId.fromInt` currently accepts every integer from 0 through 34,
including retired physical layouts 3 and 6, dedicated-local tower ID 21, and
root-excluded writable field 32
(`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:179-243,255-309,361-397`).
The canonical typed full-state writer emits fields 0, 1, 2, 4, 5, 7-20, 22-32,
and preserves MPT-native 33/34. It converts a full currency snapshot to field 5
plus unrolled info and does not emit fields 3, 6, or 21
(`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateConverter.scala:2461-2625,2632-2692`).

The current aggregate `mptRoot` includes every byte entry except a key classified
as field 32. Classification uses a lenient field-offset parser which treats every
non-`00` leading byte as a 32-byte namespace and does not prove exact key length,
namespace type, or trailing structure
(`modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:523-544,611-635`;
`modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:307-329,392-403`).
Consequently, a stray field 3, 6, or 21 leaf currently changes the signed root,
while field 32 can change replay-visible state without changing it.

Current collection codecs consume the complete input and reject a non-strictly
increasing `SortedSet`, but their `uint16` count permits up to 65,535 elements
and is not a consensus resource policy
(`modules/shared/src/main/scala/io/constellationnetwork/serde/ImmutableCodec.scala:34-49`;
`modules/shared/src/main/scala/io/constellationnetwork/serde/codecs/SortedSetCodec.scala:10-47`).
Complete decode must be followed by `encode(decoded) == raw`; otherwise a codec
which accepts more than one byte representation can still admit a consensus
alias.

Current prefix readers reconstruct several maps from `entries.values`,
`headOption`, or an embedded identity without first checking the actual physical
key. Representative confirmed sites are:

- Mg fields: `GlobalStateConverter.scala:1824-1858`.
- update parameters: `UpdateNodeParametersStateReader.scala:17-50`.
- price state: `PriceStateUpdater.scala:38-83`.
- delegated stake and collateral: `DelegatedStakeStateManager.scala:163-195`
  and `NodeCollateralStateManager.scala:254-287`.
- active token locks: `TokenLockStateManager.scala:865-872`.
- slash/dedup state: `InvalidStateProofSlashedReader.scala:70-93`.

The existing field-7 reader and the KES/genesis readers demonstrate pieces of
the required contract: they derive an expected key from homogeneous decoded
values and reject misplaced or duplicate claims
(`GlobalStateConverter.scala:1750-1819`;
`KesRegistrationStateManager.scala:164-208`;
`L0GenesisLoader.scala:454-483`). They are field-local precedents, not a
whole-image proof.

Field 33 is now another field-local precedent: its materializer retains strict
physical entries and raw bytes, requires canonical re-encoding, reproduces
`consumedAllowSpendKey(value.allowSpendHash)` exactly, and rejects duplicate
semantic hashes before map construction
(`ConsumedAllowSpendStateManager.scala:154-213`;
`ConsumedAllowSpendStateManagerMaterializationSuite.scala:102-225`). This closes
the live MPT-02 parser only. It does not prove the complete ROOT-008 image or the
restart/compaction/reorg portions of XMG-013.

## 2. Canonical physical syntax

Let the following tokens denote exact bytes, not parsed or normalized strings:

| Token | Exact bytes | Size |
|---|---|---:|
| `H` | `00` (hypergraph) | 1 |
| `E` | `00` (empty slot) | 1 |
| `A(x)` | `01 + HASH32(canonicalAddress(x))` | 33 |
| `X(h)` | `02 + h`, where `h` is exactly 32 bytes | 33 |
| `S(l)` | `03 + HASH32(l.canonicalName)` | 33 |
| `F(i)` | exactly four-byte, big-endian field integer | 4 |

`HASH32(x)` means the exact active-era `Hasher[F].hash(x)` operation and input
serialization used by `GlobalStateKey`, resolved from the candidate's exact
authenticated schema/ordinal context. It is not receiver-current configuration,
not a peer-selected algorithm, and not an informal call to a platform hash. The
registry needs fixed vectors for every hashed identity class and each supported
era before any hasher migration.

Every physical key is exactly `network || field || contract || user`. It is
represented externally only by even-length lowercase ASCII hex. Invalid digits,
uppercase, separators, odd lengths, extra suffixes, missing slots, unknown type
bytes, and alternative encodings of the same nibble path reject the complete
candidate before trie construction or mutation. `Hex` itself currently enforces
none of those properties (`modules/shared/src/main/scala/io/constellationnetwork/security/Hex.scala:24-37`).

The permitted shape families are:

| Shape | Layout | Exact size |
|---|---|---:|
| `MGScalar` | `A(mg) + F(i) + E + E` | 39 bytes |
| `GlobalAddress` | `H + F(i) + E + A(account)` | 39 bytes |
| `GlobalScopedAddress` | `H + F(7) + E + A(source)` or `H + F(7) + A(mg) + A(source)` | 39 or 71 bytes |
| `GlobalPair` | `H + F(i) + A(first) + A(second)` | 71 bytes |
| `GlobalHashed` | `H + F(i) + E + X(identityDigest)` | 39 bytes |
| `SystemHashed` | `S(label) + F(19) + E + X(identityDigest)` | 71 bytes |
| `MGEntryAddress` | `A(mg) + F(i) + E + A(account)` | 71 bytes |
| `MGEntryHash` | `A(mg) + F(i) + E + X(identityDigest)` | 71 bytes |

No parser may infer `MetagraphNamespace` versus `AddressNamespace` from the
serialized `01`; it must know the role from the selected field grammar and must
reproduce the exact bytes from the decoded semantic identity. The existing key
constructors establish the current layouts
(`GlobalStateKey.scala:411-458,460-506,546-579,589-609`).

## 3. Root-ownership notation

The manifest below uses these root codes:

- `G`: included in the complete signed GL0 `mptRoot`.
- `P(i)`: also has a dedicated state-proof root for field `i`.
- `C-inc`: included in the currency incremental root (physical field 5).
- `C-info`: included in the single info root over physical fields 25-31.
- `X`: currently excluded from `mptRoot`; this is a defect, not a permitted
  writable ownership class.
- `DENY`: no target GL0 physical leaf is legal for that ID.

The state-proof builder gives dedicated roots to fields 0, 1, 2, 7-17, and 20.
Its currency slot is a composite of field 5 and the union of fields 25-31. The
physical field-4 proof leaf is only in `G`; the similarly named state-proof slot
does not authenticate field 4 by itself. Fields 18, 19, 22-24, 33, and 34 are
aggregate-root-only (`GlobalSnapshotInfo.scala:357-387`).

## 4. Exhaustive field manifest

The "current carrier" column names the source that currently writes or
materializes the type. It is evidence about the worktree, not permission for the
target writer. All target writes must pass the same registry used by this parser
inside one exact-parent transaction.

| ID | Field | Current value and carrier | Target key / logical identity and semantic checks | Root |
|---:|---|---|---|---|
| 0 | `LastStateChannelSnapshotHashes` | `Hash`; GSI typed full/delta writer and indexed `getAllLastStateChannelSnapshotHashes` | `MGScalar`; identity `mg` comes from the exact field-0 `ActiveAddressIndex`. Require an exact bijection: one indexed MG per leaf, no dangling index and no unindexed leaf. | `G + P(0)` |
| 1 | `LastTxRefs` | `TransactionReference`; GSI typed writer and transaction-reference readers | `GlobalAddress`; account comes from exact field-1 address index. Require index/leaf bijection. | `G + P(1)` |
| 2 | `Balances` | `Balance`; GSI typed writer and balance readers | `GlobalAddress`; account comes from exact field-2 address index. Require index/leaf bijection and the economic grammar's amount bounds. | `G + P(2)` |
| 3 | `LastCurrencySnapshots` | `Signed[CurrencySnapshot]`; legacy point/index readers remain, but canonical typed writer converts it to field 5 plus unrolled info | `DENY` in live GL0. An offline import reads the upstream-v4 source representation and emits field 5/25-31 target state; it does not admit a physical field-3 leaf. | `DENY` |
| 4 | `LastCurrencySnapshotsProofs` | Merkle `Proof`; GSI typed writer and indexed/point readers | `MGScalar`; identity `mg` comes from the exact field-4 address index; require bijection. The value must prove the separately specified object and proof domain; decoder success is not proof validity. | `G` only |
| 5 | `LastIncrementalCurrencySnapshots` | `Signed[CurrencyIncrementalSnapshot]`; `currencySnapshotEntryBytes`, full/delta writer, and currency readers | `MGScalar`; identity `mg` comes from the canonical currency-population index. Exactly one field-5 leaf per populated MG and the complete required 25-31 relation below. Verify signatures/domain and exact canonical bytes before use. | `G + C-inc` |
| 6 | `LastCurrencySnapshotInfo` | `CurrencySnapshotInfo`; old monolithic type remains accepted by the enum but the writer uses fields 25-31 | `DENY`. No live decoder, writer, recovery, or diff path may accept it. Offline import reads upstream-v4 state and emits unrolled target state; it does not admit a field-6 leaf. | `DENY` |
| 7 | `ActiveAllowSpends` | `SortedSet[Signed[AllowSpend]]`; GSI typed writer, field-7 strict reader, allow-spend managers | `GlobalScopedAddress`; value must be nonempty. Every member must have the same `source`, and the contract slot must equal the framework currency scope derived under the economic grammar. Recompute the exact key. Reject duplicate event identities before set construction and require valid signatures/references. | `G + P(7)` |
| 8 | `ActiveTokenLocks` | `SortedSet[Signed[TokenLock]]`; GSI typed writer and token-lock manager | `GlobalAddress`; nonempty, every member's `source` equals the user account, exact key reproduction, semantic event-identity uniqueness, valid signatures/references. The permitted `currencyId` mix is frozen by the economic grammar rather than inferred by this parser. | `G + P(8)` |
| 9 | `TokenLockBalances` | `Balance`; GSI typed writer and indexed token-lock-balance reader | `GlobalPair`; `(currency, holder)` comes from the exact field-9 pair index. Require exact pair/leaf bijection and amount bounds. | `G + P(9)` |
| 10 | `LastAllowSpendRefs` | `AllowSpendReference`; GSI typed writer and indexed/point readers | `GlobalAddress`; account comes from exact field-10 address index. Require index/leaf bijection and a valid canonical reference shape. | `G + P(10)` |
| 11 | `LastTokenLockRefs` | `TokenLockReference`; GSI typed writer and indexed/point readers | `GlobalAddress`; account comes from exact field-11 address index. Require index/leaf bijection and a valid canonical reference shape. | `G + P(11)` |
| 12 | `UpdateNodeParameters` | `(Signed[UpdateNodeParameters], SnapshotOrdinal)`; GSI typed writer and update-parameter reader | `GlobalHashed`; require exactly one valid proof, derive `Id` from that proof, enforce the signed source/operator rule, and recompute `HASH32(id.hex)`. Reject duplicate `Id`s before map construction. | `G + P(12)` |
| 13 | `ActiveDelegatedStakes` | `SortedSet[DelegatedStakeRecord]`; GSI typed writer, stake manager/aggregator | `GlobalAddress`; nonempty and every record's signed create source equals account. Recompute key; reject duplicate create-event identities and invalid record/reference histories. | `G + P(13)` |
| 14 | `DelegatedStakesWithdrawals` | `SortedSet[PendingDelegatedStakeWithdrawal]`; GSI typed writer and stake manager | `GlobalAddress`; nonempty and every embedded create source equals account. Recompute key; reject duplicate underlying stake identities and invalid withdrawal state. | `G + P(14)` |
| 15 | `ActiveNodeCollaterals` | `SortedSet[NodeCollateralRecord]`; GSI typed writer, collateral manager/aggregator | `GlobalAddress`; nonempty and every record's signed create source equals account. Recompute key; reject duplicate create-event identities and invalid histories. | `G + P(15)` |
| 16 | `NodeCollateralWithdrawals` | `SortedSet[PendingNodeCollateralWithdrawal]`; GSI typed writer and collateral manager | `GlobalAddress`; nonempty and every embedded create source equals account. Recompute key; reject duplicate collateral identities. Its expiry relation must use rooted active-era `WithdrawalTimeLimit`, never local configuration. | `G + P(16)` |
| 17 | `PriceState` | `PriceRecord`; GSI typed writer and `PriceStateUpdater` | `GlobalHashed`; `currentPrice`, `upcomingPrice`, and `currentSum` must carry one identical `TokenPair`; derive `HASH32(base + "/" + quote)`, reproduce key, and reject duplicate pairs. Validate price/window invariants separately. | `G + P(17)` |
| 18 | `MetagraphSyncData` | `MetagraphSyncDataInfo`; GSI typed writer and indexed/point readers | `GlobalAddress`; MG identity comes from the exact field-18 address index. Require index/leaf bijection and monotone/canonical ordinal-set rules. | `G` only |
| 19 | `SystemIndex` | One of `SortedSet[Address]`, `SortedSet[(Address,Address)]`, or one of three expiry-key sets; derived GSI projection and exact index readers | `SystemHashed`; only the four registered labels are legal. Label hash, field 19, empty contract, and user digest must reproduce exactly. All values must be nonempty. The user digest is `HASH32(fieldId decimal)` for active indexes and `HASH32(epoch.show)` for expiry buckets. Identity is proven only by the relational rules in section 6. | `G` only |
| 20 | `HistoricalStakeSnapshots` | `HistoricalStakeSnapshot`; boundary writer and historical/tower readers | `GlobalHashed`; the current value does not contain its `EtaPeriod`, so the key cannot be authenticated from the leaf alone. The ratified target stores `(EtaPeriod, HistoricalStakeSnapshot)`. Exactly the active retention window is permitted. | `G + P(20)` |
| 21 | `TowerEntries` | Dedicated local `MptTowerStore`; no canonical GL0 writer | `DENY` in the GL0 image. Tower bytes are validated by their dedicated-store grammar. Reserving a taxonomy number does not authorize a global leaf. | `DENY` |
| 22 | `KesRegistrationCerts` | `SortedSet[KesRegistrationRecord]`; GSI typed writer and KES state manager | `GlobalHashed`; nonempty, every record carries one identical `operatorPeerId`, derive `HASH32(peerId)`, and reproduce key. Enforce signature/domain, parent/ordinal chain, active-era limits, semantic cert identity uniqueness, and no cross-operator key collision. | `G` only |
| 23 | `LastKesRegistrationRefs` | `KesRegistrationReference`; GSI typed writer and KES pointer resolver | `GlobalHashed`; current value lacks `PeerId`, so the key is not self-authenticating. The ratified target stores `(PeerId, KesRegistrationReference)`. Require exactly one pointer per field-22 operator and an exact unique cert match; no dangling or extra pointer. | `G` only |
| 24 | `GenesisOperatorKeys` | `GenesisOperatorConsensusKey`; genesis writer and `L0GenesisLoader` | `GlobalHashed`; value's `operatorPeerId` derives `HASH32(peerId)`. Reproduce key; reject duplicates; validate long-term signature, network/genesis domain, uniqueness of the complete KES+VRF pair, and immutable genesis-only write epoch. | `G` only |
| 25 | `MgBalances` | `(Address, Balance)`; `infoEntryBytes`/currency writer and Mg reconstruction | `MGEntryAddress`; derive `mg` from network slot population relation and account from tuple. Reproduce exact key; reject duplicate `(mg, account)` and enforce amount bounds. | `G + C-info` |
| 26 | `MgLastTxRefs` | `(Address, TransactionReference)`; same writer/reader family | `MGEntryAddress`; account from tuple, exact key, duplicate rejection, valid reference. | `G + C-info` |
| 27 | `MgLastFeeTxRefs` | `(Address, TransactionReference)`; same writer/reader family | `MGEntryAddress`; account from tuple, exact key, duplicate rejection, valid fee-reference semantics. | `G + C-info` |
| 28 | `MgLastAllowSpendRefs` | `(Address, AllowSpendReference)`; same writer/reader family | `MGEntryAddress`; account from tuple, exact key, duplicate rejection, valid reference. | `G + C-info` |
| 29 | `MgLastTokenLockRefs` | `(Address, TokenLockReference)`; same writer/reader family | `MGEntryAddress`; account from tuple, exact key, duplicate rejection, valid reference. | `G + C-info` |
| 30 | `MgActiveTokenLocks` | `(Address, SortedSet[Signed[TokenLock]])`; same writer/reader family | `MGEntryAddress`; tuple account must equal every lock source; set is nonempty; exact key; duplicate lock identities rejected. The relationship between each lock's `currencyId` and network MG is frozen by the economic grammar. | `G + C-info` |
| 31 | `MgLastMessages` | `(MessageType, Signed[CurrencyMessage])`; same writer/reader family | `MGEntryHash`; tuple type must equal `message.messageType`; signed message `metagraphId` must equal network MG; derive `HASH32(messageType.value)`, reproduce key, require valid signature/domain, and reject duplicate `(mg,type)`. | `G + C-info` |
| 32 | `MgGlobalSnapshotSyncView` | `(PeerId, Signed[GlobalSnapshotSync])`; current `infoEntryBytes` writer and Mg reconstruction | **Target GL0: `DENY`.** Current shape is `MGEntryHash`, and a transitional audit must at least require tuple peer equals the sole valid signer and exact hashed key. It cannot become authoritative because these bytes are excluded from `mptRoot`. Remove only after the exact optional replay witness and explicit ML0 population pass `ROOT-010`; retain the field in ML0 `CurrencySnapshotInfo`. | current `X`; target `DENY` |
| 33 | `ConsumedAllowSpends` | `ConsumedAllowSpend`; direct cross-shard settlement writer and consumed-state manager | `GlobalHashed`; value's `allowSpendHash` is the user digest directly, not rehashed. Reproduce exact key and reject duplicate hash identities. Decoded owner/source/destination/currency/amount/expiry/reference claims are not authorization; O-13/XMG must join retained authorization and delivery evidence before the global kernel creates the permanent nullifier. It is never removed by acknowledgement or retention. | `G` only |
| 34 | `Slashings` | `SlashedRegistryEntry`; upheld-dispute GSAM writer and slash/cooldown readers | `GlobalHashed`; derive the exact composite `HASH32(canonicalTuple(peerId, shardId, checkpointHash))`, whose byte codec must be frozen with the manifest. Reproduce key and reject duplicate triples before map/set construction. Evidence, reason, event ordinal, cooldown, and the corresponding economic effects are adjudication invariants, not trusted record claims. Replace current canonical-JSON value codec with one frozen scodec codec before schema activation. | `G` only |

Current writer/value evidence for emitted fields 0-2, 4-5, 7-20, and 22-32 is
centralized in `GlobalStateConverter.toAllStateKeyValueBytes` and the typed rebuild
(`GlobalStateConverter.scala:859-1043,1479-1524,1564-1600,2461-2687`). The Mg
tuple codecs are at `CurrencySnapshotInfoCodecs.scala:170-207`; set and compound
global codecs are at `GlobalStateMptCodecs.scala:35-143`. Field 33 has a scodec
value codec (`ConsumedAllowSpendCodec.scala:18-64`). Field 34 instead uses a
private JSON `ImmutableCodec` (`InvalidStateProofSlashedReader.scala:52-68`), so
serde migration is not complete for this manifest.

The table deliberately says seven, not eight, rooted Mg info subfields. The code
set contains only IDs 25-31 and excludes 32
(`GlobalStateKey.scala:311-335`). Existing comments which call that set eight
subfields are stale and must be corrected with the field-32 removal.

### 4.1 Current writer/reader audit

This groups the concrete current call sites without implying that a value-blind
reader is safe:

| Physical IDs | Current writer | Current reader/materializer | Audit status |
|---|---|---|---|
| 0, 1, 2, 4, 5 | `GlobalStateConverter` full-state, accumulator-delta, and typed rebuild paths | `GlobalStateConverter.MptStoreReadOps` point and rooted-index reads (`GlobalStateConverter.scala:2037-2256`) | Codec bytes are centralized; complete structural and independently composed economic validation are missing. |
| 3 | No canonical typed target writer; full currency snapshots are converted to field 5 plus info | Legacy point read and field-3/5 union materializer (`GlobalStateConverter.scala:2102-2109,2174-2256`) | Retired live shape remains accepted/readable. |
| 6 | No canonical writer; fields 25-31 replaced it | No target reader; `CurrencySnapshotInfo` codec still exists | Retired field ID remains accepted. |
| 7, 10 | GSI/accumulator typed writer | `GlobalStateConverter` field-7 validator plus `AllowSpendStateManager` (`GlobalStateConverter.scala:1750-1819`; `AllowSpendStateManager.scala:504-528`) | Field 7 has local key checks; field 10 still depends on its index. Neither substitutes for whole-image relations. |
| 8, 9, 11 | GSI/accumulator typed writer | `TokenLockStateManager` (`TokenLockStateManager.scala:865-927`) | Field 8 still uses `headOption`; index-backed 9/11 need manifest-level bijection. |
| 12 | GSI/accumulator typed writer | `UpdateNodeParametersStateReader` and duplicate `GlobalStateConverter` reader (`UpdateNodeParametersStateReader.scala:40-50`; `GlobalStateConverter.scala:2273-2279`) | Both full scans currently discard physical keys. |
| 13, 14 | GSI/accumulator typed writer | `DelegatedStakeStateManager` and `NodeStakeAggregator` (`DelegatedStakeStateManager.scala:163-195`; `NodeStakeAggregator.scala:82-113`) | Values/head selection can drop empty or misplaced records. |
| 15, 16 | GSI/accumulator typed writer | `NodeCollateralStateManager` and `NodeStakeAggregator` (`NodeCollateralStateManager.scala:254-287`; `NodeStakeAggregator.scala:82-113`) | Same value-only defect; field-16 expiry also consumes local `WithdrawalTimeLimit`. |
| 17 | GSI/accumulator typed writer | `PriceStateUpdater.materializePriceStateFromMpt` (`PriceStateUpdater.scala:77-83`) | Value supplies a pair, but current reader does not check all pair copies or physical key. |
| 18 | GSI/accumulator typed writer | `GlobalStateConverter` point/index readers (`GlobalStateConverter.scala:2147-2149`; index projection `742-759`) | Requires exact index/leaf bijection. |
| 19 | `globalSnapshotSystemIndexEntries` and typed rebuild (`GlobalStateConverter.scala:724-856,2682-2687`) | exact index/bucket readers plus allow-spend/token-lock/collateral managers | Known point checks do not yet prove the complete inverse relation or rooted parameter input. |
| 20 | GSAM period-boundary accumulator plus typed writer (`GlobalSnapshotAcceptanceManager.scala:1360-1422`) | `HistoricalStakeReader.lookup` (`HistoricalStakeReader.scala:32-59`) | Point lookup trusts caller period; raw-image key cannot be derived from current value. |
| 21 | Dedicated `MptTowerStore`, not GL0 | dedicated tower store/verifier | Active GL0 enum acceptance is wider than intended ownership. |
| 22, 23 | GSI/accumulator typed writer | `KesRegistrationStateManager` (`KesRegistrationStateManager.scala:125-231`) | Field 22 checks homogeneous operator/key; field 23 identity is recovered indirectly and missing/invalid pointers can be omitted. |
| 24 | Genesis typed writer | `L0GenesisLoader.materializeRootedGenesisOperatorKeys` (`L0GenesisLoader.scala:454-483`) | Key/duplicate checks exist; whole-image and writer-epoch rules still belong in the manifest. |
| 25-32 | `infoEntryBytes`/currency typed writer (`GlobalStateConverter.scala:1479-1524`) | `reconstructCurrencyInfoFrom` (`GlobalStateConverter.scala:1824-1858`) | Reconstruction uses `entries.values`; field 32 is additionally root-excluded. |
| 33 | `AllowSpendConsumeHandler` and `CrossShardMessageEngine.write` (`AllowSpendConsumeHandler.scala:28-56`; `GlobalSnapshotAcceptanceManager.scala:2986-2991`) | strict `ConsumedAllowSpendStateManager` materializer (`ConsumedAllowSpendStateManager.scala:154-213`) | Focused parser now enforces canonical raw bytes, exact direct-hash key, malformed/absent failure, and unique semantic identity. Whole-image/recovery XMG-013 remains open. |
| 34 | direct GSAM upheld-dispute insert (`GlobalSnapshotAcceptanceManager.scala:2950-2975`) | `InvalidStateProofSlashedReader` and `SlashCooldownReader` | Readers discard physical keys; value codec is canonical JSON, not target scodec. |

### 4.2 Target writer and reader roles

The minimum target write whitelist is below. `ROOT-007` must prove and enforce
it against the complete economic grammar and diff scope; an unlisted role has
no write authority.

| Writer role | Permitted fields | Required authority |
|---|---|---|
| immutable genesis constructor | 24 | validated genesis input before ordinal-zero root; no runtime update |
| GL0 native/global transition | 0-2, 4, unscoped 7, 8-18, 22-23 | complete deterministic validation against one exact parent; external/GSI bytes are inputs, never write authority |
| replay-certified framework checkpoint apply | 5, MG-scoped 7, 25-31 for exactly the checkpoint MG set | exact Phase-2 bases and ordered inputs; every execution signer reproduces the diff/root and extracted global intents; distinct replay threshold plus separately domain-separated minimum positive assigned noncommittee coverage; every touched MG's signed `preRoot/preVersion` equals the proposal-parent mirror; all segments install atomically; adopter verifies certificate, coverage, namespace, continuity, compare-and-set, and resulting root without ordinary currency replay |
| deterministic System-index projection | 19 | derived atomically from the same accepted post-state and rooted active-era parameters; never accepted as an independent peer/checkpoint diff |
| historical boundary transition | 20 | exact active-era boundary/retention rule and canonical stake/eta source |
| global cross-shard settlement kernel | 33 | run by every GL0 validator over exactly the checkpoint certificate's signed extracted intents, against the same parent and atomically with the namespace-confined MG diff; exact-once authorization/nullifier transition; no MG diff may write it |
| upheld slash adjudication | 34 | replayed cryptographic evidence and deterministic economic transition; assertion/decoded record alone has no authority |
| no role | 3, 6, 21, 32 | always reject in target GL0 |

Field 7 is intentionally split by its contract slot: `None` is global/native
framework state and `Some(mg)` is that MG's framework state. A checkpoint may
never write another MG's scope, fields 33/34, any global balance/nullifier, or a
System index. The current per-MG commitment omits field 7; `ROOT-007` must add the
complete MG-scoped framework write set to the checkpoint root/diff before this
writer role can activate.

Target readers consume either a previously sealed `VerifiedGlobalStateImage` or
a field-specific proof/read capability minted from the same exact authenticated
parent session. A known-key point read may avoid rebuilding the whole typed
projection, but it may not bypass a relation on which its meaning depends. Full
map, restart, catch-up, bootstrap, reorg, and raw recovery always run the whole
image grammar. No GSI reconstruction, peer response, committee receipt, or
local cache is an alternate reader authority.

`VerifiedGlobalStateImage` proves only ROOT-008 placement, codec, identity,
index, and population relations. It does not prove authorization,
conservation, backing, replay protection, or transition validity. Economic use
also requires the same-parent O-07/ECON-G/P2 transition receipt; neither artifact
can substitute for the other.

## 5. Whole-image validation algorithm

The target parser returns a sealed, immutable `VerifiedGlobalStateImage`. No
consumer receives a typed map before all steps succeed.

1. Capture the exact parent/session and immutable ordered physical entry list.
   The parser accepts no live store callback and no "latest" lookup.
2. Apply consensus-versioned limits to total entry count and total raw key/value
   bytes before hashing, decoding, allocating collections, or grouping.
3. Run the `ROOT-011` whole-image physical preflight. Require lowercase,
   even-length, valid hex; no byte/nibble alias or terminal/prefix collision; and
   no duplicate physical key. `ROOT-011` does not assign field semantics.
4. Under `ROOT-008`, parse and enforce the exact permitted key length and
   namespace/field/contract/user slot shape, then reject unknown IDs and target
   `DENY` IDs. `fieldIdFromHex` is not this parser.
5. Group while retaining every `(actualKey, rawValue)` pair. Enforce per-field,
   per-MG, per-account, per-value-byte, and collection-member limits before
   materializing domain collections.
6. Decode the complete raw value with the one codec registered for the selected
   field. Reject empty/malformed/trailing bytes. Re-encode and require exact raw
   byte equality.
7. Extract the field's semantic identity and scope. Validate every member of a
   collection, recompute the canonical physical key, and require byte equality
   with `actualKey`.
8. Reject duplicate semantic identities before constructing any `Map`,
   `SortedMap`, `Set`, or `SortedSet`. No right-biased `toMap`, comparator
   collision, `headOption`, filtering, or drop-on-error behavior is permitted.
9. Run the complete relational pass in section 6. This includes absent/empty
   rules, not just relations among present values.
10. Compute the target complete root from exactly the accepted bytes and compare
    it with the exact authenticated `(snapshotHash, ordinal, mptRoot)` anchor.
    A root mismatch or unavailable parent is `RecoveryRequired`, before any
    state, finality, store, cursor, or watermark mutation.
11. Only then expose read-only typed projections. All writes/diffs pass through
    the same registry, canonical encoder, key reproducer, limits, and relational
    validator before atomic install.

The implementation should make the registry exhaustive in `GlobalStateFieldId`
at compile time. Each active field entry owns:

```text
field id
permitted physical shape
root-ownership class
one value codec
logical identity/scope extractor
canonical key reproducer
member and field-local validator
structural/index/population relation hooks
versioned resource limits
permitted transition writer role
```

There is no generic "unknown but rooted" extension lane. A new field requires a
new active-era schema and tests before any producer can emit it.

## 6. Complete structural and population relations

### 6.1 Active-address and pair indexes

`ActiveAddressIndex` currently covers logical field labels 0, 1, 2, 3, 4, 9,
10, 11, and 18. Field label 3 is used for the union of current physical currency
arms 3/5 (`GlobalStateConverter.scala:742-769,2145-2256`). The target manifest
has no physical field 3, so the target index must name the physical field-5
currency population or use a separate non-physical logical index discriminator.
It must not keep an active `LastCurrencySnapshots` physical schema merely to
name an index.

For each registered address/pair index:

- the index entry is absent iff its target population is empty;
- a present index set is nonempty;
- every index member has exactly one canonical target leaf;
- every target leaf appears exactly once in the index;
- no address is recovered from receiver-local GSI, peer lists, caches, or
  decoded map iteration.

Each lossy network/user digest must match exactly one member under the registered
relation. Zero matches is an unindexed/misplaced leaf; more than one match is a
digest collision and rejects rather than selecting by iteration order.

### 6.2 Expiry indexes

Only `ExpiryIndexAllowSpends`, `ExpiryIndexTokenLocks`, and
`ExpiryIndexNodeCollateralWithdrawals` may use the expiry-bucket shape. Every
active expirable record appears in exactly one bucket derived from its rooted
record. Every bucket item resolves to exactly one active target with the same
hash, source, scope, and derived epoch. Empty buckets, dangling items, wrong
epochs, duplicate target membership, and omitted eligible targets reject the
whole image.

An expiry key's epoch is recovered by resolving all of its bucket members to
their rooted targets and requiring one identical derived epoch. The physical
digest must then equal `HASH32(epoch.show)`. No bounded brute-force over a local
clock or current epoch is permitted.

Allow-spend expiry derives from `lastValidEpochProgress`; token-lock expiry from
`unlockEpoch`; collateral-withdrawal expiry from `createdAt +` the rooted
active-era `WithdrawalTimeLimit`. The current projection instead receives that
last value as external context (`GlobalStateConverter.scala:771-847`), which is
the open `ECO-IDX-03` consensus-input defect.

### 6.3 Currency population

For each MG in the canonical currency index:

- exactly one field-5 incremental exists;
- fields 25-31 contain only that MG's tuples and valid identities;
- required/optional emptiness semantics match the currency schema exactly;
- the field-5 root and 25-31 union root reproduce the signed currency proof;
- field-7 entries scoped to that MG obey the same execution base and framework
  transition, although field 7 remains a separate global partition/root;
- field 32 is never sourced from or installed into the target GL0 image.

No MG leaf may exist outside the canonical population relation. Decoder success
does not select the framework, framework-with-data, or opaque lane.

### 6.4 KES/VRF registration

Fields 22 and 23 form one relation. Each operator has one nonempty, canonical
cert chain and one pointer which resolves to exactly one record in that chain.
Every record is signed in the correct network/genesis/era domain and binds the
complete KES+VRF pair. Duplicate operator claims, duplicate active key pairs,
ambiguous references, missing pointers, pointer-only operators, or reuse that
violates the frozen protocol-era rotation policy reject the image. Field 24 is immutable
genesis identity and cannot be updated by the runtime registration writer.

Registration is not eligibility. The resulting registry is later intersected
with the exact delayed canonical operator/stake roster.

### 6.5 Historical stake and tower

Field 20 contains exactly the retained historical periods selected by the
active-era retention rule, with no duplicate/misplaced period. Current production
retains `currentPeriod` through `currentPeriod - 3`
(`GlobalSnapshotAcceptanceManager.scala:1360-1422`). The retention constant and
boundary rule must be rooted/versioned if changed.

Field 21 never appears in this image. A NiPoPoW tower proof is validated against
its authenticated snapshot/root commitments and dedicated-store grammar; a
local tower entry does not become GL0 state merely because the integer 21 is
known to `GlobalStateFieldId`.

### 6.6 Nullifiers and slash records

Field 33 is a permanent set keyed by the exact allow-spend content hash carried
in each value (`swap.scala:103-137`). A second physical key for the same hash, a
key/value mismatch, or removal/recreation fails before economic use.

Field 34 is keyed by the complete `(peerId, shardId, checkpointHash)` triple
carried in each value (`InvalidStateProofSlashManager.scala:212-241`). Parser
validity does not prove guilt. The cryptographic evidence replay/adjudication
path authorizes the transition; the stored record only preserves its rooted
result. A record cannot slash, extend cooldown, or burn funds merely because its
JSON/scodec bytes decode.

## 7. Consensus resource contract

`uint16` collection counts and JVM memory availability are not protocol bounds.
Before implementation activation, one active-era parameter record must freeze:

| Required constant | Applied before |
|---|---|
| maximum complete-image entries and raw bytes | sorting, hashing, or allocation |
| maximum entries and raw bytes per field | field grouping/decoding |
| maximum raw bytes per leaf for every field | value decode |
| maximum MG population and entries per MG/field | Mg tuple allocation |
| maximum collection members for fields 7, 8, 13-16, 19, 22, and 30 | element decode/materialization |
| maximum KES history records per operator | chain validation |
| exact historical-stake retention count | field-20 relation |
| maximum expiry-bucket members and buckets per image | expiry-index materialization |
| maximum slash/nullifier growth or authenticated compaction rule | unbounded-state acceptance |

These values cannot come from local config, command-line flags, peer claims, or
wall clock. Limits must be enforced identically by producer, replay signer,
ordinary adopter, restart, catch-up, reorg, bootstrap, proof service, and offline
migration verification. A limit violation defers/rejects the candidate; it does
not truncate, sample, filter, or partially install it.

Per-candidate work limits and permanent-state growth are different contracts.
Fields 33 and 34 cannot be dropped merely to remain below a finite image limit.
If their exact-once/evidence history is compacted, the replacement accumulator and
witness protocol must preserve authorization and replay semantics and pass
`GROWTH-001`; otherwise the protocol must explicitly support continued authenticated
growth. A numeric maximum which eventually makes an otherwise valid chain halt is
not a completed resource policy.

## 8. Ratified anchors and engineering freeze gates

The owner has ratified O-17/R008-01..07 as dispositioned in
`CONSENSUS-OWNER-DECISIONS-ANSWERS.md`. The concrete identities, codecs, resource values, and
proofs below remain engineering freeze gates. Implementation can build interfaces and RED tests,
but the target schema cannot activate until those gates close.

### O-17/R008-01 - Retired ID lifecycle

**Owner-ratified direction:** delete physical field cases 3, 6, and 21 from the active GL0
manifest and `fromInt` acceptance; keep their numeric values unallocated rather
than renumbering later fields. Replace field 3's current logical currency-index
label with field 5 or a distinct logical-index enum. Delete field 32 from active
GL0 only after `ROOT-010` witness parity, then leave numeric 32 unallocated.
Upstream-v4 disk import is an offline typed transform from the upstream schema,
not a decoder for abandoned fork-only physical fields.

Numeric gaps plus an offline typed import are locked; do not renumber active later fields and do
not retain abandoned fork-only compatibility decoders.

### O-17/R008-02 - Field-20 self-authentication

`HistoricalStakeSnapshot(stakes, eta)` does not carry the `EtaPeriod` used to
derive its physical key (`StakeDistribution.scala:150-175`;
`GlobalStateKey.scala:546-552`).

**Owner-ratified direction:** because this is a greenfield fork-only MPT layout, store
`(EtaPeriod, HistoricalStakeSnapshot)` and derive the exact key from the value.
The alternative is to invert the key over the exact candidate-ordinal retention
window and require one unique match. That alternative is more contextual,
couples parsing to retention, and is easier to misuse in recovery.

### O-17/R008-03 - Field-23 self-authentication

`KesRegistrationReference` carries only ordinal and hash, not `PeerId`
(`KesRegistrationCert.scala:66-84`).

**Owner-ratified direction:** store `(PeerId, KesRegistrationReference)`, then separately
prove it points to exactly one field-22 record. The alternative is a mandatory
join against every homogeneous field-22 chain and acceptance only when exactly
one operator resolves the reference. The tuple is simpler and makes point,
prefix, recovery, and proof readers share one key-reproduction rule.

### O-17/R008-04 - Consensus resource and growth contract

**Owner-ratified structure:** use measured, versioned gas/weight-style per-candidate work limits
and a non-halting authenticated compaction/accumulator contract for permanent nullifier/slash
growth. Engineering must derive numeric limits from worst-case valid snapshots and adversarial
benchmarks with headroom; no safe values follow from the current `uint16` codecs. A finite image
cap cannot become implicit consensus expiry.

### O-17/R008-05 - Semantic identity of set members

**Owner-ratified direction:** uniqueness for economic event sets is the canonical unsigned
event/content reference hash, not Scala object equality, arrival order, or a
signed wrapper whose proof list can vary. KES uniqueness is the signed cert
reference/domain identity. The exact identity function for allow-spends,
token-locks, delegated stakes, collateral, and KES records must be frozen with
the economic grammar and test vectors.

### O-17/R008-06 - Currency scope rules inside token-lock sets

**Owner-ratified rule:** field 8 accepts only native/global locks with `currencyId == None`;
field 30 accepts only `currencyId == Some(CurrencyId(owningMetagraph))`. No cross-metagraph or
native lock belongs in a metagraph partition. The parser must reproduce and enforce this relation.

### O-17/R008-07 - Field-32 deletion checkpoint

Deletion after L-15A witness parity is ratified; field 32 is not retained. Engineering must prove the exact signed
optional full-view witness, `None` versus `Some(empty)` vectors, explicit ML0
operator population, staged/backfill/restart/reorg parity, and missing-data
behavior before activation. After the proof gate closes, target GL0 rejects field 32
at producer diff, signer, adopter, disk, peer, bootstrap, and reorg boundaries.

## 9. Test obligations

The implementation gate is generated from the manifest, not maintained as a
handwritten sample list.

For every active target ID, generate at least:

- canonical round trip and exact key/value byte vector;
- every wrong namespace/field/contract/user slot and every extra/missing byte;
- unknown/forbidden ID and wrong root-ownership behavior;
- empty, malformed, trailing, noncanonical, and over-limit value;
- two physical keys for one logical identity;
- key/value identity or scope mismatch;
- wrong collection member, mixed collection, semantic duplicate, and excessive
  member count where applicable;
- clean replay, signer replay, ordinary diff adoption, restart, peer catch-up,
  bootstrap, shallow/deep reorg, and offline transform parity.

For every target `DENY` ID (3, 6, 21, and post-ROOT-010 32), every unknown ID, and
every malformed slot shape, generate rejection vectors only. They must not have a
canonical target round-trip fixture, encoder, writer, or compatibility decoder.
Field 32's transitional source-era fixtures live only in the ROOT-010 migration
suite and cannot be accepted as target GL0 state.

Relational suites additionally mutate one side of each address index, pair
index, expiry bucket, currency population, KES pointer, historical-retention,
nullifier, and slash relation while preserving syntactically valid values. Every
mutation must reject before typed state or store mutation even when the supplied
root correctly commits the malformed bytes.

Field 32 has a dedicated transition suite: reproduce same-root/different-replay
state first; prove the witness parity on every base/recovery/reorg path; delete
GL0 field 32; then prove every attempted field-32 ingress rejects while ML0's
currency state proof still commits its local view.

## 10. Dependency and parallelization order

The apparent ordering conflict between the current plan and test waves is
resolved as follows:

1. `ROOT-011` physical syntax/preflight and bounded builder collision failure
   land before any parser or raw-state installer can activate.
2. `ROOT-010` witness implementation/RED vectors and `ROOT-008` parser work for
   final rooted fields may proceed in parallel. `ROOT-008` has no target GL0
   field-32 grammar; a transitional audit of field 32 is not authority because
   its bytes are unrooted.
3. `ROOT-010` staged/backfill/restart/reorg parity passes.
4. Field 32 is removed and denied at every GL0 boundary.
5. The final target manifest, ratified anchors, derived limits, and generated field corpus
   freeze; `ROOT-008` consumers activate and key-blind parser families migrate in
   parallel.
6. `ROOT-009` installs only a complete `ROOT-011` + `ROOT-008` verified image in
   one transactional recovery generation.

This permits useful parser and test delegation without freezing an invalid
field-32 schema or installing pre-deletion bytes as target state.

## 11. Exit criteria

`ROOT-008` is complete only when:

- every ratified anchor above is encoded and every engineering freeze gate is closed;
- one exhaustive registry covers every permitted ID and rejects every other ID;
- field 34 has one frozen scodec value codec;
- fields 3, 6, 21, and post-`ROOT-010` 32 cannot enter a GL0 image;
- all symbolic resource limits have rooted/versioned numeric values;
- all generated `ROOT-008-F00` through `ROOT-008-F34` tests pass across producer,
  signer, adopter, restart, catch-up, reorg, bootstrap, and offline import;
- no consensus materializer uses `entries.values`, right-biased map/set
  construction, `headOption`, filtering, or drop-on-decode-error behavior;
- the accepted image is root-bound to the exact snapshot/parent session and is
  installed atomically only after the complete ROOT-008 structural/relational pass
  and every independently owned economic transition/oracle gate required for its
  intended use.

Until all exit criteria and the applicable O-07/ECON-G oracle gates pass, a valid
`mptRoot` is not sufficient evidence that the decoded GL0 economic state is
canonical or economically valid.
