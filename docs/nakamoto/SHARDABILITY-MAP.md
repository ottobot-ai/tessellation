# Shardability Map of `GlobalSnapshotAcceptanceManager.accept()`

**Purpose.** Pre-design research for execution-sharding of `gl0`. Classifies every derivation step inside `GlobalSnapshotAcceptanceManager.accept()` (henceforth GSAM) as **shard-local** (per-metagraph, can run inside a shard committee using only that metagraph's state + global background config), **cross-metagraph** (references state for metagraph X while processing data from metagraph Y — requires cross-shard coordination), or **global** (gl0-wide, not per-metagraph at all).

**Sources.** All citations in this document are line-stable as of HEAD `6e49b7d5` on `feature/serde-typeclass-shim`. The pipeline lives in:

- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotAcceptanceManager.scala` (2076 LOC; the `accept()` method spans lines 977–2072).
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelEventsProcessor.scala` (395 LOC; called by GSAM at line 369–388 of the GSAM file).
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/GlobalSnapshotStateChannelAcceptanceManager.scala` (263 LOC; per-metagraph chain-link admission).
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/infrastructure/snapshot/managers/global/MetagraphSyncManager.scala` (148 LOC; updates sync-info per-metagraph from snapshots + spend-actions).
- `modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/swap/SpendActionValidator.scala` (278 LOC; **load-bearing — see §3**).
- Per-partition state managers (each scoped to one `GlobalStateFieldId`): `AllowSpendStateManager`, `TokenLockStateManager`, `DelegatedStakeStateManager`, `NodeCollateralStateManager`, `SpendTransactionBalanceManager`, `RewardAcceptanceManager`, `TransactionReferenceManager`, `PriceStateUpdater`, `NodeStakeAggregator`.

**GSI shape.** `GlobalSnapshotInfo` is defined at `modules/shared/src/main/scala/io/constellationnetwork/schema/GlobalSnapshotInfo.scala:143–170`. It is the "snapshot context" — the per-ordinal canonical state. Its fields (and corresponding `GlobalStateFieldId` partition ids from `modules/shared/src/main/scala/io/constellationnetwork/schema/mpt/GlobalStateKey.scala:178–230`) form the unit of analysis below.

**Reading convention used throughout.** When a derivation reads "from MPT," it does so through a branch-aware `GlobalStateReader[F]` (`branchAwareReader` constructed at GSAM:258–261). Under `OverlayMode.Passthrough` this collapses to a direct base-store read; under `OverlayMode.MultiBranch` it walks the parent-branch chain to honor pending writes. For the purpose of shardability classification this distinction is irrelevant — what matters is **which partition / which metagraph the read addresses.**

---

## §1 Derivation inventory

The table below enumerates every meaningful derivation step in `accept()` in roughly the order it runs. "Per-MG" = pure per-metagraph (shard-local). "Cross-MG" = reads or writes touch state for a metagraph other than the one being processed. "Global" = derivation is hypergraph-scoped (no metagraph address keys it). "Mixed" = a single derivation produces some shard-local and some global outputs.

| # | Derivation | File:line | Input scope | Output scope | Class | Notes |
|---|---|---|---|---|---|---|
| 1 | `overlay.checkout(parentTip)` + `branchTipRef.set(parentTip)` | GSAM:1053–1058 | branch tip ref | `BranchHandle[F, GlobalStateKey]` | Global | Overlay infrastructure — frames the whole accept under one parent-branch view. |
| 2 | `acceptAllowSpendBlocks` | GSAM:1060–1068, BlockAcceptanceCoordinatorManager:85–115 | `Balances` + `LastAllowSpendRefs` partitions (per source addr) | `(AllowSpendBlockAcceptanceResult, contextUpdate.lastTxRefs)` | Global (DAG-layer) | AllowSpend blocks are DAG-layer (hypergraph), not metagraph-keyed. Source addresses are user addresses; reads are per-address from hypergraph partition. No cross-MG. |
| 3 | `acceptTokenLockBlocks` | GSAM:1060–1068, BlockAcceptanceCoordinatorManager:117–172 | `Balances`, `LastTokenLockRefs`, `ActiveTokenLocks` (per source addr, for replacement lookup) | `TokenLockBlockAcceptanceResult` | Global (DAG-layer) | Same: DAG-layer token lock blocks. Replacement-lookup reads from the user's own `ActiveTokenLocks` partition. |
| 4 | `tokenLockStateManager.acceptReplacementTokenLocks` | GSAM:1080–1083, TokenLockStateManager:265–304 | `ActiveTokenLocks(source)`, `Balances(source)` per replacement tx | filtered `List[Signed[TokenLock]]` | Global | Per-user check that replacement-target lock exists + balance covers delta; lock + balance both at user-level hypergraph keys. |
| 5 | `expiredTokenLocksHoisted` via `findExpiredGlobalTokenLocksViaIndexFromMpt` | GSAM:1093–1096, TokenLockStateManager:318–352 | `ExpiryIndexTokenLocks` system partition (epoch buckets) + `ActiveTokenLocks(addr)` for hash resolution | `SortedMap[Address, SortedSet[Signed[TokenLock]]]` | Global | System index sweep across `(prevEpoch, curEpoch)`; values resolved per-address from hypergraph partition. |
| 6 | `expiredAllowSpendsHoisted` via `findExpiredGlobalAllowSpendsViaIndexFromMpt` | GSAM:1097–1100, AllowSpendStateManager:338–374 | `ExpiryIndexAllowSpends` system partition + `ActiveAllowSpends(None, addr)` for hash resolution | `SortedMap[Address, SortedSet[Signed[AllowSpend]]]` | Global | Filtered to `metagraphId.isEmpty` (`AllowSpendStateManager:358`); strictly DAG-layer. |
| 7 | `acceptInitialData → blockAcceptanceCoordinatorManager.acceptBlocks` | GSAM:1102–1114, BlockAcceptanceCoordinatorManager:62–83 | `Balances` + `LastTxRefs` per source/destination addr (via `BlockAcceptanceContext.fromMpt`) | `BlockAcceptanceResult` (accepted txs + context delta) | Global (DAG-layer) | DAG-layer transaction blocks. Source/destination addresses are user addresses in hypergraph partition. **Caveat:** destination addresses can be ANY address, including metagraph addresses, but the validation is only of source-side `(balance, lastTxRef)` — no metagraph state is read. |
| 8 | `delegatedStakeStateManager.processExistingDelegatedStakes` | GSAM:1102 (via `acceptInitialData`), DelegatedStakeStateManager:65–160 | `lastSnapshotContext.activeDelegatedStakes`, `lastSnapshotContext.delegatedStakesWithdrawals` (GSI in-memory maps), `ActiveTokenLocks(addr)` per withdrawal-owning addr | `PartitionedRecords` (existing/unexpired/expired stakes & withdrawals) | Global | Delegated stake records are keyed by staker address (user), not metagraph. Token lock cross-ref is the staker's own lock (same address). |
| 9 | `updateDelegatedStakeAcceptanceManager.accept` | GSAM:1102, called inside `acceptInitialData` GSAM:328–335 | `lastSnapshotContext` (GSI fields for accepted CDS/WDS events), `acceptedGlobalTokenLocks` | `UpdateDelegatedStakeAcceptanceResult` | Global | Delegated-stake CDS/WDS events are user-signed and apply to user partitions; targeted at `nodeId` (operator). No metagraph involvement. |
| 10 | `updateNodeParametersAcceptanceManager.acceptUpdateNodeParameters` | GSAM:337–343 | `lastSnapshotContext.updateNodeParameters` | `SortedMap[Id, Signed[UpdateNodeParameters]]` | Global | Operator-keyed only. |
| 11 | `acceptNodeCollateral → updateNodeCollateralAcceptanceManager.accept` | GSAM:1116–1123, GSAM:352–367 | `lastSnapshotContext`, `delegatedStakeAcceptanceResult`, CNC/WNC events | `UpdateNodeCollateralAcceptanceResult` | Global | Operator collateral. Same shape as delegated stake. |
| 12 | `priorUpdateNodeParameters` from MPT prefix scan | GSAM:1130–1136 | `UpdateNodeParameters` partition (hypergraph, hash-keyed across whole network) | `SortedMap[Id, (Signed, SnapshotOrdinal)]` | Global | Network-wide enumeration of all operator parameter records. |
| 13 | `priorBalances` via `materializeAllBalancesFromMpt` | GSAM:1150, SpendTransactionBalanceManager:108–119 | `ActiveAddressIndex` (sidecar for `Balances`) + per-address `Balances` reads | `SortedMap[Address, Balance]` | Global | Full DAG balance map, address-keyed. Not per-metagraph. |
| 14 | `priorLastStateChannelSnapshotHashes` via per-metagraph MPT reads | GSAM:1169–1199 | `ActiveAddressIndex` for `LastStateChannelSnapshotHashes` + per-(metagraphAddr) `metagraph(addr, LastStateChannelSnapshotHashes)` | `SortedMap[Address, Hash]` (keyed by metagraph address) | **Per-MG** (one entry per MG, but **reads cover ALL MGs**) | Each entry pertains to exactly one metagraph (the MG whose last-binary hash is recorded). Reading the full map is cross-MG enumeration; reading one entry is per-MG. The acceptance step that *consumes* this map (state-channel admission) walks all MGs — see #15. |
| 15 | `priorLastCurrencySnapshots` via per-metagraph MPT reads | GSAM:1217–1256 | 3 MG-keyed partitions: `LastCurrencySnapshots`, `LastIncrementalCurrencySnapshots`, `LastCurrencySnapshotInfo` | `SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]` | **Per-MG** (one entry per MG, **reads cover ALL MGs**) | Same shape as #14. Per-metagraph atomically, network-wide as written. |
| 16 | `processStateChannelEvents → GlobalSnapshotStateChannelEventsProcessor.process` | GSAM:1258–1272, GlobalSnapshotStateChannelEventsProcessor:113–183 | `priorLastStateChannelSnapshotHashes`, `priorLastCurrencySnapshots`, scEvents, currentBalances; per-event `reader.get[CurrencySnapshotInfo](metagraph(addr, LastCurrencySnapshotInfo))` + `reader.get[Balance](hypergraph(Balances, addr))` for fee/staking address (lines 99–105) | `StateChannelAcceptanceResult` { scSnapshots, currencySnapshots, returnedSCEvents, currencyAcceptanceBalanceUpdate, incomingCurrencySnapshots } | **Per-MG** (each event scoped to one MG; **fee-related staking-balance read is cross-MG**) | The processor groups events `by(_.address)`. The per-event `buildSnapshotFeesInfo` reads the metagraph's own `LastCurrencySnapshotInfo`, then reads `hypergraph(Balances, stakingAddr)` — `stakingAddr` is a user address but it is sourced from THIS MG's CurrencySnapshotInfo, so the read is still per-MG by provenance. Fee deduction happens against `currentBalances` (the DAG global balance map). **The fee-bearing balance is a DAG-global address — see Cross-MG entry §3.** |
| 17 | `GlobalSnapshotStateChannelAcceptanceManager.accept` (chain-link admission) | GlobalSnapshotStateChannelAcceptanceManager:66–99 | per-MG `priorLastStateChannelSnapshotHashes(address)`, in-memory `firstSeenKeysForOrdinalR` (per-(addr, parent-hash) tracking) | `(SortedMap[MGAddr, NonEmptyList[Binary]], Set[StateChannelOutput])` | **Per-MG** | `events.groupBy(_.address)` → independent per-MG `acceptForAddress`. Chain-link is intra-metagraph. |
| 18 | `processCurrencySnapshots → currencySnapshotContextFns.createContext` | GlobalSnapshotStateChannelEventsProcessor:229–384, CurrencySnapshotContextFunctions:31–55 | Per-MG `priorLastCurrencySnapshots(address)`, plus `getGlobalSnapshotByOrdinal` callback (for currency-l0 validations that consult global state) | per-MG updated `CurrencySnapshotInfo` (the cl1/cl0 application apply step run on gl0 as re-execution) | **Per-MG** (output) **but** uses `getGlobalSnapshotByOrdinal` callback (potential reads of other MGs' state) | Note: each per-MG processing step is run via `parTraverse` (line 240). Within ONE metagraph the apply uses the prior MG state; the `getGlobalSnapshotByOrdinal` callback exists to let the currency snapshot validator reach back into prior global snapshots (e.g., for fee calc). **This is the key gl0-currency-re-execution that the v1 sharding design wants to eliminate via subtree-stub MPT** (see existing `project_sharding_direction_clarified.md`). |
| 19 | `transactionReferenceManager.acceptTransactionRefs` | GSAM:1274–1277, TransactionReferenceManager:35–48 | `LastTxRefs` partition lookups for new destination addrs | `SortedMap[Address, TransactionReference]` (deltas) | Global (DAG-layer) | Per-user txn-ref tracking. |
| 20 | `priorLastTxRefs` via `materializeLastTxRefsFromMpt` | GSAM:1283, TransactionReferenceManager:50–61 | `ActiveAddressIndex(LastTxRefs)` + per-addr reads | `SortedMap[Address, TransactionReference]` | Global | DAG-layer keyset enumeration. |
| 21 | `currencyBalances` derived from each MG's `CurrencySnapshotInfo.balances` | GSAM:1287–1290 | Per-MG (from #18 output) | `SortedMap[Option[Address], SortedMap[Address, Balance]]` — outer key is metagraph contract id | **Per-MG** | Each MG's internal balance map is keyed under that MG's address. |
| 22 | `sharedArtifacts` extraction (SpendActions, PricingUpdates, GlobalSnapshotsProcessed) | GSAM:1292–1357 | Per-MG `incomingCurrencySnapshots.artifacts` | Maps grouped by emitting MG address | **Per-MG (extraction)** but the *use* of SpendActions is cross-MG — see #25 | Each MG emits its own artifacts; extraction is per-MG. |
| 23 | `calculateRewards → calculateRewardsFn` (DelegatedRewardsDistributor) | GSAM:1305–1321, GSAM:390–418, DelegatedRewardsDistributor.scala | `delegatedStakeAcceptanceResult`, `unexpiredStakes`, `epochProgress`, facilitators (per-call), `lastSnapshotContext` | `DelegatedRewardsResult` (delegator rewards, withdrawal rewards, node-operator rewards, reserved-address rewards) | Global | Rewards are computed network-wide: facilitators are gl0 consensus participants, not per-MG. Output is global. |
| 24 | `rewardAcceptanceManager.acceptRewardTxs` | GSAM:1330–1333, RewardAcceptanceManager:31–51 | per-reward-destination `Balances(addr)` read (with delta merge) | balance updates + accepted reward txs | Global (DAG-layer) | Reward credits land on user addresses. Recipients are operators / reserved addrs — no metagraph keys. |
| 25 | `validateArtifacts → spendActionValidator.validateReturningAcceptedAndRejected` | GSAM:1361–1374, SpendActionValidator.scala | `spendActions` (per-MG), `lastActiveAllowSpends` (**cross-MG: all metagraphs' active allow-spends**), `currencyBalances` (**cross-MG: every MG's per-currency balance map**), `globalBalances` (DAG balances) | `(acceptedSpend, rejectedSpend)` per emitting MG | **CROSS-MG** | **CORE FINDING.** A SpendAction emitted by metagraph X (e.g., currencyId = X) can reference `allowSpendRef` whose underlying AllowSpend lives in the `ActiveAllowSpends(Some(Y), source)` partition for another metagraph Y. The validator does the lookup via `activeAllowSpends.get(spendTransaction.currencyId.map(_.value))` (`SpendActionValidator:194`) — keyed on the *transaction's* currencyId, which can differ from the *emitting* MG. **For the self-spend path** (`allowSpendRef = None`), the validator reads `allBalances(spendTransaction.currencyId.map(_.value))` (`SpendActionValidator:249`), which is the per-MG balance map for currency `currencyId`. This means validating a SpendAction emitted by MG X may require state from MG Y. |
| 26 | `validateArtifacts → pricingUpdateValidator` | GSAM:1361–1374 | `pricingUpdates`, `lastSnapshotContext`, `epochProgress` | accepted/rejected pricing updates | Global (price-oracle-wide) | Price state is global, not per-MG. |
| 27 | `lastActiveAllowSpends` via `allowSpendStateManager.materializeActiveAllowSpendsFromMpt` | GSAM:1359, AllowSpendStateManager:445–459 | `(HypergraphNamespace, ActiveAllowSpends)` prefix scan across ALL contract scopes | `SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]` | **Cross-MG enumeration** | Outer key is `Option[Address]` where `Some(metagraphAddr)` = scoped to that MG, `None` = DAG-global. Materializing this map enumerates allow-spends from ALL metagraphs' scopes. Required by SpendActionValidator (#25). |
| 28 | `globalActiveAllowSpends` (= `lastActiveAllowSpends` rebound) | GSAM:1412 | (no new read) | (no new map) | Cross-MG | Same materialized map; just a rebinding. |
| 29 | `globalActiveTokenLocks` via `tokenLockStateManager.materializeActiveTokenLocksFromMpt` | GSAM:1413, TokenLockStateManager:732–739 | `(HypergraphNamespace, ActiveTokenLocks)` prefix scan | `SortedMap[Address, SortedSet[Signed[TokenLock]]]` | Global (DAG-only) | TokenLocks live in the DAG-layer hypergraph partition (no metagraph scope on the key — key shape is `hypergraph(ActiveTokenLocks, addr)` per `TokenLockStateManager:734`). Per-user enumeration. |
| 30 | `globalActiveTokenLocksByRef` via `buildActiveTokenLocksByRefFromMpt(addresses)` | GSAM:1422–1424, TokenLockStateManager:721–730 | `ActiveTokenLocks(addr)` for `addresses = replacementSources ∪ expiredStakers` | `Map[Hash, Signed[TokenLock]]` | Global (DAG-layer) | Per-staker token-lock lookup for unlock generation. |
| 31 | `globalLastAllowSpendRefs` via `materializeLastAllowSpendRefsFromMpt` | GSAM:1426, AllowSpendStateManager:461–474 | `ActiveAddressIndex(LastAllowSpendRefs)` + per-addr reads | `SortedMap[Address, AllowSpendReference]` | Global (DAG-layer) | User-level. |
| 32 | `globalLastTokenLockRefs` via `materializeLastTokenLockRefsFromMpt` | GSAM:1427, TokenLockStateManager:741–752 | `ActiveAddressIndex(LastTokenLockRefs)` + per-addr reads | `SortedMap[Address, TokenLockReference]` | Global (DAG-layer) | User-level. |
| 33 | `allowSpendStateManager.acceptAllowSpendsWithExpired` | GSAM:1429–1436, AllowSpendStateManager:149–273 | per-MG `activeAllowSpendsFromCurrencySnapshots`, DAG-global `globalAllowSpends`, the cross-MG `lastActiveAllowSpends`, all accepted SpendTxn refs (cross-MG), expired set (#6) | `AllowSpendAcceptanceResult` { fullState, deltas, removedKeys, expiryIndexDelta } | **Mixed** | Folds per-MG allow-spends sourced from each MG's currency snapshot (per-MG inputs `activeAllowSpendsFromCurrencySnapshots: SortedMap[MGAddr, ...]`). DAG-global allow-spend bucket consumed in same pass. Removal driven by `allAcceptedSpendTxnsAllowSpendsRefs` which is cross-MG by construction (every SpendAction's refs collapsed into one set). |
| 34 | `allowSpendStateManager.acceptAllowSpendRefs` | GSAM:1442–1445, AllowSpendStateManager:326–330 | DAG-layer `lastAllowSpendRefs` ++ `contextUpdate.lastTxRefs` | merged map | Global (DAG-layer) | Per-user. |
| 35 | `allowSpendStateManager.updateGlobalBalancesByAllowSpendsWithExpired` | GSAM:1447–1456, AllowSpendStateManager:389–433 | `updatedBalancesByRewards`, DAG-global `globalAllowSpends`, expired set (#6); per-address `Balances(addr)` reads | balances + deltas | Global (DAG-layer) | Operates on the `None` scope — DAG-global allow-spends only. Per-user balance updates. |
| 36 | `nodeCollateralStateManager.acceptNodeCollaterals` | GSAM:1458–1463, NodeCollateralStateManager:94–126 | `lastSnapshotContext.activeNodeCollaterals/nodeCollateralWithdrawals`, expired-withdrawals from `findExpiredWithdrawalsViaIndexFromMpt` | `(existing, unexpired, expired)` | Global | Operator-keyed. |
| 37 | `nodeCollateralStateManager.getUpdatedCreateNodeCollaterals` | GSAM:1466–1469, NodeCollateralStateManager:168–196 | acceptance result + unexpired collaterals | updated collaterals map | Global | Per-operator. |
| 38 | `nodeCollateralStateManager.getUpdatedWithdrawNodeCollaterals` | GSAM:1471–1475, NodeCollateralStateManager:198–219 | acceptance result, unexpired, `lastSnapshotContext`; per-staker `ActiveNodeCollaterals(addr)` reads | updated withdrawals map | Global | Per-operator. |
| 39 | `tokenLockStateManager.generateTokenUnlocks` | GSAM:1477–1484, TokenLockStateManager:664–719 | expired withdrawals (per-staker addr), `acceptedGlobalTokenLocks` (replacements), `globalActiveTokenLocksByRef` (#30) | `Map[Address, List[TokenUnlock]]` | Global | Per-user unlock derivation. |
| 40 | `tokenLockStateManager.acceptTokenLocksWithExpired` | GSAM:1486–1496, TokenLockStateManager:239–263 (then 364–445) | `globalTokenLocks` (per-source), `globalActiveTokenLocks`, generated unlocks, expired set (#5); per-touched-addr `ActiveTokenLocks(addr)` reads | `TokenLockAcceptanceResult` { fullState, deltas, removedKeys, expiryIndexDelta } | Global (DAG-layer) | Per-user folds. |
| 41 | `tokenLockStateManager.acceptTokenLockRefs` | GSAM:1498–1501, TokenLockStateManager:306–310 | `lastTokenLockRefs` ++ `contextUpdate.lastTokenLocksRefs` | merged map | Global (DAG-layer) | Per-user. |
| 42 | `priorTokenLockBalances` via `materializeTokenLockBalancesFromMpt` | GSAM:1503, TokenLockStateManager:754–777 | `ActiveAddressIndex(TokenLockBalances)` (pair-shaped sidecar) + per-pair reads | `SortedMap[MGAddr, SortedMap[HolderAddr, Balance]]` | **Per-MG entries; reads cross-MG** | Outer key IS metagraph address. Each entry is one MG's view of locked-balance per holder. |
| 43 | `tokenLockStateManager.updateTokenLockBalances` | GSAM:1504–1509, TokenLockStateManager:576–623 | `currencySnapshots` (per-MG), `priorTokenLockBalances` | `TokenLockBalanceResult` { fullState, deltas, removedKeys } | **Per-MG** | Iterates MGs from the input, derives each MG's locked-balance map from its own `CurrencySnapshotInfo.activeTokenLocks`. Pure per-MG; the outer map's MGs are independent. |
| 44 | `tokenLockStateManager.updateGlobalBalancesByTokenLocksWithExpired` | GSAM:1533–1543, TokenLockStateManager:464–545 | `updatedBalancesByAllowSpends`, `globalTokenLocks` (DAG-layer locks per source), generated unlocks, expired set (#5); per-address `Balances(addr)` reads | balances + deltas | Global (DAG-layer) | DAG-layer locks affect DAG-layer balances. Per-user. |
| 45 | `allGlobalAllowSpends` hashing (over DAG-only + per-MG global bucket) | GSAM:1545–1588 | `globalAllowSpends ⊎ lastActiveGlobalAllowSpends` (DAG-layer `None`-scope only) | `SortedMap[Address, List[Hashed[AllowSpend]]]` | Global (DAG-layer) | Hashes only the `None`-keyed allow-spends. |
| 46 | `globalSpendTransactions` extraction | GSAM:1590–1595 | Per-MG `acceptedSpendActions`, filter `_.currencyId.isEmpty` | `List[SpendTransaction]` | **Cross-MG** (input is all MGs' SpendActions; output is the DAG-global subset) | A SpendAction emitted by MG X but with `currencyId = None` means "spend DAG balance" — the validator treated it as a DAG-balance-backed spend. These all converge into one global list. |
| 47 | `spendTransactionBalanceManager.updateGlobalBalancesBySpendTransactions` | GSAM:1597–1605, SpendTransactionBalanceManager:43–99 | `updatedBalancesByTokenLocks`, `allGlobalAllowSpends`, `globalSpendTransactions`; per-address `Balances(addr)` reads | balances + deltas | Global (DAG-layer) | Operates on DAG-global balances using ALL MGs' SpendTransactions (with currencyId=None). |
| 48 | `buildMerkleTreeAndProofs` over `updatedLastCurrencySnapshots` | GSAM:1607–1610, GSAM:521–600 | All MGs' `(addr, currencyState)` pairs | Merkle tree + per-MG proof | **Cross-MG by construction** | One Merkle tree across ALL metagraphs' last currency snapshots. Each leaf is one MG's `(addr, state)`. The tree root is part of the global state proof. Per-MG proofs (`SortedMap[Address, Proof]`) are what users / light clients verify. |
| 49 | `cleanStateMaps` | GSAM:1612–1631, GSAM:608–654 | All the post-acceptance maps + prior-key sets | cleaned maps + removed-key sets | Global | Pure data shuffling; computes which addresses' partitions dropped from non-empty to empty. |
| 50 | `priceStateUpdater.materializePriceStateFromMpt` | GSAM:1645, PriceStateUpdater:77–83 | `(HypergraphNamespace, PriceState)` prefix scan | `SortedMap[TokenPair, PriceRecord]` | Global | Price state is one map per token pair — there's no per-MG dimension. |
| 51 | `priceStateUpdater.updatePriceState` | GSAM:1646–1650, PriceStateUpdater:52–75 | priorPriceState, accepted pricing updates, epochProgress | delta map | Global | Operates on global price oracle state. |
| 52 | `metagraphSyncManager.acceptMetagraphSyncData` | GSAM:1653–1661, MetagraphSyncManager:42–129 | `lastSnapshotContext.metagraphSyncData` (per-MG sync info), per-MG `incomingCurrencySnapshots`, per-MG `globalSnapshotsProcessed` from artifacts, per-MG `acceptedSpendActions`, `currentGlobalOrdinal`/`currentGlobalEpochProgress` | per-MG `MetagraphSyncDataInfo` updates | **Per-MG entries; spend-action input cross-MG** | The two private methods walk a metagraph-keyed map. `updateFromSpendActions` (lines 103–129) groups spend-transactions by `currencyId.get.value` (the target metagraph), so it updates target-MG sync data based on transactions emitted by OTHER MGs. This is a **cross-MG write**: MG X's SpendAction (targeting MG Y) writes to `metagraphSyncData(Y)`. |
| 53 | `buildGlobalSnapshotInfo` | GSAM:1663–1685, GSAM:728–802 | All accumulated maps | new `GlobalSnapshotInfo` + boundary delta | Global (composition) | Pure assembly of the post-accept GSI. |
| 54 | `computeHistoricalStakeBoundaryDelta` | GSAM:794–801, GSAM:669–716 | (boundary ordinal only) `NodeStakeAggregator.snapshotFromMpt`, `etaForPeriod` callback | boundary `(adds, removes, next)` | Global | At eta-period boundaries: reads ALL operators' stake via the §G2 NodeStakeAggregator prefix-scan. Aggregator (`NodeStakeAggregator.scala:80–115`) sums `ActiveDelegatedStakes` and `ActiveNodeCollaterals` partitions across the entire network. |
| 55 | `priorDelegatedStakeKeys` etc. via `materialize*AddressesFromMpt` | GSAM:1615–1618, DelegatedStakeStateManager:162–172, NodeCollateralStateManager:222–232 | prefix scans on `ActiveDelegatedStakes`, `DelegatedStakesWithdrawals`, `ActiveNodeCollaterals`, `NodeCollateralWithdrawals` | `Set[Address]` of keys | Global | Network-wide keysets. |
| 56 | `nodeCollateralWithdrawalExpiryIndexDelta` (gated by feature flag) | GSAM:1704–1714, GSAM:423–461 | prior-vs-new pending withdrawals map (per-staker) | system-index delta on epoch-bucketed expiry keys | Global | Per-staker (no MG axis). |
| 57 | `StateChangesAccumulator` construction | GSAM:1716–1753 | all the deltas/removals | accumulator with per-partition delta sets | Global (composition) | Defines exactly which MPT partitions get touched this ordinal. |
| 58 | `AcceptanceMptStateChanges.applyStateChanges` | GSAM:1892 | accumulator | branch-handle accumulated writes | Global (physical writer) | One MPT writer; all field deltas applied together. |
| 59 | `mptStateProofFromBytes` / `builder.buildProof` | GSAM:1939–1944 | post-write byte view | `GlobalSnapshotStateProof` | Global | One root over the whole post-accept MPT. |
| 60 | Verify-replay (cross-check) | GSAM:1946–1993 | preSyncBytes, accumulator delta | reconstructed bytes; root comparison | Global | Self-check that producer agrees with delta-replay. |
| 61 | `artifactEmissionManager.emitAllExpiredArtifacts` | GSAM:2000–2003 | expired allow-spends + expired token-locks (DAG-global) | `AllowSpendExpiration`/`TokenUnlock` artifacts | Global (DAG-layer) | Per-record. |
| 62 | `emitLocalEvents` (publisher, fire-and-forget) | GSAM:2034–2051, GSAM:809–975 | priorBalances/postBalances diff, accepted SCSnapshots, etc. | gRPC events | Global (observability) | Out-of-band; not consensus-load-bearing. |

---

## §2 Pure per-metagraph derivations (shard-local)

Derivations that, given the prior state of metagraph M plus the snapshot content emitted for M, can run entirely inside the shard responsible for M without consulting any other metagraph's state.

### §2.1 State channel chain-link admission (`GlobalSnapshotStateChannelAcceptanceManager`)

- **File:** `GlobalSnapshotStateChannelAcceptanceManager.scala:66–99`.
- **Per-MG via:** `events.groupBy(_.address).toList.traverse` — each `acceptForAddress` call sees exactly one MG's events.
- **Inputs:** `priorLastStateChannelSnapshotHashes(address)` (the MG's last accepted binary hash); `firstSeenKeysForOrdinalR` cache (per-(addr, parentHash) tracking).
- **Outputs:** `(accepted, returned)` per MG.
- **Why shard-local:** Chain-link is intra-metagraph. The check `outputs.flatMap(o => references.contains(o.lastSnapshotHash))` (`onlyPossibleReferences:191–202`) never crosses metagraph boundaries. Signature-allowance lists are per-MG (`stateChannelAllowanceLists.get(address)`).
- **Sharding fit:** Perfect. Each shard's committee processes the binaries for its assigned MGs, runs chain-link, returns `(accepted_M, returned_M)` to gl0.

### §2.2 Per-metagraph currency-snapshot re-execution (`GlobalSnapshotStateChannelEventsProcessor.processCurrencySnapshots`)

- **File:** `GlobalSnapshotStateChannelEventsProcessor.scala:229–384` (already runs under `parTraverse` per metagraph).
- **Per-MG via:** `events.toList.parTraverse { case (address, binaries) => ... }`.
- **Inputs (per MG):** `priorLastCurrencySnapshots(address)`, the metagraph's binaries, `currentBalances` (DAG-global slice for fee deduction).
- **Outputs (per MG):** updated `CurrencySnapshotInfo`, balance updates limited to that MG's fee address.
- **Why mostly shard-local:** The apply step `currencySnapshotContextFns.createContext` (line 211) takes one MG's last state + one new snapshot and produces a new state. The data-application validators are MG-specific.
- **Caveat 1 — fee deduction touches DAG balances:** `processCurrencySnapshots` deducts fees from the MG owner's DAG address (`feeAddress` resolved from `state.lastMessages.flatMap(_.get(MessageType.Owner)).map(_.address)` at line 320), then mutates `currentBalances` (the DAG-global map). This means the fee path touches a global address. Mitigations:
  - Shard locally records "MG X charged owner_X N fees this ordinal"; gl0 applies the balance debits in a batched second pass.
- **Caveat 2 — `getGlobalSnapshotByOrdinal` callback:** This callback (passed in at line 122) is consumed inside the currency-snapshot validator. Empirically (see existing CL1 currency snapshot validation), this is used to verify *self-referential* fields that point to a prior global ordinal; the lookup is on a global snapshot by ordinal (not by address), so it's an oracle, not a cross-MG state read. **Worth confirming during sharding design** by tracing every consumer of the callback inside `currencySnapshotContextFns`.

### §2.3 Per-metagraph token-lock balance derivation (`TokenLockStateManager.updateTokenLockBalances`)

- **File:** `TokenLockStateManager.scala:576–623`.
- **Per-MG via:** outer `foldLeft` iterates `currencySnapshots` whose key IS the metagraph address; each iteration builds that MG's `metagraphTokenLocksAmounts` from `info.activeTokenLocks` (which lives inside the MG's own `CurrencySnapshotInfo`).
- **Inputs (per MG):** `currencySnapshots(M)` (the MG's incremental snapshot + info), prior `tokenLockBalances(M)` slice.
- **Outputs (per MG):** post-accept `tokenLockBalances(M)`.
- **Why shard-local:** No reads of other MGs' state. Removals (per `removedKeys`) are pair-shaped `(M, holder)`, but they're computed by diffing one MG's prior map vs its new map.
- **Sharding fit:** Direct. Each shard derives its MGs' lock-balance deltas; gl0 unions the deltas and applies them.

### §2.4 Per-metagraph artifact extraction

- **File:** GSAM:1292–1357, MetagraphSyncManager:72–101 (updateFromCurrencySnapshots only).
- **Per-MG via:** `incomingCurrencySnapshots.toList.parTraverse` (in MetagraphSyncManager:79) and explicit grouping by metagraph in GSAM:1292.
- **Inputs/outputs:** A MG's emitted SharedArtifacts (SpendActions, PricingUpdates, GlobalSnapshotsProcessed) — extracted from that MG's own snapshots. The `updateFromCurrencySnapshots` subset of `MetagraphSyncManager` only touches per-MG sync data based on each MG's own snapshots being accepted.
- **Sharding fit:** Per-MG. The cross-MG side of `MetagraphSyncManager` (the `updateFromSpendActions` step at line 103–129) is in §3.

### §2.5 Per-metagraph chain of derivations that operate only on the metagraph's own data

Several reads in `accept()` are network-wide enumerations of "one entry per metagraph" — they're `materialize*FromMpt` aggregations that return a single map keyed by metagraph address. The reads themselves are cross-MG (every entry pertains to a different MG), but each ENTRY is pure-per-MG. These are:

- `priorLastStateChannelSnapshotHashes` (#14)
- `priorLastCurrencySnapshots` (#15)
- `tokenLockBalances` (#42)

**Sharding implication.** Under sharding, the natural architecture is:
- Each shard maintains the entries for its assigned MGs locally (or fetches them from a peer in the same shard).
- gl0 never sees this map materialized in full at once — it accepts deltas (one shard's per-MG batch) and stitches them into MPT writes.
- The cross-MG "read all" patterns that exist today (`materializeActiveAllowSpendsFromMpt`, `materializePriceStateFromMpt`, `materializeTokenLockBalancesFromMpt`, etc.) need replacement with either subtree-stub reads + state proofs, or per-shard authoritative views.

---

## §3 Cross-metagraph derivations (need cross-shard coordination)

These derivations read state for a metagraph other than the one whose snapshot they're processing. Each entry below: **what is read**, **why**, **is the cross-ref essential or sidesteppable**, **natural cross-shard primitive**.

### §3.1 `SpendActionValidator` allow-spend cross-currency lookup — THE BIG ONE

- **File:** `SpendActionValidator.scala:115–262`. Called from `GSAM:478–482`.
- **What it reads:** The validator receives `activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]` — the FULL map across all metagraph scopes plus the DAG-global (`None`) scope. For each spend transaction, line 193–194 does `activeAllowSpends.get(spendTransaction.currencyId.map(_.value))` — looking up active allow-spends scoped to the SpendTransaction's `currencyId`.
- **Why this is cross-MG:** A SpendAction is emitted by metagraph X (in X's currency snapshot's `artifacts`). Each SpendTransaction inside that SpendAction has a `currencyId`, which can be `None` (DAG-global), `Some(X)` (same MG), OR **`Some(Y)` for a different MG Y**. In the third case the validator looks up the allow-spend in metagraph Y's allow-spend scope.
- **Per the validator's failure modes (`SpendActionValidator:206–238`):** the validator also enforces `signedAllowSpend.destination == currencyId` (the spending MG). Combined with the `approvers` check, this enforces "MG X can spend an allow-spend belonging to user U under MG Y's scope only if MG X is U's pre-approved destination." This is the explicit cross-metagraph swap primitive.
- **For the self-spend path** (`allowSpendRef = None`, `SpendActionValidator:247–262`): the validator reads `allBalances.getOrElse(spendTransactionCurrencyAddress, SortedMap.empty)` where `spendTransactionCurrencyAddress = spendTransaction.currencyId.map(_.value)`. The balances map keyed by `Some(otherMgAddr)` was constructed in GSAM:1287–1290 from EACH MG's `CurrencySnapshotInfo.balances`. So self-spend balance check **reads metagraph Y's internal balance state** when MG X emits a SpendAction with `currencyId = Some(Y)`.
- **Essential or sidesteppable?** Architecturally essential — this is the entire cross-metagraph atomic swap primitive (see existing `project_double_use_metagraph_funding.md` memory). Removing the cross-currency case would constitute a hard fork on the SpendAction semantics. Sidestepping options:
  - **Option A (sharding-friendly subset):** restrict v1 SpendActions to `currencyId in {None, sameMG}` (i.e., no cross-MG swaps). This is a hard fork (subset). For v1, defer all `currencyId in otherMG` SpendActions to a "global cross-shard validation pass" that runs after the per-shard accept and re-uses the existing validator. Practical for low-volume cross-MG flows.
  - **Option B (inclusion-proof cross-shard read):** when shard S(X) sees a SpendAction with `currencyId = Y`, S(X) requests an inclusion proof for `ActiveAllowSpends(Some(Y), source)` from S(Y) committee, verifies, then runs the validator with the proven entries injected.
  - **Option C (defer to gl0 global pass):** S(X) marks the SpendAction "needs cross-MG validation," forwards to gl0 leader; gl0 reads the cross-MG allow-spend set from the GSI (which is gl0-authoritative) and runs the validator. This is the simplest v1 path.
- **Natural cross-shard primitive:** State request + MPT inclusion proof. The validator only needs a small set of allow-spends per cross-shard SpendAction (the ones at `(Some(Y), source_user)`), not the whole MG state. The MPT key `hypergraph(ActiveAllowSpends, Some(Y), source_user)` is point-readable.

### §3.2 SpendActionValidator currency-balance lookup for self-spend

- **File:** `SpendActionValidator.scala:247–262`.
- **What it reads:** `allBalances(Some(otherMgAddr))` — when a SpendTransaction has `allowSpendRef = None` and `currencyId = Some(Y)`, the validator checks the spender's MG-internal balance under MG Y.
- **Cross-MG flavor:** same as §3.1 — MG X's SpendAction can self-spend MG Y's currency balance (subject to `source == currencyId` line 256–259).
- **Mitigation:** same as §3.1.

### §3.3 `MetagraphSyncManager.updateFromSpendActions` — cross-MG sync-data write

- **File:** `MetagraphSyncManager.scala:103–129`.
- **What it writes:** Groups all metagraphs' SpendActions by the SpendTransactions' `currencyId.get.value` (target MG), then writes `metagraphSyncData(targetMG)` for each target MG. So MG X's emitted SpendActions can cause writes to MG Y's `metagraphSyncData`.
- **Cross-MG flavor:** WRITE-side, not READ-side. The output map indexes the modified entries by target MG, not by emitting MG.
- **Essential or sidesteppable?** This is a sync-tracking artifact (informs MG Y "you have N global ordinals worth of unapplied changes targeting you"). It is not consensus-critical for state-machine progress; it's a hint to MG Y's clients about when they should re-pull global state. **Sidesteppable** as far as sharding goes:
  - Shard S(X) emits a "spend-action-target = Y, ordinal = O" event.
  - Shard S(Y) consumes those events from a cross-shard inbox and updates its own `metagraphSyncData(Y)` entry.
- **Natural cross-shard primitive:** Async receipt / cross-shard message queue.

### §3.4 `cleanStateMaps` "active allow-spends" removed-key derivation

- **File:** `AllowSpendStateManager.scala:260–268`.
- **What it touches:** Iterates `lastActiveAllowSpends` (cross-MG map keyed by `Option[Address]` metagraph scope) and emits `(metagraphIdOpt, address)` pairs as removed-key targets when the post-accept state has dropped a `(MG, user)` allow-spend entry.
- **Cross-MG flavor:** structural — the removal-set spans MGs. But each entry is "this MG just lost this allow-spend"; the removal is per-MG semantically.
- **Sidesteppable:** Trivially — per-shard removals merged into one set at gl0.

### §3.5 Currency-snapshot fee deduction touches DAG-global balance

- **File:** `GlobalSnapshotStateChannelEventsProcessor.scala:320–357`.
- **What it touches:** Per-MG: reads MG's `Owner` address from `state.lastMessages`. Cross-MG: that owner address's balance lives in the DAG-global `Balances` partition, and the fee deduction mutates that DAG balance directly via the `balanceUpdate` accumulator returned per-MG.
- **Architecture note:** Each shard derives "MG M deducted F units from address owner_M" as a per-MG delta. These deltas are non-conflicting at the user-address level *as long as* one owner_M is not also another MG's owner — which is normally true but is **not enforced by the type system**. If two shards both deduct from the same address (e.g., a service account owning multiple MGs), the deltas conflict.
- **Mitigation:** gl0 sequentializes the balance deltas (same as the v1 sharding plan for any DAG-balance writes). Since fee math is `b - sum(deltas)`, ordering is associative — but the underflow check at `balance.minus(head.fee).toOption` (line 349) is order-sensitive (when balance is near zero, ordering decides which deductions succeed). **This is a subtle determinism risk** if shards process their MGs at different orders — for the conservative path, gl0 should re-execute fee deductions in a canonical order after gathering shard outputs.

### §3.6 `acceptAllowSpendsWithExpired` cross-MG allow-spend consumption

- **File:** `AllowSpendStateManager.scala:149–273`.
- **What it touches:** Takes `allAcceptedSpendTxnsAllowSpendsRefs` (the union of all SpendTransactions' `allowSpendRef`s, irrespective of emitting MG) and removes any matching allow-spend from the active sets. The removal can hit any `(MG, user)` scope, so a SpendAction emitted by MG X can cause an allow-spend in MG Y's scope to be consumed.
- **Cross-MG flavor:** WRITE-side cross-shard. MG X's emitted SpendAction consumes MG Y's allow-spend.
- **Essential or sidesteppable?** Essential — this is the consumption side of the cross-MG swap primitive (§3.1). Same mitigations.

### §3.7 `buildMerkleTreeAndProofs` over all metagraphs' last currency snapshots

- **File:** `GSAM:521–600`. Called at line 1607.
- **What it touches:** One Merkle tree across ALL metagraphs' `(address, lastCurrencyState)` pairs. The tree root is consumed by the GSI's `lastCurrencySnapshotsProofs` field and is part of the global state proof.
- **Cross-MG flavor:** STRUCTURAL — the root commits to all MGs at once. Per-MG proofs are derivable but the build requires all leaves.
- **Sharding solution:** Reflect-into-MPT: subtree-stub the `lastCurrencySnapshots` partition per MG, root the per-MG state under a per-MG subtree, and let the global root commit only to the per-MG subtree roots. This is the natural Merkle-tree-of-Merkle-trees pattern. The existing `LastCurrencySnapshotsProofs` partition (`GlobalStateFieldId = 4`) already separates the proofs partition from the snapshot partition; the design space is open.
- **Performance impact in v1:** Even without restructuring, this is `O(N_metagraphs)` work — the parallel `batchSize = 256` parallelization at line 544 helps but doesn't shard. For thousands of MGs this is a real bottleneck.

### §3.8 SC-binary admission's `firstSeenKeysForOrdinalR` cache

- **File:** `GlobalSnapshotStateChannelAcceptanceManager.scala:61, 140–189`.
- **What it touches:** A single in-memory `Ref[Map[(Address, Hash), (Long, Set[Hash])]]` shared across all metagraphs.
- **Cross-MG flavor:** Shared mutable state across MGs (but only used for per-MG pull/purge timing). Different MGs' entries don't interact; the map is keyed by `(Address, Hash)`.
- **Sidesteppable:** Per-MG ref local to each shard.

---

## §4 Global derivations (gl0-wide, not per-metagraph)

These derivations have no per-MG axis at all — they pertain to network-wide state. In a sharded architecture they continue to run on gl0 (or on every committee, since they're deterministic from global state).

### §4.1 DAG-layer block acceptance (transactions, allow-spend blocks, token-lock blocks)

- **Steps:** #2, #3, #7 in §1.
- **Per-user but not per-MG.** Source/destination addresses live in the DAG `Balances`/`LastTxRefs` partition; no MG axis.
- **Sharding role:** Stays on gl0 leader (one execution); alternatively, sub-shardable by user address but that's a separate axis from MG sharding.

### §4.2 Rewards distribution (`DelegatedRewardsDistributor.distribute`)

- **Step:** #23 in §1.
- **Cross-cutting:** Reads `lastSnapshotContext` for delegated-stake and node-collateral state, computes per-operator rewards, per-delegator rewards, reserved-address rewards. Facilitators are gl0 consensus participants.
- **Sharding role:** Global. Runs on gl0 leader.

### §4.3 Price oracle state (`PriceStateUpdater`)

- **Steps:** #26, #50, #51 in §1.
- **Cross-cutting:** Price state is keyed by `TokenPair`. Pricing updates come from multiple MGs but are aggregated globally.
- **Sharding role:** Global. Runs on gl0 leader OR can be moved to a dedicated "price-oracle shard" later.

### §4.4 NIPoPoW S0.4 boundary writes (`HistoricalStakeSnapshots`, `NodeStakeAggregator`)

- **Steps:** #54 in §1; supported by NodeStakeAggregator at the listed file.
- **Cross-cutting:** Runs only on boundary ordinals (`ord % R == R - 1`). Reads the entire `ActiveDelegatedStakes` + `ActiveNodeCollaterals` partitions to compute per-operator combined stake.
- **Sharding role:** Strictly global. Eta randomness is one value per period; stake distribution is one map. Even if collateral and delegated-stake records can in principle be sharded by `nodeId`, the aggregation across the network has to converge to one map for slot-leader eligibility.

### §4.5 DAG-global TokenLock balances (`updateGlobalBalancesByTokenLocks`)

- **Steps:** #40, #44 in §1.
- **Per-user.** Operates on the DAG-global `Balances` partition using DAG-layer (None-scope) token locks.
- **Sharding role:** Stays on gl0 leader for v1 (it's already global by partition). User-address sharding is a separate axis.

### §4.6 DAG-global AllowSpend balances (`updateGlobalBalancesByAllowSpends`)

- **Steps:** #35 in §1.
- **Per-user.** Operates on DAG-global `Balances` partition using DAG-layer (None-scope) allow-spends.
- **Sharding role:** Stays on gl0 leader.

### §4.7 DAG-global SpendTransactions balances (`updateGlobalBalancesBySpendTransactions`)

- **Step:** #47 in §1.
- **Mixed input source:** `globalSpendTransactions` are extracted from ALL MGs' accepted SpendActions filtered to `currencyId.isEmpty` (DAG-target). Output is DAG-balance updates.
- **Sharding role:** Stays on gl0 leader (input is cross-shard; output is DAG-global).

### §4.8 Delegated stake / Node collateral updates (`UpdateDelegatedStakeAcceptanceManager`, `UpdateNodeCollateralAcceptanceManager`)

- **Steps:** #8, #9, #10, #11 in §1.
- **Operator-keyed (no MG axis).** Records are keyed by staker address and node id.
- **Sharding role:** Stays on gl0 leader, OR could be sharded by operator (different sharding axis).

### §4.9 Pricing update validation (`PricingUpdateValidator`)

- **Step:** part of #26.
- **Cross-cutting:** Pricing updates can be emitted by any MG but they all converge to one price-oracle state.
- **Sharding role:** Same as §4.3.

### §4.10 MPT writer (`AcceptanceMptStateChanges.applyStateChanges`) and proof builder

- **Steps:** #58, #59, #60.
- **Cross-cutting:** One MPT per gl0 node; one root.
- **Sharding role:** Stays on gl0 — but the *inputs* (deltas) are produced cross-shard.

---

## §5 Implications for the shard architecture

### §5.1 What fraction is shard-local?

Counting steps and weighting by their cost:

- **Pure per-MG (shard-local) work:** SC chain-link admission (§2.1), per-MG currency-snapshot re-execution (§2.2 — usually the heaviest single piece by CPU, since it runs the cl0/cl1 data-application validators), per-MG token-lock balance derivation (§2.3), per-MG artifact extraction (§2.4), per-MG sync-data updates from snapshots (subset of #52).
- **Expected fraction:** When the workload is currency-snapshot-heavy (which is the case at scale — each MG produces a snapshot per ~7s), §2.2 dominates. **A first-cut sharding scheme that ONLY shards §2.2 already gives most of the value** (the heaviest work per MG is per-MG and is already wired into `parTraverse` at line 240).

### §5.2 What fraction is cross-MG?

- **The hard cases are SpendAction cross-currency validation (§3.1, §3.2) and its consumption (§3.6).** Volume-wise, this is a SMALL fraction of overall work (SpendActions are sparse compared to currency snapshot frequency), but it's PROTOCOL-critical for the cross-metagraph swap feature.
- **Soft cross-MG cases (sync-data writes §3.3, fee-balance writes §3.5):** Easy to handle via second-pass merging on gl0.
- **Structural cross-MG (Merkle tree §3.7):** Reorganizable via per-MG subtree roots — design exercise.

### §5.3 What fraction is global?

- **All DAG-layer work** (blocks, allow-spends, token-locks, balances when DAG-scoped, transaction refs): §4.1, §4.5, §4.6, §4.7.
- **Rewards distribution, price oracle, NIPoPoW boundary writes, delegated stake / node collateral updates:** §4.2, §4.3, §4.4, §4.8, §4.9.
- This is a meaningful fraction of code but it's WORK that needs to run on gl0 regardless of sharding. The point is sharding doesn't help here, but it doesn't need to.

### §5.4 Recommended v1 scope

**Tier 1 (shard now, gives most of the gains):**
1. Per-MG currency-snapshot re-execution (§2.2). Largest single chunk of CPU. Per-MG is structurally clean; only fee-balance bookkeeping needs careful merge.
2. SC chain-link admission (§2.1). Trivially per-MG.
3. Per-MG token-lock balance derivation (§2.3). Pure per-MG.
4. Per-MG metagraph-sync data updates from snapshots (§2.4 + half of #52).

**Tier 2 (shard with a cross-shard coordination protocol):**
5. Cross-currency SpendAction validation (§3.1, §3.2). Needs Option B (inclusion-proof cross-shard read) OR Option C (defer to gl0). Recommended v1 path: **Option C** (defer to gl0). Cross-MG SpendActions are sparse; running them on gl0 leader is cheap. Promote to Option B in v2 when volume justifies it.
6. Cross-MG sync-data writes (§3.3). Use a cross-shard message queue.

**Tier 3 (keep on gl0 for v1, don't try to shard):**
7. DAG-layer block acceptance (§4.1).
8. Rewards distribution (§4.2).
9. Price oracle (§4.3).
10. NIPoPoW boundary writes (§4.4).
11. DAG-global balance updates by AllowSpend / TokenLock (§4.5, §4.6).
12. SpendTransactions targeting DAG balance (§4.7).
13. Delegated stake / node collateral (§4.8).
14. MPT writer (§4.10) — gl0 writes the trie, but per-shard committees produce verified per-MG state proofs that gl0 stitches into a per-MG subtree.

### §5.5 v1 architecture sketch (extracted from §5.4)

```
                                     +----------------------+
                                     |  gl0 leader          |
                                     |  (global accept)     |
                                     +----------+-----------+
                                                |
                       +------------------------+------------------------+
                       |                        |                        |
                  +----v----+              +----v----+              +----v----+
                  | Shard 1 |              | Shard 2 |              | Shard K |
                  | committee|             | committee|             | committee|
                  +----+-----+             +----+-----+             +----+-----+
                       |                        |                        |
              Per-MG re-execution        Per-MG re-execution       Per-MG re-execution
              for MGs in shard 1         for MGs in shard 2        for MGs in shard K
              (§2.1, §2.2, §2.3, §2.4)   (same)                    (same)
                       |                        |                        |
                       +------------------------+------------------------+
                                                |
                                   Signed (per-MG subtree-root, per-MG deltas) packets
                                                |
                                                v
                                     +----------------------+
                                     |  gl0 leader          |
                                     |  - verifies signatures
                                     |  - stitches deltas   |
                                     |  - runs global Tier 3
                                     |  - cross-MG SpendActions (Tier 2 #5)
                                     |  - cross-MG sync writes (Tier 2 #6)
                                     |  - assembles GSI + MPT root
                                     +----------------------+
```

### §5.6 Things sharding does NOT solve

- **NIPoPoW boundary cost.** The §4.4 boundary write needs a global view of stake; aggregating across the network is unavoidable. Per-shard pre-aggregation can lower the merge cost but the boundary is still a synchronization point.
- **Cross-MG SpendAction latency.** Whether by Option C (gl0 pass) or Option B (inclusion proof), the cross-MG path is intrinsically slower than the same-MG path.
- **Merkle tree of all MGs.** The current monolithic tree (#48) must be redesigned into per-MG subtrees for the structural benefit to materialize.

---

## §6 Open questions for the designer

1. **§3.1 / §3.2 — `getGlobalSnapshotByOrdinal` callback contents.** Inside `processCurrencySnapshots → currencySnapshotContextFns.createContext`, the callback is passed deep into the per-MG validator. What does it actually read? An audit of `CurrencySnapshotValidator` (cl1 module) would confirm whether the lookup is purely on global oracle state (e.g., "what was DAG balance at ordinal O?") or whether it can reach into other MGs' state. The current code path (`CurrencySnapshotContextFunctions:31–55`) routes it as opaque; I didn't trace into cl1 in this pass.

2. **§3.5 — fee deduction ordering determinism.** Today fee deductions mutate `currentBalances` in `parTraverse` per metagraph (line 240 in the processor). Output `balanceUpdate` per-MG is then merged at line 172 with `foldLeft(...)(_ ++ _)`. With overlapping owner addresses, the LAST `++` wins (Map semantics). For unique owner addresses this is fine. Designer needs to confirm: is uniqueness of MG-owner addresses an enforced invariant, or a contingent fact? If contingent, the v1 sharding design must canonicalize the fee-deduction order at gl0.

3. **§4.2 — facilitator set under sharding.** `calculateRewardsFn` receives facilitators (line 1320–1321 via `calculateRewards`). Today these are the gl0 consensus participants. Under sharding, are the rewards still distributed only to gl0 facilitators, or also to shard committees? Probably the latter. This is a reward-design question, not a code-mapping question.

4. **§4.4 — eta-period boundary visibility across shards.** When a shard committee accepts a snapshot at the boundary ordinal, it must observe the same `etaForPeriod` value as every other shard. Today this is sourced from a callback wired into gl0's chain store (Path 1 work — see `etaForPeriod` at line 226). Shard committees need access to the same chain store or to a deterministically-broadcasted eta value. Mitigation: gl0 leader broadcasts eta at the start of each period.

5. **`SpendActionValidator` accumulator update determinism.** The validator's `processActionsForCurrency` (`SpendActionValidator:51–78`) folds over each MG's spend actions WITHIN that MG (good — per-MG fold). But the outer `foldLeftM` over `spendActions.toList` (line 80) iterates MGs in MAP iteration order — `spendActions` arrives as a `Map[Address, ...]`. **CRITICAL DETERMINISM CHECK:** At line 1344 (GSAM) the input is `sharedArtifacts.mapValues(...).filter(...).toSortedMap`, which guarantees sorted order. Inside the validator, the conversion at line 80 is `.toList` which respects the `Map`'s iteration order. **Open question:** is the Map known to be sorted at that point? Worth verifying that the validator's contract is "input MUST be SortedMap" — currently the trait signature `Map[Address, ...]` doesn't enforce this. If not, sharding could re-order across shards and produce divergent accept/reject lists.

6. **`materializeActiveAllowSpendsFromMpt` scope.** The implementation (`AllowSpendStateManager:445–459`) prefix-scans across ALL contract scopes (`hypergraphFieldPrefixAcrossContracts`). Under sharding, can each shard maintain only its own MG slice of this map plus the DAG-global (`None`) slice? Yes for the per-MG slice, but the DAG-global slice must be replicated to every shard (it's used by every shard's SpendAction validator that has DAG-currency-Id spend transactions). Probably acceptable since the DAG-global slice is small per-user.

7. **`buildActiveTokenLocksByRefFromMpt(addresses)` cross-MG coupling.** The address set fed in is `acceptedGlobalTokenLocks.map(_.value.source).toSet ++ initialData.existingStakes.expired.keySet` (GSAM:1420–1421). The expired staker addresses come from prior state (`processExistingDelegatedStakes`), which is computed without any per-MG slicing. Under sharding, the expired staker address set is global; the token-lock lookup is per-staker hypergraph reads. Not technically cross-MG since TokenLocks live in DAG-global partition, but the orchestration coupling is non-trivial.

8. **What does each shard need to materialize?** The current `accept()` builds a complete in-memory view of post-accept state in the GSI and writes deltas to MPT. Under sharding, each shard's committee needs to produce a verified per-MG delta packet PLUS enough cross-MG context for the leader to detect conflicts. Question: what's the minimum cross-MG context (e.g., "I accepted these SpendActions targeting MGs X, Y, Z; please re-verify the cross-MG ones")?

9. **MPT subtree-root design.** Existing `LastCurrencySnapshotsProofs` (FieldId 4) already separates per-MG Merkle proofs from snapshots. Is there appetite to make `lastCurrencySnapshots` itself a per-MG subtree under a global Merkle commitment (a tree of trees)? This would let shards independently compute their MG subtree roots and present them to gl0 as proofs.

10. **Cost of `priorBalances` materialization at gl0.** GSAM:1150 materializes the FULL DAG balance map every accept(). At tens of thousands of operators this is the prefix-scan cost growing with all users. Not a sharding question per se, but a scaling question that becomes more pressing as the cluster grows. Existing MPT keyset sidecar (`ActiveAddressIndex(Balances)`) helps with the keyset; the values are read in one batch via `getMany`.

11. **`StateChannelValidator.getFeeAddresses` cross-MG iteration.** Called at `GlobalSnapshotStateChannelEventsProcessor:127` over `priorLastCurrencySnapshots` (network-wide map). Each MG's fee addresses are extracted from its currency snapshot info. Under sharding, each shard would need to materialize this set for its own MGs; cross-MG access (e.g., a MG event referencing another MG's fee address) doesn't appear from the code I read but worth verifying.

12. **Are SpendActions ever validated against a state YOUNGER than the input snapshot's `lastActiveAllowSpends`?** Today `lastActiveAllowSpends` is materialized fresh per accept() at line 1359 — it reflects the post-block, post-allow-spend-block state but NOT any allow-spends already consumed earlier this round. The validator's fold-in `processActionsForCurrency` (`SpendActionValidator:60–74`) does maintain a running `allowSpendsAcc` so within-round consumption is tracked. Sharding consideration: if shard S(X) sees an allow-spend in MG Y's slice but shard S(Y) has already accepted a SpendAction consuming it in the same round, S(X)'s view is stale. Need a same-round consumption broadcast OR move all cross-MG SpendActions to a serialized gl0 pass.
