package io.constellationnetwork.dag.l0.infrastructure.snapshot

import java.security.KeyPair

import cats.Parallel
import cats.data.NonEmptySet
import cats.effect.kernel._
import cats.effect.std._
import cats.effect.syntax.all._
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.dag.l0.config.types.AppConfig
import io.constellationnetwork.dag.l0.domain.snapshot.programs.{
  GlobalSnapshotEventCutter,
  SnapshotBinaryFeeCalculator,
  UpdateNodeParametersCutter
}
import io.constellationnetwork.dag.l0.infrastructure.rewards.RewardsService
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.schema.{GlobalConsensusKind, GlobalConsensusOutcome}
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.json.{JsonBrotliBinarySerializer, JsonSerializer}
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.SharedConfig
import io.constellationnetwork.node.shared.domain.cluster.services.Session
import io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.{EtaSourceRange, EtaSourceUnavailable}
import io.constellationnetwork.node.shared.domain.nakamoto.ShardWindowContinuation
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.domain.statechannel.{FeeCalculator, FeeCalculatorConfig}
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.consensus._
import io.constellationnetwork.node.shared.infrastructure.consensus.engine.{ConsensusCommand, ConsensusEventLoop, _}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoTriggerDaemon
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.NakamotoTriggerDaemon.NakamotoTriggerState
import io.constellationnetwork.node.shared.infrastructure.consensus.state._
import io.constellationnetwork.node.shared.infrastructure.gossip.RumorHandler
import io.constellationnetwork.node.shared.infrastructure.gossip.event.EventGossipClient
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.node.RestartService
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  GlobalSnapshotStateChannelAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.node.shared.modules.{SharedServices, SharedValidators}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.gossip.RumorRaw
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Json
import org.http4s.client.Client

/** Factory and wiring surface for the Global L0 consensus runtime.
  *
  * Wires together all components and starts the consensus background stream. Returns a Consensus instance with handler (for gossip),
  * manager (external API), storage (state queries), and routes (HTTP endpoints).
  *
  * GL0 consensus is Nakamoto/Taktikos/LDD with VRF slot leadership and an exact-hash Phase-2 gadget. It must not run the inherited
  * facility/proposal/vote/lock/QC/view-change BFT lifecycle. The generic [[Consensus]] return shape and dormant engine wiring are migration
  * plumbing, not global consensus authority; ML0 may continue using the separate shared BFT engine.
  *
  * @see
  *   ConsensusEventLoop for inherited plumbing that must remain outside the target GL0 lifecycle
  * @see
  *   NakamotoTriggerDaemon for VRF slot clock implementation
  */
object GlobalSnapshotConsensus {

  // GL0 is Nakamoto-only. No env var check needed — the run-nakamoto CLI command
  // is the single source of truth. Tunables come from NAKAMOTO_* env vars below.

  /** Contiguous-window retention depth (in ordinals) for the 3c-A served signed-bytes store (`mpt_snapshot_info_signed`), derived from the
    * recommended local k₂ capacity (`NakamotoConfig.keepDepthBehindFinalized` = 100·k₁).
    *
    * The store is written at every finalized ordinal; without a cutoff it grows unbounded. We retain a CONTIGUOUS recent window (not the
    * default logarithmic, which is gappy below the head) so the serve route's resolved ordinal — the latest combined checkpoint at-or-below
    * finalized, hence recent — is always present and the 3c-A fast path never 404s into the legacy GSI re-encode path.
    *
    * Raised from the old hardcoded `512` to the recommended k₂ capacity because mainnet k₁ = 1024 already exceeds 512. This is the disk
    * tier for authenticated replay, proof service, and automatic rollback; the in-memory `MptOverlay.undoJournalRef` may use a smaller
    * fast-path window. k₂ is not a protocol phase, finality threshold, or fork-choice floor. If objective valid-chain comparison needs
    * history that is unavailable locally, the target behavior is `RecoveryRequired`: stop production/mutation, fetch exact authenticated
    * history/state, and resume ordinary density comparison only after verified reconstruction. A legacy fallback path does not authorize
    * operator-selected realignment.
    *
    * Clamped to `Int.MaxValue` because `ContiguousOrdinalCutoff.make` takes an `Int` depth; k₂ = 100·k₁ fits comfortably for any realistic
    * k₁ (mainnet 102400).
    */
  def signedBytesRetentionDepth(keepDepthBehindFinalized: Long): Int =
    math.min(math.max(1L, keepDepthBehindFinalized), Int.MaxValue.toLong).toInt

  /** Genesis time of the Nakamoto chain — Unix epoch milliseconds at which slot 0 starts.
    *
    * This is a per-cluster constant: every node in the same Nakamoto cluster MUST agree on the same value, otherwise their slot clocks
    * drift and they will never produce overlapping VRF eligibility windows.
    *
    * Resolved once at process start, in this single place, and threaded as an already-resolved `Long` to every downstream component.
    * Override via `NAKAMOTO_GENESIS_TIME_MS` at launch time (the standard cluster-launch knob, set by the deploy tooling alongside the
    * genesis snapshot). If unset, falls back to `System.currentTimeMillis()` — only suitable for single-node dev launches; multi-node
    * clusters MUST set the env var.
    *
    * Future work: derive from the genesis snapshot itself so validators discover it from the chain instead of needing the env var. See task
    * #2 / NAKAMOTO-PLAN.md.
    */
  // Genesis time now lives in typed HOCON (`nakamoto.genesis-time-ms`, env override at the conf layer) and is
  // resolved ONCE inside `make` via Clock[F].realTime when unset (solo-dev fallback, WARN) — the prior module-level
  // `sys.env` + `System.currentTimeMillis()` val was both an idiom violation and an eager class-load clock read
  // (2026-06-12 idiom audit, items #2/#3).

  /** §3 NIPoPoW genesis eta — Blake2b-256 digest of a fixed domain string. 32 bytes; used by `EtaCalculation.bootstrapEta` for periods 0
    * and 1 and as the seed for later complete-range derivations. It is never a missing-history fallback for N >= 2. Must be identical
    * across all nodes in a cluster — derived from a constant rather than env var to avoid a config-drift class of bug (different operators
    * setting different `NAKAMOTO_GENESIS_ETA` values would silently fork the chain). Future work: derive from the genesis snapshot hash so
    * it's chain-bound instead of literal-bound.
    *
    * Path 1 (heap-leak workstream): hoisted to a top-level helper so the boundary-write `etaForPeriod` callback in `make` can construct an
    * `EtaStateManager` BEFORE the GSAM (and before the chain store is built); the previous in-place definition lived inside the inner
    * nakamotoBlock Resource and wasn't reachable at the GSAM construction site.
    */
  def nakamotoGenesisEta: Array[Byte] = {
    val genesisEtaSeed = "tessellation-nakamoto-genesis-eta-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val genesisEtaDigest = new org.bouncycastle.crypto.digests.Blake2bDigest(256)
    genesisEtaDigest.update(genesisEtaSeed, 0, genesisEtaSeed.length)
    val genesisEtaBytes = new Array[Byte](32)
    genesisEtaDigest.doFinal(genesisEtaBytes, 0)
    genesisEtaBytes
  }

  /** Path 1 (heap-leak workstream): hex-encode a 32-byte eta into the [[Hash]] shape the [[GlobalSnapshotAcceptanceManager]] boundary
    * writer stores in `HistoricalStakeSnapshot.eta`. Mirrors the inverse decode in
    * [[io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.make]] (`entry.eta.value.grouped(2)…`); the round-trip is
    * byte-identical so the MPT entries are deterministic across nodes.
    */
  def etaBytesToHash(bytes: Array[Byte]): io.constellationnetwork.security.hash.Hash =
    io.constellationnetwork.security.hash.Hash(bytes.map(b => f"$b%02x").mkString)

  def make[F[_]: Async: Parallel: Random: JsonSerializer: HasherSelector: SecurityProvider: Metrics, R <: CliMethod](
    sharedCfg: SharedConfig,
    gossip: Gossip[F],
    selfId: PeerId,
    keyPair: KeyPair,
    seedlist: Option[Set[SeedlistEntry]],
    collateral: Amount,
    clusterStorage: ClusterStorage[F],
    nodeStorage: NodeStorage[F],
    globalSnapshotStorage: SnapshotStorage[F, GlobalSnapshotArtifact, GlobalSnapshotContext],
    validators: SharedValidators[F],
    sharedServices: SharedServices[F, R],
    appConfig: AppConfig,
    stateChannelPullDelay: NonNegLong,
    stateChannelPurgeDelay: NonNegLong,
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    feeConfigs: SortedMap[SnapshotOrdinal, FeeCalculatorConfig],
    client: Client[F],
    session: Session[F],
    rewardsService: RewardsService[F],
    txHasher: Hasher[F],
    restartService: RestartService[F, R],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    mptStore: MptStore[F, GlobalStateKey],
    // #56.6: branch-aware overlay over `mptStore`. Plumbed straight through to SnapshotLeaderLoop
    // so exact-hash Phase-2 qualification (decided-attestation T_weight or k1 depth) can notify the
    // overlay when a branch becomes operational. Currently a passthrough impl by default — finalize is a
    // no-op until accept() migrates to the overlay (#56.10).
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // #56.10 Phase I (multi-tip in #115): setter on `SharedStorages` for the overlay's eviction
    // `bestTipsFn`. Invoked once the chain store has been constructed below so the overlay can walk
    // back through pending branches without dropping the canonical chain under eviction pressure
    // (#56.9). Returns the FULL set of fork tips, not just bestTip — protects canonical chain
    // ancestors during fork-recovery (#115) when the local-fork chain is bestTip.
    setBestTipsFn: F[Set[io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId]] => F[Unit],
    // #117/#118 Phase 2: setter on `SharedStorages` for `GlobalStateReader.pending`'s bestTipFn.
    // Distinct from `setBestTipsFn` (used by overlay eviction): this returns the single canonical
    // bestTip so HTTP reads via the `pending` reader resolve to the canonical chain head (not a
    // tentative fork head). Wired below once `chainStore` exists; the default in SharedStorages
    // reads `lastGlobalSnapshot.head.hash` which lags during fork recovery, hence the override.
    setBestTipFn: F[Option[io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId]] => F[Unit],
    // Track-3 S4: setter on `SharedStorages` for the DEEP revert-executor's disk reader. Wired below to
    // `signedBytesStore.readState` (the contiguous k₂ signed-bytes tier) so `MptOverlay.revertToOrdinal` can rebuild
    // the base from disk for a fork ordinal below the in-memory RAM undo-journal window. The default in SharedStorages
    // is "no deep tier" (a below-window revert fails closed); only gl0 retains the deep tier, so only gl0 overrides.
    setDeepStateReader: (SnapshotOrdinal => F[Option[Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]]]) => F[Unit],
    // #117/#118 Phase 2: branch-aware reader used by the snapshot binary fee calculator (and
    // any consensus-internal call site that needs read access at the chain's bestTip). Under
    // MultiBranch this picks up the chain's pending writes; the legacy `mptStore` path saw
    // base-only and could miscalculate fees during finality stalls (#117 root cause).
    pendingReader: io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader[F],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    eventGossipClient: EventGossipClient[F, GlobalSnapshotEvent],
    loggerBundle: LoggerBundle[F],
    rumorQueue: Queue[F, Hashed[RumorRaw]],
    // Transitional ordinal-only Phase-2 watermark updated after `chainStore.finalize`. The target
    // FinalityGate is keyed by exact `(ordinal, hash)` and must publish rollback/re-follow when a
    // density reorg replaces a P2 hash; monotone ordinal state alone cannot implement that contract.
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    // Transitional legacy-named k2 retention watermark. Live code still passes it to the store as a
    // revert floor; that is an implementation gap. Target k2 is local retention/proof/recovery
    // capacity only and must never decide validity, add a phase, or constrain objective fork choice.
    nakamotoSettledOrdinalRef: Ref[F, SnapshotOrdinal],
    // Observability seam for the chain-quality HTTP route (#138). The current view still exposes
    // legacy T_count/T_depth2 kinds. Target Phase 2 is only decided-attestation T_weight OR k1 depth;
    // no count rail or later phase exists. Empty until startup; the route handles `None` as 503.
    finalityTriggerViewRef: Ref[F, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[F]]],
    // Write-restricted view over the transitional legacy k2 watermark above. It is local telemetry
    // only in the target architecture; `T_depth2` and Phase 3 must be removed rather than promoted.
    settledOrdinalTracker: io.constellationnetwork.node.shared.domain.nakamoto.SettledOrdinalTracker[F],
    // NIPoPoW route seam. Production proof publication is deliberately dark: the Ref remains `None`
    // and both endpoints return 503 until proof construction is exact-target-bound, catch-up/rebuild
    // state is durable, and hard input/output/work bounds are ratified and enforced.
    nipopowProofProviderRef: Ref[F, Option[
      io.constellationnetwork.node.shared.domain.nakamoto.nipopow.NipopowProofProvider[F]
    ]],
    // Axis 2 (gl1 inclusion-proof follow) — observability seam for the GlobalFollowRoutes
    // `GET /global-follow/slice/latest` endpoint. Populated below:
    //   - `globalFollowSliceServiceRef` gets `GlobalFollowSliceService.make(latestFinalizedSliceSourceRef.get)`,
    //     where the Ref is captured by `SnapshotLeaderLoop` at each finalize sink (the `StoredSnapshot.context`
    //     of the snapshot it just finalized) — the latest-FINALIZED GSI, NOT `lastNGlobalSnapshotStorage.getCombined`
    //     (which holds the latest PRODUCED GSI, ahead of the finalized watermark and unresolvable by a
    //     finality-gated (#122) gl1 follower). The service projects the four consumed fields Address-keyed from
    //     that finalized GSI and carries the finalized ordinal itself, so the follower's verifier forward-hashes
    //     them to reproduce gl0's stateProof.<field>Proof roots.
    // Read by `GlobalFollowRoutes` (mounted in HttpApi); the route returns 503 while it is still in its
    // pre-wiring state. Empty until this resource has produced it.
    globalFollowSliceServiceRef: Ref[F, Option[
      io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService[F]
    ]],
    // Task #12 (ml0 adopt) — observability seam for the GlobalFollowRoutes
    // `GET /global-follow/changeset?since=<ord>` endpoint. Populated below with
    // `GlobalChangeSetService.make(recentFinalizedAccumulatorsRef.get)` — the same `recentFinalizedAccumulatorsRef`
    // (slice 2b) `SnapshotLeaderLoop` promotes finalized per-ordinal accumulators into at its finalize sinks. Read by
    // `GlobalFollowRoutes` (mounted in HttpApi); the route returns 503 while it is still in its pre-wiring state.
    // ADDITIVE / observability-only — a pure read of finalized state, never feeds back into consensus.
    globalChangeSetServiceRef: Ref[F, Option[
      io.constellationnetwork.node.shared.domain.nakamoto.GlobalChangeSetService[F]
    ]],
    // Slice 10/11 (cross-shard read transport) — backing seam for `ShardProofRoutes` (`POST /shard/{id}/proof`).
    // Populated below ONLY on the sharding-active path (`shardAcceptanceDeps = Some`), where the per-shard chain
    // stores (the prover's checkpoint lookup) + the global MPT proof service are in scope. At numShards=1 it stays
    // `None` and the route serves 503. Read by `ShardProofRoutes` (mounted in HttpApi). ADDITIVE / serve-only — a
    // pure read of MPT + finalized shard-checkpoint state, never feeds back into consensus.
    shardProofServiceRef: Ref[F, Option[
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofService[F]
    ]],
    // Invoked by NakamotoSyncDaemon when a metagraph-binary arrives via gossip.
    // Routes the binary through the same pipeline as the HTTP endpoint (stateChannelService.process).
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    // (#196) Invoked by NakamotoSyncDaemon when an AllowSpendBlock arrives via gossip.
    // Feeds the same `l1AllowSpendOutput` queue that the now-removed HTTP route fed; the
    // GlobalSnapshotEventsPublisherDaemon drains the queue into the event mempool unchanged.
    enqueueAllowSpendBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    // (#196 follow-up) Sinks for inbound DAGBlock + TokenLockBlock gossip — same
    // queues GlobalSnapshotEventsPublisherDaemon drains. The HTTP routes that
    // previously fed these queues (DAGBlockRoutes via Cell pipeline, TokenLockBlockRoutes
    // via direct offer) are removed; this is now the sole entry point.
    enqueueDAGBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.Block
    ] => F[Unit],
    enqueueTokenLockBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.tokenLock.TokenLockBlock
    ] => F[Unit],
    // Created in Services.make (hoisted so HTTP routes and stateChannelService can also publish).
    sidecarClient: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra[F],
    // The atomic genesis KES+VRF registry is owned by `SharedServices`; every consensus consumer resolves one pair from it. No standalone
    // KES or VRF registry crosses this boundary.
    // Split-safety (#261, eta axis): the deferred chain-walk handle the follower / `createContext` GSAM's
    // committee-eta resolver reads (created in `TessellationIOApp.make`, threaded here via `Services.make`).
    // Set ONCE below — right after the leader's own `chainStoreForLookupRef` — to the SAME
    // `chainStore.vrfOutputRangeForPeriodFrom`-backed exact walk the leader uses, so candidate replay calls
    // `EtaStateManager.getEtaAt(P, parentHash)` against the same branch. Ambient `getEta(P)` remains only for shard code whose wire schema
    // still lacks an exact GL0 hash/root and is tracked as an open protocol gap.
    // gl0 is the only layer that sets it; a missing range is incomplete and cannot become eta for N >= 2.
    setFollowerEtaChainWalk: ((Long, Option[io.constellationnetwork.security.hash.Hash]) => F[EtaSourceRange]) => F[Unit]
  )(
    implicit supervisor: Supervisor[F],
    globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit
  ): Resource[F, GlobalSnapshotConsensus[F]] =
    for {
      globalStateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager
        .make[F](stateChannelAllowanceLists, pullDelay = stateChannelPullDelay, purgeDelay = stateChannelPurgeDelay)
        .toResource

      feeCalculator = FeeCalculator.make(feeConfigs)

      // Wrap getGlobalSnapshotByOrdinal with a chainStore fallback for Nakamoto mode.
      // snapshotStorage loses ordinal index files during fork switches; chainStore has the
      // full canonical chain. The Ref breaks the ordering dependency (chainStore is created later).
      chainStoreForLookupRef <- cats.effect.kernel.Ref
        .of[F, Option[
          io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.NakamotoChainStoreAlgebra[F]
        ]](None)
        .toResource
      // Axis 2 (gl1 inclusion-proof follow) — the latest-FINALIZED `(ordinal, GlobalSnapshotInfo)` the slice producer serves.
      // Captured by `SnapshotLeaderLoop` at each finalize sink (the `StoredSnapshot.context` of the snapshot it just finalized
      // — that GSI is genuinely in-memory at the sink, see the note at the sink). The `GlobalFollowSliceService` reads THIS,
      // NOT `lastNGlobalSnapshotStorage.getCombined` (which holds the latest PRODUCED GSI, ahead of the finalized watermark).
      // gl1 is finality-gated (#122): it can only resolve a snapshot at the slice's ordinal if that ordinal is finalized, so
      // the slice MUST carry a finalized ordinal + its GSI. `None` until the first ordinal finalizes (cold start).
      latestFinalizedSliceSourceRef <- cats.effect.kernel.Ref
        .of[F, Option[(SnapshotOrdinal, GlobalSnapshotInfo)]](None)
        .toResource
      // ── gl1 follow-slice diff ring (Axis 2, #287 "send diffs") ────────────────────────────────────
      // Bounded ring of recent finalized 5-field PROJECTIONS keyed by ordinal (NOT full GSIs). `SnapshotLeaderLoop`
      // populates it at the SAME finalize sinks that update `latestFinalizedSliceSourceRef`, storing
      // `GlobalFollowSliceService.sliceFromGsi(finalizedGsi)` trimmed to the last
      // `GlobalFollowSliceService.recentProjectionsToKeep`. The slice service reads it to compute the incremental
      // diff a gl1 follower requests via `GET /global-follow/slice?since=<ordinal>` (a pure transport optimization —
      // a follower whose `since` fell out of the ring falls back to the full slice; field-root equality rejects a
      // wrong base, so the diff can never regress correctness).
      recentFollowProjectionsRef <- cats.effect.kernel.Ref
        .of[F, scala.collection.immutable.SortedMap[
          SnapshotOrdinal,
          io.constellationnetwork.schema.nakamoto.follow.ConsumedFieldDelta
        ]](scala.collection.immutable.SortedMap.empty)
        .toResource
      // ── ml0 changeset-adopt rings (Task #12 slice 2b) ─────────────────────────────────────────────
      // STAGING map: the gl0 producer (`GlobalSnapshotConsensusFunctions`) fills this hash-keyed when it
      // builds a snapshot artifact; `SnapshotLeaderLoop` reads it at the two finalize sinks to PROMOTE the
      // just-finalized snapshot's typed per-ordinal delta into the served ring below and then remove the hash.
      // BOUNDED by the producer to `GlobalSnapshotConsensusFunctions.pendingAccumulatorsToKeep` insertions so
      // fork candidates that never finalize cannot leak. The same instance is injected into BOTH the consensus
      // functions (producer/staging) and `SnapshotLeaderLoop` (promote/drain). `None` analogue elsewhere — only
      // gl0 produces a changeset ring.
      pendingAccumulatorsRef <- cats.effect.kernel.Ref
        .of[F, Map[
          io.constellationnetwork.security.hash.Hash,
          // VALUE = `(ordinal, accumulator)` — the ordinal rides along so the finalize-sink can FINALIZED-watermark-
          // prune (drop every staged entry at-or-below the finalized tip), replacing the arbitrary size `.drop`.
          (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)
        ]](Map.empty)
        .toResource
      // 3c-A enabler — STAGING map for the signed MPT byte map (mirrors `pendingAccumulatorsRef`). The producer stages
      // the EXACT signed `postBytes` here keyed by snapshot hash at the `overlay.commit` site; `SnapshotLeaderLoop`
      // promotes the FINALIZED hash's bytes into `signedBytesStore` below. Same instance injected into BOTH the
      // consensus functions (stage) AND `SnapshotLeaderLoop` (promote/drain).
      pendingPostBytesRef <- cats.effect.kernel.Ref
        .of[F, Map[
          io.constellationnetwork.security.hash.Hash,
          (SnapshotOrdinal, Map[io.constellationnetwork.security.hex.Hex, Array[Byte]])
        ]](Map.empty)
        .toResource
      // 3c-A enabler — the served signed-bytes store, a sibling of the producer's `mpt_snapshot_info`
      // (`<...>_signed`). ONLY the current Phase-2 sink writes it, so the byte map
      // served to followers reproduces the signed `mptRoot` BY CONSTRUCTION — never the producer's async finalize-time
      // re-fold (which can diverge under MultiBranch). The 3c-A serve route reads from this store.
      //
      // Retention: this store is written at every current Phase-2 ordinal (one file per ordinal) and was previously
      // left to grow UNBOUNDED. We instead bound it with a CONTIGUOUS recent window rather than the default
      // `LogarithmicOrdinalCutoff` (whose kept set has geometric GAPS below the head). The 3c-A serve route
      // (`FinalizedSnapshotReader.latestMptEntriesResponse`) resolves the latest combined checkpoint AT-OR-BELOW the
      // finalized ordinal (`CombinedSnapshotCheckpointFileSystemStorage.getLatestOrdinalAtOrBelow`) and reads that EXACT
      // ordinal's signed bytes — a logarithmic gap at that resolved ordinal yields a 404 and forces followers onto the
      // legacy (drift-prone) `syncFromGlobalSnapshotInfo` path. A contiguous window guarantees the resolved ordinal is
      // present so the fast path always fires.
      //
      // Capacity = recommended local k2 (`NakamotoConfig.keepDepthBehindFinalized` = 100*k1), raised from the stale
      // hardcoded 512. This disk tier supports authenticated replay, proof service, and automatic rollback while the
      // in-memory journal stays smaller. k2 is never a finality/fork-choice floor. If an objective comparison exceeds
      // retained history, the target enters RecoveryRequired and reconstructs exact authenticated state before resuming.
      // (`LogarithmicOrdinalCutoff` stays the default for all other `MptStateStorage` users.)
      signedBytesStore <- io.constellationnetwork.security.mpt.storages.MptStateStorage
        .make[F](
          fs2.io.file.Path(sharedCfg.mptSnapshotInfoPath.toString + "_signed"),
          io.constellationnetwork.cutoff.ContiguousOrdinalCutoff.make(
            GlobalSnapshotConsensus.signedBytesRetentionDepth(
              sharedCfg.nakamoto.keepDepthBehindFinalized(sharedCfg.environment).value
            )
          )
        )
        .toResource
      // Point the overlay's deep revert executor at this contiguous local-retention tier. When a density
      // reorg (S3) reverts to a fork ordinal below the in-memory RAM undo-journal window, `MptOverlay.revertToOrdinal`
      // reads the signed bytes here (`deleteAbove(fork)` + `loadBytes(readState(fork))`) to rebuild a byte-identical
      // base; a fork deeper than the retained window returns `None` → fail-closed `RevertGapError`.
      _ <- setDeepStateReader(signedBytesStore.readState).toResource
      // SERVED ring: bounded ordinal-keyed ring of recent FINALIZED per-ordinal accumulators (the ml0-side
      // analogue of `recentFollowProjectionsRef`). `SnapshotLeaderLoop` promotes into it at the SAME finalize
      // sinks, trimmed to the last `GlobalChangeSetService.recentAccumulatorsToKeep`. A later slice wires
      // `GlobalChangeSetService.make(recentFinalizedAccumulatorsRef.get)` to serve `changeSetSince`. Memory
      // bound, not a consensus parameter (a follower past the ring re-fetches via full-GSI adopt; the signed
      // `mptRoot` at each delta's ordinal rejects a wrong base). Empty until the first ordinal finalizes.
      recentFinalizedAccumulatorsRef <- cats.effect.kernel.Ref
        .of[F, scala.collection.immutable.SortedMap[
          SnapshotOrdinal,
          io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
        ]](scala.collection.immutable.SortedMap.empty)
        .toResource
      getGlobalSnapshotByOrdinalWithFallback: (SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]) = {
        (ordinal: SnapshotOrdinal) =>
          getGlobalSnapshotByOrdinal(ordinal).flatMap {
            case some @ Some(_) => Async[F].pure(some: Option[Hashed[GlobalIncrementalSnapshot]])
            case None           =>
              // snapshotStorage failed (stale cache or missing ordinal file from reorg race).
              // Fall back to chainStore's in-memory store — direct ordinal scan, no disk.
              chainStoreForLookupRef.get.flatMap {
                case Some(cs) =>
                  cs.getByOrdinal(ordinal.value.value).flatMap {
                    case Some(stored) =>
                      HasherSelector[F].withCurrent(implicit h => stored.signedSnapshot.toHashed[F].map(_.some))
                    case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                  }
                case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
              }
          }
      }

      // Path 1 (heap-leak workstream) — wire `etaForPeriod` for the GSAM boundary writer.
      //
      // The boundary write at ord `% R == R - 1` packs `HistoricalStakeSnapshot(stakes, eta)` into the MPT. Without a real
      // callback here it would write `Hash.empty` (the GSAM default), permanently degrading the §3 NIPoPoW N-2 historical
      // distribution read path and silently defeating pseudo-predictability cluster-wide. Findings 1 + 2 from the reviewer.
      //
      // Pattern:
      //   - The eta resolver is an [[EtaStateManager]] (MPT-cache point read first; chainStore-walk fallback on cache miss).
      //   - The chain-walk fallback routes to an exact `chainStore.vrfOutputRangeForPeriodFrom` for `period - 1`. Because the chain store
      //     is built *later* in the inner Resource block (after this GSAM is constructed), we route the walk through
      //     [[chainStoreForLookupRef]] (the same Ref that backs the `getGlobalSnapshotByOrdinalWithFallback` indirection
      //     above) — set once by the inner block, observed by every walk thereafter.
      //   - The [[HistoricalStakeReader]] is built against `GlobalStateReader.fromMptStore(mptStore)` because GSAM accept()
      //     runs against the underlying base store at boundary-write time; the per-call branch-aware reader is only used
      //     for prior-state reads from within accept(), not for boundary lookups (the boundary key is being WRITTEN this
      //     ordinal — the reader will miss either way, and chain-walk takes over).
      //
      // Note: pre-chainStore-setup boundary writes for periods 0/1 use the explicit bootstrap. Any N >= 2 request before an exact range or
      // rooted MPT eta exists fails closed.
      etaSourceRangeFor = (sourcePeriod: Long, parentHash: Option[io.constellationnetwork.security.hash.Hash]) =>
        chainStoreForLookupRef.get.flatMap {
          case Some(cs) =>
            parentHash.fold(cs.bestTip.map(_.map(_.hash)))(hash => Async[F].pure(hash.some)).flatMap {
              case Some(fromHash) =>
                cs
                  .vrfOutputRangeForPeriodFrom(
                    sourcePeriod,
                    sharedCfg.nakamoto.etaRotationSnapshots(sharedCfg.environment).value,
                    fromHash
                  )
                  .map[EtaSourceRange] {
                    case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.VrfOutputRange.Complete(
                          outputs
                        ) =>
                      EtaSourceRange.Complete(outputs)
                    case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.VrfOutputRange.Incomplete(
                          outputs,
                          _,
                          _
                        ) =>
                      EtaSourceRange.Incomplete(outputs)
                  }
              case None => Async[F].pure[EtaSourceRange](EtaSourceRange.Incomplete(Nil))
            }
          case None => Async[F].pure[EtaSourceRange](EtaSourceRange.Incomplete(Nil))
        }
      etaResolvers <- {
        val historicalStakeReaderForOuterGsam =
          io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader
            .make[F](io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](mptStore))
        io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager
          .make[F](
            genesisEta = nakamotoGenesisEta,
            historicalStakeReader = historicalStakeReaderForOuterGsam,
            chainWalkFallback = etaSourceRangeFor
          )
          .map { mgr =>
            val ambient = (period: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
              HasherSelector[F].withCurrent(implicit hasher => mgr.getEta(period.value).map(etaBytesToHash))
            val exact = (
              period: io.constellationnetwork.schema.nakamoto.EtaPeriod,
              parent: io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
            ) => HasherSelector[F].withCurrent(implicit hasher => mgr.getEtaAt(period.value, parent.value).map(etaBytesToHash))
            ambient -> exact
          }
          .toResource
      }
      etaForPeriodCallback = etaResolvers._1
      etaForPeriodAtParentCallback = etaResolvers._2

      // ─── LocalEventsService — gl0 reactive event stream (gated on HOCON enabled) ───
      // Constructed BEFORE GSAM so the publisher can be threaded in. When `nakamoto.local-events.enabled`
      // is false (the production default), publisher = noop and no gRPC server is bound. When true,
      // builds a `Topic`-backed publisher + an `io.grpc.Server` on `nakamoto.local-events.port`
      // (default 50054 — distinct from ChainSyncInbound's 50053).
      localEventsCfg = sharedCfg.nakamoto.localEvents
      // Epoch-progress Ref so the StreamStarted envelope can report the current epoch. Updated on
      // each finalize sink below if needed; for v1 we leave it at 0 and let clients read REST.
      localEventsEpochRef <- cats.effect.kernel.Ref.of[F, Long](0L).toResource
      localEventsService <- {
        if (!localEventsCfg.enabled)
          Resource.pure[F, Option[
            io.constellationnetwork.node.shared.infrastructure.local_events.LocalEventsService.Service[F]
          ]](None)
        else
          for {
            dispatcher <- cats.effect.std.Dispatcher.parallel[F]
            svc <- io.constellationnetwork.node.shared.infrastructure.local_events.LocalEventsService
              .make[F](
                finalizedOrdinalRef = nakamotoFinalizedOrdinalRef,
                epochProgressRef = localEventsEpochRef,
                maxQueuedPerSubscriber = localEventsCfg.maxQueuedPerSubscriber,
                publisherBufferSize = localEventsCfg.publisherBufferSize,
                dispatcher = dispatcher
              )
              .toResource
            _ <- io.constellationnetwork.node.shared.infrastructure.local_events.LocalEventsService
              .serverResource[F](
                svc,
                bindAddress = localEventsCfg.bindAddress,
                port = localEventsCfg.port.value,
                shutdownGraceSeconds = localEventsCfg.shutdownGraceSeconds.value,
                ec = scala.concurrent.ExecutionContext.global
              )
          } yield Some(svc)
      }
      localEventsPublisher = localEventsService
        .map(_.publisher)
        .getOrElse(io.constellationnetwork.node.shared.infrastructure.local_events.LocalEventsPublisher.noop[F])

      // Shared committee replay processor. The producer and every execution signer use the identical currency
      // derivation before signing; selected watchtowers replay as the noncommittee collusion backstop. Current
      // ordinary GL0 acceptance also invokes this processor, but that universal replay is transitional: target
      // noncommittee adoption verifies the replay certificate and positive watchtower coverage, applies the
      // namespace-confined canonical diff to the exact Phase-2 base, and recomputes the root.
      shardScEventsProcessor = GlobalSnapshotStateChannelEventsProcessor.make[F](
        validators.stateChannelValidator,
        globalStateChannelManager,
        sharedServices.currencySnapshotContextFns,
        feeCalculator,
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
      )

      // Task #44 — UNIFY shard acceptance deps to ONE instance per node. The stateful registry (per-shard chain
      // stores, tip trackers, finality triggers, binary buffers, committee cache, adopted watermarks) is built
      // ONCE in `SharedServices.make` and reused here instead of being constructed a SECOND time. Both sites took
      // byte-identical inputs (same genesis KES/VRF registries, same kDraw/kQuorum, same seedlist, same
      // MPT-committed eta resolver — the #261 split-safety contract), so the deterministic side is unchanged. What
      // was BROKEN was the duplicated STATE: follower adoptions (this node's SharedServices verify-GSAM) landed in
      // one registry while the leader-produce GSAM + shard producers + sync daemon read a DISJOINT registry — so
      // `deps.acceptanceManager.lastAdoptedOrd` never advanced for the producers, embeds stalled in eternal
      // awaiting-embed, and the #42 ANCHOR-REORG healer fired against a dead anchor Ref (0× in 17/18 runs). Sharing
      // the one instance makes adopt ↔ produce ↔ heal observe the same state. `None` at numShards <= 1 (regression
      // bar) — the GSAM below then passes `None` for all sharding params, byte-identical to the pre-wiring call.
      shardAcceptanceDeps = sharedServices.shardAcceptanceDeps

      // ─── Signed-byte-store read-time BACKFILL (2026-07-09): heals HOLES in the contiguous `signedBytesStore` at the pinned-read miss
      // seam. Creation-side staging races (fail-closed reorg adopts whose carried GSI can't reproduce the fork's signed root,
      // same-ordinal proposal-race losses where the winner's bytes were never staged under the finalized hash, catch-up jumps) leave
      // ordinals permanently missing; when a peer stamps such an ordinal as a shard checkpoint's `executionBaseOrdinal`, the adopt-verify
      // fail-closes on EVERY subsequent checkpoint and the metagraph mirror freezes (the 2mg/2shard token-lock e2e residual: ord 227
      // holed on gl0-0/gl0-1 ⇒ `pinned ANCHOR ... unreadable` ×106/×101). The backfill pulls the signed byte map for the EXACT missing
      // ordinal from up to `nakamoto.pinned-backfill-max-peers` peers via the by-ordinal `/global-snapshots/<ord>/mpt-entries` route
      // (session-less; integrity comes from the locally committed root gate, not the transport); the reader then
      // verifies `consensusMptRoot(fetched) === the LOCALLY-committed stateProof.mptRoot@ord`, strips to `consensusRootEntries`
      // (staged map = pure function of the committed root), and persists. Fetch failure / wrong root ⇒ the exact pre-existing
      // fail-closed defer. In-flight per-ordinal dedup bounds network amplification when many per-MG reads miss the same base.
      gl0PinnedBackfill <- io.constellationnetwork.node.shared.domain.nakamoto.overlay.PinnedCurrencyInfoReader.PinnedByteBackfill
        .deduplicated[F](
          io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon.pullMptEntriesAtOrdinalFromPeer[F](
            io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient
              .make[F](client, None, sharedCfg.snapshotTimeoutsConfig),
            clusterStorage,
            sharedCfg.nakamoto.pinnedBackfillMaxPeers.value,
            sharedCfg.nakamoto.pinnedBackfillPerPeerTimeout,
            org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("PinnedByteBackfill")
          )
        )
        .toResource
      // Shared pinned reader for producer and verifier re-execution over finalized global inputs.
      gl0PinnedReader = {
        implicit val h: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.PinnedCurrencyInfoReader
          .make[F](signedBytesStore, getGlobalSnapshotByOrdinalWithFallback, backfill = Some(gl0PinnedBackfill))
      }
      // Resolve the current Phase-2 state reader at a pinned ordinal through the version-retained, root-verified
      // signed-byte store. Ordinal-only lookup is transitional; the target checkpoint also binds the exact hash
      // and state root so a density reorg cannot make the reference ambiguous. `None` defers without signing.
      // NO live fast path: `lastPersistedOrdinal == ord` does NOT imply the live store's content is state@ord (Passthrough accept
      // writes land before the watermark bumps), and that skew minted quorum-attested checkpoints whose root no honest verifier could
      // reproduce — the 2026-07-08 shard-0 shardOrd=8 wedge (see `ShardCheckpointWiring.pinnedPriorReaderAt` scaladoc).
      // Track-1 execution-base-pin (FINDING-B1): the SINGLE shared recipe (`ShardCheckpointWiring.pinnedPriorReaderAt`) — the SharedServices
      // follower rails (checkpoint replay + createContext fraud-proof validator) resolve through the SAME definition (over their
      // logarithmic `mpt_snapshot_info` byte store), so all pinned-base re-executors read one pin semantics.
      finalizedReaderAt = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
        .pinnedPriorReaderAt[F](gl0PinnedReader)

      // ─── WATCHTOWER fraud-proof re-derivation closure (W3a) — hoisted ABOVE the GSAM so the on-chain verdict can use it ───
      // The PIN-1 per-MG re-derivation — IDENTICAL encoding to the unconditional replay the acceptance manager uses
      // (`reExecDerivationAtPinnedBase`), so the recomputed root is byte-comparable against the committee-attested
      // `perMetagraphMptRoots`. Track-1 execution-base-pin: it re-derives at the DISPUTED checkpoint's `executionBaseOrdinal` (the 4th closure arg,
      // resolved via `finalizedReaderAt` — pinned to the checkpoint's base, NOT this node's live base), so a watchtower whose base runs
      // ahead of the checkpoint's does NOT recompute a different root and false-slash an honest checkpoint.
      watchtowerReDerive = {
        implicit val h: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
        val reExec = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
          .reExecDerivationAtPinnedBase[F](shardScEventsProcessor, finalizedReaderAt, getGlobalSnapshotByOrdinalWithFallback)(
            Async[F],
            Parallel[F],
            h,
            implicitly[io.constellationnetwork.json.JsonSerializer[F]],
            globalStateProofSelector
          )
        (
          mg: io.constellationnetwork.schema.address.Address,
          binaries: cats.data.NonEmptyList[
            io.constellationnetwork.security.signature.Signed[io.constellationnetwork.statechannel.StateChannelSnapshotBinary]
          ],
          anchor: io.constellationnetwork.schema.SnapshotOrdinal,
          executionBaseOrdinal: io.constellationnetwork.schema.SnapshotOrdinal
        ) => reExec(mg, binaries, anchor, executionBaseOrdinal).map(_.getOrElse(io.constellationnetwork.security.hash.Hash.empty))
      }

      // ─── WATCHTOWER on-chain dispute verdict for the GSAM accept path (W3a) ──────────────────────────────
      // The SAME deterministic validator the daemon uses, but reading the DURABLE `Slashings` MPT partition for the double-slash guard (so a
      // checkpoint already slashed in a prior ordinal yields `AlreadySlashed` ⇒ NOT upheld ⇒ no double slash). Built with the PIN-1
      // `watchtowerReDerive` closure (hoisted above) so the recomputed root matches the committee-attested `perMetagraphMptRoots`
      // byte-for-byte. `None` at `numShards = 1` (`shardAcceptanceDeps = None`) ⇒ carried fraud proofs (always empty there) are ignored ⇒
      // byte-identical regression bar. Passed into BOTH this gl0 GSAM (leader-produce + `validateArtifact`) below; the SharedServices
      // `createContext` GSAM gets its own via the same recipe (so all three mptRoot-computing paths slash identically).
      gsamInvalidStateProofValidator = shardAcceptanceDeps match {
        case Some(deps) =>
          implicit val h: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
          Some(
            io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator.make[F](
              reDerivePerMgRoot = watchtowerReDerive,
              slashedReader =
                io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader.fromMptStore[F](mptStore),
              verifyExecutionCertificate = deps.acceptanceManager.verifyExecutionCertificate
            )
          ): Option[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]]
        case None =>
          Option.empty[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]]
      }

      anchoredConsensusKeyClaims <- GlobalSnapshotAcceptanceManager
        .anchoredConsensusKeyClaimsFromRegistry(sharedServices.operatorKeyRegistry)
        .toResource

      snapshotAcceptanceManager <- GlobalSnapshotAcceptanceManager
        .make[F](
          sharedCfg.fieldsAddedOrdinals,
          sharedCfg.metagraphsSync,
          sharedCfg.environment,
          BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
          AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
          TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
          shardScEventsProcessor,
          sharedServices.updateNodeParametersAcceptanceManager,
          sharedServices.updateDelegatedStakeAcceptanceManager,
          sharedServices.updateNodeCollateralAcceptanceManager,
          validators.spendActionValidator,
          validators.pricingUpdateValidator,
          sharedServices.priceStateUpdater,
          collateral,
          sharedCfg.delegatedStaking.withdrawalTimeLimit
            .getOrElse(sharedCfg.environment, EpochProgress.MinValue),
          mptOverlay,
          loggerBundle,
          etaRotationSnapshots = sharedCfg.nakamoto.etaRotationSnapshots(sharedCfg.environment).value,
          etaForPeriod = Some(etaForPeriodAtParentCallback),
          localEventsPublisher = Some(localEventsPublisher),
          // Hierarchical-shard-checkpoints v1 acceptance-side deps. `None` at `numShards = 1` (regression bar);
          // `Some(...)` activates the shard-checkpoint admission path inside `accept()`. Task #44: this is the
          // SAME `shardAcceptanceDeps` instance the SharedServices verify-GSAM uses (`sharedServices.shardAcceptanceDeps`),
          // so the leader-produce and verify paths now share ONE acceptance manager + registry, not two mirrored ones.
          shardingConfig = shardAcceptanceDeps.map(_.shardingConfig),
          shardCheckpointAcceptanceManager = shardAcceptanceDeps.map(_.acceptanceManager),
          shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
          // Historical commitment activation remains dark until exact-hash branch recovery, durable boot replay, and recipient
          // reproduction are complete. Honest GL0 artifacts therefore carry `smtRoot = None`.
          historicalCommitmentSmt = None,
          // WATCHTOWER slashing config — threaded from the single HOCON source (same as the SharedServices GSAM) so both GSAM
          // sites apply the operator-configured, cluster-uniform slash/bounty fractions (now required — no source-level default).
          invaliditySlashingConfig = sharedCfg.nakamoto.invaliditySlashing,
          // WATCHTOWER on-chain dispute verdict (W3a): re-validate carried fraud proofs + surface the bounty slash. SAME instance the
          // leader-produce and `validateArtifact` paths share (this single GSAM). `None` at numShards=1.
          invalidStateProofValidator = gsamInvalidStateProofValidator,
          kesRegistrationAcceptanceManagerForHasher = Some { registrationHasher =>
            implicit val h: io.constellationnetwork.security.Hasher[F] = registrationHasher
            io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertAcceptanceManager.make[F](
              io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.make[F](
                validators.signedValidator,
                seedlist
              )
            )
          },
          anchoredConsensusKeyClaims = anchoredConsensusKeyClaims
        )
        .toResource

      consensusStorage <- ConsensusStorage
        .make[
          F,
          GlobalSnapshotEvent,
          GlobalSnapshotKey,
          GlobalSnapshotArtifact,
          GlobalSnapshotContext,
          GlobalSnapshotStatus,
          GlobalConsensusOutcome,
          GlobalConsensusKind
        ](appConfig.snapshot.consensus)
        .toResource

      // WATCHTOWER fraud-proof POOL (W3a) — the single shared node-local staging area. The daemon's `handleFraudProof` OFFERS
      // locally-UPHELD disputes into it; the gl0 leader producer PEEKS it to embed the `fraudProofs` consensus field. `noop` at
      // numShards=1 (no shard deps) ⇒ always-empty ⇒ no fraud proofs embedded ⇒ byte-identical regression bar.
      fraudProofPool <- shardAcceptanceDeps match {
        case Some(_) =>
          io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool.make[F]().toResource
        case None =>
          Async[F]
            .pure(io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool.noop[F])
            .toResource
      }

      consensusFunctions =
        GlobalSnapshotConsensusFunctions.make[F](
          snapshotAcceptanceManager,
          collateral,
          rewardsService,
          GlobalSnapshotEventCutter.make(
            appConfig.snapshot.consensus.eventCutter.maxBinarySizeBytes,
            SnapshotBinaryFeeCalculator.make(appConfig.shared.feeConfigs, pendingReader)
          ),
          UpdateNodeParametersCutter.make(appConfig.snapshot.consensus.eventCutter.maxUpdateNodeParametersSize),
          appConfig.environment,
          DefaultDelegatedRewardsConfigProvider,
          sharedCfg.fieldsAddedOrdinals.tessellation3Migration
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          sharedCfg.fieldsAddedOrdinals.setSumFix
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          sharedCfg.incrementalDelegatedStakingStartingOrdinal
            .getOrElse(sharedCfg.environment, SnapshotOrdinal.MinValue),
          mptStore,
          mptOverlay,
          // Gap C — feed finalized shard checkpoints into accept(). `None` at numShards=1 (regression
          // bar) ⇒ accept() receives `shardCheckpoints = SortedMap.empty`. Same `shardAcceptanceDeps`
          // instance the GSAM acceptance side, the Gap-A producers, and the Gap-B receiver use.
          shardAcceptanceDeps = shardAcceptanceDeps,
          // Task #12 slice 2b — the hash-keyed STAGING map. The producer stages each built snapshot's typed
          // per-ordinal delta here; `SnapshotLeaderLoop` (same Ref, injected below) promotes the finalized
          // ones into `recentFinalizedAccumulatorsRef`. Additive — never feeds back into consensus.
          pendingAccumulatorsRef = pendingAccumulatorsRef,
          // 3c-A enabler — the signed-bytes STAGING map (same Ref injected into `SnapshotLeaderLoop` below). Producer
          // stages the EXACT signed `postBytes` keyed by snapshot hash at the `overlay.commit` site.
          pendingPostBytesRef = pendingPostBytesRef,
          // Task #19 — staging-map backstop cap (typed HOCON, default 2048 = 2× the served ring), bumped from
          // the prior hardcoded 512 so a burst of never-finalizing forks between two finalize ticks cannot evict
          // a higher-ordinal staged entry about to finalize (which would force ml0 into a full-GSI resync).
          stagingAccumulatorsCap = sharedCfg.nakamoto.stagingAccumulatorsCap.value,
          // WATCHTOWER fraud-proof POOL (W3a) — the leader-produce path peeks this to embed the `fraudProofs` consensus field. SAME instance
          // the daemon offers into below. `noop` at numShards=1 ⇒ byte-identical regression bar.
          fraudProofPool = fraudProofPool
        )

      stateAdvancer =
        GlobalSnapshotConsensusStateAdvancer.make(
          appConfig.snapshot.consensus,
          keyPair,
          consensusStorage,
          globalSnapshotStorage,
          consensusFunctions,
          gossip,
          restartService,
          nodeStorage,
          appConfig.shared.leavingDelay,
          lastNGlobalSnapshotStorage,
          lastGlobalSnapshotStorage,
          getGlobalSnapshotByOrdinalWithFallback,
          clusterStorage,
          eventMempool,
          eventGossipClient,
          loggerBundle,
          mptStore
        )

      facilitatorSelector = FacilitatorSelector.make(
        appConfig.snapshot.consensus.maxFacilitatorCount.map(_.value)
      )

      peerQualityTracker <- PeerQualityTracker.make[F].toResource

      tcaFilter = TrailingCommonAncestorFilter.make[F]

      // In Nakamoto mode, create state ref for VRF trigger daemon
      resolvedGenesisTimeMs <- {
        val configured = sharedCfg.nakamoto.genesisTimeMs.value
        if (configured > 0L) Async[F].pure(configured)
        else
          cats.effect.Clock[F].realTime.map(_.toMillis).flatTap { fallback =>
            org.typelevel.log4cats.slf4j.Slf4jLogger
              .getLoggerFromName[F]("NakamotoConsensus")
              .warn(
                s"nakamoto.genesis-time-ms UNSET — falling back to local wall clock ($fallback). " +
                  "Solo-dev only: multi-node clusters MUST configure an identical genesis time or slot indices diverge."
              )
          }
      }.toResource
      nakamotoStateRef <- {
        val genesisEta = sharedCfg.nakamoto.genesisEtaSeed.getBytes
        Ref.of[F, NakamotoTriggerState](NakamotoTriggerState.initial(resolvedGenesisTimeMs, genesisEta)).map(Some(_))
      }.toResource

      stateCreator =
        GlobalSnapshotConsensusStateCreator.make(
          consensusFunctions,
          consensusStorage,
          gossip,
          selfId,
          seedlist,
          facilitatorSelector,
          appConfig.snapshot.consensus.deterministicConfigHash,
          peerQualityTracker,
          tcaFilter,
          eventMempool,
          nakamotoStateRef,
          appConfig.snapshot.consensus.candidateAdmissionEnabled
        )

      stateRemover =
        GlobalSnapshotConsensusStateRemover.make(
          consensusStorage,
          gossip
        )

      consensusOps = GlobalSnapshotConsensusOps.make

      stateUpdater =
        ConsensusStateUpdater.make(
          stateAdvancer,
          consensusStorage,
          consensusOps
        )

      consensusClient = ConsensusClient.make[F, GlobalSnapshotKey, GlobalConsensusOutcome](client, session)

      directPushFn = ConsensusDirectSender.makeDirectPushFn(clusterStorage, consensusClient)
      _ <- gossip.setDirectPushFn(directPushFn).toResource

      loop <-
        ConsensusEventLoop
          .build[
            F,
            GlobalSnapshotEvent,
            GlobalSnapshotKey,
            GlobalSnapshotArtifact,
            GlobalSnapshotContext,
            GlobalSnapshotStatus,
            GlobalConsensusOutcome,
            GlobalConsensusKind
          ](
            selfId,
            consensusStorage,
            stateCreator,
            stateUpdater,
            stateAdvancer,
            stateRemover,
            consensusOps,
            nodeStorage,
            clusterStorage,
            consensusFunctions,
            consensusClient,
            appConfig.snapshot.consensus,
            facilitatorSelector,
            peerQualityTracker,
            nakamotoMode = true
          )
          .toResource

      handler = GlobalConsensusHandler.make(loop.queue)

      routes = new ConsensusRoutes[
        F,
        GlobalSnapshotKey,
        GlobalSnapshotArtifact,
        GlobalSnapshotContext,
        GlobalSnapshotStatus,
        GlobalConsensusOutcome,
        GlobalConsensusKind
      ](consensusStorage, rumorQueue)

      // Nakamoto GL0: no BFT consensus trigger or loop — slot clock handles production
      triggerEvent = Async[F].unit

      // Nakamoto LDD + VRF config. `baseline`/`amplitude` arrive from HOCON as exact `Ratio` (parsed from
      // `"n/d"` strings — never Double), so the threshold computation is exact and reproducible across all
      // JVMs/CPUs by construction. No fromDoubles round-trip, no source-level default.
      lddConfig = io.constellationnetwork.schema.nakamoto.LddConfig(
        lddCutoff = sharedCfg.nakamoto.ldd.cutoff,
        offset = sharedCfg.nakamoto.ldd.offset,
        baselineDifficulty = sharedCfg.nakamoto.ldd.baseline,
        amplitude = sharedCfg.nakamoto.ldd.amplitude
      )
      // R = eta-rotation period, now DERIVED in `NakamotoConfig` as `round(3.03·k₁)` from the single
      // `nakamoto.confirmation-depth-k` knob (Ouroboros: first-2/3 nonce + last-1/3 ≥ k₁ stability; .03 margin).
      // Rotation is keyed on **ordinal**, not slot — slots are LDD-paced and lumpy; ordinals are 1:1 with
      // snapshots and give a stable R that satisfies the R ≥ 3·k₁ bound. See `docs/nakamoto/attestation-and-finality.md` §1.
      etaRotationSnapshots = sharedCfg.nakamoto.etaRotationSnapshots(sharedCfg.environment).value

      // Start the Nakamoto SnapshotLeaderLoop + sidecar bridge.
      //
      // This block runs in the outer Resource context so the long-lived resources it creates
      // (Dispatcher for the ChainSync gRPC server and the gRPC server itself) are released
      // when the app Resource tree tears down, instead of being
      // escaped via `.allocated` and leaked across test restarts / shutdown.
      _ <- {

        val pureGenesisTimeMs = resolvedGenesisTimeMs
        val nakamotoDataDir = java.nio.file.Paths.get(sys.env.getOrElse("TESSELLATION_DATA_DIR", "/tessellation/data"))
        for {
          nakLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("NakamotoConsensus").pure[F].toResource
          _ <- nakLogger
            .info(
              s"🔧 Nakamoto config: LDD(cutoff=${lddConfig.lddCutoff}, offset=${lddConfig.offset}, baseline=${lddConfig.baselineDifficulty}, amplitude=${lddConfig.amplitude}), etaRotation=${etaRotationSnapshots} snapshots, genesisTime=${pureGenesisTimeMs}"
            )
            .toResource
          // §1.1: switch from equal-weight to stake-weighted VRF election.
          // §G1 (GSI → MPT migration): the per-node stake aggregate now reads from the global-state
          // MPT via prefix-scan instead of iterating `GlobalSnapshotInfo.activeDelegatedStakes` /
          // `activeNodeCollaterals` in-memory maps. The MPT is the canonical store under the
          // GSI-to-MPT migration; reading from it directly drops a redundant in-memory mirror and
          // closes a class of byte-determinism gaps between independent node MPT builds.
          //
          // Hot-path caching: `NodeStakeAggregator.cached` memoizes the result keyed on
          // `SnapshotOrdinal` and invalidates whenever the parent ordinal advances. Without this
          // the per-slot VRF eligibility (~200 calls/sec at 8gl0+4mg+4shards) would re-run two MPT
          // prefix-scans per call. See `NodeStakeAggregator.cached` for the trade-off discussion.
          //
          // Reader: `pendingReader` is the `GlobalStateReader.pending` resolved to chain bestTip
          // under MultiBranch — matches "view of stake at the parent the consensus is voting on".
          // Falls back to base when bestTipFn returns None (pre-bootstrap window).
          //
          // §3 NIPoPoW S0.3 / §G3: historical stake distributions are written by GSAM.accept() at every
          // eta-period boundary ordinal into both the GSI map (`historicalStakeSnapshots`) and the MPT
          // partition (`GlobalStateFieldId.HistoricalStakeSnapshots`, point key derived via
          // `historicalStakeSnapshotsKey[F](period)`). The callback below is the MPT-primary reader —
          // `HistoricalStakeReader` does a single point read against the `pendingReader` branch view,
          // matching the live-stake-aggregate's view of the chain's parent branch. Warmup (pre-genesis
          // / pre-boundary) returns None and the registry's fall-through path uses the live MPT
          // aggregate.
          stakeAggregator <- io.constellationnetwork.node.shared.domain.nakamoto.NodeStakeAggregator
            .cached[F](
              io.constellationnetwork.node.shared.domain.nakamoto.NodeStakeAggregator.make[F](pendingReader),
              lastGlobalSnapshotStorage.getOrdinal
            )
            .toResource
          // §G3 — historical-stake-snapshot reader migrated from GSI iteration to MPT point read.
          // The GSAM boundary writer (`AcceptanceMptStateChanges.applyStateChanges`) lands one MPT
          // entry per stored period keyed by `historicalStakeSnapshotsKey[F](period)`; this reader
          // mirrors that key derivation against the same `pendingReader` the G1 stake aggregator uses,
          // so both the live aggregate and the N-2 historical lookback observe the chain's parent-
          // branch view under MultiBranch (consistent with the leader's commit view).
          historicalStakeReader = io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader
            .make[F](pendingReader)
          stakeRegistry <- {
            implicit val stakeHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
            io.constellationnetwork.node.shared.domain.nakamoto.StakeRegistry
              .stakeWeightedMpt[F](
                stakeAggregator,
                // Path 1 (heap-leak workstream): the partition value is now `HistoricalStakeSnapshot`
                // (stakes + eta). For `relativeStakeAt`'s N-2 lookback we project to the stake half.
                (period: io.constellationnetwork.schema.nakamoto.EtaPeriod) => historicalStakeReader.lookup(period).map(_.map(_.stakes))
              )
              .toResource
          }
          // Filter out entries marked with alias="metagraph-op". They live in the seedlist
          // so state-channel binary signature validation accepts them as known signers,
          // but they must not count as Nakamoto validators (would dilute 1/N VRF stake).
          validatorPeers = seedlist
            .map(_.collect { case e if !e.alias.exists(_.value.value == "metagraph-op") => e.peerId })
            .getOrElse(Set(selfId))
          _ <- stakeRegistry.updateValidators(validatorPeers).toResource
          tipTracker <- io.constellationnetwork.node.shared.domain.nakamoto.TipTracker
            .make[F](stakeRegistry, snowballBeta = sharedCfg.nakamoto.snowballBeta.value)
            .toResource
          // Slice S3: committee sortition + per-binary attestation aggregator. The sortition is a
          // stateless function; the aggregator holds the per-`(metagraph, parent, binary)` tally
          // until `pruneParents` is called from the finality hook. The committee DRAW target (kDraw)
          // and ADMIT quorum (kQuorum) are the cluster-uniform `nakamoto.committee` HOCON params
          // (kDraw=N ⇒ committee=everyone; admit at kQuorum). The gate itself is constructed below
          // once the operationalKeyMaker + atomic operator-key registry are in scope.
          // CommitteeSortition uses `Hasher[F]` to encode the canonical VRF input. The Selector's
          // current hasher matches the message-side encoding the rest of the consensus surface
          // uses; sortition messages aren't ordinal-bound (they key on parent hash) so picking
          // the current hasher rather than `getForOrdinal` is correct.
          committeeSortition = io.constellationnetwork.node.shared.domain.nakamoto.CommitteeSortition
            .make[F](implicitly[Async[F]], HasherSelector[F].getCurrent)
          committeeAggregator <- io.constellationnetwork.node.shared.domain.nakamoto.MetagraphAttestationAggregator
            .make[F]
            .toResource
          // Consensus signing material is provisioned out-of-band and must already match the local operator's atomic genesis KES+VRF
          // registration. Missing directories/files and either key mismatch abort Resource construction. There is deliberately no
          // in-memory/random fallback and no startup-time ad-hoc registration: a key becomes eligible only through canonical registration.
          kesSecureStoreDir <- Async[F]
            .fromEither(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.LocalOperatorKeyPairGate
                .requireSecureStoreDirectory(sys.env.get("CL_KES_SECURE_STORE_DIR"))
            )
            .toResource
          kesSecureStore <- io.constellationnetwork.security.kes.SecureStore.disk[F](kesSecureStoreDir).toResource
          localOperatorKeyMaterial <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.LocalOperatorKeyPairGate
            .loadVerified[F](
              secureStore = kesSecureStore,
              keyName = "kes-sk.bin",
              etaPeriodLength = etaRotationSnapshots.toLong,
              selfId = selfId,
              longTermKeyPair = keyPair,
              operatorKeyRegistry = sharedServices.operatorKeyRegistry
            )
          (operationalKeyMaker, verifiedLocalOperatorKeys) = localOperatorKeyMaterial
          _ <- nakLogger
            .info(
              s"Local GL0 KES+VRF material matches the preregistered operator pair for ${selfId.value.value.take(16)}... " +
                s"(KES period offset=${verifiedLocalOperatorKeys.kesPeriodOffset})"
            )
            .toResource

          // Numerics for VRF eligibility threshold. Bifrost prod precision: log1p=8, exp=38, maxIter=10000.
          // We use prec=8 / prec=38 to match Bifrost exactly. The Lentz iteration runs in exact `Ratio`
          // arithmetic so two honest nodes (different JVMs/CPUs/JIT-tiers) compute byte-identical
          // thresholds — closes the IEEE 754 nondeterminism risk.
          log1p <- io.constellationnetwork.numerics.interpreters.Log1pInterpreter.make[F](maxIterations = 10000, precision = 8).toResource
          exp <- io.constellationnetwork.numerics.interpreters.ExpInterpreter.make[F](maxIterations = 10000, precision = 38).toResource
          eligibilityChecker = io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker.make[F](log1p, exp)
          // ChainSelection needs fetchParent — but chainStore needs ChainSelection.
          // Break the cycle: create chainStore first with a lazy fetchParent that
          // uses chainStore.tipFor once it's available.
          chainStoreRef <- cats.effect.kernel.Ref
            .of[F, Option[
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.NakamotoChainStoreAlgebra[F]
            ]](None)
            .toResource
          fetchParent = (tip: io.constellationnetwork.schema.nakamoto.ChainTip) =>
            chainStoreRef.get.flatMap {
              case Some(cs) => cs.tipFor(tip.parentHash)
              case None     => cats.Applicative[F].pure(None: Option[io.constellationnetwork.schema.nakamoto.ChainTip])
            }
          // TRANSITIONAL IMPLEMENTATION GAP: live wiring still offers k1/k2 ordinal revert floors.
          // Target maxvalid-tk/maxvalid-bg remains objective across P2 density reorgs; k2 only sizes local
          // history/proof/recovery capacity and never freezes fork choice. The reader/flag below must be
          // removed or reduced to retention diagnostics when the hash-bound FinalityGate lands.
          chainSelection = io.constellationnetwork.node.shared.domain.nakamoto.ChainSelection.make[F](
            tipTracker,
            fetchParent,
            kLookback = sharedCfg.nakamoto.kLookback(sharedCfg.environment),
            sWindow = sharedCfg.nakamoto.sWindow(sharedCfg.environment),
            // Local retained-history bound for the current true-MRCA search. Exhaustion must become
            // RecoveryRequired plus authenticated reconstruction, never an operator-selected winner.
            maxAncestorDepth = sharedCfg.nakamoto.keepDepthBehindFinalized(sharedCfg.environment).value,
            // Legacy floor input retained by current executable code; forbidden by the target protocol.
            settledOrdinalReader = Some(nakamotoSettledOrdinalRef.get.map(_.value.value)),
            bandDensityReorgEnabled = sharedCfg.nakamoto.bandDensityReorgEnabled
          )
          // Local retention bound. Bounds in-memory canonical-chain retention to a sliding window; older
          // lookups fall through to disk-backed `SnapshotStorage` via
          // `NakamotoChainStore.getWithOrdinalFallback`. Path 1 of the workstream: with the disk
          // fallback wired through the exact VRF-output range API, this is intended as capacity rather than a
          // correctness gate. Missing objective comparison history must enter RecoveryRequired.
          // Migrated from `sys.env.get("NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED")` to HOCON; the
          // application.conf entry still honors `${?NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED}` so ops
          // scripts keep working.
          keepDepthBehindFinalized = sharedCfg.nakamoto.keepDepthBehindFinalized(sharedCfg.environment).value
          chainStore <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore
            .make[F](
              globalSnapshotStorage,
              chainSelection,
              nakamotoFinalizedOrdinalRef,
              // Transitional legacy k2 watermark. Current store consumption as a floor is a known target
              // violation; k2 may size retention/recovery only.
              nakamotoSettledOrdinalRef,
              keepDepthBehindFinalized,
              // Transitional flag selecting a legacy ordinal floor in live code. Target removes both floors
              // and keeps density comparison active for reversible exact-hash P2 state.
              bandDensityReorgEnabled = sharedCfg.nakamoto.bandDensityReorgEnabled
            )
            .toResource
          _ <- chainStoreRef.set(Some(chainStore)).toResource
          _ <- chainStoreForLookupRef.set(Some(chainStore)).toResource
          // Split-safety (#261, eta axis): install the SAME chain-walk the leader's committee-eta resolver uses
          // (`etaForPeriodCallback`'s `chainWalkFallback` at the top of this `make`) into the follower /
          // `createContext` GSAM's `EtaStateManager` via the deferred Ref. Both walks read the identical
          // `chainStoreForLookupRef` and call `cs.vrfOutputRangeForPeriodFrom(sourcePeriod, etaRotationSnapshots, tipHash)`, so the
          // follower's exact-parent `getEtaAt(P, parentHash)` byte-equals the leader's for EVERY period P ≥ 2 — closing the
          // `C_real ≠ C_genesis` committee split on the gl0 Download / RollbackLoader rebuild paths. Set AFTER
          // `chainStoreForLookupRef` so the very first follower walk already observes the chain store.
          _ <- setFollowerEtaChainWalk(etaSourceRangeFor).toResource
          // #56.10 Phase I (multi-tip in #115): wire the overlay's eviction `bestTipsFn` to
          // `chainStore.allTips` so ancestor protection covers EVERY viable chain head — the
          // canonical bestTip AND any tentative-branch heads being followed during fork-recovery.
          // Without this, when validator commits canonical-N before chain reorgs to it, canonical-N
          // is committed but isn't yet bestTip; subsequent eviction (with lastCommittedRef pointing
          // elsewhere) drops canonical-N's branch, breaking the parent walk for canonical-N+1.
          // Using `allTips` (every leaf in `byHash`) instead of just `bestTip` keeps the canonical
          // chain alive through fork-recovery's commit-before-reorg window. The overlay captured a
          // Ref-backed closure at SharedStorages.make time; setting the ref here completes the
          // binding.
          _ <- setBestTipsFn(
            chainStore.allTips.map(
              _.map(io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(_))
            )
          ).toResource
          // #117/#118 Phase 2: install the chain's canonical bestTip as the source for
          // `GlobalStateReader.pending`'s `bestTipBranchF`. Under MultiBranch the chain's
          // pending writes live under this branch; the reader walks pending → falls through
          // to base, so HTTP reads see the canonical view (not the lagging base). When
          // `chainStore.bestTip` is empty (pre-genesis-seed window), the reader falls back
          // to `BranchId.base` which resolves to the underlying `MptStore`.
          _ <- setBestTipFn(
            chainStore.bestTip.map(
              _.map(stored => io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(stored.hash))
            )
          ).toResource
          // #56.10 Phase H: startup base-consistency guard. Runs in the boot Resource chain
          // (after SharedStorages.make's mptStore and the chainStore just constructed above)
          // and before any request-serving — this resource block is awaited before the HTTP
          // servers start in Main.scala. A no-op for fresh nodes; fires only when crash
          // recovery left MPT base ahead of finalized.
          _ <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.MptBaseConsistency
            .assertBaseConsistentOrPrune[F](mptStore, chainStore.lastFinalizedOrdinal)(implicitly[Async[F]], nakLogger)
            .toResource
          // Seed chain store with the current head snapshot so gossip children can find their parent
          _ <- globalSnapshotStorage.head.flatMap {
            case Some((headSigned, headCtx)) =>
              HasherSelector[F].withCurrent { implicit hasher =>
                headSigned.toHashed[F].flatMap { hashed =>
                  nakLogger.info(
                    s"🌱 Seeding chain store with genesis: ordinal=${hashed.ordinal} hash=${hashed.hash.value.take(16)} " +
                      s"lastSnapshotHash=${hashed.lastSnapshotHash.value.take(16)}"
                  ) >>
                    chainStore
                      .store(
                        headSigned,
                        headCtx,
                        hashed.ordinal.value.value,
                        0L, // slot unknown for genesis
                        hashed.lastSnapshotHash,
                        Array.empty // no VRF output for genesis
                      )
                      .flatMap {
                        case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.StoreOutcome
                              .BecameSelected(_, _) =>
                          Async[F].unit
                        case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.StoreOutcome
                              .Duplicate(_, _) =>
                          nakLogger.debug("Chain store recovery head was already present")
                        case other =>
                          Async[F].raiseError[Unit](
                            new IllegalStateException(s"Chain store rejected the recovery head: outcome=$other")
                          )
                      }
                }
              }
            case None =>
              Async[F].unit
          }.toResource
          lastKnownSlotRef <- cats.effect.kernel.Ref.of[F, Option[Long]](None).toResource
          // Shared epoch state: VRF outputs from ALL sources accumulate here for eta rotation.
          // Path 1 (heap-leak workstream): genesis eta lifted to the top-level helper
          // `GlobalSnapshotConsensus.nakamotoGenesisEta` so the outer GSAM construction (above) and
          // the inner epoch state (here) read the same 32-byte value — byte-identical to the GSAM
          // boundary writer's eta and to the receiver-side `EtaStateManager` cache fallback.
          genesisEta = nakamotoGenesisEta
          epochStateRef <- cats.effect.kernel.Ref
            .of[F, io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState](
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SharedEpochState.initial(genesisEta)
            )
            .toResource
          // Shared semaphore: serialize snapshot production and gossip processing
          // so each operation sees correct parent state (Bifrost uses same pattern)
          snapshotSemaphore <- cats.effect.std.Semaphore[F](1).toResource
          productionGate <- io.constellationnetwork.node.shared.domain.nakamoto.ProductionGate.make[F].toResource
          // sidecarClient is now created in Services.make and passed in as a parameter so that
          // the HTTP state-channel route can publish metagraph binaries via the same gRPC channel.
          // Wire rumor gossip onto the sidecar transport. Outbound: every rumor passing through
          // Gossip.spread is forwarded to the libp2p sidecar via PublishRumor. Inbound: rumors
          // received from the GossipSub mesh are deserialized back to Hashed[RumorRaw] and offered
          // to rumorQueue, where the existing GossipDaemon.consumeRumors pipeline validates and
          // dispatches them via the registered RumorHandlers — meaning BFT consensus messages,
          // Tessellation events, and any other rumor type ride sidecar transport for free.
          _ <- gossip
            .setSidecarPublishFn(
              io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarRumorBridge
                .publishFn[F](sidecarClient)
            )
            .toResource
          _ <- io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarRumorBridge
            .receive[F](sidecarClient.channel, rumorQueue)
            .toResource

          // Slice S3: construct the committee gate. Sender path signs the per-metagraph attestation with
          // the operator's long-term Ed25519 key + KES product key (Slice 9 path) and gossips via the
          // sidecar; receiver path verifies all three sigs then records into the aggregator.
          //
          // Draw/quorum decouple (cluster-uniform HOCON `nakamoto.committee`, NOT sys.env — project rule):
          //   - kDraw sizes the committee DRAW (threshold = min(kDraw·σ, 1)); kDraw = N saturates ⇒
          //     committee = everyone, so P(|committee| ≥ kQuorum) = 1.
          //   - kQuorum is the admit count the gate waits for. The invariant 0 < kQuorum <= kDraw is
          //     validated fail-fast at config load (`CommitteeConfig.validated`).
          // Both are byte-identical across nodes so the draw agrees sender↔receiver and every node
          // admits at the same threshold. 8-node testnet default: kDraw = 8, kQuorum = 6.
          committeeKDraw = sharedCfg.nakamoto.committee.kDraw
          committeeKQuorum = sharedCfg.nakamoto.committee.kQuorum
          // Frozen-genesis compatibility: the local VRF secret is deterministically derived from the long-term key and the committee gate
          // refuses every proof/signature side effect unless its VK equals the same operator's rooted atomic genesis KES+VRF pair. Runtime
          // rotation remains disabled until a secure independently provisioned VRF secret is resolved with the exact candidate-parent
          // HistoricalOperatorConsensusKeyRegistry record; it must never reuse this genesis derivation as implicit authority.
          committeeVrfKeys = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop.deriveVrfKeys(keyPair)
          committeeGateLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger
            .getLoggerFromName[F]("MetagraphCommitteeGate")
            .pure[F]
            .toResource
          // All local validity-signature adapters share this period-authoritative gate. The artifact's
          // global eta period and the preregistered offset derive the only permissible KES tree step;
          // mutable `OperationalKeyMaker.currentPeriod` is checked for monotonicity, never selected as
          // authority. No bytes means no signature side effect may be published or counted.
          signKesAtRegisteredPeriod =
            (
              operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
              artifactPeriod: io.constellationnetwork.schema.nakamoto.EtaPeriod,
              message: Array[Byte]
            ) =>
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.LocalOperatorKeyPairGate
                .signingKeyFromResolved(
                  operationalKeyMaker,
                  selfId,
                  keyPair,
                  operatorKeys,
                  artifactPeriod.value
                )
                .flatMap {
                  case Left(error) =>
                    nakLogger
                      .warn(
                        s"Refusing local KES signature at artifactPeriod=${artifactPeriod.value}: ${error.getMessage}"
                      )
                      .as(Option.empty[(Int, Array[Byte])])
                  case Right(signingKey) =>
                    operationalKeyMaker.signAt(signingKey.treeStep, message).flatMap {
                      case Left(error) =>
                        nakLogger
                          .warn(
                            s"Refusing local KES signature at artifactPeriod=${artifactPeriod.value} treeStep=${signingKey.treeStep}: ${error.message}"
                          )
                          .as(Option.empty[(Int, Array[Byte])])
                      case Right(signature) =>
                        Async[F].pure[Option[(Int, Array[Byte])]](
                          Some(
                            signingKey.treeStep -> io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(signature)
                          )
                        )
                    }
                }
          committeeKesSigner = new io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSigner[F] {
            def sign(
              operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
              artifactPeriod: io.constellationnetwork.schema.nakamoto.EtaPeriod,
              message: Array[Byte]
            ): F[Option[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSignature]] =
              signKesAtRegisteredPeriod(operatorKeys, artifactPeriod, message).map(
                _.map {
                  case (treeStep, bytes) =>
                    io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSignature(treeStep, bytes)
                }
              )
          }
          // KES verifier — the artifact period plus preregistered offset determine the only valid
          // tree-internal step. The wire `sender_tree_step` is rejected unless it equals that result;
          // it is never authority. Admission is fail-closed on a missing/inactive registration,
          // mismatch, empty/decode-failed signature, or cryptographic verification failure.
          committeeKesVerifier = new io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesVerifier[F] {
            def verify(
              messageBytes: Array[Byte],
              kesSigBytes: Array[Byte],
              expectedOperatorId: io.constellationnetwork.schema.peer.PeerId,
              operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
              kesStep: Int,
              artifactPeriod: io.constellationnetwork.schema.nakamoto.EtaPeriod
            ): F[Boolean] =
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.KesGossipVerification
                .verifyAttestationByStep[F](
                  messageBytes = messageBytes,
                  kesSigBytes = kesSigBytes,
                  expectedOperatorId = expectedOperatorId,
                  operatorKeys = operatorKeys,
                  kesStep = kesStep,
                  artifactPeriod = artifactPeriod,
                  logger = committeeGateLogger
                )
          }
          // Publisher — wraps `sidecarClient.publishMetagraphAttestation`; failures are logged and
          // swallowed (gossip best-effort). Receivers re-emit if the publish lost in the libp2p mesh,
          // matching the existing fire-and-forget pattern for `publishAttestation`. `kesStep` is
          // forwarded into the proto field `sender_tree_step` so receivers verify against exactly
          // the tree-internal step the sender used.
          committeePublisher = new io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.Publisher[F] {
            def publish(
              senderPeerIdBytes: Array[Byte],
              metagraphAddress: String,
              parentHashBytes: Array[Byte],
              binaryHashBytes: Array[Byte],
              committeeVrfProof: Array[Byte],
              longTermSignature: Array[Byte],
              kesSignature: Array[Byte],
              vrfPublicKey: Array[Byte],
              kesStep: Int
            ): F[Unit] = {
              val msg = io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
                .mkMetagraphAttestation(
                  peerIdBytes = senderPeerIdBytes,
                  metagraphAddress = metagraphAddress,
                  parentHash = parentHashBytes,
                  binaryHash = binaryHashBytes,
                  committeeVrfProof = committeeVrfProof,
                  signature = longTermSignature,
                  kesSignature = kesSignature,
                  vrfPublicKey = vrfPublicKey,
                  senderTreeStep = kesStep
                )
              sidecarClient.publishMetagraphAttestation(msg).void.handleErrorWith { e =>
                committeeGateLogger.warn(s"Failed to publish metagraph attestation for $metagraphAddress: ${e.getMessage}")
              }
            }
          }
          // The signed currency payload commits an ML0 continuity ordinal and an independent exact GL0
          // globalSyncView. Only the latter may select eta, active operator keys, or KES period.
          committeeCurrencyContextForBinary: (
            (
              io.constellationnetwork.schema.address.Address,
              io.constellationnetwork.security.hash.Hash,
              Array[Byte]
            ) => F[
              Option[
                io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.CurrencyBinaryContext
              ]
            ]
          ) = (mg, parent, content) => {
            implicit val resolverLogger: org.typelevel.log4cats.Logger[F] = committeeGateLogger
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver
              .resolveCurrencyContextFromBinary[F](pendingReader, mg, parent, content)
          }
          committeeCurrencyContextFromContent = (content: Array[Byte]) =>
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver
              .currencyContextFromContent[F](content)
          // Transitional exact Phase-2 check. The target FinalityGate will mint this hash-bound
          // capability directly; the current ordinal watermark alone is insufficient.
          committeeIsExactPhase2 = (
            anchorOrdinal: io.constellationnetwork.schema.SnapshotOrdinal,
            anchorHash: io.constellationnetwork.security.hash.Hash
          ) =>
            (nakamotoFinalizedOrdinalRef.get, chainStore.bestTip).tupled.flatMap {
              case (phase2Ordinal, Some(tip)) if anchorOrdinal <= phase2Ordinal =>
                chainStore.walkBackTo(tip.hash, anchorOrdinal.value.value).map(_.contains(anchorHash))
              case _ => Async[F].pure(false)
            }
          committeeVerifyPhase2CurrencyContext = (
            context: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.CurrencyBinaryContext
          ) =>
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext
              .verify[F](context)(committeeIsExactPhase2)
          committeeGate = {
            implicit val gateLogger: org.typelevel.log4cats.Logger[F] = committeeGateLogger
            implicit val gateHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.make[F](
              selfPeerId = selfId,
              selfVrfSk = committeeVrfKeys._1,
              selfVrfVk = committeeVrfKeys._2,
              keyPair = keyPair,
              sortition = committeeSortition,
              // Admission runs independently of execution sharding. Use the genesis-loaded
              // registry retained by SharedServices even when `numShards = 1`.
              operatorKeyRegistry = sharedServices.operatorKeyRegistry,
              aggregator = committeeAggregator,
              kesSigner = committeeKesSigner,
              kesVerifier = committeeKesVerifier,
              publisher = committeePublisher,
              kDraw = committeeKDraw,
              kQuorum = committeeKQuorum,
              gateTimeoutMs = io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.DefaultGateTimeoutMs,
              pollIntervalMs = io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.DefaultPollIntervalMs
            )
          }
          // Resolve eta from the exact Phase-2 GL0 anchor's own ancestry, never from ML0 cadence or
          // whichever best tip happens to be local when the callback runs.
          committeeEtaForPhase2Anchor = (
            context: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext
          ) => {
            val currentPeriod = io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
              .rotationPeriod(context.gl0AnchorOrdinal.value.value, etaRotationSnapshots.toLong)
            // Cardano/Praos bootstrap: periods 0 and 1 genesis-derivable via bootstrapEta. The committee
            // draw eta MUST match the producer/validator eta exactly — same convention, same sites.
            if (currentPeriod <= 1)
              epochStateRef.get.map(g =>
                io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation.bootstrapEta(g.genesisEta, currentPeriod)
              )
            else
              epochStateRef.get.map(_.genesisEta).flatMap { genesis =>
                chainStore
                  .vrfOutputRangeForPeriodFrom(
                    currentPeriod - 1,
                    etaRotationSnapshots.toLong,
                    context.gl0AnchorHash
                  )
                  .flatMap {
                    case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.VrfOutputRange.Complete(
                          chainOutputs
                        ) if chainOutputs.nonEmpty =>
                      io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
                        .computeEta(genesis, currentPeriod, chainOutputs.map(_._2))
                        .pure[F]
                    case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.VrfOutputRange.Complete(_) =>
                      EtaSourceUnavailable(currentPeriod, currentPeriod - 1L, "complete range contained no VRF outputs")
                        .raiseError[F, Array[Byte]]
                    case io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore.VrfOutputRange.Incomplete(
                          chainOutputs,
                          _,
                          _
                        ) =>
                      EtaSourceUnavailable(
                        currentPeriod,
                        currentPeriod - 1L,
                        s"Phase-2 anchor ancestry incomplete after ${chainOutputs.size} outputs"
                      ).raiseError[F, Array[Byte]]
                  }
              }
          }

          _ <- nakLogger
            .info(
              s"🏛️ Committee gate constructed: kDraw=$committeeKDraw kQuorum=$committeeKQuorum, validatorPeers=${validatorPeers.size} " +
                s"(kDraw=N ⇒ committee=everyone, threshold saturates; admit at kQuorum)"
            )
            .toResource
          // #214: orphan buffer + admission cache. Hoisted from `NakamotoSyncDaemon.run` so the
          // SnapshotLeaderLoop.onFinalize drain hook below and the daemon's gossip handler share
          // the same in-memory instance. The orphan-drain on finalize unsticks chains where the
          // local admission gate did not complete but a later exact ML0/global reference makes the
          // binary's child eligible for another ordinary admission attempt.
          orphanBufferLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger
            .fromName[F]("MetagraphOrphanBuffer")
            .toResource
          orphanBuffer <- io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer
            .make[F](
              orphanBufferLogger,
              cap = sharedCfg.nakamoto.orphanBufferCap.value,
              admissionsCap = sharedCfg.nakamoto.recentAdmitCap.value
            )
            .toResource
          // Gate-aware closure for processing a `(metagraphAddress, wireBytes)` pair. Built once
          // here so both call sites (daemon gossip handler and finalize-drain hook) share the
          // same orphan buffer + admission cache. See `NakamotoSyncDaemon.makeMetagraphBinaryProcessor`.
          processOrphanedMetagraphBinary = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon
            .makeMetagraphBinaryProcessor[F](
              processMetagraphBinary = processMetagraphBinary,
              committeeGate = committeeGate,
              currencyContextFor = committeeCurrencyContextForBinary,
              currencyContextFromContent = committeeCurrencyContextFromContent,
              verifyPhase2CurrencyContext = committeeVerifyPhase2CurrencyContext,
              etaForPhase2Anchor = committeeEtaForPhase2Anchor,
              etaRotationSnapshots = etaRotationSnapshots.toLong,
              selfStake = stakeRegistry.committeeStake(selfId),
              // #29 verify-on-attach: same per-sender stake lookup the inbound-attestation receiver uses (see the
              // `senderStakeLookup` wiring below), so attestations buffered while a binary was an orphan verify
              // identically when it attaches in the admit path.
              senderStakeLookup = (peer: io.constellationnetwork.schema.peer.PeerId) => stakeRegistry.committeeStake(peer),
              orphanBuffer = orphanBuffer,
              shardBinaryBuffers = shardAcceptanceDeps
                .map(_.registry.map { case (sid, entry) => sid -> entry.binaryBuffer })
                .getOrElse(Map.empty),
              shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
              logger = orphanBufferLogger
            )
          // Keep proof publication unavailable. The staged tower finalizer/catch-up implementation is
          // intentionally not attached to the finality monitor: its current exact walk scans the full
          // tip-to-cursor interval per bounded processing chunk, has no durable clean-rebuild protocol,
          // and its builder is not bound to an immutable requested target.
          _ <- nipopowProofProviderRef.set(None).toResource

          // Axis 2 (gl1 inclusion-proof follow) — publish the gl0-side slice producer so
          // `GlobalFollowRoutes` (mounted in HttpApi) can serve `GET /global-follow/slice/latest`.
          // ADDITIVE / observability-only: the route reads this Ref and never feeds back into consensus —
          // exactly like the NipopowRoutes seam above.
          //   - The slice service reads `latestFinalizedSliceSourceRef` — the latest-FINALIZED
          //     `(ordinal, GlobalSnapshotInfo)` captured by `SnapshotLeaderLoop` at the finalize sink (the
          //     `StoredSnapshot.context` of the snapshot it just finalized). This is NOT
          //     `lastNGlobalSnapshotStorage.getCombined`, which holds the latest PRODUCED GSI — ahead of the
          //     finalized watermark and therefore UNRESOLVABLE by a finality-gated (#122) gl1 follower. The
          //     service projects the four consumed fields Address-keyed from the finalized GSI (`gsi.balances`
          //     / `gsi.lastTxRefs` / `gsi.lastAllowSpendRefs` / `gsi.lastTokenLockRefs`) and carries the
          //     finalized ordinal itself, so the follower can resolve the snapshot at that ordinal for the
          //     trusted roots and forward-hash each `(Address, value)` to reproduce gl0's
          //     stateProof.<field>Proof on recompute-and-match. No MPT store/overlay read here — the
          //     byte-identity is reproduced on the verify side, not extracted from the store.
          _ <- {
            val sliceService = io.constellationnetwork.node.shared.domain.nakamoto.GlobalFollowSliceService
              .make[F](latestFinalizedSliceSourceRef.get, recentFollowProjectionsRef.get)
            globalFollowSliceServiceRef.set(Some(sliceService))
          }.toResource

          // Task #12 (ml0 adopt) — publish the gl0-side changeset producer so `GlobalFollowRoutes` (mounted in
          // HttpApi) can serve `GET /global-follow/changeset?since=<ord>`. ADDITIVE / observability-only, exactly
          // like the slice service above: the changeset service reads the bounded `recentFinalizedAccumulatorsRef`
          // ring (slice 2b) that `SnapshotLeaderLoop` promotes finalized per-ordinal accumulators into at its
          // finalize sinks, and serves the contiguous deltas a full-state ml0 follower adopts to reach the latest
          // finalized ordinal. Never feeds back into consensus.
          _ <- {
            // Historical commitment activation is dark. Do not serve proofs from a node-local, restart-empty store while signed GL0
            // artifacts are required to carry `smtRoot = None`.
            val changeSetService = io.constellationnetwork.node.shared.domain.nakamoto.GlobalChangeSetService
              .make[F](
                recentFinalizedAccumulatorsRef.get,
                None,
                sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value
              )
            globalChangeSetServiceRef.set(Some(changeSetService))
          }.toResource

          // ─── Slice 10/11 — cross-shard subtree proof SERVE side (`POST /shard/{id}/proof`) ────
          // Construct the `ShardSubtreeProofService` and publish it on `shardProofServiceRef` so `ShardProofRoutes`
          // (mounted in HttpApi) can answer cross-shard proof queries. ONLY on the sharding-active path
          // (`shardAcceptanceDeps = Some`); at numShards=1 the Ref stays `None` and the route serves 503.
          //
          // Wiring:
          //   - subtree reconstruction = `perMgEntriesFor` below, read from the current finalized global MPT view;
          //     this is not `HistoricalMptProofService` and does not claim an exact historical generation.
          //   - shard-ownership oracle = `deps.shardAssignment` (the cluster-wide static map).
          //   - checkpoint lookup = `ShardChainStore.bestTip` per shard, lifted to its `Signed[ShardCheckpoint]`.
          //   - the proof service emits only when the reconstructed subtree root equals the checkpoint's committed
          //     per-MG root. Consumers still verify the proof against the exact GL0-anchored checkpoint they trust.
          // ADDITIVE / serve-only — a pure read of MPT + finalized shard-checkpoint state, never feeds back into
          // consensus.
          _ <- shardAcceptanceDeps match {
            case None => Async[F].unit.toResource
            case Some(deps) =>
              implicit val shardHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
              val lookupShardCheckpoint
                : io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofService.ShardCheckpointLookup[F] =
                (sid: io.constellationnetwork.schema.sharding.ShardId) =>
                  deps.registry.get(sid) match {
                    case None =>
                      Async[F].pure(
                        none[io.constellationnetwork.security.signature.Signed[io.constellationnetwork.schema.sharding.ShardCheckpoint]]
                      )
                    case Some(entry) => entry.chainStore.bestTip.map(_.map(_.signed))
                  }
              // PIN-1: the proof is built over the per-MG SUB-TRIE (component-addressable root), so the service needs a `perMgEntriesFor`
              // that reconstructs `GlobalStateConverter.currencySnapshotMgEntries` from gl0's FINALIZED state for the MG. We reuse the
              // EXACT byte-production path the follower (`MptStoreReadOps.getAllLastCurrencySnapshots`) + producer
              // (`ShardCheckpointWiring`) commit through, so the rebuilt sub-trie's `rootHash === perMetagraphMptRoots(mg)` by
              // construction (the generate path's CONSISTENCY GUARD fails SAFE → 404 on any drift, never emits a wrong proof):
              //   - reader = `GlobalStateReader.fromMptStore[F](mptStore)` — the SAME FINALIZED base reader the produce/watchtower
              //     re-exec sites use at ~1614/1720. The serve anchor is gl0's finalized base (matches the §8.3 one-snapshot
              //     read-after-write staleness bound; the §8.5 service comment above).
              //   - per-MG `data` entry mirrors `getAllLastCurrencySnapshots` scoped to ONE MG (disjoint Left/Right partitions per
              //     address): the genesis `LastCurrencySnapshots` (Left) partition first, else the `(Signed[CurrencyIncrementalSnapshot]
              //     (fieldId-5), reconstructed CurrencySnapshotInfo)` (Right) pair. `getCurrencySnapshotInfo` reconstructs the info from
              //     the unrolled `Mg*` partitions gated on the fieldId-5 incremental — the SAME inverse of `infoEntryBytes` the committed
              //     root commits to.
              //   - `None` when the MG has no reconstructible finalized currency state (never-seen / pre-genesis) ⇒ legitimate 404.
              // Routed through `GlobalStateConverter.currencySnapshotMgEntries` (gl0's exact field-32-filtered producer bytes) with the
              // SAME `globalStateProofSelector` the other PIN-1 sites pass. `shardHasher` (in scope above) drives all hashing; behind the
              // `numShards > 1` (`Some(deps)`) gate, so numShards=1 stays byte-identical (the Ref stays `None`, route serves 503).
              val perMgEntriesFor
                : io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofService.PerMgEntriesLookup[F] =
                (mg: io.constellationnetwork.schema.address.Address) => {
                  import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps.GlobalStateReaderTypedOps
                  val finalizedReader =
                    io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](mptStore)
                  finalizedReader
                    .getLastCurrencySnapshot(mg)
                    .flatMap {
                      case Some(genesis) =>
                        (Left(genesis): Either[
                          io.constellationnetwork.security.signature.Signed[
                            io.constellationnetwork.currency.schema.currency.CurrencySnapshot
                          ],
                          (
                            io.constellationnetwork.security.signature.Signed[
                              io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
                            ],
                            io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
                          )
                        ]).some.pure[F]
                      case None =>
                        (finalizedReader.getLastIncrementalCurrencySnapshot(mg), finalizedReader.getCurrencySnapshotInfo(mg)).tupled.map {
                          case (Some(inc), Some(info)) =>
                            (Right((inc, info)): Either[
                              io.constellationnetwork.security.signature.Signed[
                                io.constellationnetwork.currency.schema.currency.CurrencySnapshot
                              ],
                              (
                                io.constellationnetwork.security.signature.Signed[
                                  io.constellationnetwork.currency.schema.currency.CurrencyIncrementalSnapshot
                                ],
                                io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
                              )
                            ]).some
                          case _ => None
                        }
                    }
                    .flatMap {
                      case None => Async[F].pure(Option.empty[Map[io.constellationnetwork.security.hex.Hex, Array[Byte]]])
                      case Some(stateEither) =>
                        io.constellationnetwork.schema.mpt.GlobalStateConverter
                          .currencySnapshotMgEntries[F](SortedMap(mg -> stateEither))(
                            Async[F],
                            Parallel[F],
                            shardHasher,
                            implicitly[io.constellationnetwork.json.JsonSerializer[F]],
                            globalStateProofSelector
                          )
                          .map(_.some)
                    }
                }
              val proofService = io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSubtreeProofService.make[F](
                shardAssignment = deps.shardAssignment,
                lookupShardCheckpoint = lookupShardCheckpoint,
                perMgEntriesFor = perMgEntriesFor
              )
              (shardProofServiceRef.set(Some(proofService)) >>
                nakLogger.info(
                  "sharding ACTIVE: ShardSubtreeProofService published (serve route POST /shard/{id}/proof live; " +
                    "perMgEntriesFor LIVE — reconstructs per-MG currency state off the FINALIZED GlobalStateReader via " +
                    "currencySnapshotMgEntries; proofs emit when the rebuilt sub-trie root === the committee-attested " +
                    "perMetagraphMptRoots(mg), else 404 fail-safe)"
                )).toResource
          }

          // ─── Gap A — per-shard checkpoint producers ──────────────────────────────────────────
          // Build one `ShardCheckpointProducer` per shard the operator tracks, REUSING the SAME
          // per-shard `ShardChainStore` the acceptance side (`shardAcceptanceDeps.registry`) reads —
          // producer writes ⇒ consumer reads ⇒ the shard chain grows toward finality. `None` deps
          // (numShards <= 1, the production default) ⇒ empty map ⇒ the `SnapshotLeaderLoop` fan-out is
          // inert (regression bar). Every allocation below sits behind the `Some(deps)` gate.
          //
          // Registered VRF seed: the same `deriveVrfKeys(keyPair)._1` the GL0 leader loop uses. In the shard path it supplies a
          // key-possession proof; public execution membership and staircase duty are computed separately. KES mirrors `committeeKesSigner`
          // (signAt → `OperationalKeyMaker.encodeSignature`).
          //
          // Slice S4 — shardEta ROTATES per eta-period: the producer takes `shardEtaFor: EtaPeriod => F[Array[Byte]]`
          // and resolves it per `produce` call keyed on the checkpoint's own `epoch`, which the fan-out derives from shard best-tip ordinal.
          // The per-period gl0 eta comes from `etaForPeriodCallback` (the SAME `EtaStateManager.getEta` resolver GSAM's
          // boundary writer uses); we convert the returned `Hash` to the 32 raw digest bytes (`Hex(h.value).toBytes` —
          // the byte shape `computeShardEta` requires, mirroring `ShardSlotLeader.computeShardEta`) and feed
          // `computeShardEta(shardId, gl0Eta)`. After the first GL0 eta rotation the shard possession-proof domain rotates.
          // Determinism: `eta_epoch` is fixed at the 2/3-mark
          // of the prior period (`EtaCalculation`), so it is knowable at produce + verify time and every node keying the
          // lookup on the wire-carried `checkpoint.epoch` derives byte-identical bytes.
          shardProducers <- shardAcceptanceDeps match {
            case None =>
              Async[F]
                .pure(
                  Map.empty[
                    io.constellationnetwork.schema.sharding.ShardId,
                    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[
                      F
                    ]
                  ]
                )
                .toResource
            case Some(deps) =>
              implicit val shardHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
              implicit val shardLogger: org.typelevel.log4cats.Logger[F] = nakLogger
              val shardSlotLeader =
                io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSlotLeader.make[F](eligibilityChecker)
              val shardKesSigner = new io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSigner[F] {
                def sign(
                  operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
                  checkpointEpoch: io.constellationnetwork.schema.nakamoto.EtaPeriod,
                  message: Array[Byte]
                ): F[
                  Option[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSignature]
                ] =
                  signKesAtRegisteredPeriod(operatorKeys, checkpointEpoch, message).map(
                    _.map {
                      case (treeStep, bytes) =>
                        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSignature(treeStep, bytes)
                    }
                  )
              }
              val shardVrfKeys = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                .deriveVrfKeys(keyPair)
              // §5.7 (owner-corrected 2026-06-11): the lottery clock is the WALL-CLOCK slot grid — the producer
              // receives `currentSlot` per fan-out tick and stamps it on the envelope (signed); no anchor→slot
              // mapping exists anymore (the old `slotForGl0Anchor` identity map downsampled the lottery to
              // gl0-snapshot cadence — the run-10 Gap-A inversion).
              // Slice S4: per-period rotated gl0 eta → 32 raw digest bytes. `etaForPeriodCallback` is the SAME
              // `EtaStateManager.getEta` resolver the GSAM boundary writer uses; it returns a hex `Hash`, which we decode
              // to the 32-byte shape `computeShardEta` requires (mirrors `ShardSlotLeader.computeShardEta`'s own
              // `Hex(h.value).toBytes`). Keyed on the checkpoint's `epoch`, this is byte-identical across all nodes.
              val gl0EtaBytesForPeriod: io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Array[Byte]] =
                (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                  etaForPeriodCallback(epoch).map(h => io.constellationnetwork.security.hex.Hex(h.value).toBytes)
              deps.registry.toList.traverse {
                case (shardId, entry) =>
                  // Per-shard closure: resolve the rotated gl0 eta for the checkpoint's epoch, then domain-separate to
                  // this shard's registered-key proof eta. Computed per `produce` call (keyed on `checkpoint.epoch`), not once at
                  // construction — so the shard VRF domain rotates in lockstep with gl0.
                  val shardEtaFor: io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Array[Byte]] =
                    (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                      gl0EtaBytesForPeriod(epoch).flatMap(gl0Eta => shardSlotLeader.computeShardEta(shardId, gl0Eta))
                  io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer
                    .make[F](
                      shardId = shardId,
                      chainStore = entry.chainStore,
                      // S2 — BASE-ANCHORED window (VERSION-MODEL §4): the producer's `chainLinkOrder` anchors each MG's binary window on the
                      // gl0 DEPTH-K-FINALIZED base's per-MG `lastStateChannelSnapshotHashes` — the SAME finalized base `derivePerMgState`
                      // reads. So window-anchor == execution prior on every verifier, all on
                      // the finalized base; the window RE-INCLUDES base->adopted binaries and advances on GL0 finalization
                      // (NOT the bestTip-derived `chainStore.perMgTip`, which ran ahead of base — the run-24..27 §4 window-anchor violation).
                      finalizedBasePerMgTip = {
                        import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax.MptStoreReadOps
                        mptStore.getAllLastStateChannelSnapshotHashes
                      },
                      // NEWNESS GATE (S2-deadlock fix, runs 19-22 + ord-26 re-freeze): the gate reference is this chain's CHAIN-WIDE
                      // per-MG checkpoint frontier (the latest binary minted per MG across the noteAnchor-followed bestTip ancestry), NOT
                      // the latest-PRODUCED global GSI. `getCombined` LAGS the in-flight per-MG adoptions, so it sits BEHIND gl0's true
                      // adopt tip and let a stale re-include slip the gate → ord-26 re-froze (verified live). The chain-wide frontier is
                      // reorg-safe and at-or-AHEAD of gl0's adopt tip (gl0 only adopts what this chain minted), so requiring the next
                      // window to extend past it guarantees gl0's embed-match can continue. Exactly one checkpoint may be outstanding;
                      // advancing a child before the exact containing GL0 snapshot reaches P2 causes a permanent await-embed freeze.
                      adoptedPerMgTip = entry.chainStore.lastCheckpointedPerMgTip,
                      slotLeader = shardSlotLeader,
                      publisher = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointPublisher
                        .sidecar[F](sidecarClient),
                      selfPeerId = selfId,
                      selfKeyPair = keyPair,
                      selfVrfSk = shardVrfKeys._1,
                      selfVrfVk = shardVrfKeys._2,
                      operatorKeyRegistry = sharedServices.operatorKeyRegistry,
                      kesSigner = shardKesSigner,
                      shardEtaFor = shardEtaFor,
                      staircaseDeltaSlots = deps.shardingConfig.checkpoint.staircaseDeltaSlots,
                      // The producer and every execution signer re-execute each MG currency derivation against the pinned Phase-2 base.
                      // Selected watchtowers replay before inclusion as positive collusion coverage. Current ordinary GL0 verification also
                      // runs this processor, but that universal replay is transitional; target adopters verify the certificate/coverage,
                      // apply the namespace-confined diff, and recompute the root.
                      //
                      // S(N) READER = FINALIZED `fromMptStore(mptStore)`, NOT `pendingReader` (run-27 freeze fix). The execution prior MUST
                      // be byte-identical to the prior all replaying execution signers/watchtowers read. The producer holds exactly one
                      // unadopted checkpoint until its containing GL0 snapshot reaches P2, so no second checkpoint can run ahead of the
                      // base for an MG's currency partitions. `pendingReader` (`GlobalStateReader.pending`, best-tip
                      // overlay) instead bakes in this node's LOCAL pending state — un-adopted snapshots, advanced lastTxRefs /
                      // sync-view / allow-spend-expiry — produced by the local metagraph fold, a different input than GL0's pinned replay.
                      // So best-tip S(N) ≠ adopted S(N) even in the happy path and the producer root cannot be reproduced (run-26:
                      // ~890 ADOPT-VERIFY/node, all 8 nodes agree bit-for-bit on the recomputed root; only the producer's attested
                      // root diverged — proving gl0's prior is deterministic-finalized and the producer was the lone outlier).
                      // The execution prior is resolved AT the per-checkpoint `executionBaseOrdinal` (4th closure arg) via the
                      // hoisted `finalizedReaderAt` (fast-path live reader when the ordinal is the current base — the common case — else a
                      // version-retained pinned reader). The producer passes `executionBaseOrdinalF`'s captured savepoint as that ordinal.
                      derivePerMgState = ShardCheckpointWiring
                        .reExecDerivationAtPinnedBase[F](
                          shardScEventsProcessor,
                          finalizedReaderAt,
                          getGlobalSnapshotByOrdinalWithFallback
                        )(
                          Async[F],
                          Parallel[F],
                          shardHasher,
                          implicitly[io.constellationnetwork.json.JsonSerializer[F]],
                          globalStateProofSelector
                        ),
                      // Pinned execution-base savepoint: the base ordinal the producer stamps and executes every per-MG window over.
                      // = the signed byte store's NEWEST persisted ordinal (`pinnedExecutionBaseOrdinal`), NOT the live
                      // `mptStore.lastPersistedOrdinal`: the diff prior is resolved EXCLUSIVELY through the version-retained pinned
                      // reader now (no live fast path — the 2026-07-08 mid-fold-skew wedge), and the signed store trails the live
                      // watermark by the finalize lag, so stamping the watermark would OMIT-defer almost every mint while stamping the
                      // store's own latest is resolvable-by-construction on the minting node and finalize-synchronized on every verifier.
                      executionBaseOrdinalF = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
                        .pinnedExecutionBaseOrdinal[F](signedBytesStore),
                      // V1 permits one outstanding checkpoint per shard. Only the exact Phase-2 checkpoint anchor releases its successor.
                      lastPhase2Checkpoint = deps.acceptanceManager.lastAdoptedCheckpoint(shardId),
                      // Tier-1 idempotence cadence (task #45): re-publish a held checkpoint's bytes every N ticks
                      // instead of re-minting (which churned the hash every tick and split attestations — run-19).
                      republishEveryTicks = deps.shardingConfig.checkpoint.republishEveryTicks
                    )
                    .map(shardId -> _)
              }
                .map(_.toMap)
                .flatTap(m => nakLogger.info(s"🧩 Shard producers built: ${m.size} shard(s) (numShards>1 active)"))
                .toResource
          }

          // ─── Execution-certificate threshold closure — receiver-side replay-signature emitter ──
          // Built alongside the producers, reusing the SAME signing material (selfId, keyPair, KES adapter,
          // shard VRF sk) and per-shard proof etas. After an admissible receipt, the daemon walks canonical ancestors and emits any missing
          // local replay signatures so peers can reach the configured `kQuorum` execution-certificate threshold.
          // `None` (numShards <= 1, regression bar) ⇒ the daemon's emit branch is skipped — byte-identical no-op.
          shardCheckpointAttestationEmitter <- shardAcceptanceDeps match {
            case None =>
              Async[F]
                .pure(
                  Option.empty[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]]
                )
                .toResource
            case Some(deps) =>
              implicit val emitHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
              val shardSlotLeader =
                io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardSlotLeader.make[F](eligibilityChecker)
              val shardKesSigner = new io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSigner[F] {
                def sign(
                  operatorKeys: io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys,
                  checkpointEpoch: io.constellationnetwork.schema.nakamoto.EtaPeriod,
                  message: Array[Byte]
                ): F[
                  Option[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSignature]
                ] =
                  signKesAtRegisteredPeriod(operatorKeys, checkpointEpoch, message).map(
                    _.map {
                      case (treeStep, bytes) =>
                        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSignature(treeStep, bytes)
                    }
                  )
              }
              val shardVrfKeys = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                .deriveVrfKeys(keyPair)
              // §5.7: the attester reads the checkpoint's WIRE slot directly — no anchor→slot mapping.
              // Epoch-aware per-shard registered-key proof eta resolver — must match the producer's `shardEtaFor`
              // byte-for-byte for the same `(shardId, epoch)` so the attester's VRF message agrees. Same
              // `etaForPeriodCallback` → 32-byte decode → `computeShardEta(shardId, gl0Eta)` chain. Keyed on the
              // wire-carried `checkpoint.epoch` (passed to `emit`), NOT a wall-clock period, so a checkpoint produced
              // near an eta boundary verifies identically after it. `None` for shards not in this node's registry —
              // the same "shard not tracked locally" disposition the emitter skips on.
              val gl0EtaBytesForPeriod: io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Array[Byte]] =
                (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                  etaForPeriodCallback(epoch).map(h => io.constellationnetwork.security.hex.Hex(h.value).toBytes)
              val tipTrackerFor =
                (sid: io.constellationnetwork.schema.sharding.ShardId) => deps.registry.get(sid).map(_.tipTracker)
              val shardEtaFor: (io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.nakamoto.EtaPeriod) => F[
                Option[Array[Byte]]
              ] =
                (sid: io.constellationnetwork.schema.sharding.ShardId, epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                  if (deps.registry.contains(sid))
                    gl0EtaBytesForPeriod(epoch).flatMap(gl0Eta => shardSlotLeader.computeShardEta(sid, gl0Eta)).map(Some(_))
                  else Async[F].pure(Option.empty[Array[Byte]])
              Async[F]
                .pure(
                  Some(
                    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter.make[F](
                      selfPeerId = selfId,
                      selfKeyPair = keyPair,
                      selfVrfSk = shardVrfKeys._1,
                      selfVrfVk = shardVrfKeys._2,
                      operatorKeyRegistry = sharedServices.operatorKeyRegistry,
                      kesSigner = shardKesSigner,
                      eligibilityChecker = eligibilityChecker,
                      verifyCommitteeSignature = deps.acceptanceManager.verifyCommitteeSignature,
                      sidecarClient = sidecarClient,
                      tipTrackerFor = tipTrackerFor,
                      shardEtaFor = shardEtaFor
                    )
                  ): Option[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]]
                )
                .flatTap(_ => nakLogger.info("🧩 Shard checkpoint attestation emitter built (numShards>1 active)"))
                .toResource
          }

          // ─── WATCHTOWER fraud-proof wiring (numShards>1 AND watchtower-enabled) ────────────────────────────
          // `watchtowerReDerive` is hoisted to the OUTER for-comprehension (above the GSAM) so the on-chain GSAM dispute verdict can use the
          // SAME closure; it is in lexical scope here for the emitter + the daemon's validator.
          watchtowerFraudProofEmitter <- (shardAcceptanceDeps, sharedCfg.nakamoto.invaliditySlashing.watchtowerEnabled) match {
            case (Some(deps), true) =>
              implicit val h: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
              Async[F]
                .pure(
                  Some(
                    io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter.make[F](
                      selfPeerId = selfId,
                      selfKeyPair = keyPair,
                      acceptanceManager = deps.acceptanceManager,
                      sidecarClient = sidecarClient,
                      publishAttempts = sharedCfg.nakamoto.invaliditySlashing.fraudProofPublishAttempts,
                      publishRetryDelay = sharedCfg.nakamoto.invaliditySlashing.fraudProofPublishRetryDelay
                    )
                  ): Option[io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]]
                )
                .flatTap(_ => nakLogger.info("🛡️ Watchtower fraud-proof emitter built (numShards>1 + watchtower-enabled)"))
                .toResource
            case _ =>
              Async[F]
                .pure(Option.empty[io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]])
                .toResource
          }
          invalidStateProofValidator <- shardAcceptanceDeps match {
            case Some(deps) =>
              implicit val h: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
              Async[F]
                .pure(
                  Some(
                    io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator.make[F](
                      reDerivePerMgRoot = watchtowerReDerive,
                      // Daemon validation only stages evidence; bind its duplicate guard to the stable finalized MPT base available here.
                      // The authoritative GSAM fold revalidates against its own consensus-state reader before applying any slash.
                      slashedReader = io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader
                        .fromMptStore[F](mptStore),
                      verifyExecutionCertificate = deps.acceptanceManager.verifyExecutionCertificate
                    )
                  ): Option[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]]
                )
                .flatTap(_ => nakLogger.info("🛡️ Invalid-state-proof dispute validator built (numShards>1 active)"))
                .toResource
            case None =>
              Async[F]
                .pure(Option.empty[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]])
                .toResource
          }

          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                .run[F](
                  consensusFns = consensusFunctions,
                  snapshotStorage = globalSnapshotStorage,
                  chainStore = chainStore,
                  lastGlobalSnapshotStorage = lastGlobalSnapshotStorage,
                  lastNGlobalSnapshotStorage = lastNGlobalSnapshotStorage,
                  eventMempool = eventMempool,
                  sidecarClient = sidecarClient,
                  tipTracker = tipTracker,
                  stakeRegistry = stakeRegistry,
                  operatorKeyRegistry = sharedServices.operatorKeyRegistry,
                  nodeStorage = nodeStorage,
                  keyPair = keyPair,
                  selfId = selfId,
                  lddConfig = lddConfig,
                  eligibilityChecker = eligibilityChecker,
                  etaRotationSnapshots = etaRotationSnapshots,
                  // k₁ — typed HOCON `nakamoto.confirmation-depth-k` (replaces the prior
                  // `sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH")` read inside the loop).
                  confirmationDepthK = sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value,
                  // Legacy parameter name: k2 = 100*k1 recommended local retention/proof/recovery capacity.
                  // It is not a Phase-3 threshold or fork-choice floor in the target protocol.
                  archivalDepthK = sharedCfg.nakamoto.keepDepthBehindFinalized(sharedCfg.environment).value,
                  slotDurationMs = sharedCfg.nakamoto.slotDurationMs.value,
                  shardBootGraceSlots = shardAcceptanceDeps.map(_.shardingConfig.checkpoint.bootGraceSlots.toLong).getOrElse(30L),
                  lastKnownSlotRef = lastKnownSlotRef,
                  epochStateRef = epochStateRef,
                  genesisTimeMs = pureGenesisTimeMs,
                  snapshotSemaphore = snapshotSemaphore,
                  productionGate = productionGate,
                  mptStore = mptStore,
                  mptOverlay = mptOverlay,
                  nakamotoFinalizedOrdinalRef = nakamotoFinalizedOrdinalRef,
                  // Axis 2 (gl1 follow): capture the just-finalized snapshot's GSI here so the slice producer
                  // serves the latest-FINALIZED `(ordinal, GSI)` (resolvable by a finality-gated gl1) rather
                  // than the latest-produced one. Updated monotonically at the depth-k1 finalize sink.
                  latestFinalizedSliceSourceRef = latestFinalizedSliceSourceRef,
                  // #287 "send diffs": the bounded recent-projection ring the loop also fills at the depth-k1
                  // finalize sink, read by `GlobalFollowSliceService.sliceSince` to serve incremental gl1 follow diffs.
                  recentFollowProjectionsRef = recentFollowProjectionsRef,
                  // Task #12 slice 2b: the SAME staging map the consensus functions fill (above), read here at
                  // the depth-k1 finalize sink to promote the finalized snapshot's accumulator into the served ring.
                  pendingAccumulatorsRef = pendingAccumulatorsRef,
                  // 3c-A enabler — signed-bytes staging (same Ref the consensus functions stage into) + the served
                  // signed-bytes store the finalize sink promotes into.
                  pendingPostBytesRef = pendingPostBytesRef,
                  signedBytesStore = signedBytesStore,
                  // Task #12 slice 2b: the served changeset ring the loop fills at the depth-k1 finalize sink; a later
                  // slice wires `GlobalChangeSetService.make(recentFinalizedAccumulatorsRef.get)` to serve it.
                  recentFinalizedAccumulatorsRef = recentFinalizedAccumulatorsRef,
                  // Task #12: served-ring depth (typed HOCON, default 1024) — bumped from the prior hardcoded 256
                  // to cut ml0 full-GSI resyncs by keeping a longer lag window on the light incremental path.
                  changesetRingDepth = sharedCfg.nakamoto.changesetRingDepth.value,
                  finalityTriggerViewRef = finalityTriggerViewRef,
                  settledOrdinalTracker = settledOrdinalTracker,
                  // §1.2 Slice 5/6: parallel-sign attestations + snapshots with KES.
                  operationalKeyMaker = operationalKeyMaker,
                  verifiedLocalOperatorKeys = verifiedLocalOperatorKeys,
                  dataDir = nakamotoDataDir,
                  // Slice S3: drop aggregator entries for `(mg, parent)` pairs in the finalized
                  // snapshot's `stateChannelSnapshots`. Otherwise the tally grows monotonically.
                  //
                  // #214 piggyback: drain any orphan-buffered children waiting on each binary's
                  // value-hash. Current Phase 2 is canonical depth-k1 only while the optimistic
                  // rail is dark; if the local gate dropped the parent at `count < kTarget`,
                  // the just-finalized snapshot proves the cluster admitted it anyway. Re-feed
                  // each drained child through the SAME gate-aware processor used for fresh
                  // gossip — `processOrphanedMetagraphBinary` — NOT direct `processMetagraphBinary`.
                  // The earlier attempt (ad7d4f041, reverted in fea66fc7d) bypassed the gate and
                  // polluted the state-channel queue with locally-unverified binaries.
                  //
                  // After finalization, the child's ML0 parent identity is canonical. The processor decodes the child's signed exact GL0
                  // reference, rechecks that `(ordinal, hash)` as current Phase 2, and only then derives eta/key period and enters
                  // `attestAndAdmit`, including attestations buffered during the parent-continuity lag.
                  // Background-fire (`Async.start`) so the 30s gate timeout doesn't block the
                  // SnapshotLeaderLoop's finalize sink.
                  onFinalize = (snap: io.constellationnetwork.schema.GlobalIncrementalSnapshot) =>
                    snap.stateChannelSnapshots.toList.traverse_ {
                      case (mgAddr, binaries) =>
                        val parents = binaries.toList.map(_.value.lastSnapshotHash).toSet
                        committeeGate.pruneParents(mgAddr, parents) >>
                          HasherSelector[F].withCurrent { implicit hasher =>
                            // Drain children for EVERY binary in the batch, not just `.last`. gl0's
                            // canonical chain-link tip is the last binary's value-hash, but ml0 may
                            // have produced children chained off intermediate binaries before learning
                            // of the more recent ones (network races / split-view at production time).
                            // Each `drainChildren` call is O(1) hashmap lookup — cheap to fan-wide.
                            binaries.toList.traverse_ { binary =>
                              binary.toHashed.flatMap { hashed =>
                                orphanBuffer.drainChildren(mgAddr, hashed.hash).flatMap { drained =>
                                  drained.traverse_(child => Async[F].start(processOrphanedMetagraphBinary(mgAddr, child)).void)
                                }
                              }
                            }
                          }
                    } >> shardAcceptanceDeps.traverse_ { deps =>
                      // A shard checkpoint releases its successor only when its exact containing GL0 snapshot reaches Phase 2. Tentative
                      // GSAM acceptance is deliberately not an anchor. The finalized-range driver invokes this callback for every newly
                      // finalized canonical snapshot, including intermediate ordinals skipped by a multi-ordinal finality advance.
                      HasherSelector[F].withCurrent { implicit hasher =>
                        hasher.hash(snap).flatMap { containingGl0Hash =>
                          chainStore.get(snap.lastSnapshotHash).flatMap {
                            case None if snap.shardCheckpoints.nonEmpty =>
                              nakLogger.warn(
                                s"Shard checkpoint Phase-2 anchor deferred: gl0Ord=${snap.ordinal.value.value} " +
                                  s"gl0Hash=${containingGl0Hash.value.take(12)} parent=${snap.lastSnapshotHash.value.take(12)} " +
                                  "parent context unavailable"
                              )
                            case None => Async[F].unit
                            case Some(parent) =>
                              snap.shardCheckpoints.toList.traverse_ {
                                case (shardId, checkpoint) =>
                                  ShardWindowContinuation
                                    .wasFullyAppliedF(
                                      shardId,
                                      checkpoint,
                                      parent.context.lastStateChannelSnapshotHashes,
                                      snap.stateChannelSnapshots
                                    )
                                    .flatMap {
                                      case false =>
                                        nakLogger.warn(
                                          s"Shard checkpoint Phase-2 anchor rejected: gl0Ord=${snap.ordinal.value.value} " +
                                            s"gl0Hash=${containingGl0Hash.value.take(12)} outerShard=${shardId.value.value} " +
                                            s"checkpointShard=${checkpoint.shardId.value.value} shardOrd=${checkpoint.shardOrdinal.value} " +
                                            "checkpoint was not atomically represented by accepted state-channel snapshots"
                                        )
                                      case true =>
                                        hasher.hash(checkpoint.signingPreimage).flatMap { checkpointHash =>
                                          deps.registry.get(shardId).traverse_ { entry =>
                                            // A follower may learn this checkpoint only through the validated GL0 artifact. Recover it
                                            // into the local shard store before anchoring so an unknown hash cannot leave the next duty
                                            // producer permanently waiting for a best tip it already has authenticated bytes for.
                                            io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointChainStoreRecovery
                                              .ingestValidated(checkpoint, entry.chainStore)
                                              .flatMap { recovered =>
                                                Async[F].raiseUnless(recovered.checkpointHash === checkpointHash) {
                                                  new IllegalStateException(
                                                    s"Phase-2 checkpoint recovery hash mismatch: expected=${checkpointHash.value} " +
                                                      s"recovered=${recovered.checkpointHash.value}"
                                                  )
                                                } >>
                                                  deps.acceptanceManager
                                                    .noteAdopted(shardId, checkpoint.shardOrdinal, checkpointHash) >>
                                                  entry.chainStore.noteAnchor(checkpointHash) >>
                                                  entry.chainStore.finalize(checkpointHash)
                                              }
                                          } >>
                                            nakLogger.debug(
                                              s"Shard checkpoint Phase-2 anchor: gl0Ord=${snap.ordinal.value.value} " +
                                                s"gl0Hash=${containingGl0Hash.value.take(12)} shard=${shardId.value.value} " +
                                                s"shardOrd=${checkpoint.shardOrdinal.value} checkpoint=${checkpointHash.value.take(12)}"
                                            )
                                        }
                                    }
                              }
                          }
                        }
                      }
                    } >> {
                      // LocalEvents: emit `SnapshotFinalized` for this just-finalized snapshot. The publisher
                      // is wired up above; default = noop when local events disabled, so this call is cheap.
                      // The actual snapshot hash is computed via Hasher; the `triggerMask` is intentionally 0
                      // here because that value is computed inside SnapshotLeaderLoop's finality monitor and not
                      // routed through `onFinalize`. The test client filters on ordinal + kind, not mask.
                      HasherSelector[F].withCurrent { implicit hasher =>
                        hasher.hash(snap).flatMap { hash =>
                          val event =
                            io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events.SnapshotFinalized(
                              finalizedOrdinal = snap.ordinal.value.value,
                              snapshotHash = com.google.protobuf.ByteString.copyFrom(hash.value.getBytes),
                              slot = snap.slotCertificate.map(_.slot.value.value).getOrElse(0L),
                              triggerMask = 0 // see comment above
                            )
                          localEventsPublisher.publishSnapshotFinalized(snap.ordinal, event).attempt.void
                        }
                      }
                    },
                  // Gap A — per-shard checkpoint producers + the SAME chain stores they write into + the
                  // per-shard admission-approved binary buffers (the producer fan-out input) + the
                  // static metagraph→shard assignment. Empty / None at numShards=1 (regression bar); the
                  // fan-out in `onSlotWon` is a no-op `traverse_` over the empty map. `shardChainStores` and
                  // `shardBinaryBuffers` are projected off the SAME `shardAcceptanceDeps.registry` entries the
                  // acceptance side + the daemon's intake share, so producer writes ⇒ consumer reads ⇒ chain
                  // grows, and the metagraph gate buffers approved binaries ⇒ the fan-out reads them.
                  shardProducers = shardProducers,
                  shardChainStores = shardAcceptanceDeps
                    .map(_.registry.map { case (sid, entry) => sid -> entry.chainStore })
                    .getOrElse(Map.empty),
                  shardBinaryBuffers = shardAcceptanceDeps
                    .map(_.registry.map { case (sid, entry) => sid -> entry.binaryBuffer })
                    .getOrElse(Map.empty),
                  shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
                  // EXECUTION-SHARDING Task 2: the SAME deterministic committee draw the acceptance manager uses, so the produce
                  // membership gate is consistent with admission. `None` (numShards=1) ⇒ empty-set draw (the fan-out is gated on
                  // `shardProducers.nonEmpty` first, so it is never reached there).
                  shardCommitteeMembership = shardAcceptanceDeps
                    .map(_.committeeMembership)
                    .getOrElse((_: io.constellationnetwork.schema.sharding.ShardId, _: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                      Async[F].pure(Set.empty[io.constellationnetwork.schema.peer.PeerId])
                    )
                )
                .compile
                .drain
            )
            .toResource
          // Start Nakamoto metrics publisher (periodic chain-state gauges)
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoMetrics
                .run[F](
                  chainStore = chainStore,
                  tipTracker = tipTracker,
                  genesisTimeMs = pureGenesisTimeMs
                )
                .compile
                .drain
            )
            .toResource
          // §1.2 Slice 8: KES period evolution aligned with eta rotation cadence.
          //
          // Sender-side sign-time evolution (Slice 5/6) auto-evolves the key on every signAt call,
          // so forward security is preserved *whenever the validator signs*. But a quiet validator
          // (no VRF wins) holds stale period-N bytes until its next sign — the exposure window is
          // 1/relativeStake snapshots in expectation. This fiber tightens that window to a single
          // poll interval by proactively burning past periods at each eta boundary.
          //
          // This maintenance target is the period of the NEXT child built on `finalizedOrd`, not the period of
          // the already-finalized artifact. Therefore the finalized ordinal is intentionally treated as a parent
          // ordinal here. Snapshot sign/verify paths use `globalSnapshotArtifactPeriod(childOrdinal, R)` instead.
          // Both expressions agree for that next child: `artifactPeriod(finalizedOrd + 1) = rotationPeriod(finalizedOrd)`.
          // The cadence is snapshot-indexed, not slot-indexed. Aligns with
          // [[project_consensus_epoch_staggering]]'s "KES periods aligned with eta cadence" rule.
          //
          // GL0 has no in-memory fallback: the key maker above is backed by the mandatory secure
          // store and authenticated against the preregistered operator pair. Evolution targets the
          // operator's tree-relative step (`globalPeriod - registeredOffset`), never the raw global
          // eta period. Advancing to the raw period would irreversibly burn a rotated operator key
          // past the step receivers derive from its registration.
          _ <- supervisor
            .supervise(
              fs2.Stream
                .awakeEvery[F](scala.concurrent.duration.DurationInt(2).seconds)
                .evalMap { _ =>
                  for {
                    finalizedOrd <- nakamotoFinalizedOrdinalRef.get
                    globalPeriod = io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
                      .rotationPeriod(finalizedOrd.value.value, etaRotationSnapshots.toLong)
                    _ <- verifiedLocalOperatorKeys.treeStepFor(globalPeriod) match {
                      case Left(error) =>
                        nakLogger.debug(
                          s"KES proactive evolution deferred at finalizedOrd=${finalizedOrd.value.value}: ${error.getMessage}"
                        )
                      case Right(requiredTreeStep) =>
                        operationalKeyMaker.currentPeriod.flatMap { currentTreeStep =>
                          if (currentTreeStep > requiredTreeStep)
                            Metrics[F].incrementCounter("dag_nakamoto_kes_period_rotation_failures_total") >>
                              nakLogger.warn(
                                s"KES secret is ahead of its preregistered tree step: current=$currentTreeStep required=$requiredTreeStep " +
                                  s"globalPeriod=$globalPeriod offset=${verifiedLocalOperatorKeys.kesPeriodOffset}; refusing further evolution"
                              )
                          else if (currentTreeStep < requiredTreeStep)
                            operationalKeyMaker.evolveTo(requiredTreeStep).flatMap {
                              case Right(_) =>
                                nakLogger.info(
                                  s"KES tree step rotated: $currentTreeStep -> $requiredTreeStep " +
                                    s"(globalPeriod=$globalPeriod offset=${verifiedLocalOperatorKeys.kesPeriodOffset} " +
                                    s"finalizedOrd=${finalizedOrd.value.value})"
                                ) >>
                                  Metrics[F].incrementCounter("dag_nakamoto_kes_period_rotations_total") >>
                                  Metrics[F].updateGauge("dag_nakamoto_kes_current_period", requiredTreeStep.toLong)
                              case Left(err) =>
                                Metrics[F].incrementCounter("dag_nakamoto_kes_period_rotation_failures_total") >>
                                  nakLogger.warn(
                                    s"KES evolveTo(treeStep=$requiredTreeStep, globalPeriod=$globalPeriod) failed: $err"
                                  )
                            }
                          else cats.Applicative[F].unit
                        }
                    }
                  } yield ()
                }
                .compile
                .drain
            )
            .toResource
          // Start ChainSyncInbound gRPC server — serves local chain data to peers
          // via the sidecar. The sidecar calls this server when peers request
          // snapshots or chain points for the ChainSync protocol.
          chainSyncDispatcher <- Dispatcher.sequential[F]
          chainSyncServer = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncServer
            .make[F](
              chainStore,
              globalSnapshotStorage,
              // #259: recent-finalized snapshots + the SAME orphan buffer the daemon writes into, so a
              // peer's metagraph-binary fetch is answered from finalized state + a non-destructive
              // orphan-buffer peek.
              lastNGlobalSnapshotStorage,
              orphanBuffer,
              nakamotoDataDir,
              chainSyncDispatcher
            )(
              implicitly,
              implicitly,
              implicitly,
              scala.concurrent.ExecutionContext.global
            )
          // Server lifecycle: start it on Resource acquire, shut it down cleanly on release so
          // the listen socket is returned to the OS instead of being held by a leaked Java server.
          // `shutdown()` requests graceful close; we give it a short grace period and then drop on
          // timeout — we don't want a hung RPC from a misbehaving peer to block app teardown.
          _ <- Resource.make(
            Async[F].blocking {
              val grpcServer = io.grpc.ServerBuilder
                .forPort(50053)
                .addService(
                  io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ChainSyncInboundGrpc
                    .bindService(chainSyncServer, scala.concurrent.ExecutionContext.global)
                )
                .build()
              grpcServer.start()
              grpcServer
            }
          )(srv =>
            Async[F].blocking {
              srv.shutdown()
              srv.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)
              ()
            }
              .handleError(_ => ())
          )
          _ <- nakLogger.info("🔗 ChainSyncInbound gRPC server started on port 50053").toResource

          // Chain-sync recovery (run-20, task #A): the per-node HTTP puller for missed shard checkpoints. `None`
          // at numShards=1 (no deps). Constructed here where `client` + `clusterStorage` are in scope, then threaded
          // into the daemon's T2 absence stream.
          shardCheckpointFetcher <- shardAcceptanceDeps.traverse { deps =>
            io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ShardCheckpointFetcher
              .make[F](client, clusterStorage, deps.shardingConfig.checkpoint.pullDedupCooldownMs)
          }.toResource

          // Start NakamotoSyncDaemon: receives snapshots + attestations from gossip
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon
                .run[F](
                  channel = sidecarClient.channel,
                  chainStore = chainStore,
                  nodeStorage = nodeStorage,
                  tipTracker = tipTracker,
                  stakeRegistry = stakeRegistry,
                  operatorKeyRegistry = sharedServices.operatorKeyRegistry,
                  sidecarClient = sidecarClient,
                  selfId = selfId,
                  keyPair = keyPair,
                  lddConfig = lddConfig,
                  eligibilityChecker = eligibilityChecker,
                  lastKnownSlotRef = lastKnownSlotRef,
                  epochStateRef = epochStateRef,
                  etaRotationSnapshots = etaRotationSnapshots,
                  // k₁ — typed HOCON `nakamoto.confirmation-depth-k` (replaces the prior module-level
                  // `sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH")` read inside NakamotoSyncDaemon).
                  confirmationDepthK = sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value,
                  consensusFns = consensusFunctions,
                  snapshotStorage = globalSnapshotStorage,
                  lastGlobalSnapshotStorage = lastGlobalSnapshotStorage,
                  lastNGlobalSnapshotStorage = lastNGlobalSnapshotStorage,
                  snapshotSemaphore = snapshotSemaphore,
                  productionGate = productionGate,
                  mptStore = mptStore,
                  mptOverlay = mptOverlay,
                  // Task #12 slice-2c — same staging Ref the consensus functions (575) + leader loop (1574) hold.
                  // Lets the daemon's validator-adopt path rekey a NON-producer's staged accumulator
                  // stripped->canonical so it promotes on finalize (complete served changeset ring).
                  pendingAccumulatorsRef = pendingAccumulatorsRef,
                  pendingPostBytesRef = pendingPostBytesRef,
                  eventMempool = eventMempool,
                  dataDir = nakamotoDataDir,
                  enqueueAllowSpendBlock = enqueueAllowSpendBlock,
                  enqueueDAGBlock = enqueueDAGBlock,
                  enqueueTokenLockBlock = enqueueTokenLockBlock,
                  // §1.2 Slice 5/6/9: KES sender-side signing + receiver-side load-bearing verify.
                  // Always-on; no env flag — verification failures drop the message.
                  operationalKeyMaker = operationalKeyMaker,
                  // Gate metagraph binaries on committee threshold. Eta and active key period derive
                  // from the binary's exact signed Phase-2 GL0 anchor, never its ML0 ordinal or a local head.
                  committeeGate = committeeGate,
                  verifyPhase2CurrencyContext = committeeVerifyPhase2CurrencyContext,
                  etaForPhase2Anchor = committeeEtaForPhase2Anchor,
                  senderStakeLookup = (peer: io.constellationnetwork.schema.peer.PeerId) => stakeRegistry.committeeStake(peer),
                  processOrphanedMetagraphBinary = processOrphanedMetagraphBinary,
                  // #259: same orphan-buffer instance the processor closure writes into — the
                  // stuck-detection tick reads its pending parents to drive active recovery.
                  orphanBuffer = orphanBuffer,
                  // Gap B — acceptance-side shard deps for the receiver-routing handlers. `None` at
                  // numShards=1 (regression bar) ⇒ inbound shard-checkpoint gossip is dropped with a
                  // debug log. The SAME `shardAcceptanceDeps` instance the GSAM acceptance side + the
                  // Gap-A producers use, so the chain stores incoming checkpoints land in are the ones
                  // the producers + finality triggers read.
                  shardAcceptanceDeps = shardAcceptanceDeps,
                  // Chain-sync recovery (run-20, task #A): the HTTP puller driving the T2 absence stream.
                  shardCheckpointFetcher = shardCheckpointFetcher,
                  // Execution-certificate closure: after local replay of the exact checkpoint, sign and gossip our
                  // execution signature so peers can reach configured `kQuorum`. `None` at numShards=1 means no emit.
                  shardCheckpointAttestationEmitter = shardCheckpointAttestationEmitter,
                  // WATCHTOWER (fraud-proof part 1 + 2): re-execute each adopted checkpoint on the quorum path +
                  // gossip a FraudProofEnvelope on a per-MG root mismatch; the validator re-runs the deterministic
                  // verdict on inbound fraud proofs. `None` at numShards=1 / watchtower-disabled.
                  watchtowerFraudProofEmitter = watchtowerFraudProofEmitter,
                  invalidStateProofValidator = invalidStateProofValidator,
                  // Per-ord producer fan-out (decoupled from gl0-leader win): EVERY node fans out shard
                  // checkpoints for each canonical (best-tip) gl0 ord it receives via gossip. Reuses the
                  // SAME `shardProducers` the leader loop uses + the `shardAssignment` off `shardAcceptanceDeps`.
                  // The daemon derives `shardChainStores` in-daemon from `shardAcceptanceDeps.registry`. EMPTY /
                  // `None` at numShards=1 (regression bar) ⇒ the per-ord hook is `whenA(false)`.
                  shardProducers = shardProducers,
                  shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
                  // WATCHTOWER fraud-proof POOL (W3a): on a locally-UPHELD inbound dispute, `handleFraudProof` OFFERS the validated evidence
                  // here so the gl0 leader producer embeds it in the next snapshot's `fraudProofs` consensus field. SAME instance the producer
                  // peeks. `noop` at numShards=1 ⇒ no staging ⇒ byte-identical regression bar.
                  fraudProofPool = fraudProofPool
                )
                .compile
                .drain
            )
            .toResource

          // P-11 (#141): re-bootstrap orchestrator. Periodic ticker that detects when this
          // node has self-finalized a divergent fork and got locked out of canonical recovery
          // (the "fork-recovery deadlock"). Gated by `sharedCfg.nakamoto.rebootstrapEnabled` (typed
          // HOCON, default TRUE; env override `NAKAMOTO_REBOOTSTRAP_ENABLED`) — without it a node that
          // self-finalized a divergent branch forks the global mptRoot forever (sharded reorg storm).
          // Disabled: emits a single INFO at startup and otherwise no-op.
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.RebootstrapOrchestrator
                .run[F](
                  enabled = sharedCfg.nakamoto.rebootstrapEnabled,
                  chainStore = chainStore,
                  tipTracker = tipTracker,
                  mptOverlay = mptOverlay,
                  productionGate = productionGate,
                  snapshotSemaphore = snapshotSemaphore
                )
                .compile
                .drain
            )
            .toResource

        } yield ()
      }
      consensus = new Consensus(
        handler,
        consensusStorage,
        loop.manager,
        routes,
        consensusFunctions,
        Some(loop.healthRef),
        Some(triggerEvent)
      )
    } yield consensus
}
