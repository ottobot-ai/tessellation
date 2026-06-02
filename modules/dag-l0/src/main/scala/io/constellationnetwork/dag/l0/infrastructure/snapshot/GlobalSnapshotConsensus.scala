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

/** Factory for creating the Global L0 consensus engine.
  *
  * Wires together all components and starts the consensus background stream. Returns a Consensus instance with handler (for gossip),
  * manager (external API), storage (state queries), and routes (HTTP endpoints).
  *
  * When NAKAMOTO_ENABLED env var is set, uses VRF slot-based leader election via NakamotoTriggerDaemon instead of the traditional
  * EventTrigger + TimeTrigger system. The BFT round machinery (Facility → Proposal → Signature → Finished) remains unchanged.
  *
  * @see
  *   ConsensusEventLoop for FSM and command processing
  * @see
  *   NakamotoTriggerDaemon for VRF slot clock implementation
  */
object GlobalSnapshotConsensus {

  // GL0 is Nakamoto-only. No env var check needed — the run-nakamoto CLI command
  // is the single source of truth. Tunables come from NAKAMOTO_* env vars below.

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
  val nakamotoGenesisTimeMs: Long =
    sys.env.get("NAKAMOTO_GENESIS_TIME_MS").flatMap(_.toLongOption).getOrElse(System.currentTimeMillis())

  /** §3 NIPoPoW genesis eta — Blake2b-256 digest of a fixed domain string. 32 bytes; used to seed `EtaCalculation` for period 0 (and as the
    * empty-chain-walk fallback at every higher period, incl. the COMPUTED period 1, #259) and as the bootstrap fall-through for
    * `EtaStateManager`. Must be identical across all nodes in a cluster — derived from a constant rather than env var to avoid a
    * config-drift class of bug (different operators setting different `NAKAMOTO_GENESIS_ETA` values would silently fork the chain). Future
    * work: derive from the genesis snapshot hash so it's chain-bound instead of literal-bound.
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
    // so the finality call sites (depth-k + attestation-2/3) can notify the overlay when a branch
    // becomes canonical at an ordinal. Currently a passthrough impl by default — finalize is a
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
    // #117/#118 Phase 2: branch-aware reader used by the snapshot binary fee calculator (and
    // any consensus-internal call site that needs read access at the chain's bestTip). Under
    // MultiBranch this picks up the chain's pending writes; the legacy `mptStore` path saw
    // base-only and could miscalculate fees during finality stalls (#117 root cause).
    pendingReader: io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader[F],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    eventGossipClient: EventGossipClient[F, GlobalSnapshotEvent],
    loggerBundle: LoggerBundle[F],
    rumorQueue: Queue[F, Hashed[RumorRaw]],
    // Updated by SnapshotLeaderLoop after every chainStore.finalize call. Read by HttpApi
    // to expose /global-snapshots/latest/finalized-ordinal so CL0 can gate state-channel
    // -binary pruning on actual finality. Seeded with SnapshotOrdinal.MinIncrementalValue
    // (ordinal 1 = genesis); grows monotonically as finality advances.
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    // Observability seam for the chain-quality HTTP route (#138). Populated by SnapshotLeaderLoop
    // once the four FinalityTriggers are constructed; read by `FinalityTriggersRoutes` to answer
    // "which triggers qualified ord N?" without owning trigger references. Empty until the
    // leader-loop fiber has started — the route handles `None` as a 503.
    finalityTriggerViewRef: Ref[F, Option[io.constellationnetwork.node.shared.domain.nakamoto.FinalityTriggerView[F]]],
    // §3 NIPoPoW S5 — observability seam for the NipopowRoutes light-client endpoints. Populated
    // here in the resource block once the local `TowerStore` and snapshot storage are available.
    // Read by `NipopowRoutes` to answer GET /nakamoto/nipopow/proof and POST /nakamoto/nipopow/verify
    // without owning the builder/verifier references. Empty until this resource has produced the
    // wired provider — the route handles `None` as a 503.
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
    // §1.2 Slice 3c: (peerId → KES master VK) registry loaded from L0 genesis. Empty for the CSV-
    // genesis bootstrap path (no per-operator KES VKs registered). Read by Slice 5/6 verification
    // on every incoming attestation/snapshot to confirm the sender's KES signature against the
    // genesis-registered VK without trusting the sender to ship its own VK in-band.
    kesRegistry: io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry[F],
    // Slice S1: (peerId → VRF verification key) registry loaded from L0 genesis (`operators[].vrfPublicKey`,
    // derived via the SAME `VrfKeyDeriver.deriveVrfKeyPair` the gl0 leader loop uses). Threaded as an
    // AVAILABLE dependency into `ShardCheckpointWiring.acceptanceDeps` so a later slice (S2) can do real
    // per-signer committee sortition. NOT consumed in S1 — committee membership is still full-set, so this
    // is a no-op at every `numShards`. Empty for the CSV-genesis bootstrap path.
    vrfRegistry: io.constellationnetwork.node.shared.domain.nakamoto.VrfRegistry[F],
    // Split-safety (#261, eta axis): the deferred chain-walk handle the follower / `createContext` GSAM's
    // committee-eta resolver reads (created in `TessellationIOApp.make`, threaded here via `Services.make`).
    // Set ONCE below — right after the leader's own `chainStoreForLookupRef` — to the SAME
    // `chainStore.vrfOutputsForPeriod`-backed walk the leader uses, so the follower `EtaStateManager.getEta(P)`
    // returns byte-identical eta to the leader's for EVERY period P (incl. the cross-period checkpoint ride).
    // gl0 is the only layer that sets it; non-gl0 layers leave it `None` (empty walk → genesis, unchanged).
    setFollowerEtaChainWalk: (Long => F[List[(Long, Array[Byte])]]) => F[Unit]
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
          io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
        ]](Map.empty)
        .toResource
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
      //   - The chain-walk fallback routes to `chainStore.vrfOutputsForPeriod` for `period - 1`. Because the chain store
      //     is built *later* in the inner Resource block (after this GSAM is constructed), we route the walk through
      //     [[chainStoreForLookupRef]] (the same Ref that backs the `getGlobalSnapshotByOrdinalWithFallback` indirection
      //     above) — set once by the inner block, observed by every walk thereafter.
      //   - The [[HistoricalStakeReader]] is built against `GlobalStateReader.fromMptStore(mptStore)` because GSAM accept()
      //     runs against the underlying base store at boundary-write time; the per-call branch-aware reader is only used
      //     for prior-state reads from within accept(), not for boundary lookups (the boundary key is being WRITTEN this
      //     ordinal — the reader will miss either way, and chain-walk takes over).
      //
      // Note: pre-chainStore-setup boundary writes (genesis seed + period 0/1) compute genesisEta via EtaCalculation; this
      // is the same value `EtaStateManager.getEta` returns when the walk yields an empty list, so the pre-/post-setup
      // boundary writes are byte-identical and the MPT entry is deterministic across nodes.
      etaForPeriodCallback <- {
        val historicalStakeReaderForOuterGsam =
          io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader
            .make[F](io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](mptStore))
        val chainWalkFallback: Long => F[List[(Long, Array[Byte])]] = (sourcePeriod: Long) =>
          chainStoreForLookupRef.get.flatMap {
            case Some(cs) => cs.vrfOutputsForPeriod(sourcePeriod, sharedCfg.nakamoto.etaRotationSnapshots.value)
            case None     => Async[F].pure(List.empty[(Long, Array[Byte])])
          }
        io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager
          .make[F](
            genesisEta = nakamotoGenesisEta,
            historicalStakeReader = historicalStakeReaderForOuterGsam,
            chainWalkFallback = chainWalkFallback
          )
          .map { mgr => (period: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
            HasherSelector[F].withCurrent(implicit hasher => mgr.getEta(period.value).map(etaBytesToHash))
          }
          .toResource
      }

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

      // Hierarchical-shard-checkpoints v1 — ACCEPTANCE-side production wiring (priority 1). Gated on
      // `sharedCfg.nakamoto.sharding.numShards > 1`. At the production default `numShards = 1` this returns
      // `None` (constructs nothing) and the GSAM `make` below passes `None` for all three sharding params —
      // byte-identical to the pre-wiring call (the regression bar). See `ShardCheckpointWiring` scaladoc.
      //
      // This is the gl0-leader produce path, so the genesis-loaded `kesRegistry` (the make() param) IS in
      // scope here — passed through so the acceptance manager verifies each checkpoint signer's KES product
      // sig against the registered master VK. The active-validator set is the seedlist minus `metagraph-op`
      // aliases (same derivation as `validatorPeers` in the inner block), falling back to `{selfId}`.
      //
      // S3 committee re-execution: the SAME `GlobalSnapshotStateChannelEventsProcessor` gl0 uses for metagraph
      // snapshots is built once here and shared by (a) GSAM acceptance, (b) the shard verifier's
      // `reExecuteDerivation`, and (c) the shard producer's `derivePerMgState`. Sharing one instance is what
      // guarantees producer + verifier run the IDENTICAL currency derivation — the byte-identity contract that
      // prevents false-slashing (see `GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot`).
      shardScEventsProcessor = GlobalSnapshotStateChannelEventsProcessor.make[F](
        validators.stateChannelValidator,
        globalStateChannelManager,
        sharedServices.currencySnapshotContextFns,
        feeCalculator,
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(mptStore)
      )

      // `reExecuteDerivation` = the real S3 committee re-execution closure (replaces the
      // `ShardCheckpointWiring.noReExecDerivation` fail-closed stub). On the `T_depth1_shard` degraded path the
      // verifier re-runs each MG's derivation over its included chain at the wire-carried `gl0AnchorOrdinal` and
      // rejects (+ surfaces slash signers) on a byte-mismatch. S3 wires re-exec → reject; APPLYING the slash
      // penalty stays a separate slice (S2.0) — `adoptShardCheckpoints` still only logs the signer list.
      shardAcceptanceDeps <- ShardCheckpointWiring
        .acceptanceDeps[F](
          cfg = sharedCfg.nakamoto.sharding,
          selfPeerId = selfId,
          kesRegistry = kesRegistry,
          // EXECUTION-SHARDING: genesis-loaded VRF-VK registry — the per-operator seed for the real shard-committee sortition.
          vrfRegistry = vrfRegistry,
          activeValidators = Async[F].pure(
            seedlist
              .map(_.collect { case e if !e.alias.exists(_.value.value == "metagraph-op") => e.peerId })
              .getOrElse(Set(selfId))
          ),
          // Per-period eta for `committeeFor`: the SAME `EtaStateManager.getEta` resolver the GSAM boundary writer uses, decoded
          // from the hex `Hash` to the 32 raw eta bytes (mirrors `gl0EtaBytesForPeriod` below). MPT-committed ⇒ byte-identical
          // cluster-wide, keyed on the wire-carried `checkpoint.epoch`.
          etaForEpoch = (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
            etaForPeriodCallback(epoch).map(h => io.constellationnetwork.security.hex.Hex(h.value).toBytes),
          reExecuteDerivation = Some(
            ShardCheckpointWiring.reExecDerivation[F](shardScEventsProcessor)(Async[F], HasherSelector[F].getCurrent)
          )
        )(Async[F], HasherSelector[F].getCurrent, implicitly[SecurityProvider[F]], implicitly[Metrics[F]])
        .toResource

      // §3 NIPoPoW historical-commitment SMT store — gl0-only (this produce/verify GSAM has the finalized global-snapshot chain
      // via `getGlobalSnapshotByOrdinalWithFallback`, which accept() reads to derive each finalized ordinal's commitment). In-memory
      // reference (durable MPT producer + versioned SMT); recoverable by chain-replay like `MptTowerStore`. The SharedServices
      // (cl0/dl1 follower) GSAM is NOT given the store, so those layers keep `smtRoot = None`.
      historicalCommitmentSmtStore <- {
        implicit val h: Hasher[F] = HasherSelector[F].getCurrent
        io.constellationnetwork.node.shared.domain.nakamoto.nipopow.HistoricalCommitmentSmtStore
          .inMemory[F](sharedCfg.nakamoto.commitmentSmt.versionRootRetention.value)
      }.toResource

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
          // `maintainNodeCollateralWithdrawalExpiryIndex` left at default (false) here to preserve
          // existing behavior of this construction site — the SharedServices GSAM sets it to true,
          // but reconciling the two flags is out of scope for the Path 1 fix.
          etaRotationSnapshots = sharedCfg.nakamoto.etaRotationSnapshots.value,
          etaForPeriod = Some(etaForPeriodCallback),
          localEventsPublisher = Some(localEventsPublisher),
          // Hierarchical-shard-checkpoints v1 acceptance-side deps. `None` at `numShards = 1` (regression bar);
          // `Some(...)` activates the shard-checkpoint admission path inside `accept()`. Mirrors the SharedServices
          // GSAM construction so the gl0-leader-produce and verify paths stay consistent.
          shardingConfig = shardAcceptanceDeps.map(_.shardingConfig),
          shardCheckpointAcceptanceManager = shardAcceptanceDeps.map(_.acceptanceManager),
          shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
          // §3 NIPoPoW historical-commitment SMT: wire the gl0 store + the confirmation-depth cutoff so accept() anchors
          // `smtRoot(N)`. Same k the leader loop / sync daemon use (`nakamoto.confirmation-depth-k`, default 255).
          historicalCommitmentSmtStore = Some(historicalCommitmentSmtStore),
          confirmationDepthK = sharedCfg.nakamoto.confirmationDepthK.value
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
          pendingAccumulatorsRef = pendingAccumulatorsRef
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
      nakamotoStateRef <- {
        val genesisEta = sys.env.get("NAKAMOTO_GENESIS_ETA").map(_.getBytes).getOrElse("tessellation-nakamoto-genesis".getBytes)
        Ref.of[F, NakamotoTriggerState](NakamotoTriggerState.initial(nakamotoGenesisTimeMs, genesisEta)).map(Some(_))
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
          nakamotoStateRef
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

      // Nakamoto LDD + VRF config. Env vars are typed Double for backwards-compatible config; we lock
      // them into Ratio at boot via `Ratio.apply(double, prec)` so the threshold computation is exact
      // and reproducible across all JVMs/CPUs.
      lddConfig = {
        val default = io.constellationnetwork.schema.nakamoto.LddConfig.Default
        io.constellationnetwork.schema.nakamoto.LddConfig(
          lddCutoff = sys.env.get("NAKAMOTO_LDD_CUTOFF").flatMap(_.toIntOption).getOrElse(default.lddCutoff),
          offset = sys.env.get("NAKAMOTO_LDD_OFFSET").flatMap(_.toIntOption).getOrElse(default.offset),
          baselineDifficulty = sys.env
            .get("NAKAMOTO_LDD_BASELINE")
            .flatMap(_.toDoubleOption)
            .map(io.constellationnetwork.numerics.Ratio(_, io.constellationnetwork.schema.nakamoto.LddConfig.DoubleParsePrecision))
            .getOrElse(default.baselineDifficulty),
          amplitude = sys.env
            .get("NAKAMOTO_LDD_AMPLITUDE")
            .flatMap(_.toDoubleOption)
            .map(io.constellationnetwork.numerics.Ratio(_, io.constellationnetwork.schema.nakamoto.LddConfig.DoubleParsePrecision))
            .getOrElse(default.amplitude)
        )
      }
      slotsPerEpoch = sys.env.get("NAKAMOTO_SLOTS_PER_EPOCH").flatMap(_.toLongOption).getOrElse(60L)
      // R = 2550 = 10·k₁ (matches Cardano R/k ratio). Rotation is keyed on **ordinal**, not slot —
      // slots are LDD-paced and lumpy; ordinals are 1:1 with snapshots and give a stable R that
      // satisfies the Praos R ≥ 3·k₁ stability bound. See `docs/nakamoto/attestation-and-finality.md` §1.
      // Path 1 (heap-leak workstream): moved from `sys.env.get("NAKAMOTO_ETA_ROTATION_SNAPSHOTS")` to
      // HOCON `nakamoto.eta-rotation-snapshots` (which still honors `${?NAKAMOTO_ETA_ROTATION_SNAPSHOTS}`
      // substitution so ops scripts keep working).
      etaRotationSnapshots = sharedCfg.nakamoto.etaRotationSnapshots.value

      // Start the Nakamoto SnapshotLeaderLoop + sidecar bridge.
      //
      // This block runs in the outer Resource context so the long-lived resources it creates
      // (ChainSyncRequestQueue drainer, Dispatcher for the ChainSync gRPC server, the gRPC
      // server itself) are released when the app Resource tree tears down, instead of being
      // escaped via `.allocated` and leaked across test restarts / shutdown.
      _ <- {

        val pureGenesisTimeMs = nakamotoGenesisTimeMs
        for {
          nakLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger.getLoggerFromName[F]("NakamotoConsensus").pure[F].toResource
          _ <- nakLogger
            .info(
              s"🔧 Nakamoto config: LDD(cutoff=${lddConfig.lddCutoff}, offset=${lddConfig.offset}, baseline=${lddConfig.baselineDifficulty}, amplitude=${lddConfig.amplitude}), etaRotation=${etaRotationSnapshots} snapshots, slotsPerEpoch=${slotsPerEpoch}, genesisTime=${pureGenesisTimeMs}"
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
          tipTracker <- io.constellationnetwork.node.shared.domain.nakamoto.TipTracker.make[F](stakeRegistry).toResource
          // Slice S3: committee sortition + per-binary attestation aggregator. The sortition is a
          // stateless function; the aggregator holds the per-`(metagraph, parent, binary)` tally
          // until `pruneParents` is called from the finality hook. K_target defaults to the active
          // gl0 operator count (degenerate K=N — everyone in every committee, gate is effectively a
          // no-op). For genuine sortition set `NAKAMOTO_COMMITTEE_K_TARGET`. The gate itself is
          // constructed below once the operationalKeyMaker + kesRegistry are in scope.
          // CommitteeSortition uses `Hasher[F]` to encode the canonical VRF input. The Selector's
          // current hasher matches the message-side encoding the rest of the consensus surface
          // uses; sortition messages aren't ordinal-bound (they key on parent hash) so picking
          // the current hasher rather than `getForOrdinal` is correct.
          committeeSortition = io.constellationnetwork.node.shared.domain.nakamoto.CommitteeSortition
            .make[F](implicitly[Async[F]], HasherSelector[F].getCurrent)
          committeeAggregator <- io.constellationnetwork.node.shared.domain.nakamoto.MetagraphAttestationAggregator
            .make[F]
            .toResource
          // §1.2 Slice 3c: bootstrap an OperationalKeyMaker. Two paths:
          //   - Disk-backed (production / e2e harness): if `CL_KES_SECURE_STORE_DIR` is set, open a
          //     [[io.constellationnetwork.security.kes.SecureStore.disk]] at that directory and
          //     `OperationalKeyMaker.make` against the pre-staged `kes-sk.bin` written by the
          //     genesis generator (Slice 3b). Its master VK matches the genesis-registered VK by
          //     construction, so Slice 5 verification works end-to-end out of the box.
          //   - Fallback (dev / unit-test): no env var → in-memory store + fresh bootstrap with a
          //     SecureRandom seed. The master VK is per-JVM-run and will NOT match anything in
          //     `kesRegistry`, so Slice 5 receivers will count it as "no registry entry" — fine for
          //     local dev (warn-only) but not for cross-node verification.
          kesSecureStore <- sys.env.get("CL_KES_SECURE_STORE_DIR") match {
            case Some(dir) =>
              io.constellationnetwork.security.kes.SecureStore.disk[F](java.nio.file.Paths.get(dir)).toResource
            case None =>
              io.constellationnetwork.security.kes.SecureStore.inMemory[F].toResource
          }
          operationalKeyMaker <- sys.env.get("CL_KES_SECURE_STORE_DIR") match {
            case Some(_) =>
              // Disk path: SK was pre-staged at `<dir>/kes-sk.bin` by the genesis generator
              // (Slice 3b). Just open; do not regenerate.
              io.constellationnetwork.security.kes.OperationalKeyMaker
                .make[F](
                  secureStore = kesSecureStore,
                  keyName = "kes-sk.bin",
                  etaPeriodLength = etaRotationSnapshots.toLong
                )
            case None =>
              // Fallback: in-memory + fresh bootstrap with SecureRandom seed.
              val seed = new Array[Byte](32)
              new java.security.SecureRandom().nextBytes(seed)
              io.constellationnetwork.security.kes.OperationalKeyMaker
                .bootstrap[F](
                  secureStore = kesSecureStore,
                  keyName = "gl0-operational-kes.key",
                  seed = seed,
                  etaPeriodLength = etaRotationSnapshots.toLong
                )
          }
          // §1.2 Slice 2: bind the KES master VK (period-0 root of the super × sub tree) to the
          // operator's long-term Ed25519 key via a SHA512withECDSA signature over kesVk.value.
          // The pair (kesVk, kesVkSigByLongTerm) is the registration certificate: it lets any
          // other validator confirm that the KES VK they see in genesis (or in a future runtime
          // registration tx) was issued by this operator without anyone needing access to the
          // KES SK. KES forward security is preserved because the long-term key signs the VK,
          // not the SK — compromise of the long-term key still cannot forge KES sigs for
          // past periods (those required the corresponding SK leaf that has been erased).
          kesRegistration <- operationalKeyMaker.currentPublicKey.flatMap { kesVk =>
            io.constellationnetwork.security.signature.Signing
              .signData[F](kesVk.value)(keyPair.getPrivate)
              .map(regSig => (kesVk, regSig))
          }.toResource
          _ <- {
            val (kesVk, regSig) = kesRegistration
            val vkHex = kesVk.value.take(16).map("%02x".format(_)).mkString
            val regHex = regSig.take(16).map("%02x".format(_)).mkString
            nakLogger.info(
              s"🔐 KES bootstrap: period=0 height=${io.constellationnetwork.security.kes.OperationalKeyMaker.DefaultHeight} vk=$vkHex... step=${kesVk.step}"
            ) >>
              nakLogger.info(
                s"🔐 KES-REG: master-vk-sig=$regHex... (SHA512withECDSA over kesVk by operator long-term key, ${regSig.length}B)"
              ) >>
              // §1.2 Slice 3c: log the genesis-loaded KesRegistry size. Empty means no peers were
              // registered at genesis (CSV-genesis bootstrap, or JSON without the kesRegistrations
              // field) — Slice 5 verification will count incoming KES sigs against the empty
              // registry as "no entry" and warn-only-skip.
              kesRegistry.list.flatMap { regs =>
                nakLogger.info(s"📋 KES registry loaded: ${regs.size} peer(s) registered")
              }
          }.toResource

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
          chainSelection = io.constellationnetwork.node.shared.domain.nakamoto.ChainSelection.make[F](tipTracker, fetchParent)
          // Heap-leak Fix B — `nakamoto.keep-depth-behind-finalized` (default = k₁ = 255). Bounds
          // in-memory canonical-chain retention to a sliding window behind the finalized tip; older
          // lookups fall through to disk-backed `SnapshotStorage` via
          // `NakamotoChainStore.getWithOrdinalFallback`. Path 1 of the workstream: with the disk
          // fallback wired through `vrfOutputsForPeriod`, this is a perf knob (in-memory speed vs
          // bounded heap), not a correctness gate, even when `etaRotationSnapshots > keepDepth`.
          // Migrated from `sys.env.get("NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED")` to HOCON; the
          // application.conf entry still honors `${?NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED}` so ops
          // scripts keep working.
          keepDepthBehindFinalized = sharedCfg.nakamoto.keepDepthBehindFinalized.value
          chainStore <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoChainStore
            .make[F](
              globalSnapshotStorage,
              chainSelection,
              tipTracker,
              nakamotoFinalizedOrdinalRef,
              keepDepthBehindFinalized
            )
            .toResource
          _ <- chainStoreRef.set(Some(chainStore)).toResource
          _ <- chainStoreForLookupRef.set(Some(chainStore)).toResource
          // Split-safety (#261, eta axis): install the SAME chain-walk the leader's committee-eta resolver uses
          // (`etaForPeriodCallback`'s `chainWalkFallback` at the top of this `make`) into the follower /
          // `createContext` GSAM's `EtaStateManager` via the deferred Ref. Both walks read the identical
          // `chainStoreForLookupRef` and call `cs.vrfOutputsForPeriod(sourcePeriod, etaRotationSnapshots)`, so the
          // follower's `getEta(P)` now byte-equals the leader's for EVERY period P ≥ 2 — closing the
          // `C_real ≠ C_genesis` committee split on the gl0 Download / RollbackLoader rebuild paths. Set AFTER
          // `chainStoreForLookupRef` so the very first follower walk already observes the chain store.
          _ <- setFollowerEtaChainWalk { (sourcePeriod: Long) =>
            chainStoreForLookupRef.get.flatMap {
              case Some(cs) => cs.vrfOutputsForPeriod(sourcePeriod, sharedCfg.nakamoto.etaRotationSnapshots.value)
              case None     => Async[F].pure(List.empty[(Long, Array[Byte])])
            }
          }.toResource
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
                      .void
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
          // Shared ChainSyncManager reference. Populated by NakamotoSyncDaemon after it constructs
          // its internal manager; read-through by the ChainSyncRequestQueue worker below so the
          // reactive walkback path piggybacks on existing hash-keyed dedup + fetch. No-op until bound.
          sharedChainSyncManagerRef <- cats.effect.kernel.Ref
            .of[F, Option[
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncManager.ChainSyncManagerAlgebra[F]
            ]](None)
            .toResource
          // Reactive ChainSync trigger for the finality walkback path (see
          // ChainSyncRequestQueue docs for the full rationale). Bound directly into the outer
          // Resource — the drainer fiber is torn down cleanly on app shutdown.
          chainSyncRequestQueue <- {
            val workerFn = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncRequestQueue
              .walkbackWorker[F] _
            val reactiveWorker: Long => F[Unit] = ord =>
              sharedChainSyncManagerRef.get.flatMap {
                case Some(csm) => workerFn(sidecarClient.channel, csm).apply(ord)
                case None      =>
                  // Pre-binding window before NakamotoSyncDaemon publishes its ChainSyncManager.
                  // Brief in practice; the next 5s finality-monitor tick re-offers if the fork persists.
                  cats.Applicative[F].unit
              }
            io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.ChainSyncRequestQueue
              .make[F](reactiveWorker)
          }
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
          // sidecar; receiver path verifies all three sigs then records into the aggregator. K_target
          // defaults to the active gl0 operator count (degenerate K=N — gate effectively a no-op except
          // to prove the wiring works). Env override `NAKAMOTO_COMMITTEE_K_TARGET` for genuine sortition
          // (e.g., K=4 on N=8).
          committeeKTarget = {
            val active = validatorPeers.size
            sys.env.get("NAKAMOTO_COMMITTEE_K_TARGET").flatMap(_.toIntOption).getOrElse(math.max(1, active))
          }
          // VRF SK/VK derived from the long-term keypair via the same `VrfKeyDeriver` path the leader
          // VRF uses. For v1 sortition uses the same VRF identity as leader election; per-operator-key
          // VRF keys land later (#180).
          committeeVrfKeys = {
            val rawPrivKey: Array[Byte] = keyPair.getPrivate match {
              case ecKey: java.security.interfaces.ECPrivateKey =>
                val bytes = ecKey.getS.toByteArray
                if (bytes.length > 32) bytes.drop(bytes.length - 32)
                else if (bytes.length < 32) Array.fill(32 - bytes.length)(0.toByte) ++ bytes
                else bytes
              case other =>
                other.getEncoded.takeRight(32)
            }
            val seed = io.constellationnetwork.security.vrf.VrfKeyDeriver.deriveVrfSeed(rawPrivKey)
            val vk = new io.constellationnetwork.security.vrf.EcVrf25519().getVerificationKey(seed)
            (seed, vk)
          }
          committeeGateLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger
            .getLoggerFromName[F]("MetagraphCommitteeGate")
            .pure[F]
            .toResource
          // KES adapter — sender signs at the operator's CURRENT KES tree-internal step (read via
          // `operationalKeyMaker.currentPeriod`) and embeds that step on the wire so the receiver can
          // verify non-interactively. `signAt` returns the encoded bytes; on signer failure (e.g.
          // `StepNotMonotonic` if the caller passed a stale step) returns empty bytes which the
          // receiver-side gate treats as "no KES sig" and rejects. The gate always passes
          // `currentPeriod` here so monotonic failure cannot happen in normal operation.
          committeeKesSigner = new io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSigner[F] {
            def currentPeriod: F[Int] = operationalKeyMaker.currentPeriod
            def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]] =
              operationalKeyMaker.signAt(kesStep, message).map {
                case Right(sig) => io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(sig)
                case Left(_)    => Array.empty[Byte]
              }
          }
          // KES verifier — uses `verifyAttestationByStep` so the tree-internal step comes straight off
          // the wire (`MetagraphAttestation.sender_tree_step`). No chain-state derivation, no
          // operator-offset lookup — receiver-side verification is non-interactive. Accept matrix:
          // empty/decode-fail/verify-fail → false (reject); no-registry-entry → true (Ed25519 already
          // authenticated; Slice 10 mid-life join carve-out).
          committeeKesVerifier = new io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesVerifier[F] {
            def verify(
              messageBytes: Array[Byte],
              kesSigBytes: Array[Byte],
              attesterId: io.constellationnetwork.schema.peer.PeerId,
              attesterHex: io.constellationnetwork.security.hex.Hex,
              kesStep: Int
            ): F[Boolean] =
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.KesGossipVerification
                .verifyAttestationByStep[F](
                  messageBytes = messageBytes,
                  kesSigBytes = kesSigBytes,
                  attesterId = attesterId,
                  attesterHex = attesterHex,
                  kesStep = kesStep,
                  kesRegistry = kesRegistry,
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
          // #201: resolve the metagraph parent ordinal from `(metagraphAddress, parentHash)` via the
          // gl0 GSI's `lastStateChannelSnapshotHashes` + `lastIncrementalCurrencySnapshots` partitions.
          // The previous shortcut (gl0 finalized ordinal) was asymmetric across peers (each peer has
          // its own `nakamotoFinalizedOrdinalRef` value at the same wallclock) and unrelated to the
          // metagraph's own snapshot progress, so the KES period peers derived disagreed and verifies
          // failed. The resolver reads through `pendingReader` so under MultiBranch we pick up the
          // chain's pending writes; #118's overlay-aware reader already handles the
          // pending-vs-finalized fallback.
          committeeParentOrdinalFor: (
            (
              io.constellationnetwork.schema.address.Address,
              io.constellationnetwork.security.hash.Hash
            ) => F[
              Option[Long]
            ]
          ) = (mg, parent) => {
            implicit val resolverLogger: org.typelevel.log4cats.Logger[F] = committeeGateLogger
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.resolve[F](pendingReader, mg, parent)
          }
          // #213/#290: ADMISSION-path parent-ordinal resolver. Identical to `committeeParentOrdinalFor`
          // EXCEPT it derives the ordinal from the incoming binary's own content (ordinal − 1) once the
          // GSI identity guard (`lastStateChannelSnapshotHashes[mg] == parentHash`) passes, instead of
          // reading gl0's currency partitions — which `calculateLastCurrencySnapshots` drops via
          // `.filterNot(_.isEmpty)` whenever the mg produced no state in the window, while still writing
          // `lastStateChannelSnapshotHashes[mg]`. That GSI gap made the legacy `resolve` return `None`
          // (tip-matched, both currency partitions empty) → the admission path fail-closed → the
          // metagraph tip froze and 0 incrementals were ever admitted at 8gl0+4mg. Wired ONLY into
          // `makeMetagraphBinaryProcessor` (the admission decision). The committee attestation gate
          // keeps using the GSI-only `committeeParentOrdinalFor` on BOTH its sender and receiver paths,
          // so the committee-VRF eta (and its cross-node determinism) is byte-for-byte unchanged.
          committeeParentOrdinalForBinary: (
            (
              io.constellationnetwork.schema.address.Address,
              io.constellationnetwork.security.hash.Hash,
              Array[Byte]
            ) => F[
              Option[Long]
            ]
          ) = (mg, parent, content) => {
            implicit val resolverLogger: org.typelevel.log4cats.Logger[F] = committeeGateLogger
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver
              .resolveFromBinary[F](pendingReader, mg, parent, content)
          }
          committeeGate = {
            implicit val gateLogger: org.typelevel.log4cats.Logger[F] = committeeGateLogger
            implicit val gateHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
            io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.make[F](
              selfPeerId = selfId,
              selfVrfSk = committeeVrfKeys._1,
              selfVrfVk = committeeVrfKeys._2,
              keyPair = keyPair,
              sortition = committeeSortition,
              aggregator = committeeAggregator,
              kesSigner = committeeKesSigner,
              kesVerifier = committeeKesVerifier,
              publisher = committeePublisher,
              kTarget = committeeKTarget,
              gateTimeoutMs = io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.DefaultGateTimeoutMs,
              pollIntervalMs = io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.DefaultPollIntervalMs,
              parentOrdinalFor = committeeParentOrdinalFor
            )
          }
          // Resolve eta for a metagraph-parent ordinal. For v1 we reuse the same logic the snapshot
          // production path uses — derive period from the parent ordinal, fold over per-period VRF
          // outputs from the chain store, fall back to `genesisEta` when no chain data yet. The
          // receiver-side gate VRF input MUST agree with the sender's input; this resolver is the
          // single source of truth on both sides.
          committeeEtaForOrdinal: (Long => F[Array[Byte]]) = (parentOrdinal: Long) => {
            val currentPeriod = io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
              .rotationPeriod(parentOrdinal, etaRotationSnapshots.toLong)
            if (currentPeriod <= 0) epochStateRef.get.map(_.genesisEta)
            else
              for {
                genesis <- epochStateRef.get.map(_.genesisEta)
                chainOutputs <- chainStore.vrfOutputsForPeriod(currentPeriod - 1, etaRotationSnapshots.toLong)
              } yield
                if (chainOutputs.nonEmpty)
                  io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
                    .computeEta(genesis, currentPeriod, chainOutputs.map(_._2))
                else genesis
          }

          _ <- nakLogger
            .info(
              s"🏛️ Committee gate constructed: kTarget=$committeeKTarget, validatorPeers=${validatorPeers.size}, K=N (sortition no-op until K_TARGET < N)"
            )
            .toResource
          // #214: orphan buffer + admission cache. Hoisted from `NakamotoSyncDaemon.run` so the
          // SnapshotLeaderLoop.onFinalize drain hook below and the daemon's gossip handler share
          // the same in-memory instance. The orphan-drain on finalize unsticks chains where the
          // local committee gate dropped a binary that nevertheless reached cluster finalization
          // via 2/3-attestation or depth-k.
          orphanBufferLogger <- org.typelevel.log4cats.slf4j.Slf4jLogger
            .fromName[F]("MetagraphOrphanBuffer")
            .toResource
          orphanBuffer <- io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer
            .make[F](orphanBufferLogger)
            .toResource
          // Gate-aware closure for processing a `(metagraphAddress, wireBytes)` pair. Built once
          // here so both call sites (daemon gossip handler and finalize-drain hook) share the
          // same orphan buffer + admission cache. See `NakamotoSyncDaemon.makeMetagraphBinaryProcessor`.
          processOrphanedMetagraphBinary = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.NakamotoSyncDaemon
            .makeMetagraphBinaryProcessor[F](
              processMetagraphBinary = processMetagraphBinary,
              committeeGate = committeeGate,
              // #213/#290: admission resolver derives the parent ordinal from the incoming binary's own
              // content (ordinal − 1), not the (possibly-dropped) gl0 currency partition. Committee gate
              // wiring (below + the receiver attestation handler) is unchanged — still GSI-only.
              parentOrdinalFor = committeeParentOrdinalForBinary,
              etaForParentOrdinal = committeeEtaForOrdinal,
              selfStake = stakeRegistry.committeeStake(selfId),
              orphanBuffer = orphanBuffer,
              logger = orphanBufferLogger
            )
          // §3 NIPoPoW S3 — dedicated tower store + finalizer for the Phase-3 sink in SnapshotLeaderLoop.
          // The tower lives in its own in-memory MPT producer (NOT the shared `mptStore`); proposal §4.5
          // explicitly forbids anchoring the tower root in headers, so its bytes never enter the global
          // `mptRoot` / `stateProof`. Each node maintains its own copy; corruption recovery is a chain
          // replay (re-run LevelTrialComputer.runAll over finalized snapshots), not a state-proof rollback.
          //
          // The same `LevelTrialComputer` (using the byte-deterministic Bifrost `Exp` interpreter already
          // built for L0 eligibility) drives both the per-snapshot trial computation and the finalizer's
          // gap derivation. `HasherSelector.getCurrent` is fine here because the tower partition is local
          // and doesn't participate in consensus-bytes hashing — same trick as `gateHasher` above.
          towerStore <- {
            implicit val towerHasher: io.constellationnetwork.security.Hasher[F] = HasherSelector[F].getCurrent
            io.constellationnetwork.node.shared.domain.nakamoto.nipopow.MptTowerStore.inMemory[F].toResource
          }
          levelTrialComputer = io.constellationnetwork.node.shared.domain.nakamoto.nipopow.LevelTrialComputer.make[F](exp)
          towerFinalizer <- io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerFinalizer
            .make[F](towerStore, levelTrialComputer, lddConfig.lddCutoff.toLong)
            .toResource
          // §3 NIPoPoW S5 — wire the proof builder + verifier and publish a `NipopowProofProvider` to the HTTP layer.
          //   - Builder reads the local tower store + global snapshot storage to assemble per-level chains + L0 suffix.
          //   - Verifier uses the same `(log1p, exp)` interpreters that drive the leader-loop's eligibility check, so any
          //     proof a remote builder produces from this node's chain is byte-identical-verifiable here.
          //   - Provider bakes in `(genesisEta, etaRotationSnapshots, lddConfig)` so the route doesn't need them.
          // Provider is published once at startup — the towerStore + verifier are immutable for the lifetime of the
          // process, so re-publishing under reorg is unnecessary.
          _ <- {
            val proofBuilder = io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerProofBuilder
              .make[F](towerStore, globalSnapshotStorage)
            val proofVerifier =
              io.constellationnetwork.node.shared.domain.nakamoto.nipopow.TowerVerifier.make[F](log1p, exp)
            val provider = io.constellationnetwork.node.shared.domain.nakamoto.nipopow.NipopowProofProvider.make[F](
              proofBuilder,
              proofVerifier,
              genesisEta = genesisEta,
              etaRotationSnapshots = etaRotationSnapshots.toLong,
              lddConfig = lddConfig
            )
            nipopowProofProviderRef.set(Some(provider))
          }.toResource

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

          // ─── Gap A — per-shard checkpoint producers ──────────────────────────────────────────
          // Build one `ShardCheckpointProducer` per shard the operator tracks, REUSING the SAME
          // per-shard `ShardChainStore` the acceptance side (`shardAcceptanceDeps.registry`) reads —
          // producer writes ⇒ consumer reads ⇒ the shard chain grows toward finality. `None` deps
          // (numShards <= 1, the production default) ⇒ empty map ⇒ the `SnapshotLeaderLoop` fan-out is
          // inert (regression bar). Every allocation below sits behind the `Some(deps)` gate.
          //
          // VRF seed: the SAME `deriveVrfKeys(keyPair)._1` the gl0 leader loop uses. v1 reuses the gl0
          // leader VRF identity for the shard slot lottery (per-operator-key VRF lands later, #180), so
          // the seed must be derived identically. KES adapter mirrors the gl0 `committeeKesSigner`
          // (signAt → `OperationalKeyMaker.encodeSignature`).
          //
          // Slice S4 — shardEta ROTATES per eta-period: the producer takes `shardEtaFor: EtaPeriod => F[Array[Byte]]`
          // and resolves it per `produce` call keyed on the CHECKPOINT'S own `epoch` (== rotationPeriod(gl0AnchorOrdinal)).
          // The per-period gl0 eta comes from `etaForPeriodCallback` (the SAME `EtaStateManager.getEta` resolver GSAM's
          // boundary writer uses); we convert the returned `Hash` to the 32 raw digest bytes (`Hex(h.value).toBytes` —
          // the byte shape `computeShardEta` requires, mirroring `ShardSlotLeader.computeShardEta`) and feed
          // `computeShardEta(shardId, gl0Eta)`. After the first gl0 eta rotation the shard-leader VRF domain now rotates
          // in lockstep instead of being pinned to genesis randomness. Determinism: `eta_epoch` is fixed at the 2/3-mark
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
                def currentPeriod: F[Int] = operationalKeyMaker.currentPeriod
                def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]] =
                  operationalKeyMaker.signAt(kesStep, message).map {
                    case Right(sig) => io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(sig)
                    case Left(_)    => Array.empty[Byte]
                  }
              }
              val shardVrfSk = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                .deriveVrfKeys(keyPair)
                ._1
              // Shard-local slot mapping (MUST be identical on producer + receiver so the leader VRF
              // verify agrees): the gl0 anchor ordinal IS the shard-local slot index. NOT gl0 wall-clock
              // slot — the shard chain advances at gl0-ordinal cadence, one anchor per gl0 ord.
              val slotForGl0Anchor: SnapshotOrdinal => io.constellationnetwork.schema.nakamoto.slot.Slot =
                (ord: SnapshotOrdinal) => io.constellationnetwork.schema.nakamoto.slot.Slot.unsafeApply(ord.value.value)
              // LDD slot-gap: genesis (no parent) → the slot itself (EligibilityChecker "first wins"
              // seed); else currentSlot - parentSlot clamped to ≥ 1 (mirrors the gl0 loop's clamp).
              val slotGapFor
                : (io.constellationnetwork.schema.nakamoto.slot.Slot, Option[io.constellationnetwork.schema.nakamoto.slot.Slot]) => Long =
                (cur, parentOpt) => parentOpt.fold(cur.value.value)(p => math.max(1L, cur.value.value - p.value.value))
              val sigmaInCommittee =
                io.constellationnetwork.numerics.Ratio(1, math.max(1, deps.shardingConfig.committeeKTarget))
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
                  // this shard's leader-VRF eta. Computed per `produce` call (keyed on `checkpoint.epoch`), NOT once at
                  // construction — so the shard VRF domain rotates in lockstep with gl0.
                  val shardEtaFor: io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Array[Byte]] =
                    (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
                      gl0EtaBytesForPeriod(epoch).flatMap(gl0Eta => shardSlotLeader.computeShardEta(shardId, gl0Eta))
                  io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer
                    .make[F](
                      shardId = shardId,
                      chainStore = entry.chainStore,
                      slotLeader = shardSlotLeader,
                      publisher = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointPublisher
                        .sidecar[F](sidecarClient),
                      selfPeerId = selfId,
                      selfKeyPair = keyPair,
                      selfVrfSk = shardVrfSk,
                      kesSigner = shardKesSigner,
                      shardEtaFor = shardEtaFor,
                      sigmaInCommittee = sigmaInCommittee,
                      slotForGl0Anchor = slotForGl0Anchor,
                      slotGapFor = slotGapFor,
                      lddConfig = lddConfig,
                      // S3: the SAME committee re-execution closure the verifier uses (built from the shared
                      // `shardScEventsProcessor`), so the producer's `perMetagraphMptRoots` are recomputed
                      // byte-identically by every verifier's `reExecuteDerivation`.
                      derivePerMgState = io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
                        .reExecDerivation[F](shardScEventsProcessor)(Async[F], shardHasher)
                    )
                    .map(shardId -> _)
              }
                .map(_.toMap)
                .flatTap(m => nakLogger.info(s"🧩 Shard producers built: ${m.size} shard(s) (numShards>1 active)"))
                .toResource
          }

          // ─── T_count_shard quorum closure — receiver-side attestation emitter ──────────────────
          // Built alongside the producers, reusing the SAME signing material (selfId, keyPair, KES adapter,
          // shard VRF sk) + the SAME per-shard leader-VRF etas + slot mappings. When a received checkpoint
          // becomes a node's best tip, `NakamotoSyncDaemon.handleShardCheckpoint` invokes this emitter to sign +
          // gossip a `ShardCheckpointAttestation` — the missing seam that lets every OTHER node's `ShardTipTracker`
          // cross `⌈2·K_S/3⌉` so `T_count_shard` fires (the producer's lone self-excluded signature can't).
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
                def currentPeriod: F[Int] = operationalKeyMaker.currentPeriod
                def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]] =
                  operationalKeyMaker.signAt(kesStep, message).map {
                    case Right(sig) => io.constellationnetwork.security.kes.OperationalKeyMaker.encodeSignature(sig)
                    case Left(_)    => Array.empty[Byte]
                  }
              }
              val shardVrfSk = io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.SnapshotLeaderLoop
                .deriveVrfKeys(keyPair)
                ._1
              // MUST be byte-identical to the producer's mappings (above) so the attester's VRF message + slot-gap
              // match the producer's. The gl0 anchor ordinal IS the shard-local slot index.
              val slotForGl0Anchor: SnapshotOrdinal => io.constellationnetwork.schema.nakamoto.slot.Slot =
                (ord: SnapshotOrdinal) => io.constellationnetwork.schema.nakamoto.slot.Slot.unsafeApply(ord.value.value)
              val slotGapFor
                : (io.constellationnetwork.schema.nakamoto.slot.Slot, Option[io.constellationnetwork.schema.nakamoto.slot.Slot]) => Long =
                (cur, parentOpt) => parentOpt.fold(cur.value.value)(p => math.max(1L, cur.value.value - p.value.value))
              val sigmaInCommittee =
                io.constellationnetwork.numerics.Ratio(1, math.max(1, deps.shardingConfig.committeeKTarget))
              // Slice S4: epoch-aware per-shard leader-VRF eta resolver — MUST match the producer's `shardEtaFor`
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
                      selfVrfSk = shardVrfSk,
                      kesSigner = shardKesSigner,
                      eligibilityChecker = eligibilityChecker,
                      sidecarClient = sidecarClient,
                      tipTrackerFor = tipTrackerFor,
                      shardEtaFor = shardEtaFor,
                      sigmaInCommittee = sigmaInCommittee,
                      slotForGl0Anchor = slotForGl0Anchor,
                      slotGapFor = slotGapFor,
                      lddConfig = lddConfig
                    )
                  ): Option[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]]
                )
                .flatTap(_ => nakLogger.info("🧩 Shard checkpoint attestation emitter built (numShards>1 active)"))
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
                  nodeStorage = nodeStorage,
                  keyPair = keyPair,
                  selfId = selfId,
                  lddConfig = lddConfig,
                  eligibilityChecker = eligibilityChecker,
                  slotsPerEpoch = slotsPerEpoch,
                  etaRotationSnapshots = etaRotationSnapshots,
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
                  // than the latest-produced one. Updated monotonically at both finalize sinks.
                  latestFinalizedSliceSourceRef = latestFinalizedSliceSourceRef,
                  // #287 "send diffs": the bounded recent-projection ring the loop also fills at both finalize
                  // sinks, read by `GlobalFollowSliceService.sliceSince` to serve incremental gl1 follow diffs.
                  recentFollowProjectionsRef = recentFollowProjectionsRef,
                  // Task #12 slice 2b: the SAME staging map the consensus functions fill (above), read here at
                  // both finalize sinks to promote the finalized snapshot's accumulator into the served ring.
                  pendingAccumulatorsRef = pendingAccumulatorsRef,
                  // Task #12 slice 2b: the served changeset ring the loop fills at both finalize sinks; a later
                  // slice wires `GlobalChangeSetService.make(recentFinalizedAccumulatorsRef.get)` to serve it.
                  recentFinalizedAccumulatorsRef = recentFinalizedAccumulatorsRef,
                  chainSyncRequestQueue = chainSyncRequestQueue,
                  finalityTriggerViewRef = finalityTriggerViewRef,
                  // §1.2 Slice 5/6: parallel-sign attestations + snapshots with KES.
                  operationalKeyMaker = operationalKeyMaker,
                  // Slice S3: drop aggregator entries for `(mg, parent)` pairs in the finalized
                  // snapshot's `stateChannelSnapshots`. Otherwise the tally grows monotonically.
                  //
                  // #214 piggyback: drain any orphan-buffered children waiting on each binary's
                  // value-hash. Cluster-wide finalization (2/3 peer attestation OR depth-k) is
                  // authoritative; if the local gate dropped the parent at `count < kTarget`,
                  // the just-finalized snapshot proves the cluster admitted it anyway. Re-feed
                  // each drained child through the SAME gate-aware processor used for fresh
                  // gossip — `processOrphanedMetagraphBinary` — NOT direct `processMetagraphBinary`.
                  // The earlier attempt (ad7d4f041, reverted in fea66fc7d) bypassed the gate and
                  // polluted the state-channel queue with locally-unverified binaries.
                  //
                  // After finalize gl0's GSI contains the just-finalized binary's value-hash, so
                  // `resolveParent` inside the processor will resolve via `parentOrdinalFor` and
                  // the child enters `attestAndAdmit` normally — accumulating peer attestations
                  // already arrived during the GSI-lag window, plus the local self-attestation.
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
                  // §3 NIPoPoW S3: Phase-3 sink for the local TowerStore. Constructed above with
                  // a dedicated MPT producer (NOT in the consensus stateProof). Invoked at T_depth2
                  // archival watermark advance in SnapshotLeaderLoop.finalityMonitor.
                  towerFinalizer = towerFinalizer,
                  // Gap A — per-shard checkpoint producers + the SAME chain stores they write into + the
                  // per-shard raw-binary buffers (EXECUTION-SHARDING R-1, the producer fan-out input) + the
                  // static metagraph→shard assignment. Empty / None at numShards=1 (regression bar); the
                  // fan-out in `onSlotWon` is a no-op `traverse_` over the empty map. `shardChainStores` and
                  // `shardBinaryBuffers` are projected off the SAME `shardAcceptanceDeps.registry` entries the
                  // acceptance side + the daemon's intake share, so producer writes ⇒ consumer reads ⇒ chain
                  // grows, and the daemon buffers raw binaries ⇒ the fan-out reads them.
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
          // Period derivation matches `EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots)`
          // (snapshot-indexed, not slot-indexed — see attestation-and-finality.md §1). Aligns with
          // [[project_consensus_epoch_staggering]]'s "KES periods aligned with eta cadence" rule.
          //
          // Skipped entirely on the in-memory fallback path (no CL_KES_SECURE_STORE_DIR) — the
          // evolution would write to nowhere persistent and just waste cycles. Disk path is the
          // only one where the proactive evolution materially shrinks the exposure window.
          lastEvolvedKesPeriodRef <- Ref.of[F, Int](0).toResource
          _ <- supervisor
            .supervise(
              fs2.Stream
                .awakeEvery[F](scala.concurrent.duration.DurationInt(2).seconds)
                .evalMap { _ =>
                  for {
                    finalizedOrd <- nakamotoFinalizedOrdinalRef.get
                    currentPeriod = io.constellationnetwork.node.shared.domain.nakamoto.EtaCalculation
                      .rotationPeriod(finalizedOrd.value.value, etaRotationSnapshots.toLong)
                      .toInt
                    lastEvolved <- lastEvolvedKesPeriodRef.get
                    _ <-
                      if (currentPeriod > lastEvolved)
                        operationalKeyMaker.evolveTo(currentPeriod).flatMap {
                          case Right(_) =>
                            lastEvolvedKesPeriodRef.set(currentPeriod) >>
                              nakLogger.info(
                                s"🔐 KES period rotated: $lastEvolved → $currentPeriod (finalizedOrd=${finalizedOrd.value.value}, etaRotationSnapshots=$etaRotationSnapshots)"
                              ) >>
                              Metrics[F].incrementCounter("dag_nakamoto_kes_period_rotations_total") >>
                              Metrics[F].updateGauge("dag_nakamoto_kes_current_period", currentPeriod.toLong)
                          case Left(err) =>
                            nakLogger.warn(s"⚠️ KES evolveTo($currentPeriod) failed: $err")
                        }
                      else cats.Applicative[F].unit
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
            .make[F](chainStore, globalSnapshotStorage, chainSyncDispatcher)(
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
                  sidecarClient = sidecarClient,
                  selfId = selfId,
                  keyPair = keyPair,
                  lddConfig = lddConfig,
                  eligibilityChecker = eligibilityChecker,
                  lastKnownSlotRef = lastKnownSlotRef,
                  epochStateRef = epochStateRef,
                  etaRotationSnapshots = etaRotationSnapshots,
                  consensusFns = consensusFunctions,
                  snapshotStorage = globalSnapshotStorage,
                  lastGlobalSnapshotStorage = lastGlobalSnapshotStorage,
                  lastNGlobalSnapshotStorage = lastNGlobalSnapshotStorage,
                  snapshotSemaphore = snapshotSemaphore,
                  productionGate = productionGate,
                  mptStore = mptStore,
                  mptOverlay = mptOverlay,
                  eventMempool = eventMempool,
                  dataDir = java.nio.file.Paths.get(sys.env.getOrElse("TESSELLATION_DATA_DIR", "/tessellation/data")),
                  enqueueAllowSpendBlock = enqueueAllowSpendBlock,
                  enqueueDAGBlock = enqueueDAGBlock,
                  enqueueTokenLockBlock = enqueueTokenLockBlock,
                  sharedChainSyncManagerRef = sharedChainSyncManagerRef,
                  // §1.2 Slice 5/6/9: KES sender-side signing + receiver-side load-bearing verify.
                  // Always-on; no env flag — verification failures drop the message.
                  operationalKeyMaker = operationalKeyMaker,
                  kesRegistry = kesRegistry,
                  // Slice S3: gate metagraph binaries on committee threshold + verify received
                  // committee attestations against the aggregator. The daemon needs both an ordinal
                  // resolver (#201) and an ordinal-to-eta function (#202) — handlers chain them so
                  // the eta passed to the gate is byte-equivalent to the sender's eta.
                  committeeGate = committeeGate,
                  parentOrdinalFor = committeeParentOrdinalFor,
                  etaForParentOrdinal = committeeEtaForOrdinal,
                  senderStakeLookup = (peer: io.constellationnetwork.schema.peer.PeerId) => stakeRegistry.committeeStake(peer),
                  processOrphanedMetagraphBinary = processOrphanedMetagraphBinary,
                  // Gap B — acceptance-side shard deps for the receiver-routing handlers. `None` at
                  // numShards=1 (regression bar) ⇒ inbound shard-checkpoint gossip is dropped with a
                  // debug log. The SAME `shardAcceptanceDeps` instance the GSAM acceptance side + the
                  // Gap-A producers use, so the chain stores incoming checkpoints land in are the ones
                  // the producers + finality triggers read.
                  shardAcceptanceDeps = shardAcceptanceDeps,
                  // T_count_shard quorum closure: on best-tip receipt, sign + gossip our own attestation so
                  // peers cross ⌈2·K_S/3⌉. `None` at numShards=1 (regression bar) ⇒ no emit.
                  shardCheckpointAttestationEmitter = shardCheckpointAttestationEmitter,
                  // Per-ord producer fan-out (decoupled from gl0-leader win): EVERY node fans out shard
                  // checkpoints for each canonical (best-tip) gl0 ord it receives via gossip. Reuses the
                  // SAME `shardProducers` the leader loop uses + the `shardAssignment` off `shardAcceptanceDeps`.
                  // The daemon derives `shardChainStores` in-daemon from `shardAcceptanceDeps.registry`. EMPTY /
                  // `None` at numShards=1 (regression bar) ⇒ the per-ord hook is `whenA(false)`.
                  shardProducers = shardProducers,
                  shardAssignment = shardAcceptanceDeps.map(_.shardAssignment)
                )
                .compile
                .drain
            )
            .toResource

          // P-11 (#141): re-bootstrap orchestrator. Periodic ticker that detects when this
          // node has self-finalized a divergent fork and got locked out of canonical recovery
          // (the "fork-recovery deadlock"). Gated by `NAKAMOTO_REBOOTSTRAP_ENABLED` (default
          // FALSE) — production deployments turn it on per-node once iter e2e validates no
          // spurious fires. Disabled: emits a single INFO at startup and otherwise no-op.
          _ <- supervisor
            .supervise(
              io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.RebootstrapOrchestrator
                .run[F](
                  chainStore = chainStore,
                  tipTracker = tipTracker,
                  mptOverlay = mptOverlay,
                  productionGate = productionGate
                )
                .compile
                .drain
            )
            .toResource

          // Check for persisted backfill cursor from a previous session (crash recovery).
          // If found, resume backfill with production paused until it completes.
          backfillDataDir = java.nio.file.Paths.get(sys.env.getOrElse("TESSELLATION_DATA_DIR", "/tessellation/data"))
          existingCursor <- io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.BackfillDaemon
            .loadCursor[F](backfillDataDir)
            .toResource
          _ <- (existingCursor match {
            case Some(cursor) =>
              nakLogger.info(
                s"🔄 Resuming backfill from crash: ordinal ${cursor.currentOrdinal} → ${cursor.targetOrdinal} " +
                  s"(started at ${cursor.startedAtOrdinal})"
              ) >>
                supervisor
                  .supervise(
                    io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.BackfillDaemon
                      .run[F](cursor, sidecarClient.channel, globalSnapshotStorage, productionGate, backfillDataDir)
                      .handleErrorWith(e => nakLogger.warn(s"Backfill daemon failed on resume: ${e.getMessage}"))
                  )
                  .void
            case None =>
              Async[F].unit
          }).toResource
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
