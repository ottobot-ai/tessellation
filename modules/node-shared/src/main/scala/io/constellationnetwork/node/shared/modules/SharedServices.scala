package io.constellationnetwork.node.shared.modules

import java.security.KeyPair

import cats.Parallel
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Async
import cats.effect.std.Supervisor
import cats.syntax.all._

import io.constellationnetwork.domain.allowance_list.AllowanceListEntry
import io.constellationnetwork.domain.seedlist.SeedlistEntry
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.cli.CliMethod
import io.constellationnetwork.node.shared.config.DefaultDelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.config.types.{CollateralConfig, SharedConfig}
import io.constellationnetwork.node.shared.domain.cluster.services.{Cluster, Session}
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceManager
import io.constellationnetwork.node.shared.domain.gossip.Gossip
import io.constellationnetwork.node.shared.domain.healthcheck.LocalHealthcheck
import io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.EtaSourceRange
import io.constellationnetwork.node.shared.domain.node.UpdateNodeParametersAcceptanceManager
import io.constellationnetwork.node.shared.domain.nodeCollateral.UpdateNodeCollateralAcceptanceManager
import io.constellationnetwork.node.shared.domain.priceOracle.PriceStateUpdater
import io.constellationnetwork.node.shared.domain.statechannel.FeeCalculator
import io.constellationnetwork.node.shared.domain.swap.block.AllowSpendBlockAcceptanceManager
import io.constellationnetwork.node.shared.domain.tokenlock.block.TokenLockBlockAcceptanceManager
import io.constellationnetwork.node.shared.http.p2p.clients.NodeClient
import io.constellationnetwork.node.shared.infrastructure.block.processing.BlockAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.cluster.services.Cluster
import io.constellationnetwork.node.shared.infrastructure.gossip.{Gossip => GossipImpl}
import io.constellationnetwork.node.shared.infrastructure.healthcheck.LocalHealthcheck
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.node.RestartService
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring
import io.constellationnetwork.node.shared.infrastructure.snapshot._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency.CurrencySnapshotAcceptanceManager
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  GlobalSnapshotStateChannelAcceptanceManager,
  GlobalSnapshotStateChannelEventsProcessor
}
import io.constellationnetwork.node.shared.logger.LoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.generation.Generation
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, HasherSelector, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import fs2.concurrent.SignallingRef

object SharedServices {

  /** Default 32-byte eta seed. Periods 0/1 derive their explicit bootstrap from it; periods N >= 2 use it only together with a
    * proven-complete N-1 VRF-output range. Missing history never substitutes this seed. Mirrors the per-cluster constant the dag-l0
    * `GlobalSnapshotConsensus.nakamotoGenesisEta` helper computes.
    *
    * Constructed lazily (not via `val`) because the Blake2b digest is a stateful instance; building one per call avoids accidental cross-
    * call mutation.
    */
  def DefaultNakamotoGenesisEta: Array[Byte] = {
    val seed = "tessellation-nakamoto-genesis-eta-v1".getBytes(java.nio.charset.StandardCharsets.UTF_8)
    val digest = new org.bouncycastle.crypto.digests.Blake2bDigest(256)
    digest.update(seed, 0, seed.length)
    val out = new Array[Byte](32)
    digest.doFinal(out, 0)
    out
  }

  /** Default unavailable eta source for layers without a GL0 chain store. This is deliberately `Incomplete`, not an empty successful range:
    * periods N >= 2 must defer unless a rooted MPT eta or a proven-complete N-1 ancestry range is available.
    */
  def noopEtaChainWalk[F[_]: Async]: (Long, Option[Hash]) => F[EtaSourceRange] =
    (_: Long, _: Option[Hash]) => Async[F].pure(EtaSourceRange.Incomplete(Nil))

  /** Path 1 (heap-leak workstream): hex-encode a 32-byte eta into the [[Hash]] shape the [[GlobalSnapshotAcceptanceManager]] boundary
    * writer stores in `HistoricalStakeSnapshot.eta`. Mirrors the inverse decode in
    * [[io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager.make]] (`entry.eta.value.grouped(2)…`); the round-trip is
    * byte-identical so the MPT entries are deterministic across nodes.
    *
    * Duplicate of `GlobalSnapshotConsensus.etaBytesToHash` (in dag-l0) — the two helpers exist to avoid a node-shared → dag-l0 dependency
    * inversion; both produce identical hex bytes for any 32-byte input.
    */
  def etaBytesToHash(bytes: Array[Byte]): Hash =
    Hash(bytes.map(b => f"$b%02x").mkString)

  /** Build the `EtaPeriod => F[Hash]` callback used by [[GlobalSnapshotAcceptanceManager]] from an
    * [[io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager]]. Routes the manager's `Array[Byte]` output through
    * [[etaBytesToHash]] under `HasherSelector.withCurrent` (so the MPT cache lookup uses the same hasher the boundary writer commits with).
    */
  def etaForPeriodCallback[F[_]: Async: HasherSelector](
    mgr: io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager[F]
  ): io.constellationnetwork.schema.nakamoto.EtaPeriod => F[Hash] =
    (period: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
      HasherSelector[F].withCurrent { implicit hasher =>
        mgr.getEta(period.value).map(etaBytesToHash)
      }

  def etaForPeriodAtParentCallback[F[_]: Async: HasherSelector](
    mgr: io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager[F]
  ): (io.constellationnetwork.schema.nakamoto.EtaPeriod, io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId) => F[Hash] =
    (period, parent) =>
      HasherSelector[F].withCurrent { implicit hasher =>
        mgr.getEtaAt(period.value, parent.value).map(etaBytesToHash)
      }

  def make[F[_]: Async: Parallel: HasherSelector: SecurityProvider: Metrics: Supervisor: JsonSerializer: KryoSerializer, A <: CliMethod](
    cfg: SharedConfig,
    nodeId: PeerId,
    generation: Generation,
    keyPair: KeyPair,
    storages: SharedStorages[F],
    queues: SharedQueues[F],
    session: Session[F],
    nodeClient: NodeClient[F],
    validators: SharedValidators[F],
    seedlist: Option[Set[SeedlistEntry]],
    restartSignal: SignallingRef[F, Option[A]],
    versionHash: Hash,
    metagraphVersionHash: Hash,
    jarHash: Hash,
    collateral: CollateralConfig,
    stateChannelAllowanceLists: Option[Map[Address, NonEmptySet[PeerId]]],
    environment: AppEnvironment,
    txHasher: Hasher[F],
    allowanceList: Option[Set[AllowanceListEntry]],
    metagraphId: Option[Address],
    loggerBundle: LoggerBundle[F],
    // Path 1 (heap-leak workstream): genesis eta + chain-walk fallback for the [[EtaStateManager]]
    // that backs the GSAM boundary-write callback. Default = a constant Blake2b-domain-string bootstrap
    // and an unavailable range. Layers without a local GL0 chain store must read a rooted/pinned eta; they
    // cannot synthesize an N >= 2 eta from missing history. gl0 callers override the callback with an exact chain range
    // through a Ref (the chain store is built later in the Resource graph, so callers pass a closure that
    // reads from a `Ref[Option[NakamotoChainStoreAlgebra[F]]]`). See `GlobalSnapshotConsensus.make` for the
    // dag-l0 GSAM construction which also uses the same `EtaStateManager.make` shape.
    nakamotoGenesisEta: Array[Byte] = SharedServices.DefaultNakamotoGenesisEta,
    nakamotoEtaChainWalkFallback: Option[(Long, Option[Hash]) => F[EtaSourceRange]] = None,
    // Split-safety (#261): the genesis-derived atomic KES+VRF registry the createContext / follower GSAM uses to verify embedded shard
    // checkpoints IDENTICALLY to the gl0 produce + validateArtifact paths. With real VRF-VK committee sortition + KES, an empty registry
    // here would draw a DIFFERENT committee than the leader (and skip KES verify), so a follower could ADOPT a checkpoint the leader
    // REJECTED → StateProofMismatch split. The sole caller `TessellationIOApp.make` supplies these via the overridable
    // `nakamotoOperatorKeyRegistries` hook: gl0's `Main` loads the real genesis registry; layers that don't run shard-committee acceptance
    // (cl0/cl1/dl1/gl1) pass empty — byte-identical to before, since those layers don't activate shard-committee acceptance.
    operatorKeyRegistry: io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    currencyStateProofSelector: CurrencyStateProofSelector
  ): F[SharedServices[F, A]] =
    for {
      restartService <- RestartService.make(restartSignal, storages.cluster)

      cluster = Cluster
        .make[F](
          cfg.leavingDelay,
          cfg.http,
          nodeId,
          keyPair,
          storages.cluster,
          storages.session,
          storages.node,
          seedlist,
          restartService,
          versionHash,
          metagraphVersionHash,
          jarHash,
          environment,
          allowanceList,
          metagraphId
        )

      localHealthcheck <- LocalHealthcheck.make[F](nodeClient, storages.cluster)
      gossip <- HasherSelector[F].withCurrent(implicit hasher => GossipImpl.make[F](queues.rumor, nodeId, generation, keyPair))
      // Track-1 blocker-2a / I-PIN (Step-1): a READ-ONLY per-ordinal state-bytes store over this node's `mpt_snapshot_info` global-state
      // files (the same the finalized-base producer persists), plus the version-retained BY-ORDINAL per-MG pinned reader over it. SHARED by
      // the CSAM below (currency acceptance re-exec/validate path) and the GSAM `createContext` rail further down, so both read each
      // metagraph's pinned global prior through one instance. RETENTION CAVEAT: this store prunes with `LogarithmicOrdinalCutoff` (sparse,
      // gappy below the head) so by-ordinal reads at an arbitrary past ordinal hard-reject unless on the ladder (surfaced — a
      // contiguous/disk-backed follower store is a later slice). On the currency-l0 producer rail this store holds the metagraph's currency
      // state (not global), but CSAM reads through it ONLY on the VALIDATOR path (`pinnedGlobalSyncView` set); the producer path keeps its
      // head read, so the mismatched rail is never consulted for global sync data on produce.
      pinnedByteStore <- io.constellationnetwork.security.mpt.storages.MptStateStorage.make[F](cfg.mptSnapshotInfoPath)
      sharedPinnedCurrencyInfoReader = {
        implicit val h: Hasher[F] = HasherSelector[F].getCurrent
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.PinnedCurrencyInfoReader
          .make[F](pinnedByteStore, ord => storages.lastNGlobalSnapshot.getByOrdinal(ord))
      }
      currencySnapshotAcceptanceManager <- CurrencySnapshotAcceptanceManager.make(
        cfg.fieldsAddedOrdinals,
        cfg.environment,
        cfg.lastGlobalSnapshotsSync,
        BlockAcceptanceManager.make[F](validators.currencyBlockValidator, txHasher),
        TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
        AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
        collateral.amount,
        validators.currencyMessageValidator,
        validators.feeTransactionValidator,
        validators.globalSnapshotSyncValidator,
        storages.lastNGlobalSnapshot,
        storages.lastGlobalSnapshot,
        // Track-1 I-PIN (Step-1): on the VALIDATOR / re-exec path (`pinnedGlobalSyncView` set) `accept` reads each metagraph's committed
        // cross-shard sync data at the RECORDED `globalSyncView` through this reader instead of the node-local head. numShards=1: the
        // metagraph has no fieldId-18 entry ⇒ the read is `None` ⇒ folds nothing ⇒ byte-identical to the pre-I-PIN head read.
        pinnedCurrencyInfoReader = Some(sharedPinnedCurrencyInfoReader)
      )

      currencyEventsCutter = CurrencyEventsCutter.make[F](None)

      currencySnapshotValidator = CurrencySnapshotValidator.make[F](
        CurrencySnapshotCreator.make[F](
          cfg.fieldsAddedOrdinals.tessellation3Migration.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
          currencySnapshotAcceptanceManager,
          None,
          cfg.snapshotSize,
          currencyEventsCutter,
          storages.currencySnapshotEventValidationError
        ),
        validators.signedValidator,
        None,
        None
      )
      currencySnapshotContextFns = CurrencySnapshotContextFunctions.make(
        currencySnapshotValidator
      )
      feeCalculator = FeeCalculator.make(cfg.feeConfigs)
      globalSnapshotStateChannelManager <- GlobalSnapshotStateChannelAcceptanceManager.make(stateChannelAllowanceLists)
      updateNodeParametersAcceptanceManager = UpdateNodeParametersAcceptanceManager.make(validators.updateNodeParametersValidator)
      updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make(
        validators.updateDelegatedStakeValidator
      )
      updateNodeCollateralAcceptanceManager = UpdateNodeCollateralAcceptanceManager.make(
        validators.updateNodeCollateralValidator
      )
      priceStateUpdater = PriceStateUpdater.make[F](
        cfg.environment,
        DefaultDelegatedRewardsConfigProvider,
        io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(storages.mptStore)
      )
      // Path 1 (heap-leak workstream): construct the [[EtaStateManager]] backing the GSAM `etaForPeriod` callback.
      //
      // The manager wraps an MPT-cache point read (`HistoricalStakeReader.lookup(period)`) + a caller-supplied chain-walk fallback.
      // On the SharedServices side both the MPT cache and chain-range callback are layer-agnostic. GL0 installs an exact chain callback;
      // downstream layers use their rooted/pinned GL0 eta. If neither exists, periods N >= 2 defer instead of inventing an eta.
      //
      // Wiring through a fresh MPT-only `GlobalStateReader.fromMptStore(storages.mptStore)` matches the existing
      // `HistoricalStakeReader` usage in `GlobalSnapshotConsensus.make` — both producer and reader observe the same per-key bytes that
      // `AcceptanceMptStateChanges.applyStateChanges` writes inside `accept()`.
      sharedHistoricalStakeReader = io.constellationnetwork.node.shared.domain.nakamoto.HistoricalStakeReader
        .make[F](io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore[F](storages.mptStore))
      etaStateManager <- io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager
        .make[F](
          genesisEta = nakamotoGenesisEta,
          historicalStakeReader = sharedHistoricalStakeReader,
          chainWalkFallback = nakamotoEtaChainWalkFallback.getOrElse(SharedServices.noopEtaChainWalk[F])
        )
      sharedEtaForPeriod = SharedServices.etaForPeriodCallback[F](etaStateManager)
      sharedEtaForPeriodAtParent = SharedServices.etaForPeriodAtParentCallback[F](etaStateManager)
      // Track-3 S4: wire the overlay's base-revert hook to drop the eta walk cache on every base revert
      // (`MptOverlay.finalizeBranch` reorg-replace arms + `MptOverlay.revertToOrdinal`). After the drop,
      // `getEta` re-derives over the now-canonical chain via the MPT-lookup → chain-walk fallback
      // (bootstrap-equivalence) — there is no parallel eta-revert.
      _ <- storages.setOnBaseRevert(etaStateManager.forgetUncommitted)
      // Hierarchical-shard-checkpoints v1 — ACCEPTANCE-side production wiring (priority 1). Gated on
      // `cfg.nakamoto.sharding.numShards > 1`. At the production default `numShards = 1` this returns
      // `None` (constructs nothing) and the GSAM call below passes `None` for all three sharding params —
      // byte-identical to the pre-wiring call (the regression bar). See `ShardCheckpointWiring` scaladoc.
      //
      // The SharedServices GSAM is the verify/follower path (cl0/cl1/dl1/gl1 + gl0 follower). Activated sharding receives the same
      // genesis-loaded atomic KES+VRF registry as the GL0 producer path; missing registered keys fail checkpoint verification closed.
      // The active-validator set is the seedlist minus `metagraph-op` aliases, falling back to `{nodeId}`.
      // S3 committee re-execution: the SAME `GlobalSnapshotStateChannelEventsProcessor` this (verify/follower) GSAM
      // uses is built once and shared by the shard verifier's `reExecuteDerivation`, so the verifier re-runs the
      // identical pinned-base currency derivation a producer used — the byte-identity contract that prevents false slashing.
      shardScEventsProcessor = GlobalSnapshotStateChannelEventsProcessor
        .make[F](
          validators.stateChannelValidator,
          globalSnapshotStateChannelManager,
          currencySnapshotContextFns,
          feeCalculator,
          io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader.fromMptStore(storages.mptStore)
        )
      shardAcceptanceDeps <- ShardCheckpointWiring.acceptanceDeps[F](
        cfg = cfg.nakamoto.sharding,
        etaRotationSnapshots = cfg.nakamoto.etaRotationSnapshots(cfg.environment).value,
        // Draw/quorum decouple — cluster-uniform `nakamoto.committee` (shared with the per-metagraph gate). MUST match the gl0
        // produce path's params (split-safety #261): kDraw sizes the committee DRAW, kQuorum is the admit count `verifyEmbedded` needs.
        kDraw = cfg.nakamoto.committee.kDraw,
        kQuorum = cfg.nakamoto.committee.kQuorum,
        selfPeerId = nodeId,
        // Split-safety (#261): the createContext / follower GSAM MUST use the SAME genesis-derived atomic KES+VRF registry the gl0
        // produce path uses, or `verifyEmbedded` draws a different committee (VRF) / skips KES verify and the adopt decision
        // diverges → StateProofMismatch split. Passed in via `TessellationIOApp.nakamotoOperatorKeyRegistries` (gl0 loads the real ones;
        // other layers + tests get empty — identical to before, since those layers don't activate shard-committee acceptance).
        operatorKeyRegistry = operatorKeyRegistry,
        activeValidators = Async[F].pure(
          seedlist
            .map(_.collect { case e if !e.alias.exists(_.value.value == "metagraph-op") => e.peerId })
            .getOrElse(Set(nodeId))
        ),
        // Per-period eta for the committee draw — the SAME `EtaStateManager.getEta` resolver (`sharedEtaForPeriod`) the GSAM
        // boundary writer uses, decoded to 32 raw bytes. MPT-committed ⇒ byte-identical to the gl0 produce path's `etaForEpoch`.
        etaForEpoch = (epoch: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
          sharedEtaForPeriod(epoch).map(h => io.constellationnetwork.security.hex.Hex(h.value).toBytes),
        // STEP 6: the unconditional GL0 replay (`ShardCheckpointGl0AcceptanceManager.reExecPath`) byte-compares the recomputed
        // per-MG root against the committee-attested `perMetagraphMptRoots(mg)`, which is now the PIN-1 COMPONENT-ADDRESSABLE
        // `GlobalStateConverter.currencySnapshotMgRoot` (the MG-sub-trie rootHash, not a flat or state-presence-sensitive hash). The verifier must produce the same root;
        // `reExecDerivationAtPinnedBase` is exactly that root: it seeds the derivation from this node's adopted S(N) via the finalized
        // `GlobalStateReader`, identical to the producer's. Same shared processor as GSAM means producer and verifier roots are byte-identical.
        reExecuteDerivation = Some {
          implicit val h: Hasher[F] = HasherSelector[F].getCurrent
          // Track-1 execution-base-pin (FINDING-B1): resolve the derivation prior AT the wire-carried, committee-signed `executionBaseOrdinal` —
          // NEVER at this node's live base when the two differ. The re-derived per-MG root is base-DEPENDENT
          // (full recreation starts from cumulative balances/refs/active-sets and messages in the seed prior), so a live read on a node
          // whose finalized tip ≠ the checkpoint's base recomputes a DIFFERENT root for the SAME
          // honest checkpoint → false `RejectedReExecutionMismatch` (a durable 100% slash of the whole committee) + adopt-decision split.
          // Same shared recipe the gl0 produce/watchtower rail uses (`GlobalSnapshotConsensus.finalizedReaderAt`): ALWAYS the
          // version-retained, root-verified pinned reader over this node's `mpt_snapshot_info` byte store
          // (`sharedPinnedCurrencyInfoReader`) — NO live fast path (`lastPersistedOrdinal == ord` does not pin the live store's
          // CONTENT to state@ord; see `ShardCheckpointWiring.pinnedPriorReaderAt` for the 2026-07-08 mid-fold-skew wedge).
          // RETENTION CAVEAT: that store prunes logarithmically (sparse below the head), so an anchor can miss — the pin then FAILS
          // CLOSED (`None` ⇒ OMIT ⇒ `Hash.empty`, which `reExecPath` treats as "can't check": plain reject, NO slash) rather than
          // substituting a live base.
          val pinnedReaderAt =
            ShardCheckpointWiring.pinnedPriorReaderAt[F](sharedPinnedCurrencyInfoReader)
          val reExec =
            ShardCheckpointWiring.reExecDerivationAtPinnedBase[F](
              shardScEventsProcessor,
              pinnedReaderAt,
              storages.lastNGlobalSnapshot.getByOrdinal
            )(
              Async[F],
              Parallel[F],
              h,
              implicitly[JsonSerializer[F]],
              globalStateProofSelector
            )
          (
            mg: Address,
            binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
            anchor: SnapshotOrdinal,
            executionBaseOrdinal: SnapshotOrdinal
          ) =>
            // OMIT-ON-CAN'T-DERIVE: the pinned-base derivation returns `None` when it cannot derive a real state (pinned base
            // unresolvable, derivation deferred/crashed) — it OMITS the MG rather than emit an empty-state sentinel. Map that to
            // `Hash.empty`, the fail-closed CANNOT-RE-DERIVE sentinel: `ShardCheckpointGl0AcceptanceManager.reExecPath` buckets it as
            // "can't check" (plain `Rejected` — dropped, never admitted unverified, NO slash targets) and `watchtowerReExec` filters it —
            // never a deterministic-mismatch false slash.
            reExec(mg, binaries, anchor, executionBaseOrdinal).map(_.getOrElse(io.constellationnetwork.security.hash.Hash.empty))
        },
        // FINDING-002/EPIC-3.1 — slash-cooldown committee exclusion. Reads the `Slashings` (fieldId 34) records off the SAME
        // finalized-base `storages.mptStore` the committee draw's eta resolver (`sharedEtaForPeriod` → HistoricalStakeReader) reads,
        // pinned per epoch at the anchor `(epoch−1)·R − 1` (R = the SAME `etaRotationSnapshots` the GSAM boundary writer uses) — a pure
        // function of the wire-carried `checkpoint.epoch` over append-only consensus records in the k₁ write-frozen prefix, so every
        // node on every path that runs `verifyEmbedded` (produce + validateArtifact + follower createContext) excludes the identical
        // set (#261 split-safety; see SlashCooldownReader's scaladoc for the full uniformity argument).
        slashCooldownReader = Some(
          io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader.fromMptStore[F](
            storages.mptStore,
            cfg.nakamoto.etaRotationSnapshots(cfg.environment).value
          )(Async[F], HasherSelector[F].getCurrent)
        )
      )(Async[F], HasherSelector[F].getCurrent, implicitly[SecurityProvider[F]], implicitly[Metrics[F]])
      // WATCHTOWER on-chain dispute verdict for the `createContext` GSAM (W3a). gl0 followers re-derive the GSI via `createContext` and must
      // reproduce the signed snapshot's mptRoot — which reflects any watchtower slash — so this GSAM must apply the SAME slash. Built with
      // the PIN-1 re-derivation closure (mirroring the `reExecuteDerivation` above) + the DURABLE `Slashings`-backed double-slash reader. The
      // `createContext` path threads `signedArtifact.fraudProofs` into accept(); this validator re-validates each one identically. `None` at
      // `numShards = 1` (`shardAcceptanceDeps = None`) ⇒ carried fraud proofs (always empty there) ignored ⇒ byte-identical regression bar.
      createContextInvalidStateProofValidator = shardAcceptanceDeps match {
        case Some(deps) =>
          implicit val h: Hasher[F] = HasherSelector[F].getCurrent
          // Track-1 execution-base-pin (FINDING-B1): the SAME pinned reader-resolution as `reExecuteDerivation` above — the honest
          // re-derivation reads S(N) at the DISPUTED checkpoint's own `executionBaseOrdinal`, never this follower's live base. A follower
          // whose live tip ran AHEAD of the pinned base would otherwise recompute a different root, false-UPHOLD the fraud proof, and
          // write a slash (fieldId-34 + stake maps) into its consensus root that the pinned leader didn't → StateProofMismatch
          // mirror-freeze fork. Unresolvable anchor ⇒ fail-closed `Hash.empty` ⇒ `InvalidStateProofValidator` step 7 rejects the dispute
          // (`CannotRederive`) — an unverifiable dispute never slashes.
          val pinnedReaderAt =
            ShardCheckpointWiring.pinnedPriorReaderAt[F](sharedPinnedCurrencyInfoReader)
          val reExec =
            ShardCheckpointWiring.reExecDerivationAtPinnedBase[F](
              shardScEventsProcessor,
              pinnedReaderAt,
              storages.lastNGlobalSnapshot.getByOrdinal
            )(
              Async[F],
              Parallel[F],
              h,
              implicitly[JsonSerializer[F]],
              globalStateProofSelector
            )
          val reDerive =
            (
              mg: Address,
              binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
              anchor: SnapshotOrdinal,
              executionBaseOrdinal: SnapshotOrdinal
            ) => reExec(mg, binaries, anchor, executionBaseOrdinal).map(_.getOrElse(io.constellationnetwork.security.hash.Hash.empty))
          Some(
            io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator.make[F](
              reDerivePerMgRoot = reDerive,
              slashedReader = io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofSlashedReader
                .fromMptStore[F](storages.mptStore),
              verifyExecutionCertificate = deps.acceptanceManager.verifyExecutionCertificate
            )(Async[F], implicitly[SecurityProvider[F]], h)
          ): Option[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]]
        case None =>
          Option.empty[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]]
      }
      anchoredConsensusKeyClaims <- GlobalSnapshotAcceptanceManager.anchoredConsensusKeyClaimsFromRegistry(operatorKeyRegistry)
      globalSnapshotAcceptanceManager <- GlobalSnapshotAcceptanceManager.make(
        cfg.fieldsAddedOrdinals,
        cfg.metagraphsSync,
        cfg.environment,
        BlockAcceptanceManager.make[F](validators.blockValidator, txHasher),
        AllowSpendBlockAcceptanceManager.make[F](validators.allowSpendBlockValidator),
        TokenLockBlockAcceptanceManager.make[F](validators.tokenLockBlockValidator),
        shardScEventsProcessor,
        updateNodeParametersAcceptanceManager,
        updateDelegatedStakeAcceptanceManager,
        updateNodeCollateralAcceptanceManager,
        validators.spendActionValidator,
        validators.pricingUpdateValidator,
        priceStateUpdater,
        collateral.amount,
        cfg.delegatedStaking.withdrawalTimeLimit.getOrElse(cfg.environment, EpochProgress.MinValue),
        storages.mptOverlay,
        loggerBundle,
        // §3 NIPoPoW S0.4: the eta-rotation period R. Read at GSAM construction so the boundary-write check
        // (`ord % R == R - 1`) inside accept() is deterministic across all nodes. R is now DERIVED in
        // `NakamotoConfig` as `round(3.03·k₁)` from the single `nakamoto.confirmation-depth-k` knob (no longer a
        // standalone HOCON key) — same typed field the leader loop / GlobalSnapshotConsensus.make read, so all
        // sites stay in lockstep.
        etaRotationSnapshots = cfg.nakamoto.etaRotationSnapshots(cfg.environment).value,
        // Path 1 (heap-leak workstream): wire the eta callback so the boundary-write at `ord % R == R - 1` lands a
        // real computed eta in the `HistoricalStakeSnapshot` MPT entry instead of `Hash.empty`. The callback is backed
        // by an [[EtaStateManager]] (MPT cache + caller-supplied chain-walk fallback) constructed above.
        etaForPeriod = Some(sharedEtaForPeriodAtParent),
        // Hierarchical-shard-checkpoints v1 acceptance-side deps. `None` at `numShards = 1` (regression bar);
        // `Some(...)` activates the shard-checkpoint admission path inside `accept()`. The same
        // `ShardCheckpointWiring.acceptanceDeps` result feeds both GSAM construction sites so they stay consistent.
        shardingConfig = shardAcceptanceDeps.map(_.shardingConfig),
        shardCheckpointAcceptanceManager = shardAcceptanceDeps.map(_.acceptanceManager),
        shardAssignment = shardAcceptanceDeps.map(_.shardAssignment),
        // WATCHTOWER invalid-state-proof slashing (slashing part 3): thread the typed HOCON config from the single
        // `nakamoto.invalidity-slashing` source (NOT the GSAM-make hardcoded default) so the slash fraction/cooldown/bounty —
        // which feed the post-slash stake maps committed into the global mptRoot — are the operator-configured, cluster-uniform
        // values. This is the sole GSAM construction site, so threading here covers the whole acceptance path.
        invaliditySlashingConfig = cfg.nakamoto.invaliditySlashing,
        // WATCHTOWER on-chain dispute verdict (W3a): re-validate carried fraud proofs on the `createContext` path so gl0 followers slash
        // identically and reproduce the signed mptRoot. `None` at numShards=1.
        invalidStateProofValidator = createContextInvalidStateProofValidator,
        kesRegistrationAcceptanceManagerForHasher = Some { registrationHasher: Hasher[F] =>
          implicit val h: Hasher[F] = registrationHasher
          io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertAcceptanceManager.make[F](
            io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.make[F](
              validators.signedValidator,
              seedlist
            )
          )
        },
        anchoredConsensusKeyClaims = anchoredConsensusKeyClaims
      )
      globalSnapshotContextFns = GlobalSnapshotContextFunctions.make(
        globalSnapshotAcceptanceManager,
        updateDelegatedStakeAcceptanceManager,
        cfg.delegatedStaking.withdrawalTimeLimit.getOrElse(cfg.environment, EpochProgress.MinValue),
        cfg.fieldsAddedOrdinals.tessellation3Migration.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        cfg.fieldsAddedOrdinals.setSumFix.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        storages.mptStore,
        cfg.incrementalDelegatedStakingStartingOrdinal.getOrElse(cfg.environment, SnapshotOrdinal.MinValue),
        storages.mptOverlay
      )
    } yield
      new SharedServices[F, A](
        localHealthcheck = localHealthcheck,
        cluster = cluster,
        session = session,
        gossip = gossip,
        globalSnapshotContextFns = globalSnapshotContextFns,
        currencySnapshotContextFns = currencySnapshotContextFns,
        currencySnapshotAcceptanceManager = currencySnapshotAcceptanceManager,
        currencyEventsCutter = currencyEventsCutter,
        restart = restartService,
        updateNodeParametersAcceptanceManager = updateNodeParametersAcceptanceManager,
        updateDelegatedStakeAcceptanceManager = updateDelegatedStakeAcceptanceManager,
        updateNodeCollateralAcceptanceManager = updateNodeCollateralAcceptanceManager,
        priceStateUpdater = priceStateUpdater,
        // Track-3 S4: exposed so the follower resync-to-canonical paths (currency-l0 `StateChannel`, currency-l1
        // `CurrencySnapshotProcessor`) can drop the eta walk cache when they realign the MPT base to gl0's canonical
        // GSI — the follower analog of the gl0 overlay's base-revert hook.
        etaStateManager = etaStateManager,
        // Task #44 — the SINGLE per-node shard acceptance deps (the stateful registry: per-shard chain stores,
        // tip trackers, finality triggers, binary buffers, committee cache, adopted watermarks). Built ONCE here
        // and reused by the gl0-leader produce path: `GlobalSnapshotConsensus.make` reads
        // `sharedServices.shardAcceptanceDeps` instead of constructing a second, disjoint instance. With one
        // instance the follower verify-GSAM (this module), the leader-produce GSAM, the shard producers, the
        // sync daemon, and the #42 ANCHOR-REORG healer all observe ONE registry. `None` at numShards <= 1.
        shardAcceptanceDeps = shardAcceptanceDeps,
        // The genesis-loaded VRF identity registry is also load-bearing for the per-binary
        // admission committee, which remains active when execution sharding is disabled.
        // Expose the exact atomic registry passed into SharedServices so GL0 consensus and
        // admission never re-read a mutable genesis path or fall back to in-band keys.
        operatorKeyRegistry = operatorKeyRegistry
      ) {}
}

sealed abstract class SharedServices[F[_], A <: CliMethod] private (
  val localHealthcheck: LocalHealthcheck[F],
  val cluster: Cluster[F],
  val session: Session[F],
  val gossip: Gossip[F],
  val globalSnapshotContextFns: GlobalSnapshotContextFunctions[F],
  val currencySnapshotContextFns: CurrencySnapshotContextFunctions[F],
  val currencySnapshotAcceptanceManager: CurrencySnapshotAcceptanceManager[F],
  val currencyEventsCutter: CurrencyEventsCutter[F],
  val restart: RestartService[F, A],
  val updateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[F],
  val updateDelegatedStakeAcceptanceManager: UpdateDelegatedStakeAcceptanceManager[F],
  val updateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[F],
  val priceStateUpdater: PriceStateUpdater[F],
  // Track-3 S4: the per-layer eta resolver. Exposed so follower resync-to-canonical paths can call
  // `forgetUncommitted` on a base revert (the follower analog of the gl0 overlay's base-revert hook).
  val etaStateManager: io.constellationnetwork.node.shared.domain.nakamoto.EtaStateManager[F],
  // Task #44 — the single per-node shard acceptance deps, owned here and threaded into the gl0-leader
  // produce path (`GlobalSnapshotConsensus.make`) so adopt ↔ produce ↔ heal share ONE registry.
  val shardAcceptanceDeps: Option[ShardCheckpointWiring.AcceptanceDeps[F]],
  // Genesis-bound operator KES/VRF identities. Derived projections remain compatibility adapters;
  // this atomic registry is the only authority object threaded through the application.
  val operatorKeyRegistry: io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry[F]
)
