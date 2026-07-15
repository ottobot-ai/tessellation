package io.constellationnetwork.dag.l0.infrastructure.snapshot

import cats.Order
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.Async
import cats.effect.kernel.Ref
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.dataApplication.DataCalculatedState
import io.constellationnetwork.dag.l0.domain.snapshot.programs.UpdateNodeParametersCutter
import io.constellationnetwork.dag.l0.infrastructure.rewards.RewardsService
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.ext.cats.syntax.next._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.config.DelegatedRewardsConfigProvider
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions.InvalidArtifact
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
import io.constellationnetwork.node.shared.domain.event.EventCutter
import io.constellationnetwork.node.shared.domain.nakamoto.ShardWindowContinuation
import io.constellationnetwork.node.shared.domain.rewards.Rewards
import io.constellationnetwork.node.shared.domain.snapshot.services.GlobalL0Service
import io.constellationnetwork.node.shared.infrastructure.consensus.ConsensusLog
import io.constellationnetwork.node.shared.infrastructure.consensus.ConsensusLog.{Category, Event}
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger, TimeTrigger}
import io.constellationnetwork.node.shared.infrastructure.delegatedStake.RewardsInfoStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotAcceptanceManager,
  ShardCheckpointAcceptResult
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.{RewardsInput, _}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.UpdateDelegatedStake
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.transaction.Transaction
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelValidationType}
import io.constellationnetwork.syntax.sortedCollection.sortedMapSyntax
import io.constellationnetwork.validator.GlobalSnapshotActiveEraValidator

import eu.timepit.refined.auto._
import eu.timepit.refined.types.all.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Core consensus functions for Global Snapshot creation and validation.
  *
  * Both the leader and every follower independently call `createProposalArtifact` from the same inputs (events, lastArtifact, context,
  * facilitators). If any step is non-deterministic, followers produce a different artifact hash than the leader, triggering the slower
  * validation path (which mutates MptStore twice) and potentially causing cascading state divergence.
  *
  * '''Determinism contract''': Given identical `(lastArtifact, context, events, facilitators)`, every peer MUST produce byte-identical
  * `(GlobalSnapshotArtifact, GlobalSnapshotContext)`. All collections passed to the acceptance pipeline must be in canonical order
  * (sorted).
  */
abstract class GlobalSnapshotConsensusFunctions[F[_]: Async: SecurityProvider]
    extends SnapshotConsensusFunctions[
      F,
      GlobalSnapshotEvent,
      GlobalSnapshotArtifact,
      GlobalSnapshotContext,
      ConsensusTrigger
    ] {}

object GlobalSnapshotConsensusFunctions {

  /** Task #12 slice 2b — bound on the producer's hash-keyed STAGING map of per-ordinal accumulators awaiting finalization. Sized
    * comfortably above the served ring
    * ([[io.constellationnetwork.node.shared.domain.nakamoto.GlobalChangeSetService.recentAccumulatorsToKeep]] \= 256) so concurrent fork
    * candidates at the depth-k window all have room to stage before one finalizes and promotes; entries for candidates that never finalize
    * are evicted in arbitrary hash-iteration order on overflow (a plain `Map` is NOT insertion-ordered — `.drop` is non-deterministic). In
    * steady state the map DRAINS on finalize (promotion rekeys raw->with-cert then removes the finalized hash), so this cap is only a
    * backstop for never-finalizing forks. Pure memory bound (a dropped pre-finalize staging entry only forces a follower into the full-GSI
    * adopt fallback), never a consensus parameter.
    *
    * Since slice-2c the steady-state bound is the FINALIZED-watermark prune in `SnapshotLeaderLoop.recordFinalizedAccumulator` (drop every
    * staged entry at-or-below the finalized tip). This size cap only backstops a burst of never-finalizing forks staged BETWEEN two
    * finalize ticks. Sized comfortably above the depth-k (255) retention window plus expected fork churn, with EVERY node now staging via
    * the validator adopt path (not just the ~1/N it produced).
    */
  val pendingAccumulatorsToKeep: Int = 512

  /** Slice 14 — splice the committee attestations collected in the per-shard `ShardTipTracker` (gossiped `CommitteeMemberSignature`s, full
    * signature form) into a candidate checkpoint's `committeeSignatures`. The resulting artifact must carry at least `kQuorum` distinct
    * valid execution signatures, and `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` still re-executes every included transition.
    *
    * Pure + LEADER-PATH ONLY (callers gate on `sourceShardCheckpoints`): the produce leader enriches its candidates from its node-local
    * tracker; the resulting checkpoint is what the leader embeds + proposes, and every follower threads that SAME embedded set unchanged
    * (it does NOT re-source its own tracker), so `verifyEmbedded` re-accepts only after resolving the exact signed parent by hash from the
    * retained shard-chain index. Missing parent data fails closed rather than waiving producer duty. `committeeSignatures` is EXCLUDED from
    * `ShardCheckpointSigPreimage`, so splicing extra signers leaves the canonical checkpoint hash (chain-link + signed bytes) untouched —
    * the spliced signers each signed that same hash.
    *
    * Dedup by `peerId` (the producer's already-embedded signature is dropped from `collected` rather than double-counted) and the appended
    * signers are sorted by `peerId` hex for a canonical, reproducible `committeeSignatures` order (so the leader re-validating its own
    * artifact yields identical bytes regardless of `Map` iteration order — the BLS-design §8 canonical-ordering discipline).
    */
  private[snapshot] def spliceCommitteeSignatures(
    cp: io.constellationnetwork.schema.sharding.ShardCheckpoint,
    collected: Map[PeerId, io.constellationnetwork.schema.sharding.CommitteeMemberSignature]
  ): io.constellationnetwork.schema.sharding.ShardCheckpoint = {
    val embeddedPeers = cp.committeeSignatures.toList.iterator.map(_.peerId).toSet
    val extra = collected.values.iterator
      .filterNot(s => embeddedPeers.contains(s.peerId))
      .toList
      .sortBy(_.peerId.value.value)
    if (extra.isEmpty) cp
    else cp.copy(committeeSignatures = NonEmptyList(cp.committeeSignatures.head, cp.committeeSignatures.tail ++ extra))
  }

  def make[F[_]: Async: SecurityProvider: JsonSerializer](
    globalSnapshotAcceptanceManager: GlobalSnapshotAcceptanceManager[F],
    collateral: Amount,
    rewardsService: RewardsService[F],
    eventCutter: EventCutter[F, StateChannelEvent, DAGEvent],
    updateNodeParametersCutter: UpdateNodeParametersCutter[F],
    environment: AppEnvironment,
    delegatedRewardsConfigProvider: DelegatedRewardsConfigProvider,
    v3MigrationOrdinal: SnapshotOrdinal,
    setSumFixOrdinal: SnapshotOrdinal,
    incrementalDelegatedStakingStartingOrdinal: SnapshotOrdinal,
    mptStore: MptStore[F, GlobalStateKey],
    overlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // ─── Hierarchical-shard-checkpoints v1 — Gap C: feed finalized checkpoints into accept() ───
    // Acceptance-side per-shard deps. `None` at `numShards = 1` (the production default) ⇒ the GSAM
    // `accept(...)` call below passes `shardCheckpoints = SortedMap.empty` — byte-identical to today's
    // behavior (the regression bar). `Some(deps)` sources the phase-2-finalized checkpoint per shard
    // from the per-shard chain stores and passes them into `accept(shardCheckpoints = ...)`.
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ] = None,
    // Task #12 slice 2b — STAGING map for the gl0 changeset-ring producer. After this node builds a
    // snapshot artifact and knows its hash (`currentSnapshotHash`, the `overlay.commit` site below), it
    // stages the typed per-ordinal `StateChangesAccumulator` accept() returned, keyed by that hash. The
    // accumulator only PROMOTES into the served ordinal-keyed ring once `SnapshotLeaderLoop` finalizes the
    // matching hash (depth-k OR attestation-2/3 sink). Hash-keyed because the produced/validated snapshot's
    // ordinal isn't yet finalized here and forks at the same ordinal must not collide. BOUNDED to at most
    // `pendingAccumulatorsToKeep` entries (arbitrary-order eviction on overflow) so fork candidates that never
    // finalize cannot leak memory. Pure side effect after the artifact is sealed — never feeds back into
    // consensus/finality. Required param (no default — `Ref.of` is effectful): cl0/dl1/test sites pass
    // `Ref.of(Map.empty)` (those paths never finalize a gl0 changeset ring); production passes the shared Ref
    // from `GlobalSnapshotConsensus.make`.
    //
    // VALUE = `(ordinal, accumulator)`: the ordinal travels with the staged accumulator so the finalize-sink
    // promotion (`SnapshotLeaderLoop.recordFinalizedAccumulator`) can prune by a FINALIZED watermark — drop every
    // staged entry at-or-below the finalized tip (each is promoted-or-a-dead-fork), correct-by-construction rather
    // than the earlier arbitrary `.drop` size eviction that could discard a not-yet-finalized entry under the
    // depth-k (255) retention window + reorg churn (esp. now that EVERY node stages via the validator adopt path).
    pendingAccumulatorsRef: Ref[F, Map[Hash, (SnapshotOrdinal, StateChangesAccumulator)]],
    // 3c-A enabler (seal-time signed-bytes persistence) — STAGING map for the signed MPT byte map, mirroring
    // `pendingAccumulatorsRef`. At the `overlay.commit` site the EXACT signed `postBytes` (the same
    // `allEntriesAsBytesWithHandle(handle, ordinal)` `accept()` derived the signed `mptRoot` from) are staged keyed by
    // `currentSnapshotHash`; `SnapshotLeaderLoop.recordFinalizedAccumulator` promotes the FINALIZED hash's bytes into
    // the served signed-bytes store and watermark-prunes the rest. Same `(ordinal, value)` shape + cap eviction as the
    // accumulator staging. Required param (no default — `Ref.of` is effectful): cl0/dl1/test sites pass
    // `Ref.of(Map.empty)`; production passes the shared Ref from `GlobalSnapshotConsensus.make`.
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    // Task #19 — backstop size cap on `pendingAccumulatorsRef`. Typed `nakamoto.staging-accumulators-cap` HOCON
    // value (`SharedConfig.nakamoto.stagingAccumulatorsCap`, default 2048 = 2× the served ring), threaded from
    // `GlobalSnapshotConsensus.make`. The steady-state bound is the finalize-sink watermark prune; this only caps a
    // burst of never-finalizing forks staged BETWEEN two finalize ticks. The default mirrors `pendingAccumulatorsToKeep`
    // so the unit suite (and any caller relying on the default) is unaffected; production overrides it via HOCON.
    stagingAccumulatorsCap: Int = GlobalSnapshotConsensusFunctions.pendingAccumulatorsToKeep,
    // WATCHTOWER fraud-proof POOL (W3a) — the node-local staging area the gossip dispute consumer (`NakamotoSyncDaemon.handleFraudProof`)
    // offers locally-UPHELD `InvalidStateProofEvidence` into. On the LEADER produce path (`sourceShardCheckpoints = true`) the producer
    // peeks the pool and embeds its contents in the produced snapshot's `fraudProofs` consensus field AND threads them into `accept(...)`
    // so the slash is folded; the FOLLOWER/validator path threads the leader's embedded `artifact.fraudProofs` instead (split-safety — never
    // re-sources its own pool), so the byte-exact `recreatedArtifact === artifact` round-trip holds. `noop` (cl0/dl1/tests/`numShards = 1`)
    // ⇒ `peekAll` is always empty ⇒ no fraud proofs embedded ⇒ byte-identical to the pre-watchtower path (the regression bar). Required (no
    // default — `noop` needs `Sync[F]` which a default-arg site can't summon); the sole production wiring passes the shared instance, and the
    // `numShards = 1` wiring passes `WatchtowerFraudProofPool.noop[F]` explicitly.
    fraudProofPool: io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool[F]
  ): GlobalSnapshotConsensusFunctions[F] = new GlobalSnapshotConsensusFunctions[F] {

    private val logger = Slf4jLogger.getLoggerFromClass[F](getClass)

    def getRequiredCollateral: Amount = collateral

    // Read from consensus-agreed context (deterministic), NOT from MptStore (local mutable state).
    // After an abandoned round the MptStore may contain partial mutations that differ across nodes,
    // causing facilitatorFilter to compute different eligibility sets → fork.
    def getBalance(context: GlobalSnapshotContext, address: Address): F[Balance] =
      context.balances.getOrElse(address, Balance.empty).pure[F]

    /** Validates a leader's proposed artifact by independently reconstructing it from the same inputs.
      *
      * Called by followers when their locally-built artifact hash differs from the leader's proposal. Re-derives the consensus trigger from
      * `artifact.epochProgress` (not the local trigger) to prevent trigger-divergence false mismatches. If the reconstructed artifact
      * equals the leader's, returns Right with the validated artifact and context; otherwise returns Left with the mismatch.
      *
      * '''Side effect''': Calls `createProposalArtifact` which mutates the shared MptStore. The caller must take a savepoint before calling
      * and restore on failure to prevent partial state leaking.
      */
    override def validateArtifact(
      lastSignedArtifact: Signed[GlobalSnapshotArtifact],
      lastContext: GlobalSnapshotContext,
      trigger: ConsensusTrigger,
      artifact: GlobalSnapshotArtifact,
      facilitators: Set[PeerId],
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): F[Either[InvalidArtifact, (GlobalSnapshotArtifact, GlobalSnapshotContext)]] = {
      val dagEvents = artifact.blocks.unsorted.map(_.block).map(DAGEvent(_))
      val scEvents = artifact.stateChannelSnapshots.toList.flatMap {
        case (address, stateChannelBinaries) => stateChannelBinaries.map(StateChannelOutput(address, _)).map(StateChannelEvent(_)).toList
      }
      val allowSpendEvents = artifact.allowSpendBlocks.map(_.toList.map(AllowSpendEvent(_))).getOrElse(List.empty)
      val tokenLockEvents = artifact.tokenLockBlocks.map(_.toList.map(TokenLockEvent(_))).getOrElse(List.empty)
      val unpEvents =
        artifact.updateNodeParameters.getOrElse(SortedMap.empty[Id, Signed[UpdateNodeParameters]]).values.map(UpdateNodeParametersEvent(_))
      val cdsEvents = artifact.activeDelegatedStakes
        .getOrElse(SortedMap.empty[Address, List[Signed[UpdateDelegatedStake.Create]]])
        .values
        .flatMap(_.map(CreateDelegatedStakeEvent(_)))
      val wdsEvents = artifact.delegatedStakesWithdrawals
        .getOrElse(SortedMap.empty[Address, List[Signed[UpdateDelegatedStake.Withdraw]]])
        .values
        .flatMap(_.map(WithdrawDelegatedStakeEvent(_)))
      val cncEvents = artifact.activeNodeCollaterals
        .getOrElse(SortedMap.empty[Address, List[Signed[UpdateNodeCollateral.Create]]])
        .values
        .flatMap(_.map(CreateNodeCollateralEvent(_)))
      val wncEvents = artifact.nodeCollateralWithdrawals
        .getOrElse(SortedMap.empty[Address, List[Signed[UpdateNodeCollateral.Withdraw]]])
        .values
        .flatMap(_.map(WithdrawNodeCollateralEvent(_)))
      val kesRegistrationEvents = artifact.operatorKeyRegistrations.toList.map(KesRegistrationCertEvent(_))

      val events: Set[GlobalSnapshotEvent] =
        dagEvents ++ scEvents ++ allowSpendEvents ++ unpEvents ++ tokenLockEvents ++ cdsEvents ++ wdsEvents ++ cncEvents ++ wncEvents ++
          kesRegistrationEvents

      // Derive the consensus trigger from the artifact itself rather than trusting the local
      // consensus trigger, which may differ across nodes (e.g. a node observing EventTrigger
      // while the leader used TimeTrigger). An incremented epochProgress unambiguously means
      // TimeTrigger was used; otherwise it was EventTrigger.
      val artifactTrigger: ConsensusTrigger =
        if (artifact.epochProgress.value.value > lastSignedArtifact.epochProgress.value.value)
          TimeTrigger
        else
          EventTrigger

      // Gap C: re-derive with `sourceShardCheckpoints = false`. The leader's chosen shard checkpoints
      // already folded their per-MG binaries into the received artifact's `stateChannelSnapshots`, which
      // are extracted above as `scEvents` and replayed here — so the recreated artifact matches WITHOUT
      // re-sourcing node-local shard finality state (which could diverge from the leader and spuriously
      // fail the byte-exact `recreatedArtifact === artifact` check).
      def usingJson = createProposalArtifactInternal(
        lastSignedArtifact.ordinal,
        lastSignedArtifact,
        lastContext,
        Hasher.forJson[F],
        artifactTrigger,
        events,
        facilitators,
        getGlobalSnapshotByOrdinal,
        sourceShardCheckpoints = false,
        // Split-safety (CHANGE 5): the follower/validator re-derivation MUST thread the leader's embedded
        // `artifact.shardCheckpoints` into accept() (so it ADOPTS the same committee checkpoints via the
        // deterministic `verifyEmbedded`) AND re-embed the SAME map, so the recreated artifact's
        // `shardCheckpoints` field equals `artifact.shardCheckpoints` and `recreatedArtifact === artifact`
        // holds. The leader already filtered these to the `verifyEmbedded`-accepted subset, so the
        // follower's `verifyEmbedded` re-accepts all of them (deterministic) and the embedded set round-trips.
        incomingShardCheckpoints = artifact.shardCheckpoints,
        // Split-safety (W3a): thread the leader's embedded fraud proofs so the follower folds the SAME slash and re-embeds the SAME map.
        incomingFraudProofs = artifact.fraudProofs
      )

      def check(result: F[(GlobalSnapshotArtifact, GlobalSnapshotContext, Set[GlobalSnapshotEvent])]) =
        result.map {
          case (recreatedArtifact, context, _) =>
            // Historical commitment activation is dark: honest construction reproduces `smtRoot = None`. Exact equality rejects a
            // producer-supplied root instead of treating an unverified commitment as consensus-valid.
            if (recreatedArtifact === artifact)
              (artifact, context).asRight[InvalidArtifact]
            else
              GlobalArtifactMismatch(artifact, recreatedArtifact).asLeft[(GlobalSnapshotArtifact, GlobalSnapshotContext)]
        }

      GlobalSnapshotActiveEraValidator.validate(artifact) match {
        case Left(_) =>
          (GlobalArtifactActiveEraViolation(artifact.ordinal): InvalidArtifact)
            .asLeft[(GlobalSnapshotArtifact, GlobalSnapshotContext)]
            .pure[F]
        case Right(_) => check(usingJson)
      }
    }

    /** Builds a new GlobalIncrementalSnapshot proposal from the previous snapshot and pending events.
      *
      * '''Determinism''': This method MUST produce byte-identical output on every peer given the same inputs. All event lists extracted
      * from the unordered `events: Set[GlobalSnapshotEvent]` are sorted before being passed to the acceptance pipeline to guarantee
      * canonical processing order.
      *
      * The pipeline:
      *   1. Extract and sort events by type (DAG blocks, state channels, allow spends, token locks, etc.) 2. Cut events to fit within
      *      bounds (eventCutter) 3. Derive facilitators from the consensus facility declarations (not from proof signatures) 4. Pass sorted
      *      event lists to `GlobalSnapshotAcceptanceManager.accept()` 5. Build the `GlobalIncrementalSnapshot` artifact with all accepted
      *      data
      *
      * @param events
      *   Unordered set of events — iteration order is non-deterministic. Events are sorted after extraction to ensure deterministic
      *   acceptance.
      * @param facilitators
      *   Current round's facilitators (deterministic: all nodes must receive all facility declarations before advancing from
      *   CollectingFacilities).
      */
    // Public trait entry point (the genuine produce path — SnapshotLeaderLoop). Sources finalized
    // shard checkpoints (Gap C) so they fold into accept(). The follower re-derivation path
    // (`validateArtifact`) calls `createProposalArtifactInternal(..., sourceShardCheckpoints = false)`
    // directly so it does NOT re-source node-local finality state — the leader's folded shard binaries
    // already live in the artifact's `stateChannelSnapshots` and the follower reproduces them by
    // replaying those as ordinary SC events. Re-sourcing on the follower would compare the leader's
    // embedded set against the follower's own (possibly-divergent) chain-store view and spuriously
    // reject a valid snapshot. See the determinism note on `createProposalArtifactInternal`.
    def createProposalArtifact(
      lastKey: GlobalSnapshotKey,
      lastArtifact: Signed[GlobalSnapshotArtifact],
      snapshotContext: GlobalSnapshotContext,
      lastArtifactHasher: Hasher[F],
      trigger: ConsensusTrigger,
      events: Set[GlobalSnapshotEvent],
      facilitators: Set[PeerId],
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
    )(implicit hasher: Hasher[F]): F[(GlobalSnapshotArtifact, GlobalSnapshotContext, Set[GlobalSnapshotEvent])] =
      createProposalArtifactInternal(
        lastKey,
        lastArtifact,
        snapshotContext,
        lastArtifactHasher,
        trigger,
        events,
        facilitators,
        getGlobalSnapshotByOrdinal,
        sourceShardCheckpoints = true,
        // Produce path sources its own candidate checkpoints from the per-shard chain stores below; nothing incoming.
        incomingShardCheckpoints = SortedMap.empty,
        // Produce path peeks its own `fraudProofPool` below; nothing incoming.
        incomingFraudProofs = SortedSet.empty
      ).flatTap { case (artifact, _, _) => GlobalSnapshotActiveEraValidator.requireValid[F](artifact) }

    /** Implementation of [[createProposalArtifact]] with an explicit `sourceShardCheckpoints` gate.
      *
      * '''Gap C determinism contract.''' `sourceShardCheckpoints = true` ONLY on the genuine produce path (the public trait method, called
      * by `SnapshotLeaderLoop`). It sources candidate checkpoints from the node-local per-shard chain stores (a liveness/selection
      * decision, gossip-timing-tolerant by §7.2) and then FILTERS them to the `verifyEmbedded`-accepted subset
      * (`adoptableShardCheckpoints`). That subset is BOTH (a) passed into `accept()` — which adopts them via the deterministic
      * `verifyEmbedded` into `scSnapshots` (the adopt-won `stateChannelSnapshots`) — AND (b) embedded in the produced artifact's
      * `shardCheckpoints` field. So the embedded checkpoints exactly attest the adopted binaries.
      *
      * The follower validation path (`validateArtifact`) passes `sourceShardCheckpoints = false` and supplies the leader's embedded
      * `artifact.shardCheckpoints` via [[incomingShardCheckpoints]]. Those are already the leader's accepted subset; the follower threads
      * them into `accept()` (re-adopting the same set via the deterministic `verifyEmbedded`) and re-embeds the SAME map, so the recreated
      * artifact's `shardCheckpoints` AND `stateChannelSnapshots` match the leader's byte-for-byte. The follower resolves each checkpoint's
      * exact parent by hash from retained shard history; unavailable parent history rejects the artifact.
      *
      * At `numShards = 1` the flag is irrelevant — `shardAcceptanceDeps = None` forces `adoptableShardCheckpoints = SortedMap.empty`
      * regardless, so this method is byte-identical to the pre-wiring code on both paths (the regression bar).
      *
      * @param incomingShardCheckpoints
      *   on the follower/validator re-derivation path, the leader's embedded `artifact.shardCheckpoints`. Empty on the produce path (which
      *   sources its own). See the split-safety note above.
      */
    def createProposalArtifactInternal(
      lastKey: GlobalSnapshotKey,
      lastArtifact: Signed[GlobalSnapshotArtifact],
      snapshotContext: GlobalSnapshotContext,
      lastArtifactHasher: Hasher[F],
      trigger: ConsensusTrigger,
      events: Set[GlobalSnapshotEvent],
      facilitators: Set[PeerId],
      getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
      sourceShardCheckpoints: Boolean,
      incomingShardCheckpoints: SortedMap[
        io.constellationnetwork.schema.sharding.ShardId,
        io.constellationnetwork.schema.sharding.ShardCheckpoint
      ],
      // WATCHTOWER fraud proofs (W3a). On the FOLLOWER/validator re-derivation path (`sourceShardCheckpoints = false`) this is the leader's
      // embedded `artifact.fraudProofs`, threaded back unchanged so the recreated artifact's `fraudProofs` field round-trips byte-identically.
      // On the PRODUCE path (`sourceShardCheckpoints = true`) it is ignored — the producer peeks its node-local `fraudProofPool` instead.
      // Empty at `numShards = 1` / `noop` pool ⇒ byte-identical to the pre-watchtower path on both paths.
      incomingFraudProofs: SortedSet[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]
    )(implicit hasher: Hasher[F]): F[(GlobalSnapshotArtifact, GlobalSnapshotContext, Set[GlobalSnapshotEvent])] = {
      val scEventsBeforeCut = events.collect { case sc: StateChannelEvent => sc }
      val dagEventsBeforeCut = events.collect { case d: DAGEvent => d }
      val allowSpendEventsForAcceptance = events.collect { case as: AllowSpendEvent => as }
      val tokenLockEventsForAcceptance = events.collect { case as: TokenLockEvent => as }
      val unpEventsBeforeCut = events.collect { case unp: UpdateNodeParametersEvent => unp }

      val cdsEventsForAcceptance = events.collect { case e: CreateDelegatedStakeEvent => e }
      val wdsEventsForAcceptance = events.collect { case e: WithdrawDelegatedStakeEvent => e }
      val cncEventsForAcceptance = events.collect { case e: CreateNodeCollateralEvent => e }
      val wncEventsForAcceptance = events.collect { case e: WithdrawNodeCollateralEvent => e }
      val kesRegistrationEventsForAcceptance = events.collect { case e: KesRegistrationCertEvent => e }

      val dagEvents = dagEventsBeforeCut.filter(_.value.height > lastArtifact.height)

      val delegatedConfig = delegatedRewardsConfigProvider.getConfig()

      def shouldUseDelegatedRewards(currentOrdinal: SnapshotOrdinal, currentEpochProgress: EpochProgress): Boolean = {
        val asOfEpoch = delegatedConfig.emissionConfig
          .get(environment)
          .map(f => f(currentEpochProgress))
          .map(_.asOfEpoch)
          .getOrElse(EpochProgress.MaxValue)
        currentOrdinal.value >= v3MigrationOrdinal.value &&
        currentEpochProgress.value.value >= asOfEpoch.value.value
      }

      val classicRewardsFn: (
        Signed[GlobalSnapshotArtifact],
        SortedMap[Address, Balance],
        SortedSet[Signed[Transaction]],
        ConsensusTrigger,
        Set[GlobalSnapshotEvent],
        Option[DataCalculatedState]
      ) => F[DelegatedRewardsResult] = { (signedArtifact, balances, txs, trigger, events, calcState) =>
        rewardsService.classicRewards
          .distribute(
            signedArtifact,
            balances,
            txs,
            trigger,
            events,
            calcState
          )
          .map { rewardTxs =>
            if (signedArtifact.ordinal.value < setSumFixOrdinal.value) {
              DelegatedRewardsResult(
                delegatorRewardsMap = SortedMap.empty,
                updatedCreateDelegatedStakes = SortedMap.empty,
                updatedWithdrawDelegatedStakes = SortedMap.empty,
                nodeOperatorRewards = rewardTxs,
                reservedAddressRewards = SortedSet.empty,
                withdrawalRewardTxs = SortedSet.empty,
                totalEmittedRewardsAmount =
                  Amount(NonNegLong.unsafeFrom(rewardTxs.toList.map(_.amount.value.value).distinct.sum)) // mimic incorrect behaviour
              )
            } else {
              DelegatedRewardsResult(
                delegatorRewardsMap = SortedMap.empty,
                updatedCreateDelegatedStakes = SortedMap.empty,
                updatedWithdrawDelegatedStakes = SortedMap.empty,
                nodeOperatorRewards = rewardTxs,
                reservedAddressRewards = SortedSet.empty,
                withdrawalRewardTxs = SortedSet.empty,
                totalEmittedRewardsAmount = Amount(NonNegLong.unsafeFrom(rewardTxs.toList.map(_.amount.value.value).sum))
              )
            }
          }
      }

      val rewardsWithFacilitators: List[(Address, PeerId)] => RewardsInput => F[DelegatedRewardsResult] = {
        faciltators: List[(Address, PeerId)] =>
          {
            case ClassicRewardsInput(txs) =>
              classicRewardsFn(lastArtifact, snapshotContext.balances, txs, trigger, events, None)

            case DelegateRewardsInput(udsar, psu, ep) =>
              val ordinal = lastArtifact.ordinal.next
              if (shouldUseDelegatedRewards(ordinal, ep)) {
                rewardsService.delegatedRewards.distribute(snapshotContext, trigger, ep, faciltators, udsar, psu).map {
                  delegatedRewardsResult =>
                    if (ordinal > incrementalDelegatedStakingStartingOrdinal) {
                      val updatedCreateDelegatedStakes = delegatedRewardsResult.updatedCreateDelegatedStakes.view.mapValues { records =>
                        records.map { r =>
                          r.copy(
                            currentTokenLockRef = r.currentTokenLockRef.orElse(r.tokenLockRef.some),
                            currentAmount = r.currentAmount.orElse(r.amount.some)
                          )
                        }
                      }.to(SortedMap)

                      delegatedRewardsResult.copy(updatedCreateDelegatedStakes = updatedCreateDelegatedStakes)
                    } else {
                      delegatedRewardsResult
                    }
                }
              } else {
                classicRewardsFn(lastArtifact, snapshotContext.balances, SortedSet.empty, trigger, events, None)
              }
          }
      }

      def getLastArtifactHash = lastArtifactHasher.getLogic(lastArtifact.value.ordinal) match {
        case JsonHash => lastArtifactHasher.hash(lastArtifact.value)
        case KryoHash => lastArtifactHasher.hash(GlobalIncrementalSnapshotV1.fromGlobalIncrementalSnapshot(lastArtifact.value))
      }

      for {
        lastArtifactHash <- getLastArtifactHash
        currentOrdinal = lastArtifact.ordinal.next
        currentEpochProgress = trigger match {
          case EventTrigger => lastArtifact.epochProgress
          case TimeTrigger  => lastArtifact.epochProgress.next
        }

        (scEvents, blocksForAcceptance) <- eventCutter.cut(
          scEventsBeforeCut.toList.sortBy(_.value.address),
          dagEvents.toList,
          snapshotContext,
          currentOrdinal
        )

        unpEventsForAcceptance <- updateNodeParametersCutter.cut(unpEventsBeforeCut.toList, snapshotContext, currentOrdinal)

        lastActiveTips <- lastArtifact.activeTips(Async[F], lastArtifactHasher)
        lastDeprecatedTips = lastArtifact.tips.deprecated

        // Derive lastFacilitators from the current-round facilitators set rather than
        // lastArtifact.proofs. Different nodes collect different numbers of signatures
        // for the same snapshot (gossip is non-deterministic), so proofs.size varies
        // per node. This causes divergent nodeOperatorRewards counts (and amounts)
        // because the facilitator pool is split by facilitators.size. Using the
        // current-round facilitators is deterministic: all nodes must receive all
        // facility declarations before advancing from CollectingFacilities, so
        // state.facilitators is identical across all consensus participants.
        lastFacilitators <- facilitators.toList.traverse { peerId =>
          PeerId._Id.get(peerId).toAddress.map(_ -> peerId)
        }
        // Sort all event lists before passing to accept() to ensure deterministic ordering.
        // Events are extracted from Set[GlobalSnapshotEvent] (line 114) which has non-deterministic
        // iteration order. Without sorting, different nodes may process events in different orders,
        // causing divergent acceptance results (e.g. first-wins duplicate logic in delegated stakes).
        // All types derive Order, and Signed[T] provides Order when T has Order, ensuring
        // canonical ordering across all peers. Using .sorted (Order-based) instead of
        // .sortBy(_.show) (String-based) avoids collisions when distinct events have identical
        // Show representations.
        sortedAllowSpendEvents = allowSpendEventsForAcceptance.toList.map(_.value).sorted
        sortedTokenLockEvents = tokenLockEventsForAcceptance.toList.map(_.value).sorted
        sortedCdsEvents = cdsEventsForAcceptance.toList.map(_.value).sorted(Signed.ordering(Order[UpdateDelegatedStake.Create].toOrdering))
        sortedWdsEvents = wdsEventsForAcceptance.toList
          .map(_.value)
          .sorted(Signed.ordering(Order[UpdateDelegatedStake.Withdraw].toOrdering))
        sortedCncEvents = cncEventsForAcceptance.toList.map(_.value).sorted(Signed.ordering(Order[UpdateNodeCollateral.Create].toOrdering))
        sortedWncEvents = wncEventsForAcceptance.toList
          .map(_.value)
          .sorted(Signed.ordering(Order[UpdateNodeCollateral.Withdraw].toOrdering))
        sortedKesRegistrationEvents = kesRegistrationEventsForAcceptance.toList.map(_.value).sorted

        _ <- ConsensusLog.info(
          logger,
          Category.Proposal,
          currentOrdinal.show,
          "n/a",
          Event.ProposalEvents,
          "trigger" -> trigger.toString,
          "events.total" -> events.size.toString,
          "dag" -> blocksForAcceptance.size.toString,
          "sc" -> scEvents.size.toString,
          "allowSpend" -> sortedAllowSpendEvents.size.toString,
          "tokenLock" -> sortedTokenLockEvents.size.toString,
          "unp" -> unpEventsForAcceptance.size.toString,
          "delegStakeCreate" -> sortedCdsEvents.size.toString,
          "delegStakeWithdraw" -> sortedWdsEvents.size.toString,
          "nodeCollCreate" -> sortedCncEvents.size.toString,
          "nodeCollWithdraw" -> sortedWncEvents.size.toString,
          "operatorKeyRegistration" -> sortedKesRegistrationEvents.size.toString
        )

        // Gap C — source the phase-2-finalized shard checkpoint per shard, to feed into `accept()`.
        // Empty (byte-identical to today, the regression bar) when EITHER `numShards = 1` (deps None)
        // OR this is the follower validation re-derivation (`sourceShardCheckpoints = false` — see the
        // determinism contract on `createProposalArtifactInternal`). On the genuine produce path with
        // sharding active: for each tracked shard, advance the execution-quorum tracker, then freshly resolve the highest hash-bearing
        // checkpoint on the current best-tip ancestry with `kQuorum` distinct replay signatures. Never resolve a historical monotone
        // ordinal through the current fork: quorum observed for branch A must not qualify branch B at the same ordinal.
        // Skip shards with no currently canonical qualified checkpoint.
        //
        // ─── Q1 — deterministic inclusion cutoff ──────────────────────────────────────────────────────
        // Without a cutoff, the chosen checkpoint depends on how far this node's shard chain + attestation
        // tally have advanced at the wall-clock moment `produce` runs — gossip-timing-dependent, so two
        // honest leaders producing the SAME gl0 ord N could fold DIFFERENT shard checkpoints and diverge.
        // The cutoff makes the selection a pure function of `(N, shard chain)`: clamp to the deterministic
        // MAX checkpoint whose `gl0AnchorOrdinal <= N` (N = `currentOrdinal`, the ord being produced) AND
        // has an execution quorum. Since `gl0AnchorOrdinal` is monotone non-decreasing along the canonical
        // shard chain (each checkpoint anchors to a gl0 ord >= its parent's), walking back from the
        // qualifying checkpoint to an ancestor with `gl0AnchorOrdinal <= N` removes the timing dependence.
        // `verifyEmbedded` below still checks that the selected ancestor itself carries execution quorum;
        // quorum on a child never implies quorum on its parent. Until E8.1A makes parent Phase-2 evidence
        // artifact-validity load-bearing, a Byzantine unanchored child can therefore fail closed and stall
        // selection rather than make an under-certified ancestor valid. A checkpoint that nominated a FUTURE
        // gl0 ord (> N) is excluded this ord and folds at a later gl0 ord (loose-coupling §7.2).
        //
        // This clamp runs ONLY on the leader path (`sourceShardCheckpoints = true`). The follower path
        // (`validateArtifact`) never enters this branch — it replays the leader's embedded
        // `stateChannelSnapshots`, so it must NOT re-source node-local shard state (the split-safety
        // invariant). See the determinism contract on `createProposalArtifactInternal`.
        // Candidate checkpoints to consider this ord. Produce path: source from the node-local per-shard chain stores
        // (selection only). Follower/validator path: the leader's embedded `incomingShardCheckpoints`. Empty otherwise.
        candidateShardCheckpoints <- shardAcceptanceDeps match {
          case Some(deps) if sourceShardCheckpoints =>
            deps.registry.toList.traverse {
              case (shardId, entry) =>
                entry.finalityTriggers.advance >>
                  entry.finalityTriggers.currentQualifyingCheckpoint.flatMap {
                    case None =>
                      none[(io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint)]
                        .pure[F]
                    case Some(qualifying) =>
                      val qualifyingOrd = qualifying.signed.value.shardOrdinal
                      // ANCESTOR-FIRST SELECTION (chain-hole fix, 2026-06-10). Walk the canonical ancestry of the
                      // qualifying checkpoint and embed the OLDEST entry that (a) anchors at-or-below N (the Q1
                      // cutoff, unchanged) and (b) whose per-MG windows chain off gl0's CURRENT SC tips
                      // (`snapshotContext.lastStateChannelSnapshotHashes`, `Hash.empty` for an unseeded MG).
                      //
                      // The previous rule embedded the NEWEST qualifying checkpoint regardless of ancestry. When an
                      // ancestor never qualified (genesis fork split its attestations below kQuorum), the newest
                      // child was embedded over a CHAIN HOLE: its windows are incrementals chained past the
                      // never-adopted genesis window, the currency derivation silently produced nothing, the SC tip
                      // advanced past genesis, and the MG wedged permanently (run brkbktoet, 2026-06-10).
                      //
                      // Walking back from the QUALIFYING checkpoint preserves chain linkage through each child's
                      // `parentCheckpointHash`. A checkpoint embeds with the signatures the tracker collected, but
                      // followers never infer economic validity from that count: `verifyEmbedded` deterministically
                      // replays every included transition and compares the recreated per-MG roots.
                      //
                      // Selecting NOTHING when no entry anchors at the tips (everything already adopted) also stops
                      // the re-embed churn of already-adopted checkpoints. Empty checkpoints are structurally invalid.
                      entry.chainStore
                        .walkBackTo(qualifying.hash, qualifyingOrd.value)
                        .flatMap { chainTipFirst =>
                          val scTips = snapshotContext.lastStateChannelSnapshotHashes
                          // TRIM-AWARE match (mirrors the GSAM adoption guard): a checkpoint is the next-to-adopt
                          // if any of its windows CONTAINS the binary continuing gl0's SC tip — not only at the
                          // window head. Post-reorg, canonical windows OVERLAP the already-adopted orphaned
                          // prefix; head-only matching stalled adoption permanently (run bml994k4d shard 1)
                          // while the chain kept producing. The adoption side trims the overlap.
                          chainTipFirst.reverse.findM { h =>
                            val cp = h.signed.value
                            if (
                              cp.gl0AnchorOrdinal.value.value <= currentOrdinal.value.value &&
                              cp.derivedStateDelta.includedSnapshots.nonEmpty
                            )
                              ShardWindowContinuation.isAtomicallyContinuableF(shardId, cp, scTips)
                            else
                              false.pure[F]
                          }.flatMap {
                            case None =>
                              // embed-none observability (mirrors produce-skip). This fires BOTH when fully
                              // caught up (normal: every window already adopted, tips == newest tail) AND on an
                              // adoption-side stall — the reader disambiguates by whether the shard chain height
                              // keeps growing while this line repeats with unchanged tips.
                              //
                              // INSTRUMENTATION (run-24 genesis-seam): when nothing matches, dump per-MG the SC tip we
                              // match against vs the ACTUAL window parent-refs across the whole ancestry. If matched=false
                              // and the parentRefs never contain scTip, the producer's window anchoring (its own perMgTip)
                              // has diverged from gl0's adopted SC tip — the suspected freeze. Remove after diagnosis.
                              val anchorDiag = chainTipFirst
                                .flatMap(_.signed.value.derivedStateDelta.includedSnapshots.keys.toList)
                                .distinct
                                .map { mg =>
                                  val tip = scTips.getOrElse(mg, Hash.empty)
                                  val parentRefs = chainTipFirst.flatMap { hh =>
                                    hh.signed.value.derivedStateDelta.includedSnapshots
                                      .get(mg)
                                      .toList
                                      .flatMap(_.toList.map(_.value.lastSnapshotHash))
                                  }.distinct
                                  s"${mg.value.value.take(8)}{scTip=${tip.value.take(8)} matched=${parentRefs
                                      .contains(tip)} parentRefs=[${parentRefs.map(_.value.take(8)).mkString(",")}]}"
                                }
                                .mkString(" ")
                              logger
                                .info(
                                  s"🧩 embed-none shard=${shardId.value.value} " +
                                    s"chainLen=${chainTipFirst.size} qualifyingOrd=${qualifyingOrd.value} " +
                                    s"tips=${snapshotContext.lastStateChannelSnapshotHashes.toList.map {
                                        case (mg, hh) => s"${mg.value.value.take(8)}:${hh.value.take(8)}"
                                      }.mkString(",")} " +
                                    s"anchorDiag=$anchorDiag"
                                )
                                .as(
                                  none[
                                    (
                                      io.constellationnetwork.schema.sharding.ShardId,
                                      io.constellationnetwork.schema.sharding.ShardCheckpoint
                                    )
                                  ]
                                )
                            case Some(h) =>
                              // Slice 14: enrich the candidate with the committee attestations this node collected in
                              // the per-shard tracker (keyed by the canonical checkpoint hash = chain-store `.hash`).
                              // This is leader-path selection metadata only. The follower threads the leader's set
                              // unchanged and independently replays the checkpoint through `verifyEmbedded`.
                              entry.tipTracker
                                .signaturesFor(h.hash)
                                .map(collected => (shardId -> spliceCommitteeSignatures(h.signed.value, collected)).some)
                          }
                        }
                  }
            }
              .map(entries => SortedMap.from(entries.flatten))
          case Some(_) =>
            // Follower/validator path: thread the leader's embedded checkpoints through unchanged. They are already the
            // leader's `verifyEmbedded`-accepted subset; the filter below re-confirms each one deterministically.
            incomingShardCheckpoints.pure[F]
          case None =>
            SortedMap
              .empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint]
              .pure[F]
        }

        // CHANGE 4 — keep ONLY the checkpoints the DETERMINISTIC `verifyEmbedded` accepts. These are exactly the ones
        // GSAM's adopt path will fold into `scSnapshots`, so embedding this same subset in the produced artifact keeps the
        // `shardCheckpoints` field consistent with `stateChannelSnapshots`. `verifyEmbedded` also checks producer duty against the exact
        // retained parent by hash; missing parent history fails closed. Given the same retained history, the leader and every follower compute
        // the SAME accepted subset over the SAME candidates —
        // the follower's candidates ARE the leader's embedded set, so the subset round-trips identically (recreated
        // artifact === artifact). At `numShards = 1` (deps None) `candidateShardCheckpoints` is empty ⇒ this is empty too.
        adoptableShardCheckpoints <- shardAcceptanceDeps match {
          case Some(deps) if candidateShardCheckpoints.nonEmpty =>
            candidateShardCheckpoints.toList.traverseFilter {
              case (shardId, cp) =>
                if (shardId =!= cp.shardId)
                  none[(io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint)].pure[F]
                else
                  ShardWindowContinuation
                    .isAtomicallyContinuableF(shardId, cp, snapshotContext.lastStateChannelSnapshotHashes)
                    .flatMap {
                      case false =>
                        none[(io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint)]
                          .pure[F]
                      case true =>
                        deps.acceptanceManager.verifyEmbedded(cp).map {
                          case ShardCheckpointAcceptResult.Accepted => (shardId -> cp).some
                          case _ =>
                            none[(io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint)]
                        }
                    }
            }
              .map(entries => SortedMap.from(entries))
          case _ =>
            SortedMap
              .empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint]
              .pure[F]
        }

        // WATCHTOWER fraud proofs (W3a). PRODUCE path (`sourceShardCheckpoints = true`): peek the node-local `fraudProofPool` — the disputes
        // the gossip consumer staged locally — and embed them. FOLLOWER/validator path: thread the leader's embedded `incomingFraudProofs`
        // unchanged (split-safety — never re-source the local pool, so the recreated artifact's `fraudProofs` field round-trips byte-exactly).
        // GSAM's accept() re-validates each carried evidence DETERMINISTICALLY and slashes on UPHELD, so the leader/follower/peer fold the
        // SAME slash regardless of whose pool sourced it. Empty at `numShards = 1` / `noop` pool ⇒ byte-identical to the pre-watchtower path.
        effectiveFraudProofs <-
          if (sourceShardCheckpoints) fraudProofPool.peekAll
          else incomingFraudProofs.pure[F]

        acceptStartMs <- Async[F].monotonic.map(_.toMillis)
        (
          acceptanceResult,
          allowSpendBlockAcceptanceResult,
          tokenLockBlockAcceptanceResult,
          delegatedStakeAcceptanceResult,
          nodeCollateralAcceptanceResult,
          scSnapshots,
          returnedSCEvents,
          acceptedRewardTxs,
          snapshotInfo,
          stateProof,
          spendActions,
          updateNodeParameters,
          sharedArtifacts,
          delegatorRewardsMap,
          overlayHandle,
          // Task #12 slice 2b — the typed per-ordinal delta accept() applied. Staged hash-keyed below (at
          // the `overlay.commit` site, once `currentSnapshotHash` is known) for the gl0 changeset ring.
          stateChangesAccumulator,
          kesRegistrationAcceptanceResult
        ) <-
          globalSnapshotAcceptanceManager
            .accept(
              currentOrdinal,
              currentEpochProgress,
              lastArtifact.epochProgress,
              blocksForAcceptance.map(_.value),
              sortedAllowSpendEvents,
              sortedTokenLockEvents,
              scEvents.map(_.value),
              unpEventsForAcceptance.map(_.updateNodeParameters),
              sortedCdsEvents,
              sortedWdsEvents,
              sortedCncEvents,
              sortedWncEvents,
              snapshotContext,
              lastActiveTips,
              lastDeprecatedTips,
              rewardsWithFacilitators(lastFacilitators),
              StateChannelValidationType.Full,
              getGlobalSnapshotByOrdinal,
              // The parent's snapshot hash identifies the branch we're extending. Inside accept(),
              // `overlay.checkout(parentTip)` uses it as the branch view for prior reads; the
              // resulting handle's commit will eventually be associated under this id when the
              // wiring flips to MultiBranch (Phase J / #56.10). Until then `OverlayMode.Passthrough`
              // ignores the BranchId on every read/write — the byte-parity contract from #107
              // covers the rewire under that mode.
              io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(lastArtifactHash),
              // Gap C — the `verifyEmbedded`-accepted checkpoint subset. GSAM's `adoptShardCheckpoints` re-runs
              // `verifyEmbedded` and adopts each into `scSnapshots`; passing the already-accepted subset makes the
              // adopt-won `stateChannelSnapshots` correspond exactly to the embedded `shardCheckpoints` field below.
              // Empty at numShards=1 (regression bar): the adopt path only fires when `numShards > 1 && nonEmpty`.
              shardCheckpoints = adoptableShardCheckpoints,
              // WATCHTOWER fraud-proof artifact (W3a) — folded into the slash sink; the SAME map is embedded in the snapshot below so the
              // follower/validator threading `artifact.fraudProofs` recreates this call identically. Empty at numShards=1 / noop pool.
              fraudProofs = effectiveFraudProofs,
              kesRegistrationCerts = sortedKesRegistrationEvents
            )
        acceptEndMs <- Async[F].monotonic.map(_.toMillis)
        _ <- ConsensusLog.info(
          logger,
          Category.Proposal,
          currentOrdinal.show,
          "n/a",
          Event.AcceptTiming,
          "acceptDurationMs" -> (acceptEndMs - acceptStartMs).toString
        )
        _ <- ConsensusLog.info(
          logger,
          Category.Proposal,
          currentOrdinal.show,
          "n/a",
          Event.AcceptanceResults,
          "blocks.accepted" -> acceptanceResult.accepted.size.toString,
          "blocks.notAccepted" -> acceptanceResult.notAccepted.size.toString,
          "allowSpend.accepted" -> allowSpendBlockAcceptanceResult.accepted.size.toString,
          "tokenLock.accepted" -> tokenLockBlockAcceptanceResult.accepted.size.toString,
          "delegStakeCreate.accepted" -> delegatedStakeAcceptanceResult.acceptedCreates.size.toString,
          "delegStakeCreate.rejected" -> delegatedStakeAcceptanceResult.notAcceptedCreates.size.toString,
          "delegStakeWithdraw.accepted" -> delegatedStakeAcceptanceResult.acceptedWithdrawals.size.toString,
          "delegStakeWithdraw.rejected" -> delegatedStakeAcceptanceResult.notAcceptedWithdrawals.size.toString,
          "nodeCollCreate.accepted" -> nodeCollateralAcceptanceResult.acceptedCreates.size.toString,
          "nodeCollCreate.rejected" -> nodeCollateralAcceptanceResult.notAcceptedCreates.size.toString,
          "nodeCollWithdraw.accepted" -> nodeCollateralAcceptanceResult.acceptedWithdrawals.size.toString,
          "nodeCollWithdraw.rejected" -> nodeCollateralAcceptanceResult.notAcceptedWithdrawals.size.toString,
          "operatorKeyRegistration.accepted" -> kesRegistrationAcceptanceResult.accepted.size.toString,
          "operatorKeyRegistration.rejected" -> kesRegistrationAcceptanceResult.notAccepted.size.toString,
          "scSnapshots" -> scSnapshots.size.toString,
          "rewards" -> acceptedRewardTxs.size.toString
        )

        (deprecated, remainedActive, accepted) = getUpdatedTips(
          lastActiveTips,
          lastDeprecatedTips,
          acceptanceResult,
          currentOrdinal
        )

        (height, subHeight) <- getHeightAndSubHeight(lastArtifact, deprecated, remainedActive, accepted)

        returnedDAGEvents = getReturnedDAGEvents(acceptanceResult)

        acceptedDelegatedStakeCreates = delegatedStakeAcceptanceResult.acceptedCreates.view.mapValues(_.map(_._1)).toSortedMap
        acceptedDelegatedStakeWithdrawals = delegatedStakeAcceptanceResult.acceptedWithdrawals.view.mapValues(_.map(_._1)).toSortedMap
        acceptedNnodeCollateralCreates = nodeCollateralAcceptanceResult.acceptedCreates.view.mapValues(_.map(_._1)).toSortedMap
        acceptedNnodeCollateralWithdrawals = nodeCollateralAcceptanceResult.acceptedWithdrawals.view.mapValues(_.map(_._1)).toSortedMap

        globalSnapshot = GlobalIncrementalSnapshot(
          currentOrdinal,
          height,
          subHeight,
          lastArtifactHash,
          accepted,
          scSnapshots,
          // shardCheckpoints (§3.4) — embed the SAME `verifyEmbedded`-accepted subset GSAM adopted into `scSnapshots`
          // above, so the artifact's `shardCheckpoints` field and its `stateChannelSnapshots` (adopt-won) are mutually
          // consistent and a follower/validator threading `artifact.shardCheckpoints` back through accept() recreates
          // both byte-identically. Empty at numShards=1 (regression bar) — `adoptableShardCheckpoints` is empty there.
          adoptableShardCheckpoints,
          acceptedRewardTxs,
          delegatorRewardsMap.some,
          currentEpochProgress,
          GlobalSnapshot.nextFacilitators,
          SnapshotTips(
            deprecated = deprecated,
            remainedActive = remainedActive
          ),
          stateProof,
          SortedSet.from(allowSpendBlockAcceptanceResult.accepted).some,
          SortedSet.from(tokenLockBlockAcceptanceResult.accepted).some,
          SortedMap.from(spendActions).some,
          updateNodeParameters.some,
          sharedArtifacts.some,
          acceptedDelegatedStakeCreates.some,
          acceptedDelegatedStakeWithdrawals.some,
          acceptedNnodeCollateralCreates.some,
          acceptedNnodeCollateralWithdrawals.some,
          // WATCHTOWER fraud-proof consensus field (W3a) — embed the SAME map fed to `accept(fraudProofs=…)` above, so a follower/validator
          // threading `artifact.fraudProofs` back through accept() recreates this snapshot byte-identically (the `recreatedArtifact ===
          // artifact` round-trip). Empty at numShards=1 / noop pool (regression bar). `version`/`slotCertificate`/`eta` keep their defaults
          // here (SnapshotLeaderLoop `.copy(eta = …)`s eta later; that copy preserves this field).
          fraudProofs = effectiveFraudProofs,
          operatorKeyRegistrations = SortedSet.from(kesRegistrationAcceptanceResult.accepted.values.map(_.event))
        )
        // Phase J: commit the overlay handle once the artifact is built and we have the snapshot's
        // hash to use as the branch's `childTip`. Under MultiBranch this registers `currentSnapshotHash`
        // as a new branch in `pendingRef` (parent → this hash) so child snapshots can `checkout` against
        // it; under Passthrough this is a no-op (writes already committed inline). Failing to commit
        // here would leak the handle's accumulator (MultiBranch eviction would never see it).
        currentSnapshotHash <- hasher.hash(globalSnapshot)
        // 3c-A enabler (seal-time postBytes): capture the EXACT signed byte map from the UNCOMMITTED handle — the same
        // `allEntriesAsBytesWithHandle(handle, ordinal)` `accept()` derived the signed `mptRoot` from (GSAM:postBytes)
        // — BEFORE `overlay.commit` below registers the branch. Staged keyed by `currentSnapshotHash` after the commit;
        // only the FINALIZED hash's bytes are promoted to the served signed-bytes store, so a follower's
        // `consensusMptRoot(served)` reproduces the signed root BY CONSTRUCTION (the finalize-time `mpt_snapshot_info`
        // re-fold can diverge under MultiBranch — the 3c-A gap this closes).
        stagedPostBytes <- overlay.allEntriesAsBytesWithHandle(overlayHandle, currentOrdinal)
        _ <- overlay.commit(
          overlayHandle,
          io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(currentSnapshotHash),
          currentOrdinal
        )
        // Task #12 slice 2b — STAGE the typed per-ordinal delta keyed by this snapshot's hash, right where
        // the hash first becomes known (mirrors the `overlay.commit(BranchId(currentSnapshotHash), ...)` use
        // of the same hash above). `SnapshotLeaderLoop` PROMOTES this into the served ordinal-keyed changeset
        // ring iff the matching hash finalizes (depth-k OR attestation-2/3 sink), then removes it. Hash-keyed
        // (not ordinal) because two competing proposals at `currentOrdinal` must not collide pre-finality. The
        // steady-state bound is the finalize-sink watermark prune (`SnapshotLeaderLoop.pruneStagedAtOrBelow` drops
        // everything at-or-below the finalized tip); the size cap below is only a hard backstop for a runaway
        // never-finalizing-fork burst staged BETWEEN two finalize ticks.
        // Reached on BOTH the genuine produce path AND the follower/validator re-derivation (validateArtifact)
        // — both build a real candidate whose hash, if finalized, the sink promotes; that is intended.
        // Stage `(currentOrdinal, acc)` — the ordinal rides along so the finalize-sink can watermark-prune.
        // Task #19 — when the backstop trips, evict the LOWEST-ordinal staged entries (deterministic), NOT an
        // arbitrary hash-iteration `.drop`. The lowest ordinals are the furthest behind the depth-k (255) finality
        // window and so the least likely to still finalize; arbitrary `.drop` could instead discard a high-ordinal
        // entry about to finalize, which then never reaches the served ring → the ml0 follower hits a "ring-gap"
        // and is forced into a heavy full-GSI resync. A dropped never-finalizing entry only costs that one ordinal
        // the full-GSI fallback, never an incorrect adopt (the signed `mptRoot` rejects a wrong base). Cap is the
        // typed `nakamoto.staging-accumulators-cap` HOCON value (default 2048), threaded in from `make`.
        _ <- pendingAccumulatorsRef.update { staged =>
          val updated = staged.updated(currentSnapshotHash, (currentOrdinal, stateChangesAccumulator))
          if (updated.size > stagingAccumulatorsCap) {
            // Drop the `excess` entries with the smallest ordinal (ties broken by hash for determinism).
            val excess = updated.size - stagingAccumulatorsCap
            val toEvict = updated.toList.sortBy { case (h, (o, _)) => (o.value.value, h.value) }
              .take(excess)
              .map(_._1)
              .toSet
            updated.filterNot { case (h, _) => toEvict.contains(h) }
          } else updated
        }
        // 3c-A enabler: stage the captured SIGNED bytes keyed by `currentSnapshotHash`, mirroring the accumulator
        // staging above (same lowest-ordinal eviction backstop, same cap). `SnapshotLeaderLoop.recordFinalizedAccumulator`
        // promotes the finalized hash's bytes to the served signed-bytes store and watermark-prunes the rest — so only a
        // FINALIZED branch's signed bytes are ever served (a reorg loser's bytes never promote).
        _ <- pendingPostBytesRef.update { staged =>
          val updated = staged.updated(currentSnapshotHash, (currentOrdinal, stagedPostBytes))
          if (updated.size > stagingAccumulatorsCap) {
            val excess = updated.size - stagingAccumulatorsCap
            val toEvict = updated.toList.sortBy { case (h, (o, _)) => (o.value.value, h.value) }
              .take(excess)
              .map(_._1)
              .toSet
            updated.filterNot { case (h, _) => toEvict.contains(h) }
          } else updated
        }
        returnedEvents = returnedSCEvents.map(StateChannelEvent(_)) ++ returnedDAGEvents ++
          kesRegistrationAcceptanceResult.notAccepted.map(_._1).map(KesRegistrationCertEvent(_))
        _ <- ConsensusLog.info(
          logger,
          Category.Proposal,
          currentOrdinal.show,
          "n/a",
          Event.ArtifactBuilt,
          "height" -> globalSnapshot.height.show,
          "subHeight" -> globalSnapshot.subHeight.show,
          "epoch" -> globalSnapshot.epochProgress.show,
          "stateProof.mptRoot" -> globalSnapshot.stateProof.mptRoot.map(_.show.take(12)).getOrElse("none"),
          "stateProof.balances" -> globalSnapshot.stateProof.balancesProof.show.take(12),
          "stateProof.delegStakes" -> globalSnapshot.stateProof.activeDelegatedStakes.map(_.show.take(12)).getOrElse("none"),
          "stateProof.nodeCollaterals" -> globalSnapshot.stateProof.activeNodeCollaterals.map(_.show.take(12)).getOrElse("none")
        )
        _ <- rewardsService.calculateAndStoreRewardsInfo(globalSnapshot, snapshotInfo)
      } yield (globalSnapshot, snapshotInfo, returnedEvents)
    }

    private def getReturnedDAGEvents(
      acceptanceResult: BlockAcceptanceResult
    ): Set[GlobalSnapshotEvent] =
      acceptanceResult.notAccepted.mapFilter {
        case (signedBlock, _: BlockAwaitReason) => DAGEvent(signedBlock).some
        case _                                  => none
      }.toSet
  }

}
