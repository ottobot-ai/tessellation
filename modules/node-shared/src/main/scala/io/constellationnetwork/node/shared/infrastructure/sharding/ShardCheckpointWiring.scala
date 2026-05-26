package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.config.types.ShardingConfig
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardFinalityTriggers, ShardTipTracker}
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, ShardAssignment, VrfRegistry}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotStateChannelEventsProcessor,
  ShardCheckpointGl0AcceptanceManager
}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegInt
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Production-wiring helper for the hierarchical-shard-checkpoints v1 ACCEPTANCE side (priority 1 of the production-wiring slice).
  *
  * Constructs the per-shard consumer/admission infrastructure once, in a single place, so the TWO `GlobalSnapshotAcceptanceManager.make`
  * call sites — `SharedServices.make` (verify path) and `GlobalSnapshotConsensus.make` (gl0-leader produce path) — stay byte-consistent.
  * Both sites call [[acceptanceDeps]] and forward the returned `Option`-tuple straight into the three GSAM sharding parameters
  * (`shardingConfig`, `shardCheckpointAcceptanceManager`, `shardAssignment`).
  *
  * '''numShards = 1 regression bar.''' [[acceptanceDeps]] returns `None` whenever `cfg.numShards <= 1` (the production default). At `None`
  * the two GSAM call sites pass `None` for all three sharding params — exactly today's call — so `accept()` is byte-identical to the
  * pre-wiring code path. NOTHING is constructed on the `None` branch: no per-shard stores, no acceptance manager, no log lines beyond a
  * single one-time INFO. The activation gate (`numShards > 1`) wraps every allocation. See
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 13 and the GSAM `shardingConfig` scaladoc.
  *
  * '''Per-shard registry.''' On the activated branch we build a `Map[ShardId, (ShardChainStore, ShardTipTracker, ShardFinalityTriggers)]`
  * for shards `0 .. numShards - 1`. Each entry is the consumer-side state for one shard the gl0 operator tracks: the fork-DAG chain store
  * (Slice 5), the committee attestation tracker (Slice 6), and the composite Phase 1→2 finality triggers (Slice 6). The acceptance
  * manager's `finalityTriggers` / `chainStore` callbacks are simple `Map.get` lookups against this registry — `None` ⇒ "shard not tracked
  * locally" ⇒ reject, exactly as the manager's scaladoc specifies.
  *
  * '''committeeMembership(shardId, epoch) — v1 simplification.''' The acceptance manager's pre-check confirms each checkpoint signer is in
  * `committeeMembership(shardId, epoch)`. The authoritative per-`(shard, epoch)` committee draw is a VRF sortition over each operator's VRF
  * VK — but v1 has no cluster-wide VRF-VK registry (only the [[KesRegistry]] exists; see the manager's `verifyVrfStructural` scaladoc and
  * `[[project-cross-shard-cq-collapse-bound]]`). So for v1 the membership predicate returns the full active validator set: "a peer may be
  * in shard S's committee iff it's a known gl0 operator". This is sound at the admission layer because the manager STILL authenticates
  * every signer cryptographically — Ed25519 over the canonical preimage (recovered VK from the PeerId) + KES product sig (registry
  * carve-out for the bootstrap window) + a structural VRF-proof check. The set-membership predicate only gates "is this peer even an
  * operator"; the crypto gates "did this specific peer actually sign". Replacing the full-set predicate with a real VRF-enumerated draw is
  * the v2 follow-up (needs the VRF-VK registry + `CommitteeSortition.verifyMembership` per peer — mirrors `MetagraphCommitteeGate`'s
  * receiver path, which verifies a single arriving attestation rather than enumerating the whole committee). Until then the
  * structural+crypto checks carry the admission safety bar.
  *
  * '''reExecuteDerivation — caller-supplied (S3: real committee re-execution).''' The `T_depth1_shard` degraded path re-runs each MG's
  * derivation and compares the recomputed `mptRoot` byte-for-byte against the committee-signed value. The closure is supplied as a
  * parameter so each call site passes the SAME [[reExecDerivation]] closure built from its own
  * [[GlobalSnapshotStateChannelEventsProcessor]] (the SAME processor gl0 uses for metagraph snapshots) — that is what makes the producer's
  * `perMetagraphMptRoots` and every verifier's recomputed roots byte-identical. The `None`/[[noReExecDerivation]] fallback (fail-closed
  * `Hash.empty`) remains for callers that have not wired a processor.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): every tunable is read off the typed [[ShardingConfig]] passed in by the
  * caller from `cfg.nakamoto.sharding`. No `sys.env.get` anywhere in this helper.
  */
object ShardCheckpointWiring {

  /** The per-shard consumer-side state bundle for one shard. Held in the registry the acceptance manager's lookups close over. */
  final case class ShardRegistryEntry[F[_]](
    chainStore: ShardChainStore[F],
    tipTracker: ShardTipTracker[F],
    finalityTriggers: ShardFinalityTriggers[F]
  )

  /** The acceptance-side sharding dependencies, returned as the exact `Option`-tuple the two GSAM call sites forward into
    * `GlobalSnapshotAcceptanceManager.make`'s `(shardingConfig, shardCheckpointAcceptanceManager, shardAssignment)` params.
    *
    * `Some(...)` ⇒ `numShards > 1`, the activated path. `None` ⇒ `numShards <= 1`, the regression-bar path (call sites pass all-`None`).
    *
    * `registry` is exposed so the producer-side wiring (priority 2, deferred) can reuse the SAME per-shard chain stores rather than
    * building a second disjoint set — the producer writes into the chain store the consumer reads from.
    */
  final case class AcceptanceDeps[F[_]](
    shardingConfig: ShardingConfig,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[F],
    shardAssignment: ShardAssignment[F],
    registry: Map[ShardId, ShardRegistryEntry[F]]
  )

  /** Fail-closed fallback for the `T_depth1_shard` re-exec derivation. Returns a fixed sentinel hash for every `(metagraphAddress,
    * includedChain, gl0AnchorOrdinal)`. Used only by callers that have NOT wired a real [[reExecDerivation]] closure; both production sites
    * now pass the real one.
    *
    * '''Safety analysis.''' The re-exec path fires ONLY when a shard is degraded (no committee quorum — `T_count_shard` did not qualify —
    * but the chain advanced past `k1Shard`). On that path the manager compares this derivation's output against the committee-signed
    * `perMetagraphMptRoots(mg)`. With a fixed sentinel:
    *   - For an empty-window checkpoint (`includedSnapshots` empty — a T_alive liveness ping) there is nothing to compare, so the path
    *     reduces to pre-check + finality and accepts cleanly.
    *   - For a non-empty-window checkpoint the sentinel will (almost surely) NOT equal the real committed root, so the manager returns
    *     `RejectedReExecutionMismatch`. The checkpoint is DROPPED (fail-closed) — the binaries do NOT enter the gl0 snapshot. This is the
    *     conservative outcome: a degraded shard's non-quorum checkpoint is rejected rather than admitted on an unverified derivation. It
    *     does NOT cause a false slash: `GlobalSnapshotAcceptanceManager.processShardCheckpoints` only LOGS the rejected-mismatch signer
    *     list (the slash penalty is a separate slice, S2.0). So the worst case is "degraded-shard non-quorum checkpoints are not admitted
    *     until quorum returns", never "honest signers slashed".
    */
  def noReExecDerivation[F[_]: Async]: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash] =
    (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal) => Async[F].pure(Hash.empty)

  /** S3 committee re-execution closure — the SINGLE definition of "re-run this metagraph's derivation and compute its per-MG root", shared
    * by the producer (`ShardCheckpointProducer.derivePerMgState`) and the gl0 verifier
    * (`ShardCheckpointGl0AcceptanceManager.reExecuteDerivation`). Defining it once here is what guarantees both sides run the IDENTICAL
    * function over the same inputs — the byte-identity contract that prevents false-slashing (see
    * [[GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot]]'s determinism scaladoc).
    *
    * Delegates to `processor.deriveMetagraphRoot`, passing the checkpoint's wire-carried `gl0AnchorOrdinal` as the derivation ordinal (so
    * the fee-required cutover is computed identically on producer + verifier) and a CONSTANT `_ => None` global-snapshot lookup. The no-op
    * lookup makes the root a pure function of `(metagraphAddress, includedChain, gl0AnchorOrdinal)` — zero reads of the live `MptStore` or
    * snapshot storage, so every node (producer + every committee verifier) computes byte-identical roots regardless of its local chain
    * height. `processCurrencySnapshots` swallows any per-snapshot apply failure (its `handleErrorWith` keeps the prior state), so a `None`
    * lookup degrades deterministically rather than diverging.
    *
    * @param processor
    *   the same `GlobalSnapshotStateChannelEventsProcessor` instance gl0 uses for metagraph-snapshot acceptance (its
    *   `processCurrencySnapshots` IS the canonical derivation).
    */
  def reExecDerivation[F[_]: Async: Hasher](
    processor: GlobalSnapshotStateChannelEventsProcessor[F]
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash] = {
    // Pure-by-construction global-snapshot lookup: NEVER reads storage, so the derivation cannot pick up a node-local view.
    val noGlobalSnapshotLookup: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]] =
      (_: SnapshotOrdinal) => Async[F].pure(Option.empty[Hashed[GlobalIncrementalSnapshot]])

    (mg: Address, binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]], gl0AnchorOrdinal: SnapshotOrdinal) =>
      processor.deriveMetagraphRoot(mg, binaries, gl0AnchorOrdinal, noGlobalSnapshotLookup)(Hasher[F])
  }

  /** Build the acceptance-side sharding dependencies, gated on `cfg.numShards > 1`.
    *
    * @param cfg
    *   the typed [[ShardingConfig]] from `cfg.nakamoto.sharding`. `numShards <= 1` ⇒ returns `None` (regression bar; nothing constructed).
    * @param selfPeerId
    *   this gl0 operator's PeerId. Threaded into each per-shard [[ShardTipTracker]] (self-exclusion for `T_count_shard`) and the acceptance
    *   manager (diagnostic logging of which op spotted a deviation).
    * @param kesRegistry
    *   registered KES master VKs. Used by the acceptance manager's per-signer KES product-sig verification (registry-absent carve-out for
    *   the bootstrap window).
    * @param vrfRegistry
    *   registered per-operator VRF verification keys (Slice S1). Threaded as an AVAILABLE dependency so a later slice (S2) can swap the
    *   full-set `committeeMembership` predicate for a real per-signer `CommitteeSortition.verifyShardMembership(vrfVk, …)`. As of S1 it is
    *   NOT consumed — `committeeFor` still returns the full active validator set, so behavior is byte-identical at every `numShards`.
    * @param activeValidators
    *   callback returning the current active gl0 validator set. Used by the v1 `committeeMembership` predicate (full-set membership — see
    *   the object scaladoc). Read on every checkpoint pre-check so a validator-set change (registration/slashing) is observed without
    *   reconstruction.
    * @param reExecuteDerivation
    *   the `T_depth1_shard` re-exec derivation closure `(metagraphAddress, includedChain) => F[Hash]`. `Some(...)` (S3 wiring) ⇒ the real
    *   `GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot` closure — committee re-execution that recomputes the per-MG root and
    *   rejects (+ flags slash signers) on a byte-mismatch. `None` (the default) ⇒ [[noReExecDerivation]] (fail-closed: degraded-path
    *   non-empty checkpoints are rejected on the `Hash.empty` sentinel, never falsely admitted). Modelled as `Option` rather than a
    *   defaulted closure because Scala can't resolve `Async[F]` for `noReExecDerivation[F]` at the default-arg site (the context bound is
    *   on the method, not on the default expression) — the same constraint the GSAM `localEventsPublisher` param hits.
    */
  def acceptanceDeps[F[_]: Async: Hasher: SecurityProvider: Metrics](
    cfg: ShardingConfig,
    selfPeerId: PeerId,
    kesRegistry: KesRegistry[F],
    vrfRegistry: VrfRegistry[F],
    activeValidators: F[Set[PeerId]],
    reExecuteDerivation: Option[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash]] = None
  ): F[Option[AcceptanceDeps[F]]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring")
    val reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash] =
      reExecuteDerivation.getOrElse(noReExecDerivation[F])
    // Slice S1: the VRF-VK registry is plumbed through but not yet consumed — `committeeFor` still returns the
    // full active validator set (see object scaladoc). Bound here so the param is wired end-to-end ahead of the
    // S2 swap to `CommitteeSortition.verifyShardMembership`. Referenced to keep the unused-param warning silent.
    val _ = vrfRegistry

    if (cfg.numShards <= 1)
      // Regression bar: at the production default `numShards = 1`, construct NOTHING and return None. The two GSAM call sites then pass
      // `None` for all three sharding params — byte-identical to the pre-wiring call. The single debug log is the only observable effect.
      logger
        .debug(s"sharding inactive (numShards=${cfg.numShards} <= 1); GSAM sharding deps = None (regression bar)")
        .as(Option.empty[AcceptanceDeps[F]])
    else
      for {
        registry <- buildRegistry[F](cfg, selfPeerId)
        committeeMembership = (shardId: ShardId, epoch: EtaPeriod) => committeeFor[F](shardId, epoch, activeValidators)
        acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[F](
          finalityTriggers = (sid: ShardId) => Async[F].pure(registry.get(sid).map(_.finalityTriggers)),
          chainStore = (sid: ShardId) => Async[F].pure(registry.get(sid).map(_.chainStore)),
          committeeMembership = committeeMembership,
          kTarget = cfg.committeeKTarget,
          selfPeerId = selfPeerId,
          kesRegistry = kesRegistry,
          reExecuteDerivation = reExec
        )
        shardAssignment = ShardAssignment.make[F](cfg.numShards)
        _ <- logger.info(
          s"sharding ACTIVE: numShards=${cfg.numShards} committeeKTarget=${cfg.committeeKTarget} " +
            s"k1Shard=${cfg.finality.k1Shard} — built per-shard registry (${registry.size} shards) + gl0 acceptance manager"
        )
      } yield Some(AcceptanceDeps(cfg, acceptanceManager, shardAssignment, registry))
  }

  /** Construct the per-shard `(ShardChainStore, ShardTipTracker, ShardFinalityTriggers)` registry for shards `0 .. numShards - 1`.
    *
    * Each shard's chain store is bounded by `finality.k1Shard` (the per-shard keep-window — same value drives the `T_depth1_shard` depth
    * fallback). The triggers are constructed but NOT advanced here: the consumer-side tick loop (or each `evaluate` call's read) observes
    * the Refs; advancing happens once checkpoints arrive. Exposed package-privately so the focused wiring test can assert the registry
    * shape directly without standing up the full acceptance manager.
    */
  private[sharding] def buildRegistry[F[_]: Async: Hasher: Metrics](
    cfg: ShardingConfig,
    selfPeerId: PeerId
  ): F[Map[ShardId, ShardRegistryEntry[F]]] =
    (0 until cfg.numShards).toList.traverse { idx =>
      val shardId = ShardId(NonNegInt.unsafeFrom(idx))
      for {
        chainStore <- ShardChainStore.make[F](shardId, keepDepthBehindFinalized = cfg.finality.k1Shard)
        tipTracker <- ShardTipTracker.make[F](shardId, selfPeerId)
        triggers <- ShardFinalityTriggers.make[F](
          shardId = shardId,
          kTarget = cfg.committeeKTarget,
          k1Shard = cfg.finality.k1Shard,
          chainStore = chainStore,
          tipTracker = tipTracker
        )
      } yield shardId -> ShardRegistryEntry(chainStore, tipTracker, triggers)
    }
      .map(_.toMap)

  /** v1 `committeeMembership(shardId, epoch)` — returns the full active validator set (see object scaladoc for the safety rationale and the
    * v2 VRF-enumeration follow-up). `shardId` / `epoch` are accepted to satisfy the manager's callback shape and to leave a single
    * touch-point for the v2 swap; v1 ignores them because the membership predicate is "is this peer an operator", not "did this peer win
    * shard S's VRF draw for this epoch".
    */
  private[sharding] def committeeFor[F[_]](
    shardId: ShardId,
    epoch: EtaPeriod,
    activeValidators: F[Set[PeerId]]
  ): F[Set[PeerId]] = {
    val _ = (shardId, epoch) // referenced to keep the unused-warning silent; v2 enumerates per (shard, epoch).
    activeValidators
  }
}
