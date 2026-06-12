package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.Map

import io.constellationnetwork.node.shared.config.types.ShardingConfig
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotStateChannelEventsProcessor,
  ShardCheckpointGl0AcceptanceManager
}
import io.constellationnetwork.numerics.Ratio
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
  *
  * '''ONE instance per node (task #44).''' [[acceptanceDeps]] is invoked EXACTLY ONCE per node, inside `SharedServices.make`, which now
  * exposes the result on `SharedServices.shardAcceptanceDeps`. The gl0-leader produce path (`GlobalSnapshotConsensus.make`) REUSES that
  * same instance rather than building a second one. Both GSAMs then forward the SAME `Option`-tuple into the three sharding parameters
  * (`shardingConfig`, `shardCheckpointAcceptanceManager`, `shardAssignment`), and the shard producers / sync daemon project off the SAME
  * `registry`. This matters because [[AcceptanceDeps]] carries STATEFUL Refs (per-shard chain stores, tip trackers, finality triggers,
  * binary buffers, the committee cache, and the adopted watermarks) — two instances meant the follower's adoptions landed in a registry the
  * producers/daemon/#42-ANCHOR-REORG-healer never read (eternal awaiting-embed). The deterministic inputs were always identical (#261
  * split-safety: same genesis KES/VRF registries, same `kDraw`/`kQuorum`, same seedlist, same MPT-committed eta resolver), so collapsing to
  * one instance changes nothing deterministic — it only makes adopt ↔ produce ↔ heal share the SAME state.
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
  * '''committeeMembership(shardId, epoch) — real VRF-VK sortition.''' The acceptance manager's pre-check confirms each checkpoint signer is
  * in `committeeMembership(shardId, epoch)`; the admit quorum `verifyEmbedded` requires is the DECOUPLED cluster-uniform `kQuorum`
  * (`nakamoto.committee.kQuorum`), NOT `|committeeMembership(shardId, epoch)|` (the enumerated draw size is logged for diagnostics only).
  * [[committeeFor]] materializes that committee as a DETERMINISTIC VRF-VK-sortitioned SUBSET of the active operator set sized by the DRAW
  * target `kDraw` (size `≈ kDraw`, NOT the full `N` — though the testnet default `kDraw = N` saturates the threshold so it IS everyone) —
  * so only a metagraph's shard committee re-executes it (genuine execution segmentation). The draw is a deterministic pseudo-random
  * sortition keyed on each operator's registered VRF *VK* + the epoch eta + the shardId (see [[committeeFor]] scaladoc for the determinism
  * argument and `CommitteeSortition.isInShardCommittee` for why a VK-seeded PRF, not a true per-operator VRF eval, is the only
  * enumerable-by-a-non-member option). The structural VRF-proof check + Ed25519 + KES product sig per signer still authenticate "did this
  * specific peer sign"; the sortitioned set gates "is this peer even in shard S's committee". v1 trade-off (acceptable per design §10 —
  * honest-testnet, slashing is the v2 backstop): the committee is PREDICTABLE because VKs are public; Algorand player-replaceability is a
  * v2 hardening (would require carrying a true per-operator VRF proof on each `CommitteeMemberSignature` and verifying it with
  * `CommitteeSortition.verifyMembership`, plus the producer/emitter signing a committee-VRF proof rather than the leader-VRF proof they
  * sign today).
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

  /** The per-shard consumer-side state bundle for one shard. Held in the registry the acceptance manager's lookups close over.
    *
    * '''binaryBuffer (EXECUTION-SHARDING R-1 — the inversion intake).''' The per-shard raw-binary accumulator. The daemon's gossip-handler
    * buffers each received metagraph binary for ITS shard here; the producer-fan-out reads `binaryBuffer.snapshotPending` from the SAME
    * instance. This is what decouples shard-checkpoint production from gl0's post-chain-link `stateChannelSnapshots` map (see
    * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer]] for the determinism model — leader-proposes /
    * members-attest, so node-local selection is fine).
    */
  final case class ShardRegistryEntry[F[_]](
    chainStore: ShardChainStore[F],
    tipTracker: ShardTipTracker[F],
    finalityTriggers: ShardFinalityTriggers[F],
    binaryBuffer: ShardBinaryBuffer[F]
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
    registry: Map[ShardId, ShardRegistryEntry[F]],
    /** The SAME deterministic `committeeFor(shardId, epoch)` draw the acceptance manager's pre-check / quorum uses. Exposed so the
      * produce/attest gates (`ShardCheckpointFanOut`, the `ShardCheckpointAttestationEmitter` call site) reuse the IDENTICAL committee set:
      * a node produces/attests for shard `s` only when it is in `committeeMembership(s, epoch)`. Routing both the gate and the verifier
      * through one closure keeps "who is in the committee" defined in exactly one place.
      */
    committeeMembership: (ShardId, EtaPeriod) => F[Set[PeerId]]
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
    *     does NOT cause a false slash: `GlobalSnapshotAcceptanceManager.adoptShardCheckpoints` only LOGS the rejected-mismatch signer list
    *     (the slash penalty is a separate slice, S2.0). So the worst case is "degraded-shard non-quorum checkpoints are not admitted until
    *     quorum returns", never "honest signers slashed".
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
    * @param kDraw
    *   committee DRAW target (cluster-uniform `cfg.nakamoto.committee.kDraw`). Threaded into [[committeeFor]]'s `isInShardCommittee` draw —
    *   sizes the enumerated shard committee (`≈ kDraw`; `= N` saturates ⇒ everyone). Decoupled from the admit quorum.
    * @param kQuorum
    *   committee ADMIT quorum (cluster-uniform `cfg.nakamoto.committee.kQuorum`). The distinct-attester count `verifyEmbedded` /
    *   `ShardFinalityTriggers.tCountShard` require — DIRECTLY (no 2/3 of the draw). Invariant `0 < kQuorum <= kDraw` enforced at load.
    * @param selfPeerId
    *   this gl0 operator's PeerId. Threaded into each per-shard [[ShardTipTracker]] (self-exclusion for `T_count_shard`) and the acceptance
    *   manager (diagnostic logging of which op spotted a deviation).
    * @param kesRegistry
    *   registered KES master VKs. Used by the acceptance manager's per-signer KES product-sig verification (registry-absent carve-out for
    *   the bootstrap window).
    * @param vrfRegistry
    *   registered per-operator VRF verification keys. CONSUMED by [[committeeFor]] — each operator's registered VK is the per-operator seed
    *   for the deterministic shard-committee draw. MUST be the genesis/seedlist-loaded registry (identical cluster-wide) on EVERY path that
    *   runs `verifyEmbedded` (produce + validateArtifact + the follower createContext), or the committee — and thus the adopt decision —
    *   diverges and the cluster splits (#261). An operator absent from the registry is never sortitioned into any committee.
    * @param activeValidators
    *   callback returning the current active gl0 validator set — the candidate pool [[committeeFor]] sortitions over (also fixes `σ =
    *   1/N`). Read on every checkpoint pre-check so a validator-set change (registration/slashing) is observed without reconstruction. MUST
    *   be identical cluster-wide (seedlist minus `metagraph-op`).
    * @param etaForEpoch
    *   resolves the 32 raw eta-randomness bytes for a given eta-period — the SAME `EtaStateManager.getEta`-backed resolver the GSAM
    *   boundary writer uses (returns a hex [[Hash]] at the call site; decode to 32 bytes via `Hex(h.value).toBytes`). Feeds
    *   [[committeeFor]] keyed on the wire-carried `checkpoint.epoch`, so producer + every verifier draw the SAME committee for the SAME
    *   epoch even across an eta boundary. MPT-committed ⇒ byte-identical cluster-wide.
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
    kDraw: Int,
    kQuorum: Int,
    selfPeerId: PeerId,
    kesRegistry: KesRegistry[F],
    vrfRegistry: VrfRegistry[F],
    activeValidators: F[Set[PeerId]],
    etaForEpoch: EtaPeriod => F[Array[Byte]],
    reExecuteDerivation: Option[(Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash]] = None
  ): F[Option[AcceptanceDeps[F]]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring")
    val reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash] =
      reExecuteDerivation.getOrElse(noReExecDerivation[F])

    if (cfg.numShards <= 1)
      // Regression bar: at the production default `numShards = 1`, construct NOTHING and return None. The two GSAM call sites then pass
      // `None` for all three sharding params — byte-identical to the pre-wiring call. The single debug log is the only observable effect.
      logger
        .debug(s"sharding inactive (numShards=${cfg.numShards} <= 1); GSAM sharding deps = None (regression bar)")
        .as(Option.empty[AcceptanceDeps[F]])
    else
      for {
        registry <- buildRegistry[F](cfg, kQuorum, selfPeerId)
        // MEMOIZE the committee draw per (shardId, epoch) (2026-06-10, run b31yifl23). `committeeFor` runs one
        // VRF-sortition Hasher.hash PER VALIDATOR (~N hashes) every call, and the closure is invoked once per
        // checkpoint per CANDIDATE BRANCH during `verifyEmbedded`/`preCheck` — so under a near-tip reorg storm
        // (1650 reorg events observed) it fired ~N×1650 ≈ 165k hashes/run, the allocation spike that drove 4/5
        // gl0 nodes across the -Xmx cliff into G1 death-thrash (HTTP starved → e2e poll timeout). The committee is
        // a DETERMINISTIC function of (shardId, epoch) alone — `activeValidators` (seedlist), `vrfRegistry`
        // (genesis/registration), `etaForEpoch(epoch)` (MPT-committed), `kDraw`/σ are all cluster-uniform — so the
        // cache is semantically identical to recomputing (split-safe; pure optimization). Unbounded growth is a
        // non-issue: cardinality = numShards × in-flight epochs (~handful). Mirrors EtaStateManager.walkCacheRef.
        committeeCache <- Ref.of[F, Map[(ShardId, EtaPeriod), Set[PeerId]]](Map.empty)
        committeeMembership = (shardId: ShardId, epoch: EtaPeriod) =>
          committeeCache.get.flatMap { cache =>
            cache.get((shardId, epoch)) match {
              case Some(members) => Async[F].pure(members)
              case None =>
                committeeFor[F](shardId, epoch, activeValidators, vrfRegistry, etaForEpoch, kDraw).flatTap { members =>
                  committeeCache.update(_.updated((shardId, epoch), members))
                }
            }
          }
        acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[F](
          finalityTriggers = (sid: ShardId) => Async[F].pure(registry.get(sid).map(_.finalityTriggers)),
          chainStore = (sid: ShardId) => Async[F].pure(registry.get(sid).map(_.chainStore)),
          committeeMembership = committeeMembership,
          kDraw = kDraw,
          kQuorum = kQuorum,
          selfPeerId = selfPeerId,
          kesRegistry = kesRegistry,
          reExecuteDerivation = reExec
        )
        shardAssignment = ShardAssignment.make[F](cfg.numShards)
        _ <- logger.info(
          s"sharding ACTIVE: numShards=${cfg.numShards} kDraw=$kDraw kQuorum=$kQuorum " +
            s"k1Shard=${cfg.finality.k1Shard} — built per-shard registry (${registry.size} shards) + gl0 acceptance manager " +
            s"(real VRF-VK committee sortition)"
        )
      } yield Some(AcceptanceDeps(cfg, acceptanceManager, shardAssignment, registry, committeeMembership))
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
    kQuorum: Int,
    selfPeerId: PeerId
  ): F[Map[ShardId, ShardRegistryEntry[F]]] =
    (0 until cfg.numShards).toList.traverse { idx =>
      val shardId = ShardId(NonNegInt.unsafeFrom(idx))
      for {
        chainStore <- ShardChainStore.make[F](shardId, keepDepthBehindFinalized = cfg.finality.k1Shard)
        tipTracker <- ShardTipTracker.make[F](shardId, selfPeerId)
        triggers <- ShardFinalityTriggers.make[F](
          shardId = shardId,
          kQuorum = kQuorum,
          k1Shard = cfg.finality.k1Shard,
          chainStore = chainStore,
          tipTracker = tipTracker
        )
        // R-1: the per-shard raw-binary accumulator. The SAME instance feeds the gossip-intake (daemon) and the producer-fan-out — that
        // shared instance IS the inversion: the producer reads buffered binaries here instead of gl0's post-chain-link map.
        binaryBuffer <- ShardBinaryBuffer.make[F](shardId, cap = cfg.checkpoint.binaryBufferCap)
      } yield shardId -> ShardRegistryEntry(chainStore, tipTracker, triggers, binaryBuffer)
    }
      .map(_.toMap)

  /** Real VRF-VK-sortitioned `committeeFor(shardId, epoch)` — the deterministic committee SET for one `(shard, epoch)`.
    *
    * '''Algorithm.''' Enumerate the active operators in a STABLE order (sorted by `PeerId`, so iteration is order-independent); for each
    * operator look up its registered VRF VK in `vrfRegistry` and keep it iff `CommitteeSortition.isInShardCommittee(vrfVk, eta, shardId,
    * epoch, σ, kDraw)` — i.e. its `H(eta, shardId, epoch, vrfVk)` draw value falls below `threshold(kDraw, σ)`. An operator with NO
    * registered VK cannot be sortitioned (no seed) ⇒ excluded. `σ` (per-operator stake share) is the uniform `1/N` rule (`committeeStake`,
    * `[[project-216-committee-stake-drift-fix]]`), computed here from `N = |activeValidators|` so `threshold = kDraw/N` and the expected
    * committee size is `≈ kDraw` — the sortitioned committee `K_S`, NOT the full set `N` (and `kDraw = N` saturates to everyone). The admit
    * quorum is the SEPARATE `kQuorum`, applied in `verifyEmbedded` / `ShardFinalityTriggers`, NOT this draw.
    *
    * '''Determinism (the #261 split invariant).''' Every input is identical on every gl0 node:
    *   - `activeValidators` — seedlist (minus `metagraph-op`), loaded identically cluster-wide;
    *   - each operator's VRF VK — from `L0GenesisData.operators[].vrfPublicKey` (or the runtime registration cert), the SAME genesis bytes
    *     on every node (`VrfRegistry`/`L0GenesisLoader.buildVrfRegistry`);
    *   - `eta` — `etaForEpoch(epoch)` resolves the per-period eta from the MPT-committed `HistoricalStakeSnapshot.eta` (`EtaStateManager`),
    *     byte-identical cluster-wide once the boundary is written, keyed on the WIRE-CARRIED `checkpoint.epoch`;
    *   - `kDraw` — HOCON `nakamoto.committee.k-draw`, the same on every node;
    *   - `σ = 1/N` — derived from the same `activeValidators`. No node-local state (no chain height, no Refs, no wall-clock) enters the
    *     draw, so `committeeFor(shardId, epoch)` resolves to the SAME `Set[PeerId]` on every node — the hard requirement for
    *     `verifyEmbedded` to admit byte-identically.
    *
    * The result is returned as a sorted-order traversal collapsed into a `Set` (membership + size are all the callers need); the traversal
    * order does not affect the resulting set.
    */
  private[sharding] def committeeFor[F[_]: Async: Hasher](
    shardId: ShardId,
    epoch: EtaPeriod,
    activeValidators: F[Set[PeerId]],
    vrfRegistry: VrfRegistry[F],
    etaForEpoch: EtaPeriod => F[Array[Byte]],
    kDraw: Int
  ): F[Set[PeerId]] =
    (activeValidators, etaForEpoch(epoch)).tupled.flatMap {
      case (validators, eta) =>
        val n = validators.size
        if (n <= 0) Async[F].pure(Set.empty[PeerId])
        else {
          val sigma = Ratio(1, n) // uniform per-operator stake share (committeeStake): threshold = kDraw/N ⇒ E[|committee|] ≈ kDraw
          // Stable iteration order (sorted by PeerId) so the fold is order-independent; the result is a Set so order is moot anyway.
          validators.toList
            .sortBy(_.value.value)
            .traverse { peerId =>
              vrfRegistry.getVrfVk(peerId).flatMap {
                case None => Async[F].pure(Option.empty[PeerId]) // no registered VRF VK ⇒ not sortitionable ⇒ excluded
                case Some(vrfVk) =>
                  CommitteeSortition
                    .isInShardCommittee[F](vrfVk, eta, shardId, epoch, sigma, kDraw)
                    .map(if (_) Some(peerId) else None)
              }
            }
            .map(_.flatten.toSet)
        }
    }
}
