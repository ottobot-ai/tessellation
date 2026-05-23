package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardMetrics
import io.constellationnetwork.schema.sharding.{ShardCheckpoint, ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Per-shard chain store — Slice 5 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.5.
  *
  * Holds the fork-DAG of `Signed[ShardCheckpoint]`s produced by a single shard's mini-Taktikos chain. Each shard committee maintains one
  * instance per shard it participates in. The store is analogous to `NakamotoChainStore` for gl0 (see
  * `modules/dag-l0/src/main/scala/io/constellationnetwork/dag/l0/infrastructure/snapshot/nakamoto/NakamotoChainStore.scala`) but scoped to
  * a single shard rather than the global chain.
  *
  * '''Scope''' (per design doc §5.5):
  *   - In-memory map `byHash` of all known shard checkpoints (fork-DAG, not just canonical chain). Bounded by `keepDepthBehindFinalized` to
  *     prevent unbounded growth — analogous to the heap-leak Fix B applied to `NakamotoChainStore.byHash`.
  *   - `bestTip` selected by Taktikos `maxvalid-tk` (same algorithm used by gl0's `ChainSelection.standardCompare`, replicated here per the
  *     task constraint because `standardCompare` is private on `ChainSelection`).
  *   - `lastFinalizedOrdinal` — driven by the Phase 1→2 transition at the shard layer (§5.4). Finalize advances the local boundary and
  *     evicts entries below the keep-floor.
  *
  * '''Reuse from gl0 chain store''':
  *   - The `maxvalid-tk` rule (longer chain wins, ties broken by lower head slot, then by lower VRF output) is the same algorithm
  *     implemented at `ChainSelection.scala:180-192` (`standardCompare`). The source citation is preserved in the scaladoc on
  *     `compareMaxvalidTk` (private to the [[make]] body). Since `standardCompare` is private on `ChainSelection`, we re-implement the
  *     identical algorithm here rather than refactoring the public surface of `ChainSelection`.
  *
  * '''Why no `ParentChildTree`''':
  *   - gl0's chain store uses `ParentChildTree` because it needs to drive `ChainSelection.shouldSwitch`'s ancestor traversal and the
  *     fork-recovery `setHeadForRecovery` path. The shard chain doesn't yet have those callers — at the shard layer fork choice is local
  *     and the `walkBackTo` API is the only ancestor-traversal need. We can add `ParentChildTree` later if a real consumer materialises.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - No compat ceremony. This is a fresh per-shard store. The gl0 store stays as-is for the universal global chain.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - `keepDepthBehindFinalized` is a constructor parameter; callers wire `cfg.nakamoto.sharding.finality.k1Shard` from the Slice 2 typed
  *     config (`ShardFinalityConfig.k1Shard` defaults to 8 per `application.conf`). No `sys.env.get` anywhere.
  */
trait ShardChainStore[F[_]] {

  /** Which shard this store is scoped to. Set at construction; immutable. */
  def shardId: ShardId

  /** Store a new shard checkpoint. Returns `true` if the entry is new (not a duplicate), `false` if `byHash` already contained the hash.
    *
    * Side effects:
    *   - Updates `byHash` with the new entry
    *   - Updates `bestTip` if this entry beats the current tip per `maxvalid-tk`
    *
    * Note: `shardOrdinal`, `slot`, and `vrfOutput` are passed explicitly (rather than read off the `ShardCheckpoint`) because:
    *   - `shardOrdinal` is part of the checkpoint envelope already; passing it again is a redundant sanity check that callers and the
    *     envelope agree, matching the gl0 chain-store API shape (where `ordinal` is also passed alongside the snapshot).
    *   - `slot` and `vrfOutput` are NOT carried on the checkpoint envelope itself — they come from the slot-leader VRF outcome at the
    *     producer (§5.3), and the verifier reproduces them by running the VRF. The chain-store needs them for fork-choice (`maxvalid-tk`
    *     reads slot + vrfOutput), so they ride the API alongside the checkpoint.
    */
  def store(
    checkpoint: Signed[ShardCheckpoint],
    parentHash: Hash,
    shardOrdinal: ShardOrdinal,
    slot: Long,
    vrfOutput: Array[Byte]
  ): F[Boolean]

  /** Current canonical tip per Taktikos `maxvalid-tk`. None when the store is empty or all entries have been pruned. */
  def bestTip: F[Option[Hashed[ShardCheckpoint]]]

  /** Walk back `depth` parents from `hash`. Returns the chain in tip-first order (head of the returned list is the entry at `hash`, tail
    * walks toward genesis). Stops early if the chain breaks (parent not in `byHash`) or if `depth` entries have been collected.
    */
  def walkBackTo(hash: Hash, depth: Long): F[List[Hashed[ShardCheckpoint]]]

  /** Mark a checkpoint as locally-finalized at the shard layer (Phase 1→2 transition per §5.4). Advances `lastFinalizedOrdinal` and evicts
    * entries whose ordinal is strictly below the keep-floor (`finalized.ordinal - keepDepthBehindFinalized`, clamped to 0).
    *
    * Idempotent: calling with the same hash twice is a no-op on the second call. Monotone: never moves `lastFinalizedOrdinal` backward — a
    * finalize at an ordinal at-or-below the current finalized ordinal is dropped without side effect.
    */
  def `finalize`(checkpointHash: Hash): F[Unit]

  /** Hash lookup. Returns None for entries that have been evicted by the keep-window or never stored. */
  def getByHash(hash: Hash): F[Option[Hashed[ShardCheckpoint]]]

  /** Look up the canonical-chain entry at `shardOrdinal` by walking back from `bestTip`. Returns None when:
    *   - no bestTip is set
    *   - the requested ordinal is above the tip (future)
    *   - the requested ordinal is below the keep-floor (evicted)
    */
  def getByOrdinal(shardOrdinal: ShardOrdinal): F[Option[Hashed[ShardCheckpoint]]]

  /** Highest shard-ordinal that has been finalized via `finalize`. Defaults to `ShardOrdinal.Genesis` (0) at construction. */
  def lastFinalizedOrdinal: F[ShardOrdinal]

  /** Number of entries currently in `byHash`. Diagnostic; production callers should not depend on this for control flow. */
  def size: F[Int]
}

object ShardChainStore {

  /** Default for `keepDepthBehindFinalized` — matches the Slice 2 HOCON default for `nakamoto.sharding.finality.k1Shard`. Smaller than
    * gl0's `NakamotoChainStore.DefaultKeepDepthBehindFinalized = 255` because shard ords are sparser and degraded shards want tighter
    * depth-finality fallback (§5.4 / application.conf:358).
    */
  val DefaultKeepDepthBehindFinalized: Long = 8L

  /** Stored shard-checkpoint envelope. Wraps `Signed[ShardCheckpoint]` with the cached canonical hash + outer-proofs hash plus the
    * slot/vrfOutput pair needed for fork-choice. Mirrors gl0's `NakamotoChainStore.StoredSnapshot` shape (same role: carry the body + the
    * metadata that the fork-choice comparator reads off without re-hashing on every comparison).
    *
    * @param signedCheckpoint
    *   the on-wire envelope, including all committee signatures
    * @param hash
    *   `Hasher[F]` of the `ShardCheckpointSigPreimage` (design doc §3.3) — the bytes every committee member signed
    * @param proofsHash
    *   `Hasher[F]` of the outer `Signed` envelope's proofs set; carried so `toHashed` is byte-faithful to the standard `Hashed` contract
    *   (no synthesized placeholder)
    * @param shardOrdinal
    *   the chain height within this shard (also carried on the envelope; replicated here for fast access)
    * @param slot
    *   the gl0-wide slot at which the producer was leader (used by `maxvalid-tk` for tiebreaks)
    * @param parentHash
    *   the parent checkpoint hash, used for chain-walking
    * @param vrfOutput
    *   raw VRF bytes from the slot-leader proof; used as the final `maxvalid-tk` tiebreak (lower BigInt wins)
    */
  case class StoredShardCheckpoint(
    signedCheckpoint: Signed[ShardCheckpoint],
    hash: Hash,
    proofsHash: io.constellationnetwork.security.hash.ProofsHash,
    shardOrdinal: ShardOrdinal,
    slot: Long,
    parentHash: Hash,
    vrfOutput: Array[Byte]
  ) {

    /** Build a `Hashed[ShardCheckpoint]` from the stored fields. Both `hash` and `proofsHash` are precomputed at store-time, so this is a
      * pure projection — no hashing on the read path.
      */
    def toHashed: Hashed[ShardCheckpoint] =
      Hashed(signed = signedCheckpoint, hash = hash, proofsHash = proofsHash)
  }

  /** In-memory state held by a single shard-chain store. Pure data — all mutation goes through `Ref.modify` in the implementation. */
  case class ChainState(
    byHash: Map[Hash, StoredShardCheckpoint],
    bestTipHash: Option[Hash],
    lastFinalizedOrdinal: ShardOrdinal
  )

  object ChainState {
    val empty: ChainState = ChainState(Map.empty, None, ShardOrdinal.Genesis)
  }

  /** Construct a per-shard chain store.
    *
    * @param shardId
    *   which shard this store is scoped to. Set at construction; immutable.
    * @param keepDepthBehindFinalized
    *   in-memory retention window. Entries at `ordinal < (finalizedOrdinal - keepDepthBehindFinalized)` are evicted at each `finalize`
    *   call. Defaults to [[DefaultKeepDepthBehindFinalized]] (8). Production callers wire
    *   `sharedConfig.nakamoto.sharding.finality.k1Shard`.
    *
    * The implicit `Hasher[F]` is required because the store keys its in-memory `byHash` map by the canonical signing-preimage hash (per
    * design doc §3.3 — "the bytes a signer signs are `Hasher[F](ShardCheckpointSigPreimage)`"). Routing through the typeclass avoids
    * hand-rolled serialization (`[[feedback-use-hasher-no-manual-serialize]]`) and keeps cluster-wide hash determinism.
    */
  def make[F[_]: Async: Hasher: Metrics](
    shardId: ShardId,
    keepDepthBehindFinalized: Long = DefaultKeepDepthBehindFinalized
  ): F[ShardChainStore[F]] = {
    val outerShardId = shardId
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardChainStore[$shardId]")

    Ref.of[F, ChainState](ChainState.empty).map { stateRef =>
      new ShardChainStore[F] {

        val shardId: ShardId = outerShardId

        def store(
          checkpoint: Signed[ShardCheckpoint],
          parentHash: Hash,
          shardOrdinal: ShardOrdinal,
          slot: Long,
          vrfOutput: Array[Byte]
        ): F[Boolean] =
          // Compute both hashes:
          //   - `snapshotHash` = `Hasher[F]` over `ShardCheckpointSigPreimage` (design doc §3.3). This is the bytes every committee
          //     member signed. The store keys its `byHash` map by this so lookups by canonical-hash work everywhere.
          //   - `proofsHash` = `Hasher[F]` over the outer Signed envelope's proofs set — carried on the `Hashed` wrapper for parity with
          //     the standard contract (no synthesized placeholder).
          (deriveHash(checkpoint), checkpoint.proofsHash[F]).tupled.flatMap {
            case (snapshotHash, sigProofsHash) =>
              val stored = StoredShardCheckpoint(
                signedCheckpoint = checkpoint,
                hash = snapshotHash,
                proofsHash = sigProofsHash,
                shardOrdinal = shardOrdinal,
                slot = slot,
                parentHash = parentHash,
                vrfOutput = vrfOutput
              )

              stateRef.modify { state =>
                if (state.byHash.contains(snapshotHash)) {
                  // Duplicate — already stored. Idempotent no-op.
                  (state, logger.debug(s"store: duplicate hash=${snapshotHash.value.take(12)}; ignoring").as(false))
                } else {
                  val newByHash = state.byHash + (snapshotHash -> stored)

                  // Resolve current tip defensively — if `bestTipHash` points at a hash no longer in `byHash` (eviction race), treat as no
                  // best tip. Mirrors the `resolvedBest = state.bestTipHash.flatMap(...)` pattern in `NakamotoChainStore.store`.
                  val resolvedBest: Option[StoredShardCheckpoint] =
                    state.bestTipHash.flatMap(state.byHash.get)

                  val newBestTipHash: Hash = resolvedBest match {
                    case None => snapshotHash // first store OR stale best tip — incoming becomes the best
                    case Some(curBest) =>
                      if (compareMaxvalidTk(stored, curBest) > 0) snapshotHash else curBest.hash
                  }

                  val newState = state.copy(byHash = newByHash, bestTipHash = Some(newBestTipHash))
                  // Slice 19: emit per-shard `dag_nakamoto_shard_chain_height{shard_id}` gauge whenever the bestTip moves.
                  // The gauge follows the highest-stored-tip ord, so we only emit on the three branches where the bestTip
                  // ACTUALLY advanced (bootstrap, linear-extension, reorg). The alternate-branch case keeps the prior tip.
                  val newTipOrd: ShardOrdinal = ShardOrdinal(
                    newState.byHash.get(newBestTipHash).map(_.shardOrdinal.value).getOrElse(0L)
                  )
                  val effect: F[Boolean] = (newBestTipHash === snapshotHash, resolvedBest) match {
                    case (true, None) =>
                      logger
                        .info(s"store: chain bootstrapped at shardOrdinal=${shardOrdinal.value} slot=$slot")
                        .productR(ShardMetrics.setChainHeight[F](outerShardId, newTipOrd))
                        .as(true)
                    case (true, Some(prior)) if stored.parentHash === prior.hash =>
                      logger
                        .debug(s"store: linear extension shardOrdinal=${shardOrdinal.value} slot=$slot")
                        .productR(ShardMetrics.setChainHeight[F](outerShardId, newTipOrd))
                        .as(true)
                    case (true, Some(prior)) =>
                      logger
                        .info(
                          s"store: reorg — new tip shardOrdinal=${shardOrdinal.value} slot=$slot beats " +
                            s"prior shardOrdinal=${prior.shardOrdinal.value} slot=${prior.slot}"
                        )
                        .productR(ShardMetrics.setChainHeight[F](outerShardId, newTipOrd))
                        .as(true)
                    case (false, _) =>
                      logger
                        .debug(
                          s"store: alternate branch shardOrdinal=${shardOrdinal.value} slot=$slot " +
                            s"(parent=${parentHash.value.take(8)}, not switching from currentBest)"
                        )
                        .as(true)
                  }

                  (newState, effect)
                }
              }.flatten
          }

        def bestTip: F[Option[Hashed[ShardCheckpoint]]] =
          stateRef.get.map { state =>
            state.bestTipHash.flatMap(state.byHash.get).map(_.toHashed)
          }

        def walkBackTo(hash: Hash, depth: Long): F[List[Hashed[ShardCheckpoint]]] =
          stateRef.get.map { state =>
            if (depth <= 0L) Nil
            else {
              val acc = scala.collection.mutable.ListBuffer.empty[Hashed[ShardCheckpoint]]
              var current = state.byHash.get(hash)
              var remaining = depth
              while (current.isDefined && remaining > 0L) {
                val cur = current.get
                acc += cur.toHashed
                remaining -= 1L
                if (remaining > 0L) current = state.byHash.get(cur.parentHash)
                else current = None
              }
              acc.toList
            }
          }

        def `finalize`(checkpointHash: Hash): F[Unit] =
          stateRef.modify { state =>
            state.byHash.get(checkpointHash) match {
              case None =>
                // Finalize on an unknown hash — no-op + warn. We could record the ordinal-only if the caller supplied it, but for the
                // shard-layer use case (Phase 1→2 transitions are driven by `FinalityTrigger` on entries we know about) this branch is
                // a misuse signal worth surfacing.
                (
                  state,
                  logger.warn(
                    s"finalize: unknown hash=${checkpointHash.value.take(12)}; ignoring (callers should only finalize stored hashes)"
                  )
                )

              case Some(target) =>
                val targetOrd = target.shardOrdinal
                // Monotonicity guard: never move lastFinalizedOrdinal backward. If the requested ordinal is at-or-below the current
                // finalized boundary, this is either a duplicate finalize (idempotent) or a stale message — drop without side effect.
                if (targetOrd.value <= state.lastFinalizedOrdinal.value && state.byHash.size > 0) {
                  (
                    state,
                    logger.debug(
                      s"finalize: shardOrdinal=${targetOrd.value} ≤ lastFinalized=${state.lastFinalizedOrdinal.value}; no-op"
                    )
                  )
                } else {
                  // Compute the canonical chain from the target back to (genesis or eviction boundary). Used by the eviction predicate
                  // to keep canonical-chain entries within the keep-window and drop everything else.
                  val canonicalHashes = scala.collection.mutable.Set.empty[Hash]
                  var cursor = state.byHash.get(checkpointHash)
                  while (cursor.isDefined) {
                    canonicalHashes += cursor.get.hash
                    cursor = state.byHash.get(cursor.get.parentHash)
                  }

                  // keepFloor: lowest ordinal we retain. Anything at `ordinal < keepFloor` (canonical or orphan) gets dropped.
                  // Clamp at 0 so genesis / early-chain (ord < keepDepth) is a no-op.
                  val keepFloor: Long = math.max(0L, targetOrd.value - keepDepthBehindFinalized)

                  // Prune predicate (mirrors NakamotoChainStore.finalize):
                  //   - keep anything strictly above the finalized ordinal (tentative successors of the finalized tip)
                  //   - keep canonical-chain entries down to keepFloor
                  //   - drop everything else
                  val pruned = state.byHash.filter {
                    case (h, s) =>
                      s.shardOrdinal.value > targetOrd.value ||
                      (canonicalHashes.contains(h) && s.shardOrdinal.value >= keepFloor)
                  }
                  val prunedCount = state.byHash.size - pruned.size

                  // If the previous best tip got pruned (fork branch losing finality), clear it so subsequent stores reseed.
                  val newBestTip = state.bestTipHash.filter(pruned.contains)

                  val nextState = state.copy(
                    byHash = pruned,
                    bestTipHash = newBestTip,
                    lastFinalizedOrdinal = targetOrd
                  )
                  (
                    nextState,
                    logger
                      .info(
                        s"finalize: shardOrdinal=${targetOrd.value} keepFloor=$keepFloor dropped=$prunedCount " +
                          s"remaining=${pruned.size}"
                      )
                      // Slice 19: emit per-shard `dag_nakamoto_shard_chain_finalized_ordinal{shard_id}`. The advance is
                      // monotone (guarded above by `targetOrd <= lastFinalizedOrdinal` no-op), so the gauge tracks the highest
                      // ord we've ever finalized.
                      .productR(ShardMetrics.setChainFinalized[F](outerShardId, targetOrd))
                  )
                }
            }
          }.flatten

        def getByHash(hash: Hash): F[Option[Hashed[ShardCheckpoint]]] =
          stateRef.get.map(_.byHash.get(hash).map(_.toHashed))

        def getByOrdinal(shardOrdinal: ShardOrdinal): F[Option[Hashed[ShardCheckpoint]]] =
          stateRef.get.map { state =>
            state.bestTipHash.flatMap(state.byHash.get) match {
              case None      => None
              case Some(tip) =>
                // Walk back from bestTip until we find the requested ordinal or drop off the bottom.
                if (shardOrdinal.value > tip.shardOrdinal.value) None // requested future
                else {
                  var cursor: Option[StoredShardCheckpoint] = Some(tip)
                  while (cursor.exists(_.shardOrdinal.value > shardOrdinal.value))
                    cursor = state.byHash.get(cursor.get.parentHash)
                  cursor.filter(_.shardOrdinal.value == shardOrdinal.value).map(_.toHashed)
                }
            }
          }

        def lastFinalizedOrdinal: F[ShardOrdinal] =
          stateRef.get.map(_.lastFinalizedOrdinal)

        def size: F[Int] =
          stateRef.get.map(_.byHash.size)

        /** Derive a deterministic hash for an envelope via the `Hasher[F]` typeclass — matches
          * `[[feedback-use-hasher-no-manual-serialize]]`. The hash MUST match what the committee signers signed (the bytes derived from
          * `ShardCheckpointSigPreimage`) so that the store's `byHash` keys are consistent with how every other slice in this system
          * identifies a checkpoint. Per design doc §3.3 "Canonical hash for signatures" the signed bytes are
          * `Hasher[F](ShardCheckpointSigPreimage)`.
          */
        private def deriveHash(checkpoint: Signed[ShardCheckpoint]): F[Hash] =
          Hasher[F].hash(checkpoint.value.signingPreimage)

        /** Taktikos `maxvalid-tk`: longer chain wins; ties broken by lower head slot; ties broken by lower VRF output (BigInt unsigned).
          *
          * Returns positive when `a` beats `b`; negative when `b` beats `a`; zero when they're indistinguishable on these criteria.
          *
          * Identical algorithm to `ChainSelection.standardCompare`
          * (`modules/node-shared/src/main/scala/io/constellationnetwork/node/shared/domain/nakamoto/ChainSelection.scala:180-192`). That
          * method is `private` on the gl0 `ChainSelection` impl so we replicate the algorithm here rather than refactoring the public
          * surface of `ChainSelection`. If a future refactor exposes a shared comparator, this site should re-route through it.
          */
        private def compareMaxvalidTk(a: StoredShardCheckpoint, b: StoredShardCheckpoint): Int =
          if (a.shardOrdinal.value != b.shardOrdinal.value) {
            // Longer chain wins
            if (a.shardOrdinal.value > b.shardOrdinal.value) 1 else -1
          } else if (a.slot != b.slot) {
            // Earlier slot wins (harder lottery)
            if (a.slot < b.slot) 1 else -1
          } else {
            // Lower VRF output wins (deterministic). Use BigInt-unsigned comparison (signum=1) to mirror
            // ChainSelection.compareVrfOutputs.
            val bigA = BigInt(1, a.vrfOutput)
            val bigB = BigInt(1, b.vrfOutput)
            -bigA.compare(bigB) // negate so the LOWER BigInt wins (returns positive)
          }
      }
    }
  }
}
