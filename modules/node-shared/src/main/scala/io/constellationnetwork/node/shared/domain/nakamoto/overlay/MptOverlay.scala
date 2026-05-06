package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.annotation.tailrec

import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{MptStore, MptTxAction}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaTrie
import io.constellationnetwork.security.mpt.producer.MerklePatriciaError
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.implicits._

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Branch identity within the overlay. Reuses chain-store snapshot identity (the `Hash` of a snapshot/block tip) so that callers can pass
  * `chainStore.bestTip` (or any known parent hash) without any conversion. The opaque-ness target of the rev3 plan is approximated here as
  * a value-class wrapper — Scala 2.13 has no opaque types, but the wrapper preserves "do not concatenate / mistakenly compare with raw
  * Hash".
  */
final case class BranchId(value: Hash) extends AnyVal

object BranchId {

  /** Sentinel id reserved for the passthrough overlay's single notional branch. Not a real chain-store hash; passthrough callers do not
    * depend on its exact value, only on the fact that it is stable for the lifetime of the overlay.
    */
  val passthrough: BranchId = BranchId(Hash.empty)

  /** Conventional id for "no pending branch — I just want to read the finalized base." Any `BranchId` not present in the multi-branch
    * overlay's pending map resolves to the base, so this is purely a documentation alias.
    */
  val base: BranchId = BranchId(Hash.empty)
}

/** Outcome of `MptOverlay.finalizeBranch`. The plan (#56.6) defines this as the single return value the finality sink reads after each
  * call.
  */
sealed trait FinalizationOutcome
object FinalizationOutcome {

  /** A branch's deltas were folded forward into the on-disk base, and zero or more non-canonical branches were dropped.
    *   - In multi-branch mode, `keysApplied` counts the distinct trie keys touched (`upserts.size + removals.size` of the merged
    *     chain-of-deltas). `branchesDropped` counts pending branch entries removed from the overlay other than the canonical chain itself.
    *   - In passthrough mode, this is never returned — the passthrough always returns `NoOp` because there is no overlay state to fold.
    */
  final case class Folded(keysApplied: Int, branchesDropped: Int) extends FinalizationOutcome

  /** Idempotency / passthrough sentinel: nothing to fold. Returned when `(ordinal, hash)` already finalized at the same canonical, or when
    * the overlay is in passthrough mode and the underlying `MptStore` already holds the canonical state, or when the requested branch isn't
    * tracked in the overlay (so there's nothing to apply).
    */
  case object NoOp extends FinalizationOutcome
}

/** Mutable handle for accumulating writes against a checked-out branch.
  *
  * In passthrough mode the writes go straight to the underlying `MptStore`. In multi-branch mode the writes accumulate into a `ChangeSet`
  * keyed by `BranchId`, and only `commit` materializes the per-branch view.
  *
  * `BranchHandle` is intentionally narrower than `MptStore` — only mutations needed during snapshot construction. Reads always go through
  * the overlay (`MptOverlay.get` / `getAllForPrefix`) keyed by `BranchId`, since the value visible at a branch is `base ⊕ chain-of-deltas`,
  * not `base + uncommitted local writes`.
  */
trait BranchHandle[F[_], K] {
  def insert[V: ImmutableCodec](key: K, value: V): F[Unit]
  def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit]
  def remove(key: K): F[Unit]
  def remove(keys: List[K]): F[Unit]
  def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit]

  /** The branch this handle was checked out against — provided so callers can later pass it to `commit` / `get` without bookkeeping. */
  def parent: BranchId
}

/** Branch-aware view over an `MptStore`.
  *
  * Goal (rev3 plan, #56): each pending branch gets its own state view (deltas over a finalized base). Only the finalized state is
  * persisted; pending branches live in memory. When finality advances, the canonical branch's diffs apply forward; non-canonical branches
  * are dropped.
  *
  * '''Layering''': WRAP, not parameterize. The 14+ existing `MptStore` call sites stay untouched until callers are migrated to the overlay
  * one at a time. `base` is exposed so legacy paths can keep working during migration.
  *
  * Two impls live behind `MptOverlay.make(enabled, ...)`:
  *   - '''passthrough''' (#56.2): correctness-equivalent to direct `MptStore` use; `BranchId` is ignored.
  *   - '''multi-branch''' (#56.4): per-branch `ChangeSet` accumulation with sibling isolation, lazy fall-through to base on read miss, and
  *     fold-forward on finalize.
  *
  * '''Sidecar / per-field root contract (#56.4.5)''':
  *   - The overlay accumulates writes as raw `Hex → Array[Byte]` pairs without distinguishing partition kind. Sidecar partitions
  *     (`ActiveAddressIndex`, expiry indices for AllowSpend / TokenLock / NodeCollateral) and user-field partitions (Balances, LastTxRefs,
  *     etc.) all flow through the same `ChangeSet`. They share the producer's hex keyspace.
  *   - '''Global root''' (`buildRoot(branch, ordinal)`): includes EVERY partition — sidecars and user fields — composed via
  *     `MerklePatriciaTrie.withChanges` from the on-disk base trie at `ordinal`. This is the consensus-relevant root used in stateProof
  *     verification.
  *   - '''Per-field root algorithm already exists''' at `GlobalStateConverter.buildPerFieldMptRoots`: it groups `Map[GlobalStateKey,
  *     Array[Byte]]` by `_._1.fieldId` (structured component of `GlobalStateKey`, not a Hex-prefix slice) and builds a fresh
  *     `MerklePatriciaTrie.makeParallelFromBytes` per group. Sidecar entries land under `GlobalStateFieldId.SystemIndex`; they are produced
  *     by `buildPerFieldMptRoots` but the consumer in `GlobalSnapshotInfo.stateProofBuilder` only reads user-visible fieldIds (`Balances`,
  *     `LastTxRefs`, etc.) via `fieldRoot(id)` lookups, so sidecars are effectively excluded at the StateProof boundary.
  *   - '''#56.5 work''': re-source the input map (`Map[GlobalStateKey, Array[Byte]]`) from the OVERLAY's branch-scoped view (so a pending
  *     branch's stateProof reflects its deltas), then feed it into the existing `buildPerFieldMptRoots`. The algorithm doesn't change; the
  *     producer of the kvPairs does.
  *   - '''Finalization atomicity''': a single `finalizeBranch` writes ALL partition deltas (sidecars + user fields) to the base in one
  *     savepoint-bracketed transaction (`MptStore.withTransaction`). If the apply fails mid-stream, the base rolls back to its pre-call
  *     state. There is no partial-partition state — the contract is all-or-nothing across the entire merged chain.
  */
trait MptOverlay[F[_], K] {

  /** The underlying finalized-state store. Exposed for migration / non-overlay-aware call sites. New code should prefer overlay reads. */
  def base: MptStore[F, K]

  /** Block-tree topology shared with consensus. Reused — no separate tree maintained by the overlay. */
  def parentChildTree: ParentChildTree[F]

  /** Check out a writable handle whose writes will become a child of `parent`. In multi-branch mode the returned handle accumulates writes
    * into a per-handle `ChangeSet`; reads via the overlay against any other branch do NOT see these writes until `commit` registers them.
    */
  def checkout(parent: BranchId): F[BranchHandle[F, K]]

  /** Commit the writes accumulated on `branch` as the child branch identified by `childTip` at `ordinal`. Multi-branch: registers the
    * accumulated `ChangeSet` keyed by `childTip` and links `(parent → childTip)` in `parentChildTree`. Passthrough: associates the tip in
    * the parent-child tree (so consensus topology stays accurate) and otherwise no-ops — writes already landed in the underlying store.
    */
  def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit]

  /** Read at a specific branch view. Multi-branch: walks `branch → ... → base` through the pending-branches map, returning the first delta
    * hit (upsert or removal); falls through to `MptStore.get` on miss. Passthrough: ignores `branch`, delegates to `MptStore.get`.
    */
  def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]]

  /** Prefix scan at a specific branch view. Multi-branch: composes base prefix-scan with the chain's accumulated upserts (decoded to V) and
    * removals (filtered to `prefix`). Passthrough: ignores `branch`, delegates to `MptStore.getAllForPrefix`.
    */
  def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]]

  /** Build the per-branch trie at `ordinal`. Multi-branch: takes the on-disk base trie at `ordinal` and applies the chain's merged
    * `ChangeSet` via `MerklePatriciaTrie.withChanges`. Passthrough: ignores `branch`, delegates to `MptStore.build`.
    */
  def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  /** Finality sink (#56.6 wires the call site). Idempotent on `(ordinal, canonical)`. Errors loudly when called twice at the same `ordinal`
    * with different `canonical` hashes (depth-k vs attestation-2/3 disagreement during partition).
    *
    * Multi-branch: walks `canonical → ... → base` through the pending-branches map, merges all chain `ChangeSet`s, applies the merged delta
    * to the underlying producer, and clears the entire pending-branches map. (More nuanced eviction — keeping non-conflicting siblings —
    * lands in #56.9.) Passthrough: always returns `NoOp` because there is no overlay state to fold.
    */
  def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome]
}

object MptOverlay {

  /** Per-branch entry in the overlay's pending-branches map. Lives only in memory — restart wipes pending state and the node resyncs from
    * the finalized base + `ChainSync`.
    */
  private final case class BranchEntry(
    parent: BranchId,
    changes: ChangeSet,
    ordinal: SnapshotOrdinal
  )

  /** Default cap on pending branches. Per the rev3 plan: 4 branches × 32 ordinals × ~50 KB ≈ ~6 MB. Eviction (#56.9) drops the
    * lowest-scoring non-canonical branch when this is exceeded.
    */
  val DefaultMaxPendingBranches: Int = 4

  /** Operating mode of an `MptOverlay`. Replaces the rev3 `enabled: Boolean` flag (#56.10 Phase J).
    *
    *   - `Passthrough`: correctness-equivalent to direct `MptStore` use. The `BranchId` argument is ignored on every call; commits land in
    *     the underlying store immediately. Used during the migration window so e2e/unit tests can prove parity between old and new paths.
    *   - `MultiBranch(maxPendingBranches)`: per-branch `ChangeSet` accumulation with sibling isolation. Pending state lives in memory; only
    *     `finalizeBranch` folds it into the underlying store. Cap on pending branches — eviction (#56.9) drops the lowest-scoring
    *     non-ancestor branch on commit when exceeded.
    *
    * Transient: this ADT is migration scaffolding, not a permanent rollout flag. After #56.11 the only valid mode is `MultiBranch` and the
    * sealed trait can be retired entirely.
    */
  sealed trait OverlayMode extends Product with Serializable
  object OverlayMode {
    case object Passthrough extends OverlayMode
    final case class MultiBranch(maxPendingBranches: Int) extends OverlayMode

    /** Default for production wiring: multi-branch with the canonical cap. Used by `SharedStorages` once accept() has migrated to the
      * overlay (Phase D). Until then production wiring stays on `Passthrough` to preserve byte-for-byte parity with the legacy path.
      */
    val productionDefault: OverlayMode = MultiBranch(DefaultMaxPendingBranches)
  }

  /** Factory dispatching on `OverlayMode`.
    *
    * `toHex` is plumbed in alongside `underlying` so the multi-branch impl can encode keys before the per-handle accumulator stores them.
    * For passthrough, `toHex` is unused — kept on the signature so swapping modes doesn't change call sites.
    *
    * `bestTipFn` (#56.9): callback the overlay queries during eviction to identify ancestors of the canonical chain — these are NEVER
    * evicted. Pass `Async[F].pure(none[BranchId])` for "no ancestor protection" (purely score-based eviction); production deployments
    * should plumb this from `chainStore.bestTip` (Phase I) so depth-k / attestation-2/3 finality can still walk back through pending
    * branches.
    */
  def make[F[_]: Async: Hasher, K](
    mode: OverlayMode,
    underlying: MptStore[F, K],
    pcTree: ParentChildTree[F],
    toHex: K => F[Hex],
    bestTipFn: F[Option[BranchId]]
  ): F[MptOverlay[F, K]] =
    mode match {
      case OverlayMode.Passthrough             => Async[F].pure(passthrough(underlying, pcTree))
      case OverlayMode.MultiBranch(maxPending) => MultiBranch[F, K](underlying, pcTree, toHex, maxPending, bestTipFn)
    }

  /** Single-branch passthrough — correctness-equivalent to using `MptStore` directly. The `BranchId` argument on every method is ignored.
    * `commit` registers the branch in `parentChildTree` so consensus topology stays consistent with what multi-branch mode needs.
    */
  def passthrough[F[_]: Async, K](
    underlying: MptStore[F, K],
    pcTree: ParentChildTree[F]
  ): MptOverlay[F, K] =
    new MptOverlay[F, K] {
      def base: MptStore[F, K] = underlying
      def parentChildTree: ParentChildTree[F] = pcTree

      def checkout(parentBranch: BranchId): F[BranchHandle[F, K]] =
        Async[F].pure(new PassthroughHandle[F, K](underlying, parentBranch))

      def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit] =
        // Writes already landed in `underlying` during the handle's lifetime; this just performs the
        // topology bookkeeping and the per-ordinal trie checkpoint that legacy `syncFromStateChanges`
        // used to do at its tail. Without the checkpoint, `builder.buildProof` (which calls
        // `producer.buildForOrdinal` + `producer.getRootHashForOrdinal`) cannot resolve the just-written
        // ordinal — that's the post-write contract the algebra has to honor when the GSAM rewire (#56.10
        // Phase D) routes through `overlay.commit` instead of `mptStore.syncFromStateChanges`. Multi-branch
        // mode does the equivalent inside `finalizeBranch.foldIntoBase`.
        //
        // Skip self-association if checkout was given the same id (e.g. genesis bootstrap), which would
        // otherwise create a self-loop in the parent-child tree.
        for {
          _ <- underlying.commit(ordinal)
          _ <-
            if (branch.parent.value === childTip.value) Async[F].unit
            else pcTree.associate(childTip.value, branch.parent.value)
        } yield ()

      def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]] =
        underlying.get(key)

      def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]] =
        underlying.getAllForPrefix[V](prefix)

      def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
        underlying.build(ordinal)

      def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome] =
        // Passthrough has no pending state, so finalization is always a no-op. The conflict-detection
        // contract (error on (ordinal, hashA) then (ordinal, hashB)) is meaningful only in multi-branch
        // mode where pending state actually exists; passthrough is invisible to the comparator.
        Async[F].pure(FinalizationOutcome.NoOp)
    }

  private final class PassthroughHandle[F[_], K](
    underlying: MptStore[F, K],
    val parent: BranchId
  ) extends BranchHandle[F, K] {
    def insert[V: ImmutableCodec](key: K, value: V): F[Unit] = underlying.insert(key, value)
    def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit] = underlying.insert(entries)
    def remove(key: K): F[Unit] = underlying.remove(key)
    def remove(keys: List[K]): F[Unit] = underlying.remove(keys)
    def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] = underlying.update(toUpsert, toRemove)
  }

  /** Per-handle local accumulator. Mutates a `Ref[ChangeSet]` and is otherwise inert against `MptStore` until `commit` registers the
    * accumulated state in the overlay's pending-branches map.
    *
    * Encoding is identical to `MptStore.Impl.encode` so a key that flows through this handle and is later folded to base is byte-equivalent
    * to one written via `MptStore.insert` directly.
    */
  private final class MultiBranchHandle[F[_]: Async, K](
    toHex: K => F[Hex],
    val accRef: Ref[F, ChangeSet],
    val parent: BranchId
  ) extends BranchHandle[F, K] {

    private def encode[V: ImmutableCodec](v: V): Array[Byte] =
      ImmutableCodec[V].immutableBytes(v).toArray

    def insert[V: ImmutableCodec](key: K, value: V): F[Unit] =
      toHex(key).flatMap { hex =>
        val bytes = encode(value)
        accRef.update(cs => ChangeSet(cs.upserts.updated(hex, bytes), cs.removals - hex))
      }

    def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit] =
      if (entries.isEmpty) Async[F].unit
      else
        entries.toList.traverse { case (k, v) => toHex(k).map(_ -> encode(v)) }.flatMap { hexed =>
          val newPairs = hexed.toMap
          accRef.update(cs => ChangeSet(cs.upserts ++ newPairs, cs.removals -- newPairs.keySet))
        }

    def remove(key: K): F[Unit] =
      toHex(key).flatMap { hex =>
        accRef.update(cs => ChangeSet(cs.upserts - hex, cs.removals + hex))
      }

    def remove(keys: List[K]): F[Unit] =
      if (keys.isEmpty) Async[F].unit
      else
        keys.traverse(toHex).flatMap { hexes =>
          val toRemove = hexes.toSet
          accRef.update(cs => ChangeSet(cs.upserts -- toRemove, cs.removals ++ toRemove))
        }

    def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] =
      for {
        upsertHexed <- toUpsert.toList.traverse { case (k, v) => toHex(k).map(_ -> encode(v)) }
        removeHexed <- toRemove.toList.traverse(toHex)
        upMap = upsertHexed.toMap
        rmSet = removeHexed.toSet
        // Apply removals first then upserts — matches `MerklePatriciaTrie.withChanges` ordering, so a key
        // present in BOTH `toUpsert` and `toRemove` ends up upserted (consistent with `MptStore.update`).
        _ <- accRef.update { cs =>
          val afterRemove = ChangeSet(cs.upserts -- rmSet, cs.removals ++ rmSet)
          ChangeSet(afterRemove.upserts ++ upMap, afterRemove.removals -- upMap.keySet)
        }
      } yield ()
  }

  /** Multi-branch overlay implementation.
    *
    * State:
    *   - `pendingRef: Map[BranchId, BranchEntry]` — every committed branch above the finalized base. A branch not in this map resolves to
    *     "the base" for read purposes.
    *   - `finalizedRef: Map[SnapshotOrdinal, BranchId]` — finalized canonical at each ordinal, for `finalizeBranch` idempotency and
    *     cross-ordinal conflict detection.
    *   - `mutex: Semaphore` — serializes `commit` and `finalizeBranch` against each other so the chain walk + base apply in
    *     `finalizeBranch` is atomic w.r.t. concurrent commits.
    *
    * Read path: `walkChainForKey` is a pure tail-recursion over `pendingRef.get`. It starts at the requested branch and walks up parent
    * pointers, returning:
    *   - `Some(None)` — the chain explicitly removed `key` at some level, so the answer is `None` (do NOT fall through).
    *   - `Some(Some(bytes))` — the chain has an upsert for `key`; deserialize and return.
    *   - `None` — no level in the chain mentions `key`, fall through to the base `MptStore`.
    *
    * Merge ordering convention (`mergedChain`): walking leaf→root, each parent's `ChangeSet` is `parent.merge(childAcc)`. `merge` is "later
    * wins", so if leaf is the most recent commit, `parent.merge(leaf)` correctly applies parent first then leaf — the chronological order.
    */
  private object MultiBranch {

    def apply[F[_]: Async: Hasher, K](
      underlying: MptStore[F, K],
      pcTree: ParentChildTree[F],
      toHex: K => F[Hex],
      maxPendingBranches: Int,
      bestTipFn: F[Option[BranchId]]
    ): F[MptOverlay[F, K]] =
      (
        Ref.of[F, Map[BranchId, BranchEntry]](Map.empty),
        Ref.of[F, Map[SnapshotOrdinal, BranchId]](Map.empty),
        Semaphore[F](1)
      ).mapN { (pendingRef, finalizedRef, mutex) =>
        new Impl[F, K](
          underlying,
          pcTree,
          toHex,
          pendingRef,
          finalizedRef,
          mutex,
          maxPendingBranches,
          bestTipFn
        ): MptOverlay[F, K]
      }

    private final class Impl[F[_]: Async: Hasher, K](
      underlying: MptStore[F, K],
      pcTree: ParentChildTree[F],
      toHex: K => F[Hex],
      pendingRef: Ref[F, Map[BranchId, BranchEntry]],
      finalizedRef: Ref[F, Map[SnapshotOrdinal, BranchId]],
      mutex: Semaphore[F],
      maxPendingBranches: Int,
      bestTipFn: F[Option[BranchId]]
    ) extends MptOverlay[F, K] {

      private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

      def base: MptStore[F, K] = underlying
      def parentChildTree: ParentChildTree[F] = pcTree

      def checkout(parentBranch: BranchId): F[BranchHandle[F, K]] =
        Ref.of[F, ChangeSet](ChangeSet.empty).map { acc =>
          new MultiBranchHandle[F, K](toHex, acc, parentBranch)
        }

      def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit] = {
        // Casting back to MultiBranchHandle to read its accumulator. Safe because checkout always returns
        // MultiBranchHandle in this impl. Using a class match avoids exposing `accRef` on the public trait.
        val handle = branch match {
          case h: MultiBranchHandle[F, K] @unchecked => h
          case other =>
            throw new IllegalArgumentException(
              s"Multi-branch overlay received a non-MultiBranchHandle on commit: ${other.getClass.getName}"
            )
        }
        mutex.permit.use { _ =>
          for {
            changes <- handle.accRef.get
            _ <- pendingRef.update(_.updated(childTip, BranchEntry(handle.parent, changes, ordinal)))
            // Eviction (#56.9): single-shot — each commit adds exactly one branch, so at most one over the
            // cap. Read pending+evict+write while still holding the mutex so a concurrent commit can't see
            // a momentarily-over-cap state.
            _ <- pendingRef.get.flatMap(evictIfOverCap).flatMap(pendingRef.set)
            _ <-
              if (handle.parent.value === childTip.value) Async[F].unit
              else pcTree.associate(childTip.value, handle.parent.value)
          } yield ()
        }
      }

      /** Eviction policy (#56.9). Drops the lowest-scoring non-ancestor branch when `pending.size` exceeds the cap. Score is `(ordinal asc,
        * BranchId.value lex asc)` — lowest gets evicted. Ancestors of `bestTipFn`'s tip are never evicted (would break finality walk-back).
        * If `bestTipFn` returns `None`, no ancestor protection is applied. If ALL pending branches are ancestors, eviction is skipped with
        * a warn log — the overlay rides over-cap until finalization releases ancestors.
        */
      private def evictIfOverCap(pending: Map[BranchId, BranchEntry]): F[Map[BranchId, BranchEntry]] =
        if (pending.size <= maxPendingBranches) pending.pure[F]
        else
          bestTipFn.flatMap { bestTipOpt =>
            val ancestors = bestTipOpt.fold(Set.empty[BranchId])(walkAncestorsInPending(_, pending))
            val candidates = pending.view.filterKeys(id => !ancestors.contains(id)).toMap
            if (candidates.isEmpty)
              logger
                .warn(
                  s"[MptOverlay] Eviction skipped: all ${pending.size} pending branches are ancestors of bestTip; " +
                    s"cap=$maxPendingBranches. Overlay will ride over-cap until finalization releases ancestors."
                )
                .as(pending)
            else {
              val (evictId, evictEntry) = candidates.toList.minBy {
                case (id, entry) => (entry.ordinal.value.value, id.value.value)
              }
              logger
                .info(
                  s"[MptOverlay] Evicting branch=${evictId.value} ordinal=${evictEntry.ordinal} (cap=$maxPendingBranches, " +
                    s"pending=${pending.size}, ancestors=${ancestors.size})"
                )
                .as(pending - evictId)
            }
          }

      def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]] =
        for {
          hex <- toHex(key)
          pending <- pendingRef.get
          chainResult = walkChainForKey(branch, hex, pending)
          out <- chainResult match {
            case Some(None)        => none[V].pure[F]
            case Some(Some(bytes)) => deserializeBytes[V](bytes)
            case None              => underlying.get[V](key)
          }
        } yield out

      def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]] =
        for {
          baseEntries <- underlying.getAllForPrefix[V](prefix)
          pending <- pendingRef.get
          merged = mergedChain(branch, pending)
          filteredUpserts = merged.upserts.filter { case (hex, _) => hex.value.startsWith(prefix.value) }
          filteredRemovals = merged.removals.filter(_.value.startsWith(prefix.value))
          decoded <- filteredUpserts.toList.traverseFilter {
            case (hex, bytes) => deserializeBytes[V](bytes).map(_.map(hex -> _))
          }
        } yield (baseEntries -- filteredRemovals) ++ decoded.toMap

      def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
        underlying.build(ordinal).flatMap {
          case Left(err) => Async[F].pure(Left(err): Either[MerklePatriciaError, MerklePatriciaTrie])
          case Right(baseTrie) =>
            pendingRef.get.flatMap { pending =>
              val merged = mergedChain(branch, pending)
              if (merged.isEmpty) Async[F].pure(Right(baseTrie): Either[MerklePatriciaError, MerklePatriciaTrie])
              else
                baseTrie
                  .withChanges[F](merged.upserts, merged.removals)
                  .map(t => Right(t): Either[MerklePatriciaError, MerklePatriciaTrie])
            }
        }

      def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome] =
        mutex.permit.use { _ =>
          finalizedRef.get.flatMap { finalized =>
            finalized.get(ordinal) match {
              case Some(prev) if prev.value === canonical.value =>
                // Already finalized at exactly this (ordinal, hash); nothing to do.
                FinalizationOutcome.NoOp.pure[F].widen[FinalizationOutcome]
              case Some(prev) =>
                Async[F].raiseError[FinalizationOutcome](
                  new IllegalStateException(
                    s"MptOverlay finality conflict at ordinal=$ordinal: previously finalized=${prev.value} " +
                      s"now requested=${canonical.value}"
                  )
                )
              case None =>
                pendingRef.get.flatMap { pending =>
                  pending.get(canonical) match {
                    case None =>
                      // Branch never registered with the overlay. Record the finalization (so future calls at
                      // this ordinal are conflict-checked) but there is nothing to apply.
                      finalizedRef
                        .update(_.updated(ordinal, canonical))
                        .as(FinalizationOutcome.NoOp: FinalizationOutcome)
                    case Some(_) =>
                      val merged = mergedChain(canonical, pending)
                      val droppedCount = pending.size - countAncestors(canonical, pending)
                      for {
                        _ <- foldIntoBase(merged, ordinal)
                        _ <- pendingRef.set(Map.empty)
                        _ <- finalizedRef.update(_.updated(ordinal, canonical))
                        _ <- logger.info(
                          s"[MptOverlay] Finalized branch=${canonical.value} at ordinal=$ordinal: " +
                            s"keysApplied=${merged.size} branchesDropped=$droppedCount"
                        )
                      } yield FinalizationOutcome.Folded(merged.size, droppedCount)
                  }
                }
            }
          }
        }

      /** Atomically apply the merged chain delta to the underlying base store.
        *
        * The bracket via `MptStore.withTransaction` ensures partial application cannot persist: if any of the producer-level operations
        * (remove → insertBytes → commit) fails, the savepoint restores the base to its pre-call state. This is the partition-atomicity
        * contract from #56.4.5: a single `finalizeBranch` writes either ALL partitions' deltas (sidecars + user fields) or NONE.
        *
        * `Rethrow.rethrow` on the producer-level `Either` results lifts a `MerklePatriciaError` into `F` so the transaction rolls back
        * rather than swallowing the failure (the legacy `.void` would have left the base half-applied without surfacing the error).
        */
      private def foldIntoBase(merged: ChangeSet, ordinal: SnapshotOrdinal): F[Unit] =
        underlying.withTransaction {
          val producer = underlying.underlying
          for {
            _ <-
              if (merged.removals.nonEmpty) producer.remove(merged.removals.toList).rethrow
              else Async[F].unit
            _ <-
              if (merged.upserts.nonEmpty) producer.insertBytes(merged.upserts).rethrow
              else Async[F].unit
            _ <- underlying.commit(ordinal)
          } yield ((), MptTxAction.Commit)
        }

      private def deserializeBytes[V: ImmutableCodec](bytes: Array[Byte]): F[Option[V]] =
        if (bytes == null || bytes.isEmpty) none[V].pure[F]
        else
          scodec.bits.ByteVector.view(bytes).fromImmutableBytes[V] match {
            case Right(v)  => v.some.pure[F]
            case Left(err) => logger.warn(s"MptOverlay.deserializeBytes: scodec decode failed: $err") >> none[V].pure[F]
          }
    }

    /** Walk the chain leaf→root looking for `hex`. Returns:
      *   - `Some(None)` if the chain explicitly removed the key at some level (caller must NOT fall through to base — the explicit removal
      *     wins).
      *   - `Some(Some(bytes))` if some level upserted it.
      *   - `None` if the chain never mentions the key (caller falls through to base).
      *
      * Pure tail recursion — no F effects, hashtable lookups only.
      */
    @tailrec
    private def walkChainForKey(
      branch: BranchId,
      hex: Hex,
      pending: Map[BranchId, BranchEntry]
    ): Option[Option[Array[Byte]]] =
      pending.get(branch) match {
        case None => None
        case Some(entry) =>
          if (entry.changes.removals.contains(hex)) Some(None)
          else
            entry.changes.upserts.get(hex) match {
              case Some(bytes) => Some(Some(bytes))
              case None        => walkChainForKey(entry.parent, hex, pending)
            }
      }

    /** Walk the chain leaf→root merging `ChangeSet`s in chronological order. At each level, `entry.changes.merge(acc)` ensures the parent's
      * changes are applied first (older), then the accumulated child-side delta on top (newer). Pure recursion.
      */
    @tailrec
    private def mergedChain(
      branch: BranchId,
      pending: Map[BranchId, BranchEntry],
      acc: ChangeSet = ChangeSet.empty
    ): ChangeSet =
      pending.get(branch) match {
        case None        => acc
        case Some(entry) => mergedChain(entry.parent, pending, entry.changes.merge(acc))
      }

    /** Number of branches in `pending` that are on the chain from `branch` to base (inclusive). Used by `finalizeBranch` to compute
      * `branchesDropped = pending.size - ancestors`.
      */
    @tailrec
    private def countAncestors(branch: BranchId, pending: Map[BranchId, BranchEntry], acc: Int = 0): Int =
      pending.get(branch) match {
        case None        => acc
        case Some(entry) => countAncestors(entry.parent, pending, acc + 1)
      }

    /** Set of `BranchId`s on the chain from `branch` to base (inclusive of `branch` itself). Used by eviction (#56.9) to identify branches
      * that must NOT be dropped because they are ancestors of the canonical tip — finality walk-back depends on them being present.
      *
      * Walks parent pointers in `pending` only; stops when a parent isn't in `pending` (= reached the finalized base) OR when revisiting an
      * already-seen branch (defensive cycle guard, though `pendingRef` should never contain cycles).
      */
    @tailrec
    private def walkAncestorsInPending(
      branch: BranchId,
      pending: Map[BranchId, BranchEntry],
      acc: Set[BranchId] = Set.empty
    ): Set[BranchId] =
      if (acc.contains(branch)) acc
      else {
        val updated = acc + branch
        pending.get(branch) match {
          case Some(entry) => walkAncestorsInPending(entry.parent, pending, updated)
          case None        => updated
        }
      }
  }
}
