package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.MptStore
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaTrie
import io.constellationnetwork.security.mpt.producer.MerklePatriciaError
import io.constellationnetwork.serde.ImmutableCodec

/** Branch identity within the overlay. Reuses chain-store snapshot identity (the `Hash` of a snapshot/block tip) so that callers can pass
  * `chainStore.bestTip` (or any known parent hash) without any conversion. The opaque-ness target of the rev3 plan is approximated here as a
  * value-class wrapper — Scala 2.13 has no opaque types, but the wrapper preserves "do not concatenate / mistakenly compare with raw Hash".
  */
final case class BranchId(value: Hash) extends AnyVal

object BranchId {

  /** Sentinel id reserved for the passthrough overlay's single notional branch. Not a real chain-store hash; passthrough callers do not
    * depend on its exact value, only on the fact that it is stable for the lifetime of the overlay.
    */
  val passthrough: BranchId = BranchId(Hash.empty)
}

/** Outcome of `MptOverlay.finalizeBranch`. The plan (#56.6) defines this as the single return value the finality sink reads after each call.
  */
sealed trait FinalizationOutcome
object FinalizationOutcome {

  /** A branch's deltas were folded forward into the on-disk base, and zero or more non-canonical branches were dropped.
    *   - In multi-branch mode (#56.4+), `keysApplied` and `branchesDropped` are populated from the actual fold-forward.
    *   - In passthrough mode (#56.2), this is never returned — the passthrough always returns `NoOp` because there is no overlay state to
    *     fold.
    */
  final case class Folded(keysApplied: Int, branchesDropped: Int) extends FinalizationOutcome

  /** Idempotency / passthrough sentinel: nothing to fold. Returned when `(ordinal, hash)` already finalized, or when the overlay is in
    * passthrough mode and the underlying `MptStore` already holds the canonical state.
    */
  case object NoOp extends FinalizationOutcome
}

/** Mutable handle for accumulating writes against a checked-out branch.
  *
  * In passthrough mode the writes go straight to the underlying `MptStore`. In multi-branch mode (#56.4) the writes accumulate into a
  * `ChangeSet` keyed by `BranchId`, and only `commit` materializes the per-branch view.
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
  * Goal (rev3 plan, #56): each pending branch gets its own state view (deltas over a finalized base). Only the finalized state is persisted;
  * pending branches live in memory. When finality advances, the canonical branch's diffs apply forward; non-canonical branches are dropped.
  *
  * '''#56.2 milestone (this file)''': defines the trait and ships a single passthrough impl that delegates straight to `MptStore`. The
  * passthrough is correctness-equivalent to the current direct-`MptStore` model — `checkout` / `commit` are no-ops, reads/writes hit
  * `MptStore` regardless of the supplied `BranchId`. This lets the overlay be wired into call sites behind a feature flag without changing
  * behavior. Multi-branch semantics arrive in #56.4.
  *
  * '''Layering''': WRAP, not parameterize. The 14+ existing `MptStore` call sites stay untouched until callers are migrated to the overlay
  * one at a time. `base` is exposed so legacy paths can keep working during migration.
  */
trait MptOverlay[F[_], K] {

  /** The underlying finalized-state store. Exposed for migration / non-overlay-aware call sites. New code should prefer overlay reads. */
  def base: MptStore[F, K]

  /** Block-tree topology shared with consensus. Reused — no separate tree maintained by the overlay. */
  def parentChildTree: ParentChildTree[F]

  /** Check out a writable handle whose writes will become a child of `parent`. In passthrough mode the returned handle delegates straight to
    * the underlying `MptStore`; the `parent` argument is retained on the handle for later `commit`.
    */
  def checkout(parent: BranchId): F[BranchHandle[F, K]]

  /** Commit the writes accumulated on `branch` as the child branch identified by `childTip` at `ordinal`. Multi-branch mode: registers
    * `(parent → childTip)` in `parentChildTree` and stores the accumulated `ChangeSet` keyed by `childTip`. Passthrough mode: associates the
    * tip in the parent-child tree (so consensus topology stays accurate) and otherwise no-ops — writes already landed in the underlying
    * store.
    */
  def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit]

  /** Read at a specific branch view. Passthrough: ignores `branch`, delegates to `MptStore.get`. */
  def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]]

  /** Prefix scan at a specific branch view. Passthrough: ignores `branch`, delegates to `MptStore.getAllForPrefix`. */
  def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]]

  /** Build the per-branch trie at `ordinal`. Passthrough: ignores `branch`, delegates to `MptStore.build`. Multi-branch (#56.5): composes
    * the on-disk base trie with the branch's accumulated `ChangeSet` via `MerklePatriciaTrie.withChanges`.
    */
  def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  /** Finality sink (#56.6). Idempotent on `(ordinal, canonical)`. Passthrough always returns `NoOp` — there is no overlay state to fold.
    * Multi-branch: applies the canonical branch's accumulated delta to the on-disk base, drops non-canonical branches at `ordinal`, prunes
    * `parentChildTree` below the new finalized height.
    *
    * Errors loudly when called twice at the same `ordinal` with different `canonical` hashes (depth-k vs attestation-2/3 disagreement
    * during partition).
    */
  def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome]
}

object MptOverlay {

  /** Factory that dispatches on the `MPT_OVERLAY_ENABLED` feature flag. While #56.2 is the active milestone there is only one impl —
    * passthrough — so both branches return passthrough. When the multi-branch impl lands in #56.4 this is the dispatch point that swaps in
    * the real overlay.
    *
    * Default expected wiring (caller sets `enabled = false` or omits): passthrough (correctness-equivalent to direct `MptStore` use).
    */
  def make[F[_]: Async, K](
    enabled: Boolean,
    underlying: MptStore[F, K],
    pcTree: ParentChildTree[F]
  ): F[MptOverlay[F, K]] = {
    val _ = enabled // multi-branch impl arrives in #56.4; until then both flag values route to passthrough
    Async[F].pure(passthrough(underlying, pcTree))
  }

  /** Single-branch passthrough — correctness-equivalent to using `MptStore` directly. The `BranchId` argument on every method is ignored.
    * `commit` registers the branch in `parentChildTree` so consensus topology stays consistent with what multi-branch mode will need.
    *
    * Use case: feature-flag `MPT_OVERLAY_ENABLED = false` (default) wires this impl, so toggling the flag does not change behavior. When
    * #56.4 lands the multi-branch impl, switching the flag on swaps the wiring.
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
        // Topology bookkeeping only — writes already landed in `underlying` during the handle's lifetime.
        // Skip self-association if checkout was given the same id (e.g. genesis bootstrap), which would
        // otherwise create a self-loop in the parent-child tree.
        if (branch.parent.value === childTip.value) Async[F].unit
        else pcTree.associate(childTip.value, branch.parent.value)

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
}
