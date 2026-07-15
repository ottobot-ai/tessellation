package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec

/** Read-side algebra: branch-scoped reads against an `MptOverlay`. A manager that holds only `GlobalStateReader[F]` cannot mutate state —
  * compile-time bypass-proof. Reads always observe the branch view at construction time (`overlay-at-parent`), never the raw underlying
  * `MptStore`.
  */
trait GlobalStateReader[F[_]] {
  def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]]
  def getStrict[V: ImmutableCodec](key: GlobalStateKey): F[StrictMptRead[V]]
  def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]]
  def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]]
  def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]]
}

object GlobalStateReader {

  /** Adapt an `MptStore[F, GlobalStateKey]` as a `GlobalStateReader[F]`. Used by call sites still threading `MptStore` through legacy paths
    * (catch-up / bootstrap, finalized-base reads). Branch-aware reads belong on the overlay path; this adapter is for migration scaffolding
    * where a manager has been converted to `GlobalStateReader[F]` but its consumer still wires it from an `MptStore`.
    */
  def fromMptStore[F[_]](store: MptStore[F, GlobalStateKey]): GlobalStateReader[F] = new GlobalStateReader[F] {
    def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]] = store.get[V](key)
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): F[StrictMptRead[V]] = store.getStrict[V](key)
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] = store.getMany[V](keys)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] = store.getAllForPrefix[V](prefix)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      store.getAllForPrefixStrict[V](prefix)
  }

  /** Branch-aware reader bound to a specific `parent`. Routes every read to `overlay.get(parent, ...)`, so under MultiBranch the
    * prior-state view picks up parent-branch entries that haven't yet been folded into the underlying base. Used by the per-manager
    * prior-state lookups inside `accept()`, where reading at `parentTip` (not `overlay.base`) is what makes a child snapshot see its
    * parent's pending writes (#56.10 Phase J).
    */
  def fromOverlay[F[_]: cats.Monad](
    overlay: MptOverlay[F, GlobalStateKey],
    parent: BranchId
  ): GlobalStateReader[F] = dynamic[F](overlay, cats.Applicative[F].pure(parent))

  /** Branch-aware reader whose `parent` is read from `currentParent` on every call. Lets a manager constructed once at GSAM `make` stay
    * valid across many `accept()` calls — each call rebinds the reader's view by writing to a `Ref[F, BranchId]` whose `.get` is wired in
    * here. Caller MUST serialize accept() (consensus FSM + snapshot semaphore both do this); a parallel writer would race the Ref. Under
    * Passthrough the BranchId is ignored on every read, so this collapses to base-equivalent reads.
    */
  def dynamic[F[_]: cats.Monad](
    overlay: MptOverlay[F, GlobalStateKey],
    currentParent: F[BranchId]
  ): GlobalStateReader[F] = new GlobalStateReader[F] {
    import cats.syntax.flatMap._
    import cats.syntax.functor._
    def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]] =
      currentParent.flatMap(p => overlay.get[V](p, key))
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): F[StrictMptRead[V]] =
      currentParent.flatMap(p => overlay.getStrict[V](p, key))
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] =
      if (keys.isEmpty) cats.Applicative[F].pure(Map.empty)
      else
        currentParent.flatMap { p =>
          cats.Traverse[List].traverse(keys)(k => overlay.get[V](p, k).map(_.map(k -> _))).map(_.flatten.toMap)
        }
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
      currentParent.flatMap(p => overlay.getAllForPrefix[V](p, prefix))
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      currentParent.flatMap(p => overlay.getAllForPrefixStrict[V](p, prefix))
  }

  /** GL0 HTTP / read-path constructor (#117/#118 Phase 2). Reads at the chain's current best tip so callers see the canonical
    * pending-or-base view — under `OverlayMode.MultiBranch` the chain's pending writes haven't landed in the underlying base yet, and
    * legacy `MptStore` reads would lag by `finalizeBranch.foldIntoBase` cycles (~5s healthy, minutes under finality stalls). When
    * `bestTipBranchF` resolves to `None` (pre-bootstrap window or follower with no chain store), reads fall back to `BranchId.base`, which
    * the multi-branch overlay resolves directly to the underlying `MptStore` (no pending walk). Under `OverlayMode.Passthrough` the
    * `BranchId` is ignored on every read, so `pending` collapses to base-equivalent reads.
    *
    * Followers (gl1/cl1/dl1/ml0) MUST NOT call this — they should construct `finalized` instead, since per-layer rule says followers see
    * only finalized gl0 state. Compile-time guard: a follower module has no `MptOverlay` in scope at its `Services.make`, so it cannot
    * construct `pending`.
    */
  def pending[F[_]: cats.Monad](
    overlay: MptOverlay[F, GlobalStateKey],
    bestTipBranchF: F[Option[BranchId]]
  ): GlobalStateReader[F] =
    dynamic[F](overlay, cats.Functor[F].map(bestTipBranchF)(_.getOrElse(BranchId.base)))

  /** Follower read-path constructor (#117/#118 Phase 2). Reads from the finalized base `MptStore` directly — equivalent to `fromMptStore`.
    * Provided as a named factory so call sites in follower modules (gl1/cl1/dl1/ml0) make the "finalized-only" choice explicit at
    * construction time and don't accidentally pick up overlay-pending state. Has the same type as `pending` but no `MptOverlay` dependency.
    */
  def finalized[F[_]](store: MptStore[F, GlobalStateKey]): GlobalStateReader[F] =
    fromMptStore[F](store)

  /** Empty reader that always returns no values. Used by tests / wirings where no MPT is available — equivalent to "no prior state". */
  def empty[F[_]: cats.Applicative]: GlobalStateReader[F] = new GlobalStateReader[F] {
    def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]] = cats.Applicative[F].pure(None)
    def getStrict[V: ImmutableCodec](key: GlobalStateKey): F[StrictMptRead[V]] =
      cats.Applicative[F].pure(StrictMptRead.Absent)
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] =
      cats.Applicative[F].pure(Map.empty)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
      cats.Applicative[F].pure(Map.empty)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      cats.Applicative[F].pure(List.empty)
  }
}

/** Write-side algebra: accumulate deltas in a checked-out branch handle. Mutations are local to this writer's branch until commit; a
  * sibling branch checked out from the same overlay observes none of these writes until `MptOverlay.commit` registers them.
  */
trait GlobalStateWriter[F[_]] {
  def insert[V: ImmutableCodec](key: GlobalStateKey, value: V): F[Unit]
  def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit]
  def remove(key: GlobalStateKey): F[Unit]
  def remove(keys: List[GlobalStateKey]): F[Unit]
  def update[V: ImmutableCodec](toUpsert: Map[GlobalStateKey, V], toRemove: Set[GlobalStateKey]): F[Unit]
}

/** Combined reader + writer for the acceptance pipeline.
  *
  * `GlobalSnapshotAcceptanceManager.accept()` constructs a single `AcceptanceMpt[F]` at the top of the call (against the parent's branch
  * tip) and threads it through the per-field managers; subsystems that only need to read take `GlobalStateReader[F]` directly.
  *
  * '''Read-write contract within accept()''': the acceptance pipeline reads all priors from MPT before computing deltas, computes each
  * field's deltas in memory, then writes once at the end via the writer. There is no within-call read-your-own-write requirement: the
  * reader observes the parent-branch view, the writer accumulates locally, and commit registers the accumulated delta as a child of the
  * parent. This matches the existing `MptStore`-based pipeline's semantics; the algebra preserves them while moving away from the live
  * mutating store.
  */
trait AcceptanceMpt[F[_]] extends GlobalStateReader[F] with GlobalStateWriter[F]

object AcceptanceMpt {

  /** Construct from an overlay + branch context.
    *
    *   - `parent` is the branch the writer's commits will be a child of. Reads see `overlay-at-parent`.
    *   - `handle` carries the writer's per-call delta. Writes accumulate into this handle's `ChangeSet`; readers checked out from a sibling
    *     branch on the same overlay observe none of these writes.
    *
    * Reads route to `overlay.get(parent, key)` — branch-aware, walking the overlay's pending chain back to the finalized base.
    */
  def fromOverlay[F[_]: Sync](
    overlay: MptOverlay[F, GlobalStateKey],
    parent: BranchId,
    handle: BranchHandle[F, GlobalStateKey]
  ): AcceptanceMpt[F] = new AcceptanceMpt[F] {

    def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]] =
      overlay.get[V](parent, key)

    def getStrict[V: ImmutableCodec](key: GlobalStateKey): F[StrictMptRead[V]] =
      overlay.getStrict[V](parent, key)

    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] =
      if (keys.isEmpty) Map.empty[GlobalStateKey, V].pure[F]
      else
        keys.traverseFilter { k =>
          overlay.get[V](parent, k).map(_.map(k -> _))
        }.map(_.toMap)

    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
      overlay.getAllForPrefix[V](parent, prefix)

    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      overlay.getAllForPrefixStrict[V](parent, prefix)

    def insert[V: ImmutableCodec](key: GlobalStateKey, value: V): F[Unit] = handle.insert(key, value)
    def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit] = handle.insert(entries)
    def remove(key: GlobalStateKey): F[Unit] = handle.remove(key)
    def remove(keys: List[GlobalStateKey]): F[Unit] = handle.remove(keys)
    def update[V: ImmutableCodec](toUpsert: Map[GlobalStateKey, V], toRemove: Set[GlobalStateKey]): F[Unit] =
      handle.update(toUpsert, toRemove)
  }
}
