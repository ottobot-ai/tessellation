package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.kernel.Sync
import cats.syntax.all._

import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec

/** Read-side algebra: branch-scoped reads against an `MptOverlay`. A manager that holds only `GlobalStateReader[F]` cannot mutate state —
  * compile-time bypass-proof. Reads always observe the branch view at construction time (`overlay-at-parent`), never the raw underlying
  * `MptStore`.
  */
trait GlobalStateReader[F[_]] {
  def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]]
  def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]]
  def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]]
}

object GlobalStateReader {

  /** Adapt an `MptStore[F, GlobalStateKey]` as a `GlobalStateReader[F]`. Used by call sites still threading `MptStore` through legacy paths
    * (catch-up / bootstrap, finalized-base reads). Branch-aware reads belong on the overlay path; this adapter is for migration scaffolding
    * where a manager has been converted to `GlobalStateReader[F]` but its consumer still wires it from an `MptStore`.
    */
  def fromMptStore[F[_]](store: MptStore[F, GlobalStateKey]): GlobalStateReader[F] = new GlobalStateReader[F] {
    def get[V: ImmutableCodec](key: GlobalStateKey): F[Option[V]] = store.get[V](key)
    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] = store.getMany[V](keys)
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] = store.getAllForPrefix[V](prefix)
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

    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): F[Map[GlobalStateKey, V]] =
      if (keys.isEmpty) Map.empty[GlobalStateKey, V].pure[F]
      else
        keys.traverseFilter { k =>
          overlay.get[V](parent, k).map(_.map(k -> _))
        }.map(_.toMap)

    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
      overlay.getAllForPrefix[V](parent, prefix)

    def insert[V: ImmutableCodec](key: GlobalStateKey, value: V): F[Unit] = handle.insert(key, value)
    def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit] = handle.insert(entries)
    def remove(key: GlobalStateKey): F[Unit] = handle.remove(key)
    def remove(keys: List[GlobalStateKey]): F[Unit] = handle.remove(keys)
    def update[V: ImmutableCodec](toUpsert: Map[GlobalStateKey, V], toRemove: Set[GlobalStateKey]): F[Unit] =
      handle.update(toUpsert, toRemove)
  }
}
