package io.constellationnetwork.node.shared.domain.snapshot.storage

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.snapshot.Snapshot
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher}

trait SnapshotStorage[F[_], S <: Snapshot, State] {

  def prepend(snapshot: Signed[S], state: State)(implicit hasher: Hasher[F]): F[Boolean]

  def head: F[Option[(Signed[S], State)]]
  def headSnapshot: F[Option[Signed[S]]]

  def get(ordinal: SnapshotOrdinal): F[Option[Signed[S]]]
  def getHashed(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[F]): F[Option[Hashed[S]]]

  def get(hash: Hash): F[Option[Signed[S]]]
  def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[F]): F[Option[Hash]]

  /** Reset head to the given snapshot for incremental recovery. Unlike prepend, this does not require sequential ordinals — it directly
    * sets the head and writes to disk (deleting any existing file at that ordinal first). Use for validated snapshots only. For
    * deferred-validation paths, use setTentativeHead.
    */
  def setHeadForRecovery(snapshot: Signed[S], state: State)(implicit hasher: Hasher[F]): F[Unit]

  /** Set head in-memory only, deferring the disk write until confirmHead is called. Used by Nakamoto reorg/catch-up paths where content
    * validation is deferred. The snapshot is served via head/get immediately but is not persisted.
    */
  def setTentativeHead(snapshot: Signed[S], state: State)(implicit hasher: Hasher[F]): F[Unit]

  /** Persist a tentative snapshot to disk by hash. Deletes any existing file at that ordinal, writes the confirmed snapshot, and evicts all
    * other tentative entries at that ordinal.
    */
  def confirmHead(hash: Hash): F[Unit]

  /** Discard all tentative entries at ordinals <= the given finalized ordinal. Called when finality is reached — unconfirmed tentative
    * snapshots below finality are stale.
    */
  def pruneTentative(finalizedOrdinal: SnapshotOrdinal): F[Unit]

}
