package io.constellationnetwork.security.smt

import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.smt.node.{SmtNode, SmtNodeOps}

/** The versioned / historical layer — the JMT-defining piece (Part C.2 slice S3). Makes "prove key K = V at finalized ordinal N" a
  * first-class operation: every committed version's root node is retained (bounded), and a proof for `(version, key)` is generated directly
  * against that version's node — INCLUSION or ABSENCE — with no in-memory branch replay (the gap #287 / B.2 closes here).
  *
  * In-memory model (the Diem/JMT version-prefixed-node idea, expressed via structural sharing rather than on-disk node keys): commits build
  * a fresh root [[SmtNode]] from the previous one; unchanged subtrees are SHARED across consecutive versions, so retaining `R` versions
  * costs `Θ(R × changed-nodes-per-commit)`, not `R` full trees. Retention is bounded (the oldest version roots are dropped, like the
  * NIPoPoW historical-stake retention pattern); once a version's root is dropped, the subtrees it uniquely held become garbage-collectable.
  *
  * This is the in-memory reference for the layer; an on-disk version-keyed store (true JMT `NodeKey = version|nibble_path`) is the
  * durability follow-up and is intentionally out of scope here.
  */
trait VersionedSmt[F[_]] {

  /** Commit a new version: apply `removes` then `upserts` (upsert-wins) on top of the latest committed state, record the resulting root
    * under `version`, prune to the retention bound, and return the new [[SmtRoot]]. Commits are expected in non-decreasing `version` order;
    * a commit at an already-seen version overwrites it (idempotent re-commit of the same changes reproduces the same root).
    */
  def commit(version: SnapshotOrdinal, upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SmtRoot]

  /** The root committed at `version`, or `None` if that version is not under retention. */
  def rootAt(version: SnapshotOrdinal): F[Option[SmtRoot]]

  /** Prove `key`'s status (INCLUSION or ABSENCE) against the root committed at `version`. `Left(UnknownVersion)` if `version` is not
    * retained.
    */
  def proveAt(version: SnapshotOrdinal, key: Hex): F[Either[SmtProofError, SmtProof]]

  /** The set of versions currently under retention (ascending). */
  def retainedVersions: F[List[SnapshotOrdinal]]
}

object VersionedSmt {

  /** Internal state: the per-version retained root nodes (sorted by ordinal) and the latest committed root node (the base for the next
    * commit). `live` is the node of the highest committed version (or [[SmtNode.Empty]] before any commit).
    */
  private final case class State(versions: SortedMap[SnapshotOrdinal, SmtNode], live: SmtNode)

  /** Create a versioned SMT retaining the most recent `retention` committed versions (must be >= 1). */
  def make[F[_]: Async: Hasher](retention: Int): F[VersionedSmt[F]] = {
    val bound = math.max(1, retention)
    Ref.of[F, State](State(SortedMap.empty[SnapshotOrdinal, SmtNode], SmtNode.Empty)).map { stateRef =>
      new VersionedSmt[F] {

        def commit(version: SnapshotOrdinal, upserts: Map[Hex, Array[Byte]], removes: Set[Hex]): F[SmtRoot] =
          for {
            state <- stateRef.get
            afterRemoves <- removes.toList.foldLeftM(state.live) { (acc, key) =>
              SmtHashing.position[F](key).flatMap(pos => SmtNodeOps.remove[F](acc, pos, 0))
            }
            afterUpserts <- upserts.toList.foldLeftM(afterRemoves) {
              case (acc, (key, value)) =>
                for {
                  pos <- SmtHashing.position[F](key)
                  vd <- Hasher[F].hashBytes(value)
                  next <- SmtNodeOps.insert[F](acc, key, pos, vd, value, 0)
                } yield next
            }
            newVersions = prune(state.versions.updated(version, afterUpserts))
            _ <- stateRef.set(State(newVersions, afterUpserts))
          } yield SmtRoot(afterUpserts.digest)

        def rootAt(version: SnapshotOrdinal): F[Option[SmtRoot]] =
          stateRef.get.map(_.versions.get(version).map(node => SmtRoot(node.digest)))

        def proveAt(version: SnapshotOrdinal, key: Hex): F[Either[SmtProofError, SmtProof]] =
          stateRef.get.flatMap { state =>
            state.versions.get(version) match {
              case None => (SmtProofError.UnknownVersion(version): SmtProofError).asLeft[SmtProof].pure[F]
              case Some(node) =>
                SmtHashing.position[F](key).flatMap(pos => SmtNodeOps.prove[F](node, key, pos))
            }
          }

        def retainedVersions: F[List[SnapshotOrdinal]] =
          stateRef.get.map(_.versions.keys.toList)

        /** Keep only the most-recent `bound` versions by ordinal. */
        private def prune(versions: SortedMap[SnapshotOrdinal, SmtNode]): SortedMap[SnapshotOrdinal, SmtNode] =
          if (versions.size <= bound) versions
          else SortedMap.from(versions.toList.takeRight(bound))
      }
    }
  }
}
