package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptyChain
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.security.hash.Hash

/** Parent-child tree for chain snapshots (adapted from Bifrost's EventSourcedState pattern).
  *
  * Tracks parent→child relationships for all known snapshots. Enables:
  *   - Common ancestor finding (for fork detection)
  *   - Chain traversal (for reorg: unapply back to ancestor, apply forward on winning fork)
  *   - Height queries (for chain selection)
  *
  * Backed by an in-memory Ref[Map] — sufficient for Nakamoto consensus where we only need to track snapshots between finality boundaries
  * (finalized snapshots can be pruned).
  */
trait ParentChildTree[F[_]] {

  /** Returns the parent hash of the given child. None if root or unknown. */
  def parentOf(child: Hash): F[Option[Hash]]

  /** Register a child→parent relationship. */
  def associate(child: Hash, parent: Hash): F[Unit]

  /** Get height (distance from root/genesis). */
  def heightOf(hash: Hash): F[Long]

  /** Find the common ancestor of two hashes. Returns (pathFromA, pathFromB) where both paths end at the common ancestor (inclusive). The
    * common ancestor is the HEAD of each returned chain. The original hash is the LAST element of each returned chain.
    */
  def findCommonAncestor(a: Hash, b: Hash): F[(NonEmptyChain[Hash], NonEmptyChain[Hash])]

  /** Prune entries at or below the given height. Keeps the entry at exactHeight as the new root. */
  def pruneBelow(belowHeight: Long): F[Int]
}

object ParentChildTree {

  /** In-memory implementation backed by Ref. Stores (parent, height) for each hash.
    */
  def make[F[_]: Async]: F[ParentChildTree[F]] =
    Ref.of[F, Map[Hash, (Hash, Long)]](Map.empty).map { ref =>
      new ParentChildTree[F] {

        def parentOf(child: Hash): F[Option[Hash]] =
          ref.get.map(_.get(child).map(_._1))

        def associate(child: Hash, parent: Hash): F[Unit] =
          ref.update { m =>
            val parentHeight = m.get(parent).map(_._2).getOrElse(0L)
            m.updated(child, (parent, parentHeight + 1))
          }

        def heightOf(hash: Hash): F[Long] =
          ref.get.map(_.get(hash).map(_._2).getOrElse(0L))

        def findCommonAncestor(a: Hash, b: Hash): F[(NonEmptyChain[Hash], NonEmptyChain[Hash])] =
          if (a === b)
            (NonEmptyChain.one(a), NonEmptyChain.one(b)).pure[F]
          else
            for {
              aHeight <- heightOf(a)
              bHeight <- heightOf(b)
              // Bring both to same height
              aChainAtEqual <- traverseBackToHeight(NonEmptyChain.one(a), aHeight, Math.min(aHeight, bHeight))
              bChainAtEqual <- traverseBackToHeight(NonEmptyChain.one(b), bHeight, Math.min(aHeight, bHeight))
              // Walk both back until heads match
              result <- (aChainAtEqual, bChainAtEqual).iterateUntilM {
                case (aChain, bChain) =>
                  (prependWithParent(aChain), prependWithParent(bChain)).tupled
              } { case (aChain, bChain) => aChain.head === bChain.head }
            } yield result

        def pruneBelow(belowHeight: Long): F[Int] =
          ref.modify { m =>
            val (keep, prune) = m.partition { case (_, (_, h)) => h >= belowHeight }
            (keep, prune.size)
          }

        private def prependWithParent(chain: NonEmptyChain[Hash]): F[NonEmptyChain[Hash]] =
          parentOf(chain.head).flatMap {
            case Some(parent) => chain.prepend(parent).pure[F]
            case None =>
              Async[F].raiseError(
                new NoSuchElementException(s"No parent found for ${chain.head.value.take(12)} — possibly pruned or unknown")
              )
          }

        private def traverseBackToHeight(
          chain: NonEmptyChain[Hash],
          currentHeight: Long,
          targetHeight: Long
        ): F[NonEmptyChain[Hash]] =
          if (currentHeight <= targetHeight) chain.pure[F]
          else
            prependWithParent(chain).flatMap(newChain => traverseBackToHeight(newChain, currentHeight - 1, targetHeight))
      }
    }
}
