package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{ChainSelection, ParentChildTree, TipTracker}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, HasherSelector}

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Nakamoto-aware chain storage that handles forks and reorgs.
  *
  * Unlike tessellation's linear SnapshotStorage.prepend (which rejects non-sequential parents), this store maintains:
  *   - A map of all known snapshots by hash
  *   - The current "best tip" as determined by ChainSelection
  *   - Proper reorg support: when a better chain is received, update the canonical head
  *
  * It wraps the underlying SnapshotStorage for actual persistence, using setHeadForRecovery only during reorgs (which is appropriate — it
  * IS a recovery from a shorter/weaker chain).
  */
object NakamotoChainStore {

  /** A stored snapshot with its context and chain metadata */
  case class StoredSnapshot(
    signedSnapshot: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    ordinal: Long,
    slot: Long,
    parentHash: Hash,
    hash: Hash,
    vrfOutput: Array[Byte] = Array.empty
  )

  case class ChainState(
    byHash: Map[Hash, StoredSnapshot], // All known snapshots indexed by hash
    bestTipHash: Option[Hash], // Current best chain tip hash
    lastFinalizedOrdinal: Long // Last finalized ordinal — snapshots below this can be pruned
  )

  object ChainState {
    val empty: ChainState = ChainState(Map.empty, None, 0L)
  }

  trait NakamotoChainStoreAlgebra[F[_]] {

    /** Store a new snapshot. If it extends the best chain or creates a better fork, update the tip. Returns true if the snapshot was new
      * (not a duplicate).
      */
    def store(
      signedSnapshot: Signed[GlobalIncrementalSnapshot],
      context: GlobalSnapshotInfo,
      ordinal: Long,
      slot: Long,
      parentHash: Hash,
      vrfOutput: Array[Byte]
    ): F[Boolean]

    /** Get the current best chain tip */
    def bestTip: F[Option[StoredSnapshot]]

    /** Get the current best tip's slot (for LDD gap calculation) */
    def bestTipSlot: F[Option[Long]]

    /** Get the current best tip's ordinal */
    def bestTipOrdinal: F[Option[Long]]

    /** Number of snapshots in the chain store */
    def chainLength: F[Int]

    /** Number of distinct fork tips (snapshots that aren't parents of other snapshots) */
    def forkCount: F[Int]

    /** Last finalized ordinal */
    def lastFinalizedOrdinal: F[Long]

    /** Get a snapshot by hash */
    def get(hash: Hash): F[Option[StoredSnapshot]]

    /** Get the chain of snapshots from tip back to genesis (or pruning point) */
    def chainFromTip: F[List[StoredSnapshot]]

    /** Get VRF outputs for snapshots in the first 2/3 of a rotation period (for eta calculation). Walks from bestTip — use
      * vrfOutputsForPeriodFrom for fork-aware queries.
      */
    def vrfOutputsForPeriod(period: Long, etaRotationSlots: Long): F[List[(Long, Array[Byte])]]

    /** Get VRF outputs for a rotation period by walking backward from a specific hash. Used to compute eta for an incoming snapshot on a
      * potentially different fork.
      */
    def vrfOutputsForPeriodFrom(period: Long, etaRotationSlots: Long, fromHash: Hash): F[List[(Long, Array[Byte])]]

    /** Mark a snapshot as finalized and prune older fork branches. Keeps the finalized chain but removes orphaned snapshots with ordinal <=
      * finalizedOrdinal that aren't ancestors of the finalized tip.
      */
    def finalize(hash: Hash, ordinal: Long): F[Unit]

    /** Walk the canonical chain from `startHash` backward to find the hash at the given ordinal. */
    def walkBackTo(startHash: Hash, targetOrdinal: Long): F[Option[Hash]]

    /** Get current chain state size (number of stored snapshots) */
    def size: F[Int]

    /** Get the ParentChildTree for chain traversal (used by ChainSelection, reorgs). */
    def tree: ParentChildTree[F]

    /** Get a ChainTip for a given hash (for ChainSelection traversal). */
    def tipFor(hash: Hash): F[Option[ChainTip]]
  }

  def make[F[_]: Async: HasherSelector](
    underlyingStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    chainSelection: ChainSelection[F],
    tipTracker: TipTracker[F]
  ): F[NakamotoChainStoreAlgebra[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoChainStore")

    for {
      stateRef <- Ref.of[F, ChainState](ChainState.empty)
      pcTree <- ParentChildTree.make[F]
    } yield {
      new NakamotoChainStoreAlgebra[F] {

        def store(
          signedSnapshot: Signed[GlobalIncrementalSnapshot],
          context: GlobalSnapshotInfo,
          ordinal: Long,
          slot: Long,
          parentHash: Hash,
          vrfOutput: Array[Byte]
        ): F[Boolean] =
          HasherSelector[F].withCurrent { implicit hasher =>
            signedSnapshot.toHashed[F].flatMap { hashed =>
              val snapshotHash = hashed.hash
              val stored = StoredSnapshot(signedSnapshot, context, ordinal, slot, parentHash, snapshotHash, vrfOutput)
              val vrfHex = VrfOutput(Hex(vrfOutput.map("%02x".format(_)).mkString))
              val newTip = ChainTip(
                snapshotHash,
                Slot(NonNegLong.unsafeFrom(slot)),
                ordinal,
                parentHash,
                vrfHex
              )

              stateRef.modify { state =>
                if (state.byHash.contains(snapshotHash)) {
                  // Duplicate — already stored
                  (state, false.pure[F])
                } else {
                  val newByHash = state.byHash + (snapshotHash -> stored)

                  state.bestTipHash match {
                    case None =>
                      // First snapshot — it's automatically the best
                      val newState = state.copy(byHash = newByHash, bestTipHash = Some(snapshotHash))
                      (
                        newState,
                        pcTree.associate(snapshotHash, parentHash) >>
                          persistHead(stored, snapshotHash) >> logger
                            .info(
                              s"🏗️ Chain initialized at ordinal=$ordinal slot=$slot"
                            )
                            .as(true)
                      )

                    case Some(currentBestHash) =>
                      val currentBest = state.byHash(currentBestHash)
                      val currentTip = ChainTip(
                        currentBestHash,
                        Slot(NonNegLong.unsafeFrom(currentBest.slot)),
                        currentBest.ordinal,
                        currentBest.parentHash,
                        VrfOutput(Hex("00" * 64)) // placeholder for current tip's VRF
                      )

                      val newState = state.copy(byHash = newByHash)

                      val effect = pcTree.associate(snapshotHash, parentHash) >> chainSelection.shouldSwitch(currentTip, newTip).flatMap {
                        case true =>
                          // Better chain — reorg
                          stateRef.update(_.copy(bestTipHash = Some(snapshotHash))) >>
                            persistHead(stored, snapshotHash) >>
                            logger
                              .info(
                                s"Chain reorg: ordinal=$ordinal slot=$slot beats previous tip ordinal=${currentBest.ordinal} slot=${currentBest.slot}"
                              )
                              .as(true)

                        case false if parentHash === currentBestHash =>
                          // Extends current chain — normal case
                          stateRef.update(_.copy(bestTipHash = Some(snapshotHash))) >>
                            persistLinear(stored) >>
                            logger.debug(s"Chain extended to ordinal=$ordinal slot=$slot").as(true)

                        case false =>
                          // Weaker fork — store but don't switch
                          logger
                            .debug(
                              s"🔀 Stored fork snapshot ordinal=$ordinal slot=$slot (not switching)"
                            )
                            .as(true)
                      }

                      (newState, effect)
                  }
                }
              }.flatten
            }
          }

        def bestTip: F[Option[StoredSnapshot]] =
          stateRef.get.map(s => s.bestTipHash.flatMap(s.byHash.get))

        def bestTipSlot: F[Option[Long]] =
          bestTip.map(_.map(_.slot))

        def bestTipOrdinal: F[Option[Long]] =
          bestTip.map(_.map(_.ordinal))

        def chainLength: F[Int] =
          stateRef.get.map(_.byHash.size)

        def forkCount: F[Int] =
          stateRef.get.map { state =>
            val parentHashes = state.byHash.values.map(_.parentHash).toSet
            state.byHash.keys.count(h => !parentHashes.contains(h))
          }

        def lastFinalizedOrdinal: F[Long] =
          stateRef.get.map(_.lastFinalizedOrdinal)

        def get(hash: Hash): F[Option[StoredSnapshot]] =
          stateRef.get.map(_.byHash.get(hash))

        def chainFromTip: F[List[StoredSnapshot]] =
          stateRef.get.map { state =>
            state.bestTipHash match {
              case None          => Nil
              case Some(tipHash) =>
                // Walk back from tip through parents
                val chain = scala.collection.mutable.ListBuffer.empty[StoredSnapshot]
                var current = state.byHash.get(tipHash)
                while (current.isDefined) {
                  chain += current.get
                  current = state.byHash.get(current.get.parentHash)
                }
                chain.toList
            }
          }

        def vrfOutputsForPeriod(period: Long, etaRotationSlots: Long): F[List[(Long, Array[Byte])]] =
          stateRef.get.map { state =>
            collectVrfOutputsForPeriod(state, period, etaRotationSlots, state.bestTipHash)
          }

        def vrfOutputsForPeriodFrom(period: Long, etaRotationSlots: Long, fromHash: Hash): F[List[(Long, Array[Byte])]] =
          stateRef.get.map { state =>
            collectVrfOutputsForPeriod(state, period, etaRotationSlots, Some(fromHash))
          }

        private def collectVrfOutputsForPeriod(
          state: ChainState,
          period: Long,
          etaRotationSlots: Long,
          startHash: Option[Hash]
        ): List[(Long, Array[Byte])] = {
          val periodStart = period * etaRotationSlots
          val cutoff = periodStart + (etaRotationSlots * 2 / 3)
          // Walk chain from the given starting hash backward.
          // Using byHash.values would include fork branches, causing different nodes
          // to compute different eta values → VRF verification failures at rotation boundaries.
          val canonicalSnapshots = scala.collection.mutable.ListBuffer.empty[StoredSnapshot]
          var current = startHash.flatMap(state.byHash.get)
          while (current.isDefined && current.get.slot >= periodStart) {
            if (current.get.slot < cutoff && current.get.vrfOutput.nonEmpty)
              canonicalSnapshots += current.get
            current = state.byHash.get(current.get.parentHash)
          }
          canonicalSnapshots.toList
            .sortBy(_.slot)
            .map(s => (s.slot, s.vrfOutput))
        }

        def finalize(hash: Hash, ordinal: Long): F[Unit] =
          stateRef.modify { state =>
            // Collect hashes on the canonical chain from finalized tip backward
            val canonicalHashes = scala.collection.mutable.Set.empty[Hash]
            var current = state.byHash.get(hash)
            while (current.isDefined) {
              canonicalHashes += current.get.hash
              current = state.byHash.get(current.get.parentHash)
            }

            // Prune: remove snapshots with ordinal <= finalized that aren't on canonical chain
            val pruned = state.byHash.filter {
              case (h, s) =>
                s.ordinal > ordinal || canonicalHashes.contains(h)
            }
            val prunedCount = state.byHash.size - pruned.size

            (
              state.copy(byHash = pruned, lastFinalizedOrdinal = ordinal),
              logger.info(s"🔒 Finalized ordinal=$ordinal, pruned $prunedCount orphan snapshots (${pruned.size} remaining)")
            )
          }.flatten

        def walkBackTo(startHash: Hash, targetOrdinal: Long): F[Option[Hash]] =
          stateRef.get.map { state =>
            var current = state.byHash.get(startHash)
            while (current.isDefined && current.get.ordinal > targetOrdinal)
              current = state.byHash.get(current.get.parentHash)
            current.filter(_.ordinal == targetOrdinal).map(_.hash)
          }

        def size: F[Int] =
          stateRef.get.map(_.byHash.size)

        val tree: ParentChildTree[F] = pcTree

        def tipFor(hash: Hash): F[Option[ChainTip]] =
          stateRef.get.map(_.byHash.get(hash).map { stored =>
            ChainTip(
              hash = stored.hash,
              slot = Slot(NonNegLong.unsafeFrom(stored.slot)),
              ordinal = stored.ordinal,
              parentHash = stored.parentHash,
              vrfOutput = VrfOutput(Hex(stored.vrfOutput.map("%02x".format(_)).mkString))
            )
          })

        /** Persist to underlying storage by extending the linear chain */
        private def persistLinear(stored: StoredSnapshot)(implicit hasher: Hasher[F]): F[Unit] =
          underlyingStorage.prepend(stored.signedSnapshot, stored.context).flatMap {
            case true  => logger.debug(s"💾 Prepended ordinal=${stored.ordinal} to linear storage")
            case false =>
              // prepend failed (parent mismatch) — fall back to setHead
              logger.warn(s"⚠️ prepend failed for ordinal=${stored.ordinal}, setting head for reorg") >>
                underlyingStorage.setHeadForRecovery(stored.signedSnapshot, stored.context).void
          }

        /** Persist during reorg — always uses setHead since we're switching chains */
        private def persistHead(stored: StoredSnapshot, hash: Hash)(implicit hasher: Hasher[F]): F[Unit] =
          underlyingStorage.setHeadForRecovery(stored.signedSnapshot, stored.context) >>
            logger.debug(s"💾 Set head to ordinal=${stored.ordinal} hash=${hash.value.take(8)}")
      }
    }
  }
}
