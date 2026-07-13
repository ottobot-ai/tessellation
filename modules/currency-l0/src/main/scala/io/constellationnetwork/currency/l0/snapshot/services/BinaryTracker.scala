package io.constellationnetwork.currency.l0.snapshot.services

import cats.effect.Ref
import cats.effect.kernel.Async
import cats.kernel.Eq
import cats.syntax.all._

import scala.collection.immutable.Queue

import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import derevo.cats.eqv
import derevo.derive
import eu.timepit.refined.auto._
import eu.timepit.refined.cats._
import eu.timepit.refined.types.all.NonNegLong

@derive(eqv)
sealed trait TrackedBinary

case class PendingBinary(
  binary: Hashed[StateChannelSnapshotBinary],
  currencySnapshotOrdinal: SnapshotOrdinal,
  enqueuedAtOrdinal: SnapshotOrdinal,
  sendsSoFar: NonNegLong
) extends TrackedBinary

case class ConfirmedBinary(
  pendingBinary: PendingBinary,
  confirmationProof: GlobalSnapshotConfirmationProof
) extends TrackedBinary

case class GlobalSnapshotConfirmationProof(
  globalHash: Hash,
  globalOrdinal: SnapshotOrdinal,
  globalEpochProgress: io.constellationnetwork.schema.epoch.EpochProgress
)

object GlobalSnapshotConfirmationProof {
  def fromGlobalSnapshot(snapshot: Hashed[GlobalIncrementalSnapshot]): GlobalSnapshotConfirmationProof =
    GlobalSnapshotConfirmationProof(snapshot.hash, snapshot.ordinal, snapshot.epochProgress)
}

case class TrackerState(
  tracked: Queue[TrackedBinary],
  cap: NonNegLong,
  retryMode: Boolean,
  noConfirmationsSinceRetryCount: NonNegLong,
  backoffExponent: NonNegLong,
  // Hashes of pending binaries observed in a gl0 best-tip snapshot (unfinalized).
  // Used as an *operational signal* to keep retry-cap healthy under G1 finality-
  // gating: a binary that landed in gl0 best-tip is progressing through gl0
  // consensus, so retry-mode should not panic and shrink cap as if no confirmations
  // are happening. Distinct from `ConfirmedBinary` status — soft observations do
  // NOT promote Pending→Confirmed, so a best-tip reorg cannot strand the binary in
  // confirmed-but-never-pruned state.
  //
  // Cleared per-hash when the binary is truly confirmed (markAsConfirmed) or pruned
  // (pruneFinalizedBelow). #123.
  softObservedHashes: Set[Hash],
  // Transitional ordinal high-water mark from GL0's current Phase-2 GSI view. It is
  // monotone here and drives stale-Pending GC, but target Phase 2 is hash-bound and
  // density-reorgable. A replacement may require exact older binaries this watermark
  // deleted. Retention/requeue must become branch-aware before this GC is production-safe.
  highestConfirmedCurrencyOrd: SnapshotOrdinal
)

object TrackerState {
  def empty: TrackerState = TrackerState(
    tracked = Queue.empty[TrackedBinary],
    cap = NonNegLong.unsafeFrom(4L),
    retryMode = false,
    noConfirmationsSinceRetryCount = NonNegLong.MinValue,
    backoffExponent = NonNegLong.MinValue,
    softObservedHashes = Set.empty,
    highestConfirmedCurrencyOrd = SnapshotOrdinal.MinValue
  )
}

trait BinaryTracker[F[_]] {
  def enqueue(
    binary: Hashed[StateChannelSnapshotBinary],
    currencySnapshotOrdinal: SnapshotOrdinal,
    enqueuedAtGlobal: SnapshotOrdinal
  ): F[Unit]
  def markAsSent(binaryHash: Hash): F[Unit]

  /** Mark binaries as confirmed and (optionally) advance the GC watermark to gl0's authoritative
    * `lastCurrencySnapshots[ourIdentifier].ordinal`.
    *
    * The watermark MUST come from gl0's `GlobalSnapshotInfo.lastCurrencySnapshots` (the authoritative gl0 view), not inferred from local
    * hash matches. Even under BFT metagraph consensus (where ml0 nodes vote on a single binary per ord and don't race), brief metagraph
    * forks can cause our local Pendings to be on an abandoned branch gl0 didn't pick. The local-hash signal is silent in those cases (we
    * never see our hash in gl0's snapshot). gl0's known ord makes those stale Pendings GC-able regardless.
    */
  def markAsConfirmed(
    confirmedHashes: Set[Hash],
    proof: GlobalSnapshotConfirmationProof,
    gl0KnownCurrencyOrd: Option[SnapshotOrdinal]
  ): F[Unit]

  /** Mark binaries as observed in an unfinalized gl0 snapshot (best-tip).
    *
    * Operational signal only — does NOT promote tracked entries to ConfirmedBinary. Used by RetryStrategy to keep cap healthy under G1
    * finality-gating: a binary that landed in gl0 best-tip is progressing through consensus, so retry-mode shouldn't shrink cap as if no
    * progress is happening. Safe under best-tip reorgs because the binary stays Pending and re-sends continue — if reorged out, the next
    * true confirmation just doesn't arrive and the soft observation eventually decays via `markAsConfirmed`/`pruneFinalizedBelow` clearing
    * the entry. #123.
    */
  def softObserve(hashes: Set[Hash]): F[Unit]
  def getPendingToRetry(cap: Int): F[List[PendingBinary]]
  def getState: F[TrackerState]
  def updateState(f: TrackerState => TrackerState): F[Unit]
  def clear: F[Unit]
  def pruneConfirmed: F[Unit]

  /** Prune confirmed binaries whose exact containing GL0 hash has reached the current operational phase.
    *
    * Fixes the reorg-loses-binaries bug: with the original [[pruneConfirmed]], a binary is dropped from the tracker as soon as it is seen
    * in any GL0 snapshot. If that GL0 snapshot is later orphaned in a Nakamoto reorg, the binary is gone and the metagraph builds the next
    * snapshot referencing a `lastSnapshotHash` that no longer exists in GL0's canonical chain — a permanent gap.
    *
    * The current method accepts only an ordinal watermark, so it cannot distinguish same-ordinal hash replacement. Target retention must
    * bind the exact containing Phase-2 hash and re-promote/requeue on a density replacement. Binaries in P0/P1 snapshots stay retained.
    *
    * Passing a locally observed snapshot ordinal when the Phase-2 reference is unavailable is unsafe and must defer rather than prune.
    */
  def pruneFinalizedBelow(lastFinalizedGlobalOrdinal: SnapshotOrdinal): F[Unit]
}

object BinaryTracker {
  def make[F[_]: Async]: F[BinaryTracker[F]] =
    Ref.of[F, TrackerState](TrackerState.empty).map { stateRef =>
      new BinaryTracker[F] {
        def enqueue(binary: Hashed[StateChannelSnapshotBinary], currencySnapshotOrdinal: SnapshotOrdinal, enqueuedAt: SnapshotOrdinal)
          : F[Unit] =
          stateRef.update { state =>
            state.copy(tracked = state.tracked :+ PendingBinary(binary, currencySnapshotOrdinal, enqueuedAt, NonNegLong.MinValue))
          }

        def markAsSent(binaryHash: Hash): F[Unit] =
          stateRef.update { state =>
            val updatedTracked = state.tracked.map {
              case PendingBinary(binary, currencySnapshotOrdinal, enqueuedAt, sendsSoFar) if binary.hash === binaryHash =>
                PendingBinary(binary, currencySnapshotOrdinal, enqueuedAt, NonNegLong.unsafeFrom(sendsSoFar.value + 1))
              case other => other
            }
            state.copy(tracked = updatedTracked)
          }

        def markAsConfirmed(
          confirmedHashes: Set[Hash],
          proof: GlobalSnapshotConfirmationProof,
          gl0KnownCurrencyOrd: Option[SnapshotOrdinal]
        ): F[Unit] =
          stateRef.update { state =>
            val indexedTracked = state.tracked.zipWithIndex

            val maybeHighestConfirmationIndex = indexedTracked.collect {
              case (PendingBinary(binaryData, _, _, _), index) if confirmedHashes.contains(binaryData.hash) => index
            }.maxOption

            val promoted = indexedTracked.map {
              case (pendingBinary @ PendingBinary(_, _, _, _), index) if index <= maybeHighestConfirmationIndex.getOrElse(-1) =>
                ConfirmedBinary(pendingBinary, proof)
              case (other, _) => other
            }

            // Transitional high-water mark from the current GL0 Phase-2 GSI view. It never
            // decreases here, so it cannot represent an exact-hash density replacement.
            val newWatermark = gl0KnownCurrencyOrd.fold(state.highestConfirmedCurrencyOrd) { gl0Ord =>
              if (gl0Ord.value.value > state.highestConfirmedCurrencyOrd.value.value) gl0Ord
              else state.highestConfirmedCurrencyOrd
            }

            // Current GC deletes Pending binaries strictly below the ordinal watermark.
            // This bounds the queue but is not reorg-safe: target retention must keep or
            // recover the exact branch inputs until replacement/rebase/ack obligations end.
            val gcd = promoted.filterNot {
              case p: PendingBinary => p.currencySnapshotOrdinal.value.value < newWatermark.value.value
              case _                => false
            }

            // Clear soft observations for hashes now confirmed or GC'd.
            val remainingHashes = gcd.collect { case p: PendingBinary => p.binary.hash }.toSet
            val remainingSoftObs = state.softObservedHashes.intersect(remainingHashes)

            state.copy(
              tracked = gcd,
              softObservedHashes = remainingSoftObs,
              highestConfirmedCurrencyOrd = newWatermark
            )
          }

        def softObserve(hashes: Set[Hash]): F[Unit] =
          stateRef.update { state =>
            // Only retain observations for hashes that are still Pending in tracked.
            // Confirmed binaries don't need this signal, and untracked hashes have nothing
            // to amplify in retry-mode logic.
            val pendingHashes = state.tracked.collect { case p: PendingBinary => p.binary.hash }.toSet
            val novel = hashes.intersect(pendingHashes) -- state.softObservedHashes
            if (novel.isEmpty) state
            else state.copy(softObservedHashes = state.softObservedHashes ++ novel)
          }

        def getPendingToRetry(cap: Int): F[List[PendingBinary]] =
          stateRef.get.map { state =>
            state.tracked.collect { case p: PendingBinary => p }.take(cap).toList
          }

        def getState: F[TrackerState] = stateRef.get

        def updateState(f: TrackerState => TrackerState): F[Unit] = stateRef.update(f)

        def clear: F[Unit] = stateRef.set(TrackerState.empty)

        def pruneConfirmed: F[Unit] =
          stateRef.update { state =>
            val updatedTracked = state.tracked.filterNot(_.isInstanceOf[ConfirmedBinary])
            state.copy(tracked = updatedTracked)
          }

        def pruneFinalizedBelow(lastFinalizedGlobalOrdinal: SnapshotOrdinal): F[Unit] =
          stateRef.update { state =>
            val updatedTracked = state.tracked.filterNot {
              case ConfirmedBinary(_, proof) =>
                proof.globalOrdinal.value.value <= lastFinalizedGlobalOrdinal.value.value
              case _ => false
            }
            // After pruning, drop soft observations for hashes that are no longer tracked
            // (they were pruned, so we're done with them).
            val remainingHashes = updatedTracked.collect { case p: PendingBinary => p.binary.hash }.toSet
            state.copy(tracked = updatedTracked, softObservedHashes = state.softObservedHashes.intersect(remainingHashes))
          }
      }
    }
}
