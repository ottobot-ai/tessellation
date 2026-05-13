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
  // High-water mark of currency-snapshot ordinals known to be confirmed at gl0
  // (across the entire run, monotonic — never decreases). Pending binaries whose
  // `currencySnapshotOrdinal <= highestConfirmedCurrencyOrd` are *definitionally*
  // superseded: gl0 has accepted a later currency snapshot, so the older one was
  // either a sibling that lost the race or a member of a local fork gl0 abandoned.
  // Used by `markAsConfirmed` to GC stale Pendings on every confirmation tick.
  // Without this, m0's queue accumulates indefinitely under chain-link drift,
  // slowing throughput (#125, observed in iter25).
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
  def markAsConfirmed(confirmedHashes: Set[Hash], proof: GlobalSnapshotConfirmationProof): F[Unit]

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

  /** Prune confirmed binaries whose containing GL0 snapshot has reached finality.
    *
    * Fixes the reorg-loses-binaries bug: with the original [[pruneConfirmed]], a binary is dropped from the tracker as soon as it is seen
    * in any GL0 snapshot. If that GL0 snapshot is later orphaned in a Nakamoto reorg, the binary is gone and the metagraph builds the next
    * snapshot referencing a `lastSnapshotHash` that no longer exists in GL0's canonical chain — a permanent gap.
    *
    * This variant only prunes confirmed binaries whose `proof.globalOrdinal <= lastFinalizedGlobalOrdinal`, where the finalized ordinal
    * must come from GL0's authoritative finality marker (depth-k or attestation-2/3, whichever fires first). Binaries confirmed in
    * still-unfinalized GL0 snapshots stay in the tracker; if their containing snapshot is orphaned they will be re-promoted to pending and
    * re-sent on the next worker tick.
    *
    * In BFT GL0 mode every snapshot is immediately final, so passing the snapshot's own ordinal here is equivalent to the legacy behavior —
    * the new method is strictly safer and a drop-in replacement.
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

        def markAsConfirmed(confirmedHashes: Set[Hash], proof: GlobalSnapshotConfirmationProof): F[Unit] =
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

            // Update high-water mark using the highest currencyOrd among entries we
            // just confirmed (or had already confirmed earlier). Monotonic — only
            // grows. We read from `promoted` because the indexed-prefix promotion
            // above already converted all matched Pendings to ConfirmedBinary.
            val newWatermark = promoted.collect {
              case ConfirmedBinary(p, _) => p.currencySnapshotOrdinal
            }.maxOption
              .fold(state.highestConfirmedCurrencyOrd) { maxOrd =>
                if (maxOrd.value.value > state.highestConfirmedCurrencyOrd.value.value) maxOrd
                else state.highestConfirmedCurrencyOrd
              }

            // GC superseded Pendings: any PendingBinary whose currencySnapshotOrdinal
            // is STRICTLY LESS than the watermark is definitionally stale (gl0 has
            // accepted a later currency snapshot, so this one was either on a local
            // fork gl0 abandoned or simply too old to matter). Dropping them prevents
            // queue growth under chain-link drift (#125, iter25 data-with-fee root
            // cause). Safe because gl0's `lastStateChannelSnapshotHashes[addr]` has
            // already advanced past these — they would only ever yield `chain-link
            // rejection → returnedSCEvents` forever, never actually landing.
            //
            // We use strict `<` (not `<=`) so unmatched siblings at the *same* ord as
            // a confirmed binary survive: a sibling could still legitimately chain
            // forward if it was the canonical-pick at gl0 we just haven't observed yet.
            // The next confirmation tick advances watermark past their ord and they
            // get cleaned up naturally.
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
