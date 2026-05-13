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
  // High-water mark sourced from gl0's authoritative `GlobalSnapshotInfo
  // .lastCurrencySnapshots[ourIdentifier].ordinal` — the ord of the last currency
  // snapshot gl0 has accepted for our metagraph. Monotonic (never decreases). Used
  // by `markAsConfirmed` to GC superseded Pendings.
  //
  // Why this is safe under BFT metagraph consensus: ml0 nodes don't race siblings,
  // they vote on a single binary at each currencyOrd. The only way Pendings below
  // gl0's known ord can exist is a brief metagraph fork (rare; resolved by ml0
  // re-syncing to gl0's pick). Those forked-off Pendings are definitionally past:
  // gl0 has accepted a later snapshot, so older versions on the abandoned branch
  // can never land. Dropping them prevents queue growth under chain drift
  // (#125, iter25 data-with-fee root cause).
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

            // High-water mark: gl0's authoritative `lastCurrencySnapshots[ourIdentifier]
            // .ordinal` from GlobalSnapshotInfo. The ord of the last currency snapshot
            // gl0 has accepted for our metagraph. Monotonic — never decreases. Falls
            // back to the existing watermark when GSI doesn't yet have an entry for
            // our identifier (e.g. very-early bootstrap before our first binary lands).
            val newWatermark = gl0KnownCurrencyOrd.fold(state.highestConfirmedCurrencyOrd) { gl0Ord =>
              if (gl0Ord.value.value > state.highestConfirmedCurrencyOrd.value.value) gl0Ord
              else state.highestConfirmedCurrencyOrd
            }

            // GC superseded Pendings: any PendingBinary whose currencySnapshotOrdinal
            // is STRICTLY LESS than gl0's known current ord is past — gl0 has accepted
            // a later snapshot for us, so no version of our binary at this ord can land.
            //
            // Strict `<` (not `<=`) preserves binaries at the exact same ord as gl0's
            // current. Under BFT metagraph consensus there's typically only one binary
            // per ord, and if it's ours we just confirmed it (now ConfirmedBinary, not
            // PendingBinary). But during a transient metagraph fork our Pending at the
            // gl0-known ord might be on the abandoned branch — those drop on the next
            // confirmation tick once gl0 advances to ord+1.
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
