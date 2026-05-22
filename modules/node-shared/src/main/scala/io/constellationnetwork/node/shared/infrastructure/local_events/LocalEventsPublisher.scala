package io.constellationnetwork.node.shared.infrastructure.local_events

import cats.Applicative
import cats.syntax.applicative._

import io.constellationnetwork.node.shared.infrastructure.local_events.proto.local_events._
import io.constellationnetwork.schema.SnapshotOrdinal

/** Producer-side seam for the [[LocalEventsService]] gRPC stream. GSAM and the gl0 finality monitor call methods on this trait; the
  * production wiring routes them into an FS2 `Topic[F, EventEnvelope]` that the service fans out to subscribers. Test / currency-l0 sites
  * pass `LocalEventsPublisher.noop` so the GSAM constructor doesn't require the full server machinery.
  *
  *   - All methods are `F[Unit]` — emission never blocks consensus. Slow-subscriber pruning is the gRPC server's responsibility, not the
  *     publisher's.
  *   - All methods are idempotent at the wire level: re-emitting an event after a crash recovery is safe (subscribers see a duplicate
  *     `seq` from the previous session, fixed by the per-session reset).
  *   - Group methods (e.g. `publishBalanceChanges`) take a `List` so callers can batch the per-ordinal diff into a single allocation;
  *     the publisher writes one envelope per element to the topic.
  */
trait LocalEventsPublisher[F[_]] {

  /** Publish a finality event. Called by `SnapshotLeaderLoop`'s `finalityMonitor` right after `chainStore.finalize` succeeds and the
    * high-water-mark Ref is advanced. The envelope's `ordinal` field is the finalized ordinal.
    */
  def publishSnapshotFinalized(ordinal: SnapshotOrdinal, event: SnapshotFinalized): F[Unit]

  /** Publish a batch of balance diffs derived inside GSAM's `accept()`. The `cause` field is best-effort; callers can pass a free-form
    * tag (`"block"`, `"reward"`, `"tokenlock"`, etc.) — receivers ignore values they don't recognize.
    */
  def publishBalanceChanges(ordinal: SnapshotOrdinal, changes: List[BalanceChange]): F[Unit]

  /** Publish a batch of token-lock state transitions (`CREATED` / `EXPIRED` / `WITHDRAWN`). The expired set comes from the GSAM-hoisted
    * single walk (per the design's Q2 user override).
    */
  def publishTokenLockChanges(ordinal: SnapshotOrdinal, changes: List[TokenLockStateChange]): F[Unit]

  /** Publish a batch of allow-spend state transitions (`CREATED` / `CONSUMED` / `EXPIRED`). Same hoisting story as token locks.
    */
  def publishAllowSpendChanges(ordinal: SnapshotOrdinal, changes: List[AllowSpendStateChange]): F[Unit]

  /** Publish a batch of accepted transactions. Source is the GSAM `blockResult.accepted` flat-map.
    */
  def publishTransactionsAccepted(ordinal: SnapshotOrdinal, txs: List[TransactionAccepted]): F[Unit]

  /** Publish metagraph-snapshot acceptance events (one per `(metagraph, snapshot)` rolled into this gl0 ordinal) and any DAG-side balance
    * changes derived from the same processor output. Two lists in one call so the publisher emits both as a single GSAM-side
    * write-through.
    */
  def publishMetagraphEvents(
    ordinal: SnapshotOrdinal,
    snapshots: List[MetagraphSnapshotAccepted],
    balances: List[MetagraphBalanceChange]
  ): F[Unit]
}

object LocalEventsPublisher {

  /** No-op publisher. Wired into currency-l0 and any GSAM-construction site that doesn't run the LocalEvents server (cl0, tests).
    * Doesn't allocate per call — `().pure[F]` is the only effect.
    */
  def noop[F[_]: Applicative]: LocalEventsPublisher[F] =
    new LocalEventsPublisher[F] {
      def publishSnapshotFinalized(ordinal: SnapshotOrdinal, event: SnapshotFinalized): F[Unit] = ().pure[F]
      def publishBalanceChanges(ordinal: SnapshotOrdinal, changes: List[BalanceChange]): F[Unit] = ().pure[F]
      def publishTokenLockChanges(ordinal: SnapshotOrdinal, changes: List[TokenLockStateChange]): F[Unit] = ().pure[F]
      def publishAllowSpendChanges(ordinal: SnapshotOrdinal, changes: List[AllowSpendStateChange]): F[Unit] = ().pure[F]
      def publishTransactionsAccepted(ordinal: SnapshotOrdinal, txs: List[TransactionAccepted]): F[Unit] = ().pure[F]
      def publishMetagraphEvents(
        ordinal: SnapshotOrdinal,
        snapshots: List[MetagraphSnapshotAccepted],
        balances: List[MetagraphBalanceChange]
      ): F[Unit] = ().pure[F]
    }
}
