package io.constellationnetwork.schema.mpt

import cats.Parallel
import cats.effect.std.Semaphore
import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.producer._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.implicits._

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Captured snapshot of an MptStore's internal state. Call `restore` to roll back the store to the state at the time this savepoint was
  * created. Used to undo mutations from failed artifact validation (e.g. stateProof divergence).
  */
trait MptStoreSavepoint[F[_]] {
  def restore: F[Unit]
}

/** Outcome of a transaction body — must be returned explicitly so the caller cannot accidentally leave a partial mutation in the store. The
  * bracket pattern (`MptStore.withTransaction`) restores the savepoint on `Rollback` and on body failure; `Commit` keeps mutations.
  */
sealed trait MptTxAction
object MptTxAction {
  case object Commit extends MptTxAction
  case object Rollback extends MptTxAction
}

/** Content-addressable key-value store with MPT commitment.
  *
  * Values are encoded via the canonical `ImmutableCodec[V]` typeclass (scodec-backed, byte-exact). A value type must have an
  * `ImmutableCodec` instance to be stored — this replaces the earlier circe-based encoding and locks in consensus-stable byte layouts at
  * the storage layer.
  *
  * Keys `K` are projected to `Hex` via the constructor-supplied `toHex` function (typically a hash of the canonical key encoding — for
  * `GlobalStateKey` this is `GlobalStateKey.toHex`).
  */
trait MptStore[F[_], K] {
  def get[V: ImmutableCodec](key: K): F[Option[V]]
  def getStrict[V: ImmutableCodec](key: K): F[StrictMptRead[V]]
  def getMany[V: ImmutableCodec](keys: List[K]): F[Map[K, V]]

  /** Decoded view of `producer.entriesWithPrefix(prefix)`. Returns `Map[Hex, V]` rather than `Map[K, V]` because the key encoding (`toHex`)
    * is one-way for hashed-component namespaces — consumers that need `Map[Address, V]` either decode the value's `source` field
    * (AllowSpend/TokenLock/etc.) or pair this with a sidecar address index.
    */
  def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]]
  def insert[V: ImmutableCodec](key: K, value: V): F[Unit]
  def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit]
  def remove(key: K): F[Unit]
  def remove(keys: List[K]): F[Unit]
  def contains(key: K): F[Boolean]
  def isEmpty: F[Boolean]
  def clear: F[Unit]
  def build(ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]]
  def sync[V: ImmutableCodec](newState: Map[K, V], ordinal: SnapshotOrdinal): F[Unit]

  /** Finalize an incremental sync at `ordinal` without inserting new entries. Use after a sequence of typed `insert[V]` calls to trigger
    * build + persist + last-synced-ordinal bookkeeping (same tail work as `sync`, but for callers that did the insert phase themselves with
    * per-field codecs).
    */
  def commit(ordinal: SnapshotOrdinal): F[Unit]

  /** Last ordinal at which the trie was synced / committed / built. `None` for a freshly-constructed store with no writes yet. Read-only
    * accessor used by the boot-time base-consistency check (#56.10 Phase H) — if the persisted base is ahead of `chainStore`'s finalized
    * head (e.g. crash mid-fold-forward), the operator can prune via `deleteAbove(finalizedOrdinal)` or rebuild from the finalized snapshot.
    */
  def lastPersistedOrdinal: F[Option[SnapshotOrdinal]]
  def syncFull[V: ImmutableCodec](newState: Map[K, V], ordinal: SnapshotOrdinal): F[Unit]
  def syncFullIfNeeded[V: ImmutableCodec](newState: => F[Map[K, V]], ordinal: SnapshotOrdinal): F[Unit]
  def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit]
  def underlying: StatefulMerklePatriciaProducer[F]

  /** Snapshot of every `(Hex key → raw bytes)` entry currently in the producer. Use for independent cross-checks — feed into a second
    * producer implementation (e.g. `MerklePatriciaTrie.makeParallelFromBytes`) to build an MPT independently of the incremental writer and
    * compare roots. Byte-stream is the MPT canonical form; no decoding required.
    */
  def allEntriesAsBytes: F[Map[Hex, Array[Byte]]]

  /** Load a pre-signed hex-keyed byte map VERBATIM at `ordinal` — the 3c-A "MPT is the state" primitive
    * (`docs/serde/FINISH-3C-EXECUTION-PLAN.md` §3c-A). Unlike [[sync]]/[[syncFull]], which re-encode a typed `Map[K,V]` through the
    * per-field `ImmutableCodec`, this stores the exact bytes that were signed (no re-encode). So a follower that loads gl0's served
    * `stateProof`-bytes and recomputes `GlobalSnapshotInfo.sidecarFreeMptRoot(entries)` obtains the producer's signed `mptRoot` BY
    * CONSTRUCTION — eliminating the `recomputed ≠ signed` drift the `syncFromGlobalSnapshotInfo` re-encode path exhibits. Mirrors
    * [[syncFull]]'s clear→insert→persist→build→bookkeep tail, minus the codec round-trip.
    */
  def loadBytes(entries: Map[Hex, Array[Byte]], ordinal: SnapshotOrdinal): F[Unit]

  /** Byte-faithful reload of the node's OWN persisted MPT state at `ordinal` — the boot/download counterpart of [[loadBytes]] (FINDING-S01
    * completion). The persisted byte map is exactly what the producer held when it persisted at that ordinal — including the MPT-native
    * consensus partitions (`ConsumedAllowSpends` 33 / `Slashings` 34) that a from-GSI rebuild cannot reconstruct — so a successful load
    * reproduces the signed `stateProof.mptRoot` BY CONSTRUCTION on an uncorrupted store. Returns `false` (store untouched) when the
    * producer has no persistence backend or nothing is persisted at `ordinal`; callers that hold the signed root should verify via
    * `syncFromPersistedMptVerified` (GlobalStateConverter syntax), which restores the pre-load state on a root mismatch.
    */
  def loadPersisted(ordinal: SnapshotOrdinal): F[Boolean]
  def deleteAbove(ordinal: SnapshotOrdinal): F[Unit]

  /** Capture a snapshot of all internal state (producer state + last synced ordinal). The returned savepoint can restore the store to this
    * exact state, undoing any mutations that occurred after the savepoint was created.
    */
  def savepoint: F[MptStoreSavepoint[F]]

  /** Run `body` as a transaction. Mutations made via this MptStore between entry and the body's completion are tracked by an automatic
    * savepoint. The body MUST yield `(A, MptTxAction)`:
    *   - `MptTxAction.Commit` keeps the mutations
    *   - `MptTxAction.Rollback` restores the savepoint If the body raises an error, mutations are rolled back and the error is re-raised.
    *
    * The whole bracket runs under `withExclusiveLock` so concurrent transactions serialize.
    *
    * Use this instead of bare `savepoint`/`restore` for any compound operation that may keep OR discard its mutations depending on a
    * post-mutation outcome (e.g. validating an incoming artifact whose acceptance depends on whether it wins ChainSelection).
    */
  def withTransaction[A](body: F[(A, MptTxAction)]): F[A]

  /** Run an effect while holding the MPT mutation lock. Use this to serialize compound operations (e.g. remove + insert + sync) that must
    * not interleave with syncFull.
    */
  def withExclusiveLock[A](fa: F[A]): F[A]
}

object MptStore {

  def make[F[_]: Async: Parallel: Hasher, K](
    producer: StatefulMerklePatriciaProducer[F],
    toHex: K => F[Hex]
  ): F[MptStore[F, K]] =
    (Ref.of[F, Option[SnapshotOrdinal]](None), Semaphore[F](1)).mapN { (lastSyncedOrdinalRef, mutex) =>
      new Impl[F, K](producer, toHex, lastSyncedOrdinalRef, mutex): MptStore[F, K]
    }

  private final class Impl[F[_]: Async: Parallel: Hasher, K](
    producer: StatefulMerklePatriciaProducer[F],
    toHex: K => F[Hex],
    lastSyncedOrdinalRef: Ref[F, Option[SnapshotOrdinal]],
    mutex: Semaphore[F]
  ) extends MptStore[F, K] {

    private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)
    private val BatchSize = 5000

    private def persistAsync(ordinal: SnapshotOrdinal): F[Unit] =
      producer match {
        case p: StatefulWithPersistenceMerklePatriciaProducer[F] =>
          Async[F]
            .start(
              p.persist(ordinal).handleErrorWith { err =>
                logger.error(err)(s"[MptStore] Background persist or retention cutoff failed for ordinal=$ordinal")
              }
            )
            .void
        case _ =>
          Async[F].unit
      }

    private def encode[V: ImmutableCodec](v: V): Array[Byte] =
      ImmutableCodec[V].immutableBytes(v).toArray

    private def toHexEntries[V: ImmutableCodec](data: Map[K, V]): F[Map[Hex, Array[Byte]]] =
      if (data.isEmpty) Map.empty[Hex, Array[Byte]].pure[F]
      else if (data.size <= BatchSize) {
        data.toList.parTraverse {
          case (k, v) => toHex(k).map(_ -> encode(v))
        }.map(_.toMap)
      } else {
        val batches = data.toList.grouped(BatchSize).toList
        batches.parTraverse { batch =>
          batch.parTraverse {
            case (k, v) => toHex(k).map(_ -> encode(v))
          }
        }.map(_.flatten.toMap)
      }

    private def deserializeBytes[V: ImmutableCodec](bytes: Array[Byte]): F[Option[V]] =
      if (bytes == null || bytes.isEmpty)
        logger.warn("MptStore.deserializeBytes: null or empty input") >> none[V].pure[F]
      else
        scodec.bits.ByteVector.view(bytes).fromImmutableBytes[V] match {
          case Right(v) => v.some.pure[F]
          case Left(err) =>
            logger.warn(s"MptStore.deserializeBytes: scodec decode failed: $err") >> none[V].pure[F]
        }

    private def deserializePrefixBytes[V: ImmutableCodec](hex: Hex, bytes: Array[Byte]): F[V] =
      if (bytes == null || bytes.isEmpty)
        Async[F].raiseError(
          new IllegalStateException(s"MptStore.getAllForPrefix: null/empty bytes at hex=${hex.value}")
        )
      else
        scodec.bits.ByteVector.view(bytes).fromImmutableBytes[V] match {
          case Right(v) => v.pure[F]
          case Left(err) =>
            Async[F].raiseError(
              new IllegalStateException(s"MptStore.getAllForPrefix: undecodable bytes at hex=${hex.value}: $err")
            )
        }

    override def get[V: ImmutableCodec](key: K): F[Option[V]] =
      for {
        hex <- toHex(key)
        entries <- producer.entries
        bytesOpt = entries.get(hex)
        result <- bytesOpt match {
          case Some(bytes) if bytes != null && bytes.nonEmpty =>
            deserializeBytes[V](bytes)
          case Some(_) =>
            logger.warn(s"MptStore.get: Found null/empty bytes for hex=$hex") >> none[V].pure[F]
          case None =>
            none[V].pure[F]
        }
      } yield result

    override def getStrict[V: ImmutableCodec](key: K): F[StrictMptRead[V]] =
      for {
        hex <- toHex(key)
        entries <- producer.entries
      } yield entries.get(hex).fold[StrictMptRead[V]](StrictMptRead.Absent)(StrictMptRead.fromStoredBytes[V])

    override def getMany[V: ImmutableCodec](keys: List[K]): F[Map[K, V]] =
      if (keys.isEmpty) Map.empty[K, V].pure[F]
      else
        for {
          hexKeys <- keys.parTraverse(k => toHex(k).map(k -> _))
          entries <- producer.entries
          results <- hexKeys.traverseFilter {
            case (k, hex) =>
              entries.get(hex) match {
                case Some(bytes) if bytes != null && bytes.nonEmpty =>
                  deserializeBytes[V](bytes).map(_.map(k -> _))
                case Some(_) =>
                  logger.warn(s"MptStore.getMany: Found null/empty bytes for hex=$hex") >> none[(K, V)].pure[F]
                case None =>
                  none[(K, V)].pure[F]
              }
          }
        } yield results.toMap

    override def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] =
      producer.entriesWithPrefix(prefix).flatMap { matched =>
        matched.toList
          .sortBy(_._1.value)
          .traverse {
            case (hex, bytes) => deserializePrefixBytes[V](hex, bytes).map(hex -> _)
          }
          .map(_.toMap)
      }

    override def insert[V: ImmutableCodec](key: K, value: V): F[Unit] =
      for {
        hex <- toHex(key)
        _ <- producer.insertBytes(Map(hex -> encode(value))).void
      } yield ()

    override def insert[V: ImmutableCodec](data: Map[K, V]): F[Unit] =
      if (data.isEmpty) Async[F].unit
      else
        for {
          entries <- toHexEntries(data)
          _ <- producer.insertBytes(entries).void
        } yield ()

    override def remove(key: K): F[Unit] =
      for {
        hex <- toHex(key)
        _ <- producer.remove(List(hex)).void
      } yield ()

    override def remove(keys: List[K]): F[Unit] =
      if (keys.isEmpty) Async[F].unit
      else
        for {
          hexKeys <- keys.parTraverse(toHex)
          _ <- producer.remove(hexKeys).void
        } yield ()

    override def contains(key: K): F[Boolean] =
      for {
        hex <- toHex(key)
        entries <- producer.entries
      } yield entries.contains(hex)

    override def isEmpty: F[Boolean] =
      producer.entries.map(_.isEmpty)

    override def clear: F[Unit] =
      logger.info("[MptStore] Clearing store") >> producer.clear

    override def build(snapshotOrdinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
      producer.buildForOrdinal(snapshotOrdinal)

    override def syncFull[V: ImmutableCodec](newState: Map[K, V], ordinal: SnapshotOrdinal): F[Unit] =
      // Caller-serialized — see `withTransaction` comment. Bootstrap / download paths are
      // single-fiber; in-flight production paths serialize via `snapshotSemaphore`.
      if (newState.isEmpty) {
        logger.info("[MptStore] Empty sync, skipping") >>
          clear >> lastSyncedOrdinalRef.set(Some(ordinal))
      } else
        for {
          currentEntries <- producer.entries
          currentSize = currentEntries.size
          _ <-
            logger
              .warn(
                s"[MptStore] syncFull REDUCING entry count: $currentSize → ${newState.size} at ordinal=$ordinal. " +
                  s"State loss possible — check upstream context."
              )
              .whenA(currentSize > 0 && newState.size < currentSize)
          _ <- logger.info(s"[MptStore] Full sync with ${newState.size} entries (was $currentSize)")
          _ <- clear
          newEntries <- toHexEntries(newState)
          _ <- producer.insertBytes(newEntries).void
          _ <- persistAsync(ordinal)
          _ <- build(ordinal)
          _ <- lastSyncedOrdinalRef.set(Some(ordinal))
        } yield ()

    override def loadBytes(entries: Map[Hex, Array[Byte]], ordinal: SnapshotOrdinal): F[Unit] =
      // 3c-A: store the SIGNED byte map verbatim — same clear→insert→persist→build→bookkeep tail as `syncFull`, but the input is the
      // already-encoded `(Hex → bytes)` map (NO `toHexEntries` codec round-trip). `producer.insertBytes(...).void` matches `syncFull`;
      // a partial/corrupt load is caught downstream by the follower's `sidecarFreeMptRoot(entries) === signed mptRoot` verify gate.
      if (entries.isEmpty)
        logger.info(s"[MptStore] loadBytes empty at ordinal=$ordinal, clearing") >>
          clear >> lastSyncedOrdinalRef.set(Some(ordinal))
      else
        for {
          _ <- logger.info(s"[MptStore] loadBytes ${entries.size} signed entries VERBATIM at ordinal=$ordinal (no re-encode)")
          _ <- clear
          _ <- producer.insertBytes(entries).void
          _ <- persistAsync(ordinal)
          _ <- build(ordinal).void
          _ <- lastSyncedOrdinalRef.set(Some(ordinal))
        } yield ()

    override def syncFullIfNeeded[V: ImmutableCodec](newState: => F[Map[K, V]], ordinal: SnapshotOrdinal): F[Unit] =
      lastSyncedOrdinalRef.modify { lastOrdinal =>
        val needsSync = lastOrdinal.forall(_ =!= ordinal)
        if (needsSync) (Some(ordinal), true)
        else (lastOrdinal, false)
      }.flatMap { needsSync =>
        if (needsSync) newState.flatMap(syncFull(_, ordinal))
        else logger.debug(s"[MptStore] Skipping sync, already synced at ordinal $ordinal")
      }

    override def sync[V: ImmutableCodec](updates: Map[K, V], ordinal: SnapshotOrdinal): F[Unit] =
      if (updates.isEmpty) Async[F].unit
      else
        for {
          _ <- logger.debug(s"[MptStore] Incremental sync with ${updates.size} entries at ordinal=$ordinal")
          _ <- insert(updates)
          _ <- persistAsync(ordinal)
          _ <- build(ordinal).void
          _ <- lastSyncedOrdinalRef.set(Some(ordinal))
        } yield ()

    override def commit(ordinal: SnapshotOrdinal): F[Unit] =
      for {
        _ <- logger.debug(s"[MptStore] Commit at ordinal=$ordinal")
        _ <- persistAsync(ordinal)
        _ <- build(ordinal).void
        _ <- lastSyncedOrdinalRef.set(Some(ordinal))
      } yield ()

    override def lastPersistedOrdinal: F[Option[SnapshotOrdinal]] =
      lastSyncedOrdinalRef.get

    override def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] =
      for {
        _ <- remove(toRemove.toList)
        _ <- insert(toUpsert)
      } yield ()

    override def underlying: StatefulMerklePatriciaProducer[F] = producer

    override def allEntriesAsBytes: F[Map[Hex, Array[Byte]]] = producer.entries

    override def loadPersisted(ordinal: SnapshotOrdinal): F[Boolean] =
      producer match {
        case p: StatefulWithPersistenceMerklePatriciaProducer[F] =>
          p.load(ordinal).flatTap { loaded =>
            // Same build + last-synced bookkeeping tail as `loadBytes`, minus `persistAsync` (the bytes just came FROM disk).
            (logger.info(s"[MptStore] loadPersisted: restored persisted state at ordinal=$ordinal VERBATIM (no re-encode)") >>
              build(ordinal).void >>
              lastSyncedOrdinalRef.set(Some(ordinal))).whenA(loaded)
          }
        case _ =>
          false.pure[F]
      }

    override def deleteAbove(ordinal: SnapshotOrdinal): F[Unit] =
      producer match {
        case p: StatefulWithPersistenceMerklePatriciaProducer[F] =>
          logger.info(s"[MptStore] Deleting above ordinal=$ordinal") >> p.deleteAbove(ordinal)
        case _ =>
          Async[F].unit
      }

    override def savepoint: F[MptStoreSavepoint[F]] =
      for {
        producerSP <- producer.savepoint
        savedOrdinal <- lastSyncedOrdinalRef.get
      } yield
        new MptStoreSavepoint[F] {
          def restore: F[Unit] =
            producerSP.restore >> lastSyncedOrdinalRef.set(savedOrdinal)
        }

    override def withTransaction[A](body: F[(A, MptTxAction)]): F[A] =
      // Caller-serialized: the body may call other MPT methods that internally take their own
      // serialization (syncFrom*, syncFull). If we wrapped the bracket in `withExclusiveLock` the
      // nested re-acquisition would deadlock the same non-reentrant semaphore. Production callers
      // (onSlotWon, handleSnapshot) already serialize through `snapshotSemaphore`; HTTP-side
      // callers (HistoricalMptProofService) wrap the call site with `withExclusiveLock` themselves.
      savepoint.flatMap { sp =>
        body.attempt.flatMap {
          case Right((a, MptTxAction.Commit))   => a.pure[F]
          case Right((a, MptTxAction.Rollback)) => sp.restore.as(a)
          case Left(err)                        => sp.restore >> err.raiseError[F, A]
        }
      }

    override def withExclusiveLock[A](fa: F[A]): F[A] =
      mutex.permit.use(_ => fa)
  }
}
