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
  def syncFull[V: ImmutableCodec](newState: Map[K, V], ordinal: SnapshotOrdinal): F[Unit]
  def syncFullIfNeeded[V: ImmutableCodec](newState: => F[Map[K, V]], ordinal: SnapshotOrdinal): F[Unit]
  def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit]
  def underlying: StatefulMerklePatriciaProducer[F]

  /** Snapshot of every `(Hex key → raw bytes)` entry currently in the producer. Use for independent cross-checks — feed into a second
    * producer implementation (e.g. `MerklePatriciaTrie.makeParallelFromBytes`) to build an MPT independently of the incremental writer and
    * compare roots. Byte-stream is the MPT canonical form; no decoding required.
    */
  def allEntriesAsBytes: F[Map[Hex, Array[Byte]]]
  def deleteAbove(ordinal: SnapshotOrdinal): F[Unit]

  /** Capture a snapshot of all internal state (producer state + last synced ordinal). The returned savepoint can restore the store to this
    * exact state, undoing any mutations that occurred after the savepoint was created.
    */
  def savepoint: F[MptStoreSavepoint[F]]

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
                logger.error(err)(s"[MptStore] Background persist failed for ordinal=$ordinal") >>
                  p.applyCutoff(ordinal).handleErrorWith { cutoffErr =>
                    logger.error(cutoffErr)(s"[MptStore] Cutoff after failed persist also failed for ordinal=$ordinal")
                  }
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
        matched.toList.traverseFilter {
          case (hex, bytes) if bytes != null && bytes.nonEmpty =>
            deserializeBytes[V](bytes).map(_.map(hex -> _))
          case (hex, _) =>
            logger.warn(s"MptStore.getAllForPrefix: Found null/empty bytes for hex=$hex") >> none[(Hex, V)].pure[F]
        }.map(_.toMap)
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
      mutex.permit.use { _ =>
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
      }

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

    override def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] =
      for {
        _ <- remove(toRemove.toList)
        _ <- insert(toUpsert)
      } yield ()

    override def underlying: StatefulMerklePatriciaProducer[F] = producer

    override def allEntriesAsBytes: F[Map[Hex, Array[Byte]]] = producer.entries

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

    override def withExclusiveLock[A](fa: F[A]): F[A] =
      mutex.permit.use(_ => fa)
  }
}
