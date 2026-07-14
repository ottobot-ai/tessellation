package io.constellationnetwork.security.mpt.producer

import java.security.MessageDigest

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.cutoff.OrdinalCutoff
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.security.mpt.storages.MptStateStorage

import fs2.Stream
import fs2.io.file.Path
import io.circe.syntax._
import io.circe.{Encoder, Json}
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Stateful MPT producer with filesystem persistence and incremental updates.
  *
  * Design:
  *   - Data is stored in stateRef (source of truth)
  *   - Trie is cached in trieRef and updated incrementally
  *   - Pending changes are tracked for incremental updates
  *   - Full rebuild only when no cached trie exists
  */
class FileSystemMerklePatriciaProducer[F[_]: Async: Parallel: Hasher: JsonSerializer](
  stateRef: Ref[F, Map[Hex, Array[Byte]]],
  trieRef: Ref[F, Option[MerklePatriciaTrie]],
  pendingInsertsRef: Ref[F, Map[Hex, Array[Byte]]],
  pendingRemovesRef: Ref[F, List[Hex]],
  storage: MptStateStorage[F],
  rootHashCacheRef: Ref[F, Map[SnapshotOrdinal, MptRoot]],
  lastBuiltOrdinalRef: Ref[F, Option[SnapshotOrdinal]]
) extends StatefulWithPersistenceMerklePatriciaProducer[F] {

  private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)
  private val parallelProducer: ParallelMerklePatriciaProducer[F] = ParallelMerklePatriciaProducer[F]
  private val BatchSize = 5000
  private val MaxCacheSize = 50

  private def copyEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    FileSystemMerklePatriciaProducer.copyEntries(entries)

  private def stageChanges(
    byteEntries: Map[Hex, Array[Byte]],
    removals: List[Hex]
  ): F[Either[MerklePatriciaError, Unit]] =
    Async[F].uncancelable { _ =>
      stateRef
        .modify[Either[MerklePatriciaError, Unit]] { current =>
          val retainedKeys = current.keySet -- removals
          (for {
            _ <- PhysicalTrieKeyValidator.validateEachKey(removals)
            _ <- PhysicalTrieKeyValidator.validateInsertion(retainedKeys, byteEntries.keys)
          } yield ()) match {
            case Left(error) => current -> Left(error)
            case Right(_) =>
              val candidate = byteEntries.foldLeft(current -- removals) {
                case (entries, (key, value)) => entries.updated(key, value)
              }
              candidate -> Right(())
          }
        }
        .flatMap {
          case Left(error) => (error: MerklePatriciaError).asLeft[Unit].pure[F]
          case Right(_) =>
            val effectiveRemovals = removals.filterNot(byteEntries.contains)
            (pendingRemovesRef.update(existing => (existing.filterNot(byteEntries.contains) ++ effectiveRemovals).distinct) >>
              pendingInsertsRef.update { existing =>
                byteEntries.foldLeft(existing -- effectiveRemovals) {
                  case (entries, (key, value)) => entries.updated(key, value)
                }
              }).as(().asRight[MerklePatriciaError])
        }
    }

  override def getProver: F[MerklePatriciaSingleInclusionProver[F]] =
    build.flatMap {
      case Right(trie) => parallelProducer.getProver(trie)
      case Left(err)   => Async[F].raiseError(err)
    }

  override def entries: F[Map[Hex, Array[Byte]]] =
    stateRef.get.map(copyEntries)

  override def physicalKeys: F[Set[Hex]] =
    stateRef.get.map(_.keySet)

  override def entry(key: Hex): F[Option[Array[Byte]]] =
    stateRef.get.map(_.get(key).map(FileSystemMerklePatriciaProducer.copyBytes))

  override def entriesForKeys(keys: Set[Hex]): F[Map[Hex, Array[Byte]]] =
    stateRef.get.map { entries =>
      keys.iterator.flatMap(key => entries.get(key).map(bytes => key -> FileSystemMerklePatriciaProducer.copyBytes(bytes))).toMap
    }

  override def entryCount: F[Int] = stateRef.get.map(_.size)

  override def entriesWithPrefix(prefix: Hex): F[Map[Hex, Array[Byte]]] =
    stateRef.get.map(entries =>
      copyEntries(entries.filter { case (key, _) => StatefulMerklePatriciaProducer.hasNibblePrefix(key, prefix) })
    )

  def entriesAsJson: F[Map[Hex, Json]] =
    stateRef.get.flatMap { state =>
      state.toList.traverse {
        case (k, bytes) =>
          JsonSerializer[F].deserialize[Json](bytes).flatMap {
            case Right(json) => (k -> json).pure[F]
            case Left(err)   => Async[F].raiseError[(Hex, Json)](err)
          }
      }.map(_.toMap)
    }

  def getAsJson(key: Hex): F[Option[Json]] =
    stateRef.get.flatMap { state =>
      state.get(key).traverse { bytes =>
        JsonSerializer[F].deserialize[Json](bytes).flatMap {
          case Right(json) => json.pure[F]
          case Left(err)   => Async[F].raiseError[Json](err)
        }
      }
    }

  override def build: F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    for {
      currentTrie <- trieRef.get
      pendingInserts <- pendingInsertsRef.get
      pendingRemoves <- pendingRemovesRef.get
      state <- stateRef.get

      result <- (currentTrie, pendingInserts.isEmpty, pendingRemoves.isEmpty) match {
        case (_, _, _) if state.isEmpty =>
          (OperationError("Cannot build trie with no entries"): MerklePatriciaError)
            .asLeft[MerklePatriciaTrie]
            .pure[F]

        case (Some(trie), true, true) =>
          logger.debug("[MPT] Returning cached trie (no pending changes)") >>
            trie.asRight[MerklePatriciaError].pure[F]

        case (Some(trie), _, _) =>
          applyIncrementalUpdates(trie, pendingInserts, pendingRemoves)

        case (None, _, _) =>
          fullBuild(state)
      }
    } yield result

  override def buildForOrdinal(ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    build.flatMap {
      case Right(trie) =>
        val rootHash = trie.rootHash
        (cacheRootHash(ordinal, rootHash) >> lastBuiltOrdinalRef.set(Some(ordinal)))
          .as(trie.asRight[MerklePatriciaError])
      case Left(err) =>
        err.asLeft[MerklePatriciaTrie].pure[F]
    }

  override def getRootHashForOrdinal(ordinal: SnapshotOrdinal): F[Option[MptRoot]] =
    rootHashCacheRef.get.map(_.get(ordinal))

  override def getCurrentRootHash: F[Option[MptRoot]] =
    trieRef.get.map(_.map(_.rootHash))

  override def getLastBuiltOrdinal: F[Option[SnapshotOrdinal]] =
    lastBuiltOrdinalRef.get

  private def cacheRootHash(ordinal: SnapshotOrdinal, rootHash: MptRoot): F[Unit] =
    rootHashCacheRef.update { cache =>
      val updated = cache + (ordinal -> rootHash)
      if (updated.size > MaxCacheSize) {
        // Keep only the latest 50 by ordinal
        updated.toList.sortBy(_._1).takeRight(MaxCacheSize).toMap
      } else {
        updated
      }
    }

  private def fullBuild(state: Map[Hex, Array[Byte]]): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    for {
      _ <- logger.info(s"[MPT] Full build from ${state.size} entries (no cached trie)")
      result <- parallelProducer.createFromBytes(state).attempt.flatMap {
        case Right(trie) =>
          for {
            _ <- trieRef.set(Some(trie))
            _ <- pendingInsertsRef.set(Map.empty)
            _ <- pendingRemovesRef.set(List.empty)
            _ <- logger.info(s"[MPT] Full build completed")
          } yield trie.asRight[MerklePatriciaError]
        case Left(e: MerklePatriciaError) =>
          e.asLeft[MerklePatriciaTrie].pure[F]
        case Left(e) =>
          (OperationError(e.getMessage): MerklePatriciaError).asLeft[MerklePatriciaTrie].pure[F]
      }
    } yield result

  private def applyIncrementalUpdates(
    currentTrie: MerklePatriciaTrie,
    inserts: Map[Hex, Array[Byte]],
    removes: List[Hex]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    for {
      _ <- logger.debug(s"[MPT] Applying incremental updates: ${inserts.size} inserts, ${removes.size} removes")

      // Sort removes by CompactNibblePath ordering for deterministic trie structure
      sortedRemoves = removes.sortBy(hex => CompactNibblePath.fromHexString(hex.value))

      afterRemoves <-
        if (removes.isEmpty) currentTrie.rootNode.pure[F]
        else IncrementalTrieOps.removeMultiple[F](currentTrie.rootNode, sortedRemoves)

      result <-
        if (inserts.isEmpty) {
          val newTrie = MerklePatriciaTrie(afterRemoves)
          for {
            _ <- trieRef.set(Some(newTrie))
            _ <- pendingInsertsRef.set(Map.empty)
            _ <- pendingRemovesRef.set(List.empty)
            _ <- logger.debug(s"[MPT] Incremental update completed")
          } yield newTrie.asRight[MerklePatriciaError]
        } else {
          for {
            // Batch hash computation for better parallelism on large inserts
            insertEntries <-
              if (inserts.size <= BatchSize) {
                inserts.toList.parTraverse {
                  case (hex, bytes) => Hasher[F].hashBytes(bytes).map(hash => (hex, hash))
                }
              } else {
                // Process in parallel batches and flatten
                val batches = inserts.toList.grouped(BatchSize).toList
                batches.parTraverse { batch =>
                  batch.parTraverse {
                    case (hex, bytes) => Hasher[F].hashBytes(bytes).map(hash => (hex, hash))
                  }
                }.map(_.flatten)
              }
            // Sort by CompactNibblePath ordering to match full-build insertion order (deterministic trie structure)
            sortedInsertEntries = insertEntries.sortBy { case (hex, _) => CompactNibblePath.fromHexString(hex.value) }
            finalRoot <- IncrementalTrieOps.insertMultiple[F](afterRemoves, sortedInsertEntries)
            newTrie = MerklePatriciaTrie(finalRoot)
            _ <- trieRef.set(Some(newTrie))
            _ <- pendingInsertsRef.set(Map.empty)
            _ <- pendingRemovesRef.set(List.empty)
            _ <- logger.debug(
              s"[MPT] Incremental update completed"
            )
          } yield newTrie.asRight[MerklePatriciaError]
        }
    } yield result

  override def insert[A: Encoder](data: Map[Hex, A]): F[Either[MerklePatriciaError, Unit]] =
    if (data.isEmpty) ().asRight[MerklePatriciaError].pure[F]
    else
      for {
        _ <- logger.debug(s"[MPT] Inserting ${data.size} entries")
        byteEntries <- data.toList.traverse {
          case (k, v) => JsonSerializer[F].serialize(v.asJson).map(k -> _)
        }.map(_.toMap)
        result <- stageChanges(byteEntries, List.empty)
      } yield result

  def insertBytes(data: Map[Hex, Array[Byte]]): F[Either[MerklePatriciaError, Unit]] =
    if (data.isEmpty) ().asRight[MerklePatriciaError].pure[F]
    else {
      val owned = copyEntries(data)
      logger.debug(s"[MPT] Inserting ${owned.size} byte entries") >> stageChanges(owned, List.empty)
    }

  override def replaceBytes(
    upserts: Map[Hex, Array[Byte]],
    removals: List[Hex]
  ): F[Either[MerklePatriciaError, Unit]] =
    if (upserts.isEmpty && removals.isEmpty) ().asRight[MerklePatriciaError].pure[F]
    else stageChanges(copyEntries(upserts), removals)

  override def update[A: Encoder](key: Hex, value: A): F[Either[MerklePatriciaError, Unit]] =
    PhysicalTrieKeyValidator.validateKey(key) match {
      case Left(error) => (error: MerklePatriciaError).asLeft[Unit].pure[F]
      case Right(_) =>
        stateRef.get.flatMap { state =>
          if (!state.contains(key))
            (OperationError(s"Key not found: $key"): MerklePatriciaError).asLeft[Unit].pure[F]
          else
            for {
              bytes <- JsonSerializer[F].serialize(value.asJson)
              _ <- stateRef.update(_ + (key -> bytes))
              _ <- pendingInsertsRef.update(_ + (key -> bytes))
            } yield ().asRight[MerklePatriciaError]
        }
    }

  override def remove(keys: List[Hex]): F[Either[MerklePatriciaError, Unit]] =
    logger.debug(s"[MPT] Removing ${keys.size} entries").whenA(keys.nonEmpty) >> replaceBytes(Map.empty, keys)

  override def clear: F[Unit] =
    logger.info("[MPT] Clearing state") >>
      stateRef.set(Map.empty) >>
      trieRef.set(None) >>
      pendingInsertsRef.set(Map.empty) >>
      pendingRemovesRef.set(List.empty) >>
      rootHashCacheRef.set(Map.empty) >>
      lastBuiltOrdinalRef.set(None)

  override def buildHexMap(data: Map[GlobalStateKey, Json]): F[Map[Hex, Array[Byte]]] =
    (if (data.size <= BatchSize)
       data.toList.parTraverse {
         case (key, value) =>
           for {
             hex <- GlobalStateKey.toHex[F](key)
             bytes <- JsonSerializer[F].serialize(value)
           } yield hex -> bytes
       }
     else {
       val batches = data.toList.grouped(BatchSize).toList
       batches.parTraverse { batch =>
         batch.parTraverse {
           case (key, value) =>
             for {
               hex <- GlobalStateKey.toHex[F](key)
               bytes <- JsonSerializer[F].serialize(value)
             } yield hex -> bytes
         }
       }.map(_.flatten)
     }).flatMap(PhysicalTrieKeyValidator.materializeEntries(_).liftTo[F])

  override def persist(ordinal: SnapshotOrdinal): F[Unit] =
    for {
      state <- stateRef.get
      // An empty image is still a real state generation. Skipping it can resurrect an older non-empty image on restart.
      // Never prune other recovery generations after this write reports failure. This ordering does not make the legacy direct
      // overwrite crash-safe; durable image publication is a separate migration.
      _ <- storage.writeState(ordinal, state) >> applyCutoff(ordinal)
    } yield ()

  override def load(ordinal: SnapshotOrdinal): F[Boolean] =
    for {
      _ <- logger.info(s"[MPT] Loading state for ordinal=$ordinal")

      loaded <- storage.readState(ordinal).flatMap {
        case Some(state) =>
          PhysicalTrieKeyValidator.validateKeys(state.keys) match {
            case Left(error) => error.raiseError[F, Boolean]
            case Right(_) =>
              for {
                _ <- logger.info(s"[MPT] Found ${state.size} entries")
                _ <- stateRef.set(copyEntries(state))
                _ <- trieRef.set(None)
                _ <- pendingInsertsRef.set(Map.empty)
                _ <- pendingRemovesRef.set(List.empty)
                _ <- logger.info(s"[MPT] State loaded")
              } yield true
          }

        case None =>
          logger.info(s"[MPT] No state found for ordinal=$ordinal") >> false.pure[F]
      }
    } yield loaded

  def loadOrBuild(
    ordinal: SnapshotOrdinal,
    buildData: => F[Map[Hex, Array[Byte]]]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    load(ordinal).flatMap {
      case true => buildForOrdinal(ordinal)
      case false =>
        for {
          _ <- logger.info("[MPT] Building from provided data")
          data <- buildData
          _ <- PhysicalTrieKeyValidator.validateKeys(data.keys).fold(_.raiseError[F, Unit], _ => Async[F].unit)
          _ <- stateRef.set(copyEntries(data))
          _ <- trieRef.set(None)
          _ <- pendingInsertsRef.set(Map.empty)
          _ <- pendingRemovesRef.set(List.empty)
          result <- buildForOrdinal(ordinal)
        } yield result
    }

  override def deleteAbove(ordinal: SnapshotOrdinal): F[Unit] =
    logger.info(s"[MPT] Deleting above the ordinal=$ordinal") >> storage.deleteAbove(ordinal)

  override def listStoredOrdinals: F[Stream[F, SnapshotOrdinal]] =
    storage.listStoredOrdinals.map(Stream.emits)

  override def applyCutoff(currentOrdinal: SnapshotOrdinal): F[Unit] =
    logger.info(s"[MPT] Applying cutoff at ordinal=$currentOrdinal") >> storage.applyCutoff(currentOrdinal)

  override def savepoint: F[ProducerSavepoint[F]] =
    for {
      savedState <- stateRef.get
      savedTrie <- trieRef.get
      savedPendingInserts <- pendingInsertsRef.get
      savedPendingRemoves <- pendingRemovesRef.get
      savedRootHashCache <- rootHashCacheRef.get
      savedLastBuiltOrdinal <- lastBuiltOrdinalRef.get
    } yield
      new ProducerSavepoint[F] {
        def restore: F[Unit] =
          stateRef.set(savedState) >>
            trieRef.set(savedTrie) >>
            pendingInsertsRef.set(savedPendingInserts) >>
            pendingRemovesRef.set(savedPendingRemoves) >>
            rootHashCacheRef.set(savedRootHashCache) >>
            lastBuiltOrdinalRef.set(savedLastBuiltOrdinal)
      }
}

object FileSystemMerklePatriciaProducer {

  private[producer] def copyBytes(bytes: Array[Byte]): Array[Byte] =
    if (bytes eq null) null else bytes.clone()

  private[producer] def copyEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    entries.iterator.map { case (key, bytes) => key -> copyBytes(bytes) }.toMap

  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](
    path: Path,
    initial: Map[Hex, Array[Byte]] = Map.empty
  ): F[FileSystemMerklePatriciaProducer[F]] =
    for {
      _ <- PhysicalTrieKeyValidator.validateKeys(initial.keys).fold(_.raiseError[F, Unit], _ => Async[F].unit)
      stateRef <- Ref.of[F, Map[Hex, Array[Byte]]](copyEntries(initial))
      trieRef <- Ref.of[F, Option[MerklePatriciaTrie]](None)
      pendingInsertsRef <- Ref.of[F, Map[Hex, Array[Byte]]](Map.empty)
      pendingRemovesRef <- Ref.of[F, List[Hex]](List.empty)
      storage <- MptStateStorage.make[F](path)
      rootHashCacheRef <- Ref.of[F, Map[SnapshotOrdinal, MptRoot]](Map.empty)
      lastBuiltOrdinalRef <- Ref.of[F, Option[SnapshotOrdinal]](None)
    } yield
      new FileSystemMerklePatriciaProducer[F](
        stateRef,
        trieRef,
        pendingInsertsRef,
        pendingRemovesRef,
        storage,
        rootHashCacheRef,
        lastBuiltOrdinalRef
      )

  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](
    path: Path,
    cutoffLogic: OrdinalCutoff,
    initial: Map[Hex, Array[Byte]]
  ): F[FileSystemMerklePatriciaProducer[F]] =
    for {
      _ <- PhysicalTrieKeyValidator.validateKeys(initial.keys).fold(_.raiseError[F, Unit], _ => Async[F].unit)
      stateRef <- Ref.of[F, Map[Hex, Array[Byte]]](copyEntries(initial))
      trieRef <- Ref.of[F, Option[MerklePatriciaTrie]](None)
      pendingInsertsRef <- Ref.of[F, Map[Hex, Array[Byte]]](Map.empty)
      pendingRemovesRef <- Ref.of[F, List[Hex]](List.empty)
      storage <- MptStateStorage.make[F](path, cutoffLogic)
      rootHashCacheRef <- Ref.of[F, Map[SnapshotOrdinal, MptRoot]](Map.empty)
      lastBuiltOrdinalRef <- Ref.of[F, Option[SnapshotOrdinal]](None)
    } yield
      new FileSystemMerklePatriciaProducer[F](
        stateRef,
        trieRef,
        pendingInsertsRef,
        pendingRemovesRef,
        storage,
        rootHashCacheRef,
        lastBuiltOrdinalRef
      )
}
