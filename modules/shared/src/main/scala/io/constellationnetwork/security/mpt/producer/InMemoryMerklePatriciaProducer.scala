package io.constellationnetwork.security.mpt.producer

import cats.Parallel
import cats.data.EitherT
import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver
import io.constellationnetwork.security.mpt.{MerklePatriciaTrie, MptRoot}

import io.circe.syntax._
import io.circe.{Encoder, Json}

final class InMemoryMerklePatriciaProducer[F[_]: Async: Hasher: Parallel: JsonSerializer] private (
  stateRef: Ref[F, Map[Hex, Array[Byte]]],
  trieRef: Ref[F, Option[MerklePatriciaTrie]],
  pendingInsertsRef: Ref[F, Map[Hex, Array[Byte]]],
  pendingRemovesRef: Ref[F, List[Hex]],
  rootHashCacheRef: Ref[F, Map[SnapshotOrdinal, MptRoot]],
  lastBuiltOrdinalRef: Ref[F, Option[SnapshotOrdinal]],
  override val physicalKeyPolicy: PhysicalTrieKeyPolicy
) extends StatefulMerklePatriciaProducer[F] {

  private val parallelProducer: ParallelMerklePatriciaProducer[F] = ParallelMerklePatriciaProducer[F]
  private val MaxCacheSize = 50

  private def copyEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    InMemoryMerklePatriciaProducer.copyEntries(entries)

  private def stageChanges(
    byteEntries: Map[Hex, Array[Byte]],
    removals: List[Hex]
  ): F[Either[MerklePatriciaError, Unit]] =
    Async[F].uncancelable { _ =>
      stateRef
        .modify[Either[MerklePatriciaError, Unit]] { current =>
          (for {
            _ <- physicalKeyPolicy.validateEach(removals)
            _ <- physicalKeyPolicy.validateInsertion(current.keySet -- removals, byteEntries.keys)
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
    stateRef.get.map(_.get(key).map(InMemoryMerklePatriciaProducer.copyBytes))

  override def entriesForKeys(keys: Set[Hex]): F[Map[Hex, Array[Byte]]] =
    stateRef.get.map { entries =>
      keys.iterator.flatMap(key => entries.get(key).map(bytes => key -> InMemoryMerklePatriciaProducer.copyBytes(bytes))).toMap
    }

  override def entryCount: F[Int] = stateRef.get.map(_.size)

  override def entriesWithPrefix(prefix: Hex): F[Map[Hex, Array[Byte]]] =
    stateRef.get.map(entries =>
      copyEntries(entries.filter { case (key, _) => StatefulMerklePatriciaProducer.hasNibblePrefix(key, prefix) })
    )

  override def build: F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    for {
      currentTrie <- trieRef.get
      pendingInserts <- pendingInsertsRef.get
      pendingRemoves <- pendingRemovesRef.get

      result <- (currentTrie, pendingInserts.isEmpty, pendingRemoves.isEmpty) match {
        case (None, _, _) =>
          fullBuild

        case (Some(trie), true, true) =>
          trie.asRight[MerklePatriciaError].pure[F]

        case (Some(trie), _, _) =>
          incrementalUpdate(trie, pendingInserts, pendingRemoves)
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

  private def fullBuild: F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    for {
      currentEntries <- entries
      result <-
        if (currentEntries.isEmpty) {
          (OperationError("Cannot build trie with no entries"): MerklePatriciaError)
            .asLeft[MerklePatriciaTrie]
            .pure[F]
        } else {
          parallelProducer.createFromBytes(currentEntries).attempt.flatMap {
            case Right(trie) =>
              (trieRef.set(Some(trie)) >>
                pendingInsertsRef.set(Map.empty) >>
                pendingRemovesRef.set(List.empty)).as(trie.asRight[MerklePatriciaError])
            case Left(e: MerklePatriciaError) =>
              e.asLeft[MerklePatriciaTrie].pure[F]
            case Left(e) =>
              (OperationError(e.getMessage): MerklePatriciaError)
                .asLeft[MerklePatriciaTrie]
                .pure[F]
          }
        }
    } yield result

  private def incrementalUpdate(
    currentTrie: MerklePatriciaTrie,
    inserts: Map[Hex, Array[Byte]],
    removes: List[Hex]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] = {

    def applyRemoves(trie: MerklePatriciaTrie): EitherT[F, MerklePatriciaError, MerklePatriciaTrie] =
      if (removes.isEmpty) EitherT.rightT(trie)
      else EitherT(parallelProducer.remove(trie, removes))

    def applyInserts(trie: MerklePatriciaTrie): EitherT[F, MerklePatriciaError, MerklePatriciaTrie] =
      if (inserts.isEmpty) EitherT.rightT(trie)
      else EitherT(parallelProducer.insertFromBytes(trie, inserts))

    def clearPending(trie: MerklePatriciaTrie): EitherT[F, MerklePatriciaError, Unit] =
      EitherT.liftF(
        trieRef.set(Some(trie)) >>
          pendingInsertsRef.set(Map.empty) >>
          pendingRemovesRef.set(List.empty)
      )

    (for {
      afterRemoves <- applyRemoves(currentTrie)
      finalTrie <- applyInserts(afterRemoves)
      _ <- clearPending(finalTrie)
    } yield finalTrie).value
  }

  override def insert[A: Encoder](data: Map[Hex, A]): F[Either[MerklePatriciaError, Unit]] =
    if (data.isEmpty) ().asRight[MerklePatriciaError].pure[F]
    else {
      for {
        byteEntries <- data.toList.traverse {
          case (k, v) =>
            JsonSerializer[F].serialize(v.asJson).map(k -> _)
        }.map(_.toMap)
        result <- stageChanges(byteEntries, List.empty)
      } yield result
    }

  override def insertBytes(data: Map[Hex, Array[Byte]]): F[Either[MerklePatriciaError, Unit]] =
    if (data.isEmpty) ().asRight[MerklePatriciaError].pure[F]
    else {
      val owned = copyEntries(data)
      stageChanges(owned, List.empty)
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
          if (!state.contains(key)) {
            (OperationError(s"Key not found for update: $key"): MerklePatriciaError)
              .asLeft[Unit]
              .pure[F]
          } else {
            for {
              bytes <- JsonSerializer[F].serialize(value.asJson)
              _ <- stateRef.update(_ + (key -> bytes))
              _ <- pendingInsertsRef.update(_ + (key -> bytes))
            } yield ().asRight[MerklePatriciaError]
          }
        }
    }

  override def remove(keys: List[Hex]): F[Either[MerklePatriciaError, Unit]] =
    replaceBytes(Map.empty, keys)

  override def clear: F[Unit] =
    stateRef.set(Map.empty) >>
      trieRef.set(None) >>
      pendingInsertsRef.set(Map.empty) >>
      pendingRemovesRef.set(List.empty) >>
      rootHashCacheRef.set(Map.empty) >>
      lastBuiltOrdinalRef.set(None)

  override def buildHexMap(data: Map[GlobalStateKey, Json]): F[Map[Hex, Array[Byte]]] = {
    val BatchSize = 5000

    val encodedPairs =
      if (data.size <= BatchSize)
        data.toList.parTraverse {
          case (key, value) =>
            for {
              hex <- GlobalStateKey.toHex[F](key)
              bytes <- JsonSerializer[F].serialize(value)
            } yield hex -> bytes
        }
      else
        data.toList
          .grouped(BatchSize)
          .toList
          .traverse { batch =>
            batch.parTraverse {
              case (key, value) =>
                for {
                  hex <- GlobalStateKey.toHex[F](key)
                  bytes <- JsonSerializer[F].serialize(value)
                } yield hex -> bytes
            } <* Async[F].cede
          }
          .map(_.flatten)

    encodedPairs.flatMap { entries =>
      physicalKeyPolicy.validateComplete(entries.map(_._1)).liftTo[F].as(entries.toMap)
    }
  }

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

object InMemoryMerklePatriciaProducer {

  private[producer] def copyBytes(bytes: Array[Byte]): Array[Byte] =
    if (bytes eq null) null else bytes.clone()

  private[producer] def copyEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    entries.iterator.map { case (key, bytes) => key -> copyBytes(bytes) }.toMap

  def make[F[_]: Async: Hasher: Parallel: JsonSerializer](
    initial: Map[Hex, Array[Byte]] = Map.empty
  ): F[InMemoryMerklePatriciaProducer[F]] =
    makeWithPolicy(initial, PhysicalTrieKeyPolicy.Generic)

  def makeWithFixedWidthKeys[F[_]: Async: Hasher: Parallel: JsonSerializer](
    widthBytes: Int,
    initial: Map[Hex, Array[Byte]] = Map.empty
  ): F[InMemoryMerklePatriciaProducer[F]] =
    PhysicalTrieKeyPolicy.fixedWidth(widthBytes).liftTo[F].flatMap(makeWithPolicy(initial, _))

  private def makeWithPolicy[F[_]: Async: Hasher: Parallel: JsonSerializer](
    initial: Map[Hex, Array[Byte]],
    physicalKeyPolicy: PhysicalTrieKeyPolicy
  ): F[InMemoryMerklePatriciaProducer[F]] =
    for {
      _ <- physicalKeyPolicy.validateComplete(initial.keys).fold(_.raiseError[F, Unit], _ => Async[F].unit)
      stateRef <- Ref.of[F, Map[Hex, Array[Byte]]](copyEntries(initial))
      trieRef <- Ref.of[F, Option[MerklePatriciaTrie]](None)
      pendingInsertsRef <- Ref.of[F, Map[Hex, Array[Byte]]](Map.empty)
      pendingRemovesRef <- Ref.of[F, List[Hex]](List.empty)
      rootHashCacheRef <- Ref.of[F, Map[SnapshotOrdinal, MptRoot]](Map.empty)
      lastBuiltOrdinalRef <- Ref.of[F, Option[SnapshotOrdinal]](None)
    } yield
      new InMemoryMerklePatriciaProducer[F](
        stateRef,
        trieRef,
        pendingInsertsRef,
        pendingRemovesRef,
        rootHashCacheRef,
        lastBuiltOrdinalRef,
        physicalKeyPolicy
      )
}
