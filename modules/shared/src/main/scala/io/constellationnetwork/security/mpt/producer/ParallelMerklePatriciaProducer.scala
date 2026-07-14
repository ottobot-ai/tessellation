package io.constellationnetwork.security.mpt.producer

import java.util.concurrent.{CountDownLatch, Executors}

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import scala.collection.mutable

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt._
import io.constellationnetwork.security.mpt.prover.MerklePatriciaSingleInclusionProver

import io.circe.Encoder
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Parallel MPT producer - builds trie with hashes computed during construction. Nodes are immutable with pre-computed digests.
  */
class ParallelMerklePatriciaProducer[F[_]: Hasher: Async: Parallel: JsonSerializer](
  maxThreads: Int = Runtime.getRuntime.availableProcessors()
) extends MerklePatriciaProducer[F] {

  private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)
  private val ParallelDepthThreshold = 10

  def getProver(trie: MerklePatriciaTrie): F[MerklePatriciaSingleInclusionProver[F]] =
    MerklePatriciaSingleInclusionProver.make[F](trie).pure[F]

  /** Create trie from pre-serialized bytes. Hashes are computed during node construction.
    */
  def createFromBytes(data: Map[Hex, Array[Byte]]): F[MerklePatriciaTrie] =
    PhysicalTrieKeyValidator.validateKeys(data.keys) match {
      case Left(error) => error.raiseError[F, MerklePatriciaTrie]
      case Right(_) if data.isEmpty =>
        MerklePatriciaNode.Branch.empty[F].map(MerklePatriciaTrie(_))
      case Right(_) =>
        for {
          _ <- logger.info(s"[MPT] Creating trie from ${data.size} entries")
          entries <- Async[F].blocking(prepareAndSortBytes(data))
          dataHashes <- batchComputeDataHashes(entries)
          root <- buildTree(entries, dataHashes, 0, entries.length, 0)
          _ <- logger.info(s"[MPT] Created trie")
        } yield MerklePatriciaTrie(root)
    }

  def create[A: Encoder](data: Map[Hex, A]): F[MerklePatriciaTrie] =
    if (data.isEmpty) {
      MerklePatriciaNode.Branch.empty[F].map(MerklePatriciaTrie(_))
    } else {
      for {
        byteData <- data.toList.parTraverse {
          case (k, v) =>
            JsonSerializer[F].serialize(v).map(k -> _)
        }.map(_.toMap)
        trie <- createFromBytes(byteData)
      } yield trie
    }

  def insert[A: Encoder](
    current: MerklePatriciaTrie,
    data: Map[Hex, A]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    if (data.isEmpty) current.asRight[MerklePatriciaError].pure[F]
    else
      validateInsertCandidate(current, data.keys) match {
        case Left(error) => (error: MerklePatriciaError).asLeft[MerklePatriciaTrie].pure[F]
        case Right(_) =>
          (for {
            // Convert to bytes and compute hashes
            entries <- data.toList.parTraverse {
              case (hex, value) =>
                for {
                  bytes <- JsonSerializer[F].serialize(value)
                  hash <- Hasher[F].hashBytes(bytes)
                } yield (hex, hash)
            }
            sortedEntries = entries.sortBy { case (hex, _) => CompactNibblePath.fromHexString(hex.value) }
            // Apply incremental inserts
            newRoot <- IncrementalTrieOps.insertMultiple[F](current.rootNode, sortedEntries)
          } yield MerklePatriciaTrie(newRoot).asRight[MerklePatriciaError])
            .handleError(e => (OperationError(e.getMessage): MerklePatriciaError).asLeft)
      }

  def insertFromBytes(
    current: MerklePatriciaTrie,
    data: Map[Hex, Array[Byte]]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    if (data.isEmpty) current.asRight[MerklePatriciaError].pure[F]
    else
      validateInsertCandidate(current, data.keys) match {
        case Left(error) => (error: MerklePatriciaError).asLeft[MerklePatriciaTrie].pure[F]
        case Right(_) =>
          (for {
            // Compute hashes for byte data
            entries <- data.toList.parTraverse {
              case (hex, bytes) =>
                Hasher[F].hashBytes(bytes).map(hash => (hex, hash))
            }
            sortedEntries = entries.sortBy { case (hex, _) => CompactNibblePath.fromHexString(hex.value) }
            // Apply incremental inserts
            newRoot <- IncrementalTrieOps.insertMultiple[F](current.rootNode, sortedEntries)
          } yield MerklePatriciaTrie(newRoot).asRight[MerklePatriciaError])
            .handleError(e => (OperationError(e.getMessage): MerklePatriciaError).asLeft)
      }

  def remove(
    current: MerklePatriciaTrie,
    keys: List[Hex]
  ): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
    if (keys.isEmpty) current.asRight[MerklePatriciaError].pure[F]
    else
      PhysicalTrieKeyValidator.validateEachKey(keys) match {
        case Left(error) => (error: MerklePatriciaError).asLeft[MerklePatriciaTrie].pure[F]
        case Right(_) =>
          val sortedKeys = keys.sortBy(hex => CompactNibblePath.fromHexString(hex.value))
          IncrementalTrieOps
            .removeMultiple[F](current.rootNode, sortedKeys)
            .map(newRoot => MerklePatriciaTrie(newRoot).asRight[MerklePatriciaError])
            .handleError(e => (OperationError(e.getMessage): MerklePatriciaError).asLeft)
      }

  private def currentPhysicalKeys(current: MerklePatriciaTrie): List[Hex] =
    MerklePatriciaTrie.collectLeafNodesWithPaths(current).map(_._1)

  private def validateInsertCandidate(
    current: MerklePatriciaTrie,
    insertKeys: Iterable[Hex]
  ): Either[PhysicalTrieKeyError, Unit] =
    PhysicalTrieKeyValidator.validateInsertion(currentPhysicalKeys(current).toSet, insertKeys)

  private def prepareAndSortBytes(data: Map[Hex, Array[Byte]]): Array[(CompactNibblePath, Array[Byte])] = {
    val dataArray = data.toArray
    val results = new Array[(CompactNibblePath, Array[Byte])](dataArray.length)
    val numThreads = maxThreads
    val chunkSize = math.max(1, (dataArray.length + numThreads - 1) / numThreads)
    val executor = Executors.newFixedThreadPool(numThreads)
    val latch = new CountDownLatch(numThreads)

    for (i <- 0 until numThreads) {
      val startIdx = i * chunkSize
      val endIdx = math.min(startIdx + chunkSize, dataArray.length)
      executor.submit(new Runnable {
        def run(): Unit =
          try {
            var j = startIdx
            while (j < endIdx) {
              val (hex, bytes) = dataArray(j)
              results(j) = (CompactNibblePath.fromHexString(hex.value), bytes)
              j += 1
            }
          } finally latch.countDown()
      })
    }
    latch.await()
    executor.shutdown()

    java.util.Arrays.parallelSort(
      results,
      (a: (CompactNibblePath, Array[Byte]), b: (CompactNibblePath, Array[Byte])) => CompactNibblePath.ordering.compare(a._1, b._1)
    )
    results
  }

  private def batchComputeDataHashes(entries: Array[(CompactNibblePath, Array[Byte])]): F[Array[String]] = {
    val numEntries = entries.length
    // Use smaller batches for better parallelism - aim for ~1000 entries per batch
    val batchSize = math.max(100, math.min(1000, numEntries / (maxThreads * 2)))

    for {
      results <- Async[F].delay(new Array[String](numEntries))
      batches = entries.indices.toList.grouped(batchSize).toList
      // Process batches in parallel, and within each batch also use parallel traversal
      _ <- batches.parTraverse_ { batch =>
        batch.parTraverse_ { i =>
          Hasher[F].hashBytes(entries(i)._2).flatMap { hash =>
            Async[F].delay(results(i) = hash.value)
          }
        }
      }
    } yield results
  }

  private def buildTree(
    entries: Array[(CompactNibblePath, Array[Byte])],
    dataHashes: Array[String],
    start: Int,
    end: Int,
    depth: Int
  ): F[MerklePatriciaNode] = {
    val size = end - start

    // Add cede point at shallow depths to prevent CPU starvation
    val maybeCede = if (depth <= 3 && size > 1000) Async[F].cede else Async[F].unit

    maybeCede >> {
      if (size == 0) {
        MerklePatriciaNode.Branch.empty[F].widen
      } else if (size == 1) {
        val (path, _) = entries(start)
        val dataHash = Hash(dataHashes(start))
        val remaining = if (depth >= path.length) CompactNibblePath.empty else path.drop(depth)
        MerklePatriciaNode.Leaf.fromCompact[F](remaining, dataHash).widen
      } else {
        buildWithGroups(entries, dataHashes, start, end, depth)
      }
    }
  }

  private def buildWithGroups(
    entries: Array[(CompactNibblePath, Array[Byte])],
    dataHashes: Array[String],
    start: Int,
    end: Int,
    depth: Int
  ): F[MerklePatriciaNode] =
    ParallelMerklePatriciaProducer.ensureGroupProgress(entries, start, end, depth) match {
      case Left(error) => error.raiseError[F, MerklePatriciaNode]
      case Right(_) =>
        val groups = findGroups(entries, start, end, depth)

        if (groups.length == 1) {
          val (nibbleValue, gs, ge) = groups(0)
          buildTree(entries, dataHashes, gs, ge, depth + 1).flatMap { child =>
            createSingleGroupNode(nibbleValue, child)
          }
        } else {
          val useParallel = depth <= ParallelDepthThreshold || groups.length >= 4

          val buildChildren: F[List[(Nibble, MerklePatriciaNode)]] =
            if (useParallel) {
              groups.toList.parTraverse {
                case (nibbleValue, gs, ge) =>
                  buildTree(entries, dataHashes, gs, ge, depth + 1).map(Nibble.unsafe(nibbleValue) -> _)
              }
            } else {
              groups.toList.traverse {
                case (nibbleValue, gs, ge) =>
                  buildTree(entries, dataHashes, gs, ge, depth + 1).map(Nibble.unsafe(nibbleValue) -> _)
              }
            }

          buildChildren.flatMap { children =>
            // Sort by nibble value for deterministic branch construction
            MerklePatriciaNode.Branch(children.sortBy(_._1.value).toMap).widen
          }
        }
    }

  @inline private def findGroups(
    entries: Array[(CompactNibblePath, Array[Byte])],
    start: Int,
    end: Int,
    depth: Int
  ): mutable.ArrayBuffer[(Byte, Int, Int)] = {
    val groups = mutable.ArrayBuffer[(Byte, Int, Int)]()
    var groupStart = start
    var currentNibble = entries(start)._1.getOrEmpty(depth)

    var i = start + 1
    while (i < end) {
      val nibble = entries(i)._1.getOrEmpty(depth)
      if (nibble != currentNibble) {
        groups += ((currentNibble, groupStart, i))
        groupStart = i
        currentNibble = nibble
      }
      i += 1
    }
    groups += ((currentNibble, groupStart, end))
    groups
  }

  private def createSingleGroupNode(
    nibbleValue: Byte,
    child: MerklePatriciaNode
  ): F[MerklePatriciaNode] =
    child match {
      case branch: MerklePatriciaNode.Branch =>
        MerklePatriciaNode.Extension.fromCompact(CompactNibblePath.single(nibbleValue), branch).widen

      case ext: MerklePatriciaNode.Extension =>
        MerklePatriciaNode.Extension.fromCompact(CompactNibblePath.single(nibbleValue) ++ ext.sharedPath, ext.child).widen

      case leaf: MerklePatriciaNode.Leaf =>
        MerklePatriciaNode.Leaf.fromCompact(CompactNibblePath.single(nibbleValue) ++ leaf.remainingPath, leaf.dataDigest).widen
    }
}

object ParallelMerklePatriciaProducer {
  private[mpt] def ensureGroupProgress(
    entries: Array[(CompactNibblePath, Array[Byte])],
    start: Int,
    end: Int,
    depth: Int
  ): Either[NonShrinkingPhysicalTrieGroup, Unit] = {
    var terminalIndex = -1
    var index = start

    while (index < end && terminalIndex < 0) {
      if (entries(index)._1.length <= depth) terminalIndex = index
      index += 1
    }

    if (terminalIndex < 0 || end - start <= 1) Right(())
    else {
      val conflictingIndex = if (terminalIndex == start) start + 1 else start
      Left(
        NonShrinkingPhysicalTrieGroup(
          depth,
          entries(terminalIndex)._1.toHex,
          entries(conflictingIndex)._1.toHex
        )
      )
    }
  }

  def apply[F[_]: Hasher: Async: Parallel: JsonSerializer]: ParallelMerklePatriciaProducer[F] =
    new ParallelMerklePatriciaProducer[F]()
}
