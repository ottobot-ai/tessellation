package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{MptStore, MptTxAction, StrictMptRead}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §3 NIPoPoW S3 — typed key for the [[MptTowerStore]] partition. Encoded as a lexicographically-sortable hex string `(level, ordinal)` so
  * a prefix scan against the level component returns entries in ordinal order. Not a `GlobalStateKey` — the tower lives in its own MPT
  * producer and its bytes never enter the consensus `mptRoot` / `stateProof` (proposal §4.5: tower root is NOT anchored in headers; each
  * node maintains its own tower locally, recreatable from chain replay).
  */
final case class TowerEntryKey(level: Int, ordinal: SnapshotOrdinal)

final case class InvalidTowerLevel(level: Int)
    extends IllegalArgumentException(s"tower level must be in [1, ${SuperLevelParams.SuperLevelCount}], got $level")

final case class DuplicateTowerTrialLevel(level: Int) extends IllegalArgumentException(s"tower trial list contains duplicate level $level")

object TowerEntryKey {

  private[nipopow] val PartitionName = "MptTowerStore"
  private val EncodedChars = 24
  private val MaxOrdinal = BigInt(Long.MaxValue)

  private def encodeValid(key: TowerEntryKey): Hex =
    Hex(f"${key.level}%08x${key.ordinal.value.value}%016x")

  /** Fixed-width hex layout: `<level as 8 hex chars> <ordinal as 16 hex chars>` = 24 chars total. Lex-sortable, so:
    *   - `levelPrefix(µ)` followed by `producer.entriesWithPrefix` returns every level-µ entry.
    *   - Within a level, lex order on the 16-hex ordinal component matches numeric ordinal order (unsigned padding).
    *
    * Width choice: int32 covers `SuperLevelParams.SuperLevelCount = 9` with ample headroom; int64 covers `SnapshotOrdinal`'s underlying
    * `NonNegLong`.
    */
  def toHex(key: TowerEntryKey): Either[InvalidTowerLevel, Hex] =
    validateLevel(key.level).as(encodeValid(key))

  def toHexF[F[_]: cats.MonadThrow](key: TowerEntryKey): F[Hex] =
    toHex(key).liftTo[F]

  /** Prefix for `producer.entriesWithPrefix` to scope a scan to level `µ`. Just the first 8 hex chars (the level component). */
  def levelPrefix(level: Int): Hex = Hex(f"$level%08x")

  private[nipopow] def validateLevel(level: Int): Either[InvalidTowerLevel, Unit] =
    Either.cond(level >= 1 && level <= SuperLevelParams.SuperLevelCount, (), InvalidTowerLevel(level))

  private[nipopow] def decode(hex: Hex): Either[DurableNipopowRecoveryRequired, TowerEntryKey] =
    decodeCandidate(hex).flatMap(key => DurableNipopowKeyCodec.canonical(PartitionName, hex, encodeValid(key)).as(key))

  private[nipopow] def decodeAll(keys: Iterable[Hex]): Either[DurableNipopowRecoveryRequired, List[(Hex, TowerEntryKey)]] =
    DurableNipopowKeyCodec.decodeAll[TowerEntryKey](
      PartitionName,
      keys,
      decodeCandidate,
      encodeValid,
      key => s"level=${key.level},ordinal=${key.ordinal.value.value}"
    )

  private def decodeCandidate(hex: Hex): Either[DurableNipopowRecoveryRequired, TowerEntryKey] =
    for {
      value <- DurableNipopowKeyCodec.fixedWidthHex(PartitionName, hex, EncodedChars)
      levelLong <- DurableNipopowKeyCodec.boundedUnsigned(
        PartitionName,
        hex,
        "level",
        value.substring(0, 8),
        BigInt(SuperLevelParams.SuperLevelCount)
      )
      level = levelLong.toInt
      _ <- Either.cond(
        level >= 1,
        (),
        MalformedDurableNipopowKey(PartitionName, hex, s"level is out of range: $level")
      )
      ordinalLong <- DurableNipopowKeyCodec.boundedUnsigned(
        PartitionName,
        hex,
        "ordinal",
        value.substring(8, 24),
        MaxOrdinal
      )
      ordinal <- eu.timepit.refined.types.numeric.NonNegLong
        .from(ordinalLong)
        .leftMap(reason => MalformedDurableNipopowKey(PartitionName, hex, s"ordinal is out of range: $reason"))
    } yield TowerEntryKey(level, SnapshotOrdinal(ordinal))
}

/** §3 NIPoPoW S3 — MPT-backed [[TowerStore]] implementation. Persists the per-level superblock-hit index to a dedicated MPT producer (no
  * fold into the global stateProof). Lex-sortable [[TowerEntryKey]] bytes define the durable grammar. Construction validates the complete
  * durable image and materializes an immutable, per-level ordered read index; steady-state tower reads do not repeatedly decode or sort the
  * full MPT image.
  *
  * '''Concurrency.''' Single producer at the finality sink ([[TowerFinalizer]]). Every public operation uses the underlying
  * `MptStore.withExclusiveLock`. Append and prune keep producer I/O cancelable inside a savepoint transaction, then publish the matching
  * private index under a masked boundary only after the transaction succeeds.
  *
  * '''Recoverability.''' Tower is derived; corruption recovery is a chain replay (re-run [[LevelTrialComputer.runAll]] over finalized
  * snapshots and re-`appendAtFinality`). NOT included in the consensus stateProof — see proposal §4.5.
  */
object MptTowerStore {

  private final case class TowerIndex(byLevel: SortedMap[Int, SortedMap[SnapshotOrdinal, Hash]]) {

    def entriesAtLevel(level: Int, since: SnapshotOrdinal): List[TowerEntry] =
      byLevel
        .getOrElse(level, SortedMap.empty[SnapshotOrdinal, Hash])
        .iteratorFrom(since)
        .map { case (ordinal, hash) => TowerEntry(level, ordinal, hash) }
        .toList

    def latestAt(level: Int): Option[TowerEntry] =
      byLevel.get(level).flatMap(_.lastOption).map { case (ordinal, hash) => TowerEntry(level, ordinal, hash) }

    def cumulativeCount(level: Int): Long =
      byLevel.getOrElse(level, SortedMap.empty[SnapshotOrdinal, Hash]).size.toLong

    def append(ordinal: SnapshotOrdinal, snapshotHash: Hash, passes: Vector[LevelTrial]): TowerIndex =
      passes.foldLeft(this) {
        case (index, trial) if trial.passed =>
          val levelEntries = index.byLevel.getOrElse(trial.level, SortedMap.empty[SnapshotOrdinal, Hash])
          index.copy(byLevel = index.byLevel.updated(trial.level, levelEntries.updated(ordinal, snapshotHash)))
        case (index, _) => index
      }

    def pruneBelow(keepFrom: SnapshotOrdinal): TowerIndex =
      copy(byLevel = byLevel.view.mapValues(_.dropWhile { case (ordinal, _) => ordinal < keepFrom }).to(SortedMap))

    def keysBelow(keepFrom: SnapshotOrdinal): List[TowerEntryKey] =
      byLevel.toList.flatMap {
        case (level, entries) =>
          entries.iterator.takeWhile { case (ordinal, _) => ordinal < keepFrom }.map { case (ordinal, _) => TowerEntryKey(level, ordinal) }
      }
  }

  private object TowerIndex {
    val empty: TowerIndex = TowerIndex(SortedMap.empty)

    def from(entries: List[(TowerEntryKey, Hash)]): TowerIndex =
      entries.foldLeft(empty) {
        case (index, (key, hash)) =>
          val levelEntries = index.byLevel.getOrElse(key.level, SortedMap.empty[SnapshotOrdinal, Hash])
          index.copy(byLevel = index.byLevel.updated(key.level, levelEntries.updated(key.ordinal, hash)))
      }
  }

  /** Wrap an existing `MptStore[F, TowerEntryKey]` as a [[TowerStore]]. The store's underlying producer MUST be tower-dedicated — sharing
    * it with the global GSI producer would conflate tower bytes into the consensus `mptRoot`, which proposal §4.5 forbids.
    *
    * Construction is effectful because it is the recovery boundary: the complete durable image is decoded exactly once into a private
    * immutable per-level index. Malformed keys, duplicate logical identities, and malformed hash values fail construction. After this point
    * the wrapped store and producer are exclusively owned here; mutating either through a retained raw reference is unsupported.
    */
  def make[F[_]: Async](store: MptStore[F, TowerEntryKey]): F[TowerStore[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("MptTowerStore")

    loadIndex(store).flatMap(Ref.of[F, TowerIndex]).map { indexRef =>
      new TowerStore[F] {

        override def appendAtFinality(
          ordinal: SnapshotOrdinal,
          snapshotHash: Hash,
          passes: Vector[LevelTrial]
        ): F[Unit] =
          store.withExclusiveLock {
            Async[F].uncancelable { poll =>
              for {
                _ <- validateTrials(passes).liftTo[F]
                toInsert = passes.collect {
                  case trial if trial.passed => TowerEntryKey(trial.level, ordinal) -> snapshotHash
                }.toMap
                // Keep producer I/O cancelable so MptStore can restore its savepoint. Once it
                // commits, remain masked through the matching private-index publication.
                _ <- poll(
                  store.withTransaction(
                    (store.insert[Hash](toInsert) >> store.commit(ordinal))
                      .as(((), MptTxAction.Commit: MptTxAction))
                  )
                )
                  .whenA(toInsert.nonEmpty)
                _ <- indexRef.update(_.append(ordinal, snapshotHash, passes)).whenA(toInsert.nonEmpty)
              } yield ()
            }
          }

        override def entriesAtLevel(level: Int, since: SnapshotOrdinal): F[List[TowerEntry]] =
          store.withExclusiveLock {
            TowerEntryKey.validateLevel(level).liftTo[F] >> indexRef.get.map(_.entriesAtLevel(level, since))
          }

        override def latestAt(level: Int): F[Option[TowerEntry]] =
          store.withExclusiveLock {
            TowerEntryKey.validateLevel(level).liftTo[F] >> indexRef.get.map(_.latestAt(level))
          }

        override def cumulativeCount(level: Int): F[Long] =
          store.withExclusiveLock {
            TowerEntryKey.validateLevel(level).liftTo[F] >> indexRef.get.map(_.cumulativeCount(level))
          }

        override def pruneBelow(keepFrom: SnapshotOrdinal): F[Unit] =
          store.withExclusiveLock {
            Async[F].uncancelable { poll =>
              for {
                current <- indexRef.get
                toRemove = current.keysBelow(keepFrom)
                _ <- logger.debug(s"[MptTowerStore] Pruning ${toRemove.size} entries below ordinal=$keepFrom").whenA(toRemove.nonEmpty)
                _ <- poll(
                  store.withTransaction(
                    store.remove(toRemove).as(((), MptTxAction.Commit: MptTxAction))
                  )
                )
                  .whenA(toRemove.nonEmpty)
                _ <- indexRef.set(current.pruneBelow(keepFrom)).whenA(toRemove.nonEmpty)
              } yield ()
            }
          }

        private def validateTrials(passes: Vector[LevelTrial]): Either[IllegalArgumentException, Unit] =
          passes.toList.traverse_(trial => TowerEntryKey.validateLevel(trial.level)).flatMap { _ =>
            passes
              .groupBy(_.level)
              .collectFirst { case (level, trials) if trials.sizeCompare(1) > 0 => DuplicateTowerTrialLevel(level) }
              .toLeft(())
          }
      }
    }
  }

  private def loadIndex[F[_]: Async](store: MptStore[F, TowerEntryKey]): F[TowerIndex] =
    store.withExclusiveLock {
      for {
        physicalKeys <- store.underlying.physicalKeys
        decoded <- TowerEntryKey.decodeAll(physicalKeys).liftTo[F]
        values <- store.underlying.entriesForKeys(physicalKeys)
        _ <- DurableNipopowEnumerationChanged(TowerEntryKey.PartitionName)
          .raiseError[F, Unit]
          .whenA(values.keySet != physicalKeys)
        entries <- decoded.traverse {
          case (physicalKey, logicalKey) =>
            StrictMptRead.fromStoredBytes[Hash](values(physicalKey)) match {
              case StrictMptRead.Present(hash, _) => (logicalKey -> hash).pure[F]
              case StrictMptRead.Malformed(reason, _) =>
                MalformedDurableNipopowValue(TowerEntryKey.PartitionName, physicalKey, reason).raiseError[F, (TowerEntryKey, Hash)]
              case StrictMptRead.Absent =>
                DurableNipopowEnumerationChanged(TowerEntryKey.PartitionName).raiseError[F, (TowerEntryKey, Hash)]
            }
        }
      } yield TowerIndex.from(entries)
    }

  /** Build a fresh MPT-backed tower store with its own in-memory producer + store. For tests and v1 production (each node maintains its
    * tower locally; persistence across restarts is a chain-replay recovery, not a load).
    *
    * Disk persistence ([[io.constellationnetwork.security.mpt.producer.FileSystemMerklePatriciaProducer]]) can be substituted in a later
    * slice without changing the [[TowerStore]] surface — same `MptStore.make[F, TowerEntryKey]` wiring.
    */
  def inMemory[F[_]: Async: Parallel: Hasher: JsonSerializer]: F[TowerStore[F]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[F]()
      store <- MptStore.make[F, TowerEntryKey](producer, TowerEntryKey.toHexF[F])
      tower <- make[F](store)
    } yield tower

}
