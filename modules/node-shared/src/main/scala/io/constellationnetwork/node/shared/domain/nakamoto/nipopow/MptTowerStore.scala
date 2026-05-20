package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.MptStore
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

object TowerEntryKey {

  /** Fixed-width hex layout: `<level as 8 hex chars> <ordinal as 16 hex chars>` = 24 chars total. Lex-sortable, so:
    *   - `levelPrefix(µ)` followed by `producer.entriesWithPrefix` returns every level-µ entry.
    *   - Within a level, lex order on the 16-hex ordinal component matches numeric ordinal order (unsigned padding).
    *
    * Width choice: int32 covers `SuperLevelParams.SuperLevelCount = 9` with ample headroom; int64 covers `SnapshotOrdinal`'s underlying
    * `NonNegLong`.
    */
  def toHexF[F[_]: cats.Applicative](key: TowerEntryKey): F[Hex] =
    Hex(f"${key.level}%08x${key.ordinal.value.value}%016x").pure[F]

  /** Prefix for `producer.entriesWithPrefix` to scope a scan to level `µ`. Just the first 8 hex chars (the level component). */
  def levelPrefix(level: Int): Hex = Hex(f"$level%08x")
}

/** §3 NIPoPoW S3 — MPT-backed [[TowerStore]] implementation. Persists the per-level superblock-hit index to a dedicated MPT producer (no
  * fold into the global stateProof). The MPT layer gives ordered prefix scan via lex-sortable [[TowerEntryKey]] hex, which the
  * `entriesAtLevel` reader uses to materialize `µ`-level entries in ordinal order.
  *
  * '''Concurrency.''' Single producer at the finality sink ([[TowerFinalizer]]); reads are lock-free `producer.entries` snapshots. The
  * underlying `MptStore.withExclusiveLock` is not required here because the finalizer is single-fiber and pruning is rare.
  *
  * '''Recoverability.''' Tower is derived; corruption recovery is a chain replay (re-run [[LevelTrialComputer.runAll]] over finalized
  * snapshots and re-`appendAtFinality`). NOT included in the consensus stateProof — see proposal §4.5.
  */
object MptTowerStore {

  /** Wrap an existing `MptStore[F, TowerEntryKey]` as a [[TowerStore]]. The store's underlying producer MUST be tower-dedicated — sharing
    * it with the global GSI producer would conflate tower bytes into the consensus `mptRoot`, which proposal §4.5 forbids.
    */
  def make[F[_]: Async](store: MptStore[F, TowerEntryKey]): TowerStore[F] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("MptTowerStore")

    new TowerStore[F] {

      override def appendAtFinality(
        ordinal: SnapshotOrdinal,
        snapshotHash: Hash,
        passes: Vector[LevelTrial]
      ): F[Unit] = {
        val toInsert: Map[TowerEntryKey, Hash] = passes.collect {
          case trial if trial.passed => TowerEntryKey(trial.level, ordinal) -> snapshotHash
        }.toMap
        if (toInsert.isEmpty) Async[F].unit
        else store.insert[Hash](toInsert) >> store.commit(ordinal)
      }

      override def entriesAtLevel(level: Int, since: SnapshotOrdinal): F[List[TowerEntry]] =
        store.getAllForPrefix[Hash](TowerEntryKey.levelPrefix(level)).map { hexMap =>
          val sinceHex = f"$level%08x${since.value.value}%016x"
          hexMap.toList.filter { case (hex, _) => hex.value >= sinceHex }
            .sortBy(_._1.value)
            .flatMap {
              case (hex, hash) => parseOrdinalFromHex(hex).map(ord => TowerEntry(level, ord, hash))
            }
        }

      override def latestAt(level: Int): F[Option[TowerEntry]] =
        store.getAllForPrefix[Hash](TowerEntryKey.levelPrefix(level)).map { hexMap =>
          if (hexMap.isEmpty) None
          else {
            val (hex, hash) = hexMap.maxBy(_._1.value)
            parseOrdinalFromHex(hex).map(ord => TowerEntry(level, ord, hash))
          }
        }

      override def cumulativeCount(level: Int): F[Long] =
        store.getAllForPrefix[Hash](TowerEntryKey.levelPrefix(level)).map(_.size.toLong)

      override def pruneBelow(keepFrom: SnapshotOrdinal): F[Unit] =
        for {
          allEntries <- store.underlying.entries
          // Tower partition is the entire underlying producer, so EVERY hex key is a TowerEntryKey
          // (no namespace separation needed). Filter to entries whose embedded ordinal < keepFrom.
          toRemove = allEntries.keys.toList.filter { hex =>
            parseOrdinalFromHex(hex).exists(_.value.value < keepFrom.value.value)
          }
          _ <- logger.debug(s"[MptTowerStore] Pruning ${toRemove.size} entries below ordinal=$keepFrom").whenA(toRemove.nonEmpty)
          _ <- store.underlying.remove(toRemove).void.whenA(toRemove.nonEmpty)
        } yield ()
    }
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
    } yield make[F](store)

  /** Parse the 16-hex ordinal suffix from a [[TowerEntryKey]]-encoded hex. Length-checked; returns `None` on malformed hex (defensive — the
    * partition is single-writer so malformed entries shouldn't appear, but a typed parse failure beats a `.toLong` throw).
    */
  private[nipopow] def parseOrdinalFromHex(hex: Hex): Option[SnapshotOrdinal] = {
    val s = hex.value
    if (s.length != 24) None
    else
      scala.util
        .Try(java.lang.Long.parseUnsignedLong(s.substring(8, 24), 16))
        .toOption
        .flatMap(v => eu.timepit.refined.types.numeric.NonNegLong.from(v).toOption)
        .map(nn => SnapshotOrdinal(nn))
  }
}
