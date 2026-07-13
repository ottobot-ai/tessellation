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
import io.constellationnetwork.security.smt._
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §3 NIPoPoW — typed key for the durable per-ordinal-commitment partition of [[HistoricalCommitmentSmtStore]]. Encoded as a fixed-width,
  * lexicographically-sortable hex string of the currently selected ordinal, EXACTLY like [[TowerEntryKey]]'s ordinal component, so a full
  * prefix scan returns commitments in ordinal order for chain-replay recovery.
  */
final case class CommitmentKey(ordinal: SnapshotOrdinal)

object CommitmentKey {

  /** 16-hex-char (int64, unsigned-padded) encoding of the ordinal — lex order matches numeric ordinal order. This is BOTH the durable MPT
    * key and the SMT leaf key (the SMT hashes it to a uniform 256-bit position), so an inclusion proof is keyed by the same canonical
    * ordinal encoding the durable store uses.
    */
  def toHex(ordinal: SnapshotOrdinal): Hex = Hex(f"${ordinal.value.value}%016x")

  def toHexF[F[_]: cats.Applicative](key: CommitmentKey): F[Hex] = toHex(key.ordinal).pure[F]
}

/** §3 NIPoPoW — the UNBOUNDED, on-disk SMT keyed by snapshot ordinal whose single root is anchored as `smtRoot` in the gl0
  * `GlobalSnapshotStateProof`. Each leaf is a [[PerOrdinalCommitment]] (commitment hash) for one currently selected historical ordinal.
  *
  * '''Two layers, the MptTowerStore relationship.''' The DURABLE substrate is a scodec-coded, MPT-backed commitment KV (`MptStore[F,
  * CommitmentKey]`, value = the ordinal's commitment [[Hash]]) — the same on-disk persistence shape as `MptTowerStore`'s partition. The SMT
  * itself is a DERIVED in-memory index ([[VersionedSmt]], structural-sharing) recomputable from the persisted commitments by a chain replay
  * ([[replayFrom]]) — again exactly the `MptTowerStore` model (durable inputs + derived reads, recoverable by replay). Crucially the
  * commitment KV is its OWN MPT producer, NOT a `GlobalStateKey` partition, so its bytes NEVER enter the consensus
  * `mptRoot`/`hypergraphRoot` — keeping the hypergraph root independent of `smtRoot` (the circularity rule).
  *
  * '''Current cutoff.''' The live implementation computes `smtRoot(N) = SMT({ (i, commitment_i) : 0 ≤ i ≤ N−k }).root`, where `k` is the k1
  * depth trigger. This is a lag, not a size bound. Expressed on [[VersionedSmt]]: the eligible ordinal `j = N−k` is inserted under `version
  * \= N`, so [[rootForSnapshot]]`(N)` = `VersionedSmt.rootAt(N)` = the root over all leaves `i ≤ N−k`.
  *
  * '''Circularity-free.''' Only ordinals `i ≤ N−k` are committed, so snapshot N's OWN `incrementalSnapshotHash` is never in `smtRoot(N)`
  * (it first appears in `smtRoot(N+k)`); references descend strictly toward genesis (well-founded — see [[PerOrdinalCommitment]]).
  *
  * '''Target gap: Phase-2 reorg.''' k1 makes a snapshot operational; it does not make that ordinal/hash immutable. A later density reorg
  * may replace a committed hash at the same ordinal. This ordinal-keyed append path has no exact-hash branch identity or atomic
  * rollback/rebuild transaction, even though `smtRoot` is consensus state. Before this root is load-bearing, the store must version
  * commitments by exact canonical reference and reproduce the replacement root before snapshot production. A retention miss enters
  * `RecoveryRequired`; k1 cannot be treated as a fork-choice floor.
  *
  * '''Version-root retention.''' [[VersionedSmt]] retains the most-recent `versionRetention` ROOTS (for [[proveAt]] of recent past
  * ordinals); the accumulated `live` leaf tree is never pruned. So the tree is unbounded; only the queryable-historical-root WINDOW is
  * bounded — a separate knob from the current `≤ N−k` lag.
  */
trait HistoricalCommitmentSmtStore[F[_]] {

  /** Legacy-named append: record the commitment for current-canonical ordinal `eligibleOrdinal` (`= snapshotOrdinal − k`) as a leaf and
    * snapshot the resulting root under version `snapshotOrdinal`. Persists the commitment hash to the durable KV first (recovery
    * substrate), then folds it into the live SMT. Idempotent: re-appending the same `(snapshotOrdinal, eligibleOrdinal, commitment)`
    * reproduces the same root (upsert with the same value; the SMT is order-/repeat-independent). Returns `smtRoot(snapshotOrdinal)`.
    */
  def appendAtFinality(snapshotOrdinal: SnapshotOrdinal, eligibleOrdinal: SnapshotOrdinal, commitment: PerOrdinalCommitment): F[SmtRoot]

  /** The current `smtRoot` to anchor in snapshot `snapshotOrdinal`'s state proof: the root over commitments `≤ snapshotOrdinal − k`. `None`
    * if no version was recorded for `snapshotOrdinal` (e.g. the genesis/warmup window `N ≤ k`, or before any append — the caller maps
    * `None` to "no smtRoot field", which is deterministic across nodes).
    */
  def rootForSnapshot(snapshotOrdinal: SnapshotOrdinal): F[Option[SmtRoot]]

  /** Inclusion (or absence) proof of `targetOrdinal`'s [[PerOrdinalCommitment]] against `smtRoot(snapshotOrdinal)`. `Left(UnknownVersion)`
    * if `snapshotOrdinal`'s root is not retained.
    */
  def proveAt(snapshotOrdinal: SnapshotOrdinal, targetOrdinal: SnapshotOrdinal): F[Either[SmtProofError, SmtProof]]

  /** The commitment hash persisted for `ordinal`, or `None` if not present in the durable KV. */
  def commitmentHashAt(ordinal: SnapshotOrdinal): F[Option[Hash]]

  /** Chain-replay recovery: rebuild the in-memory SMT version-roots from the durable commitment KV. Re-applies every persisted `(ordinal i,
    * commitmentHash)` as a leaf and re-snapshots the root under version `i + k` (so `rootForSnapshot` is reproduced for the retained
    * window). Idempotent for one ordinal-selected history. Called at boot before the first proof is built. `k` is the current k1 lag; this
    * replay does not yet reconstruct an exact-hash density replacement.
    */
  def replayFrom(k: Long): F[Unit]
}

object HistoricalCommitmentSmtStore {

  /** Wrap a durable `MptStore[F, CommitmentKey]` + an in-memory [[VersionedSmt]] as a [[HistoricalCommitmentSmtStore]].
    *
    * The `k` cutoff is owned by the CALLER: [[appendAtFinality]] takes `(snapshotOrdinal, eligibleOrdinal)` explicitly, so the store is a
    * pure ordinal-keyed structure with no snapshot-lookup dependency. The caller (GSAM wiring) computes `eligibleOrdinal = snapshotOrdinal
    * − k` from the existing k1 lag and is responsible for passing the matching `k` to [[replayFrom]]. This ownership does not make the
    * resulting ordinal immutable.
    */
  def make[F[_]: Async: Hasher](
    durable: MptStore[F, CommitmentKey],
    versioned: VersionedSmt[F]
  ): F[HistoricalCommitmentSmtStore[F]] =
    Async[F].delay {
      val logger = Slf4jLogger.getLoggerFromName[F]("HistoricalCommitmentSmtStore")

      new HistoricalCommitmentSmtStore[F] {

        def appendAtFinality(
          snapshotOrdinal: SnapshotOrdinal,
          eligibleOrdinal: SnapshotOrdinal,
          commitment: PerOrdinalCommitment
        ): F[SmtRoot] =
          for {
            commitHash <- PerOrdinalCommitment.commitmentHash[F](commitment)
            // Durable persist first (recovery substrate), then derive the SMT leaf. The durable KV value is the leaf VALUE bytes the SMT
            // commits — keep them identical so a replay reproduces byte-identical leaves.
            _ <- durable.insert[Hash](CommitmentKey(eligibleOrdinal), commitHash)
            _ <- durable.commit(eligibleOrdinal)
            leafKey = CommitmentKey.toHex(eligibleOrdinal)
            leafValue = Hex(commitHash.value).toBytes
            root <- versioned.commit(snapshotOrdinal, Map(leafKey -> leafValue), Set.empty)
          } yield root

        def rootForSnapshot(snapshotOrdinal: SnapshotOrdinal): F[Option[SmtRoot]] =
          versioned.rootAt(snapshotOrdinal)

        def proveAt(snapshotOrdinal: SnapshotOrdinal, targetOrdinal: SnapshotOrdinal): F[Either[SmtProofError, SmtProof]] =
          versioned.proveAt(snapshotOrdinal, CommitmentKey.toHex(targetOrdinal))

        def commitmentHashAt(ordinal: SnapshotOrdinal): F[Option[Hash]] =
          durable.get[Hash](CommitmentKey(ordinal))

        def replayFrom(replayK: Long): F[Unit] =
          durable.getAllForPrefix[Hash](Hex("")).flatMap { all =>
            // Parse each durable key's ordinal, fold in ascending ordinal order so version-roots are recorded monotonically (the SMT result
            // is order-independent, but VersionedSmt prunes by version, so ascending keeps the most-recent window correct).
            val ordered: List[(SnapshotOrdinal, Hash)] =
              all.toList.flatMap {
                case (hex, hash) => parseOrdinalFromHex(hex).map(_ -> hash)
              }.sortBy(_._1.value.value)

            ordered.traverse_ {
              case (eligibleOrdinal, commitHash) =>
                val version = SnapshotOrdinal(NonNegLong.unsafeFrom(eligibleOrdinal.value.value + replayK))
                val leafKey = CommitmentKey.toHex(eligibleOrdinal)
                val leafValue = Hex(commitHash.value).toBytes
                versioned.commit(version, Map(leafKey -> leafValue), Set.empty).void
            } >> logger.debug(s"[HistoricalCommitmentSmtStore] Replayed ${ordered.size} commitments (k=$replayK)")
          }
      }
    }

  /** Build a fresh store with an in-memory durable producer + in-memory versioned SMT. For tests and v1 production (each node maintains its
    * commitment index locally; cross-restart persistence is a chain-replay recovery, not a load — same as `MptTowerStore.inMemory`).
    * Disk-backed `FileSystemMerklePatriciaProducer` can be substituted without changing this surface.
    */
  def inMemory[F[_]: Async: Parallel: Hasher: JsonSerializer](
    versionRetention: Int
  ): F[HistoricalCommitmentSmtStore[F]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[F]()
      durable <- MptStore.make[F, CommitmentKey](producer, CommitmentKey.toHexF[F])
      versioned <- VersionedSmt.make[F](versionRetention)
      store <- make[F](durable, versioned)
    } yield store

  /** Parse the 16-hex ordinal from a [[CommitmentKey]]-encoded hex (length-checked; `None` on malformed). */
  private[nipopow] def parseOrdinalFromHex(hex: Hex): Option[SnapshotOrdinal] = {
    val s = hex.value
    if (s.length != 16) None
    else
      scala.util
        .Try(java.lang.Long.parseUnsignedLong(s, 16))
        .toOption
        .flatMap(v => NonNegLong.from(v).toOption)
        .map(SnapshotOrdinal(_))
  }

  /** A versioned SMT that retains ALL version-roots (no pruning). Convenience for tests that assert on old roots; production passes a
    * bounded `versionRetention`.
    */
  private[nipopow] val UnboundedVersionRetention: Int = Int.MaxValue
}
