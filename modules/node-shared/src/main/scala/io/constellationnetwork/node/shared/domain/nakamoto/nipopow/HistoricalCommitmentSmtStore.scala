package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.Parallel
import cats.effect.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{MptStore, StrictMptRead}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.smt._
import io.constellationnetwork.serde.codecs.instances.HashCodec._

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §3 NIPoPoW — typed key for the per-ordinal recovery partition of [[HistoricalCommitmentSmtStore]]. Encoded as a fixed-width,
  * lexicographically-sortable hex string of the currently selected ordinal, EXACTLY like [[TowerEntryKey]]'s ordinal component, so a full
  * prefix scan returns commitments in ordinal order for chain-replay recovery.
  */
final case class CommitmentKey(ordinal: SnapshotOrdinal)

object CommitmentKey {

  private[nipopow] val PartitionName = "HistoricalCommitmentSmtStore"
  private val EncodedChars = 16
  private val MaxOrdinal = BigInt(Long.MaxValue)

  /** 16-hex-char (int64, unsigned-padded) encoding of the ordinal — lex order matches numeric ordinal order. This is BOTH the recovery MPT
    * key and the SMT leaf key (the SMT hashes it to a uniform 256-bit position), so an inclusion proof is keyed by the same canonical
    * ordinal encoding the recovery store uses.
    */
  def toHex(ordinal: SnapshotOrdinal): Hex = Hex(f"${ordinal.value.value}%016x")

  def toHexF[F[_]: cats.Applicative](key: CommitmentKey): F[Hex] = toHex(key.ordinal).pure[F]

  private[nipopow] def decode(hex: Hex): Either[DurableNipopowRecoveryRequired, CommitmentKey] =
    decodeCandidate(hex).flatMap(key => DurableNipopowKeyCodec.canonical(PartitionName, hex, toHex(key.ordinal)).as(key))

  private[nipopow] def decodeAll(keys: Iterable[Hex]): Either[DurableNipopowRecoveryRequired, List[(Hex, CommitmentKey)]] =
    DurableNipopowKeyCodec.decodeAll[CommitmentKey](
      PartitionName,
      keys,
      decodeCandidate,
      key => toHex(key.ordinal),
      key => s"ordinal=${key.ordinal.value.value}"
    )

  private def decodeCandidate(hex: Hex): Either[DurableNipopowRecoveryRequired, CommitmentKey] =
    for {
      value <- DurableNipopowKeyCodec.fixedWidthHex(PartitionName, hex, EncodedChars)
      ordinalLong <- DurableNipopowKeyCodec.boundedUnsigned(PartitionName, hex, "ordinal", value, MaxOrdinal)
      ordinal <- NonNegLong
        .from(ordinalLong)
        .leftMap(reason => MalformedDurableNipopowKey(PartitionName, hex, s"ordinal is out of range: $reason"))
    } yield CommitmentKey(SnapshotOrdinal(ordinal))
}

final case class InvalidHistoricalReplayLag(replayK: Long)
    extends IllegalArgumentException(s"historical commitment replay lag must be nonnegative, got $replayK")

/** §3 NIPoPoW — the unbounded, ordinal-keyed historical commitment SMT whose target root is the `smtRoot` field in the GL0
  * `GlobalSnapshotStateProof`. Each leaf is a [[PerOrdinalCommitment]] hash for one currently selected historical ordinal.
  *
  * '''Two layers, the MptTowerStore relationship.''' The recovery substrate is a scodec-coded, MPT-backed commitment KV (`MptStore[F,
  * CommitmentKey]`, value = the ordinal's commitment [[Hash]]). The SMT itself is a derived in-memory index ([[VersionedSmt]],
  * structural-sharing) recomputable from one complete retained KV image by [[replayFrom]]. Current GL0 production wiring uses the in-memory
  * constructor, and `MptStore.commit` does not provide a crash-durability receipt; production disk/bootstrap wiring and crash atomicity
  * remain STOR-01 and REC-004 work. Crucially the commitment KV is its OWN MPT producer, NOT a `GlobalStateKey` partition, so its bytes
  * NEVER enter the consensus `mptRoot`/`hypergraphRoot` — keeping the hypergraph root independent of `smtRoot` (the circularity rule).
  *
  * '''Staged cutoff.''' When explicitly exercised, this implementation computes `smtRoot(N) = SMT({ (i, commitment_i) : 0 ≤ i ≤ N−k
  * }).root`, where `k` is the k1 depth trigger. This is a lag, not a size bound. Expressed on [[VersionedSmt]]: the eligible ordinal `j =
  * N−k` is inserted under `version = N`, so [[rootForSnapshot]]`(N)` = `VersionedSmt.rootAt(N)` = the root over all leaves `i ≤ N−k`.
  * Active-era GL0 requires `GlobalSnapshotStateProof.smtRoot = None`; this store is not wired as snapshot validity authority.
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
    * snapshot the resulting root under version `snapshotOrdinal`. Submits the commitment hash to the recovery KV first, then folds it into
    * the live SMT; that call ordering is not a crash-durability guarantee. Re-appending the exact same tuple is idempotent only while
    * `snapshotOrdinal` is still the latest derived version. Re-committing an older version after later commits is not supported and
    * exact-hash reorg recovery remains open. Returns `smtRoot(snapshotOrdinal)`.
    */
  def appendAtFinality(snapshotOrdinal: SnapshotOrdinal, eligibleOrdinal: SnapshotOrdinal, commitment: PerOrdinalCommitment): F[SmtRoot]

  /** The staged future-era `smtRoot` candidate for `snapshotOrdinal`: the root over commitments `≤ snapshotOrdinal − k`. `None` if no
    * version was recorded for `snapshotOrdinal` (e.g. the genesis/warmup window `N < k`, or before any append). Active-era callers must
    * leave the signed state-proof field absent regardless of this result.
    */
  def rootForSnapshot(snapshotOrdinal: SnapshotOrdinal): F[Option[SmtRoot]]

  /** Inclusion (or absence) proof of `targetOrdinal`'s [[PerOrdinalCommitment]] against `smtRoot(snapshotOrdinal)`. `Left(UnknownVersion)`
    * if `snapshotOrdinal`'s root is not retained.
    */
  def proveAt(snapshotOrdinal: SnapshotOrdinal, targetOrdinal: SnapshotOrdinal): F[Either[SmtProofError, SmtProof]]

  /** The commitment hash present in the current recovery KV image for `ordinal`, or `None` if absent. */
  def commitmentHashAt(ordinal: SnapshotOrdinal): F[Option[Hash]]

  /** Chain-replay recovery: rebuild the in-memory SMT version-roots from the recovery commitment KV. The caller supplies the authenticated
    * terminal eligible ordinal, and replay requires the exact contiguous range `0..expectedEligible`; a missing prefix, interior ordinal,
    * or suffix enters authenticated recovery instead of constructing a reduced root. Every `(ordinal i, commitmentHash)` is re-applied as a
    * leaf and the root is re-snapshotted under version `i + k` (so `rootForSnapshot` is reproduced for the retained window). The
    * replacement is built from empty in isolation and published in one swap; malformed/incomplete input, hashing failure, or cancellation
    * before that swap preserves the complete previously published derived tree. It is intended to run at boot before the first proof is
    * built; production boot/disk wiring remains open under REC-004. `k` is the current k1 lag; this derived-state swap does not make the
    * recovery KV crash-atomic and does not yet reconstruct an exact-hash density replacement.
    */
  def replayFrom(k: Long, expectedEligible: SnapshotOrdinal): F[Unit]
}

object HistoricalCommitmentSmtStore {

  /** Wrap a recovery `MptStore[F, CommitmentKey]` and an internally owned, replaceable [[VersionedSmt]] as a
    * [[HistoricalCommitmentSmtStore]].
    *
    * The `k` cutoff is owned by the CALLER: [[appendAtFinality]] takes `(snapshotOrdinal, eligibleOrdinal)` explicitly, so the store is a
    * pure ordinal-keyed structure with no snapshot-lookup dependency. The caller (GSAM wiring) computes `eligibleOrdinal = snapshotOrdinal
    * − k` from the existing k1 lag and is responsible for passing the matching `k` to [[replayFrom]]. This ownership does not make the
    * resulting ordinal immutable.
    */
  def make[F[_]: Async: Hasher](
    durable: MptStore[F, CommitmentKey],
    versionRetention: Int
  ): F[HistoricalCommitmentSmtStore[F]] =
    for {
      initial <- VersionedSmt.make[F](versionRetention)
      current <- Ref.of[F, VersionedSmt[F]](initial)
    } yield {
      val logger = Slf4jLogger.getLoggerFromName[F]("HistoricalCommitmentSmtStore")

      new HistoricalCommitmentSmtStore[F] {

        def appendAtFinality(
          snapshotOrdinal: SnapshotOrdinal,
          eligibleOrdinal: SnapshotOrdinal,
          commitment: PerOrdinalCommitment
        ): F[SmtRoot] =
          durable.withExclusiveLock {
            for {
              commitHash <- PerOrdinalCommitment.commitmentHash[F](commitment)
              // Submit the recovery entry to the KV before deriving the SMT leaf. MptStore.commit currently launches detached
              // persistence (STOR-01), so this ordering is not a crash-durability receipt. Keep the KV and SMT value bytes identical so a
              // complete retained image replays to byte-identical leaves.
              _ <- durable.insert[Hash](CommitmentKey(eligibleOrdinal), commitHash)
              _ <- durable.commit(eligibleOrdinal)
              leafKey = CommitmentKey.toHex(eligibleOrdinal)
              leafValue = Hex(commitHash.value).toBytes
              versioned <- current.get
              root <- versioned.commit(snapshotOrdinal, Map(leafKey -> leafValue), Set.empty)
            } yield root
          }

        def rootForSnapshot(snapshotOrdinal: SnapshotOrdinal): F[Option[SmtRoot]] =
          durable.withExclusiveLock(current.get.flatMap(_.rootAt(snapshotOrdinal)))

        def proveAt(snapshotOrdinal: SnapshotOrdinal, targetOrdinal: SnapshotOrdinal): F[Either[SmtProofError, SmtProof]] =
          durable.withExclusiveLock(current.get.flatMap(_.proveAt(snapshotOrdinal, CommitmentKey.toHex(targetOrdinal))))

        def commitmentHashAt(ordinal: SnapshotOrdinal): F[Option[Hash]] =
          durable.withExclusiveLock(durable.get[Hash](CommitmentKey(ordinal)))

        def replayFrom(replayK: Long, expectedEligible: SnapshotOrdinal): F[Unit] =
          durable.withExclusiveLock {
            Async[F].uncancelable { poll =>
              poll {
                for {
                  _ <- Either.cond(replayK >= 0L, (), InvalidHistoricalReplayLag(replayK)).liftTo[F]
                  entries <- validatedDurableEntries
                  sorted = entries.sortBy(_._1.ordinal.value.value)
                  _ <- validateCompleteRange(sorted, expectedEligible).liftTo[F]
                  // Validate every derived version before building the replacement. The existing tree remains published while the fresh
                  // tree is built, so malformed input, hashing failure, or cancellation cannot expose a replay prefix.
                  ordered <- sorted.traverse {
                    case (key, commitHash) =>
                      val eligible = key.ordinal.value.value
                      val version = BigInt(eligible) + BigInt(replayK)
                      Either
                        .cond(
                          version <= BigInt(Long.MaxValue),
                          (SnapshotOrdinal(NonNegLong.unsafeFrom(version.longValue)), key.ordinal, commitHash),
                          MalformedDurableNipopowKey(
                            CommitmentKey.PartitionName,
                            CommitmentKey.toHex(key.ordinal),
                            s"eligible ordinal $eligible plus replay lag $replayK exceeds ${Long.MaxValue}"
                          )
                        )
                  }
                    .liftTo[F]
                  replacement <- VersionedSmt.make[F](versionRetention)
                  _ <- ordered.traverse_ {
                    case (version, eligibleOrdinal, commitHash) =>
                      val leafKey = CommitmentKey.toHex(eligibleOrdinal)
                      val leafValue = Hex(commitHash.value).toBytes
                      replacement.commit(version, Map(leafKey -> leafValue), Set.empty).void
                  }
                  _ <- logger.debug(s"[HistoricalCommitmentSmtStore] Prepared ${ordered.size} replayed commitments (k=$replayK)")
                } yield replacement
              }.flatMap(current.set)
            }
          }

        private def validatedDurableEntries: F[List[(CommitmentKey, Hash)]] =
          for {
            image <- durable.allEntriesAsBytes
            decoded <- CommitmentKey.decodeAll(image.keys).liftTo[F]
            entries <- decoded.traverse {
              case (physicalKey, logicalKey) =>
                StrictMptRead.fromStoredBytes[Hash](image(physicalKey)) match {
                  case StrictMptRead.Present(hash, _) => (logicalKey -> hash).pure[F]
                  case StrictMptRead.Malformed(reason, _) =>
                    MalformedDurableNipopowValue(CommitmentKey.PartitionName, physicalKey, reason)
                      .raiseError[F, (CommitmentKey, Hash)]
                  case StrictMptRead.Absent =>
                    DurableNipopowEnumerationChanged(CommitmentKey.PartitionName).raiseError[F, (CommitmentKey, Hash)]
                }
            }
          } yield entries

        private def validateCompleteRange(
          entries: List[(CommitmentKey, Hash)],
          expectedEligible: SnapshotOrdinal
        ): Either[IncompleteDurableNipopowHistory, Unit] = {
          val expectedCount = BigInt(expectedEligible.value.value) + 1
          val ordinalMismatch = entries.iterator.zipWithIndex.collectFirst {
            case ((key, _), index) if key.ordinal.value.value != index.toLong =>
              (index.toLong, key.ordinal.some)
          }
          val mismatch = ordinalMismatch.orElse {
            if (BigInt(entries.size) < expectedCount) Some(entries.size.toLong -> none[SnapshotOrdinal])
            else if (BigInt(entries.size) > expectedCount && expectedCount <= BigInt(Int.MaxValue)) {
              val index = expectedCount.toInt
              Some(index.toLong -> entries(index)._1.ordinal.some)
            } else None
          }

          Either.cond(
            BigInt(entries.size) == expectedCount && mismatch.isEmpty,
            (),
            IncompleteDurableNipopowHistory(
              CommitmentKey.PartitionName,
              expectedEligible,
              entries.size,
              mismatch.fold(expectedCount.min(BigInt(Long.MaxValue)).longValue)(_._1),
              mismatch.flatMap(_._2)
            )
          )
        }
      }
    }

  /** Build a fresh store with an in-memory recovery producer and in-memory versioned SMT. Current GL0 production wiring uses this
    * constructor, so the recovery image does not survive restart. A disk-backed producer can be substituted without changing this surface,
    * but production boot wiring and crash-atomic publication remain REC-004 and STOR-01 work.
    */
  def inMemory[F[_]: Async: Parallel: Hasher: JsonSerializer](
    versionRetention: Int
  ): F[HistoricalCommitmentSmtStore[F]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[F]()
      durable <- MptStore.make[F, CommitmentKey](producer, CommitmentKey.toHexF[F])
      store <- make[F](durable, versionRetention)
    } yield store

  /** A versioned SMT that retains ALL version-roots (no pruning). Convenience for tests that assert on old roots; production passes a
    * bounded `versionRetention`.
    */
  private[nipopow] val UnboundedVersionRetention: Int = Int.MaxValue
}
