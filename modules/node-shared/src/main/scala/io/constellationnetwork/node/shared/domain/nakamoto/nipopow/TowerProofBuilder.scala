package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** §3 NIPoPoW S4.2 — builds a [[TowerProof]] from a local [[TowerStore]] + [[SnapshotStorage]].
  *
  * '''Algorithm''' (per `docs/nakamoto/NIPOPOW-IMPLEMENTATION-PLAN.md` §S4):
  *   1. Read the local tip ordinal from the snapshot storage. 2. Collect the L0 suffix: the `k` most-recent snapshots `[tip-k+1, tip]`
  *      ordered ascending. 3. For each super-level µ ∈ {1..9}, call `store.entriesAtLevel(µ, since)` → fetch each entry's snapshot from
  *      [[SnapshotStorage]] → extract the slot certificate → assemble [[TowerProofHeader]]. 4. Pack everything into a [[TowerProof]].
  *
  * '''Behavior under missing data''':
  *   - Snapshots without a `slotCertificate` (pre-activation) are silently skipped — they contributed no L0/level-µ hits. The level-chain
  *     simply has a shorter prefix in that case; downstream verifier accepts a shorter chain.
  *   - Snapshots present in the tower but missing from storage (storage gap, e.g. partial backfill) are also skipped with a debug log line.
  *     The verifier sees a shorter chain in that level; density check may flag this.
  *
  * '''Determinism + thread safety''': pure-read against immutable inputs (the tower store and snapshot storage are read-only from this
  * call's perspective). Calls from concurrent fibers produce independent proofs; no shared mutable state in the builder itself.
  */
trait TowerProofBuilder[F[_]] {

  /** Build a proof anchored at `since`. The `since` ordinal is inclusive — entries with `ordinal == since` are included.
    *
    * Default `since = SnapshotOrdinal.MinValue` (= 0) anchors at genesis; typical light-client invocation provides a recent finalized
    * checkpoint ordinal to keep the proof small.
    *
    * `k` is the L0 suffix length; defaults to [[TowerProof.DefaultSuffixLength]].
    */
  def build(since: SnapshotOrdinal, k: Int): F[TowerProof]

  /** Convenience overload — anchors at genesis with default suffix length. */
  def buildFromGenesis: F[TowerProof] =
    build(SnapshotOrdinal.MinValue, TowerProof.DefaultSuffixLength)
}

object TowerProofBuilder {

  /** Construct a builder backed by a [[TowerStore]] and a [[SnapshotStorage]] for the global chain.
    *
    * @param tower
    *   read source for level-µ entries (one per super-level).
    * @param snapshotStorage
    *   read source for the actual `Signed[GlobalIncrementalSnapshot]` payloads — needed for slot certificate extraction.
    */
  def make[F[_]: Async](
    tower: TowerStore[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo]
  ): TowerProofBuilder[F] = new TowerProofBuilder[F] {

    private val logger = Slf4jLogger.getLoggerFromName[F]("TowerProofBuilder")

    /** Convert a [[Signed]]`[`[[GlobalIncrementalSnapshot]]`]` + its `eta` field + finalized hash into a [[TowerProofHeader]]. Returns
      * `None` for pre-activation snapshots (no `slotCertificate`).
      */
    private def toHeader(
      signed: Signed[GlobalIncrementalSnapshot],
      snapshotHash: Hash
    ): Option[TowerProofHeader] = {
      val s = signed.value
      val signers = signed.proofs.toNonEmptyList.map(_.id.toPeerId).distinct
      // A Nakamoto snapshot has one producer. Without that unique identity a portable
      // verifier cannot resolve the certificate key from the operator registry.
      s.slotCertificate.filter(_ => signers.size == 1).map { cert =>
        TowerProofHeader(
          ordinal = s.ordinal,
          producerId = signers.head,
          slot = cert.slot,
          parentSlot = cert.parentSlot,
          vrfProof = cert.vrfProof,
          vrfOutput = cert.vrfOutput,
          vrfPublicKey = cert.vrfPublicKey,
          eta = cert.eta,
          activePoolSize = cert.activePoolSize,
          subchainLevelCounts = cert.subchainLevelCounts,
          snapshotHash = snapshotHash
        )
      }
    }

    /** Fetch a snapshot at `ord` and lift it into a [[TowerProofHeader]], injecting the canonical snapshot hash directly (known from either
      * the tower entry or the successor's `lastSnapshotHash`). Returns `None` if storage doesn't have the snapshot, or if it has no
      * `slotCertificate`.
      */
    private def fetchHeaderWithHash(
      ord: SnapshotOrdinal,
      snapshotHash: Hash
    ): F[Option[TowerProofHeader]] =
      snapshotStorage.get(ord).map {
        case None         => None
        case Some(signed) => toHeader(signed, snapshotHash)
      }

    /** Fetch L0 suffix entries — walks backwards from `tip` to `tip - k + 1`. We need the canonical content-address for each suffix
      * snapshot; recomputing via the JSON `Hasher[F]` would add a typeclass dep — instead we look up `lastSnapshotHash` of `ord+1` to learn
      * `ord`'s hash. For the tip itself, we use the snapshot's `lastSnapshotHash` slot from `tip+1` if present, falling back to
      * `Hash.empty` (verifier ignores L0 suffix hash since the suffix isn't tower-anchored).
      */
    private def collectL0Suffix(tip: SnapshotOrdinal, k: Int): F[Vector[TowerProofHeader]] = {
      val tipValue = tip.value.value
      val firstValue = math.max(0L, tipValue - k.toLong + 1L)
      val ordinals = (firstValue to tipValue).toVector.flatMap(v => NonNegLong.from(v).toOption.map(SnapshotOrdinal(_)))
      ordinals.traverse(fetchSuffixHeader).map(_.flatten)
    }

    /** For an L0 suffix snapshot, look up the snapshot's true content-address by reading `lastSnapshotHash` from the SUCCESSOR snapshot
      * (ord + 1). If no successor exists (we're at the tip), we use `Hash.empty` as a benign sentinel — the verifier doesn't
      * cross-reference the tip's hash against a tower entry (it's not a tower entry by construction; it's the leading L0 suffix item).
      */
    private def fetchSuffixHeader(ord: SnapshotOrdinal): F[Option[TowerProofHeader]] =
      snapshotStorage.get(ord).flatMap {
        case None => Async[F].pure(None: Option[TowerProofHeader])
        case Some(signed) =>
          for {
            successor <- NonNegLong
              .from(ord.value.value + 1L)
              .toOption
              .map(SnapshotOrdinal(_))
              .traverse(snapshotStorage.get)
              .map(_.flatten)
            // Successor.lastSnapshotHash is `ord`'s content-address — the same Hash that gets stored
            // by TowerFinalizer when this ordinal was finalized. If no successor, fall back to empty.
            snapshotHash = successor
              .map(_.value.lastSnapshotHash)
              .getOrElse(Hash.empty)
          } yield toHeader(signed, snapshotHash)
      }

    /** Materialize a single level's chain — for each tower entry at level µ at-or-after `since`, look up the snapshot and build a header.
      */
    private def buildLevelChain(level: Int, since: SnapshotOrdinal): F[Vector[TowerProofHeader]] =
      for {
        entries <- tower.entriesAtLevel(level, since)
        headers <- entries.traverse(e => fetchHeaderWithHash(e.ordinal, e.snapshotHash))
      } yield headers.flatten.toVector

    override def build(since: SnapshotOrdinal, k: Int): F[TowerProof] = {
      val effectiveK = math.max(1, k)
      snapshotStorage.head.flatMap {
        case None =>
          // No tip yet — return the canonical empty proof.
          logger.debug("[TowerProofBuilder] No snapshot head — returning empty proof").as(TowerProof.Empty)
        case Some((signed, _)) =>
          val tip = signed.value.ordinal
          for {
            l0Suffix <- collectL0Suffix(tip, effectiveK)
            chains <- (1 to SuperLevelParams.SuperLevelCount).toList.traverse { µ =>
              buildLevelChain(µ, since).map(c => µ -> c)
            }
            // Filter out empty chains for cleanliness; verifier treats absent levels as zero hits since `since`.
            nonEmpty: SortedMap[Int, Vector[TowerProofHeader]] = SortedMap.from(chains.filter(_._2.nonEmpty))
            // Defensive sort: builder produces ascending order naturally, but pin the contract.
            sortedSuffix = l0Suffix.sortBy(_.ordinal.value.value)
            sortedChains = nonEmpty.view.mapValues(_.sortBy(_.ordinal.value.value)).to(SortedMap)
          } yield TowerProof(since, tip, sortedSuffix, sortedChains.toMap)
      }
    }
  }

}
