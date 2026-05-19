package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.Logger

/** In-memory bridge over gl0's GSI lag for metagraph state-channel binaries. Two coupled concerns:
  *
  *   1. '''Orphan buffer.''' Holds wire bytes of binaries whose `lastSnapshotHash` (parent) the local GSI doesn't yet recognize. Drained
  *      when the parent binary itself becomes resolvable.
  *   2. '''Recent-admission cache.''' Holds `(metagraphAddress, valueHash) → metagraphOrdinal` for binaries we just admitted via the
  *      committee gate. Allows the resolver wrapper to answer "what's this binary's metagraph ordinal?" before the GSI catches up (which
  *      only happens on the next gl0 snapshot finalize, ~7s later).
  *
  * '''Why this exists (#213).''' At 8gl0+4mg+4shards the metagraph CL0 ("ml0") produces incremental binaries faster than gl0 can admit
  * them through the committee gate. Between the moment gl0 finalizes a metagraph's genesis binary into its global state and the moment the
  * first incremental binary lands at the resolver, ml0 may have produced several more incremental binaries chained off the first. The first
  * incremental's parent matches the genesis hash, but by the time gl0 sees binary ord=N the parent points to ord=(N-1), a binary gl0 never
  * recorded. `MetagraphParentOrdinalResolver` fail-closes on every such binary; the metagraph chain is then permanently stuck from gl0's
  * point of view.
  *
  * '''Design — orphan side.''' Keep a bounded in-memory map of unresolvable binaries keyed by the parent hash they reference. When a binary
  * IS admitted (`committeeGate.attestAndAdmit` returns true), the daemon calls `drainChildren(mg, acceptedValueHash)` to pull out any
  * orphans whose parent equals the just-accepted binary's value-hash, and re-enters them through the same handler. The chain unwinds in
  * order.
  *
  * '''Design — recent-admission side.''' Even after we admit binary X, X is only queued — it doesn't land in gl0's GSI until the next
  * global snapshot finalizes. So when we drain X's children and they re-query `MetagraphParentOrdinalResolver`, the resolver still returns
  * `None` and the children re-buffer. To break that, after admit we record `(mg, X.value.hash) → mgOrd_X` and have the resolver wrapper
  * check this map FIRST. The drained child's `parentOrdinalFor` lookup now returns the cached ord and proceeds. The cache stays valid for
  * the (typically ~7s) window until gl0 finalizes a snapshot containing X; after that the GSI has the same answer and the cache is
  * redundant. Bounded — old entries fall off.
  *
  * '''Bounded.''' We cap the total buffered entries at a small ceiling to prevent OOM under adversarial / runaway-producer conditions. When
  * the cap is hit, the oldest entry is evicted on insertion (FIFO order across all metagraphs). This is sufficient for the legitimate
  * gap-filling use case (small lag between gl0 and ml0); adversarial fan-out of unrelated parents falls off the back as new entries arrive.
  * The recent-admission cache uses the same cap independently.
  *
  * '''In-memory only.''' Survives across runtime but not restart. On restart gl0 re-bootstraps its GSI from the chain store; any orphans
  * outstanding at shutdown will be re-broadcast by ml0's sidecar durable outbox and re-buffered. Persisting the buffer would just duplicate
  * the sidecar's job.
  *
  * '''Hash identity.''' The orphan-buffer key and the admission-cache key both use the metagraph binary's *value* hash
  * (`Signed[StateChannelSnapshotBinary].toHashed.hash` — i.e. `signed.value.hash`, NOT `Hasher.hashBytes(wire_bytes)`). This matches both
  * writer-sides: gl0's `GlobalSnapshotAcceptanceManager` writes `nel.last.toHashed.map(_.hash)` into `lastStateChannelSnapshotHashes`
  * (`GlobalSnapshotAcceptanceManager.scala:908`), and ml0's `StateChannelSnapshotService` sets the next binary's `lastSnapshotHash` to the
  * prior accepted binary's `toHashed.hash` (`StateChannelSnapshotService.scala:112`). The wire-bytes hash used by the committee gate
  * aggregator is a different identity and is irrelevant here.
  *
  * '''Not a substitute for chain-sync.''' If gl0 misses binaries beyond what the sidecar outbox retains (large outages), an orphan buffer
  * alone cannot recover — a metagraph-binary fetch protocol is the longer-term fix. The buffer is the smallest sufficient mechanism for
  * the 4-mg scale race.
  *
  * '''Payload type.''' We store the raw `Array[Byte]` of the wire-level `pb.MetagraphBinary.binary` field plus the metagraph address (the
  * proto wrapper), not the parsed `Signed[StateChannelSnapshotBinary]`. This way re-drain hands the same byte sequence back to the
  * daemon's handler which already knows how to deserialize and process it; no separate "post-deserialize re-entry" code path.
  */
trait MetagraphOrphanBuffer[F[_]] {

  /** Buffer a binary whose parent hash isn't yet recognized by gl0. Returns the new buffer size (post-insertion, useful for metrics).
    * Idempotent: if the same `(metagraphAddress, parentHash, wireBytes)` triple is already buffered the buffer is unchanged and the
    * existing size is returned. This is load-bearing under the sidecar's durable outbox re-broadcast pattern.
    */
  def record(metagraphAddress: Address, parentHash: Hash, wireBytes: Array[Byte]): F[Int]

  /** Drain and return every buffered binary whose `(metagraphAddress, parentHash)` matches the just-accepted binary's value-hash, so the
    * caller can re-enter them through the handler. Removes the returned entries from the buffer. Returns wire-bytes; the caller
    * deserializes via the same path used for fresh gossip.
    */
  def drainChildren(metagraphAddress: Address, acceptedValueHash: Hash): F[List[Array[Byte]]]

  /** Total number of buffered orphan entries across all metagraphs. Test + metrics aid. */
  def size: F[Int]

  /** Record that the committee gate just admitted a binary with value-hash `valueHash` for metagraph `metagraphAddress`, at metagraph
    * ordinal `mgOrdinal`. Caller computes the value-hash as `signed.toHashed.map(_.hash)` and the ordinal as `parentOrdinal + 1`. The entry
    * remains in the cache until evicted by FIFO; correctness is unaffected because once gl0's GSI catches up, the resolver answers from
    * `lastStateChannelSnapshotHashes` directly.
    */
  def recordAdmission(metagraphAddress: Address, valueHash: Hash, mgOrdinal: Long): F[Unit]

  /** Look up a recently-admitted binary's metagraph ordinal. Returns `Some(mgOrdinal)` if we admitted a binary with
    * `(metagraphAddress, valueHash)` in the recent past. Used by the resolver wrapper to bridge the gap between admit (in-memory) and
    * the next gl0 snapshot finalize (GSI). When `None`, the caller falls through to the GSI-backed `MetagraphParentOrdinalResolver`.
    */
  def lookupAdmittedOrd(metagraphAddress: Address, valueHash: Hash): F[Option[Long]]

  /** Total number of cached admissions across all metagraphs. Test + metrics aid. */
  def admissionsSize: F[Int]
}

object MetagraphOrphanBuffer {

  /** Default cap on total buffered binaries. 256 is generous for the 4-mg scale (each metagraph might lag by a handful of binaries) and
    * small enough that worst-case memory is bounded (256 × ~10 KiB per binary ≈ 2.5 MiB). Overridable via `NAKAMOTO_ORPHAN_BUFFER_CAP`.
    */
  val DefaultCap: Int =
    sys.env.get("NAKAMOTO_ORPHAN_BUFFER_CAP").flatMap(_.toIntOption).getOrElse(256)

  /** Default cap on the recent-admission cache. Larger than the orphan cap because admissions persist for the ~7s GSI catch-up window
    * and we want headroom across all metagraphs during that window. 1024 entries × ~96 B (Address + Hash + Long + bookkeeping) ≈ 100 KB.
    * Overridable via `NAKAMOTO_RECENT_ADMIT_CAP`.
    */
  val DefaultAdmissionsCap: Int =
    sys.env.get("NAKAMOTO_RECENT_ADMIT_CAP").flatMap(_.toIntOption).getOrElse(1024)

  /** Buffer key. Keying on `(metagraphAddress, parentHash)` lets us drain all children of a freshly-accepted binary in one lookup. */
  private final case class Key(metagraphAddress: Address, parentHash: Hash)

  /** Internal entry. `seq` is a monotonic counter for FIFO eviction; `wireBytes` is the queued payload. We compare bytes by content
    * via `java.util.Arrays.equals` (Scala's `Array[Byte]` `==` is identity-only).
    */
  private final case class Entry(seq: Long, wireBytes: Array[Byte])

  /** Recent-admission entry. `seq` is monotonic for FIFO eviction; `mgOrdinal` is the value we return on lookup. */
  private final case class AdmissionEntry(seq: Long, mgOrdinal: Long)

  /** Build an in-memory buffer. */
  def make[F[_]: Async](
    logger0: Logger[F],
    cap: Int = DefaultCap,
    admissionsCap: Int = DefaultAdmissionsCap
  ): F[MetagraphOrphanBuffer[F]] =
    for {
      stateRef <- Ref.of[F, Map[Key, List[Entry]]](Map.empty)
      seqRef <- Ref.of[F, Long](0L)
      admissionsRef <- Ref.of[F, Map[(Address, Hash), AdmissionEntry]](Map.empty)
      admissionsSeqRef <- Ref.of[F, Long](0L)
    } yield new MetagraphOrphanBuffer[F] {

      private val logger: Logger[F] = logger0

      def record(metagraphAddress: Address, parentHash: Hash, wireBytes: Array[Byte]): F[Int] =
        for {
          nextSeq <- seqRef.updateAndGet(_ + 1L)
          newSize <- stateRef.modify { state =>
            val key = Key(metagraphAddress, parentHash)
            val existing = state.getOrElse(key, Nil)
            if (existing.exists(e => java.util.Arrays.equals(e.wireBytes, wireBytes)))
              (state, totalSize(state))
            else {
              val withNew = state.updated(key, Entry(nextSeq, wireBytes) :: existing)
              val sized = enforceCap(withNew, cap)
              (sized, totalSize(sized))
            }
          }
          _ <- logger.debug(
            s"📦 orphan-buffer: recorded mg=$metagraphAddress parent=${parentHash.value.take(12)}... bufferSize=$newSize"
          )
        } yield newSize

      def drainChildren(metagraphAddress: Address, acceptedValueHash: Hash): F[List[Array[Byte]]] =
        stateRef
          .modify { state =>
            val key = Key(metagraphAddress, acceptedValueHash)
            state.get(key) match {
              case None => (state, Nil)
              case Some(entries) =>
                // Drain in insertion order: entries were prepended (newest first), so reverse for chronological replay.
                val drained = entries.reverse.map(_.wireBytes)
                (state - key, drained)
            }
          }
          .flatTap { drained =>
            if (drained.isEmpty) Async[F].unit
            else
              logger.info(
                s"📤 orphan-buffer: draining ${drained.length} child binary(s) for mg=$metagraphAddress accepted=${acceptedValueHash.value
                    .take(12)}..."
              )
          }

      def size: F[Int] = stateRef.get.map(totalSize)

      def recordAdmission(metagraphAddress: Address, valueHash: Hash, mgOrdinal: Long): F[Unit] =
        for {
          nextSeq <- admissionsSeqRef.updateAndGet(_ + 1L)
          _ <- admissionsRef.update { admissions =>
            val withNew = admissions.updated((metagraphAddress, valueHash), AdmissionEntry(nextSeq, mgOrdinal))
            enforceAdmissionsCap(withNew, admissionsCap)
          }
          _ <- logger.debug(
            s"📌 orphan-buffer: cached admission mg=$metagraphAddress valueHash=${valueHash.value.take(12)}... mgOrd=$mgOrdinal"
          )
        } yield ()

      def lookupAdmittedOrd(metagraphAddress: Address, valueHash: Hash): F[Option[Long]] =
        admissionsRef.get.map(_.get((metagraphAddress, valueHash)).map(_.mgOrdinal))

      def admissionsSize: F[Int] = admissionsRef.get.map(_.size)
    }

  /** Total entry count across all keys. Linear in number of keys; cheap given the small cap. */
  private def totalSize(state: Map[Key, List[Entry]]): Int =
    state.values.foldLeft(0)((acc, entries) => acc + entries.length)

  /** Enforce the cap by evicting the oldest entry (lowest seq) across the entire buffer. Repeats until size ≤ cap. Linear search across
    * keys per eviction; that's O(keys × evictions) which under bounded-cap conditions is bounded. We avoid a heap for simplicity given the
    * cap is small (default 256).
    */
  private def enforceCap(state: Map[Key, List[Entry]], cap: Int): Map[Key, List[Entry]] = {
    @scala.annotation.tailrec
    def loop(s: Map[Key, List[Entry]]): Map[Key, List[Entry]] =
      if (totalSize(s) <= cap) s
      else {
        val oldest = s.iterator.flatMap { case (k, es) => es.map(e => (k, e)) }.minByOption(_._2.seq)
        oldest match {
          case None => s
          case Some((key, entry)) =>
            val remaining = s(key).filterNot(_.seq == entry.seq)
            val updated = if (remaining.isEmpty) s - key else s.updated(key, remaining)
            loop(updated)
        }
      }
    loop(state)
  }

  /** FIFO eviction on the recent-admissions map. Drops the oldest (lowest seq) entries until size ≤ cap. */
  private def enforceAdmissionsCap(
    admissions: Map[(Address, Hash), AdmissionEntry],
    cap: Int
  ): Map[(Address, Hash), AdmissionEntry] =
    if (admissions.size <= cap) admissions
    else {
      val sortedByAge = admissions.toList.sortBy(_._2.seq)
      sortedByAge.drop(admissions.size - cap).toMap
    }
}
