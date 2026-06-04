package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash

import org.typelevel.log4cats.Logger

/** In-memory bridge over gl0's GSI lag for metagraph state-channel binaries. Three coupled concerns:
  *
  *   1. '''Orphan buffer.''' Holds wire bytes of binaries whose `lastSnapshotHash` (parent) the local GSI doesn't yet recognize. Drained
  *      when the parent binary itself becomes resolvable. 2. '''Recent-admission cache.''' Holds `(metagraphAddress, valueHash) →
  *      metagraphOrdinal` for binaries we just admitted via the committee gate. Allows the resolver wrapper to answer "what's this binary's
  *      metagraph ordinal?" before the GSI catches up (which only happens on the next gl0 snapshot finalize, ~7s later). 3. '''Pending
  *      parent-ordinal cache.''' Holds `(metagraphAddress, wireHash) → parentOrdinal` for binaries this node has RESOLVED (whether or not
  *      it then admitted them), keyed on the wire-bytes hash. Lets the committee-attestation receiver recover a binary's eta even when the
  *      binary is in-flight (parent at the tip → never buffered → invisible to the orphan-buffer scan) — without it the committee threshold
  *      is reached zero times and metagraph chains freeze at genesis. See [[recordPendingParentOrdinal]].
  *
  * '''Why this exists (#213).''' At 8gl0+4mg+4shards the metagraph CL0 ("ml0") produces incremental binaries faster than gl0 can admit them
  * through the committee gate. Between the moment gl0 finalizes a metagraph's genesis binary into its global state and the moment the first
  * incremental binary lands at the resolver, ml0 may have produced several more incremental binaries chained off the first. The first
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
  * alone cannot recover — a metagraph-binary fetch protocol is the longer-term fix. The buffer is the smallest sufficient mechanism for the
  * 4-mg scale race.
  *
  * '''Payload type.''' We store the raw `Array[Byte]` of the wire-level `pb.MetagraphBinary.binary` field plus the metagraph address (the
  * proto wrapper), not the parsed `Signed[StateChannelSnapshotBinary]`. This way re-drain hands the same byte sequence back to the daemon's
  * handler which already knows how to deserialize and process it; no separate "post-deserialize re-entry" code path.
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

  /** The distinct `(metagraphAddress, parentHash)` keys currently buffered — i.e. the set of parent hashes this node is waiting on. Used by
    * the daemon's stuck-detection tick (#259): a parent that persists across multiple ticks is one the local committee gate never admitted
    * and the orphan-drain never resolved, so the daemon actively PULLS the missing binary by hash from a peer over ChainSync. Read-only.
    */
  def listPendingParents: F[List[(Address, Hash)]]

  /** NON-DESTRUCTIVELY scan the buffered wire bytes for `metagraphAddress` and return the FIRST entry whose *value*-hash equals
    * `valueHash`, or `None`. The caller supplies `valueHashOf` — a deserialize-then-hash function (the serve handler passes
    * `JsonSerializer.deserialize` + `Signed.toHashed.map(_.hash)`) — so this module stays free of the heavy JSON/Hasher dependencies and
    * the hash identity is computed by exactly the gossip-path code. Used by the ChainSync serve handler (#259) to answer a peer's
    * FetchMetagraphBinaries by value-hash when the binary is sitting in our orphan buffer but not yet in a finalized snapshot.
    *
    * '''Non-destructive (HARD requirement).''' Serving MUST NEVER mutate the buffer — unlike `drainChildren`, this does not remove the
    * matched entry. A peer fetching a binary does not change our local admission state.
    *
    * '''Bounded scan.''' Only `metagraphAddress`'s entries are scanned, and the buffer is capped (`DefaultCap`), so the deserialize cost is
    * bounded. Returns on the first match (short-circuits the effectful scan).
    *
    * '''Value-hash identity (#259 correction D).''' `valueHash` and the hashes produced by `valueHashOf` are *value*-hashes
    * (`Signed[StateChannelSnapshotBinary].toHashed.hash` = `signed.value.hash`), matching the orphan-buffer key convention and gl0's
    * `lastStateChannelSnapshotHashes` — NOT wire-bytes digests.
    */
  def peekForValueHash(metagraphAddress: Address, valueHash: Hash)(
    valueHashOf: Array[Byte] => F[Option[Hash]]
  ): F[Option[Array[Byte]]]

  /** NON-DESTRUCTIVELY scan the buffered wire bytes for `metagraphAddress` and return the FIRST entry whose *wire-bytes* hash equals
    * `wireHash`, or `None`. Mirrors [[peekForValueHash]] but keys on the WIRE-BYTES digest (`Hasher.hashBytes(wireBytes)`), the identity
    * the committee-gate aggregator + `pb.MetagraphAttestation.binaryHash` use — NOT the value-hash. The caller supplies `wireHashOf` (a
    * `Hasher.hashBytes` callback) so this module stays free of the heavy Hasher dependency and the hash identity is computed by exactly the
    * gossip/gate-path code.
    *
    * '''Why this exists (#213/#290 receiver-side content lookup).''' The metagraph-attestation receiver
    * (`NakamotoSyncDaemon.handleMetagraphAttestation`) must resolve the metagraph parent ordinal from the ATTESTED BINARY's OWN content
    * (deterministic across peers), but the wire attestation carries only hashes, not the binary bytes. The attested binary — whose
    * `lastSnapshotHash == att.parentHash` and whose wire digest == `att.binaryHash` — is sitting in this orphan buffer (it arrived via
    * gossip but gl0 could not yet admit it: the exact deadlock state). This peek retrieves that binary's bytes by its wire digest so the
    * receiver can derive the parent ordinal (and thus the committee-VRF eta) from content. If the binary has NOT arrived yet (attestation
    * raced ahead of it), this returns `None` and the receiver fails closed — never trusting the sender's claimed ordinal.
    *
    * '''Non-destructive (HARD requirement).''' Peeking MUST NEVER mutate the buffer — recording an attestation does not change our local
    * admission state. '''Bounded scan''' over only `metagraphAddress`'s entries; returns on the first match (short-circuits the effectful
    * scan).
    */
  def peekForWireHash(metagraphAddress: Address, wireHash: Hash)(
    wireHashOf: Array[Byte] => F[Option[Hash]]
  ): F[Option[Array[Byte]]]

  /** Record that the committee gate just admitted a binary with value-hash `valueHash` for metagraph `metagraphAddress`, at metagraph
    * ordinal `mgOrdinal`. Caller computes the value-hash as `signed.toHashed.map(_.hash)` and the ordinal as `parentOrdinal + 1`. The entry
    * remains in the cache until evicted by FIFO; correctness is unaffected because once gl0's GSI catches up, the resolver answers from
    * `lastStateChannelSnapshotHashes` directly.
    */
  def recordAdmission(metagraphAddress: Address, valueHash: Hash, mgOrdinal: Long): F[Unit]

  /** Look up a recently-admitted binary's metagraph ordinal. Returns `Some(mgOrdinal)` if we admitted a binary with `(metagraphAddress,
    * valueHash)` in the recent past. Used by the resolver wrapper to bridge the gap between admit (in-memory) and the next gl0 snapshot
    * finalize (GSI). When `None`, the caller falls through to the GSI-backed `MetagraphParentOrdinalResolver`.
    */
  def lookupAdmittedOrd(metagraphAddress: Address, valueHash: Hash): F[Option[Long]]

  /** Total number of cached admissions across all metagraphs. Test + metrics aid. */
  def admissionsSize: F[Int]

  /** Record the parent ordinal THIS node resolved for a metagraph binary, keyed by the binary's WIRE-bytes hash
    * (`Hasher.hashBytes(wireBytes)` == `pb.MetagraphAttestation.binaryHash`), NOT its value-hash. Written by the daemon in `processBytes`
    * the instant it resolves a binary's parent ordinal (before `attestAndAdmit`).
    *
    * '''Why this exists (#213/#290 — committee-threshold liveness).''' A binary being attested has its parent AT THE TIP, so it is NOT
    * buffered — it is in-flight through `attestAndAdmit`. The receiver (`handleMetagraphAttestation`) derives the committee-VRF eta from
    * the binary's parent ordinal, and looked it up ONLY via `peekForWireHash` (an orphan-buffer scan), which by construction cannot see an
    * in-flight (non-buffered) binary. So every peer attested next-in-line binaries that no peer could resolve on the receive path → eta
    * undrivable → attestation dropped → the committee threshold was reached ZERO times → metagraph chains froze at genesis. Caching the
    * parent ordinal here at resolve-time (when this node processes the same gossiped binary) lets the receiver recover it by
    * `att.binaryHash`. Self-bootstrapping: the first incremental resolves via the tip-identity guard, admits, and `recordAdmission` then
    * carries the chain.
    *
    * Keyed on the WIRE hash to match the attestation wire field; bounded FIFO, sharing the admission cache's cap.
    */
  def recordPendingParentOrdinal(metagraphAddress: Address, wireHash: Hash, parentOrdinal: Long): F[Unit]

  /** Look up the parent ordinal cached by [[recordPendingParentOrdinal]] for the binary whose wire-bytes hash is `wireHash` (==
    * `att.binaryHash`). `Some` once this node has processed the binary (the common case — the binary gossips ahead of its attestations);
    * the receiver computes the committee-VRF eta from it. `None` falls through to the buffered-content path (`peekForWireHash`).
    */
  def lookupPendingParentOrdinal(metagraphAddress: Address, wireHash: Hash): F[Option[Long]]
}

object MetagraphOrphanBuffer {

  /** Fallback cap on total buffered binaries, used only when [[make]] is called without an explicit `cap` (tests). Production threads the
    * typed `nakamoto.orphan-buffer-cap` HOCON value (default 1024) through `GlobalSnapshotConsensus` — see
    * `SharedConfig.nakamoto.orphanBufferCap` (overridable via the `${?NAKAMOTO_ORPHAN_BUFFER_CAP}` substitution in `application.conf`).
    * #259 defense-in-depth: under larger outages / GossipSub churn a node can fall behind by more than a handful of binaries on a
    * metagraph, and the orphan buffer must hold enough of the chain for the active-recovery fetch to walk it back. Worst-case memory is
    * still bounded (1024 × ~10 KiB per binary ≈ 10 MiB) and only the in-flight backlog, not steady-state.
    */
  val DefaultCap: Int = 1024

  /** Fallback cap on the recent-admission cache, used only when [[make]] is called without an explicit `admissionsCap` (tests). Production
    * threads the typed `nakamoto.recent-admit-cap` HOCON value (default 4096) — see `SharedConfig.nakamoto.recentAdmitCap` (overridable via
    * `${?NAKAMOTO_RECENT_ADMIT_CAP}`). Kept proportionally larger than the orphan cap (4×, the original 1024/256 ratio → 4096) because
    * admissions persist for the ~7s GSI catch-up window and we want headroom across all metagraphs during that window, especially when
    * active recovery replays a backlog of buffered binaries in a burst. 4096 entries × ~96 B (Address + Hash + Long + bookkeeping) ≈ 400
    * KB.
    */
  val DefaultAdmissionsCap: Int = 4096

  /** Buffer key. Keying on `(metagraphAddress, parentHash)` lets us drain all children of a freshly-accepted binary in one lookup. */
  private final case class Key(metagraphAddress: Address, parentHash: Hash)

  /** Internal entry. `seq` is a monotonic counter for FIFO eviction; `wireBytes` is the queued payload. We compare bytes by content via
    * `java.util.Arrays.equals` (Scala's `Array[Byte]` `==` is identity-only).
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
      // #213/#290 liveness: (wireHash → parentOrdinal) for binaries this node has resolved, so inbound
      // committee attestations for in-flight (non-buffered) binaries can recover the eta. Same shape +
      // cap as the admission cache, keyed on the wire-bytes hash instead of the value hash.
      pendingParentOrdinalRef <- Ref.of[F, Map[(Address, Hash), AdmissionEntry]](Map.empty)
      pendingParentSeqRef <- Ref.of[F, Long](0L)
    } yield
      new MetagraphOrphanBuffer[F] {

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
          stateRef.modify { state =>
            val key = Key(metagraphAddress, acceptedValueHash)
            state.get(key) match {
              case None          => (state, Nil)
              case Some(entries) =>
                // Drain in insertion order: entries were prepended (newest first), so reverse for chronological replay.
                val drained = entries.reverse.map(_.wireBytes)
                (state - key, drained)
            }
          }.flatTap { drained =>
            if (drained.isEmpty) Async[F].unit
            else
              logger.info(
                s"📤 orphan-buffer: draining ${drained.length} child binary(s) for mg=$metagraphAddress accepted=${acceptedValueHash.value
                    .take(12)}..."
              )
          }

        def size: F[Int] = stateRef.get.map(totalSize)

        def listPendingParents: F[List[(Address, Hash)]] =
          stateRef.get.map { state =>
            // Only keys with at least one buffered entry are "pending". `enforceCap`/`drainChildren`
            // remove emptied keys, so in practice every key is non-empty — guard anyway for safety.
            // NB: iterate via `.iterator` BEFORE collecting to a List. Calling `.collect` directly on a
            // `Map` whose partial function returns a 2-tuple rebuilds a `Map`, silently collapsing keys
            // that share the same metagraph Address (e.g. two distinct parent hashes under one mg) — a
            // single mg with multiple stuck parents would then be under-reported. The iterator path
            // preserves every distinct `(Address, Hash)`.
            state.iterator.collect {
              case (key, entries) if entries.nonEmpty => (key.metagraphAddress, key.parentHash)
            }.toList
          }

        def peekForValueHash(metagraphAddress: Address, valueHash: Hash)(
          valueHashOf: Array[Byte] => F[Option[Hash]]
        ): F[Option[Array[Byte]]] =
          stateRef.get.flatMap { state =>
            // Gather only this metagraph's buffered wire bytes (bounded by the cap). Insertion order is
            // newest-first within a key; the scan returns on the first value-hash match regardless of order.
            val candidates: List[Array[Byte]] =
              state.iterator.collect {
                case (key, entries) if key.metagraphAddress === metagraphAddress => entries.map(_.wireBytes)
              }.flatten.toList

            // Effectful short-circuiting scan: deserialize+hash each candidate, stop at the first whose
            // value-hash matches. NON-DESTRUCTIVE — `stateRef` is read-only here. No `unsafeRunSync`.
            candidates.collectFirstSomeM { bytes =>
              valueHashOf(bytes).map {
                case Some(h) if h === valueHash => Some(bytes)
                case _                          => None
              }
            }
          }

        def peekForWireHash(metagraphAddress: Address, wireHash: Hash)(
          wireHashOf: Array[Byte] => F[Option[Hash]]
        ): F[Option[Array[Byte]]] =
          stateRef.get.flatMap { state =>
            // Gather only this metagraph's buffered wire bytes (bounded by the cap). The scan returns
            // on the first wire-digest match. NON-DESTRUCTIVE — `stateRef` is read-only here.
            val candidates: List[Array[Byte]] =
              state.iterator.collect {
                case (key, entries) if key.metagraphAddress === metagraphAddress => entries.map(_.wireBytes)
              }.flatten.toList

            candidates.collectFirstSomeM { bytes =>
              wireHashOf(bytes).map {
                case Some(h) if h === wireHash => Some(bytes)
                case _                         => None
              }
            }
          }

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

        def recordPendingParentOrdinal(metagraphAddress: Address, wireHash: Hash, parentOrdinal: Long): F[Unit] =
          for {
            nextSeq <- pendingParentSeqRef.updateAndGet(_ + 1L)
            _ <- pendingParentOrdinalRef.update { pending =>
              // `AdmissionEntry.mgOrdinal` carries the PARENT ordinal here (same (seq, Long) shape, different semantics).
              val withNew = pending.updated((metagraphAddress, wireHash), AdmissionEntry(nextSeq, parentOrdinal))
              enforceAdmissionsCap(withNew, admissionsCap)
            }
          } yield ()

        def lookupPendingParentOrdinal(metagraphAddress: Address, wireHash: Hash): F[Option[Long]] =
          pendingParentOrdinalRef.get.map(_.get((metagraphAddress, wireHash)).map(_.mgOrdinal))
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
