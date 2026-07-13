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
  *      binary-context cache.''' Holds `(metagraphAddress, wireHash) → CurrencyBinaryContext` for resolved binaries. The receiver rechecks
  *      the exact signed GL0 anchor as current Phase 2 before deriving eta or a KES period; the metagraph ordinal is only ML0 continuity.
  *      See [[recordPendingBinaryContext]].
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
  * `None` and the children re-buffer. To break that, the processor records `(mg, X.value.hash) -> mgOrd_X` after resolving X's parent and
  * checks this map first for a child. The child still has to carry and pass an exact Phase-2 GL0 anchor check; this cache establishes only
  * ML0 continuity. The entry becomes redundant once GL0 state records X and eventually falls off the bounded cache.
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

  /** Record the signed currency context THIS node resolved for a metagraph binary, keyed by the binary's WIRE-bytes hash
    * (`Hasher.hashBytes(wireBytes)` == `pb.MetagraphAttestation.binaryHash`), NOT its value-hash. Written before `attestAndAdmit`.
    *
    * '''Why this exists (#213/#290 — committee-threshold liveness).''' A binary being attested has its parent AT THE TIP, so it is NOT
    * buffered — it is in-flight through `attestAndAdmit`. Caching the complete signed context lets the receiver recheck the exact GL0
    * anchor and derive byte-identical eta/key-period inputs without trusting the sender or a node-local head.
    *
    * Keyed on the WIRE hash to match the attestation wire field; bounded FIFO, sharing the admission cache's cap.
    */
  def recordPendingBinaryContext(
    metagraphAddress: Address,
    wireHash: Hash,
    context: MetagraphParentOrdinalResolver.CurrencyBinaryContext
  ): F[Unit]

  /** Look up the signed context cached by [[recordPendingBinaryContext]] for the binary whose wire-bytes hash is `wireHash`. The caller
    * must recheck its exact GL0 anchor as current Phase 2; cache residence is never finality authority.
    */
  def lookupPendingBinaryContext(
    metagraphAddress: Address,
    wireHash: Hash
  ): F[Option[MetagraphParentOrdinalResolver.CurrencyBinaryContext]]

  /** Phase-0 of verify-on-attach (#29). Buffer an inbound committee attestation UN-VERIFIED, keyed by the binary's WIRE hash
    * (`att.binaryHash`). Held while the attested binary is still an orphan — its parent not yet admitted to the canonical chain, so the
    * binary sits on a tine this node is not (yet) attached to. We deliberately do NOT spend Ed25519+KES+VRF here: during an eta-rotation
    * fork storm most competing tines get evicted, and verifying their attestations speculatively is the CPU sink that starved gl0 producers
    * off the air. When the binary ATTACHES — drains from the orphan buffer and enters the `resolveParent == Some` admit path —
    * [[drainAttestations]] releases these for verification (Phase 1), just before the committee threshold check. Idempotent per
    * `senderPeerId`: a gossip re-delivery of the same sender's attestation is not re-buffered. Bounded FIFO across all in-flight binaries.
    */
  def bufferAttestation(
    metagraphAddress: Address,
    binaryHash: Hash,
    att: MetagraphCommitteeGate.IncomingAttestation
  ): F[Unit]

  /** Phase-1 of verify-on-attach (#29). Remove and return (chronological order) the attestations buffered against `(metagraphAddress,
    * binaryHash)`. Called at the attach point — the binary's `resolveParent == Some` admit path — so the now-canonical binary's
    * attestations are verified + tallied just before the committee threshold gate. Empty if none were buffered (the common in-flight case,
    * where attestations arrive after the binary and take the eager-verify Tier-1 path instead).
    */
  def drainAttestations(
    metagraphAddress: Address,
    binaryHash: Hash
  ): F[List[MetagraphCommitteeGate.IncomingAttestation]]

  /** Diagnostic: total Phase-0 (un-verified) attestations currently buffered across all binaries. */
  def attestationsBufferSize: F[Int]
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

  /** Cap on Phase-0 buffered (un-verified) attestations across all in-flight orphan binaries (#29). Each orphan can accrue up to the
    * committee size in attestations before it attaches; at the orphan cap × ~16-member committees this stays a few thousand. Bounded-FIFO
    * like the binary buffer (oldest evicted on overflow). 8192 × ~320 B (PeerId + 2 hashes + 3 sig byte-arrays + bookkeeping) ≈ 2.6 MB
    * worst case. Tests use the default; production can thread an explicit value through [[make]] like the other caps.
    */
  val DefaultAttestationsCap: Int = 8192

  /** Buffer key. Keying on `(metagraphAddress, parentHash)` lets us drain all children of a freshly-accepted binary in one lookup. */
  private final case class Key(metagraphAddress: Address, parentHash: Hash)

  /** Internal entry. `seq` is a monotonic counter for FIFO eviction; `wireBytes` is the queued payload. We compare bytes by content via
    * `java.util.Arrays.equals` (Scala's `Array[Byte]` `==` is identity-only).
    */
  private final case class Entry(seq: Long, wireBytes: Array[Byte])

  /** Recent-admission entry. `seq` is monotonic for FIFO eviction; `mgOrdinal` is the value we return on lookup. */
  private final case class AdmissionEntry(seq: Long, mgOrdinal: Long)

  private final case class PendingContextEntry(
    seq: Long,
    context: MetagraphParentOrdinalResolver.CurrencyBinaryContext
  )

  /** Phase-0 buffered-attestation entry (#29). `seq` is monotonic for FIFO eviction; `att` is the un-verified inbound attestation. */
  private final case class AttestationEntry(seq: Long, att: MetagraphCommitteeGate.IncomingAttestation)

  /** Build an in-memory buffer. */
  def make[F[_]: Async](
    logger0: Logger[F],
    cap: Int = DefaultCap,
    admissionsCap: Int = DefaultAdmissionsCap,
    attestationsCap: Int = DefaultAttestationsCap
  ): F[MetagraphOrphanBuffer[F]] =
    for {
      stateRef <- Ref.of[F, Map[Key, List[Entry]]](Map.empty)
      seqRef <- Ref.of[F, Long](0L)
      admissionsRef <- Ref.of[F, Map[(Address, Hash), AdmissionEntry]](Map.empty)
      admissionsSeqRef <- Ref.of[F, Long](0L)
      // In-flight signed currency context, keyed on wire hash. Exact Phase-2 status is rechecked at use.
      pendingContextRef <- Ref.of[F, Map[(Address, Hash), PendingContextEntry]](Map.empty)
      pendingParentSeqRef <- Ref.of[F, Long](0L)
      // #29 verify-on-attach: Phase-0 un-verified attestations keyed by (mg, WIRE hash). Drained + verified
      // when the attested binary attaches (its `resolveParent == Some` admit path). Bounded-FIFO via `seq`.
      attestationsRef <- Ref.of[F, Map[(Address, Hash), List[AttestationEntry]]](Map.empty)
      attestationsSeqRef <- Ref.of[F, Long](0L)
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

        def recordPendingBinaryContext(
          metagraphAddress: Address,
          wireHash: Hash,
          context: MetagraphParentOrdinalResolver.CurrencyBinaryContext
        ): F[Unit] =
          for {
            nextSeq <- pendingParentSeqRef.updateAndGet(_ + 1L)
            _ <- pendingContextRef.update { pending =>
              val withNew = pending.updated((metagraphAddress, wireHash), PendingContextEntry(nextSeq, context))
              enforcePendingContextCap(withNew, admissionsCap)
            }
          } yield ()

        def lookupPendingBinaryContext(
          metagraphAddress: Address,
          wireHash: Hash
        ): F[Option[MetagraphParentOrdinalResolver.CurrencyBinaryContext]] =
          pendingContextRef.get.map(_.get((metagraphAddress, wireHash)).map(_.context))

        def bufferAttestation(
          metagraphAddress: Address,
          binaryHash: Hash,
          att: MetagraphCommitteeGate.IncomingAttestation
        ): F[Unit] =
          for {
            nextSeq <- attestationsSeqRef.updateAndGet(_ + 1L)
            _ <- attestationsRef.update { state =>
              val key = (metagraphAddress, binaryHash)
              val existing = state.getOrElse(key, Nil)
              // Idempotent per sender: a gossip re-delivery of the same sender's attestation is not re-buffered.
              if (existing.exists(_.att.senderPeerId === att.senderPeerId)) state
              else enforceAttestationsCap(state.updated(key, AttestationEntry(nextSeq, att) :: existing), attestationsCap)
            }
          } yield ()

        def drainAttestations(
          metagraphAddress: Address,
          binaryHash: Hash
        ): F[List[MetagraphCommitteeGate.IncomingAttestation]] =
          attestationsRef.modify { state =>
            val key = (metagraphAddress, binaryHash)
            state.get(key) match {
              case None => (state, Nil)
              // Entries prepended (newest first) → reverse for chronological replay.
              case Some(entries) => (state - key, entries.reverse.map(_.att))
            }
          }

        def attestationsBufferSize: F[Int] =
          attestationsRef.get.map(_.values.foldLeft(0)((acc, es) => acc + es.length))
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

  private def enforcePendingContextCap(
    pending: Map[(Address, Hash), PendingContextEntry],
    cap: Int
  ): Map[(Address, Hash), PendingContextEntry] =
    if (pending.size <= cap) pending
    else {
      val sortedByAge = pending.toList.sortBy(_._2.seq)
      sortedByAge.drop(pending.size - cap).toMap
    }

  /** FIFO eviction on the Phase-0 attestation buffer (#29). Drops the oldest (lowest seq) attestations across all binaries until the total
    * ≤ cap. Mirrors [[enforceCap]] (the binary buffer): linear per eviction, bounded under the cap.
    */
  private def enforceAttestationsCap(
    state: Map[(Address, Hash), List[AttestationEntry]],
    cap: Int
  ): Map[(Address, Hash), List[AttestationEntry]] = {
    def total(s: Map[(Address, Hash), List[AttestationEntry]]): Int =
      s.values.foldLeft(0)((acc, es) => acc + es.length)
    @scala.annotation.tailrec
    def loop(s: Map[(Address, Hash), List[AttestationEntry]]): Map[(Address, Hash), List[AttestationEntry]] =
      if (total(s) <= cap) s
      else {
        val oldest = s.iterator.flatMap { case (k, es) => es.map(e => (k, e)) }.minByOption(_._2.seq)
        oldest match {
          case None => s
          case Some((key, entry)) =>
            val remaining = s(key).filterNot(_.seq == entry.seq)
            loop(if (remaining.isEmpty) s - key else s.updated(key, remaining))
        }
      }
    loop(state)
  }
}
