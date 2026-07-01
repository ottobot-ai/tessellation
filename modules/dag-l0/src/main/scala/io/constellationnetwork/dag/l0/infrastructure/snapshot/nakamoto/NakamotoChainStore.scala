package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{ChainSelection, ParentChildTree, TipTracker}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Nakamoto-aware chain storage that handles forks and reorgs.
  *
  * Unlike tessellation's linear SnapshotStorage.prepend (which rejects non-sequential parents), this store maintains:
  *   - A map of all known snapshots by hash
  *   - The current "best tip" as determined by ChainSelection
  *   - Proper reorg support: when a better chain is received, update the canonical head
  *
  * It wraps the underlying SnapshotStorage for actual persistence, using setHeadForRecovery only during reorgs (which is appropriate — it
  * IS a recovery from a shorter/weaker chain).
  */
object NakamotoChainStore {

  /** A stored snapshot with its context and chain metadata */
  case class StoredSnapshot(
    signedSnapshot: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    ordinal: Long,
    slot: Long,
    parentHash: Hash,
    hash: Hash,
    vrfOutput: Array[Byte] = Array.empty
  )

  case class ChainState(
    byHash: Map[Hash, StoredSnapshot], // All known snapshots indexed by hash
    bestTipHash: Option[Hash], // Current best chain tip hash
    lastFinalizedOrdinal: Long // Last finalized ordinal — snapshots below this can be pruned
  )

  object ChainState {
    val empty: ChainState = ChainState(Map.empty, None, 0L)
  }

  trait NakamotoChainStoreAlgebra[F[_]] {

    /** Store a new snapshot. If it extends the best chain or creates a better fork, update the tip. Returns true if the snapshot was new
      * (not a duplicate).
      */
    def store(
      signedSnapshot: Signed[GlobalIncrementalSnapshot],
      context: GlobalSnapshotInfo,
      ordinal: Long,
      slot: Long,
      parentHash: Hash,
      vrfOutput: Array[Byte]
    ): F[Boolean]

    /** Get the current best chain tip */
    def bestTip: F[Option[StoredSnapshot]]

    /** Get the current best tip's slot (for LDD gap calculation) */
    def bestTipSlot: F[Option[Long]]

    /** Get the current best tip's ordinal */
    def bestTipOrdinal: F[Option[Long]]

    /** Number of snapshots in the chain store */
    def chainLength: F[Int]

    /** Number of distinct fork tips (snapshots that aren't parents of other snapshots) */
    def forkCount: F[Int]

    /** Set of all distinct fork-tip hashes (every snapshot in `byHash` that isn't the parent of another stored snapshot). Includes the
      * canonical bestTip AND any tentative-branch heads we're following during fork-recovery. The MptOverlay uses this to protect ancestors
      * of every viable chain head from cap-eviction (#115): when validator commits canonical-N before the chain reorgs to it, canonical-N
      * isn't yet `bestTip` but IS in `allTips`, so its ancestors are protected.
      */
    def allTips: F[Set[Hash]]

    /** Last finalized ordinal */
    def lastFinalizedOrdinal: F[Long]

    /** Get a snapshot by hash. In-memory only — returns None for entries Fix B has evicted below `keepDepthBehindFinalized` even though the
      * snapshot is on disk. Pure-hash callers (bootstrap-only) get this semantic; callers that already know the expected ordinal should
      * prefer [[getWithOrdinalFallback]].
      */
    def get(hash: Hash): F[Option[StoredSnapshot]]

    /** Get a snapshot by hash, falling back to disk-backed `SnapshotStorage` when the in-memory `byHash` map misses. The fallback uses
      * `expectedOrdinal` to read the snapshot from disk (which indexes by ordinal); on a successful disk read the returned snapshot's hash
      * is checked against the requested `hash` to defend against forks (the disk's ordinal-N snapshot may belong to a different chain than
      * the one the caller is walking — in which case we return None rather than a wrong-chain entry).
      *
      * '''Why hash + ordinal both.''' Disk indexes by ordinal; chain-walk callers know the parent's ordinal (`current.ordinal - 1`) and its
      * hash. Combining them lets a single point read on disk produce a hash-verified result even though disk has no hash index. Tests this
      * is the load-bearing path for:
      *   - [[vrfOutputsForPeriod]] / [[collectVrfOutputsForPeriod]] walking back to `periodStart` of period N-1. With Fix B's k₁-bounded
      *     in-memory retention the walk hits the eviction floor after ~k₁ ords; the fallback recovers VRF outputs from disk so eta rotation
      *     is preserved across the eviction boundary (Path 1, Finding 1).
      *   - [[ChainSyncServer.serveSnapshots]] hash-keyed peer queries (Path 1, Finding 2).
      *
      * '''Why not always disk-first.''' In-memory `byHash` is O(1) hash-map lookup; disk is a file open + parse. The hot path (chain
      * extension, finality, depth-k traversal) operates within `keepDepthBehindFinalized` ords of the tip and never misses in-memory. Disk
      * read is exercised only on the cross-eviction-boundary tail.
      */
    def getWithOrdinalFallback(hash: Hash, expectedOrdinal: Long): F[Option[StoredSnapshot]]

    /** Get the chain of snapshots from tip back to genesis (or pruning point) */
    def chainFromTip: F[List[StoredSnapshot]]

    /** Get VRF outputs for snapshots in the first 2/3 of a rotation period (for eta calculation). Walks from bestTip — use
      * vrfOutputsForPeriodFrom for fork-aware queries. The Long in the returned pairs is the snapshot's **ordinal** (rotation periods are
      * keyed on ordinal to satisfy the R ≥ 3·k₁ stability bound; see `docs/nakamoto/attestation-and-finality.md` §1).
      */
    def vrfOutputsForPeriod(period: Long, etaRotationSnapshots: Long): F[List[(Long, Array[Byte])]]

    /** Get VRF outputs for a rotation period by walking backward from a specific hash. Used to compute eta for an incoming snapshot on a
      * potentially different fork. The Long in the returned pairs is the snapshot's **ordinal**.
      */
    def vrfOutputsForPeriodFrom(period: Long, etaRotationSnapshots: Long, fromHash: Hash): F[List[(Long, Array[Byte])]]

    /** Mark a snapshot as finalized and prune older fork branches. Keeps the finalized chain but removes orphaned snapshots with ordinal <=
      * finalizedOrdinal that aren't ancestors of the finalized tip.
      */
    def finalize(hash: Hash, ordinal: Long): F[Unit]

    /** Walk the canonical chain from `startHash` backward to find the hash at the given ordinal. */
    def walkBackTo(startHash: Hash, targetOrdinal: Long): F[Option[Hash]]

    /** Find a snapshot by ordinal in the in-memory store (scans all entries, not just canonical chain). */
    def getByOrdinal(ordinal: Long): F[Option[StoredSnapshot]]

    /** Get current chain state size (number of stored snapshots) */
    def size: F[Int]

    /** Get the ParentChildTree for chain traversal (used by ChainSelection, reorgs). */
    def tree: ParentChildTree[F]

    /** Get a ChainTip for a given hash (for ChainSelection traversal). */
    def tipFor(hash: Hash): F[Option[ChainTip]]

    /** Divergent-self-finalize signal (P-11, task #141). Increments each time `store` refuses a different-hash write at-or-below the
      * finalized ordinal — i.e. each time the finality-safety gate rejects the canonical chain's hash because this node has already
      * locally-finalized a divergent hash at that ordinal.
      *
      * Reset to 0 by `unsafe_clearFinality`. The `RebootstrapOrchestrator` reads this counter to decide when to trigger a full reset. The
      * counter is `F[Long]` (read-only from orchestrator's perspective) so concurrent refuses from the store path can advance it without
      * coordination.
      */
    def divergentRefuseCount: F[Long]

    /** Latest divergent-self-finalize event: the (ordinal, canonicalHash) we last refused to store because our local-finalized hash at that
      * ordinal differs. Cleared by `unsafe_clearFinality`. Read by the orchestrator to log which ordinal we were stuck at when divergence
      * was detected.
      */
    def divergentRefuseSample: F[Option[(Long, Hash)]]

    /** Re-bootstrap escape hatch (P-11, task #141). Reset the chain store's internal state so a fresh canonical chain can be ingested
      * without tripping the finality-safety gate that was permanently refusing the canonical hash:
      *   - `byHash` cleared
      *   - `bestTipHash` cleared
      *   - `lastFinalizedOrdinal` reset to 0
      *   - `divergentRefuseCounterRef` / `divergentRefuseSampleRef` cleared
      *
      * Breaks the monotonicity invariant of `finalize` (which only ever advances finality) — the prefix `unsafe_` signals callers must hold
      * the right gates (production paused, chain-sync about to fire) before invoking. Do NOT call from the consensus path.
      *
      * Returns `true` if any state was actually cleared (used for logging).
      */
    def unsafe_clearFinality: F[Boolean]
  }

  /** Default for `keepDepthBehindFinalized` — equals the operational confirmation depth k₁ (the default for `NAKAMOTO_CONFIRMATION_DEPTH`,
    * see `SnapshotLeaderLoop.ConfirmationDepthK`). Tests and call sites that don't override receive this value; the production call site
    * (`GlobalSnapshotConsensus`) reads `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED` to override.
    *
    * Heap-leak Fix B: bounds the in-memory `ChainState.byHash` canonical-chain retention to a sliding window of `keepDepthBehindFinalized`
    * ordinals behind the finalized tip. Older canonical entries are evicted on every `finalize` call and reads fall through to disk-backed
    * `SnapshotStorage` (which retains every persisted snapshot for the lifetime of the data directory).
    *
    * `byHash` was previously unbounded; the 12h soak at commit `fb3394078` showed it growing to ~7-8 GiB after ~5300 finalized ordinals
    * (each `StoredSnapshot` carrying a full `GlobalSnapshotInfo` with embedded per-metagraph `lastCurrencySnapshots`). With this default,
    * 255 ordinals × ~1.4 MiB ≈ 360 MiB worst case — well below the GC pressure threshold.
    */
  val DefaultKeepDepthBehindFinalized: Long = 255L

  def make[F[_]: Async: HasherSelector](
    underlyingStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    chainSelection: ChainSelection[F],
    tipTracker: TipTracker[F],
    // Finalized-ordinal guard. `store` refuses to write a snapshot whose ordinal is
    // at-or-below finalized when its hash differs from the already-stored one — that
    // would silently rewrite finalized content (the gl0-2-divergent-517 class of bug
    // we saw: local slot-win produced own snapshot at ord N that was already finalized
    // via gossip with a different hash, and blindly overwrote it, causing permanent
    // cross-node hash disagreement at ord N).
    //
    // Writes above finalized are always allowed (that's the normal reorg path). Writes
    // at-or-below finalized with MATCHING hash are no-ops (legitimate re-delivery or
    // download-replay). Only differing-hash writes at-or-below finalized are refused.
    nakamotoFinalizedOrdinalRef: Ref[F, SnapshotOrdinal],
    // Track-3 S1.5 "marker split". The DISTINCT k₂ "settled" ordinal source — the deepest ordinal past the Phase-2→Phase-3
    // archival gate (k₂ = 100·k₁), advanced ONLY by SnapshotLeaderLoop's `T_depth2` sink (via the `SettledOrdinalTracker` that
    // shares this exact ref, so there is ONE settled source). This is NOT `nakamotoFinalizedOrdinalRef` (k₁) — the two markers are
    // separate refs and advance independently, with the invariant `settled ≤ finalized` maintained by construction (k₂ is the
    // deeper window: T_depth2 always trails T_depth1).
    //
    // In S1.5 the store only RESETS this ref (in `unsafe_clearFinality`, in lock-step with the k₁ ref) — it is threaded in now so
    // it is AVAILABLE to the store-gate + fork-choice, but the store-gate (the `ordinal <= finalized` finality-safety check in
    // `store`) and the production floor (`SnapshotLeaderLoop`) stay keyed on the k₁ `nakamotoFinalizedOrdinalRef`. Re-keying the
    // store-gate + `shouldSwitch` onto this settled ref is Track-3 S3; wiring it now (without S3) would either be inert or drag
    // production to k₂ — the `86f390130` self-contradiction that got reverted.
    nakamotoSettledOrdinalRef: Ref[F, SnapshotOrdinal],
    // Heap-leak Fix B. Number of ordinals BEHIND the finalized tip to retain in
    // `ChainState.byHash`. Defaults to [[DefaultKeepDepthBehindFinalized]] = 255 (k₁,
    // matching the operational confirmation depth). The production call site overrides via
    // `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED`.
    //
    // Safety: chain-selection / fork-choice operates only on the top-k₁ chain entries
    // (`ChainSelection.shouldSwitch` walks at most `ConfirmationDepthK` ordinals via
    // `chainStore.tipFor`; `walkBackTo` falls through to disk via `SnapshotStorage.getHash`
    // when the in-memory chain breaks). Older `chainStore.get(hash)` lookups (e.g. the
    // depth-finality `chainStore.get(canonicalHash)` site) target ordinals at depth ≤ k₁
    // from the tip, so retaining `k₁` ordinals back keeps the consensus hot path entirely
    // in-memory.
    //
    // Caveats observed during implementation (worth feeding back to operators):
    //   - `vrfOutputsForPeriod(period - 1, etaRotationSnapshots)` walks back up to one
    //     full eta-rotation window. Default rotation is 2550 ords (10·k₁); with
    //     `keepDepthBehindFinalized = 255`, walks below 255 ords behind the finalized
    //     tip return a partial set. This is fine for e2e tests (rotation period 0 never
    //     leaves genesis-eta on the typical ~500-ord runs) but production deployments
    //     should set `NAKAMOTO_KEEP_DEPTH_BEHIND_FINALIZED` to at least
    //     `2 * NAKAMOTO_ETA_ROTATION_SNAPSHOTS` if `etaRotationSnapshots > 255`.
    //   - `ChainSyncServer.serveSnapshots` answers `NotFound` for hashes not in `byHash`;
    //     historical-query peers can fall back to `serveByRange` (disk-backed) or full
    //     catch-up.
    keepDepthBehindFinalized: Long = DefaultKeepDepthBehindFinalized
  ): F[NakamotoChainStoreAlgebra[F]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoChainStore")

    for {
      stateRef <- Ref.of[F, ChainState](ChainState.empty)
      pcTree <- ParentChildTree.make[F]
      // Divergent-self-finalize signal (P-11). Incremented on every refused different-hash
      // write at-or-below finalized. Reset by `unsafe_clearFinality`. Used by the
      // RebootstrapOrchestrator to detect that this node has self-finalized a divergent fork.
      divergentRefuseCounterRef <- Ref.of[F, Long](0L)
      divergentRefuseSampleRef <- Ref.of[F, Option[(Long, Hash)]](None)
    } yield {
      new NakamotoChainStoreAlgebra[F] {

        def store(
          signedSnapshot: Signed[GlobalIncrementalSnapshot],
          context: GlobalSnapshotInfo,
          ordinal: Long,
          slot: Long,
          parentHash: Hash,
          vrfOutput: Array[Byte]
        ): F[Boolean] =
          HasherSelector[F].withCurrent { implicit hasher =>
            signedSnapshot.toHashed[F].flatMap { hashed =>
              val snapshotHash = hashed.hash
              val stored = StoredSnapshot(signedSnapshot, context, ordinal, slot, parentHash, snapshotHash, vrfOutput)
              val vrfHex = VrfOutput(Hex(vrfOutput.map("%02x".format(_)).mkString))
              val newTip = ChainTip(
                snapshotHash,
                Slot(NonNegLong.unsafeFrom(slot)),
                ordinal,
                parentHash,
                vrfHex
              )

              // Finality-safety gate. If `ordinal` is at-or-below finalized AND a DIFFERENT hash is
              // already stored at that ordinal, refuse the write — accepting would silently rewrite
              // finalized content and cause cross-node hash divergence (the gl0-2-divergent-517
              // class of bug: local slot-win produced own snapshot at ord N that was already
              // finalized via gossip with a different hash, and blindly overwrote it, leading to
              // permanent cross-node hash disagreement at ord N).
              //
              // Matching-hash writes at-or-below finalized fall through (legitimate re-delivery
              // / download replay). Writes above finalized always pass (normal reorg path, still
              // finality-safe because unfinalized).
              val tryStore: F[Boolean] = stateRef.modify { state =>
                if (state.byHash.contains(snapshotHash)) {
                  // Duplicate — already stored
                  (state, false.pure[F])
                } else {
                  val newByHash = state.byHash + (snapshotHash -> stored)

                  // Resolve the current best tip defensively. The bestTipHash field can briefly
                  // point at a hash no longer in byHash if the finalize-pruning path removed it
                  // (or under a future race we haven't yet root-caused). When that happens we
                  // treat it as "no best tip" rather than throwing — the incoming snapshot then
                  // bootstraps a fresh tip, which is the correct recovery for catch-up: a node
                  // that fell behind and is being reseeded from the network tip should accept
                  // the new tip unconditionally. See task #9 in NAKAMOTO-PLAN.md.
                  val resolvedBest = state.bestTipHash.flatMap(h => state.byHash.get(h).map(s => (h, s)))

                  resolvedBest match {
                    case None =>
                      // First snapshot OR stale best tip — incoming becomes the best
                      val newState = state.copy(byHash = newByHash, bestTipHash = Some(snapshotHash))
                      (
                        newState,
                        pcTree.associate(snapshotHash, parentHash) >>
                          persistHead(stored, snapshotHash) >> logger
                            .info(
                              if (state.bestTipHash.isDefined)
                                s"🏗️ Chain reseeded at ordinal=$ordinal slot=$slot (previous best tip ${state.bestTipHash.map(_.value.take(12)).getOrElse("?")} no longer in store)"
                              else
                                s"🏗️ Chain initialized at ordinal=$ordinal slot=$slot"
                            )
                            .as(true)
                      )

                    case Some((currentBestHash, currentBest)) =>
                      // currentBestHash and currentBest both bound from the destructured pair
                      val currentTip = ChainTip(
                        currentBestHash,
                        Slot(NonNegLong.unsafeFrom(currentBest.slot)),
                        currentBest.ordinal,
                        currentBest.parentHash,
                        VrfOutput(Hex(currentBest.vrfOutput.map("%02x".format(_)).mkString))
                      )

                      val newState = state.copy(byHash = newByHash)

                      // Extension-first dispatch. Three architecturally distinct outcomes from a peer-received
                      // snapshot:
                      //
                      //   1. CANONICAL EXTENSION: incoming snapshot's parentHash IS our current bestTip. The new
                      //      tip naturally extends the chain we're already on. `prepend` is the optimized linear
                      //      append, with the `isNextSnapshot` invariant check. No state proofs need re-anchoring,
                      //      no MPT base reset is implied. (Pre-#117 fix this case was conflated with reorg
                      //      because `chainSelection.shouldSwitch` returns true for any longer chain — including
                      //      a child of currentBestTip — so it routed through `setHeadForRecovery` and logged
                      //      "Chain reorg" misleadingly. The DEBUG-level "Chain extended" leg below was dead code.)
                      //
                      //   2. TRUE REORG: incoming snapshot's parentHash is NOT our current bestTip AND
                      //      ChainSelection picks the incoming chain over ours (longer / denser / lower-VRF
                      //      tiebreak per maxvalid-tk + maxvalid-bg). We're switching to a different branch.
                      //      `setHeadForRecovery` is the right API — it allows non-sequential head movement.
                      //
                      //   3. ALTERNATE BRANCH: incoming snapshot is on a different branch and ChainSelection
                      //      keeps our current tip. Store the snapshot in the chain store as an alternate branch
                      //      head (so eviction / future reorg-detection sees it) but don't change bestTip.
                      //
                      // The order matters: case 1 must be checked BEFORE invoking `shouldSwitch`, because
                      // `shouldSwitch` cannot distinguish "child of current" from "competing chain at higher
                      // ord" — both make the candidate win in `compare`.
                      val effect = pcTree.associate(snapshotHash, parentHash) >> {
                        if (parentHash === currentBestHash) {
                          // CASE 1: canonical chain extension — append linearly
                          stateRef.update(_.copy(bestTipHash = Some(snapshotHash))) >>
                            persistLinear(stored) >>
                            logger.debug(s"Chain extended to ordinal=$ordinal slot=$slot").as(true)
                        } else {
                          chainSelection.shouldSwitch(currentTip, newTip).flatMap {
                            case true =>
                              // CASE 2: real reorg — different branch wins
                              stateRef.update(_.copy(bestTipHash = Some(snapshotHash))) >>
                                persistHead(stored, snapshotHash) >>
                                logger
                                  .info(
                                    s"Chain reorg: ordinal=$ordinal slot=$slot (parent=${parentHash.value.take(8)}) beats " +
                                      s"previous tip ordinal=${currentBest.ordinal} slot=${currentBest.slot} " +
                                      s"hash=${currentBestHash.value.take(8)}"
                                  )
                                  .as(true)
                            case false =>
                              // CASE 3: alternate branch loses ChainSelection — store but don't switch
                              logger
                                .debug(
                                  s"🔀 Stored alternate branch snapshot ordinal=$ordinal slot=$slot " +
                                    s"(parent=${parentHash.value.take(8)}, not switching from currentBest=${currentBestHash.value.take(8)})"
                                )
                                .as(true)
                          }
                        }
                      }

                      (newState, effect)
                  }
                }
              }.flatten

              nakamotoFinalizedOrdinalRef.get.flatMap { finalized =>
                // Chain-store API is Long-indexed; compare against the finalized ordinal's Long value.
                val finalizedLong = finalized.value.value
                if (ordinal <= finalizedLong) {
                  stateRef.get.flatMap { state =>
                    state.byHash.values.find(_.ordinal == ordinal).map(_.hash) match {
                      case Some(existingHash) if existingHash =!= snapshotHash =>
                        // P-11 (#141): the divergent-self-finalize trip-wire. We've already finalized
                        // a different hash at this ordinal; the incoming write IS the canonical chain
                        // trying to overwrite our locally-finalized divergent fork. Increment the
                        // signal counter + record the sample so the RebootstrapOrchestrator can detect
                        // the lock-out and trigger reset.
                        divergentRefuseCounterRef.update(_ + 1L) >>
                          divergentRefuseSampleRef.set(Some((ordinal, snapshotHash))) >>
                          logger
                            .warn(
                              s"REFUSED store: finality-safety violation — ordinal=$ordinal is at-or-below finalized=${finalized.show}, " +
                                s"existing=${existingHash.value.take(12)}, new=${snapshotHash.value.take(12)}. " +
                                s"Dropping write; this node previously finalized the existing snapshot and must not rewrite it. " +
                                s"[P-11 divergent-refuse counter incremented]"
                            )
                            .as(false)
                      case _ => tryStore
                    }
                  }
                } else tryStore
              }
            }
          }

        def bestTip: F[Option[StoredSnapshot]] =
          stateRef.get.map(s => s.bestTipHash.flatMap(s.byHash.get))

        def bestTipSlot: F[Option[Long]] =
          bestTip.map(_.map(_.slot))

        def bestTipOrdinal: F[Option[Long]] =
          bestTip.map(_.map(_.ordinal))

        def chainLength: F[Int] =
          stateRef.get.map(_.byHash.size)

        def forkCount: F[Int] =
          stateRef.get.map { state =>
            val parentHashes = state.byHash.values.map(_.parentHash).toSet
            state.byHash.keys.count(h => !parentHashes.contains(h))
          }

        def allTips: F[Set[Hash]] =
          stateRef.get.map { state =>
            val parentHashes = state.byHash.values.map(_.parentHash).toSet
            state.byHash.keySet -- parentHashes
          }

        def lastFinalizedOrdinal: F[Long] =
          stateRef.get.map(_.lastFinalizedOrdinal)

        def get(hash: Hash): F[Option[StoredSnapshot]] =
          stateRef.get.map(_.byHash.get(hash))

        def getWithOrdinalFallback(hash: Hash, expectedOrdinal: Long): F[Option[StoredSnapshot]] =
          stateRef.get.flatMap(_.byHash.get(hash) match {
            case some @ Some(_) => Async[F].pure(some)
            case None           =>
              // In-memory miss — Fix B may have evicted this entry. Try disk via `SnapshotStorage`.
              // SnapshotStorage indexes by ordinal; we use the caller-supplied `expectedOrdinal` to do
              // the point read, then verify the returned snapshot's hash matches the requested `hash`.
              // The hash-verify defends against forks: disk may hold an ordinal-N snapshot from a
              // chain different from the one the caller is walking. Hash mismatch ⇒ None (caller's
              // chain-walk treats this as broken and falls through to whatever non-canonical handling
              // they want).
              if (expectedOrdinal < 0L) Async[F].pure(None)
              else {
                val ord = SnapshotOrdinal(NonNegLong.unsafeFrom(expectedOrdinal))
                HasherSelector[F].withCurrent { implicit hasher =>
                  underlyingStorage.get(ord).flatMap {
                    case None => Async[F].pure(None: Option[StoredSnapshot])
                    case Some(signedSnap) =>
                      signedSnap.toHashed[F].flatMap { hashed: Hashed[GlobalIncrementalSnapshot] =>
                        if (hashed.hash =!= hash)
                          // Disk's ordinal-N snapshot is from a different chain than the caller is
                          // walking. Don't return it — chain-walks must stay on their requested
                          // chain or correctness breaks.
                          logger
                            .debug(
                              s"getWithOrdinalFallback ordinal=$expectedOrdinal hashMismatch: " +
                                s"requested=${hash.value.take(12)} disk=${hashed.hash.value.take(12)}"
                            )
                            .as(None)
                        else {
                          // Reconstruct a `StoredSnapshot` from the on-disk record. The slot
                          // certificate (when present) carries slot / parentSlot / vrfOutput; on
                          // legacy / cert-less snapshots we synthesize zero defaults (those code
                          // paths are pre-Nakamoto and won't be visited via this fallback under
                          // production Nakamoto config). `parentHash` lives on the incremental
                          // snapshot itself (`lastSnapshotHash`).
                          val signed = hashed.signed
                          val cert = signed.value.slotCertificate
                          val slot = cert.map(_.slot.value.value).getOrElse(0L)
                          val parentHash = signed.value.lastSnapshotHash
                          val vrfBytes = cert.map(_.vrfOutput.value.toBytes).getOrElse(Array.empty[Byte])
                          // Disk doesn't persist `GlobalSnapshotInfo`; the StoredSnapshot.context
                          // is left as the snapshot's `info` slice from the toGlobalSnapshotInfo
                          // path. For the chain-walk consumers wired by Path 1
                          // (`vrfOutputsForPeriod` and `ChainSyncServer.serveSnapshots`) the
                          // `context` field is unused on the fallback path — they read only
                          // `signedSnapshot`, `ordinal`, `parentHash`, and `vrfOutput`. We populate
                          // an empty placeholder GSI to satisfy the record shape; if a future
                          // consumer reads `context` on the fallback path it should be migrated to
                          // pull from `SnapshotStorage.head` / dedicated GSI storage instead.
                          val placeholderGsi: GlobalSnapshotInfo =
                            io.constellationnetwork.schema
                              .GlobalSnapshotInfoV1(
                                scala.collection.immutable.SortedMap.empty,
                                scala.collection.immutable.SortedMap.empty,
                                scala.collection.immutable.SortedMap.empty
                              )
                              .toGlobalSnapshotInfo
                          val stored = StoredSnapshot(
                            signedSnapshot = signed,
                            context = placeholderGsi,
                            ordinal = expectedOrdinal,
                            slot = slot,
                            parentHash = parentHash,
                            hash = hash,
                            vrfOutput = vrfBytes
                          )
                          Async[F].pure(Some(stored): Option[StoredSnapshot])
                        }
                      }
                  }
                }
              }
          })

        def chainFromTip: F[List[StoredSnapshot]] =
          stateRef.get.map { state =>
            state.bestTipHash match {
              case None          => Nil
              case Some(tipHash) =>
                // Walk back from tip through parents
                val chain = scala.collection.mutable.ListBuffer.empty[StoredSnapshot]
                var current = state.byHash.get(tipHash)
                while (current.isDefined) {
                  chain += current.get
                  current = state.byHash.get(current.get.parentHash)
                }
                chain.toList
            }
          }

        // TODO(eta-finality-gate): start this walk from the FINALIZED tip, not `bestTipHash`.
        //
        // Ouroboros finality requires the eta nonce to be computed from FINALIZED rho values: with
        // R = round(3.03·k₁) the collection cutoff (`periodStart + 2R/3`) sits ~R/3 ≈ k₁ behind the
        // period end, so by the time period N's eta is consumed (producing in period N+1) those inputs
        // SHOULD be past depth-k. Walking from `bestTipHash` instead admits rho values from a
        // not-yet-finalized fork head, which a later reorg could change → eta divergence at the
        // rotation boundary.
        //
        // NOT changed here, deliberately — doing it correctly is non-trivial and consensus-critical:
        //   1. `ChainState` (this file, ~:42) tracks `lastFinalizedOrdinal: Long` but does NOT retain a
        //      finalized-tip HASH — `finalize(hash, ordinal)` (~:605) records only the ordinal (+ prunes)
        //      and discards the canonical hash. So there is no finalized-tip hash to start from; it must
        //      be resolved from `lastFinalizedOrdinal` (and may be below the in-memory keep-floor for
        //      large R, requiring a disk `SnapshotStorage.getHash(ordinal)` hop).
        //   2. `confirmationDepthK` is not plumbed into this store / method, so the alternative origin
        //      `(bestTip.ordinal - k₁)` is not computable here without threading it in.
        //   3. TIMING HAZARD: production calls `vrfOutputsForPeriod(currentPeriod - 1, …)` right as the
        //      tip enters period N (tip ≈ N·R). At that instant `lastFinalizedOrdinal` can be as low as
        //      `bestTip.ordinal − k₁`, i.e. just BELOW the period-(N−1) cutoff `N·R − R/3`. Starting the
        //      walk from the finalized tip there would TRUNCATE the `[periodStart, cutoff)` set and yield
        //      a DIFFERENT eta than the current best-tip walk — a cluster-splitting consensus change that
        //      every node must flip together and validate e2e.
        // Tracked for the eta-amortization rework; until then the existing best-tip walk is preserved so
        // behavior is byte-identical to the validated baseline.
        def vrfOutputsForPeriod(period: Long, etaRotationSnapshots: Long): F[List[(Long, Array[Byte])]] =
          stateRef.get.flatMap { state =>
            collectVrfOutputsForPeriod(state, period, etaRotationSnapshots, state.bestTipHash)
          }

        def vrfOutputsForPeriodFrom(period: Long, etaRotationSnapshots: Long, fromHash: Hash): F[List[(Long, Array[Byte])]] =
          stateRef.get.flatMap { state =>
            collectVrfOutputsForPeriod(state, period, etaRotationSnapshots, Some(fromHash))
          }

        // Filters by **ordinal**, not slot — rotation periods are snapshot-indexed so R satisfies the
        // Praos R ≥ 3·k₁ stability bound (see `docs/nakamoto/attestation-and-finality.md` §1). The
        // returned Long is the snapshot's ordinal.
        //
        // Path 1 (heap-leak workstream): the walk back to `periodStart` of period N-1 reaches O(R) ords
        // behind the tip. Under Fix B's k₁-bounded `byHash` retention (default 255), the walk crosses
        // the eviction floor for any R > k₁ (default R = 2550 = 10·k₁). To preserve eta-rotation
        // determinism across the boundary we route every hop through [[getWithOrdinalFallback]]: each
        // parent lookup tries in-memory `byHash` first; on miss it falls through to disk-backed
        // `SnapshotStorage.get(ordinal)` with hash-verify. This is the load-bearing fix for Finding 1
        // (eta silently degrading to genesis when Fix B evicts pre-rotation VRF outputs).
        private def collectVrfOutputsForPeriod(
          state: ChainState,
          period: Long,
          etaRotationSnapshots: Long,
          startHash: Option[Hash]
        ): F[List[(Long, Array[Byte])]] = {
          val periodStart = period * etaRotationSnapshots
          val cutoff = periodStart + (etaRotationSnapshots * 2 / 3)
          // Walk chain from the given starting hash backward.
          // Using byHash.values would include fork branches, causing different nodes
          // to compute different eta values → VRF verification failures at rotation boundaries.
          //
          // Iterative monadic walk: at each hop, look up the parent via `getWithOrdinalFallback`.
          // The starting hop is supplied by `startHash` (no ordinal known a priori), so we have to
          // bootstrap from the in-memory lookup if available; for chains whose `startHash` has been
          // evicted the caller's chain context already broke (this method is only invoked with a
          // known live `bestTip` or a known-recent fork hash, both within `keepDepthBehindFinalized`
          // of the tip).
          def goImpl(
            current: Option[StoredSnapshot],
            acc: List[StoredSnapshot]
          ): F[List[StoredSnapshot]] =
            current match {
              case None => Async[F].pure(acc)
              case Some(cur) =>
                if (cur.ordinal < periodStart) Async[F].pure(acc)
                else {
                  val nextAcc =
                    if (cur.ordinal < cutoff && cur.vrfOutput.nonEmpty) cur :: acc
                    else acc
                  // Parent ordinal is `cur.ordinal - 1` (chain is linear by construction once we're
                  // walking back from a tip). Use that to drive `getWithOrdinalFallback` so the disk
                  // path engages when in-memory retention has evicted the parent.
                  if (cur.ordinal <= 0L) Async[F].pure(nextAcc)
                  else getWithOrdinalFallback(cur.parentHash, cur.ordinal - 1L).flatMap(goImpl(_, nextAcc))
                }
            }

          val startStored = startHash.flatMap(state.byHash.get)
          // Bootstrap: when `startHash` is itself in-memory we use it directly. The hash-only path
          // (`get(hash)`) is in-memory-only; if `startHash` has been evicted the caller's chain
          // context already broke (we have no `expectedOrdinal` for it). In practice every call site
          // supplies a `startHash` taken from `bestTip` or a recent fork head, both within
          // `keepDepthBehindFinalized` of the tip.
          goImpl(startStored, Nil).map { collected =>
            collected.sortBy(_.ordinal).map(s => (s.ordinal, s.vrfOutput))
          }
        }

        def finalize(hash: Hash, ordinal: Long): F[Unit] =
          stateRef.modify { state =>
            if (!state.byHash.contains(hash)) {
              // Finality was reached for a hash this node hasn't processed yet (attestations
              // arrived before the snapshot itself). We can't compute the canonical chain — and
              // a naive prune would wipe everything at or below this ordinal because
              // canonicalHashes would be empty. Skip pruning, just record the new finalized
              // ordinal so subsequent catch-up paths know how far ahead the network is.
              (
                state.copy(lastFinalizedOrdinal = math.max(state.lastFinalizedOrdinal, ordinal)),
                logger.warn(
                  s"🔒 Finalize called for unknown hash=${hash.value.take(12)}.. ordinal=$ordinal — recording finalized ordinal but skipping prune (snapshot not yet received)"
                )
              )
            } else {
              // Collect hashes on the canonical chain from finalized tip backward
              val canonicalHashes = scala.collection.mutable.Set.empty[Hash]
              var current = state.byHash.get(hash)
              var walkReachedGenesis = false
              while (current.isDefined) {
                canonicalHashes += current.get.hash
                if (current.get.ordinal <= 1) walkReachedGenesis = true
                current = state.byHash.get(current.get.parentHash)
              }

              // Heap-leak Fix B — bound canonical-chain retention.
              //
              // `keepFloor` is the lowest ordinal we retain in `byHash`. Anything at
              // `ordinal < keepFloor` (canonical or orphan) is dropped on this finalize.
              // We clamp to 0 so genesis / early-chain (ord < keepDepth) is a no-op:
              // `keepFloor = max(0, ordinal - keepDepth)`.
              //
              // Pre-Fix-B behaviour kept the canonical chain end-to-end (filter retained
              // `canonicalHashes.contains(h) || !walkReachedGenesis` regardless of ordinal),
              // which leaked O(chain length) over the 12h soak. Post-Fix-B the canonical
              // chain is bounded to a sliding window of `keepDepthBehindFinalized` ordinals
              // behind finality. Reads at older ordinals fall through to disk via
              // `SnapshotStorage.get(hash)` / `SnapshotStorage.getHash(ordinal)` in the
              // existing `walkBackTo` impl and the `getByOrdinal` callers that already
              // disk-first.
              val keepFloor = math.max(0L, ordinal - keepDepthBehindFinalized)

              // Prune predicate:
              //   - keep anything strictly above the finalized ordinal (these are tentative
              //     successors; eviction happens only at-or-below finality)
              //   - keep canonical-chain entries down to `keepFloor`
              //   - if the canonical walk broke before reaching genesis, keep entries at-or-
              //     above `keepFloor` even if not on the just-walked canonical chain (we can't
              //     verify what's canonical below the break point, so be conservative within
              //     the keep-floor window; entries strictly below `keepFloor` are still
              //     evicted because they're below the finality-safety horizon regardless)
              val pruned = state.byHash.filter {
                case (h, s) =>
                  s.ordinal > ordinal ||
                  (canonicalHashes.contains(h) && s.ordinal >= keepFloor) ||
                  (!walkReachedGenesis && s.ordinal >= keepFloor)
              }
              val prunedCount = state.byHash.size - pruned.size

              // Approximate-bytes-dropped estimate for the CHAINSTORE-EVICT log line.
              // `Signed[GlobalIncrementalSnapshot] + GlobalSnapshotInfo` ≈ 1.4 MiB per
              // entry in the 12h soak's measurement (with 4 metagraphs). The constant is
              // intentionally illustrative — call it a back-of-envelope tracker, not a
              // precise size accountant.
              val approxBytesFreedMiB = (prunedCount.toLong * 1400L) / 1024L

              // CRITICAL: if the previous best tip got pruned (it was on a fork branch that
              // lost finality), clear bestTipHash so subsequent stores reseed correctly. The
              // alternative — leaving bestTipHash dangling — caused NakamotoChainStore.store
              // to throw NoSuchElementException on the next call. See task #9.
              val newBestTip = state.bestTipHash.filter(pruned.contains)
              val bestTipCleared = state.bestTipHash.isDefined && newBestTip.isEmpty

              (
                state.copy(byHash = pruned, bestTipHash = newBestTip, lastFinalizedOrdinal = ordinal),
                logger.info(
                  s"🔒 Finalized ordinal=$ordinal, pruned $prunedCount orphan snapshots (${pruned.size} remaining)" +
                    (if (bestTipCleared) s" [best tip cleared — was on pruned fork branch]" else "")
                ) >>
                  (if (prunedCount > 0)
                     logger.info(
                       s"CHAINSTORE-EVICT ordinal=$ordinal keepFloor=$keepFloor " +
                         s"keepDepthBehindFinalized=$keepDepthBehindFinalized dropped=$prunedCount " +
                         s"remaining=${pruned.size} ~freedKiB=$approxBytesFreedMiB"
                     )
                   else
                     logger.debug(
                       s"CHAINSTORE-EVICT ordinal=$ordinal keepFloor=$keepFloor no-op (nothing below floor)"
                     ))
              )
            }
          }.flatten

        def walkBackTo(startHash: Hash, targetOrdinal: Long): F[Option[Hash]] =
          stateRef.get.flatMap { state =>
            // Walk in-memory chain first
            var current = state.byHash.get(startHash)
            while (current.isDefined && current.get.ordinal > targetOrdinal)
              current = state.byHash.get(current.get.parentHash)

            current.filter(_.ordinal == targetOrdinal).map(_.hash) match {
              case some @ Some(_) => Async[F].pure(some)
              case None           =>
                // In-memory chain broke (gap from catch-up/reorg).
                // Fall back to disk: read snapshot at targetOrdinal directly.
                import eu.timepit.refined.types.numeric.NonNegLong
                HasherSelector[F].withCurrent { implicit hasher =>
                  underlyingStorage.getHash(SnapshotOrdinal(NonNegLong.unsafeFrom(targetOrdinal)))
                }
            }
          }

        def getByOrdinal(ordinal: Long): F[Option[StoredSnapshot]] =
          stateRef.get.map(_.byHash.values.find(_.ordinal == ordinal))

        def size: F[Int] =
          stateRef.get.map(_.byHash.size)

        val tree: ParentChildTree[F] = pcTree

        def tipFor(hash: Hash): F[Option[ChainTip]] =
          stateRef.get.map(_.byHash.get(hash).map { stored =>
            ChainTip(
              hash = stored.hash,
              slot = Slot(NonNegLong.unsafeFrom(stored.slot)),
              ordinal = stored.ordinal,
              parentHash = stored.parentHash,
              vrfOutput = VrfOutput(Hex(stored.vrfOutput.map("%02x".format(_)).mkString))
            )
          })

        /** Persist to underlying storage by extending the linear chain */
        private def persistLinear(stored: StoredSnapshot)(implicit hasher: Hasher[F]): F[Unit] =
          underlyingStorage.prepend(stored.signedSnapshot, stored.context).flatMap {
            case true  => logger.debug(s"💾 Prepended ordinal=${stored.ordinal} to linear storage")
            case false =>
              // prepend failed (parent mismatch) — fall back to setHead
              logger.warn(s"⚠️ prepend failed for ordinal=${stored.ordinal}, setting head for reorg") >>
                underlyingStorage.setHeadForRecovery(stored.signedSnapshot, stored.context).void
          }

        /** Persist during reorg — always uses setHead since we're switching chains */
        private def persistHead(stored: StoredSnapshot, hash: Hash)(implicit hasher: Hasher[F]): F[Unit] =
          underlyingStorage.setHeadForRecovery(stored.signedSnapshot, stored.context) >>
            logger.debug(s"💾 Set head to ordinal=${stored.ordinal} hash=${hash.value.take(8)}")

        def divergentRefuseCount: F[Long] =
          divergentRefuseCounterRef.get

        def divergentRefuseSample: F[Option[(Long, Hash)]] =
          divergentRefuseSampleRef.get

        def unsafe_clearFinality: F[Boolean] =
          for {
            before <- stateRef.get
            // Capture pre-reset state so the WARN log surfaces what was dropped. Useful for
            // post-mortem analysis ("we self-finalized hash X at ord N, then re-bootstrapped").
            preChainSize = before.byHash.size
            preFinalized = before.lastFinalizedOrdinal
            preBestTip = before.bestTipHash
            _ <- stateRef.set(ChainState.empty)
            _ <- divergentRefuseCounterRef.set(0L)
            _ <- divergentRefuseSampleRef.set(None)
            _ <- nakamotoFinalizedOrdinalRef.set(SnapshotOrdinal.MinValue)
            // Track-3 S1.5: reset the DISTINCT k₂ settled marker in lock-step with the k₁ finalized ref. Resetting both together
            // preserves the `settled ≤ finalized` invariant through a re-bootstrap (leaving settled high while finalized drops to
            // MinValue would transiently violate it). This ref is shared with `SettledOrdinalTracker`, whose only other writer is
            // the monotone `T_depth2` sink — so this reset is the one place the settled marker can move backward, and it is gated by
            // the same `unsafe_` re-bootstrap contract as the finalized reset.
            _ <- nakamotoSettledOrdinalRef.set(SnapshotOrdinal.MinValue)
            anyCleared = preChainSize > 0 || preFinalized > 0 || preBestTip.isDefined
            _ <-
              if (anyCleared)
                logger.warn(
                  s"⚠️ CHAIN-STORE-UNSAFE-RESET: dropped $preChainSize stored snapshots, " +
                    s"lastFinalizedOrdinal=$preFinalized→0, bestTip=${preBestTip.fold("none")(_.value.take(12))}→none " +
                    s"(re-bootstrap recovery). Caller must re-seed via chainStore.store from canonical chain."
                )
              else
                logger.info("CHAIN-STORE-UNSAFE-RESET: no-op (chain store already empty)")
          } yield anyCleared
      }
    }
  }
}
