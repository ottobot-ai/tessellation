package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Semaphore
import cats.syntax.all._

import scala.annotation.tailrec
import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaTrie
import io.constellationnetwork.security.mpt.producer.{MerklePatriciaError, StatefulMerklePatriciaProducer}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.implicits._

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Branch identity within the overlay. Reuses chain-store snapshot identity (the `Hash` of a snapshot/block tip) so that callers can pass
  * `chainStore.bestTip` (or any known parent hash) without any conversion. The opaque-ness target of the rev3 plan is approximated here as
  * a value-class wrapper — Scala 2.13 has no opaque types, but the wrapper preserves "do not concatenate / mistakenly compare with raw
  * Hash".
  */
final case class BranchId(value: Hash) extends AnyVal

object BranchId {

  /** Sentinel id reserved for the passthrough overlay's single notional branch. Not a real chain-store hash; passthrough callers do not
    * depend on its exact value, only on the fact that it is stable for the lifetime of the overlay.
    */
  val passthrough: BranchId = BranchId(Hash.empty)

  /** Conventional id for "no pending branch — I just want to read the finalized base." Any `BranchId` not present in the multi-branch
    * overlay's pending map resolves to the base, so this is purely a documentation alias.
    */
  val base: BranchId = BranchId(Hash.empty)
}

/** Outcome of `MptOverlay.finalizeBranch`. The plan (#56.6) defines this as the single return value the finality sink reads after each
  * call.
  */
sealed trait FinalizationOutcome
object FinalizationOutcome {

  /** A branch's deltas were folded forward into the on-disk base, and zero or more non-canonical branches were dropped.
    *   - In multi-branch mode, `keysApplied` counts the distinct trie keys touched (`upserts.size + removals.size` of the merged
    *     chain-of-deltas). `branchesDropped` counts pending branch entries removed from the overlay other than the canonical chain itself.
    *   - In passthrough mode, this is never returned — the passthrough always returns `NoOp` because there is no overlay state to fold.
    */
  final case class Folded(keysApplied: Int, branchesDropped: Int) extends FinalizationOutcome

  /** Idempotency / passthrough sentinel: nothing to fold. Returned when `(ordinal, hash)` already finalized at the same canonical, or when
    * the overlay is in passthrough mode and the underlying `MptStore` already holds the canonical state, or when the requested branch isn't
    * tracked in the overlay (so there's nothing to apply).
    */
  case object NoOp extends FinalizationOutcome
}

/** Outcome of `MptOverlay.revertToOrdinal` (Track-3 S4 revert-executor). Reverting the on-disk base to a fork ordinal so a denser branch
  * can be re-folded takes ONE of two disjoint mechanical paths, distinguished here for telemetry / the caller's re-fold bookkeeping:
  */
sealed trait RevertOutcome
object RevertOutcome {

  /** SHALLOW path: the fork ordinal was inside the in-memory `undoJournalRef` window, so the base was reverted by replaying the per-ordinal
    * reverse deltas in DESCENDING (LIFO) order — `undoStepsApplied` counts the reverse deltas applied (one per reverted ordinal). Each
    * reverse delta was captured against the base as it stood at ITS OWN fold, so descending order is load-bearing.
    */
  final case class Shallow(undoStepsApplied: Int) extends RevertOutcome

  /** DEEP path: the fork ordinal was below the in-memory RAM window (the journal could not reach it), so the base was rebuilt from the
    * disk-retained signed bytes at the fork ordinal — `deleteAbove(fork)` + `loadBytes(readState(fork))`. `baseEntriesLoaded` counts the
    * entries in the loaded byte map. Requires the disk tier (Track-3 S2 contiguous `signedBytesStore` to k₂) to hold `fork`.
    */
  final case class Deep(baseEntriesLoaded: Int) extends RevertOutcome

  /** Nothing to revert: the base is already at-or-below the fork ordinal (idempotent second call, or a fork at/above the current tip). In
    * passthrough mode this is ALWAYS returned (there is no overlay base to revert).
    */
  case object NoOp extends RevertOutcome
}

/** Fail-closed error raised by `MptOverlay.revertToOrdinal` when the fork ordinal is reachable via NEITHER the in-memory undo journal (the
  * journal has been pruned below it) NOR the disk tier (the contiguous signed-bytes store no longer retains `forkOrdinal` — it is deeper
  * than k₂, or the deep reader is unwired). Raising (rather than silently under-reverting to the nearest reachable ordinal) is deliberate:
  * a base that reverted LESS than requested would re-fold the denser branch onto a mismatched anchor and silently diverge — the exact
  * consensus-safety hazard S4 exists to prevent.
  */
final case class RevertGapError(
  forkOrdinal: SnapshotOrdinal,
  currentTip: Option[SnapshotOrdinal],
  reason: String
) extends RuntimeException(
      s"MptOverlay.revertToOrdinal: cannot reach fork ordinal=${forkOrdinal.value.value} " +
        s"(current base tip=${currentTip.map(_.value.value).map(_.toString).getOrElse("none")}); $reason. " +
        s"Failing closed rather than under-reverting."
    )

/** Mutable handle for accumulating writes against a checked-out branch.
  *
  * In passthrough mode the writes go straight to the underlying `MptStore`. In multi-branch mode the writes accumulate into a `ChangeSet`
  * keyed by `BranchId`, and only `commit` materializes the per-branch view.
  *
  * `BranchHandle` is intentionally narrower than `MptStore` — only mutations needed during snapshot construction. Reads always go through
  * the overlay (`MptOverlay.get` / `getAllForPrefix`) keyed by `BranchId`, since the value visible at a branch is `base ⊕ chain-of-deltas`,
  * not `base + uncommitted local writes`.
  */
trait BranchHandle[F[_], K] {
  def insert[V: ImmutableCodec](key: K, value: V): F[Unit]
  def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit]
  def remove(key: K): F[Unit]
  def remove(keys: List[K]): F[Unit]
  def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit]

  /** The branch this handle was checked out against — provided so callers can later pass it to `commit` / `get` without bookkeeping. */
  def parent: BranchId
}

/** Branch-aware view over an `MptStore`.
  *
  * Goal (rev3 plan, #56): each pending branch gets its own state view (deltas over a finalized base). Only the finalized state is
  * persisted; pending branches live in memory. When finality advances, the canonical branch's diffs apply forward; non-canonical branches
  * are dropped.
  *
  * '''Layering''': WRAP, not parameterize. The 14+ existing `MptStore` call sites stay untouched until callers are migrated to the overlay
  * one at a time. `base` is exposed so legacy paths can keep working during migration.
  *
  * Two impls live behind `MptOverlay.make(enabled, ...)`:
  *   - '''passthrough''' (#56.2): correctness-equivalent to direct `MptStore` use; `BranchId` is ignored.
  *   - '''multi-branch''' (#56.4): per-branch `ChangeSet` accumulation with sibling isolation, lazy fall-through to base on read miss, and
  *     fold-forward on finalize.
  *
  * '''System-index / per-field root contract (#56.4.5)''':
  *   - The overlay accumulates writes as raw `Hex → Array[Byte]` pairs without distinguishing partition kind. Rooted System partitions
  *     (`ActiveAddressIndex`, expiry indices for AllowSpend / TokenLock / NodeCollateral) and user-field partitions (Balances, LastTxRefs,
  *     etc.) all flow through the same `ChangeSet`. They share the producer's hex keyspace.
  *   - '''Raw overlay root''' (`buildRoot(branch, ordinal)`): includes every stored partition and is composed via
  *     `MerklePatriciaTrie.withChanges` from the on-disk base trie at `ordinal`. The signed consensus `mptRoot` must instead be computed
  *     from the branch byte view through `GlobalStateKey.consensusRootEntries`: that retains every economic System index and temporarily
  *     excludes field 32. A raw overlay root is therefore not itself state-proof authority.
  *   - '''Per-field root algorithm already exists''' at `GlobalStateConverter.buildPerFieldMptRoots`: it groups `Map[GlobalStateKey,
  *     Array[Byte]]` by `_._1.fieldId` (structured component of `GlobalStateKey`, not a Hex-prefix slice) and builds a fresh
  *     `MerklePatriciaTrie.makeParallelFromBytes` per group. Consensus-index entries land under `GlobalStateFieldId.SystemIndex`; they are
  *     produced by `buildPerFieldMptRoots` but the consumer in `GlobalSnapshotInfo.stateProofBuilder` only reads user-visible fieldIds
  *     (`Balances`, `LastTxRefs`, etc.) via `fieldRoot(id)` lookups. System indices have no exposed per-field slot but remain covered by
  *     the aggregate consensus `mptRoot`.
  *   - '''#56.5 work''': re-source the input map (`Map[GlobalStateKey, Array[Byte]]`) from the OVERLAY's branch-scoped view (so a pending
  *     branch's stateProof reflects its deltas), then feed it into the existing `buildPerFieldMptRoots`. The algorithm doesn't change; the
  *     producer of the kvPairs does.
  *   - '''Finalization atomicity''': a single `finalizeBranch` writes ALL partition deltas (rooted System indices + user fields) to the
  *     base in one savepoint-bracketed transaction (`MptStore.withTransaction`). If the apply fails mid-stream, the base rolls back to its
  *     pre-call state. There is no partial-partition state — the contract is all-or-nothing across the entire merged chain.
  */
trait MptOverlay[F[_], K] {

  /** The underlying finalized-state store. Exposed for migration / non-overlay-aware call sites. New code should prefer overlay reads. */
  def base: MptStore[F, K]

  /** Block-tree topology shared with consensus. Reused — no separate tree maintained by the overlay. */
  def parentChildTree: ParentChildTree[F]

  /** Check out a writable handle whose writes will become a child of `parent`. In multi-branch mode the returned handle accumulates writes
    * into a per-handle `ChangeSet`; reads via the overlay against any other branch do NOT see these writes until `commit` registers them.
    */
  def checkout(parent: BranchId): F[BranchHandle[F, K]]

  /** Commit the writes accumulated on `branch` as the child branch identified by `childTip` at `ordinal`. Multi-branch: registers the
    * accumulated `ChangeSet` keyed by `childTip` and links `(parent → childTip)` in `parentChildTree`. Passthrough: associates the tip in
    * the parent-child tree (so consensus topology stays accurate) and otherwise no-ops — writes already landed in the underlying store.
    */
  def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit]

  /** Re-key a previously-committed pending branch from `oldChildTip` to `newChildTip`. Used by the Nakamoto leader path: `accept()` /
    * `createProposalArtifact` commits the handle at `hash(rawArtifact)` (the artifact returned by the acceptance pipeline). The leader then
    * mutates the artifact (`copy(slotCertificate = Some(cert), eta = Some(eta))`) before signing and persisting; the chain's canonical
    * reference for ordinal N is the with-cert hash, but the overlay branch is keyed by the pre-cert hash. Without rekeying, ordinal N+1's
    * `checkout(BranchId(getLastArtifactHash))` fails to find the parent and falls through to base, missing N's pending writes — surfaces as
    * `priorLastCurrencySnapshots`-empty divergence. Multi-branch: looks up the entry under `oldChildTip`; if found, removes it and inserts
    * at `newChildTip` (preserving `parent`, `changes`, `ordinal`); also associates `newChildTip → entry.parent` in `parentChildTree`.
    * Passthrough: no-op (no `pendingRef`).
    *
    * Idempotent: if `oldChildTip` is not present (already rekeyed or never registered) and `newChildTip` is not present, this is a no-op.
    */
  def rekey(oldChildTip: BranchId, newChildTip: BranchId): F[Unit]

  /** Drop a previously-committed pending branch. Used by the Nakamoto leader path when production is abandoned (gate closed before sign or
    * `chainStore.store` refused) — the rawArtifact's branch was committed inside `createProposalArtifact` but the snapshot will never reach
    * the chain, so the `pendingRef` entry is garbage. Multi-branch: removes from `pendingRef` (parent-child tree edges remain — they're
    * pure topology and don't affect reads). Passthrough: no-op. Idempotent: missing branch is fine.
    */
  def discardBranch(branchId: BranchId): F[Unit]

  /** Read at a specific branch view. Multi-branch: walks `branch → ... → base` through the pending-branches map, returning the first delta
    * hit (upsert or removal); falls through to `MptStore.get` on miss. Passthrough: ignores `branch`, delegates to `MptStore.get`.
    */
  def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]]

  /** Strict counterpart to [[get]] that preserves malformed stored bytes instead of collapsing them into absence. Pending upserts and base
    * reads use the same decoder; an explicit pending removal is [[StrictMptRead.Absent]].
    */
  def getStrict[V: ImmutableCodec](branch: BranchId, key: K): F[StrictMptRead[V]]

  /** Prefix scan at a specific branch view. Multi-branch: composes base prefix-scan with the chain's accumulated upserts (decoded to V) and
    * removals (filtered to `prefix`). Passthrough: ignores `branch`, delegates to `MptStore.getAllForPrefix`.
    */
  def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]]

  /** Branch-view prefix scan which retains every nibble-prefix-matched physical key and malformed value in deterministic key order. This
    * parser does not prove that `branch` exists; exact-parent availability and whole-image physical-key grammar validation remain separate
    * activation gates.
    */
  def getAllForPrefixStrict[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[List[StrictMptEntry[V]]]

  /** Build the per-branch trie at `ordinal`. Multi-branch: takes the on-disk base trie at `ordinal` and applies the chain's merged
    * `ChangeSet` via `MerklePatriciaTrie.withChanges`. Passthrough: ignores `branch`, delegates to `MptStore.build`.
    */
  def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]]

  /** Read all entries as raw `Map[Hex, Array[Byte]]` at a specific branch view. Multi-branch: composes base entries with the chain's
    * accumulated upserts and removals (`mergedChain` walked leaf→root). Passthrough: ignores `branch`, delegates to
    * `MptStore.allEntriesAsBytes`. Used by GSAM's verify-replay cross-check to obtain the parent-branch view bytes that the replay (`prev ⊖
    * removes ⊕ upserts`) applies the delta on top of — under MultiBranch, reading raw `underlying.allEntriesAsBytes` is wrong because
    * pending writes in the parent's chain aren't folded into base yet.
    */
  def allEntriesAsBytes(branch: BranchId): F[Map[Hex, Array[Byte]]]

  /** Defensively copied raw branch-view enumeration. Multi-branch capture is serialized against overlay mutation, but not against callers
    * that bypass the overlay and mutate the base store directly. This does not authenticate branch identity, root, or finality.
    */
  def allEntriesStrict(branch: BranchId): F[List[StrictMptRawEntry]]

  /** Read all entries as raw `Map[Hex, Array[Byte]]` including a checked-out handle's pending writes (i.e. the post-write view BEFORE
    * `commit` registers them in the overlay). Multi-branch: equivalent to `allEntriesAsBytes(handle.parent)` merged with handle's
    * accumulator. Passthrough: handle writes have already landed in `underlying` (no accumulator), so this delegates to
    * `MptStore.allEntriesAsBytes`.
    *
    * Used by GSAM's proof construction (Phase J): the `mptRoot` baked into the snapshot's `GlobalSnapshotStateProof` must reflect the
    * post-write state — under MultiBranch, that means base + parent chain + the handle's accumulated `ChangeSet`. The legacy
    * `producer.getRootHashForOrdinal` path goes stale under MultiBranch because the producer has no per-ordinal commit until
    * `finalizeBranch.foldIntoBase`.
    */
  def allEntriesAsBytesWithHandle(handle: BranchHandle[F, K], ordinal: SnapshotOrdinal): F[Map[Hex, Array[Byte]]]

  /** Finality sink (#56.6 wires the call site). Idempotent on `(ordinal, canonical)`. When called at the same `ordinal` with a different
    * `canonical` hash (reorg replay — followers re-pull through `setForRecovery`), accepts the new canonical, folds its pending chain
    * (idempotent: no-op if nothing pending) and updates the finality marker. The reorg-replace path is essential for follower recovery
    * (#113); without it, every reorg crashed `GlobalSnapshotAlignment` and the node was stuck. NakamotoSyncDaemon (gl0 leader path) is
    * still expected never to hit this case in practice — depth-k confirmation precludes a different-hash re-finalization at the same ord —
    * so the path is benign for gl0.
    *
    * Multi-branch: walks `canonical → ... → base` through the pending-branches map, merges all chain `ChangeSet`s, applies the merged delta
    * to the underlying producer, and clears the entire pending-branches map. (More nuanced eviction — keeping non-conflicting siblings —
    * lands in #56.9.) Passthrough: always returns `NoOp` because there is no overlay state to fold.
    */
  def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome]

  /** Local in-memory retention prune. Current callers include legacy k1/T_depth2 watermarks, but neither watermark is a protocol Phase 3 or
    * an absolute no-reorg floor. Target P2 remains density-reorgable; callers may drop RAM history only when exact authenticated disk/proof
    * reconstruction remains available, otherwise a deeper required comparison must enter `RecoveryRequired`.
    *
    * Pruning is purely a memory bound — the production code path produces identical `mptRoot`s before and after the call. Idempotent: a
    * second call with the same `ord` is a no-op; a call with a lower `ord` is a no-op for entries already pruned.
    *
    * Multi-branch: prunes `undoJournalRef` and `finalizedRef` entries below `ord`. This operation is not a consensus decision and must not
    * destroy the only exact reconstruction path. Passthrough: no-op.
    */
  def pruneBelow(ord: SnapshotOrdinal): F[Unit]

  /** Current in-memory sizes of the overlay's history accumulators (Track-3 S2 telemetry). Read-only, allocation-light snapshot — NO
    * `Metrics[F]` dependency is threaded into `make`; instead a caller that already has `Metrics[F]` (`SnapshotLeaderLoop.finalityMonitor`)
    * reads this each finality tick, emits `dag_nakamoto_overlay_undo_journal_size` from `undoJournal`, and increments an over-bound counter
    * when `JournalSizes.overBound(k₁)` holds.
    *
    * The `undoJournal` count is the load-bearing signal: it is the IN-MEMORY reverse-delta window that MUST stay bounded to the operational
    * k₁ fast-path (the Heap-leak Fix A prune calls `pruneBelow` at the k₁ watermark). The disk-backed signed-bytes store holds the deep
    * history to k₂ (Track-3 S2) — RAM does NOT. If `undoJournal` ever climbs past the k₁ window, the RAM journal is leaking toward k₂ (the
    * regression S2's disk/RAM split exists to prevent), and the over-bound counter fires as the sentinel.
    *
    * Multi-branch: live sizes of `undoJournalRef` / `finalizedRef` / `pendingRef`. Passthrough: all-zero (no per-branch in-memory history).
    */
  def journalSizes: F[MptOverlay.JournalSizes]

  /** Re-bootstrap escape hatch (P-11, task #141). Clear `pendingRef`, `finalizedRef`, `lastCommittedBranchRef`, and `undoJournalRef`
    * unconditionally. The underlying base `MptStore` is NOT touched — the caller (`RebootstrapOrchestrator`) is responsible for resyncing
    * base via `mptStore.syncFromGlobalSnapshotInfo` after this returns. Equivalent to "fresh overlay over the existing base".
    *
    * Breaks the all-or-nothing finality contract (`finalizeBranch` walks `pendingRef`+`undoJournalRef` consistently) — the prefix `unsafe_`
    * signals callers must hold the mutex-equivalent (production paused, no in-flight commits) before invoking.
    *
    * Multi-branch: clears all four Refs. Passthrough: no-op (no per-branch state to reset).
    */
  def unsafe_reset: F[Unit]

  /** Track-3 S4 revert-executor. Revert the on-disk base to the state as of `forkOrdinal` so a denser branch (deep density reorg in the
    * `(k₂, k₁]` band, S3) can be re-folded onto it. This method ONLY reverts the base + clears pending overlay state; the RE-FOLD itself is
    * performed by the caller re-driving the denser branch's snapshots through the ordinary `commit` / `finalizeBranch` fold path, so the
    * follower-verified consensus `mptRoot` matches by construction (no bespoke re-apply).
    *
    * Two disjoint mechanical paths, chosen by whether `forkOrdinal` is reachable from the IN-MEMORY reverse-delta journal:
    *   - '''SHALLOW''' — `forkOrdinal` is within the bounded RAM `undoJournalRef` window (Heap-leak Fix A keeps it to the operational k₁):
    *     replay the per-ordinal reverse deltas for every ordinal above `forkOrdinal` in DESCENDING (LIFO) order via the byte-deterministic
    *     `applyUndoAt`. Descending order is load-bearing — each reverse delta was captured against the base at ITS OWN fold.
    *   - '''DEEP''' — `forkOrdinal` is below the RAM window (journal pruned, cannot reach): rebuild the base from the disk tier — prune the
    *     producer's on-disk state above `forkOrdinal` (`deleteAbove`), read the disk-retained signed bytes at `forkOrdinal` (Track-3 S2's
    *     contiguous `signedBytesStore` to k₂), and load them VERBATIM as the base (`loadBytes`, same primitive follower resync uses, so the
    *     recomputed root equals the signed root by construction).
    *
    * '''Fail-closed''': if `forkOrdinal` is reachable via NEITHER path (journal pruned below it AND the disk tier no longer retains it —
    * deeper than k₂ or the deep reader is unwired), this raises [[RevertGapError]] rather than silently under-reverting to the nearest
    * reachable ordinal (which would re-fold onto a mismatched anchor and diverge).
    *
    * After reverting, `pendingRef` is cleared and `lastCommittedBranchRef` is reset (mirroring the finalize reorg-replace arms) so the
    * subsequent re-fold starts from a clean overlay, and the base-revert hook (eta `forgetUncommitted`) fires. Runs under the same mutex as
    * `finalizeBranch` / `pruneBelow`. Passthrough: always [[RevertOutcome.NoOp]] (no overlay base to revert).
    */
  def revertToOrdinal(forkOrdinal: SnapshotOrdinal): F[RevertOutcome]
}

object MptOverlay {

  /** Read-only snapshot of the overlay's in-memory history-accumulator sizes (Track-3 S2 memory-budget telemetry). Returned by
    * `MptOverlay.journalSizes`.
    *
    *   - `undoJournal`: entries in `undoJournalRef` — the per-ordinal reverse-delta RAM window (#121). The bounded fast-path that Fix A
    *     prunes at the operational k₁ watermark. This is the count the `dag_nakamoto_overlay_undo_journal_size` gauge tracks.
    *   - `finalizedMarkers`: entries in `finalizedRef` (cross-ordinal conflict-detection markers, also k₁-bounded by Fix A).
    *   - `pendingBranches`: entries in `pendingRef` (live pending branches, bounded by the eviction cap — not by prune).
    */
  final case class JournalSizes(undoJournal: Int, finalizedMarkers: Int, pendingBranches: Int) {

    /** True when the RAM undo-journal has grown past its intended fast-path window `windowBound` (the operational k₁). A healthy node sits
      * far below k₁ (folds happen at depth k₁ and Fix A prunes at the k₁ watermark, so only a thin band survives); exceeding k₁ means the
      * prune is not keeping the journal bounded and RAM is drifting toward the k₂ disk depth — the leak S2's disk/RAM split prevents.
      */
    def overBound(windowBound: Long): Boolean = undoJournal.toLong > windowBound
  }

  object JournalSizes {
    val empty: JournalSizes = JournalSizes(0, 0, 0)
  }

  /** Per-branch entry in the overlay's pending-branches map. Lives only in memory — restart wipes pending state and the node resyncs from
    * the finalized base + `ChainSync`.
    */
  private final case class BranchEntry(
    parent: BranchId,
    changes: ChangeSet,
    ordinal: SnapshotOrdinal
  )

  /** Default cap on pending branches. Per the rev3 plan: 4 branches × 32 ordinals × ~50 KB ≈ ~6 MB. Eviction (#56.9) drops the
    * lowest-scoring non-canonical branch when this is exceeded.
    */
  val DefaultMaxPendingBranches: Int = 4

  /** Operating mode of an `MptOverlay`. Replaces the rev3 `enabled: Boolean` flag (#56.10 Phase J).
    *
    *   - `Passthrough`: correctness-equivalent to direct `MptStore` use. The `BranchId` argument is ignored on every call; commits land in
    *     the underlying store immediately. Used during the migration window so e2e/unit tests can prove parity between old and new paths.
    *   - `MultiBranch(maxPendingBranches)`: per-branch `ChangeSet` accumulation with sibling isolation. Pending state lives in memory; only
    *     `finalizeBranch` folds it into the underlying store. Cap on pending branches — eviction (#56.9) drops the lowest-scoring
    *     non-ancestor branch on commit when exceeded.
    *
    * Transient: this ADT is migration scaffolding, not a permanent rollout flag. After #56.11 the only valid mode is `MultiBranch` and the
    * sealed trait can be retired entirely.
    */
  sealed trait OverlayMode extends Product with Serializable
  object OverlayMode {
    case object Passthrough extends OverlayMode
    final case class MultiBranch(maxPendingBranches: Int) extends OverlayMode

    /** Default for production wiring: multi-branch with the canonical cap. Used by `SharedStorages` once accept() has migrated to the
      * overlay (Phase D). Until then production wiring stays on `Passthrough` to preserve byte-for-byte parity with the legacy path.
      */
    val productionDefault: OverlayMode = MultiBranch(DefaultMaxPendingBranches)
  }

  /** Factory dispatching on `OverlayMode`.
    *
    * `toHex` is plumbed in alongside `underlying` so the multi-branch impl can encode keys before the per-handle accumulator stores them.
    * For passthrough, `toHex` is unused — kept on the signature so swapping modes doesn't change call sites.
    *
    * `bestTipsFn` (#56.9, multi-tip in #115): callback the overlay queries during eviction to identify ancestors of every viable chain head
    * — ancestors of any tip in the returned set are NEVER evicted. Pass `Async[F].pure(Set.empty[BranchId])` for "no ancestor protection"
    * (purely score-based eviction); production deployments should plumb this from `chainStore.allTips` (Phase I) so depth-k /
    * attestation-2/3 finality can still walk back through pending branches AND fork-recovery doesn't lose the canonical chain when the
    * local-fork chain is bestTip.
    */
  def make[F[_]: Async: Hasher, K](
    mode: OverlayMode,
    underlying: MptStore[F, K],
    pcTree: ParentChildTree[F],
    toHex: K => F[Hex],
    bestTipsFn: F[Set[BranchId]],
    // Track-3 S4 DEEP revert-executor source: the disk tier that retains the signed per-ordinal byte map CONTIGUOUSLY to k₂ (production =
    // `signedBytesStore.readState`, wired post-construction from `GlobalSnapshotConsensus`). `revertToOrdinal(fork)` uses it when `fork`
    // is below the RAM undo-journal window. `None` = "no deep tier" (a below-window revert fails closed), which keeps the passthrough /
    // follower / test wiring byte-identical to before S4. (`Option` rather than a defaulted function because a default value cannot see the
    // method's own `Async[F]` implicit — the concrete "always-None" reader is materialized below where `Async` is in scope.)
    deepStateReader: Option[SnapshotOrdinal => F[Option[Map[Hex, Array[Byte]]]]] = None,
    // Track-3 S4 base-revert hook: fired inside every base-reverting overlay path (`revertToOrdinal` + the finalize reorg-replace arms).
    // Production wiring passes `etaStateManager.forgetUncommitted` (deferred, set post-construction from `SharedServices`) so a reverted
    // base drops the stale in-process eta walk cache. `None` = no-op (byte-identical to pre-S4).
    onBaseRevert: Option[F[Unit]] = None
  ): F[MptOverlay[F, K]] =
    mode match {
      case OverlayMode.Passthrough => Async[F].pure(passthrough(underlying, pcTree))
      case OverlayMode.MultiBranch(maxPending) =>
        MultiBranch[F, K](
          underlying,
          pcTree,
          toHex,
          maxPending,
          bestTipsFn,
          deepStateReader.getOrElse((_: SnapshotOrdinal) => Async[F].pure(none[Map[Hex, Array[Byte]]])),
          onBaseRevert.getOrElse(Async[F].unit)
        )
    }

  /** Single-branch passthrough — correctness-equivalent to using `MptStore` directly. The `BranchId` argument on every method is ignored.
    * `commit` registers the branch in `parentChildTree` so consensus topology stays consistent with what multi-branch mode needs.
    */
  def passthrough[F[_]: Async, K](
    underlying: MptStore[F, K],
    pcTree: ParentChildTree[F]
  ): MptOverlay[F, K] =
    new MptOverlay[F, K] {
      def base: MptStore[F, K] = underlying
      def parentChildTree: ParentChildTree[F] = pcTree

      def checkout(parentBranch: BranchId): F[BranchHandle[F, K]] =
        Async[F].pure(new PassthroughHandle[F, K](underlying, parentBranch))

      def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit] =
        // Writes already landed in `underlying` during the handle's lifetime; this just performs the
        // topology bookkeeping and the per-ordinal trie checkpoint that legacy `syncFromStateChanges`
        // used to do at its tail. Without the checkpoint, `builder.buildProof` (which calls
        // `producer.buildForOrdinal` + `producer.getRootHashForOrdinal`) cannot resolve the just-written
        // ordinal — that's the post-write contract the algebra has to honor when the GSAM rewire (#56.10
        // Phase D) routes through `overlay.commit` instead of `mptStore.syncFromStateChanges`. Multi-branch
        // mode does the equivalent inside `finalizeBranch.foldIntoBase`.
        //
        // Skip self-association if checkout was given the same id (e.g. genesis bootstrap), which would
        // otherwise create a self-loop in the parent-child tree.
        for {
          _ <- underlying.commit(ordinal)
          _ <-
            if (branch.parent.value === childTip.value) Async[F].unit
            else pcTree.associate(childTip.value, branch.parent.value)
        } yield ()

      def rekey(oldChildTip: BranchId, newChildTip: BranchId): F[Unit] =
        // Passthrough has no `pendingRef` — writes already landed in `underlying`. The rekey is a no-op.
        Async[F].unit

      def discardBranch(branchId: BranchId): F[Unit] =
        // Passthrough has no `pendingRef` — writes already landed in `underlying`. Discard is a no-op
        // here; correctness for abandoned proposals depends on the caller's `mptStore.withTransaction`
        // wrapper rolling back the underlying writes via savepoint, which is independent of this call.
        Async[F].unit

      def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]] =
        underlying.get(key)

      def getStrict[V: ImmutableCodec](branch: BranchId, key: K): F[StrictMptRead[V]] =
        underlying.getStrict(key)

      def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]] =
        underlying.getAllForPrefix[V](prefix)

      def getAllForPrefixStrict[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[List[StrictMptEntry[V]]] =
        underlying.getAllForPrefixStrict[V](prefix)

      def allEntriesAsBytes(branch: BranchId): F[Map[Hex, Array[Byte]]] =
        underlying.allEntriesAsBytes

      def allEntriesStrict(branch: BranchId): F[List[StrictMptRawEntry]] =
        underlying.allEntriesStrict

      def allEntriesAsBytesWithHandle(handle: BranchHandle[F, K], ordinal: SnapshotOrdinal): F[Map[Hex, Array[Byte]]] =
        // Passthrough: handle writes already landed in `underlying` (PassthroughHandle delegates directly).
        underlying.allEntriesAsBytes

      def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
        underlying.build(ordinal)

      def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome] =
        // Passthrough has no pending state, so finalization is always a no-op. The conflict-detection
        // contract (error on (ordinal, hashA) then (ordinal, hashB)) is meaningful only in multi-branch
        // mode where pending state actually exists; passthrough is invisible to the comparator.
        Async[F].pure(FinalizationOutcome.NoOp)

      def pruneBelow(ord: SnapshotOrdinal): F[Unit] =
        // Passthrough has no in-memory history (no `undoJournalRef`, no `pendingRef`, no `finalizedRef`);
        // writes already landed in `underlying` during the handle's lifetime. Trivially a no-op.
        Async[F].unit

      def journalSizes: F[MptOverlay.JournalSizes] =
        // Passthrough keeps no per-branch in-memory history — nothing to size.
        Async[F].pure(MptOverlay.JournalSizes.empty)

      def unsafe_reset: F[Unit] =
        // Passthrough has no in-memory state to drop; writes already landed in `underlying`.
        // The base `MptStore` reset is the caller's responsibility (`syncFromGlobalSnapshotInfo`).
        Async[F].unit

      def revertToOrdinal(forkOrdinal: SnapshotOrdinal): F[RevertOutcome] =
        // Passthrough keeps no per-branch overlay state and no reverse-delta journal — there is nothing for the
        // executor to revert. Base realignment on the passthrough path is the caller's job (follower resync via
        // `syncFromGlobalSnapshotInfo` / `loadBytes`). Always NoOp, mirroring `finalizeBranch`.
        Async[F].pure(RevertOutcome.NoOp: RevertOutcome)
    }

  private final class PassthroughHandle[F[_], K](
    underlying: MptStore[F, K],
    val parent: BranchId
  ) extends BranchHandle[F, K] {
    def insert[V: ImmutableCodec](key: K, value: V): F[Unit] = underlying.insert(key, value)
    def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit] = underlying.insert(entries)
    def remove(key: K): F[Unit] = underlying.remove(key)
    def remove(keys: List[K]): F[Unit] = underlying.remove(keys)
    def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] = underlying.update(toUpsert, toRemove)
  }

  /** Per-handle local accumulator. Mutates a `Ref[ChangeSet]` and is otherwise inert against `MptStore` until `commit` registers the
    * accumulated state in the overlay's pending-branches map.
    *
    * Encoding is identical to `MptStore.Impl.encode` so a key that flows through this handle and is later folded to base is byte-equivalent
    * to one written via `MptStore.insert` directly.
    */
  private final class MultiBranchHandle[F[_]: Async, K](
    toHex: K => F[Hex],
    val accRef: Ref[F, ChangeSet],
    val parent: BranchId
  ) extends BranchHandle[F, K] {

    private def encode[V: ImmutableCodec](v: V): Array[Byte] =
      ImmutableCodec[V].immutableBytes(v).toArray

    def insert[V: ImmutableCodec](key: K, value: V): F[Unit] =
      toHex(key).flatMap { hex =>
        val bytes = encode(value)
        accRef.update(cs => ChangeSet(cs.upserts.updated(hex, bytes), cs.removals - hex))
      }

    def insert[V: ImmutableCodec](entries: Map[K, V]): F[Unit] =
      if (entries.isEmpty) Async[F].unit
      else
        entries.toList.traverse { case (k, v) => toHex(k).map(_ -> encode(v)) }.flatMap { hexed =>
          val newPairs = hexed.toMap
          accRef.update(cs => ChangeSet(cs.upserts ++ newPairs, cs.removals -- newPairs.keySet))
        }

    def remove(key: K): F[Unit] =
      toHex(key).flatMap { hex =>
        accRef.update(cs => ChangeSet(cs.upserts - hex, cs.removals + hex))
      }

    def remove(keys: List[K]): F[Unit] =
      if (keys.isEmpty) Async[F].unit
      else
        keys.traverse(toHex).flatMap { hexes =>
          val toRemove = hexes.toSet
          accRef.update(cs => ChangeSet(cs.upserts -- toRemove, cs.removals ++ toRemove))
        }

    def update[V: ImmutableCodec](toUpsert: Map[K, V], toRemove: Set[K]): F[Unit] =
      for {
        upsertHexed <- toUpsert.toList.traverse { case (k, v) => toHex(k).map(_ -> encode(v)) }
        removeHexed <- toRemove.toList.traverse(toHex)
        upMap = upsertHexed.toMap
        rmSet = removeHexed.toSet
        // Apply removals first then upserts — matches `MerklePatriciaTrie.withChanges` ordering, so a key
        // present in BOTH `toUpsert` and `toRemove` ends up upserted (consistent with `MptStore.update`).
        _ <- accRef.update { cs =>
          val afterRemove = ChangeSet(cs.upserts -- rmSet, cs.removals ++ rmSet)
          ChangeSet(afterRemove.upserts ++ upMap, afterRemove.removals -- upMap.keySet)
        }
      } yield ()
  }

  /** Multi-branch overlay implementation.
    *
    * State:
    *   - `pendingRef: Map[BranchId, BranchEntry]` — every committed branch above the finalized base. A branch not in this map resolves to
    *     "the base" for read purposes.
    *   - `finalizedRef: Map[SnapshotOrdinal, BranchId]` — finalized canonical at each ordinal, for `finalizeBranch` idempotency and
    *     cross-ordinal conflict detection.
    *   - `mutex: Semaphore` — serializes `commit` and `finalizeBranch` against each other so the chain walk + base apply in
    *     `finalizeBranch` is atomic w.r.t. concurrent commits.
    *
    * Read path: `walkChainForKey` is a pure tail-recursion over `pendingRef.get`. It starts at the requested branch and walks up parent
    * pointers, returning:
    *   - `Some(None)` — the chain explicitly removed `key` at some level, so the answer is `None` (do NOT fall through).
    *   - `Some(Some(bytes))` — the chain has an upsert for `key`; deserialize and return.
    *   - `None` — no level in the chain mentions `key`, fall through to the base `MptStore`.
    *
    * Merge ordering convention (`mergedChain`): walking leaf→root, each parent's `ChangeSet` is `parent.merge(childAcc)`. `merge` is "later
    * wins", so if leaf is the most recent commit, `parent.merge(leaf)` correctly applies parent first then leaf — the chronological order.
    */
  private object MultiBranch {

    def apply[F[_]: Async: Hasher, K](
      underlying: MptStore[F, K],
      pcTree: ParentChildTree[F],
      toHex: K => F[Hex],
      maxPendingBranches: Int,
      bestTipsFn: F[Set[BranchId]],
      deepStateReader: SnapshotOrdinal => F[Option[Map[Hex, Array[Byte]]]],
      onBaseRevert: F[Unit]
    ): F[MptOverlay[F, K]] =
      (
        Ref.of[F, Map[BranchId, BranchEntry]](Map.empty),
        Ref.of[F, Map[SnapshotOrdinal, BranchId]](Map.empty),
        // `lastCommittedBranchRef` (#113): tracks the most-recently-committed branch's id.
        // Used as an additional eviction-protection tip alongside `bestTipsFn`. Solves the
        // lag race where `lastGlobalSnapshotStorage`-backed tips update AFTER persist,
        // so during the persist window the just-committed branches would be the only eviction
        // candidates and get dropped — exactly the freshest chain we need to keep.
        Ref.of[F, Option[BranchId]](none[BranchId]),
        // `undoJournalRef` (#121): per-ordinal reverse-delta against base. Every successful
        // `foldIntoBase(forward, ord)` captures the keys-touched portion of base BEFORE the
        // fold and stores a `ChangeSet` that, when applied to base, would undo the fold.
        // On reorg-replace finality (a different canonical re-finalizes ord N), the existing
        // entry is replayed first to restore base to its pre-N state, then the new canonical's
        // forward delta (if available in pending) is folded and journaled. This plugs the
        // "Base may still hold rejected writes" leak documented in #121 / iter19. In-memory
        // per node — local-only state, not consensus-visible.
        Ref.of[F, SortedMap[Long, ChangeSet]](SortedMap.empty[Long, ChangeSet]),
        Semaphore[F](1)
      ).mapN { (pendingRef, finalizedRef, lastCommittedBranchRef, undoJournalRef, mutex) =>
        new Impl[F, K](
          underlying,
          pcTree,
          toHex,
          pendingRef,
          finalizedRef,
          lastCommittedBranchRef,
          undoJournalRef,
          mutex,
          maxPendingBranches,
          bestTipsFn,
          deepStateReader,
          onBaseRevert
        ): MptOverlay[F, K]
      }

    private final class Impl[F[_]: Async: Hasher, K](
      underlying: MptStore[F, K],
      pcTree: ParentChildTree[F],
      toHex: K => F[Hex],
      pendingRef: Ref[F, Map[BranchId, BranchEntry]],
      finalizedRef: Ref[F, Map[SnapshotOrdinal, BranchId]],
      lastCommittedBranchRef: Ref[F, Option[BranchId]],
      undoJournalRef: Ref[F, SortedMap[Long, ChangeSet]],
      mutex: Semaphore[F],
      maxPendingBranches: Int,
      bestTipsFn: F[Set[BranchId]],
      deepStateReader: SnapshotOrdinal => F[Option[Map[Hex, Array[Byte]]]],
      onBaseRevert: F[Unit]
    ) extends MptOverlay[F, K] {

      private val logger = Slf4jLogger.getLoggerFromName[F](this.getClass.getName)

      private def copyRawEntries(entries: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
        entries.iterator.map { case (key, bytes) => key -> (if (bytes eq null) null else bytes.clone()) }.toMap

      def base: MptStore[F, K] = underlying
      def parentChildTree: ParentChildTree[F] = pcTree

      def checkout(parentBranch: BranchId): F[BranchHandle[F, K]] =
        Ref.of[F, ChangeSet](ChangeSet.empty).map { acc =>
          new MultiBranchHandle[F, K](toHex, acc, parentBranch)
        }

      def commit(branch: BranchHandle[F, K], childTip: BranchId, ordinal: SnapshotOrdinal): F[Unit] = {
        // Casting back to MultiBranchHandle to read its accumulator. Safe because checkout always returns
        // MultiBranchHandle in this impl. Using a class match avoids exposing `accRef` on the public trait.
        val handle = branch match {
          case h: MultiBranchHandle[F, K] @unchecked => h
          case other =>
            throw new IllegalArgumentException(
              s"Multi-branch overlay received a non-MultiBranchHandle on commit: ${other.getClass.getName}"
            )
        }
        mutex.permit.use { _ =>
          for {
            changes <- handle.accRef.get
            _ <- pendingRef.update(_.updated(childTip, BranchEntry(handle.parent, changes, ordinal)))
            // Track this commit as the most-recent. Read by `evictIfOverCap` so the just-committed
            // branch and its ancestors are protected even when the external `bestTipsFn` lags.
            _ <- lastCommittedBranchRef.set(childTip.some)
            // Eviction (#56.9): single-shot — each commit adds exactly one branch, so at most one over the
            // cap. Read pending+evict+write while still holding the mutex so a concurrent commit can't see
            // a momentarily-over-cap state.
            _ <- pendingRef.get.flatMap(evictIfOverCap).flatMap(pendingRef.set)
            _ <-
              if (handle.parent.value === childTip.value) Async[F].unit
              else pcTree.associate(childTip.value, handle.parent.value)
          } yield ()
        }
      }

      def rekey(oldChildTip: BranchId, newChildTip: BranchId): F[Unit] =
        if (oldChildTip.value === newChildTip.value) Async[F].unit
        else
          mutex.permit.use { _ =>
            pendingRef.get.flatMap { before =>
              before.get(oldChildTip) match {
                case None =>
                  // Idempotent: nothing to rekey. Caller may have already rekeyed (e.g. retry path)
                  // or the leader's `createProposalArtifact` ran with `OverlayMode.Passthrough` and
                  // `pendingRef` is empty by construction.
                  Async[F].unit
                case Some(entry) =>
                  for {
                    _ <- pendingRef.update(p => (p - oldChildTip).updated(newChildTip, entry))
                    // #115: keep `lastCommittedBranchRef` consistent with the rekey. Without this,
                    // the ref keeps pointing at the now-removed `oldChildTip`, and `walkAncestorsInPending`
                    // returns just `{oldChildTip}` (acc + branch when not in pending) — providing zero
                    // real ancestor protection during the eviction firing of the NEXT commit. With this
                    // update, lastCommittedRef-based protection still walks the canonical chain even
                    // before the next commit registers a new lastCommitted.
                    _ <- lastCommittedBranchRef.update {
                      case Some(current) if current.value === oldChildTip.value => newChildTip.some
                      case other                                                => other
                    }
                    _ <-
                      if (entry.parent.value === newChildTip.value) Async[F].unit
                      else pcTree.associate(newChildTip.value, entry.parent.value)
                  } yield ()
              }
            }
          }

      def discardBranch(branchId: BranchId): F[Unit] =
        mutex.permit.use(_ => pendingRef.update(_ - branchId))

      /** Eviction policy (#56.9, refined #113, multi-tip in #115). Drops the lowest-scoring non-ancestor branch when `pending.size` exceeds
        * the cap. Score is `(ordinal asc, BranchId.value lex asc)` — lowest gets evicted. Multiple protected tips, each contributing its
        * full ancestor walk-back to the protection set:
        *
        *   - `bestTipsFn`'s tips — externally-supplied set of viable chain heads. On dag-l0 this comes from `chainStore.allTips` (every
        *     leaf in `byHash`, i.e. the canonical chain head AND every tentative-branch head being followed during fork-recovery). On
        *     followers it comes from `lastGlobalSnapshotStorage` (singleton). Returning the full set instead of just `bestTip` solves the
        *     fork-recovery race (#115): when validator commits canonical-N before the chain reorgs to it, canonical-N isn't yet bestTip but
        *     its ancestors are needed for `mergedChain` walks at canonical-N+1. Returning all tips protects them.
        *   - `lastCommittedBranchRef`'s tip — most-recently-committed branch (set inside `commit` before this fn runs). Catches the lag
        *     window where the upstream sources backing `bestTipsFn` update AFTER overlay commit (e.g. `chainStore.store` runs after overlay
        *     commit on the leader path; during the persist window the just-committed branches would otherwise be the only eviction
        *     candidates and get dropped — exactly the freshest chain we need to keep).
        *
        * If both sources return empty, no ancestor protection applies (purely score-based eviction). If ALL pending branches are protected,
        * eviction is skipped with a warn log — the overlay rides over-cap until finalization releases ancestors.
        */
      private def evictIfOverCap(pending: Map[BranchId, BranchEntry]): F[Map[BranchId, BranchEntry]] =
        if (pending.size <= maxPendingBranches) pending.pure[F]
        else
          (bestTipsFn, lastCommittedBranchRef.get).tupled.flatMap {
            case (bestTips, lastCommittedOpt) =>
              val bestTipsAncestors =
                bestTips.foldLeft(Set.empty[BranchId])((acc, tip) => acc ++ walkAncestorsInPending(tip, pending))
              val lastCommittedAncestors = lastCommittedOpt.fold(Set.empty[BranchId])(walkAncestorsInPending(_, pending))
              val ancestors = bestTipsAncestors ++ lastCommittedAncestors
              val candidates = pending.view.filterKeys(id => !ancestors.contains(id)).toMap
              if (candidates.isEmpty)
                logger
                  .warn(
                    s"[MptOverlay] Eviction skipped: all ${pending.size} pending branches are protected " +
                      s"(bestTips=${bestTips.size}, bestTips-ancestors=${bestTipsAncestors.size}, " +
                      s"lastCommitted-ancestors=${lastCommittedAncestors.size}); " +
                      s"cap=$maxPendingBranches. Overlay will ride over-cap until finalization releases ancestors."
                  )
                  .as(pending)
              else {
                val (evictId, evictEntry) = candidates.toList.minBy {
                  case (id, entry) => (entry.ordinal.value.value, id.value.value)
                }
                logger
                  .info(
                    s"[MptOverlay] Evicting branch=${evictId.value} ordinal=${evictEntry.ordinal} (cap=$maxPendingBranches, " +
                      s"pending=${pending.size}, ancestors=${ancestors.size}, bestTips=${bestTips.size})"
                  )
                  .as(pending - evictId)
              }
          }

      def get[V: ImmutableCodec](branch: BranchId, key: K): F[Option[V]] =
        for {
          hex <- toHex(key)
          pending <- pendingRef.get
          chainResult = walkChainForKey(branch, hex, pending)
          out <- chainResult match {
            case Some(None)        => none[V].pure[F]
            case Some(Some(bytes)) => deserializeBytes[V](bytes)
            case None              => underlying.get[V](key)
          }
        } yield out

      def getStrict[V: ImmutableCodec](branch: BranchId, key: K): F[StrictMptRead[V]] =
        for {
          hex <- toHex(key)
          pending <- pendingRef.get
          chainResult = walkChainForKey(branch, hex, pending)
          out <- chainResult match {
            case Some(None)        => (StrictMptRead.Absent: StrictMptRead[V]).pure[F]
            case Some(Some(bytes)) => StrictMptRead.fromStoredBytes[V](bytes).pure[F]
            case None              => underlying.getStrict[V](key)
          }
        } yield out

      def getAllForPrefix[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[Map[Hex, V]] =
        for {
          baseEntries <- underlying.getAllForPrefix[V](prefix)
          pending <- pendingRef.get
          merged = mergedChain(branch, pending)
          filteredUpserts = merged.upserts.filter { case (hex, _) => StatefulMerklePatriciaProducer.hasNibblePrefix(hex, prefix) }
          filteredRemovals = merged.removals.filter(StatefulMerklePatriciaProducer.hasNibblePrefix(_, prefix))
          decoded <- filteredUpserts.toList.sortBy(_._1.value).traverse {
            case (hex, bytes) => deserializePrefixBytes[V](hex, bytes).map(hex -> _)
          }
        } yield (baseEntries -- filteredRemovals) ++ decoded.toMap

      def getAllForPrefixStrict[V: ImmutableCodec](branch: BranchId, prefix: Hex): F[List[StrictMptEntry[V]]] =
        mutex.permit.use { _ =>
          for {
            baseEntries <- underlying.rawEntriesForPrefixStrict(prefix)
            pending <- pendingRef.get
            merged = mergedChain(branch, pending)
            baseMap = baseEntries.iterator.map {
              case StrictMptRawEntry(key, bytes) => key -> bytes.fold[Array[Byte]](null)(_.toArray)
            }.toMap
            filteredUpserts = merged.upserts.filter {
              case (key, _) =>
                StatefulMerklePatriciaProducer.hasNibblePrefix(key, prefix)
            }
            filteredRemovals = merged.removals.filter(StatefulMerklePatriciaProducer.hasNibblePrefix(_, prefix))
            branchEntries = filteredUpserts.foldLeft(baseMap -- filteredRemovals) {
              case (entries, (key, value)) => entries.updated(key, value)
            }
          } yield StrictMptRead.decodeEntries[V](branchEntries)
        }

      def buildRoot(branch: BranchId, ordinal: SnapshotOrdinal): F[Either[MerklePatriciaError, MerklePatriciaTrie]] =
        underlying.build(ordinal).flatMap {
          case Left(err) => Async[F].pure(Left(err): Either[MerklePatriciaError, MerklePatriciaTrie])
          case Right(baseTrie) =>
            pendingRef.get.flatMap { pending =>
              val merged = mergedChain(branch, pending)
              if (merged.isEmpty) Async[F].pure(Right(baseTrie): Either[MerklePatriciaError, MerklePatriciaTrie])
              else
                baseTrie
                  .withChanges[F](merged.upserts, merged.removals)
                  .map(t => Right(t): Either[MerklePatriciaError, MerklePatriciaTrie])
            }
        }

      private def allEntriesAsBytesUnlocked(branch: BranchId): F[Map[Hex, Array[Byte]]] =
        for {
          baseEntries <- underlying.allEntriesAsBytes
          pending <- pendingRef.get
          merged = mergedChain(branch, pending)
        } yield copyRawEntries((baseEntries -- merged.removals) ++ merged.upserts)

      def allEntriesAsBytes(branch: BranchId): F[Map[Hex, Array[Byte]]] =
        mutex.permit.use(_ => allEntriesAsBytesUnlocked(branch))

      def allEntriesStrict(branch: BranchId): F[List[StrictMptRawEntry]] =
        allEntriesAsBytes(branch).map(StrictMptRead.captureRawEntries)

      def allEntriesAsBytesWithHandle(handle: BranchHandle[F, K], ordinal: SnapshotOrdinal): F[Map[Hex, Array[Byte]]] = {
        // Same type-cast pattern as `commit` — checkout always returns a MultiBranchHandle in this impl, so the
        // cast is safe. Avoids exposing `accRef` on the public trait while keeping the post-write view computable.
        val mb = handle match {
          case h: MultiBranchHandle[F, K] @unchecked => h
          case other =>
            throw new IllegalArgumentException(
              s"Multi-branch overlay received a non-MultiBranchHandle in allEntriesAsBytesWithHandle: ${other.getClass.getName}"
            )
        }
        mutex.permit.use { _ =>
          for {
            baseEntries <- underlying.allEntriesAsBytes
            pending <- pendingRef.get
            parentMerged = mergedChain(mb.parent, pending)
            handleChanges <- mb.accRef.get
            // Chronological compose: parent's accumulated chain first, then handle's local pending writes on top.
            // `merge` enforces "later wins" + strips upsert/removal overlap.
            combined = parentMerged.merge(handleChanges)
          } yield copyRawEntries((baseEntries -- combined.removals) ++ combined.upserts)
        }
      }

      def finalizeBranch(canonical: BranchId, ordinal: SnapshotOrdinal): F[FinalizationOutcome] =
        mutex.permit.use { _ =>
          finalizedRef.get.flatMap { finalized =>
            finalized.get(ordinal) match {
              case Some(prev) if prev.value === canonical.value =>
                // Already finalized at exactly this (ordinal, hash); nothing to do.
                FinalizationOutcome.NoOp.pure[F].widen[FinalizationOutcome]
              case Some(prev) =>
                // Reorg-style re-finalization: the same ordinal is being finalized with a different canonical
                // hash than before. Followers (gl1/cl1/dl1/ml0) hit this path when `setForRecovery` resets
                // their LastSnapshotStorage to ord N < currentOrdinal and the GlobalSnapshotPullingProcess
                // re-pulls + re-validates each snapshot through `createContext`. The previous behavior raised
                // and aborted the recovery loop entirely (#113); instead, accept the new canonical as the
                // post-reorg truth, fold its pending chain (idempotent under MultiBranch when the prior
                // finalization already cleared pending), and update the finalizedRef. The fold may overwrite
                // base entries the prior canonical wrote — that is correct: the new canonical is the new state
                // at this ordinal, and base must reflect it. NakamotoSyncDaemon (gl0 leader path) cannot hit
                // this case because depth-k confirmation precludes a different-hash re-finalization at the
                // same ord; it remains an internal-consistency bug there if this branch ever fires on gl0.
                pendingRef.get.flatMap { pending =>
                  pending.get(canonical) match {
                    case None =>
                      // Reorg-replace where the NEW canonical was never overlay-registered locally. Two cases:
                      //   1. Idempotent — pending genuinely empty (a prior canonical's fold already cleared it).
                      //   2. Bug surface (#116, iter14 evidence): pending still holds fork branches from the
                      //      locally-rejected chain. Eviction (`evictIfOverCap`) and `walkAncestorsInPending`
                      //      would treat them as protected via `lastCommittedBranchRef`, blocking real eviction
                      //      and skewing chain walks. Worse: if the orphan branches' deltas were earlier folded
                      //      into base on a prior fork-side finalize, base now holds rejected writes. Reads at
                      //      `parentTip=canonical` walk pending→empty→base, returning the contaminated bytes
                      //      (manifests as StateProofMismatch on followers, divergent local-built proofs on
                      //      gl0 — see iter14 forensics: gl0-2 ord 81 PRE 64545a6f vs canonical cad91eed).
                      // The overlay alone cannot reconstruct missing canonical deltas (caller never pushed
                      // them via `commit`). What it CAN do: drop orphan pending so eviction/walks no longer
                      // protect rejected ancestors, and reset `lastCommittedBranchRef` so the next commit
                      // does not protect a now-stale tip. Caller is responsible for resyncing base via
                      // `syncFromGlobalSnapshotInfo` when it can detect the case (gl0 NakamotoSyncDaemon
                      // catch-up does this on reorg adoption; gl1/dl1 do it via `recoverFromOrphan` +
                      // `setForRecovery` on StateProofMismatch). The WARN log surfaces the case so operators
                      // can see when canonical was never overlay-registered locally.
                      val droppedCount = pending.size
                      val cleanup =
                        if (droppedCount > 0)
                          pendingRef.set(Map.empty) >> lastCommittedBranchRef.set(none)
                        else Async[F].unit
                      for {
                        // #121: Replay the previous canonical's undo entry to revert base to its pre-`ordinal` state.
                        // This plugs the "Base may still hold rejected writes" leak: prior versions of this code path
                        // dropped pending but left the prior fold's writes in base, contaminating subsequent reads at
                        // `parentTip=canonical` (overlay walks pending→empty→base, sees the rejected bytes).
                        undoApplied <- applyUndoAt(ordinal.value.value, ordinal)
                        _ <- cleanup
                        // Track-3 S4: base just reverted (prior canonical's fold undone) — drop the stale eta walk cache
                        // so the adopted canonical's eta re-derives over the reverted chain (bootstrap-equivalence).
                        _ <- onBaseRevert
                        _ <- finalizedRef.update(_.updated(ordinal, canonical))
                        _ <-
                          if (droppedCount > 0)
                            logger.warn(
                              s"[MptOverlay] Reorg-replace finality at ordinal=$ordinal: prev=${prev.value} " +
                                s"new=${canonical.value} — canonical NOT in pending; dropped $droppedCount " +
                                s"orphan fork branch(es) from pendingRef + reset lastCommittedBranchRef. " +
                                s"Undo applied=$undoApplied (base reverted to pre-ord state). " +
                                s"Caller still needs to resync base via syncFromGlobalSnapshotInfo to apply " +
                                s"the new canonical's deltas."
                            )
                          else
                            logger.info(
                              s"[MptOverlay] Reorg-replace finality at ordinal=$ordinal: prev=${prev.value} " +
                                s"new=${canonical.value} (pending already empty, no fold needed). " +
                                s"Undo applied=$undoApplied."
                            )
                      } yield
                        if (droppedCount > 0) FinalizationOutcome.Folded(0, droppedCount): FinalizationOutcome
                        else FinalizationOutcome.NoOp: FinalizationOutcome
                    case Some(_) =>
                      val merged = mergedChain(canonical, pending)
                      val droppedCount = pending.size - countAncestors(canonical, pending)
                      for {
                        // #121: Revert prior canonical's writes from base before folding the new canonical.
                        // Without this, keys that the OLD canonical wrote but the NEW canonical doesn't touch
                        // would retain the OLD canonical's bytes after the fold, causing silent state divergence.
                        undoApplied <- applyUndoAt(ordinal.value.value, ordinal)
                        // Track-3 S4: prior canonical's writes just reverted from base — drop the stale eta walk cache
                        // so the new canonical's eta re-derives over the reverted-then-refolded chain (bootstrap-equivalence).
                        _ <- onBaseRevert
                        _ <- foldIntoBase(merged, ordinal)
                        _ <- pendingRef.set(Map.empty)
                        _ <- lastCommittedBranchRef.set(none)
                        _ <- finalizedRef.update(_.updated(ordinal, canonical))
                        _ <- logger.info(
                          s"[MptOverlay] Reorg-replace finality at ordinal=$ordinal: prev=${prev.value} " +
                            s"new=${canonical.value} keysApplied=${merged.size} branchesDropped=$droppedCount " +
                            s"undoApplied=$undoApplied"
                        )
                      } yield FinalizationOutcome.Folded(merged.size, droppedCount)
                  }
                }
              case None =>
                pendingRef.get.flatMap { pending =>
                  pending.get(canonical) match {
                    case None =>
                      // Branch never registered with the overlay. Record the finalization (so future calls at
                      // this ordinal are conflict-checked) but there is nothing to apply.
                      finalizedRef
                        .update(_.updated(ordinal, canonical))
                        .as(FinalizationOutcome.NoOp: FinalizationOutcome)
                    case Some(_) =>
                      val merged = mergedChain(canonical, pending)
                      val droppedCount = pending.size - countAncestors(canonical, pending)
                      for {
                        _ <- foldIntoBase(merged, ordinal)
                        _ <- pendingRef.set(Map.empty)
                        // pendingRef cleared → any prior `lastCommittedBranchRef` now points at a branch
                        // that's no longer in pending (its ancestor walk yields zero protection per the
                        // "branch not in pending" leaf in walkAncestorsInPending). Reset to None so a
                        // future commit can re-establish the protection cleanly. Same rationale as the
                        // reorg-replace `case Some` arm above (#116).
                        _ <- lastCommittedBranchRef.set(none)
                        _ <- finalizedRef.update(_.updated(ordinal, canonical))
                        _ <- logger.info(
                          s"[MptOverlay] Finalized branch=${canonical.value} at ordinal=$ordinal: " +
                            s"keysApplied=${merged.size} branchesDropped=$droppedCount"
                        )
                      } yield FinalizationOutcome.Folded(merged.size, droppedCount)
                  }
                }
            }
          }
        }

      def pruneBelow(ord: SnapshotOrdinal): F[Unit] =
        // Local retention prune driven by current legacy watermark wiring. k2 does not exclude
        // objective density reorgs. Removed RAM state must remain exactly reconstructible from
        // authenticated storage/proofs; otherwise comparison enters RecoveryRequired.
        //
        // `pendingRef` is deliberately NOT touched here — pending branches are managed by the commit/finalize
        // path (cleared on `finalizeBranch.foldIntoBase`, evicted on cap exceedance) and any live branch with
        // `entry.ordinal < ord` would already be unreachable in practice; touching pendingRef here would
        // cross-cut concerns and is unnecessary because eviction + finalize already bound it.
        //
        // ParentChildTree is shared with consensus and has its own owner-driven `pruneBelow`; pruning it
        // from inside the overlay would double-prune via a second owner.
        //
        // Single shared mutex with commit/finalize so the prune is consistent with concurrent fold operations.
        mutex.permit.use { _ =>
          val ordValue = ord.value.value
          for {
            preCounts <- (undoJournalRef.get, finalizedRef.get).tupled.map {
              case (uj, fz) => (uj.size, fz.size)
            }
            (preUndo, preFinalized) = preCounts
            // `rangeFrom` on `SortedMap[Long, _]` returns the sub-map of entries with key >= `ordValue`,
            // i.e. drops all entries strictly below the archival watermark. O(log n) under TreeMap.
            _ <- undoJournalRef.update(journal => journal.rangeFrom(ordValue))
            _ <- finalizedRef.update(_.filter { case (k, _) => k.value.value >= ordValue })
            postCounts <- (undoJournalRef.get, finalizedRef.get).tupled.map {
              case (uj, fz) => (uj.size, fz.size)
            }
            (postUndo, postFinalized) = postCounts
            droppedUndo = preUndo - postUndo
            droppedFinalized = preFinalized - postFinalized
            _ <-
              if (droppedUndo > 0 || droppedFinalized > 0)
                logger.info(
                  s"OVERLAY-PRUNE-BELOW ord=$ordValue pre=$preUndo undoJournal entries / $preFinalized finalized entries " +
                    s"post=$postUndo undoJournal entries / $postFinalized finalized entries " +
                    s"(dropped undo=$droppedUndo, finalized=$droppedFinalized)"
                )
              else
                logger.debug(
                  s"OVERLAY-PRUNE-BELOW ord=$ordValue no-op (already pruned: $preUndo undoJournal, $preFinalized finalized)"
                )
          } yield ()
        }

      def journalSizes: F[MptOverlay.JournalSizes] =
        // Read-only telemetry snapshot (Track-3 S2). No mutex: each `.get` is atomic and a momentarily-inconsistent
        // cross-ref view is harmless for a memory-budget gauge (the counts are advisory, not consensus-load-bearing).
        (undoJournalRef.get, finalizedRef.get, pendingRef.get).mapN {
          case (uj, fz, pd) => MptOverlay.JournalSizes(uj.size, fz.size, pd.size)
        }

      def unsafe_reset: F[Unit] =
        // P-11 re-bootstrap reset (task #141). Wipes ALL in-memory overlay state so the
        // post-reset path starts fresh: a future `commit` will register a new pending branch
        // against an empty pending map; a future `finalizeBranch` will see no prior canonical
        // at any ord (no idempotency conflict, no reorg-replace path). Base `MptStore` is
        // NOT touched here — caller resyncs base via `syncFromGlobalSnapshotInfo` once the
        // canonical chain head is known.
        //
        // Single mutex with commit/finalize/pruneBelow so we cannot race a concurrent fold.
        mutex.permit.use { _ =>
          for {
            preCounts <- (pendingRef.get, finalizedRef.get, undoJournalRef.get).tupled.map {
              case (p, f, u) => (p.size, f.size, u.size)
            }
            (pPending, pFinalized, pUndo) = preCounts
            _ <- pendingRef.set(Map.empty)
            _ <- finalizedRef.set(Map.empty)
            _ <- lastCommittedBranchRef.set(none)
            _ <- undoJournalRef.set(SortedMap.empty[Long, ChangeSet])
            _ <- logger.warn(
              s"⚠️ OVERLAY-UNSAFE-RESET: dropped $pPending pending branches, $pFinalized finalized markers, " +
                s"$pUndo undoJournal entries (re-bootstrap recovery). Base MptStore NOT touched; caller " +
                s"must resync via syncFromGlobalSnapshotInfo."
            )
          } yield ()
        }

      /** Track-3 S4 revert-executor (see the trait scaladoc). Shallow (RAM journal) / deep (disk readState) / gap (fail-closed). Runs under
        * the same `mutex` as `finalizeBranch` / `pruneBelow` so the revert is atomic w.r.t. concurrent commits/folds.
        */
      def revertToOrdinal(forkOrdinal: SnapshotOrdinal): F[RevertOutcome] =
        mutex.permit.use { _ =>
          val forkLong = forkOrdinal.value.value
          for {
            journal <- undoJournalRef.get
            tipOpt <- underlying.lastPersistedOrdinal
            outcome <- tipOpt.map(_.value.value) match {
              case Some(tip) if tip > forkLong =>
                // Need to revert the band (forkLong, tip]. SHALLOW iff the RAM journal holds EVERY ordinal in that
                // band (contiguous down to forkLong+1); otherwise the journal was pruned below the fork → DEEP.
                val needed: Seq[Long] = (forkLong + 1L) to tip
                if (needed.forall(journal.contains)) {
                  // SHALLOW: replay the per-ordinal reverse deltas DESCENDING (LIFO). Each was captured against the
                  // base at ITS OWN fold, so descending order is load-bearing. `applyUndoAt` consumes (removes) each
                  // journal entry it replays and commits at `forkOrdinal`, so after the loop the base state AND its
                  // persisted-ordinal label are exactly `forkOrdinal`, and no journal entry above the fork survives.
                  val descending = journal.keySet.filter(_ > forkLong).toList.sorted.reverse
                  for {
                    _ <- descending.traverse_(o => applyUndoAt(o, forkOrdinal))
                    _ <- postRevertCleanup(forkLong)
                    _ <- logger.info(
                      s"[MptOverlay] SHALLOW revert to ordinal=$forkLong: replayed ${descending.size} RAM undo-journal " +
                        s"reverse-delta(s) descending from tip=$tip. Base reverted; pending cleared for re-fold."
                    )
                  } yield RevertOutcome.Shallow(descending.size): RevertOutcome
                } else
                  deepRevert(forkOrdinal, forkLong, tipOpt)
              case other =>
                // Base already at-or-below the fork — nothing above the fork to revert. Still clear pending overlay
                // state + drop the eta cache so an idempotent second call / a redundant caller lands cleanly.
                for {
                  _ <- postRevertCleanup(forkLong)
                  _ <- logger.debug(
                    s"[MptOverlay] revertToOrdinal($forkLong) NoOp: base tip=${other.map(_.toString).getOrElse("none")} " +
                      s"at-or-below fork (nothing above fork to revert). Pending cleared."
                  )
                } yield RevertOutcome.NoOp: RevertOutcome
            }
          } yield outcome
        }

      /** DEEP path of `revertToOrdinal`: the RAM journal cannot reach `forkOrdinal`, so rebuild the base from the disk tier. Prune the
        * producer's on-disk state above the fork, read the disk-retained signed bytes at the fork (Track-3 S2's contiguous
        * `signedBytesStore` to k₂ in production, via the injected `deepStateReader`), and load them VERBATIM (`loadBytes` — same primitive
        * follower resync uses, so the recomputed root equals the signed root by construction). Fail closed if the disk tier no longer
        * retains the fork.
        */
      private def deepRevert(
        forkOrdinal: SnapshotOrdinal,
        forkLong: Long,
        tipOpt: Option[SnapshotOrdinal]
      ): F[RevertOutcome] =
        deepStateReader(forkOrdinal).flatMap {
          case Some(state) =>
            for {
              _ <- underlying.deleteAbove(forkOrdinal)
              _ <- underlying.loadBytes(state, forkOrdinal)
              // The RAM journal entries above the fork are now stale (base was rebuilt from disk, not unwound), so drop
              // them; entries at-or-below the fork stay as valid undo info for any subsequent shallower revert.
              _ <- undoJournalRef.update(_.rangeTo(forkLong))
              _ <- postRevertCleanup(forkLong)
              _ <- logger.info(
                s"[MptOverlay] DEEP revert to ordinal=$forkLong: RAM journal could not reach it; rebuilt base from " +
                  s"${state.size} disk-retained signed entries (deleteAbove + loadBytes). Pending cleared for re-fold."
              )
            } yield RevertOutcome.Deep(state.size): RevertOutcome
          case None =>
            logger.error(
              s"[MptOverlay] revertToOrdinal($forkLong) GAP: fork ordinal is below the RAM undo-journal window AND " +
                s"absent from the disk-retained signed-bytes tier (deeper than k₂ or deep reader unwired). Failing closed."
            ) >>
              (RevertGapError(
                forkOrdinal,
                tipOpt,
                "fork ordinal reachable via neither the RAM undo journal nor the disk signed-bytes tier"
              ): Throwable).raiseError[F, RevertOutcome]
        }

      /** Common tail for every `revertToOrdinal` arm: clear pending overlay state so the re-fold starts clean (mirroring the finalize
        * reorg-replace arms), drop finalized markers strictly above the fork (so the re-fold re-finalizes those ordinals via the clean
        * `case None` path rather than a spurious reorg-replace), and fire the base-revert hook (eta `forgetUncommitted`).
        */
      private def postRevertCleanup(forkLong: Long): F[Unit] =
        pendingRef.set(Map.empty) >>
          lastCommittedBranchRef.set(none) >>
          finalizedRef.update(_.filter { case (o, _) => o.value.value <= forkLong }) >>
          onBaseRevert

      /** Atomically apply the merged chain delta to the underlying base store.
        *
        * The bracket via `MptStore.withTransaction` ensures partial application cannot persist: if any of the producer-level operations
        * (remove → insertBytes → commit) fails, the savepoint restores the base to its pre-call state. This is the partition-atomicity
        * contract from #56.4.5: a single `finalizeBranch` writes either ALL partitions' deltas (rooted System indices + user fields) or
        * NONE.
        *
        * `Rethrow.rethrow` on the producer-level `Either` results lifts a `MerklePatriciaError` into `F` so the transaction rolls back
        * rather than swallowing the failure (the legacy `.void` would have left the base half-applied without surfacing the error).
        *
        * The `foldRaw` overload writes a delta to base without journaling — used by `applyUndoAt` to replay a captured reverse delta (which
        * would otherwise recurse forever, journaling reversals of reversals). The public `foldIntoBase` wraps it with `captureReverseDelta`
        * + `undoJournalRef` writes to enable atomic rollback on reorg-replace finality (#121).
        */
      private def foldIntoBase(merged: ChangeSet, ordinal: SnapshotOrdinal): F[Unit] =
        for {
          reverse <- captureReverseDelta(merged)
          _ <- foldRaw(merged, ordinal)
          _ <- undoJournalRef.update(_.updated(ordinal.value.value, reverse))
        } yield ()

      private def foldRaw(delta: ChangeSet, ordinal: SnapshotOrdinal): F[Unit] =
        underlying.withTransaction {
          val producer = underlying.underlying
          for {
            _ <-
              if (delta.removals.nonEmpty) producer.remove(delta.removals.toList).rethrow
              else Async[F].unit
            _ <-
              if (delta.upserts.nonEmpty) producer.insertBytes(delta.upserts).rethrow
              else Async[F].unit
            _ <- underlying.commit(ordinal)
          } yield ((), MptTxAction.Commit)
        }

      /** Read the current base state for the keys touched by `forward` and build a `ChangeSet` whose application to base would undo
        * `forward`'s effect:
        *   - keys present in base before the fold → reverse `upserts` (restore the pre-fold value)
        *   - keys absent in base before the fold → reverse `removals` (drop the now-inserted entry)
        *
        * The producer copies only the touched values, so capture is O(|touched|) after one in-memory lookup pass and does not clone the
        * complete global image while `finalizeBranch` holds `mutex.permit`. The captured view is consistent with the upcoming `foldRaw`
        * write for callers that respect overlay-owned base mutation.
        */
      private def captureReverseDelta(forward: ChangeSet): F[ChangeSet] = {
        val touched = forward.upserts.keySet ++ forward.removals

        underlying.underlying.entriesForKeys(touched).map { current =>
          val (presentBefore, absentBefore) = touched.partition(current.contains)
          val reverseUpserts: Map[Hex, Array[Byte]] = presentBefore.iterator.map(k => k -> current(k)).toMap
          val reverseRemovals: Set[Hex] = absentBefore.filter(forward.upserts.contains)
          ChangeSet(reverseUpserts, reverseRemovals)
        }
      }

      /** Replay `undoJournalRef[ord]` against base and drop the entry. Returns `true` if an entry existed and was applied, `false` if no
        * entry was present at that ordinal. Idempotent: a second call at the same ord is a NoOp.
        *
        * Used by `finalizeBranch` reorg-replace paths to revert the previous canonical's writes from base before the new canonical (which
        * may or may not be locally registered in `pendingRef`) is folded or simply recorded.
        */
      private def applyUndoAt(ord: Long, contextOrdinal: SnapshotOrdinal): F[Boolean] =
        undoJournalRef.modify { journal =>
          journal.get(ord) match {
            case Some(reverse) => (journal - ord, Some(reverse))
            case None          => (journal, None)
          }
        }.flatMap {
          case Some(reverse) if !reverse.isEmpty =>
            foldRaw(reverse, contextOrdinal).as(true)
          case Some(_) => true.pure[F] // empty reverse was journaled (degenerate case); count as applied
          case None    => false.pure[F]
        }

      private def deserializeBytes[V: ImmutableCodec](bytes: Array[Byte]): F[Option[V]] =
        if (bytes == null || bytes.isEmpty) none[V].pure[F]
        else
          scodec.bits.ByteVector.view(bytes).fromImmutableBytes[V] match {
            case Right(v)  => v.some.pure[F]
            case Left(err) => logger.warn(s"MptOverlay.deserializeBytes: scodec decode failed: $err") >> none[V].pure[F]
          }

      private def deserializePrefixBytes[V: ImmutableCodec](hex: Hex, bytes: Array[Byte]): F[V] =
        if (bytes == null || bytes.isEmpty)
          Async[F].raiseError(
            new IllegalStateException(s"MptOverlay.getAllForPrefix: null/empty bytes at hex=${hex.value}")
          )
        else
          scodec.bits.ByteVector.view(bytes).fromImmutableBytes[V] match {
            case Right(v) => v.pure[F]
            case Left(err) =>
              Async[F].raiseError(
                new IllegalStateException(s"MptOverlay.getAllForPrefix: undecodable bytes at hex=${hex.value}: $err")
              )
          }
    }

    /** Walk the chain leaf→root looking for `hex`. Returns:
      *   - `Some(None)` if the chain explicitly removed the key at some level (caller must NOT fall through to base — the explicit removal
      *     wins).
      *   - `Some(Some(bytes))` if some level upserted it.
      *   - `None` if the chain never mentions the key (caller falls through to base).
      *
      * Pure tail recursion — no F effects, hashtable lookups only.
      */
    @tailrec
    private def walkChainForKey(
      branch: BranchId,
      hex: Hex,
      pending: Map[BranchId, BranchEntry]
    ): Option[Option[Array[Byte]]] =
      pending.get(branch) match {
        case None => None
        case Some(entry) =>
          if (entry.changes.removals.contains(hex)) Some(None)
          else
            entry.changes.upserts.get(hex) match {
              case Some(bytes) => Some(Some(bytes))
              case None        => walkChainForKey(entry.parent, hex, pending)
            }
      }

    /** Walk the chain leaf→root merging `ChangeSet`s in chronological order. At each level, `entry.changes.merge(acc)` ensures the parent's
      * changes are applied first (older), then the accumulated child-side delta on top (newer). Pure recursion.
      */
    @tailrec
    private def mergedChain(
      branch: BranchId,
      pending: Map[BranchId, BranchEntry],
      acc: ChangeSet = ChangeSet.empty
    ): ChangeSet =
      pending.get(branch) match {
        case None        => acc
        case Some(entry) => mergedChain(entry.parent, pending, entry.changes.merge(acc))
      }

    /** Number of branches in `pending` that are on the chain from `branch` to base (inclusive). Used by `finalizeBranch` to compute
      * `branchesDropped = pending.size - ancestors`.
      */
    @tailrec
    private def countAncestors(branch: BranchId, pending: Map[BranchId, BranchEntry], acc: Int = 0): Int =
      pending.get(branch) match {
        case None        => acc
        case Some(entry) => countAncestors(entry.parent, pending, acc + 1)
      }

    /** Set of `BranchId`s on the chain from `branch` to base (inclusive of `branch` itself). Used by eviction (#56.9) to identify branches
      * that must NOT be dropped because they are ancestors of the canonical tip — finality walk-back depends on them being present.
      *
      * Walks parent pointers in `pending` only; stops when a parent isn't in `pending` (= reached the finalized base) OR when revisiting an
      * already-seen branch (defensive cycle guard, though `pendingRef` should never contain cycles).
      */
    @tailrec
    private def walkAncestorsInPending(
      branch: BranchId,
      pending: Map[BranchId, BranchEntry],
      acc: Set[BranchId] = Set.empty
    ): Set[BranchId] =
      if (acc.contains(branch)) acc
      else {
        val updated = acc + branch
        pending.get(branch) match {
          case Some(entry) => walkAncestorsInPending(entry.parent, pending, updated)
          case None        => updated
        }
      }
  }
}
