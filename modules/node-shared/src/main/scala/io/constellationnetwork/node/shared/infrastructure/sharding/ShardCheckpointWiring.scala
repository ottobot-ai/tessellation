package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.{Map, SortedMap}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshot, CurrencySnapshotInfo}
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.ShardingConfig
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotStateChannelEventsProcessor,
  ShardCheckpointGl0AcceptanceManager
}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.GlobalStateConverter
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal, StateProofSelector}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegInt
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Production-wiring helper for the hierarchical-shard-checkpoints v1 ACCEPTANCE side (priority 1 of the production-wiring slice).
  *
  * Constructs the per-shard consumer/admission infrastructure once, in a single place, so the TWO `GlobalSnapshotAcceptanceManager.make`
  * call sites — `SharedServices.make` (verify path) and `GlobalSnapshotConsensus.make` (gl0-leader produce path) — stay byte-consistent.
  *
  * '''ONE instance per node (task #44).''' [[acceptanceDeps]] is invoked EXACTLY ONCE per node, inside `SharedServices.make`, which now
  * exposes the result on `SharedServices.shardAcceptanceDeps`. The gl0-leader produce path (`GlobalSnapshotConsensus.make`) REUSES that
  * same instance rather than building a second one. Both GSAMs then forward the SAME `Option`-tuple into the three sharding parameters
  * (`shardingConfig`, `shardCheckpointAcceptanceManager`, `shardAssignment`), and the shard producers / sync daemon project off the SAME
  * `registry`. This matters because [[AcceptanceDeps]] carries STATEFUL Refs (per-shard chain stores, tip trackers, finality triggers,
  * binary buffers, the committee cache, and the adopted watermarks) — two instances meant the follower's adoptions landed in a registry the
  * producers/daemon/#42-ANCHOR-REORG-healer never read (eternal awaiting-embed). The deterministic inputs were always identical (#261
  * split-safety: same atomic genesis KES+VRF registry, same `kDraw`/`kQuorum`, same seedlist, same MPT-committed eta resolver), so
  * collapsing to one instance changes nothing deterministic — it only makes adopt ↔ produce ↔ heal share the SAME state.
  *
  * '''numShards = 1 regression bar.''' [[acceptanceDeps]] returns `None` whenever `cfg.numShards <= 1` (the production default). At `None`
  * the two GSAM call sites pass `None` for all three sharding params — exactly today's call — so `accept()` is byte-identical to the
  * pre-wiring code path. NOTHING is constructed on the `None` branch: no per-shard stores, no acceptance manager, no log lines beyond a
  * single one-time INFO. The activation gate (`numShards > 1`) wraps every allocation. See
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 13 and the GSAM `shardingConfig` scaladoc.
  *
  * '''Per-shard registry.''' On the activated branch we build a `Map[ShardId, (ShardChainStore, ShardTipTracker, ShardFinalityTriggers)]`
  * for shards `0 .. numShards - 1`. Each entry is the consumer-side state for one shard the gl0 operator tracks: the fork-DAG chain store
  * (Slice 5), the committee attestation tracker (Slice 6), and the hash-bound execution-quorum selector (Slice 6). The registry is shared
  * by checkpoint production, gossip intake, and anchor following. Embedded verification currently resolves producer-duty parent context
  * from this receiver-local registry too. That fails closed when the parent is absent, but is not yet portable consensus evidence: nodes
  * with and without prior shard gossip can disagree on the same embedded child. `SHARD-C-009` remains RED until the proposal parent binds a
  * rooted per-shard parent reference (or equivalent inclusion proof).
  *
  * '''committeeMembership(shardId, epoch) — public deterministic VK-hash draw.''' This is the execution committee, distinct from the true
  * per-binary VRF admission committee. The acceptance manager confirms each checkpoint signer is in `committeeMembership(shardId, epoch)`.
  * `kQuorum` is the mandatory execution-certificate threshold, and every carried state-validity signature must come from a member that
  * independently reproduced the exact CL1 result. [[committeeFor]] materializes the execution committee as a deterministic VK-hash-selected
  * subset of the active operator set — minus every operator with an unexpired `Slashings` (fieldId 34) cooldown at the epoch's anchor
  * (FINDING-002/EPIC-3.1, see [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader]]; the exclusion is
  * floored so ≥ `kQuorum` eligible validators always remain) — sized by the DRAW target `kDraw` (size `≈ kDraw`, NOT the full `N` — though
  * the testnet default `kDraw = N` saturates the threshold so it IS everyone). Current `verifyEmbedded` also makes every GL0 adopter
  * replay; that universal replay is transitional. Target ordinary noncommittee adoption verifies the execution certificate, positive
  * watchtower coverage, exact base/continuity/namespace, applies the canonical diff, and recomputes its root without recreating CL1
  * execution. The draw is a deterministic pseudo-random sortition keyed on each operator's registered VRF *VK* + the epoch eta + the
  * shardId (see [[committeeFor]] scaladoc for the determinism argument and `CommitteeSortition.isInShardCommittee` for why a VK-seeded
  * public hash, not a true per-operator VRF eval, is the only enumerable-by-a-non-member option). The per-signer predicates then
  * authenticate "did this specific peer sign": Ed25519 + KES product sig over the checkpoint hash, AND a REAL `EcVrf25519` verify of each
  * `CommitteeMemberSignature.vrfProof` under the signer's registered VRF VK (from the atomic operator-key registry) over the canonical
  * `(shardEta(shardId, epoch), checkpoint.slot)` message — the SAME proof the producer's
  * `ShardCheckpointAttestationEmitter`/`ShardSlotLeader.membershipProof` computes; the enumerated `committeeFor` set gates "is this peer
  * even in shard S's committee". v1 trade-off (acceptable per design §10 — honest-testnet, slashing is the v2 backstop): the committee SET
  * is PREDICTABLE because VKs are public (the per-signer VRF proof binds each signature to its drawn member, but does not hide the draw);
  * Algorand player-replaceability (a per-`(shard, epoch)` membership VRF whose output is unknowable from the VK alone, verified with
  * `CommitteeSortition.verifyMembership`) is a v2 hardening.
  *
  * '''reExecuteDerivation — transitional caller-supplied replay.''' Current receive and GL0-adoption paths re-run each MG derivation and
  * compare the recomputed `mptRoot` byte-for-byte against the committee-signed value. The closure is supplied as a parameter so each call
  * site passes the SAME [[reExecDerivationAtPinnedBase]] closure built from its own [[GlobalSnapshotStateChannelEventsProcessor]] and a
  * reader for the signed Phase-2 execution base. That is what makes the current producer's roots and every verifier's recomputed roots
  * byte-identical. The `None`/[[noReExecDerivation]] fallback (fail-closed `Hash.empty`) remains for callers that have not wired a
  * processor.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): every tunable is read off the typed [[ShardingConfig]] passed in by the
  * caller from `cfg.nakamoto.sharding`. No `sys.env.get` anywhere in this helper.
  */
object ShardCheckpointWiring {

  /** The per-shard consumer-side state bundle for one shard. Held in the registry the acceptance manager's lookups close over.
    *
    * '''binaryBuffer.''' The per-shard admission-approved binary accumulator. The metagraph gate buffers each approved binary for its shard
    * here; the producer fan-out reads `binaryBuffer.snapshotPending` from the SAME instance. This is what decouples shard-checkpoint
    * production from gl0's post-chain-link `stateChannelSnapshots` map (see
    * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer]] for the determinism model — leader-proposes /
    * members-attest, so node-local selection is fine).
    */
  final case class ShardRegistryEntry[F[_]](
    chainStore: ShardChainStore[F],
    tipTracker: ShardTipTracker[F],
    finalityTriggers: ShardFinalityTriggers[F],
    binaryBuffer: ShardBinaryBuffer[F]
  )

  /** The acceptance-side sharding dependencies, returned as the exact `Option`-tuple the two GSAM call sites forward into
    * `GlobalSnapshotAcceptanceManager.make`'s `(shardingConfig, shardCheckpointAcceptanceManager, shardAssignment)` params.
    *
    * `Some(...)` ⇒ `numShards > 1`, the activated path. `None` ⇒ `numShards <= 1`, the regression-bar path (call sites pass all-`None`).
    *
    * `registry` is exposed so the producer-side wiring (priority 2, deferred) can reuse the SAME per-shard chain stores rather than
    * building a second disjoint set — the producer writes into the chain store the consumer reads from.
    */
  final case class AcceptanceDeps[F[_]](
    shardingConfig: ShardingConfig,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[F],
    shardAssignment: ShardAssignment[F],
    registry: Map[ShardId, ShardRegistryEntry[F]],
    /** The SAME deterministic `committeeFor(shardId, epoch)` draw the acceptance manager's pre-check / quorum uses. Exposed so the
      * produce/attest gates (`ShardCheckpointFanOut`, the `ShardCheckpointAttestationEmitter` call site) reuse the IDENTICAL committee set:
      * a node produces/attests for shard `s` only when it is in `committeeMembership(s, epoch)`. Routing both the gate and the verifier
      * through one closure keeps "who is in the committee" defined in exactly one place.
      */
    committeeMembership: (ShardId, EtaPeriod) => F[Set[PeerId]]
  )

  /** Fail-closed fallback for checkpoint re-execution. Returns a fixed sentinel hash for every `(metagraphAddress, includedChain,
    * gl0AnchorOrdinal)`. Used only by callers that have not wired a real [[reExecDerivationAtPinnedBase]] closure; both production sites
    * now pass the real one.
    *
    * '''Safety analysis.''' Intake, attestation, and embedded-adoption paths all replay the checkpoint. The manager compares this
    * derivation's output against the committee-signed `perMetagraphMptRoots(mg)`. With a fixed sentinel:
    *   - Empty checkpoints are rejected before this fallback can authorize anything.
    *   - For a content-bearing checkpoint the `Hash.empty` sentinel is the manager's CANNOT-RE-DERIVE marker: `reExecPath` buckets it as
    *     "this node can't check" and returns a plain `Rejected` — the checkpoint is DROPPED (fail-closed; the binaries do NOT enter the gl0
    *     snapshot) but the signers are NOT slash targets. This matters because `RejectedReExecutionMismatch` now feeds the DURABLE 100%
    *     `InvalidStateProof` slash (`GlobalSnapshotAcceptanceManager.adoptShardCheckpoints` → `WatchtowerSlashRequest`), which demands an
    *     AFFIRMATIVE pinned-base re-derivation mismatch as evidence — a sentinel from an unwired closure (or an unresolvable pinned
    *     execution-base) is not evidence of committee deviation. The checkpoint is never admitted while replay is unavailable, and honest
    *     signers are not slashed for a local inability to replay.
    */
  def noReExecDerivation[F[_]: Async]
    : (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => F[Hash] =
    (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) => Async[F].pure(Hash.empty)

  /** The single reader-resolution recipe for every [[reExecDerivationAtPinnedBase]] caller: resolve the finalized [[GlobalStateReader]] AT
    * the wire-carried, committee-signed pinned ordinal (`executionBaseOrdinal`), NEVER at this node's live base when the two differ.
    *
    * '''Why one definition.''' Full currency recreation is base-dependent: balances, references, active sets, and messages all begin at the
    * pinned prior, and `currencySnapshotMgRoot` commits the recreated result. So every rail that recomputes the root for comparison against
    * `perMetagraphMptRoots(mg)` — the gl0 produce/watchtower rail (`GlobalSnapshotConsensus.finalizedReaderAt`), the SharedServices
    * unconditional `reExecuteDerivation`, and the SharedServices `createContext` fraud-proof validator — MUST read the SAME pinned base the
    * producer executed over, or an honest committee's root is not reproduced and the mismatch feeds the 100% `InvalidStateProof` slash (the
    * false-slash + split this pin kills). One shared definition keeps all three rails byte-identical, mirroring
    * [[reExecDerivationAtPinnedBase]] itself.
    *
    * '''Resolution — ALWAYS the version-retained [[PinnedCurrencyInfoReader.pinnedReaderAt]]''' (which verifies the retained bytes
    * reproduce the pinned snapshot's committed `mptRoot`). There is deliberately NO live-store fast path. The original fast path served
    * `GlobalStateReader.fromMptStore(mptStore)` whenever `ord == mptStore.lastPersistedOrdinal` — but that equivalence is UNSOUND: the live
    * store is a MUTABLE VIEW whose content-vs-watermark relationship is unsynchronized. In Passthrough overlay mode the accept-path writes
    * land in the base store THROUGHOUT an ordinal's processing and `MptStore.commit(ordinal)` bumps `lastPersistedOrdinal` only at the very
    * end, so `lastPersistedOrdinal == N` holds while the content is anywhere from state@N to a MID-FOLD/POST-FOLD state of N+1+. Live wedge
    * (2026-07-08, 2mg/2shard token-lock e2e): gl0-1 minted shard-0 `shardOrdinal=8` stamped `executionBaseOrdinal=18` while its live store
    * already carried the shardOrd-7 adopt (prior read `inc@10,bal=15`; the TRUE committed state@18 was `inc@7,bal=14`) — the diff was cut
    * over the drifted content, quorum attested it (every committee member's fast path saw the same drifted view), and every honest adopter
    * — applying the wire diff onto the verified state@18 — recomputed a root that never matched the attested one. The checkpoint re-offered
    * and dropped at EVERY gl0 ordinal, the per-MG mirror froze, and the metagraph's `activeTokenLocks` never reached gl0. The pinned read
    * is the only version-pinned source; the fast path's byte-map-materialization saving was never worth an unpinned prior. (Forcing test:
    * `ExecutionBasePinReExecutionSuite` "forcing (iii)" — mid-fold watermark skew.)
    *
    * '''Fail-closed.''' `None` when the anchor is unresolvable (evicted below the byte store's retention — logarithmic on the follower
    * rail, contiguous k₂ on gl0 — or not yet reached). The caller then OMITs (defers) / maps to the `Hash.empty` "cannot re-derive"
    * sentinel, which the consumers treat as "can't check" (plain reject / dispute-not-upheld) — NEVER a live-base substitute, NEVER a
    * slash.
    */
  def pinnedPriorReaderAt[F[_]](
    pinnedReader: PinnedCurrencyInfoReader[F]
  ): SnapshotOrdinal => F[Option[GlobalStateReader[F]]] =
    (ord: SnapshotOrdinal) => pinnedReader.pinnedReaderAt(ord)

  /** Track-1 execution-base-pin — the ordinal the producer STAMPS as
    * [[io.constellationnetwork.schema.sharding.ShardCheckpoint.executionBaseOrdinal]] and cuts every per-MG diff over: the NEWEST ordinal
    * the version-retained signed byte store can actually SERVE (its latest persisted state), NOT the live `mptStore.lastPersistedOrdinal`.
    *
    * '''Why not the live watermark.''' [[pinnedPriorReaderAt]] resolves the diff prior EXCLUSIVELY through the version-retained,
    * root-verified pinned reader (see its scaladoc for the mid-fold-skew wedge the live fast path caused). The signed byte store is written
    * at the FINALIZE sink and therefore TRAILS `lastPersistedOrdinal` by a few ordinals — stamping the live watermark would make the pinned
    * read miss on almost every mint (`cannot resolve pinned execution-base — OMIT (defer)`, the DAG4Bawb producer chase in the 2026-07-08
    * run) and stall checkpoint production. Stamping the store's own latest ordinal makes the base resolvable-by-construction on the minting
    * node; committee re-executors and gl0 adopters resolve it from their own (finalize-synchronized) signed stores.
    *
    * `SnapshotOrdinal.MinValue` before the first finalize-sink write — `produceInner` then OMITs (defers) until history exists, which is
    * exactly the fail-closed contract. `numShards = 1` never builds checkpoints, so this is dead there (regression bar preserved).
    */
  def pinnedExecutionBaseOrdinal[F[_]: Async](signedBytesStore: MptStateStorage[F]): F[SnapshotOrdinal] =
    signedBytesStore.findLatestOrdinal.map(_.getOrElse(SnapshotOrdinal.MinValue))

  /** The pinned per-MG derivation shared by producer and verifier. It returns only the recreated per-MG MPT root. Every GL0 verifier
    * independently recreates the state.
    *
    * '''Diff base = `S(N)` from the adopted, chain-linked best-tip (PIN-4 — NOT empty-prior, NOT the undo journal).''' The committee +
    * every verifier are gl0 nodes that ALREADY adopted checkpoint N (the chain-link guard enforces in-order adoption), so `S(N)` — the
    * prior checkpoint's cumulative per-MG currency state — is already in their overlay best-tip. `priorStateReader` is exactly that
    * best-tip `GlobalStateReader`; `S(N)` is reconstructed from it via [[GlobalStateConverter.reconstructCurrencyInfoFrom]] (all eight
    * serialized `Mg*` fields, including the transitional root-excluded sync view, plus fieldId-7 allow-spends) and the fieldId-5
    * incremental. Snapshots execute IN ORDER (chain-link), so cumulative state, allow-spends, and token-locks accumulate. The
    * per-currency-snapshot `gl0AnchorOrdinal` is the metagraph's fee-cutover/exec CONTEXT only.
    *
    * '''Derivation.''' Runs the SAME full recreation as the global adopter, with finalized global-snapshot lookup, and seeds
    * `priorLastCurrencySnapshots` with `S(N)` (`Right((priorInc, S(N)))`, or `Left(genesis)` at the metagraph's genesis window, or absent
    * for a never-seen MG) INSTEAD of `SortedMap.empty`. The LAST resulting per-MG `CurrencySnapshotWithState` is `next` (mirrors
    * `calculateLastCurrencySnapshots`).
    *
    * '''Root (PIN-1).''' `root = GlobalStateConverter.currencySnapshotMgRoot(SortedMap(mg -> next))` — the COMPONENT-ADDRESSABLE per-MG MPT
    * root (the `rootHash` of the standalone trie over the MG's fieldId-5 incremental + `infoSubFields` `Mg*` entries, one leaf per
    * account), NOT the old flat `Hasher.hash((incrementalRoot, infoRoot))` (which could not back a single-leaf inclusion proof) and NOT the
    * an empty-prior or Some/None-sensitive hash. This `Hash` is what `perMetagraphMptRoots(mg)` carries (that field is `SortedMap[Address,
    * Hash]`); the gl0 verifier recomputes the IDENTICAL `currencySnapshotMgRoot` over its post-apply state, and `ShardSubtreeProofService`
    * witnesses a single `(field, account)` leaf against it. '''All three PIN-1 sites + TaskB must route through
    * `currencySnapshotMgRoot`.'''
    */
  def reExecDerivationAtPinnedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    // Track-1 execution-base-pin: the prior reader is resolved PER CALL at the checkpoint's `executionBaseOrdinal` (the 4th closure arg), NOT fixed
    // at construction to the node-local live base. This is what makes the producer's diff-prior + derivation-prior and every re-executor's
    // (committee/watchtower) read the SAME pinned base `S(N)`. `None` ⇒ this node cannot resolve the pinned base (evicted below retention,
    // or not reached) ⇒ OMIT (defer) rather than derive over a WRONG base. Callers wire only the version-retained pinned reader.
    priorReaderAt: SnapshotOrdinal => F[Option[GlobalStateReader[F]]],
    // Currency recreation resolves the snapshot's signed `globalSyncView` through this finalized, hash-checked lookup. Supplying a
    // node-local head or `None` would either fork the transition inputs or make the verifier fall back to trusting claimed fields.
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => F[Option[Hash]] = {
    import GlobalStateReaderOps._
    type CurrencyState = Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]

    val emptyInfo: CurrencySnapshotInfo =
      CurrencySnapshotInfo(SortedMap.empty, SortedMap.empty, None, None, None, None, None, None, None)

    // [REEXEC-DIAG] (2026-06-13, REMOVE after e2e): pin why the producer emits the empty-state fallback (96271a6c) for every MG —
    // priorState/getCurrencySnapshotInfo values, the lastStateOpt=None branch, and the SWALLOWED handleErrorWith exception.
    val reExecDiagLogger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring.reExecDiag")
    def descPrior(p: Option[CurrencyState]): String = p match {
      case None                     => "None"
      case Some(Left(g))            => s"Left(genesis@${g.value.ordinal.value.value})"
      case Some(Right((inc, info))) => s"Right(inc@${inc.value.ordinal.value.value},bal=${info.balances.size})"
    }

    /** S(N) for this MG from the adopted best-tip: `Right((priorInc, info))` (has an incremental), `Left(genesis)` (post-genesis
      * pre-first-incremental window), or `None` (never seen by gl0 ⇒ empty prior; the genesis binary in the window seeds it).
      */
    def priorState(priorStateReader: GlobalStateReader[F], mg: Address): F[Option[CurrencyState]] =
      priorStateReader.getLastIncrementalCurrencySnapshot(mg).flatMap {
        case Some(inc) =>
          priorStateReader
            .getCurrencySnapshotInfo(mg)
            .map {
              case Some(info) => Right((inc, info)): CurrencyState
              case None       => Right((inc, emptyInfo)): CurrencyState // defensive: incremental present but info empty
            }
            .map(_.some)
        case None =>
          priorStateReader.getLastCurrencySnapshot(mg).map(_.map(g => Left(g): CurrencyState))
      }

    (
      mg: Address,
      binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
      gl0AnchorOrdinal: SnapshotOrdinal,
      executionBaseOrdinal: SnapshotOrdinal
    ) =>
      // Track-1 execution-base-pin: resolve the prior reader AT `executionBaseOrdinal` (the producer's stamped base, cluster-uniform). A `None`
      // means this node cannot serve the pinned base (below retention, or not yet reached) — OMIT (defer) rather than seed the derivation
      // from a WRONG base and attest a root no honest verifier reproduces.
      priorReaderAt(executionBaseOrdinal).flatMap {
        case None =>
          reExecDiagLogger
            .warn(
              s"[execution-base-pin] mg=${mg.value.value.take(10)} cannot resolve pinned execution-base ord=${executionBaseOrdinal.value.value} " +
                s"(evicted/not-reached) — OMIT (defer)"
            )
            .as(None: Option[Hash])
        case Some(priorStateReader) =>
          // The derivation prior is the full pinned S(N) CurrencyState; the genesis `Left` is needed to seed the state fold.
          priorState(priorStateReader, mg).flatMap { priorOpt =>
            val priorMap: SortedMap[Address, CurrencyState] =
              priorOpt.fold(SortedMap.empty[Address, CurrencyState])(p => SortedMap(mg -> p))

            reExecDiagLogger.info(
              s"[REEXEC-DIAG] mg=${mg.value.value.take(10)} anchor=${gl0AnchorOrdinal.value.value} windowSize=${binaries.size} " +
                s"priorOpt=${descPrior(priorOpt)}"
            ) >>
              // Full currency recreation against the checkpoint's pinned prior and the snapshot's pinned finalized GL0 view. The structured
              // checkpoint boundary takes the canonical OLDEST-FIRST input and refuses to derive a root unless the processor returned that
              // exact complete Signed-binary sequence. A valid prefix followed by an invalid binary is not execution of the signed window.
              processor
                .processCurrencySnapshotsWithCompleteConsumption(
                  gl0AnchorOrdinal,
                  SortedMap.empty[Address, Balance],
                  priorMap,
                  SortedMap(mg -> binaries),
                  getGlobalSnapshotByOrdinal
                )
                .flatMap { replay =>
                  replay.completeResult(mg) match {
                    case None =>
                      reExecDiagLogger
                        .warn(
                          s"[REEXEC-DIAG] mg=${mg.value.value.take(10)} checkpoint window was only partially or ambiguously processed " +
                            s"(windowSize=${binaries.size}, disposition=${replay.consumption(mg)}) — OMIT (defer)"
                        )
                        .as(None: Option[Hash])
                    case Some((pairs, _)) =>
                      // Mirror calculateLastCurrencySnapshots: the LAST resulting state across the completely re-executed chain is `next`.
                      val lastStateOpt: Option[CurrencyState] = pairs.toList.flatMap(_._2).lastOption
                      lastStateOpt match {
                        case Some(next) =>
                          // The root and optional transport diff are cut over the full recreation output. No metagraph or committee field
                          // replaces `infoOf(next)`.
                          // CONTIGUITY (I3 / run-27b) — RELAXED to advisory. The window anchors at `finalizedBasePerMgTip` (S2 §4) and the diff prior
                          // is read at `executionBaseOrdinal` — the SAME finalized base by construction — so a window chaining from the base advances
                          // `base.ordinal` by EXACTLY `windowSize`; the old OMIT is now an invariant. Keep the check as a diagnostic only (a violation
                          // means a base read-skew, caught fail-closed downstream by the adopter's now-unconditional GAP-1 balances/refs compare — a
                          // drop, never silent corruption) and PROCEED to derive rather than defer (removing a false-defer liveness hazard).
                          val contiguousWithBase: Boolean = (priorOpt, next) match {
                            case (Some(Right((priorInc, _))), Right((nextInc, _))) =>
                              nextInc.value.ordinal.value.value === priorInc.value.ordinal.value.value + binaries.size.toLong
                            case _ => true
                          }
                          for {
                            _ <-
                              if (contiguousWithBase) Async[F].unit
                              else
                                reExecDiagLogger.warn(
                                  s"[REEXEC-DIAG] mg=${mg.value.value.take(10)} window NOT contiguous with pinned execution-base " +
                                    s"(${descPrior(priorOpt)} windowSize=${binaries.size} nextOrd=${next.toOption
                                        .map(_._1.value.ordinal.value.value)
                                        .getOrElse(-1L)}) — base read-skew; proceeding (the adopter's full re-exec root check is fail-closed)"
                                )
                            // PIN-1: COMPONENT-ADDRESSABLE per-MG root — the rootHash of the MG sub-trie over the fieldId-5 incremental + the
                            // `infoSubFields` `Mg*` entries (one leaf per account), via the shared `currencySnapshotMgRoot`. The gl0 follower
                            // recomputes the IDENTICAL `currencySnapshotMgRoot` over its post-apply state — all three PIN-1 sites route through that
                            // one helper, so the bytes are identical by construction.
                            root <- GlobalStateConverter.currencySnapshotMgRoot[F](SortedMap(mg -> next))
                            // DIAG: committee's attested per-sub-field root breakdown — match `root=` here to gl0's
                            // `[ACCEPTANCE/ADOPT-VERIFY] attested=` line to pin the diverging half (inc vs info) + `Mg*` sub-field.
                            cmtDiag <- GlobalStateConverter.currencySnapshotFieldRootsDiag[F](SortedMap(mg -> next))
                            _ <- reExecDiagLogger.info(
                              s"[REEXEC-FIELDS] mg=${mg.value.value.take(10)} root=${root.value.take(16)} $cmtDiag"
                            )
                          } yield Some(root): Option[Hash]
                        case None =>
                          // OMIT-ON-CAN'T-DERIVE (2026-06-13). The derivation produced NO state — the genesis-bootstrap race: this MG's
                          // genesis was already consumed by an earlier shard checkpoint (perMgTip advanced past it) BUT this producer node's
                          // best-tip prior reader has not yet seen gl0 ADOPT that genesis (the ~6-min embed/quorum warmup), so `priorOpt=None`
                          // AND the window head is a non-genesis incremental → `processCurrencySnapshots`'s genesis-window
                          // guard drops the window. We must NOT commit an empty-state root + empty diff: once committee-quorumed that
                          // "couldn't-derive" sentinel is a PERMANENT lie — every gl0 later recomputes the real non-empty root from its
                          // now-adopted S(N), mismatches the attested empty sentinel forever, and drops the MG's currency advance (the run-26
                          // freeze). Instead OMIT this MG: its binaries stay pending, `perMgTip` does not advance, and it re-derives correctly
                          // on a later checkpoint once the prior is adopted (the pipeline self-heals).
                          reExecDiagLogger
                            .warn(s"[REEXEC-DIAG] mg=${mg.value.value.take(10)} lastStateOpt=None — OMIT (defer until prior adopted)")
                            .as(None: Option[Hash])
                      }
                  }
                }
                .handleErrorWith { e =>
                  // A derivation crash is likewise NOT a committable state — OMIT this MG (defer) rather than attest an empty-state root
                  // every verifier would mismatch. The MG re-derives cleanly on a later checkpoint over the same chain.
                  reExecDiagLogger
                    .warn(e)(s"[REEXEC-DIAG] mg=${mg.value.value.take(10)} DERIVATION CRASH → OMIT (defer)")
                    .as(None: Option[Hash])
                }
          }
      } // close priorReaderAt(executionBaseOrdinal).flatMap
  }

  /** Build the acceptance-side sharding dependencies, gated on `cfg.numShards > 1`.
    *
    * @param cfg
    *   the typed [[ShardingConfig]] from `cfg.nakamoto.sharding`. `numShards <= 1` ⇒ returns `None` (regression bar; nothing constructed).
    * @param kDraw
    *   committee DRAW target (cluster-uniform `cfg.nakamoto.committee.kDraw`). Threaded into [[committeeFor]]'s public VK-hash draw — sizes
    *   the enumerated shard committee (`≈ kDraw`; `= N` saturates ⇒ everyone). Decoupled from the admit quorum.
    * @param kQuorum
    *   mandatory execution quorum (cluster-uniform `cfg.nakamoto.committee.kQuorum`). The producer-side tracker and the deterministic
    *   embedded checkpoint verifier both compare distinct execution signers directly against this value.
    * @param selfPeerId
    *   this GL0 operator's PeerId. Threaded into each per-shard [[ShardTipTracker]] (optional self-excluded diagnostics) and the acceptance
    *   manager (diagnostic logging of which op spotted a deviation).
    * @param operatorKeyRegistry
    *   atomic registered KES+VRF identities. [[committeeFor]] uses the VRF half as the deterministic draw seed and the acceptance manager
    *   uses the KES half for per-signer verification. The projections come from this single object, so a VRF-only committee identity cannot
    *   exist. It MUST be identical cluster-wide on every path that runs `verifyEmbedded`; an absent operator is never sortitioned.
    * @param activeValidators
    *   callback returning the current active gl0 validator set — the candidate pool [[committeeFor]] sortitions over (also fixes `σ =
    *   1/N`). Read on every checkpoint pre-check so a validator-set change (registration/slashing) is observed without reconstruction. MUST
    *   be identical cluster-wide (seedlist minus `metagraph-op`).
    * @param etaForEpoch
    *   resolves the 32 raw eta-randomness bytes for a given eta-period — the SAME `EtaStateManager.getEta`-backed resolver the GSAM
    *   boundary writer uses (returns a hex [[Hash]] at the call site; decode to 32 bytes via `Hex(h.value).toBytes`). Feeds
    *   [[committeeFor]] keyed on the wire-carried `checkpoint.epoch`, so producer + every verifier draw the SAME committee for the SAME
    *   epoch even across an eta boundary. MPT-committed ⇒ byte-identical cluster-wide. ALSO feeds the per-shard `shardEtaFor` resolver
    *   built below (the gl0 eta → `ShardSlotLeader.computeShardEta(shardId, gl0Eta)` chain) that the acceptance manager's real
    *   committee-VRF verify consumes — so the eta the committee was DRAWN under and the eta each signer's VRF proof is VERIFIED under share
    *   one source.
    * @param etaRotationSnapshots
    *   positive execution-committee rotation length `R`. The acceptance manager derives the only valid wire checkpoint epoch as
    *   `floor(gl0AnchorOrdinal / R)` before committee lookup, using the same pure helper as the producer. Production currently injects this
    *   from node HOCON. This rejects only inconsistent ordinal/epoch pairs: because an older `gl0AnchorOrdinal` remains admissible, a
    *   producer can still select an older favorable epoch and its matching committee. A canonical genesis/rooted parameter commitment and
    *   exact proposal-parent hash-bound Phase-2 anchor/freshness proof remain RED consensus gates.
    * @param reExecuteDerivation
    *   the pinned-base re-exec derivation closure. `Some(...)` recomputes the canonical per-MG root and rejects (+ flags slash signers) on
    *   a byte-mismatch. `None` (the default) ⇒ [[noReExecDerivation]] (fail-closed: non-empty checkpoints are rejected on the `Hash.empty`
    *   sentinel, never falsely admitted). Modelled as `Option` rather than a defaulted closure because Scala can't resolve `Async[F]` for
    *   `noReExecDerivation[F]` at the default-arg site (the context bound is on the method, not on the default expression) — the same
    *   constraint the GSAM `localEventsPublisher` param hits.
    * @param slashCooldownReader
    *   FINDING-002/EPIC-3.1 — the per-operator cooldown gate over the `Slashings` (fieldId 34) partition [[committeeFor]] excludes on.
    *   Production (`SharedServices`) passes `Some(SlashCooldownReader.fromMptStore(storages.mptStore, R))` — the SAME store + eta-period
    *   length the committee's `etaForEpoch` resolver reads, so the exclusion is a pure function of the wire-carried epoch (see the reader's
    *   scaladoc for the anchor/uniformity contract). MUST be identical cluster-wide on every path that runs `verifyEmbedded`, or the
    *   committee — and thus the adopt decision — diverges (#261). `None` ⇒ [[SlashCooldownReader.noExclusion]] (draw byte-identical to the
    *   no-exclusion path; primarily useful for focused tests, with the same modelling constraint as `reExecuteDerivation`).
    */
  def acceptanceDeps[F[_]: Async: Hasher: SecurityProvider: Metrics](
    cfg: ShardingConfig,
    etaRotationSnapshots: Long,
    kDraw: Int,
    kQuorum: Int,
    selfPeerId: PeerId,
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    activeValidators: F[Set[PeerId]],
    etaForEpoch: EtaPeriod => F[Array[Byte]],
    reExecuteDerivation: Option[
      (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => F[Hash]
    ] = None,
    slashCooldownReader: Option[SlashCooldownReader[F]] = None
  ): F[Option[AcceptanceDeps[F]]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring")
    val reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => F[Hash] =
      reExecuteDerivation.getOrElse(noReExecDerivation[F])
    val slashCooldown: SlashCooldownReader[F] =
      slashCooldownReader.getOrElse(SlashCooldownReader.noExclusion[F])

    if (cfg.numShards <= 1)
      // Regression bar: at the production default `numShards = 1`, construct NOTHING and return None. The two GSAM call sites then pass
      // `None` for all three sharding params — byte-identical to the pre-wiring call. The single debug log is the only observable effect.
      logger
        .debug(s"sharding inactive (numShards=${cfg.numShards} <= 1); GSAM sharding deps = None (regression bar)")
        .as(Option.empty[AcceptanceDeps[F]])
    else
      for {
        registry <- buildRegistry[F](cfg, kQuorum, selfPeerId)
        // MEMOIZE the committee draw per (shardId, epoch) (2026-06-10, run b31yifl23). `committeeFor` runs one
        // VRF-sortition Hasher.hash PER VALIDATOR (~N hashes) every call, and the closure is invoked once per
        // checkpoint per CANDIDATE BRANCH during `verifyEmbedded`/`preCheck` — so under a near-tip reorg storm
        // (1650 reorg events observed) it fired ~N×1650 ≈ 165k hashes/run, the allocation spike that drove 4/5
        // gl0 nodes across the -Xmx cliff into G1 death-thrash (HTTP starved → e2e poll timeout). The committee is
        // a DETERMINISTIC function of (shardId, epoch) alone — `activeValidators` (seedlist), the atomic operator-key registry
        // (genesis/registration), `etaForEpoch(epoch)` (MPT-committed), `kDraw`/σ, and the epoch-anchored slash
        // exclusion (FINDING-002 — a pure function of the epoch over the append-only fieldId-34 records at the
        // epoch's settled anchor) are all cluster-uniform — so the cache is semantically identical to recomputing
        // (split-safe; pure optimization). SETTLED-ONLY caching: a draw for an epoch whose slash anchor this node's
        // base has NOT yet reached (`committeeForMeta._2 = false` — an early/adversarial draw for a far-future
        // wire-carried epoch) is returned but NOT memoized, so it cannot pin a stale exclusion for that epoch (the
        // recompute at real use time, anchor settled, is exact) — mirroring EtaStateManager's rule of never caching
        // its degenerate fallback. Unbounded growth is a non-issue: cardinality = numShards × in-flight epochs
        // (~handful). Mirrors EtaStateManager.walkCacheRef.
        committeeCache <- Ref.of[F, Map[(ShardId, EtaPeriod), Set[PeerId]]](Map.empty)
        committeeMembership = (shardId: ShardId, epoch: EtaPeriod) =>
          committeeCache.get.flatMap { cache =>
            cache.get((shardId, epoch)) match {
              case Some(members) => Async[F].pure(members)
              case None =>
                committeeForMeta[F](
                  shardId,
                  epoch,
                  activeValidators,
                  operatorKeyRegistry,
                  etaForEpoch,
                  kDraw,
                  kQuorum,
                  slashCooldown
                ).flatMap {
                  case (members, cacheable) =>
                    Async[F].whenA(cacheable)(committeeCache.update(_.updated((shardId, epoch), members))).as(members)
                }
            }
          }
        // Per-shard registered-key proof eta resolver. Same `etaForEpoch` source the public committee draw reads, fed through the same
        // `ShardSlotLeader.computeShardEta(shardId, gl0Eta)` derivation the producer's
        // `ShardCheckpointAttestationEmitter` uses — so the eta a signer's `vrfProof` is VERIFIED under byte-matches the eta the producer
        // SIGNED under, across an eta boundary (keyed on the wire-carried `checkpoint.epoch`). `computeShardEta` is Hasher-only (no
        // EligibilityChecker needed here). MPT-committed eta ⇒ cluster-uniform. Returns `Some(_)` always on the active path (the eta is
        // always resolvable once the boundary is written); `None` fails verification closed.
        shardEtaFor = (sid: ShardId, epoch: EtaPeriod) =>
          etaForEpoch(epoch).flatMap(gl0Eta => ShardSlotLeader.computeShardEta[F](sid, gl0Eta)).map(_.some)
        shardAssignment = ShardAssignment.make[F](cfg.numShards)
        producerDutyValidator = ShardCheckpointProducerDutyValidator.make[F](
          staircaseDeltaSlots = cfg.checkpoint.staircaseDeltaSlots,
          shardEtaFor = shardEtaFor,
          parentCheckpoint = (sid: ShardId, parentHash: Hash) =>
            registry
              .get(sid)
              .fold(Async[F].pure(Option.empty[io.constellationnetwork.schema.sharding.ShardCheckpoint]))(
                _.chainStore.getByHash(parentHash).map(_.map(_.signed.value))
              )
        )
        acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[F](
          executionQuorum = kQuorum,
          etaRotationSnapshots = etaRotationSnapshots,
          committeeMembership = committeeMembership,
          operatorKeyRegistry = operatorKeyRegistry,
          shardAssignment = shardAssignment,
          shardEtaFor = shardEtaFor,
          producerDutyValidator = producerDutyValidator,
          reExecuteDerivation = reExec
        )
        _ <- logger.info(
          s"sharding ACTIVE: numShards=${cfg.numShards} kDraw=$kDraw kQuorum=$kQuorum " +
            s"retainedCheckpoints=${cfg.retention.retainedCheckpoints} — built per-shard registry (${registry.size} shards) + " +
            s"gl0 acceptance manager " +
            s"(public deterministic VK-hash committee draw)"
        )
      } yield Some(AcceptanceDeps(cfg, acceptanceManager, shardAssignment, registry, committeeMembership))
  }

  /** Construct the per-shard `(ShardChainStore, ShardTipTracker, ShardFinalityTriggers)` registry for shards `0 .. numShards - 1`.
    *
    * Each shard's chain store is bounded by `retention.retainedCheckpoints`. The execution-quorum tracker is constructed but not advanced
    * here; advancing happens once checkpoints arrive. Retention is not a checkpoint-qualification threshold.
    */
  private[sharding] def buildRegistry[F[_]: Async: Hasher: Metrics](
    cfg: ShardingConfig,
    kQuorum: Int,
    selfPeerId: PeerId
  ): F[Map[ShardId, ShardRegistryEntry[F]]] =
    (0 until cfg.numShards).toList.traverse { idx =>
      val shardId = ShardId(NonNegInt.unsafeFrom(idx))
      for {
        chainStore <- ShardChainStore.make[F](shardId, keepDepthBehindFinalized = cfg.retention.retainedCheckpoints)
        tipTracker <- ShardTipTracker.make[F](shardId, selfPeerId)
        triggers <- ShardFinalityTriggers.make[F](
          shardId = shardId,
          kQuorum = kQuorum,
          chainStore = chainStore,
          tipTracker = tipTracker
        )
        // Per-shard admission-approved binary accumulator. The SAME instance feeds the gate output and producer fan-out, so
        // shared instance IS the inversion: the producer reads buffered binaries here instead of gl0's post-chain-link map.
        binaryBuffer <- ShardBinaryBuffer.make[F](shardId, cap = cfg.checkpoint.binaryBufferCap)
      } yield shardId -> ShardRegistryEntry(chainStore, tipTracker, triggers, binaryBuffer)
    }
      .map(_.toMap)

  /** Public deterministic VK-hash `committeeFor(shardId, epoch)` — the committee set for one `(shard, epoch)`.
    *
    * '''Algorithm.''' Resolve the epoch's SLASH-COOLDOWN exclusion first (`slashCooldown.excludedForEpoch(epoch)` — FINDING-002/EPIC-3.1,
    * see [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.SlashCooldownReader]] for the epoch-anchored uniformity contract)
    * and drop the excluded operators from the candidate pool, never below `kQuorum` eligible validators (the Polkadot `UpToLimit` floor in
    * `SlashCooldownReader.effectiveExclusion`). Then enumerate the POST-EXCLUSION pool in a STABLE order (sorted by `PeerId`, so iteration
    * is order-independent); for each operator resolve its atomic registered pair and keep it iff its VRF VK satisfies
    * `CommitteeSortition.isInShardCommittee(vrfVk, eta, shardId, epoch, σ, kDraw)` — i.e. its `H(eta, shardId, epoch, vrfVk)` draw value
    * falls below `threshold(kDraw, σ)`. An operator with no registered atomic pair is removed before the draw. `σ` is an independent
    * uniform `1/N` execution-draw weight, computed from the preregistered, active, post-cooldown pool (the draw threshold's denominator
    * shrinks with the eligible set) so `threshold = kDraw/N` and the expected committee size stays `≈ kDraw` over the eligible operators
    * (and `kDraw >= N` saturates to every eligible one). The admit quorum is the SEPARATE `kQuorum`, applied in `verifyEmbedded` /
    * `ShardFinalityTriggers`, NOT this draw — but because every checkpoint signer must pass the committee-membership pre-check, an operator
    * excluded HERE can never count toward that quorum either (the FINDING-002 "slash never bites" closure).
    *
    * '''Determinism (the #261 split invariant).''' Every input is identical on every gl0 node:
    *   - `activeValidators` — seedlist (minus `metagraph-op`), loaded identically cluster-wide;
    *   - each operator's VRF VK — currently from the `vrfVk` in a long-term-signed atomic `L0GenesisData.operators` record, loaded through
    *     the same paired genesis registry as its KES master VK (`L0GenesisLoader.buildOperatorKeyRegistry`). Runtime activation/rotation
    *     requires E2K's branch-historical unified registry;
    *   - `eta` — `etaForEpoch(epoch)` resolves the per-period eta from the MPT-committed `HistoricalStakeSnapshot.eta` (`EtaStateManager`),
    *     byte-identical cluster-wide once the boundary is written, keyed on the WIRE-CARRIED `checkpoint.epoch`;
    *   - `kDraw` / `kQuorum` — HOCON `nakamoto.committee.{k-draw,k-quorum}`, the same on every node;
    *   - the excluded set — a pure function of `epoch` over the consensus-written, append-only `Slashings` (fieldId 34) records at the
    *     epoch's anchor ordinal (one full eta-period below the draw — inside the k₁ write-frozen common prefix), read from the SAME
    *     `mptStore` the eta resolver reads;
    *   - `σ = 1/|pool|` — derived from the same `activeValidators`, intersected with the registered-pair population, minus the same
    *     excluded set. No node-local state (no chain height, no Refs, no wall-clock) enters the draw, so `committeeFor(shardId, epoch)`
    *     resolves to the SAME `Set[PeerId]` on every node — the hard requirement for `verifyEmbedded` to admit byte-identically.
    *
    * The result is returned as a sorted-order traversal collapsed into a `Set` (membership + size are all the callers need); the traversal
    * order does not affect the resulting set.
    */
  private[sharding] def committeeFor[F[_]: Async: Hasher](
    shardId: ShardId,
    epoch: EtaPeriod,
    activeValidators: F[Set[PeerId]],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    etaForEpoch: EtaPeriod => F[Array[Byte]],
    kDraw: Int,
    kQuorum: Int,
    slashCooldown: SlashCooldownReader[F]
  ): F[Set[PeerId]] =
    committeeForMeta[F](shardId, epoch, activeValidators, operatorKeyRegistry, etaForEpoch, kDraw, kQuorum, slashCooldown).map(_._1)

  /** [[committeeFor]] plus the CACHEABILITY verdict: `_2 = false` iff the epoch's slash anchor is not yet settled on this node's base
    * (`SlashCooldownReader.EpochExclusion.anchorSettled`), in which case the excluded set may still be incomplete and the result MUST NOT
    * be memoized (the `acceptanceDeps` committee cache consults this — an early/adversarial draw for a far-future wire-carried epoch must
    * not pin a stale committee for that epoch; a fresh draw at real use time, anchor long settled, is exact and identical cluster-wide).
    */
  private[sharding] def committeeForMeta[F[_]: Async: Hasher](
    shardId: ShardId,
    epoch: EtaPeriod,
    activeValidators: F[Set[PeerId]],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    etaForEpoch: EtaPeriod => F[Array[Byte]],
    kDraw: Int,
    kQuorum: Int,
    slashCooldown: SlashCooldownReader[F]
  ): F[(Set[PeerId], Boolean)] =
    (activeValidators, etaForEpoch(epoch), slashCooldown.excludedForEpoch(epoch)).tupled.flatMap {
      case (validators, eta, exclusion) =>
        // Registration is an eligibility prerequisite, so filter it BEFORE computing the draw denominator or cooldown floor. Counting a
        // seedlist-only peer in N and dropping it afterward depresses the expected committee size and can make kQuorum unreachable.
        validators.toList
          .sortBy(_.value.value)
          .traverse { peerId =>
            ActiveOperatorConsensusKeys.resolve(operatorKeyRegistry, peerId, epoch).map(_.map(peerId -> _))
          }
          .flatMap { resolved =>
            val registered = resolved.flatten.toMap
            val registeredPeers = registered.keySet
            // FINDING-002/EPIC-3.1: drop unexpired-cooldown operators before the draw, floored so at least `kQuorum` preregistered validators
            // remain. Empty `Slashings` means the pool is exactly the registered active population.
            val excluded = SlashCooldownReader.effectiveExclusion(registeredPeers, exclusion.candidates, kQuorum)
            val pool = registered -- excluded
            val n = pool.size
            if (n <= 0) Async[F].pure((Set.empty[PeerId], exclusion.anchorSettled))
            else {
              val sigma = Ratio(1, n) // uniform draw weight over the eligible registered pool
              pool.toList
                .sortBy(_._1.value.value)
                .traverse {
                  case (peerId, registeredPair) =>
                    CommitteeSortition
                      .isInShardCommittee[F](registeredPair.vrfPublicKey.toBytes, eta, shardId, epoch, sigma, kDraw)
                      .map(if (_) Some(peerId) else None)
                }
                .map(members => (members.flatten.toSet, exclusion.anchorSettled))
            }
          }
    }
}
