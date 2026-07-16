package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.Parallel
import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.{Map, SortedMap}

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.ShardingConfig
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.domain.nakamoto.slashing.{InvalidStateProofBatchReplay, SlashCooldownReader}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  GlobalSnapshotStateChannelEventsProcessor,
  ShardCheckpointGl0AcceptanceManager,
  SpendTransactionBalanceManager
}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
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
  * '''reExecuteDerivation — transitional caller-supplied replay.''' Current receive and GL0-adoption paths replay the complete checkpoint
  * batch in one canonical metagraph order and compare every recomputed `mptRoot` byte-for-byte against the committee-signed value. Each
  * production call site wires [[reExecDerivationsAtPinnedBaseBatch]] from its [[GlobalSnapshotStateChannelEventsProcessor]] and a reader
  * for the retained execution base. The scalar [[reExecDerivationAtPinnedBase]] delegates to that batch implementation for focused one-MG
  * callers. The `None`/[[noReExecDerivation]] fallback (fail-closed `Hash.empty`) remains for callers without a processor.
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
    *     snapshot) and the signers are NOT slash targets. `RejectedReExecutionMismatch` is only a typed local rejection/diagnostic result;
    *     it cannot reach the consensus slash sink. Only artifact-carried fraud evidence independently revalidated by every GL0 node may
    *     create a `WatchtowerSlashRequest`. A sentinel from an unwired closure (or an unresolvable pinned execution-base) is not evidence
    *     of committee deviation. The checkpoint is never admitted while replay is unavailable, and honest signers are not slashed for a
    *     local inability to replay.
    */
  def noReExecDerivation[F[_]: Async]
    : (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => F[Hash] =
    (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: GlobalSnapshotStateRef) =>
      Async[F].pure(Hash.empty)

  /** The single reader-resolution recipe for every [[reExecDerivationAtPinnedBase]] caller: resolve the finalized [[GlobalStateReader]] at
    * the wire-carried, committee-signed exact `executionBase`, never at this node's live base when the two differ.
    *
    * '''Why one definition.''' Full currency recreation is base-dependent: balances, references, active sets, and messages all begin at the
    * pinned prior, and `currencySnapshotMgRoot` commits the recreated result. So every rail that recomputes the root for comparison against
    * `perMetagraphMptRoots(mg)` — the gl0 produce/watchtower rail (`GlobalSnapshotConsensus.finalizedReaderAt`), the SharedServices
    * unconditional `reExecuteDerivation`, and the SharedServices `createContext` fraud-proof validator — MUST read the SAME pinned base the
    * producer executed over, or an honest committee's root is not reproduced. A local mismatch now rejects without slashing; independently
    * replayed, artifact-carried fraud evidence can still reach the 100% `InvalidStateProof` slash, so exact base agreement remains
    * necessary to prevent false evidence and adopt-decision splits. One shared definition keeps all three rails byte-identical, mirroring
    * [[reExecDerivationAtPinnedBase]] itself.
    *
    * '''Resolution — ALWAYS the version-retained [[PinnedCurrencyInfoReader.pinnedReaderAt]]''' (which verifies the retained bytes
    * reproduce the pinned snapshot's committed `mptRoot`). There is deliberately NO live-store fast path. The original fast path served
    * `GlobalStateReader.fromMptStore(mptStore)` whenever `ord == mptStore.lastPersistedOrdinal` — but that equivalence is UNSOUND: the live
    * store is a MUTABLE VIEW whose content-vs-watermark relationship is unsynchronized. In Passthrough overlay mode the accept-path writes
    * land in the base store THROUGHOUT an ordinal's processing and `MptStore.commit(ordinal)` bumps `lastPersistedOrdinal` only at the very
    * end, so `lastPersistedOrdinal == N` holds while the content is anywhere from state@N to a MID-FOLD/POST-FOLD state of N+1+. Live wedge
    * (2026-07-08, 2mg/2shard token-lock e2e): gl0-1 minted shard-0 `shardOrdinal=8` stamped base ordinal 18 while its live store already
    * carried the shardOrd-7 adopt (prior read `inc@10,bal=15`; the TRUE committed state@18 was `inc@7,bal=14`) — the diff was cut over the
    * drifted content, quorum attested it (every committee member's fast path saw the same drifted view), and every honest adopter —
    * applying the wire diff onto the verified state@18 — recomputed a root that never matched the attested one. The checkpoint re-offered
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
  ): GlobalSnapshotStateRef => F[Option[GlobalStateReader[F]]] =
    (executionBase: GlobalSnapshotStateRef) => pinnedReader.pinnedReaderAt(executionBase)

  /** Locate the newest retained ordinal that can be considered as an execution-base candidate. This ordinal is not replay authority: the
    * producer must resolve it to a complete [[GlobalSnapshotStateRef]] before replay or signing. The current caller checks that claim
    * against its transitional finality view; the target hash-bound Phase-2 lease remains separate. The locator reads the version-retained
    * signed byte store's latest persisted state, not the live `mptStore.lastPersistedOrdinal`.
    *
    * '''Why not the live watermark.''' [[pinnedPriorReaderAt]] resolves the diff prior EXCLUSIVELY through the version-retained,
    * root-verified pinned reader (see its scaladoc for the mid-fold-skew wedge the live fast path caused). The signed byte store is written
    * at the FINALIZE sink and therefore TRAILS `lastPersistedOrdinal` by a few ordinals — stamping the live watermark would make the pinned
    * read miss on almost every mint (`cannot resolve pinned execution-base — OMIT (defer)`, the DAG4Bawb producer chase in the 2026-07-08
    * run) and stall checkpoint production. Selecting the store's own latest ordinal makes the candidate locally retained; complete identity
    * resolution and byte/root verification still decide whether it can become a signed claim. Phase-2 authority is a separate prerequisite.
    *
    * `SnapshotOrdinal.MinValue` before the first finalize-sink write — `produceInner` then OMITs (defers) until history exists, which is
    * exactly the fail-closed contract. `numShards = 1` never builds checkpoints, so this is dead there (regression bar preserved).
    */
  def latestRetainedExecutionBaseCandidateOrdinal[F[_]: Async](signedBytesStore: MptStateStorage[F]): F[SnapshotOrdinal] =
    signedBytesStore.findLatestOrdinal.map(_.getOrElse(SnapshotOrdinal.MinValue))

  /** Capture the exact execution-base identity, complete currency state, every rooted state-channel tip, and all rooted global balances
    * from the same retained reader view. A missing reader yields `None`; strict index/entry decoding failures remain effect failures, so
    * neither replay nor a validity signature can follow malformed state.
    */
  def pinnedExecutionBase[F[_]: Async: Hasher](
    executionBaseF: F[GlobalSnapshotStateRef],
    priorReaderAt: GlobalSnapshotStateRef => F[Option[GlobalStateReader[F]]]
  ): F[Option[ShardCheckpointProducer.PinnedExecutionBase]] = {
    import GlobalStateReaderOps._

    executionBaseF.flatMap { stateRef =>
      priorReaderAt(stateRef).flatMap(
        _.traverse(reader =>
          (
            reader.materializeLastCurrencySnapshots,
            reader.materializeLastStateChannelSnapshotHashes,
            SpendTransactionBalanceManager.make(reader).materializeAllBalancesFromMpt
          ).tupled.map {
            case (priorCurrencySnapshots, perMgTips, balances) =>
              ShardCheckpointProducer.PinnedExecutionBase(stateRef, perMgTips, priorCurrencySnapshots, balances)
          }
        )
      )
    }
  }

  private def unavailableBatch(
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): SortedMap[Address, Option[Hash]] =
    windows.keysIterator.map(_ -> Option.empty[Hash]).to(SortedMap)

  /** Replay one complete checkpoint batch against one immutable execution base. The processor's SortedMap fold is the consensus order for
    * shared fee payers, so every later metagraph observes the absolute balance updates accepted for earlier metagraphs. A generic
    * incomplete result is conservatively unavailable: legacy replay uses the same prefix shape for deterministic rejection and swallowed
    * local dependency failures. It cannot become slash evidence until the processor supplies a typed deterministic rejection.
    */
  private def replayCheckpointAtResolvedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    gl0AnchorOrdinal: SnapshotOrdinal,
    executionBase: ShardCheckpointProducer.PinnedExecutionBase
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[InvalidStateProofBatchReplay] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring.reExecResolved")
    val basePairsConsistent = windows.keysIterator.forall { mg =>
      executionBase.priorCurrencySnapshots.contains(mg) === executionBase.perMgTips.contains(mg)
    }
    val expectedParentHashes = SortedMap.from(windows.keysIterator.map { mg =>
      mg -> executionBase.perMgTips.getOrElse(mg, Hash.empty)
    })

    if (!basePairsConsistent)
      logger
        .warn("[execution-base-pin] checkpoint has a one-sided currency/tip prior — OMIT complete batch")
        .as[InvalidStateProofBatchReplay](InvalidStateProofBatchReplay.Unavailable)
    else
      Ref
        .of[F, Boolean](false)
        .flatMap { missingHistoricalSnapshot =>
          val trackedGlobalSnapshotLookup = (ordinal: SnapshotOrdinal) =>
            getGlobalSnapshotByOrdinal(ordinal).flatTap(snapshot => missingHistoricalSnapshot.set(true).whenA(snapshot.isEmpty))

          processor
            .processCurrencySnapshotsWithCompleteConsumption(
              gl0AnchorOrdinal,
              executionBase.balances,
              executionBase.priorCurrencySnapshots,
              expectedParentHashes,
              windows,
              trackedGlobalSnapshotLookup
            )
            .flatMap(replay => missingHistoricalSnapshot.get.tupleLeft(replay))
            .flatMap {
              case (_, true) =>
                logger
                  .warn("[execution-base-pin] checkpoint replay could not resolve referenced GL0 history — OMIT complete batch")
                  .as[InvalidStateProofBatchReplay](InvalidStateProofBatchReplay.Unavailable)
              case (replay, false) =>
                val complete = replay.allInputsCompletelyConsumed && replay.completeResults.keySet === windows.keySet

                if (!complete)
                  logger
                    .warn("[execution-base-pin] checkpoint batch incomplete without typed deterministic cause — OMIT complete batch")
                    .as[InvalidStateProofBatchReplay](InvalidStateProofBatchReplay.Unavailable)
                else
                  windows.toList.traverse {
                    case (mg, _) =>
                      replay.completeResult(mg).flatMap { case (pairs, _) => pairs.toList.flatMap(_._2).lastOption } match {
                        case None => (mg -> Option.empty[Hash]).pure[F]
                        case Some(state) =>
                          GlobalStateConverter.currencySnapshotMgRoot[F](SortedMap(mg -> state)).map(root => mg -> root.some)
                      }
                  }.map[InvalidStateProofBatchReplay] { derived =>
                    NonEmptyList.fromList(derived).flatMap(_.traverse { case (mg, root) => root.map(mg -> _) }) match {
                      case Some(roots) => InvalidStateProofBatchReplay.Reproduced(SortedMap.from(roots.toList))
                      case None        => InvalidStateProofBatchReplay.Unavailable
                    }
                  }
            }
        }
        .handleErrorWith(error =>
          logger
            .warn(error)("[execution-base-pin] resolved-base batch replay failed — OMIT complete batch")
            .as[InvalidStateProofBatchReplay](InvalidStateProofBatchReplay.Unavailable)
        )
  }

  private def reExecAllAtResolvedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    windows: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    gl0AnchorOrdinal: SnapshotOrdinal,
    executionBase: ShardCheckpointProducer.PinnedExecutionBase
  )(
    implicit stateProofSelector: StateProofSelector
  ): F[SortedMap[Address, Option[Hash]]] =
    replayCheckpointAtResolvedBase(processor, getGlobalSnapshotByOrdinal, windows, gl0AnchorOrdinal, executionBase).map {
      case InvalidStateProofBatchReplay.Reproduced(roots) => roots.view.mapValues(_.some).to(SortedMap)
      case _                                              => unavailableBatch(windows)
    }

  /** Producer batch replay over the exact immutable base object that anchored the checkpoint windows. No reader is re-resolved here. */
  def reExecDerivationsAtResolvedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): (
    SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    SnapshotOrdinal,
    ShardCheckpointProducer.PinnedExecutionBase
  ) => F[SortedMap[Address, Option[Hash]]] =
    (windows, gl0AnchorOrdinal, executionBase) =>
      reExecAllAtResolvedBase(processor, getGlobalSnapshotByOrdinal, windows, gl0AnchorOrdinal, executionBase)

  /** Receiver batch replay resolves and strictly materializes one retained base for the complete checkpoint, then reuses it across MGs. */
  def reExecDerivationsAtPinnedBaseBatch[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    priorReaderAt: GlobalSnapshotStateRef => F[Option[GlobalStateReader[F]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): (
    SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    SnapshotOrdinal,
    GlobalSnapshotStateRef
  ) => F[SortedMap[Address, Option[Hash]]] = {
    val replayResolved = reExecDerivationsAtResolvedBase(processor, getGlobalSnapshotByOrdinal)

    (windows, gl0AnchorOrdinal, executionBase) =>
      pinnedExecutionBase(executionBase.pure[F], priorReaderAt).flatMap {
        case Some(executionBase) => replayResolved(windows, gl0AnchorOrdinal, executionBase)
        case None                => windows.keysIterator.map(_ -> Option.empty[Hash]).to(SortedMap).pure[F]
      }
        .handleError(_ => windows.keysIterator.map(_ -> Option.empty[Hash]).to(SortedMap))
  }

  /** Portable invalid-state-proof replay. It resolves one pinned base and executes the complete checkpoint batch exactly once. The disputed
    * metagraph remains an evidence selector only; it never narrows execution or resets shared balances/dependencies.
    */
  def reExecCheckpointAtPinnedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    priorReaderAt: GlobalSnapshotStateRef => F[Option[GlobalStateReader[F]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): (
    SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    SnapshotOrdinal,
    GlobalSnapshotStateRef
  ) => F[InvalidStateProofBatchReplay] =
    (windows, gl0AnchorOrdinal, executionBase) =>
      pinnedExecutionBase(executionBase.pure[F], priorReaderAt).flatMap {
        case Some(executionBase) =>
          replayCheckpointAtResolvedBase(processor, getGlobalSnapshotByOrdinal, windows, gl0AnchorOrdinal, executionBase)
        case None => Async[F].pure[InvalidStateProofBatchReplay](InvalidStateProofBatchReplay.Unavailable)
      }
        .handleError(_ => InvalidStateProofBatchReplay.Unavailable)

  /** Compatibility scalar for focused replay/fraud callers. Production checkpoint paths use [[reExecDerivationsAtPinnedBaseBatch]] so
    * shared fee-payer balances are serialized once across the complete checkpoint. The scalar delegates to that exact batch implementation
    * with one metagraph; it has no separate transition semantics.
    */
  def reExecDerivationAtPinnedBase[F[_]: Async: Parallel: Hasher: JsonSerializer](
    processor: GlobalSnapshotStateChannelEventsProcessor[F],
    priorReaderAt: GlobalSnapshotStateRef => F[Option[GlobalStateReader[F]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit stateProofSelector: StateProofSelector
  ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => F[Option[Hash]] = {
    val replayBatch = reExecDerivationsAtPinnedBaseBatch(processor, priorReaderAt, getGlobalSnapshotByOrdinal)

    (mg, binaries, gl0AnchorOrdinal, executionBase) =>
      replayBatch(SortedMap(mg -> binaries), gl0AnchorOrdinal, executionBase).map(_.getOrElse(mg, None))
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
    *   the pinned-base re-exec derivation closure. `Some(...)` recomputes the canonical per-MG root and returns a typed local rejection on
    *   a byte-mismatch; that result cannot slash without separately carried and revalidated fraud evidence. `None` (the default) ⇒
    *   [[noReExecDerivation]] (fail-closed: non-empty checkpoints are rejected on the `Hash.empty` sentinel, never falsely admitted).
    *   Modelled as `Option` rather than a defaulted closure because Scala can't resolve `Async[F]` for `noReExecDerivation[F]` at the
    *   default-arg site (the context bound is on the method, not on the default expression) — the same constraint the GSAM
    *   `localEventsPublisher` param hits.
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
      (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => F[Hash]
    ] = None,
    reExecuteDerivations: Option[
      (
        SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        SnapshotOrdinal,
        GlobalSnapshotStateRef
      ) => F[SortedMap[Address, Hash]]
    ] = None,
    slashCooldownReader: Option[SlashCooldownReader[F]] = None
  ): F[Option[AcceptanceDeps[F]]] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointWiring")
    val reExec: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => F[Hash] =
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
          reExecuteDerivation = reExec,
          reExecuteDerivations = reExecuteDerivations
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
