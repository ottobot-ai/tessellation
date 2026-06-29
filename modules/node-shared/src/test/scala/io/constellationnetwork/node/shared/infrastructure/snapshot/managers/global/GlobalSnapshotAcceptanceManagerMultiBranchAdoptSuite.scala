package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.env.AppEnvironment
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.domain.block.processing._
import io.constellationnetwork.node.shared.domain.delegatedStake.{UpdateDelegatedStakeAcceptanceManager, UpdateDelegatedStakeValidator}
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.node.{UpdateNodeParametersAcceptanceManager, UpdateNodeParametersAcceptanceResult}
import io.constellationnetwork.node.shared.domain.nodeCollateral.{
  UpdateNodeCollateralAcceptanceManager,
  UpdateNodeCollateralAcceptanceResult
}
import io.constellationnetwork.node.shared.domain.priceOracle.PricingUpdateValidator.PricingUpdateValidationErrorOr
import io.constellationnetwork.node.shared.domain.priceOracle.{PriceStateUpdater, PricingUpdateValidator}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.SpendActionValidationErrorOr
import io.constellationnetwork.node.shared.domain.swap.block._
import io.constellationnetwork.node.shared.domain.tokenlock.block._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.DelegatedRewardsResult
import io.constellationnetwork.node.shared.logger.Slf4jLoggerBundle
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{PricingUpdate, SpendAction}
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nodeCollateral.UpdateNodeCollateral
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.swap.{AllowSpend, AllowSpendBlock}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import weaver.MutableIOSuite

/** S0 of the sharded-currency-mirror endgame (docs/nakamoto/SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md).
  *
  * '''What this pins (VERSION-MODEL §4).''' The three references to "the prior S(N)" — producer window-anchor, producer diff-prior, and the
  * gl0 follower APPLY-prior — must all read the SAME finalized base. On HEAD the follower apply-prior reads the BRANCH-aware reader
  * (`GlobalSnapshotAcceptanceManager.accept` builds `priorLastCurrencySnapshots` through the branch-aware `mpt` reader, and
  * `deriveAdoptedCurrencyState`'s `priorInfoOf` reads from that map), while the committee cuts its diff over the FINALIZED BASE. When the
  * branch carries an intervening committed incremental ahead of the base (`branch != base`), the HEAD follower reconstructed
  * `reconstructInfoFromDiff(branchPrior, diff_over_base)` whose per-MG root did NOT equal the committee-attested root, and the MG was
  * '''DROPPED''' (`[ACCEPTANCE/ADOPT-VERIFY] ... MISMATCH ... DROPPING`). That asymmetry — drop at `branch != base`, adopt at `branch ==
  * base`, '''with the identical checkpoint''' — IS the run-24→27 disease.
  *
  * '''S1 STATE (this revision).''' The follower now base-anchors BOTH halves for sharded MGs: (i) its apply-prior info reads the finalized
  * base (`GlobalSnapshotAcceptanceManager.accept`'s `shardedInfoMode`), and (ii) its per-MG currency-WRITE removal-prior reads that same
  * base (`AcceptanceMptStateChanges.applyStateChanges(currencyInfoRemovalPrior = Some(baseReader))`) so the `Mg*` removal set matches the
  * accumulator-delta verify-replay (which carries no `Mg*` info removals) and the #107 writer self-check still holds. With both halves on
  * base, a `branch != base` MG now ADOPTS. Tests (A) and (C) are the GREEN guards (both assert adopt); (A) additionally keeps the
  * prior-level §4-mechanism asserts (recompute-over-base === attested, recompute-over-branch != attested) proving the follower reads base
  * despite the branch diverging. (B) is the unchanged branch==base control. These pass at pipelineDepth=1; deeper windows are S2's job.
  *
  * '''Two make-or-break construction constraints''' (each verified against HEAD; miss either ⇒ a false-green that proves nothing):
  *
  *   1. '''The MG MUST be in the Right (incremental) arm WITH a carried diff.''' The Left(genesis)/None arms return `emptyInfo` regardless
  *      of branch depth, so `branchPrior == basePrior == emptyInfo`, recompute === attested, NO drop — GREEN-on-HEAD (the trap). Here the
  *      finalized base already holds the MG's `CurrencySnapshotInfo` (the unrolled `Mg*` entries + the `LastIncrementalCurrencySnapshots`
  *      key + the `LastCurrencySnapshots` active-address index), so `priorLastCurrencySnapshots(mg)` resolves to a `Right((inc, info))`,
  *      and the checkpoint carries a NON-EMPTY diff over that base prior. (See `seedBaseAndRoundTrip` + `writeRightArm` +
  *      `mkRightArmCheckpoint`.)
  *
  * 2. '''The harness drives the PRODUCTION MultiBranch overlay with a NON-passthrough child `BranchId` threaded as `accept(parentTip =
  * ...)`.''' `GlobalSnapshotAcceptanceManagerShardingSuite` builds `MptOverlay.passthrough` and passes `parentTip = BranchId.passthrough`
  * (== base == `BranchId(Hash.empty)`) — structurally incapable of `branch > base`. Here the overlay is
  * `MptOverlay.make(OverlayMode.MultiBranch(...))`; an INTERVENING per-MG incremental+info is written into a checked-out child branch
  * (`childTip`) so its branch reader returns a DIFFERENT prior than the base `MptStore`; that `childTip` is threaded as `parentTip`. (See
  * `mkOverlayWithBranch`.)
  *
  * '''Producer-truth.''' The carried diff and the committee-attested per-MG root are computed with the EXACT production functions the
  * follower recomputes against (`ChangeSet.currencyInfoChangeSet`, `ChangeSet.reconstructInfoFromDiff`, the PIN-1
  * `GlobalStateConverter.currencySnapshotMgRoot` — the component-addressable per-MG sub-trie root), so producer and verifier agree by
  * construction and the only thing that flips drop↔adopt is which prior the follower reads (branch vs base).
  */
object GlobalSnapshotAcceptanceManagerMultiBranchAdoptSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp, j)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  // The state-proof selector must be active (low ordinal) so currencySnapshotFieldRoots / the GSI state-proof include the unrolled fields.
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  private val acceptOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))

  // ============================================================================
  // Address / hash / value fixtures
  // ============================================================================

  private def mkAddress(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  private val genesisHash: Hash = Hash("0" * 64)
  private val epochZero: EtaPeriod = EtaPeriod(0L)

  /** Sentinel `Signed[StateChannelSnapshotBinary]` — its `lastSnapshotHash = Hash.empty` so the GSAM CHAIN-HOLE GUARD finds it at idx 0
    * against the default `Hash.empty` SC tip (an unseeded MG) and adopts the window. The captor/deriving processor never inspects content.
    */
  private def mkSignedBinary(content: Array[Byte]): Signed[StateChannelSnapshotBinary] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(lastSnapshotHash = Hash.empty, content = content, fee = SnapshotFee(NonNegLong.unsafeFrom(0L))),
      NonEmptySet.of(sentinelProof)
    )
  }

  /** A `Signed[CurrencyIncrementalSnapshot]` built with a sentinel proof (no keypair needed — the GSAM only round-trips it through the
    * `LastIncrementalCurrencySnapshots` codec and re-hashes its bytes for the `incrementalRoot`; signature validity is irrelevant here).
    */
  private def mkSignedIncremental(snapOrdinal: Long): Signed[CurrencyIncrementalSnapshot] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("33" * 64)), Signature(Hex("44" * 70)))
    val snap = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(snapOrdinal),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      epochProgress = EpochProgress.MinValue,
      dataApplication = None,
      messages = None,
      globalSnapshotSyncs = None,
      feeTransactions = None,
      artifacts = None,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      globalSyncView = None
    )
    Signed(snap, NonEmptySet.of(sentinelProof))
  }

  /** A `CurrencySnapshotInfo` carrying exactly one balance entry — used as both base prior and branch prior (same key, different value), so
    * the branch's `MgBalances` upsert cleanly OVERRIDES the base entry and `reconstructCurrencyInfoFrom` yields a different prior at the
    * branch than at the base. The other sub-maps stay empty (mirrors the `reconstructCurrencyInfoFrom` always-`.some` shape).
    */
  private def infoWithBalance(holder: Address, amount: Long): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap(holder -> Balance(NonNegLong.unsafeFrom(amount))),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  // ============================================================================
  // Checkpoint construction
  // ============================================================================

  private def mkCommitteeSig: CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = io.constellationnetwork.schema.peer.PeerId(Hex("ab" * 64)),
      vrfProof = Hex(""),
      ed25519Sig = Hex(""),
      kesProductSig = Hex(""),
      kesTreeStep = 0
    )

  /** Build a checkpoint whose `derivedStateDelta` carries, for `mg`: the head binary (`includedSnapshots`), the committee byte-diff
    * (`perMetagraphStateDiff`), and the committee-attested per-MG root (`perMetagraphMptRoots`). The diff + root are PRODUCER-TRUTH:
    * computed over `basePriorRT` (the round-tripped base prior) so that `reconstructInfoFromDiff(basePriorRT, diff) === nextInfo` and the
    * attested root === `currencySnapshotMgRoot(mg -> Right((inc, nextInfo)))`.
    */
  private def mkRightArmCheckpoint(
    mg: Address,
    binary: Signed[StateChannelSnapshotBinary],
    diff: ShardCurrencyStateDiff,
    attestedRoot: Hash
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = genesisHash,
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(2L)),
      slot = io.constellationnetwork.schema.nakamoto.slot.Slot.unsafeApply(2L),
      derivedStateDelta = ShardDerivedStateDelta(
        perMetagraphMptRoots = SortedMap(mg -> attestedRoot),
        perMetagraphStateDiff = SortedMap(mg -> diff),
        includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary)),
        tokenLockBalancesDelta = SortedMap.empty,
        perMetagraphArtifacts = SortedMap.empty,
        perMetagraphSyncDataDelta = SortedMap.empty
      ),
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(mkCommitteeSig),
      epoch = epochZero
    )

  // ============================================================================
  // Producer-truth diff + attested root (the EXACT functions the follower recomputes against)
  // ============================================================================

  /** Compute the committee byte-diff over `basePrior` and the PIN-1 attested per-MG root over `next`, using the production functions. By
    * the `reconstructInfoFromDiff` round-trip invariant, every gl0 follower whose APPLY-prior equals `basePrior` recomputes `next` and
    * matches the root (ADOPT); a follower whose prior differs (the branch) reconstructs a different info and mismatches (DROP).
    */
  private def producerTruth(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    basePrior: CurrencySnapshotInfo,
    next: CurrencySnapshotInfo
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[(ShardCurrencyStateDiff, Hash)] =
    for {
      changeSet <- ChangeSet.currencyInfoChangeSet[IO](mg, basePrior, next)
      wireDiff = ChangeSet.toWire(changeSet)
      nextState = Right((inc, next)): CurrencySnapshotWithState
      // PIN-1: the attested per-MG root is the COMPONENT-ADDRESSABLE `currencySnapshotMgRoot` (MG-sub-trie rootHash), the exact root the
      // GSAM follower recomputes in `deriveAdoptedCurrencyState`.
      attestedRoot <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mg -> nextState))
    } yield (wireDiff, attestedRoot)

  /** Recompute the per-MG PIN-1 root EXACTLY as the GSAM follower does (`reconstructInfoFromDiff(prior, diff)` → `currencySnapshotMgRoot` —
    * the component-addressable per-MG sub-trie root), for an arbitrary `prior`. Lets a test assert, at the prior level, that the recompute
    * \=== attested over the BASE prior but != attested over the BRANCH prior — i.e. the drop is exactly the §4 mechanism.
    */
  private def followerRecomputeRoot(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    prior: CurrencySnapshotInfo,
    wireDiff: ShardCurrencyStateDiff
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Hash] =
    for {
      nextInfo <- ChangeSet.reconstructInfoFromDiff[IO](mg, prior, ChangeSet.fromWire(wireDiff))
      nextState = Right((inc, nextInfo)): CurrencySnapshotWithState
      // PIN-1: recompute the component-addressable per-MG root EXACTLY as the GSAM follower does.
      recomputed <- GlobalStateConverter.currencySnapshotMgRoot[IO](SortedMap(mg -> nextState))
    } yield recomputed

  // ============================================================================
  // Seeding the finalized base / a branch with Right-arm per-MG state
  // ============================================================================

  /** Write the full Right-arm prior for `mg` (`info`) into the supplied `CurrencyInfoMpt` writer via the SAME production writer gl0 uses
    * (`GlobalStateConverter.writeCurrencyInfo`), PLUS the `LastIncrementalCurrencySnapshots` fieldId-5 key. This is exactly the on-disk
    * shape `accept()`'s `priorLastCurrencySnapshots` build reads back: a present fieldId-5 incremental ⇒ the `Right` arm, with the info
    * reconstructed from the unrolled `Mg*` prefix.
    */
  private def writeRightArm(
    writer: GlobalStateConverter.CurrencyInfoMpt[IO],
    insertInc: Map[GlobalStateKey, Signed[CurrencyIncrementalSnapshot]] => IO[Unit],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    info: CurrencySnapshotInfo,
    priorInfoForRemovals: CurrencySnapshotInfo
  )(implicit h: Hasher[IO]): IO[Unit] =
    for {
      _ <- GlobalStateConverter.writeCurrencyInfo[IO](mg, info, priorInfoForRemovals, writer)
      _ <- insertInc(Map(GlobalStateKey.metagraph(mg, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> inc))
    } yield ()

  /** Build a fresh MptStore. */
  private def freshStore(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  /** Seed the BASE store directly with: the Right-arm info (`basePrior`) for `mg`, the fieldId-5 incremental, and the
    * `LastCurrencySnapshots` active-address index marking `mg` (so `accept()`'s `priorLastCurrencySnapshots` keyset includes it). Returns
    * the round-tripped base prior the GSAM will actually read back, so the producer diff is cut over the BYTE-IDENTICAL prior.
    */
  private def seedBaseAndRoundTrip(
    store: MptStore[IO, GlobalStateKey],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    basePrior: CurrencySnapshotInfo
  )(implicit h: Hasher[IO]): IO[CurrencySnapshotInfo] = {
    val emptyInfo = infoWithBalance(mkAddress("__never__"), 0L).copy(balances = SortedMap.empty)
    val storeWriter = GlobalStateConverter.CurrencyInfoMpt.fromMptStore(store)
    for {
      _ <- writeRightArm(storeWriter, store.insert[Signed[CurrencyIncrementalSnapshot]](_), mg, inc, basePrior, emptyInfo)
      indexKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.LastCurrencySnapshots)
      _ <- store.insert[SortedSet[Address]](indexKey, SortedSet(mg))
      reader = GlobalStateConverter.CurrencyInfoMpt.fromMptStore(store)
      roundTripped <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, reader)
    } yield roundTripped
  }

  /** Build the production MultiBranch overlay over the (already base-seeded) store. If `branchPrior` is supplied, an INTERVENING per-MG
    * incremental+info is committed into a child branch so the branch reader returns `branchPrior` for `mg` (overriding the base
    * `basePrior`); the returned `BranchId` is that child tip (threaded as `parentTip`). If `None`, the returned `BranchId` is `base`
    * (passthrough-equivalent) — the CONTROL where `branch == base`.
    */
  private def mkOverlayWithBranch(
    store: MptStore[IO, GlobalStateKey],
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    basePrior: CurrencySnapshotInfo,
    branchPrior: Option[CurrencySnapshotInfo]
  )(implicit h: Hasher[IO]): IO[(MptOverlay[IO, GlobalStateKey], BranchId)] =
    for {
      pcTree <- io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      parentTip <- branchPrior match {
        case None => BranchId.base.pure[IO]
        case Some(bp) =>
          val childTip = BranchId(Hash("c" * 64))
          for {
            handle <- overlay.checkout(BranchId.base)
            acceptanceMpt = AcceptanceMpt.fromOverlay[IO](overlay, BranchId.base, handle)
            branchWriter = CurrencyInfoMptAdapters.mptFor(acceptanceMpt)
            // Write the intervening committed incremental+info into the child branch: the branch prior OVERRIDES the base prior for `mg`.
            // The `priorInfoForRemovals = basePrior` issues the removal keys needed so the branch reflects `bp` (not a union with base).
            _ <- writeRightArm(branchWriter, acceptanceMpt.insert[Signed[CurrencyIncrementalSnapshot]](_), mg, inc, bp, basePrior)
            _ <- overlay.commit(handle, childTip, acceptOrdinal)
          } yield childTip
      }
    } yield (overlay, parentTip)

  // ============================================================================
  // The deriving processor — puts the MG in the RIGHT arm of calculatedCurrencyState
  // ============================================================================

  /** A processor whose `processCurrencySnapshots` emits, per adopted MG, a single `(binary, Some(Right((inc, seedInfo))))` pair. The GSAM
    * adopt path then sees `calculatedCurrencyState(mg) = Right((inc, seedInfo))` (so `derivedState.isLeft == false` and the
    * apply-and-verify runs with `lastIncremental = inc`), and OVERRIDES `seedInfo` with the verified
    * `reconstructInfoFromDiff(priorInfoOf(mg), diff)`. The base-path `process` is never load-bearing here (sharded MGs are filtered out by
    * CHANGE-3; we pass no raw scEvents).
    */
  private def derivingProcessor(
    inc: Signed[CurrencyIncrementalSnapshot],
    seedInfo: CurrencySnapshotInfo
  ): GlobalSnapshotStateChannelEventsProcessor[IO] =
    new GlobalSnapshotStateChannelEventsProcessor[IO] {
      override def process(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        events: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[StateChannelAcceptanceResult] =
        StateChannelAcceptanceResult(
          accepted = SortedMap.empty[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
          calculatedCurrencyState = SortedMap.empty[Address, CurrencySnapshotWithState],
          returned = Set.empty[StateChannelOutput],
          balanceUpdate = SortedMap.empty[Address, Balance],
          incomingCurrencySnapshotsWithState = SortedMap.empty[Address, List[CurrencySnapshotWithState]]
        ).pure[IO]

      override def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]],
        adoptionMode: GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode
      )(implicit hasher: Hasher[IO]): IO[SortedMap[Address, MetagraphAcceptanceResult]] =
        events.map {
          case (addr, bins) =>
            val pairs: NonEmptyList[BinaryCurrencyPair] =
              bins.map(b => (b, (Right((inc, seedInfo)): CurrencySnapshotWithState).some))
            addr -> ((pairs, SortedMap.empty[Address, Balance]))
        }.pure[IO]

      override def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, CurrencySnapshotWithState],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult = {
        val lastStatePerAddress: SortedMap[Address, CurrencySnapshotWithState] =
          processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2).lastOption }.collect { case (key, Some(s)) => key -> s }
        val incoming: SortedMap[Address, List[CurrencySnapshotWithState]] =
          processed.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2) }.filterNot { case (_, list) => list.isEmpty }
        StateChannelAcceptanceResult(
          accepted = processed.map { case (k, (v, _)) => k -> v.map(_._1) },
          calculatedCurrencyState = priorLastCurrencySnapshots.concat(lastStatePerAddress),
          returned = returned,
          balanceUpdate = SortedMap.empty[Address, Balance],
          incomingCurrencySnapshotsWithState = incoming
        )
      }

      override def deriveMetagraphRoot(
        metagraphAddress: Address,
        binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
        snapshotOrdinal: SnapshotOrdinal,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => IO[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[IO]): IO[Hash] = Hash.empty.pure[IO]
    }

  // ============================================================================
  // Stub checkpoint manager — always Accepted (verifyEmbedded is the GSAM adopt-decision call)
  // ============================================================================

  private final case class StubAcceptanceManager(callsRef: Ref[IO, List[ShardCheckpoint]]) extends ShardCheckpointGl0AcceptanceManager[IO] {
    override def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(ShardCheckpointAcceptResult.Accepted)
    override def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] =
      callsRef.update(_ :+ checkpoint).as(ShardCheckpointAcceptResult.Accepted)
    override def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit
    override def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)
    override def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)
    override def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] =
      IO.pure(List.empty[WatchtowerMismatch])
  }

  // ============================================================================
  // GSAM-under-test wiring (over an explicit overlay + the deriving processor)
  // ============================================================================

  private def mkShardingConfig(numShards: Int): ShardingConfig =
    ShardingConfig(
      numShards = numShards,
      finality = ShardFinalityConfig(k1Shard = 8L),
      checkpoint = ShardCheckpointConfig(tAliveMs = 10000L, tBurst = 100, binaryBufferCap = 4096),
      observability = ShardObservabilityConfig(tPartitionHardMs = 600000L),
      slashing = ShardSlashingConfig(maxMissedPctPerEpoch = 33, minDenominatorPerEpoch = 5L)
    )

  private val noopRewardsFn: io.constellationnetwork.node.shared.infrastructure.snapshot.RewardsInput => IO[DelegatedRewardsResult] =
    _ =>
      DelegatedRewardsResult(
        SortedMap.empty,
        SortedMap.empty,
        SortedMap.empty,
        SortedSet.empty,
        SortedSet.empty,
        SortedSet.empty,
        Amount.empty
      ).pure[IO]

  private def mkManager(
    overlay: MptOverlay[IO, GlobalStateKey],
    stateChannelEventsProcessor: GlobalSnapshotStateChannelEventsProcessor[IO],
    checkpointManager: ShardCheckpointGl0AcceptanceManager[IO]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], j: JsonSerializer[IO]): IO[GlobalSnapshotAcceptanceManager[IO]] = {
    val mockBlockAcceptanceManager: BlockAcceptanceManager[IO] = new BlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[Block]],
        context: BlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldValidateCollateral: Boolean
      )(implicit hasher: Hasher[IO]): IO[BlockAcceptanceResult] =
        BlockAcceptanceResult(
          accepted = List.empty,
          notAccepted = List.empty,
          contextUpdate = BlockAcceptanceContextUpdate(SortedMap.empty, SortedMap.empty, Map.empty)
        ).pure[IO]
      override def acceptBlock(
        block: Signed[Block],
        context: BlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldValidateCollateral: Boolean
      )(implicit hasher: Hasher[IO]): IO[Either[BlockNotAcceptedReason, (BlockAcceptanceContextUpdate, UsageCount)]] =
        (BlockAcceptanceContextUpdate(SortedMap.empty, SortedMap.empty, Map.empty), initUsageCount).asRight.pure[IO]
    }

    val mockAllowSpendBlockAcceptanceManager: AllowSpendBlockAcceptanceManager[IO] = new AllowSpendBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[AllowSpendBlock]],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[AllowSpendBlockAcceptanceResult] =
        AllowSpendBlockAcceptanceResult(AllowSpendBlockAcceptanceContextUpdate.empty, List.empty, List.empty).pure[IO]
      override def acceptBlock(
        block: Signed[AllowSpendBlock],
        context: AllowSpendBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[AllowSpendBlockNotAcceptedReason, AllowSpendBlockAcceptanceContextUpdate]] =
        AllowSpendBlockAcceptanceContextUpdate.empty.asRight.pure[IO]
    }

    val mockTokenLockBlockAcceptanceManager: TokenLockBlockAcceptanceManager[IO] = new TokenLockBlockAcceptanceManager[IO] {
      override def acceptBlocksIteratively(
        blocks: List[Signed[TokenLockBlock]],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[TokenLockBlockAcceptanceResult] =
        TokenLockBlockAcceptanceResult(TokenLockBlockAcceptanceContextUpdate.empty, blocks, List.empty).pure[IO]
      override def acceptBlock(
        block: Signed[TokenLockBlock],
        context: TokenLockBlockAcceptanceContext[IO],
        snapshotOrdinal: SnapshotOrdinal,
        shouldPerformMetagraphSpecificValidations: Boolean,
        lastGlobalSnapshotEpochProgress: Option[EpochProgress]
      )(implicit hasher: Hasher[IO]): IO[Either[TokenLockBlockNotAcceptedReason, TokenLockBlockAcceptanceContextUpdate]] =
        TokenLockBlockAcceptanceContextUpdate.empty.asRight.pure[IO]
    }

    val mockUpdateNodeParametersAcceptanceManager: UpdateNodeParametersAcceptanceManager[IO] =
      new UpdateNodeParametersAcceptanceManager[IO] {
        override def acceptUpdateNodeParameters(
          events: List[Signed[io.constellationnetwork.schema.node.UpdateNodeParameters]],
          lastSnapshotContext: GlobalSnapshotInfo
        ): IO[UpdateNodeParametersAcceptanceResult] =
          UpdateNodeParametersAcceptanceResult(List.empty, List.empty).pure[IO]
      }

    val updateDelegatedStakeValidator =
      UpdateDelegatedStakeValidator.make[IO](io.constellationnetwork.security.signature.SignedValidator.make[IO], None)
    val updateDelegatedStakeAcceptanceManager = UpdateDelegatedStakeAcceptanceManager.make[IO](updateDelegatedStakeValidator)

    val mockUpdateNodeCollateralAcceptanceManager: UpdateNodeCollateralAcceptanceManager[IO] =
      new UpdateNodeCollateralAcceptanceManager[IO] {
        override def accept(
          createEvents: List[Signed[UpdateNodeCollateral.Create]],
          withdrawEvents: List[Signed[UpdateNodeCollateral.Withdraw]],
          lastSnapshotContext: GlobalSnapshotInfo,
          epochProgress: EpochProgress,
          ordinal: SnapshotOrdinal,
          delegatedStakeAcceptanceResult: io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeAcceptanceResult
        ): IO[UpdateNodeCollateralAcceptanceResult] =
          UpdateNodeCollateralAcceptanceResult(SortedMap.empty, List.empty, SortedMap.empty, List.empty).pure[IO]
      }

    val mockSpendActionValidator: SpendActionValidator[IO] = new SpendActionValidator[IO] {
      override def validate(
        spendAction: SpendAction,
        activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        allBalances: Map[Option[Address], SortedMap[Address, Balance]],
        currencyId: Address
      ): IO[SpendActionValidationErrorOr[SpendAction]] = spendAction.validNec.pure[IO]
      override def validateReturningAcceptedAndRejected(
        spendActions: Map[Address, List[SpendAction]],
        activeAllowSpends: SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]],
        allBalances: Map[Option[Address], SortedMap[Address, Balance]]
      ): IO[(Map[Address, List[SpendAction]], Map[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]])] =
        (
          Map.empty[Address, List[SpendAction]],
          Map.empty[Address, List[(SpendAction, List[SpendActionValidator.SpendActionValidationError])]]
        ).pure[IO]
    }

    val mockPricingUpdateValidator: PricingUpdateValidator[IO] = new PricingUpdateValidator[IO] {
      override def validate(
        pricingUpdate: PricingUpdate,
        currencyId: Address,
        lastContext: GlobalSnapshotInfo,
        epochProgress: EpochProgress
      ): IO[PricingUpdateValidationErrorOr[PricingUpdate]] = pricingUpdate.validNec.pure[IO]
      override def validateReturningAcceptedAndRejected(
        pricingUpdates: Map[Address, List[PricingUpdate]],
        lastContext: GlobalSnapshotInfo,
        epochProgress: EpochProgress
      ): IO[(List[PricingUpdate], List[(PricingUpdate, List[PricingUpdateValidator.PricingUpdateValidationError])])] =
        (List.empty, List.empty).pure[IO]
    }

    val mockPriceStateUpdater: PriceStateUpdater[IO] = new PriceStateUpdater[IO] {
      override def updatePriceState(
        lastPriceState: SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord],
        acceptedPricingUpdates: List[PricingUpdate],
        epochProgress: EpochProgress
      )(implicit hasher: Hasher[IO]): IO[SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord]] =
        SortedMap.empty[priceOracle.TokenPair, priceOracle.PriceRecord].pure[IO]
      override def materializePriceStateFromMpt(
        implicit hasher: Hasher[IO]
      ): IO[SortedMap[priceOracle.TokenPair, priceOracle.PriceRecord]] =
        SortedMap.empty[priceOracle.TokenPair, priceOracle.PriceRecord].pure[IO]
    }

    implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    Slf4jLoggerBundle.makeUnsafe[IO].flatMap { loggerBundle =>
      GlobalSnapshotAcceptanceManager
        .make[IO](
          FieldsAddedOrdinals(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty, Map.empty),
          MetagraphsSyncConfig(PosInt(100)),
          AppEnvironment.Dev,
          blockAcceptanceManager = mockBlockAcceptanceManager,
          allowSpendBlockAcceptanceManager = mockAllowSpendBlockAcceptanceManager,
          tokenLockBlockAcceptanceManager = mockTokenLockBlockAcceptanceManager,
          stateChannelEventsProcessor = stateChannelEventsProcessor,
          updateNodeParametersAcceptanceManager = mockUpdateNodeParametersAcceptanceManager,
          updateDelegatedStakeAcceptanceManager = updateDelegatedStakeAcceptanceManager,
          updateNodeCollateralAcceptanceManager = mockUpdateNodeCollateralAcceptanceManager,
          spendActionValidator = mockSpendActionValidator,
          pricingUpdateValidator = mockPricingUpdateValidator,
          priceStateUpdater = mockPriceStateUpdater,
          collateral = Amount.empty,
          withdrawalTimeLimit = EpochProgress(4L),
          loggerBundle = loggerBundle,
          overlay = overlay,
          shardingConfig = Some(mkShardingConfig(4)),
          shardCheckpointAcceptanceManager = Some(checkpointManager),
          shardAssignment = Some(ShardAssignment.make[IO](numShards = 4))
        )
    }
  }

  /** A `GlobalSnapshotInfo` whose `lastCurrencySnapshots` already contains `mg -> Right((inc, gsiInfo))`. The GSAM unions this with the
    * MPT-derived prior (the #113 per-address fallback) — but the MPT (branch/base) is the authoritative source for our seeded MG, so the
    * branch-vs-base divergence is what `priorInfoOf` reads. We still populate it so the keyset/fallback path is exercised consistently.
    */
  private def gsiWith(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    gsiInfo: CurrencySnapshotInfo
  ): GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      lastStateChannelSnapshotHashes = SortedMap.empty,
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.empty,
      lastCurrencySnapshots = SortedMap(mg -> (Right((inc, gsiInfo)): CurrencySnapshotWithState)),
      lastCurrencySnapshotsProofs = SortedMap.empty,
      activeAllowSpends = None,
      activeTokenLocks = None,
      tokenLockBalances = None,
      lastAllowSpendRefs = None,
      lastTokenLockRefs = None,
      updateNodeParameters = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty),
      historicalStakeSnapshots = SortedMap.empty
    )

  /** Run `accept()` and return the committed GSI (tuple element `_9`) so tests can inspect whether `mg`'s currency advanced. */
  private def runAccept(
    mgr: GlobalSnapshotAcceptanceManager[IO],
    checkpoint: ShardCheckpoint,
    parentTip: BranchId,
    lastSnapshotInfo: GlobalSnapshotInfo
  ): IO[GlobalSnapshotInfo] =
    mgr
      .accept(
        ordinal = acceptOrdinal,
        epochProgress = EpochProgress(10L),
        previousEpochProgress = EpochProgress.MinValue,
        blocksForAcceptance = List.empty,
        allowSpendBlocksForAcceptance = List.empty,
        tokenLockBlocksForAcceptance = List.empty,
        scEvents = List.empty,
        unpEvents = List.empty,
        cdsEvents = List.empty,
        wdsEvents = List.empty,
        cncEvents = List.empty,
        wncEvents = List.empty,
        lastSnapshotContext = lastSnapshotInfo,
        lastActiveTips = SortedSet.empty,
        lastDeprecatedTips = SortedSet.empty,
        calculateRewardsFn = noopRewardsFn,
        validationType = StateChannelValidationType.Full,
        getGlobalSnapshotByOrdinal = _ => None.pure[IO],
        parentTip = parentTip,
        shardCheckpoints = SortedMap(checkpoint.shardId -> checkpoint)
      )
      .map(_._9)

  /** Did `mg`'s committed currency advance to `nextInfo`? True iff the GSI's `lastCurrencySnapshots(mg)` is a `Right` whose info balances
    * equal `nextInfo.balances` (the discriminating field — base/branch priors and `nextInfo` all carry a distinct single balance).
    */
  private def advancedTo(gsi: GlobalSnapshotInfo, mg: Address, nextInfo: CurrencySnapshotInfo): Boolean =
    gsi.lastCurrencySnapshots.get(mg) match {
      case Some(Right((_, info))) => info.balances == nextInfo.balances
      case _                      => false
    }

  // ============================================================================
  // Shared scenario builder: one MG past first incremental, base prior + a target next.
  // ============================================================================

  private val mg: Address = mkAddress("s0-spine-mg")
  private val holder: Address = mkAddress("s0-spine-holder")
  private val holderBranchExtra: Address = mkAddress("s0-spine-holder-branch-extra")
  private val inc: Signed[CurrencyIncrementalSnapshot] = mkSignedIncremental(snapOrdinal = 2L)

  // S(N) the committee diffs OVER (the finalized base): one holder.
  private val basePriorRaw: CurrencySnapshotInfo = infoWithBalance(holder, 100L)

  // S(N+1) the committee re-exec produced: that SAME holder, advanced. The minimal diff `changeSet(basePrior, next)` therefore upserts ONLY
  // `holder`'s balance — it carries NO entry for `holderBranchExtra`. That is the load-bearing asymmetry: the diff overwrites the prior's
  // `holder` value regardless of prior (so a divergence in `holder`'s value alone would be MASKED by the upsert — the original false-green),
  // but it leaves any branch-only key UNTOUCHED.
  private val nextInfo: CurrencySnapshotInfo = infoWithBalance(holder, 200L)

  // The intervening BRANCH prior (`branch != base`): the same `holder` PLUS an EXTRA holder the base/next never mention. Applying the
  // base-cut diff to this prior leaves `holderBranchExtra` in place ⇒ `reconstructInfoFromDiff(branchPrior, diff)` differs from `next` ⇒ the
  // recomputed per-MG root != attested ⇒ DROP. Applying it to the base prior yields exactly `next` ⇒ ADOPT.
  private val branchPriorRaw: CurrencySnapshotInfo =
    basePriorRaw.copy(balances = basePriorRaw.balances + (holderBranchExtra -> Balance(NonNegLong.unsafeFrom(999L))))

  private val headBinary: Signed[StateChannelSnapshotBinary] = mkSignedBinary("s0-head-binary".getBytes("UTF-8"))

  // ============================================================================
  // HARNESS PRECONDITION — make-or-break constraint #2, asserted at the reader level.
  // Proves the MultiBranch overlay genuinely expresses `branch != base`: the SAME MG reconstructs a DIFFERENT prior at the child branch
  // (`branchPrior`, with the extra holder) than at the finalized base (`basePrior`). If this ever collapses to base==branch the spine (A)
  // would be a false-green (it would assert a drop the code makes for an unrelated reason). This is the structural guard the existing
  // Sharding suite cannot provide (it is passthrough-only, where every read collapses to base).
  // ============================================================================

  test("HARNESS: branch reader reconstructs branchPrior, base reader reconstructs basePrior (branch != base is real)") { res =>
    implicit val (h, sp, j) = res
    for {
      store <- freshStore
      basePriorRT <- seedBaseAndRoundTrip(store, mg, inc, basePriorRaw)
      ob <- mkOverlayWithBranch(store, mg, inc, basePriorRT, branchPrior = Some(branchPriorRaw))
      (overlay, childTip) = ob
      baseReader = CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, BranchId.base))
      branchReader = CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, childTip))
      baseInfo <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, baseReader)
      branchInfo <- GlobalStateConverter.reconstructCurrencyInfoFrom[IO](mg, branchReader)
    } yield
      expect.all(
        baseInfo.balances == basePriorRaw.balances,
        branchInfo.balances == branchPriorRaw.balances,
        baseInfo.balances != branchInfo.balances // the load-bearing inequality: branch > base
      )
  }

  // ============================================================================
  // (A) THE SPINE — S1 GREEN guard: branch > base, the follower base-anchors its apply-prior AND its currency write ⇒ MG ADOPTED.
  // ============================================================================

  // Was RED-on-HEAD-by-design (it asserted the run-24→27 DROP); after S1 the follower reads BOTH its apply-prior info and its currency-write
  // removal-prior from the FINALIZED BASE, so an MG whose branch runs ahead of base now reconstructs `reconstructInfoFromDiff(basePrior,
  // diff_over_base)` === attested and ADOPTS. The prior-level §4-MECHANISM asserts are KEPT verbatim — they prove the follower reads the
  // base DESPITE the branch diverging: recompute-over-BASE === attested (adopt) while recompute-over-BRANCH != attested (the drop the HEAD
  // code made). The end-to-end assert FLIPS from `!advancedTo` to `advancedTo`: this is now the permanent S1 regression guard that the
  // base-anchored follower converges where the branch-anchored follower forked.
  test("(A) SPINE [S1 GREEN]: MG past first-incremental, branch != base, follower base-anchors apply-prior + currency write ⇒ MG ADOPTS") {
    res =>
      implicit val (h, sp, j) = res
      for {
        store <- freshStore
        // Constraint 1 (Right-arm-with-diff): seed the FINALIZED BASE with the MG's prior currency info (`basePriorRT`).
        basePriorRT <- seedBaseAndRoundTrip(store, mg, inc, basePriorRaw)
        // Producer-truth: diff cut over the BASE prior; attested root over `nextInfo`. (reconstructInfoFromDiff(basePriorRT, diff) === next.)
        truth <- producerTruth(mg, inc, basePriorRT, nextInfo)
        (wireDiff, attestedRoot) = truth
        checkpoint = mkRightArmCheckpoint(mg, headBinary, wireDiff, attestedRoot)
        // Constraint 2 (branch > base): commit an INTERVENING incremental+info into a child branch so its reader returns `branchPrior`.
        ob <- mkOverlayWithBranch(store, mg, inc, basePriorRT, branchPrior = Some(branchPriorRaw))
        (overlay, childTip) = ob
        // The round-tripped branch prior the follower's `priorInfoOf` WOULD read at `parentTip = childTip` if it anchored on the branch.
        branchPriorRT <- GlobalStateConverter
          .reconstructCurrencyInfoFrom[IO](mg, CurrencyInfoMptAdapters.readerFor(GlobalStateReader.fromOverlay[IO](overlay, childTip)))
        // Producer-truth at the PRIOR level (the §4 mechanism in isolation): recompute the PIN-1 root the GSAM follower would, over each prior.
        recomputedOverBase <- followerRecomputeRoot(mg, inc, basePriorRT, wireDiff)
        recomputedOverBranch <- followerRecomputeRoot(mg, inc, branchPriorRT, wireDiff)
        callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
        mgr <- mkManager(overlay, derivingProcessor(inc, basePriorRT), StubAcceptanceManager(callsRef))
        // The follower runs at `parentTip = childTip` (branch > base); S1 makes its apply-prior + write read the BASE despite that.
        gsi <- runAccept(mgr, checkpoint, parentTip = childTip, lastSnapshotInfo = gsiWith(mg, inc, branchPriorRaw))
        calls <- callsRef.get
      } yield
        expect.all(
          // The checkpoint was verified (the adopt path ran) ...
          calls.size == 1,
          // §4 mechanism, in isolation (KEPT from the RED spine): the committee cut the diff over BASE, so recompute-over-base === attested
          // (adopt), but recompute-over-BRANCH != attested (the drop the HEAD follower made). This proves the asymmetry is branch-vs-base.
          recomputedOverBase == attestedRoot,
          recomputedOverBranch != attestedRoot,
          // ... and end-to-end the MG ADOPTED — its committed currency ADVANCED to `nextInfo`. The follower base-anchored its apply-prior
          // (reconstruct over BASE === attested) AND its currency write (the `Mg*` removal set anchored at base, so `postBytes ==`
          // verify-replay `expectedBytes`, no #107 writer divergence). This is the S1 fix: diff-prior == apply-prior == write, all on base.
          advancedTo(gsi, mg, nextInfo)
        )
  }

  // ============================================================================
  // (B) THE CONTROL — same checkpoint, branch == base ⇒ MG ADOPTED. Proves it's the §4 bug, not a fake root.
  // ============================================================================

  test("(B) CONTROL: SAME checkpoint/diff/attestedRoot but parentTip == base (branch == base) ⇒ MG IS ADOPTED (currency advances)") { res =>
    implicit val (h, sp, j) = res
    for {
      store <- freshStore
      basePriorRT <- seedBaseAndRoundTrip(store, mg, inc, basePriorRaw)
      truth <- producerTruth(mg, inc, basePriorRT, nextInfo)
      (wireDiff, attestedRoot) = truth
      checkpoint = mkRightArmCheckpoint(mg, headBinary, wireDiff, attestedRoot)
      // NO intervening branch — branchPrior = None ⇒ parentTip == base ⇒ the follower APPLY-prior reads the BASE (`basePriorRT`).
      ob <- mkOverlayWithBranch(store, mg, inc, basePriorRT, branchPrior = None)
      (overlay, baseTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      mgr <- mkManager(overlay, derivingProcessor(inc, basePriorRT), StubAcceptanceManager(callsRef))
      gsi <- runAccept(mgr, checkpoint, parentTip = baseTip, lastSnapshotInfo = gsiWith(mg, inc, basePriorRaw))
      calls <- callsRef.get
    } yield
      expect.all(
        calls.size == 1,
        // branch == base ⇒ reconstruct(basePrior, diff_over_base) === next === attested ⇒ ADOPT. The MG's currency ADVANCES to `nextInfo`.
        // (A) drops and (B) adopts with the IDENTICAL checkpoint ⇒ the only difference is branch-vs-base ⇒ proves the §4 violation specifically.
        advancedTo(gsi, mg, nextInfo)
      )
  }

  // ============================================================================
  // (C) THE CONVERGENCE TARGET — now GREEN under S1: a minimal branch > base adopt, asserted purely end-to-end.
  // ============================================================================

  // Same construction as (A) (branch > base, diff cut over base), asserting the POST-S1 outcome: the MG ADOPTS. On HEAD this FAILED (the
  // apply-prior read the branch ⇒ mismatch ⇒ drop), so it was `.ignore`d as a forward-looking placeholder. After S1 — the follower reads
  // BOTH its apply-prior info AND its currency-write removal-prior from the finalized base for sharded MGs — the follower reconstructs
  // `reconstructInfoFromDiff(basePrior, diff)` === attested at any branch depth (at pipelineDepth=1; deeper windows are S2's producer job)
  // ⇒ recompute === attested ⇒ ADOPT, with no #107 writer divergence. Un-ignored as the S1 green gate (a leaner end-to-end twin of (A)).
  test(
    "(C) CONVERGENCE [S1 GREEN]: branch != base ADOPTS now that the follower apply-prior + currency write read the finalized base"
  ) { res =>
    implicit val (h, sp, j) = res
    for {
      store <- freshStore
      basePriorRT <- seedBaseAndRoundTrip(store, mg, inc, basePriorRaw)
      truth <- producerTruth(mg, inc, basePriorRT, nextInfo)
      (wireDiff, attestedRoot) = truth
      checkpoint = mkRightArmCheckpoint(mg, headBinary, wireDiff, attestedRoot)
      ob <- mkOverlayWithBranch(store, mg, inc, basePriorRT, branchPrior = Some(branchPriorRaw))
      (overlay, childTip) = ob
      callsRef <- Ref.of[IO, List[ShardCheckpoint]](List.empty)
      mgr <- mkManager(overlay, derivingProcessor(inc, basePriorRT), StubAcceptanceManager(callsRef))
      gsi <- runAccept(mgr, checkpoint, parentTip = childTip, lastSnapshotInfo = gsiWith(mg, inc, branchPriorRaw))
    } yield expect(advancedTo(gsi, mg, nextInfo)) // GREEN under S1 (base-anchored follower); was RED on HEAD (branch-anchored).
  }
}
