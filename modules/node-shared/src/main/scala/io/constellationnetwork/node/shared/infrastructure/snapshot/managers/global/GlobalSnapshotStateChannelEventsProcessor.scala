package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data._
import cats.effect.Async
import cats.syntax.all._
import cats.{Order, Parallel}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.currency.schema.globalSnapshotSync.GlobalSnapshotSync
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{CurrencyInfoMptAdapters, GlobalStateReader}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelValidator.{StateChannelValidationError, getFeeAddresses}
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.schema.ID.{Id, IdOps}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance, BalanceArithmeticError}
import io.constellationnetwork.schema.currencyMessage._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{RewardTransaction, Transaction, TransactionReference}
import io.constellationnetwork.schema.{CurrencyStateProofSelector, GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import io.circe.Decoder
import io.circe.disjunctionCodecs._
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait GlobalSnapshotStateChannelEventsProcessor[F[_]] {
  type BinaryCurrencyPair = (Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])
  type BalanceUpdate = SortedMap[Address, Balance]
  type MetagraphAcceptanceResult = (NonEmptyList[BinaryCurrencyPair], BalanceUpdate)

  def process(
    snapshotOrdinal: SnapshotOrdinal,
    currentBalances: SortedMap[Address, Balance],
    priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
    priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
    events: List[StateChannelOutput],
    validationType: StateChannelValidationType,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(implicit hasher: Hasher[F]): F[StateChannelAcceptanceResult]

  /** ORDER CONTRACT: `events` NELs must be NEWEST-FIRST (the legacy chain-link path's prepend-built convention — the implementation
    * reverses internally and processes oldest-first); the returned NELs are OLDEST-FIRST (`.last` = the newest binary, which is what the
    * SC-tip setter reads). Callers holding oldest-first windows (shard-checkpoint `chainLinkOrder` output) MUST reverse before calling —
    * see `deriveAdoptedCurrencyState` and `deriveMetagraphRoot`. Feeding oldest-first silently breaks multi-binary windows: the
    * genesis-decode branch sees the newest incremental as "head" and the state fold runs in reverse (the 2026-06-10 seeding failure).
    */
  def processCurrencySnapshots(
    snapshotOrdinal: SnapshotOrdinal,
    currentBalances: SortedMap[Address, Balance],
    priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
    events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    // #259 adopt (numShards>1). Selects how the second-and-subsequent incremental advances the per-MG
    // `CurrencySnapshotInfo` in the `Right` branch:
    //   - `Recreate` (DEFAULT) — the legacy gl0 path: re-derive the full state via `createContext`
    //     (proposal-artifact recreate + byte-equality). This is what the chain-link `process()` path and the
    //     `deriveMetagraphRoot` committee re-exec use; keeping it the default makes every existing call site
    //     (numShards=1, `deriveMetagraphRoot`) byte-identical (the regression bar).
    //   - `AdoptFromSignedFields` — DERIVE the per-MG state by replaying the adopted, committee-attested signed
    //     binary's OWN already-accepted events (blocks, GIVEN rewards, token-locks, allow-spends, messages) onto the
    //     prior Info, then VERIFY the derived root against the binary's committed `stateProof` (economic-security
    //     gate; fall back to prior balances on mismatch, log loudly, never crash). The ordinal is taken from the
    //     signed binary — NOT recomputed as `prior.ordinal.next`. This is the fix for the
    //     `lastCurrencySnapshots`-frozen-at-genesis freeze: `createContext` re-derives ordinal 1 against gl0's
    //     genesis prior and never matches the incoming ordinal N, so the commitment never advances. gl0 MAINTAINS +
    //     VALIDATES the full metagraph currency state (balances/refs/token-locks/allow-spends/messages) so cl1 can
    //     bootstrap from `lastCurrencySnapshots(identifier)`. The derivation is a pure function of `(signed binary,
    //     prior Info)` — split-safe (leader embeds checkpoint, follower re-adopts identically).
    adoptionMode: GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode =
      GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode.Recreate
  )(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, MetagraphAcceptanceResult]]

  /** Assemble a [[StateChannelAcceptanceResult]] from the per-MG output of [[processCurrencySnapshots]].
    *
    * This is the SINGLE definition of the "accepted-map → result" assembly (the `calculateLastCurrencySnapshots` + result construction that
    * the [[process]] tail performs). It is shared by:
    *   - [[process]] — the legacy/standard chain-link path (passes the chain-linked `accepted` + the `returnedSCEvents`).
    *   - `GlobalSnapshotAcceptanceManager.deriveAdoptedCurrencyState` (#259 adopt path) — passes the committee-adopted `accepted` and an
    *     empty `returned` (adopted snapshots are committee-accepted, never gl0-returned).
    *   - `GlobalSnapshotAcceptanceManagerAdoptParitySuite` — so the test exercises the EXACT production assembly (N1; no hand-copy).
    *
    * Pure function of its arguments: `calculatedCurrencyState = priorLastCurrencySnapshots ++ lastStatePerAddress`, and the `accepted` /
    * `incomingCurrencySnapshotsWithState` / `balanceUpdate` projections off `processed`.
    */
  def assembleAcceptanceResult(
    processed: SortedMap[Address, MetagraphAcceptanceResult],
    priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
    returned: Set[StateChannelOutput]
  ): StateChannelAcceptanceResult

  /** Re-execute one metagraph's included SC-binary chain and return its canonical per-MG MPT root — the hierarchical-shard-checkpoints S3
    * committee re-execution primitive (`docs/nakamoto/SHARD-SORTITION-WORKSTREAM-PLAN.md` slice S3).
    *
    * '''What it computes.''' Runs the SAME [[processCurrencySnapshots]] derivation gl0 uses for metagraph snapshots over the single-MG
    * window `Map(metagraphAddress -> binaries)`, takes the LAST resulting [[CurrencySnapshotWithState]] (mirrors
    * `calculateLastCurrencySnapshots`, which is what feeds `GlobalSnapshotInfo.lastCurrencySnapshots` →
    * `GlobalSnapshotAcceptanceManager.buildMerkleTreeAndProofs` → `stateProof.lastCurrencySnapshotsProof`). When a state is derived it
    * hashes the bare `(metagraphAddress, lastState)` — byte-identical to the per-MG leaf `buildMerkleTreeAndProofs` hashes (`(address,
    * state).hash`), i.e. the per-MG canonical root the design's `ShardDerivedStateDelta.perMetagraphMptRoots` carries. When NO state is
    * derived (empty-prior incremental-only window) it hashes the address alone as a deterministic sentinel. The S3 contract is
    * producer-root == verifier-root (both call THIS function over the same inputs), which holds on both branches.
    *
    * '''Determinism contract (the S3 false-slashing crux — Q2).''' The root MUST be a pure function of the included `binaries` alone, so
    * the producer and EVERY committee verifier compute byte-identical results regardless of when/where they re-execute. To guarantee that,
    * the re-execution runs with `priorLastCurrencySnapshots = SortedMap.empty` — it does NOT read prior metagraph state from the live
    * `MptStore` (which would differ across nodes: the producer's fan-out runs after the gl0 MPT was committed to the produced ord —
    * POST-apply — while the gl0 verifier re-runs inside `accept()` PRE-apply, and the gossip-handler verifier reads whatever ordinal the
    * live MPT currently holds). Reading the live MPT here would make honest committee members compute divergent roots and falsely slash
    * each other.
    *
    * '''Consequence (documented boundary).''' With an empty prior, the root is fully correct and meaningful for any chain whose head begins
    * from a full `CurrencySnapshot` (genesis-rooted, or any window carrying a full snapshot) — `processCurrencySnapshots` seeds the prior
    * state from `fullSnapshot.value.info` self-containedly. For incremental-ONLY windows over a non-empty prior, the empty-prior re-exec
    * yields a deterministic but prior-agnostic root; carrying the real gl0-finalized prior at `gl0AnchorOrdinal` byte-identically across
    * nodes needs an ordinal-pinned base read (or a re-exec-gated parent-result cache) and is a follow-up. The byte-identity contract — and
    * therefore the no-false-slashing safety bar — holds in BOTH cases because producer and verifier run the IDENTICAL function over the
    * IDENTICAL inputs.
    *
    * @param metagraphAddress
    *   the metagraph this root is for. The single key of the re-execution window.
    * @param binaries
    *   the metagraph's included SC-binary chain for this checkpoint (the `ShardCheckpoint.derivedStateDelta.includedSnapshots(mg)`
    *   `NonEmptyList`). Re-executed head-to-tail by `processCurrencySnapshots`.
    * @param snapshotOrdinal
    *   the gl0 anchor ordinal the checkpoint rides into. Feeds the fee-required cutover (`feeCalculator.isFeeRequired`) so the producer +
    *   verifier agree on whether fees are deducted. Wire-carried (`ShardCheckpoint.gl0AnchorOrdinal`) so all members pass the same value.
    * @param getGlobalSnapshotByOrdinal
    *   passthrough to `processCurrencySnapshots` (used by `applyCurrencySnapshot` for cross-snapshot context). For S3 the producer +
    *   verifier wire the same gl0 snapshot lookup; on the no-prior path it is only consulted for the second-and-subsequent incremental,
    *   which the empty-prior window does not reach for a genesis-rooted chain.
    */
  def deriveMetagraphRoot(
    metagraphAddress: Address,
    binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
    snapshotOrdinal: SnapshotOrdinal,
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(implicit hasher: Hasher[F]): F[Hash]
}

object GlobalSnapshotStateChannelEventsProcessor {

  /** Selects how `processCurrencySnapshots` advances a metagraph's per-MG `CurrencySnapshotInfo` in the second-and-subsequent incremental
    * (`Right`) branch. See the `adoptionMode` param scaladoc on the trait method.
    */
  sealed trait CurrencyAdoptionMode
  object CurrencyAdoptionMode {

    /** Legacy gl0 path: re-derive the full per-MG state via `createContext` (recreate proposal artifact + byte-equality). Used by the
      * chain-link `process()` path and the `deriveMetagraphRoot` committee re-exec; the default so existing call sites stay byte-identical.
      */
    case object Recreate extends CurrencyAdoptionMode

    /** #259 adopt: DERIVE the per-MG state by replaying the adopted, committee-attested signed binary's OWN already-accepted events
      * (blocks, GIVEN rewards, token-locks, allow-spends, messages) onto the prior Info, ordinal taken from the binary (no `createContext`,
      * no reward re-derivation), then VERIFY the derived root against the committed `stateProof`. gl0 maintains + validates the FULL
      * metagraph currency state; on root mismatch it falls back to prior balances (logs loudly) while still advancing the ordinal, so the
      * freeze stays dead and no unverified balance map is ever committed.
      */
    case object AdoptFromSignedFields extends CurrencyAdoptionMode
  }

  def make[F[_]: Async: JsonSerializer: Parallel](
    stateChannelValidator: StateChannelValidator[F],
    stateChannelManager: GlobalSnapshotStateChannelAcceptanceManager[F],
    currencySnapshotContextFns: CurrencySnapshotContextFunctions[F],
    feeCalculator: FeeCalculator[F],
    reader: GlobalStateReader[F]
  ) =
    new GlobalSnapshotStateChannelEventsProcessor[F] {
      private val logger = Slf4jLogger.getLoggerFromClass[F](GlobalSnapshotStateChannelEventsProcessor.getClass)

      def deserialize[A: Decoder](binary: Signed[StateChannelSnapshotBinary]): F[Option[A]] =
        JsonSerializer[F].deserialize[A](binary.value.content).map(_.toOption)

      // Staking balance behavioral equivalence: for metagraphs with only a full snapshot
      // (Left case), the old fetchStakingBalance returned Balance.empty. With MptStore,
      // getCurrencySnapshotInfo returns a CurrencySnapshotInfo created via toCurrencySnapshotInfo
      // which sets lastMessages = None, so fetchStakingAddress returns None and we still get
      // Balance.empty — preserving the same behavior.
      def buildSnapshotFeesInfo(
        event: StateChannelOutput,
        allFeesAddresses: Map[Address, Set[Address]]
      )(implicit hasher: Hasher[F]): F[SnapshotFeesInfo] =
        event.snapshotBinary.value.lastSnapshotHash match {
          case hash if hash == Hash.empty => SnapshotFeesInfo.empty.pure // genesis
          case _ =>
            deserialize[Signed[CurrencyIncrementalSnapshot]](event.snapshotBinary).flatMap {
              case None =>
                logger.warn(s"Could not get snapshot fee info after deserializing event $event, using empty snapshot fees") >>
                  SnapshotFeesInfo.empty.pure
              case Some(snapshot) =>
                for {
                  // Reconstruct from the UNROLLED per-entry `Mg*` partitions (the `LastCurrencySnapshotInfo` blob is no longer written;
                  // see GlobalStateConverter unroll). Gate on the fieldId-5 incremental so a genesis-only metagraph reads `None` — the same
                  // `Some`/`None` distinction the old blob read produced; the surrounding `fetchStakingAddress`/None-handling is unchanged.
                  hasIncremental <- reader
                    .get[Signed[CurrencyIncrementalSnapshot]](
                      GlobalStateKey.metagraph(event.address, GlobalStateFieldId.LastIncrementalCurrencySnapshots)
                    )
                    .map(_.isDefined)
                  maybeCurrencyInfo <-
                    if (hasIncremental)
                      GlobalStateConverter
                        .reconstructCurrencyInfoFrom[F](event.address, CurrencyInfoMptAdapters.readerFor(reader))
                        .map(_.some)
                    else none[CurrencySnapshotInfo].pure[F]
                  stakingAddr = maybeCurrencyInfo.flatMap(fetchStakingAddress)
                  stakingBalance <- stakingAddr.fold(Balance.empty.pure[F]) { addr =>
                    reader.get[Balance](GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr)).map(_.getOrElse(Balance.empty))
                  }
                  sortedMessagesDesc = snapshot.value.messages.map(_.toList.sortBy(-_.ordinal.value.value))
                  maybeOwnerAddress = sortedMessagesDesc.flatMap(_.find(_.messageType === MessageType.Owner)).map(_.address)
                  maybeStakingAddress = sortedMessagesDesc.flatMap(_.find(_.messageType === MessageType.Staking)).map(_.address)
                } yield SnapshotFeesInfo(allFeesAddresses, stakingBalance, maybeOwnerAddress, maybeStakingAddress)
            }
        }

      def process(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: List[StateChannelOutput],
        validationType: StateChannelValidationType,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[StateChannelAcceptanceResult] = {
        // getFeeAddresses iterates `priorLastCurrencySnapshots` (materialized once by the caller from MPT)
        // to collect fee addresses across all metagraphs. The staking-balance lookups inside
        // buildSnapshotFeesInfo remain MptStore-backed (per-event, per-key) so we do not duplicate that here.
        val allFeesAddresses: Map[Address, Set[Address]] = getFeeAddresses(priorLastCurrencySnapshots)
        type Acc = (Map[Address, Set[Address]], List[ValidatedNec[(Address, StateChannelValidationError), StateChannelOutput]])

        events
          .sortBy(_.address)
          .foldLeftM[F, Acc]((allFeesAddresses, List.empty)) {
            case ((prevAllFeeAddresses, alreadyProcessed), event) =>
              buildSnapshotFeesInfo(event, prevAllFeeAddresses).flatMap { snapshotFeesInfo =>
                val validationV = validationType match {
                  case StateChannelValidationType.Full =>
                    stateChannelValidator.validate(event, snapshotOrdinal, snapshotFeesInfo)
                  case StateChannelValidationType.Historical =>
                    stateChannelValidator.validateHistorical(event, snapshotOrdinal, snapshotFeesInfo)
                }

                validationV.map {
                  case valid @ Validated.Valid(event) =>
                    val updatedAllFeesAddresses = prevAllFeeAddresses.updatedWith(event.address) { existing =>
                      val added = Set(snapshotFeesInfo.ownerAddress, snapshotFeesInfo.stakingAddress).flatten
                      existing.map(_ ++ added).orElse(added.some)
                    }
                    (updatedAllFeesAddresses, alreadyProcessed :+ valid)
                  case invalid @ Validated.Invalid(_) =>
                    (prevAllFeeAddresses, alreadyProcessed :+ invalid.errorMap(error => (event.address, error)))
                }
              }
          }
          .map { case (_, processedEvents) => processedEvents.partitionMap(_.toEither) }
          .flatTap { case (invalid, _) => logger.warn(s"Invalid state channels events: $invalid").whenA(invalid.nonEmpty) }
          .flatMap {
            case (_, validatedEvents) =>
              processStateChannelEvents(snapshotOrdinal, priorLastStateChannelSnapshotHashes, validatedEvents)
          }
          .flatMap {
            case (scSnapshots, returnedSCEvents) =>
              processCurrencySnapshots(
                snapshotOrdinal,
                currentBalances,
                priorLastCurrencySnapshots,
                scSnapshots,
                getGlobalSnapshotByOrdinal
              ).map { accepted =>
                // Shared assembly (N1) — IDENTICAL construction the #259 adopt path runs, so the chain-link result and the
                // adopt result are byte-equivalent for the same `accepted` map (the load-bearing #259 byte-exactness claim).
                assembleAcceptanceResult(accepted, priorLastCurrencySnapshots, returnedSCEvents)
              }
          }
      }

      def assembleAcceptanceResult(
        processed: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        returned: Set[StateChannelOutput]
      ): StateChannelAcceptanceResult = {
        val (lastCurrencyStates, incomingCurrencyState) = calculateLastCurrencySnapshots(processed, priorLastCurrencySnapshots)
        val finalScSnapshots = processed.map { case (k, (v, _)) => k -> v.map(_._1) }
        // TODO: ASSUMING that owner addresses are restricted from being shared at this point
        val balanceUpdates = processed.values.map(_._2).foldLeft(SortedMap.empty[Address, Balance])(_ ++ _)

        StateChannelAcceptanceResult(
          finalScSnapshots,
          lastCurrencyStates,
          returned,
          balanceUpdates,
          incomingCurrencyState
        )
      }

      private def calculateLastCurrencySnapshots(
        processedCurrencySnapshots: SortedMap[Address, MetagraphAcceptanceResult],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]]
      ): (SortedMap[Address, CurrencySnapshotWithState], SortedMap[Address, List[CurrencySnapshotWithState]]) = {
        val lastCurrencySnapshotPerAddress =
          processedCurrencySnapshots.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2).lastOption }.collect {
            case (key, Some(state)) => key -> state
          }

        val lastCurrencySnapshots =
          processedCurrencySnapshots.map { case (k, (v, _)) => k -> v.toList.flatMap(_._2) }.filterNot { case (_, list) => list.isEmpty }

        (
          priorLastCurrencySnapshots.concat(lastCurrencySnapshotPerAddress),
          lastCurrencySnapshots
        )
      }

      private def applyCurrencySnapshot(
        currencyAddress: Address,
        lastState: CurrencySnapshotInfo,
        lastSnapshot: Signed[CurrencyIncrementalSnapshot],
        snapshot: Signed[CurrencyIncrementalSnapshot],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[CurrencySnapshotInfo] =
        currencySnapshotContextFns
          .createContext(
            CurrencySnapshotContext(currencyAddress, lastState),
            lastSnapshot,
            snapshot,
            getGlobalSnapshotByOrdinal
          )
          .map(_.snapshotInfo)

      /** #259 adopt (`CurrencyAdoptionMode.AdoptFromSignedFields`). DERIVE the per-MG `CurrencySnapshotInfo` by replaying the adopted,
        * committee-attested signed binary's OWN already-accepted events onto the prior `lastState` — replacing the diverging
        * `createContext` recreate-and-byte-compare derivation that freezes `lastCurrencySnapshots` at genesis when gl0 folds a
        * shard-tip-relative delta window against its own genesis prior (re-derives ordinal 1, never matches the incoming ordinal N).
        *
        * '''gl0 MAINTAINS + VALIDATES the metagraph currency state.''' Unlike the earlier roots-only variant (which emptied balances/refs
        * and broke cl1 bootstrap — cl1 aligns its first currency state to gl0's `lastCurrencySnapshots(identifier)` Info,
        * `CurrencySnapshotProcessor.scala:355-373`, and cannot bootstrap from an empty balance map), gl0 now holds the FULL per-MG state:
        *   - `balances` — replay each accepted block's transactions (per-source `−amount −fee`, per-destination `+amount`, mirroring
        *     `BlockAcceptanceLogic.processBalances`) then add the snapshot's GIVEN `rewards` (per-destination `+amount`, mirroring
        *     `BalanceOpsManager.acceptRewardTxs`). Rewards are taken AS-GIVEN from `snapshot.value.rewards` — NOT recomputed; reward
        *     recomputation is the consensus-derived divergence that froze the recreate path.
        *   - `lastTxRefs` — the last (highest-ordinal) `TransactionReference` per source across accepted blocks; fresh destinations seed
        *     `emptyCurrency(identifier)` (mirrors `BlockAcceptanceOpsManager.acceptTransactionRefs`).
        *   - `activeTokenLocks` / `lastTokenLockRefs` — accepted token-locks grouped by source, merged into prior, then EXPIRED locks
        *     dropped (`unlockEpoch < globalSyncView.epochProgress`, mirroring `TokenLockOpsManager`'s expiry filter); refs = last per
        *     source.
        *   - `activeAllowSpends` / `lastAllowSpendRefs` — accepted allow-spends grouped by source, merged into prior, then EXPIRED
        *     allow-spends dropped (`lastValidEpochProgress < globalSyncView.epochProgress`); refs = last per source.
        *   - `globalSnapshotSyncView` — the exact `MessageValidationOpsManager.acceptGlobalSnapshotSyncs` fold: this snapshot's
        *     `globalSnapshotSyncs` sorted by (`parentOrdinal`, full `Order[Signed[GlobalSnapshotSync]]`), folded latest-per-peer into the
        *     prior view.
        *   - `lastMessages` — CARRIED FORWARD: the prior `lastState.lastMessages` merged with THIS snapshot's accepted `messages`,
        *     latest-per-`MessageType` (the exact `MessageValidationOpsManager.acceptMessages` fold). This is the genuine cross-MG
        *     dependency `getFeeAddresses` / `fetchOwnerAddress` / `fetchStakingAddress` read for owner/staking config and fee deduction.
        *
        * Option SHAPES (None vs Some(empty)) on the candidate are mirrored from the binary's committed `stateProof` — the proof hashes each
        * Option via `traverse(_.hash)`, so shape mismatch alone would fail field comparison even with identical contents.
        *
        * '''Economic-security gate (PER-FIELD root verification).''' The derived Info's `stateProof(ordinal)` (the same 9-field hash the
        * metagraph's ml0 committed via `CurrencySnapshotAcceptanceManager` `csi.stateProof`) is compared against the binary's committed
        * `snapshot.value.stateProof` FIELD BY FIELD. Each of the 9 fields is adopted iff ITS proof component matches; on a per-field
        * mismatch that field alone is carried forward from `lastState` (kept at its last VERIFIED value) and a LOUD warn logs the
        * committed-vs-derived proof pair. This keeps fields gl0 CAN reproduce (tx-refs, token-locks, messages, sync-view) fresh even when a
        * field it CANNOT fully reproduce (`balances` shaped by cross-shard spend-actions / fee deduction) diverges. The ordinal still
        * advances at the call site (`processCurrencySnapshots` commits `Right((snapshot, info))`, ordinal taken from `snapshot`), so the
        * freeze stays dead while gl0 never commits an UNVERIFIED field value. The global snapshot is never crashed.
        *
        * CONSUMER CAVEAT: downstream consensus readers treat parts of this mirror as authoritative — `si.balances` feeds cross-metagraph
        * spend-action validation (`GlobalSnapshotAcceptanceManager.currencyBalances`), `activeTokenLocks` feeds token-lock balance deltas
        * (`TokenLockStateManager`), `activeAllowSpends` feeds metagraph-scoped allow-spend acceptance. A per-field-stale value is
        * last-VERIFIED, never unverified — but it can LAG the metagraph's true state until the roots-only reshape
        * (docs/nakamoto/ROOTS-ONLY-SHARDING-ARCHITECTURE.md §3.4) moves those readers off the mirror.
        *
        * '''Determinism / split-safety.''' A pure deterministic function of `(snapshot, lastState)` — replays only already-accepted events
        * (no re-validation, no node-local shard-store / global-snapshot read, no `Double`; exact integer `Amount` arithmetic; hashing
        * routed through `Hasher[F]`). Every gl0 node (leader producing, follower, validator) builds the byte-identical `Right((snapshot,
        * info))` from the byte-identical adopted binary + prior, so the per-MG Merkle leaf and `lastCurrencySnapshotsProof` are
        * byte-identical cross-node.
        */
      private def deriveAdoptedCurrencyInfo(
        address: Address,
        lastState: CurrencySnapshotInfo,
        snapshot: Signed[CurrencyIncrementalSnapshot]
      )(implicit hasher: Hasher[F]): F[CurrencySnapshotInfo] = {
        implicit val currencyStateProofSelector: CurrencyStateProofSelector = CurrencyStateProofSelector.instance

        val artifact = snapshot.value

        // lastMessages carry-forward — the exact MessageValidationOpsManager.acceptMessages fold (ascending parentOrdinal,
        // latest-per-MessageType wins). `messages` already holds only committee-accepted messages, so no re-validation here.
        val priorLastMessages: SortedMap[MessageType, Signed[CurrencyMessage]] =
          lastState.lastMessages.getOrElse(SortedMap.empty[MessageType, Signed[CurrencyMessage]])
        val nextLastMessages: SortedMap[MessageType, Signed[CurrencyMessage]] =
          artifact.messages
            .map(_.toList.sortBy(_.parentOrdinal))
            .getOrElse(List.empty)
            .foldLeft(priorLastMessages) { (acc, message) =>
              acc.updated(message.value.messageType, message)
            }
        val nextLastMessagesOpt = if (nextLastMessages.isEmpty) None else Some(nextLastMessages)

        // Accepted events carried in the signed snapshot.
        val acceptedTransactions: List[Signed[Transaction]] =
          artifact.blocks.toList.flatMap(_.block.value.transactions.toSortedSet.toList)
        val acceptedTokenLocks: List[Signed[TokenLock]] =
          artifact.tokenLockBlocks.toList.flatMap(_.toList).flatMap(_.value.tokenLocks.toSortedSet.toList)
        val acceptedAllowSpends: List[Signed[AllowSpend]] =
          artifact.allowSpendBlocks.toList.flatMap(_.toList).flatMap(_.value.transactions.toSortedSet.toList)

        // Balances: replay accepted block txs (BlockAcceptanceLogic.processBalances semantics) then GIVEN rewards
        // (BalanceOpsManager.acceptRewardTxs semantics). Exact Amount arithmetic; on underflow/overflow the result will not
        // match the committed stateProof and we fall back (see below) — we never throw here.
        def applyDelta(
          balances: Either[BalanceArithmeticError, SortedMap[Address, Balance]],
          addr: Address,
          op: Balance => Either[BalanceArithmeticError, Balance]
        ): Either[BalanceArithmeticError, SortedMap[Address, Balance]] =
          balances.flatMap { bs =>
            op(bs.getOrElse(addr, Balance.empty)).map(updated => bs.updated(addr, updated))
          }

        val derivedBalancesE: Either[BalanceArithmeticError, SortedMap[Address, Balance]] = {
          val afterTxs = acceptedTransactions.foldLeft(lastState.balances.asRight[BalanceArithmeticError]) { (acc, signedTx) =>
            val tx = signedTx.value
            // Mirror BlockAcceptanceLogic.processBalances: per-source `−amount −fee`, per-destination `+amount`.
            // `TransactionAmount`/`TransactionFee` convert to `Amount` via their companion `toAmount` implicits.
            val amount: Amount = tx.amount
            val fee: Amount = tx.fee
            val withDebit = applyDelta(applyDelta(acc, tx.source, _.minus(amount)), tx.source, _.minus(fee))
            applyDelta(withDebit, tx.destination, _.plus(amount))
          }
          // NOTE: token-lock / allow-spend / unlock / fee-tx / cross-shard-spend BALANCE effects are intentionally
          // NOT reproduced here. Matching ml0's `balances` byte-for-byte would mean re-running its FULL acceptance
          // (currency-id filtering, epoch-based expiry, cross-shard spend-action data gl0 does not hold) — the thing
          // execution-sharding exists to avoid. Instead the gate below adopts PER FIELD: gl0 commits each field it
          // CAN reproduce (token-locks/allow-spends/refs/sync-view/messages — every one verified against the
          // committee-attested proof) and carries `balances` forward for the ordinals it can't reproduce them.
          artifact.rewards.foldLeft(afterTxs) { (acc, reward) =>
            val rewardAmount: Amount = reward.amount
            applyDelta(acc, reward.destination, _.plus(rewardAmount))
          }
        }

        // lastTxRefs: last (highest-ordinal) reference per source, plus emptyCurrency seed for fresh destinations
        // (BlockAcceptanceOpsManager.acceptTransactionRefs semantics). Fold over hashed refs is deterministic.
        for {
          initialTxRef <- TransactionReference.emptyCurrency[F](address)

          txRefUpdates <- acceptedTransactions.traverse { signedTx =>
            TransactionReference.of[F](signedTx).map(signedTx.value.source -> _)
          }.map(_.foldLeft(Map.empty[Address, TransactionReference]) {
            case (acc, (src, ref)) => acc.updatedWith(src)(_.fold(ref)(prev => if (ref.ordinal > prev.ordinal) ref else prev).some)
          })
          updatedTxRefsBase = lastState.lastTxRefs ++ txRefUpdates
          newDestinations = acceptedTransactions.map(_.value.destination).toSet -- updatedTxRefsBase.keySet
          nextLastTxRefs = updatedTxRefsBase ++ newDestinations.toList.map(_ -> initialTxRef)

          tokenLockRefUpdates <- acceptedTokenLocks.traverse { signedTl =>
            TokenLockReference.of[F](signedTl).map(signedTl.value.source -> _)
          }.map(_.foldLeft(Map.empty[Address, TokenLockReference]) {
            case (acc, (src, ref)) => acc.updatedWith(src)(_.fold(ref)(prev => if (ref.ordinal > prev.ordinal) ref else prev).some)
          })
          priorTokenLockRefs = lastState.lastTokenLockRefs.getOrElse(SortedMap.empty[Address, TokenLockReference])
          nextTokenLockRefs = priorTokenLockRefs ++ tokenLockRefUpdates

          allowSpendRefUpdates <- acceptedAllowSpends.traverse { signedAs =>
            AllowSpendReference.of[F](signedAs).map(signedAs.value.source -> _)
          }.map(_.foldLeft(Map.empty[Address, AllowSpendReference]) {
            case (acc, (src, ref)) => acc.updatedWith(src)(_.fold(ref)(prev => if (ref.ordinal > prev.ordinal) ref else prev).some)
          })
          priorAllowSpendRefs = lastState.lastAllowSpendRefs.getOrElse(SortedMap.empty[Address, AllowSpendReference])
          nextAllowSpendRefs = priorAllowSpendRefs ++ allowSpendRefUpdates

          // active token-locks / allow-spends: incoming grouped by source, merged into prior (mirrors `incoming |+| prior`
          // shape from CurrencySnapshotAcceptanceManager; gl0 holds the committed set, expiry/unlock is re-verified via the
          // stateProof gate below rather than recomputed here — node-local epoch is not split-safe).
          // The global epoch THIS snapshot synced to — exactly what ml0's TokenLockOpsManager / AllowSpendOpsManager
          // pass as `lastGlobalSnapshotEpochProgress` for expiry. It rides in the signed snapshot (`globalSyncView`),
          // so gl0 has it deterministically; when absent (pre-sync genesis snapshots, which carry no locks/allow-spends)
          // no expiry is applied. Used below to DROP expired entries — ml0 removes them, so an add-only gl0 merge would
          // retain stale (expired) locks/allow-spends and diverge from the committed proof forever (the token-lock
          // EXPIRATION test: a prior lock expires in ml0, gl0 keeps it, so `{stale,new} ≠ committed {new}`).
          syncEpoch = artifact.globalSyncView.map(_.epochProgress)
          incomingTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] =
            acceptedTokenLocks.groupBy(_.value.source).map { case (src, tls) => src -> SortedSet.from(tls) }.to(SortedMap)
          priorActiveTokenLocks = lastState.activeTokenLocks.getOrElse(SortedMap.empty[Address, SortedSet[Signed[TokenLock]]])
          nextActiveTokenLocks = (priorActiveTokenLocks |+| incomingTokenLocks).map {
            case (addr, locks) => addr -> locks.filter(l => syncEpoch.forall(e => l.value.unlockEpoch.forall(_ >= e)))
          }.filter { case (_, s) => s.nonEmpty }

          incomingAllowSpends: SortedMap[Address, SortedSet[Signed[AllowSpend]]] =
            acceptedAllowSpends.groupBy(_.value.source).map { case (src, as) => src -> SortedSet.from(as) }.to(SortedMap)
          priorActiveAllowSpends = lastState.activeAllowSpends.getOrElse(SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]])
          nextActiveAllowSpends = (priorActiveAllowSpends |+| incomingAllowSpends).map {
            case (addr, as) => addr -> as.filter(a => syncEpoch.forall(e => a.value.lastValidEpochProgress >= e))
          }.filter { case (_, s) => s.nonEmpty }

          // globalSnapshotSyncView: prior view + this snapshot's accepted globalSnapshotSyncs, keyed by the signer's PeerId,
          // byte-identical to MessageValidationOpsManager.acceptGlobalSnapshotSyncs (sort by parentOrdinal asc then default
          // Signed order so latest-per-peer wins). The metagraph UPDATES this every snapshot, so the prior code's plain
          // carry-forward (`= lastState.globalSnapshotSyncView`) mismatched the committed proof on every post-migration
          // ordinal -> the gate fell back -> lastCurrencySnapshots froze at the seed (#259 token-lock / allow-spend / spend).
          syncOrdering = Order
            .whenEqual[Signed[GlobalSnapshotSync]](Order.by(_.value.parentOrdinal), Order[Signed[GlobalSnapshotSync]])
            .toOrdering
          nextGlobalSnapshotSyncView = artifact.globalSnapshotSyncs
            .map(_.toList.sorted(syncOrdering))
            .getOrElse(List.empty)
            .foldLeft(lastState.globalSnapshotSyncView.getOrElse(SortedMap.empty[PeerId, Signed[GlobalSnapshotSync]])) { (acc, sync) =>
              acc.updated(sync.proofs.head.id.toPeerId, sync)
            }

          // Build the candidate derived Info. The Option SHAPE of each field must equal the committed stateProof's
          // (`stateProof` maps `None -> None`, `Some(map) -> Some(hash(map))` via `traverse(_.hash)`), and the metagraph emits
          // `Some(map)`-even-when-empty post-tessellation3-migration (`None` pre-migration). Drive the shape off the committed
          // proof per field so it matches both eras; the replayed map is the content, the economic-security gate below verifies
          // the hash. (Prior `if (nextX.isEmpty) lastState.X else Some(nextX)` produced `None`/stale on the empty case, which
          // never matched the metagraph's post-migration `Some(empty)`.)
          derivedBalances = derivedBalancesE.getOrElse(lastState.balances)
          committedProof = artifact.stateProof
          candidate = CurrencySnapshotInfo(
            lastTxRefs = nextLastTxRefs,
            balances = derivedBalances,
            lastMessages = nextLastMessagesOpt,
            lastFeeTxRefs = None,
            lastAllowSpendRefs = committedProof.lastAllowSpendRefsProof.map(_ => nextAllowSpendRefs),
            activeAllowSpends = committedProof.activeAllowSpends.map(_ => nextActiveAllowSpends),
            globalSnapshotSyncView = committedProof.globalSnapshotSync.map(_ => nextGlobalSnapshotSyncView),
            lastTokenLockRefs = committedProof.lastTokenLockRefsProof.map(_ => nextTokenLockRefs),
            activeTokenLocks = committedProof.activeTokenLocks.map(_ => nextActiveTokenLocks)
          )

          // Economic-security gate, PER FIELD: commit each derived field whose hash matches the committee-attested
          // committed proof (verified, safe); carry the prior value forward for any field gl0 cannot reproduce
          // (notably `balances`, which needs currency-id / epoch-expiry / cross-shard-spend effects). Pure per-field
          // hash compare ⇒ every gl0 node yields the same `adopted` ⇒ split-safe; gl0 never commits an unverified
          // field. This replaces the prior all-or-nothing fallback that discarded EVERY verified field (token-locks,
          // refs, sync-view) the moment `balances` diverged — which froze lastCurrencySnapshots at the seed.
          derivedProof <- candidate.stateProof[F](artifact.ordinal)
          adopted = CurrencySnapshotInfo(
            lastTxRefs =
              if (derivedProof.lastTxRefsProof === committedProof.lastTxRefsProof) candidate.lastTxRefs else lastState.lastTxRefs,
            balances = if (derivedProof.balancesProof === committedProof.balancesProof) candidate.balances else lastState.balances,
            lastMessages =
              if (derivedProof.lastMessagesProof === committedProof.lastMessagesProof) candidate.lastMessages else lastState.lastMessages,
            lastFeeTxRefs =
              if (derivedProof.lastFeeTxRefsProof === committedProof.lastFeeTxRefsProof) candidate.lastFeeTxRefs
              else lastState.lastFeeTxRefs,
            lastAllowSpendRefs =
              if (derivedProof.lastAllowSpendRefsProof === committedProof.lastAllowSpendRefsProof) candidate.lastAllowSpendRefs
              else lastState.lastAllowSpendRefs,
            activeAllowSpends =
              if (derivedProof.activeAllowSpends === committedProof.activeAllowSpends) candidate.activeAllowSpends
              else lastState.activeAllowSpends,
            globalSnapshotSyncView =
              if (derivedProof.globalSnapshotSync === committedProof.globalSnapshotSync) candidate.globalSnapshotSyncView
              else lastState.globalSnapshotSyncView,
            lastTokenLockRefs =
              if (derivedProof.lastTokenLockRefsProof === committedProof.lastTokenLockRefsProof) candidate.lastTokenLockRefs
              else lastState.lastTokenLockRefs,
            activeTokenLocks =
              if (derivedProof.activeTokenLocks === committedProof.activeTokenLocks) candidate.activeTokenLocks
              else lastState.activeTokenLocks
          )
          _ <- Async[F].whenA(derivedProof =!= committedProof)(
            logger.warn(
              s"[ACCEPTANCE/ADOPT] address=${address.show} ordinal=${artifact.ordinal.show} per-field adopt: derived stateProof " +
                s"differs from committed — committing matched fields, carrying prior forward for the rest (typically `balances`, " +
                s"not reproducible from pure replay). committed=${committedProof.show} derived=${derivedProof.show}"
            )
          )
          // [OVERPRUNE-DIAG] (2026-06-13, REMOVE after e2e): disambiguate the run-26 allow-spend drop — expiry over-prune
          // (divergence #1: gl0 syncEpoch > ml0's expiry epoch) vs empty-window extraction vs compounding-empty-prior. Logs gl0's
          // syncEpoch, the prior/incoming/after-expiry/adopted allow-spend counts, whether the per-field gate matched (false =
          // carry-forward fired), and the lastValidEpochProgress of anything the expiry filter cut (to see if syncEpoch is anomalously
          // high vs the allow-spend deadlines). Pairs with the ml0-side [OVERPRUNE-DIAG/ml0] epoch-divergence line.
          _ <- {
            def asCount(m: SortedMap[Address, SortedSet[Signed[AllowSpend]]]): Int = m.values.map(_.size).sum
            val priorAs = asCount(priorActiveAllowSpends)
            val incAs = asCount(incomingAllowSpends)
            val afterExpiryAs = asCount(nextActiveAllowSpends)
            val adoptedAs = adopted.activeAllowSpends.fold(0)(asCount)
            val droppedByExpiry = (priorActiveAllowSpends |+| incomingAllowSpends).values.flatten.toList
              .filter(a => syncEpoch.exists(e => a.value.lastValidEpochProgress < e))
              .map(_.value.lastValidEpochProgress.value.value)
            Async[F].whenA(priorAs + incAs > 0 || adoptedAs > 0)(
              logger.info(
                s"[OVERPRUNE-DIAG/gl0] mg=${address.show.take(10)} ord=${artifact.ordinal.show} " +
                  s"syncEpoch=${syncEpoch.map(_.value.value.toString).getOrElse("none")} " +
                  s"allowSpends[prior=$priorAs inc=$incAs afterExpiry=$afterExpiryAs adopted=$adoptedAs] " +
                  s"asGateMatch=${derivedProof.activeAllowSpends === committedProof.activeAllowSpends} " +
                  s"droppedByExpiry=${droppedByExpiry.size}${if (droppedByExpiry.nonEmpty) droppedByExpiry.take(3).mkString("(lve=", ",", ")")
                    else ""}"
              )
            )
          }
        } yield adopted
      }

      /** Processes currency snapshots for each metagraph address, applying fee deduction logic.
        *
        * Fee deduction follows three cases per binary:
        *   1. Fee not required (pre-fee-ordinal or fee waived): accept the binary unconditionally. 2. Fee required but no fee address
        *      (owner address missing from currency messages): reject the binary — we cannot deduct fees without a destination address. 3.
        *      Fee required with fee address: look up the metagraph owner's balance first in the local accumulator (tracks balance changes
        *      within this batch), then fall back to MptStore. If the balance covers the fee, deduct it and accept; otherwise reject
        *      remaining binaries.
        */
      def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
        adoptionMode: GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode =
          GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode.Recreate
      )(implicit hasher: Hasher[F]): F[SortedMap[Address, MetagraphAcceptanceResult]] = {
        import GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode

        // The second-and-subsequent incremental advances the per-MG `CurrencySnapshotInfo`. Under `Recreate` (the legacy default,
        // numShards=1 + `deriveMetagraphRoot`) gl0 re-derives the full state via `createContext` — byte-identical to before. Under
        // `AdoptFromSignedFields` (#259 adopt path) gl0 DERIVES the full state by replaying the signed binary's own already-accepted
        // events onto the prior Info and verifies the derived root against the committed `stateProof`, breaking the
        // `createContext`-against-genesis freeze while still maintaining + validating the metagraph currency state.
        def deriveNextCurrencyInfo(
          address: Address,
          lastState: CurrencySnapshotInfo,
          lastIncremental: Signed[CurrencyIncrementalSnapshot],
          snapshot: Signed[CurrencyIncrementalSnapshot]
        ): F[CurrencySnapshotInfo] =
          adoptionMode match {
            case CurrencyAdoptionMode.Recreate =>
              applyCurrencySnapshot(address, lastState, lastIncremental, snapshot, getGlobalSnapshotByOrdinal)
            case CurrencyAdoptionMode.AdoptFromSignedFields =>
              deriveAdoptedCurrencyInfo(address, lastState, snapshot)
          }

        val isFeeRequired = feeCalculator.isFeeRequired(snapshotOrdinal)

        events.toList.parTraverse {
          case (address, binaries) =>
            type Result = Option[MetagraphAcceptanceResult]
            type Agg = (Result, List[Signed[StateChannelSnapshotBinary]])

            val stubBinary: Signed[StateChannelSnapshotBinary] = Signed(
              StateChannelSnapshotBinary(Hash.empty, Array.emptyByteArray, SnapshotFee.MinValue),
              NonEmptySet.one(SignatureProof(Id(Hex("")), Signature(Hex(""))))
            )

            val emptyBalanceUpdate = SortedMap.empty[Address, Balance]

            // initialState reads from `priorLastCurrencySnapshots` (materialized via the
            // `getAllLastCurrencySnapshots` MPT prefix-scan) rather than a synthetic
            // `lastGlobalSnapshotInfo.copy(...)`, because the Left(fullSnapshot) vs Right(incremental, info)
            // distinction matters: the Left branch handles the first-incremental-over-full transition without
            // calling applyCurrencySnapshot. The materialize preserves Left bindings via the per-address
            // `Left(Signed[CurrencySnapshot])` partition lookup.
            val initialState =
              priorLastCurrencySnapshots
                .get(address)
                .map(init => (stubBinary, init.some))
                .map(s => (NonEmptyList.one(s), SortedMap.empty[Address, Balance]))

            (initialState, binaries.toList.reverse)
              .tailRecM[F, Result] {
                case (state, Nil) => state.asRight[Agg].pure[F]

                case (None, head :: tail) =>
                  deserialize[Signed[CurrencySnapshot]](head).flatMap {
                    case Some(snapshot) => // full snapshot - we don't subtract fee
                      Async[F].pure(
                        (
                          (NonEmptyList.one((head, snapshot.asLeft.some)), emptyBalanceUpdate).some,
                          tail
                        ).asLeft[Result]
                      )
                    case None =>
                      adoptionMode match {
                        case CurrencyAdoptionMode.AdoptFromSignedFields =>
                          // GENESIS-WINDOW GUARD (2026-06-10): on the adopt path, an unseeded MG's window MUST start with
                          // its full genesis snapshot. Accepting a non-genesis head with `none` state (the legacy
                          // fee-not-required branch below) advances the SC tip past the unprocessed genesis with ZERO
                          // currency state — the silent half of the chain-hole wedge. Drop the WHOLE window loudly; the
                          // ancestor checkpoint carrying the genesis adopts at a later ord and this window then chains.
                          // Unreachable once the GSAM anchor guard holds — defense in depth.
                          logger.error(
                            s"Adopt-mode genesis-window guard: mg=${address.show} window head is not a full genesis " +
                              s"snapshot while gl0 has no prior currency state — dropping window (no SC-tip advance)"
                          ) >> Async[F].pure(none.asRight[Agg])
                        case CurrencyAdoptionMode.Recreate =>
                          // Legacy/numShards=1 behavior, byte-identical: accept the binary stateless if fee is not required.
                          Async[F].pure(
                            if (isFeeRequired) none.asRight[Agg]
                            else ((NonEmptyList.one((head, none)), emptyBalanceUpdate).some, tail).asLeft[Result]
                          )
                      }
                  }

                case (Some((nel, balanceUpdate)), head :: tail) =>
                  val current: Result = (nel, balanceUpdate).some
                  nel.head match {
                    case (_, None) =>
                      deserialize[Signed[CurrencySnapshot]](head).map {
                        case Some(snapshot) => // full snapshot - we don't subtract fee
                          (
                            (nel.prepend((head, snapshot.asLeft.some)), balanceUpdate).some,
                            tail
                          ).asLeft
                        case None => // no full snapshot yet - we only accept the binary if fee is not required
                          if (isFeeRequired) current.asRight
                          else ((nel.prepend((head, none)), balanceUpdate).some, tail).asLeft
                      }

                    case (_, lastCurrState @ Some(Left(fullSnapshot))) =>
                      deserialize[Signed[CurrencyIncrementalSnapshot]](head).map {
                        case Some(snapshot) => // first incremental - we don't subtract fee
                          (
                            (
                              nel.prepend((head, (snapshot, fullSnapshot.value.info.toCurrencySnapshotInfo).asRight.some)),
                              balanceUpdate
                            ).some,
                            tail
                          ).asLeft
                        case None => // no first incremental yet - we only accept the binary if fee is not required
                          if (isFeeRequired) current.asRight
                          else ((nel.prepend((head, lastCurrState)), balanceUpdate).some, tail).asLeft
                      }

                    case (_, lastCurrState @ Some(Right((lastIncremental, lastState)))) =>
                      deserialize[Signed[CurrencyIncrementalSnapshot]](head).flatMap {
                        case Some(snapshot) => // second or subsequent incremental snapshot - we do subtract fee
                          deriveNextCurrencyInfo(
                            address,
                            lastState,
                            lastIncremental,
                            snapshot
                          ).flatMap { state =>
                            val maybeFeeAddress = state.lastMessages.flatMap(_.get(MessageType.Owner)).map(_.address)

                            // Fee deduction: if fee is required, we need a fee address (owner address from
                            // currency messages). Without one we reject. With one, we check the local balance
                            // accumulator first (to account for fees already deducted earlier in this batch),
                            // falling back to `currentBalances` for the initial balance lookup.
                            //
                            // `currentBalances` is the in-progress balance map (`priorBalances ++` block-level
                            // delta) materialized once by GSAM before calling `process`. It is deliberately NOT
                            // a per-event MptStore read because accept() mutates the MptStore as a side-effect
                            // (syncFromStateChanges). When validateArtifact calls accept() a second time, the
                            // MptStore would already reflect the validator's own proposal computation,
                            // producing a different balance than the leader saw — causing
                            // currencyAcceptanceBalanceUpdate to diverge. Pinning to a snapshot value avoids
                            // that, and also correctly reflects block-level balance changes that the MptStore
                            // does not yet contain at the time of fee calculation.
                            maybeFeeAddress
                              .filter(_ => isFeeRequired)
                              .fold(
                                if (!isFeeRequired)
                                  ((nel.prepend((head, (snapshot, state).asRight.some)), balanceUpdate).some, tail).asLeft[Result].pure[F]
                                else
                                  current.asRight[Agg].pure[F]
                              ) { feeAddress =>
                                val localBalance = balanceUpdate.get(feeAddress)
                                val contextBalance = currentBalances.getOrElse(feeAddress, Balance.empty)
                                localBalance.getOrElse(contextBalance).pure[F].map { balance =>
                                  // We're inside the Some(feeAddress) handler, so isFeeRequired is always true here.
                                  // If fee deduction succeeds, continue processing; otherwise reject remaining binaries.
                                  (balance.minus(head.fee).toOption.map(uBalance => balanceUpdate + (feeAddress -> uBalance)) match {
                                    case Some(newBalanceUpdate) =>
                                      ((nel.prepend((head, (snapshot, state).asRight.some)), newBalanceUpdate).some, tail)
                                        .asLeft[Result]
                                    case None => // insufficient balance to cover fee — reject remaining binaries
                                      current.asRight[Agg]
                                  }): Either[Agg, Result]
                                }
                              }
                          }.handleErrorWith { e => // we don't accept neither binary nor incremental
                            logger.warn(e)(
                              s"Currency snapshot of ordinal ${snapshot.value.ordinal.show} for address ${address.show} couldn't be applied"
                            ) >> Async[F].pure(current.asRight)
                          }
                        case None => // again we only let it through if fee is not required
                          if (isFeeRequired)
                            Async[F].pure(current.asRight) // was: none.asRight but why clean it out rather than using current state?
                          else ((nel.prepend((head, lastCurrState)), balanceUpdate).some, tail).asLeft.pure[F]
                      }
                  }
              }
              .map(_.map { case (snaps, balances) => (snaps.reverse, balances) })
              .map { maybeProcessed =>
                initialState match {
                  case Some(_) => maybeProcessed.flatMap { case (nel, balances) => NonEmptyList.fromList(nel.tail).map((_, balances)) }
                  case None    => maybeProcessed
                }
              }
              .map(result => address -> result)
        }.map { results =>
          results.foldLeft(SortedMap.empty[Address, MetagraphAcceptanceResult]) {
            case (acc, (address, Some(result))) => acc + (address -> result)
            case (acc, (_, None))               => acc
          }
        }
      }

      def deriveMetagraphRoot(
        metagraphAddress: Address,
        binaries: NonEmptyList[Signed[StateChannelSnapshotBinary]],
        snapshotOrdinal: SnapshotOrdinal,
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[Hash] =
        // Re-run the SAME currency derivation gl0 uses, scoped to this single MG. Empty `priorLastCurrencySnapshots` (and empty
        // `currentBalances`) keeps the result a PURE function of `binaries` — the producer + every verifier compute byte-identical
        // roots regardless of their live MPT state (the S3 false-slashing crux; see the trait scaladoc).
        //
        // ORDER CONTRACT (2026-06-10): `processCurrencySnapshots` expects NEWEST-FIRST input (it reverses internally — the
        // legacy chain-link prepend-built convention). Checkpoint windows arrive OLDEST-FIRST (`chainLinkOrder` unfolds
        // anchor→tip), so reverse here. Producer and re-exec verifier share THIS function, so both flip together (the
        // byte-identity contract is preserved); without the reverse, multi-binary windows folded newest-first.
        processCurrencySnapshots(
          snapshotOrdinal,
          SortedMap.empty[Address, Balance],
          SortedMap.empty[Address, CurrencySnapshotWithState],
          SortedMap(metagraphAddress -> binaries.reverse),
          getGlobalSnapshotByOrdinal,
          // ADOPT mode, NOT Recreate (2026-06-10, run bebwls7ps): Recreate's `createContext` validation
          // performs global-snapshot lookups (globalSyncView checks) through
          // `GlobalSnapshotOpsManager.getGlobalSnapshotWithRetry` — ~31s of exponential-backoff retries
          // per miss. This derivation is wired with `noGlobalSnapshotLookup` (pure None BY DESIGN — the
          // split-safety contract forbids node-local global reads here), so every lookup paid the full
          // 31s to learn a statically-known answer. During the seeding wave the sub-quorum re-exec rail
          // runs INSIDE proposal validation, and gl0 production crawled to ~1 ordinal per 2-3 minutes
          // (the ord-33..40 stalls of runs 9-10). AdoptFromSignedFields replays the binary's own
          // accepted events with ZERO global lookups — the same derivation algebra the gl0 mirror uses —
          // and the producer + every re-exec verifier share THIS function, so both flip together and the
          // byte-identity contract holds (greenfield: roots change, deployed atomically).
          GlobalSnapshotStateChannelEventsProcessor.CurrencyAdoptionMode.AdoptFromSignedFields
        ).flatMap { accepted =>
          // Mirror `calculateLastCurrencySnapshots`: the LAST resulting state across the re-executed chain is what feeds
          // `lastCurrencySnapshots` → `buildMerkleTreeAndProofs`.
          val lastState: Option[CurrencySnapshotWithState] =
            accepted.get(metagraphAddress).flatMap { case (pairs, _) => pairs.toList.flatMap(_._2).lastOption }
          lastState match {
            // Canonical per-MG Merkle leaf — byte-identical to `GlobalSnapshotAcceptanceManager.buildMerkleTreeAndProofs`'s
            // `(address, state).hash` over the JsonHash logic (this is the leaf the gl0 metagraph-tree is built from).
            case Some(state) => hasher.hash((metagraphAddress, state))
            // No state derived (e.g. an incremental-only chain over an empty prior). Deterministic address-only sentinel so the
            // producer + verifier still agree on a value (the byte-identity contract holds — both reach this branch identically).
            case None => hasher.hash(metagraphAddress)
          }
        }.handleErrorWith { _ =>
          // A derivation crash (e.g. a malformed binary whose `content` fails brotli decompression — the `(None, head :: tail)` deserialize
          // path in `processCurrencySnapshots` does not catch that) maps to the SAME deterministic address-only sentinel. Keeping
          // `deriveMetagraphRoot` total + node-agnostic preserves the byte-identity contract (producer + every verifier crash identically
          // on the identical malformed input) rather than propagating a non-deterministic failure into the slot-leader / accept path.
          hasher.hash(metagraphAddress)
        }

      private def processStateChannelEvents(
        ordinal: SnapshotOrdinal,
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        events: List[StateChannelOutput]
      )(implicit hasher: Hasher[F]): F[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Set[StateChannelOutput])] =
        stateChannelManager.accept(ordinal, priorLastStateChannelSnapshotHashes, events)

    }

}
