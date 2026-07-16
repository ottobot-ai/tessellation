package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data._
import cats.effect.Async
import cats.syntax.all._
import cats.{Monad, Parallel}

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{CurrencyInfoMptAdapters, GlobalStateReader}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelValidator.{StateChannelValidationError, getFeeAddresses}
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage._
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId, GlobalStateKey}
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
import org.typelevel.log4cats.slf4j.Slf4jLogger

trait GlobalSnapshotStateChannelEventsProcessor[F[_]] {
  type BinaryCurrencyPair = GlobalSnapshotStateChannelEventsProcessor.BinaryCurrencyPair
  type BalanceUpdate = GlobalSnapshotStateChannelEventsProcessor.BalanceUpdate
  type MetagraphAcceptanceResult = GlobalSnapshotStateChannelEventsProcessor.MetagraphAcceptanceResult

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
    * SC-tip setter reads). Direct callers holding oldest-first windows MUST reverse before calling. Checkpoint callers instead use
    * [[processCurrencySnapshotsWithCompleteConsumption]], which owns that conversion and verifies exact complete consumption. Feeding
    * oldest-first directly silently breaks multi-binary windows: the genesis-decode branch sees the newest incremental as "head" and the
    * state fold runs in reverse (the 2026-06-10 seeding failure).
    */
  def processCurrencySnapshots(
    snapshotOrdinal: SnapshotOrdinal,
    currentBalances: SortedMap[Address, Balance],
    priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
    events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Address, MetagraphAcceptanceResult]]

  /** Checkpoint replay boundary over the exact canonical OLDEST-FIRST signed-binary windows.
    *
    * [[processCurrencySnapshots]] deliberately retains the upstream-v4 partial-acceptance behavior: when a later binary cannot be decoded,
    * recreated, authorized, or charged, it may return the valid prefix. That behavior is useful to ordinary non-checkpoint callers, but a
    * shard execution signature covers the complete checkpoint window. Consequently, a prefix result must never be mistaken for complete
    * execution merely because its last recreated state has a well-formed root.
    *
    * This method preserves the legacy transition function and adds a structured consumption disposition. It reverses each canonical
    * checkpoint window for [[processCurrencySnapshots]]'s NEWEST-FIRST input contract, then hashes and compares the returned OLDEST-FIRST
    * `Signed[StateChannelSnapshotBinary]` sequence against every exact original signed input. Hashing the complete `Signed` envelope binds
    * content, parent, fee, and proofs; a same-length substitution is incomplete just like a missing suffix.
    */
  final def processCurrencySnapshotsWithCompleteConsumption(
    snapshotOrdinal: SnapshotOrdinal,
    currentBalances: SortedMap[Address, Balance],
    priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
      CurrencySnapshot
    ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
    expectedParentHashes: SortedMap[Address, Hash],
    orderedEvents: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  )(
    implicit hasher: Hasher[F],
    F: Monad[F]
  ): F[GlobalSnapshotStateChannelEventsProcessor.CurrencySnapshotProcessingResult] =
    GlobalSnapshotStateChannelEventsProcessor
      .classifyCheckpointLineage(expectedParentHashes, orderedEvents)
      .flatMap { invalidLineage =>
        val replayableEvents = orderedEvents.filterNot { case (address, _) => invalidLineage.contains(address) }
        val processedF =
          if (replayableEvents.isEmpty) SortedMap.empty[Address, MetagraphAcceptanceResult].pure[F]
          else
            processCurrencySnapshots(
              snapshotOrdinal,
              currentBalances,
              priorLastCurrencySnapshots,
              replayableEvents.map { case (address, binaries) => address -> binaries.reverse },
              getGlobalSnapshotByOrdinal
            )

        processedF
          .flatMap(GlobalSnapshotStateChannelEventsProcessor.classifyCheckpointConsumption(replayableEvents, _))
          .map(_.withConsumptionOverrides(invalidLineage))
      }

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

}

object GlobalSnapshotStateChannelEventsProcessor {

  type BinaryCurrencyPair = (Signed[StateChannelSnapshotBinary], Option[CurrencySnapshotWithState])
  type BalanceUpdate = SortedMap[Address, Balance]
  type MetagraphAcceptanceResult = (NonEmptyList[BinaryCurrencyPair], BalanceUpdate)

  sealed trait CurrencyWindowConsumption extends Product with Serializable {
    final def isComplete: Boolean = this match {
      case CurrencyWindowConsumption.Complete => true
      case _                                  => false
    }
  }

  object CurrencyWindowConsumption {
    case object Complete extends CurrencyWindowConsumption
    final case class Incomplete(expectedInputs: Int, processedInputs: Int) extends CurrencyWindowConsumption
    case object MissingExpectedParent extends CurrencyWindowConsumption
    final case class InvalidOuterParentLink(inputIndex: Int, expectedParent: Hash, actualParent: Hash) extends CurrencyWindowConsumption
    case object UnexpectedOutput extends CurrencyWindowConsumption
  }

  final class CurrencySnapshotProcessingResult private[global] (
    private val processed: SortedMap[Address, MetagraphAcceptanceResult],
    private val windowConsumption: SortedMap[Address, CurrencyWindowConsumption]
  ) {
    def consumption(address: Address): Option[CurrencyWindowConsumption] = windowConsumption.get(address)

    def completelyConsumed(address: Address): Boolean =
      consumption(address).exists(_.isComplete)

    def allInputsCompletelyConsumed: Boolean =
      windowConsumption.nonEmpty && windowConsumption.values.forall(_.isComplete)

    /** The only checkpoint-validity accessor: no per-MG replay result is exposed unless every exact ordered Signed input was consumed. */
    def completeResult(address: Address): Option[MetagraphAcceptanceResult] =
      Option.when(completelyConsumed(address))(()).flatMap(_ => processed.get(address))

    /** All and only exact, completely consumed checkpoint windows. Unexpected or incomplete processor output is excluded. */
    def completeResults: SortedMap[Address, MetagraphAcceptanceResult] =
      processed.filter { case (address, _) => completelyConsumed(address) }

    private[global] def withConsumptionOverrides(
      overrides: SortedMap[Address, CurrencyWindowConsumption]
    ): CurrencySnapshotProcessingResult =
      new CurrencySnapshotProcessingResult(processed -- overrides.keySet, windowConsumption ++ overrides)
  }

  private[global] object CurrencySnapshotProcessingResult {
    def apply(
      processed: SortedMap[Address, MetagraphAcceptanceResult],
      windowConsumption: SortedMap[Address, CurrencyWindowConsumption]
    ): CurrencySnapshotProcessingResult =
      new CurrencySnapshotProcessingResult(processed, windowConsumption)
  }

  /** Compare returned OLDEST-FIRST signed envelopes with the exact checkpoint inputs. Package-visible for focused substitution/reordering
    * tests; checkpoint callers consume only [[CurrencySnapshotProcessingResult.completeResult]].
    */
  private[global] def classifyCheckpointConsumption[F[_]: Monad: Hasher](
    orderedEvents: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    processed: SortedMap[Address, MetagraphAcceptanceResult]
  ): F[CurrencySnapshotProcessingResult] = {
    import CurrencyWindowConsumption._

    orderedEvents.toList.traverse {
      case (address, expected) =>
        val actual = processed.get(address).map { case (pairs, _) => pairs.map(_._1) }
        val expectedHashesF = expected.toList.traverse(Hasher[F].hash(_))
        val actualHashesF = actual.traverse(_.toList.traverse(Hasher[F].hash(_)))

        (expectedHashesF, actualHashesF).mapN {
          case (expectedHashes, Some(actualHashes)) if expectedHashes === actualHashes =>
            address -> Complete
          case (_, actualHashes) =>
            address -> Incomplete(expected.size, actualHashes.fold(0)(_.size))
        }
    }.map { expectedDispositions =>
      val unexpectedDispositions = (processed.keySet -- orderedEvents.keySet).toList.map { address =>
        address -> UnexpectedOutput
      }

      CurrencySnapshotProcessingResult(processed, SortedMap.from(expectedDispositions ++ unexpectedDispositions))
    }
  }

  /** Validate the outer state-channel lineage before invoking currency recreation. The parent of the first binary is the exact tip from the
    * checkpoint's pinned execution-base reader; every later parent is the canonical VALUE hash of the preceding binary. The enclosing
    * `Signed` hash remains the complete-consumption identity and is deliberately not used as the state-channel parent hash.
    */
  private[global] def classifyCheckpointLineage[F[_]: Monad: Hasher](
    expectedParentHashes: SortedMap[Address, Hash],
    orderedEvents: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): F[SortedMap[Address, CurrencyWindowConsumption]] = {
    import CurrencyWindowConsumption._

    def firstMismatch(
      binaries: List[Signed[StateChannelSnapshotBinary]],
      expectedParent: Hash,
      inputIndex: Int
    ): F[Option[InvalidOuterParentLink]] =
      binaries match {
        case Nil => none[InvalidOuterParentLink].pure[F]
        case binary :: tail =>
          val actualParent = binary.value.lastSnapshotHash
          if (actualParent =!= expectedParent)
            InvalidOuterParentLink(inputIndex, expectedParent, actualParent).some.pure[F]
          else
            Hasher[F]
              .hash(binary.value)
              .flatMap(nextExpected => firstMismatch(tail, nextExpected, inputIndex + 1))
      }

    orderedEvents.toList.traverse {
      case (address, binaries) =>
        expectedParentHashes.get(address) match {
          case None => (address -> (MissingExpectedParent: CurrencyWindowConsumption)).some.pure[F]
          case Some(expectedParent) =>
            firstMismatch(binaries.toList, expectedParent, 0)
              .map(_.map(mismatch => address -> (mismatch: CurrencyWindowConsumption)))
        }
    }
      .map(dispositions => SortedMap.from(dispositions.flatten))
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
        // processCurrencySnapshots threads fee-payer balances in this same canonical metagraph order. Each per-MG map is therefore a
        // cumulative absolute update, and the right-biased merge retains the latest checked debit when metagraphs share a fee payer.
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

      /** Processes currency snapshots for each metagraph address, applying fee deduction logic.
        *
        * Fee deduction follows three cases per binary:
        *   1. Fee not required (pre-fee-ordinal or fee waived): accept the binary unconditionally. 2. Fee required but no fee address
        *      (owner address missing from currency messages): reject the binary — we cannot deduct fees without a destination address. 3.
        *      Fee required with fee address: look up the metagraph owner's balance first in the local accumulator (tracks balance changes
        *      within this metagraph window), then fall back to the canonically threaded global balance accumulator. If the balance covers
        *      the fee, deduct it and accept; otherwise reject remaining binaries.
        */
      def processCurrencySnapshots(
        snapshotOrdinal: SnapshotOrdinal,
        currentBalances: SortedMap[Address, Balance],
        priorLastCurrencySnapshots: SortedMap[Address, Either[Signed[
          CurrencySnapshot
        ], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]],
        events: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[SortedMap[Address, MetagraphAcceptanceResult]] = {
        // There is only one economic transition function: recreate the signed artifact from its events and pinned global inputs.
        def deriveNextCurrencyInfo(
          address: Address,
          lastState: CurrencySnapshotInfo,
          lastIncremental: Signed[CurrencyIncrementalSnapshot],
          snapshot: Signed[CurrencyIncrementalSnapshot]
        ): F[CurrencySnapshotInfo] =
          applyCurrencySnapshot(address, lastState, lastIncremental, snapshot, getGlobalSnapshotByOrdinal)

        val isFeeRequired = feeCalculator.isFeeRequired(snapshotOrdinal)
        val allFeesAddresses = StateChannelValidator.getFeeAddresses(priorLastCurrencySnapshots)

        def validateForGlobalExecution(
          address: Address,
          binary: Signed[StateChannelSnapshotBinary],
          availableFeeAddresses: Map[Address, Set[Address]]
        ): F[StateChannelValidator.StateChannelValidationErrorOr[StateChannelOutput]] = {
          val output = StateChannelOutput(address, binary)
          if (binary.value.lastSnapshotHash === Hash.empty)
            stateChannelValidator.validate(output, snapshotOrdinal, SnapshotFeesInfo.empty)
          else
            deserialize[Signed[CurrencyIncrementalSnapshot]](binary).flatMap {
              case None => stateChannelValidator.validate(output, snapshotOrdinal, SnapshotFeesInfo.empty)
              case Some(snapshot) =>
                val priorInfo = priorLastCurrencySnapshots.get(address).collect { case Right((_, info)) => info }
                val stakingBalance = priorInfo
                  .flatMap(fetchStakingAddress)
                  .flatMap(currentBalances.get)
                  .getOrElse(Balance.empty)
                val messagesDesc = snapshot.value.messages.map(_.toList.sortBy(-_.ordinal.value.value))
                val ownerAddress = messagesDesc.flatMap(_.find(_.messageType === MessageType.Owner)).map(_.address)
                val stakingAddress = messagesDesc.flatMap(_.find(_.messageType === MessageType.Staking)).map(_.address)
                stateChannelValidator.validate(
                  output,
                  snapshotOrdinal,
                  SnapshotFeesInfo(availableFeeAddresses, stakingBalance, ownerAddress, stakingAddress)
                )
            }
        }

        type Processed = SortedMap[Address, MetagraphAcceptanceResult]
        type GlobalFeeAccumulator = (SortedMap[Address, Balance], Map[Address, Set[Address]], Processed)

        // `events` is a SortedMap, so this fold is the consensus order for cross-metagraph access to a shared fee payer. Parallel replay
        // from one prior balance lets two windows both debit 100 -> 90 and makes the final right-biased map waive one fee. Publish each
        // accepted window's checked absolute updates before evaluating the next metagraph instead.
        events.toList
          .foldLeftM[F, GlobalFeeAccumulator]((currentBalances, allFeesAddresses, SortedMap.empty)) {
            case ((availableBalances, availableFeeAddresses, processed), (address, binaries)) =>
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

              binaries.toList
                .traverse(binary => validateForGlobalExecution(address, binary, availableFeeAddresses))
                .flatMap { authorizations =>
                  if (authorizations.forall(_.isValid))
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
                              deserialize[Signed[CurrencyIncrementalSnapshot]](head).flatMap {
                                case Some(_) =>
                                  // A decodable CL1 incremental needs a globally recreated prior. Advancing its state-channel tip without one
                                  // would make the economic transition permanently unexecutable.
                                  logger.error(
                                    s"Currency recreation requires a full genesis snapshot: mg=${address.show}; dropping incremental-only window"
                                  ) >> Async[F].pure(none.asRight[Agg])
                                case None =>
                                  // Opaque DL1 state is authenticated carriage, not CL1 state. Preserve the binary/tip before the fee cutover,
                                  // but attach no currency state and therefore no balance, supply, active-set, or reference-map effect.
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
                              deserialize[Signed[CurrencySnapshot]](head).flatMap {
                                case Some(snapshot) =>
                                  ((nel.prepend((head, snapshot.asLeft.some)), balanceUpdate).some, tail).asLeft[Result].pure[F]
                                case None =>
                                  deserialize[Signed[CurrencyIncrementalSnapshot]](head).map {
                                    case Some(_)                => current.asRight[Agg]
                                    case None if !isFeeRequired => ((nel.prepend((head, none)), balanceUpdate).some, tail).asLeft[Result]
                                    case None                   => current.asRight[Agg]
                                  }
                              }

                            case (_, Some(Left(fullSnapshot))) =>
                              deserialize[Signed[CurrencyIncrementalSnapshot]](head).flatMap {
                                case Some(snapshot) => // first incremental - recreate it, but don't subtract a state-channel fee
                                  implicit val selector: io.constellationnetwork.schema.StateProofSelector =
                                    CurrencyStateProofSelector.instance
                                  CurrencyIncrementalSnapshot
                                    .fromCurrencySnapshot[F](fullSnapshot.value)
                                    .flatMap { previousValue =>
                                      val previous = Signed(previousValue, fullSnapshot.proofs)
                                      deriveNextCurrencyInfo(
                                        address,
                                        fullSnapshot.value.info.toCurrencySnapshotInfo,
                                        previous,
                                        snapshot
                                      ).map { state =>
                                        (
                                          (nel.prepend((head, (snapshot, state).asRight.some)), balanceUpdate).some,
                                          tail
                                        ).asLeft[Result]
                                      }
                                    }
                                    .handleErrorWith { error =>
                                      logger.warn(error)(
                                        s"First currency incremental for address ${address.show} failed full recreation"
                                      ) >> current.asRight[Agg].pure[F]
                                    }
                                case None => current.asRight[Agg].pure[F]
                              }

                            case (_, Some(Right((lastIncremental, lastState)))) =>
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
                                    // accumulator first (to account for fees already deducted earlier in this metagraph window),
                                    // falling back to `availableBalances`, which includes prior metagraph windows in canonical order.
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
                                          ((nel.prepend((head, (snapshot, state).asRight.some)), balanceUpdate).some, tail)
                                            .asLeft[Result]
                                            .pure[F]
                                        else
                                          current.asRight[Agg].pure[F]
                                      ) { feeAddress =>
                                        val localBalance = balanceUpdate.get(feeAddress)
                                        val contextBalance = availableBalances.getOrElse(feeAddress, Balance.empty)
                                        localBalance.getOrElse(contextBalance).pure[F].map { balance =>
                                          // We're inside the Some(feeAddress) handler, so isFeeRequired is always true here.
                                          // If fee deduction succeeds, continue processing; otherwise reject remaining binaries.
                                          (balance
                                            .minus(head.fee)
                                            .toOption
                                            .map(uBalance => balanceUpdate + (feeAddress -> uBalance)) match {
                                            case Some(newBalanceUpdate) =>
                                              ((nel.prepend((head, (snapshot, state).asRight.some)), newBalanceUpdate).some, tail)
                                                .asLeft[Result]
                                            case None => // insufficient balance to cover fee — reject remaining binaries
                                              current.asRight[Agg]
                                          }): Either[Agg, Result]
                                        }
                                      }
                                  }.handleErrorWith { error => // we don't accept neither binary nor incremental
                                    logger.warn(error)(
                                      s"Currency snapshot of ordinal ${snapshot.value.ordinal.show} for address ${address.show} couldn't be applied"
                                    ) >> Async[F].pure(current.asRight)
                                  }
                                case None => current.asRight[Agg].pure[F]
                              }
                          }
                      }
                      .map(_.map { case (snaps, balances) => (snaps.reverse, balances) })
                      .map { maybeProcessed =>
                        initialState match {
                          case Some(_) =>
                            maybeProcessed.flatMap { case (nel, balances) => NonEmptyList.fromList(nel.tail).map((_, balances)) }
                          case None => maybeProcessed
                        }
                      }
                      .map {
                        case Some(result @ (_, balanceUpdate)) =>
                          val acceptedFeeAddresses = result._1.toList
                            .flatMap(_._2.toList)
                            .flatMap {
                              case Left(_)          => List.empty[Address]
                              case Right((_, info)) => info.lastMessages.toList.flatMap(_.values.map(_.address))
                            }
                            .toSet
                          val nextFeeAddresses =
                            if (acceptedFeeAddresses.isEmpty) availableFeeAddresses
                            else
                              availableFeeAddresses.updatedWith(address) { existing =>
                                (existing.getOrElse(Set.empty) ++ acceptedFeeAddresses).some
                              }
                          (availableBalances ++ balanceUpdate, nextFeeAddresses, processed + (address -> result))
                        case None =>
                          (availableBalances, availableFeeAddresses, processed)
                      }
                  else
                    logger
                      .warn(s"Rejected unauthenticated state-channel window for currency address=${address.show}")
                      .as((availableBalances, availableFeeAddresses, processed))
                }
          }
          .map(_._3)
      }

      private def processStateChannelEvents(
        ordinal: SnapshotOrdinal,
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        events: List[StateChannelOutput]
      )(implicit hasher: Hasher[F]): F[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Set[StateChannelOutput])] =
        stateChannelManager.accept(ordinal, priorLastStateChannelSnapshotHashes, events)

    }

}
