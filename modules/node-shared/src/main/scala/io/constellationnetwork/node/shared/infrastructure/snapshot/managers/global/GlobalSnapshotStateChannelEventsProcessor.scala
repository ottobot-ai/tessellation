package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Parallel
import cats.data._
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.syntax.validated.validatedSyntax
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelAcceptanceResult.CurrencySnapshotWithState
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelValidator.{StateChannelValidationError, getFeeAddresses}
import io.constellationnetwork.node.shared.domain.statechannel._
import io.constellationnetwork.node.shared.infrastructure.snapshot.CurrencySnapshotContextFunctions
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher}
import io.constellationnetwork.serde.codecs.instances.CurrencySnapshotInfoCodecs.currencySnapshotInfoImmutableCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary, StateChannelValidationType}

import io.circe.Decoder
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
}

object GlobalSnapshotStateChannelEventsProcessor {
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
      ): F[SnapshotFeesInfo] =
        event.snapshotBinary.value.lastSnapshotHash match {
          case hash if hash == Hash.empty => SnapshotFeesInfo.empty.pure // genesis
          case _ =>
            deserialize[Signed[CurrencyIncrementalSnapshot]](event.snapshotBinary).flatMap {
              case None =>
                logger.warn(s"Could not get snapshot fee info after deserializing event $event, using empty snapshot fees") >>
                  SnapshotFeesInfo.empty.pure
              case Some(snapshot) =>
                for {
                  maybeCurrencyInfo <- reader.get[CurrencySnapshotInfo](
                    GlobalStateKey.metagraph(event.address, GlobalStateFieldId.LastCurrencySnapshotInfo)
                  )
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
                val (lastCurrencyStates, incomingCurrencyState) = calculateLastCurrencySnapshots(accepted, priorLastCurrencySnapshots)
                val finalScSnapshots = accepted.map { case (k, (v, _)) => k -> v.map(_._1) }
                // TODO: ASSUMING that owner addresses are restricted from being shared at this point
                val balanceUpdates = accepted.values.map(_._2).foldLeft(SortedMap.empty[Address, Balance])(_ ++ _)

                StateChannelAcceptanceResult(
                  finalScSnapshots,
                  lastCurrencyStates,
                  returnedSCEvents,
                  balanceUpdates,
                  incomingCurrencyState
                )
              }
          }
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
        getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
      )(implicit hasher: Hasher[F]): F[SortedMap[Address, MetagraphAcceptanceResult]] = {
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
                  deserialize[Signed[CurrencySnapshot]](head).map {
                    case Some(snapshot) => // full snapshot - we don't subtract fee
                      (
                        (NonEmptyList.one((head, snapshot.asLeft.some)), emptyBalanceUpdate).some,
                        tail
                      ).asLeft
                    case None => // no full snapshot yet - we only accept the binary if fee is not required
                      if (isFeeRequired) none.asRight
                      else ((NonEmptyList.one((head, none)), emptyBalanceUpdate).some, tail).asLeft
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
                          applyCurrencySnapshot(
                            address,
                            lastState,
                            lastIncremental,
                            snapshot,
                            getGlobalSnapshotByOrdinal
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

      private def processStateChannelEvents(
        ordinal: SnapshotOrdinal,
        priorLastStateChannelSnapshotHashes: SortedMap[Address, Hash],
        events: List[StateChannelOutput]
      )(implicit hasher: Hasher[F]): F[(SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]], Set[StateChannelOutput])] =
        stateChannelManager.accept(ordinal, priorLastStateChannelSnapshotHashes, events)

    }

}
