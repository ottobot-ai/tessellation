package io.constellationnetwork.dag.l0.domain.statechannel

import cats.data.NonEmptyList
import cats.data.Validated.{Invalid, Valid}
import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.domain.cell.{L0Cell, L0CellInput}
import io.constellationnetwork.node.shared.domain.statechannel.StateChannelValidator.{StateChannelValidationError, getFeeAddresses}
import io.constellationnetwork.node.shared.domain.statechannel.{SnapshotFeesInfo, StateChannelValidator}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.currencyMessage.{fetchOwnerAddress, fetchStakingAddress}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelOutput

import org.typelevel.log4cats.slf4j.Slf4jLogger

trait StateChannelService[F[_]] {
  def process(
    stateChannel: StateChannelOutput,
    globalSnapshotAndState: (Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)
  )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[StateChannelValidationError], Unit]]
}

object StateChannelService {

  def make[F[_]: Async](
    mkDagCell: L0Cell.Mk[F],
    stateChannelValidator: StateChannelValidator[F],
    mptStore: MptStore[F, GlobalStateKey]
  ): StateChannelService[F] =
    new StateChannelService[F] {
      private val logger = Slf4jLogger.getLoggerFromClass[F](StateChannelService.getClass)

      def process(
        stateChannelOutput: StateChannelOutput,
        globalSnapshotAndState: (Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)
      )(implicit hasher: Hasher[F]): F[Either[NonEmptyList[StateChannelValidationError], Unit]] = {
        val (snapshot, state) = globalSnapshotAndState
        // Note: getFeeAddresses still reads from state (GlobalSnapshotInfo) directly because it
        // iterates all lastCurrencySnapshots to collect fee addresses — a bulk operation not suited
        // for per-key MptStore lookups. The staking/owner balance lookups below use MptStore.
        val allFeesAddresses: Map[Address, Set[Address]] = getFeeAddresses(state)

        // Staking balance note: for full-snapshot-only metagraphs, getCurrencySnapshotInfo
        // returns CurrencySnapshotInfo with lastMessages = None (via toCurrencySnapshotInfo),
        // so fetchStakingAddress returns None → Balance.empty, matching old behavior.
        for {
          // [#113-DIAG] gl0-side: ml0 -> gl0 binary INGRESS via HTTP route. If we never see this log
          // for the metagraphAddr during the L0-token reverse polling window, ml0 isn't sending
          // (Hypothesis 1). If we see it but no later [#113-DIAG] process ENTRY for that addr, the
          // binary was rejected before reaching the consensus path.
          _ <- logger.info(
            s"[#113-DIAG] gl0 INGRESS metagraph=${stateChannelOutput.address.show.take(8)} " +
              s"lastHash=${stateChannelOutput.snapshotBinary.value.lastSnapshotHash.value.take(12)} " +
              s"snapshot.headOrd=${snapshot.ordinal.show}"
          )

          maybeCurrencyInfo <- mptStore.getCurrencySnapshotInfo(stateChannelOutput.address)

          stakingAddr = maybeCurrencyInfo.flatMap(fetchStakingAddress)
          ownerAddr = maybeCurrencyInfo.flatMap(fetchOwnerAddress)

          staked <- stakingAddr.fold(Balance.empty.pure[F]) { addr =>
            mptStore.getBalance(addr).map(_.getOrElse(Balance.empty))
          }

          snapshotFeesInfo = SnapshotFeesInfo(allFeesAddresses, staked, ownerAddr, stakingAddr)

          validations <- stateChannelValidator.validate(stateChannelOutput, snapshot.ordinal, snapshotFeesInfo)
          result <- validations match {
            case Valid(_) =>
              logger.info(
                s"[#113-DIAG] gl0 INGRESS VALID metagraph=${stateChannelOutput.address.show.take(8)} " +
                  s"-> enqueue to L0Cell"
              ) >>
                mkDagCell(L0CellInput.HandleStateChannelSnapshot(stateChannelOutput))
                  .run()
                  .as(().asRight[NonEmptyList[StateChannelValidationError]])
            case Invalid(errors) =>
              logger
                .warn(
                  s"[#113-DIAG] gl0 INGRESS INVALID metagraph=${stateChannelOutput.address.show.take(8)} " +
                    s"errors=${errors.toList}"
                )
                .as(errors.toNonEmptyList.asLeft[Unit])
          }
        } yield result
      }
    }
}
