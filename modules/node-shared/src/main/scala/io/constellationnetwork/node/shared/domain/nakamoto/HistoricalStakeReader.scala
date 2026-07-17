package io.constellationnetwork.node.shared.domain.nakamoto

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef, HistoricalStakeSnapshot}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.StatefulMerklePatriciaProducer
import io.constellationnetwork.security.{Hasher, HasherSelector}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{historicalImmutableCodec => historicalStakeSnapshotImmutable}

/** Staged exact-parent N-2 stake view for future GL0 leader eligibility.
  *
  * Both the historical period record and the bounded genesis-warmup aggregate are decoded from one immutable parent image. The overlay
  * proves requested/ancestor availability while holding its mutation mutex; this reader then reproduces both the supplied finalized-base
  * root and requested-parent root with the same consensus projection used by snapshot state proofs before interpreting any bytes.
  *
  * This capability is deliberately not wired into producer or follower eligibility yet. Activation requires crash-safe finality/base
  * journaling, authenticated restart/reorg reconstruction, a delayed canonical validator roster, cheap identity/signature gates before
  * full-state capture, and a bounded multi-parent cache.
  */
trait HistoricalStakeReader[F[_]] {
  def lookup(period: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[HistoricalStakeSnapshot]]
}

/** Dark exact-parent capability. This is intentionally a distinct type from the live ambient reader so it cannot be substituted into
  * `StakeRegistry` before the activation prerequisites above are implemented.
  */
trait ExactParentHistoricalStakeReader[F[_]] {
  def lookupExact(
    base: GlobalSnapshotStateRef,
    parent: GlobalSnapshotStateRef,
    period: EtaPeriod
  ): F[Either[ParentStateError, HistoricalStakeReader.ParentStakeView]]
}

object HistoricalStakeReader {

  sealed trait ParentStakeView extends Product with Serializable

  object ParentStakeView {
    final case class Historical(snapshot: HistoricalStakeSnapshot) extends ParentStakeView
    final case class GenesisWarmup(aggregate: Map[PeerId, BigInt]) extends ParentStakeView
  }

  def make[F[_]: Async](reader: GlobalStateReader[F]): HistoricalStakeReader[F] =
    new HistoricalStakeReader[F] {
      def lookup(period: EtaPeriod)(implicit hasher: Hasher[F]): F[Option[HistoricalStakeSnapshot]] = {
        val _ = historicalStakeSnapshotImmutable
        GlobalStateKey.historicalStakeSnapshotsKey[F](period).flatMap(reader.get[HistoricalStakeSnapshot])
      }
    }

  def exact[F[_]: Async: Parallel: JsonSerializer: HasherSelector](
    overlay: MptOverlay[F, GlobalStateKey],
    etaRotationSnapshots: Long
  ): ExactParentHistoricalStakeReader[F] =
    new ExactParentHistoricalStakeReader[F] {

      def lookupExact(
        base: GlobalSnapshotStateRef,
        parent: GlobalSnapshotStateRef,
        period: EtaPeriod
      ): F[Either[ParentStateError, ParentStakeView]] =
        overlay.captureExactParentState(base, parent).flatMap {
          case Left(error) => Async[F].pure(Left(error))
          case Right(capture) =>
            val baseBytes = capture.baseByteMap
            val parentBytes = capture.parentByteMap

            (
              HasherSelector[F].forOrdinal(base.ordinal) { implicit baseHasher =>
                GlobalSnapshotInfo.consensusMptRoot[F](baseBytes)
              },
              HasherSelector[F].forOrdinal(parent.ordinal) { implicit parentHasher =>
                if (base == parent) GlobalSnapshotInfo.consensusMptRoot[F](baseBytes)
                else GlobalSnapshotInfo.consensusMptRoot[F](parentBytes)
              }
            ).tupled.flatMap {
              case (observedBaseRoot, _) if observedBaseRoot =!= base.mptRoot.value =>
                Async[F].pure(Left(ParentStateError.ParentStateRootMismatch(base, observedBaseRoot)))
              case (_, observedParentRoot) if observedParentRoot =!= parent.mptRoot.value =>
                Async[F].pure(Left(ParentStateError.ParentStateRootMismatch(parent, observedParentRoot)))
              case _ =>
                readView(parentBytes, parent, period)
            }
        }

      private def readView(
        bytes: Map[Hex, Array[Byte]],
        parent: GlobalSnapshotStateRef,
        period: EtaPeriod
      ): F[Either[ParentStateError, ParentStakeView]] =
        if (period.value < 0L)
          HasherSelector[F].forOrdinal(parent.ordinal) { implicit parentHasher =>
            readWarmupAggregate(bytes).map(aggregate => Right(ParentStakeView.GenesisWarmup(aggregate)))
          }
        else
          historicalWriteOrdinal(period) match {
            case Left(error) => Async[F].pure(Left(error))
            case Right(writeOrdinal) if writeOrdinal.value.value > parent.ordinal.value.value =>
              Async[F].pure(Left(ParentStateError.HistoricalStakeUnavailable(parent, period)))
            case Right(writeOrdinal) =>
              HasherSelector[F].forOrdinal(writeOrdinal) { implicit writeHasher =>
                for {
                  historicalKey <- GlobalStateKey.historicalStakeSnapshotsKey[F](period)
                  historicalHex <- GlobalStateKey.toHex[F](historicalKey)
                  historical <- decodeHistorical(bytes.get(historicalHex), historicalHex)
                } yield
                  historical
                    .map(snapshot => ParentStakeView.Historical(snapshot): ParentStakeView)
                    .toRight(ParentStateError.HistoricalStakeUnavailable(parent, period))
              }
          }

      private def readWarmupAggregate(
        bytes: Map[Hex, Array[Byte]]
      )(implicit hasher: Hasher[F]): F[Map[PeerId, BigInt]] =
        for {
          delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveDelegatedStakes)
          collateralPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.ActiveNodeCollaterals)
          delegated <- StakeCollateralMptReader.materializeActiveDelegatedStakesFromRaw[F](
            rawEntriesForPrefix(bytes, delegatedPrefix)
          )
          collateral <- StakeCollateralMptReader.materializeActiveNodeCollateralsFromRaw[F](
            rawEntriesForPrefix(bytes, collateralPrefix)
          )
        } yield {
          val delegatedAggregate = delegated.valuesIterator
            .flatMap(_.iterator)
            .foldLeft(Map.empty[PeerId, BigInt]) { (acc, record) =>
              val peer = record.event.value.nodeId
              acc.updated(peer, acc.getOrElse(peer, BigInt(0)) + BigInt(record.amount.value.value))
            }

          val aggregate = collateral.valuesIterator
            .flatMap(_.iterator)
            .foldLeft(delegatedAggregate) { (acc, record) =>
              val peer = record.event.value.nodeId
              acc.updated(peer, acc.getOrElse(peer, BigInt(0)) + BigInt(record.event.value.amount.value.value))
            }

          aggregate
        }

      private def historicalWriteOrdinal(period: EtaPeriod): Either[ParentStateError, SnapshotOrdinal] =
        if (etaRotationSnapshots <= 0L)
          Left(ParentStateError.InvalidEtaRotationSnapshots(etaRotationSnapshots))
        else
          Either
            .catchOnly[ArithmeticException] {
              val periodExclusive = Math.addExact(period.value, 1L)
              SnapshotOrdinal.unsafeApply(Math.subtractExact(Math.multiplyExact(periodExclusive, etaRotationSnapshots), 1L))
            }
            .leftMap(_ => ParentStateError.HistoricalStakeWriteOrdinalOverflow(period, etaRotationSnapshots))

      private def decodeHistorical(
        bytes: Option[Array[Byte]],
        physicalKey: Hex
      ): F[Option[HistoricalStakeSnapshot]] = {
        val _ = historicalStakeSnapshotImmutable
        StrictMptRead.fromOptionalStoredBytes[HistoricalStakeSnapshot](bytes) match {
          case StrictMptRead.Absent => none[HistoricalStakeSnapshot].pure[F]
          case StrictMptRead.Present(value, _) =>
            value.some.pure[F]
          case StrictMptRead.Malformed(reason, _) =>
            Async[F].raiseError(
              StrictMptRead.MalformedConsensusMptValue("historical stake snapshot", physicalKey, reason)
            )
        }
      }

      private def rawEntriesForPrefix(
        bytes: Map[Hex, Array[Byte]],
        prefix: Hex
      ): List[StrictMptRawEntry] =
        StrictMptRead.captureRawEntries(
          bytes.filter { case (key, _) => StatefulMerklePatriciaProducer.hasNibblePrefix(key, prefix) }
        )
    }
}
