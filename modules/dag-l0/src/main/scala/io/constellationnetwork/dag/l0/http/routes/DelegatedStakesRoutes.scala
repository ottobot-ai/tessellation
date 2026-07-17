package io.constellationnetwork.dag.l0.http.routes

import cats.data.Validated.{Invalid, Valid}
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.dag.l0.domain.delegatedStake.{CreateDelegatedStakeOutput, DelegatedStakeOutput, WithdrawDelegatedStakeOutput}
import io.constellationnetwork.ext.http4s.AddressVar
import io.constellationnetwork.kernel._
import io.constellationnetwork.node.shared.domain.delegatedStake.UpdateDelegatedStakeValidator
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps._
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.delegatedStake.RewardsInfoStorage
import io.constellationnetwork.routes.internal._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.shapes._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.dsl.Http4sDsl
import org.typelevel.log4cats.slf4j.Slf4jLogger
import shapeless._
import shapeless.syntax.singleton._

final case class DelegatedStakesRoutes[F[_]: Async: Hasher](
  mkCell: DelegatedStakeOutput => Cell[F, StackF, _, Either[CellError, Ω], _],
  validator: UpdateDelegatedStakeValidator[F],
  snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
  nodeStorage: NodeStorage[F],
  withdrawalTimeLimit: EpochProgress,
  rewardsInfoStorage: RewardsInfoStorage[F],
  reader: GlobalStateReader[F]
) extends Http4sDsl[F]
    with PublicRoutes[F] {

  import org.http4s.circe.CirceEntityCodec.circeEntityDecoder

  private val logger = Slf4jLogger.getLoggerFromName[F]("DelegatedStakesLogger")

  protected val prefixPath: InternalUrlPrefix = "/delegated-stakes"

  private def getDelegatedStakesInfo(address: Address): F[DelegatedStakesInfo] =
    for {
      lastStakes <- reader.getDelegatedStakes(address).map(_.getOrElse(SortedSet.empty[DelegatedStakeRecord]))
      lastWithdrawals <- reader.getDelegatedStakeWithdrawals(address).map(_.getOrElse(SortedSet.empty[PendingDelegatedStakeWithdrawal]))
      stakes <- lastStakes.toList.traverse {
        case record: DelegatedStakeRecord =>
          DelegatedStakeReference
            .of(record.event)
            .map(ref => (record, ref))
      }
      active <- stakes.traverse {
        case (record, delegatedStakeRef) =>
          val totalAmountF = Async[F].fromEither(
            NonNegLong
              .from(record.rewards.value + record.amount.value)
              .leftMap(err => new IllegalArgumentException(s"Failed to create non-negative total: $err"))
              .map(Amount(_))
          )

          totalAmountF.map { total =>
            DelegatedStakeInfo(
              nodeId = record.event.nodeId,
              acceptedOrdinal = record.createdAt,
              tokenLockRef = record.tokenLockRef,
              amount = record.amount,
              fee = record.event.fee,
              hash = delegatedStakeRef.hash,
              withdrawalStartEpoch = None,
              withdrawalEndEpoch = None,
              rewardAmount = record.rewards,
              totalBalance = total
            )
          }
      }

      withdrawals <- lastWithdrawals.toList.traverse {
        case w: PendingDelegatedStakeWithdrawal =>
          DelegatedStakeReference
            .of(w.event)
            .map(ref => (w, ref))
      }
      pending <- withdrawals.traverse {
        case (record, delegatedStakeRef) =>
          val totalAmountF = Async[F].fromEither(
            NonNegLong
              .from(record.rewards.value + record.amount.value)
              .leftMap(err => new IllegalArgumentException(s"Failed to create non-negative total: $err"))
              .map(Amount(_))
          )

          totalAmountF.map { total =>
            DelegatedStakeInfo(
              nodeId = record.event.nodeId,
              acceptedOrdinal = record.acceptedOrdinal,
              tokenLockRef = record.tokenLockRef,
              amount = record.amount,
              fee = record.event.fee,
              hash = delegatedStakeRef.hash,
              withdrawalStartEpoch = record.createdAt.some,
              withdrawalEndEpoch = (record.createdAt |+| withdrawalTimeLimit).some,
              rewardAmount = record.rewards,
              totalBalance = total
            )
          }
      }
    } yield
      DelegatedStakesInfo(
        address = address,
        activeDelegatedStakes = active,
        pendingWithdrawals = pending
      )

  private def getLastReference(
    address: Address
  ): F[DelegatedStakeReference] =
    reader.getDelegatedStakes(address).flatMap { maybeDelegatedStakes =>
      maybeDelegatedStakes
        .getOrElse(SortedSet.empty[DelegatedStakeRecord])
        .toList
        .sortBy(_.event.ordinal)
        .lastOption
        .traverse(record => DelegatedStakeReference.of(record.event))
        .map(_.getOrElse(DelegatedStakeReference.empty))
    }

  protected val public: HttpRoutes[F] = HttpRoutes.of[F] {
    case req @ POST -> Root =>
      snapshotStorage.head.flatMap {
        case None => ServiceUnavailable()
        case Some(_) =>
          for {
            signed <- req.as[Signed[UpdateDelegatedStake.Create]]
            result <- validator.validateCreateDelegatedStake(signed, reader)
            response <- result match {
              case Valid(validSigned) =>
                logger.info(s"Accepted create delegated stake from ${validSigned.proofs.map(_.id).map(PeerId.fromId)}") >>
                  mkCell(CreateDelegatedStakeOutput(validSigned)).run().flatMap {
                    case Right(_) => validSigned.toHashed.flatMap(hashed => Ok(("hash" ->> hashed.hash) :: HNil))
                    case Left(_)  => InternalServerError("Failed to update cell.")
                  }

              case Invalid(errors) =>
                logger.warn(s"Invalid create delegated stake: $errors") >>
                  BadRequest(errors.mkString_("\n"))
            }
          } yield response
      }

    case req @ PUT -> Root =>
      snapshotStorage.head.flatMap {
        case None => ServiceUnavailable()
        case Some(_) =>
          for {
            signed <- req.as[Signed[UpdateDelegatedStake.Withdraw]]
            result <- validator.validateWithdrawDelegatedStake(signed, reader)
            response <- result match {
              case Valid(validSigned) =>
                logger.info(s"Accepted withdraw delegated stake from ${validSigned.proofs.map(_.id).map(PeerId.fromId)}") >>
                  mkCell(WithdrawDelegatedStakeOutput(validSigned)).run().flatMap {
                    case Right(_) =>
                      validSigned.toHashed.flatMap(hashed => Ok(("hash" ->> hashed.hash) :: HNil))
                    case Left(_) =>
                      InternalServerError("Failed to update cell.")
                  }

              case Invalid(errors) =>
                logger.warn(s"Invalid withdraw delegated stake: $errors") >>
                  BadRequest(errors.mkString_("\n"))
            }
          } yield response
      }

    case GET -> Root / AddressVar(address) / "info" =>
      snapshotStorage.head.flatMap {
        case Some(_) =>
          Ok(getDelegatedStakesInfo(address))
        case None => ServiceUnavailable()
      }

    case GET -> Root / "last-reference" / AddressVar(address) =>
      snapshotStorage.head.flatMap {
        case Some(_) =>
          Ok(getLastReference(address))
        case None => ServiceUnavailable()
      }

    case GET -> Root / "rewards-info" =>
      rewardsInfoStorage.getRewardsInfo.flatMap {
        case Some(rewardsInfo) => Ok(rewardsInfo)
        case None              => NotFound()
      }
  }
}
