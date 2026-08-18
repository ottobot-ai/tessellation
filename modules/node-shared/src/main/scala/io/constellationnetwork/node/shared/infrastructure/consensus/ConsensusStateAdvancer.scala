package io.constellationnetwork.node.shared.infrastructure.consensus

import cats.data.StateT
import cats.effect.{Async, Clock}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.DurationInt

import io.constellationnetwork.node.shared.config.types.ConsensusConfig
import io.constellationnetwork.schema.peer.PeerId

import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger

case class Previous[A](a: A)

trait ConsensusStateAdvancer[F[_], Key, Artifact, Context, Status, Outcome, Kind] {

  type State = ConsensusState[Key, Status, Outcome, Kind]
  type Resources = ConsensusResources[Artifact, Kind]

  def getConsensusOutcome(
    state: ConsensusState[Key, Status, Outcome, Kind]
  ): Option[(Previous[Key], Outcome)]

  def advanceStatus(resources: ConsensusResources[Artifact, Kind]): StateT[F, ConsensusState[Key, Status, Outcome, Kind], F[Unit]]

  def logger(implicit async: Async[F]): SelfAwareStructuredLogger[F] = Slf4jLogger.getLoggerFromName[F]("ConsensusStateAdvancer")

  protected def maybeGetAllDeclarations[A](
    state: State,
    resources: Resources,
    config: ConsensusConfig
  )(
    getter: PeerDeclarations => Option[A]
  )(implicit asyncF: Async[F]): F[Option[SortedMap[PeerId, A]]] = {
    val maybeAllDeclarations =
      state.facilitators.value.traverse { peerId =>
        resources.peerDeclarationsMap
          .get(peerId)
          .flatMap(getter)
          .map((peerId, _))
      }.map(SortedMap.from(_))

    for {
      now <- Clock[F].monotonic
      elapsed = now - resources.updatedAt
      isStale = elapsed > config.peersDeclarationTimeout
      _ <- logger
        .warn(
          s"Still waiting for declarations from all facilitators; consensus does not proceed with partial declarations. " +
            s"Elapsed since last declaration: ${elapsed.toSeconds}s, staleness threshold: ${config.peersDeclarationTimeout.toSeconds}s"
        )
        .whenA(isStale && maybeAllDeclarations.isEmpty)
    } yield maybeAllDeclarations
  }
}
