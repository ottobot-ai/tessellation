package io.constellationnetwork.node.shared.infrastructure.sharding

import cats.Applicative

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointProducerDutyValidator
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardCheckpoint

/** Test-only bypass for suites exercising manager predicates other than producer duty. Production wiring has no bypass implementation. */
object TestCheckpointDutyValidator {
  def allow[F[_]: Applicative]: ShardCheckpointProducerDutyValidator[F] =
    new ShardCheckpointProducerDutyValidator[F] {
      def validate(checkpoint: ShardCheckpoint, committee: Set[PeerId]): F[Either[String, Unit]] = {
        val _ = (checkpoint, committee)
        Applicative[F].pure(Right(()))
      }
    }
}
