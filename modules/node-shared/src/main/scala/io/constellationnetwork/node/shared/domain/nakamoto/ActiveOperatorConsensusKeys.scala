package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.kernel.Sync
import cats.syntax.all._

import scala.util.Try

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId

/** Fail-closed projection of one genesis operator KES+VRF registration at an artifact period.
  *
  * This compatibility helper deliberately accepts only the frozen genesis [[OperatorConsensusKeyRegistry]]. Runtime registrations are
  * rejected even when their embedded certificate is internally consistent: this API has no exact candidate-parent/N-2 context and therefore
  * cannot decide whether such a record was active on the artifact's branch. Runtime rotations must be resolved through
  * [[HistoricalOperatorConsensusKeyRegistry]]; callers must never substitute a receiver-current registry for that contract.
  */
object ActiveOperatorConsensusKeys {

  def resolve[F[_]: Sync](
    registry: OperatorConsensusKeyRegistry[F],
    peerId: PeerId,
    artifactPeriod: EtaPeriod
  ): F[Option[OperatorConsensusKeys]] =
    registry.get(peerId).map(_.filter(isValidAt(_, peerId, artifactPeriod)))

  def isValidAt(keys: OperatorConsensusKeys, peerId: PeerId, artifactPeriod: EtaPeriod): Boolean = {
    val isStructurallyValid =
      artifactPeriod.value >= 0L &&
        keys.operatorPeerId === peerId &&
        keys.kes.vk.value.length == 32 &&
        keys.kes.vk.step == 0 &&
        keys.kes.offset >= 0L &&
        keys.kes.offset == keys.effectiveFromPeriod.value &&
        keys.effectiveFromPeriod.value <= artifactPeriod.value &&
        keys.vrfPublicKey.toBytes.length == VrfPublicKey.ExpectedLength

    isStructurallyValid &&
    keys.registration.isEmpty &&
    keys.effectiveFromPeriod == EtaPeriod.Zero &&
    keys.kes.offset == 0L
  }

  def treeStep(keys: OperatorConsensusKeys, artifactPeriod: EtaPeriod): Option[Int] =
    Try(Math.subtractExact(artifactPeriod.value, keys.effectiveFromPeriod.value)).toOption.collect {
      case step if step >= 0L && step <= Int.MaxValue.toLong => step.toInt
    }
}
