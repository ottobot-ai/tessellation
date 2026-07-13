package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{ActiveOperatorConsensusKeys, EtaCalculation}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker

import eu.timepit.refined.auto._

/** Load-bearing KES verification for GL0 snapshots and validity attestations.
  *
  * Every public method accepts one already-resolved atomic KES+VRF pair. There is no KES-only registry lookup at this boundary. The pair
  * must be structurally valid and active at the artifact period; the artifact period and pair offset determine the only permissible tree
  * step. Empty, malformed, inactive, pending, or cryptographically invalid evidence fails closed.
  */
object KesGossipVerification {

  private final case class MetricKeys(
    noRegistryEntry: Metrics.MetricKey,
    invalid: Metrics.MetricKey,
    noSignature: Metrics.MetricKey,
    decodeFailed: Metrics.MetricKey,
    verified: Metrics.MetricKey
  )

  private val attestationMetrics = MetricKeys(
    noRegistryEntry = "dag_nakamoto_kes_attestations_no_registry_entry_total",
    invalid = "dag_nakamoto_kes_attestations_invalid_total",
    noSignature = "dag_nakamoto_kes_attestations_no_sig_total",
    decodeFailed = "dag_nakamoto_kes_attestations_decode_failed_total",
    verified = "dag_nakamoto_kes_attestations_verified_total"
  )

  private val metagraphAttestationMetrics = MetricKeys(
    noRegistryEntry = "dag_nakamoto_kes_mg_attestations_no_registry_entry_total",
    invalid = "dag_nakamoto_kes_mg_attestations_invalid_total",
    noSignature = "dag_nakamoto_kes_mg_attestations_no_sig_total",
    decodeFailed = "dag_nakamoto_kes_mg_attestations_decode_failed_total",
    verified = "dag_nakamoto_kes_mg_attestations_verified_total"
  )

  private val snapshotMetrics = MetricKeys(
    noRegistryEntry = "dag_nakamoto_kes_snapshots_no_registry_entry_total",
    invalid = "dag_nakamoto_kes_snapshots_invalid_total",
    noSignature = "dag_nakamoto_kes_snapshots_no_sig_total",
    decodeFailed = "dag_nakamoto_kes_snapshots_decode_failed_total",
    verified = "dag_nakamoto_kes_snapshots_verified_total"
  )

  def verifyAttestation[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    attesterId: peer.PeerId,
    attesterHex: Hex,
    tipOrdinal: Long,
    operatorKeys: OperatorConsensusKeys,
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] = {
    val artifactPeriod = EtaCalculation.globalSnapshotArtifactPeriod(tipOrdinal, etaRotationSnapshots)
    verifyForArtifact(
      messageBytes,
      kesSigBytes,
      attesterId,
      attesterHex,
      operatorKeys,
      artifactPeriod,
      wireStep = None,
      metricKeys = attestationMetrics,
      tag = "KES-ATT",
      detail = s"ord=$tipOrdinal",
      logger
    )
  }

  /** Verify metagraph-admission KES evidence. `kesStep` is wire evidence and must equal the step independently derived from the same active
    * atomic pair and exact-GL0-anchor artifact period used for VRF membership verification.
    */
  def verifyAttestationByStep[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    expectedOperatorId: peer.PeerId,
    operatorKeys: OperatorConsensusKeys,
    kesStep: Int,
    artifactPeriod: EtaPeriod,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] =
    verifyForArtifact(
      messageBytes,
      kesSigBytes,
      expectedOperatorId,
      expectedOperatorId.value,
      operatorKeys,
      artifactPeriod,
      wireStep = kesStep.some,
      metricKeys = metagraphAttestationMetrics,
      tag = "KES-MGATT",
      detail = s"period=${artifactPeriod.value}",
      logger
    )

  def verifySnapshot[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    producerId: peer.PeerId,
    producerHex: Hex,
    ordinal: Long,
    operatorKeys: OperatorConsensusKeys,
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] = {
    val artifactPeriod = EtaCalculation.globalSnapshotArtifactPeriod(ordinal, etaRotationSnapshots)
    verifyForArtifact(
      messageBytes,
      kesSigBytes,
      producerId,
      producerHex,
      operatorKeys,
      artifactPeriod,
      wireStep = None,
      metricKeys = snapshotMetrics,
      tag = "KES-SNAP",
      detail = s"ord=$ordinal",
      logger
    )
  }

  private def verifyForArtifact[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    peerId: peer.PeerId,
    peerHex: Hex,
    operatorKeys: OperatorConsensusKeys,
    artifactPeriod: EtaPeriod,
    wireStep: Option[Int],
    metricKeys: MetricKeys,
    tag: String,
    detail: String,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] =
    if (!ActiveOperatorConsensusKeys.isValidAt(operatorKeys, peerId, artifactPeriod))
      Metrics[F].incrementCounter(metricKeys.noRegistryEntry) >>
        logger
          .warn(
            s"$tag no valid active atomic operator-key pair for $detail period=${artifactPeriod.value} " +
              s"from=${peerHex.value.take(16)}...; rejecting"
          )
          .as(false)
    else
      ActiveOperatorConsensusKeys.treeStep(operatorKeys, artifactPeriod) match {
        case None =>
          Metrics[F].incrementCounter(metricKeys.invalid) >>
            logger.warn(s"$tag invalid registered period for $detail; rejecting").as(false)
        case Some(expectedStep) if wireStep.exists(_ != expectedStep) =>
          Metrics[F].incrementCounter(metricKeys.invalid) >>
            logger
              .warn(s"$tag wire step=${wireStep.get} != registered step=$expectedStep for $detail; rejecting")
              .as(false)
        case Some(_) if kesSigBytes.isEmpty =>
          Metrics[F].incrementCounter(metricKeys.noSignature) >>
            logger.warn(s"$tag missing signature for $detail from=${peerHex.value.take(16)}...; rejecting").as(false)
        case Some(expectedStep) =>
          OperationalKeyMaker.decodeSignature(kesSigBytes) match {
            case Left(error) =>
              Metrics[F].incrementCounter(metricKeys.decodeFailed) >>
                logger.warn(s"$tag decode failed for $detail: ${error.message}; rejecting").as(false)
            case Right(signature) =>
              val valid = OperationalKeyMaker.verify(signature, messageBytes, operatorKeys.kes.vk.copy(step = expectedStep))
              if (valid)
                Metrics[F].incrementCounter(metricKeys.verified) >>
                  logger
                    .info(
                      s"$tag verified $detail period=${artifactPeriod.value} step=$expectedStep " +
                        s"from=${peerHex.value.take(16)}..."
                    )
                    .as(true)
              else
                Metrics[F].incrementCounter(metricKeys.invalid) >>
                  logger.warn(s"$tag invalid signature for $detail step=$expectedStep; rejecting").as(false)
          }
      }
}
