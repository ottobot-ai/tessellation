package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{EtaCalculation, KesRegistry}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker

import eu.timepit.refined.auto._

/** §1.2 Slice 5/6 — warn-only KES verification of incoming attestations + snapshots.
  *
  * Pulled out of [[NakamotoSyncDaemon]] so the verification logic + counter taxonomy live in one
  * place and are independently unit-testable. The daemon retains responsibility for routing (call
  * the right helper after the Ed25519 path succeeds); this object owns the matrix:
  *
  *   - empty wire field  → `*_no_sig_total`            (no log)
  *   - decode failure    → `*_decode_failed_total`     (WARN)
  *   - sig + no registry → `*_no_registry_entry_total` (DEBUG)
  *   - sig + verify OK   → `*_verified_total`          (INFO)
  *   - sig + verify fail → `*_invalid_total`           (WARN — do NOT reject)
  *
  * '''Warn-only'''. This slice does not reject anything on KES failure — Ed25519 remains the
  * load-bearing path until Slice 9 flips KES to be authoritative.
  */
private[nakamoto] object KesGossipVerification {

  /** Verify a KES signature attached to an attestation. `messageBytes` is the Ed25519-signed
    * attestation-hash bytes (the same bytes the Ed25519 path verified). Period is recomputed
    * from `tipOrdinal` for log context only — the actual KES verify reconstructs the period
    * from the secret-key tree's internal step.
    */
  def verifyAttestation[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    attesterId: peer.PeerId,
    attesterHex: Hex,
    tipOrdinal: Long,
    kesRegistry: KesRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val tag = "KES-ATT"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_sig_total")
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_decode_failed_total") >>
            logger.warn(
              s"⚠️ $tag decode failed for ord=$tipOrdinal from=${attesterHex.value.take(16)}...: ${err.message}"
            )
        case Right(kSig) =>
          kesRegistry.getKesVk(attesterId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_registry_entry_total") >>
                logger.debug(
                  s"$tag no registry entry for ord=$tipOrdinal from=${attesterHex.value.take(16)}... — skipping verify"
                )
            case Some(vk) =>
              val ok = OperationalKeyMaker.verify(kSig, messageBytes, vk)
              val kesPeriod = EtaCalculation.rotationPeriod(tipOrdinal, etaRotationSnapshots)
              if (ok)
                Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_verified_total") >>
                  logger.info(
                    s"🔐 $tag verified ord=$tipOrdinal period=$kesPeriod from=${attesterHex.value.take(16)}..."
                  )
              else
                Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_invalid_total") >>
                  logger.warn(
                    s"⚠️ $tag invalid ord=$tipOrdinal period=$kesPeriod from=${attesterHex.value.take(16)}... — warn-only, not rejecting"
                  )
          }
      }
    }
  }

  /** Verify a KES signature attached to a snapshot. `messageBytes` is the snapshot-hash bytes
    * (the same bytes the producer signed on the sender side).
    */
  def verifySnapshot[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    producerId: peer.PeerId,
    producerHex: Hex,
    ordinal: Long,
    kesRegistry: KesRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    val tag = "KES-SNAP"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_sig_total")
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_decode_failed_total") >>
            logger.warn(
              s"⚠️ $tag decode failed for ord=$ordinal from=${producerHex.value.take(16)}...: ${err.message}"
            )
        case Right(kSig) =>
          kesRegistry.getKesVk(producerId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_registry_entry_total") >>
                logger.debug(
                  s"$tag no registry entry for ord=$ordinal from=${producerHex.value.take(16)}... — skipping verify"
                )
            case Some(vk) =>
              val ok = OperationalKeyMaker.verify(kSig, messageBytes, vk)
              val kesPeriod = EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots)
              if (ok)
                Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_verified_total") >>
                  logger.info(
                    s"🔐 $tag verified ord=$ordinal period=$kesPeriod from=${producerHex.value.take(16)}..."
                  )
              else
                Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_invalid_total") >>
                  logger.warn(
                    s"⚠️ $tag invalid ord=$ordinal period=$kesPeriod from=${producerHex.value.take(16)}... — warn-only, not rejecting"
                  )
          }
      }
    }
  }
}
