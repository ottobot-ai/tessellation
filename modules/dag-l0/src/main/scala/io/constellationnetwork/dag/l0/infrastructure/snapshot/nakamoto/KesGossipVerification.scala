package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{EtaCalculation, KesRegistry}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker

import eu.timepit.refined.auto._

/** §1.2 — KES verification of incoming attestations + snapshots, load-bearing.
  *
  * Pulled out of [[NakamotoSyncDaemon]] so the verification logic + counter taxonomy live in one place and are independently unit-testable.
  * Slice 9 made KES authoritative (no warn-only escape hatch); the daemon drops any message that returns `false`. Behavior matrix:
  *
  *   - empty wire field → `*_no_sig_total` + WARN, return false (reject)
  *   - decode failure → `*_decode_failed_total` + WARN, return false
  *   - sig + no registry → `*_no_registry_entry_total` + DEBUG, return true (Ed25519 already authenticated; carve-out for Slice 10 runtime
  *     registration where a newly-joined operator's reg-cert tx may not yet have finalized when their first sigs arrive)
  *   - sig + verify OK → `*_verified_total` + INFO, return true
  *   - sig + verify fail → `*_invalid_total` + WARN, return false
  */
// Slice S3: the access modifier was relaxed from `private[nakamoto]` so the
// `MetagraphCommitteeGate` adapter constructed in `GlobalSnapshotConsensus` can
// reuse this verify path. The KES accept/reject matrix (no-sig → reject, decode-fail →
// reject, no-registry → accept, verify-fail → reject) is unchanged.
object KesGossipVerification {

  /** Verify a KES signature attached to an attestation. `messageBytes` is the Ed25519-signed attestation-hash bytes (the same bytes the
    * Ed25519 path verified). The KES step is rebound to `globalPeriod - operator.offset` so operators registered mid-life (Slice 10) sign
    * relative to their own tree's offset rather than global eta period zero.
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
  ): F[Boolean] = {
    val tag = "KES-ATT"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_sig_total") >>
        logger.warn(s"⚠️ $tag missing sig — rejecting ord=$tipOrdinal from=${attesterHex.value.take(16)}...").as(false)
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_decode_failed_total") >>
            logger
              .warn(s"⚠️ $tag decode failed for ord=$tipOrdinal from=${attesterHex.value.take(16)}...: ${err.message} — rejecting")
              .as(false)
        case Right(kSig) =>
          kesRegistry.getKesVk(attesterId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_registry_entry_total") >>
                logger
                  .debug(
                    s"$tag no registry entry for ord=$tipOrdinal from=${attesterHex.value.take(16)}... — accepting (Ed25519 already authenticated)"
                  )
                  .as(true)
            case Some(entry) =>
              // Registry holds the master VK captured at registration (step=0 in the tree) plus the
              // operator's eta-period offset. Sender signs at the *current* product step (=
              // globalPeriod - offset); `SumComposition.verify` uses `kesVk.step` in its left-vs-right
              // tree-walk so verifying with a step that doesn't match the sender's mis-walks the path
              // and rejects every sig. Rebind step to `globalPeriod - operator.offset` so verify
              // reconstructs the same path the sender used. Root bytes are invariant; only step changes.
              val globalPeriod = EtaCalculation.rotationPeriod(tipOrdinal, etaRotationSnapshots).toInt
              val treeInternalStep = globalPeriod - entry.offset.toInt
              if (treeInternalStep < 0)
                Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_invalid_total") >>
                  logger
                    .warn(
                      s"⚠️ $tag from operator with offset=${entry.offset} can't sign at globalPeriod=$globalPeriod (treeInternalStep would be $treeInternalStep — operator not yet active)"
                    )
                    .as(false)
              else {
                val vkAtStep = entry.vk.copy(step = treeInternalStep)
                val ok = OperationalKeyMaker.verify(kSig, messageBytes, vkAtStep)
                if (ok)
                  Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_verified_total") >>
                    logger
                      .info(
                        s"🔐 $tag verified ord=$tipOrdinal period=$globalPeriod step=$treeInternalStep from=${attesterHex.value.take(16)}..."
                      )
                      .as(true)
                else
                  Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_invalid_total") >>
                    logger
                      .warn(
                        s"⚠️ $tag invalid ord=$tipOrdinal period=$globalPeriod step=$treeInternalStep from=${attesterHex.value.take(16)}... — rejecting"
                      )
                      .as(false)
              }
          }
      }
    }
  }

  /** Verify a KES signature attached to a snapshot. Same accept/reject semantics as [[verifyAttestation]]. */
  def verifySnapshot[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    producerId: peer.PeerId,
    producerHex: Hex,
    ordinal: Long,
    kesRegistry: KesRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] = {
    val tag = "KES-SNAP"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_sig_total") >>
        logger.warn(s"⚠️ $tag missing sig — rejecting ord=$ordinal from=${producerHex.value.take(16)}...").as(false)
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_decode_failed_total") >>
            logger
              .warn(s"⚠️ $tag decode failed for ord=$ordinal from=${producerHex.value.take(16)}...: ${err.message} — rejecting")
              .as(false)
        case Right(kSig) =>
          kesRegistry.getKesVk(producerId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_registry_entry_total") >>
                logger
                  .debug(
                    s"$tag no registry entry for ord=$ordinal from=${producerHex.value.take(16)}... — accepting (Ed25519 already authenticated)"
                  )
                  .as(true)
            case Some(entry) =>
              val globalPeriod = EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots).toInt
              val treeInternalStep = globalPeriod - entry.offset.toInt
              if (treeInternalStep < 0)
                Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_invalid_total") >>
                  logger
                    .warn(
                      s"⚠️ $tag from operator with offset=${entry.offset} can't sign at globalPeriod=$globalPeriod (treeInternalStep would be $treeInternalStep — operator not yet active)"
                    )
                    .as(false)
              else {
                val vkAtStep = entry.vk.copy(step = treeInternalStep)
                val ok = OperationalKeyMaker.verify(kSig, messageBytes, vkAtStep)
                if (ok)
                  Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_verified_total") >>
                    logger
                      .info(
                        s"🔐 $tag verified ord=$ordinal period=$globalPeriod step=$treeInternalStep from=${producerHex.value.take(16)}..."
                      )
                      .as(true)
                else
                  Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_invalid_total") >>
                    logger
                      .warn(
                        s"⚠️ $tag invalid ord=$ordinal period=$globalPeriod step=$treeInternalStep from=${producerHex.value.take(16)}... — rejecting"
                      )
                      .as(false)
              }
          }
      }
    }
  }
}
