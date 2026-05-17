package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{EtaCalculation, KesRegistry}
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.schema.peer
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.OperationalKeyMaker

import eu.timepit.refined.auto._

/** §1.2 Slice 5/6/9 — KES verification of incoming attestations + snapshots.
  *
  * Pulled out of [[NakamotoSyncDaemon]] so the verification logic + counter taxonomy live in one place and are independently unit-testable.
  * The daemon retains responsibility for routing (call the right helper after the Ed25519 path succeeds); this object owns the matrix:
  *
  *   - empty wire field → `*_no_sig_total` (warn when enforcing)
  *   - decode failure → `*_decode_failed_total` (WARN)
  *   - sig + no registry → `*_no_registry_entry_total` (DEBUG; never rejects — see note)
  *   - sig + verify OK → `*_verified_total` (INFO)
  *   - sig + verify fail → `*_invalid_total` (WARN)
  *
  * '''Slice 9 enforcement''': when `enforce=true`, the helpers return `false` for no-sig / decode-fail / invalid; the daemon drops the
  * message on `false`. When `enforce=false`, the helpers always return `true` (preserves the warn-only behavior from Slices 5/6 so legacy
  * CSV-genesis paths and the soak iter prior to flip still work).
  *
  * '''Why no-registry-entry never rejects''': the registry today is built from genesis. With Slice 10 (#179, runtime registration cert), an
  * operator can submit a `RegisterKesVk` tx mid-life — between submission and finality, the GSI's `activeKesRegistrations` lags. If we
  * rejected on `None` from `getKesVk`, a newly-joined operator's first batch of attestations would be dropped before their reg-cert
  * settled. Symmetric handling on both sides of the rotation cliff is more important than catching impersonation at this gate (Ed25519
  * already authenticates the peer; KES only adds forward-secure non-repudiation).
  */
private[nakamoto] object KesGossipVerification {

  /** Verify a KES signature attached to an attestation. `messageBytes` is the Ed25519-signed attestation-hash bytes (the same bytes the
    * Ed25519 path verified). Period is recomputed from `tipOrdinal` for log context and for the step-rebind below.
    *
    * Returns `true` if the attestation should be accepted, `false` if it should be dropped. When `enforce=false` always returns `true`
    * (warn-only).
    */
  def verifyAttestation[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    attesterId: peer.PeerId,
    attesterHex: Hex,
    tipOrdinal: Long,
    kesRegistry: KesRegistry[F],
    etaRotationSnapshots: Long,
    enforce: Boolean,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] = {
    val tag = "KES-ATT"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_sig_total") >>
        Async[F]
          .whenA(enforce) {
            logger.warn(s"⚠️ $tag missing sig — rejecting ord=$tipOrdinal from=${attesterHex.value.take(16)}...")
          }
          .as(!enforce)
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_decode_failed_total") >>
            logger
              .warn(
                s"⚠️ $tag decode failed for ord=$tipOrdinal from=${attesterHex.value
                    .take(16)}...: ${err.message}${if (enforce) " — rejecting" else ""}"
              )
              .as(!enforce)
        case Right(kSig) =>
          kesRegistry.getKesVk(attesterId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_no_registry_entry_total") >>
                logger
                  .debug(
                    s"$tag no registry entry for ord=$tipOrdinal from=${attesterHex.value.take(16)}... — accepting (Ed25519 already authenticated)"
                  )
                  .as(true)
            case Some(vk) =>
              // Registry holds the master VK captured at bootstrap (step=0). Sender signs
              // at the *current* product step (= kesPeriod). `SumComposition.verify` uses
              // `kesVk.step` in its left-vs-right tree-walk heuristic, so verify with the
              // master VK literally would mis-walk the Merkle path and reject every
              // post-rotation sig. Rebind step to the period derived from the wire ordinal
              // so verify reconstructs the same path the sender used. The root bytes are
              // invariant under evolution; only the `step` index changes.
              val kesPeriod = EtaCalculation.rotationPeriod(tipOrdinal, etaRotationSnapshots).toInt
              val vkAtPeriod = vk.copy(step = kesPeriod)
              val ok = OperationalKeyMaker.verify(kSig, messageBytes, vkAtPeriod)
              if (ok)
                Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_verified_total") >>
                  logger
                    .info(
                      s"🔐 $tag verified ord=$tipOrdinal period=$kesPeriod from=${attesterHex.value.take(16)}..."
                    )
                    .as(true)
              else
                Metrics[F].incrementCounter("dag_nakamoto_kes_attestations_invalid_total") >>
                  logger
                    .warn(
                      s"⚠️ $tag invalid ord=$tipOrdinal period=$kesPeriod from=${attesterHex.value.take(16)}...${if (enforce) " — rejecting"
                        else " — warn-only, not rejecting"}"
                    )
                    .as(!enforce)
          }
      }
    }
  }

  /** Verify a KES signature attached to a snapshot. `messageBytes` is the snapshot-hash bytes (the same bytes the producer signed on the
    * sender side). Same accept/reject semantics as [[verifyAttestation]].
    */
  def verifySnapshot[F[_]: Async: Metrics](
    messageBytes: Array[Byte],
    kesSigBytes: Array[Byte],
    producerId: peer.PeerId,
    producerHex: Hex,
    ordinal: Long,
    kesRegistry: KesRegistry[F],
    etaRotationSnapshots: Long,
    enforce: Boolean,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Boolean] = {
    val tag = "KES-SNAP"
    if (kesSigBytes.isEmpty) {
      Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_sig_total") >>
        Async[F]
          .whenA(enforce) {
            logger.warn(s"⚠️ $tag missing sig — rejecting ord=$ordinal from=${producerHex.value.take(16)}...")
          }
          .as(!enforce)
    } else {
      OperationalKeyMaker.decodeSignature(kesSigBytes) match {
        case Left(err) =>
          Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_decode_failed_total") >>
            logger
              .warn(
                s"⚠️ $tag decode failed for ord=$ordinal from=${producerHex.value.take(16)}...: ${err.message}${if (enforce) " — rejecting"
                  else ""}"
              )
              .as(!enforce)
        case Right(kSig) =>
          kesRegistry.getKesVk(producerId).flatMap {
            case None =>
              Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_no_registry_entry_total") >>
                logger
                  .debug(
                    s"$tag no registry entry for ord=$ordinal from=${producerHex.value.take(16)}... — accepting (Ed25519 already authenticated)"
                  )
                  .as(true)
            case Some(vk) =>
              // Same step-rebind as `verifyAttestation` above. See that comment.
              val kesPeriod = EtaCalculation.rotationPeriod(ordinal, etaRotationSnapshots).toInt
              val vkAtPeriod = vk.copy(step = kesPeriod)
              val ok = OperationalKeyMaker.verify(kSig, messageBytes, vkAtPeriod)
              if (ok)
                Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_verified_total") >>
                  logger
                    .info(
                      s"🔐 $tag verified ord=$ordinal period=$kesPeriod from=${producerHex.value.take(16)}..."
                    )
                    .as(true)
              else
                Metrics[F].incrementCounter("dag_nakamoto_kes_snapshots_invalid_total") >>
                  logger
                    .warn(
                      s"⚠️ $tag invalid ord=$ordinal period=$kesPeriod from=${producerHex.value.take(16)}...${if (enforce) " — rejecting"
                        else " — warn-only, not rejecting"}"
                    )
                    .as(!enforce)
          }
      }
    }
  }
}
