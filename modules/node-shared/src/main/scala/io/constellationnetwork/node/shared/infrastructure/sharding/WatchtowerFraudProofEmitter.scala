package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.KeyPair

import cats.effect.kernel.Async
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{ShardCheckpointGl0AcceptanceManager, WatchtowerMismatch}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{FraudProofEnvelope, ShardCheckpoint}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, SecurityProvider}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** WATCHTOWER fraud-proof EMITTER (fraud-proof part 1, producer side) — the approval-check seam.
  *
  * '''What it does.''' For an ADOPTED/finalized [[ShardCheckpoint]] (the daemon invokes [[emit]] on the became-best-tip adopt path, the
  * same seam the [[ShardCheckpointAttestationEmitter]] fires on), it runs [[ShardCheckpointGl0AcceptanceManager.watchtowerReExec]] — the
  * per-MG re-derivation that runs EVEN WHEN QUORUM WAS MET (the whole point: catch a quorum-signed wrong root that `verifyEmbedded`
  * admitted on signatures alone). For each mismatch it builds, signs, and gossips a [[FraudProofEnvelope]] on the gl0-wide `fraud-proof`
  * topic. Every gl0 then INDEPENDENTLY re-runs the deterministic verdict
  * (`io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator`) and slashes the committee on UPHELD.
  *
  * '''Mirrors the proven flow''' (per the slashing / standard-patterns rules — do NOT invent):
  *   - '''Adopt-time gate.''' Invoked by the daemon only for a checkpoint that became this node's canonical best tip — byte-identical to
  *     the attestation emitter's gate. So the watchtower runs on the SAME adopted checkpoints, on a node whose finalized `S(N)` equals the
  *     producer's diff base (the in-order chain-hole-guard invariant) ⇒ an honest checkpoint never mismatches.
  *   - '''Signing.''' Ed25519 (long-term key) over the canonical `FraudProofSigPreimage` hash — the same key + Hasher discipline the
  *     attestation emitter uses, so the on-chain verdict recovers the submitter VK from `submitterId` and verifies.
  *   - '''Failure model.''' Re-exec failures are logged + swallowed; publish failures are retried a bounded number of times
  *     (`nakamoto.invalidity-slashing.fraud-proof-publish-*` — a fraud proof is slashing evidence and must survive a transient local
  *     sidecar hiccup; once the RPC lands, the sidecar's durable outbox owns delivery) and then swallowed. The adopt path MUST NOT block on
  *     the watchtower: `emit` runs on the daemon's background fiber, so the retries never stall gossip intake. A persistent failure is
  *     observable as "no fraud proofs despite a wrong root", not as a crashed receive path.
  *
  * '''numShards = 1 no-op / disabled.''' Constructed only on the activated path (`numShards > 1` AND `watchtower-enabled`); the daemon
  * receives `None` otherwise and never emits.
  */
trait WatchtowerFraudProofEmitter[F[_]] {

  /** Re-execute an adopted checkpoint and, for each metagraph whose committee-attested root this node did not reproduce, sign + gossip a
    * fraud proof. No-op when the re-execution reproduces every attested root (the honest-checkpoint common case).
    */
  def emit(checkpoint: ShardCheckpoint): F[Unit]
}

object WatchtowerFraudProofEmitter {

  /** @param selfPeerId
    *   this operator's identity, stamped as the fraud proof's `submitterId` (bounty recipient + signature subject).
    * @param selfKeyPair
    *   long-term Ed25519 keypair — signs the canonical `FraudProofSigPreimage` hash.
    * @param acceptanceManager
    *   the SAME [[ShardCheckpointGl0AcceptanceManager]] the adopt path uses — its [[ShardCheckpointGl0AcceptanceManager.watchtowerReExec]]
    *   holds the PIN-1 re-exec closure (finalized base), so the re-derived roots are byte-comparable with the attested ones.
    * @param sidecarClient
    *   gossip transport — publishes the encoded fraud proof on the gl0-wide `fraud-proof` topic. Once the RPC lands, the sidecar's durable
    *   outbox republishes until TTL — so the retry below only has to survive the LOCAL hop (a restarting/unreachable sidecar process).
    * @param publishAttempts
    *   total publish attempts before giving up (`nakamoto.invalidity-slashing.fraud-proof-publish-attempts`). A fraud proof is slashing
    *   EVIDENCE — a single warn-and-drop on a transient local gRPC failure silently disarmed the watchtower tooth. Retries run on the
    *   daemon's background fiber (the adopt path is never blocked); after the last attempt the failure is still swallowed (WARN) so the
    *   receive path can't crash.
    * @param publishRetryDelay
    *   delay between attempts (`nakamoto.invalidity-slashing.fraud-proof-publish-retry-delay`) — sized to ride out a sidecar restart.
    */
  def make[F[_]: Async: SecurityProvider: Hasher](
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    publishAttempts: Int = 3,
    publishRetryDelay: FiniteDuration = 2.seconds
  ): WatchtowerFraudProofEmitter[F] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("WatchtowerFraudProofEmitter")

    new WatchtowerFraudProofEmitter[F] {

      def emit(checkpoint: ShardCheckpoint): F[Unit] =
        acceptanceManager
          .watchtowerReExec(checkpoint)
          .flatMap { mismatches =>
            if (mismatches.isEmpty) Async[F].unit
            else
              Hasher[F].hash(checkpoint.signingPreimage).flatMap { checkpointHash =>
                mismatches.traverse_ { mm: WatchtowerMismatch =>
                  buildSignAndGossip(checkpoint, checkpointHash, mm)
                }
              }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Watchtower re-exec failed for shard=${checkpoint.shardId.value.value}: ${err.getMessage}")
          }

      private def buildSignAndGossip(
        checkpoint: ShardCheckpoint,
        checkpointHash: io.constellationnetwork.security.hash.Hash,
        mm: WatchtowerMismatch
      ): F[Unit] = {
        // The challenger's reference root (the honest re-derived value) is carried as the witness + challengerDerivation — a HINT; the
        // on-chain verdict recomputes it and never trusts it.
        val witness: Hex = Hex(mm.reDerivedRoot.value)
        val unsigned = FraudProofEnvelope(
          shardId = checkpoint.shardId,
          disputedCheckpointHash = checkpointHash,
          metagraphAddress = mm.metagraphAddress,
          gl0AnchorOrdinal = checkpoint.gl0AnchorOrdinal,
          claimedDerivation = mm.attestedRoot,
          challengerDerivation = mm.reDerivedRoot,
          reexecutionWitness = witness,
          challengerSignature = Hex(""), // filled below
          submitterId = selfPeerId
        )
        for {
          digest <- Hasher[F].hash(unsigned.signingPreimage)
          sig <- Signing.signData[F](digest.getBytes)(selfKeyPair.getPrivate)
          signed = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
          wire = FraudProofWireCodecs.toWire(signed)
          _ <- publishWithRetry(wire, checkpoint, checkpointHash, mm, attempt = 1)
        } yield ()
      }

      /** Bounded-retry publish. Retries BOTH failure shapes — a raised gRPC error (sidecar down) and an `ok = false` response (e.g. the
        * sidecar's fraud topic not joined) — because either way the evidence has not left the node. Never raises: after the final attempt
        * the failure is WARN-swallowed (design rule: the adopt/receive path must not crash on watchtower trouble; a persistent failure is
        * observable as "no fraud proofs despite a wrong root").
        */
      private def publishWithRetry(
        wire: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.FraudProofEnvelopeWire,
        checkpoint: ShardCheckpoint,
        checkpointHash: io.constellationnetwork.security.hash.Hash,
        mm: WatchtowerMismatch,
        attempt: Int
      ): F[Unit] = {

        def retryOrGiveUp(reason: String): F[Unit] =
          if (attempt < publishAttempts)
            logger.warn(
              s"⚠️ Watchtower fraud-proof publish failed (attempt $attempt/$publishAttempts, retrying in $publishRetryDelay): " +
                s"shard=${checkpoint.shardId.value.value} checkpoint=${checkpointHash.value.take(12)}: $reason"
            ) >> Async[F].sleep(publishRetryDelay) >> publishWithRetry(wire, checkpoint, checkpointHash, mm, attempt + 1)
          else
            logger.warn(
              s"⚠️ Watchtower fraud-proof publish FAILED after $publishAttempts attempt(s) — evidence NOT gossiped: " +
                s"shard=${checkpoint.shardId.value.value} checkpoint=${checkpointHash.value.take(12)}: $reason"
            )

        sidecarClient
          .publishFraudProof(wire)
          .attempt
          .flatMap {
            case Right(resp) if resp.ok =>
              logger.warn(
                s"🛡️ WATCHTOWER fraud proof GOSSIPED: shard=${checkpoint.shardId.value.value} " +
                  s"shardOrd=${checkpoint.shardOrdinal.value} mg=${mm.metagraphAddress.value.value.take(10)} " +
                  s"attested=${mm.attestedRoot.value.take(12)} honest=${mm.reDerivedRoot.value.take(12)} " +
                  s"checkpoint=${checkpointHash.value.take(12)}" +
                  (if (attempt > 1) s" (attempt $attempt/$publishAttempts)" else "")
              )
            case Right(resp) => retryOrGiveUp(s"sidecar not-ok: ${resp.error}")
            case Left(err)   => retryOrGiveUp(err.getMessage)
          }
      }
    }
  }
}
