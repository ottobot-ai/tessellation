package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.charset.StandardCharsets
import java.security.KeyPair

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSigner
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardId}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signing

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Receiver-side attestation emitter for replay-valid shard checkpoints. It closes the configured `T_count_shard` selection loop by
  * publishing this operator's signature after checkpoint validation and CL1 recreation succeed.
  *
  * '''Mirrors the proven global flow''' (per the slashing / standard-patterns rules — do NOT invent a new scheme):
  *   - '''Canonical-ancestor gate.''' After every admissible receipt, the caller walks the current canonical shard ancestry and emits for
  *     replay-valid checkpoints not already self-attested. This covers parents that arrive after their descendants.
  *   - '''Signing.''' Ed25519 (long-term) + KES product, both over the canonical checkpoint hash's UTF-8 bytes — the EXACT bytes
  *     [[ShardCheckpointProducer]] signs (`Hasher[F](ShardCheckpointSigPreimage)` ⇒ `preimageHash.getBytes`). Reuses the same
  *     [[ShardCheckpointProducer.KesSigner]] adapter the producer uses (`OperationalKeyMaker`-backed).
  *   - '''VRF key-possession proof.''' Computes a real VRF proof over `(shardEta, slot)`. Receivers verify it under the registered VRF VK.
  *     Execution membership itself is the separately enumerable public VK-hash set; this proof does not secretly draw membership or prove
  *     producer duty.
  *   - '''Local record.''' The emitted attestation is recorded into the local tracker and gossiped. `T_count_shard` counts distinct valid
  *     committee signatures including self, with `kQuorum >= 2`; this trigger selects replay-valid checkpoints but never replaces GL0
  *     replay.
  *
  * '''numShards = 1 no-op.''' This emitter is only constructed on the `numShards > 1` activated path (the gl0-leader-produce wiring in
  * `GlobalSnapshotConsensus.make`, alongside the producers). At `numShards = 1` the daemon receives `None` and the became-best-tip emit
  * branch is skipped entirely — byte-identical to the pre-wiring daemon.
  *
  * '''Failure model''' (mirrors `ShardCheckpointPublisher.sidecar` + `emitTipAttestation`): publish failures are logged + swallowed; the
  * gossip handler MUST NOT block on a publish failure. A persistent failure is observable in cluster-level metrics (the shard never reaches
  * `T_count_shard`), not by raising errors on the receive path.
  *
  * '''Use Hasher rule''' (per `[[feedback-use-hasher-no-manual-serialize]]`): the canonical hash is supplied by the caller
  * (`Hasher[F].hash(checkpoint.signingPreimage)` — the same value the chain store keys by). This emitter signs that hash's UTF-8 bytes; it
  * does NOT hand-roll any serialization.
  */
trait ShardCheckpointAttestationEmitter[F[_]] {

  /** Sign, record locally, and gossip an attestation for a replay-valid checkpoint on the canonical shard ancestry.
    *
    * @param shardId
    *   the shard the checkpoint belongs to. Selects the per-shard membership-proof eta and the local [[ShardTipTracker]] for the
    *   self-record.
    * @param checkpointHash
    *   canonical `Hasher[F](ShardCheckpointSigPreimage)` hash — the bytes every committee member signs (design doc §3.3). The caller
    *   already computed this to key the chain store, so it's passed in rather than re-derived.
    * @param slot
    *   the checkpoint's signed wire slot. The registered-key possession message `(shardEta, slot)` is byte-equivalent on producer +
    *   attester because both read the SAME wire field.
    * @param epoch
    *   the checkpoint's sortition epoch (the wire-carried `checkpoint.epoch`). Slice S4: load-bearing — it is the key for the shard-eta
    *   lookup (`shardEtaFor(shardId, epoch)`). The producer signed its key-possession proof under the eta of this epoch, so the attester
    *   MUST resolve the eta for the SAME epoch (not a wall-clock period) to compute a byte-equivalent VRF message.
    */
  def emit(
    shardId: ShardId,
    checkpointHash: Hash,
    slot: Slot,
    epoch: EtaPeriod
  ): F[Unit]
}

object ShardCheckpointAttestationEmitter {

  /** Construct an emitter capturing this operator's signing material + the per-shard VRF domains.
    *
    * @param selfPeerId
    *   this operator's identity, stamped on the emitted [[CommitteeMemberSignature.peerId]] and used to key the local self-record.
    * @param selfKeyPair
    *   long-term Ed25519 keypair. Signs the canonical checkpoint hash bytes for [[CommitteeMemberSignature.ed25519Sig]] (same key the
    *   producer's committee sig + outer envelope use).
    * @param selfVrfSk
    *   registered VRF secret. Used to prove key possession over `(shardEta, slot)` via `EligibilityChecker.vrfProofForSlot`; execution
    *   membership comes from the public VK-hash draw and producer duty comes from the staircase schedule.
    * @param kesSigner
    *   KES product signer (the SAME [[ShardCheckpointProducer.KesSigner]] adapter the producer uses). Yields `kesTreeStep` +
    *   `kesProductSig`.
    * @param eligibilityChecker
    *   used purely for `vrfProofForSlot` (the deterministic VRF proof primitive). MUST be the same instance the rest of the consensus path
    *   uses so the proof bytes are byte-equivalent with a future verifier.
    * @param sidecarClient
    *   gossip transport. Publishes the encoded attestation on the per-shard `shard-checkpoint-attestation` topic.
    * @param tipTrackerFor
    *   `shardId => Option[ShardTipTracker[F]]` — the SAME per-shard trackers the acceptance side / finality triggers read (typically
    *   `shardId => registry.get(shardId).map(_.tipTracker)`). The self-record writes here so `allAttestations` stays complete; the local
    *   threshold count still excludes self by default.
    * @param shardEtaFor
    *   `(shardId, epoch) => F[Option[Array[Byte]]]` — resolves the 32-byte per-shard registered-key proof eta
    *   (`ShardSlotLeader.computeShardEta`) for the GIVEN eta-period (Slice S4). `None` ⇒ this shard is not a tracked committee shard
    *   locally (skip silently). MUST be derived from the eta of the CHECKPOINT'S epoch (the `emit` `epoch` arg, sourced from the
    *   wire-carried `checkpoint.epoch`), NOT a wall-clock period — a checkpoint produced near an eta boundary may be verified after it, so
    *   producer + every verifier MUST key the eta lookup on `checkpoint.epoch` to derive byte-identical bytes (the load-bearing determinism
    *   invariant: divergent shard eta makes the registered-key proof invalid and stalls the shard chain). Production wiring closes over
    *   `etaForPeriod(epoch) => computeShardEta(shardId, gl0Eta)`.
    */
  def make[F[_]: Async: SecurityProvider](
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    selfVrfSk: Array[Byte],
    kesSigner: KesSigner[F],
    eligibilityChecker: EligibilityChecker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTrackerFor: ShardId => Option[ShardTipTracker[F]],
    shardEtaFor: (ShardId, EtaPeriod) => F[Option[Array[Byte]]]
  ): ShardCheckpointAttestationEmitter[F] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointAttestationEmitter")

    new ShardCheckpointAttestationEmitter[F] {

      def emit(
        shardId: ShardId,
        checkpointHash: Hash,
        slot: Slot,
        epoch: EtaPeriod
      ): F[Unit] =
        // Resolve shard eta keyed on the checkpoint's wire-carried epoch, not a wall-clock period. The producer signed its possession proof under `computeShardEta(shardId, etaForPeriod(epoch))`;
        // every verifier MUST re-derive the SAME eta for the SAME `epoch` or registered-key proof verification disagrees and the shard chain
        // stalls. The epoch eta remains resolvable after a boundary.
        shardEtaFor(shardId, epoch).flatMap {
          case None =>
            // No shard proof eta means this node does not track the shard. Skip silently; this is the
            // same "shard not tracked locally" disposition the acceptance side uses; we simply don't attest.
            logger.debug(s"emit: shard=${shardId.value.value} has no shardEta (not a tracked committee shard); skipping attestation")
          case Some(shardEta) =>
            val currentSlot: Slot = slot // the envelope's wire slot (design §5.7) — same field every verifier reads
            // The canonical hash bytes every committee member signs (design doc §3.3). UTF-8 of the hex Hash string — identical to the
            // producer's `preimageHash.getBytes` path.
            val msgBytes = checkpointHash.value.getBytes(StandardCharsets.UTF_8)
            for {
              // Registered-key possession proof over `(shardEta, slot)`. This is neither the public VK-hash membership draw nor a producer
              // duty claim; any registered committee member can produce a valid proof for its own attestation.
              vrfProof <- Async[F].delay(eligibilityChecker.vrfProofForSlot(selfVrfSk, currentSlot, shardEta))
              // Ed25519 long-term sig over the canonical hash bytes — mirrors `ShardCheckpointProducer`'s `Signing.signData`.
              edSig <- Signing.signData[F](msgBytes)(selfKeyPair.getPrivate)
              kesStep <- kesSigner.currentPeriod
              kesSig <- kesSigner.signAt(kesStep, msgBytes)
              committeeSig = CommitteeMemberSignature(
                peerId = selfPeerId,
                vrfProof = Hex.fromBytes(vrfProof),
                ed25519Sig = Hex.fromBytes(edSig),
                kesProductSig = Hex.fromBytes(kesSig),
                kesTreeStep = kesStep
              )
              attestation = ShardCheckpointWireCodecs.ShardCheckpointAttestation(
                shardId = shardId,
                checkpointHash = checkpointHash,
                attesterSignature = committeeSig
              )
              // Record locally first (mirror `emitTipAttestation`: self sees its own attestation). The default `excludeSelf = true` on the
              // tracker keeps this out of OUR threshold count (P-11b) — it is the gossiped copy below that lifts every OTHER node to quorum.
              _ <- tipTrackerFor(shardId) match {
                case Some(tracker) => tracker.recordAttestation(checkpointHash, selfPeerId, committeeSig)
                case None          => Async[F].unit
              }
              wire = ShardCheckpointWireCodecs.shardCheckpointAttestationToWire(attestation)
              _ <- sidecarClient
                .publishShardCheckpointAttestation(wire)
                .flatMap { resp =>
                  if (resp.ok)
                    logger.info(
                      s"🧩 ShardCheckpointAttestation emit: shard=${shardId.value.value} " +
                        s"checkpoint=${checkpointHash.value.take(12)} slot=${slot.value.value} kesStep=$kesStep"
                    )
                  else
                    logger.warn(
                      s"⚠️ ShardCheckpointAttestation publish returned not-ok: shard=${shardId.value.value} " +
                        s"checkpoint=${checkpointHash.value.take(12)}: ${resp.error}"
                    )
                }
                .handleErrorWith { err =>
                  logger.warn(
                    s"⚠️ ShardCheckpointAttestation publish failed: shard=${shardId.value.value} " +
                      s"checkpoint=${checkpointHash.value.take(12)}: ${err.getMessage}"
                  )
                }
            } yield ()
        }
    }
  }
}
