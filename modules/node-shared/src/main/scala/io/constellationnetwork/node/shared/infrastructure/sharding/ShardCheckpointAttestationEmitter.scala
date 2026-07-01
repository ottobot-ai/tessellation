package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.charset.StandardCharsets
import java.security.KeyPair

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.EligibilityChecker
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer.KesSigner
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardId}
import io.constellationnetwork.security.SecurityProvider
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signing

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Receiver-side attestation EMITTER for shard checkpoints — closes the `T_count_shard` quorum loop.
  *
  * '''The missing seam this fills.''' The shard-checkpoint path produces (winning slot leader fires a
  * [[io.constellationnetwork.schema.sharding.ShardCheckpoint]] carrying its own one [[CommitteeMemberSignature]]) and propagates (sidecar
  * gossips the envelope cross-node), but until this emitter every non-producing committee member only RECORDED the producer's single
  * signature into its [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker]] and never signed + published its OWN
  * attestation. With self-exclusion (`attestationCountFor(excludeSelf = true)`, #133/P-11b) the producer's lone signature is excluded from
  * every other node's threshold count, so `⌈2·K_S/3⌉` was unreachable and `T_count_shard` never fired. This emitter is the exact shard
  * analog of the gl0 `NakamotoSyncDaemon.emitAttestation` / `emitTipAttestation` seam: when a received checkpoint becomes a node's best
  * tip, the node signs and gossips a
  * [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs.ShardCheckpointAttestation]] so OTHER nodes'
  * trackers cross the quorum.
  *
  * '''Mirrors the proven global flow''' (per the slashing / standard-patterns rules — do NOT invent a new scheme):
  *   - '''Became-best-tip gate.''' The caller (`NakamotoSyncDaemon.handleShardCheckpoint`) only invokes [[emit]] when
  *     `ShardChainStore.store(...)` returned `isNew = true` AND the stored checkpoint became the canonical bestTip — byte-identical to the
  *     gl0 `becameBest = isNew && bestTipOpt.exists(_.hash === thisHash)` gate (`NakamotoSyncDaemon.scala:808`). So each node attests at
  *     most once per winning hash (P-9 `becameBestTip`-gated emission).
  *   - '''Signing.''' Ed25519 (long-term) + KES product, both over the canonical checkpoint hash's UTF-8 bytes — the EXACT bytes
  *     [[ShardCheckpointProducer]] signs (`Hasher[F](ShardCheckpointSigPreimage)` ⇒ `preimageHash.getBytes`). Reuses the same
  *     [[ShardCheckpointProducer.KesSigner]] adapter the producer uses (`OperationalKeyMaker`-backed).
  *   - '''VRF membership proof.''' Computes a real VRF proof over the same `(shardEta, slot)` message the slot-leader lottery draws from
  *     (`EligibilityChecker.vrfProofForSlot`). v1 acceptance does not cryptographically verify the gossiped attestation's VRF proof (it
  *     only structurally checks the checkpoint-embedded committee signatures — see `ShardCheckpointGl0AcceptanceManager.preCheck` scaladoc
  *     and `ShardCheckpointWiring.committeeMembership` v1 full-set rule), but a real proof keeps the wire field cryptographically
  *     meaningful for the v2 VRF-enumeration follow-up rather than shipping a placeholder.
  *   - '''Self-exclusion.''' The emitted attestation is recorded into the LOCAL tracker too (mirrors `emitTipAttestation` recording
  *     `selfId` locally first), but `ShardTipTracker.attestationCountFor`'s default `excludeSelf = true` keeps it out of the LOCAL
  *     threshold count (P-11b: a node must not self-finalize a divergent shard fork). It is the GOSSIPED copy that drives quorum at every
  *     OTHER node.
  *
  * '''numShards = 1 no-op.''' This emitter is only constructed on the `numShards > 1` activated path (the gl0-leader-produce wiring in
  * `GlobalSnapshotConsensus.make`, alongside the producers). At `numShards = 1` the daemon receives `None` and the became-best-tip emit
  * branch is skipped entirely — byte-identical to the pre-wiring daemon.
  *
  * '''Failure model''' (mirrors `ShardCheckpointPublisher.sidecar` + `emitTipAttestation`): publish failures are logged + swallowed; the
  * gossip handler MUST NOT block on a publish failure. A persistent failure is observable in cluster-level metrics (the shard never reaches
  * `T_count_shard`), not by raising errors on the receive path.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`): every tunable (`sigmaInCommittee`, `lddConfig`, the slot/eta mappings)
  * is a constructor param wired from typed `SharedConfig.nakamoto.*` values by the caller. No `sys.env.get` anywhere.
  *
  * '''Use Hasher rule''' (per `[[feedback-use-hasher-no-manual-serialize]]`): the canonical hash is supplied by the caller
  * (`Hasher[F].hash(checkpoint.signingPreimage)` — the same value the chain store keys by). This emitter signs that hash's UTF-8 bytes; it
  * does NOT hand-roll any serialization.
  */
trait ShardCheckpointAttestationEmitter[F[_]] {

  /** Sign + record-locally + gossip an attestation for a checkpoint that just became this node's best tip.
    *
    * @param shardId
    *   the shard the checkpoint belongs to. Selects the per-shard `shardEta` (leader-VRF domain) and the local [[ShardTipTracker]] for the
    *   self-record.
    * @param checkpointHash
    *   canonical `Hasher[F](ShardCheckpointSigPreimage)` hash — the bytes every committee member signs (design doc §3.3). The caller
    *   already computed this to key the chain store, so it's passed in rather than re-derived.
    * @param slot
    *   the checkpoint's WIRE slot (design §5.7 — the signed lottery clock carried on the envelope). The VRF message `(shardEta, slot)` is
    *   byte-equivalent on producer + attester because both read the SAME wire field.
    * @param epoch
    *   the checkpoint's sortition epoch (the wire-carried `checkpoint.epoch`). Slice S4: load-bearing — it is the key for the
    *   shard-leader-VRF eta lookup (`shardEtaFor(shardId, epoch)`). The producer signed its leader proof under the eta of THIS epoch, so
    *   the attester MUST resolve the eta for the SAME epoch (not a wall-clock period) to compute a byte-equivalent VRF message.
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
    *   slot-leader VRF secret. Used to compute the committee-membership VRF proof over `(shardEta, slot)` via
    *   `EligibilityChecker.vrfProofForSlot` — the same secret + message the slot-leader lottery draws from.
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
    *   `(shardId, epoch) => F[Option[Array[Byte]]]` — resolves the 32-byte per-shard leader-VRF eta (`ShardSlotLeader.computeShardEta`) for
    *   the GIVEN eta-period (Slice S4). `None` ⇒ this shard is not a tracked committee shard locally (skip silently). MUST be derived from
    *   the eta of the CHECKPOINT'S epoch (the `emit` `epoch` arg, sourced from the wire-carried `checkpoint.epoch`), NOT a wall-clock
    *   period — a checkpoint produced near an eta boundary may be verified after it, so producer + every verifier MUST key the eta lookup
    *   on `checkpoint.epoch` to derive byte-identical bytes (the load-bearing determinism invariant: divergent shardEta ⇒ `verifyLeader`
    *   disagreement ⇒ the shard chain stalls). Production wiring closes over `etaForPeriod(epoch) ⇒ computeShardEta(shardId, gl0Eta)`.
    * @param sigmaInCommittee
    *   this operator's stake share within the shard committee (v1 stable-σ rule: `1 / K_S`). Not load-bearing for the proof bytes (the
    *   proof is over `(shardEta, slot)` only) but carried for symmetry / future threshold use.
    * @param lddConfig
    *   per-shard LDD config (production threads the consensus config from the wiring site). Carried for symmetry; unused by
    *   `vrfProofForSlot` itself.
    */
  def make[F[_]: Async: SecurityProvider](
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    selfVrfSk: Array[Byte],
    kesSigner: KesSigner[F],
    eligibilityChecker: EligibilityChecker[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    tipTrackerFor: ShardId => Option[ShardTipTracker[F]],
    shardEtaFor: (ShardId, EtaPeriod) => F[Option[Array[Byte]]],
    sigmaInCommittee: Ratio,
    lddConfig: LddConfig
  ): ShardCheckpointAttestationEmitter[F] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointAttestationEmitter")
    // `sigmaInCommittee` + `lddConfig` are captured for symmetry with the producer + the v2 verification follow-up; the v1 VRF proof is over
    // `(shardEta, slot)` only (membership proof), so they don't enter the proof bytes today.
    val _ = (sigmaInCommittee, lddConfig)

    new ShardCheckpointAttestationEmitter[F] {

      def emit(
        shardId: ShardId,
        checkpointHash: Hash,
        slot: Slot,
        epoch: EtaPeriod
      ): F[Unit] =
        // Slice S4: resolve the shard-leader-VRF eta keyed on the CHECKPOINT'S epoch (the wire-carried `checkpoint.epoch`, passed in as
        // `epoch`), NOT a wall-clock period. The producer signed its leader proof under `computeShardEta(shardId, etaForPeriod(epoch))`;
        // every verifier MUST re-derive the SAME eta for the SAME `epoch` or `ShardSlotLeader.verifyLeader` disagrees and the shard chain
        // stalls. `epoch == rotationPeriod(gl0AnchorOrdinal)` and `eta_epoch` is fixed at the 2/3-mark of the prior period, so it is knowable
        // here even for a checkpoint produced near an eta boundary and verified after it.
        shardEtaFor(shardId, epoch).flatMap {
          case None =>
            // No leader-VRF eta for this shard ⇒ we don't track it as a committee participant. Skip silently (debug) — this is the
            // same "shard not tracked locally" disposition the acceptance side uses; we simply don't attest.
            logger.debug(s"emit: shard=${shardId.value.value} has no shardEta (not a tracked committee shard); skipping attestation")
          case Some(shardEta) =>
            val currentSlot: Slot = slot // the envelope's wire slot (design §5.7) — same field every verifier reads
            // The canonical hash bytes every committee member signs (design doc §3.3). UTF-8 of the hex Hash string — identical to the
            // producer's `preimageHash.getBytes` path.
            val msgBytes = checkpointHash.value.getBytes(StandardCharsets.UTF_8)
            for {
              // VRF membership proof over `(shardEta, slot)` — the SAME message the slot-leader lottery draws from. Deterministic +
              // verifiable later via `ShardSlotLeader.verifyLeader`. NOT a leadership claim: a non-leader still has a valid proof of having
              // EVALUATED the VRF at this slot; the threshold gate (which the producer passed) is not re-asserted on the attestation path.
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
