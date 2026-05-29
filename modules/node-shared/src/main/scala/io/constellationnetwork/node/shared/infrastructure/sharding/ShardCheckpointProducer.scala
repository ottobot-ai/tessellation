package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.KeyPair

import cats.Applicative
import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.kernel.{Async, Ref, Sync}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardSlotLeader}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.SignatureProof
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hashed, Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Sidecar publisher for `Signed[ShardCheckpoint]` envelopes produced by [[ShardCheckpointProducer]].
  *
  * Decouples the producer's pure logic from the on-wire gossip path. Production wiring will pass a sidecar-backed publisher (Slice 14 —
  * `pb.ShardCheckpoint` over the per-shard libp2p GossipSub topic `shard-checkpoint-<shardId>`); tests pass [[noop]] or [[recording]].
  *
  * '''Why a single-method trait''' — the producer is content-agnostic; it has one job, "hand the envelope to whoever is responsible for
  * gossiping it". Matching the [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.Publisher]] shape so the same
  * pattern is consistent across slices.
  */
trait ShardCheckpointPublisher[F[_]] {

  /** Publish a produced (signed) checkpoint envelope. Failures are the publisher's responsibility to log + swallow — they MUST NOT block
    * the producer's slot-leader path (mirrors the `MetagraphCommitteeGate.Publisher` failure model).
    */
  def publish(checkpoint: Signed[ShardCheckpoint]): F[Unit]
}

object ShardCheckpointPublisher {

  /** No-op publisher. Useful when the producer is wired in a context that doesn't gossip (e.g. unit tests that only inspect the produced
    * envelope via `produce(...)`'s return value).
    */
  def noop[F[_]: Applicative]: ShardCheckpointPublisher[F] =
    (_: Signed[ShardCheckpoint]) => Applicative[F].unit

  /** Recording publisher: every published checkpoint is appended (in publish order) to a `Ref`. The returned
    * `F[List[Signed[ShardCheckpoint]]]` snapshots the current recorded list — repeated calls reflect any new publishes that happened in
    * between. Pattern matches the `Ref`-backed publisher stubs already in use by `MetagraphCommitteeGateSuite` (see `Publisher.recording`
    * in tests).
    */
  def recording[F[_]: Sync]: F[(ShardCheckpointPublisher[F], F[List[Signed[ShardCheckpoint]]])] =
    Ref.of[F, List[Signed[ShardCheckpoint]]](List.empty).map { ref =>
      val publisher: ShardCheckpointPublisher[F] =
        (cp: Signed[ShardCheckpoint]) => ref.update(_ :+ cp)
      val read: F[List[Signed[ShardCheckpoint]]] = ref.get
      (publisher, read)
    }

  /** Slice 14: sidecar-backed publisher. Encodes the envelope via [[ShardCheckpointWireCodecs.signedShardCheckpointToWire]] and forwards to
    * `SidecarClient.publishShardCheckpoint`.
    *
    * '''Failure model''' (mirrors `MetagraphCommitteeGate.Publisher`):
    *   - Wire-codec failures (`JsonSerializer` round-trip throws): logged + swallowed. The producer's slot-leader path MUST NOT block on a
    *     publish failure.
    *   - gRPC failure (`ok = false` response or future-side exception): logged + swallowed.
    *
    * Failures are intentionally not propagated up — the producer's contract is "fire-and-forget once we've signed". A persistent publisher
    * failure is observable in cluster-level metrics (Slice 9's gl0 acceptance manager will record absent shard checkpoints), not by raising
    * errors at the producer.
    */
  def sidecar[F[_]: Async: io.constellationnetwork.json.JsonSerializer: org.typelevel.log4cats.Logger](
    sidecarClient: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra[F]
  ): ShardCheckpointPublisher[F] = {
    val logger = org.typelevel.log4cats.Logger[F]
    new ShardCheckpointPublisher[F] {
      def publish(checkpoint: Signed[ShardCheckpoint]): F[Unit] =
        ShardCheckpointWireCodecs
          .signedShardCheckpointToWire[F](checkpoint)
          .flatMap { wire =>
            sidecarClient.publishShardCheckpoint(wire).flatMap { resp =>
              if (resp.ok)
                // Observability seam: the publish itself is otherwise silent (the producer's
                // `produce: emitted ...` INFO fires regardless of whether this sidecar call
                // succeeded). A dedicated INFO here makes the actual on-wire gossip publish
                // visible per shard/ordinal so cross-node propagation can be traced end-to-end.
                logger.info(
                  s"ShardCheckpointPublisher.sidecar: published " +
                    s"shard=${checkpoint.value.shardId.value.value} shardOrd=${checkpoint.value.shardOrdinal.value} " +
                    s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}"
                )
              else
                logger.warn(
                  s"ShardCheckpointPublisher.sidecar: sidecar PublishShardCheckpoint returned not-ok " +
                    s"(shardId=${checkpoint.value.shardId.value.value} shardOrdinal=${checkpoint.value.shardOrdinal.value} " +
                    s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}): ${resp.error}"
                )
            }
          }
          .handleErrorWith { err =>
            logger.warn(
              s"ShardCheckpointPublisher.sidecar: publish failed for " +
                s"shardId=${checkpoint.value.shardId.value.value} shardOrdinal=${checkpoint.value.shardOrdinal.value} " +
                s"gl0Anchor=${checkpoint.value.gl0AnchorOrdinal.value.value}: ${err.getMessage}"
            )
          }
    }
  }
}

/** Produces (and signs, and publishes) one [[ShardCheckpoint]] per call — slice 8 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §6.
  *
  * '''Scope''' (per slice 8 task spec):
  *   1. Take the queue of pending per-MG SC binary snapshots assigned to this shard (input — caller manages the queue).
  *   1. Run [[ShardSlotLeader.isLeader]] for this node's `(vrfSk, shardEta, slot, σ_in_committee)` draw. If we don't win this slot, return
  *      `None` and do NOT publish (per design doc §6.3 — "only winning slot leaders fire checkpoints; other committee members attest later
  *      via gossip", which is slice 14 territory).
  *   1. If we win, compute the [[ShardDerivedStateDelta]] via the injectable per-MG derivation callback (slice 8 doesn't reach into
  *      `TokenLockStateManager` etc. directly — that wiring is slice 9 / 13).
  *   1. Read `parentCheckpointHash` and `parentShardOrdinal` from `chainStore.bestTip` (genesis = `Hash.empty` + `ShardOrdinal.Genesis` for
  *      the first checkpoint).
  *   1. Build a single [[CommitteeMemberSignature]] attaching this node's VRF proof + Ed25519 sig + KES product sig over the canonical
  *      [[ShardCheckpointSigPreimage]] bytes (`signingPreimage`).
  *   1. Wrap in [[Signed]] with the operator's standard long-term Ed25519 [[SignatureProof]] (consumer-facing envelope shape, mirrors how
  *      the gl0 leader signs `GlobalIncrementalSnapshot`).
  *   1. Publish via [[ShardCheckpointPublisher.publish]] and return the produced envelope.
  *
  * '''What this slice does NOT do''':
  *   - Manage the pending-snapshot queue (caller-supplied via `pendingSnapshots`).
  *   - Reach directly into `TokenLockStateManager`, `MetagraphSyncManager` etc. — those slot in via `derivePerMgState` callback. Slice 9 /
  *     13 will wire the actual derivation closure.
  *   - Aggregate signatures from other committee members. v1 only attaches THIS node's sig; others sign + gossip in slice 14.
  *   - Wire the GossipSub side — that's slice 14. Uses [[ShardCheckpointPublisher]] abstraction here.
  *   - Modify `GlobalSnapshotAcceptanceManager` (GSAM). Wiring into GSAM is slice 9 / 13.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh producer for the shard layer. No compat ceremony.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - All knobs (`kTarget`, `k1Shard`, etc. — though this slice's producer itself doesn't read any of those) come from constructor params;
  *     production wiring will pass typed `SharedConfig.nakamoto.sharding.*` values. No `sys.env.get` anywhere.
  *
  * '''Use Hasher rule''' (per `[[feedback-use-hasher-no-manual-serialize]]`):
  *   - Canonical preimage hash routes through `Hasher[F].hash(ShardCheckpointSigPreimage)` via `checkpoint.signingPreimage` (defined on the
  *     envelope itself in slice 1). No hand-rolled Blake2b / byte-concat surface for the signing bytes.
  */
trait ShardCheckpointProducer[F[_]] {

  /** Try to produce + sign + publish one checkpoint for this shard.
    *
    * Inputs:
    *   - `pendingSnapshots`: per-MG queued SC binary chains for the MGs in this shard. Caller manages the queue; producer reads it once,
    *     emits the checkpoint, leaves the queue alone (caller drops accepted entries after observing the produced envelope).
    *   - `gl0AnchorOrdinal`: the gl0 ord this checkpoint will ride into (loose coupling; gl0 accepts at this ord or any later — see design
    *     doc §7.2).
    *   - `epoch`: the sortition epoch the local committee was drawn from (passed through to the envelope's `epoch` field so verifiers can
    *     look up the right active set for VRF verification).
    *
    * Returns:
    *   - `Some(checkpoint)` if this node won the slot lottery AND there's something to publish — the caller can take the produced hash (via
    *     `checkpoint.signingPreimage` ⇒ `Hasher[F]`) for local tracking.
    *   - `None` if this node didn't win the slot OR `pendingSnapshots` is empty AND no T_alive liveness ping is due (slice 8 v1 — T_alive
    *     liveness pings are handled at the caller layer; this slice returns `None` on empty input regardless. Future slice can introduce a
    *     T_alive override flag if needed).
    */
  def produce(
    pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
    gl0AnchorOrdinal: SnapshotOrdinal,
    epoch: EtaPeriod
  ): F[Option[Signed[ShardCheckpoint]]]
}

object ShardCheckpointProducer {

  /** Algebra describing the KES product signer for this operator. Mirrors
    * [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.KesSigner]] — the producer doesn't depend on the concrete
    * `OperationalKeyMakerAlgebra` so the test path can stub it.
    *
    * '''Why we use this trait instead of a raw `SecretKeyKesProduct`.''' The design-doc-style signature in the slice 8 task spec proposes
    * passing the secret-key bytes directly. That doesn't work for our codebase because `SecretKeyKesProduct` is `private[kes]` (see
    * `models.scala` scaladoc — "package-private: callers must not handle secret-key material directly — the read-once lifecycle is enforced
    * by [[OperationalKeyMakerAlgebra]]"). The right abstraction is the existing `KesSigner` trait, which `OperationalKeyMakerAlgebra` is a
    * natural producer of. Wiring code (slice 9/13) constructs the adapter from `operationalKeyMaker.currentPeriod` + `signAt`.
    */
  trait KesSigner[F[_]] {

    /** Current KES tree-internal step the in-memory key holds. The producer embeds this on the envelope's
      * [[CommitteeMemberSignature.kesTreeStep]] field so receivers can verify non-interactively (same #211 wire-step pattern).
      */
    def currentPeriod: F[Int]

    /** Sign `message` at `kesStep`. Returns empty bytes on signer failure — the produced checkpoint will carry an empty `kesProductSig` in
      * that case, and the receiver-side verifier will reject. Callers should always pass `currentPeriod` here.
      */
    def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]]
  }

  object KesSigner {

    /** A KES signer that returns a fixed pre-supplied byte payload. Useful in tests that don't want to bootstrap a full KES key; the
      * payload bytes are placeholders, the produced checkpoint just carries them verbatim on `kesProductSig`. Period is also fixed.
      */
    def fixed[F[_]: Applicative](period: Int, signatureBytes: Array[Byte]): KesSigner[F] =
      new KesSigner[F] {
        def currentPeriod: F[Int] = Applicative[F].pure(period)
        def signAt(kesStep: Int, message: Array[Byte]): F[Array[Byte]] = Applicative[F].pure(signatureBytes)
      }
  }

  /** Construct a [[ShardCheckpointProducer]].
    *
    * @param shardId
    *   the shard this producer is scoped to. Immutable; one producer instance per shard the operator participates in.
    * @param chainStore
    *   per-shard chain store (Slice 5). Read to determine `parentCheckpointHash` + `parentShardOrdinal` for the new checkpoint.
    * @param slotLeader
    *   per-shard slot leader (Slice 7). Drives the VRF gating that decides whether this node may produce in the current slot.
    * @param publisher
    *   gossip-path abstraction. Production wires a sidecar-backed impl (slice 14); tests pass [[ShardCheckpointPublisher.recording]] or
    *   [[ShardCheckpointPublisher.noop]].
    * @param selfPeerId
    *   this operator's identity. Carried on the produced [[CommitteeMemberSignature.peerId]].
    * @param selfKeyPair
    *   this operator's long-term Ed25519 keypair. Used for both the outer [[Signed]] envelope's [[SignatureProof]] and the inner
    *   [[CommitteeMemberSignature.ed25519Sig]] over the canonical preimage hash.
    * @param selfVrfSk
    *   this operator's slot-leader VRF secret. Drives the `slotLeader.isLeader(...)` draw and supplies the VRF proof carried on
    *   [[CommitteeMemberSignature.vrfProof]].
    * @param kesSigner
    *   this operator's KES product signer. Yields the `kesTreeStep` + `kesProductSig` carried on the envelope's
    *   [[CommitteeMemberSignature]]. The injectable trait shape keeps the producer testable without a full KES bootstrap.
    * @param shardEtaFor
    *   `epoch => F[shardEta]` — resolves the 32-byte shard-leader-VRF eta for the GIVEN eta-period (Slice S4). The producer calls this once
    *   per `produce(...)` keyed on the checkpoint's own `epoch` arg, so the shard-leader lottery rotates in lockstep with the gl0 eta
    *   instead of being pinned to genesis randomness. Production wiring closes over `etaForPeriod(epoch) ⇒ computeShardEta(shardId,
    *   gl0Eta)` (the per-period rotated gl0 eta from `EtaStateManager.getEta`); tests pass a closure returning a fixed precomputed eta.
    *
    * '''Determinism invariant (Slice S4).''' The eta MUST be resolved for the CHECKPOINT'S epoch (the `epoch` arg threaded onto the
    * envelope), NOT the current wall-clock period — a checkpoint produced near an eta boundary may be verified after the boundary, and
    * producer + every verifier must derive the SAME shard-leader eta or `verifyLeader` disagrees and the shard chain stalls. The eta for
    * the checkpoint's epoch is always knowable here: `epoch == rotationPeriod(gl0AnchorOrdinal)` and `eta_N` is fixed at the 2/3-mark of
    * period `N-1` (per `EtaCalculation`), strictly before any ordinal in period `N`.
    * @param sigmaInCommittee
    *   this operator's stake share within the shard committee. Per v1 stable-σ rule (`[[project-216-committee-stake-drift-fix]]`) this is
    *   `1 / K_S`. Production wiring passes the typed value; tests inject directly.
    * @param slotForGl0Anchor
    *   pure function mapping `(gl0AnchorOrdinal)` to the per-shard `Slot` used in the leader VRF draw + maxvalid-tk tiebreaks. The simplest
    *   production wiring is `ord => Slot.unsafeApply(ord.value.value * snapshotsPerSecond)` — the exact mapping is the caller's choice and
    *   depends on the slot-cadence convention. Tests pass identity-ish functions.
    * @param slotGapFor
    *   pure function from `(currentSlot, parentSlotOpt)` returning the LDD slot-gap. Genesis case (no parent): caller supplies a sensible
    *   default — typically the slot itself (matches `EligibilityChecker`'s "first wins always" semantics at the chain seed).
    * @param lddConfig
    *   per-shard LDD config. Production wiring uses `LddConfig.Default` (matches gl0) unless a per-shard tuning is later introduced.
    * @param derivePerMgState
    *   injectable per-MG derivation `(metagraphAddress, includedChain, gl0AnchorOrdinal) => F[Hash]` that re-runs the metagraph's currency
    *   derivation over its full included SC-binary chain and returns the canonical per-MG MPT root. The `gl0AnchorOrdinal` (the
    *   checkpoint's own anchor) feeds the fee-required cutover so the producer + verifier agree. Production wiring (S3) closes over
    *   `GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot` — the SAME derivation gl0 uses for metagraph snapshots — so the
    *   committee verifier (`ShardCheckpointGl0AcceptanceManager.reExecuteDerivation`) recomputes a byte-identical `Hash` over the same
    *   inputs. For tests, pass a fake closure that returns a deterministic stub hash per MG.
    *
    * The implicit `Hasher[F]` is required for the canonical preimage hash; `SecurityProvider[F]` is required for the Ed25519 sign path
    * (`Signing.signData`).
    */
  def make[F[_]: Async: Hasher: SecurityProvider](
    shardId: ShardId,
    chainStore: ShardChainStore[F],
    slotLeader: ShardSlotLeader[F],
    publisher: ShardCheckpointPublisher[F],
    selfPeerId: PeerId,
    selfKeyPair: KeyPair,
    selfVrfSk: Array[Byte],
    kesSigner: KesSigner[F],
    shardEtaFor: EtaPeriod => F[Array[Byte]],
    sigmaInCommittee: Ratio,
    slotForGl0Anchor: SnapshotOrdinal => Slot,
    slotGapFor: (Slot, Option[Slot]) => Long,
    lddConfig: LddConfig,
    derivePerMgState: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => F[Hash]
  ): F[ShardCheckpointProducer[F]] = Async[F].delay {
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardCheckpointProducer[$shardId]")

    // Capture the slot-leader VRF cache state — keyed by (slot, gl0AnchorOrdinal). Slice 8 v1 doesn't need cross-call state, but the
    // capture is required because `Slf4jLogger` returns a fresh instance every time and we want one stable logger per producer.
    new ShardCheckpointProducer[F] {

      def produce(
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal,
        epoch: EtaPeriod
      ): F[Option[Signed[ShardCheckpoint]]] =
        // Slice 8 v1: empty input ⇒ nothing to checkpoint. T_alive liveness pings (which permit empty payloads) are deferred to a future
        // slice that introduces an explicit `forceEmptyAlive: Boolean` flag — slice 8 keeps the contract simple.
        if (pendingSnapshots.isEmpty) {
          logger.debug(s"produce: empty pendingSnapshots; nothing to checkpoint").as(None: Option[Signed[ShardCheckpoint]])
        } else {
          // Resolve the parent — `bestTip` `None` ⇒ genesis (parent = Hash.empty, parent ord = Genesis). `perMgTip` is derived from the
          // SAME best tip (empty at genesis), so the chain-link anchor and the parent envelope are read consistently.
          (chainStore.bestTip, chainStore.perMgTip).tupled.flatMap {
            case (bestTipOpt, perMgTip) =>
              val parentHash: Hash = bestTipOpt.map(_.hash).getOrElse(Hash.empty)
              val parentOrd: ShardOrdinal = bestTipOpt.map(_.signed.value.shardOrdinal).getOrElse(ShardOrdinal.Genesis)
              // Parent slot derivation:
              //   - Genesis case: no parent slot (caller's `slotGapFor` handles `None`).
              //   - Non-genesis: the parent envelope itself doesn't carry the parent's slot; we approximate by reading the parent's
              //     `gl0AnchorOrdinal` and applying the same `slotForGl0Anchor` mapping the producer uses for `currentSlot`. This keeps
              //     slot-derivation routed through one pure function rather than scattering Slot ⇄ ord conversions across the codebase.
              val parentSlotOpt: Option[Slot] = bestTipOpt.map(t => slotForGl0Anchor(t.signed.value.gl0AnchorOrdinal))
              val nextShardOrdinal: ShardOrdinal = parentOrd.next
              val currentSlot: Slot = slotForGl0Anchor(gl0AnchorOrdinal)
              val slotGap: Long = slotGapFor(currentSlot, parentSlotOpt)

              // R-2: chain-link-order the buffered binaries off the shard's OWN tip (NOT gl0's). MGs with no admissible chain this round
              // are omitted; if NOTHING chains off the tip, there is nothing to checkpoint → return None (don't emit an empty checkpoint
              // and don't burn the slot lottery on it). This is the inversion's core: the admissible set comes from the shard's prior
              // checkpoint, not gl0's post-chain-link `stateChannelSnapshots`.
              chainLinkOrder(pendingSnapshots, perMgTip).flatMap { orderedSnapshots =>
                if (orderedSnapshots.isEmpty)
                  logger
                    .debug(
                      s"produce: ${pendingSnapshots.size} MG(s) buffered but none chain-link off the shard tip at " +
                        s"gl0Anchor=${gl0AnchorOrdinal.value.value}; nothing to checkpoint this round"
                    )
                    .as(None: Option[Signed[ShardCheckpoint]])
                else
                  // Slice S4: resolve the shard-leader-VRF eta for the CHECKPOINT'S OWN `epoch` (the one stamped on the envelope below),
                  // NOT a wall-clock period. `epoch == rotationPeriod(gl0AnchorOrdinal)`, and `eta_epoch` is fixed at the 2/3-mark of the
                  // prior period (`EtaCalculation`), so it is knowable here and every verifier — who keys the same lookup on the
                  // wire-carried `checkpoint.epoch` — derives byte-identical bytes. This is the load-bearing determinism invariant: if
                  // producer + verifier disagreed on shardEta, `ShardSlotLeader.verifyLeader` would reject and the shard chain stalls.
                  shardEtaFor(epoch).flatMap { shardEta =>
                    slotLeader
                      .isLeader(selfVrfSk, shardEta, currentSlot, slotGap, sigmaInCommittee, lddConfig)
                      .flatMap {
                        case None =>
                          // Not the slot leader — per design doc §6.3, other committee members attest later via gossip (slice 14).
                          logger
                            .debug(
                              s"produce: not slot leader at slot=${currentSlot.value.value} gl0Anchor=${gl0AnchorOrdinal.value.value}; " +
                                s"deferring to gossip-path attestation (slice 14)"
                            )
                            .as(None: Option[Signed[ShardCheckpoint]])

                        case Some((vrfProof, _)) =>
                          // Won the lottery — build the checkpoint, sign it, publish it.
                          for {
                            delta <- assembleDelta(orderedSnapshots, gl0AnchorOrdinal)
                            checkpoint = ShardCheckpoint(
                              shardId = shardId,
                              parentCheckpointHash = parentHash,
                              shardOrdinal = nextShardOrdinal,
                              gl0AnchorOrdinal = gl0AnchorOrdinal,
                              derivedStateDelta = delta,
                              emittedReceipts = List.empty,
                              // Placeholder — populated below by replacing with the real committee-member sig. NonEmptyList requires at
                              // least one element to construct; we use a throwaway sentinel and overwrite in `.copy(...)`. Cleaner than
                              // threading the signing into the case-class constructor.
                              committeeSignatures = NonEmptyList.of(placeholderSig),
                              epoch = epoch
                            )
                            // Compute the canonical preimage hash via Hasher[F]. This is the bytes every committee member signs (§3.3).
                            preimageHash <- Hasher[F].hash(checkpoint.signingPreimage)
                            // Sign with Ed25519 long-term + KES product. Both sign the canonical preimage hash's UTF-8 bytes (`getBytes`
                            // matches `MetagraphCommitteeGate.messageBytes` — Hasher result's UTF-8 byte form is what other sign paths use).
                            msgBytes = preimageHash.getBytes
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
                            finalCheckpoint = checkpoint.copy(committeeSignatures = NonEmptyList.of(committeeSig))
                            // Wrap in Signed envelope. The outer Signed contract uses the operator's long-term Ed25519 signature over the
                            // envelope's value bytes; this is the canonical "this operator authored this message" attestation that gl0 and
                            // other peers use to authenticate the gossip path. Mirrors how `GlobalIncrementalSnapshot` is signed.
                            proof <- SignatureProof.fromHash(selfKeyPair, preimageHash)
                            signedCheckpoint = Signed(finalCheckpoint, NonEmptySet.of(proof))
                            _ <- publisher.publish(signedCheckpoint)
                            _ <- logger.info(
                              s"produce: emitted shardOrdinal=${nextShardOrdinal.value} gl0Anchor=${gl0AnchorOrdinal.value.value} " +
                                s"slot=${currentSlot.value.value} mgs=${orderedSnapshots.keys.size} kesStep=$kesStep"
                            )
                          } yield Some(signedCheckpoint): Option[Signed[ShardCheckpoint]]
                      }
                  }
              }
          }
        }

      /** Build the [[ShardDerivedStateDelta]] from the already chain-link-ordered per-MG snapshots.
        *
        * '''Scope''': `perMetagraphMptRoots` (from the injectable re-exec callback) and `includedSnapshots` (the chain-ordered chains) are
        * populated. The other fields (`tokenLockBalancesDelta`, `perMetagraphArtifacts`, `perMetagraphSyncDataDelta`) are left empty —
        * those per-MG derivations migrate from gl0 to the shard in a later slice.
        *
        * '''Input is pre-ordered (R-2).''' `orderedSnapshots` has already been chain-link-ordered by [[chainLinkOrder]] off the shard's own
        * `perMgTip`, so each MG's `NonEmptyList` is strictly parent→child. `derivePerMgState` is therefore re-executed over a correct
        * chain, and `includedSnapshots` carries the SAME ordered chain gl0 adopts / re-derives.
        */
      private def assembleDelta(
        orderedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        gl0AnchorOrdinal: SnapshotOrdinal
      ): F[ShardDerivedStateDelta] =
        // For each MG, call the injectable `derivePerMgState(mg, includedChain, gl0AnchorOrdinal)` over the FULL per-MG chain (S3). The
        // closure re-runs the metagraph's currency derivation (`GlobalSnapshotStateChannelEventsProcessor.deriveMetagraphRoot`)
        // head-to-tail and returns the canonical per-MG MPT root. Passing the whole ordered `NonEmptyList` (not just the head) re-executes
        // a multi-binary window correctly; passing the wire-carried `gl0AnchorOrdinal` makes the fee cutover byte-identical to what the gl0
        // verifier's `reExecuteDerivation` re-runs over the same `includedSnapshots(mg)`.
        orderedSnapshots.toList.traverse {
          case (mg, snaps) =>
            derivePerMgState(mg, snaps, gl0AnchorOrdinal).map(h => mg -> h)
        }.map { perMg =>
          ShardDerivedStateDelta(
            perMetagraphMptRoots = SortedMap.from(perMg),
            includedSnapshots = orderedSnapshots,
            tokenLockBalancesDelta = SortedMap.empty,
            perMetagraphArtifacts = SortedMap.empty,
            perMetagraphSyncDataDelta = SortedMap.empty
          )
        }

      /** Chain-link-order the buffered binaries off the shard's own prior-checkpoint per-MG tip (EXECUTION-SHARDING design R-2).
        *
        * For each metagraph, anchored on `perMgTip.getOrElse(mg, Hash.empty)` (genesis = `Hash.empty`), unfold the longest parent→child
        * chain over `pendingSnapshots(mg)`: the first binary must carry `lastSnapshotHash == anchor`, the next must reference the first's
        * `Hasher[F]` hash, and so on. This is the SAME unfold `GlobalSnapshotStateChannelAcceptanceManager.selectStateChannels` runs, but
        * (a) on raw `Signed[StateChannelSnapshotBinary]` rather than gl0's `StateChannelOutputWithHash`, and (b) anchored on the SHARD'S
        * own tip, NOT gl0's `lastStateChannelSnapshotHashes`. The node-local firstSeen / pull-delay registry is intentionally NOT brought
        * over — single leader, no multi-proposer convergence needed (see [[ShardBinaryBuffer]] determinism note).
        *
        * A metagraph with no binary chaining off its tip this round is OMITTED from the result. When multiple binaries share the same
        * parent (honest metagraphs produce a linear chain, so this is rare), the one with the most signatures wins, ties broken by lowest
        * canonical hash — a deterministic choice so the leader's own re-derivation is well-defined.
        */
      private def chainLinkOrder(
        pendingSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        perMgTip: SortedMap[Address, Hash]
      ): F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] = {
        import io.constellationnetwork.security.signature.Signed.SignedOps
        pendingSnapshots.toList.traverse {
          case (mg, binaries) =>
            val anchor = perMgTip.getOrElse(mg, Hash.empty)
            // Hash each candidate once, then group by the parent it references (its `lastSnapshotHash`).
            binaries.toList
              .traverse(b => SignedOps(b).toHashed[F].map(h => (h.hash, b)))
              .map { hashed =>
                val byParent: Map[Hash, List[(Hash, Signed[StateChannelSnapshotBinary])]] =
                  hashed.groupBy(_._2.value.lastSnapshotHash)

                // Unfold the chain from the anchor; deterministic pick when a parent has multiple children.
                @annotation.tailrec
                def unfold(
                  current: Hash,
                  acc: List[Signed[StateChannelSnapshotBinary]]
                ): List[Signed[StateChannelSnapshotBinary]] =
                  byParent.get(current) match {
                    case None | Some(Nil) => acc.reverse
                    case Some(candidates) =>
                      val (pickedHash, pickedBinary) =
                        candidates.sortBy { case (h, b) => (-b.proofs.size, h.value) }.head
                      unfold(pickedHash, pickedBinary :: acc)
                  }

                NonEmptyList.fromList(unfold(anchor, Nil)).map(mg -> _)
              }
        }
          .map(pairs => SortedMap.from(pairs.flatten)(Address.OrderingInstance))
      }

      /** Placeholder committee-member signature used at envelope-construction time; immediately overwritten via `.copy(committeeSignatures
        * \= ...)` after the real signature is computed. The signing flow needs the envelope shape (sans the signature itself) to compute
        * the canonical preimage hash, so this is a chicken-and-egg dance: we build a sentinel envelope, hash its preimage (the preimage
        * excludes `committeeSignatures` per slice 1 design), sign the hash, then replace the sentinel with the real signature.
        *
        * '''Why a constant placeholder instead of `Option[NonEmptyList[...]]`.''' The envelope's `committeeSignatures: NonEmptyList[...]`
        * is consensus-load-bearing (slice 1 scaladoc — "empty would be vacuously safe but is a protocol-violation signal — keep the wire
        * shape forbid it"). Wrapping in `Option` for producer-internal convenience would force every downstream consumer to handle the
        * `None` case. Cleaner to use a placeholder that's never seen outside this method.
        */
      private val placeholderSig: CommitteeMemberSignature =
        CommitteeMemberSignature(
          peerId = selfPeerId,
          vrfProof = Hex(""),
          ed25519Sig = Hex(""),
          kesProductSig = Hex(""),
          kesTreeStep = 0
        )
    }
  }

  /** Convenience: derive a producer's expected output hash without running `produce(...)`. The canonical preimage hash IS the bytes the
    * committee member's signatures cover, and IS the chain-link `parentCheckpointHash` the next checkpoint will reference.
    *
    * Exposed for tests / callers that want to track the produced hash without re-deriving the preimage projection at the call site. Pure
    * delegation to `checkpoint.signingPreimage` + `Hasher[F].hash`.
    */
  def hashOf[F[_]: Hasher](checkpoint: ShardCheckpoint): F[Hash] =
    Hasher[F].hash(checkpoint.signingPreimage)

  /** Convenience: pull out the (hash, ordinal) of a produced envelope's canonical signing target. Returns the same `Hashed` shape the
    * chain-store consumes. Exposed for tests / callers that want to inspect the produced envelope without re-running the hash derivation.
    *
    * NOT load-bearing for the producer's main flow (the publisher just gets the `Signed[ShardCheckpoint]` envelope); separate convenience
    * helper for symmetry with `ShardChainStore.StoredShardCheckpoint.toHashed`.
    */
  def hashed[F[_]: Async: Hasher](signed: Signed[ShardCheckpoint]): F[Hashed[ShardCheckpoint]] =
    for {
      hash <- Hasher[F].hash(signed.value.signingPreimage)
      proofsHash <- {
        import io.constellationnetwork.security.signature.Signed.SignedOps
        SignedOps(signed).proofsHash[F]
      }
    } yield Hashed(signed, hash, proofsHash)

  // Unused convenience for matching against `SortedSet`-style codecs in test fixtures. Kept here so test files can refer to
  // `ShardCheckpointProducer.sortedSet(...)` for readability without re-importing in three places.
  private[sharding] def sortedSet[A: Ordering](xs: A*): SortedSet[A] = SortedSet(xs: _*)
}
