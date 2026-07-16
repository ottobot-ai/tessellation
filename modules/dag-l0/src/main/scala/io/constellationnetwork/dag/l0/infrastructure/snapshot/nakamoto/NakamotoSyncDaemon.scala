package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.security.KeyPair

import cats.effect.kernel._
import cats.effect.std._
import cats.effect.syntax.all._
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.{DAGEvent, GlobalSnapshotEvent}
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.node.NodeStorage
import io.constellationnetwork.node.shared.domain.snapshot.storage.{LastNGlobalSnapshotStorage, LastSnapshotStorage, SnapshotStorage}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.{GossipStream, SidecarClient}
import io.constellationnetwork.node.shared.infrastructure.mempool.EventMempool
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  ShardCheckpointAcceptResult,
  VerifiedShardCheckpoint,
  VerifiedShardCheckpointFailure
}
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{LddConfig, TipAttestation => DomainTipAttestation}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.EcVrf25519

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.grpc.ManagedChannel
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Subscribes to sidecar GossipSub for live snapshots and attestations.
  *
  * Uses NakamotoChainStore for fork-aware storage instead of raw SnapshotStorage.prepend. Records attestations in TipTracker for finality
  * tracking.
  */
object NakamotoSyncDaemon {

  private val CatchUpThreshold = 6L

  // #259 metagraph-binary active-recovery (stuck-detection tick) tuning. Fixed internal cadence —
  // NOT a consensus-critical value (recovery is additive + re-gated), so kept as plain constants
  // rather than HOCON config (mirrors CatchUpThreshold above).
  //   - Tick every 20s: the orphan-buffer pending-parents poll cadence.
  //   - A parent must persist across ≥2 ticks (≥~20s genuinely stuck, not a mid-drain blip) before
  //     we fetch — encoded as `consecutiveTicks >= 2` at the call site.
  //   - After a fetch attempt, don't refetch the same (mg, parentHash) for 60s even if still pending.
  private val StuckRefetchTickInterval: scala.concurrent.duration.FiniteDuration = 20.seconds
  private val StuckRefetchCooldown: scala.concurrent.duration.FiniteDuration = 60.seconds

  // Recovery is node-local scheduling, not artifact validity. These hard bounds prevent an authenticated
  // committee key from turning one orphan lineage into unbounded recursion or retained protobuf memory.
  private val MaxShardCheckpointAncestryRecoveryDepth: Int = 16
  private val MaxPendingShardCheckpointCount: Int = 64
  private val MaxPendingShardCheckpointBytes: Long = 16L * 1024L * 1024L

  // Local scheduling bounds only; none of these values participates in artifact validity or execution.
  // Each subscription attempt allocates fresh lanes, and `runWorkerGeneration` owns every worker for
  // exactly that attempt. A reconnect therefore cannot leave an old handler mutating current state.
  private val SnapshotIntakeCapacity = 1024
  private val TipAttestationCapacity = 4096
  private val MetagraphBinaryCapacity = 256
  private val MetagraphAttestationCapacity = 4096
  private val NativeBlockCapacity = 1024
  private val ShardCheckpointCapacity = 256
  private val ShardCheckpointAttestationCapacity = 4096
  private val FraudProofCapacity = 256

  // The upstream GossipStream permits at most 32 MiB per encoded envelope and 64 MiB per
  // subscription-generation callback buffer. Each variable-payload lane repeats that 64 MiB
  // byte budget, so it can retain at most two maximum-size envelopes. The three cryptographic
  // attestation lanes carry fixed-shape hashes/keys/proofs/signatures (KES is ~700 bytes) and
  // reserve 8 MiB each with a fail-closed 64 KiB per-envelope ceiling. The aggregate retained
  // encoded-byte ceiling for all eight daemon lanes is therefore 344 MiB, independent of item count.
  private val VariableLaneByteCapacity = GossipStream.BufferLimits.Default.maxQueuedBytes
  private val VariableMessageMaxBytes = GossipStream.BufferLimits.Default.maxMessageBytes.toLong
  private val AttestationLaneByteCapacity = 8L * 1024L * 1024L
  private val AttestationMessageMaxBytes = 64L * 1024L

  private val MetagraphBinaryParallelism = 16
  private val MetagraphAttestationParallelism = 8
  private val NativeBlockParallelism = 8
  private val ShardCheckpointParallelism = 4
  private val ShardCheckpointAttestationParallelism = 8
  private val FraudProofParallelism = 4

  /** One bounded, generation-owned worker lane.
    *
    * The shared subscription demultiplexer must never block on a full family lane: a metagraph-binary handler can be waiting for an
    * attestation that is later on that same subscription stream. Exposing only `tryOffer` makes overload rejection explicit and prevents
    * cross-family deadlock/head-of-line blocking. Once sealed, the lane rejects new offers and lets every previously accepted queued or
    * in-flight handler finish before the subscription generation is replaced. Canceling the generation owner still cancels the lane
    * immediately.
    */
  private[nakamoto] sealed trait GenerationWorkerOfferResult

  private[nakamoto] object GenerationWorkerOfferResult {
    case object Accepted extends GenerationWorkerOfferResult
    final case class MessageTooLarge(encodedBytes: Long, maxMessageBytes: Long) extends GenerationWorkerOfferResult
    final case class ByteCapacityExceeded(encodedBytes: Long, maxQueuedBytes: Long) extends GenerationWorkerOfferResult
    final case class ItemCapacityExceeded(maxQueuedItems: Int) extends GenerationWorkerOfferResult
    final case class InvalidEncodedSize(encodedBytes: Long) extends GenerationWorkerOfferResult
    case object GenerationClosed extends GenerationWorkerOfferResult
  }

  private[nakamoto] final case class GenerationWorkerFailed(underlying: Throwable)
      extends RuntimeException("generation worker failed", underlying)

  private[nakamoto] trait GenerationWorker[F[_]] {
    def seal: F[Unit]
    def awaitDrained: F[Unit]
    def awaitFailure: F[Throwable]
    def run: fs2.Stream[F, Unit]
  }

  private[nakamoto] final case class GenerationWorkerLane[F[_], A](
    tryOffer: A => F[GenerationWorkerOfferResult],
    seal: F[Unit],
    awaitDrained: F[Unit],
    awaitFailure: F[Throwable],
    run: fs2.Stream[F, Unit]
  ) extends GenerationWorker[F]

  private[nakamoto] object GenerationWorkerLane {
    private final case class Reserved[A](value: A, encodedBytes: Long)
    private final case class LaneState(accepting: Boolean, outstanding: Long)

    def bounded[F[_]: Async, A](
      capacity: Int,
      maxConcurrent: Int,
      maxQueuedBytes: Long,
      maxMessageBytes: Long
    )(
      encodedSize: A => Long
    )(handle: A => F[Unit]): F[GenerationWorkerLane[F, A]] =
      for {
        _ <- Async[F].raiseWhen(capacity <= 0)(new IllegalArgumentException(s"worker lane capacity must be positive: $capacity"))
        _ <- Async[F].raiseWhen(maxConcurrent <= 0)(
          new IllegalArgumentException(s"worker lane concurrency must be positive: $maxConcurrent")
        )
        _ <- Async[F].raiseWhen(maxQueuedBytes <= 0L)(
          new IllegalArgumentException(s"worker lane byte capacity must be positive: $maxQueuedBytes")
        )
        _ <- Async[F].raiseWhen(maxMessageBytes <= 0L || maxMessageBytes > maxQueuedBytes)(
          new IllegalArgumentException(
            s"worker lane message limit must be positive and <= byte capacity: message=$maxMessageBytes capacity=$maxQueuedBytes"
          )
        )
        queue <- Queue.bounded[F, Reserved[A]](capacity)
        bytePermits <- Semaphore[F](maxQueuedBytes)
        state <- Ref.of[F, LaneState](LaneState(accepting = true, outstanding = 0L))
        admissionMutex <- Mutex[F]
        drained <- Deferred[F, Unit]
        failure <- Deferred[F, Throwable]
      } yield {
        def completeDrainWhen(shouldComplete: Boolean): F[Unit] =
          Async[F].whenA(shouldComplete)(drained.complete(()).void)

        def finishReservation(reserved: Reserved[A]): F[Unit] =
          Async[F].uncancelable { _ =>
            bytePermits.releaseN(reserved.encodedBytes) >>
              state.modify { current =>
                if (current.outstanding <= 0L)
                  (
                    current,
                    Left[Throwable, Boolean](
                      new IllegalStateException("generation worker outstanding reservation underflow")
                    )
                  )
                else {
                  val next = current.copy(outstanding = current.outstanding - 1L)
                  (next, Right(!next.accepting && next.outstanding == 0L))
                }
              }
                .flatMap(_.liftTo[F])
                .flatMap(completeDrainWhen)
          }

        GenerationWorkerLane(
          value =>
            admissionMutex.lock.surround {
              Async[F].uncancelable { _ =>
                state.get.flatMap {
                  case LaneState(false, _) =>
                    Async[F].pure(GenerationWorkerOfferResult.GenerationClosed)
                  case LaneState(true, _) =>
                    Async[F].delay(encodedSize(value)).flatMap { rawBytes =>
                      val chargedBytes = rawBytes.max(1L)
                      if (rawBytes < 0L)
                        Async[F].pure(GenerationWorkerOfferResult.InvalidEncodedSize(rawBytes))
                      else if (chargedBytes > maxMessageBytes)
                        Async[F].pure(GenerationWorkerOfferResult.MessageTooLarge(chargedBytes, maxMessageBytes))
                      else
                        bytePermits.tryAcquireN(chargedBytes).flatMap {
                          case false =>
                            Async[F].pure(GenerationWorkerOfferResult.ByteCapacityExceeded(chargedBytes, maxQueuedBytes))
                          case true =>
                            state.update(current => current.copy(outstanding = current.outstanding + 1L)) >>
                              queue.tryOffer(Reserved(value, chargedBytes)).flatMap {
                                case true => Async[F].pure(GenerationWorkerOfferResult.Accepted)
                                case false =>
                                  state.update(current => current.copy(outstanding = current.outstanding - 1L)) >>
                                    bytePermits
                                      .releaseN(chargedBytes)
                                      .as(GenerationWorkerOfferResult.ItemCapacityExceeded(capacity))
                              }
                        }
                    }
                }
              }
            },
          admissionMutex.lock.surround {
            Async[F].uncancelable { _ =>
              state.modify { current =>
                val sealedState = current.copy(accepting = false)
                (sealedState, sealedState.outstanding == 0L)
              }
                .flatMap(completeDrainWhen)
            }
          },
          drained.get,
          failure.get,
          fs2.Stream
            .fromQueueUnterminated(queue)
            .parEvalMapUnordered(maxConcurrent)(reserved =>
              handle(reserved.value)
                .onError(error => failure.complete(error).void)
                .guarantee(finishReservation(reserved))
            )
        )
      }
  }

  /** Couple all workers to one subscription generation. Clean or errored source termination seals admission and drains every accepted
    * queued/in-flight item before the generation exits; a source error is rethrown only after that drain. An unexpected worker termination
    * fails the generation immediately and resource finalization cancels the source and sibling workers. Canceling the outer owner also
    * cancels the complete resource scope immediately, so no worker can outlive its generation.
    */
  private[nakamoto] def runWorkerGeneration[F[_]: Async](
    source: fs2.Stream[F, Unit],
    workers: List[GenerationWorker[F]]
  ): fs2.Stream[F, Unit] = {
    val ownedGeneration: Resource[F, (Deferred[F, Either[Throwable, Unit]], Deferred[F, Throwable])] =
      for {
        sourceExit <- Resource.eval(Deferred[F, Either[Throwable, Unit]])
        workerFailure <- Resource.eval(Deferred[F, Throwable])
        _ <- source.compile.drain.attempt.flatMap(sourceExit.complete(_).void).background
        _ <- workers.traverse_ { worker =>
          worker.awaitFailure.flatMap(workerFailure.complete(_).void).background.void >>
            worker.run.compile.drain.attempt.flatMap {
              case Left(error) => workerFailure.complete(error).void
              case Right(()) =>
                workerFailure
                  .complete(new IllegalStateException("generation worker terminated before its owning generation"))
                  .void
            }.background.void
        }
      } yield (sourceExit, workerFailure)

    fs2.Stream.resource(ownedGeneration).flatMap {
      case (sourceExit, workerFailure) =>
        fs2.Stream.eval {
          Async[F].race(sourceExit.get, workerFailure.get).flatMap {
            case Right(error) => GenerationWorkerFailed(error).raiseError[F, Unit]
            case Left(sourceResult) =>
              workers.traverse_(_.seal) >>
                Async[F].race(workers.traverse_(_.awaitDrained), workerFailure.get).flatMap {
                  case Right(error) => GenerationWorkerFailed(error).raiseError[F, Unit]
                  case Left(())     => sourceResult.liftTo[F]
                }
          }
        }
    }
  }

  private sealed trait NativeBlockIngress

  private object NativeBlockIngress {
    final case class AllowSpend(value: pb.AllowSpendBlock) extends NativeBlockIngress
    final case class Dag(value: pb.DAGBlock) extends NativeBlockIngress
    final case class TokenLock(value: pb.TokenLockBlock) extends NativeBlockIngress

    def encodedSize(value: NativeBlockIngress): Long = value match {
      case AllowSpend(block) => block.serializedSize.toLong
      case Dag(block)        => block.serializedSize.toLong
      case TokenLock(block)  => block.serializedSize.toLong
    }
  }

  /** Per-(metagraph, parentHash) bookkeeping for the #259 stuck-detection tick. `consecutiveTicks` counts how many consecutive ticks this
    * parent has stayed pending in the orphan buffer (reset to 0 — by omission from the carry-forward map — once it resolves);
    * `lastFetchAtMillis` is the wall-clock ms of the last active fetch attempt, used for the per-pair refetch cooldown. Wall-clock here is
    * fine: it gates a best-effort recovery cadence, not consensus.
    */
  final case class StuckParentState(consecutiveTicks: Int, lastFetchAtMillis: Long)

  // Confirmation depth k₁ — same as SnapshotLeaderLoop.ConfirmationDepthK; the boundary between Tier 2 (sequential
  // walk-back) and Tier 3 (full catch-up + backfill): gaps > k mean the network has finalized past our tip, so
  // sequential fetch won't work. NO module-level sys.env read here anymore — the value is threaded in as the
  // `confirmationDepthK` parameter of `run` -> `handleSnapshot` from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value`
  // (project rule: HOCON over scattered env reads).

  private val vrf = EcVrf25519.default

  /** Decode the greenfield ChainSync payload shape. `snapshot` is required and `context` is optional; the latter is transport metadata
    * only. The caller requires a validated parent context and runs full replay before any chain or MPT write.
    */
  private[nakamoto] def decodeFetchedSnapshotPayload(
    payload: Array[Byte]
  ): Option[Signed[GlobalIncrementalSnapshot]] =
    if (payload.isEmpty) None
    else
      io.circe.parser
        .parse(new String(payload, java.nio.charset.StandardCharsets.UTF_8))
        .toOption
        .flatMap(_.hcursor.get[Signed[GlobalIncrementalSnapshot]]("snapshot").toOption)

  /** Bind every routing/eligibility field in the protobuf envelope to the signed snapshot body before the envelope can select a parent,
    * borrow a producer's stake, pass KES, or provide chain-store metadata. The sidecar treats protobuf fields as opaque and
    * unauthenticated; the signed body and its slot certificate are the only source of truth.
    */
  private[nakamoto] def validateSnapshotEnvelope[F[_]: Async: HasherSelector](
    snap: pb.Snapshot,
    signedSnapshot: Signed[GlobalIncrementalSnapshot]
  ): F[Either[String, Unit]] =
    HasherSelector[F].withCurrent { implicit hasher =>
      signedSnapshot.toHashed[F].map { hashed =>
        val body = signedSnapshot.value
        val transportHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val transportParent = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val producerBytes = snap.producerId.toByteArray
        val signerMatches = signedSnapshot.proofs.toNonEmptyList.exists { proof =>
          java.util.Arrays.equals(proof.id.hex.toBytes, producerBytes)
        }

        def sameBytes(left: Array[Byte], right: Array[Byte]): Boolean =
          java.util.Arrays.equals(left, right)

        val commonChecks = List(
          Either.cond(transportHash === hashed.hash, (), s"transport hash ${transportHash.value} != signed body hash ${hashed.hash.value}"),
          Either.cond(
            snap.ordinal == body.ordinal.value.value,
            (),
            s"transport ordinal ${snap.ordinal} != signed ordinal ${body.ordinal.value.value}"
          ),
          Either.cond(
            transportParent === body.lastSnapshotHash,
            (),
            s"transport parent ${transportParent.value} != signed parent ${body.lastSnapshotHash.value}"
          ),
          Either.cond(signerMatches, (), "transport producer_id is not a signer of the snapshot body")
        )

        val certificateChecks = body.slotCertificate match {
          case None =>
            List(Left("signed snapshot is missing its required slot certificate"))
          case Some(cert) =>
            val derivedVrfOutput = vrf.vrfProofToHash(snap.vrfProof.toByteArray)
            List(
              Either
                .cond(cert.slot.value.value == snap.slot, (), s"certificate slot ${cert.slot.value.value} != transport slot ${snap.slot}"),
              Either.cond(
                cert.parentSlot.value.value == snap.parentSlot,
                (),
                s"certificate parentSlot ${cert.parentSlot.value.value} != transport parentSlot ${snap.parentSlot}"
              ),
              Either.cond(sameBytes(cert.vrfProof.toBytes, snap.vrfProof.toByteArray), (), "certificate VRF proof != transport proof"),
              Either.cond(
                sameBytes(cert.vrfPublicKey.toBytes, snap.vrfPublicKey.toByteArray),
                (),
                "certificate VRF public key != transport key"
              ),
              Either.cond(
                Hex.fromBytes(snap.eta.toByteArray).value == cert.eta.value,
                (),
                "certificate eta != transport eta"
              ),
              Either.cond(body.eta.contains(cert.eta), (), "snapshot eta != slot-certificate eta"),
              Either.cond(
                derivedVrfOutput.exists(sameBytes(_, cert.vrfOutput.toBytes)),
                (),
                "certificate VRF output is not derived from its proof"
              )
            )
        }

        (commonChecks ++ certificateChecks).collectFirst { case Left(reason) => reason }.toLeft(())
      }
    }

  /** S3 attestation-inversion gate: decides whether a re-exec/validated `ShardCheckpoint` may be adopted as best-tip and attested.
    *
    * Mirrors the global chain's validate-by-replay → adopt → attest discipline: a node adopts (+ counts the signers' attestations + emits
    * its own attestation) ONLY for a checkpoint that passed `ShardCheckpointGl0AcceptanceManager.evaluate`'s signer checks and mandatory
    * replay. A `Rejected` (pre-check/unavailable-replay failure) or `RejectedReExecutionMismatch` (wrong derivation) checkpoint is NOT
    * admissible — never adopted, signers never counted toward quorum, no attestation emitted. `PendingMoreAttestations` is admissible
    * (replay-valid but still waiting for enough independently replaying committee signatures).
    *
    * Pure + package-visible so the inversion gate is unit-testable without standing up the full gossip handler.
    */
  private[nakamoto] def shardCheckpointAdmissible(result: ShardCheckpointAcceptResult): Boolean =
    result match {
      case ShardCheckpointAcceptResult.Accepted                          => true
      case ShardCheckpointAcceptResult.PendingMoreAttestations           => true
      case ShardCheckpointAcceptResult.Rejected(_)                       => false
      case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(_, _) => false
    }

  /** Only an affirmative local reproduction mismatch is eligible to trigger construction of portable fraud evidence. A plain rejection
    * includes unavailable replay state and every structural/authentication failure, none of which says that an authenticated execution
    * quorum signed an incorrect root. The emitter independently requires the complete certificate before it publishes.
    */
  private[nakamoto] def shardCheckpointFraudEvidenceEligible(failure: VerifiedShardCheckpointFailure): Boolean =
    failure match {
      case _: VerifiedShardCheckpointFailure.ReExecutionMismatch => true
      case _: VerifiedShardCheckpointFailure.Rejected            => false
    }

  /** Run checkpoint recovery work only after the claimed producer has passed the cheap, parent-independent committee signature gate.
    *
    * Missing-parent fetch, replay, and storage can all consume substantially more resources than this check. Keeping the continuation
    * behind an `Either` makes it impossible for an unauthenticated or noncommittee envelope to trigger any of those effects. Full
    * producer-duty, quorum, and replay validation still run after the exact parent is available.
    */
  private[nakamoto] def afterCheckpointProducerPreAuthorization[F[_]: cats.Monad, A](
    verification: F[Either[String, Unit]]
  )(authorized: => F[A]): F[Either[String, A]] =
    verification.flatMap {
      case Left(reason) => (Left(reason): Either[String, A]).pure[F]
      case Right(())    => authorized.map(a => Right(a): Either[String, A])
    }

  /** Do not enter producer-duty validation or framework replay until bounded recovery proved the exact parent is locally available. The
    * recovery effect may recursively process ancestors; only its final boolean selects the validation continuation.
    */
  private[nakamoto] def afterCheckpointParentRecovery[F[_]: cats.Monad, A](
    parentReady: F[Boolean]
  )(defer: => F[A], validateAndReplay: => F[A]): F[A] =
    parentReady.flatMap {
      case false => defer
      case true  => validateAndReplay
    }

  private[nakamoto] final case class PendingCheckpointChild[A](
    missingParent: Hash,
    childId: Hash,
    child: A,
    retainedBytes: Long
  )

  private[nakamoto] final case class PendingCheckpointChildren[A](
    entries: Vector[PendingCheckpointChild[A]],
    retainedBytes: Long
  )

  private[nakamoto] object PendingCheckpointChildren {
    def empty[A]: PendingCheckpointChildren[A] = PendingCheckpointChildren(Vector.empty, 0L)
  }

  private[nakamoto] final case class PendingCheckpointBufferResult(buffered: Boolean, duplicate: Boolean, evicted: Int)

  /** Retain an authenticated child until its exact parent is stored. The buffer is FIFO bounded by both item count and encoded bytes;
    * duplicate signing-preimage hashes do not consume another slot. This is recovery scheduling only: draining always re-enters the full
    * producer-duty, signature, replay, and storage path.
    */
  private[nakamoto] def bufferBoundedPendingCheckpointChild[F[_], A](
    missingParent: Hash,
    childId: Hash,
    child: A,
    retainedBytes: Long,
    maxCount: Int,
    maxBytes: Long,
    pendingRef: Ref[F, PendingCheckpointChildren[A]]
  ): F[PendingCheckpointBufferResult] =
    pendingRef.modify { state =>
      val normalizedBytes = retainedBytes.max(0L)
      if (state.entries.exists(_.childId === childId))
        (state, PendingCheckpointBufferResult(buffered = false, duplicate = true, evicted = 0))
      else if (maxCount <= 0 || maxBytes <= 0L || normalizedBytes > maxBytes)
        (state, PendingCheckpointBufferResult(buffered = false, duplicate = false, evicted = 0))
      else {
        val appended = state.entries :+ PendingCheckpointChild(missingParent, childId, child, normalizedBytes)

        @annotation.tailrec
        def trim(
          entries: Vector[PendingCheckpointChild[A]],
          bytes: Long,
          evicted: Int
        ): (Vector[PendingCheckpointChild[A]], Long, Int) =
          if (entries.size <= maxCount && bytes <= maxBytes) (entries, bytes, evicted)
          else
            entries.headOption match {
              case None       => (Vector.empty, 0L, evicted)
              case Some(head) => trim(entries.tail, bytes - head.retainedBytes, evicted + 1)
            }

        val (kept, keptBytes, evicted) = trim(appended, state.retainedBytes + normalizedBytes, 0)
        val childRetained = kept.exists(_.childId === childId)
        (
          PendingCheckpointChildren(kept, keptBytes),
          PendingCheckpointBufferResult(buffered = childRetained, duplicate = false, evicted = evicted)
        )
      }
    }

  /** Atomically remove the authenticated children waiting on `storedParent`; callers process the returned values through the ordinary
    * checkpoint handler. A child that still lacks deeper ancestry is re-buffered under that exact missing hash.
    */
  private[nakamoto] def drainBoundedPendingCheckpointChildren[F[_], A](
    storedParent: Hash,
    pendingRef: Ref[F, PendingCheckpointChildren[A]]
  ): F[List[A]] =
    pendingRef.modify { state =>
      val (ready, waiting) = state.entries.partition(_.missingParent === storedParent)
      val waitingBytes = waiting.foldLeft(0L)(_ + _.retainedBytes)
      (PendingCheckpointChildren(waiting, waitingBytes), ready.iterator.map(_.child).toList)
    }

  /** Signed-byte-store backfill transport (2026-07-09) — pull a peer's SIGNED MPT byte map at an EXACT finalized `ordinal` over the
    * by-ordinal `/global-snapshots/<ord>/mpt-entries` route. Consumed by `PinnedCurrencyInfoReader.PinnedByteBackfill` to heal HOLES in the
    * local signed store at the ordinal component of a stamped shard-checkpoint execution base.
    *
    * TRANSPORT-ONLY, deliberately UNVERIFIED here: the pinned reader verifies `consensusMptRoot(fetched) === its OWN locally-committed
    * `stateProof.mptRoot@ordinal`` before anything is staged or served. Byte integrity comes from that root gate, not the transport. Tries
    * up to `maxPeers` responsive peers (sorted by peer id for stable behavior — the VERIFIED outcome is peer-independent, root-determined)
    * with a per-try `perPeerTimeout` so a hung peer cannot stall the accept fold; a peer without the ordinal 404s and the next is tried.
    * `None` on total miss — the caller stays fail-closed. `maxPeers = 0` disables the transport outright (config kill-switch). Never
    * raises.
    */
  private[snapshot] def pullMptEntriesAtOrdinalFromPeer[F[_]: Async](
    l0GlobalSnapshotClient: io.constellationnetwork.node.shared.http.p2p.clients.L0GlobalSnapshotClient[F],
    clusterStorage: io.constellationnetwork.node.shared.domain.cluster.storage.ClusterStorage[F],
    maxPeers: Int,
    perPeerTimeout: scala.concurrent.duration.FiniteDuration,
    logger: org.typelevel.log4cats.Logger[F]
  )(ordinal: SnapshotOrdinal): F[Option[Map[Hex, Array[Byte]]]] = {
    def pullFrom(p: io.constellationnetwork.schema.peer.Peer): F[Option[Map[Hex, Array[Byte]]]] =
      Async[F]
        .timeoutTo(
          l0GlobalSnapshotClient
            .getMptEntriesAt(ordinal)
            .run(io.constellationnetwork.schema.peer.L0Peer.fromPeer(p))
            .map(_.some),
          perPeerTimeout,
          (None: Option[Map[Hex, Array[Byte]]]).pure[F]
        )
        .handleErrorWith { e =>
          logger
            .debug(
              s"[backfill] signed-bytes pull ord=${ordinal.show} from peer ${p.id.show.take(16)} failed (${e.getMessage}), trying next"
            )
            .as(None: Option[Map[Hex, Array[Byte]]])
        }

    def tryInOrder(remaining: List[io.constellationnetwork.schema.peer.Peer]): F[Option[Map[Hex, Array[Byte]]]] =
      remaining match {
        case Nil => (None: Option[Map[Hex, Array[Byte]]]).pure[F]
        case p :: tail =>
          pullFrom(p).flatMap {
            case some @ Some(_) => (some: Option[Map[Hex, Array[Byte]]]).pure[F]
            case None           => tryInOrder(tail)
          }
      }

    if (maxPeers <= 0) (None: Option[Map[Hex, Array[Byte]]]).pure[F]
    else
      clusterStorage.getResponsivePeers
        .flatMap(peers => tryInOrder(peers.toList.sortBy(_.id.show).take(maxPeers)))
        .handleErrorWith(e =>
          logger.warn(e)(s"[backfill] could not pull signed MPT bytes at ord=${ordinal.show} from any peer (fail-closed defer)").as(None)
        )
  }

  /** Append a parent-missing item to the orphan buffer without authorizing any state transition. The only production consumer of the
    * buffered value is [[drainBufferedChildren]], which re-enters the normal validation path after its parent has been stored.
    */
  private[nakamoto] def bufferPendingChild[F[_], A](
    missingParent: Hash,
    child: A,
    pendingParentRef: Ref[F, Map[Hash, List[A]]]
  ): F[Unit] =
    pendingParentRef.update { pending =>
      pending.updated(missingParent, pending.getOrElse(missingParent, List.empty) :+ child)
    }

  /** Delegate to the validator-owned issuer check before exposing replayed payload to the current post-replay receive path. Envelope and
    * KES gates still precede this helper at the sole production call site; replay alone is deliberately not an authenticated signing or
    * preference capability.
    */
  private[nakamoto] def commitReplayValidated[F[_]: cats.Monad](
    result: NakamotoSnapshotValidator.ValidationResult
  )(
    commit: (Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo) => F[Unit]
  ): F[Boolean] =
    NakamotoSnapshotValidator.consumeReplayValidated(result)(commit)

  /** Atomically remove children waiting on `storedHash`, then process them sequentially in insertion order. `processChild` is the normal
    * snapshot handler in production; a child that fails replay never stores and therefore never invokes another drain, leaving its
    * descendants buffered behind the invalid hash.
    */
  private[nakamoto] def drainBufferedChildren[F[_]: cats.Monad, A](
    storedHash: Hash,
    pendingParentRef: Ref[F, Map[Hash, List[A]]]
  )(
    onDrain: List[A] => F[Unit],
    processChild: A => F[Unit]
  ): F[Unit] =
    pendingParentRef.modify { pending =>
      val children = pending.getOrElse(storedHash, List.empty)
      (pending - storedHash, children)
    }.flatMap { children =>
      onDrain(children) >> children.traverse_(processChild)
    }

  /** After storing a snapshot, check if any buffered gossip snapshots were waiting for it as their parent. If so, process them via
    * handleSnapshot (which will now find the parent in the chain store and validate successfully). This creates a validation cascade from
    * the shared genesis ancestor through the gossip chain.
    */
  private def drainPendingChildren[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    storedHash: Hash,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ (Tier-2 vs Tier-3 gap boundary). Forwarded from `run`; sourced from
    // `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` (replaces the prior module-level sys.env read).
    confirmationDepthK: Long,
    consensusFns: GlobalSnapshotConsensusFunctions[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-staged accumulator
    // stripped->canonical and thus promotes on finalize (complete served ring; was per-producer-sparse).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    // 3c-A enabler — signed-bytes staging map. NO LONGER validate-only: the ADOPT paths (reorg / realign / legacy catch-up) now stage
    // their root-verified byte maps here too (signed-byte-store FIDELITY, 2026-07-09 — adopted ordinals must not stay permanent holes in
    // `mpt_snapshot_info_signed`, or exact execution-base reads fail closed on every node that adopted that ordinal).
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    // Hierarchical-shard-checkpoints v1 — per-ord producer fan-out, threaded through `handleSnapshot`
    // so drained children also fan out shard checkpoints on their becameBestTip path. EMPTY / `None` at
    // numShards=1 (regression bar).
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // EXECUTION-SHARDING: per-shard admission-gated binary buffers — the producer fan-out input.
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded through the drain → handleSnapshot cascade.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    drainBufferedChildren(storedHash, pendingParentRef)(
      children =>
        if (children.isEmpty) Async[F].unit
        else
          logger.info(
            s"🔗 Draining ${children.size} buffered snapshot(s) whose parent ${storedHash.value.take(12)} is now available"
          ),
      childSnap =>
        handleSnapshot(
          childSnap,
          stateRef,
          pendingParentRef,
          chainStore,
          nodeStorage,
          tipTracker,
          stakeRegistry,
          operatorKeyRegistry,
          sidecarClient,
          selfId,
          keyPair,
          lddConfig,
          eligibilityChecker,
          lastKnownSlotRef,
          epochStateRef,
          etaRotationSnapshots,
          confirmationDepthK,
          consensusFns,
          snapshotStorage,
          lastGlobalSnapshotStorage,
          lastNGlobalSnapshotStorage,
          productionGate,
          mptStore,
          mptOverlay,
          pendingAccumulatorsRef,
          pendingPostBytesRef,
          eventMempool,
          chainSyncManager,
          channel,
          dataDir,
          operationalKeyMaker,
          shardProducers,
          shardChainStores,
          shardBinaryBuffers,
          shardAssignment,
          shardCommitteeMembership,
          logger
        )
    )

  /** Reconcile DAG events after a replay-validated snapshot becomes the canonical tip.
    *
    * Transactions whose parent no longer matches canonical GL0 state cannot become valid on the selected branch. Other event types remain
    * queued for their own validation paths.
    */
  private def staleMempoolHashes[F[_]: Async](
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    context: GlobalSnapshotInfo
  ): F[Set[Hash]] =
    for {
      hashes <- eventMempool.getEventHashes
      events <- eventMempool.getMultiple(hashes)
      staleHashes = events.collect {
        case (hash, hashed) =>
          hashed.signed.value match {
            case DAGEvent(signedBlock) =>
              val isStale = signedBlock.value.transactions.exists { signedTx =>
                val transaction = signedTx.value
                val canonicalRef = context.lastTxRefs.getOrElse(transaction.source, TransactionReference.empty)
                canonicalRef =!= TransactionReference.empty && canonicalRef =!= transaction.parent
              }
              Option.when(isStale)(hash)
            case _ => None
          }
      }.flatten.toSet
    } yield staleHashes

  final case class SyncState(
    networkTipOrdinal: Long,
    networkTipHash: Option[Hash],
    localTipOrdinal: Long,
    isReady: Boolean,
    lastCatchUpAttemptMs: Long = 0L,
    // Highest ordinal a Tier-3 catch-up has ADOPTED (max-monotone). A pulled child remains inert while its parent is absent, so
    // `chainStore.bestTipOrdinal` can still report the old connected tip. The Tier-3 gap-check maxes bestTip with this watermark to avoid
    // repeatedly requesting the same window while hash-keyed parent recovery connects the gap.
    lastCatchUpAdoptedOrdinal: Long = 0L
  )

  object SyncState {
    def initial: SyncState = SyncState(0L, None, 0L, isReady = false)
  }

  def run[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    channel: ManagedChannel,
    subscriptionReadiness: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarSubscriptionReadiness[F],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ — threaded from `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` at the
    // GlobalSnapshotConsensus.make call site (replaces the prior module-level
    // `sys.env.get("NAKAMOTO_CONFIRMATION_DEPTH")` read; project rule: HOCON over scattered env reads).
    // Used as the Tier-2 (sequential walk-back) vs Tier-3 (full catch-up) gap boundary in `handleSnapshot`.
    confirmationDepthK: Long,
    consensusFns: GlobalSnapshotConsensusFunctions[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    snapshotSemaphore: Semaphore[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `handleSnapshot` -> `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-
    // staged accumulator stripped->canonical and thus promotes on finalize (complete served ring).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    // 3c-A enabler — signed-bytes staging map used by replay validation. Authoritative recovery paths do not write it.
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    dataDir: java.nio.file.Path,
    // (#196) Sink for inbound AllowSpendBlock gossip — same queue
    // GlobalSnapshotEventsPublisherDaemon drains into the event mempool.
    // Replaces the HTTP POST path from AllowSpendBlockRoutes (which was
    // wired to `queues.l1AllowSpendOutput`); the new outbox-backed gossip
    // path feeds the same queue from a different transport.
    enqueueAllowSpendBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    // (#196 follow-up) Sinks for inbound DAGBlock + TokenLockBlock gossip.
    // Replace the HTTP POST paths from DAGBlockRoutes (`/dag/l1-output`, via Cell
    // pipeline → `queues.l1Output`) and TokenLockBlockRoutes (`/dag/l1-token-lock-output`,
    // via direct offer → `queues.l1TokenLockOutput`). The Cell pipeline for DAGBlock
    // was a pure pass-through in the L0Cell (processDAGL1 → enqueueDAGL1Data →
    // queue.offer); the new gossip path skips that no-op layer.
    enqueueDAGBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.Block
    ] => F[Unit],
    enqueueTokenLockBlock: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.tokenLock.TokenLockBlock
    ] => F[Unit],
    // §1.2 Slice 5/6/9: KES parallel-signing infrastructure (sender) + load-bearing
    // receiver-side verify. The OperationalKeyMaker signs attestations + snapshots with
    // the operator's KES product key at the period derived from `EtaCalculation.rotationPeriod`.
    // The atomic operator-key registry supplies the paired KES+VRF identity used by both checks.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    // Receiver-side metagraph-attestation verification. A cached signed currency context is not
    // authority: its exact GL0 anchor is rechecked as current Phase 2 immediately before eta/key
    // derivation and signature verification.
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    verifyPhase2CurrencyContext: MetagraphParentOrdinalResolver.CurrencyBinaryContext => F[
      Option[MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext]
    ],
    etaForPhase2Anchor: MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext => F[Array[Byte]],
    // Receiver-side σ lookup. The committee VRF threshold is `K · σ_sender`, so the verifier
    // needs the SENDER's stake, not ours. Passed as a callback to keep the daemon agnostic of
    // the StakeRegistry's flavor.
    senderStakeLookup: io.constellationnetwork.schema.peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    // #214: pre-built gate-aware closure that processes a single `(metagraphAddress, wireBytes)`
    // pair through the same committee gate + orphan buffer path. Constructed in
    // `GlobalSnapshotConsensus.make` via `makeMetagraphBinaryProcessor`, so the SAME instance is
    // shared with the `SnapshotLeaderLoop.onFinalize` orphan-drain hook. The shared instance is
    // what makes the drain work: both sites must address the same in-memory orphan buffer.
    processOrphanedMetagraphBinary: (
      io.constellationnetwork.schema.address.Address,
      Array[Byte]
    ) => F[Unit],
    // #259: the SAME orphan-buffer instance the `processOrphanedMetagraphBinary` closure writes into
    // (built in `GlobalSnapshotConsensus.make`). The stuck-detection tick below reads its pending
    // parents via `listPendingParents` to decide which missing binaries to actively pull from peers.
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    // ─── Hierarchical-shard-checkpoints v1 — Gap B receiver routing ──────────────────────────────
    // Acceptance-side per-shard deps (registry of `(chainStore, tipTracker, finalityTriggers)` +
    // acceptance manager). `None` at `numShards = 1` (regression bar) ⇒ incoming `ShardCheckpoint` /
    // `ShardCheckpointAttestation` gossip is dropped with a single debug log, byte-identical to the
    // pre-wiring daemon. `Some(deps)` activates cross-node checkpoint reconstruction + chain growth.
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ] = None,
    // Chain-sync recovery (run-20, task #A): pull a missed shard checkpoint from a peer over HTTP instead of
    // waiting minutes for GossipSub re-gossip (which dedups Tier-1's identical re-publish bytes). Drives the T1
    // orphan trigger (missing parent on receipt) + the T2 absence stream (stuck/empty tip ⇒ pull tip+1; an empty
    // store pulls ordinal 1, the genesis-miss case). `None` at numShards=1 (regression bar) ⇒ no pull machinery.
    shardCheckpointFetcher: Option[ShardCheckpointFetcher[F]] = None,
    // Hierarchical-shard-checkpoints v1 — replay-backed execution certificate closure. When a received `ShardCheckpoint`
    // enters the canonical shard ancestry, `handleShardCheckpoint` invokes this emitter only with a capability minted by
    // mandatory local replay, then signs + gossips a `ShardCheckpointAttestation`. `None` at
    // `numShards = 1` (regression bar) ⇒ no emit; byte-identical to the pre-wiring daemon. Built only on the
    // gl0-leader-produce path (`GlobalSnapshotConsensus.make`), where the operator's signing material lives.
    shardCheckpointAttestationEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]
    ] = None,
    // WATCHTOWER (fraud-proof part 1): re-execute each ADOPTED checkpoint on the quorum path and gossip a
    // FraudProofEnvelope on a per-MG root mismatch (catches a quorum-signed wrong root). `None` at numShards=1
    // or `watchtower-enabled=false`. Built on the gl0-leader-produce path where the operator's signing material
    // + the SAME acceptance manager (its PIN-1 re-exec closure) live.
    watchtowerFraudProofEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]
    ] = None,
    // WATCHTOWER (fraud-proof part 2): DETERMINISTIC dispute verdict. On receiving a `FraudProofEnvelope`, every
    // gl0 INDEPENDENTLY re-runs this over the disputed checkpoint's OWN bytes (never trusting the challenger) and
    // decides UPHELD iff attested ≠ honest-re-derived. `None` at numShards=1.
    invalidStateProofValidator: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]
    ] = None,
    // ─── Hierarchical-shard-checkpoints v1 — per-ord producer fan-out (decoupled from gl0-leader win) ──
    // Per-shard checkpoint producers + the static metagraph→shard assignment, threaded so EVERY node fans
    // out shard checkpoints for each canonical (best-tip) gl0 ord it receives via gossip — NOT only the
    // gl0 slot winner. The gl0 leader still fans out for its OWN produced ord via `SnapshotLeaderLoop`
    // (GossipSub doesn't echo a publisher its own message), so together these fire the fan-out exactly
    // once per canonical ord per node. `shardChainStores` is derived in-daemon from
    // `shardAcceptanceDeps.registry` (the SAME instances the acceptance side + producers share). EMPTY /
    // `None` at `numShards = 1` (regression bar) ⇒ the per-ord hook is gated `whenA(false)` ⇒ no allocation.
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ] = Map.empty,
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]] = None,
    // WATCHTOWER fraud-proof POOL (W3a): `handleFraudProof` OFFERS a locally-UPHELD inbound dispute here so the gl0 leader producer embeds it
    // in the next snapshot's `fraudProofs` consensus field (where EVERY node re-validates + slashes it deterministically). The sole
    // production wiring passes the shared instance; at `numShards = 1` it passes `WatchtowerFraudProofPool.noop[F]` ⇒ offers discard ⇒ no
    // fraud proofs ever embedded ⇒ byte-identical regression bar. Required (no default — `noop` needs `Sync[F]`, not summonable at a
    // default-arg site).
    fraudProofPool: io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool[F]
  )(
    implicit globalStateProofSelector: io.constellationnetwork.schema.GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): fs2.Stream[F, Unit] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoSyncDaemon")

    // Per-shard chain stores derived from the acceptance-side registry (the SAME instances the producers
    // write into + the consumer reads). Empty at numShards=1 (`shardAcceptanceDeps = None`). Mirrors the
    // projection `GlobalSnapshotConsensus.make` does for the leader loop's `shardChainStores` param.
    val shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ] =
      shardAcceptanceDeps
        .map(_.registry.map { case (sid, entry) => sid -> entry.chainStore })
        .getOrElse(Map.empty)

    // Per-shard admission-approved binary buffers, projected off the SAME registry. The metagraph gate
    // writes into these only after quorum admission; the producer fan-out reads them. This
    // shared instance is the inversion — the producer's input comes from buffered binaries, not gl0's
    // post-chain-link `stateChannelSnapshots`. Empty at numShards=1.
    val shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ] =
      shardAcceptanceDeps
        .map(_.registry.map { case (sid, entry) => sid -> entry.binaryBuffer })
        .getOrElse(Map.empty)

    // EXECUTION-SHARDING Task 2: the deterministic committee draw, projected from the SAME `shardAcceptanceDeps` the acceptance
    // manager closes over, so the daemon's producer fan-out gates produce on the IDENTICAL committee the verifier admits against.
    // `None` (numShards=1) ⇒ empty-set draw (never reached: the fan-out is gated on `shardProducers.nonEmpty` first).
    val shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]] =
      shardAcceptanceDeps
        .map(_.committeeMembership)
        .getOrElse((_: io.constellationnetwork.schema.sharding.ShardId, _: io.constellationnetwork.schema.nakamoto.EtaPeriod) =>
          Async[F].pure(Set.empty[peer.PeerId])
        )

    // Buffer for gossip snapshots whose parent isn't in the chain store yet.
    // Keyed by missing parent hash → list of raw gossip snapshots waiting for that parent.
    // When a snapshot is stored in the chain store, we check this buffer and validate any
    // snapshots that were waiting for it. This creates a validation cascade from genesis.
    fs2.Stream.eval(Ref.of[F, Map[Hash, List[pb.Snapshot]]](Map.empty)).flatMap { pendingParentRef =>
      // Metagraph state-channel binary orphan buffer (#213) is now constructed by the caller
      // (GlobalSnapshotConsensus.make, #214) and threaded into this daemon as
      // `processOrphanedMetagraphBinary`. The caller shares the same buffer instance with the
      // SnapshotLeaderLoop.onFinalize drain hook so finalized-parent children re-enter through
      // the same gate-aware path used here.
      fs2.Stream
        .eval(
          (
            Ref.of[F, SyncState](SyncState.initial),
            Ref.of[F, PendingCheckpointChildren[pb.ShardCheckpointWire]](PendingCheckpointChildren.empty)
          ).tupled
        )
        .flatMap {
          case (stateRef, pendingShardCheckpointRef) =>
            // ChainSyncManager for active parent fetching. Uses a Ref to break the
            // circular dependency: handleSnapshot needs chainSyncManager, but
            // chainSyncManager's callback needs handleSnapshot.
            fs2.Stream
              .eval(
                Ref.of[F, Option[ChainSyncManager.ChainSyncManagerAlgebra[F]]](None).flatMap { csRef =>
                  ChainSyncManager
                    .make[F](
                      channel,
                      { resp: io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse[pb.Snapshot] =>
                        // #56.8: ADT pattern-match on the chain-sync disposition. Today's `handleSnapshot`
                        // pipeline doesn't yet differentiate finalized vs provisional — both are validated
                        // and stored identically. The structural distinction lands here so #56.10's
                        // overlay-aware accept() can route Provisional fetches to overlay-local commits
                        // (matching codex's NakamotoSyncDaemon catch in plan rev4 phase E) and Finalized
                        // fetches straight to base. NotFound logs and unwinds the inflight tracker.
                        import io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse
                        val fetchedSnap: Option[pb.Snapshot] = resp match {
                          case ChainSyncStateResponse.Finalized(snap)      => Some(snap)
                          case ChainSyncStateResponse.Provisional(snap, _) => Some(snap)
                          case ChainSyncStateResponse.NotFound(_)          => None
                        }
                        fetchedSnap match {
                          case Some(snap) =>
                            csRef.get.flatMap {
                              case Some(csm) =>
                                snapshotSemaphore.permit.use { _ =>
                                  handleSnapshot(
                                    snap,
                                    stateRef,
                                    pendingParentRef,
                                    chainStore,
                                    nodeStorage,
                                    tipTracker,
                                    stakeRegistry,
                                    operatorKeyRegistry,
                                    sidecarClient,
                                    selfId,
                                    keyPair,
                                    lddConfig,
                                    eligibilityChecker,
                                    lastKnownSlotRef,
                                    epochStateRef,
                                    etaRotationSnapshots,
                                    confirmationDepthK,
                                    consensusFns,
                                    snapshotStorage,
                                    lastGlobalSnapshotStorage,
                                    lastNGlobalSnapshotStorage,
                                    productionGate,
                                    mptStore,
                                    mptOverlay,
                                    pendingAccumulatorsRef,
                                    pendingPostBytesRef,
                                    eventMempool,
                                    csm,
                                    channel,
                                    dataDir,
                                    operationalKeyMaker,
                                    shardProducers,
                                    shardChainStores,
                                    shardBinaryBuffers,
                                    shardAssignment,
                                    shardCommitteeMembership,
                                    logger
                                  )
                                }
                              case None => Async[F].unit
                            }
                          case None =>
                            // NotFound — peer doesn't have the requested hash. Inflight tracking is
                            // unwound by ChainSyncManager's `guaranteeCase`; nothing further to do here.
                            Async[F].unit
                        }
                      }
                    )
                    .flatTap(csm => csRef.set(Some(csm)))
                }
              )
              .flatMap { chainSyncManager =>
                // The sidecar or gRPC channel terminates this stream on an actual transport failure; the reconnect wrapper below then
                // resubscribes while preserving pendingParentRef/stateRef/chainSyncManager. Message silence is valid in a quiet or fresh
                // single-validator network and is never treated as failure. Transport liveness comes from the channel's HTTP/2 keepalive.
                def gossipStream: fs2.Stream[F, Unit] = {
                  def processSnapshot(snap: pb.Snapshot): F[Unit] = {
                    val incomingOrdinal = snap.ordinal
                    chainStore.bestTipOrdinal.flatMap { currentBestOrdinal =>
                      val wouldWin = incomingOrdinal > currentBestOrdinal.getOrElse(0L)
                      val handle = snapshotSemaphore.permit.use { _ =>
                        handleSnapshot(
                          snap,
                          stateRef,
                          pendingParentRef,
                          chainStore,
                          nodeStorage,
                          tipTracker,
                          stakeRegistry,
                          operatorKeyRegistry,
                          sidecarClient,
                          selfId,
                          keyPair,
                          lddConfig,
                          eligibilityChecker,
                          lastKnownSlotRef,
                          epochStateRef,
                          etaRotationSnapshots,
                          confirmationDepthK,
                          consensusFns,
                          snapshotStorage,
                          lastGlobalSnapshotStorage,
                          lastNGlobalSnapshotStorage,
                          productionGate,
                          mptStore,
                          mptOverlay,
                          pendingAccumulatorsRef,
                          pendingPostBytesRef,
                          eventMempool,
                          chainSyncManager,
                          channel,
                          dataDir,
                          operationalKeyMaker,
                          shardProducers,
                          shardChainStores,
                          shardBinaryBuffers,
                          shardAssignment,
                          shardCommitteeMembership,
                          logger
                        )
                      }

                      if (wouldWin)
                        productionGate.pause(ProductionGate.BetterGossipReceived) >>
                          handle.guarantee(productionGate.resume(ProductionGate.BetterGossipReceived))
                      else handle
                    }
                  }

                  fs2.Stream.eval {
                    for {
                      overflowCounts <- Ref.of[F, Map[String, Long]](Map.empty)
                      snapshotLane <- GenerationWorkerLane
                        .bounded[F, pb.Snapshot](
                          SnapshotIntakeCapacity,
                          1,
                          VariableLaneByteCapacity,
                          VariableMessageMaxBytes
                        )(_.serializedSize.toLong)(processSnapshot)
                      tipAttestationLane <- GenerationWorkerLane
                        .bounded[F, pb.TipAttestation](
                          TipAttestationCapacity,
                          1,
                          AttestationLaneByteCapacity,
                          AttestationMessageMaxBytes
                        )(_.serializedSize.toLong)(att =>
                          handleAttestation(att, tipTracker, operatorKeyRegistry, etaRotationSnapshots, logger)
                        )
                      metagraphBinaryLane <- GenerationWorkerLane.bounded[F, pb.MetagraphBinary](
                        MetagraphBinaryCapacity,
                        MetagraphBinaryParallelism,
                        VariableLaneByteCapacity,
                        VariableMessageMaxBytes
                      )(_.serializedSize.toLong)(mb => handleMetagraphBinary(mb, processOrphanedMetagraphBinary, logger))
                      metagraphAttestationLane <- GenerationWorkerLane.bounded[F, pb.MetagraphAttestation](
                        MetagraphAttestationCapacity,
                        MetagraphAttestationParallelism,
                        AttestationLaneByteCapacity,
                        AttestationMessageMaxBytes
                      )(_.serializedSize.toLong)(att =>
                        handleMetagraphAttestation(
                          att,
                          committeeGate,
                          verifyPhase2CurrencyContext,
                          etaForPhase2Anchor,
                          etaRotationSnapshots,
                          senderStakeLookup,
                          orphanBuffer,
                          logger
                        )
                      )
                      nativeBlockLane <- GenerationWorkerLane.bounded[F, NativeBlockIngress](
                        NativeBlockCapacity,
                        NativeBlockParallelism,
                        VariableLaneByteCapacity,
                        VariableMessageMaxBytes
                      )(NativeBlockIngress.encodedSize)(work =>
                        work match {
                          case NativeBlockIngress.AllowSpend(asb) =>
                            handleAllowSpendBlock(asb, enqueueAllowSpendBlock, logger)
                          case NativeBlockIngress.Dag(blk) =>
                            handleDAGBlock(blk, enqueueDAGBlock, logger)
                          case NativeBlockIngress.TokenLock(blk) =>
                            handleTokenLockBlock(blk, enqueueTokenLockBlock, logger)
                        }
                      )
                      shardCheckpointLane <- GenerationWorkerLane.bounded[F, pb.ShardCheckpointWire](
                        ShardCheckpointCapacity,
                        ShardCheckpointParallelism,
                        VariableLaneByteCapacity,
                        VariableMessageMaxBytes
                      )(_.serializedSize.toLong)(cp =>
                        handleShardCheckpoint(
                          cp,
                          shardAcceptanceDeps,
                          shardCheckpointAttestationEmitter,
                          watchtowerFraudProofEmitter,
                          shardCheckpointFetcher,
                          pendingShardCheckpointRef,
                          MaxShardCheckpointAncestryRecoveryDepth,
                          selfId,
                          logger
                        )
                      )
                      shardAttestationLane <- GenerationWorkerLane.bounded[F, pb.ShardCheckpointAttestationWire](
                        ShardCheckpointAttestationCapacity,
                        ShardCheckpointAttestationParallelism,
                        AttestationLaneByteCapacity,
                        AttestationMessageMaxBytes
                      )(_.serializedSize.toLong)(att => handleShardCheckpointAttestation(att, shardAcceptanceDeps, logger))
                      fraudProofLane <- GenerationWorkerLane.bounded[F, pb.FraudProofEnvelopeWire](
                        FraudProofCapacity,
                        FraudProofParallelism,
                        VariableLaneByteCapacity,
                        VariableMessageMaxBytes
                      )(_.serializedSize.toLong)(fp =>
                        handleFraudProof(fp, shardAcceptanceDeps, invalidStateProofValidator, fraudProofPool, logger)
                      )
                    } yield
                      (
                        overflowCounts,
                        snapshotLane,
                        tipAttestationLane,
                        metagraphBinaryLane,
                        metagraphAttestationLane,
                        nativeBlockLane,
                        shardCheckpointLane,
                        shardAttestationLane,
                        fraudProofLane
                      )
                  }.flatMap {
                    case (
                          overflowCounts,
                          snapshotLane,
                          tipAttestationLane,
                          metagraphBinaryLane,
                          metagraphAttestationLane,
                          nativeBlockLane,
                          shardCheckpointLane,
                          shardAttestationLane,
                          fraudProofLane
                        ) =>
                      def enqueueOrReject[A](
                        lane: GenerationWorkerLane[F, A],
                        value: A,
                        family: String,
                        item: String,
                        recovery: String
                      ): F[Unit] =
                        lane.tryOffer(value).flatMap {
                          case GenerationWorkerOfferResult.Accepted => Async[F].unit
                          case rejected =>
                            val (reason, bound) = rejected match {
                              case GenerationWorkerOfferResult.MessageTooLarge(actual, maximum) =>
                                "message_too_large" -> s"encodedBytes=$actual maxMessageBytes=$maximum"
                              case GenerationWorkerOfferResult.ByteCapacityExceeded(actual, maximum) =>
                                "byte_capacity" -> s"encodedBytes=$actual maxQueuedBytes=$maximum"
                              case GenerationWorkerOfferResult.ItemCapacityExceeded(maximum) =>
                                "item_capacity" -> s"maxQueuedItems=$maximum"
                              case GenerationWorkerOfferResult.InvalidEncodedSize(actual) =>
                                "invalid_size" -> s"encodedBytes=$actual"
                              case GenerationWorkerOfferResult.GenerationClosed =>
                                "generation_closed" -> "lane admission is sealed"
                              case GenerationWorkerOfferResult.Accepted =>
                                "accepted" -> ""
                            }
                            val countKey = s"$family:$reason"
                            overflowCounts.updateAndGet { counts =>
                              counts.updated(countKey, counts.getOrElse(countKey, 0L) + 1L)
                            }.flatMap { counts =>
                              val count = counts(countKey)
                              Metrics[F].incrementCounter(
                                "dag_nakamoto_gossip_worker_overflow",
                                Seq(
                                  Metrics.unsafeLabelName("topic_family") -> family,
                                  Metrics.unsafeLabelName("reason") -> reason
                                )
                              ) >>
                                Async[F].whenA(count == 1L || count % 1024L == 0L) {
                                  logger.warn(
                                    s"Gossip worker rejected input: family=$family reason=$reason rejectionCount=$count $bound " +
                                      s"item=$item; recovery=$recovery"
                                  )
                                }
                            }
                        }

                      // The daemon declares the exact non-rumor families it consumes. Demultiplexing is nonblocking:
                      // a full metagraph-binary lane must not prevent the attestation needed by an in-flight gate from
                      // reaching its separately reserved lane. The same rule prevents native/shard/fraud floods from
                      // starving finality traffic. Overflow is an explicit rejection with a metric and family-specific
                      // recovery statement; no receipt becomes validity or execution authority here.
                      val gossip = GossipStream
                        .subscribe[F](
                          channel,
                          SidecarClient.SubscriptionProfile.nakamotoSync(shardAcceptanceDeps.isDefined),
                          subscriptionReadiness
                        )
                        .evalMap { msg =>
                          msg.body match {
                            case pb.GossipMessage.Body.Snapshot(snap) =>
                              enqueueOrReject(
                                snapshotLane,
                                snap,
                                "snapshot",
                                s"ordinal=${snap.ordinal}",
                                "exact-hash/ordinal ChainSync recovery can refetch the candidate"
                              )

                            case pb.GossipMessage.Body.Attestation(att) =>
                              // Serial by receipt: the current sticky accumulator is order-sensitive and telemetry-only.
                              enqueueOrReject(
                                tipAttestationLane,
                                att,
                                "tip_attestation",
                                s"ordinal=${att.tipOrdinal}",
                                "the optimistic sample may be incomplete; Nakamoto depth fallback remains available"
                              )

                            case pb.GossipMessage.Body.MetagraphBinary(mb) =>
                              enqueueOrReject(
                                metagraphBinaryLane,
                                mb,
                                "metagraph_binary",
                                s"address=${mb.address}",
                                "a later child can trigger parent-hash pull; loss of an unextended tail remains a durable-retry gap"
                              )

                            case pb.GossipMessage.Body.MetagraphAttestation(att) =>
                              enqueueOrReject(
                                metagraphAttestationLane,
                                att,
                                "metagraph_attestation",
                                s"address=${att.metagraphAddress}",
                                "execution/admission quorum waits for a later re-emission; receive-side durable retry remains open"
                              )

                            // This lane performs only wire decode and enqueue. It does not authenticate economics and cannot
                            // replace the mandatory universal GL0 execution of admitted native GL1 transitions.
                            case pb.GossipMessage.Body.AllowSpendBlock(asb) =>
                              enqueueOrReject(
                                nativeBlockLane,
                                NativeBlockIngress.AllowSpend(asb),
                                "native_allow_spend",
                                s"bytes=${asb.payload.size()}",
                                "the original GL1 sender must retry; receive-side durable retry and pre-enqueue authentication remain open"
                              )

                            case pb.GossipMessage.Body.DagBlock(blk) =>
                              enqueueOrReject(
                                nativeBlockLane,
                                NativeBlockIngress.Dag(blk),
                                "native_dag",
                                s"bytes=${blk.payload.size()}",
                                "the original GL1 sender must retry; receive-side durable retry and pre-enqueue authentication remain open"
                              )

                            case pb.GossipMessage.Body.TokenLockBlock(blk) =>
                              enqueueOrReject(
                                nativeBlockLane,
                                NativeBlockIngress.TokenLock(blk),
                                "native_token_lock",
                                s"bytes=${blk.payload.size()}",
                                "the original GL1 sender must retry; receive-side durable retry and pre-enqueue authentication remain open"
                              )

                            case pb.GossipMessage.Body.ShardCheckpoint(cp) =>
                              enqueueOrReject(
                                shardCheckpointLane,
                                cp,
                                "shard_checkpoint",
                                s"shard=${cp.shardId} ordinal=${cp.shardOrdinal}",
                                "bounded parent/hash and ordinal absence recovery can refetch the checkpoint"
                              )

                            case pb.GossipMessage.Body.ShardCheckpointAttestation(att) =>
                              enqueueOrReject(
                                shardAttestationLane,
                                att,
                                "shard_checkpoint_attestation",
                                s"shard=${att.shardId} bytes=${att.serializedSize}",
                                "checkpoint quorum waits for a later replay-backed re-attestation or checkpoint recovery"
                              )

                            case pb.GossipMessage.Body.FraudProof(fp) =>
                              enqueueOrReject(
                                fraudProofLane,
                                fp,
                                "fraud_proof",
                                s"bytes=${fp.serializedSize}",
                                "no slash or state change occurs; challenger retry/receive-side durable evidence recovery remains open"
                              )

                            case _: pb.GossipMessage.Body.Rumor =>
                              Async[F].unit

                            case _: pb.GossipMessage.Body.Started =>
                              Async[F].raiseError[Unit](
                                GossipStream.SubscriptionProtocolError(
                                  "SubscribeStarted escaped GossipStream's first-message handshake"
                                )
                              )

                            case pb.GossipMessage.Body.Empty =>
                              Async[F].unit
                          }
                        }

                      runWorkerGeneration(
                        gossip,
                        List(
                          snapshotLane,
                          tipAttestationLane,
                          metagraphBinaryLane,
                          metagraphAttestationLane,
                          nativeBlockLane,
                          shardCheckpointLane,
                          shardAttestationLane,
                          fraudProofLane
                        )
                      )
                  }.handleErrorWith {
                    case workerFailure: GenerationWorkerFailed =>
                      fs2.Stream.raiseError[F](workerFailure)
                    case e =>
                      fs2.Stream.eval(
                        logger.warn(s"Gossip stream error: ${e.getMessage}. Reconnecting in 5s...")
                      ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream
                  } ++ fs2.Stream.eval(
                    // A clean server completion is also reconnectable. gRPC errors retain their exact Throwable and take the error branch.
                    logger.warn("Gossip stream terminated (sidecar connection lost). Reconnecting in 5s...")
                  ) ++ fs2.Stream.sleep_[F](5.seconds) ++ gossipStream
                }

                // #259: stuck-parent active-recovery tick. A metagraph binary whose parent the local
                // committee gate never admitted (and whose orphan-drain never resolved) sits in the
                // orphan buffer forever — every later binary chains off it and re-buffers, so the
                // metagraph stalls from this node's view. This tick detects parents that persist across
                // ≥2 ticks (a real timeout, not a transient mid-drain blip) and actively PULLS the
                // missing binary by its value-hash from a peer over the existing ChainSync protocol,
                // then re-feeds it through the SAME gate-aware path gossip uses (`processOrphanedMetagraphBinary`
                // via `handleMetagraphBinary`). Purely additive recovery — never trust-on-fetch (the
                // re-fed binary is re-validated by the committee gate).
                //
                // Rate-limited per `(mg, parentHash)`: after a fetch attempt we don't retry that pair
                // for `StuckRefetchCooldown` (60s) even if it stays pending, so a genuinely-missing
                // binary (no peer has it) doesn't hammer the mesh.
                //
                // The orphan's parent hash IS the value-hash of the un-admitted binary we need (orphan
                // buffer keys on `parentHash == missing-binary.value.hash`), so we request
                // `binaryHashes = [parentHash]` directly — the serve handler matches by value-hash.
                def stuckDetectionStream: fs2.Stream[F, Unit] =
                  fs2.Stream
                    .eval(
                      Ref.of[F, Map[
                        (io.constellationnetwork.schema.address.Address, Hash),
                        NakamotoSyncDaemon.StuckParentState
                      ]](Map.empty)
                    )
                    .flatMap { stuckStateRef =>
                      fs2.Stream.fixedRate[F](StuckRefetchTickInterval).evalMap { _ =>
                        Async[F].realTimeInstant.map(_.toEpochMilli).flatMap { now =>
                          orphanBuffer.listPendingParents.flatMap { pending =>
                            val pendingSet = pending.toSet
                            stuckStateRef.modify { prev =>
                              // Carry forward only still-pending parents (a resolved parent resets its
                              // tick count). Bump consecutiveTicks for each currently-pending key.
                              val bumped = pendingSet.iterator.map { key =>
                                val prior = prev.getOrElse(key, NakamotoSyncDaemon.StuckParentState(0, 0L))
                                key -> prior.copy(consecutiveTicks = prior.consecutiveTicks + 1)
                              }.toMap
                              // Due-to-fetch: seen across ≥2 ticks AND past the per-pair cooldown.
                              val due = bumped.collect {
                                case (key, st)
                                    if st.consecutiveTicks >= 2 && (now - st.lastFetchAtMillis) >= StuckRefetchCooldown.toMillis =>
                                  key
                              }.toList
                              // Stamp lastFetchAt on the due keys so the cooldown starts now.
                              val withStamp = due.foldLeft(bumped) { (acc, key) =>
                                acc.updated(key, acc(key).copy(lastFetchAtMillis = now))
                              }
                              (withStamp, due)
                            }.flatMap { due =>
                              if (due.isEmpty) Async[F].unit
                              else
                                logger.info(
                                  s"🔎 ChainSync stuck-detection: ${due.size} stuck metagraph parent(s) past timeout — actively fetching: " +
                                    due.map { case (mg, h) => s"$mg/${h.value.take(12)}" }.mkString(", ")
                                ) >>
                                  due.traverse_ {
                                    case (mg, parentHash) =>
                                      chainSyncManager.fetchMetagraphBinaries(mg, List(parentHash)).flatMap { responses =>
                                        if (responses.isEmpty)
                                          logger.info(
                                            s"🔎 ChainSync stuck-detection: no peer had mg=$mg parent=${parentHash.value.take(12)} (will retry after cooldown)"
                                          )
                                        else
                                          responses.traverse_ { resp =>
                                            val mb = pb.MetagraphBinary(address = mg.value.value, binary = resp.signedBinary)
                                            // Re-feed through the SAME gossip entry point — goes through the
                                            // committee gate + drains buffered children on admit.
                                            handleMetagraphBinary(
                                              mb,
                                              processOrphanedMetagraphBinary,
                                              logger
                                            )
                                          }
                                      }
                                  }
                            }
                          }
                        }
                      }
                    }
                    .handleErrorWith { e =>
                      // Never let a recovery-tick failure kill the daemon; log and restart the ticker.
                      fs2.Stream.eval(
                        logger.warn(s"ChainSync stuck-detection tick error: ${e.getMessage}. Restarting in 30s...")
                      ) ++ fs2.Stream.sleep_[F](30.seconds) ++ stuckDetectionStream
                    }

                // ─── Shard-checkpoint chain-sync: T2 absence detection (run-20, task #A) ───────────────────────────
                // Per active shard, every `absenceTickIntervalMs`, if the local tip has not advanced for ≥ `stuckMs`
                // (an EMPTY store counts as stuck at ordinal 0), PULL `tip+1` from a peer (empty ⇒ ordinal 1, the
                // genesis-miss case) and re-feed it through the normal accept path. This is what gossip alone cannot
                // do: a missed checkpoint can't be re-gossip-recovered (Tier-1 re-publishes identical bytes that
                // GossipSub dedups), so it waited minutes for the seen-cache TTL — the run-20 stall. The fetcher's
                // own `(shard, ordinal)` cooldown rate-limits re-requests, so firing `due` every tick is safe.
                def shardAbsenceStream: fs2.Stream[F, Unit] =
                  (shardAcceptanceDeps, shardCheckpointFetcher) match {
                    case (Some(deps), Some(fetcher)) if deps.registry.nonEmpty =>
                      val cfg = deps.shardingConfig.checkpoint
                      fs2.Stream
                        .eval(Ref.of[F, Map[io.constellationnetwork.schema.sharding.ShardId, (Long, Long)]](Map.empty))
                        .flatMap { stallRef =>
                          fs2.Stream.fixedRate[F](cfg.absenceTickIntervalMs.millis).evalMap { _ =>
                            Async[F].realTimeInstant.map(_.toEpochMilli).flatMap { now =>
                              deps.registry.toList.traverse_ {
                                case (shardId, entry) =>
                                  // Track the ADOPTED watermark (NOT the local tip): the run-22 stall was adopted frozen at
                                  // 51 while the node kept producing its own tip to 53 — a tip-based trigger never fires
                                  // there. When the adopted watermark stalls for `stuckMs`, PULL `adopted+1` from a peer (the
                                  // canonical checkpoint reaching quorum elsewhere) so this node reorgs onto it and quorum
                                  // concentrates. Handles BOTH cases: genesis-miss (adopted=0 ⇒ pull ord 1) and the
                                  // cross-node convergence stall (adopted=51 ⇒ pull 52). `entry` retained for symmetry.
                                  val _ = entry
                                  deps.acceptanceManager.lastAdoptedOrd(shardId).flatMap { adoptedOpt =>
                                    val curOrd = adoptedOpt.map(_.value).getOrElse(0L)
                                    stallRef.modify { m =>
                                      val (lastOrd, lastAdvance) = m.getOrElse(shardId, (curOrd, now))
                                      if (curOrd > lastOrd) (m.updated(shardId, (curOrd, now)), false) // advanced — reset
                                      else (m.updated(shardId, (lastOrd, lastAdvance)), (now - lastAdvance) >= cfg.stuckMs)
                                    }.flatMap { due =>
                                      Async[F].whenA(due) {
                                        val nextOrd = io.constellationnetwork.schema.sharding.ShardOrdinal(curOrd + 1L)
                                        fetcher.fetchByOrdinal(shardId, nextOrd).flatMap {
                                          case Some(signed) =>
                                            io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                              .signedShardCheckpointToWire[F](signed)
                                              .flatMap(w =>
                                                handleShardCheckpoint(
                                                  w,
                                                  shardAcceptanceDeps,
                                                  shardCheckpointAttestationEmitter,
                                                  watchtowerFraudProofEmitter,
                                                  shardCheckpointFetcher,
                                                  pendingShardCheckpointRef,
                                                  MaxShardCheckpointAncestryRecoveryDepth,
                                                  selfId,
                                                  logger
                                                )
                                              )
                                          case None => Async[F].unit
                                        }
                                      }
                                    }
                                  }
                              }
                            }
                          }
                        }
                        .handleErrorWith { e =>
                          fs2.Stream.eval(
                            logger.warn(s"shard absence-detection tick error: ${e.getMessage}. Restarting in 30s...")
                          ) ++ fs2.Stream.sleep_[F](30.seconds) ++ shardAbsenceStream
                        }
                    case _ => fs2.Stream.empty
                  }

                gossipStream.concurrently(stuckDetectionStream).concurrently(shardAbsenceStream)
              }
        }
    }
  }

  private def handleSnapshot[F[_]: Async: cats.Parallel: JsonSerializer: SecurityProvider: HasherSelector: Metrics](
    snap: pb.Snapshot,
    stateRef: Ref[F, SyncState],
    pendingParentRef: Ref[F, Map[Hash, List[pb.Snapshot]]],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    nodeStorage: NodeStorage[F],
    tipTracker: TipTracker[F],
    stakeRegistry: StakeRegistry[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    sidecarClient: SidecarClient.SidecarClientAlgebra[F],
    selfId: peer.PeerId,
    keyPair: KeyPair,
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    epochStateRef: Ref[F, SharedEpochState],
    etaRotationSnapshots: Long,
    // Confirmation depth k₁ (Tier-2 vs Tier-3 gap boundary). Forwarded from `run`; sourced from
    // `sharedCfg.nakamoto.confirmationDepthK(sharedCfg.environment).value` (replaces the prior module-level sys.env read).
    confirmationDepthK: Long,
    consensusFns: GlobalSnapshotConsensusFunctions[F],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    mptStore: MptStore[F, GlobalStateKey],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[F, GlobalStateKey],
    // Task #12 slice-2c — the gl0 changeset STAGING map (same Ref the consensus functions + leader loop hold).
    // Threaded into `NakamotoSnapshotValidator.validate` so a NON-producer rekeys its just-staged accumulator
    // stripped->canonical and thus promotes on finalize (complete served ring; was per-producer-sparse).
    pendingAccumulatorsRef: Ref[
      F,
      Map[Hash, (SnapshotOrdinal, io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator)]
    ],
    // 3c-A enabler — signed-bytes staging map. NO LONGER validate-only: the ADOPT paths (reorg / realign / legacy catch-up) now stage
    // their root-verified byte maps here too (signed-byte-store FIDELITY, 2026-07-09 — adopted ordinals must not stay permanent holes in
    // `mpt_snapshot_info_signed`, or exact execution-base reads fail closed on every node that adopted that ordinal).
    pendingPostBytesRef: Ref[F, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    chainSyncManager: ChainSyncManager.ChainSyncManagerAlgebra[F],
    channel: ManagedChannel,
    dataDir: java.nio.file.Path,
    // §1.2 Slice 5/6/9: KES infrastructure for sender-side signing + receiver-side load-bearing verify.
    operationalKeyMaker: io.constellationnetwork.security.kes.OperationalKeyMakerAlgebra[F],
    // Hierarchical-shard-checkpoints v1 — per-ord producer fan-out, threaded through to
    // `processValidSnapshotInner`'s becameBestTip branch. EMPTY / `None` at numShards=1 (regression bar).
    shardProducers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointProducer[F]
    ],
    shardChainStores: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore[F]
    ],
    // Per-shard admission-approved binary buffers — the producer fan-out input.
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    // EXECUTION-SHARDING Task 2: deterministic committee draw, threaded through to `processValidSnapshotInner`'s fan-out gate.
    shardCommitteeMembership: (
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.schema.nakamoto.EtaPeriod
    ) => F[Set[peer.PeerId]],
    logger: org.typelevel.log4cats.Logger[F]
  )(
    implicit globalStateProofSelector: GlobalStateProofSelector,
    withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit,
    supervisor: Supervisor[F]
  ): F[Unit] =
    for {
      producerShort <- Async[F].pure(snap.producerId.toByteArray.take(4).map("%02x".format(_)).mkString)
      _ <- logger.info(
        s"📥 Received snapshot ordinal=${snap.ordinal} slot=${snap.slot} parentSlot=${snap.parentSlot} from=$producerShort"
      )

      // Per-producer counter for cross-peer eligibility / dominance analysis. Tagged
      // with the 8-char producer prefix so it can be queried as
      // `topk(N, dag_nakamoto_snapshots_received_by_producer) by (producer_id)`.
      _ <- Metrics[F].incrementCounter(
        "dag_nakamoto_snapshots_received_by_producer",
        Seq(Metrics.unsafeLabelName("producer_id") -> producerShort)
      )

      // Deserialize payload
      parsed = decodeFetchedSnapshotPayload(snap.payload.toByteArray)
      envelopeError <- parsed match {
        case Some(signedSnapshot) =>
          validateSnapshotEnvelope[F](snap, signedSnapshot).map(_.left.toOption)
        case None =>
          Async[F].pure(Option.empty[String])
      }
      metadataBound = parsed.nonEmpty && envelopeError.isEmpty

      genesisEta <- epochStateRef.get.map(_.genesisEta)
      // Use parentSlot from gossip message for gap (same inputs as producer used)
      slotGap = snap.slot - snap.parentSlot
      parentHash = Hash(new String(snap.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))

      // Chain-derived eta: must be computed from the INCOMING snapshot's chain,
      // not our local best tip. The producer used eta derived from its own chain
      // (walking back from its parent). If we use our local tip's chain, we'll
      // compute a different eta when forks diverge → VRF verification fails.
      //
      // Strategy: walk from the incoming snapshot's parentHash.
      // Fallback: if the incoming snapshot carries an eta field, use it directly
      // (trust-but-verify: we verify the snapshot's chain ancestry separately).
      //
      // Rotation period is keyed on the **predecessor ordinal** (`snap.ordinal - 1`),
      // mirroring the producer side at `SnapshotLeaderLoop:281` where `lastChainOrdinal`
      // (= bestTipOrdinal at production time, = N-1 for snap N) is the divisor input.
      // Period assignment must be a function of the snapshot itself so every honest
      // verifier reaches the same conclusion regardless of where their local bestTip is.
      // Key on the PARENT ordinal (snap.ordinal - 1) to match the producer, which keys on
      // its bestTip (= parent) ordinal — using `snap.ordinal` directly causes an off-by-one
      // at every R boundary. Combined with the Cardano/Praos bootstrap (periods 0 and 1 =
      // bootstrapEta, no VRF fold), the first rotation no longer depends on period-0 outputs
      // at all. See `docs/nakamoto/attestation-and-finality.md` §1.
      artifactPeriod = EtaCalculation.globalSnapshotArtifactPeriod(snap.ordinal, etaRotationSnapshots)
      currentPeriod = artifactPeriod.value
      producerId = peer.PeerId(Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString))
      operatorKeys <-
        if (!metadataBound)
          Async[F].pure(
            Option.empty[io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys]
          )
        else
          NakamotoSnapshotValidator.resolveRegisteredOperatorKeys(
            operatorKeyRegistry,
            producerId,
            snap.vrfPublicKey.toByteArray,
            artifactPeriod
          )
      eta <-
        if (!metadataBound) {
          Async[F].pure(none[Array[Byte]])
        } else if (currentPeriod <= 1) {
          // Cardano/Praos bootstrap: periods 0 and 1 are genesis-derivable (distinct, no VRF dependency).
          // Every honest verifier computes the identical value with no chain-walk, so the period 0→1
          // boundary can't fork on disagreement about period 0's VRF outputs (the ord≈R / iter35 wedge).
          Async[F].pure(EtaCalculation.bootstrapEta(genesisEta, currentPeriod).some)
        } else {
          chainStore.vrfOutputRangeForPeriodFrom(currentPeriod - 1, etaRotationSnapshots, parentHash).flatMap {
            case NakamotoChainStore.VrfOutputRange.Complete(chainOutputs) if chainOutputs.nonEmpty =>
              Async[F].pure(EtaCalculation.computeEta(genesisEta, currentPeriod, chainOutputs.map(_._2)).some)
            case NakamotoChainStore.VrfOutputRange.Complete(_) =>
              logger
                .warn(
                  s"Refusing snapshot eta derivation for period=$currentPeriod parent=${parentHash.value.take(12)}: " +
                    s"the proved-complete source interval contains no VRF outputs"
                )
                .as(none[Array[Byte]])
            case incomplete: NakamotoChainStore.VrfOutputRange.Incomplete =>
              logger
                .warn(
                  s"Deferring snapshot eta derivation for period=$currentPeriod parent=${parentHash.value.take(12)}: " +
                    s"source ancestry is incomplete at hash=${incomplete.missingHash.value.take(12)} " +
                    s"ordinal=${incomplete.expectedOrdinal.fold("unknown")(_.toString)}; producer-carried eta is evidence only"
                )
                .flatTap(_ => chainSyncManager.requestMissing(incomplete.missingHash))
                .as(none[Array[Byte]])
          }
        }

      // KES is a store-boundary validity condition. Verify it before global replay and before `chainStore.store`; otherwise an invalid-KES
      // candidate can become bestTip and later receive attestations even though `processValidSnapshot` declines its state update.
      kesOk <- operatorKeys match {
        case None => Async[F].pure(false)
        case Some(keys) =>
          KesGossipVerification.verifySnapshot(
            messageBytes = snap.hash.toByteArray,
            kesSigBytes = snap.kesSignature.toByteArray,
            producerId = producerId,
            producerHex = Hex(snap.producerId.toByteArray.map("%02x".format(_)).mkString),
            ordinal = snap.ordinal,
            operatorKeys = keys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
      }

      // Validation pipeline + chainStore.store. Pre-Phase E this body ran inside
      // `mptStore.withTransaction` so `accept()`'s mid-flight MPT mutations could be
      // rolled back when the snapshot didn't become the new bestTip. Phase D
      // (commit caa3559e) routed `accept()` through the overlay algebra
      // (`overlay.checkout` / `overlay.commit`); under `MptOverlay.OverlayMode.MultiBranch`
      // each download lands in its own pending `ChangeSet` — non-canonical branches sit
      // in pending until eviction (#56.9) drops them or `finalizeBranch` promotes the
      // canonical chain. The savepoint bracket is obsolete and removed here: the
      // overlay's branching IS the rollback. Under the current `Passthrough` wiring,
      // writes still land in base immediately and we lose the daemon-side rollback
      // semantics — that's intentional migration scaffolding, byte-equivalent to the
      // legacy path under #107's parity gate. The proper isolation arrives when
      // Phase J flips production wiring to `MultiBranch`.
      validationResult <-
        if (parsed.isEmpty)
          Async[F].pure(NakamotoSnapshotValidator.PayloadMissing(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
        else if (envelopeError.nonEmpty)
          logger
            .warn(s"Rejecting snapshot with unbound transport metadata: ${envelopeError.get}")
            .as(NakamotoSnapshotValidator.ContentMismatch(s"transport: ${envelopeError.get}"): NakamotoSnapshotValidator.ValidationResult)
        else if (!kesOk)
          Async[F].pure(NakamotoSnapshotValidator.KesInvalid(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
        else
          (parsed, operatorKeys) match {
            case (Some(signedSnapshot), Some(keys)) =>
              chainStore.get(parentHash).flatMap {
                case Some(parentStored) =>
                  eta.fold[F[NakamotoSnapshotValidator.ValidationResult]](
                    Async[F].pure(NakamotoSnapshotValidator.HistoricalEtaUnavailable(currentPeriod, parentHash))
                  ) { verifiedEta =>
                    NakamotoSnapshotValidator.validate[F](
                      signedSnapshot = signedSnapshot,
                      slot = snap.slot,
                      vrfProof = snap.vrfProof.toByteArray,
                      vrfPublicKey = snap.vrfPublicKey.toByteArray,
                      producerIdBytes = snap.producerId.toByteArray,
                      eta = verifiedEta,
                      slotGap = slotGap,
                      etaRotationSnapshots = etaRotationSnapshots,
                      stakeRegistry = stakeRegistry,
                      operatorKeys = keys,
                      lddConfig = lddConfig,
                      eligibilityChecker = eligibilityChecker,
                      consensusFns = consensusFns,
                      lastSignedArtifact = parentStored.signedSnapshot,
                      lastContext = parentStored.context,
                      getByOrdinal = { (ordinal: SnapshotOrdinal) =>
                        snapshotStorage.get(ordinal).flatMap {
                          case Some(s) => HasherSelector[F].withCurrent(implicit h => s.toHashed[F].map(_.some))
                          case None =>
                            chainStore.getByOrdinal(ordinal.value.value).flatMap {
                              case Some(stored) =>
                                HasherSelector[F].withCurrent(implicit h => stored.signedSnapshot.toHashed[F].map(_.some))
                              case None => Async[F].pure(None: Option[Hashed[GlobalIncrementalSnapshot]])
                            }
                        }
                      },
                      mptOverlay = mptOverlay,
                      pendingAccumulatorsRef = pendingAccumulatorsRef,
                      pendingPostBytesRef = pendingPostBytesRef
                    )
                  }
                case None =>
                  // Parent not in chain store. Three-tier gap handling:
                  // Tier 1 (<=6): buffer + ChainSync parent fetch (normal gossip latency)
                  // Tier 2 (>6, <=k): sequential walk-back via ChainSync (moderate drift)
                  // Tier 3 (>k): full catch-up — network finalized past us
                  (chainStore.bestTipOrdinal, stateRef.get).flatMapN { (localBestOrdinal, syncSt) =>
                    // Recovery livelock guard: while a parent-missing child is buffered, `bestTipOrdinal` still reports the connected tip.
                    // Max it with the highest already-requested catch-up ordinal so gossip cannot restart the same large-gap request before
                    // hash-keyed parent recovery connects it.
                    val localOrd = math.max(localBestOrdinal.getOrElse(0L), syncSt.lastCatchUpAdoptedOrdinal)
                    val gap = snap.ordinal - localOrd
                    if (gap > confirmationDepthK) {
                      logger.warn(
                        s"Tier 3: gap=$gap > k=$confirmationDepthK for ordinal=${snap.ordinal}. " +
                          s"Buffering until ancestry is available for full global replay; producer-carried state is not authoritative."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    } else if (gap > CatchUpThreshold) {
                      logger.info(
                        s"⏳ Tier 2: gap=$gap (>$CatchUpThreshold, <=$confirmationDepthK) for ordinal=${snap.ordinal}. " +
                          s"Sequential walk-back from parent ${parentHash.value.take(12)}."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    } else {
                      logger.info(
                        s"⏳ Tier 1: Parent ${parentHash.value.take(12)} not in chain store for ordinal=${snap.ordinal} (gap=$gap). Buffering."
                      ) >>
                        bufferPendingChild(parentHash, snap, pendingParentRef) >>
                        chainSyncManager.requestMissing(parentHash) >>
                        Async[F].pure(
                          NakamotoSnapshotValidator.ParentBuffered: NakamotoSnapshotValidator.ValidationResult
                        )
                    }
                  }
              }
            case _ =>
              Async[F].pure(NakamotoSnapshotValidator.PayloadMissing(snap.ordinal): NakamotoSnapshotValidator.ValidationResult)
          }
      replayCommitted <- commitReplayValidated(validationResult) { (validSnapshot, validContext) =>
        for {
          validHash <- HasherSelector[F].withCurrent(implicit h => validSnapshot.toHashed[F].map(_.hash))
          // Archive already-verified KES evidence before selection. A local evidence-write failure must not leave a newly selected
          // in-memory tip whose canonical projection was never attempted.
          _ <- SnapshotKesStorage.put[F](dataDir, validHash, snap.kesSignature.toByteArray)
          // Selection and its production fence form one uncancelable local handoff. Without this bracket, cancellation may land after
          // `store` publishes a new selected tip but before the next effect pauses production on its incomplete local projection.
          storeOutcome <- Async[F].uncancelable { _ =>
            chainStore
              .store(
                validSnapshot,
                validContext,
                snap.ordinal,
                snap.slot,
                parentHash,
                vrf.vrfProofToHash(snap.vrfProof.toByteArray).getOrElse(snap.vrfProof.toByteArray)
              )
              .flatTap {
                case NakamotoChainStore.StoreOutcome.BecameSelected(_, _) =>
                  productionGate.pause(ProductionGate.ReorgInProgress)
                case _ => Async[F].unit
              }
          }
          storeAccepted = storeOutcome match {
            case _: NakamotoChainStore.StoreOutcome.Rejected => false
            case _                                           => true
          }
          selected = storeOutcome match {
            case NakamotoChainStore.StoreOutcome.BecameSelected(exact, _) => exact.some
            // Duplicate equality is not execution/context authority: persisted recovery state can select bytes before this intake's exact
            // validation receipt is durably bound. Retry therefore waits for storeValidated/a projection journal in a later tranche.
            case _ => none[NakamotoChainStore.SelectedTip]
          }
          _ <- storeOutcome match {
            case _: NakamotoChainStore.StoreOutcome.StoredAlternate =>
              logger.info(
                s"Stored Nakamoto snapshot at ordinal=${snap.ordinal} as an alternate branch; no canonical effects are authorized"
              )
            case NakamotoChainStore.StoreOutcome.Rejected(reason, _) =>
              // Full replay stages the candidate under its canonical hash before the
              // chain store applies the finality floor. A store rejection means that hash
              // is neither retained ancestry nor an eligible alternate, so its staged
              // economic state and serve data must be removed symmetrically.
              mptOverlay.discardBranch(
                io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(validHash)
              ) >>
                pendingAccumulatorsRef.update(_ - validHash) >>
                pendingPostBytesRef.update(_ - validHash) >>
                logger.warn(s"Rejected replay-valid Nakamoto snapshot at ordinal=${snap.ordinal}: $reason")
            case _ => Async[F].unit
          }
          _ <- Async[F].whenA(storeAccepted) {
            processValidSnapshot(
              snap,
              selected,
              stateRef,
              chainStore,
              lastKnownSlotRef,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              eventMempool,
              logger
            )
          }
          // Only a stored or duplicate replay-valid parent may release waiting children. A rejected
          // candidate is not ancestry merely because its transition replayed.
          storedHash = Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
          _ <- Async[F].whenA(storeAccepted) {
            drainPendingChildren(
              storedHash,
              stateRef,
              pendingParentRef,
              chainStore,
              nodeStorage,
              tipTracker,
              stakeRegistry,
              operatorKeyRegistry,
              sidecarClient,
              selfId,
              keyPair,
              lddConfig,
              eligibilityChecker,
              lastKnownSlotRef,
              epochStateRef,
              etaRotationSnapshots,
              confirmationDepthK,
              consensusFns,
              snapshotStorage,
              lastGlobalSnapshotStorage,
              lastNGlobalSnapshotStorage,
              productionGate,
              mptStore,
              mptOverlay,
              pendingAccumulatorsRef,
              pendingPostBytesRef,
              eventMempool,
              chainSyncManager,
              channel,
              dataDir,
              operationalKeyMaker,
              shardProducers,
              shardChainStores,
              shardBinaryBuffers,
              shardAssignment,
              shardCommitteeMembership,
              logger
            )
          }
        } yield ()
      }
      _ <- Async[F].unlessA(replayCommitted) {
        (validationResult: NakamotoSnapshotValidator.ValidationResult) match {
          case NakamotoSnapshotValidator.ParentNotFound =>
            // Fail closed. Parentless state cannot be authorized by the producer's signature or by reproducing its self-claimed root.
            Metrics[F].incrementCounter("dag_nakamoto_parentless_snapshots_rejected") >>
              logger.warn(
                s"Rejecting parentless snapshot ordinal=${snap.ordinal} slot=${snap.slot}; ancestry must be fetched and the transition replayed"
              )
          case cm: NakamotoSnapshotValidator.ContentMismatch =>
            Metrics[F].incrementCounter("dag_nakamoto_content_mismatch_rejected") >>
              logger.warn(
                s"Rejecting globally replay-invalid snapshot ordinal=${snap.ordinal} slot=${snap.slot}: ${cm.detail}"
              )
          case NakamotoSnapshotValidator.ParentBuffered =>
            // Already buffered for validation when parent arrives — nothing more to do.
            Async[F].unit
          case invalid: NakamotoSnapshotValidator.Invalid =>
            Metrics[F].incrementCounter("dag_nakamoto_snapshots_rejected") >>
              logger.warn(s"❌ REJECTED snapshot slot=${snap.slot} ordinal=${snap.ordinal}: $invalid")
          case _ => Async[F].unit
        }
      }
    } yield ()

  /** Process a replay-valid snapshot after chain storage.
    *
    * Network-tip telemetry may advance for any retained snapshot. Canonical storage, mempool reconciliation, and readiness effects require
    * the exact `BecameSelected` observation to remain current under the chain-store mutation lock.
    */
  private def processValidSnapshot[F[_]: Async: HasherSelector: Metrics](
    snap: pb.Snapshot,
    selected: Option[NakamotoChainStore.SelectedTip],
    stateRef: Ref[F, SyncState],
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[F],
    lastKnownSlotRef: Ref[F, Option[Long]],
    snapshotStorage: SnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastGlobalSnapshotStorage: LastSnapshotStorage[F, GlobalIncrementalSnapshot, GlobalSnapshotInfo],
    lastNGlobalSnapshotStorage: LastNGlobalSnapshotStorage[F],
    productionGate: ProductionGate[F],
    eventMempool: EventMempool[F, GlobalSnapshotEvent, GlobalStateKey],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    for {
      // Update network tip tracking
      _ <- stateRef.update { s =>
        if (snap.ordinal > s.networkTipOrdinal)
          s.copy(
            networkTipOrdinal = snap.ordinal,
            networkTipHash = Some(Hash(new String(snap.hash.toByteArray, java.nio.charset.StandardCharsets.UTF_8)))
          )
        else s
      }

      // Whole-pool inspection is deliberately outside the chain-store mutation lock. The exact-selection CAS below removes only this
      // precomputed set; events inserted after this scan remain queued for later validation rather than extending the consensus lock.
      staleHashes <- selected.fold(Set.empty[Hash].pure[F]) { expected =>
        staleMempoolHashes(eventMempool, expected.snapshot.context)
      }

      // Canonical-state updates only while this exact selection remains current in the chain store.
      // The production-default MPT overlay is MultiBranch, so replay mutations remain isolated under their exact branch until promotion.
      // This CAS separately protects revision-unaware local projection stores and the mempool from an interleaved selection change.
      canonicalOutcome <- selected.traverse { expected =>
        chainStore.runCanonicalEffectsIfCurrent(expected) { canonical =>
          val effects = HasherSelector[F].withCurrent { implicit hasher =>
            canonical.signedSnapshot.toHashed[F].flatMap { hashed =>
              snapshotStorage.setHeadForRecovery(canonical.signedSnapshot, canonical.context) >>
                lastGlobalSnapshotStorage.setForRecovery(hashed, canonical.context) >>
                lastNGlobalSnapshotStorage.setForRecovery(hashed, canonical.context) >>
                lastKnownSlotRef.set(Some(canonical.slot))
            }
          } >>
            stateRef.get.flatMap { state =>
              if (!state.isReady) {
                val caughtUp = state.networkTipOrdinal - canonical.ordinal <= CatchUpThreshold
                if (caughtUp)
                  // This is daemon-local catch-up telemetry only. Main is the sole owner of the externally visible Ready transition and
                  // publishes it only after both current Subscribe lanes acknowledge the same sidecar process.
                  stateRef
                    .update(_.copy(isReady = true, localTipOrdinal = canonical.ordinal))
                    .as(state.networkTipOrdinal.some)
                else none[Long].pure[F]
              } else none[Long].pure[F]
            }.flatTap { _ =>
              // Eviction is intentionally the last irreversible projection effect. If any storage or readiness write above fails, inputs
              // remain available for deterministic retry while production stays fenced.
              eventMempool.remove(staleHashes).whenA(staleHashes.nonEmpty)
            }

          // Resume only after every projection sink succeeds. A partial multi-store write has no durable retry journal yet, so an error
          // must fail-stop production rather than reopen it on a potentially split local projection.
          effects.flatTap(_ => productionGate.resume(ProductionGate.ReorgInProgress))
        }
      }
      _ <- canonicalOutcome.traverse_ {
        case NakamotoChainStore.CanonicalEffectsOutcome.Applied(applied, readyNetworkTip) =>
          val canonical = applied.snapshot
          logger.debug(s"Updated canonical storage to ordinal=${canonical.ordinal} slot=${canonical.slot}") >>
            Metrics[F].incrementCounter("dag_nakamoto_snapshots_received") >>
            Metrics[F].updateGauge("dag_nakamoto_ordinal", canonical.ordinal) >>
            canonical.signedSnapshot.value.slotCertificate.traverse_ { certificate =>
              Metrics[F].recordDistribution(
                "dag_nakamoto_slot_gap",
                (canonical.slot - certificate.parentSlot.value.value).toInt
              )
            } >>
            logger.info(s"Mempool reconciliation evicted ${staleHashes.size} stale DAG event(s)") >>
            readyNetworkTip.traverse_ { networkTip =>
              logger.info(
                s"Receiver reached its local catch-up threshold (local=${canonical.ordinal}, network=$networkTip); lifecycle Ready remains Main-owned"
              )
            }
        case NakamotoChainStore.CanonicalEffectsOutcome.StaleSelection(current) =>
          logger.info(
            s"Skipped stale receiver canonical effects for ordinal=${snap.ordinal}; " +
              s"current=${current.fold("none")(tip => s"${tip.snapshot.ordinal}:${tip.snapshot.hash.value.take(12)}")}"
          )
      }

      // Shard-checkpoint fan-out remains in SnapshotLeaderLoop's per-slot tick. This receiver path performs no shard duty.

      // A replay-valid selected tip is not enough to authorize optimistic signing.
      // Emission stays dark until this exact validation result is carried as an
      // opaque replay receipt through an atomic exact-tip preference capability.

      // §1.2 Slice 9: KES verification of the incoming snapshot's `kes_signature` field is
      // run BEFORE this body via `processValidSnapshot`'s outer `kesGate`. By the time we
      // reach here the gate has already passed (otherwise the snapshot was rejected and
      // this code is unreachable).

    } yield ()

  private def handleAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    att: pb.TipAttestation,
    tipTracker: TipTracker[F],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    etaRotationSnapshots: Long,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    decodeTipAttestation(att) match {
      case Left(reason) =>
        logger.warn(s"Rejecting malformed tip attestation: $reason")

      case Right(decoded) =>
        val attesterId = peer.PeerId(decoded.attesterHex)

        if (decoded.signature.isEmpty) {
          logger.warn(
            s"⚠️ Rejecting unsigned attestation for ordinal=${decoded.domain.tipOrdinal} " +
              s"from=${decoded.attesterHex.value.take(16)}..."
          )
        } else {
          // Verify signature using the same Hasher pipeline: JSON-encode the domain TipAttestation → hash → verify
          HasherSelector[F].withCurrent { implicit hasher =>
            for {
              attHash <- decoded.domain.hash
              longTermVerification <- decoded.attesterHex
                .toPublicKey[F]
                .flatMap(publicKey => Signing.verifySignature[F](attHash.getBytes, decoded.signature)(publicKey))
                .attempt
              _ <- longTermVerification match {
                case Left(error) =>
                  logger.warn(
                    s"Rejecting malformed tip-attestation identity/signature for ordinal=${decoded.domain.tipOrdinal} " +
                      s"from=${decoded.attesterHex.value.take(16)}...: ${Option(error.getMessage).getOrElse(error.getClass.getSimpleName)}"
                  )
                case Right(false) =>
                  logger.warn(
                    s"⚠️ Rejecting attestation with invalid signature for ordinal=${decoded.domain.tipOrdinal} " +
                      s"from=${decoded.attesterHex.value.take(16)}..."
                  )
                case Right(true) =>
                  // Chronos-prep: capture OUR local clock when the peer's attestation lands so
                  // `TipTracker.recordAttestation` can compare against the peer's claimed
                  // `attestedAt`. Badly-skewed peers (or attackers forging timestamps) are
                  // dropped inside the tracker before they pollute `T_count` finality (#136).
                  Clock[F].realTime.map(_.toMillis).flatMap { nowMs =>
                    ActiveOperatorConsensusKeys
                      .resolve(
                        operatorKeyRegistry,
                        attesterId,
                        EtaCalculation.globalSnapshotArtifactPeriod(decoded.domain.tipOrdinal, etaRotationSnapshots)
                      )
                      .flatMap {
                        case None =>
                          logger.warn(
                            s"Rejecting tip attestation from=${decoded.attesterHex.value.take(16)}...: " +
                              s"no active preregistered atomic KES+VRF pair"
                          )
                        case Some(keys) =>
                          KesGossipVerification
                            .verifyAttestation(
                              messageBytes = attHash.getBytes,
                              kesSigBytes = decoded.kesSignature,
                              attesterId = attesterId,
                              attesterHex = decoded.attesterHex,
                              tipOrdinal = decoded.domain.tipOrdinal,
                              operatorKeys = keys,
                              etaRotationSnapshots = etaRotationSnapshots,
                              logger = logger
                            )
                            .flatMap { kesOk =>
                              if (kesOk)
                                tipTracker.recordAttestation(attesterId, decoded.domain, nowMs) >>
                                  logger.info(
                                    s"📨 Attestation for ordinal=${decoded.domain.tipOrdinal} from=${decoded.attesterHex.value.take(16)}..."
                                  )
                              else Async[F].unit
                            }
                      }
                  }
              }
            } yield ()
          }
        }
    }

  private[nakamoto] final case class DecodedTipAttestation(
    domain: DomainTipAttestation,
    attesterHex: Hex,
    signature: Array[Byte],
    kesSignature: Array[Byte]
  )

  /** Decode untrusted protobuf fields without throwing. Numeric wire values are signed `int64`; negative values must be rejected before
    * refined constructors, hashing, public-key parsing, or signature verification.
    */
  private[nakamoto] def decodeTipAttestation(att: pb.TipAttestation): Either[String, DecodedTipAttestation] =
    for {
      slot <- NonNegLong.from(att.tipSlot).leftMap(_ => s"negative tipSlot=${att.tipSlot}")
      _ <- Either.cond(att.tipOrdinal >= 0L, (), s"negative tipOrdinal=${att.tipOrdinal}")
      _ <- Either.cond(att.attestedAt >= 0L, (), s"negative attestedAt=${att.attestedAt}")
      attesterBytes = att.attesterId.toByteArray
      _ <- Either.cond(attesterBytes.length == 64, (), s"invalid attesterId length=${attesterBytes.length}, expected=64")
      tipHash = Hash(new String(att.tipHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
      attesterHex = Hex.fromBytes(attesterBytes)
    } yield
      DecodedTipAttestation(
        DomainTipAttestation(tipHash, Slot(slot), att.tipOrdinal, att.attestedAt),
        attesterHex,
        att.signature.toByteArray,
        att.kesSignature.toByteArray
      )

  /** Route an incoming AllowSpendBlock from gossip into the same `l1AllowSpendOutput` queue the (now-removed) HTTP POST endpoint populated.
    * Sender serializes `Signed[AllowSpendBlock]` via JsonSerializer in `Swap.sendBlockToL0`; we use the same typeclass to deserialize.
    * Errors (decode failure) are logged and swallowed — gossip is fire-and-forget. (#196)
    */
  private def handleAllowSpendBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    asb: pb.AllowSpendBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.swap.AllowSpendBlock
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.swap.AllowSpendBlock
    import io.constellationnetwork.security.signature.Signed

    val bytes = asb.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[AllowSpendBlock]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting allow-spend-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueAllowSpendBlock failed: ${e.getMessage}"))
      }
  }

  /** Route an incoming DAG block from gossip into the same `l1Output` queue the (now-removed) HTTP POST endpoint populated. The
    * DAGBlockRoutes pipeline ran through L0Cell.processDAGL1 → EnqueueDAGL1Data → queue.offer; that path was a pure pass-through with no
    * extra validation, so the new gossip path enqueues directly. Sender serializes `Signed[Block]` via JsonSerializer in
    * `StateChannel.sendBlockToL0`; we use the same typeclass to deserialize. Errors are logged and swallowed — gossip is fire-and-forget.
    * (#196 follow-up)
    */
  private def handleDAGBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    blk: pb.DAGBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.Block
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.Block
    import io.constellationnetwork.security.signature.Signed

    val bytes = blk.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[Block]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting dag-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueDAGBlock failed: ${e.getMessage}"))
      }
  }

  /** Route an incoming TokenLockBlock from gossip into the same `l1TokenLockOutput` queue the (now-removed) HTTP POST endpoint populated.
    * Sender serializes `Signed[TokenLockBlock]` via JsonSerializer in `TokenLock.sendBlockToL0`; we use the same typeclass to deserialize.
    * Errors are logged and swallowed — gossip is fire-and-forget. (#196 follow-up)
    */
  private def handleTokenLockBlock[F[_]: Async: io.constellationnetwork.json.JsonSerializer](
    blk: pb.TokenLockBlock,
    enqueue: io.constellationnetwork.security.signature.Signed[
      io.constellationnetwork.schema.tokenLock.TokenLockBlock
    ] => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.tokenLock.TokenLockBlock
    import io.constellationnetwork.security.signature.Signed

    val bytes = blk.payload.toByteArray
    io.constellationnetwork.json
      .JsonSerializer[F]
      .deserialize[Signed[TokenLockBlock]](bytes)
      .flatMap {
        case Left(err) =>
          logger.warn(s"⚠️ Rejecting token-lock-block gossip: decode failed (${err.getMessage})")
        case Right(signed) =>
          enqueue(signed)
            .handleErrorWith(e => logger.warn(s"⚠️ enqueueTokenLockBlock failed: ${e.getMessage}"))
      }
  }

  /** Slice S3 receiver: route an incoming `pb.MetagraphAttestation` into the [[MetagraphCommitteeGate]] verifier + tally. Verification
    * failures are logged inside the gate; this function only translates the proto wire shape into the gate's `IncomingAttestation` domain
    * value and looks up the canonical eta for the sender's metagraph-parent ordinal.
    *
    * The proto fields map 1:1 to `IncomingAttestation`:
    *   - `att.peerId` (UTF-8 hex bytes) → `senderPeerId`
    *   - `att.vrfPublicKey` (32 bytes) → `senderVrfVk`
    *   - `att.metagraphAddress` (DAG base58 string) → `metagraphAddress`
    *   - `att.parentHash` (UTF-8 bytes of canonical Hash hex) → `parentHash`
    *   - `att.binaryHash` (UTF-8 bytes of canonical Hash hex) → `binaryHash`
    *   - `att.committeeVrfProof` → `committeeVrfProof`
    *   - `att.signature` → `longTermSignature`
    *   - `att.kesSignature` → `kesSignature`
    *   - `att.senderTreeStep` (uint32 wire field) → `senderTreeStep`
    *
    * '''Why `vrf_public_key` is on the wire.''' It is comparison evidence only. `MetagraphCommitteeGate` resolves the sender's key from the
    * canonical registry, requires an exact byte match, and verifies the proof under the resolved key. An unknown sender or replacement key
    * fails closed. The runtime target replaces the current frozen-genesis lookup with the candidate-parent historical registration view;
    * this transport field never becomes an authority source.
    */
  private def handleMetagraphAttestation[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector](
    att: pb.MetagraphAttestation,
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    verifyPhase2CurrencyContext: MetagraphParentOrdinalResolver.CurrencyBinaryContext => F[
      Option[MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext]
    ],
    etaForPhase2Anchor: MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext => F[Array[Byte]],
    etaRotationSnapshots: Long,
    senderStakeLookup: peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    // Holds unresolved binaries, recent ML0 continuity, signed GL0 contexts, and deferred attestations.
    // Cached context is never authority: the receiver rechecks its exact GL0 anchor before verification.
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate.IncomingAttestation
    import eu.timepit.refined.refineV

    refineV[DAGAddressRefined](att.metagraphAddress) match {
      case Left(err) =>
        logger.warn(s"⚠️ Rejecting metagraph-attestation: invalid address '${att.metagraphAddress}' ($err)")
      case Right(refined) =>
        val metagraphAddress = Address(refined)
        val senderHex = Hex(att.peerId.toByteArray.map("%02x".format(_)).mkString)
        val senderPeerId = peer.PeerId(senderHex)
        val parentHash = Hash(new String(att.parentHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val binaryHash = Hash(new String(att.binaryHash.toByteArray, java.nio.charset.StandardCharsets.UTF_8))
        val senderVrfVk = att.vrfPublicKey.toByteArray
        val incoming = IncomingAttestation(
          senderPeerId = senderPeerId,
          senderVrfVk = senderVrfVk,
          metagraphAddress = metagraphAddress,
          parentHash = parentHash,
          binaryHash = binaryHash,
          committeeVrfProof = att.committeeVrfProof.toByteArray,
          longTermSignature = att.signature.toByteArray,
          kesSignature = att.kesSignature.toByteArray,
          senderTreeStep = att.senderTreeStep
        )
        // Verify-on-attach derives committee eta and key period only from the binary's exact signed GL0
        // anchor, without spending crypto on tines this node has not attached. Two paths follow:
        //
        //   TIER 1 — IN-FLIGHT → eager verify. When this node processed the same gossiped binary in
        //       `processBytes` cached `(wireHash -> signed context)` before attesting. A cache hit means the
        //       binary's ML0 parent was resolved and the exact GL0 anchor can now be rechecked —
        //       it is attaching NOW (running the `resolveParent == Some` admit path) — so it is a canonical
        //       candidate and its attestation is worth verifying immediately, so `attestAndAdmit`'s threshold
        //       sees it. `att.binaryHash` == the wire digest this node hashed in `processBytes`, so keys match.
        //   PHASE 0 — NOT IN-FLIGHT → buffer, defer verify (#29). Otherwise the binary is orphan-buffered (its
        //       parent isn't admitted — a tine we aren't attached to) or hasn't arrived. We do NOT verify now:
        //       speculatively crypto-checking attestations for fork-storm tines that mostly get evicted is the
        //       committee-gate CPU sink that starved gl0 producers off the air. `bufferAttestation` holds it
        //       un-verified; when the binary ATTACHES (drains into the admit path) `drainAttestations` replays
        //       it for verification right before the threshold gate. Losing tines' attestations expire
        //       un-verified — zero crypto. (This subsumes the old buffered-content-deserialize tier AND the
        //       fail-closed drop: a not-yet-arrived binary's attestation now waits in the buffer rather than
        //       being dropped, and verifies if/when the binary lands and attaches.)
        //
        // A cached signed context is not authorization. Both eager and deferred paths recheck that its exact
        // `(ordinal, hash)` is current Phase 2 before deriving eta or the active VRF/KES period.
        def bufferUnverified(reason: String): F[Unit] =
          orphanBuffer.bufferAttestation(metagraphAddress, binaryHash, incoming) >>
            logger.debug(
              s"Phase-0 buffered committee-attestation (unverified) mg=$metagraphAddress " +
                s"binary=${binaryHash.value.take(12)}... parent=${parentHash.value.take(12)}... peer=$senderPeerId reason=$reason"
            )

        val recordWithContext: MetagraphParentOrdinalResolver.CurrencyBinaryContext => F[Unit] = context =>
          verifyPhase2CurrencyContext(context).flatMap {
            case None => bufferUnverified("signed-gl0-anchor-not-current-phase2")
            case Some(verified) =>
              etaForPhase2Anchor(verified).flatMap { eta =>
                val artifactPeriod = io.constellationnetwork.schema.nakamoto.EtaPeriod(
                  EtaCalculation.rotationPeriod(verified.gl0AnchorOrdinal.value.value, etaRotationSnapshots)
                )
                committeeGate.recordReceivedAttestation(incoming, eta, artifactPeriod, senderStakeLookup)
              }
          }
        orphanBuffer.lookupPendingBinaryContext(metagraphAddress, binaryHash).flatMap {
          case Some(context) =>
            // Tier 1 — the binary is IN-FLIGHT: its parent is at the tip, so it is attaching NOW (running the
            // `resolveParent == Some` admit path in `processBytes`). Verify eagerly so `attestAndAdmit`'s
            // threshold sees this attestation immediately. `recordPendingBinaryContext` seeded this lookup at
            // resolve time; Phase-2 status is rechecked above before any signature is counted.
            recordWithContext(context)
          case None =>
            // Phase-0 of verify-on-attach (#29). The binary is NOT in-flight — it is either orphan-buffered
            // (its parent isn't admitted: a tine we aren't attached to) or simply hasn't arrived yet. EITHER
            // way we must NOT spend Ed25519+KES+VRF on it now: speculatively verifying attestations for tines
            // that mostly get evicted during an eta-rotation fork storm is the committee-gate CPU sink that
            // starved gl0 producers off the air. Buffer the attestation UN-VERIFIED (keyed by the wire hash);
            // when the binary ATTACHES (drains into the `resolveParent == Some` admit path) `drainAttestations`
            // releases it for verification (Phase 1), right before the threshold gate. If the binary never
            // attaches (losing tine) the buffered attestation is FIFO-evicted un-verified — zero crypto spent.
            bufferUnverified("binary-context-not-attached")
        }
    }
  }

  /** Gap B — handle an incoming `ShardCheckpointWire` from gossip (load-bearing cross-node reconstruction).
    *
    * Steps (per the wiring plan §B):
    *   1. Decode the wire into the schema-side `ShardCheckpoint` via `ShardCheckpointWireCodecs.shardCheckpointFromWire`.
    *   1. Look up `deps.registry.get(shardId)`; drop if the shard is untracked locally.
    *   1. Preserve the signed slot carried by the wire. Derive the chain-store-only `vrfOutput` from the producer's first
    *      `CommitteeMemberSignature.vrfProof` via `vrfProofToHash`, so producer and receiver store byte-identical fork-choice metadata.
    *      Re-wrap the bare `ShardCheckpoint` into a `Signed[ShardCheckpoint]` from the committee signatures present (the codec scaladoc
    *      describes the re-wrap — the producer's own committee `ed25519Sig` IS `signData(preimageHash)`, byte-identical to the outer
    *      `Signed` proof it built).
    *   1. Run `deps.acceptanceManager.evaluateForSigning(checkpoint)`, including framework replay. Receive a [[VerifiedShardCheckpoint]]
    *      only for `Accepted` or `PendingMoreAttestations`; an affirmative mismatch may trigger certificate-gated portable fraud evidence
    *      but never storage or attestation.
    *   1. For a verified checkpoint only, store it in `entry.chainStore` and record its carried committee signatures.
    *   1. Before each missing execution signature, reuse that exact receipt capability or replay the corresponding stored ancestor once.
    *
    * `None` deps (numShards=1, regression bar) ⇒ single debug log + drop.
    */
  /** Bound on the retroactive ancestor-attestation walk (chain-not-tip, 2026-06-11). Genesis stalls involve a handful of ordinals; anything
    * deeper is covered progressively by subsequent best-tip events.
    */
  private val MaxAncestorAttestWalk: Int = 16

  private def handleShardCheckpoint[F[_]: Async: JsonSerializer: HasherSelector: Metrics](
    cp: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ShardCheckpointWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    shardCheckpointAttestationEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointAttestationEmitter[F]
    ],
    // WATCHTOWER (fraud-proof part 1): re-execute a certificate-authenticated checkpoint and gossip a FraudProofEnvelope
    // on a per-MG root mismatch. `None` at numShards=1 or when watchtower-enabled=false ⇒ no approval-check. Fired
    // directly for the typed affirmative-mismatch rejection (before storage/attestation), and defensively on the
    // replay-valid became-best-tip seam.
    watchtowerFraudProofEmitter: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofEmitter[F]
    ],
    // T1 orphan-by-hash chain-sync (task #A): drives the parent-pull below — a received checkpoint whose parent is
    // absent pulls the missing parent and re-feeds it through THIS method, converging a divergent-sibling node onto
    // canonical. `None` ⇒ no pull (numShards=1 or unwired).
    shardCheckpointFetcher: Option[ShardCheckpointFetcher[F]],
    pendingShardCheckpointRef: Ref[F, PendingCheckpointChildren[pb.ShardCheckpointWire]],
    ancestryRecoveryDepth: Int,
    selfId: peer.PeerId,
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAcceptanceDeps match {
      case None =>
        logger.debug("Received ShardCheckpoint but sharding inactive (numShards=1); dropping")
      case Some(deps) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
          .shardCheckpointFromWire[F](cp)
          .flatMap { checkpoint =>
            deps.registry.get(checkpoint.shardId) match {
              case None =>
                logger.debug(
                  s"Received ShardCheckpoint for untracked shard=${checkpoint.shardId.value.value}; dropping"
                )
              case Some(entry) =>
                HasherSelector[F].withCurrent { implicit hasher =>
                  Async[F].defer {
                    afterCheckpointProducerPreAuthorization(
                      deps.acceptanceManager.verifyCommitteeSignature(checkpoint, checkpoint.producerSignature)
                    ) {
                      // §5.7 slot validity bound — strictly monotone vs the parent's wire slot (when the parent is
                      // known; orphans are bounded retroactively when the parent connects via the chain-store's
                      // connectivity gate). The replay-capability manager also enforces the exact shuffled-staircase owner
                      // against that retained parent before replay/store/countersign. No consensus-pinned upper slot-skew
                      // bound exists yet; receiver wall clock is deliberately not introduced as an artifact-validity input.
                      // The gossip caller already detached this whole handler. Recover ancestry sequentially inside that fiber so
                      // `evaluateForSigning` cannot run before the exact parent is stored. Every recursively fetched parent re-enters this
                      // method and therefore passes producer pre-authorization before it can trigger its own fetch. The depth bound prevents
                      // an authenticated adversary from creating an unbounded walk; an unresolved child is retained in the bounded buffer
                      // and retried only when its exact parent later passes full validation and storage.
                      val parentReadyF: F[Boolean] =
                        if (checkpoint.parentCheckpointHash === Hash.empty) true.pure[F]
                        else
                          entry.chainStore.getByHash(checkpoint.parentCheckpointHash).flatMap {
                            case Some(_)                            => true.pure[F]
                            case None if ancestryRecoveryDepth <= 0 => false.pure[F]
                            case None =>
                              shardCheckpointFetcher.fold(false.pure[F]) { fetcher =>
                                fetcher.fetchByHash(checkpoint.shardId, checkpoint.parentCheckpointHash).flatMap {
                                  case None => false.pure[F]
                                  case Some(parentSigned) =>
                                    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                      .signedShardCheckpointToWire[F](parentSigned)
                                      .flatMap(w =>
                                        handleShardCheckpoint(
                                          w,
                                          shardAcceptanceDeps,
                                          shardCheckpointAttestationEmitter,
                                          watchtowerFraudProofEmitter,
                                          shardCheckpointFetcher,
                                          pendingShardCheckpointRef,
                                          ancestryRecoveryDepth - 1,
                                          selfId,
                                          logger
                                        )
                                      ) >>
                                      entry.chainStore.getByHash(checkpoint.parentCheckpointHash).map(_.isDefined)
                                }
                              }
                          }

                      afterCheckpointParentRecovery(parentReadyF)(
                        defer = Hasher[F].hash(checkpoint.signingPreimage).flatMap { checkpointHash =>
                          bufferBoundedPendingCheckpointChild(
                            checkpoint.parentCheckpointHash,
                            checkpointHash,
                            cp,
                            cp.toByteArray.length.toLong,
                            MaxPendingShardCheckpointCount,
                            MaxPendingShardCheckpointBytes,
                            pendingShardCheckpointRef
                          ).flatMap { buffered =>
                            val diagnostic =
                              s"shard=${checkpoint.shardId.value.value} shardOrd=${checkpoint.shardOrdinal.value} " +
                                s"parent=${checkpoint.parentCheckpointHash.value.take(16)} depthRemaining=$ancestryRecoveryDepth " +
                                s"buffered=${buffered.buffered} duplicate=${buffered.duplicate} evicted=${buffered.evicted}"
                            if (buffered.buffered || buffered.duplicate)
                              logger.info(s"ShardCheckpoint deferred pending exact parent: $diagnostic")
                            else
                              logger.warn(s"ShardCheckpoint pending-parent buffer refused child: $diagnostic")
                          }
                        },
                        validateAndReplay = entry.chainStore.getByHash(checkpoint.parentCheckpointHash).flatMap { parentOpt =>
                          val slotMonotone = parentOpt.forall(_.signed.value.slot.value.value < checkpoint.slot.value.value)
                          if (!slotMonotone)
                            logger.warn(
                              s"🧩 ShardCheckpoint REJECTED (slot not monotone vs parent): shard=${checkpoint.shardId.value.value} " +
                                s"shardOrd=${checkpoint.shardOrdinal.value} slot=${checkpoint.slot.value.value} " +
                                s"parentSlot=${parentOpt.map(_.signed.value.slot.value.value).getOrElse(-1L)}"
                            )
                          else
                            // S3 — FIX THE ATTESTATION INVERSION. Enforce the shard replay capability boundary:
                            // re-exec/validate the checkpoint FIRST, and
                            // only adopt it into the fork DAG + count its signers' attestations + emit OUR own attestation if the
                            // derivation matched. Previously the emit fired on `becameBestTip` BEFORE `evaluate`, so a node could attest
                            // (and adopt) a checkpoint whose derivation it had not re-run. `evaluate` now runs `reExecuteDerivation`
                            // unconditionally, before the checkpoint enters the store or local quorum count. A
                            // `Rejected`/`RejectedReExecutionMismatch` result is DROPPED — not stored as adoptable, signers NOT counted, NO
                            // attestation emitted (a re-exec deviator must not have its checkpoint adopted nor be rewarded with our
                            // attestation). The generation-owned shard worker keeps the whole replay/store/emit sequence off the shared
                            // demultiplexer while retaining cancellation ownership.
                            // S1 — exact-hash GL0 Phase-2 operational anchor. This is density-reorgable, not an immutable finality floor.
                            // Pull GL0's latest Phase-2 checkpoint hash for this shard into fork choice. When the operational anchor IS in
                            // the local store, `noteAnchor` reorgs onto it (the #42 heal). When it is ABSENT — this node followed a divergent
                            // LONGER tine and never received the Phase-2 checkpoint (the run-24 freeze: GL0 selected C7A on tine α while local
                            // maxvalid-tk kept extending tine β) — `noteAnchor`
                            // silently REFUSES a not-yet-stored hash (`ShardChainStore`: "never replace a known anchor with a
                            // not-yet-stored hash"), so the anchor can never bite and the longer rogue tine wins forever. Fix:
                            // fetch the Phase-2 anchor by hash inside the owning shard worker and re-feed it; the re-feed recursively pulls its
                            // ancestry via the T1 trigger above until it connects, and the NEXT receipt's `noteAnchor` then succeeds
                            // and `compareAnchoredMaxvalid` collapses the fork onto the finalized tine. Receipt-piggybacked +
                            // idempotent; the fetch is best-effort/deduped in `ShardCheckpointFetcher`.
                            deps.acceptanceManager.lastAdoptedAnchor(checkpoint.shardId).flatMap {
                              case None => Async[F].unit
                              case Some(anchorHash) =>
                                entry.chainStore.getByHash(anchorHash).flatMap {
                                  case Some(_) => entry.chainStore.noteAnchor(anchorHash)
                                  case None =>
                                    shardCheckpointFetcher.fold(Async[F].unit) { fetcher =>
                                      fetcher.fetchByHash(checkpoint.shardId, anchorHash).flatMap {
                                        case Some(anchorSigned) =>
                                          logger.info(
                                            s"🧩 ShardCheckpoint Phase-2-anchor FETCH: shard=${checkpoint.shardId.value.value} " +
                                              s"anchor=${anchorHash.value.take(12)} absent locally — pulling GL0 operational tine (run-24 S1)"
                                          ) >>
                                            io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
                                              .signedShardCheckpointToWire[F](anchorSigned)
                                              .flatMap(w =>
                                                handleShardCheckpoint(
                                                  w,
                                                  shardAcceptanceDeps,
                                                  shardCheckpointAttestationEmitter,
                                                  watchtowerFraudProofEmitter,
                                                  shardCheckpointFetcher,
                                                  pendingShardCheckpointRef,
                                                  MaxShardCheckpointAncestryRecoveryDepth,
                                                  selfId,
                                                  logger
                                                )
                                              )
                                        case None => Async[F].unit
                                      }
                                    }
                                }
                            } >>
                              deps.acceptanceManager.evaluateForSigning(checkpoint).flatMap {
                                case Left(failure) =>
                                  val emitPortableEvidence = shardCheckpointFraudEvidenceEligible(failure)
                                  logger.info(
                                    s"🧩 ShardCheckpoint rx shard=${checkpoint.shardId.value.value} " +
                                      s"shardOrd=${checkpoint.shardOrdinal.value} gl0Anchor=${checkpoint.gl0AnchorOrdinal.value.value} " +
                                      s"signers=${checkpoint.committeeSignatures.size} evaluate=${failure.acceptanceResult} " +
                                      (if (emitPortableEvidence)
                                         s"— NOT adopted/attested; emitting only certificate-authenticated portable mismatch evidence"
                                       else s"— NOT adopted/attested (re-exec unavailable or pre-check/structural reject; no evidence)")
                                  ) >> Async[F].whenA(emitPortableEvidence) {
                                    watchtowerFraudProofEmitter.fold(Async[F].unit)(_.emit(checkpoint))
                                  }
                                case Right(verifiedCheckpoint) =>
                                  // The capability above was minted by this exact intake replay. Store/count/sign are unreachable for a
                                  // rejected or mismatching checkpoint, and the received checkpoint is not replayed a second time merely to
                                  // satisfy the emitter API.
                                  val checkpointHash = verifiedCheckpoint.signingPreimageHash
                                  val adoptAndAttest =
                                    io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointChainStoreRecovery
                                      .ingestValidated(checkpoint, entry.chainStore)
                                      .flatMap { recovered =>
                                        Async[F].raiseUnless(recovered.checkpointHash === checkpointHash) {
                                          new IllegalStateException(
                                            s"verified checkpoint recovery hash mismatch: capability=${checkpointHash.value} " +
                                              s"recovered=${recovered.checkpointHash.value}"
                                          )
                                        } >>
                                          // Became-best-tip gate — byte-identical to the gl0 `becameBest = isNew && bestTipOpt.exists(_.hash ===
                                          // thisHash)` seam. `store` returns `isNew`; if the (re-exec-validated) checkpoint is now the canonical
                                          // bestTip (per ShardChainStore maxvalid-tk fork choice), THIS node attests once for this winning hash.
                                          entry.chainStore.bestTip.flatMap { bestTipOpt =>
                                            val becameBestTip = recovered.inserted && bestTipOpt.exists(_.hash === checkpointHash)
                                            // WATCHTOWER approval-check (fraud-proof part 1): on adopting this checkpoint as canonical best tip,
                                            // defensively re-execute its complete batch after the emitter re-verifies the execution certificate,
                                            // and gossip a FraudProofEnvelope on any mismatch. Affirmative intake mismatches take the earlier
                                            // rejection branch and emit without adoption. This successful-path replay remains inside the
                                            // bounded generation-owned shard worker, so reconnect/cancellation cannot leave stale replay or
                                            // publication mutating state. `None` (numShards=1 / watchtower disabled) ⇒ skipped. Only on
                                            // becameBestTip: a non-canonical sibling is not adopted, so its effects are never applied — no need
                                            // to dispute it (and re-deriving it against our S(N) could false-mismatch).
                                            Async[F].whenA(becameBestTip) {
                                              watchtowerFraudProofEmitter match {
                                                case None          => Async[F].unit
                                                case Some(emitter) => emitter.emit(checkpoint)
                                              }
                                            } >>
                                              // Record every committee signer (the producer's own sig seeds 1 attestation) into the tip tracker —
                                              // only now that re-exec validated the checkpoint (so a wrong-derivation envelope never inflates quorum).
                                              checkpoint.committeeSignatures.toList
                                                .traverse_(sig => entry.tipTracker.recordAttestation(checkpointHash, sig.peerId, sig)) >>
                                              // Execution-certificate closure: sign + gossip after local replay so every other node's tracker
                                              // can reach configured `kQuorum`. `None`
                                              // emitter (numShards=1 regression bar) ⇒ no emit. The surrounding bounded shard worker owns the
                                              // multi-step sign/publish lifecycle.
                                              //
                                              // ATTEST ON EVERY ADMISSIBLE RECEIPT, not only on becameBestTip (2026-06-11, run bc5a17r12):
                                              // the best-tip-only gate + the tip-triggered ancestor walk still missed OUT-OF-ORDER arrivals —
                                              // when ord 12 gossips in before ord 11, 12 becomes best tip while 11 is unknown (the ancestor
                                              // walk hits getByHash=None and stops), and when 11 lands later it is NOT a new best tip, so no
                                              // emit ever fires for it. Checkpoints 11/12 then sat below quorum for ~3 min (19 consecutive
                                              // embed-none ords) — the admission plateaus that expired DoubleUse's allow-spend mid-flight.
                                              // The canonical-chain walk below starts from the CURRENT best tip on every receipt and attests
                                              // anything stored + not yet self-attested, so a late-arriving parent is attested the moment it
                                              // lands. Idempotent (tracker self-record) and cheap (≤16 tracker lookups per received envelope).
                                              Async[F]
                                                .whenA(true) {
                                                  shardCheckpointAttestationEmitter match {
                                                    case None          => Async[F].unit
                                                    case Some(emitter) =>
                                                      // ATTEST THE CHAIN, NOT JUST THE TIP (2026-06-11, run bp4wrh5zq): members previously attested
                                                      // only the checkpoint that was the best tip AT THE MOMENT IT ARRIVED. During bootstrap, nodes
                                                      // join the shard topics staggered over minutes while the producer keeps extending, so early
                                                      // shardOrds scroll past before most members are listening — the genesis checkpoint sat at
                                                      // signers=1 for 7+ minutes, and because gl0 embedding is ancestor-first, the WHOLE shard chain's
                                                      // admission was blocked behind the under-attested ancestor (the run-6 DoubleUse spend-action
                                                      // timeout). On becoming best tip, also attest every stored canonical ancestor this node has not
                                                      // yet attested. The membership gate is deliberately per ancestor and uses that ancestor's OWN
                                                      // epoch: gating the whole walk on the received checkpoint's committee would skip valid older
                                                      // duties across committee rotation. Verifiers dedupe by peerId and independently check membership.
                                                      def attestMissingAncestors(h: Hash, remaining: Int): F[Unit] =
                                                        if (remaining <= 0 || h === Hash.empty) Async[F].unit
                                                        else
                                                          entry.chainStore.getByHash(h).flatMap {
                                                            case None => Async[F].unit // deeper than our stored view — stop
                                                            case Some(anc) =>
                                                              val ancCp = anc.signed.value
                                                              entry.tipTracker.signaturesFor(anc.hash).flatMap { sigs =>
                                                                Async[F].whenA(!sigs.contains(selfId)) {
                                                                  deps.committeeMembership(ancCp.shardId, ancCp.epoch).flatMap { ancCommittee =>
                                                                    Async[F].whenA(ancCommittee.contains(selfId)) {
                                                                      // Ancestors are subject to the same intake replay gate as the received tip. `verifyEmbedded`
                                                                      // is deliberately wrong here because it requires a completed kQuorum, while these signatures
                                                                      // are what close that quorum. Reuse the received checkpoint's capability when it is this
                                                                      // ancestor; every other signing attempt calls `evaluate` exactly once before emit.
                                                                      val verifiedAncestorF =
                                                                        if (anc.hash === verifiedCheckpoint.signingPreimageHash)
                                                                          (Some(verifiedCheckpoint): Option[VerifiedShardCheckpoint])
                                                                            .pure[F]
                                                                        else
                                                                          deps.acceptanceManager
                                                                            .evaluateForSigning(ancCp)
                                                                            .flatMap {
                                                                              case Right(verifiedAncestor) =>
                                                                                (Some(verifiedAncestor): Option[
                                                                                  VerifiedShardCheckpoint
                                                                                ])
                                                                                  .pure[F]
                                                                              case Left(failure) =>
                                                                                logger
                                                                                  .warn(
                                                                                    s"ShardCheckpoint skip ancestor attestation: " +
                                                                                      s"shard=${ancCp.shardId.value.value} shardOrd=${ancCp.shardOrdinal.value} " +
                                                                                      s"executionResult=${failure.acceptanceResult}"
                                                                                  )
                                                                                  .as(Option.empty[VerifiedShardCheckpoint])
                                                                            }

                                                                      verifiedAncestorF.flatMap {
                                                                        case None => Async[F].unit
                                                                        case Some(verifiedAncestor) =>
                                                                          logger.info(
                                                                            s"ShardCheckpoint attest-ancestor after re-exec: " +
                                                                              s"shard=${ancCp.shardId.value.value} shardOrd=${ancCp.shardOrdinal.value} " +
                                                                              s"slot=${ancCp.slot.value.value}"
                                                                          ) >> emitter.emit(verifiedAncestor)
                                                                      }
                                                                    }
                                                                  }
                                                                } >> attestMissingAncestors(ancCp.parentCheckpointHash, remaining - 1)
                                                              }
                                                          }

                                                      // Walk from the CURRENT canonical tip (not the received envelope): covers the received
                                                      // checkpoint when it IS the tip, late-arriving ancestors when it is not, and any other
                                                      // unattested canonical entries in between.
                                                      entry.chainStore.bestTip.flatMap {
                                                        case None      => Async[F].unit
                                                        case Some(tip) => attestMissingAncestors(tip.hash, MaxAncestorAttestWalk)
                                                      }
                                                  }
                                                }
                                                .as(becameBestTip)
                                          }
                                      }
                                  adoptAndAttest.flatMap { becameBestTip =>
                                    logger.info(
                                      s"🧩 ShardCheckpoint rx shard=${checkpoint.shardId.value.value} " +
                                        s"shardOrd=${checkpoint.shardOrdinal.value} gl0Anchor=${checkpoint.gl0AnchorOrdinal.value.value} " +
                                        s"signers=${checkpoint.committeeSignatures.size} becameBestTip=$becameBestTip " +
                                        s"evaluate=replay-valid (capability minted)"
                                    ) >>
                                      drainBoundedPendingCheckpointChildren(checkpointHash, pendingShardCheckpointRef).flatMap {
                                        _.traverse_(pendingChild =>
                                          handleShardCheckpoint(
                                            pendingChild,
                                            shardAcceptanceDeps,
                                            shardCheckpointAttestationEmitter,
                                            watchtowerFraudProofEmitter,
                                            shardCheckpointFetcher,
                                            pendingShardCheckpointRef,
                                            MaxShardCheckpointAncestryRecoveryDepth,
                                            selfId,
                                            logger
                                          )
                                        )
                                      }
                                  }
                              }
                        }
                      )
                    }.flatMap {
                      case Left(reason) =>
                        logger.warn(
                          s"Rejecting ShardCheckpoint before parent recovery: shard=${checkpoint.shardId.value.value} " +
                            s"shardOrd=${checkpoint.shardOrdinal.value} producer=${checkpoint.producerSignature.peerId.value.value.take(16)}... " +
                            s"reason=$reason"
                        )
                      case Right(()) => Async[F].unit
                    }
                  }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle ShardCheckpoint: ${err.getMessage}")
          }
    }

  /** Gap B — handle an incoming `ShardCheckpointAttestationWire` from gossip. Decode, resolve the referenced checkpoint from the local
    * shard store, then run the SAME full per-signer pre-check as checkpoint admission (committee membership + Ed25519 + KES + VRF) before
    * recording the attester into the per-shard tip tracker. `None` deps (numShards=1) ⇒ drop with a debug log.
    *
    * '''Why every predicate runs before recording.''' The gl0 leader splices every tracker signature into a candidate checkpoint, and
    * `verifyEmbedded` rejects the whole checkpoint if any signer fails any pre-check. Recording an Ed25519-valid outsider (or a committee
    * member's first-seen signature with invalid KES/VRF bytes) would therefore poison that checkpoint indefinitely. Unknown hashes are
    * dropped rather than cached: without the checkpoint bytes there is no shard/epoch/slot context in which committee, KES, or VRF can be
    * verified. Only a signature accepted by `verifyCommitteeSignature` reaches the tracker.
    */
  private def handleShardCheckpointAttestation[F[_]: Async: SecurityProvider: HasherSelector: Metrics](
    att: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.ShardCheckpointAttestationWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAcceptanceDeps match {
      case None =>
        logger.debug("Received ShardCheckpointAttestation but sharding inactive (numShards=1); dropping")
      case Some(deps) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWireCodecs
          .shardCheckpointAttestationFromWire[F](att)
          .flatMap { attestation =>
            deps.registry.get(attestation.shardId) match {
              case None =>
                logger.debug(
                  s"Received ShardCheckpointAttestation for untracked shard=${attestation.shardId.value.value}; dropping"
                )
              case Some(entry) =>
                val attesterId = attestation.attesterSignature.peerId
                entry.chainStore.getByHash(attestation.checkpointHash).flatMap {
                  case None =>
                    logger.warn(
                      s"Rejecting ShardCheckpointAttestation for unknown checkpoint shard=${attestation.shardId.value.value} " +
                        s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}..."
                    )
                  case Some(stored) if stored.signed.value.shardId =!= attestation.shardId =>
                    logger.warn(
                      s"Rejecting ShardCheckpointAttestation with shard mismatch wire=${attestation.shardId.value.value} " +
                        s"checkpoint=${stored.signed.value.shardId.value.value} hash=${attestation.checkpointHash.value.take(12)}"
                    )
                  case Some(stored) =>
                    deps.acceptanceManager
                      .verifyCommitteeSignature(stored.signed.value, attestation.attesterSignature)
                      .flatMap {
                        case Left(reason) =>
                          logger.warn(
                            s"Rejecting invalid ShardCheckpointAttestation shard=${attestation.shardId.value.value} " +
                              s"checkpoint=${attestation.checkpointHash.value.take(12)} from=${attesterId.value.value.take(16)}... " +
                              s"reason=$reason"
                          )
                        case Right(()) =>
                          entry.tipTracker.recordAttestation(attestation.checkpointHash, attesterId, attestation.attesterSignature) >>
                            logger.debug(
                              s"ShardCheckpointAttestation rx shard=${attestation.shardId.value.value} " +
                                s"checkpoint=${attestation.checkpointHash.value.take(12)} attester=${attesterId.value.value.take(12)}"
                            )
                      }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle ShardCheckpointAttestation: ${err.getMessage}")
          }
    }

  /** WATCHTOWER dispute consumer (fraud-proof part 2) — handle an incoming `FraudProofEnvelopeWire`.
    *
    * '''Deterministic verdict, recomputed not trusted.''' Decode the envelope, resolve the disputed `ShardCheckpoint` from the local
    * per-shard chain store BY HASH (the checkpoint the committee signed — its bytes are the verdict's inputs), build the
    * [[io.constellationnetwork.schema.slashing.InvalidStateProofEvidence]], and run
    * [[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator]] — which re-derives the honest per-MG root
    * from the checkpoint's OWN `includedSnapshots` (pure / finalized-base) and upholds iff it differs from the committee-attested root. The
    * verdict is identical on every gl0 because it recomputes; the challenger's claimed roots in the envelope are never read for the
    * verdict.
    *
    * '''On UPHELD''': log loudly (this slice surfaces the verdict + the slash-target committee). The AUTHORITATIVE 100% slash is applied by
    * the GSAM accept path when an `InvalidStateProofEvidence` (carrying the full checkpoint) lands in a global snapshot — submitting that
    * evidence to the L0 mempool is the remaining wiring (see the GSAM `InvalidStateProofSlashManager` sink + its TODO). On NOT-upheld (the
    * honest-committee floor) / any rejection, log + drop — a frivolous or forged fraud proof has no effect.
    *
    * '''Checkpoint not in local store''': we cannot re-derive (the checkpoint may have been pruned, or we never tracked this shard). Drop
    * with a debug log — the on-chain evidence path carries the full checkpoint and does NOT depend on local availability.
    *
    * `None` deps / validator (numShards=1) ⇒ drop with a debug log.
    */
  private def handleFraudProof[F[_]: Async: JsonSerializer: HasherSelector: SecurityProvider: Metrics](
    fpWire: io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar.FraudProofEnvelopeWire,
    shardAcceptanceDeps: Option[
      io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.AcceptanceDeps[F]
    ],
    invalidStateProofValidator: Option[
      io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator[F]
    ],
    fraudProofPool: io.constellationnetwork.node.shared.infrastructure.sharding.WatchtowerFraudProofPool[F],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    (shardAcceptanceDeps, invalidStateProofValidator) match {
      case (Some(deps), Some(validator)) =>
        io.constellationnetwork.node.shared.infrastructure.sharding.FraudProofWireCodecs
          .fromWire[F](fpWire)
          .flatMap { fp =>
            deps.registry.get(fp.shardId) match {
              case None =>
                logger.debug(s"🛡️ FraudProof for untracked shard=${fp.shardId.value.value}; dropping")
              case Some(entry) =>
                entry.chainStore.getByHash(fp.disputedCheckpointHash).flatMap {
                  case None =>
                    logger.debug(
                      s"🛡️ FraudProof: disputed checkpoint ${fp.disputedCheckpointHash.value.take(12)} not in local store " +
                        s"(shard=${fp.shardId.value.value}); cannot re-derive verdict locally — dropping (on-chain evidence carries it)"
                    )
                  case Some(hashedCp) =>
                    val cp = hashedCp.signed.value
                    val attested = cp.derivedStateDelta.perMetagraphMptRoots
                      .get(fp.metagraphAddress)
                      .getOrElse(io.constellationnetwork.security.hash.Hash.empty)
                    val evidence = io.constellationnetwork.schema.slashing.InvalidStateProofEvidence(
                      shardId = fp.shardId,
                      disputedCheckpoint = cp,
                      metagraphAddress = fp.metagraphAddress,
                      attestedRoot = attested,
                      fraudProof = fp
                    )
                    validator.validate(evidence).flatMap {
                      case Right(upheld) =>
                        // UPHELD locally — OFFER the validated evidence into the node-local pool so the gl0 leader embeds it as the
                        // `fraudProofs` consensus artifact (W3a). The on-chain GSAM accept path re-validates it DETERMINISTICALLY on every
                        // node and applies the 100% slash + bounty to the challenger; this offer is a pure liveness aid (the authoritative
                        // verdict + slash are the consensus fold, never this pool). The set keys on the dispute's `(shardId, checkpointHash)`.
                        fraudProofPool.offer(upheld) >>
                          logger.warn(
                            s"🛡️ WATCHTOWER dispute UPHELD: shard=${fp.shardId.value.value} " +
                              s"checkpoint=${fp.disputedCheckpointHash.value.take(12)} mg=${fp.metagraphAddress.value.value.take(10)} " +
                              s"slashTargets=${evidence.slashTargets.size} — committee signed a wrong derivation (queued for on-chain 100% " +
                              s"InvalidStateProof slash via the fraudProofs consensus artifact)"
                          )
                      case Left(rejection) =>
                        logger.info(
                          s"🛡️ WATCHTOWER dispute NOT upheld (no slash): shard=${fp.shardId.value.value} " +
                            s"checkpoint=${fp.disputedCheckpointHash.value.take(12)} reason=$rejection"
                        )
                    }
                }
            }
          }
          .handleErrorWith { err =>
            logger.warn(s"⚠️ Failed to handle FraudProof: ${err.getMessage}")
          }
      case _ =>
        logger.debug("Received FraudProof but sharding/watchtower inactive (numShards=1); dropping")
    }

  /** Route an incoming state channel binary from gossip into the [[MetagraphCommitteeGate]] — the gate computes the committee sortition for
    * this node, emits an attestation if selected, and waits for configured `kQuorum` attestations to land in the aggregator before
    * admitting the binary into the local acceptance pipeline.
    *
    * The sender serialized Signed[StateChannelSnapshotBinary] via the project's JsonSerializer (JSON + Brotli); we use the same typeclass
    * to deserialize. Decode failures and gate timeouts both result in the binary being dropped (WARN-logged with enough detail for an
    * operator to diagnose).
    *
    * '''Why the gate runs here, not deeper.''' `processMetagraphBinary` is invoked from two call sites: (a) this gossip handler, and (b)
    * `StateChannelRoutes` over HTTP for CL0-originated binaries. The HTTP path is local-only on the receiving gl0; that gl0's local state
    * isn't load-bearing without entering a finalized global snapshot, and a global snapshot only finalizes once 2/3 of gl0s attest. Each
    * peer gl0 receiving the snapshot proposal applies the same gate on the snapshot's `stateChannelSnapshots` entries — so a binary that
    * skipped the local HTTP gate at gl0-A will still be gated at every other gl0 when it arrives via gossip. The cluster-wide safety
    * property holds against an adversarial submission at a single gl0.
    */
  /** Build the gate-aware metagraph-binary processor: a closure that takes `(metagraphAddress, wireBytes)` and runs the binary through the
    * committee gate (#192 Slice S3), processing it on admit and orphan-buffering it on parent-not-yet-resolved (#213). The same closure is
    * invoked from both call sites that need this path:
    *
    *   - `handleMetagraphBinary` (the daemon's gossip handler) — for fresh `pb.MetagraphBinary` arrivals on the wire.
    *   - `SnapshotLeaderLoop.onFinalize` (via `GlobalSnapshotConsensus.make`) — for orphan-drain on gl0 snapshot finalize (#214). When gl0
    *     finalizes a global snapshot containing a metagraph binary that some peers' local gates dropped, those local nodes' orphan-buffered
    *     children of the just-finalized binary become resolvable (the parent's value-hash now lives in gl0's GSI). Re-feeding them through
    *     THIS gate-aware processor — not direct `processMetagraphBinary` — is the load-bearing safety property: the local gate still
    *     attests and the child gets cluster-confirmed normally. The earlier attempt (reverted, ad7d4f041 → fea66fc7d) bypassed the gate and
    *     polluted the state-channel queue.
    *
    * '''Why a factory.''' Both call sites need the same closure, and the closure recursively re-enters itself when draining children of a
    * just-admitted parent. Defining the closure top-level (rather than nested inside `handleMetagraphBinary`) lets the finalize-hook
    * callable share the exact same shape — same orphan buffer, same admission cache, same recursive drain.
    */
  def makeMetagraphBinaryProcessor[F[_]: Async: io.constellationnetwork.json.JsonSerializer: HasherSelector: Metrics](
    processMetagraphBinary: io.constellationnetwork.statechannel.StateChannelOutput => F[Unit],
    committeeGate: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate[F],
    // Decode the signed framework-currency context after applying the metagraph-parent identity guard.
    currencyContextFor: (
      io.constellationnetwork.schema.address.Address,
      io.constellationnetwork.security.hash.Hash,
      Array[Byte]
    ) => F[Option[MetagraphParentOrdinalResolver.CurrencyBinaryContext]],
    // Content-only form used only after the admission cache has already proved the metagraph parent.
    currencyContextFromContent: Array[Byte] => F[Option[MetagraphParentOrdinalResolver.CurrencyBinaryContext]],
    // Mint a typed exact-Phase-2 capability. This must recheck current canonical GL0 hash every call.
    verifyPhase2CurrencyContext: MetagraphParentOrdinalResolver.CurrencyBinaryContext => F[
      Option[MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext]
    ],
    etaForPhase2Anchor: MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext => F[Array[Byte]],
    etaRotationSnapshots: Long,
    selfStake: F[io.constellationnetwork.numerics.Ratio],
    // #29 verify-on-attach: per-sender stake lookup used to verify the attestations we BUFFERED un-verified
    // while a binary was an orphan, replayed at attach time below (`drainAttestations`). Same lookup the
    // inbound-attestation receiver uses — `stakeRegistry.committeeStake`.
    senderStakeLookup: io.constellationnetwork.schema.peer.PeerId => F[io.constellationnetwork.numerics.Ratio],
    orphanBuffer: io.constellationnetwork.node.shared.domain.nakamoto.MetagraphOrphanBuffer[F],
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ] = Map.empty[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]] = None,
    logger: org.typelevel.log4cats.Logger[F]
  ): (io.constellationnetwork.schema.address.Address, Array[Byte]) => F[Unit] = {
    import io.constellationnetwork.schema.address.Address
    import io.constellationnetwork.statechannel.{StateChannelOutput, StateChannelSnapshotBinary}
    import io.constellationnetwork.security.signature.Signed

    // Resolver wrapper that bridges gl0's GSI lag. After we admit a binary X via the committee gate,
    // X is enqueued for processing but won't land in gl0's `lastStateChannelSnapshotHashes` until the
    // next global snapshot becomes operational (~7s later). Without a shortcut, any drained child of X
    // misses the local ML0 tip and immediately re-buffers, never making progress.
    // The orphan buffer's admission cache holds `(mg, X.value.hash) → mgOrd_X` for that GSI-lag window.
    //
    // ARITHMETIC (cache fast-path == content derivation, end-to-end): on admit of binary B we record
    // `(mg, valueHash(B)) → parentOrdinal + 1 = ord(B)` (B's own ordinal). The next child C chains off B
    // (`C.parentHash = valueHash(B)`, `ord(C) = ord(B)+1`). Resolving C's parent ordinal: cache hit on
    // `valueHash(B)` → `ord(B)`; content path → `ord(C) − 1 = ord(B)`. Identical. (Genesis ord 0 → first
    // incremental ord 1 has parent ordinal 0; admitted ord 1 records admission value 1; child ord 2 resolves 1.)
    // `incomingContent` is THIS binary's `content` bytes, fed to the content-derivation resolver on cache miss.
    def resolveContext(
      address: Address,
      parentHash: Hash,
      incomingContent: Array[Byte]
    ): F[
      Option[(MetagraphParentOrdinalResolver.CurrencyBinaryContext, MetagraphParentOrdinalResolver.Phase2CurrencyBinaryContext)]
    ] =
      orphanBuffer
        .lookupAdmittedOrd(address, parentHash)
        .flatMap {
          case Some(cachedParentOrdinal) =>
            currencyContextFromContent(incomingContent).map(
              _.filter(_.metagraphParentOrdinal == cachedParentOrdinal)
            )
          case None => currencyContextFor(address, parentHash, incomingContent)
        }
        .flatMap(_.traverse(context => verifyPhase2CurrencyContext(context).map(_.map(context -> _))).map(_.flatten))

    // After a binary is admitted, drain any orphans whose parent equals the just-accepted binary's
    // *value*-hash (`signed.value.hash`) — same hash gl0's GSAM writes into `lastStateChannelSnapshotHashes`.
    // Each drained child runs through the same path, which may itself unblock further descendants, so
    // the chain unwinds in order. #213.
    def processBytes(address: Address, bytes: Array[Byte]): F[Unit] =
      io.constellationnetwork.json
        .JsonSerializer[F]
        .deserialize[Signed[StateChannelSnapshotBinary]](bytes)
        .flatMap {
          case Left(err) =>
            logger.warn(s"⚠️ Rejecting metagraph-binary gossip: decode failed for $address (${err.getMessage})")
          case Right(signed) =>
            val output = StateChannelOutput(address, signed)
            val parentHash = signed.value.lastSnapshotHash
            // Hash the wire bytes to produce the binary hash — uses the same `Hasher[F]` surface that
            // the gate's verifier uses to match `binaryHash` across observers. The bytes here are the
            // serialized Signed[StateChannelSnapshotBinary]; both sender and receiver hash the same
            // wire payload so the binary-hash agrees byte-for-byte.
            HasherSelector[F].withCurrent { implicit hasher =>
              Hasher[F].hashBytes(bytes).flatMap { binaryHash =>
                // Gate inputs: sigma_self plus eta and key period derived from the binary's exact,
                // currently Phase-2 GL0 anchor. Metagraph ordinal is only continuity state.
                //
                // Resolver returns None when gl0's GSI doesn't yet have a metagraph snapshot under
                // this parent hash AND the admission cache doesn't either. At 4-mg+committee-gate
                // scale this is the normal state for any binary chained off something we haven't
                // admitted yet: ml0 races ahead of gl0's gate cadence and chains forward off
                // binaries gl0 hasn't seen yet. Buffer in the orphan pool keyed by parentHash;
                // when the matching binary IS admitted, drainChildren replays them in chronological
                // order. #213.
                resolveContext(address, parentHash, signed.value.content).flatMap {
                  case None =>
                    orphanBuffer.record(address, parentHash, bytes).flatMap { sz =>
                      // Occupancy gauge (eval-instrumentation): the orphan backlog is the leading indicator
                      // of a metagraph that can't get its binaries admitted (committee-gate timeout cascade
                      // → chain-link break → orphans pile up). Watch this approach `nakamoto.orphan-buffer-cap`
                      // (default 1024) — a monotonic climb is the "buffer filling up" wedge.
                      Metrics[F].updateGauge("dag_nakamoto_orphan_buffer_size", sz) >>
                        logger.info(
                          s"Orphan-buffered mg=$address parent=${parentHash.value.take(12)}... " +
                            s"(parent unknown, missing signed GL0 anchor, or anchor not exact Phase 2) bufferSize=$sz"
                        )
                    }
                  case Some((context, verifiedContext)) =>
                    val parentOrdinal = context.metagraphParentOrdinal
                    for {
                      // Cache the signed context for eager inbound-attestation verification. Receivers recheck
                      // exact Phase-2 status; cache residence does not preserve authorization across a reorg.
                      _ <- orphanBuffer.recordPendingBinaryContext(address, binaryHash, context)
                      // The just-resolved binary's *value* hash — the chain-link identity the NEXT binary's
                      // `lastSnapshotHash` points at (GlobalSnapshotAcceptanceManager.scala:908 /
                      // StateChannelSnapshotService.scala:112). NOT `binaryHash` (the wire-bytes digest). Uses
                      // the `implicit hasher` already in scope from the enclosing `HasherSelector.withCurrent`.
                      valueHash <- signed.toHashed.map(_.hash)
                      // #213/#290 admission-lag fix: seed the value-hash→ordinal admission cache the instant we
                      // RESOLVE the binary (we are in the `Some` branch, so `resolveContext` already verified its
                      // parent matched this peer's recorded tip / a cached ancestor — the identity guard ran),
                      // NOT only after the local committee gate admits it below. `parentOrdinal + 1` is THIS
                      // binary's own metagraph ordinal — a pure function of its content (ordinal − 1, then + 1),
                      // so every honest node caches the byte-identical value; recording it neither weakens the
                      // identity guard. It does not feed committee eta or key-period selection; those use only the
                      // separately checked exact GL0 anchor. WHY at resolve-time, not admit-time: this node's local gate can TIME OUT (a
                      // few peers were transiently behind and buffered this binary instead of attesting, so the
                      // kQuorum was not reached) even though the binary's place in the chain is fixed and the
                      // cluster admits it via 2/3-attestation or depth-k. If we cached only on local-admit, the
                      // very next child would miss `lookupAdmittedOrd`, fall through to the tip-guarded resolver,
                      // find gl0's GSI tip still trailing this not-yet-finalized binary → `parentHash mismatch`
                      // → orphan-buffer; every successor then re-buffers off it (the 733-mismatch / orphan
                      // re-buffer loop with gl0 trailing ml0). Caching the deterministic ordinal here lets the
                      // child resolve and enter `attestAndAdmit` regardless of THIS node's gate outcome on the
                      // parent. The post-admit `recordAdmission` is now redundant and folded into this single
                      // unconditional write.
                      _ <- orphanBuffer.recordAdmission(address, valueHash, parentOrdinal + 1L)
                      eta <- etaForPhase2Anchor(verifiedContext)
                      // Phase 1 of verify-on-attach (#29). The binary has ATTACHED (its parent resolved → we're on
                      // the admit path). Verify NOW the attestations we buffered un-verified while it was an orphan,
                      // so `attestAndAdmit`'s threshold below counts them. Only this canonical (attached) binary's
                      // attestations get the crypto; losing tines' buffered attestations are never drained here and
                      // expire un-verified — that is the fork-storm CPU the speculative path was burning. A single
                      // forged/stale buffered attestation is logged + skipped, never aborts admission.
                      _ <- orphanBuffer.drainAttestations(address, binaryHash).flatMap { buffered =>
                        val artifactPeriod = io.constellationnetwork.schema.nakamoto.EtaPeriod(
                          EtaCalculation.rotationPeriod(verifiedContext.gl0AnchorOrdinal.value.value, etaRotationSnapshots)
                        )
                        buffered.traverse_ { bufferedAtt =>
                          committeeGate
                            .recordReceivedAttestation(bufferedAtt, eta, artifactPeriod, senderStakeLookup)
                            .handleErrorWith(e => logger.debug(s"⚠️ buffered attestation verify failed mg=$address: ${e.getMessage}"))
                        }
                      }
                      sigma <- selfStake
                      artifactPeriod = io.constellationnetwork.schema.nakamoto.EtaPeriod(
                        EtaCalculation.rotationPeriod(verifiedContext.gl0AnchorOrdinal.value.value, etaRotationSnapshots)
                      )
                      admitted <- committeeGate.attestAndAdmit(address, parentHash, binaryHash, eta, artifactPeriod, sigma)
                      _ <-
                        if (admitted)
                          (processMetagraphBinary(output) *>
                            Async[F].whenA(shardBinaryBuffers.nonEmpty)(
                              bufferAdmittedBinaryForShard(output, shardBinaryBuffers, shardAssignment, logger)
                            ))
                            .handleErrorWith(e => logger.warn(s"⚠️ processMetagraphBinary failed for $address: ${e.getMessage}"))
                            .flatMap(_ => orphanBuffer.drainChildren(address, valueHash))
                            .flatMap(_.traverse_(child => processBytes(address, child)))
                        else Async[F].unit
                    } yield ()
                }
              }
            }
        }

    processBytes
  }

  private def handleMetagraphBinary[F[_]](
    mb: pb.MetagraphBinary,
    processOrphanedBinary: (io.constellationnetwork.schema.address.Address, Array[Byte]) => F[Unit],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] = {
    import io.constellationnetwork.schema.address.{Address, DAGAddressRefined}
    import eu.timepit.refined.refineV

    refineV[DAGAddressRefined](mb.address) match {
      case Left(err) =>
        logger.warn(s"⚠️ Rejecting metagraph-binary gossip: invalid address '${mb.address}' ($err)")
      case Right(refined) =>
        val address = Address(refined)
        val bytes = mb.binary.toByteArray
        processOrphanedBinary(address, bytes)
    }
  }

  /** Buffer an admission-approved metagraph binary into the buffer for its execution shard. This function is called only after
    * `MetagraphCommitteeGate.attestAndAdmit` returns true; raw gossip receipt and ChainSync fetch are not admission conditions.
    *
    * '''Determinism model.''' Leader-proposes / members-attest: the buffer is node-local and need NOT converge across committee members —
    * only the shard slot leader builds the checkpoint from its own buffer; others attest the gossiped envelope (see
    * [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer]]). So we just deserialize, resolve the shard via the
    * SAME deterministic `ShardAssignment.shardIdFor` mapping, and `bufferBinary` (idempotent by hash) — no multi-proposer machinery.
    *
    * Only invoked when `shardBinaryBuffers.nonEmpty` (`numShards > 1`).
    */
  private def bufferAdmittedBinaryForShard[F[_]: Async: HasherSelector: Metrics](
    output: io.constellationnetwork.statechannel.StateChannelOutput,
    shardBinaryBuffers: Map[
      io.constellationnetwork.schema.sharding.ShardId,
      io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardBinaryBuffer[F]
    ],
    shardAssignment: Option[io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment[F]],
    logger: org.typelevel.log4cats.Logger[F]
  ): F[Unit] =
    shardAssignment match {
      case Some(assignment) =>
        val address = output.address
        HasherSelector[F].withCurrent { implicit hasher =>
          assignment.shardIdFor(address).flatMap { sid =>
            // Metagraph→shard mapping gauge (eval-instrumentation): emit one series per (mg, shard) so
            // `count by (shard_id) (dag_nakamoto_metagraph_shard_assignment)` shows the per-shard
            // metagraph load at a glance — a shard carrying >1 mg while another sits idle is the
            // imbalance that overloads one committee (the data-with-fee wedge). Idempotent (value 1).
            Metrics[F].updateGauge(
              "dag_nakamoto_metagraph_shard_assignment",
              1,
              Seq(
                Metrics.unsafeLabelName("shard_id") -> sid.value.value.toString,
                Metrics.unsafeLabelName("metagraph") -> address.value.value
              )
            ) >>
              (shardBinaryBuffers.get(sid) match {
                case Some(buffer) => buffer.bufferBinary(address, output.snapshotBinary)
                case None =>
                  logger.debug(s"Shard buffer missing for shard=${sid.value.value} (mg=$address); skipping admitted binary")
              })
          }
        }
      case None => Async[F].unit
    }

}
