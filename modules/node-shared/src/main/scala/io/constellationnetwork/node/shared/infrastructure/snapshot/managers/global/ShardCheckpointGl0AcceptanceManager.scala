package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptyList
import cats.effect.kernel.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardCheckpointProducerDutyValidator
import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardMetrics
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GlobalSnapshotStateRef}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Result of running gl0-side admission on a single [[ShardCheckpoint]].
  *
  * '''Why a sealed ADT''' — callers branch on three distinct outcomes (accept, defer, reject) plus an affirmative local re-execution
  * mismatch. Modelling as `Either[String, Boolean]` would conflate "cannot check" with "reproduced a different root." The latter may
  * trigger construction of portable evidence but never authorizes a local slash. Per `[[feedback-no-string-matching]]`, branches are typed;
  * the `reason` strings are diagnostic-only.
  */
sealed trait ShardCheckpointAcceptResult extends Product with Serializable

object ShardCheckpointAcceptResult {

  /** The checkpoint carries a valid distinct execution certificate and GL0 reproduced every included per-MG root.
    */
  case object Accepted extends ShardCheckpointAcceptResult

  /** Intake replay succeeded, but the checkpoint does not yet carry a distinct execution quorum. It remains admissible for committee replay
    * and attestation, but cannot be embedded. Consensus verification never returns this result.
    */
  case object PendingMoreAttestations extends ShardCheckpointAcceptResult

  /** The checkpoint failed a structural, signer, quorum, or replay check. `reason` is diagnostic; the typed branch is the control-flow
    * signal.
    */
  final case class Rejected(reason: String) extends ShardCheckpointAcceptResult

  /** GL0 affirmatively recomputed at least one different per-MG root. Signatures and quorum never suppress this result.
    */
  final case class RejectedReExecutionMismatch(reason: String, committeeSigners: List[PeerId]) extends ShardCheckpointAcceptResult
}

/** Capability proving that this node ran GL0 checkpoint intake validation, including deterministic framework replay, for the exact
  * [[checkpoint]]. The attestation emitter accepts this capability instead of naked routing/hash fields, so receipt, chain position, and
  * signature count cannot reach a state-validity signing API.
  *
  * Construction is private to the concrete manager returned by [[ShardCheckpointGl0AcceptanceManager.make]]. The capability binds the
  * replayed checkpoint to its canonical [[ShardCheckpoint.signingPreimage]] hash; callers cannot supply either an acceptance verdict or a
  * hash independently of the bytes that concrete manager replayed.
  */
sealed trait VerifiedShardCheckpoint extends Product with Serializable {
  def checkpoint: ShardCheckpoint
  def signingPreimageHash: Hash
}

/** Typed failure returned when a checkpoint cannot produce a [[VerifiedShardCheckpoint]]. */
sealed trait VerifiedShardCheckpointFailure extends Product with Serializable {
  def acceptanceResult: ShardCheckpointAcceptResult
}

object VerifiedShardCheckpointFailure {

  final case class Rejected(reason: String) extends VerifiedShardCheckpointFailure {
    val acceptanceResult: ShardCheckpointAcceptResult = ShardCheckpointAcceptResult.Rejected(reason)
  }

  final case class ReExecutionMismatch(reason: String, committeeSigners: List[PeerId]) extends VerifiedShardCheckpointFailure {
    val acceptanceResult: ShardCheckpointAcceptResult =
      ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, committeeSigners)
  }
}

/** Redundant watchtower finding for a per-MG root not reproduced by independent CL1 framework replay. Primary checkpoint intake already
  * executes the same check; this surface exists for independently gossiped fraud evidence. This replay concerns only sharded `CL1 -> ML0 ->
  * GL0` transitions; it does not replace or narrow universal GL0 execution of native `GL1 -> GL0` DAG-token transitions.
  *
  * @param metagraphAddress
  *   the metagraph whose derivation diverged
  * @param attestedRoot
  *   the committee-signed `perMetagraphMptRoots(metagraphAddress)` (the claim)
  * @param reDerivedRoot
  *   the root this node re-derived (the honest value) — same PIN-1 encoding as the attested root (the closure is the SAME one the mandatory
  *   pinned-base replay uses, so the comparison is byte-meaningful)
  */
final case class WatchtowerMismatch(
  metagraphAddress: io.constellationnetwork.schema.address.Address,
  attestedRoot: Hash,
  reDerivedRoot: Hash
)

/** gl0-side admission of [[ShardCheckpoint]]s — Slice 9 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §7 (especially §7.3).
  *
  * Sits between the shard committee's checkpoint gossip and the gl0 leader's per-ord accept loop. For each candidate checkpoint included in
  * the next gl0 snapshot:
  *
  *   1. '''Pre-checks''' (always): intake and embedded verification first prove that the retained head signature belongs to the exact
  *      parent-relative staircase producer. Then every committee signer must be (a) in the committee for `(shardId, epoch)`, (b) verifiable
  *      by their registered long-term Ed25519 VK, (c) verifiable by their KES product VK at the embedded tree-internal step, and (d) prove
  *      possession of their registered VRF key over the canonical `(shardEta, slot)` message. Failure on ANY check ⇒ rejection.
  *
  *   1. '''Execution''' (always): recreate every included CL1 window and compare every claimed root. Missing inputs or claims fail closed.
  *
  *   1. '''Execution certificate''' (always): require at least `kQuorum` distinct, valid execution-committee signatures. Shard-chain depth
  *      is never a substitute for independently reproduced execution.
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh manager for the shard-checkpoint admission path. No compat ceremony with [[GlobalSnapshotStateChannelAcceptanceManager]] —
  *     Slice 13 rewires GSAM to consume this; this slice creates the manager standalone.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - The execution quorum is injected from the validated committee configuration. There are no environment reads in this component.
  *
  * '''Decoupling rule''':
  *   - All shard-scope lookups are injected as callbacks (`committeeMembership`, `reExecuteDerivation`). Tests can stub each path
  *     independently; production wiring owns the per-shard state.
  */
trait ShardCheckpointGl0AcceptanceManager[F[_]] {

  /** gl0 leader runs this for each candidate [[ShardCheckpoint]] under consideration for inclusion in the next gl0 snapshot.
    *
    * Intake calls this before storing and attesting a candidate. Every carried signature and the deterministic transition are validated
    * first. A replay-valid candidate below quorum returns [[ShardCheckpointAcceptResult.PendingMoreAttestations]] so independently
    * replaying committee members can add signatures; it is not yet eligible for embedding.
    *
    * Returns one of the [[ShardCheckpointAcceptResult]] variants; the caller branches on the variant for inclusion / deferral / rejection
    * (+ optional slashing evidence emission).
    */
  def evaluate(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult]

  /** Run intake validation exactly once and return an attestation capability only when framework replay succeeds. Implementations outside
    * [[ShardCheckpointGl0AcceptanceManager.make]] cannot construct the sealed capability, even if their ordinary [[evaluate]] method claims
    * `Accepted`.
    */
  def evaluateForSigning(
    checkpoint: ShardCheckpoint
  ): F[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]]

  /** Consensus-critical verifier. It checks retained producer duty against the exact parent checkpoint, checks every signer, requires the
    * distinct execution quorum, and recreates every included CL1 transition. Missing retained parent data fails closed.
    */
  def verifyEmbedded(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult]

  /** Verify that the checkpoint carries a complete execution certificate before any signer can become a slash target. This runs the exact
    * epoch, structure, committee-membership, Ed25519, registered KES, registered VRF, distinct-signer, and execution-quorum checks used by
    * [[verifyEmbedded]], but deliberately does not replay the claimed state transition or consult receiver-local producer-duty history.
    * Producer scheduling is enforced on ordinary intake/adoption; it is not relevant to whether each authenticated execution signer vouched
    * for a wrong root, and a pruned local shard parent must not change a slashing verdict. Fraud-proof adjudication invokes this first,
    * then performs its own pinned-base replay to decide whether the authenticated signers actually deviated.
    */
  def verifyExecutionCertificate(checkpoint: ShardCheckpoint): F[Either[String, Unit]]

  /** Validate one after-the-fact committee attestation against a locally known checkpoint. This is the admission gate used before inserting
    * a gossiped signature into [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker]]: committee membership,
    * Ed25519, KES, and VRF are checked with the exact same predicates as [[evaluate]] / [[verifyEmbedded]]. The checkpoint's signature
    * preimage excludes `committeeSignatures`, so checking a singleton signature does not change the bytes the attester signed.
    */
  def verifyCommitteeSignature(
    checkpoint: ShardCheckpoint,
    signature: CommitteeMemberSignature
  ): F[Either[String, Unit]]

  /** WATCHTOWER approval-check (fraud-proof part 1) — replay a certificate-authenticated checkpoint and surface every metagraph whose
    * committee-attested `perMetagraphMptRoots` this node did NOT reproduce. The caller may supply either a replay-valid adopted checkpoint
    * or a typed affirmative-mismatch intake candidate; the fraud emitter verifies the complete certificate before invoking this method.
    *
    * This reuses the exact complete-batch replay used by primary admission (`ShardCheckpointWiring.reExecDerivationsAtPinnedBaseBatch`,
    * PIN-1 encoding, seeded from the checkpoint's exact retained execution base), so the recomputed roots are byte-comparable against the
    * attested ones. The scalar closure is only the compatibility fallback for focused callers.
    *
    * '''Determinism / no-false-positive contract.''' The closure resolves the checkpoint's exact signed `executionBase` and replays the
    * complete signed binary batch; it does not read the receiver's live head. A non-derivable/contiguity-gap batch maps every root to
    * `Hash.empty`, so unavailable local state yields "I can't check" rather than mismatch evidence. The on-chain VERDICT
    * ([[io.constellationnetwork.node.shared.domain.nakamoto.slashing.InvalidStateProofValidator]]) is the deterministic adjudicator; this
    * method is the node-local TRIGGER and may be conservative.
    *
    * Empty `includedSnapshots` yields no mismatch evidence, but primary acceptance rejects the checkpoint as structurally invalid.
    */
  def watchtowerReExec(checkpoint: ShardCheckpoint): F[List[WatchtowerMismatch]]

  /** Record that the exact checkpoint at `shardOrdinal` for `shardId` was carried by a canonical GL0 snapshot that reached Phase 2.
    * Tentative proposal/acceptance must never call this method. Max-monotone by shard ordinal; the future hash-bound Phase-2 reorg
    * coordinator owns replacement/rollback semantics. The producer consumes the atomic ordinal/hash pair only as a fail-closed production
    * policy gate.
    */
  def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): F[Unit]

  /** Highest Phase-2-anchored shard ordinal for `shardId` on this node (None before the first anchor). */
  def lastAdoptedOrd(shardId: ShardId): F[Option[ShardOrdinal]]

  /** Anchor-compatibility (task #42): canonical hash of the most recently Phase-2-anchored checkpoint for `shardId` — the fork-choice
    * anchor the daemon feeds into [[io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardChainStore.noteAnchor]].
    */
  def lastAdoptedAnchor(shardId: ShardId): F[Option[Hash]]

  /** Atomic checkpoint reference for the latest exact shard checkpoint whose containing GL0 snapshot reached Phase 2. Producer policy must
    * use this pair rather than separately sampled ordinal/hash reads.
    */
  def lastAdoptedCheckpoint(shardId: ShardId): F[Option[(ShardOrdinal, Hash)]]
}

object ShardCheckpointGl0AcceptanceManager {

  /** The only signing-capability implementation. Both the class and constructor are private to this companion; its sole allocation site is
    * the replay-backed concrete manager built by [[make]].
    */
  private final case class VerifiedImpl(checkpoint: ShardCheckpoint, signingPreimageHash: Hash) extends VerifiedShardCheckpoint

  /** Construct a manager. All shard-scope dependencies are callbacks so the trait stays free of the concrete per-shard registries (those
    * live in the GSAM wiring slice).
    *
    * @param executionQuorum
    *   minimum number of distinct execution-committee signers carried by an adoptable checkpoint. This is checked directly against the
    *   checkpoint bytes on both producer and follower paths; node-local attestation-tracker depth or state cannot replace it.
    * @param etaRotationSnapshots
    *   positive execution-committee period length `R`. The manager derives `checkpoint.epoch` from the signed `gl0AnchorOrdinal` with the
    *   same pure function used by the producer and rejects a mismatch before committee lookup or replay. This only rejects inconsistent
    *   pairs: a producer can still choose an older ordinal and its matching favorable epoch because this manager receives neither the exact
    *   proposal-parent Phase-2 hash nor a freshness constraint. Production's local parameter source also remains consensus-unsafe until
    *   network/genesis/era bound.
    * @param committeeMembership
    *   `(shardId, epoch) => F[Set[PeerId]]`. The committee draw for this `(shard, epoch)`. Used in the pre-check to confirm each signer is
    *   actually a committee member at the claimed epoch. Empty set ⇒ any signer fails the membership pre-check ⇒ reject.
    * @param operatorKeyRegistry
    *   atomic preregistered KES+VRF identities. Every signer is resolved once and the same record supplies both the KES master key and VRF
    *   verification key; sender-carried keys and KES steps are evidence, never authority. This current-view parameter is restricted to the
    *   committed period-zero genesis pair. Runtime records fail closed until checkpoints bind an exact Phase-2 GL0 anchor hash and this
    *   boundary can resolve them through [[HistoricalOperatorConsensusKeyRegistry]]; a receiver-current runtime view is never sufficient.
    * @param shardEtaFor
    *   `(shardId, epoch) => F[Option[Array[Byte]]]` — resolves the 32-byte per-shard possession-proof eta
    *   (`ShardSlotLeader.computeShardEta`) for the GIVEN eta-period, keyed on the WIRE-CARRIED `checkpoint.epoch`. The SAME resolver the
    *   producer's emitter (`shardEtaFor` at the `ShardCheckpointAttestationEmitter.make` call site) uses, so producer + verifier derive
    *   byte-identical eta bytes across an eta boundary. `None` fails closed; unverifiable signatures never count. MPT-committed eta ⇒
    *   byte-identical cluster-wide.
    * @param producerDutyValidator
    *   validates the retained head signature against the shuffled-staircase owner derived from the exact parent checkpoint, committee, eta,
    *   ordinal, and signed slot. It runs before replay capability creation, storage, countersigning, or embedded adoption. Production
    *   currently resolves the parent from a receiver-local shard store; missing data rejects, but that local dependency leaves
    *   `SHARD-C-009` RED until proposal-parent-bound portable evidence replaces it.
    * @param reExecuteDerivation
    *   `(metagraphAddress, includedChain, gl0AnchorOrdinal) => F[Hash]`. Called for every checkpoint. Production wiring passes the closure
    *   that re-runs `ShardCheckpointWiring.reExecDerivationAtPinnedBase` (the same derivation the producer uses, seeded from this node's
    *   adopted `S(N)`) and returns its per-MG root — the PIN-1 COMPONENT-ADDRESSABLE `GlobalStateConverter.currencySnapshotMgRoot` (the
    *   MG-sub-trie rootHash) — which is then compared byte-for-byte against the committee-signed `perMetagraphMptRoots(mg)` (the SAME
    *   root). GSAM independently reruns the transition and never adopts committee-provided economic state.
    */
  def make[F[_]: Async: Hasher: SecurityProvider: Metrics](
    executionQuorum: Int,
    etaRotationSnapshots: Long,
    committeeMembership: (ShardId, EtaPeriod) => F[Set[PeerId]],
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    shardAssignment: ShardAssignment[F],
    shardEtaFor: (ShardId, EtaPeriod) => F[Option[Array[Byte]]],
    producerDutyValidator: ShardCheckpointProducerDutyValidator[F],
    // The 4th arg is the checkpoint's exact `executionBase`, so mandatory replay seeds S(N) at the same pinned
    // base the producer executed over.
    reExecuteDerivation: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, GlobalSnapshotStateRef) => F[Hash],
    reExecuteDerivations: Option[
      (
        SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        SnapshotOrdinal,
        GlobalSnapshotStateRef
      ) => F[SortedMap[Address, Hash]]
    ] = None
  ): F[ShardCheckpointGl0AcceptanceManager[F]] = {

    require(etaRotationSnapshots > 0L, s"etaRotationSnapshots must be positive, got $etaRotationSnapshots")

    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointGl0AcceptanceManager")

    cats.effect.Ref.of[F, Map[ShardId, (Long, Hash)]](Map.empty).map { lastAdoptedR =>
      new ShardCheckpointGl0AcceptanceManager[F] {

        def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): F[Unit] =
          lastAdoptedR.update { m =>
            val cur = m.get(shardId).map(_._1).getOrElse(0L)
            if (shardOrdinal.value > cur) m.updated(shardId, (shardOrdinal.value, checkpointHash)) else m
          }

        def lastAdoptedOrd(shardId: ShardId): F[Option[ShardOrdinal]] =
          lastAdoptedR.get.map(_.get(shardId).map { case (o, _) => ShardOrdinal(o) })

        def lastAdoptedAnchor(shardId: ShardId): F[Option[Hash]] =
          lastAdoptedR.get.map(_.get(shardId).map(_._2))

        def lastAdoptedCheckpoint(shardId: ShardId): F[Option[(ShardOrdinal, Hash)]] =
          lastAdoptedR.get.map(_.get(shardId).map { case (o, hash) => (ShardOrdinal(o), hash) })

        def verifyCommitteeSignature(
          checkpoint: ShardCheckpoint,
          signature: CommitteeMemberSignature
        ): F[Either[String, Unit]] =
          preCheck(checkpoint, Some(List(signature)), validateProducerDuty = false)

        private def evaluateIntake(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] =
          verifyForIntake(checkpoint).flatTap {
            case ShardCheckpointAcceptResult.Accepted =>
              ShardMetrics.incCheckpointAccepted[F](checkpoint.shardId, ShardMetrics.Path.ExecutionQuorum)
            case _ => Async[F].unit
          }

        def evaluate(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] =
          evaluateIntake(checkpoint)

        def evaluateForSigning(
          checkpoint: ShardCheckpoint
        ): F[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] =
          evaluateIntake(checkpoint).flatMap {
            case ShardCheckpointAcceptResult.Accepted | ShardCheckpointAcceptResult.PendingMoreAttestations =>
              Hasher[F].hash(checkpoint.signingPreimage).map(hash => Right(VerifiedImpl(checkpoint, hash)))
            case ShardCheckpointAcceptResult.Rejected(reason) =>
              Async[F].pure(Left(VerifiedShardCheckpointFailure.Rejected(reason)))
            case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, committeeSigners) =>
              Async[F].pure(Left(VerifiedShardCheckpointFailure.ReExecutionMismatch(reason, committeeSigners)))
          }

        def verifyEmbedded(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] =
          verifyForAdoption(checkpoint, "verifyEmbedded")

        def verifyExecutionCertificate(checkpoint: ShardCheckpoint): F[Either[String, Unit]] =
          preCheck(checkpoint, signaturesOverride = None, validateProducerDuty = false).map {
            _.flatMap { _ =>
              val distinctSigners = distinctSignerCount(checkpoint)
              Either.cond(
                distinctSigners >= executionQuorum,
                (),
                s"execution quorum missing: distinctSigners=$distinctSigners required=$executionQuorum"
              )
            }
          }

        private def verifyForIntake(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] =
          preCheck(checkpoint, signaturesOverride = None, validateProducerDuty = true).flatMap {
            case Left(reason) => rejectPreCheck(checkpoint, "evaluate", reason)
            case Right(())    =>
              // Intake must replay before it can admit the candidate for storage or emit a state-validity signature. A matching result may
              // remain pending while other independently replaying committee members add signatures.
              reExecPath(checkpoint).map {
                case ShardCheckpointAcceptResult.Accepted if distinctSignerCount(checkpoint) < executionQuorum =>
                  ShardCheckpointAcceptResult.PendingMoreAttestations: ShardCheckpointAcceptResult
                case result => result
              }
          }

        private def verifyForAdoption(
          checkpoint: ShardCheckpoint,
          path: String
        ): F[ShardCheckpointAcceptResult] =
          preCheck(checkpoint, signaturesOverride = None, validateProducerDuty = true).map {
            _.flatMap { _ =>
              val distinctSigners = distinctSignerCount(checkpoint)
              Either.cond(
                distinctSigners >= executionQuorum,
                (),
                s"execution quorum missing: distinctSigners=$distinctSigners required=$executionQuorum"
              )
            }
          }.flatMap {
            case Left(reason) => rejectPreCheck(checkpoint, path, reason)
            case Right(())    =>
              // Every carried signature and the execution quorum were checked above. The certificate is mandatory but not sufficient:
              // replay/root validation still decides whether the economic transition is acceptable.
              reExecPath(checkpoint)
          }

        private def distinctSignerCount(checkpoint: ShardCheckpoint): Int =
          checkpoint.committeeSignatures.toList.iterator.map(_.peerId).toSet.size

        private def rejectPreCheck(
          checkpoint: ShardCheckpoint,
          path: String,
          reason: String
        ): F[ShardCheckpointAcceptResult] =
          logger
            .warn(s"$path reject pre-check: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} reason=$reason") >>
            ShardMetrics
              .incCheckpointRejected[F](checkpoint.shardId, ShardMetrics.RejectReason.fromDiagnostic(reason))
              .as(ShardCheckpointAcceptResult.Rejected(reason): ShardCheckpointAcceptResult)

        private def reExecuteAll(checkpoint: ShardCheckpoint): F[SortedMap[Address, Hash]] = {
          val included = checkpoint.derivedStateDelta.includedSnapshots
          reExecuteDerivations
            .fold(
              included.toList.traverse {
                case (mg, snaps) =>
                  reExecuteDerivation(mg, snaps, checkpoint.gl0AnchorOrdinal, checkpoint.executionBase).map(mg -> _)
              }
                .map(SortedMap.from(_))
            )(_(included, checkpoint.gl0AnchorOrdinal, checkpoint.executionBase))
            .map { derived =>
              if (derived.keySet === included.keySet) derived
              else SortedMap.from(included.keysIterator.map(_ -> Hash.empty))
            }
        }

        def watchtowerReExec(checkpoint: ShardCheckpoint): F[List[WatchtowerMismatch]] = {
          val included = checkpoint.derivedStateDelta.includedSnapshots
          val claimedRoots = checkpoint.derivedStateDelta.perMetagraphMptRoots
          // Re-derive the complete checkpoint batch through the same PIN-1 path primary acceptance uses at the carried execution-base
          // ordinal. This runs even when quorum was met so a certificate cannot suppress detection of a signed wrong root.
          reExecuteAll(checkpoint).map { reDerivedByMg =>
            included.toList.map {
              case (mg, _) =>
                val reDerived = reDerivedByMg.getOrElse(mg, Hash.empty)
                claimedRoots.get(mg) match {
                  // `Hash.empty` from the closure = "this node can't derive this MG yet" (contiguity gap / lagging S(N) / OMIT path), NOT
                  // "the committee is wrong" — filter it out so the watchtower never disputes on incomplete local state.
                  case _ if reDerived === Hash.empty            => None
                  case Some(attested) if attested === reDerived => None
                  case Some(attested)                           => Some(WatchtowerMismatch(mg, attested, reDerived))
                  // Included binaries but no attested root for this MG — structurally invalid; surface with Hash.empty as the attested
                  // sentinel so the daemon can raise a dispute (the verdict's `None`-attested branch upholds it).
                  case None => Some(WatchtowerMismatch(mg, Hash.empty, reDerived))
                }
            }.flatten
          }.flatTap { mismatches =>
            Async[F].whenA(mismatches.nonEmpty) {
              logger.warn(
                s"🛡️ WATCHTOWER re-exec MISMATCH: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} " +
                  s"mismatchedMgs=${mismatches.map(_.metagraphAddress.value.value.take(10)).mkString(",")} " +
                  s"(quorum-signed root not reproduced — raising fraud proof)"
              )
            }
          }
        }

        /** Pre-check pipeline. Epoch, certificate-shape, and producer-duty checks run before any signer signature verification:
          *   1. derive the expected execution epoch from the signed `gl0AnchorOrdinal` and reject a wire mismatch before committee lookup
          *   1. reject duplicate committee peer IDs
          *   1. resolve the deterministic committee and reject a signature list larger than that committee
          *   1. for intake and embedded validation, resolve the exact parent and require the retained head signer to own the corresponding
          *      shuffled-staircase duty; unavailable or inconsistent parent context fails closed
          *
          * Then, for each `CommitteeMemberSignature`, run four predicates in order (cheapest first):
          *   1. signer's peerId is in `committeeMembership(checkpoint.shardId, checkpoint.epoch)`
          *   1. Ed25519 sig verifies under the signer's long-term VK
          *   1. KES product sig verifies under the registered master VK at the tree-internal step derived from `checkpoint.epoch -
          *      registeredOffset`; the wire step must equal that value and cannot select it
          *   1. VRF proof verifies under the signer's registered VRF VK over the canonical `(shardEta(shardId, epoch), checkpoint.slot)`
          *      message — REAL `EcVrf25519` verify; missing registry entry or eta rejects
          *
          * Returns `Right(())` if every signer passes every predicate, `Left(reason)` on the first failure. Reason is a short diagnostic
          * string (signer index + which predicate failed) — diagnostic only, never parsed for control flow (per
          * `[[feedback-no-string-matching]]`).
          */
        private def preCheck(
          checkpoint: ShardCheckpoint,
          signaturesOverride: Option[List[CommitteeMemberSignature]],
          validateProducerDuty: Boolean
        ): F[Either[String, Unit]] = {
          val signatures = signaturesOverride.getOrElse(checkpoint.committeeSignatures.toList)
          val distinctSignerCount = signatures.iterator.map(_.peerId).toSet.size
          val expectedEpoch = EtaCalculation.executionShardEpoch(checkpoint.gl0AnchorOrdinal, etaRotationSnapshots)

          if (checkpoint.epoch =!= expectedEpoch)
            Async[F].pure(
              Left(
                s"checkpoint epoch mismatch: wire=${checkpoint.epoch.value} expected=${expectedEpoch.value} " +
                  s"for gl0AnchorOrdinal=${checkpoint.gl0AnchorOrdinal.value.value} etaRotationSnapshots=$etaRotationSnapshots"
              ): Either[String, Unit]
            )
          else if (distinctSignerCount =!= signatures.size)
            Async[F].pure(
              Left(
                s"duplicate committee peer IDs: signatures=${signatures.size} distinctPeerIds=$distinctSignerCount"
              ): Either[String, Unit]
            )
          else
            committeeMembership(checkpoint.shardId, checkpoint.epoch).flatMap { expectedCommittee =>
              if (signatures.size > expectedCommittee.size)
                Async[F].pure(
                  Left(
                    s"signature-list cardinality=${signatures.size} exceeds deterministic committee size=${expectedCommittee.size}; " +
                      "at least one signer not in committee"
                  ): Either[String, Unit]
                )
              else {
                val assignedShardsF = checkpoint.derivedStateDelta.includedSnapshots.keys.toList.traverse { mg =>
                  shardAssignment.shardIdFor(mg).map(mg -> _)
                }

                // Hash the canonical signing pre-image once — every signer signed over these same bytes (per design doc §3.3).
                val preimageHashF: F[Hash] = Hasher[F].hash(checkpoint.signingPreimage)

                val producerDutyF =
                  if (validateProducerDuty) producerDutyValidator.validate(checkpoint, expectedCommittee)
                  else Async[F].pure(Right(()): Either[String, Unit])

                producerDutyF.flatMap {
                  case left @ Left(_) => Async[F].pure(left)
                  case Right(()) =>
                    (preimageHashF, assignedShardsF).tupled.flatMap {
                      case (preimageHash, assignedShards) =>
                        val msgBytes = preimageHash.getBytes
                        val includedKeys = checkpoint.derivedStateDelta.includedSnapshots.keySet
                        val rootKeys = checkpoint.derivedStateDelta.perMetagraphMptRoots.keySet
                        val wrongShard = assignedShards.collect { case (mg, assigned) if assigned =!= checkpoint.shardId => mg -> assigned }
                        val structuralFailure =
                          if (includedKeys =!= rootKeys)
                            Some(s"included/root metagraph key mismatch: included=${includedKeys.size} roots=${rootKeys.size}")
                          else if (wrongShard.nonEmpty)
                            Some(
                              s"metagraph assigned to wrong shard: ${wrongShard.map { case (mg, assigned) => s"$mg->$assigned" }.mkString(",")} " +
                                s"checkpointShard=${checkpoint.shardId}"
                            )
                          else None
                        // foldM short-circuits on Left — the first failing signer wins. `.zipWithIndex` exposes a stable signer ordinal for
                        // diagnostic messages without revealing peer-identity details to the log line itself.
                        signatures.zipWithIndex
                          .foldM[F, Either[String, Unit]](
                            structuralFailure.toLeft(())
                          ) {
                            case (Right(()), (sig, idx)) =>
                              for {
                                // (1) committee membership — cheap set lookup.
                                membershipOk <- Async[F].pure(expectedCommittee.contains(sig.peerId))
                                result <-
                                  if (!membershipOk)
                                    Async[F].pure(
                                      Left(
                                        s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... not in committee for epoch=${checkpoint.epoch.value}"
                                      ): Either[String, Unit]
                                    )
                                  else
                                    for {
                                      // (2) Ed25519 over `preimageHash` bytes. Mirrors `MetagraphCommitteeGate.recordReceivedAttestation`:
                                      // `messageBytes ← Hasher[F].hash(...).map(_.getBytes)`, then `Signing.verifySignature` against the
                                      // peer's long-term VK (recovered from the PeerId).
                                      edOk <- verifyEd25519(msgBytes, sig.ed25519Sig.toBytes, sig.peerId)
                                      step2 <-
                                        if (!edOk)
                                          Async[F].pure(
                                            Left(
                                              s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... Ed25519 sig verify failed"
                                            ): Either[
                                              String,
                                              Unit
                                            ]
                                          )
                                        else
                                          operatorKeyRegistry.get(sig.peerId).flatMap {
                                            case None =>
                                              Async[F].pure(
                                                Left(
                                                  s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... has no preregistered KES+VRF identity"
                                                ): Either[String, Unit]
                                              )
                                            case Some(keys) if keys.registration.nonEmpty =>
                                              Async[F].pure(
                                                Left(
                                                  s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... runtime KES+VRF identity requires exact-parent historical resolution"
                                                ): Either[String, Unit]
                                              )
                                            case Some(keys) if !validOperatorPair(sig.peerId, keys) =>
                                              Async[F].pure(
                                                Left(
                                                  s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... has a malformed KES+VRF registration"
                                                ): Either[String, Unit]
                                              )
                                            case Some(keys) if keys.effectiveFromPeriod.value > checkpoint.epoch.value =>
                                              Async[F].pure(
                                                Left(
                                                  s"signer[$idx] peerId=${sig.peerId.value.value
                                                      .take(16)}... KES+VRF identity is not active at epoch=${checkpoint.epoch.value}"
                                                ): Either[String, Unit]
                                              )
                                            case Some(keys) =>
                                              // (3) KES product sig. This exact active paired registration chooses the master key and
                                              // offset. The checkpoint epoch chooses the tree-relative step; the carried step is evidence
                                              // that must match that derivation and is never authority.
                                              verifyKes(
                                                msgBytes,
                                                sig.kesProductSig.toBytes,
                                                keys,
                                                checkpoint.epoch,
                                                sig.kesTreeStep
                                              ).flatMap { kesOk =>
                                                if (!kesOk)
                                                  Async[F].pure(
                                                    Left(
                                                      s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... KES sig verify failed"
                                                    ): Either[
                                                      String,
                                                      Unit
                                                    ]
                                                  )
                                                else
                                                  // (4) VRF proves possession of the VRF secret paired with the SAME registration used
                                                  // for KES. Committee membership remains the separate public sortition predicate.
                                                  verifyVrf(
                                                    checkpoint.shardId,
                                                    checkpoint.epoch,
                                                    checkpoint.slot,
                                                    keys,
                                                    sig.vrfProof.toBytes
                                                  ).map { vrfOk =>
                                                    if (!vrfOk)
                                                      Left(
                                                        s"signer[$idx] peerId=${sig.peerId.value.value
                                                            .take(16)}... committee-VRF proof verify failed for shardId=${checkpoint.shardId} epoch=${checkpoint.epoch.value} slot=${checkpoint.slot.value.value}"
                                                      ): Either[String, Unit]
                                                    else
                                                      Right(()): Either[String, Unit]
                                                  }
                                              }
                                          }
                                    } yield step2
                              } yield result
                            case (left @ Left(_), _) => Async[F].pure(left) // already failed earlier signer; propagate
                          }
                    }
                }
              }
            }
        }

        /** Re-exec path. For each MG in `checkpoint.includedSnapshots`, run `reExecuteDerivation` against the full binary chain and compare
          * the recomputed hash against the committee-signed `perMetagraphMptRoots(mg)`. Empty checkpoints are rejected because they carry
          * no economic transition to replay and must not advance the shard checkpoint lineage.
          *
          * All-match ⇒ `Accepted`. Any AFFIRMATIVE mismatch (a real re-derived root ≠ the claimed root) ⇒ `RejectedReExecutionMismatch`
          * with the entire signer list for diagnostic and fraud-evidence construction. This local result rejects the checkpoint but is not
          * portable slash evidence and cannot reach the consensus slash sink; only an artifact-carried fraud proof independently
          * revalidated by every GL0 node may slash. The first mismatch carries the reason; we don't short-circuit because complete
          * diagnostics are more valuable than the trivial CPU saved on this exceptional path.
          *
          * '''CANNOT-RE-DERIVE fail-closed (Track-1 execution-base-pin, FINDING-B1).''' `Hash.empty` from the closure is the wiring's
          * fail-closed "cannot re-derive" sentinel (`reExecDerivationAtPinnedBase` OMITted: the exact `executionBase` is unresolvable below
          * this node's retention / not yet reached, the derivation deferred/crashed, or no closure is wired) — "THIS NODE can't check", NOT
          * "the committee deviated". Exactly the reading [[watchtowerReExec]] applies when it filters `Hash.empty`. Such an MG must NOT
          * feed `RejectedReExecutionMismatch`: that result means an affirmative mismatch and may trigger construction of separately
          * portable fraud evidence, while a sentinel from an unwired or unavailable local base is not evidence of committee deviation.
          * Instead the checkpoint is REJECTED plain (fail-closed — never admitted unverified, never falsely slashed); it re-offers once the
          * node can serve the pinned base or quorum returns.
          */
        private def reExecPath(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] = {
          val included = checkpoint.derivedStateDelta.includedSnapshots
          val claimedRoots = checkpoint.derivedStateDelta.perMetagraphMptRoots
          val signers = checkpoint.committeeSignatures.toList.map(_.peerId)

          if (included.isEmpty)
            (ShardCheckpointAcceptResult.Rejected("empty checkpoint is not a valid v1 production window"): ShardCheckpointAcceptResult)
              .pure[F]
          else
            // Per-MG verdict: Right(mg) = root reproduced; Left((mg, reason, affirmativeMismatch)) = not reproduced. The boolean distinguishes
            // a real derived-root mismatch from the can't-check sentinel; it does not authorize a slash.
            reExecuteAll(checkpoint).map { reDerivedByMg =>
              included.toList.map {
                case (mg, _) =>
                  // Full per-MG chain + the checkpoint's wire-carried `gl0AnchorOrdinal` + exact pinned `executionBase`. Mirrors
                  // `ShardCheckpointProducer.assembleDelta` — the producer derives the per-MG root off the whole included chain at the same
                  // anchor over the same pinned base, and the gl0 verifier re-runs the SAME derivation off the SAME inputs. Byte-identical
                  // inputs ⇒ byte-identical roots (the S3 no-false-slashing contract).
                  val actual = reDerivedByMg.getOrElse(mg, Hash.empty)
                  claimedRoots.get(mg) match {
                    // CANNOT-RE-DERIVE sentinel — fail closed, no slash (see scaladoc). Checked FIRST so a sentinel never counts as an
                    // affirmative mismatch against any claimed value (or a missing claim).
                    case _ if actual === Hash.empty =>
                      Left(
                        (
                          mg,
                          s"cannot re-derive MG $mg at pinned executionBase=${checkpoint.executionBase.ordinal.value.value}/" +
                            s"${checkpoint.executionBase.hash.value.take(12)} " +
                            s"(re-exec unavailable: pinned base unresolvable/OMIT) — fail-closed drop, NO slash",
                          false
                        )
                      )
                    case Some(claimed) if claimed === actual => Right(mg)
                    case Some(claimed) =>
                      Left(
                        (mg, s"re-exec mismatch for MG $mg: claimed=${claimed.value.take(16)}... actual=${actual.value.take(16)}...", true)
                      )
                    case None =>
                      Left((mg, s"re-exec mismatch for MG $mg: claimed=<missing> actual=${actual.value.take(16)}...", true))
                  }
              }
            }.flatMap { results =>
              val failures = results.collect { case Left(x) => x }
              val mismatches = failures.filter { case (_, _, affirmativeMismatch) => affirmativeMismatch }
              val uncheckable = failures.filterNot { case (_, _, affirmativeMismatch) => affirmativeMismatch }
              if (failures.isEmpty) {
                logger
                  .info(
                    s"checkpoint re-exec OK: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} " +
                      s"mgsReExecuted=${included.size}"
                  ) >>
                  (ShardCheckpointAcceptResult.Accepted: ShardCheckpointAcceptResult).pure[F]
              } else if (mismatches.nonEmpty) {
                // Affirmative deviation evidence on ≥1 MG wins over any uncheckable sibling: the committee signed a root no honest
                // pinned-base re-execution of its own signed binaries produces.
                val firstReason = mismatches.head._2
                val mismatchedMgs = mismatches.map(_._1).map(_.value.value).mkString(", ")
                logger
                  .warn(
                    s"reject checkpoint re-exec mismatch: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} " +
                      s"mismatchedMgs=[$mismatchedMgs] firstReason=$firstReason committeeSigners=${signers.map(_.value.value.take(16) + "...").mkString(",")}"
                  ) >>
                  ShardMetrics
                    .incCheckpointRejected[F](checkpoint.shardId, ShardMetrics.RejectReason.ReExecMismatch)
                    .as(
                      ShardCheckpointAcceptResult.RejectedReExecutionMismatch(firstReason, signers): ShardCheckpointAcceptResult
                    )
              } else {
                // ONLY can't-check sentinels — fail closed WITHOUT slash targets: plain Rejected (dropped, re-offered later).
                val firstReason = uncheckable.head._2
                val uncheckableMgs = uncheckable.map(_._1).map(_.value.value).mkString(", ")
                logger
                  .warn(
                    s"reject checkpoint re-exec CANNOT-RE-DERIVE (fail-closed, no slash): shardId=${checkpoint.shardId} " +
                      s"shardOrd=${checkpoint.shardOrdinal.value} uncheckableMgs=[$uncheckableMgs] firstReason=$firstReason"
                  ) >>
                  ShardMetrics
                    .incCheckpointRejected[F](checkpoint.shardId, ShardMetrics.RejectReason.ReExecMismatch)
                    .as(ShardCheckpointAcceptResult.Rejected(firstReason): ShardCheckpointAcceptResult)
              }
            }
        }

        /** Verify Ed25519 over `msgBytes` using the signer's long-term VK recovered from the `PeerId`. Mirrors
          * [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate]] receiver path lines 485-487: `senderPubKey ←
          * att.senderPeerId.value.toPublicKey[F]; Signing.verifySignature[F](msgBytes, sig)(senderPubKey)`.
          *
          * Failures inside the `toPublicKey` / `verifySignature` paths are caught and turned into `false` — we never want a corrupt
          * signature wire field to crash the gl0 accept loop. Diagnostic is emitted by the caller's pre-check loop, not here.
          */
        private def verifyEd25519(
          msgBytes: Array[Byte],
          sigBytes: Array[Byte],
          peerId: PeerId
        ): F[Boolean] =
          if (sigBytes.isEmpty) Async[F].pure(false)
          else
            (for {
              pubKey <- peerId.value.toPublicKey[F]
              ok <- Signing.verifySignature[F](msgBytes, sigBytes)(pubKey)
            } yield ok).handleError(_ => false)

        /** Defense-in-depth validation for registry implementations that are not constructed by [[OperatorConsensusKeyRegistry.make]]. This
          * current-view boundary accepts only the complete committed genesis pair. Runtime records are rejected above until checkpoint
          * evidence carries an exact Phase-2 GL0 anchor hash and this manager can use [[HistoricalOperatorConsensusKeyRegistry]]; accepting
          * a best-tip/current runtime record here would make sibling validity local.
          */
        private def validOperatorPair(peerId: PeerId, keys: OperatorConsensusKeys): Boolean =
          keys.registration.isEmpty &&
            keys.operatorPeerId === peerId &&
            keys.kes.vk.value.length == 32 &&
            keys.kes.vk.step == 0 &&
            keys.kes.offset == 0L &&
            keys.effectiveFromPeriod === EtaPeriod.Zero &&
            keys.vrfPublicKey.toBytes.length == 32

        /** Verify the KES product sig under the signer's registered master VK at the only step valid for the checkpoint epoch. The expected
          * tree-relative step is `artifactPeriod - registeredOffset`; the wire field must equal it. This prevents a signer from selecting a
          * stale/future KES step even when the product signature itself verifies.
          *
          * Missing registry entries, empty signatures, decode failures, invalid steps, and failed verification all reject. There is no
          * Ed25519-only bootstrap exception on an economic checkpoint.
          */
        private def verifyKes(
          msgBytes: Array[Byte],
          kesSigBytes: Array[Byte],
          keys: OperatorConsensusKeys,
          artifactPeriod: EtaPeriod,
          kesTreeStep: Int
        ): F[Boolean] =
          if (kesSigBytes.isEmpty) Async[F].pure(false)
          else {
            val entry = keys.kes
            val expectedStep = BigInt(artifactPeriod.value) - BigInt(entry.offset)
            Async[F].pure {
              if (
                artifactPeriod.value < 0L || entry.offset < 0L || expectedStep < 0 || expectedStep > Int.MaxValue ||
                kesTreeStep != expectedStep.intValue
              ) false
              else
                OperationalKeyMaker.decodeSignature(kesSigBytes) match {
                  case Left(_) => false
                  case Right(kSig) =>
                    val vkAtStep = entry.vk.copy(step = kesTreeStep)
                    OperationalKeyMaker.verify(kSig, msgBytes, vkAtStep)
                }
            }
          }

        /** REAL committee-VRF membership verify — confirms the signer's wire-carried `vrfProof` is a valid `EcVrf25519` proof under its
          * REGISTERED VRF VK over the canonical `(shardEta(shardId, epoch), slot)` message (the SAME message the producer's
          * `ShardCheckpointAttestationEmitter` / `ShardSlotLeader.membershipProof` signs via `EligibilityChecker.vrfProofForSlot`).
          *
          * '''The message (byte-identical with the producer).''' `EligibilityChecker.vrfProofForSlot` proves over `shardEta (32) || slot
          * (8, big-endian)`. We reconstruct that exact preimage: `shardEta = shardEtaFor(shardId, epoch)` (the SAME resolver the emitter
          * uses — `ShardSlotLeader.computeShardEta` over the epoch's gl0 eta) and `slotBytes = ByteBuffer.putLong(slot.value)`. The verify
          * is `EcVrf25519.vrfVerify(vk, msg, proof)` — pure, deterministic, no LDD/threshold re-assertion (committee SET membership is the
          * separate set-lookup pre-check (1); this binds the signature to its drawn member).
          *
          * '''Determinism (the #261 split invariant).''' Every input is cluster-uniform: the active atomic operator-key record,
          * `shardEtaFor` (MPT-committed eta + `Hasher`-based `computeShardEta`), and `EcVrf25519.vrfVerify` (pure). So every honest GL0
          * node reaches the same verdict for the same signer.
          *
          * Missing VK/eta, empty or malformed proof bytes, and verification exceptions all fail closed without crashing the GL0 accept
          * loop.
          */
        private def verifyVrf(
          shardId: ShardId,
          epoch: EtaPeriod,
          slot: Slot,
          keys: OperatorConsensusKeys,
          proofBytes: Array[Byte]
        ): F[Boolean] =
          if (proofBytes.isEmpty) Async[F].pure(false)
          else
            shardEtaFor(shardId, epoch).map {
              case Some(shardEta) if shardEta.length == 32 =>
                // Real cryptographic verify. Reconstruct the producer's exact `(shardEta || slot)` message
                // (EligibilityChecker.vrfProofForSlot byte shape) and verify the proof under the registered VK.
                val slotBytes = java.nio.ByteBuffer.allocate(8).putLong(slot.value.value).array()
                val msg = shardEta ++ slotBytes
                try EcVrf25519.default.vrfVerify(keys.vrfPublicKey.toBytes, msg, proofBytes)
                catch { case _: Throwable => false }
              case _ => false
            }
      }
    }
  }

}
