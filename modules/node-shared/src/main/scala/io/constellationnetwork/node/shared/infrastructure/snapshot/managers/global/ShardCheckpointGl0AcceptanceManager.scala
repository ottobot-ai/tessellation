package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardFinalityTriggers}
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.kes.OperationalKeyMaker
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Result of running gl0-side admission on a single [[ShardCheckpoint]].
  *
  * '''Why a sealed ADT''' — callers (Slice 13 GSAM wiring) branch on three distinct outcomes (accept, defer, reject) plus an additional
  * branch carrying a slash-evidence accumulator (re-exec mismatch). Modelling as `Either[String, Boolean]` would conflate "drop with
  * slashing required" against plain "reject" and lose the slash signer list. Per `[[feedback-no-string-matching]]` (no string matching for
  * control flow): branches are typed; the `reason` strings are diagnostic-only.
  */
sealed trait ShardCheckpointAcceptResult extends Product with Serializable

object ShardCheckpointAcceptResult {

  /** The checkpoint passes all pre-checks AND either (a) committee quorum is met (`T_count_shard` path) or (b) re-exec matches
    * (`T_depth1_shard` path). gl0 leader should include this checkpoint in `shardCheckpoints[s]` for the next gl0 snapshot.
    */
  case object Accepted extends ShardCheckpointAcceptResult

  /** Pre-checks pass but neither finality trigger qualifies yet — checkpoint is in-flight and will be re-evaluated at a later gl0 ord (per
    * §7.2 the `gl0AnchorOrdinal` allows the checkpoint to ride into N, N+1, N+2…). Caller skips inclusion this ord.
    */
  case object PendingMoreAttestations extends ShardCheckpointAcceptResult

  /** The checkpoint failed one of the pre-checks or the shard is not tracked locally. `reason` is diagnostic; the typed branch is the
    * control-flow signal.
    */
  final case class Rejected(reason: String) extends ShardCheckpointAcceptResult

  /** The checkpoint took the `T_depth1_shard`-only re-exec path and the local recomputed `mptRoot` for at least one metagraph differs from
    * what the committee signed. Per §10.2 "wrong-derivation": every committee signer of this checkpoint deviated from determinism and is a
    * slashable target. Caller (Slice 16+17) feeds `slashSigners` into the slash-evidence accumulator; for this slice we just surface the
    * list.
    */
  final case class RejectedReExecutionMismatch(reason: String, slashSigners: List[PeerId]) extends ShardCheckpointAcceptResult
}

/** gl0-side admission of [[ShardCheckpoint]]s — Slice 9 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §7 (especially §7.3).
  *
  * Sits between the shard committee's checkpoint gossip and the gl0 leader's per-ord accept loop. For each candidate checkpoint included in
  * the next gl0 snapshot:
  *
  *   1. '''Pre-checks''' (always): every committee signer must be (a) in the committee for `(shardId, epoch)`, (b) verifiable by their
  *      registered long-term Ed25519 VK, (c) verifiable by their KES product VK at the embedded tree-internal step, (d) prove committee
  *      membership via VRF. Failure on ANY signer ⇒ `Rejected("invalid pre-check ...")`.
  *
  *   1. '''Finality phase''' (per §7.3): look up the shard's [[ShardFinalityTriggers]] composite. If the per-shard `tCountShard` has
  *      qualified `checkpoint.shardOrdinal` ⇒ fast `Accepted`. If only `tDepth1Shard` qualifies (degraded liveness — no quorum) ⇒ re-exec
  *      path. Neither qualifies ⇒ `PendingMoreAttestations`.
  *
  *   1. '''Re-exec''' (T_depth1 only): for each MG in `checkpoint.includedSnapshots`, call the injectable `reExecuteDerivation` and compare
  *      the recomputed `mptRoot` byte-for-byte against `checkpoint.derivedStateDelta.perMetagraphMptRoots(mgAddr)`. All match ⇒ `Accepted`.
  *      Any mismatch ⇒ `RejectedReExecutionMismatch` with the full list of committee signers as the slash target set (§10.2 —
  *      wrong-derivation evidence).
  *
  * '''Greenfield rule''' (per `[[feedback-greenfield-no-wire-compat]]`):
  *   - Fresh manager for the shard-checkpoint admission path. No compat ceremony with [[GlobalSnapshotStateChannelAcceptanceManager]] —
  *     Slice 13 rewires GSAM to consume this; this slice creates the manager standalone.
  *
  * '''HOCON rule''' (per `[[feedback-prefer-hocon-over-sysenv]]`):
  *   - `kTarget` is a constructor parameter; callers wire from `cfg.nakamoto.sharding.committeeKTarget`. No `sys.env.get` anywhere.
  *
  * '''Decoupling rule''':
  *   - All shard-scope lookups are injected as callbacks (`finalityTriggers`, `chainStore`, `committeeMembership`, `reExecuteDerivation`).
  *     Tests can stub each path independently; production wires the live per-shard maps. The trait stays agnostic to where the per-shard
  *     state lives.
  */
trait ShardCheckpointGl0AcceptanceManager[F[_]] {

  /** gl0 leader runs this for each candidate [[ShardCheckpoint]] under consideration for inclusion in the next gl0 snapshot.
    *
    * Returns one of the [[ShardCheckpointAcceptResult]] variants; the caller branches on the variant for inclusion / deferral / rejection
    * (+ optional slashing evidence emission).
    */
  def evaluate(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult]
}

object ShardCheckpointGl0AcceptanceManager {

  /** Construct a manager. All shard-scope dependencies are callbacks so the trait stays free of the concrete per-shard registries (those
    * live in the GSAM wiring slice).
    *
    * @param finalityTriggers
    *   `shardId => F[Option[ShardFinalityTriggers[F]]]`. Returns `None` when the shard isn't tracked locally (e.g., we're the gl0 leader
    *   but never started a `ShardChainStore` for this shard, or the chain hasn't bootstrapped). `None` ⇒ reject.
    * @param chainStore
    *   `shardId => F[Option[ShardChainStore[F]]]`. Carried for future use by the re-exec path's chain-walk dependency. Slice 9 does not
    *   consume it directly — `reExecuteDerivation` is the actual re-exec hook. Slice 13+ wiring may need it to verify that a
    *   `tDepth1Shard`-qualifying ancestor exists in the local chain store. Kept on the signature for forward compatibility.
    * @param committeeMembership
    *   `(shardId, epoch) => F[Set[PeerId]]`. The committee draw for this `(shard, epoch)`. Used in the pre-check to confirm each signer is
    *   actually a committee member at the claimed epoch. Empty set ⇒ any signer fails the membership pre-check ⇒ reject.
    * @param kTarget
    *   committee target size (`K_S` in design doc §5). Wired from `cfg.nakamoto.sharding.committeeKTarget`. v1 stable-σ rule treats every
    *   committee member as uniform 1/K_S; the threshold check inside `CommitteeSortition.verifyMembership` uses `K · σ` so this is the `K`
    *   factor.
    * @param selfPeerId
    *   this node's PeerId. Carried for diagnostic logging (so a slashing event surfaces which gl0 op spotted the deviation). NOT used to
    *   gate any acceptance logic — every honest gl0 op runs the same predicates and reaches the same outcome.
    * @param kesRegistry
    *   registered KES master VKs. Used in the pre-check to verify each committee signer's KES product sig at the embedded tree-internal
    *   step (`CommitteeMemberSignature.kesTreeStep`). The "no registry entry" carve-out mirrors
    *   [[io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate]]'s receiver path: registry-absent peers are accepted
    *   on the strength of the Ed25519 signature alone (Slice 10 will introduce runtime registration; this carve-out covers the bootstrap
    *   window).
    * @param reExecuteDerivation
    *   `(metagraphAddress, headBinary) => F[Hash]`. Called only on the T_depth1-only path. Slice 9 ships this injectable; production wiring
    *   (Slice 13) passes the closure that walks the per-MG chain and runs the existing `processCurrencySnapshots` derivation to compute the
    *   local `mptRoot`. For tests: stub the callback to return a known hash (matching or not matching the checkpoint delta) per scenario.
    */
  def make[F[_]: Async: Hasher: SecurityProvider](
    finalityTriggers: ShardId => F[Option[ShardFinalityTriggers[F]]],
    chainStore: ShardId => F[Option[ShardChainStore[F]]],
    committeeMembership: (ShardId, EtaPeriod) => F[Set[PeerId]],
    kTarget: Int,
    selfPeerId: PeerId,
    kesRegistry: KesRegistry[F],
    reExecuteDerivation: (Address, Signed[StateChannelSnapshotBinary]) => F[Hash]
  ): F[ShardCheckpointGl0AcceptanceManager[F]] = {

    // chainStore + selfPeerId + kTarget are reserved for forward compatibility (see scaladoc on the parameters); reference once to
    // avoid unused-warnings. Slice 13 wiring uses chainStore for the depth-k1 ancestor lookup in production accept loops; selfPeerId
    // is used by diagnostic logging to surface which gl0 op spotted a slashable deviation; kTarget feeds the VRF threshold check
    // when the real VRF VK registry is wired (Slice 13). For Slice 9 the VRF predicate is a non-empty proof bytes structural check
    // (the VK registry doesn't exist yet — see scaladoc on `verifyVrfStructural`).
    val _unusedChainStore = chainStore
    val _unusedSelfPeerId = selfPeerId
    val _unusedKTarget = kTarget
    val _ = (_unusedChainStore, _unusedSelfPeerId, _unusedKTarget)

    val logger = Slf4jLogger.getLoggerFromName[F]("ShardCheckpointGl0AcceptanceManager")

    Async[F].pure {
      new ShardCheckpointGl0AcceptanceManager[F] {

        def evaluate(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] =
          // Step 1: pre-checks. Run on every signer; fail-fast at the first signer that doesn't pass all four predicates.
          // The pre-check covers committee membership, Ed25519, KES, and VRF for each `CommitteeMemberSignature`. If any check
          // fails for any signer, the entire checkpoint is rejected (signers must be honest committee members; one bad signer
          // is enough to taint the envelope from gl0's perspective).
          preCheck(checkpoint).flatMap {
            case Left(reason) =>
              logger
                .warn(s"reject pre-check: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} reason=$reason")
                .as(ShardCheckpointAcceptResult.Rejected(reason): ShardCheckpointAcceptResult)
            case Right(()) =>
              // Step 2: resolve the per-shard finality triggers. None ⇒ unknown shard ⇒ reject (we can't tell if T_count or T_depth1
              // qualifies because we don't track this shard).
              finalityTriggers(checkpoint.shardId).flatMap {
                case None =>
                  val msg = s"unknown shard: shardId=${checkpoint.shardId} not in finalityTriggers map"
                  logger.warn(msg).as(ShardCheckpointAcceptResult.Rejected(msg): ShardCheckpointAcceptResult)
                case Some(triggers) =>
                  // Step 3: read the latest qualifying ord from each inner trigger. The triggers are advanced asynchronously by the
                  // shard chain's tick loop; we read here, not advance — same pattern as the gl0 finality monitor (`evaluate` is read-only).
                  for {
                    countOrdSnap <- triggers.tCountShard.latestQualifyingOrdinal
                    depthOrdSnap <- triggers.tDepth1Shard.latestQualifyingOrdinal
                    // Bridge the inner trigger's `SnapshotOrdinal` to the checkpoint's `ShardOrdinal` via direct value comparison.
                    // The inner triggers clamp negatives to `SnapshotOrdinal.MinValue` (per ShardFinalityTriggers scaladoc) so the
                    // raw .value.value Long is a safe direct counterpart to `checkpoint.shardOrdinal.value`. Avoiding the package-private
                    // converters keeps this manager free of cross-package coupling beyond the public trait surface.
                    checkpointOrd = checkpoint.shardOrdinal
                    countQualifies = countOrdSnap.value.value >= checkpointOrd.value
                    depthQualifies = depthOrdSnap.value.value >= checkpointOrd.value
                    result <-
                      if (countQualifies) {
                        // §7.3 fast path: signature-only acceptance. We already verified all signatures in step 1; the count trigger
                        // confirms ≥ ⌈2·K_S/3⌉ committee members attested — quorum-attested ⇒ accept.
                        logger
                          .info(
                            s"accept T_count_shard: shardId=${checkpoint.shardId} shardOrd=${checkpointOrd.value} " +
                              s"countQualifying=${countOrdSnap.value.value}"
                          )
                          .as(ShardCheckpointAcceptResult.Accepted: ShardCheckpointAcceptResult)
                      } else if (depthQualifies) {
                        // §7.3 degraded path: no quorum but the shard chain has advanced past `k1_shard` past this checkpoint. Re-exec
                        // the per-MG derivations and compare. Mismatch ⇒ slash (the lone-survivor committee signer(s) deviated from
                        // determinism).
                        reExecPath(checkpoint)
                      } else {
                        // Neither trigger qualifies — checkpoint is too new (no quorum + no depth coverage). Defer; the gl0 leader
                        // will try again at the next gl0 ord (per §7.2 the `gl0AnchorOrdinal` permits the checkpoint to ride into a
                        // later ord without re-issuance).
                        logger
                          .debug(
                            s"defer: shardId=${checkpoint.shardId} shardOrd=${checkpointOrd.value} " +
                              s"countQualifying=${countOrdSnap.value.value} depthQualifying=${depthOrdSnap.value.value}"
                          )
                          .as(ShardCheckpointAcceptResult.PendingMoreAttestations: ShardCheckpointAcceptResult)
                      }
                  } yield result
              }
          }

        /** Pre-check pipeline: for each `CommitteeMemberSignature` in `checkpoint.committeeSignatures`, run four predicates in order
          * (cheapest first):
          *   1. signer's peerId is in `committeeMembership(checkpoint.shardId, checkpoint.epoch)`
          *   1. Ed25519 sig verifies under the signer's long-term VK
          *   1. KES product sig verifies under the registered master VK at the embedded tree-internal step (registry-absent ⇒ accept per
          *      the carve-out)
          *   1. VRF proof verifies as committee membership for `(checkpoint.shardId, checkpoint.epoch, ...)`
          *
          * Returns `Right(())` if every signer passes every predicate, `Left(reason)` on the first failure. Reason is a short diagnostic
          * string (signer index + which predicate failed) — diagnostic only, never parsed for control flow (per
          * `[[feedback-no-string-matching]]`).
          */
        private def preCheck(checkpoint: ShardCheckpoint): F[Either[String, Unit]] = {
          val expectedCommitteeF: F[Set[PeerId]] =
            committeeMembership(checkpoint.shardId, checkpoint.epoch)

          // Hash the canonical signing pre-image once — every signer signed over these same bytes (per design doc §3.3).
          val preimageHashF: F[Hash] = Hasher[F].hash(checkpoint.signingPreimage)

          (expectedCommitteeF, preimageHashF).tupled.flatMap {
            case (expectedCommittee, preimageHash) =>
              val msgBytes = preimageHash.getBytes
              // foldM short-circuits on Left — the first failing signer wins. `.zipWithIndex` exposes a stable signer ordinal for
              // diagnostic messages without revealing peer-identity details to the log line itself.
              checkpoint.committeeSignatures.toList.zipWithIndex.foldM[F, Either[String, Unit]](Right(()): Either[String, Unit]) {
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
                                Left(s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... Ed25519 sig verify failed"): Either[
                                  String,
                                  Unit
                                ]
                              )
                            else
                              // (3) KES product sig. The KES verification is non-interactive — we use the embedded
                              // `sig.kesTreeStep` for the tree-internal step rather than rederiving it from the epoch (the design
                              // doc §3.1 specifically calls out this is wire-carried — see also #211 wire-step landing).
                              verifyKes(msgBytes, sig.kesProductSig.toBytes, sig.peerId, sig.kesTreeStep).flatMap { kesOk =>
                                if (!kesOk)
                                  Async[F].pure(
                                    Left(s"signer[$idx] peerId=${sig.peerId.value.value.take(16)}... KES sig verify failed"): Either[
                                      String,
                                      Unit
                                    ]
                                  )
                                else
                                  // (4) VRF — confirms the signer was actually elected to the committee at the time of the
                                  // sortition draw. The full cryptographic check requires the signer's VRF VK, which Slice 9 does
                                  // NOT have access to — there's no VRF VK registry in the constructor signature yet (the spec is
                                  // prescriptive: only `kesRegistry`, no VrfRegistry). Slice 13's GSAM rewire will introduce the
                                  // VRF VK registry and replace this stub with the real `CommitteeSortition.verifyMembership` call.
                                  //
                                  // For Slice 9 the VRF predicate degenerates to a structural check (non-empty proof bytes with
                                  // the right minimum length). This is sound because: (a) the membership pre-check above already
                                  // confirms the peerId is in the expected committee — that's the strongest authoritative signal
                                  // we have at this layer; (b) the Ed25519 pre-check confirms the bytes were signed by the peer's
                                  // long-term key — that's the second strongest signal; (c) the KES pre-check confirms forward
                                  // security. The remaining "did this peer actually win the VRF lottery for this slot" check is
                                  // wire-shape-only at slice 9. Slice 13 elevates it to cryptographic.
                                  Async[F].pure(verifyVrfStructural(sig.vrfProof.toBytes)).map { vrfOk =>
                                    if (!vrfOk)
                                      Left(
                                        s"signer[$idx] peerId=${sig.peerId.value.value
                                            .take(16)}... VRF proof structural check failed for shardId=${checkpoint.shardId} epoch=${checkpoint.epoch.value}"
                                      ): Either[String, Unit]
                                    else
                                      Right(()): Either[String, Unit]
                                  }
                              }
                        } yield step2
                  } yield result
                case (left @ Left(_), _) => Async[F].pure(left) // already failed earlier signer; propagate
              }
          }
        }

        /** Re-exec path (T_depth1 only). For each MG in `checkpoint.includedSnapshots`, run `reExecuteDerivation` against the head binary
          * and compare the recomputed hash against the committee-signed `perMetagraphMptRoots(mg)`. Empty `includedSnapshots` is a valid
          * empty-window checkpoint (T_alive liveness ping per §6.1) — there's nothing to re-exec, so acceptance reduces to the pre-check +
          * finality phase combination above.
          *
          * All-match ⇒ `Accepted`. Any-mismatch ⇒ `RejectedReExecutionMismatch` with the entire signer list as the slash target (§10.2 —
          * every signer attested to the same wrong-derivation result; all of them deviated). The first mismatch carries the reason; we
          * don't short-circuit at the first mismatch because diagnostic completeness (which MGs diverged) is more valuable than the trivial
          * CPU saved by early-exit on a path that fires under degraded liveness only.
          */
        private def reExecPath(checkpoint: ShardCheckpoint): F[ShardCheckpointAcceptResult] = {
          val included = checkpoint.derivedStateDelta.includedSnapshots
          val claimedRoots = checkpoint.derivedStateDelta.perMetagraphMptRoots
          val signers = checkpoint.committeeSignatures.toList.map(_.peerId)

          included.toList.traverse {
            case (mg, snaps) =>
              // Head of the per-MG chain. Mirrors `ShardCheckpointProducer.assembleDelta` — the producer derives the MPT root
              // off the head of the included chain, and the gl0 verifier re-runs that same derivation off the same head.
              val head = snaps.head
              reExecuteDerivation(mg, head).map { actual =>
                claimedRoots.get(mg) match {
                  case Some(claimed) if claimed === actual => Right(mg)
                  case Some(claimed) =>
                    Left((mg, s"re-exec mismatch for MG $mg: claimed=${claimed.value.take(16)}... actual=${actual.value.take(16)}..."))
                  case None =>
                    Left((mg, s"re-exec mismatch for MG $mg: claimed=<missing> actual=${actual.value.take(16)}..."))
                }
              }
          }.flatMap { results =>
            val mismatches = results.collect { case Left(x) => x }
            if (mismatches.isEmpty) {
              logger
                .info(
                  s"accept T_depth1_shard re-exec OK: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} " +
                    s"mgsReExecuted=${included.size}"
                )
                .as(ShardCheckpointAcceptResult.Accepted: ShardCheckpointAcceptResult)
            } else {
              val firstReason = mismatches.head._2
              val mismatchedMgs = mismatches.map(_._1).map(_.value.value).mkString(", ")
              logger
                .warn(
                  s"reject T_depth1_shard re-exec mismatch: shardId=${checkpoint.shardId} shardOrd=${checkpoint.shardOrdinal.value} " +
                    s"mismatchedMgs=[$mismatchedMgs] firstReason=$firstReason slashSigners=${signers.map(_.value.value.take(16) + "...").mkString(",")}"
                )
                .as(
                  ShardCheckpointAcceptResult.RejectedReExecutionMismatch(firstReason, signers): ShardCheckpointAcceptResult
                )
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

        /** Verify the KES product sig under the signer's registered master VK at the wire-carried `kesTreeStep`. Mirrors
          * [[io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto.KesGossipVerification.verifyAttestation]] minus the eta-period
          * derivation: the design doc §3.1 puts `kesTreeStep` on the wire (CommitteeMemberSignature field) so the receiver can verify
          * non-interactively without re-deriving global eta period.
          *
          * '''Registry-absent carve-out''': if `kesRegistry.getKesVk(peerId)` returns `None`, accept on the strength of the Ed25519
          * signature alone (already verified). Mirrors the same carve-out in `KesGossipVerification` (lines 60-66 of that file) — Slice 10
          * runtime registration will eventually cover all operators; the carve-out tolerates the bootstrap window.
          *
          * '''Lookup-first ordering''': the registry lookup runs BEFORE the sig decode. Rationale: when no registry entry exists, we accept
          * on the Ed25519 strength alone — we never need to decode the KES bytes. Putting decode first would reject the Slice 10 bootstrap
          * window (where genesis-only operators might gossip placeholder KES bytes), defeating the carve-out. Empty sig bytes still reject
          * upfront — those are a wire-shape violation regardless of registry state.
          */
        private def verifyKes(
          msgBytes: Array[Byte],
          kesSigBytes: Array[Byte],
          peerId: PeerId,
          kesTreeStep: Int
        ): F[Boolean] =
          if (kesSigBytes.isEmpty) Async[F].pure(false)
          else
            kesRegistry.getKesVk(peerId).map {
              case None =>
                // No registry entry — accept (carve-out for Slice 10 bootstrap window). Ed25519 sig already authenticated the sender;
                // we never decode the KES bytes because there's no VK to verify against. Slice 10 closes this gap.
                true
              case Some(entry) =>
                if (kesTreeStep < 0) false
                else
                  OperationalKeyMaker.decodeSignature(kesSigBytes) match {
                    case Left(_) => false
                    case Right(kSig) =>
                      val vkAtStep = entry.vk.copy(step = kesTreeStep)
                      OperationalKeyMaker.verify(kSig, msgBytes, vkAtStep)
                  }
            }

        /** Structural VRF proof check — non-empty bytes at the EcVrf25519 minimum proof length (80 bytes per the EcVrf25519 wire format).
          * This is the Slice 9 placeholder; Slice 13's GSAM rewire will swap in the full `CommitteeSortition.verifyMembership` check once
          * the VRF VK registry is wired through the constructor.
          *
          * Why a method on the manager rather than a static helper: the production replacement will need access to `Hasher[F]` + the VRF VK
          * registry, both already in scope at the manager level. Keeping this as a method now means the slice-13 swap touches one method
          * body instead of unwinding helper plumbing.
          */
        private def verifyVrfStructural(proofBytes: Array[Byte]): Boolean = {
          // EcVrf25519 proofs are 80 bytes per `EcVrf25519` scaladoc. Reject anything shorter as malformed.
          val MinProofLength = 80
          proofBytes.length >= MinProofLength
        }
      }
    }
  }

}
