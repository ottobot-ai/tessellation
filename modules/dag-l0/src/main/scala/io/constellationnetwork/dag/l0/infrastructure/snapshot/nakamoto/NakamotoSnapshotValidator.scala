package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.kernel.Async
import cats.syntax.all._

import io.constellationnetwork.dag.l0.infrastructure.snapshot._
import io.constellationnetwork.dag.l0.infrastructure.snapshot.event.GlobalSnapshotEvent
import io.constellationnetwork.node.shared.domain.consensus.ConsensusFunctions
import io.constellationnetwork.node.shared.domain.nakamoto.{EligibilityChecker, StakeRegistry}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.consensus.trigger.{ConsensusTrigger, EventTrigger}
import io.constellationnetwork.schema.mpt.GlobalStateKey
import io.constellationnetwork.schema.nakamoto.LddConfig
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Validates Nakamoto snapshots received via gossip.
  *
  * Verification pipeline:
  *   1. VRF proof verification — producer was legitimately elected for this slot 2. Signature verification — snapshot was signed by the
  *      claimed producer 3. SlotCertificate verification — cert fields match gossip message fields 4. Content validation — state
  *      transitions are correct (via ConsensusFunctions.validateArtifact)
  */
object NakamotoSnapshotValidator {

  sealed abstract class ValidationResult
  final case class Valid(snapshot: Signed[GlobalIncrementalSnapshot], context: GlobalSnapshotInfo) extends ValidationResult

  sealed abstract class Invalid extends ValidationResult
  case object ParentNotFound extends Invalid
  case object ParentBuffered extends Invalid
  final case class VrfFailed(slot: Long, detail: String) extends Invalid
  final case class SignatureInvalid(ordinal: Long) extends Invalid
  final case class ContentMismatch(detail: String) extends Invalid
  final case class VrfOnlyFailed(slot: Long) extends Invalid

  def validate[F[_]: Async: SecurityProvider: HasherSelector](
    signedSnapshot: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    slot: Long,
    vrfProof: Array[Byte],
    vrfPublicKey: Array[Byte],
    producerIdBytes: Array[Byte],
    eta: Array[Byte],
    slotGap: Long,
    stakeRegistry: StakeRegistry[F],
    lddConfig: LddConfig,
    eligibilityChecker: EligibilityChecker[F],
    consensusFns: ConsensusFunctions[F, GlobalSnapshotEvent, GlobalSnapshotKey, GlobalSnapshotArtifact, GlobalSnapshotContext],
    lastSignedArtifact: Signed[GlobalIncrementalSnapshot],
    lastContext: GlobalSnapshotInfo,
    getByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]],
    mptOverlay: io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay[
      F,
      io.constellationnetwork.schema.mpt.GlobalStateKey
    ]
  ): F[ValidationResult] = {
    val logger = Slf4jLogger.getLoggerFromName[F]("NakamotoValidator")
    val producerHex = Hex(producerIdBytes.map("%02x".format(_)).mkString)
    val producerId = PeerId(producerHex)

    for {
      // ── Step 1: VRF proof verification ──
      producerStake <- stakeRegistry.relativeStake(producerId)

      vrfValid <-
        if (vrfPublicKey.isEmpty || vrfProof.isEmpty) false.pure[F]
        else
          eligibilityChecker.verifyEligibility(
            vrfVK = vrfPublicKey,
            slot = Slot(NonNegLong.unsafeFrom(slot)),
            slotGap = slotGap,
            eta = eta,
            relativeStake = producerStake,
            config = lddConfig,
            proof = vrfProof
          )

      result <-
        if (!vrfValid) {
          logger
            .warn(s"❌ VRF failed: slot=$slot producer=${producerHex.value.take(8)} stake=$producerStake gap=$slotGap")
            .as(VrfFailed(slot, s"producer=${producerHex.value.take(8)} stake=$producerStake gap=$slotGap"): ValidationResult)
        } else {
          // ── Step 2: Signature verification ──
          HasherSelector[F].withCurrent { implicit hasher =>
            signedSnapshot.hasValidSignature[F].flatMap { sigValid =>
              if (!sigValid) {
                logger
                  .warn(s"❌ Signature invalid: slot=$slot ordinal=${signedSnapshot.ordinal}")
                  .as(SignatureInvalid(signedSnapshot.ordinal.value.value): ValidationResult)
              } else {
                // ── Step 3: SlotCertificate verification ──
                val certResult = signedSnapshot.value.slotCertificate match {
                  case None =>
                    // Pre-activation snapshots don't have certs — accept for now
                    Right(())
                  case Some(cert) =>
                    // Verify cert slot matches gossip slot
                    if (cert.slot.value.value != slot)
                      Left(s"SlotCertificate slot (${cert.slot.value.value}) != gossip slot ($slot)")
                    // Verify VRF proof in cert matches gossip proof
                    else if (!java.util.Arrays.equals(cert.vrfProof.toBytes, vrfProof))
                      Left("SlotCertificate VRF proof doesn't match gossip proof")
                    // Verify VRF public key matches
                    else if (!java.util.Arrays.equals(cert.vrfPublicKey.toBytes, vrfPublicKey))
                      Left("SlotCertificate VRF public key doesn't match gossip key")
                    else
                      Right(())
                }

                certResult match {
                  case Left(reason) =>
                    logger.warn(s"❌ Cert mismatch: $reason").as(ContentMismatch(s"cert: $reason"): ValidationResult)
                  case Right(_) =>
                    // ── Step 4: Content validation ──
                    // Compare received artifact against locally-recreated one.
                    // Strip Nakamoto-specific fields (slotCertificate, eta) before comparison
                    // since createProposalArtifact doesn't populate them — producer adds them post-creation.
                    val strippedReceived = signedSnapshot.value.copy(slotCertificate = None, eta = None)
                    consensusFns
                      .validateArtifact(
                        lastSignedArtifact,
                        lastContext,
                        EventTrigger,
                        strippedReceived,
                        Set(producerId),
                        getByOrdinal
                      )
                      .flatMap {
                        case Right((_, validatedContext)) =>
                          // Content matches AND we have properly derived state (stateProof correct).
                          //
                          // Phase J / MultiBranch: `validateArtifact` ran `createProposalArtifact(strippedReceived)`
                          // which committed the overlay handle at `hash(strippedReceived)` — i.e. the no-cert hash
                          // (line 502 of GlobalSnapshotConsensusFunctions). The chain's canonical reference for this
                          // ordinal is the WITH-cert hash (`hash(signedSnapshot.value)`). Without rekeying, ordinal
                          // N+1's `checkout(BranchId(getLastArtifactHash))` falls through to base on this node and
                          // misses N's pending writes — surfaces as `priorLastCurrencySnapshots` / balances divergence
                          // (validating a peer's WITH-cert artifact without aligning the overlay key to the
                          // canonical chain hash).
                          //
                          // Mirror the leader-side rekey in SnapshotLeaderLoop: walk the local pendingRef from
                          // stripped-hash to canonical-hash so the next ordinal's checkout finds the parent.
                          for {
                            strippedHash <- hasher.hash(strippedReceived)
                            canonicalHash <- hasher.hash(signedSnapshot.value)
                            _ <- mptOverlay.rekey(
                              io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(strippedHash),
                              io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId(canonicalHash)
                            )
                            _ <- logger.debug(s"✅ Full content validation passed: slot=$slot ordinal=${signedSnapshot.ordinal}")
                          } yield Valid(signedSnapshot, validatedContext): ValidationResult
                        case Left(err) =>
                          val logMsg = err match {
                            case gam: io.constellationnetwork.node.shared.infrastructure.snapshot.GlobalArtifactMismatch =>
                              val leader = gam.expected
                              val own = gam.found
                              val diffs = List.newBuilder[String]
                              if (leader.ordinal =!= own.ordinal) diffs += s"ordinal(recv=${leader.ordinal},own=${own.ordinal})"
                              if (leader.lastSnapshotHash =!= own.lastSnapshotHash)
                                diffs += s"lastHash(recv=${leader.lastSnapshotHash.show.take(12)},own=${own.lastSnapshotHash.show.take(12)})"
                              if (leader.epochProgress =!= own.epochProgress)
                                diffs += s"epoch(recv=${leader.epochProgress},own=${own.epochProgress})"
                              val spDiffList: List[String] =
                                if (leader.stateProof =!= own.stateProof) {
                                  val lp = leader.stateProof
                                  val op = own.stateProof
                                  val spDiffs = List.newBuilder[String]
                                  if (lp.lastStateChannelSnapshotHashesProof =!= op.lastStateChannelSnapshotHashesProof)
                                    spDiffs += "scHashes"
                                  if (lp.lastTxRefsProof =!= op.lastTxRefsProof) spDiffs += "txRefs"
                                  if (lp.balancesProof =!= op.balancesProof) spDiffs += "balances"
                                  if (lp.lastCurrencySnapshotsProof =!= op.lastCurrencySnapshotsProof) spDiffs += "currSnapshots"
                                  if (lp.activeAllowSpends =!= op.activeAllowSpends) spDiffs += "allowSpends"
                                  if (lp.activeTokenLocks =!= op.activeTokenLocks) spDiffs += "tokenLocks"
                                  if (lp.tokenLockBalances =!= op.tokenLockBalances) spDiffs += "tokenLockBal"
                                  if (lp.lastAllowSpendRefs =!= op.lastAllowSpendRefs) spDiffs += "allowSpendRefs"
                                  if (lp.lastTokenLockRefs =!= op.lastTokenLockRefs) spDiffs += "tokenLockRefs"
                                  if (lp.updateNodeParameters =!= op.updateNodeParameters) spDiffs += "nodeParams"
                                  if (lp.activeDelegatedStakes =!= op.activeDelegatedStakes) spDiffs += "delegStakes"
                                  if (lp.delegatedStakesWithdrawals =!= op.delegatedStakesWithdrawals) spDiffs += "delegWithdraw"
                                  if (lp.activeNodeCollaterals =!= op.activeNodeCollaterals) spDiffs += "nodeCollat"
                                  if (lp.nodeCollateralWithdrawals =!= op.nodeCollateralWithdrawals) spDiffs += "collatWithdraw"
                                  if (lp.priceState =!= op.priceState) spDiffs += "priceState"
                                  if (lp.lastGlobalSnapshotsWithCurrency =!= op.lastGlobalSnapshotsWithCurrency) spDiffs += "globalWithCurr"
                                  if (lp.mptRoot =!= op.mptRoot) spDiffs += "mptRoot"
                                  val result = spDiffs.result()
                                  diffs += s"stateProof[${result.mkString(",")}]"
                                  result
                                } else List.empty[String]
                              if (leader.rewards =!= own.rewards) diffs += s"rewards(recv=${leader.rewards.size},own=${own.rewards.size})"
                              if (leader.tips =!= own.tips) diffs += "tips"
                              val diffList = diffs.result()
                              val diffStr = if (diffList.isEmpty) "no-field-diff-detected" else diffList.mkString(",")
                              // 0harden phase 2: zero tolerance. Any diff between the leader's artifact and
                              // our locally-reconstructed one is rejected — including the rolled-up mptRoot.
                              // Per-field MPT-derived hashes are deterministic from MPT entries; the global
                              // mptRoot is deterministic from the same MPT producer state. If undo-journal
                              // fork rollback ever produces a transient mptRoot diff, ContentMismatch will
                              // surface it as a catch-up signal — which is the correct response.
                              val _ = spDiffList
                              val msg = s"❌ Content REJECTED: slot=$slot diffs=[$diffStr]"
                              (msg, false)
                            case _ =>
                              (s"❌ Content validation fail: slot=$slot err=$err", false)
                          }
                          if (logMsg._2)
                            logger.info(logMsg._1).as(Valid(signedSnapshot, context): ValidationResult)
                          else
                            logger.warn(logMsg._1).as(ContentMismatch(logMsg._1): ValidationResult)
                      }
                }
              }
            }
          }
        }
    } yield result
  }
}
