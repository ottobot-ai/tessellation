package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.Applicative
import cats.syntax.applicative._

import scala.util.Try

import io.constellationnetwork.node.shared.domain.nakamoto.kes.{KesRegistrationCertValidator, OperatorConsensusKeys}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{ShardId, ShardOrdinal}
import io.constellationnetwork.security.hash.Hash

/** Exact historical identity boundary used by slashing validators.
  *
  * Slashing may resolve an operator key only from the canonical state that governed the signed offence. A current registry, a frozen
  * genesis projection, a wire-carried key, or a receiver-selected branch is not an implementation of this interface. Until retained history
  * can prove a unique context, the resolver must return [[SlashingOperatorKeyResolution.HistoricalStateUnavailable]].
  *
  * Current schema blockers are represented in the contexts rather than hidden:
  *
  *   - admission attestations bind a metagraph parent hash but no exact GL0 `(ordinal, hash, root)` or eta period;
  *   - shard checkpoints bind an exact execution-state claim, but this offence context still carries only the legacy GL0 anchor ordinal and
  *     no authenticated Phase-2 capability or operator-registry witness.
  *
  * A production resolver therefore needs retained canonical evidence that uniquely connects these signed fields to the exact historical GL0
  * key view. If it cannot construct that proof, validation is unverifiable and cannot slash.
  */
trait SlashingOperatorKeyResolver[F[_]] {
  def resolve(
    peerId: PeerId,
    context: SlashingOffenceContext
  ): F[SlashingOperatorKeyResolution]
}

object SlashingOperatorKeyResolver {

  /** Explicit fail-closed resolver for wiring that has no exact historical registry view. There is deliberately no adapter from the
    * read-only `KesRegistry`/`VrfRegistry` compatibility projections or from `MutableKesRegistry`: none proves the exact canonical key view
    * that governed the signed offence.
    */
  def unavailable[F[_]: Applicative](reason: HistoricalStateUnavailableReason): SlashingOperatorKeyResolver[F] =
    new SlashingOperatorKeyResolver[F] {
      def resolve(peerId: PeerId, context: SlashingOffenceContext): F[SlashingOperatorKeyResolution] =
        (SlashingOperatorKeyResolution.HistoricalStateUnavailable(reason): SlashingOperatorKeyResolution).pure[F]
    }

  /** Constructor for a branch-historical implementation (and focused tests). The supplied function owns the proof that its resolved state
    * is the unique state governing `context`; validator-side structural checks still reject malformed or inactive pairs.
    */
  def make[F[_]](
    resolveF: (PeerId, SlashingOffenceContext) => F[SlashingOperatorKeyResolution]
  ): SlashingOperatorKeyResolver[F] =
    new SlashingOperatorKeyResolver[F] {
      def resolve(peerId: PeerId, context: SlashingOffenceContext): F[SlashingOperatorKeyResolution] = resolveF(peerId, context)
    }
}

sealed trait SlashingOffenceContext extends Product with Serializable

object SlashingOffenceContext {

  /** Signed admission-equivocation identity. `metagraphParentHash` is exact for the metagraph chain, but the current attestation schema
    * does not itself bind the GL0 anchor or eta period. A resolver must recover a unique retained mapping or return unavailable.
    */
  final case class MetagraphAdmission(
    metagraphAddress: Address,
    metagraphParentHash: Hash,
    binaryHash: Hash
  ) extends SlashingOffenceContext

  /** Signed execution-checkpoint identity. `declaredPeriod` and `gl0AnchorOrdinal` are in the checkpoint preimage. Although the checkpoint
    * now carries an exact execution-state claim, this context does not yet carry or authenticate it and therefore cannot select a unique
    * branch-historical registry view.
    */
  final case class ShardCheckpointExecution(
    shardId: ShardId,
    parentCheckpointHash: Hash,
    shardOrdinal: ShardOrdinal,
    gl0AnchorOrdinal: SnapshotOrdinal,
    declaredPeriod: EtaPeriod
  ) extends SlashingOffenceContext
}

sealed trait SlashingOperatorKeyResolution extends Product with Serializable

object SlashingOperatorKeyResolution {

  /** Atomic pair and randomness proven for the exact offence context.
    *
    * For a runtime pair, `Resolved` additionally asserts that its canonical inclusion and signed effective period satisfy the mandatory
    * two-complete-eta activation delay. If the resolver cannot prove inclusion timing from the same historical branch, it must return
    * `HistoricalStateUnavailable`; a cert's self-claimed effective period alone is insufficient.
    *
    * `vrfEta` is the exact 32-byte eta used by the artifact's VRF domain: global admission eta for
    * [[SlashingOffenceContext.MetagraphAdmission]], shard eta for [[SlashingOffenceContext.ShardCheckpointExecution]].
    */
  final case class Resolved(
    keys: OperatorConsensusKeys,
    offencePeriod: EtaPeriod,
    vrfEta: Array[Byte]
  ) extends SlashingOperatorKeyResolution

  final case class HistoricalStateUnavailable(
    reason: HistoricalStateUnavailableReason
  ) extends SlashingOperatorKeyResolution
}

sealed trait HistoricalStateUnavailableReason extends Product with Serializable

object HistoricalStateUnavailableReason {
  case object ExactOffenceContextNotCommitted extends HistoricalStateUnavailableReason
  case object HistoryPruned extends HistoricalStateUnavailableReason
  case object MissingOperatorRegistration extends HistoricalStateUnavailableReason
  case object AmbiguousHistoricalState extends HistoricalStateUnavailableReason
  case object HistoricalRandomnessUnavailable extends HistoricalStateUnavailableReason
  case object RegistryStateInvalid extends HistoricalStateUnavailableReason
}

/** A slashing validator has three semantically distinct results. Only [[SlashingValidationResult.Valid]] proves guilt and may reach a slash
  * transition. In particular, unavailable history is not an invalid signature and cannot be converted into guilt.
  */
sealed trait SlashingValidationResult[+R, +A] extends Product with Serializable

object SlashingValidationResult {
  final case class Valid[A](value: A) extends SlashingValidationResult[Nothing, A]
  final case class Invalid[R](reason: R) extends SlashingValidationResult[R, Nothing]
  final case class Unverifiable(reason: SlashingUnverifiableReason) extends SlashingValidationResult[Nothing, Nothing]
}

sealed trait SlashingUnverifiableReason extends Product with Serializable

object SlashingUnverifiableReason {
  final case class HistoricalKeyStateUnavailable(
    context: SlashingOffenceContext,
    reason: HistoricalStateUnavailableReason
  ) extends SlashingUnverifiableReason

  final case class ResolvedOperatorMismatch(expected: PeerId, resolved: PeerId) extends SlashingUnverifiableReason
  final case class ResolvedPeriodMismatch(signed: EtaPeriod, resolved: EtaPeriod) extends SlashingUnverifiableReason
  case object ResolvedRandomnessMismatch extends SlashingUnverifiableReason
  case object ResolvedAtomicPairMismatch extends SlashingUnverifiableReason
  final case class ResolvedPairNotActive(effectiveFrom: EtaPeriod, offencePeriod: EtaPeriod) extends SlashingUnverifiableReason
  case object MalformedAtomicPair extends SlashingUnverifiableReason
  case object InvalidResolvedVrfEta extends SlashingUnverifiableReason
  case object KesStepOutOfRange extends SlashingUnverifiableReason
  case object CheckpointSignerExclusivityNotSpecified extends SlashingUnverifiableReason
}

private[slashing] object ResolvedSlashingOperatorKeys {

  def validate(
    expectedPeerId: PeerId,
    resolution: SlashingOperatorKeyResolution.Resolved
  ): Either[SlashingUnverifiableReason, Unit] = {
    val keys = resolution.keys
    val kesBytes = keys.kes.vk.value
    val vrfBytes = keys.vrfPublicKey.toBytes
    val structurallyValid =
      kesBytes.length == KesRegistrationCertValidator.KesMasterVerificationKeyLength &&
        keys.kes.vk.step == 0 &&
        keys.kes.offset >= 0L &&
        keys.effectiveFromPeriod.value >= 0L &&
        keys.kes.offset == keys.effectiveFromPeriod.value &&
        vrfBytes.length == VrfPublicKey.ExpectedLength

    if (keys.operatorPeerId != expectedPeerId)
      Left(SlashingUnverifiableReason.ResolvedOperatorMismatch(expectedPeerId, keys.operatorPeerId))
    else if (!structurallyValid)
      Left(SlashingUnverifiableReason.MalformedAtomicPair)
    else if (resolution.vrfEta.length != 32)
      Left(SlashingUnverifiableReason.InvalidResolvedVrfEta)
    else if (resolution.offencePeriod.value < 0L || keys.effectiveFromPeriod.value > resolution.offencePeriod.value)
      Left(SlashingUnverifiableReason.ResolvedPairNotActive(keys.effectiveFromPeriod, resolution.offencePeriod))
    else
      keys.registration match {
        case None =>
          if (keys.effectiveFromPeriod == EtaPeriod.Zero && keys.kes.offset == 0L) Right(())
          else Left(SlashingUnverifiableReason.MalformedAtomicPair)
        case Some(record) =>
          val cert = record.event.value
          val sameKes = Try(cert.kesMasterVK.toBytes).toOption.exists(bytes => java.security.MessageDigest.isEqual(bytes, kesBytes))
          val sameVrf = Try(cert.vrfPublicKey.toBytes).toOption.exists(bytes => java.security.MessageDigest.isEqual(bytes, vrfBytes))
          if (
            cert.operatorPeerId == expectedPeerId && sameKes && sameVrf && cert.kesMasterVKStep == keys.kes.vk.step &&
            cert.offset == keys.kes.offset && cert.effectiveFromPeriod == keys.effectiveFromPeriod
          ) Right(())
          else Left(SlashingUnverifiableReason.MalformedAtomicPair)
      }
  }

  def expectedKesStep(
    resolution: SlashingOperatorKeyResolution.Resolved
  ): Either[SlashingUnverifiableReason, Int] = {
    val difference = BigInt(resolution.offencePeriod.value) - BigInt(resolution.keys.kes.offset)
    if (difference < 0 || difference > Int.MaxValue) Left(SlashingUnverifiableReason.KesStepOutOfRange)
    else Right(difference.intValue)
  }

  def sameAtomicPair(
    a: SlashingOperatorKeyResolution.Resolved,
    b: SlashingOperatorKeyResolution.Resolved
  ): Boolean =
    a.keys.operatorPeerId == b.keys.operatorPeerId &&
      a.keys.kes.offset == b.keys.kes.offset &&
      a.keys.kes.vk.step == b.keys.kes.vk.step &&
      a.keys.effectiveFromPeriod == b.keys.effectiveFromPeriod &&
      java.security.MessageDigest.isEqual(a.keys.kes.vk.value, b.keys.kes.vk.value) &&
      java.security.MessageDigest.isEqual(a.keys.vrfPublicKey.toBytes, b.keys.vrfPublicKey.toBytes)
}
