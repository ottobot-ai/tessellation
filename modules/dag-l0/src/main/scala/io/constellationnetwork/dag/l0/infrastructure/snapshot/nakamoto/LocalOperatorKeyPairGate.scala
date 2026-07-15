package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.file.{Path, Paths}
import java.security.KeyPair

import cats.effect.kernel.{Async, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{ActiveOperatorConsensusKeys, KesRegistryEntry, OperatorConsensusKeyRegistry}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.kes.{OperationalKeyMaker, OperationalKeyMakerAlgebra, SecureStore}
import io.constellationnetwork.security.vrf.VrfKeyDeriver

/** Fail-closed boundary between local secret material and the preregistered GL0 operator identity.
  *
  * A value of [[VerifiedLocalOperatorKeys]] can only be obtained when the node's long-term identity, KES master verification key, and VRF
  * verification key all match the entries registered for the same `PeerId`. Wire-carried keys are not accepted here. The current frozen
  * registries are paired projections of the atomically verified genesis operator record; runtime use must replace them with an exact
  * candidate-parent historical paired-key reader before runtime rotations become consensus-active.
  */
object LocalOperatorKeyPairGate {

  sealed abstract class Rejection(message: String) extends IllegalStateException(message)

  final case class MissingOperatorRegistration(peerId: PeerId)
      extends Rejection(s"Local GL0 operator ${short(peerId)} has no preregistered atomic KES+VRF pair")

  final case class InactiveOrInvalidOperatorRegistration(peerId: PeerId, artifactPeriod: Long)
      extends Rejection(
        s"Local GL0 operator ${short(peerId)} has no valid active atomic KES+VRF pair at eta period $artifactPeriod"
      )

  final case class LongTermIdentityMismatch(expected: PeerId, derived: PeerId)
      extends Rejection(
        s"Configured local GL0 PeerId ${short(expected)} does not match the loaded long-term key ${short(derived)}"
      )

  final case class InvalidKesRegistration(peerId: PeerId, reason: String)
      extends Rejection(s"Local GL0 operator ${short(peerId)} has an invalid preregistered KES key: $reason")

  final case class KesMasterKeyMismatch(peerId: PeerId)
      extends Rejection(s"Local KES secret material does not match the preregistered KES master key for ${short(peerId)}")

  final case class VrfKeyMismatch(peerId: PeerId)
      extends Rejection(s"Locally derived VRF key does not match the preregistered VRF key for ${short(peerId)}")

  final case class InvalidKesPeriod(globalPeriod: Long, offset: Long, reason: String)
      extends Rejection(s"Cannot derive local KES step from global eta period=$globalPeriod and registered offset=$offset: $reason")

  final case class KesSecretAlreadyAhead(peerId: PeerId, currentStep: Int, requiredStep: Int)
      extends Rejection(
        s"Local KES secret for ${short(peerId)} is already at tree step $currentStep, ahead of required step $requiredStep"
      )

  case object MissingSecureStoreDirectory
      extends Rejection("CL_KES_SECURE_STORE_DIR is required for GL0 consensus; no ephemeral KES fallback is permitted")

  final case class RegisteredSigningKey(operatorKeys: OperatorConsensusKeys, treeStep: Int)

  /** Capability carried into consensus production after the complete local operator-key pair has matched. Only the offset is exposed;
    * public key arrays remain owned by the registries and cannot be mutated through this value.
    */
  final case class VerifiedLocalOperatorKeys private[nakamoto] (peerId: PeerId, kesPeriodOffset: Long) {
    def treeStepFor(globalEtaPeriod: Long): Either[Rejection, Int] =
      LocalOperatorKeyPairGate.treeStepFor(globalEtaPeriod, kesPeriodOffset)

    /** Resolve the registered tree-relative step and reject a local secret that has already evolved beyond it. Being behind is valid:
      * `OperationalKeyMaker.signAt` atomically evolves to the returned step before signing.
      */
    def signingStepFor[F[_]: Async](
      globalEtaPeriod: Long,
      keyMaker: OperationalKeyMakerAlgebra[F]
    ): F[Either[Rejection, Int]] =
      treeStepFor(globalEtaPeriod) match {
        case Left(error) => Async[F].pure(Left(error))
        case Right(requiredStep) =>
          keyMaker.currentPeriod.map { currentStep =>
            if (currentStep > requiredStep) Left(KesSecretAlreadyAhead(peerId, currentStep, requiredStep))
            else Right(requiredStep)
          }
      }
  }

  def requireSecureStoreDirectory(configured: Option[String]): Either[Rejection, Path] =
    configured
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Paths.get(_))
      .toRight(MissingSecureStoreDirectory)

  /** Open already-provisioned KES material and authenticate it against the complete preregistered operator pair. `OperationalKeyMaker.make`
    * rejects an absent or malformed secure-store entry; this method never generates replacement material.
    */
  def loadVerified[F[_]: Async](
    secureStore: SecureStore[F],
    keyName: String,
    etaPeriodLength: Long,
    selfId: PeerId,
    longTermKeyPair: KeyPair,
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F]
  ): Resource[F, (OperationalKeyMakerAlgebra[F], VerifiedLocalOperatorKeys)] =
    OperationalKeyMaker
      .make[F](secureStore, keyName, etaPeriodLength)
      .evalMap { keyMaker =>
        verify(keyMaker, selfId, longTermKeyPair, operatorKeyRegistry).map(keyMaker -> _)
      }

  def verify[F[_]: Async](
    keyMaker: OperationalKeyMakerAlgebra[F],
    selfId: PeerId,
    longTermKeyPair: KeyPair,
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F]
  ): F[VerifiedLocalOperatorKeys] = {
    val derivedPeerId = PeerId.fromPublic(longTermKeyPair.getPublic)
    val (_, localVrfVk) = VrfKeyDeriver.deriveVrfKeyPair(longTermKeyPair)

    (
      keyMaker.currentPublicKey,
      keyMaker.currentPeriod,
      operatorKeyRegistry.get(selfId)
    ).tupled.flatMap {
      case (_, _, _) if derivedPeerId =!= selfId =>
        Async[F].raiseError[VerifiedLocalOperatorKeys](LongTermIdentityMismatch(selfId, derivedPeerId))

      case (_, _, None) =>
        Async[F].raiseError[VerifiedLocalOperatorKeys](MissingOperatorRegistration(selfId))

      case (localKesVk, localKesStep, Some(keys)) if !ActiveOperatorConsensusKeys.isValidAt(keys, selfId, keys.effectiveFromPeriod) =>
        Async[F].raiseError[VerifiedLocalOperatorKeys](
          InactiveOrInvalidOperatorRegistration(selfId, keys.effectiveFromPeriod.value)
        )

      case (localKesVk, localKesStep, Some(keys)) =>
        val registeredKes = keys.kes
        val registeredVrfVk = keys.vrfPublicKey.toBytes
        validateKesRegistration(selfId, registeredKes, localKesVk.step, localKesStep) match {
          case Left(error) => Async[F].raiseError[VerifiedLocalOperatorKeys](error)
          case Right(()) if !localKesVk.value.sameElements(registeredKes.vk.value) =>
            Async[F].raiseError[VerifiedLocalOperatorKeys](KesMasterKeyMismatch(selfId))
          case Right(()) if !localVrfVk.sameElements(registeredVrfVk) =>
            Async[F].raiseError[VerifiedLocalOperatorKeys](VrfKeyMismatch(selfId))
          case Right(()) =>
            Async[F].pure(VerifiedLocalOperatorKeys(selfId, registeredKes.offset))
        }
    }
  }

  /** Re-resolve the complete local pair immediately before a consensus signature. This prevents a stale or advanced secure-store key from
    * signing merely because startup once succeeded.
    */
  def registeredSigningKey[F[_]: Async](
    keyMaker: OperationalKeyMakerAlgebra[F],
    selfId: PeerId,
    longTermKeyPair: KeyPair,
    operatorKeyRegistry: OperatorConsensusKeyRegistry[F],
    globalEtaPeriod: Long
  ): F[Either[Rejection, RegisteredSigningKey]] =
    ActiveOperatorConsensusKeys.resolve(operatorKeyRegistry, selfId, EtaPeriod(globalEtaPeriod)).flatMap {
      case None       => Async[F].pure(Left(MissingOperatorRegistration(selfId)))
      case Some(keys) => signingKeyFromResolved(keyMaker, selfId, longTermKeyPair, keys, globalEtaPeriod)
    }

  /** Authenticate one pair already resolved from the frozen-genesis compatibility registry. No second registry lookup occurs, so the KES
    * step and VRF proof/signature use the same atomic record.
    *
    * This raw `OperatorConsensusKeys` boundary deliberately rejects runtime records through [[ActiveOperatorConsensusKeys.isValidAt]]. A
    * runtime migration must replace it with a non-forgeable capability minted by the exact candidate-parent historical resolver; structural
    * validity of a caller-supplied runtime record is not proof of canonical inclusion, the N-2 delay, or operator eligibility.
    */
  def signingKeyFromResolved[F[_]: Async](
    keyMaker: OperationalKeyMakerAlgebra[F],
    selfId: PeerId,
    longTermKeyPair: KeyPair,
    keys: OperatorConsensusKeys,
    globalEtaPeriod: Long
  ): F[Either[Rejection, RegisteredSigningKey]] = {
    val localPeerId = PeerId.fromPublic(longTermKeyPair.getPublic)
    val (_, localVrfVk) = VrfKeyDeriver.deriveVrfKeyPair(longTermKeyPair)
    val artifactPeriod = EtaPeriod(globalEtaPeriod)

    (keyMaker.currentPublicKey, keyMaker.currentPeriod).tupled.map {
      case (localKesVk, localKesStep) =>
        val registeredKes = keys.kes
        for {
          _ <- Either.cond(
            localPeerId === selfId,
            (),
            LongTermIdentityMismatch(selfId, localPeerId): Rejection
          )
          _ <- Either.cond(
            ActiveOperatorConsensusKeys.isValidAt(keys, selfId, artifactPeriod),
            (),
            InactiveOrInvalidOperatorRegistration(selfId, globalEtaPeriod): Rejection
          )
          _ <- validateKesRegistration(selfId, registeredKes, localKesVk.step, localKesStep)
          _ <- Either.cond(
            java.security.MessageDigest.isEqual(localKesVk.value, registeredKes.vk.value),
            (),
            KesMasterKeyMismatch(selfId): Rejection
          )
          _ <- Either.cond(
            java.security.MessageDigest.isEqual(localVrfVk, keys.vrfPublicKey.toBytes),
            (),
            VrfKeyMismatch(selfId): Rejection
          )
          requiredStep <- treeStepFor(globalEtaPeriod, registeredKes.offset)
          _ <- Either.cond(
            localKesStep <= requiredStep,
            (),
            KesSecretAlreadyAhead(selfId, localKesStep, requiredStep): Rejection
          )
        } yield RegisteredSigningKey(keys, requiredStep)
    }
  }

  def treeStepFor(globalEtaPeriod: Long, registeredOffset: Long): Either[Rejection, Int] =
    if (globalEtaPeriod < 0L)
      Left(InvalidKesPeriod(globalEtaPeriod, registeredOffset, "global period is negative"))
    else if (registeredOffset < 0L)
      Left(InvalidKesPeriod(globalEtaPeriod, registeredOffset, "registered offset is negative"))
    else
      Either
        .catchOnly[ArithmeticException](Math.subtractExact(globalEtaPeriod, registeredOffset))
        .leftMap(_ => InvalidKesPeriod(globalEtaPeriod, registeredOffset, "period subtraction overflowed"))
        .flatMap { step =>
          if (step < 0L) Left(InvalidKesPeriod(globalEtaPeriod, registeredOffset, "operator key is not active yet"))
          else if (step > Int.MaxValue.toLong)
            Left(InvalidKesPeriod(globalEtaPeriod, registeredOffset, "tree step exceeds the supported Int range"))
          else Right(step.toInt)
        }

  private def validateKesRegistration(
    peerId: PeerId,
    registered: KesRegistryEntry,
    localPublicStep: Int,
    localCurrentStep: Int
  ): Either[Rejection, Unit] =
    if (registered.vk.step != 0)
      Left(InvalidKesRegistration(peerId, s"master verification-key step must be 0, found ${registered.vk.step}"))
    else if (registered.offset < 0L)
      Left(InvalidKesRegistration(peerId, s"period offset must be nonnegative, found ${registered.offset}"))
    else if (localCurrentStep < 0)
      Left(InvalidKesRegistration(peerId, s"local KES step must be nonnegative, found $localCurrentStep"))
    else if (localPublicStep != localCurrentStep)
      Left(
        InvalidKesRegistration(
          peerId,
          s"local KES public-key step $localPublicStep does not match OperationalKeyMaker step $localCurrentStep"
        )
      )
    else Right(())

  private def short(peerId: PeerId): String = peerId.value.value.take(16)
}
