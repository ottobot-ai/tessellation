package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.ByteBuffer
import java.security.{KeyPair, MessageDigest}

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardCheckpoint}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.{OperationalKeyMaker, SecureStore}
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.{EcVrf25519, VrfKeyDeriver}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Test-only signer for checkpoint acceptance tests.
  *
  * Every returned signature uses real Ed25519, KES, and VRF material from one explicit atomic genesis registration. This helper
  * deliberately does not model runtime activation: that requires an exact candidate-parent historical registry, which a mutable
  * current-view fixture cannot reproduce safely. `sign` never registers or rotates keys as a side effect.
  */
final class RegisteredCheckpointSigner private (
  registrations: Ref[IO, Map[PeerId, RegisteredCheckpointSigner.Registration]]
) {

  import RegisteredCheckpointSigner._

  val defaultShardEta: Array[Byte] = Array.tabulate[Byte](32)(i => (i * 7 + 1).toByte)

  /** The only public registration view. Consensus fixtures must resolve the complete KES+VRF pair with `get`; the projections required by
    * the compatibility algebra are derived inside this registry and are not independent mutable authorities.
    */
  val operatorKeyRegistry: OperatorConsensusKeyRegistry[IO] = new OperatorConsensusKeyRegistry[IO] { registry =>
    override def get(peerId: PeerId): IO[Option[OperatorConsensusKeys]] =
      registrations.get.map(_.get(peerId).map(_.operatorKeys))

    override def list: IO[Map[PeerId, OperatorConsensusKeys]] =
      registrations.get.map(_.iterator.map { case (peerId, registration) => peerId -> registration.operatorKeys }.toMap)

    override val kesRegistry: KesRegistry[IO] = new KesRegistry[IO] {
      override def getKesVk(peerId: PeerId): IO[Option[KesRegistryEntry]] =
        registry.get(peerId).map(_.map(_.kes))

      override def list: IO[Map[PeerId, KesRegistryEntry]] =
        registry.list.map(_.view.mapValues(_.kes).toMap)
    }

    override val vrfRegistry: VrfRegistry[IO] = new VrfRegistry[IO] {
      override def getVrfVk(peerId: PeerId): IO[Option[Array[Byte]]] =
        registry.get(peerId).map(_.map(_.vrfPublicKey.toBytes))

      override def list: IO[Map[PeerId, Array[Byte]]] =
        registry.list.map(_.view.mapValues(_.vrfPublicKey.toBytes).toMap)
    }
  }

  /** Real producer/emitter KES adapter backed by the same loader-validated genesis pair exposed through [[operatorKeyRegistry]]. A
    * caller-supplied pair is comparison evidence only: signing fails closed unless it is byte-identical to the preregistered pair.
    */
  val producerKesSigner: ShardCheckpointProducer.KesSigner[IO] = new ShardCheckpointProducer.KesSigner[IO] {
    override def sign(
      operatorKeys: OperatorConsensusKeys,
      checkpointEpoch: EtaPeriod,
      message: Array[Byte]
    ): IO[Option[ShardCheckpointProducer.KesSignature]] =
      registrations.get.flatMap { current =>
        current.get(operatorKeys.operatorPeerId) match {
          case Some(registration) if samePair(operatorKeys, registration.operatorKeys) =>
            ActiveOperatorConsensusKeys.treeStep(registration.operatorKeys, checkpointEpoch) match {
              case Some(requiredStep) =>
                signKes(message, registration, requiredStep).map {
                  case (bytes, step) =>
                    Some(ShardCheckpointProducer.KesSignature(step, bytes))
                }
              case None => IO.pure(None)
            }
          case _ => IO.pure(None)
        }
      }
  }

  /** Register a complete pair in the fixture's committed genesis population. Runtime registrations belong in historical-registry tests and
    * are intentionally unsupported here.
    */
  def preregisterGenesis(keyPair: KeyPair, peerId: PeerId)(implicit securityProvider: SecurityProvider[IO]): IO[Unit] =
    registerGenesis(keyPair, peerId)

  private def registerGenesis(keyPair: KeyPair, peerId: PeerId)(implicit securityProvider: SecurityProvider[IO]): IO[Unit] = {
    val expectedPeerId = PeerId.fromPublic(keyPair.getPublic)

    for {
      _ <- IO.raiseWhen(expectedPeerId =!= peerId)(
        new IllegalArgumentException(s"checkpoint test registration peer/key mismatch peer=${peerId.value.value.take(16)}")
      )
      generated <- CanonicalOperatorConsensusFixture.generateForIdentity(keyPair)
      registration = Registration(
        generated.resolvedPair,
        generated.encodedKesSecret.clone(),
        generated.vrfSecret.clone()
      )
      result <- registrations.modify { current =>
        if (current.contains(peerId))
          current -> Left(new IllegalStateException(s"checkpoint test identity already registered peer=${peerId.value.value.take(16)}"))
        else {
          val candidatePairs =
            current.iterator.map { case (id, value) => id -> value.operatorKeys }.toMap.updated(peerId, registration.operatorKeys)
          OperatorConsensusKeyRegistry.make[IO](candidatePairs)
          current.updated(peerId, registration) -> Right(())
        }
      }
      _ <- IO.fromEither(result)
      _ <- IO(java.util.Arrays.fill(generated.encodedKesSecret, 0.toByte))
      _ <- IO(java.util.Arrays.fill(generated.vrfSecret, 0.toByte))
    } yield ()
  }

  def sign(
    checkpoint: ShardCheckpoint,
    keyPair: KeyPair,
    peerId: PeerId,
    shardEta: Array[Byte]
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[CommitteeMemberSignature] =
    for {
      registration <- registrations.get.flatMap { current =>
        IO.fromOption(current.get(peerId))(
          new IllegalStateException(s"checkpoint signer is not preregistered peer=${peerId.value.value.take(16)}")
        )
      }
      expectedPeerId = PeerId.fromPublic(keyPair.getPublic)
      (_, derivedVrfVk) = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      _ <- IO.raiseWhen(expectedPeerId =!= peerId)(
        new IllegalArgumentException(s"checkpoint signing peer/key mismatch peer=${peerId.value.value.take(16)}")
      )
      _ <- IO.raiseUnless(
        derivedVrfVk.length == VrfPublicKey.ExpectedLength &&
          registration.operatorKeys.vrfPublicKey.toBytes.length == io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey.ExpectedLength &&
          MessageDigest.isEqual(derivedVrfVk, registration.operatorKeys.vrfPublicKey.toBytes)
      )(
        new IllegalStateException(s"checkpoint signing VRF key is not the preregistered key peer=${peerId.value.value.take(16)}")
      )
      requiredKesStep = BigInt(checkpoint.epoch.value) - BigInt(registration.operatorKeys.kes.offset)
      _ <- IO.raiseUnless(requiredKesStep >= 0 && requiredKesStep <= Int.MaxValue)(
        new IllegalStateException(
          s"checkpoint signing genesis registration is not active peer=${peerId.value.value.take(16)} " +
            s"activation=${registration.operatorKeys.kes.offset} checkpoint=${checkpoint.epoch.value}"
        )
      )
      preimageHash <- Hasher[IO].hash(checkpoint.signingPreimage)
      message = preimageHash.getBytes
      ed25519Sig <- Signing.signData[IO](message)(keyPair.getPrivate)
      kes <- signKes(message, registration, requiredKesStep.intValue)
      (kesProductSig, kesTreeStep) = kes
      slotBytes = ByteBuffer.allocate(8).putLong(checkpoint.slot.value.value).array()
      vrfProof <- IO(EcVrf25519.default.vrfProof(registration.vrfSecret, shardEta ++ slotBytes))
    } yield
      CommitteeMemberSignature(
        peerId = peerId,
        vrfProof = Hex.fromBytes(vrfProof),
        ed25519Sig = Hex.fromBytes(ed25519Sig),
        kesProductSig = Hex.fromBytes(kesProductSig),
        kesTreeStep = kesTreeStep
      )

  def sign(
    checkpoint: ShardCheckpoint,
    keyPair: KeyPair,
    peerId: PeerId
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[CommitteeMemberSignature] =
    sign(checkpoint, keyPair, peerId, defaultShardEta)

  /** Recover the exact fork-choice output from a signature emitted by this preregistered fixture. Test chain stores must cache this value,
    * never unrelated sentinel bytes, so their selected checkpoints retain coherent VRF evidence.
    */
  def proofDerivedVrfOutput(signature: CommitteeMemberSignature): IO[Array[Byte]] =
    registrations.get.flatMap { current =>
      IO.raiseUnless(current.contains(signature.peerId))(
        new IllegalArgumentException(
          s"checkpoint VRF proof belongs to an unregistered test identity peer=${signature.peerId.value.value.take(16)}"
        )
      ) >>
        IO.fromOption(EcVrf25519.default.vrfProofToHash(signature.vrfProof.toBytes))(
          new IllegalArgumentException("registered checkpoint VRF proof cannot be converted to a fork-choice output")
        )
    }

  private def signKes(message: Array[Byte], registration: Registration, requiredStep: Int): IO[(Array[Byte], Int)] =
    for {
      store <- SecureStore.inMemory[IO]
      _ <- store.write("checkpoint-test-kes.bin", registration.encodedKesSecretKey)
      result <- OperationalKeyMaker.make[IO](store, "checkpoint-test-kes.bin", etaPeriodLength = 100L).use { signer =>
        for {
          signature <- signer.signAt(requiredStep, message)
          valid <- IO.fromEither(signature.leftMap(e => new IllegalStateException(s"test KES signing failed: $e")))
        } yield (OperationalKeyMaker.encodeSignature(valid), requiredStep)
      }
    } yield result

  private def samePair(left: OperatorConsensusKeys, right: OperatorConsensusKeys): Boolean =
    left.operatorPeerId == right.operatorPeerId &&
      left.kes.offset == right.kes.offset &&
      left.kes.vk.step == right.kes.vk.step &&
      left.effectiveFromPeriod == right.effectiveFromPeriod &&
      left.registration == right.registration &&
      MessageDigest.isEqual(left.kes.vk.value, right.kes.vk.value) &&
      MessageDigest.isEqual(left.vrfPublicKey.toBytes, right.vrfPublicKey.toBytes)
}

object RegisteredCheckpointSigner {
  private final case class Registration(
    operatorKeys: OperatorConsensusKeys,
    encodedKesSecretKey: Array[Byte],
    vrfSecret: Array[Byte]
  )

  def make: IO[RegisteredCheckpointSigner] =
    Ref.of[IO, Map[PeerId, Registration]](Map.empty).map(new RegisteredCheckpointSigner(_))
}
