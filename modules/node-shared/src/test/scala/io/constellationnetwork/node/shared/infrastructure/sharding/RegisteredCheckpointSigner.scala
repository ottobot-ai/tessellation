package io.constellationnetwork.node.shared.infrastructure.sharding

import java.nio.ByteBuffer
import java.security.{KeyPair, MessageDigest}

import cats.effect.IO
import cats.effect.kernel.Ref
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry, VrfRegistry}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.{CommitteeMemberSignature, ShardCheckpoint}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.{OperationalKeyMaker, SecureStore}
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.{EcVrf25519, VrfKeyDeriver}
import io.constellationnetwork.security.{Hasher, SecurityProvider}

/** Test-only signer for checkpoint acceptance tests.
  *
  * Every returned signature uses real Ed25519, KES, and VRF material. The exposed registries are updated before `sign` returns, so tests
  * exercise the production fail-closed verification path without a missing-registry exception.
  */
final class RegisteredCheckpointSigner private (
  kesEntries: Ref[IO, Map[PeerId, KesRegistryEntry]],
  vrfEntries: Ref[IO, Map[PeerId, Array[Byte]]]
) {

  val defaultShardEta: Array[Byte] = Array.tabulate[Byte](32)(i => (i * 7 + 1).toByte)

  val kesRegistry: KesRegistry[IO] = new KesRegistry[IO] {
    def getKesVk(peerId: PeerId): IO[Option[KesRegistryEntry]] = kesEntries.get.map(_.get(peerId))
    def list: IO[Map[PeerId, KesRegistryEntry]] = kesEntries.get
  }

  val vrfRegistry: VrfRegistry[IO] = new VrfRegistry[IO] {
    def getVrfVk(peerId: PeerId): IO[Option[Array[Byte]]] = vrfEntries.get.map(_.get(peerId))
    def list: IO[Map[PeerId, Array[Byte]]] = vrfEntries.get
  }

  def sign(
    checkpoint: ShardCheckpoint,
    keyPair: KeyPair,
    peerId: PeerId,
    shardEta: Array[Byte]
  )(implicit hasher: Hasher[IO], securityProvider: SecurityProvider[IO]): IO[CommitteeMemberSignature] =
    for {
      preimageHash <- Hasher[IO].hash(checkpoint.signingPreimage)
      message = preimageHash.getBytes
      ed25519Sig <- Signing.signData[IO](message)(keyPair.getPrivate)
      kes <- signKes(message, peerId)
      (kesProductSig, kesTreeStep) = kes
      (vrfSeed, vrfVk) = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      slotBytes = ByteBuffer.allocate(8).putLong(checkpoint.slot.value.value).array()
      vrfProof <- IO(EcVrf25519.default.vrfProof(vrfSeed, shardEta ++ slotBytes))
      _ <- vrfEntries.update(_.updated(peerId, vrfVk))
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

  private def signKes(message: Array[Byte], peerId: PeerId): IO[(Array[Byte], Int)] = {
    val seed = MessageDigest.getInstance("SHA-256").digest(peerId.value.toBytes)
    for {
      store <- SecureStore.inMemory[IO]
      material <- OperationalKeyMaker.generateFreshKesKeyMaterial[IO](seed, height = (2, 2), offset = 0L)
      (encodedSecretKey, masterVerificationKey) = material
      _ <- store.write("checkpoint-test-kes.bin", encodedSecretKey)
      result <- OperationalKeyMaker.make[IO](store, "checkpoint-test-kes.bin", etaPeriodLength = 100L).use { signer =>
        for {
          step <- signer.currentPeriod
          signature <- signer.signAt(step, message)
          valid <- IO.fromEither(signature.leftMap(e => new IllegalStateException(s"test KES signing failed: $e")))
        } yield (OperationalKeyMaker.encodeSignature(valid), step)
      }
      _ <- kesEntries.update(_.updated(peerId, KesRegistryEntry(masterVerificationKey, offset = 0L)))
    } yield result
  }
}

object RegisteredCheckpointSigner {
  def make: IO[RegisteredCheckpointSigner] =
    for {
      kes <- Ref.of[IO, Map[PeerId, KesRegistryEntry]](Map.empty)
      vrf <- Ref.of[IO, Map[PeerId, Array[Byte]]](Map.empty)
    } yield new RegisteredCheckpointSigner(kes, vrf)
}
