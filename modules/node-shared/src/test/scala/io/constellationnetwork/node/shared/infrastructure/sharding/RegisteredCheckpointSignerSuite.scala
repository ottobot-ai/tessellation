package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.MessageDigest

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ActiveOperatorConsensusKeys
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

object RegisteredCheckpointSignerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], RegisteredCheckpointSigner)

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(hasher: Hasher[IO]) = Hasher.forJson[IO]
      signer <- RegisteredCheckpointSigner.make.asResource
    } yield (hasher, sp, signer)

  private def shell(peerId: PeerId, epoch: EtaPeriod): ShardCheckpoint =
    ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal.Root.next,
      gl0AnchorOrdinal = SnapshotOrdinal.MinValue,
      slot = Slot.unsafeApply(1L),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.one(CommitteeMemberSignature(peerId, Hex(""), Hex(""), Hex(""), kesTreeStep = 0)),
      epoch = epoch,
      executionBase = io.constellationnetwork.node.shared.ShardCheckpointTestFixtures.defaultExecutionBase
    )

  test("signing is rejected when identity is not in the atomic genesis registry") { res =>
    implicit val (hasher, securityProvider, signer) = res
    val checkpointEpoch = EtaPeriod(5L)

    for {
      unregisteredKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      unregisteredPeer = PeerId.fromPublic(unregisteredKeyPair.getPublic)
      unregistered <- signer.sign(shell(unregisteredPeer, checkpointEpoch), unregisteredKeyPair, unregisteredPeer).attempt
    } yield expect(unregistered.isLeft)
  }

  test("an exact committed genesis pair is signable and exposed only as an atomic record") { res =>
    implicit val (hasher, securityProvider, signer) = res
    val checkpointEpoch = EtaPeriod(8L)

    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(keyPair.getPublic)
      _ <- signer.preregisterGenesis(keyPair, peerId)
      signature <- signer.sign(shell(peerId, checkpointEpoch), keyPair, peerId)
      registeredPair <- signer.operatorKeyRegistry.get(peerId)
      activePair <- ActiveOperatorConsensusKeys.resolve(signer.operatorKeyRegistry, peerId, checkpointEpoch)
      derivedVrf = VrfKeyDeriver.deriveVrfKeyPair(keyPair)._2
    } yield
      expect.all(
        signature.peerId == peerId,
        registeredPair.exists(_.registration.isEmpty),
        registeredPair.exists(_.effectiveFromPeriod == EtaPeriod.Zero),
        registeredPair.exists(pair => MessageDigest.isEqual(pair.vrfPublicKey.toBytes, derivedVrf)),
        activePair.nonEmpty
      )
  }

  test("period-zero eligibility requires an explicit genesis registration") { res =>
    implicit val (hasher, securityProvider, signer) = res

    for {
      genesisKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      genesisPeer = PeerId.fromPublic(genesisKeyPair.getPublic)
      _ <- signer.preregisterGenesis(genesisKeyPair, genesisPeer)
      signature <- signer.sign(shell(genesisPeer, EtaPeriod.Zero), genesisKeyPair, genesisPeer)
      activePair <- ActiveOperatorConsensusKeys.resolve(signer.operatorKeyRegistry, genesisPeer, EtaPeriod.Zero)
    } yield expect.all(signature.peerId == genesisPeer, activePair.nonEmpty)
  }

  test("a different local keypair cannot use another operator's preregistration") { res =>
    implicit val (hasher, securityProvider, signer) = res
    val checkpointEpoch = EtaPeriod(9L)

    for {
      registeredKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      registeredPeer = PeerId.fromPublic(registeredKeyPair.getPublic)
      _ <- signer.preregisterGenesis(registeredKeyPair, registeredPeer)
      substitutedKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      result <- signer.sign(shell(registeredPeer, checkpointEpoch), substitutedKeyPair, registeredPeer).attempt
    } yield expect(result.isLeft)
  }
}
