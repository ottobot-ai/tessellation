package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.nakamoto.GenesisOperatorConsensusKey
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.{GlobalSnapshotInfo, GlobalStateProofSelector, SnapshotOrdinal}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GenesisOperatorConsensusKeyCodec.immutableCodec

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

object GenesisOperatorKeyCommitmentSuite extends MutableIOSuite {
  override type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(json: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield (json, Hasher.forJson[IO], sp)

  private implicit val selector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.MinValue)
  private implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  private val meta = L0GenesisMeta("test", "2026-07-13T00:00:00Z", "test", 0L, Nil)

  private def data(index: Int, networkMagic: String = "test")(implicit sp: SecurityProvider[IO]): IO[L0GenesisData] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      peerId = io.constellationnetwork.schema.peer.PeerId.fromPublic(keyPair.getPublic)
      address = keyPair.getPublic.toAddress.value.value
      kesVk = Array.tabulate[Byte](32)(i => (index * 37 + i + 1).toByte)
      vrfVk = VrfKeyDeriver.deriveVrfKeyPair(keyPair)._2
      signature <- Signing.signData[IO](
        L0GenesisOperator.signaturePreimage(
          networkMagic,
          0L,
          0L,
          peerId.value.toBytes,
          address,
          kesVk,
          0,
          0L,
          vrfVk
        )
      )(keyPair.getPrivate)
      operator = L0GenesisOperator(
        peerId.value.value,
        address,
        Hex.fromBytes(kesVk).value,
        0,
        0L,
        Hex.fromBytes(vrfVk).value,
        Hex.fromBytes(signature).value
      )
    } yield L0GenesisData(meta, networkMagic, 0L, 0L, L0GenesisProtocolParams.default, List(operator), Nil, Nil, Nil)

  private def store(implicit hasher: Hasher[IO], json: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      result <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield result

  test("changing the valid signed genesis operator set changes the canonical MPT root") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      first <- data(1)
      second <- data(2)
      firstInfo <- L0GenesisLoader.augmentSnapshotInfo[IO](GlobalSnapshotInfo.empty, first)
      secondInfo <- L0GenesisLoader.augmentSnapshotInfo[IO](GlobalSnapshotInfo.empty, second)
      firstProof <- firstInfo.stateProof[IO](SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
      secondProof <- secondInfo.stateProof[IO](SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
    } yield
      expect(firstInfo.genesisOperatorKeys.nonEmpty) &&
        expect(secondInfo.genesisOperatorKeys.nonEmpty) &&
        expect(firstProof.mptRoot.nonEmpty) &&
        expect(secondProof.mptRoot.nonEmpty) &&
        expect(firstProof.mptRoot =!= secondProof.mptRoot)
  }

  test("rooted records round-trip through MPT and local startup material must match") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      canonicalData <- data(3)
      wrongLocalData <- data(4)
      info <- L0GenesisLoader.augmentSnapshotInfo[IO](GlobalSnapshotInfo.empty, canonicalData)
      mpt <- store
      _ <- mpt.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
      materialized <- L0GenesisLoader.materializeRootedGenesisOperatorKeys[IO](mpt)
      canonicalLocal <- L0GenesisLoader.buildOperatorKeyRegistry[IO](canonicalData)
      _ <- L0GenesisLoader.requireLocalRegistryMatchesRooted[IO](canonicalLocal, materialized)
      wrongLocal <- L0GenesisLoader.buildOperatorKeyRegistry[IO](wrongLocalData)
      mismatch <- L0GenesisLoader.requireLocalRegistryMatchesRooted[IO](wrongLocal, materialized).attempt
    } yield
      expect.same(info.genesisOperatorKeys, materialized) &&
        expect(mismatch.swap.exists(_.getMessage.contains("does not match the root-authenticated genesis identity")))
  }

  test("mutating a rooted key without the operator signature fails closed") { res =>
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      fixture <- data(5)
      records <- L0GenesisLoader.buildGenesisOperatorKeys[IO](fixture)
      (peerId, record) = records.head
      mutated = records.updated(peerId, record.copy(vrfPublicKey = VrfPublicKey(Hex("77" * 32))))
      result <- L0GenesisLoader.buildOperatorKeyRegistryFromRooted[IO](mutated).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("rooted long-term signature is invalid")))
  }

  test("rooted records from different network contexts cannot form one genesis identity") { res =>
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      first <- data(6, "network-a").flatMap(L0GenesisLoader.buildGenesisOperatorKeys[IO])
      second <- data(7, "network-b").flatMap(L0GenesisLoader.buildGenesisOperatorKeys[IO])
      mixed = SortedMap.from(first.toList ++ second.toList)
      result <- L0GenesisLoader.buildOperatorKeyRegistryFromRooted[IO](mixed).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("one network/activation context")))
  }

  test("MPT reader rejects a valid signed record stored under another operator key") { res =>
    implicit val json: JsonSerializer[IO] = res._1
    implicit val hasher: Hasher[IO] = res._2
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      first <- data(8).flatMap(L0GenesisLoader.buildGenesisOperatorKeys[IO])
      second <- data(9).flatMap(L0GenesisLoader.buildGenesisOperatorKeys[IO])
      record = first.head._2
      wrongPeer = second.head._1
      mpt <- store
      wrongKey <- GlobalStateKey.genesisOperatorKey[IO](wrongPeer)
      _ <- mpt.insert[GenesisOperatorConsensusKey](wrongKey, record)
      _ <- mpt.commit(SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
      result <- L0GenesisLoader.materializeRootedGenesisOperatorKeys[IO](mpt).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("misplaced=")))
  }

  test("ordinary state deltas preserve the immutable genesis identity") { res =>
    implicit val hasher: Hasher[IO] = res._2
    implicit val sp: SecurityProvider[IO] = res._3

    for {
      fixture <- data(10)
      info <- L0GenesisLoader.augmentSnapshotInfo[IO](GlobalSnapshotInfo.empty, fixture)
      after = io.constellationnetwork.schema.mpt.GlobalStateConverter.applyAccumulatorToGSI(
        info,
        io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator()
      )
      registry <- L0GenesisLoader.buildOperatorKeyRegistryFromRooted[IO](after.genesisOperatorKeys)
      pairs <- registry.list
    } yield
      expect.same(info.genesisOperatorKeys, after.genesisOperatorKeys) &&
        expect(pairs.values.forall(keys => keys.registration.isEmpty && keys.effectiveFromPeriod.value === 0L))
  }
}
