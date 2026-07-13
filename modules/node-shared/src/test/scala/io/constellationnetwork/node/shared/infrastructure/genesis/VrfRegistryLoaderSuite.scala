package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

object VrfRegistryLoaderSuite extends MutableIOSuite {

  override type Res = SecurityProvider[IO]

  override def sharedResource: Resource[IO, Res] = SecurityProvider.forAsync[IO]

  private val meta = L0GenesisMeta("test", "2026-05-26T00:00:00Z", "test", 0L, Nil)
  private val protocolParams = L0GenesisProtocolParams(1, 2550L, "00" * 32, 0L)

  private def baseData(
    operators: List[L0GenesisOperator],
    networkMagic: String = "test",
    activationOrdinal: Long = 0L,
    startingEpochProgress: Long = 0L
  ): L0GenesisData =
    L0GenesisData(
      _meta = meta,
      networkMagic = networkMagic,
      activationOrdinal = activationOrdinal,
      startingEpochProgress = startingEpochProgress,
      protocolParams = protocolParams,
      operators = operators,
      delegatedStakes = Nil,
      nodeCollaterals = Nil,
      initialBalances = Nil
    )

  private def operatorAndRegistration(index: Int)(implicit sp: SecurityProvider[IO]) =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(keyPair.getPublic)
      (_, vrfVk) = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      kesVk = Array.tabulate[Byte](32)(i => (index * 37 + i + 1).toByte)
      address = keyPair.getPublic.toAddress.value.value
      preimage = L0GenesisOperator.signaturePreimage(
        "test",
        0L,
        0L,
        peerId.value.toBytes,
        address,
        kesVk,
        0,
        0L,
        vrfVk
      )
      signature <- Signing.signData[IO](preimage)(keyPair.getPrivate)
      operator = L0GenesisOperator(
        peerId = peerId.value.value,
        address = address,
        kesMasterVk = Hex.fromBytes(kesVk).value,
        kesMasterVkStep = 0,
        kesPeriodOffset = 0L,
        vrfVk = Hex.fromBytes(vrfVk).value,
        longTermSignature = Hex.fromBytes(signature).value
      )
    } yield (keyPair, operator)

  test("paired loader preserves the generator/runtime VRF derivation contract") { implicit sp =>
    for {
      records <- (0 until 4).toList.traverse(operatorAndRegistration)
      pairedRegistry <- L0GenesisLoader.buildOperatorKeyRegistry[IO](
        baseData(records.map(_._2))
      )
      kesRegistry = pairedRegistry.kesRegistry
      vrfRegistry = pairedRegistry.vrfRegistry
      pairedEntries <- pairedRegistry.list
      kesEntries <- kesRegistry.list
      vrfEntries <- vrfRegistry.list
      checks <- records.traverse {
        case (keyPair, _) =>
          val peerId = PeerId.fromPublic(keyPair.getPublic)
          val expected = VrfKeyDeriver.deriveVrfKeyPair(keyPair)._2
          vrfRegistry.getVrfVk(peerId).map(loaded => expect(loaded.exists(java.util.Arrays.equals(_, expected))))
      }
    } yield
      expect.same(4, pairedEntries.size) &&
        expect.same(pairedEntries.keySet, kesEntries.keySet) &&
        expect.same(pairedEntries.keySet, vrfEntries.keySet) &&
        checks.combineAll
  }

  test("derived registry projections cannot mutate the paired genesis source") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      pairedRegistry <- L0GenesisLoader.buildOperatorKeyRegistry[IO](baseData(List(record._2)))
      kesRegistry = pairedRegistry.kesRegistry
      vrfRegistry = pairedRegistry.vrfRegistry
      peerId = PeerId(Hex(record._2.peerId))
      firstKes <- kesRegistry.getKesVk(peerId)
      firstVrf <- vrfRegistry.getVrfVk(peerId)
      _ <- IO {
        firstKes.foreach(entry => entry.vk.value(0) = (entry.vk.value(0) ^ 0xff).toByte)
        firstVrf.foreach(bytes => bytes(0) = (bytes(0) ^ 0xff).toByte)
      }
      secondKes <- kesRegistry.getKesVk(peerId)
      secondVrf <- vrfRegistry.getVrfVk(peerId)
    } yield
      expect(secondKes.exists(_.vk.value.sameElements(Hex(record._2.kesMasterVk).toBytes))) &&
        expect(secondVrf.exists(_.sameElements(Hex(record._2.vrfVk).toBytes)))
  }

  test("paired loader rejects malformed and wrong-length VRF keys") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      malformed <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2.copy(vrfVk = "zz" * 32))))
        .attempt
      short <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2.copy(vrfVk = "11" * 31))))
        .attempt
    } yield
      expect(malformed.swap.exists(_.getMessage.contains("not valid hexadecimal"))) &&
        expect(short.swap.exists(_.getMessage.contains("exactly 32 bytes")))
  }

  test("paired loader rejects duplicate operator identities before map construction") { implicit sp =>
    for {
      first <- operatorAndRegistration(0)
      second <- operatorAndRegistration(1)
      duplicate = second._2.copy(peerId = first._2.peerId)
      result <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(first._2, duplicate)))
        .attempt
    } yield expect(result.swap.exists(_.getMessage.contains("duplicate operator PeerId")))
  }

  test("paired loader rejects VRF key reuse across distinct operators") { implicit sp =>
    for {
      first <- operatorAndRegistration(0)
      second <- operatorAndRegistration(1)
      reused = second._2.copy(vrfVk = first._2.vrfVk)
      result <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(first._2, reused)))
        .attempt
    } yield expect(result.swap.exists(_.getMessage.contains("duplicate VRF verification key")))
  }

  test("paired loader rejects malformed and wrong-length PeerIds") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      malformed <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2.copy(peerId = "gg" * 64))))
        .attempt
      short <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2.copy(peerId = "11" * 63))))
        .attempt
    } yield
      expect(malformed.swap.exists(_.getMessage.contains("not valid hexadecimal"))) &&
        expect(short.swap.exists(_.getMessage.contains("exactly 64 bytes")))
  }

  test("paired loader rejects VRF substitution under an otherwise valid operator record") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      substituted = record._2.copy(vrfVk = "42" * 32)
      result <- L0GenesisLoader.buildOperatorKeyRegistry[IO](baseData(List(substituted))).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }

  test("paired loader rejects an operator-key record replayed into another network") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      result <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2), networkMagic = "other-network"))
        .attempt
    } yield expect(result.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }

  test("paired loader rejects an operator-key record replayed at another genesis activation context") { implicit sp =>
    for {
      record <- operatorAndRegistration(0)
      changedActivation <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2), activationOrdinal = 1L))
        .attempt
      changedEpochProgress <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](baseData(List(record._2), startingEpochProgress = 1L))
        .attempt
    } yield
      expect(changedActivation.swap.exists(_.getMessage.contains("longTermSignature does not bind"))) &&
        expect(changedEpochProgress.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }

  test("paired loader rejects an address that does not derive from the signed PeerId") { implicit sp =>
    for {
      first <- operatorAndRegistration(0)
      second <- operatorAndRegistration(1)
      substituted = first._2.copy(address = second._2.address)
      result <- L0GenesisLoader.buildOperatorKeyRegistry[IO](baseData(List(substituted))).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("address does not match its PeerId")))
  }
}
