package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

object KesRegistryLoaderSuite extends MutableIOSuite {

  override type Res = SecurityProvider[IO]

  override def sharedResource: Resource[IO, Res] = SecurityProvider.forAsync[IO]

  private val meta = L0GenesisMeta("test", "2026-05-16T00:00:00Z", "test", 0L, Nil)
  private val protocolParams = L0GenesisProtocolParams(1, 2550L, "00" * 32, 0L)

  private def data(
    operators: List[L0GenesisOperator]
  ): L0GenesisData =
    L0GenesisData(meta, "test", 0L, 0L, protocolParams, operators, Nil, Nil, Nil)

  private def record(index: Int)(implicit sp: SecurityProvider[IO]) =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(keyPair.getPublic)
      vrfVk = VrfKeyDeriver.deriveVrfKeyPair(keyPair)._2
      kesVk = Array.tabulate[Byte](32)(i => (index * 41 + i + 1).toByte)
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
    } yield operator

  test("paired loader round-trips one KES master key under its operator identity") { implicit sp =>
    for {
      pair <- record(0)
      pairedRegistry <- L0GenesisLoader.buildOperatorKeyRegistry[IO](data(List(pair)))
      peerId = PeerId(Hex(pair.peerId))
      keys <- pairedRegistry.get(peerId)
    } yield
      expect(keys.exists(_.kes.vk.value.sameElements(Hex(pair.kesMasterVk).toBytes))) &&
        expect.same(Some(0), keys.map(_.kes.vk.step)) &&
        expect.same(Some(0L), keys.map(_.kes.offset)) &&
        expect(keys.exists(_.vrfPublicKey.toBytes.sameElements(Hex(pair.vrfVk).toBytes)))
  }

  test("paired loader rejects an empty operator-key anchor") { implicit sp =>
    L0GenesisLoader
      .buildOperatorKeyRegistry[IO](data(Nil))
      .attempt
      .map(result => expect(result.swap.exists(_.getMessage.contains("operators is empty"))))
  }

  test("paired loader rejects duplicate operator identities and KES key reuse") { implicit sp =>
    for {
      first <- record(0)
      second <- record(1)
      duplicateIdentity <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(first, second.copy(peerId = first.peerId))))
        .attempt
      reusedKey = second.copy(kesMasterVk = first.kesMasterVk)
      duplicateKey <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(first, reusedKey)))
        .attempt
    } yield
      expect(duplicateIdentity.swap.exists(_.getMessage.contains("duplicate operator PeerId"))) &&
        expect(duplicateKey.swap.exists(_.getMessage.contains("duplicate KES master verification key")))
  }

  test("paired loader rejects malformed/wrong-length KES keys and non-genesis step or offset") { implicit sp =>
    for {
      pair <- record(0)
      malformed <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(pair.copy(kesMasterVk = "zz" * 32))))
        .attempt
      short <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(pair.copy(kesMasterVk = "11" * 31))))
        .attempt
      stepped <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(pair.copy(kesMasterVkStep = 1))))
        .attempt
      offset <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(pair.copy(kesPeriodOffset = 1L))))
        .attempt
    } yield
      expect(malformed.swap.exists(_.getMessage.contains("not valid hexadecimal"))) &&
        expect(short.swap.exists(_.getMessage.contains("exactly 32 bytes"))) &&
        expect(stepped.swap.exists(_.getMessage.contains("kesMasterVkStep=0"))) &&
        expect(offset.swap.exists(_.getMessage.contains("kesPeriodOffset=0")))
  }

  test("paired loader verifies that longTermSignature binds the complete record") { implicit sp =>
    for {
      first <- record(0)
      second <- record(1)
      wrongSignature = first.copy(longTermSignature = second.longTermSignature)
      result <- L0GenesisLoader
        .buildOperatorKeyRegistry[IO](data(List(wrongSignature)))
        .attempt
    } yield expect(result.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }

  test("paired loader rejects KES substitution under an otherwise valid operator record") { implicit sp =>
    for {
      pair <- record(0)
      substituted = pair.copy(kesMasterVk = "42" * 32)
      result <- L0GenesisLoader.buildOperatorKeyRegistry[IO](data(List(substituted))).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }

  test("paired loader binds the PeerId and address to the exact KES/VRF pair") { implicit sp =>
    for {
      first <- record(0)
      second <- record(1)
      substitutedIdentity = first.copy(peerId = second.peerId, address = second.address)
      result <- L0GenesisLoader.buildOperatorKeyRegistry[IO](data(List(substitutedIdentity))).attempt
    } yield expect(result.swap.exists(_.getMessage.contains("longTermSignature does not bind")))
  }
}
