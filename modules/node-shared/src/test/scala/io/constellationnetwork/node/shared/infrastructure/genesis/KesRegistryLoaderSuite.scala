package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.IO

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

/** Round-trip tests for `L0GenesisLoader.buildKesRegistry` — the Slice 3 entry point that turns the optional `kesRegistrations` field of an
  * L0 genesis fixture into a runtime `KesRegistry[F]`.
  */
object KesRegistryLoaderSuite extends SimpleIOSuite {

  // 128 hex chars = 64 bytes of fake "PeerId" — matches the PeerId hex length convention.
  private def peerHex(seed: Char): String = (seed.toString * 128).take(128)
  private def peerId(seed: Char): PeerId = Id(Hex(peerHex(seed))).toPeerId

  // 64 hex chars = 32 bytes — matches the default Blake2b-256 VK length used by KesProduct.
  private def vkHex(seed: Char): String = (seed.toString * 64).take(64)

  // Signature hex — arbitrary bytes for the binding test (the loader does not verify the binding at parse time;
  // that's a Slice 9 strict-mode follow-up).
  private val regSigHex: String = "ab" * 71 // 142 hex chars, 71 bytes (typical SHA512withECDSA sig)

  private val meta = L0GenesisMeta(
    generatorVersion = "test",
    generatedAt = "2026-05-16T00:00:00Z",
    invocation = "test",
    seed = 0L,
    expectedProperties = Nil
  )

  private val protocolParams = L0GenesisProtocolParams(
    lddCutoff = 1,
    etaRotationSnapshots = 2550L,
    genesisEta = "00" * 32,
    startingEpochProgress = 0L
  )

  private def baseData(regs: Option[List[L0GenesisKesRegistration]]): L0GenesisData =
    L0GenesisData(
      _meta = meta,
      networkMagic = "test",
      activationOrdinal = 0L,
      startingEpochProgress = 0L,
      protocolParams = protocolParams,
      operators = Nil,
      delegatedStakes = Nil,
      nodeCollaterals = Nil,
      initialBalances = Nil,
      kesRegistrations = regs
    )

  test("buildKesRegistry: empty (None) → registry has no entries") {
    for {
      reg <- L0GenesisLoader.buildKesRegistry[IO](baseData(None))
      lookup <- reg.getKesVk(peerId('a'))
      all <- reg.list
    } yield expect.same(None, lookup) && expect.same(0, all.size)
  }

  test("buildKesRegistry: empty list → registry has no entries") {
    for {
      reg <- L0GenesisLoader.buildKesRegistry[IO](baseData(Some(Nil)))
      lookup <- reg.getKesVk(peerId('a'))
      all <- reg.list
    } yield expect.same(None, lookup) && expect.same(0, all.size)
  }

  test("buildKesRegistry: single registration round-trips through hex decode") {
    val regs = List(
      L0GenesisKesRegistration(
        peerId = peerHex('a'),
        kesVk = vkHex('1'),
        kesVkStep = 0,
        longTermSig = regSigHex,
        offset = 0L
      )
    )
    for {
      reg <- L0GenesisLoader.buildKesRegistry[IO](baseData(Some(regs)))
      lookup <- reg.getKesVk(peerId('a'))
      missing <- reg.getKesVk(peerId('b'))
      all <- reg.list
    } yield
      expect.same(1, all.size) &&
        expect.same(None, missing) &&
        expect(lookup.isDefined) &&
        expect.same(lookup.map(_.vk.step), Some(0)) &&
        expect.same(lookup.map(_.offset), Some(0L)) &&
        expect(lookup.map(_.vk.value.toList) == Some(Hex(vkHex('1')).toBytes.toList))
  }

  test("buildKesRegistry: multiple registrations all land in the registry; duplicate peerId keeps last") {
    val regs = List(
      L0GenesisKesRegistration(peerHex('a'), vkHex('1'), 0, regSigHex, 0L),
      L0GenesisKesRegistration(peerHex('b'), vkHex('2'), 0, regSigHex, 0L),
      L0GenesisKesRegistration(peerHex('c'), vkHex('3'), 0, regSigHex, 0L)
    )
    for {
      reg <- L0GenesisLoader.buildKesRegistry[IO](baseData(Some(regs)))
      all <- reg.list
      a <- reg.getKesVk(peerId('a'))
      b <- reg.getKesVk(peerId('b'))
      c <- reg.getKesVk(peerId('c'))
    } yield
      expect.same(3, all.size) &&
        expect(a.exists(_.vk.value.toList == Hex(vkHex('1')).toBytes.toList)) &&
        expect(b.exists(_.vk.value.toList == Hex(vkHex('2')).toBytes.toList)) &&
        expect(c.exists(_.vk.value.toList == Hex(vkHex('3')).toBytes.toList))
  }

  test("buildKesRegistry: non-zero offset is carried through to the KesRegistryEntry") {
    val regs = List(
      L0GenesisKesRegistration(peerHex('a'), vkHex('1'), 0, regSigHex, offset = 0L),
      L0GenesisKesRegistration(peerHex('b'), vkHex('2'), 0, regSigHex, offset = 42L),
      L0GenesisKesRegistration(peerHex('c'), vkHex('3'), 0, regSigHex, offset = 9999L)
    )
    for {
      reg <- L0GenesisLoader.buildKesRegistry[IO](baseData(Some(regs)))
      a <- reg.getKesVk(peerId('a'))
      b <- reg.getKesVk(peerId('b'))
      c <- reg.getKesVk(peerId('c'))
    } yield
      expect.same(a.map(_.offset), Some(0L)) &&
        expect.same(b.map(_.offset), Some(42L)) &&
        expect.same(c.map(_.offset), Some(9999L))
  }

}
