package io.constellationnetwork.node.shared.infrastructure.genesis

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

/** Round-trip + determinism-contract tests for `L0GenesisLoader.buildVrfRegistry` — the Slice S1 entry point that turns the per-operator
  * `vrfPublicKey` field of an L0 genesis fixture into a runtime `VrfRegistry[F]`. Mirrors `KesRegistryLoaderSuite`.
  *
  * '''The determinism contract (the load-bearing test).''' The generator populates `L0GenesisOperator.vrfPublicKey` via
  * `VrfKeyDeriver.deriveVrfKeyPair(operatorKeyPair)` — the SAME derivation the gl0 snapshot leader loop applies at runtime
  * (`SnapshotLeaderLoop.deriveVrfKeys`, which now delegates to the same shared helper). This suite generates real secp256k1 operator
  * keypairs, builds genesis operators with `vrfPublicKey` set that way, loads the registry, and asserts each loaded VRF VK byte-equals
  * `VrfKeyDeriver.deriveVrfKeyPair(kp)._2`. If this ever drifts (e.g. a re-implemented EC-scalar normalization), a later slice's
  * `CommitteeSortition.verifyShardMembership` would reject every honest signer — so this byte-identity is the contract that protects it.
  */
object VrfRegistryLoaderSuite extends MutableIOSuite {

  override type Res = SecurityProvider[IO]

  override def sharedResource: Resource[IO, Res] = SecurityProvider.forAsync[IO]

  private val meta = L0GenesisMeta(
    generatorVersion = "test",
    generatedAt = "2026-05-26T00:00:00Z",
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

  private def baseData(operators: List[L0GenesisOperator]): L0GenesisData =
    L0GenesisData(
      _meta = meta,
      networkMagic = "test",
      activationOrdinal = 0L,
      startingEpochProgress = 0L,
      protocolParams = protocolParams,
      operators = operators,
      delegatedStakes = Nil,
      nodeCollaterals = Nil,
      initialBalances = Nil,
      kesRegistrations = None
    )

  /** Build an `L0GenesisOperator` with `vrfPublicKey` populated EXACTLY as the generator does. */
  private def operatorFrom(kp: java.security.KeyPair): L0GenesisOperator = {
    val (_, vrfVk) = VrfKeyDeriver.deriveVrfKeyPair(kp)
    L0GenesisOperator(
      peerId = PeerId.fromPublic(kp.getPublic).value.value,
      address = kp.getPublic.toAddress.value.value,
      vrfPublicKey = Some(Hex.fromBytes(vrfVk).value),
      kesPublicKey = None
    )
  }

  test("buildVrfRegistry: no operators → registry has no entries") { implicit sp =>
    for {
      reg <- L0GenesisLoader.buildVrfRegistry[IO](baseData(Nil))
      all <- reg.list
    } yield expect.same(0, all.size)
  }

  test("buildVrfRegistry: operator with vrfPublicKey = None → not in registry") { implicit sp =>
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      op = operatorFrom(kp).copy(vrfPublicKey = None)
      reg <- L0GenesisLoader.buildVrfRegistry[IO](baseData(List(op)))
      lookup <- reg.getVrfVk(PeerId.fromPublic(kp.getPublic))
      all <- reg.list
    } yield expect.same(None, lookup.map(_.toList)) && expect.same(0, all.size)
  }

  test("buildVrfRegistry: loaded vrfVk byte-equals deriveVrfKeyPair(kp)._2 (DETERMINISM CONTRACT)") { implicit sp =>
    for {
      kps <- (0 until 4).toList.traverse(_ => KeyPairGenerator.makeKeyPair[IO])
      ops = kps.map(operatorFrom)
      reg <- L0GenesisLoader.buildVrfRegistry[IO](baseData(ops))
      all <- reg.list
      checks <- kps.traverse { kp =>
        val expectedVk = VrfKeyDeriver.deriveVrfKeyPair(kp)._2
        reg.getVrfVk(PeerId.fromPublic(kp.getPublic)).map { loaded =>
          expect(loaded.isDefined) &&
          expect(loaded.exists(b => java.util.Arrays.equals(b, expectedVk)))
        }
      }
    } yield checks.foldLeft(expect.same(4, all.size))(_ && _)
  }

  test("buildVrfRegistry: lookup for an unregistered peer returns None") { implicit sp =>
    for {
      kpA <- KeyPairGenerator.makeKeyPair[IO]
      kpB <- KeyPairGenerator.makeKeyPair[IO]
      reg <- L0GenesisLoader.buildVrfRegistry[IO](baseData(List(operatorFrom(kpA))))
      present <- reg.getVrfVk(PeerId.fromPublic(kpA.getPublic))
      missing <- reg.getVrfVk(PeerId.fromPublic(kpB.getPublic))
    } yield expect(present.isDefined) && expect.same(None, missing.map(_.toList))
  }

  test("buildVrfRegistry: malformed vrfPublicKey hex is dropped (peer absent, no boot failure)") { implicit sp =>
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      op = operatorFrom(kp).copy(vrfPublicKey = Some("zz-not-hex"))
      reg <- L0GenesisLoader.buildVrfRegistry[IO](baseData(List(op)))
      lookup <- reg.getVrfVk(PeerId.fromPublic(kp.getPublic))
      all <- reg.list
    } yield expect.same(None, lookup.map(_.toList)) && expect.same(0, all.size)
  }

  test("buildVrfRegistry: empty registry default (VrfRegistry.empty) returns None for any peer") { implicit sp =>
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      reg = io.constellationnetwork.node.shared.domain.nakamoto.VrfRegistry.empty[IO]
      lookup <- reg.getVrfVk(PeerId.fromPublic(kp.getPublic))
    } yield expect.same(None, lookup.map(_.toList))
  }
}
