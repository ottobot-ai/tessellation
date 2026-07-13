package io.constellationnetwork.node.shared.domain.nakamoto

import java.security.{KeyPair, MessageDigest}

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.genesis.types._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.infrastructure.genesis.L0GenesisLoader
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.{OperationalKeyMaker, OperationalKeyMakerAlgebra, SecureStore}
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

/** Canonical test-only period-zero operator identity.
  *
  * Construction follows the production genesis-record authority path: one long-term identity signs the complete KES+VRF pair, the genesis
  * loader validates that signed commitment, and the immutable atomic registry is rematerialized through the rooted-record loader. The
  * matching local secrets are available only for producing test artifacts; no split KES or VRF registry is exposed. This helper does not
  * fabricate an enclosing MPT proof; the genesis commitment suites cover MPT insertion and root authentication separately.
  *
  * This fixture models only the rooted period-zero exception. Runtime activation requires exact candidate-parent history and the N-2
  * eligibility view; callers must use [[HistoricalOperatorConsensusKeyRegistry]] tests for that lifecycle.
  */
final class CanonicalOperatorConsensusFixture private (
  val operatorKeyRegistry: OperatorConsensusKeyRegistry[IO],
  val resolvedPair: OperatorConsensusKeys,
  private val vrfSecret: Array[Byte],
  /** Test-only local secret matching the long-term identity that signed the rooted genesis pair. Never use this as registry authority. */
  val localLongTermKeyPairForConsensusTest: KeyPair,
  val kesSigner: OperationalKeyMakerAlgebra[IO]
) {

  /** Defensive copy of the local secret corresponding to `resolvedPair.vrfPublicKey`. */
  def localVrfSecret: Array[Byte] = vrfSecret.clone()

  /** Sign an operative test artifact with the long-term identity that committed `resolvedPair` at genesis. */
  def signWithOperatorIdentity(bytes: Array[Byte])(implicit securityProvider: SecurityProvider[IO]): IO[Array[Byte]] =
    Signing.signData[IO](bytes)(localLongTermKeyPairForConsensusTest.getPrivate)
}

object CanonicalOperatorConsensusFixture {

  private val NetworkMagic = "canonical-operator-consensus-fixture"
  private val KesKeyName = "canonical-operator-consensus-fixture-kes.bin"
  private val KesHeight = (2, 2)
  private val EtaPeriodLength = 100L

  private val Meta = L0GenesisMeta("test", "2026-07-13T00:00:00Z", "test", 0L, Nil)
  private val ProtocolParams = L0GenesisProtocolParams(1, 2550L, "00" * 32, 0L)

  final case class Generated(
    registry: OperatorConsensusKeyRegistry[IO],
    resolvedPair: OperatorConsensusKeys,
    vrfSecret: Array[Byte],
    identityKeyPair: KeyPair,
    encodedKesSecret: Array[Byte]
  )

  def make: Resource[IO, CanonicalOperatorConsensusFixture] =
    SecurityProvider.forAsync[IO].flatMap { implicit securityProvider =>
      for {
        generated <- Resource.eval(generate)
        store <- Resource.eval(SecureStore.inMemory[IO])
        _ <- Resource.eval(store.write(KesKeyName, generated.encodedKesSecret))
        kesSigner <- OperationalKeyMaker.make[IO](store, KesKeyName, EtaPeriodLength)
        localKesVk <- Resource.eval(kesSigner.currentPublicKey)
        _ <- Resource.eval(
          IO.raiseUnless(
            localKesVk.step == generated.resolvedPair.kes.vk.step &&
              MessageDigest.isEqual(localKesVk.value, generated.resolvedPair.kes.vk.value)
          )(new IllegalStateException("fixture KES signer does not match the rooted atomic genesis pair"))
        )
        _ <- Resource.eval(IO(java.util.Arrays.fill(generated.encodedKesSecret, 0.toByte)))
      } yield
        new CanonicalOperatorConsensusFixture(
          generated.registry,
          generated.resolvedPair,
          generated.vrfSecret,
          generated.identityKeyPair,
          kesSigner
        )
    }

  def makePopulation(count: Int): Resource[IO, CanonicalOperatorConsensusPopulation] =
    List
      .fill(count)(make)
      .sequence
      .evalMap { operators =>
        operators
          .traverse(_.operatorKeyRegistry.list)
          .map { entries =>
            val pairs = entries.foldLeft(Map.empty[PeerId, OperatorConsensusKeys])(_ ++ _)
            new CanonicalOperatorConsensusPopulation(operators, OperatorConsensusKeyRegistry.make[IO](pairs))
          }
      }

  private def generate(implicit securityProvider: SecurityProvider[IO]): IO[Generated] =
    KeyPairGenerator.makeKeyPair[IO].flatMap(generateForIdentity)

  /** Build loader-validated, signed period-zero material for an already generated long-term identity. Test helpers that need several
    * independently signing operators use this instead of fabricating `OperatorConsensusKeys` directly.
    */
  def generateForIdentity(identityKeyPair: KeyPair)(implicit securityProvider: SecurityProvider[IO]): IO[Generated] =
    for {
      _ <- IO.unit
      peerId = PeerId.fromPublic(identityKeyPair.getPublic)
      address = identityKeyPair.getPublic.toAddress.value.value
      (vrfSecret, vrfPublicKey) = VrfKeyDeriver.deriveVrfKeyPair(identityKeyPair)
      kesSeed = MessageDigest
        .getInstance("SHA-256")
        .digest(peerId.value.toBytes ++ "canonical-test-kes".getBytes("UTF-8"))
      kesMaterial <- OperationalKeyMaker.generateFreshKesKeyMaterial[IO](kesSeed, KesHeight, offset = 0L)
      (encodedKesSecret, kesMasterVerificationKey) = kesMaterial
      preimage = L0GenesisOperator.signaturePreimage(
        NetworkMagic,
        activationOrdinal = 0L,
        startingEpochProgress = 0L,
        peerId.value.toBytes,
        address,
        kesMasterVerificationKey.value,
        kesMasterVerificationKey.step,
        kesPeriodOffset = 0L,
        vrfPublicKey
      )
      longTermSignature <- Signing.signData[IO](preimage)(identityKeyPair.getPrivate)
      operator = L0GenesisOperator(
        peerId = peerId.value.value,
        address = address,
        kesMasterVk = Hex.fromBytes(kesMasterVerificationKey.value).value,
        kesMasterVkStep = kesMasterVerificationKey.step,
        kesPeriodOffset = 0L,
        vrfVk = Hex.fromBytes(vrfPublicKey).value,
        longTermSignature = Hex.fromBytes(longTermSignature).value
      )
      data = L0GenesisData(
        _meta = Meta,
        networkMagic = NetworkMagic,
        activationOrdinal = 0L,
        startingEpochProgress = 0L,
        protocolParams = ProtocolParams,
        operators = List(operator),
        delegatedStakes = Nil,
        nodeCollaterals = Nil,
        initialBalances = Nil
      )
      genesisRecords <- L0GenesisLoader.buildGenesisOperatorKeys[IO](data)
      registry <- L0GenesisLoader.buildOperatorKeyRegistryFromRooted[IO](genesisRecords)
      resolvedPair <- registry
        .get(peerId)
        .flatMap(IO.fromOption(_)(new IllegalStateException("rooted fixture pair was not materialized")))
      _ <- IO.raiseUnless(
        resolvedPair.operatorPeerId == peerId &&
          resolvedPair.effectiveFromPeriod == EtaPeriod.Zero &&
          resolvedPair.registration.isEmpty &&
          MessageDigest.isEqual(resolvedPair.vrfPublicKey.toBytes, vrfPublicKey) &&
          MessageDigest.isEqual(resolvedPair.kes.vk.value, kesMasterVerificationKey.value)
      )(new IllegalStateException("rooted fixture pair does not match the generated local key material"))
    } yield Generated(registry, resolvedPair, vrfSecret.clone(), identityKeyPair, encodedKesSecret)
}

final class CanonicalOperatorConsensusPopulation private[nakamoto] (
  val operators: List[CanonicalOperatorConsensusFixture],
  val operatorKeyRegistry: OperatorConsensusKeyRegistry[IO]
) {
  val peerIds: Set[PeerId] = operators.iterator.map(_.resolvedPair.operatorPeerId).toSet
}
