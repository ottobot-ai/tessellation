package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.SignedValidator
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Acceptance-manager tests for Slice 10 (#179). Validates partitioning, replay-rejection across operators, and chain-link enforcement
  * across multiple certs in one batch.
  */
object KesRegistrationCertAcceptanceManagerSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], KeyPair, PeerId)

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    kp <- KeyPairGenerator.makeKeyPair[IO].asResource
    operatorId = PeerId.fromPublic(kp.getPublic)
  } yield (j, h, sp, kp, operatorId)

  private val futureEpoch: EpochProgress = EpochProgress(NonNegLong(200L))

  private def mkCert(
    operatorId: PeerId,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = Hex("aabbccddeeff"),
      kesMasterVKStep = 0,
      offset = 0L,
      effectiveFromEpoch = futureEpoch,
      ordinal = ordinal,
      parent = parent
    )

  test("acceptance result partitions valid vs invalid certs correctly") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId)
    val validator = KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None)
    val manager = KesRegistrationCertAcceptanceManager.make[IO](validator)
    for {
      signed <- forAsyncHasher(cert, kp)
      result <- manager.accept(
        List(signed),
        SortedMap.empty,
        SortedMap.empty,
        EpochProgress(NonNegLong(100L)),
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.size == 1,
        result.notAccepted.isEmpty,
        result.accepted.contains(operatorId),
        result.accepted(operatorId).event == signed,
        result.accepted(operatorId).acceptedAt == SnapshotOrdinal.MinValue
      )
  }

  test("acceptance rejects a cert whose ordinal is not strictly greater than lastRef") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    // lastRef.ordinal = 2, submitted cert.ordinal = 2 (must be > 2)
    val lastRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(2L)), io.constellationnetwork.security.hash.Hash.empty)
    val cert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)), parent = lastRef)
    val validator = KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None)
    val manager = KesRegistrationCertAcceptanceManager.make[IO](validator)
    for {
      signed <- forAsyncHasher(cert, kp)
      result <- manager.accept(
        List(signed),
        SortedMap(operatorId -> lastRef),
        SortedMap.empty,
        EpochProgress(NonNegLong(100L)),
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 1
      )
  }

  test("acceptance accepts certs from multiple operators in one batch") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      cert1 = mkCert(operatorId)
      cert2 = mkCert(operator2)
      signed1 <- forAsyncHasher(cert1, kp)
      signed2 <- forAsyncHasher(cert2, kp2)
      validator = KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None)
      manager = KesRegistrationCertAcceptanceManager.make[IO](validator)
      result <- manager.accept(
        List(signed1, signed2),
        SortedMap.empty,
        SortedMap.empty,
        EpochProgress(NonNegLong(100L)),
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.size == 2,
        result.accepted.contains(operatorId),
        result.accepted.contains(operator2),
        result.notAccepted.isEmpty
      )
  }
}
