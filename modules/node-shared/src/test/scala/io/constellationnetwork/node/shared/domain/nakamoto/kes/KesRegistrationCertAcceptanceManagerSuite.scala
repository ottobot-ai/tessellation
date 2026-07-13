package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.RegistrationEvaluationContext
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
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

  private val registrationParentHash = Hash("aa" * 32)
  private val futurePeriod: EtaPeriod = EtaPeriod(200L)
  private val context = RegistrationEvaluationContext(registrationParentHash, EtaPeriod(100L))

  private def mkCert(
    operatorId: PeerId,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    kesMasterVK: Hex = Hex("11" * KesRegistrationCertValidator.KesMasterVerificationKeyLength),
    vrfPublicKey: Hex = Hex("22" * 32)
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = kesMasterVK,
      kesMasterVKStep = 0,
      offset = futurePeriod.value,
      vrfPublicKey = vrfPublicKey,
      effectiveFromPeriod = futurePeriod,
      registrationParentHash = registrationParentHash,
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
        RegisteredConsensusKeyOwnership.empty,
        context,
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

  test("acceptance rejects a cert whose ordinal is not the exact next ordinal") { res =>
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
        RegisteredConsensusKeyOwnership.empty,
        context,
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 1
      )
  }

  test("acceptance rejects every conflicting same-operator candidate before map conversion") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val cert1 = mkCert(operatorId)
    val cert2 = cert1.copy(vrfPublicKey = Hex("33" * 32))
    val manager = KesRegistrationCertAcceptanceManager.make[IO](
      KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None)
    )
    for {
      signed1 <- forAsyncHasher(cert1, kp)
      signed2 <- forAsyncHasher(cert2, kp)
      result <- manager.accept(
        List(signed1, signed2),
        SortedMap.empty,
        SortedMap.empty,
        RegisteredConsensusKeyOwnership.empty,
        context,
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 2,
        result.notAccepted.forall(_._2.exists {
          case _: KesRegistrationCertValidator.ConflictingOperatorRegistrations => true
          case _                                                                => false
        })
      )
  }

  test("acceptance accepts certs from multiple operators in one batch") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      cert1 = mkCert(operatorId)
      cert2 = mkCert(operator2, kesMasterVK = Hex("33" * 32), vrfPublicKey = Hex("44" * 32))
      signed1 <- forAsyncHasher(cert1, kp)
      signed2 <- forAsyncHasher(cert2, kp2)
      validator = KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None)
      manager = KesRegistrationCertAcceptanceManager.make[IO](validator)
      result <- manager.accept(
        List(signed1, signed2),
        SortedMap.empty,
        SortedMap.empty,
        RegisteredConsensusKeyOwnership.empty,
        context,
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

  test("rejects a KES key already anchored to another operator, comparing decoded bytes") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      ownership <- IO.fromEither(
        RegisteredConsensusKeyOwnership
          .fromState(
            List(RegisteredConsensusKeyClaim(operatorId, Hex("ab" * 32), Hex("22" * 32))),
            SortedMap.empty
          )
          .leftMap(errors => new IllegalStateException(errors.toList.mkString(",")))
      )
      candidate = mkCert(operator2, kesMasterVK = Hex("AB" * 32), vrfPublicKey = Hex("33" * 32))
      signed <- forAsyncHasher(candidate, kp2)
      manager = KesRegistrationCertAcceptanceManager.make[IO](KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None))
      result <- manager.accept(
        List(signed),
        SortedMap.empty,
        SortedMap.empty,
        ownership,
        context,
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 1,
        result.notAccepted.head._2.exists {
          case KesRegistrationCertValidator.KesKeyAlreadyRegistered(_, `operator2`, owners) => owners == List(operatorId)
          case _                                                                            => false
        }
      )
  }

  test("rejects a VRF key already present in retained runtime history for another operator") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      priorSigned <- forAsyncHasher(mkCert(operatorId), kp)
      priorRecord = KesRegistrationRecord(priorSigned, SnapshotOrdinal.MinValue)
      ownership <- IO.fromEither(
        RegisteredConsensusKeyOwnership
          .fromState(Nil, SortedMap(operatorId -> SortedSet(priorRecord)))
          .leftMap(errors => new IllegalStateException(errors.toList.mkString(",")))
      )
      candidate = mkCert(operator2, kesMasterVK = Hex("33" * 32), vrfPublicKey = Hex("22" * 32))
      signed <- forAsyncHasher(candidate, kp2)
      manager = KesRegistrationCertAcceptanceManager.make[IO](KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None))
      result <- manager.accept(
        List(signed),
        SortedMap.empty,
        SortedMap.empty,
        ownership,
        context,
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 1,
        result.notAccepted.head._2.exists {
          case KesRegistrationCertValidator.VrfKeyAlreadyRegistered(_, `operator2`, owners) => owners == List(operatorId)
          case _                                                                            => false
        }
      )
  }

  test("rejects every operator involved in same-batch KES or VRF reuse") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      cert1 = mkCert(operatorId, kesMasterVK = Hex("55" * 32), vrfPublicKey = Hex("66" * 32))
      cert2 = mkCert(operator2, kesMasterVK = Hex("55" * 32), vrfPublicKey = Hex("66" * 32))
      signed1 <- forAsyncHasher(cert1, kp)
      signed2 <- forAsyncHasher(cert2, kp2)
      manager = KesRegistrationCertAcceptanceManager.make[IO](KesRegistrationCertValidator.make[IO](SignedValidator.make[IO], None))
      result <- manager.accept(
        List(signed1, signed2),
        SortedMap.empty,
        SortedMap.empty,
        RegisteredConsensusKeyOwnership.empty,
        context,
        SnapshotOrdinal.MinValue
      )
    } yield
      expect.all(
        result.accepted.isEmpty,
        result.notAccepted.size == 2,
        result.notAccepted.forall {
          case (_, errors) =>
            errors.exists(_.isInstanceOf[KesRegistrationCertValidator.KesKeyAlreadyRegistered]) &&
            errors.exists(_.isInstanceOf[KesRegistrationCertValidator.VrfKeyAlreadyRegistered])
        }
      )
  }

  test("ownership construction fails closed on cross-operator historical collisions and mis-keyed MPT records") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    for {
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      operator2 = PeerId.fromPublic(kp2.getPublic)
      signed1 <- forAsyncHasher(mkCert(operatorId), kp)
      signed2 <- forAsyncHasher(mkCert(operator2), kp2)
      colliding = RegisteredConsensusKeyOwnership.fromState(
        Nil,
        SortedMap(
          operatorId -> SortedSet(KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue)),
          operator2 -> SortedSet(KesRegistrationRecord(signed2, SnapshotOrdinal.MinValue))
        )
      )
      misKeyed = RegisteredConsensusKeyOwnership.fromState(
        Nil,
        SortedMap(operator2 -> SortedSet(KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue)))
      )
    } yield
      expect.all(
        colliding.left.exists(errors =>
          errors.exists(_.isInstanceOf[ConflictingRegisteredKesKeyOwners]) &&
            errors.exists(_.isInstanceOf[ConflictingRegisteredVrfKeyOwners])
        ),
        misKeyed.left.exists(_.exists(_.isInstanceOf[RegistrationStateOperatorMismatch]))
      )
  }
}
