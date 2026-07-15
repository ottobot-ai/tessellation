package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{CanonicalOperatorConsensusFixture, OperatorConsensusKeyRegistry}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.{OperationalKeyMaker, SecureStore, VerificationKeyKesProduct}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.IOSuite

object LocalOperatorKeyPairGateSuite extends IOSuite {

  override type Res = SecurityProvider[IO]

  override def sharedResource: Resource[IO, Res] = SecurityProvider.forAsync[IO]

  private val KeyName = "kes-sk.bin"

  private def freshKes(seedByte: Byte): IO[(Array[Byte], VerificationKeyKesProduct)] =
    OperationalKeyMaker.generateFreshKesKeyMaterial[IO](Array.fill[Byte](32)(seedByte), height = (2, 2), offset = 0L)

  private def runtimePair(genesis: OperatorConsensusKeys, effectiveFrom: EtaPeriod): OperatorConsensusKeys = {
    val cert = KesRegistrationCert(
      operatorPeerId = genesis.operatorPeerId,
      kesMasterVK = Hex.fromBytes(genesis.kes.vk.value),
      kesMasterVKStep = genesis.kes.vk.step,
      offset = effectiveFrom.value,
      vrfPublicKey = Hex.fromBytes(genesis.vrfPublicKey.toBytes),
      effectiveFromPeriod = effectiveFrom,
      registrationParentHash = Hash("aa" * 32),
      ordinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(1L)),
      parent = KesRegistrationReference.empty
    )
    val proof = SignatureProof(Id(genesis.operatorPeerId.value), Signature(Hex("7f" * 64)))
    val record = KesRegistrationRecord(Signed(cert, NonEmptySet.one(proof)), SnapshotOrdinal.unsafeApply(9L))

    genesis.copy(
      kes = genesis.kes.copy(offset = effectiveFrom.value),
      effectiveFromPeriod = effectiveFrom,
      registration = Some(record)
    )
  }

  test("missing secure-store KES material fails closed and never generates a replacement") { implicit securityProvider =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        store <- SecureStore.inMemory[IO]
        result <- LocalOperatorKeyPairGate
          .loadVerified[IO](
            store,
            KeyName,
            etaPeriodLength = 100L,
            operator.resolvedPair.operatorPeerId,
            operator.localLongTermKeyPairForConsensusTest,
            operator.operatorKeyRegistry
          )
          .use(_ => IO.unit)
          .attempt
        remaining <- store.list
      } yield
        expect(result.left.exists(_.getMessage.contains(s"SecureStore has no entry '$KeyName'"))) &&
          expect(remaining.isEmpty, "the gate must not bootstrap or persist replacement KES material")
    }
  }

  test("loaded KES secret whose master key differs from the preregistered pair is rejected") { implicit securityProvider =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        (localKesBytes, _) <- freshKes(0x22.toByte)
        store <- SecureStore.inMemory[IO]
        _ <- store.write(KeyName, localKesBytes)
        result <- LocalOperatorKeyPairGate
          .loadVerified[IO](
            store,
            KeyName,
            etaPeriodLength = 100L,
            operator.resolvedPair.operatorPeerId,
            operator.localLongTermKeyPairForConsensusTest,
            operator.operatorKeyRegistry
          )
          .use(_ => IO.unit)
          .attempt
      } yield expect(result.left.exists(_.isInstanceOf[LocalOperatorKeyPairGate.KesMasterKeyMismatch]))
    }
  }

  test("locally derived VRF key whose bytes differ from the preregistered pair is rejected") { implicit securityProvider =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        otherKeyPair <- KeyPairGenerator.makeKeyPair[IO]
        (_, wrongVrfVk) = VrfKeyDeriver.deriveVrfKeyPair(otherKeyPair)
        wrongPair = operator.resolvedPair.copy(vrfPublicKey = VrfPublicKey.fromBytes(wrongVrfVk))
        wrongRegistry = OperatorConsensusKeyRegistry.make[IO](Map(wrongPair.operatorPeerId -> wrongPair))
        result <- LocalOperatorKeyPairGate
          .verify[IO](
            operator.kesSigner,
            operator.resolvedPair.operatorPeerId,
            operator.localLongTermKeyPairForConsensusTest,
            wrongRegistry
          )
          .attempt
      } yield expect(result.left.exists(_.isInstanceOf[LocalOperatorKeyPairGate.VrfKeyMismatch]))
    }
  }

  test("exact preregistered PeerId, KES master key, and VRF key produce a tree-relative signing capability") { implicit securityProvider =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      for {
        verified <- LocalOperatorKeyPairGate.verify[IO](
          operator.kesSigner,
          operator.resolvedPair.operatorPeerId,
          operator.localLongTermKeyPairForConsensusTest,
          operator.operatorKeyRegistry
        )
        result = (verified.peerId, verified.treeStepFor(9L), verified.treeStepFor(-1L))
      } yield
        expect.same(operator.resolvedPair.operatorPeerId, result._1) &&
          expect.same(Right(9), result._2) &&
          expect(result._3.left.exists(_.isInstanceOf[LocalOperatorKeyPairGate.InvalidKesPeriod]))
    }
  }

  test("current-registry signing never promotes a runtime record without exact historical resolution") { implicit securityProvider =>
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val runtime = runtimePair(operator.resolvedPair, EtaPeriod(2L))
      val currentRegistry = OperatorConsensusKeyRegistry.make[IO](Map(runtime.operatorPeerId -> runtime))

      LocalOperatorKeyPairGate
        .registeredSigningKey[IO](
          operator.kesSigner,
          runtime.operatorPeerId,
          operator.localLongTermKeyPairForConsensusTest,
          currentRegistry,
          globalEtaPeriod = 2L
        )
        .map(result => expect(result.left.exists(_.isInstanceOf[LocalOperatorKeyPairGate.MissingOperatorRegistration])))
    }
  }

  test("GL0 secure-store directory is mandatory") { _ =>
    IO.pure(
      expect.same(Left(LocalOperatorKeyPairGate.MissingSecureStoreDirectory), LocalOperatorKeyPairGate.requireSecureStoreDirectory(None)) &&
        expect.same(
          Left(LocalOperatorKeyPairGate.MissingSecureStoreDirectory),
          LocalOperatorKeyPairGate.requireSecureStoreDirectory(Some("  "))
        )
    )
  }
}
