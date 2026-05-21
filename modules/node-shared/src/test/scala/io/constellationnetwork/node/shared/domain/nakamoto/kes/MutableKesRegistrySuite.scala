package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Golden tests for the MutableKesRegistry overlay (Slice 10 / #179). Asserts:
  *
  *   - genesis-only lookups continue to resolve from the base registry
  *   - a runtime cert with `effectiveFromEpoch <= currentEpoch` overrides the genesis entry
  *   - a runtime cert is held as "pending" while `currentEpoch < effectiveFromEpoch`
  *   - rotations apply (newer accepted cert with `effectiveFromEpoch <= currentEpoch` overrides earlier one)
  *   - replay of the same cert (same operator + ordinal) is a no-op
  */
object MutableKesRegistrySuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], KeyPair, PeerId)

  def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
    kp <- KeyPairGenerator.makeKeyPair[IO].asResource
    operatorId = PeerId.fromPublic(kp.getPublic)
  } yield (j, h, sp, kp, operatorId)

  private def mkCert(
    operatorId: PeerId,
    effectiveFromEpoch: EpochProgress,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    kesMasterVK: Hex = Hex("11" * 32),
    offset: Long = 0L
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = kesMasterVK,
      kesMasterVKStep = 0,
      offset = offset,
      effectiveFromEpoch = effectiveFromEpoch,
      ordinal = ordinal,
      parent = parent
    )

  test("lookups fall through to the genesis registry when no runtime certs are present") { res =>
    implicit val (_, _, _, _, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val genesisEntry = KesRegistryEntry(genesisVk, 0L)
    val base = KesRegistry.make[IO](Map(operatorId -> genesisEntry))
    for {
      mut <- MutableKesRegistry.make[IO](base)
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(50L)))
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.vk == genesisVk),
        result.exists(_.offset == 0L)
      )
  }

  test("a runtime cert whose effective-epoch has arrived overrides genesis") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("aa" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      mut <- MutableKesRegistry.make[IO](base)
      _ <- mut.applyAccepted(SortedMap(operatorId -> KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)))
      // currentEpoch (150) >= effectiveFromEpoch (100), so runtime entry wins.
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
    } yield
      expect.all(
        result.isDefined,
        // Runtime VK is non-empty and equals the cert's bytes
        result.exists(_.vk.value.length == 32),
        result.exists(_.vk.value.sameElements(Hex("aa" * 32).toBytes)),
        result.exists(_.vk != genesisVk)
      )
  }

  test("a runtime cert with future effective-epoch is held pending — genesis still wins") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(300L)), kesMasterVK = Hex("bb" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      mut <- MutableKesRegistry.make[IO](base)
      _ <- mut.applyAccepted(SortedMap(operatorId -> KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)))
      // currentEpoch (100) < effectiveFromEpoch (300), so runtime is pending; genesis still resolves.
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(100L)))
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.vk == genesisVk)
      )
  }

  test("later rotation overrides earlier runtime cert once its effective-epoch arrives") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val base = KesRegistry.empty[IO]
    val cert1 = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("aa" * 32))
    for {
      signed1 <- forAsyncHasher(cert1, kp)
      hashed1 <- signed1.toHashed
      ref1 = KesRegistrationReference.of(hashed1)
      cert2 = mkCert(
        operatorId,
        EpochProgress(NonNegLong(200L)),
        ordinal = KesRegistrationOrdinal(NonNegLong(2L)),
        parent = ref1,
        kesMasterVK = Hex("cc" * 32)
      )
      signed2 <- forAsyncHasher(cert2, kp)
      mut <- MutableKesRegistry.make[IO](base)
      _ <- mut.applyAccepted(SortedMap(operatorId -> KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue)))
      _ <- mut.applyAccepted(SortedMap(operatorId -> KesRegistrationRecord(signed2, SnapshotOrdinal.MinValue)))
      // currentEpoch (150) >= cert1.effective (100) but < cert2.effective (200) → cert1 active
      mid <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // currentEpoch (250) >= both → cert2 active (higher ordinal)
      later <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(250L)))
    } yield
      expect.all(
        mid.exists(_.vk.value.sameElements(Hex("aa" * 32).toBytes)),
        later.exists(_.vk.value.sameElements(Hex("cc" * 32).toBytes))
      )
  }

  test("replay of the same (operator, ordinal) is a no-op") { res =>
    implicit val (_, h, sp, kp, operatorId) = res
    val base = KesRegistry.empty[IO]
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      mut <- MutableKesRegistry.make[IO](base)
      _ <- mut.applyAccepted(SortedMap(operatorId -> record))
      _ <- mut.applyAccepted(SortedMap(operatorId -> record))
      certs <- mut.runtimeCertsFor(operatorId)
    } yield expect.same(1, certs.size)
  }
}
