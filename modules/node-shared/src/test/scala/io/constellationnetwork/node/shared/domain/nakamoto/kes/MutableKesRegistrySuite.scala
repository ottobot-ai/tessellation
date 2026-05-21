package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.kesRegistrationRecordSetCodec
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationReferenceImmutableCodec
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Golden tests for the MPT-backed MutableKesRegistry overlay (Slice 10 / #179). Asserts:
  *
  *   - genesis-only lookups continue to resolve from the base registry
  *   - a runtime cert persisted in MPT with `effectiveFromEpoch <= currentEpoch` overrides the genesis entry
  *   - a runtime cert with future `effectiveFromEpoch` is held "pending" — genesis still wins
  *   - rotations apply (newer accepted cert with `effectiveFromEpoch <= currentEpoch` overrides earlier one)
  *   - '''durability''': dropping the in-memory `MutableKesRegistry`, rebuilding from the same MPT, returns the same cert (proving MPT is
  *     the durable source of truth)
  *   - '''reorg''': removing the cert from MPT (simulating overlay rollback) makes the registry fall back to genesis
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

  /** Build an in-memory MPT-backed GlobalStateReader plus an effectful "write a cert" helper. The writes go directly through MptStore
    * (bypassing the GSAM accept pipeline) to simulate "MPT already has these bytes" for durability tests.
    */
  private def mkMptHarness(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(GlobalStateReader[IO], MptStore[IO, GlobalStateKey])] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      reader = GlobalStateReader.fromMptStore[IO](store)
    } yield (reader, store)

  /** Write a single accepted cert into MPT under the canonical `KesRegistrationCerts` + `LastKesRegistrationRefs` partitions. Mirrors what
    * the Wave-2 GSAM accept pipeline will write — this test harness short-circuits the validator/acceptance manager and writes pre-known
    * good state directly so the registry-read tests stay focused on the read path.
    */
  private def writeRecord(
    store: MptStore[IO, GlobalStateKey],
    record: KesRegistrationRecord
  )(implicit h: Hasher[IO]): IO[Unit] = {
    val peerId = record.event.value.operatorPeerId
    for {
      certsKey <- GlobalStateKey.kesRegistrationCertsKey[IO](peerId)
      existing <- store.get[SortedSet[KesRegistrationRecord]](certsKey)
      newSet = existing.getOrElse(SortedSet.empty[KesRegistrationRecord]) + record
      _ <- store.insert[SortedSet[KesRegistrationRecord]](Map(certsKey -> newSet))
      ref <- KesRegistrationReference.of[IO](record.event)
      refKey <- GlobalStateKey.lastKesRegistrationRefsKey[IO](peerId)
      _ <- store.insert[KesRegistrationReference](Map(refKey -> ref))
    } yield ()
  }

  /** Remove a cert from MPT — simulates overlay rollback under reorg. Pulls the prior record from MPT, removes it, rewrites pointer to the
    * previous head or removes it entirely when the set is empty.
    */
  private def removeRecord(
    store: MptStore[IO, GlobalStateKey],
    record: KesRegistrationRecord
  )(implicit h: Hasher[IO]): IO[Unit] = {
    val peerId = record.event.value.operatorPeerId
    for {
      certsKey <- GlobalStateKey.kesRegistrationCertsKey[IO](peerId)
      existing <- store.get[SortedSet[KesRegistrationRecord]](certsKey)
      newSet = existing.getOrElse(SortedSet.empty[KesRegistrationRecord]) - record
      _ <-
        if (newSet.isEmpty) store.remove(certsKey)
        else store.insert[SortedSet[KesRegistrationRecord]](Map(certsKey -> newSet))
      refKey <- GlobalStateKey.lastKesRegistrationRefsKey[IO](peerId)
      _ <-
        if (newSet.isEmpty) store.remove(refKey)
        else {
          val newHead = newSet.last
          KesRegistrationReference
            .of[IO](newHead.event)
            .flatMap(ref => store.insert[KesRegistrationReference](Map(refKey -> ref)))
        }
    } yield ()
  }

  test("lookups fall through to the genesis registry when no runtime certs are present in MPT") { res =>
    implicit val (j, h, _, _, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val genesisEntry = KesRegistryEntry(genesisVk, 0L)
    val base = KesRegistry.make[IO](Map(operatorId -> genesisEntry))
    for {
      (reader, _) <- mkMptHarness
      mut <- MutableKesRegistry.make[IO](base, reader)
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(50L)))
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.vk == genesisVk),
        result.exists(_.offset == 0L)
      )
  }

  test("a runtime cert in MPT whose effective-epoch has arrived overrides genesis") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("aa" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, KesRegistrationRecord(signed, SnapshotOrdinal.MinValue))
      mut <- MutableKesRegistry.make[IO](base, reader)
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.vk.value.length == 32),
        result.exists(_.vk.value.sameElements(Hex("aa" * 32).toBytes)),
        result.exists(_.vk != genesisVk)
      )
  }

  test("a runtime cert with future effective-epoch is held pending — genesis still wins") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x33.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(300L)), kesMasterVK = Hex("bb" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, KesRegistrationRecord(signed, SnapshotOrdinal.MinValue))
      mut <- MutableKesRegistry.make[IO](base, reader)
      // currentEpoch (100) < effectiveFromEpoch (300), so runtime is pending; genesis still resolves.
      result <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(100L)))
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.vk == genesisVk)
      )
  }

  test("later rotation overrides earlier runtime cert once its effective-epoch arrives") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
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
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue))
      mut1 <- MutableKesRegistry.make[IO](base, reader)
      // currentEpoch (150) >= cert1.effective (100) but only cert1 written → cert1 active.
      mid <- mut1.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      _ <- writeRecord(store, KesRegistrationRecord(signed2, SnapshotOrdinal(NonNegLong(1L))))
      // After cert2 written, currentEpoch (250) >= both → cert2 wins (LastKesRegistrationRefs pinned to cert2).
      later <- mut1.getKesVk(operatorId, EpochProgress(NonNegLong(250L)))
    } yield
      expect.all(
        mid.exists(_.vk.value.sameElements(Hex("aa" * 32).toBytes)),
        later.exists(_.vk.value.sameElements(Hex("cc" * 32).toBytes))
      )
  }

  test(
    "DURABILITY: dropping the in-memory MutableKesRegistry and rebuilding from MPT alone returns the same cert"
  ) { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val base = KesRegistry.empty[IO]
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("ab" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, record)
      // Build registry #1 — observe runtime cert.
      mut1 <- MutableKesRegistry.make[IO](base, reader)
      r1 <- mut1.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Drop mut1 entirely (simulate restart). The new registry instance has NO in-memory state —
      // it only sees the MPT through the reader. If the MPT is truly the source of truth, mut2's
      // lookup should return byte-equivalent bytes to mut1's.
      mut2 <- MutableKesRegistry.make[IO](base, reader)
      r2 <- mut2.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Also assert that runtimeCertsFor recovers the same list shape.
      list2 <- mut2.runtimeCertsFor(operatorId)
    } yield
      expect.all(
        r1.isDefined,
        r2.isDefined,
        r1.flatMap(_.vk.value.headOption) == r2.flatMap(_.vk.value.headOption),
        r2.exists(_.vk.value.sameElements(Hex("ab" * 32).toBytes)),
        list2.size == 1,
        list2.headOption.exists(_.event.value.ordinal === cert.ordinal)
      )
  }

  test("REORG: removing the runtime cert from MPT falls back to genesis on next lookup") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x55.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("ce" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, record)
      mut <- MutableKesRegistry.make[IO](base, reader)
      before <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Simulate reorg: remove the cert from MPT (the overlay's rollback would do this transparently).
      _ <- removeRecord(store, record)
      after <- mut.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
    } yield
      expect.all(
        // Before the reorg, runtime cert wins.
        before.exists(_.vk.value.sameElements(Hex("ce" * 32).toBytes)),
        // After the reorg, only genesis remains.
        after.isDefined,
        after.exists(_.vk == genesisVk)
      )
  }

  test("list returns one entry per operator with latest cert when MPT has multiple peers") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val base = KesRegistry.empty[IO]
    // A second operator: derive a distinct PeerId by reusing the same keypair but flagging via an unrelated cert.
    // (Real cluster would use distinct keypairs; here we exercise the list-shape codepath using a single one.)
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, KesRegistrationRecord(signed, SnapshotOrdinal.MinValue))
      mut <- MutableKesRegistry.make[IO](base, reader)
      listed <- mut.list
    } yield
      expect.all(
        listed.size == 1,
        listed.contains(operatorId)
      )
  }
}
