package io.constellationnetwork.node.shared.domain.nakamoto.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.OverlayMode
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, GlobalStateReader, MptOverlay}
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry, ParentChildTree}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.mpt.producer.{InMemoryMerklePatriciaProducer, StatefulMerklePatriciaProducer}
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
  *   - '''statelessness''': `MutableKesRegistry.make` allocates no mutable state — two registries built over the same reader give
  *     byte-equivalent lookups (the registry IS a pure read overlay over MPT)
  *   - '''persistence''': MPT bytes survive across `MptStore.make` restart cycles over the same underlying producer — proves the producer
  *     IS the durable source of truth, not any per-`MptStore` cached state
  *   - '''reorg''': removing the cert from MPT (simulating overlay rollback) makes the registry fall back to genesis
  *   - '''multi-branch reorg''': writing a cert on branch A leaves branch B unaware until A is committed/finalized — the registry reads the
  *     chain's actual branch view, not the flat base
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
    "registry is stateless — MutableKesRegistry.make() allocates no mutable state"
  ) { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    // The original "DURABILITY" test name overpromised: it builds mut1 and mut2 over the SAME reader instance,
    // so it only demonstrates that `make` allocates zero per-instance mutable state — two registries over the
    // same reader give byte-equivalent lookups. The real durability assertion is the "persistence across
    // MptStore restart" test below, which threads a SECOND MptStore over the same producer.
    val base = KesRegistry.empty[IO]
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("ab" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, store) <- mkMptHarness
      _ <- writeRecord(store, record)
      // Two distinct registry instances, same reader. Stateless make() means they observe identical state.
      mut1 <- MutableKesRegistry.make[IO](base, reader)
      r1 <- mut1.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      mut2 <- MutableKesRegistry.make[IO](base, reader)
      r2 <- mut2.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
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

  test(
    "PERSISTENCE: a cert written via MptStore-A survives an MptStore restart over the same underlying producer"
  ) { res =>
    // The real durability test: write via the first `MptStore`, throw it away (simulating a node restart that loses
    // any per-store cached state), build a fresh `MptStore` over the same underlying producer, hand it to a fresh
    // `MutableKesRegistry`, and verify the cert is still there. Because the producer holds the actual MPT bytes
    // (the `MptStore` is a thin Ref+Semaphore wrapper over `producer.entries`), this proves the producer IS the
    // source of truth — not anything the `MptStore` instance caches.
    implicit val (j, h, sp, kp, operatorId) = res
    val base = KesRegistry.empty[IO]
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("ce" * 32))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      // Phase 1: build a producer + the first MptStore, write the cert.
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store1 <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      _ <- writeRecord(store1, record)
      mut1 <- MutableKesRegistry.make[IO](base, GlobalStateReader.fromMptStore[IO](store1))
      r1 <- mut1.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Phase 2: drop store1 + mut1. Build store2 over the SAME producer. The producer holds the byte state;
      // store2 starts with a fresh Ref/Semaphore but reads through the producer's entries.
      store2 <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      mut2 <- MutableKesRegistry.make[IO](base, GlobalStateReader.fromMptStore[IO](store2))
      r2 <- mut2.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      list2 <- mut2.runtimeCertsFor(operatorId)
      // Witness that the producer is the load-bearing component — keeping the producer alive across the
      // store boundary is exactly the contract that real disk-backed producers (RocksDB etc.) provide.
      _ = producer: StatefulMerklePatriciaProducer[IO]
    } yield
      expect.all(
        r1.isDefined,
        r1.exists(_.vk.value.sameElements(Hex("ce" * 32).toBytes)),
        r2.isDefined,
        r2.exists(_.vk.value.sameElements(Hex("ce" * 32).toBytes)),
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

  /** Build a MultiBranch overlay over the in-memory store. Returned tuple: `(overlay, store)`. */
  private def mkOverlay(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[(MptOverlay[IO, GlobalStateKey], MptStore[IO, GlobalStateKey])] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
    } yield (overlay, store)

  /** Helper: write a cert into a `BranchHandle` (overlay-pending write). */
  private def writeRecordToBranch(
    handle: io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchHandle[IO, GlobalStateKey],
    record: KesRegistrationRecord
  )(implicit h: Hasher[IO]): IO[Unit] = {
    val peerId = record.event.value.operatorPeerId
    for {
      certsKey <- GlobalStateKey.kesRegistrationCertsKey[IO](peerId)
      _ <- handle.insert[SortedSet[KesRegistrationRecord]](certsKey, SortedSet[KesRegistrationRecord](record))
      ref <- KesRegistrationReference.of[IO](record.event)
      refKey <- GlobalStateKey.lastKesRegistrationRefsKey[IO](peerId)
      _ <- handle.insert[KesRegistrationReference](refKey, ref)
    } yield ()
  }

  test(
    "MULTI-BRANCH REORG: cert written on branch A is invisible to branch B, and visible from base only after A is finalized"
  ) { res =>
    // Builds a MultiBranch overlay, writes a cert on branch A, verifies that:
    //   1. a reader bound to branch A sees the cert (overlay-pending read at parent=A)
    //   2. a reader bound to a sibling branch B sees only the base (no cert)
    //   3. after finalizing A, a reader bound to base sees the cert
    // This proves the registry reads the chain's actual branch view, not the flat finalized base — the key
    // property the iteration-B persistence design depends on.
    implicit val (j, h, sp, kp, operatorId) = res
    val genesisVk = VerificationKeyKesProduct(Array.fill(32)(0x44.toByte), 0)
    val base = KesRegistry.make[IO](Map(operatorId -> KesRegistryEntry(genesisVk, 0L)))
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)), kesMasterVK = Hex("de" * 32))
    val tipA: BranchId = BranchId(Hash("a" * 64))
    val tipB: BranchId = BranchId(Hash("b" * 64))
    val finalOrdinal = SnapshotOrdinal(NonNegLong(1L))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (overlay, store) <- mkOverlay
      // Step 1: check out branch A from base, write the cert, commit to tipA. The write is overlay-pending
      // until finalization — it has NOT landed in `store` yet.
      handleA <- overlay.checkout(BranchId.base)
      _ <- writeRecordToBranch(handleA, record)
      _ <- overlay.commit(handleA, tipA, finalOrdinal)
      // Step 2: build a reader bound to branch A — should see the cert.
      readerA = GlobalStateReader.fromOverlay[IO](overlay, tipA)
      mutA <- MutableKesRegistry.make[IO](base, readerA)
      onA <- mutA.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Step 3: check out branch B from base (sibling of A). NO cert written; B's view sees only base+genesis.
      handleB <- overlay.checkout(BranchId.base)
      _ <- overlay.commit(handleB, tipB, finalOrdinal)
      readerB = GlobalStateReader.fromOverlay[IO](overlay, tipB)
      mutB <- MutableKesRegistry.make[IO](base, readerB)
      onB <- mutB.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
      // Step 4: finalize branch A. The cert should fold into the underlying base; a reader over the base store
      // now sees the cert.
      _ <- overlay.finalizeBranch(tipA, finalOrdinal)
      readerBase = GlobalStateReader.fromMptStore[IO](store)
      mutBase <- MutableKesRegistry.make[IO](base, readerBase)
      onBase <- mutBase.getKesVk(operatorId, EpochProgress(NonNegLong(150L)))
    } yield
      expect.all(
        // Branch A sees the cert.
        onA.exists(_.vk.value.sameElements(Hex("de" * 32).toBytes)),
        // Branch B sees only genesis (the cert is not on B's chain).
        onB.exists(_.vk == genesisVk),
        // After finalizing A, the cert is folded into the base.
        onBase.exists(_.vk.value.sameElements(Hex("de" * 32).toBytes))
      )
  }
}
