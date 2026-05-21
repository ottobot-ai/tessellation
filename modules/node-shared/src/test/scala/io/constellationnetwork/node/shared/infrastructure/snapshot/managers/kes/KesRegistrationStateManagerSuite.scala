package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.kes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertAcceptanceResult
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.kesRegistrationRecordSetCodec
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationReferenceImmutableCodec
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for the MPT-backed `KesRegistrationStateManager` (Slice 10 / #179).
  *
  * Covers:
  *
  *   - `materializeFromMpt` returns `None` for an empty MPT (no cert ever written).
  *   - `materializeFromMpt` returns the latest accepted record when present.
  *   - `materializeFromMpt` correctly resolves multi-cert histories via the `LastKesRegistrationRefs` pointer.
  *   - `materializeAllFromMpt` does a clean prefix scan and returns the latest per peer.
  *   - `getUpdatedKesRegistrationCerts` is idempotent on `(peerId, ordinal)` replays.
  *   - `getUpdatedLastRefs` advances pointers for newly-accepted peers and preserves prior pointers for untouched peers.
  *
  * Mirrors the test style of `NodeCollateralStateManager`-adjacent suites (no `NodeCollateralStateManagerSuite` exists in the repo today;
  * closest match is `TokenLockStateManagerSuite`).
  */
object KesRegistrationStateManagerSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO], KeyPair, PeerId)

  override def sharedResource: Resource[IO, Res] = for {
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
    kesMasterVK: Hex = Hex("11" * 32)
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = kesMasterVK,
      kesMasterVKStep = 0,
      offset = 0L,
      effectiveFromEpoch = effectiveFromEpoch,
      ordinal = ordinal,
      parent = parent
    )

  private def mkHarness(
    implicit h: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(GlobalStateReader[IO], MptStore[IO, GlobalStateKey])] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      reader = GlobalStateReader.fromMptStore[IO](store)
    } yield (reader, store)

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

  test("materializeFromMpt returns None for an empty registry") { res =>
    implicit val (j, h, _, _, operatorId) = res
    for {
      (reader, _) <- mkHarness
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.materializeFromMpt(operatorId)
    } yield expect(result.isEmpty)
  }

  test("materializeFromMpt returns the only cert when one is written") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, store) <- mkHarness
      _ <- writeRecord(store, record)
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.materializeFromMpt(operatorId)
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.event.value.ordinal === cert.ordinal),
        result.exists(_.event.value.kesMasterVK == Hex("11" * 32))
      )
  }

  test("materializeFromMpt with multi-cert history resolves to the latest via LastKesRegistrationRefs pointer") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
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
        kesMasterVK = Hex("bb" * 32)
      )
      signed2 <- forAsyncHasher(cert2, kp)
      record1 = KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue)
      record2 = KesRegistrationRecord(signed2, SnapshotOrdinal(NonNegLong(1L)))
      (reader, store) <- mkHarness
      _ <- writeRecord(store, record1)
      _ <- writeRecord(store, record2)
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.materializeFromMpt(operatorId)
    } yield
      expect.all(
        result.isDefined,
        result.exists(_.event.value.ordinal === cert2.ordinal),
        result.exists(_.event.value.kesMasterVK == Hex("bb" * 32))
      )
  }

  test("materializeAllFromMpt returns latest cert per peer via prefix scan") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, store) <- mkHarness
      _ <- writeRecord(store, record)
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.materializeAllFromMpt
    } yield
      expect.all(
        result.size == 1,
        result.contains(operatorId),
        result(operatorId).event.value.ordinal === cert.ordinal
      )
  }

  test("materializeAllFromMpt returns empty SortedMap when MPT has no registered certs") { res =>
    implicit val (j, h, _, _, _) = res
    for {
      (reader, _) <- mkHarness
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.materializeAllFromMpt
    } yield expect(result.isEmpty)
  }

  test("materializeChainForPeer returns full history sorted earliest-first") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
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
      record1 = KesRegistrationRecord(signed1, SnapshotOrdinal.MinValue)
      record2 = KesRegistrationRecord(signed2, SnapshotOrdinal(NonNegLong(1L)))
      (reader, store) <- mkHarness
      _ <- writeRecord(store, record1)
      _ <- writeRecord(store, record2)
      manager = KesRegistrationStateManager.make[IO](reader)
      chain <- manager.materializeChainForPeer(operatorId)
    } yield
      expect.all(
        chain.size == 2,
        chain.headOption.exists(_.event.value.ordinal === cert1.ordinal),
        chain.lastOption.exists(_.event.value.ordinal === cert2.ordinal)
      )
  }

  test("getUpdatedKesRegistrationCerts is idempotent on (peerId, ordinal) replays") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, _) <- mkHarness
      manager = KesRegistrationStateManager.make[IO](reader)
      result1 = manager.getUpdatedKesRegistrationCerts(
        KesRegistrationCertAcceptanceResult(SortedMap(operatorId -> record), Nil),
        SortedMap.empty[PeerId, SortedSet[KesRegistrationRecord]]
      )
      // Replay the same accept against the existing prior state.
      result2 = manager.getUpdatedKesRegistrationCerts(
        KesRegistrationCertAcceptanceResult(SortedMap(operatorId -> record), Nil),
        result1
      )
    } yield
      expect.all(
        result1.size == 1,
        result1(operatorId).size == 1,
        // Idempotency: replay leaves set size unchanged.
        result2(operatorId).size == 1
      )
  }

  test("getUpdatedLastRefs advances accepted pointers and preserves untouched ones") { res =>
    implicit val (j, h, sp, kp, operatorId) = res
    val cert = mkCert(operatorId, EpochProgress(NonNegLong(100L)))
    val priorPeer = PeerId(Hex("ff" * 64))
    val priorRef = KesRegistrationReference(KesRegistrationOrdinal(NonNegLong(7L)), io.constellationnetwork.security.hash.Hash("ab" * 32))
    val priorRefs: SortedMap[PeerId, KesRegistrationReference] = SortedMap(priorPeer -> priorRef)
    for {
      signed <- forAsyncHasher(cert, kp)
      record = KesRegistrationRecord(signed, SnapshotOrdinal.MinValue)
      (reader, _) <- mkHarness
      manager = KesRegistrationStateManager.make[IO](reader)
      result <- manager.getUpdatedLastRefs(
        KesRegistrationCertAcceptanceResult(SortedMap(operatorId -> record), Nil),
        priorRefs
      )
    } yield
      expect.all(
        // Prior pointer for an operator with no new cert is preserved.
        result.get(priorPeer).contains(priorRef),
        // New pointer for the accepted operator matches the cert's ordinal.
        result.get(operatorId).exists(_.ordinal === cert.ordinal)
      )
  }
}
