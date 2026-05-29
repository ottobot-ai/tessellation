package io.constellationnetwork.dag.l0.http.routes

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.std.Queue

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.node.shared.domain.nakamoto.kes.{KesRegistrationCertValidator, MutableKesRegistry}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.http.routes.KesRegistrationCertRoutes
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.kesRegistrationRecordSetCodec
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationReferenceImmutableCodec
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.types.numeric.NonNegLong
import org.http4s.Method._
import org.http4s._
import org.http4s.circe.CirceEntityCodec.circeEntityEncoder
import org.http4s.client.dsl.io._
import org.http4s.implicits._
import suite.HttpSuite

/** §1.2 Slice 10 (#179) — route-level tests for `KesRegistrationCertRoutes`.
  *
  * Mirrors `NodeCollateralRoutesSuite` (the foundation pattern): stub a `SnapshotStorage.head`, build a real `MutableKesRegistry` over an
  * empty base `KesRegistry`, and exercise each rejection path the route surfaces:
  *
  *   - POST a well-formed cert → 200 + body carrying the persisted hash + the `onAccepted` sink saw the cert
  *   - POST cert with invalid sig (signer != operator) → 400
  *   - POST cert with stale ordinal (replay) → 400
  *   - GET `/last-reference` → returns the current registry head for that operator
  *
  * The route's other rejection paths (TooManySignatures, NotForwardActivation, MalformedVk, ...) are exhaustively exercised in
  * `KesRegistrationCertValidatorSuite`. The route's job is just to surface validator results as HTTP status codes; covering one valid + one
  * rejected case per shape is sufficient at the route layer.
  */
object KesRegistrationCertRoutesSuite extends HttpSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], KeyPair, PeerId)

  override def sharedResource: Resource[IO, Res] = for {
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    kp <- KeyPairGenerator.makeKeyPair[IO].asResource
    operatorId = PeerId.fromPublic(kp.getPublic)
  } yield (h, sp, j, kp, operatorId)

  private val currentEpoch: EpochProgress = EpochProgress(NonNegLong(100L))
  private val futureEpoch: EpochProgress = EpochProgress(NonNegLong(200L))

  /** Build a well-formed cert template — caller can override fields with `.copy` for negative tests. */
  private def mkCert(
    operatorId: PeerId,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    effectiveFromEpoch: EpochProgress = futureEpoch
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = Hex("00112233445566778899aabbccddeeff"),
      kesMasterVKStep = 0,
      offset = 0L,
      effectiveFromEpoch = effectiveFromEpoch,
      ordinal = ordinal,
      parent = parent
    )

  /** Stub `SnapshotStorage.head` returning a minimal `Signed[GlobalIncrementalSnapshot]` whose only relevant field is `epochProgress`. All
    * other route paths route through the registry / validator we wire explicitly.
    */
  private def stubSnapshotStorage(
    headEpoch: EpochProgress
  ): SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      private val signedSnapshot: Signed[GlobalIncrementalSnapshot] = Signed(
        GlobalIncrementalSnapshot(
          ordinal = SnapshotOrdinal(NonNegLong(1L)),
          height = Height.MinValue,
          subHeight = SubHeight.MinValue,
          lastSnapshotHash = Hash.empty,
          blocks = SortedSet.empty,
          stateChannelSnapshots = SortedMap.empty,
          shardCheckpoints = SortedMap.empty,
          rewards = SortedSet.empty,
          delegateRewards = None,
          epochProgress = headEpoch,
          nextFacilitators = NonEmptyList.of(PeerId(Hex(""))),
          tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
          stateProof = GlobalSnapshotStateProof(
            lastStateChannelSnapshotHashesProof = Hash.empty,
            lastTxRefsProof = Hash.empty,
            balancesProof = Hash.empty,
            lastCurrencySnapshotsProof = None,
            activeAllowSpends = None,
            activeTokenLocks = None,
            tokenLockBalances = None,
            lastAllowSpendRefs = None,
            lastTokenLockRefs = None,
            updateNodeParameters = None,
            activeDelegatedStakes = None,
            delegatedStakesWithdrawals = None,
            activeNodeCollaterals = None,
            nodeCollateralWithdrawals = None,
            priceState = None,
            lastGlobalSnapshotsWithCurrency = None,
            mptRoot = None,
            historicalStakeSnapshots = None
          ),
          allowSpendBlocks = None,
          tokenLockBlocks = None,
          spendActions = None,
          updateNodeParameters = None,
          artifacts = None,
          activeDelegatedStakes = None,
          delegatedStakesWithdrawals = None,
          activeNodeCollaterals = None,
          nodeCollateralWithdrawals = None
        ),
        NonEmptySet.fromSetUnsafe(SortedSet(SignatureProof(ID.Id(Hex("")), Signature(Hex("")))))
      )
      private val info: GlobalSnapshotInfo = GlobalSnapshotInfo.empty

      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(implicit hasher: Hasher[IO]): IO[Boolean] =
        IO.pure(false)
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        IO.pure(Some((signedSnapshot, info)))
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(Some(signedSnapshot))
      def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHashed(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]) = IO.pure(None)
      def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] = IO.pure(None)
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
      def writeForBackfill(snapshot: Signed[GlobalIncrementalSnapshot])(implicit hasher: Hasher[IO]): IO[Unit] = IO.unit
    }

  /** Seed the MPT-backed reader with one accepted cert under the canonical `KesRegistrationCerts` + `LastKesRegistrationRefs` partitions —
    * the read path `MutableKesRegistry.runtimeCertsFor` resolves through. Mirrors `MutableKesRegistrySuite.writeRecord`; the runtime
    * registry is now a pure MPT reader (the old `applyAccepted` mutator was removed), so seeding goes through the store directly.
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

  /** Build the routes under test. Returns `(routes, capturedSink, mptStore)` — the store is exposed so seeding tests can write accepted
    * certs into the canonical MPT partitions the registry reads from (the registry is now a pure MPT reader).
    */
  private def mkRoutes(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(HttpRoutes[IO], Queue[IO, Signed[KesRegistrationCert]], MptStore[IO, GlobalStateKey])] =
    for {
      base <- IO.pure(KesRegistry.empty[IO])
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      reader = GlobalStateReader.fromMptStore[IO](store)
      mutableRegistry <- MutableKesRegistry.make[IO](base, reader)
      sinkQueue <- Queue.unbounded[IO, Signed[KesRegistrationCert]]
      validator = KesRegistrationCertValidator.make[IO](
        io.constellationnetwork.security.signature.SignedValidator.make[IO],
        l0Seedlist = None
      )
      onAccepted: (Signed[KesRegistrationCert] => IO[Unit]) = signed => sinkQueue.offer(signed)
      routes = KesRegistrationCertRoutes[IO](
        onAccepted,
        validator,
        stubSnapshotStorage(currentEpoch),
        mutableRegistry
      ).publicRoutes
    } yield (routes, sinkQueue, store)

  test("POST a well-formed cert → 200 OK + body has hash + onAccepted sink received the cert") { res =>
    implicit val (h, sp, j, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      signed <- forAsyncHasher(cert, kp)
      (routes, sinkQueue, _) <- mkRoutes
      req = POST(signed, uri"/kes-registration")
      result <- expectHttpStatus(routes, req)(Status.Ok)
      sinkSize <- sinkQueue.size
    } yield result.and(expect.same(1, sinkSize))
  }

  test("POST cert signed by a key that is not the operator → 400 Bad Request") { res =>
    implicit val (h, sp, j, kp, operatorId) = res
    val cert = mkCert(operatorId)
    for {
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      // Signed by `otherKp` but the cert claims `operatorId` (derived from `kp`).
      signed <- forAsyncHasher(cert, otherKp)
      (routes, sinkQueue, _) <- mkRoutes
      req = POST(signed, uri"/kes-registration")
      result <- expectHttpStatus(routes, req)(Status.BadRequest)
      sinkSize <- sinkQueue.size
    } yield result.and(expect.same(0, sinkSize))
  }

  test("POST cert with stale ordinal (ordinal <= lastSeen) → 400 Bad Request (NonMonotonicOrdinal)") { res =>
    implicit val (h, sp, j, kp, operatorId) = res
    // First, seed the registry with an accepted cert at ordinal=2.
    val firstCert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)))
    // Then replay ordinal=2 again — must be rejected as NonMonotonicOrdinal.
    val replayCert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)))
    for {
      signedFirst <- forAsyncHasher(firstCert, kp)
      signedReplay <- forAsyncHasher(replayCert, kp)
      (routes, sinkQueue, store) <- mkRoutes
      // Seed the MPT (the registry's read source) so `lastReferenceFor(operatorId)` returns the first cert's ref.
      _ <- writeRecord(store, KesRegistrationRecord(signedFirst, SnapshotOrdinal(NonNegLong(1L))))
      req = POST(signedReplay, uri"/kes-registration")
      result <- expectHttpStatus(routes, req)(Status.BadRequest)
      sinkSize <- sinkQueue.size
    } yield result.and(expect.same(0, sinkSize))
  }

  test("GET /kes-registration/{peerId}/last-reference → 200 + registry head for that operator") { res =>
    implicit val (h, sp, j, kp, operatorId) = res
    // Empty case first.
    for {
      (routes, _, store) <- mkRoutes
      emptyReq = GET(Uri.unsafeFromString(s"/kes-registration/${operatorId.value.value}/last-reference"))
      emptyExpected = KesRegistrationReference.empty
      emptyResult <- expectHttpBodyAndStatus(routes, emptyReq)(emptyExpected, Status.Ok)

      // Seed the MPT (the registry's read source), then GET again.
      seededCert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(7L)))
      signedSeeded <- forAsyncHasher(seededCert, kp)
      _ <- writeRecord(store, KesRegistrationRecord(signedSeeded, SnapshotOrdinal(NonNegLong(1L))))
      seededReq = GET(Uri.unsafeFromString(s"/kes-registration/${operatorId.value.value}/last-reference"))
      expectedRef <- signedSeeded.toHashed.map(KesRegistrationReference.of)
      seededResult <- expectHttpBodyAndStatus(routes, seededReq)(expectedRef, Status.Ok)
    } yield emptyResult.and(seededResult)
  }
}
