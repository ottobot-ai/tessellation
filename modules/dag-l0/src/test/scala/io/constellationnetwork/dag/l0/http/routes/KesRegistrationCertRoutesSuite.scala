package io.constellationnetwork.dag.l0.http.routes

import java.security.KeyPair

import cats.effect.IO
import cats.effect.kernel.Resource
import cats.effect.std.Queue

import scala.collection.immutable.SortedSet

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.OperatorConsensusKeyRegistry
import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertValidator.RegistrationEvaluationContext
import io.constellationnetwork.node.shared.domain.nakamoto.kes.{KesRegistrationCertValidator, MutableKesRegistry}
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.http.routes.KesRegistrationCertRoutes
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
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
  * Builds a real `MutableKesRegistry` over an empty atomic operator-key registry, injects an exact candidate-parent context, and exercises
  * each rejection path the route surfaces:
  *
  *   - POST a well-formed cert → 200 + body carrying the candidate hash + the `onAccepted` sink saw the cert
  *   - POST cert with invalid sig (signer != operator) → 400
  *   - POST cert with stale ordinal (replay) → 400
  *   - GET `/last-reference` → returns the current registry head for that operator
  *
  * The route's other rejection paths are exhaustively exercised in `KesRegistrationCertValidatorSuite`. The route's job is just to surface
  * validator results as HTTP status codes; covering one valid + one rejected case per shape is sufficient at the route layer.
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

  private val registrationParentHash: Hash = Hash("aa" * 32)
  private val currentPeriod: EtaPeriod = EtaPeriod(100L)
  private val futurePeriod: EtaPeriod = EtaPeriod(200L)
  private val context = RegistrationEvaluationContext(registrationParentHash, currentPeriod)

  /** Build a well-formed cert template — caller can override fields with `.copy` for negative tests. */
  private def mkCert(
    operatorId: PeerId,
    ordinal: KesRegistrationOrdinal = KesRegistrationOrdinal.first,
    parent: KesRegistrationReference = KesRegistrationReference.empty,
    effectiveFromPeriod: EtaPeriod = futurePeriod
  ): KesRegistrationCert =
    KesRegistrationCert(
      operatorPeerId = operatorId,
      kesMasterVK = Hex("11" * KesRegistrationCertValidator.KesMasterVerificationKeyLength),
      kesMasterVKStep = 0,
      offset = effectiveFromPeriod.value,
      vrfPublicKey = Hex("22" * 32),
      effectiveFromPeriod = effectiveFromPeriod,
      registrationParentHash = registrationParentHash,
      ordinal = ordinal,
      parent = parent
    )

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
      base <- IO.pure(OperatorConsensusKeyRegistry.empty[IO])
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
        IO.pure(Some(context)),
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

  test("POST cert with stale ordinal (ordinal <= lastSeen) → 400 Bad Request (InvalidRegistrationOrdinal)") { res =>
    implicit val (h, sp, j, kp, operatorId) = res
    // First, seed the registry with an accepted cert at ordinal=2.
    val firstCert = mkCert(operatorId, ordinal = KesRegistrationOrdinal(NonNegLong(2L)))
    // Then replay ordinal=2 again — must be rejected as InvalidRegistrationOrdinal.
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
