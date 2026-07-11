package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Ref, Resource}

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphCommitteeGate._
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Slice S3 integration tests for [[MetagraphCommitteeGate]].
  *
  * Coverage maps to the spec checklist:
  *   - (a) sender-in-committee path emits an attestation via the Publisher stub
  *   - (b) sender-not-in-committee path is silent (no publish, no aggregator record)
  *   - (c) gate blocks until threshold is reached (admit returns true)
  *   - (d) gate times out and drops (admit returns false) under non-quorum
  *   - (e) KES-invalid attestation is rejected by the receiver path (no aggregator record)
  *
  * The KES + committee VRF surfaces are stubbed at the algebra level (`KesSigner`, `KesVerifier`, `Publisher`) so the test exercises the
  * gate's orchestration without spinning up the full SecureStore + EcVrf25519 wire path — those have their own dedicated suites
  * (`OperationalKeyMakerSuite`, `KesGossipVerificationSuite`, `CommitteeSortitionSuite`).
  */
object MetagraphCommitteeGateSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private implicit val logger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLoggerFromName[IO]("MetagraphCommitteeGateSuite")

  // No-op Metrics — the committee-gate now emits sortition / admit counters; tests don't assert
  // on them (separate suite would, see CountingMetrics elsewhere) but the implicit must be
  // resolvable at the make site.
  private implicit val testMetrics: Metrics[IO] =
    CountingMetrics.instance(Ref.unsafe[IO, Map[String, Int]](Map.empty))

  // ===== test fixtures =====

  private def mkAddress(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  private def mkParent(tag: String): Hash =
    Hash.fromBytes(tag.getBytes("UTF-8"))

  private def mkBinaryHash(tag: String): Hash =
    Hash((tag + "0" * 64).take(64))

  private def vrfVkFor(vrfSk: Array[Byte]): Array[Byte] =
    new io.constellationnetwork.security.vrf.EcVrf25519().getVerificationKey(vrfSk)

  private def vrfRegistry(entries: (PeerId, Array[Byte])*): VrfRegistry[IO] =
    VrfRegistry.make[IO](entries.toMap)

  /** Stub publisher — captures every published attestation in a Ref so the test can assert on them. */
  private def stubPublisher: IO[(Publisher[IO], Ref[IO, List[StubPublished]])] =
    Ref.of[IO, List[StubPublished]](Nil).map { ref =>
      val publisher = new Publisher[IO] {
        def publish(
          senderPeerIdBytes: Array[Byte],
          metagraphAddress: String,
          parentHashBytes: Array[Byte],
          binaryHashBytes: Array[Byte],
          committeeVrfProof: Array[Byte],
          longTermSignature: Array[Byte],
          kesSignature: Array[Byte],
          vrfPublicKey: Array[Byte],
          kesStep: Int
        ): IO[Unit] =
          ref.update(
            StubPublished(
              new String(senderPeerIdBytes.map(b => f"$b%02x").mkString),
              metagraphAddress,
              new String(parentHashBytes, java.nio.charset.StandardCharsets.UTF_8),
              new String(binaryHashBytes, java.nio.charset.StandardCharsets.UTF_8),
              committeeVrfProof.length,
              longTermSignature.length,
              kesSignature.length,
              vrfPublicKey.length,
              kesStep
            ) :: _
          )
      }
      (publisher, ref)
    }

  private final case class StubPublished(
    senderHex: String,
    metagraphAddress: String,
    parentHash: String,
    binaryHash: String,
    proofLen: Int,
    edSigLen: Int,
    kesSigLen: Int,
    vrfVkLen: Int,
    kesStep: Int
  )

  /** Stub KES signer that always returns a deterministic 8-byte placeholder. `currentPeriod` returns a fixed value (7) so the gate's sender
    * path embeds a recognizable non-zero step on the wire; tests can assert against it. The receiver-side test stubs KES-verify directly;
    * the sender-side test only needs `signAt` to produce non-empty bytes so the gate doesn't tag the wire field as "empty KES".
    */
  private val stubKesSigner: KesSigner[IO] = new KesSigner[IO] {
    def currentPeriod: IO[Int] = IO.pure(7)
    def signAt(kesStep: Int, message: Array[Byte]): IO[Array[Byte]] =
      IO.pure(Array.fill[Byte](8)(0x42.toByte))
  }

  /** Stub KES verifier with an injectable accept rule. The default `acceptAll` returns true; tests that exercise the reject path swap in
    * `rejectAll` to simulate a load-bearing KES failure.
    */
  private def stubKesVerifier(acceptFn: Array[Byte] => Boolean): KesVerifier[IO] = new KesVerifier[IO] {
    def verify(
      messageBytes: Array[Byte],
      kesSigBytes: Array[Byte],
      attesterId: PeerId,
      attesterHex: Hex,
      kesStep: Int
    ): IO[Boolean] = IO.pure(acceptFn(kesSigBytes))
  }

  /** Build a `CommitteeSortition` and `Aggregator` pair plus a real `KeyPair`. K target controls in/out of committee — picking `K * sigma
    * >= 1` puts us always-in-committee (threshold saturation), `K = 0` would be invalid; for "out of committee" we use σ=0.
    */
  private def buildSortition(
    implicit h: Hasher[IO]
  ): IO[(CommitteeSortition[IO], MetagraphAttestationAggregator[IO])] =
    for {
      agg <- MetagraphAttestationAggregator.make[IO]
      sortition = CommitteeSortition.make[IO]
    } yield (sortition, agg)

  // ===== (a) sender-in-committee path emits attestation =====

  test("(a) sender in committee — publisher emits attestation + aggregator records self") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-a")
    val parent = mkParent("p-a")
    val binary = mkBinaryHash("bin-a")
    val eta = Array.fill[Byte](32)(0x01.toByte)
    // kDraw=1, σ=1 ⇒ in committee; self-records (count 1). kQuorum=1 ⇒ admit at 1 ⇒ admit=true.
    // This isolates "the sender path emits" from the threshold logic — covered separately in (c)/(d).
    val kDraw = 1
    val kQuorum = 1
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      vrfSk = Array.fill[Byte](32)(0x55.toByte)
      selfVrfVk = vrfVkFor(vrfSk)
      (sortition, agg) <- buildSortition
      (publisher, publishedRef) <- stubPublisher
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = kp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> selfVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 500L,
        pollIntervalMs = 25L
      )
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, sigmaOperatorKey = Ratio(1, 1))
      published <- publishedRef.get
      count <- agg.countFor(mg, parent, binary)
    } yield
      // kDraw · σ = 1 → in committee; sender path emits exactly one publish and records self;
      // kQuorum = 1 → admit true.
      expect(admitted)
        .and(expect(published.length == 1))
        .and(expect(count == 1))
        .and(expect(published.head.metagraphAddress == mg.value.value))
        .and(expect(published.head.kesSigLen == 8)) // stubKesSigner returns 8 bytes
        .and(expect(published.head.edSigLen > 0)) // long-term Ed25519 sig length is non-zero
        // The sender embeds the KES tree-internal step on the wire (proto sender_tree_step).
        // stubKesSigner.currentPeriod returns 7, so that's exactly what should land here.
        .and(expect(published.head.kesStep == 7))
  }

  // ===== (b) sender-not-in-committee path is silent =====

  test("(b) sender not in committee — no publish, no aggregator record") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-b")
    val parent = mkParent("p-b")
    val binary = mkBinaryHash("bin-b")
    val eta = Array.fill[Byte](32)(0x02.toByte)
    val kDraw = 1 // kDraw · σ = 1 · 0 = 0 → never in committee
    val kQuorum = 1
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      vrfSk = Array.fill[Byte](32)(0x66.toByte)
      selfVrfVk = vrfVkFor(vrfSk)
      (sortition, agg) <- buildSortition
      (publisher, publishedRef) <- stubPublisher
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = kp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> selfVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 200L,
        pollIntervalMs = 25L
      )
      // σ = 0 → not in committee
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, sigmaOperatorKey = Ratio.Zero)
      published <- publishedRef.get
      count <- agg.countFor(mg, parent, binary)
    } yield
      // Not in committee → no publish, no self-record. Gate times out → admit false.
      expect(!admitted)
        .and(expect(published.isEmpty))
        .and(expect(count == 0))
  }

  // ===== (c) gate blocks until threshold reached =====

  test("(c) gate admits as soon as aggregator count reaches kQuorum") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-c")
    val parent = mkParent("p-c")
    val binary = mkBinaryHash("bin-c")
    val eta = Array.fill[Byte](32)(0x03.toByte)
    // Draw/quorum decouple: admit waits for kQuorum=4 distinct attesters DIRECTLY. kDraw is irrelevant here (σ=0 ⇒ sender skips).
    val kDraw = 6
    val kQuorum = 4
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      vrfSk = Array.fill[Byte](32)(0x77.toByte)
      selfVrfVk = vrfVkFor(vrfSk)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      // Pre-seed 3 attestations from peers (below quorum). We use σ=0 so the sender path skips, then
      // separately race the quorum-reach with an external recorder fiber that adds the 4th.
      _ <- agg.record(mg, parent, binary, PeerId(Hex("aa" * 64)))
      _ <- agg.record(mg, parent, binary, PeerId(Hex("bb" * 64)))
      _ <- agg.record(mg, parent, binary, PeerId(Hex("cc" * 64)))
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = kp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> selfVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 2000L,
        pollIntervalMs = 25L
      )
      // Schedule the 4th attestation to arrive after a small delay — gate must wait then admit.
      _ <- (IO.sleep(100.millis) >> agg.record(mg, parent, binary, PeerId(Hex("dd" * 64)))).start
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, sigmaOperatorKey = Ratio.Zero)
      finalCount <- agg.countFor(mg, parent, binary)
    } yield
      // Initial 3 + scheduled 1 = 4 ≥ kQuorum (4) → quorum met within timeout
      expect(admitted)
        .and(expect(finalCount == 4))
  }

  // ===== (d) gate times out and drops =====

  test("(d) gate times out when threshold never reached") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-d")
    val parent = mkParent("p-d")
    val binary = mkBinaryHash("bin-d")
    val eta = Array.fill[Byte](32)(0x04.toByte)
    val kDraw = 6
    val kQuorum = 4 // need 4 attestations; we'll only supply 2
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      vrfSk = Array.fill[Byte](32)(0x88.toByte)
      selfVrfVk = vrfVkFor(vrfSk)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      _ <- agg.record(mg, parent, binary, PeerId(Hex("aa" * 64)))
      _ <- agg.record(mg, parent, binary, PeerId(Hex("bb" * 64)))
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = kp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> selfVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 250L,
        pollIntervalMs = 25L
      )
      // σ=0 so sender skips; no other arrivals → timeout
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, sigmaOperatorKey = Ratio.Zero)
      finalCount <- agg.countFor(mg, parent, binary)
    } yield
      // Only 2 pre-seeded attestations stay; gate returns false.
      expect(!admitted)
        .and(expect(finalCount == 2))
  }

  // ===== (e) KES-invalid receiver attestation rejected =====

  test("(e) receiver path rejects KES-invalid attestation — no aggregator record") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-e")
    val parent = mkParent("p-e")
    val binary = mkBinaryHash("bin-e")
    val eta = Array.fill[Byte](32)(0x05.toByte)
    val kDraw = 100
    val kQuorum = 1 // receiver-reject test: quorum is never reached (record never happens), value is incidental
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      senderKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      senderId = PeerId.fromPublic(senderKp.getPublic)
      vrfSk = Array.fill[Byte](32)(0x99.toByte)
      senderVrfVk = Array.fill[Byte](32)(0xaa.toByte)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      // KES verifier rejects everything — simulates an adversary forging a committee attestation
      // without a valid KES sig. Gate must drop before calling aggregator.record.
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = Array.fill[Byte](32)(0xab.toByte),
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(senderId -> senderVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => false), // <-- KES rejects
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 200L,
        pollIntervalMs = 25L
      )
      // Build a fake-but-structurally-valid incoming attestation. The long-term Ed25519 sig
      // needs to verify against the sender's pubkey, so we sign the canonical message bytes
      // with the senderKp's private key — then KES rejects regardless.
      msgBytes <- MetagraphCommitteeGate.messageBytes[IO](senderId, mg, parent, binary)
      edSig <- io.constellationnetwork.security.signature.Signing.signData[IO](msgBytes)(senderKp.getPrivate)
      incoming = IncomingAttestation(
        senderPeerId = senderId,
        senderVrfVk = senderVrfVk, // VRF would also fail since the proof is fake — we tag KES-fail first
        metagraphAddress = mg,
        parentHash = parent,
        binaryHash = binary,
        committeeVrfProof = Array.fill[Byte](64)(0xbb.toByte),
        longTermSignature = edSig,
        kesSignature = Array.fill[Byte](8)(0xcc.toByte), // present but verifier rejects
        senderTreeStep = 0
      )
      _ <- gate.recordReceivedAttestation(incoming, eta, _ => IO.pure(Ratio(1, 8)))
      count <- agg.countFor(mg, parent, binary)
    } yield
      // Verifier rejected → aggregator untouched
      expect(count == 0)
  }

  // ===== (f) #213/#290 gate no longer re-resolves the parent ordinal — it trusts the supplied eta =====
  //
  // The gate USED to take a `parentOrdinalFor` and fail-close (sender skip / receiver drop) when it
  // returned None. That GSI-only re-resolve returned None on the empty-currency-partition path and
  // deadlocked the metagraph tip. The responsibility for resolving the parent ordinal (from the
  // incoming binary's OWN content, via `MetagraphParentOrdinalResolver.resolveFromBinary`) and
  // computing the eta now lives entirely in the CALLER (daemon / admission processor); the gate just
  // signs/verifies against the eta it is handed. This test pins the new contract: an in-committee
  // sender with a supplied eta publishes + self-records (no internal veto), and the receiver records
  // a fully-valid attestation verified against that same eta.

  test("(f) gate publishes/records against the supplied eta — no internal parent-ordinal veto") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-f")
    val parent = mkParent("p-f")
    val binary = mkBinaryHash("bin-f")
    val eta = Array.fill[Byte](32)(0x07.toByte)
    val kDraw = 1 // kDraw · σ = 1 puts the sender in committee; gate must publish + self-record
    val kQuorum = 1
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      vrfSk = Array.fill[Byte](32)(0xa1.toByte)
      selfVrfVk = vrfVkFor(vrfSk)
      (sortition, agg) <- buildSortition
      (publisher, publishedRef) <- stubPublisher
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = vrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> selfVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 500L,
        pollIntervalMs = 25L
      )
      // Sender path — σ=1 makes us in-committee; with no internal parent-ordinal veto the gate
      // publishes and self-records, and kQuorum = 1 → admit true.
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, sigmaOperatorKey = Ratio(1, 1))
      published <- publishedRef.get
      countAfterSender <- agg.countFor(mg, parent, binary)
    } yield
      // No internal veto: the in-committee sender publishes once and self-records; admit succeeds.
      expect(admitted)
        .and(expect(published.length == 1))
        .and(expect(countAfterSender == 1))
  }

  // ===== happy-path receiver record (sanity) =====

  test("receiver path records when all three sigs accept") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-recv")
    val parent = mkParent("p-recv")
    val binary = mkBinaryHash("bin-recv")
    val eta = Array.fill[Byte](32)(0x06.toByte)
    val kDraw = 100
    val kQuorum = 1
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      senderKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      senderId = PeerId.fromPublic(senderKp.getPublic)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      // Build a valid sender draw — VRF SK from a deterministic seed, kDraw·σ=100·(1/1)=100 → in committee
      senderVrfSk = Array.fill[Byte](32)(0xdd.toByte)
      senderVrfVk = new io.constellationnetwork.security.vrf.EcVrf25519().getVerificationKey(senderVrfSk)
      drawResult <- sortition.isInCommittee(senderVrfSk, eta, mg, parent, Ratio(1, 1), kDraw)
      proof = drawResult.map(_._1).getOrElse(Array.empty[Byte])
      // Build canonical message bytes + Ed25519 sig from senderKp.private
      msgBytes <- MetagraphCommitteeGate.messageBytes[IO](senderId, mg, parent, binary)
      edSig <- io.constellationnetwork.security.signature.Signing.signData[IO](msgBytes)(senderKp.getPrivate)
      // Stub KES verifier accepts (sig length > 0 → accept)
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = Array.fill[Byte](32)(0xee.toByte),
        selfVrfVk = Array.fill[Byte](32)(0xab.toByte),
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(senderId -> senderVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_.nonEmpty),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = kQuorum,
        gateTimeoutMs = 200L,
        pollIntervalMs = 25L
      )
      incoming = IncomingAttestation(
        senderPeerId = senderId,
        senderVrfVk = senderVrfVk,
        metagraphAddress = mg,
        parentHash = parent,
        binaryHash = binary,
        committeeVrfProof = proof,
        longTermSignature = edSig,
        kesSignature = Array.fill[Byte](8)(0xff.toByte), // non-empty → stub verifier accepts
        senderTreeStep = 0
      )
      _ <- gate.recordReceivedAttestation(incoming, eta, _ => IO.pure(Ratio(1, 1)))
      count <- agg.countFor(mg, parent, binary)
    } yield expect(count == 1)
  }

  test("receiver rejects a valid proof under an unregistered in-band VRF key") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-unregistered-vrf")
    val parent = mkParent("p-unregistered-vrf")
    val binary = mkBinaryHash("bin-unregistered-vrf")
    val eta = Array.fill[Byte](32)(0x21.toByte)
    val kDraw = 100
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      senderKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      senderId = PeerId.fromPublic(senderKp.getPublic)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      senderVrfSk = Array.fill[Byte](32)(0x22.toByte)
      senderVrfVk = vrfVkFor(senderVrfSk)
      draw <- sortition.isInCommittee(senderVrfSk, eta, mg, parent, Ratio(1, 1), kDraw)
      proof = draw.map(_._1).getOrElse(Array.emptyByteArray)
      msgBytes <- MetagraphCommitteeGate.messageBytes[IO](senderId, mg, parent, binary)
      edSig <- io.constellationnetwork.security.signature.Signing.signData[IO](msgBytes)(senderKp.getPrivate)
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = Array.fill[Byte](32)(0x23.toByte),
        selfVrfVk = Array.fill[Byte](32)(0x24.toByte),
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = VrfRegistry.empty[IO],
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = 1,
        gateTimeoutMs = 100L,
        pollIntervalMs = 25L
      )
      _ <- gate.recordReceivedAttestation(
        IncomingAttestation(
          senderPeerId = senderId,
          senderVrfVk = senderVrfVk,
          metagraphAddress = mg,
          parentHash = parent,
          binaryHash = binary,
          committeeVrfProof = proof,
          longTermSignature = edSig,
          kesSignature = Array[Byte](1),
          senderTreeStep = 0
        ),
        eta,
        _ => IO.pure(Ratio(1, 1))
      )
      count <- agg.countFor(mg, parent, binary)
    } yield expect(count == 0)
  }

  test("receiver rejects key grinding when the in-band VRF key differs from registration") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-mismatched-vrf")
    val parent = mkParent("p-mismatched-vrf")
    val binary = mkBinaryHash("bin-mismatched-vrf")
    val eta = Array.fill[Byte](32)(0x31.toByte)
    val kDraw = 100
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      senderKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      senderId = PeerId.fromPublic(senderKp.getPublic)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      attackerVrfSk = Array.fill[Byte](32)(0x32.toByte)
      attackerVrfVk = vrfVkFor(attackerVrfSk)
      registeredVrfVk = vrfVkFor(Array.fill[Byte](32)(0x33.toByte))
      draw <- sortition.isInCommittee(attackerVrfSk, eta, mg, parent, Ratio(1, 1), kDraw)
      proof = draw.map(_._1).getOrElse(Array.emptyByteArray)
      msgBytes <- MetagraphCommitteeGate.messageBytes[IO](senderId, mg, parent, binary)
      edSig <- io.constellationnetwork.security.signature.Signing.signData[IO](msgBytes)(senderKp.getPrivate)
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = Array.fill[Byte](32)(0x34.toByte),
        selfVrfVk = Array.fill[Byte](32)(0x35.toByte),
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(senderId -> registeredVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = kDraw,
        kQuorum = 1,
        gateTimeoutMs = 100L,
        pollIntervalMs = 25L
      )
      _ <- gate.recordReceivedAttestation(
        IncomingAttestation(
          senderPeerId = senderId,
          senderVrfVk = attackerVrfVk,
          metagraphAddress = mg,
          parentHash = parent,
          binaryHash = binary,
          committeeVrfProof = proof,
          longTermSignature = edSig,
          kesSignature = Array[Byte](1),
          senderTreeStep = 0
        ),
        eta,
        _ => IO.pure(Ratio(1, 1))
      )
      count <- agg.countFor(mg, parent, binary)
    } yield expect(count == 0)
  }

  test("sender does not self-count when its local VRF key differs from registration") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-self-mismatched-vrf")
    val parent = mkParent("p-self-mismatched-vrf")
    val binary = mkBinaryHash("bin-self-mismatched-vrf")
    val eta = Array.fill[Byte](32)(0x41.toByte)
    for {
      selfKp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(selfKp.getPublic)
      (sortition, agg) <- buildSortition
      (publisher, publishedRef) <- stubPublisher
      selfVrfSk = Array.fill[Byte](32)(0x42.toByte)
      selfVrfVk = vrfVkFor(selfVrfSk)
      registeredVrfVk = vrfVkFor(Array.fill[Byte](32)(0x43.toByte))
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = selfVrfSk,
        selfVrfVk = selfVrfVk,
        keyPair = selfKp,
        sortition = sortition,
        vrfRegistry = vrfRegistry(selfId -> registeredVrfVk),
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = 1,
        kQuorum = 1,
        gateTimeoutMs = 100L,
        pollIntervalMs = 25L
      )
      admitted <- gate.attestAndAdmit(mg, parent, binary, eta, Ratio(1, 1))
      count <- agg.countFor(mg, parent, binary)
      published <- publishedRef.get
    } yield expect(!admitted).and(expect(count == 0)).and(expect(published.isEmpty))
  }

  // ===== pruneParents passthrough =====

  test("pruneParents forwards to aggregator") { res =>
    implicit val (h, sp, _) = res
    val mg = mkAddress("mg-prune")
    val parent = mkParent("p-prune")
    val binary = mkBinaryHash("bin-prune")
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      selfId = PeerId.fromPublic(kp.getPublic)
      (sortition, agg) <- buildSortition
      (publisher, _) <- stubPublisher
      _ <- agg.record(mg, parent, binary, PeerId(Hex("aa" * 64)))
      gate = MetagraphCommitteeGate.make[IO](
        selfPeerId = selfId,
        selfVrfSk = Array.fill[Byte](32)(0x11.toByte),
        selfVrfVk = Array.fill[Byte](32)(0xab.toByte),
        keyPair = kp,
        sortition = sortition,
        vrfRegistry = VrfRegistry.empty[IO],
        aggregator = agg,
        kesSigner = stubKesSigner,
        kesVerifier = stubKesVerifier(_ => true),
        publisher = publisher,
        kDraw = 100,
        kQuorum = 1,
        gateTimeoutMs = 100L,
        pollIntervalMs = 25L
      )
      before <- agg.countFor(mg, parent, binary)
      _ <- gate.pruneParents(mg, Set(parent))
      after <- agg.countFor(mg, parent, binary)
    } yield expect(before == 1).and(expect(after == 0))
  }
}
