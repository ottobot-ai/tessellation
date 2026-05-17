package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._

import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.SimpleIOSuite

/** §1.2 Slice 5/6 verify-path round-trip tests.
  *
  * Asserts the warn-only behavior matrix for both [[KesGossipVerification.verifyAttestation]] and
  * [[KesGossipVerification.verifySnapshot]]:
  *
  *   - sender's signature + receiver's verification round-trip cleanly when the kesRegistry has the
  *     sender's master VK (hard correctness claim from the slice spec)
  *   - empty wire field → no-sig counter increments
  *   - present sig + missing registry entry → no-registry-entry counter
  *   - present sig + wrong VK in registry → invalid counter (registry pre-seeded with a different VK)
  *
  * Uses [[CountingMetrics]] to assert per-counter increments and the existing
  * [[OperationalKeyMaker.bootstrap]] pattern from `OperationalKeyMakerSuite` for deterministic
  * sender keys (in-memory store + fixed seed).
  */
object KesGossipVerificationSuite extends SimpleIOSuite {

  private implicit val logger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLoggerFromName[IO]("KesGossipVerificationSuite")

  // Real PeerId hex (128 chars = 64 bytes). We never serialize through the network; the loader
  // only needs `Hex.toBytes` to round-trip.
  private def peerHex(seed: Char): Hex = Hex((seed.toString * 128).take(128))
  private def peerId(seed: Char): PeerId = Id(peerHex(seed)).toPeerId

  // Etarotation kept large so `EtaCalculation.rotationPeriod(ordinal=10, _) = 0` — we don't
  // exercise period evolution in this slice, just the verify path.
  private val etaRotationSnapshots: Long = 2550L
  private val testOrdinal: Long = 10L
  private val testMessageBytes: Array[Byte] = "test-attestation-or-snapshot-hash".getBytes("UTF-8")

  private def setup: IO[(cats.effect.kernel.Ref[IO, Map[String, Int]], Metrics[IO])] =
    CountingMetrics.make

  /** Build a signing OperationalKeyMaker with a deterministic seed; return both the algebra
    * (for `signAt`) and the master VK (for the registry).
    */
  private def buildSigner(seedByte: Byte): IO[(OperationalKeyMakerAlgebra[IO], VerificationKeyKesProduct)] =
    SecureStore.inMemory[IO].flatMap { store =>
      val seed = Array.fill[Byte](32)(seedByte)
      OperationalKeyMaker
        .bootstrap[IO](store, "kes-test.key", seed, etaPeriodLength = 100L, height = (2, 2))
        .use { kmaker =>
          kmaker.currentPublicKey.map((kmaker, _))
        }
        .flatMap {
          case (_, vk) =>
            // Re-bootstrap with the same seed so the in-memory key returns to step 0 — we need a
            // fresh signer (the `.use` block above closed the resource).
            SecureStore.inMemory[IO].flatMap { store2 =>
              val seed2 = Array.fill[Byte](32)(seedByte)
              OperationalKeyMaker
                .bootstrap[IO](store2, "kes-test.key", seed2, etaPeriodLength = 100L, height = (2, 2))
                .allocated
                .map { case (kmaker, _) => (kmaker, vk) }
            }
        }
    }

  // ============================================================
  // Attestation path
  // ============================================================

  test("attestation: signed-then-verified round-trips when registry has sender's master VK") {
    for {
      (counters, metrics) <- setup
      (signer, vk) <- buildSigner(0x11.toByte)
      registry = KesRegistry.make[IO](Map(peerId('a') -> vk))
      sigResult <- signer.signAt(0, testMessageBytes)
      sigBytes = sigResult.toOption.get
      wireBytes = OperationalKeyMaker.encodeSignature(sigBytes)
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          attesterId = peerId('a'),
          attesterHex = peerHex('a'),
          tipOrdinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_decode_failed_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total"))
  }

  test("attestation: empty wire field → no-sig counter, no other increments") {
    for {
      (counters, metrics) <- setup
      registry = KesRegistry.empty[IO]
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = Array.empty[Byte],
          attesterId = peerId('b'),
          attesterHex = peerHex('b'),
          tipOrdinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total"))
  }

  test("attestation: present sig + missing registry entry → no-registry-entry counter") {
    for {
      (counters, metrics) <- setup
      (signer, _) <- buildSigner(0x22.toByte)
      // Registry has someone else's entry, not the attester
      (_, otherVk) <- buildSigner(0x33.toByte)
      registry = KesRegistry.make[IO](Map(peerId('z') -> otherVk))
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          attesterId = peerId('a'), // not in registry
          attesterHex = peerHex('a'),
          tipOrdinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total"))
  }

  test("attestation: present sig + wrong VK in registry → invalid counter (warn-only)") {
    for {
      (counters, metrics) <- setup
      (signerA, _) <- buildSigner(0x44.toByte) // signs the actual message
      (_, wrongVk) <- buildSigner(0x55.toByte) // different VK seeded in registry
      registry = KesRegistry.make[IO](Map(peerId('a') -> wrongVk))
      sigResult <- signerA.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          attesterId = peerId('a'),
          attesterHex = peerHex('a'),
          tipOrdinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_invalid_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total"))
  }

  test("attestation: garbage wire bytes → decode-failed counter") {
    for {
      (counters, metrics) <- setup
      garbage = Array.fill[Byte](16)(0xff.toByte) // 4-byte length prefix = -1 → MalformedTree
      registry = KesRegistry.empty[IO]
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = garbage,
          attesterId = peerId('a'),
          attesterHex = peerHex('a'),
          tipOrdinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_decode_failed_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total"))
  }

  // ============================================================
  // Snapshot path (same matrix)
  // ============================================================

  test("snapshot: signed-then-verified round-trips when registry has sender's master VK") {
    for {
      (counters, metrics) <- setup
      (signer, vk) <- buildSigner(0x66.toByte)
      registry = KesRegistry.make[IO](Map(peerId('p') -> vk))
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifySnapshot[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          producerId = peerId('p'),
          producerHex = peerHex('p'),
          ordinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_verified_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_snapshots_invalid_total"))
  }

  test("snapshot: empty wire field → no-sig counter") {
    for {
      (counters, metrics) <- setup
      registry = KesRegistry.empty[IO]
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifySnapshot[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = Array.empty[Byte],
          producerId = peerId('q'),
          producerHex = peerHex('q'),
          ordinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_no_sig_total"))
  }

  test("snapshot: present sig + missing registry entry → no-registry-entry counter") {
    for {
      (counters, metrics) <- setup
      (signer, _) <- buildSigner(0x77.toByte)
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      registry = KesRegistry.empty[IO]
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifySnapshot[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          producerId = peerId('p'),
          producerHex = peerHex('p'),
          ordinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_no_registry_entry_total"))
  }

  test("snapshot: present sig + wrong VK in registry → invalid counter (warn-only)") {
    for {
      (counters, metrics) <- setup
      (signer, _) <- buildSigner(0x88.toByte)
      (_, wrongVk) <- buildSigner(0x99.toByte)
      registry = KesRegistry.make[IO](Map(peerId('p') -> wrongVk))
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      _ <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifySnapshot[IO](
          messageBytes = testMessageBytes,
          kesSigBytes = wireBytes,
          producerId = peerId('p'),
          producerHex = peerHex('p'),
          ordinal = testOrdinal,
          kesRegistry = registry,
          etaRotationSnapshots = etaRotationSnapshots,
          logger = logger
        )
      }
      finalCounters <- counters.get
    } yield
      expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_invalid_total")) &&
        expect.same(None, finalCounters.get("dag_nakamoto_kes_snapshots_verified_total"))
  }
}
