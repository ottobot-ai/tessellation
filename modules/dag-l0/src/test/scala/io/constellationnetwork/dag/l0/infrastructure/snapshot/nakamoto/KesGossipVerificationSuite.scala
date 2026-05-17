package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
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
  * Asserts the warn-only behavior matrix for both [[KesGossipVerification.verifyAttestation]] and [[KesGossipVerification.verifySnapshot]]:
  *
  *   - sender's signature + receiver's verification round-trip cleanly when the kesRegistry has the sender's master VK (hard correctness
  *     claim from the slice spec)
  *   - empty wire field → no-sig counter increments
  *   - present sig + missing registry entry → no-registry-entry counter
  *   - present sig + wrong VK in registry → invalid counter (registry pre-seeded with a different VK)
  *
  * Uses [[CountingMetrics]] to assert per-counter increments and the existing [[OperationalKeyMaker.bootstrap]] pattern from
  * `OperationalKeyMakerSuite` for deterministic sender keys (in-memory store + fixed seed).
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

  /** Build a signing OperationalKeyMaker with a deterministic seed; return both the algebra (for `signAt`) and the master VK (for the
    * registry).
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
      registry = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(vk, 0L)))
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
      registry = KesRegistry.make[IO](Map(peerId('z') -> KesRegistryEntry(otherVk, 0L)))
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
      registry = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(wrongVk, 0L)))
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
      registry = KesRegistry.make[IO](Map(peerId('p') -> KesRegistryEntry(vk, 0L)))
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
      registry = KesRegistry.make[IO](Map(peerId('p') -> KesRegistryEntry(wrongVk, 0L)))
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

  // ============================================================
  // Slice 9 — load-bearing return-value matrix
  // ============================================================
  //
  // Verification is unconditional now (no warn-only fallback). These tests pin the return-
  // value mapping so the daemon's drop-on-false behavior can't silently flip.

  test("return-value matrix: valid→true, wrong/missing/decode-fail→false, no-registry→true (carve-out)") {
    for {
      (_, metrics) <- setup
      (signer, vk) <- buildSigner(0xaa.toByte)
      (_, wrongVk) <- buildSigner(0xbb.toByte)
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      registryGood = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(vk, 0L)))
      registryWrong = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(wrongVk, 0L)))
      registryEmpty = KesRegistry.empty[IO]

      okGood <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          wireBytes,
          peerId('a'),
          peerHex('a'),
          testOrdinal,
          registryGood,
          etaRotationSnapshots,
          logger
        )
      }
      okWrong <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          wireBytes,
          peerId('a'),
          peerHex('a'),
          testOrdinal,
          registryWrong,
          etaRotationSnapshots,
          logger
        )
      }
      okEmpty <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          wireBytes,
          peerId('a'),
          peerHex('a'),
          testOrdinal,
          registryEmpty,
          etaRotationSnapshots,
          logger
        )
      }
      okNoSig <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          Array.empty[Byte],
          peerId('a'),
          peerHex('a'),
          testOrdinal,
          registryGood,
          etaRotationSnapshots,
          logger
        )
      }
      okDecodeFail <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          Array.fill[Byte](16)(0xff.toByte),
          peerId('a'),
          peerHex('a'),
          testOrdinal,
          registryGood,
          etaRotationSnapshots,
          logger
        )
      }
    } yield
      expect(okGood, "valid sig + valid registry must return true") &&
        expect(!okWrong, "wrong VK in registry must return false") &&
        expect(okEmpty, "no registry entry must return true (Ed25519 already authenticated; Slice 10 carve-out)") &&
        expect(!okNoSig, "missing wire field must return false") &&
        expect(!okDecodeFail, "decode failure must return false")
  }

  // ============================================================
  // Offset semantics — operators registered mid-life
  // ============================================================
  //
  // An operator with `offset = K` has their tree's step 0 active at global eta period K. Sigs
  // signed at global period K+N use tree-internal step N. The receiver computes
  // `step = globalPeriod - offset` and verifies with `vk.copy(step = ...)`. Two cases pinned:
  //   (a) offset > 0 + matching sender step → verify succeeds
  //   (b) offset > globalPeriod → step would be negative → verify rejects with no actual crypto check

  test("offset > 0: sig at global period K+N round-trips via tree-internal step N") {
    // etaRotation=1 makes globalPeriod == tipOrdinal — easier to reason about.
    val rot: Long = 1L
    val offset: Long = 5L
    val globalPeriod: Int = 7 // tree-internal step = 7 - 5 = 2
    for {
      (_, metrics) <- setup
      (signer, vk) <- buildSigner(0x11.toByte)
      // Sender at tree-internal step 2
      sigResult <- signer.signAt(globalPeriod - offset.toInt, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      registry = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(vk, offset)))
      ok <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          wireBytes,
          peerId('a'),
          peerHex('a'),
          tipOrdinal = globalPeriod.toLong, // with rot=1 this is the global period
          kesRegistry = registry,
          etaRotationSnapshots = rot,
          logger = logger
        )
      }
    } yield expect(ok, s"offset=$offset + sig at tree step=${globalPeriod - offset} must verify at globalPeriod=$globalPeriod")
  }

  test("offset > globalPeriod: tree-internal step would be negative — rejected without crypto check") {
    val rot: Long = 1L
    val offset: Long = 50L
    val globalPeriod: Int = 10 // step = 10 - 50 = -40 — out of range
    for {
      (_, metrics) <- setup
      (signer, vk) <- buildSigner(0x22.toByte)
      sigResult <- signer.signAt(0, testMessageBytes)
      wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
      registry = KesRegistry.make[IO](Map(peerId('a') -> KesRegistryEntry(vk, offset)))
      ok <- {
        implicit val m: Metrics[IO] = metrics
        KesGossipVerification.verifyAttestation[IO](
          testMessageBytes,
          wireBytes,
          peerId('a'),
          peerHex('a'),
          tipOrdinal = globalPeriod.toLong,
          kesRegistry = registry,
          etaRotationSnapshots = rot,
          logger = logger
        )
      }
    } yield expect(!ok, s"operator registered at offset=$offset can't sign at globalPeriod=$globalPeriod — reject")
  }
}
