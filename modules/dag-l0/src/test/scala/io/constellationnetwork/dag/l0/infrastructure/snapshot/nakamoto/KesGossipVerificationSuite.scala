package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{CanonicalOperatorConsensusFixture, KesRegistryEntry}
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._

import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.SimpleIOSuite

/** §1.2 Slice 5/6 verify-path round-trip tests.
  *
  * Asserts the fail-closed behavior matrix for both [[KesGossipVerification.verifyAttestation]] and
  * [[KesGossipVerification.verifySnapshot]]:
  *
  *   - sender's signature + receiver's verification round-trip cleanly under the already-resolved atomic KES+VRF identity
  *   - empty wire field → no-sig counter increments
  *   - present sig + missing registry entry → no-registry-entry counter
  *   - present sig + wrong VK in registry → invalid counter (registry pre-seeded with a different VK)
  *
  * Uses [[CountingMetrics]] to assert per-counter increments. Positive artifacts are signed by a loader-validated canonical genesis
  * operator pair; deterministic raw KES signers are confined to explicit wrong-key negative cases.
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

  /** Models the absence of an upstream exact-pair resolution. Production callers stop before this verifier; the defensive verifier still
    * rejects a capability whose identity does not match the artifact's actor.
    */
  private def invalidKeys(operator: PeerId, keys: OperatorConsensusKeys): OperatorConsensusKeys =
    keys.copy(operatorPeerId = if (operator == peerId('z')) peerId('y') else peerId('z'))

  private def signWithCanonical(
    operator: CanonicalOperatorConsensusFixture,
    step: Int = 0
  ): IO[Array[Byte]] =
    operator.kesSigner
      .signAt(step, testMessageBytes)
      .map(result => OperationalKeyMaker.encodeSignature(result.toOption.get))

  /** Build a deliberately nonregistered OperationalKeyMaker for wrong-signature/replacement-key negative cases.
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
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        wireBytes <- signWithCanonical(operator)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            attesterId = operatorId,
            attesterHex = operatorId.value,
            tipOrdinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(accepted, "a valid registered attestation must be accepted") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_decode_failed_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total"))
    }
  }

  test("attestation: empty wire field → no-sig counter, no other increments") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = Array.empty[Byte],
            attesterId = operatorId,
            attesterHex = operatorId.value,
            tipOrdinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "a missing attestation KES signature must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total"))
    }
  }

  test("attestation: present sig + missing registry entry → no-registry-entry counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      val operatorKeys = invalidKeys(operatorId, operator.resolvedPair)
      for {
        (counters, metrics) <- setup
        wireBytes <- signWithCanonical(operator)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            attesterId = operatorId,
            attesterHex = operatorId.value,
            tipOrdinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "an unregistered attester must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_invalid_total"))
    }
  }

  test("attestation: present sig + wrong VK in registry → reject and increment invalid counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        (signerA, _) <- buildSigner(0x44.toByte)
        sigResult <- signerA.signAt(0, testMessageBytes)
        wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            attesterId = operatorId,
            attesterHex = operatorId.value,
            tipOrdinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "an attestation signed by a key other than the registered KES key must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_invalid_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_verified_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_registry_entry_total"))
    }
  }

  test("attestation: garbage wire bytes → decode-failed counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        garbage = Array.fill[Byte](16)(0xff.toByte) // 4-byte length prefix = -1 → MalformedTree
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = garbage,
            attesterId = operatorId,
            attesterHex = operatorId.value,
            tipOrdinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "a malformed attestation KES signature must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_decode_failed_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_attestations_no_sig_total"))
    }
  }

  test("all gossip verification rails reject the seven-empty-field KES container without raising") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      val emptyFieldContainer = Array.fill[Byte](7 * Integer.BYTES)(0)

      for {
        (counters, metrics) <- setup
        attestation <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification
            .verifyAttestation[IO](
              messageBytes = testMessageBytes,
              kesSigBytes = emptyFieldContainer,
              attesterId = operatorId,
              attesterHex = operatorId.value,
              tipOrdinal = testOrdinal,
              operatorKeys = operatorKeys,
              etaRotationSnapshots = etaRotationSnapshots,
              logger = logger
            )
            .attempt
        }
        snapshot <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification
            .verifySnapshot[IO](
              messageBytes = testMessageBytes,
              kesSigBytes = emptyFieldContainer,
              producerId = operatorId,
              producerHex = operatorId.value,
              ordinal = testOrdinal,
              operatorKeys = operatorKeys,
              etaRotationSnapshots = etaRotationSnapshots,
              logger = logger
            )
            .attempt
        }
        metagraph <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification
            .verifyAttestationByStep[IO](
              messageBytes = testMessageBytes,
              kesSigBytes = emptyFieldContainer,
              expectedOperatorId = operatorId,
              operatorKeys = operatorKeys,
              kesStep = 0,
              artifactPeriod = EtaPeriod.Zero,
              logger = logger
            )
            .attempt
        }
        finalCounters <- counters.get
      } yield
        expect(attestation.toOption.contains(false)) &&
          expect(snapshot.toOption.contains(false)) &&
          expect(metagraph.toOption.contains(false)) &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_attestations_decode_failed_total")) &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_decode_failed_total")) &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_mg_attestations_decode_failed_total"))
    }
  }

  // ============================================================
  // Snapshot path (same matrix)
  // ============================================================

  test("snapshot: signed-then-verified round-trips when registry has sender's master VK") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        wireBytes <- signWithCanonical(operator)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifySnapshot[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            producerId = operatorId,
            producerHex = operatorId.value,
            ordinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(accepted, "a valid registered snapshot must be accepted") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_verified_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_snapshots_invalid_total"))
    }
  }

  test("snapshot: empty wire field → no-sig counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifySnapshot[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = Array.empty[Byte],
            producerId = operatorId,
            producerHex = operatorId.value,
            ordinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "a snapshot without a KES signature must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_no_sig_total"))
    }
  }

  test("snapshot: present sig + missing registry entry → no-registry-entry counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      val operatorKeys = invalidKeys(operatorId, operator.resolvedPair)
      for {
        (counters, metrics) <- setup
        wireBytes <- signWithCanonical(operator)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifySnapshot[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            producerId = operatorId,
            producerHex = operatorId.value,
            ordinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "an unregistered snapshot producer must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_no_registry_entry_total"))
    }
  }

  test("snapshot: present sig + wrong VK in registry → reject and increment invalid counter") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorKeys = operator.resolvedPair
      val operatorId = operatorKeys.operatorPeerId
      for {
        (counters, metrics) <- setup
        (signer, _) <- buildSigner(0x88.toByte)
        sigResult <- signer.signAt(0, testMessageBytes)
        wireBytes = OperationalKeyMaker.encodeSignature(sigResult.toOption.get)
        accepted <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifySnapshot[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            producerId = operatorId,
            producerHex = operatorId.value,
            ordinal = testOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = etaRotationSnapshots,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(!accepted, "a snapshot signed by a key other than the registered KES key must be rejected") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_snapshots_invalid_total")) &&
          expect.same(None, finalCounters.get("dag_nakamoto_kes_snapshots_verified_total"))
    }
  }

  // ============================================================
  // Slice 9 — load-bearing return-value matrix
  // ============================================================
  //
  // Verification is unconditional now (no warn-only fallback). These tests pin the return-
  // value mapping so the daemon's drop-on-false behavior can't silently flip.

  test("return-value matrix: only a valid registered KES signature returns true") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      for {
        (_, metrics) <- setup
        (_, wrongVk) <- buildSigner(0xbb.toByte)
        wireBytes <- signWithCanonical(operator)
        keysGood = operator.resolvedPair
        keysWrong = keysGood.copy(kes = KesRegistryEntry(wrongVk, 0L))
        keysMissing = invalidKeys(operatorId, keysGood)

        okGood <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            wireBytes,
            operatorId,
            operatorId.value,
            testOrdinal,
            keysGood,
            etaRotationSnapshots,
            logger
          )
        }
        okWrong <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            wireBytes,
            operatorId,
            operatorId.value,
            testOrdinal,
            keysWrong,
            etaRotationSnapshots,
            logger
          )
        }
        okEmpty <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            wireBytes,
            operatorId,
            operatorId.value,
            testOrdinal,
            keysMissing,
            etaRotationSnapshots,
            logger
          )
        }
        okNoSig <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            Array.empty[Byte],
            operatorId,
            operatorId.value,
            testOrdinal,
            keysGood,
            etaRotationSnapshots,
            logger
          )
        }
        okDecodeFail <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            Array.fill[Byte](16)(0xff.toByte),
            operatorId,
            operatorId.value,
            testOrdinal,
            keysGood,
            etaRotationSnapshots,
            logger
          )
        }
      } yield
        expect(okGood, "valid sig + valid registry must return true") &&
          expect(!okWrong, "wrong VK in registry must return false") &&
          expect(!okEmpty, "no registry entry must return false") &&
          expect(!okNoSig, "missing wire field must return false") &&
          expect(!okDecodeFail, "decode failure must return false")
    }
  }

  test("metagraph admission KES-by-step is fail-closed when the operator is not registered") {
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      for {
        (counters, metrics) <- setup
        wireBytes <- signWithCanonical(operator)
        registered = operator.resolvedPair
        valid <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestationByStep[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            expectedOperatorId = operatorId,
            operatorKeys = registered,
            kesStep = 0,
            artifactPeriod = EtaPeriod.Zero,
            logger = logger
          )
        }
        unregistered <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestationByStep[IO](
            messageBytes = testMessageBytes,
            kesSigBytes = wireBytes,
            expectedOperatorId = operatorId,
            operatorKeys = invalidKeys(operatorId, registered),
            kesStep = 0,
            artifactPeriod = EtaPeriod.Zero,
            logger = logger
          )
        }
        finalCounters <- counters.get
      } yield
        expect(valid, "registered KES identity must verify") &&
          expect(!unregistered, "missing KES registration must reject admission attestation") &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_mg_attestations_verified_total")) &&
          expect.same(Some(1), finalCounters.get("dag_nakamoto_kes_mg_attestations_no_registry_entry_total"))
    }
  }

  // ============================================================
  // Artifact-period semantics and runtime-registration boundary
  // ============================================================
  //
  // The frozen current-view verifier accepts only genesis pairs. It derives a genesis key's tree step from the artifact's parent period.
  // Runtime offsets are resolved only by HistoricalOperatorConsensusKeyRegistry; feeding one through this compatibility path must reject.

  test("genesis pair at artifact period N round-trips via tree step N") {
    val rot: Long = 1L
    val artifactPeriod: Int = 2
    val tipOrdinal: Long = 3L // verification uses the parent ordinal, so rotationPeriod(3 - 1, 1) = 2
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      for {
        (_, metrics) <- setup
        wireBytes <- signWithCanonical(operator, artifactPeriod)
        operatorKeys = operator.resolvedPair
        ok <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            wireBytes,
            operatorId,
            operatorId.value,
            tipOrdinal = tipOrdinal,
            operatorKeys = operatorKeys,
            etaRotationSnapshots = rot,
            logger = logger
          )
        }
      } yield expect(ok, s"genesis pair at artifact period=$artifactPeriod must verify at the same tree step")
    }
  }

  test("runtime-offset pair is rejected by the current-view verifier even after its claimed activation") {
    val rot: Long = 1L
    val offset: Long = 2L
    val tipOrdinal: Long = 4L
    CanonicalOperatorConsensusFixture.make.use { operator =>
      val operatorId = operator.resolvedPair.operatorPeerId
      for {
        (_, metrics) <- setup
        wireBytes <- signWithCanonical(operator, 1)
        runtimePair = operator.resolvedPair.copy(
          kes = operator.resolvedPair.kes.copy(offset = offset),
          effectiveFromPeriod = EtaPeriod(offset)
        )
        ok <- {
          implicit val m: Metrics[IO] = metrics
          KesGossipVerification.verifyAttestation[IO](
            testMessageBytes,
            wireBytes,
            operatorId,
            operatorId.value,
            tipOrdinal = tipOrdinal,
            operatorKeys = runtimePair,
            etaRotationSnapshots = rot,
            logger = logger
          )
        }
      } yield expect(!ok, "runtime registration must be resolved from exact candidate-parent history, never current view")
    }
  }
}
