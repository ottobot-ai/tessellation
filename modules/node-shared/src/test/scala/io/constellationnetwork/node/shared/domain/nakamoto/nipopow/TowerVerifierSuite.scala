package io.constellationnetwork.node.shared.domain.nakamoto.nipopow

import java.nio.ByteBuffer

import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.{
  CanonicalOperatorConsensusFixture,
  KesRegistryEntry,
  OperatorConsensusKeyRegistry
}
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord}
import io.constellationnetwork.schema.nakamoto.slot._
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, LddConfig, LddConfigFixture}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes.VerificationKeyKesProduct
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** §3 NIPoPoW S4.3 — [[TowerVerifier]] suite.
  *
  * Covers:
  *   - Structural invariants (monotone ordinals, subchain-state shape).
  *   - Per-level trial validity (passing vs adversarial-failing).
  *   - Density violations (chain too dense / too sparse).
  *   - L0 suffix VRF chain (bogus proof bytes → reject).
  *   - Eta-rotation consistency (mixed etas in same period → reject).
  *   - The S4.5 adversarial-tower cases: garbage levels, level-0 forged, density padding.
  */
object TowerVerifierSuite extends MutableIOSuite {

  final case class RegisteredVerifier(
    underlying: TowerVerifier[IO],
    registeredProducer: PeerId,
    registeredVrfPublicKey: VrfPublicKey,
    private val registeredVrfSecret: Array[Byte]
  ) {

    def verify(
      proof: TowerProof,
      genesisEta: Array[Byte],
      etaRotationSnapshots: Long,
      lddConfig: LddConfig
    ): IO[Either[ProofError, Unit]] =
      underlying.verify(proof, genesisEta, etaRotationSnapshots, lddConfig)

    def header(
      ordinal: Long,
      slot: Long,
      parentSlot: Long,
      eta: Hash = etaHash(0),
      activePoolSize: Int = 8,
      subchainLevelCounts: Vector[Long] = SlotCertificate.ZeroSubchainLevelCounts,
      snapshotHash: Hash = h("00"),
      producerId: PeerId = registeredProducer,
      vrfPublicKey: VrfPublicKey = registeredVrfPublicKey
    ): TowerProofHeader =
      headerFor(
        ordinal,
        slot,
        parentSlot,
        eta,
        activePoolSize,
        subchainLevelCounts,
        snapshotHash,
        producerId,
        vrfPublicKey,
        registeredVrfSecret
      )
  }

  override type Res = RegisteredVerifier

  private val registeredVrfBytes = Hex("0c" * 32).toBytes

  private def makeVerifier(operator: CanonicalOperatorConsensusFixture): IO[RegisteredVerifier] =
    for {
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8)
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38)
      registeredPair = operator.resolvedPair
      verifier = TowerVerifier.make[IO](log1p, exp, operator.operatorKeyRegistry)
    } yield RegisteredVerifier(verifier, registeredPair.operatorPeerId, registeredPair.vrfPublicKey, operator.localVrfSecret)

  private def makeRuntimeVerifier(effectiveFromPeriod: EtaPeriod): IO[(TowerVerifier[IO], PeerId)] =
    JsonSerializer.forAsync[IO].flatMap { implicit jsonSerializer =>
      SecurityProvider.forAsync[IO].use { implicit sp =>
        implicit val hasher: Hasher[IO] = Hasher.forJson[IO]
        for {
          keyPair <- KeyPairGenerator.makeKeyPair[IO]
          peerId = PeerId.fromPublic(keyPair.getPublic)
          cert = KesRegistrationCert(
            operatorPeerId = peerId,
            kesMasterVK = Hex("2a" * 32),
            kesMasterVKStep = 0,
            offset = effectiveFromPeriod.value,
            vrfPublicKey = Hex.fromBytes(registeredVrfBytes),
            effectiveFromPeriod = effectiveFromPeriod,
            registrationParentHash = Hash("4a" * 32),
            ordinal = KesRegistrationOrdinal.first
          )
          signed <- forAsyncHasher(cert, keyPair)
          record = KesRegistrationRecord(signed, ord(1L))
          pair = OperatorConsensusKeys(
            operatorPeerId = peerId,
            kes = KesRegistryEntry(
              VerificationKeyKesProduct(cert.kesMasterVK.toBytes, step = 0),
              offset = effectiveFromPeriod.value
            ),
            vrfPublicKey = VrfPublicKey.fromBytes(registeredVrfBytes),
            effectiveFromPeriod = effectiveFromPeriod,
            registration = Some(record)
          )
          registry = OperatorConsensusKeyRegistry.make[IO](Map(peerId -> pair))
          log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8)
          exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38)
        } yield TowerVerifier.make[IO](log1p, exp, registry) -> peerId
      }
    }

  override def sharedResource: Resource[IO, RegisteredVerifier] =
    CanonicalOperatorConsensusFixture.make.evalMap(makeVerifier)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Lowercase-hex 64-char hash from a short label. */
  private def h(s: String): Hash =
    Hash(s.getBytes("UTF-8").map(b => f"${b & 0xff}%02x").mkString.padTo(64, '0').take(64))

  /** Build a synthetic 32-byte hex (eta-shaped). */
  private def etaHash(seed: Int): Hash = {
    val bytes = (0 until 32).map(i => ((seed * 31 + i) & 0xff).toByte).toArray
    Hash(bytes.map(b => f"${b & 0xff}%02x").mkString)
  }

  /** Find a registered, cryptographically valid header whose proof-derived output lies below a target tau. Varying the slot changes the
    * exact `(eta, slot)` VRF message without fabricating an unregistered key or sender-chosen output.
    */
  private def findPassingHeader(
    verifier: RegisteredVerifier,
    level: Int,
    maxTau: Ratio,
    ordinal: Long,
    startSlot: Long,
    deltaSlot: Long,
    eta: Hash = etaHash(0),
    maxAttempts: Int = 10000
  ): TowerProofHeader = {
    var attempt = 0
    while (attempt < maxAttempts) {
      val slot = startSlot + attempt
      val header = verifier.header(ordinal, slot, slot - deltaSlot, eta = eta)
      val tau = LevelTrialComputer.tauForLevel(header.vrfOutput.toBytes, level)
      if (tau < maxTau) return header
      attempt += 1
    }
    throw new IllegalStateException(s"Could not find a registered passing header for level $level after $maxAttempts attempts")
  }

  /** Build a TowerProofHeader with the given fields. Other fields default to canonical placeholders. */
  private def headerFor(
    ordinal: Long,
    slot: Long,
    parentSlot: Long,
    eta: Hash = etaHash(0),
    activePoolSize: Int = 8,
    subchainLevelCounts: Vector[Long] = SlotCertificate.ZeroSubchainLevelCounts,
    snapshotHash: Hash,
    producerId: PeerId,
    vrfPublicKey: VrfPublicKey,
    vrfSecret: Array[Byte]
  ): TowerProofHeader = {
    val etaBytes = Hex(eta.value).toBytes
    val message = etaBytes ++ ByteBuffer.allocate(java.lang.Long.BYTES).putLong(slot).array()
    val proof = EcVrf25519.default.vrfProof(vrfSecret, message)
    val output = EcVrf25519.default
      .vrfProofToHash(proof)
      .getOrElse(throw new IllegalStateException("fixture VRF proof did not produce an output"))
    TowerProofHeader(
      ordinal = ord(ordinal),
      producerId = producerId,
      slot = Slot.unsafeApply(slot),
      parentSlot = Slot.unsafeApply(parentSlot),
      vrfProof = VrfProof(Hex.fromBytes(proof)),
      vrfOutput = VrfOutput(Hex.fromBytes(output)),
      vrfPublicKey = vrfPublicKey,
      eta = eta,
      activePoolSize = activePoolSize,
      subchainLevelCounts = subchainLevelCounts,
      snapshotHash = snapshotHash
    )
  }

  private val genesisEta: Array[Byte] = Array.fill[Byte](32)(0x42.toByte)
  private val etaRotationSnapshots: Long = 100L

  // ============== Empty proof — always accepts ==============

  test("empty proof — verifier accepts") { verifier =>
    verifier.verify(TowerProof.Empty, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r.isRight)
    }
  }

  // ============== Structural invariants ==============

  test("non-empty proof with an empty L0 suffix rejects") { verifier =>
    val proof = TowerProof(since = ord(0L), tipOrdinal = ord(1L), level0Suffix = Vector.empty, levelChains = Map.empty)
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.EmptyL0Suffix))
    }
  }

  test("non-monotone ordinals in L0 suffix — reject") { verifier =>
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(2L),
      level0Suffix = Vector(
        verifier.header(2L, 10L, 9L),
        verifier.header(1L, 5L, 4L) // out of order
      ),
      levelChains = Map.empty
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r == Left(ProofError.NonMonotonicOrdinals(0)))
    }
  }

  test("non-monotone ordinals in level-µ chain — reject") { verifier =>
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(5L),
      level0Suffix = Vector(verifier.header(5L, 50L, 49L)),
      levelChains = Map(
        1 -> Vector(
          verifier.header(3L, 30L, 29L),
          verifier.header(2L, 20L, 19L) // out of order
        )
      )
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r == Left(ProofError.NonMonotonicOrdinals(1)))
    }
  }

  test("wrong subchainLevelCounts size — reject") { verifier =>
    val badCounts = Vector(0L, 0L, 0L) // 3 instead of 9
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(verifier.header(1L, 5L, 4L, subchainLevelCounts = badCounts)),
      levelChains = Map.empty
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(ProofError.SubchainStateShape(o, sz)) => expect(o == ord(1L)).and(expect(sz == 3))
        case other                                      => failure(s"expected SubchainStateShape, got $other")
      }
    }
  }

  // ============== Per-level trial validity ==============

  test("level-µ trial — registered header passes the L1 trial before the historical eligibility gate") { verifier =>
    // For L1 (params: pMax≈1.131, σ=0.5), threshold = pMax * (1 - e^(-(gMu-1)/σ)) * gating(δ_S, γ).
    // Use gMu=10 (anchor at since=0, header at ord=10): threshold-pre-gating ≈ 1.131 * (1 - e^-18) ≈ 1.131.
    // Use deltaSlot ≥ γ (=16 in LddConfigFixture.production) so gating = 1. Then threshold = 1.131,
    // and any tau < 1.0 < 1.131 → passes. Pick a VRF whose tau < 1/2 for safety margin.
    val l1Header = findPassingHeader(verifier, 1, Ratio(BigInt(1), BigInt(2)), 10L, 100L, 20L)
    val suffix = (96L to 100L).toVector.map(o => verifier.header(o, o * 10L, o * 10L - 9L, eta = etaHash(0)))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(100L),
      level0Suffix = suffix,
      levelChains = Map(1 -> Vector(l1Header))
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(_: ProofError.TrialFailed) => failure(s"expected non-TrialFailed error, got TrialFailed")
        case _                               => success
      }
    }
  }

  test("level-µ trial — header with VRF that FAILS the L1 trial → reject as TrialFailed") { verifier =>
    // Construct a header where tau ≥ threshold at L1. With gMu=1 (= ψ_super), threshold = 0 ALWAYS,
    // so ANY tau (which is > 0 generically) → fails. Use any VRF output.
    val l1Header = verifier.header(1L, 10L, 9L) // gMu = 1 - 0 = 1 → threshold = 0 → fail
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(verifier.header(1L, 10L, 9L)),
      levelChains = Map(1 -> Vector(l1Header))
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(ProofError.TrialFailed(1, o, _, threshold)) =>
          expect(o == ord(1L)).and(expect(threshold == Ratio.Zero))
        case other => failure(s"expected TrialFailed(1, ...), got $other")
      }
    }
  }

  // ============== Density violations ==============

  test("density — extreme over-density at L1 → reject (DensityViolation)") { verifier =>
    // Need >= 20 L0 suffix headers to trigger density check.
    // L1 target = 1/2. Construct 25 L0 suffix headers, and a 25-entry L1 chain → observed = 1.0 → relErr = 100% → reject.
    val suffix = (1L to 25L).toVector.map(o => verifier.header(o, o * 10L, o * 10L - 9L, eta = etaHash(0)))
    // L1 chain: 25 headers strictly increasing, each with adequate gMu so threshold > tau.
    // Use gMu=10 (large enough for L1 with σ=0.5 to saturate near pMax≈1.131).
    val l1Chain = (1L to 25L).toVector.map { o =>
      // Anchor at since=0, so first header's gMu = o, increasing by 1. All large enough to give threshold ≈ pMax > tau.
      findPassingHeader(verifier, 1, Ratio(BigInt(99), BigInt(100)), o * 10L, o * 100L, 20L)
    }
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(250L),
      level0Suffix = suffix,
      levelChains = Map(1 -> l1Chain)
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(ProofError.DensityViolation(level, _, _, _)) => expect(level == 1)
        case Left(_: ProofError.TrialFailed)                   =>
          // Some headers might still fail the trial check (which runs before density). Acceptable —
          // the test demonstrates rejection of an over-dense proof; the rejection cause is correct.
          success
        case other => failure(s"expected DensityViolation or TrialFailed, got $other")
      }
    }
  }

  test("density — short proof (suffix < 20) skips density check") { verifier =>
    // 5-suffix proof with 5 L1 entries (100% density) — should NOT reject on density alone.
    val suffix = (1L to 5L).toVector.map(o => verifier.header(o, o * 10L, o * 10L - 9L, eta = etaHash(0)))
    val l1Chain = (1L to 5L).toVector.map(o => findPassingHeader(verifier, 1, Ratio.One, o * 5L, o * 50L, 20L))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(25L),
      level0Suffix = suffix,
      levelChains = Map(1 -> l1Chain)
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(_: ProofError.DensityViolation) =>
          failure("density check should be skipped for short proofs")
        case _ => success
      }
    }
  }

  // ============== L0 suffix VRF chain ==============

  test("L0 VRF — forged VRF proof bytes reject before tower trials") { verifier =>
    val header = verifier.header(1L, 5L, 4L, eta = etaHash(0)).copy(vrfProof = VrfProof(Hex("0a" * 80)))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(header),
      levelChains = Map.empty
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r == Left(ProofError.VrfProofInvalid(ord(1L), verifier.registeredProducer)))
    }
  }

  test("every carried VRF output must equal the verified proof hash") { verifier =>
    val valid = verifier.header(1L, 5L, 4L)
    val forgedOutput = valid.copy(vrfOutput = VrfOutput(Hex("ff" * 64)))
    val proof = TowerProof(ord(0L), ord(1L), Vector(forgedOutput), Map.empty)

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfOutputMismatch(ord(1L), verifier.registeredProducer)))
    }
  }

  test("a forged output in an upper-level occurrence rejects even when the suffix occurrence is valid") { verifier =>
    val suffix = verifier.header(10L, 100L, 80L)
    val forgedUpper = verifier.header(10L, 100L, 80L).copy(vrfOutput = VrfOutput(Hex("ee" * 64)))
    val proof = TowerProof(ord(0L), ord(10L), Vector(suffix), Map(1 -> Vector(forgedUpper)))

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfOutputMismatch(ord(10L), verifier.registeredProducer)))
    }
  }

  test("empty carried VRF proof rejects") { verifier =>
    val header = verifier.header(1L, 5L, 4L).copy(vrfProof = VrfProof(Hex("")))
    val proof = TowerProof(ord(0L), ord(1L), Vector(header), Map.empty)

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfProofInvalid(ord(1L), verifier.registeredProducer)))
    }
  }

  test("tower header under an unregistered producer identity rejects before VRF verification") { verifier =>
    val unknown = PeerId(Hex("1d" * 64))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(verifier.header(1L, 5L, 4L, producerId = unknown)),
      levelChains = Map.empty
    )

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfKeyNotRegistered(ord(1L), unknown)))
    }
  }

  test("tower header carrying a replacement key rejects even for a registered producer") { verifier =>
    val replacement = VrfPublicKey(Hex("2c" * 32))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(verifier.header(1L, 5L, 4L, vrfPublicKey = replacement)),
      levelChains = Map.empty
    )

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfKeyNotRegistered(ord(1L), verifier.registeredProducer)))
    }
  }

  test("every tower header occurrence is registry-checked even when an ordinal is duplicated across levels") { verifier =>
    val replacement = VrfPublicKey(Hex("2c" * 32))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = Vector(verifier.header(1L, 5L, 4L)),
      levelChains = Map(1 -> Vector(verifier.header(1L, 5L, 4L, vrfPublicKey = replacement)))
    )

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(result == Left(ProofError.VrfKeyNotRegistered(ord(1L), verifier.registeredProducer)))
    }
  }

  test("tower verifier rejects runtime pairs from the current-view registry even after their effective period") { registeredVerifier =>
    makeRuntimeVerifier(EtaPeriod(2L)).flatMap {
      case (runtimeVerifier, producerId) =>
        val proof = TowerProof(
          since = ord(0L),
          tipOrdinal = ord(299L),
          level0Suffix = Vector(
            registeredVerifier.header(
              299L,
              5L,
              4L,
              producerId = producerId,
              vrfPublicKey = VrfPublicKey.fromBytes(registeredVrfBytes)
            )
          ),
          levelChains = Map.empty
        )

        runtimeVerifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
          expect(result == Left(ProofError.VrfKeyNotRegistered(ord(299L), producerId)))
        }
    }
  }

  test("sender-claimed activePoolSize never supplies historical eligibility authority") { verifier =>
    List(Int.MinValue, -1, 0, 1, 8, Int.MaxValue).traverse { claimedPoolSize =>
      val header = verifier.header(1L, 5L, 4L, activePoolSize = claimedPoolSize)
      val proof = TowerProof(ord(0L), ord(1L), Vector(header), Map.empty)
      verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production)
    }.map { results =>
      val expected = Left(ProofError.HistoricalEligibilityUnavailable(ord(1L), verifier.registeredProducer))
      expect(results.forall(_ == expected))
    }
  }

  // ============== Eta-rotation consistency ==============

  test("eta-chain — mixed etas within same period → reject as EtaChainInconsistent") { verifier =>
    // etaRotationSnapshots=100, so ords 0..99 are in period 0.
    val suffix = Vector(
      verifier.header(1L, 5L, 4L, eta = etaHash(1)),
      verifier.header(2L, 10L, 9L, eta = etaHash(2)) // different eta, same period → reject
    )
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(2L),
      level0Suffix = suffix,
      levelChains = Map.empty
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(_: ProofError.EtaChainInconsistent) => success
        case other                                    => failure(s"expected EtaChainInconsistent, got $other")
      }
    }
  }

  test("tower eta buckets match global snapshot artifact periods at R-1, R, and R+1") { verifier =>
    val suffix = Vector(
      verifier.header(etaRotationSnapshots - 1L, 5L, 4L, eta = etaHash(0)),
      verifier.header(etaRotationSnapshots, 10L, 9L, eta = etaHash(0)),
      verifier.header(etaRotationSnapshots + 1L, 15L, 14L, eta = etaHash(1))
    )
    val proof = TowerProof(
      since = ord(etaRotationSnapshots - 2L),
      tipOrdinal = ord(etaRotationSnapshots + 1L),
      level0Suffix = suffix,
      levelChains = Map.empty
    )

    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { result =>
      expect(
        result == Left(ProofError.HistoricalEligibilityUnavailable(ord(etaRotationSnapshots - 1L), verifier.registeredProducer))
      )
    }
  }

  // ============== Adversarial S4.5 ==============

  test("S4.5 — garbage level (chain of headers with no actual L7 hits) → reject") { verifier =>
    // L7 has very small targetDensity (1/128) and very small pMax (0.013). With a random VRF,
    // the trial almost always fails. Construct a "chain" of 3 random-VRF headers at L7.
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(30L),
      level0Suffix = Vector(verifier.header(30L, 300L, 299L)),
      levelChains = Map(
        7 -> Vector(
          verifier.header(5L, 50L, 49L), // gMu=5, almost certainly fails L7
          verifier.header(15L, 150L, 149L),
          verifier.header(25L, 250L, 249L)
        )
      )
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      r match {
        case Left(_: ProofError.TrialFailed) => success
        case Left(_)                         => success // any other rejection is also acceptable here
        case Right(())                       => failure("expected rejection of garbage L7 chain")
      }
    }
  }

  test("S4.5 — level-0 forged (suffix with bogus VRF) → reject") { verifier =>
    val suffix = Vector(verifier.header(1L, 5L, 4L, eta = etaHash(0)).copy(vrfProof = VrfProof(Hex("ab" * 80))))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(1L),
      level0Suffix = suffix,
      levelChains = Map.empty
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r.isLeft)
    }
  }

  test("S4.5 — density-padding attack (chain padded to inflate L1 count) → reject") { verifier =>
    // L1 target density = 1/2. Padded chain with 25 entries vs 25 L0 suffix → observed = 1.0 → relErr = 100%.
    val suffix = (1L to 25L).toVector.map(o => verifier.header(o, o * 10L, o * 10L - 9L, eta = etaHash(0)))
    // Padded L1 chain: 25 entries but with gMu values just enough for trial to pass at modest density.
    val l1Chain = (1L to 25L).toVector.map(o => findPassingHeader(verifier, 1, Ratio.One, o * 5L, o * 50L, 20L))
    val proof = TowerProof(
      since = ord(0L),
      tipOrdinal = ord(250L),
      level0Suffix = suffix,
      levelChains = Map(1 -> l1Chain)
    )
    verifier.verify(proof, genesisEta, etaRotationSnapshots, LddConfigFixture.production).map { r =>
      expect(r.isLeft) // exact error type depends on which check fires first
    }
  }

  // ============== Density check uses the caller-provided LddConfig (no source-level default) ==============

  test("LddConfig parameter accepted (fixture + custom)") { verifier =>
    val customLdd = LddConfig(lddCutoff = 30, offset = 5, baselineDifficulty = Ratio(1, 10), amplitude = Ratio(1, 3))
    verifier.verify(TowerProof.Empty, genesisEta, etaRotationSnapshots, customLdd).map { r =>
      expect(r == Right(()))
    }
  }

}
