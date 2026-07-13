package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.security.KeyPair

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.slashing.{MetagraphAttestation, SlashableEvidence, SlashingRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

/** Slice S4a — coverage for [[SlashableEvidenceValidator]] per `SLASHING-DESIGN.md` §4.1.
  *
  * The core tests map 1:1 to the design checklist:
  *   - happy path (all 9 steps pass on a real equivocation)
  *   - 9 negative-path tests, one per `SlashingRejection` variant
  *
  * Plus 1 property test: an honest committee member who signs exactly ONE attestation can't produce SlashableEvidence (step 4 always fires
  * when both binaryHashes are equal).
  *
  * '''Fixture strategy.''' The operative KES+VRF pair comes from a long-term-signed genesis record validated by `L0GenesisLoader` and
  * rematerialized through the immutable atomic registry. The matching local secrets build attestations through the production crypto path;
  * a faked-signature happy-path test would not catch registry or KES verification regressions.
  */
object SlashableEvidenceValidatorSuite extends MutableIOSuite {

  override type Res = ((Hasher[IO], SecurityProvider[IO]), CanonicalOperatorConsensusFixture)

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      operator <- CanonicalOperatorConsensusFixture.make
    } yield ((h, sp), operator)

  // ===== fixture builders =====

  /** A complete equivocation fixture — one canonically registered committee member signs two DIFFERENT metagraph binaries on the SAME
    * parent hash. A separate submitter signs the bounty digest with their long-term Ed25519 key.
    */
  private case class EquivocationFixture(
    operator: CanonicalOperatorConsensusFixture,
    submitterKp: KeyPair,
    submitterPeerId: PeerId,
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHashA: Hash,
    binaryHashB: Hash,
    eta: Array[Byte],
    kTarget: Int,
    sigmaOperatorKey: Ratio
  ) {

    val offenderPeerId: PeerId = operator.resolvedPair.operatorPeerId
    def offenderVrfSk: Array[Byte] = operator.localVrfSecret
    def offenderVrfVk: Array[Byte] = operator.resolvedPair.vrfPublicKey.toBytes
    def kesMakerResource: Resource[IO, OperationalKeyMakerAlgebra[IO]] = Resource.pure(operator.kesSigner)

    def canonicalResolution(offencePeriod: EtaPeriod, vrfEta: Array[Byte]): IO[SlashingOperatorKeyResolution] =
      operator.operatorKeyRegistry.get(offenderPeerId).map {
        case Some(keys) => SlashingOperatorKeyResolution.Resolved(keys, offencePeriod, vrfEta)
        case None =>
          SlashingOperatorKeyResolution.HistoricalStateUnavailable(
            HistoricalStateUnavailableReason.MissingOperatorRegistration
          )
      }

    def keyResolver(): SlashingOperatorKeyResolver[IO] =
      SlashingOperatorKeyResolver.make[IO] { (peerId, context) =>
        val contextMatches = context match {
          case SlashingOffenceContext.MetagraphAdmission(mg, parent, binary) =>
            mg == metagraphAddress && parent == parentHash && Set(binaryHashA, binaryHashB).contains(binary)
          case _ => false
        }
        if (peerId == offenderPeerId && contextMatches) canonicalResolution(EtaPeriod.Zero, eta)
        else
          IO.pure(
            SlashingOperatorKeyResolution.HistoricalStateUnavailable(
              HistoricalStateUnavailableReason.AmbiguousHistoricalState
            )
          )
      }

    def keyResolver(resolution: SlashingOperatorKeyResolution): SlashingOperatorKeyResolver[IO] =
      SlashingOperatorKeyResolver.make[IO] { (peerId, context) =>
        val contextMatches = context match {
          case SlashingOffenceContext.MetagraphAdmission(mg, parent, binary) =>
            mg == metagraphAddress && parent == parentHash && Set(binaryHashA, binaryHashB).contains(binary)
          case _ => false
        }
        IO.pure(
          if (peerId == offenderPeerId && contextMatches) resolution
          else
            SlashingOperatorKeyResolution.HistoricalStateUnavailable(
              HistoricalStateUnavailableReason.AmbiguousHistoricalState
            )
        )
      }

    /** Build a [[MetagraphAttestation]] body for the given binary, using the real KES + VRF keys.
      *
      * The KES sig is produced by the OperationalKeyMaker; the committee VRF proof is produced by `CommitteeSortition.isInCommittee`. No
      * outer Ed25519 envelope — slashing evidence consumes bare attestation bodies (see the schema scaladoc for why).
      */
    def signAttestation(
      binary: Hash,
      sortition: CommitteeSortition[IO],
      kesMaker: OperationalKeyMakerAlgebra[IO]
    )(implicit h: Hasher[IO]): IO[MetagraphAttestation] =
      for {
        drawOpt <- sortition.isInCommittee(offenderVrfSk, eta, metagraphAddress, parentHash, sigmaOperatorKey, kTarget)
        proof = drawOpt.map(_._1).getOrElse(Array.empty[Byte])
        // Canonical attestation message bytes — same recipe the production sender uses.
        msgBytes <- MetagraphCommitteeGate.messageBytes[IO](offenderPeerId, metagraphAddress, parentHash, binary)
        kesStep <- kesMaker.currentPeriod
        kesSigEither <- kesMaker.signAt(kesStep, msgBytes)
        kesSig = kesSigEither match {
          case Right(sig) => OperationalKeyMaker.encodeSignature(sig)
          case Left(_)    => Array.empty[Byte]
        }
      } yield
        MetagraphAttestation(
          peerId = offenderPeerId,
          metagraphAddress = metagraphAddress,
          parentHash = parentHash,
          binaryHash = binary,
          committeeVrfProof = Hex.fromBytes(proof),
          vrfPublicKey = Hex.fromBytes(offenderVrfVk),
          kesSignature = Hex.fromBytes(kesSig),
          senderTreeStep = kesStep
        )

    def buildEvidence(
      sortition: CommitteeSortition[IO],
      kesMaker: OperationalKeyMakerAlgebra[IO]
    )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[SlashableEvidence] =
      for {
        attA <- signAttestation(binaryHashA, sortition, kesMaker)
        attB <- signAttestation(binaryHashB, sortition, kesMaker)
        evidence <- buildEvidenceFrom(attA, attB)
      } yield evidence

    def buildEvidenceFrom(
      attA: MetagraphAttestation,
      attB: MetagraphAttestation
    )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[SlashableEvidence] =
      for {
        digestBytes <- SlashableEvidenceValidator.bountyDigestBytes[IO](attA, attB, submitterPeerId)
        sigBytes <- Signing.signData[IO](digestBytes)(submitterKp.getPrivate)
      } yield
        SlashableEvidence(
          evidenceA = attA,
          evidenceB = attB,
          submitterId = submitterPeerId,
          bountySignature = Signature(Hex.fromBytes(sigBytes))
        )
  }

  /** Construct a fixture with K · σ = 1 (everyone in-committee — keeps the VRF draw saturating so we always get a valid proof).
    *
    * Distinct binary hashes on the same parent ⇒ equivocation.
    */
  private def freshFixture(
    operator: CanonicalOperatorConsensusFixture
  )(implicit sp: SecurityProvider[IO]): IO[EquivocationFixture] =
    for {
      submitterKp <- KeyPairGenerator.makeKeyPair[IO]
    } yield
      EquivocationFixture(
        operator = operator,
        submitterKp = submitterKp,
        submitterPeerId = PeerId.fromPublic(submitterKp.getPublic),
        metagraphAddress = Address.fromBytes("mg-equiv".getBytes("UTF-8")),
        parentHash = Hash.fromBytes("parent-1".getBytes("UTF-8")),
        binaryHashA = Hash.fromBytes("binary-A".getBytes("UTF-8")),
        binaryHashB = Hash.fromBytes("binary-B".getBytes("UTF-8")),
        eta = Array.fill[Byte](32)(0x07.toByte),
        kTarget = 1,
        sigmaOperatorKey = Ratio(1, 1)
      )

  // ===== happy path — all 9 steps pass =====

  test("happy: full equivocation evidence validates") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield matches(result) { case SlashingValidationResult.Valid(_) => success }
  }

  // ===== step 1 — identity mismatch =====

  test("step 1: identity mismatch — evidenceA.peerId != evidenceB.peerId rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      otherPeer = PeerId.fromPublic(otherKp.getPublic)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          attB <- f.signAttestation(f.binaryHashB, sortition, kesMaker)
          // Mutate the body of attB to have a different peerId — but keep the (now stale) sig.
          // The validator should reject at step 1 BEFORE reaching the KES check.
          mutatedB = attB.copy(peerId = otherPeer)
          evidence <- f.buildEvidenceFrom(attA, mutatedB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.IdentityMismatch(a, b)) =>
          expect(a == f.offenderPeerId).and(expect(b == otherPeer))
      }
  }

  // ===== step 2 — subject mismatch =====

  test("step 2: subject mismatch — different metagraphAddresses rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherMg = Address.fromBytes("mg-other".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          attB <- f.signAttestation(f.binaryHashB, sortition, kesMaker)
          mutatedB = attB.copy(metagraphAddress = otherMg)
          evidence <- f.buildEvidenceFrom(attA, mutatedB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.SubjectMismatch(a, b)) =>
          expect(a == f.metagraphAddress).and(expect(b == otherMg))
      }
  }

  // ===== step 3 — parent mismatch =====

  test("step 3: parent mismatch — different parentHash rejects (load-bearing identity)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherParent = Hash.fromBytes("parent-2".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          attB <- f.signAttestation(f.binaryHashB, sortition, kesMaker)
          mutatedB = attB.copy(parentHash = otherParent)
          evidence <- f.buildEvidenceFrom(attA, mutatedB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.ParentMismatch(a, b)) =>
          expect(a == f.parentHash).and(expect(b == otherParent))
      }
  }

  // ===== step 4 — duplicate binary =====

  test("step 4: duplicate binary — identical binaryHash rejects (not equivocation)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          // Same binary in B — duplicate retransmission, not equivocation.
          attB <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          evidence <- f.buildEvidenceFrom(attA, attB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.DuplicateBinary(b)) => expect(b == f.binaryHashA)
      }
  }

  // ===== step 5 — invalid KES signature =====

  test("step 5: invalid KES — tampered kesSignature on evidenceA rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          attB <- f.signAttestation(f.binaryHashB, sortition, kesMaker)
          // Tamper KES sig bytes — flip the first byte. Resulting bytes will decode (likely) but
          // verify under master VK will fail.
          tamperedHex = {
            val raw = attA.kesSignature.toBytes
            raw(0) = (raw(0) ^ 0xff).toByte
            Hex.fromBytes(raw)
          }
          tamperedA = attA.copy(kesSignature = tamperedHex)
          evidence <- f.buildEvidenceFrom(tamperedA, attB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.InvalidKesSignature.OnEvidenceA) => success
      }
  }

  // ===== step 6 — invalid committee VRF =====

  test("step 6: invalid committee VRF — tampered VRF proof on evidenceB rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          attA <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          attB <- f.signAttestation(f.binaryHashB, sortition, kesMaker)
          // Replace the VRF proof on B with garbage of the same length so the EcVrf25519 verifier rejects.
          garbageProof = Array.fill[Byte](attB.committeeVrfProof.toBytes.length)(0xee.toByte)
          tamperedB = attB.copy(committeeVrfProof = Hex.fromBytes(garbageProof))
          evidence <- f.buildEvidenceFrom(attA, tamperedB)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.InvalidCommitteeVrf.OnEvidenceB) => success
      }
  }

  test("step 6: evidence-carried VRF key is never authority") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      results <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val replacementKey = Array.fill[Byte](f.offenderVrfVk.length)(0x33.toByte)
        val wrongPair = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(vrfPublicKey = VrfPublicKey.fromBytes(replacementKey)),
          EtaPeriod.Zero,
          f.eta
        )
        val unavailable = SlashingOperatorKeyResolver.unavailable[IO](HistoricalStateUnavailableReason.HistoryPruned)

        def validator(resolver: SlashingOperatorKeyResolver[IO]) =
          SlashableEvidenceValidator.make[IO](
            keyResolver = resolver,
            sortition = sortition,
            slashedReader = SlashedSeenReader.neverSlashed[IO]
          )

        def validate(resolver: SlashingOperatorKeyResolver[IO], evidence: SlashableEvidence) =
          validator(resolver).validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )

        for {
          original <- f.buildEvidence(sortition, kesMaker)
          carriedReplacementA = original.evidenceA.copy(vrfPublicKey = Hex.fromBytes(replacementKey))
          carriedReplacement <- f.buildEvidenceFrom(carriedReplacementA, original.evidenceB)
          carriedResult <- validate(f.keyResolver(), carriedReplacement)
          wrongPairResult <- validate(f.keyResolver(wrongPair), original)
          unavailableResult <- validate(unavailable, original)
        } yield (carriedResult, wrongPairResult, unavailableResult)
      }
    } yield {
      val expected = SlashingValidationResult.Invalid(SlashingRejection.InvalidCommitteeVrf.OnEvidenceA)
      expect.same(expected, results._1) &&
      expect.same(expected, results._2) &&
      matches(results._3) {
        case SlashingValidationResult.Unverifiable(
              SlashingUnverifiableReason.HistoricalKeyStateUnavailable(_, HistoricalStateUnavailableReason.HistoryPruned)
            ) =>
          success
      }
    }
  }

  test("historical pair: stale or wrong wire KES step cannot prove guilt") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        for {
          periodOne <- f.canonicalResolution(EtaPeriod(1L), f.eta)
          validator = SlashableEvidenceValidator.make[IO](
            f.keyResolver(periodOne),
            sortition,
            SlashedSeenReader.neverSlashed[IO]
          )
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(evidence, f.sigmaOperatorKey, f.sigmaOperatorKey, f.kTarget, currentEpoch = 1L)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.InvalidKesSignature.OnEvidenceA) => success
      }
  }

  test("same metagraph parent across eta rotation is not equivocation") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val resolver = SlashingOperatorKeyResolver.make[IO] { (peerId, context) =>
          context match {
            case SlashingOffenceContext.MetagraphAdmission(mg, parent, binary)
                if peerId == f.offenderPeerId && mg == f.metagraphAddress && parent == f.parentHash && binary == f.binaryHashA =>
              f.canonicalResolution(EtaPeriod.Zero, f.eta)
            case SlashingOffenceContext.MetagraphAdmission(mg, parent, binary)
                if peerId == f.offenderPeerId && mg == f.metagraphAddress && parent == f.parentHash && binary == f.binaryHashB =>
              f.canonicalResolution(EtaPeriod(1L), Array.fill[Byte](32)(0x08.toByte))
            case _ =>
              IO.pure(
                SlashingOperatorKeyResolution.HistoricalStateUnavailable(
                  HistoricalStateUnavailableReason.AmbiguousHistoricalState
                )
              )
          }
        }
        val validator = SlashableEvidenceValidator.make[IO](resolver, sortition, SlashedSeenReader.neverSlashed[IO])
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(evidence, f.sigmaOperatorKey, f.sigmaOperatorKey, f.kTarget, currentEpoch = 1L)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(
              SlashingRejection.AdmissionDrawPeriodMismatch(EtaPeriod(0L), EtaPeriod(1L))
            ) =>
          success
      }
  }

  test("historical pair: not-yet-active registration is unverifiable and never guilt") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val future = EtaPeriod(2L)
        val premature = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(
            kes = KesRegistryEntry(f.operator.resolvedPair.kes.vk, offset = future.value),
            effectiveFromPeriod = future
          ),
          EtaPeriod(1L),
          f.eta
        )
        val validator = SlashableEvidenceValidator.make[IO](
          f.keyResolver(premature),
          sortition,
          SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(evidence, f.sigmaOperatorKey, f.sigmaOperatorKey, f.kTarget, currentEpoch = 1L)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Unverifiable(
              SlashingUnverifiableReason.ResolvedPairNotActive(EtaPeriod(2L), EtaPeriod(1L))
            ) =>
          success
      }
  }

  test("historical pair: KES from a different pair cannot be combined with the registered VRF key") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherKes <- OperationalKeyMaker
        .generateFreshKesKeyMaterial[IO](Array.fill[Byte](32)(0x66.toByte), height = (2, 2), offset = 0L)
        .map(_._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val halfPair = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(kes = KesRegistryEntry(otherKes, f.operator.resolvedPair.kes.offset)),
          EtaPeriod.Zero,
          f.eta
        )
        val validator = SlashableEvidenceValidator.make[IO](
          f.keyResolver(halfPair),
          sortition,
          SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(evidence, f.sigmaOperatorKey, f.sigmaOperatorKey, f.kTarget, currentEpoch = 0L)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.InvalidKesSignature.OnEvidenceA) => success
      }
  }

  // ===== step 7 — already slashed =====

  test("step 7: already slashed — SlashedSeenReader returns true rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val seenReader = SlashedSeenReader.fromSet[IO](
          Set((f.offenderPeerId, f.metagraphAddress, f.parentHash))
        )
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = seenReader
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.AlreadySlashed(peer, mg, parent)) =>
          expect(peer == f.offenderPeerId)
            .and(expect(mg == f.metagraphAddress))
            .and(expect(parent == f.parentHash))
      }
  }

  // ===== step 8 — evidence window expired =====

  test("step 8: evidence window expired — currentEpoch > eventEpoch + window rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          // The resolver proves offencePeriod=0; 200 > 0 + 100, so the evidence is expired.
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 200L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.EvidenceWindowExpired(cur, evt, win)) =>
          expect(cur == 200L).and(expect(evt == 0L)).and(expect(win == 100L))
      }
  }

  // ===== step 9 — invalid bounty signature =====

  test("step 9: invalid bounty signature — tampered bountySignature rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          // Re-sign the bounty digest with the WRONG key — verifier will reject under
          // submitterId's pubkey.
          digestBytes <- SlashableEvidenceValidator.bountyDigestBytes[IO](
            evidence.evidenceA,
            evidence.evidenceB,
            evidence.submitterId
          )
          wrongSig <- Signing.signData[IO](digestBytes)(otherKp.getPrivate)
          tamperedEvidence = evidence.copy(bountySignature = Signature(Hex.fromBytes(wrongSig)))
          r <- validator.validate(
            evidence = tamperedEvidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.InvalidBountySignature) => success
      }
  }

  // ===== property: single honest attestation can't be slashed =====

  test("property: a single honest attestation duplicated cannot produce SlashableEvidence (step 4 always fires)") { res =>
    implicit val (h, sp) = res._1
    // The honest committee member signs exactly ONE binary on a parent. Even if an adversary tries to construct evidence by passing the
    // same attestation twice (or any two attestations with the same binaryHash), step 4 (distinct binaries) MUST fire. This is the
    // load-bearing property of the slashing safety bar: requiring TWO genuinely different binaries on the same parent.
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          keyResolver = f.keyResolver(),
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          honest <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          // Pass the SAME attestation as both evidenceA and evidenceB — identical binaryHash.
          evidence <- f.buildEvidenceFrom(honest, honest)
          r <- validator.validate(
            evidence = evidence,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(SlashingRejection.DuplicateBinary(b)) => expect(b == f.binaryHashA)
      }
  }
}
