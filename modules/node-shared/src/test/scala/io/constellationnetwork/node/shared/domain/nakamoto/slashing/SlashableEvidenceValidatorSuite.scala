package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.security.KeyPair

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.slashing.{MetagraphAttestation, SlashableEvidence, SlashingRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.signature.signature.Signature
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import weaver.MutableIOSuite

/** Slice S4a — coverage for [[SlashableEvidenceValidator]] per `SLASHING-DESIGN.md` §4.1.
  *
  * 10 tests, 1:1 with the design checklist:
  *   - happy path (all 9 steps pass on a real equivocation)
  *   - 9 negative-path tests, one per `SlashingRejection` variant
  *
  * Plus 1 property test: an honest committee member who signs exactly ONE attestation can't produce SlashableEvidence (step 4 always fires
  * when both binaryHashes are equal).
  *
  * '''Fixture strategy.''' Real KES key (via `OperationalKeyMaker.bootstrap`) + real committee VRF (via `CommitteeSortition.make`) + real
  * Ed25519 long-term key (via `KeyPairGenerator.makeKeyPair`). Building the attestations through the same path the production sender uses
  * keeps the test true to the algebra — a faked-signature happy-path test would not catch KES verification regressions in the validator.
  */
object SlashableEvidenceValidatorSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  // ===== fixture builders =====

  /** A complete equivocation fixture — single committee member (`offenderKp`) with both KES and VRF keys, signs two DIFFERENT metagraph
    * binaries on the SAME parent hash. Plus a submitter (`submitterKp`) who signs the bounty digest with their long-term Ed25519 key.
    */
  private case class EquivocationFixture(
    offenderKp: KeyPair,
    offenderPeerId: PeerId,
    offenderVrfSk: Array[Byte],
    offenderVrfVk: Array[Byte],
    offenderKesVk: VerificationKeyKesProduct,
    kesMakerResource: Resource[IO, OperationalKeyMakerAlgebra[IO]],
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

    def kesRegistry: KesRegistry[IO] =
      KesRegistry.make[IO](Map(offenderPeerId -> KesRegistryEntry(offenderKesVk, offset = 0L)))

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
  private def freshFixture(implicit sp: SecurityProvider[IO]): IO[EquivocationFixture] =
    for {
      offenderKp <- KeyPairGenerator.makeKeyPair[IO]
      submitterKp <- KeyPairGenerator.makeKeyPair[IO]
      // Deterministic VRF SK for reproducibility — committee sortition will draw the same proof every run.
      offenderVrfSk = Array.fill[Byte](32)(0x55.toByte)
      offenderVrfVk = new EcVrf25519().getVerificationKey(offenderVrfSk)
      // Build the KES store & bootstrap a fresh master key for the offender.
      store <- SecureStore.inMemory[IO]
      seed = Array.fill[Byte](32)(0x11.toByte)
      kesMaterial <- OperationalKeyMaker.generateFreshKesKeyMaterial[IO](seed, height = (2, 2), offset = 0L)
      (encodedSk, masterVk) = kesMaterial
      _ <- store.write("kes-sk.bin", encodedSk)
    } yield
      EquivocationFixture(
        offenderKp = offenderKp,
        offenderPeerId = PeerId.fromPublic(offenderKp.getPublic),
        offenderVrfSk = offenderVrfSk,
        offenderVrfVk = offenderVrfVk,
        offenderKesVk = masterVk,
        kesMakerResource = OperationalKeyMaker.make[IO](store, "kes-sk.bin", etaPeriodLength = 100L),
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
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(
            evidence = evidence,
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield expect(result.isRight)
  }

  // ===== step 1 — identity mismatch =====

  test("step 1: identity mismatch — evidenceA.peerId != evidenceB.peerId rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      otherPeer = PeerId.fromPublic(otherKp.getPublic)
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.IdentityMismatch(a, b)) =>
          expect(a == f.offenderPeerId).and(expect(b == otherPeer))
      }
  }

  // ===== step 2 — subject mismatch =====

  test("step 2: subject mismatch — different metagraphAddresses rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherMg = Address.fromBytes("mg-other".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.SubjectMismatch(a, b)) =>
          expect(a == f.metagraphAddress).and(expect(b == otherMg))
      }
  }

  // ===== step 3 — parent mismatch =====

  test("step 3: parent mismatch — different parentHash rejects (load-bearing identity)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherParent = Hash.fromBytes("parent-2".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.ParentMismatch(a, b)) =>
          expect(a == f.parentHash).and(expect(b == otherParent))
      }
  }

  // ===== step 4 — duplicate binary =====

  test("step 4: duplicate binary — identical binaryHash rejects (not equivocation)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.DuplicateBinary(b)) => expect(b == f.binaryHashA)
      }
  }

  // ===== step 5 — invalid KES signature =====

  test("step 5: invalid KES — tampered kesSignature on evidenceA rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.InvalidKesSignature.OnEvidenceA) => success
      }
  }

  // ===== step 6 — invalid committee VRF =====

  test("step 6: invalid committee VRF — tampered VRF proof on evidenceB rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.InvalidCommitteeVrf.OnEvidenceB) => success
      }
  }

  // ===== step 7 — already slashed =====

  test("step 7: already slashed — SlashedSeenReader returns true rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val seenReader = SlashedSeenReader.fromSet[IO](
          Set((f.offenderPeerId, f.metagraphAddress, f.parentHash))
        )
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
          sortition = sortition,
          slashedReader = seenReader
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          r <- validator.validate(
            evidence = evidence,
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.AlreadySlashed(peer, mg, parent)) =>
          expect(peer == f.offenderPeerId)
            .and(expect(mg == f.metagraphAddress))
            .and(expect(parent == f.parentHash))
      }
  }

  // ===== step 8 — evidence window expired =====

  test("step 8: evidence window expired — currentEpoch > eventEpoch + window rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO],
          evidenceWindowEpochs = 100L
        )
        for {
          evidence <- f.buildEvidence(sortition, kesMaker)
          // eventEpoch=5, current=200 → 200 > 5 + 100 ⇒ expired
          r <- validator.validate(
            evidence = evidence,
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 200L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.EvidenceWindowExpired(cur, evt, win)) =>
          expect(cur == 200L).and(expect(evt == 5L)).and(expect(win == 100L))
      }
  }

  // ===== step 9 — invalid bounty signature =====

  test("step 9: invalid bounty signature — tampered bountySignature rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherKp <- KeyPairGenerator.makeKeyPair[IO]
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
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
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.InvalidBountySignature) => success
      }
  }

  // ===== property: single honest attestation can't be slashed =====

  test("property: a single honest attestation duplicated cannot produce SlashableEvidence (step 4 always fires)") { res =>
    implicit val (h, sp) = res
    // The honest committee member signs exactly ONE binary on a parent. Even if an adversary tries to construct evidence by passing the
    // same attestation twice (or any two attestations with the same binaryHash), step 4 (distinct binaries) MUST fire. This is the
    // load-bearing property of the slashing safety bar: requiring TWO genuinely different binaries on the same parent.
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val sortition = CommitteeSortition.make[IO]
        val validator = SlashableEvidenceValidator.make[IO](
          kesRegistry = f.kesRegistry,
          sortition = sortition,
          slashedReader = SlashedSeenReader.neverSlashed[IO]
        )
        for {
          honest <- f.signAttestation(f.binaryHashA, sortition, kesMaker)
          // Pass the SAME attestation as both evidenceA and evidenceB — identical binaryHash.
          evidence <- f.buildEvidenceFrom(honest, honest)
          r <- validator.validate(
            evidence = evidence,
            eta = f.eta,
            sigmaForEvidenceA = f.sigmaOperatorKey,
            sigmaForEvidenceB = f.sigmaOperatorKey,
            kTarget = f.kTarget,
            currentEpoch = 5L,
            eventEpoch = 5L
          )
        } yield r
      }
    } yield
      matches(result) {
        case Left(SlashingRejection.DuplicateBinary(b)) => expect(b == f.binaryHashA)
      }
  }
}
