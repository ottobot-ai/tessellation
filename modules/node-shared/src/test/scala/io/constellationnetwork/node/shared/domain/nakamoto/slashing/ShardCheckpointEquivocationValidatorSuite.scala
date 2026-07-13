package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.{CanonicalOperatorConsensusFixture, KesRegistryEntry}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT, VrfPublicKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._
import io.constellationnetwork.security.vrf.EcVrf25519
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Slice 16 — coverage for [[ShardCheckpointEquivocationValidator]] per `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.1.
  *
  * 8 tests, 1:1 with the task spec:
  *   - happy path (all identity and cryptographic checks pass on two competing children at one shard height and execution period)
  *   - 7 negative-path tests, one per [[ShardCheckpointEquivocationRejection]] case the design lists:
  *     - different parents → `ParentMismatch`
  *     - different shards → `ShardMismatch`
  *     - same child → `SameChildHash`
  *     - signer not in childA → `SignerNotPresent.OnChildA`
  *     - signer not in childB → `SignerNotPresent.OnChildB`
  *     - invalid signature on childA → `InvalidEd25519Signature.OnChildA`
  *     - invalid signature on childB → `InvalidEd25519Signature.OnChildB`
  *
  * '''Fixture strategy.''' Mirrors [[SlashableEvidenceValidatorSuite]]: the operative identity is a long-term-signed, loader-validated,
  * rooted genesis KES+VRF pair. Each child is signed through the producer crypto recipe with that pair's matching local Ed25519, KES, and
  * VRF secrets. A faked-signature happy path would not catch registry or verification regressions.
  *
  * '''Two distinct children.''' Both children use the same shard ordinal and parent; the signed GL0 anchor/slot differs so their canonical
  * preimages are distinct. A different shard ordinal is a different height and cannot be treated as equivocation.
  */
object ShardCheckpointEquivocationValidatorSuite extends MutableIOSuite {

  override type Res = ((Hasher[IO], SecurityProvider[IO]), CanonicalOperatorConsensusFixture)

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      operator <- CanonicalOperatorConsensusFixture.make
    } yield ((h, sp), operator)

  // ===== fixture builders =====

  private val anchorA: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L))
  private val anchorB: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(101L))
  private val ordinalA: ShardOrdinal = ShardOrdinal(1L)
  private val ordinalB: ShardOrdinal = ShardOrdinal(1L)

  /** Captures the rooted operator identity and exact signed checkpoint contexts needed to produce equivocation evidence.
    */
  private case class EquivocationFixture(
    operator: CanonicalOperatorConsensusFixture,
    shardId: ShardId,
    parentCheckpointHash: Hash,
    epoch: EtaPeriod,
    shardEta: Array[Byte]
  ) {

    val offenderPeerId: PeerId = operator.resolvedPair.operatorPeerId
    def offenderVrfSk: Array[Byte] = operator.localVrfSecret
    def kesMakerResource: Resource[IO, OperationalKeyMakerAlgebra[IO]] = Resource.pure(operator.kesSigner)

    private def contextMatches(context: SlashingOffenceContext): Boolean = context match {
      case SlashingOffenceContext.ShardCheckpointExecution(sid, parent, ordinal, anchor, declaredPeriod) =>
        sid == shardId && parent == parentCheckpointHash && ordinal == ordinalA &&
        Set(anchorA, anchorB).contains(anchor) && declaredPeriod == epoch
      case _ => false
    }

    def canonicalResolution: IO[SlashingOperatorKeyResolution] =
      operator.operatorKeyRegistry.get(offenderPeerId).map {
        case Some(keys) => SlashingOperatorKeyResolution.Resolved(keys, epoch, shardEta)
        case None =>
          SlashingOperatorKeyResolution.HistoricalStateUnavailable(
            HistoricalStateUnavailableReason.MissingOperatorRegistration
          )
      }

    def keyResolver(): SlashingOperatorKeyResolver[IO] =
      SlashingOperatorKeyResolver.make[IO] { (peerId, context) =>
        if (peerId == offenderPeerId && contextMatches(context)) canonicalResolution
        else
          IO.pure(
            SlashingOperatorKeyResolution.HistoricalStateUnavailable(
              HistoricalStateUnavailableReason.AmbiguousHistoricalState
            )
          )
      }

    def keyResolver(resolution: SlashingOperatorKeyResolution): SlashingOperatorKeyResolver[IO] =
      SlashingOperatorKeyResolver.make[IO] { (peerId, context) =>
        IO.pure(
          if (peerId == offenderPeerId && contextMatches(context)) resolution
          else
            SlashingOperatorKeyResolution.HistoricalStateUnavailable(
              HistoricalStateUnavailableReason.AmbiguousHistoricalState
            )
        )
      }

    /** Build a [[ShardCheckpoint]] with the offender's REAL Ed25519 + REAL KES signature contribution.
      *
      * The envelope-construction dance follows the producer's recipe (`ShardCheckpointProducer.produce`):
      *   1. Build the envelope with a placeholder signature so we can compute the canonical preimage hash.
      *   1. Hash the preimage via `Hasher[F]` (this is the bytes both Ed25519 and KES sign).
      *   1. Compute the real Ed25519 sig over the preimage-hash bytes.
      *   1. Compute the real KES product sig over the same bytes at the current tree-internal step.
      *   1. Replace the placeholder with the real [[CommitteeMemberSignature]].
      */
    def buildChild(
      shardOrdinal: ShardOrdinal,
      gl0AnchorOrdinal: SnapshotOrdinal,
      kesMaker: OperationalKeyMakerAlgebra[IO]
    )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpoint] = {
      val placeholderSig = CommitteeMemberSignature(
        peerId = offenderPeerId,
        vrfProof = Hex(""),
        ed25519Sig = Hex(""),
        kesProductSig = Hex(""),
        kesTreeStep = 0
      )
      val shell = ShardCheckpoint(
        shardId = shardId,
        parentCheckpointHash = parentCheckpointHash,
        shardOrdinal = shardOrdinal,
        gl0AnchorOrdinal = gl0AnchorOrdinal,
        slot = SlotT.unsafeApply(gl0AnchorOrdinal.value.value),
        derivedStateDelta = ShardDerivedStateDelta.empty,
        committeeSignatures = NonEmptyList.of(placeholderSig),
        epoch = epoch
      )
      for {
        preimageHash <- Hasher[IO].hash(shell.signingPreimage)
        msgBytes = preimageHash.getBytes
        edSig <- operator.signWithOperatorIdentity(msgBytes)
        kesStep <- kesMaker.currentPeriod
        kesSigEither <- kesMaker.signAt(kesStep, msgBytes)
        kesSigBytes = kesSigEither match {
          case Right(s) => OperationalKeyMaker.encodeSignature(s)
          case Left(_)  => Array.empty[Byte]
        }
        slotBytes = java.nio.ByteBuffer.allocate(8).putLong(shell.slot.value.value).array()
        vrfProof = EcVrf25519.default.vrfProof(offenderVrfSk, shardEta ++ slotBytes)
        sig = CommitteeMemberSignature(
          peerId = offenderPeerId,
          vrfProof = Hex.fromBytes(vrfProof),
          ed25519Sig = Hex.fromBytes(edSig),
          kesProductSig = Hex.fromBytes(kesSigBytes),
          kesTreeStep = kesStep
        )
      } yield shell.copy(committeeSignatures = NonEmptyList.of(sig))
    }

    /** Build a child where the offender's signature is REMOVED from the committee list (used by the SignerNotPresent tests). */
    def buildChildWithoutOffender(
      shardOrdinal: ShardOrdinal,
      gl0AnchorOrdinal: SnapshotOrdinal,
      kesMaker: OperationalKeyMakerAlgebra[IO]
    )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpoint] =
      // Build with offender then replace the sig list with one belonging to a DIFFERENT peer. Keeps the envelope shape valid
      // (NonEmptyList requires at least one element) while removing the offender's attribution.
      buildChild(shardOrdinal, gl0AnchorOrdinal, kesMaker).flatMap { signed =>
        KeyPairGenerator.makeKeyPair[IO].map { strangerKp =>
          val strangerPeer = PeerId.fromPublic(strangerKp.getPublic)
          val strangerSig = signed.committeeSignatures.head.copy(peerId = strangerPeer)
          signed.copy(committeeSignatures = NonEmptyList.of(strangerSig))
        }
      }
  }

  /** Construct an equivocation fixture from the canonically validated rooted genesis pair and its matching local secrets. */
  private def freshFixture(operator: CanonicalOperatorConsensusFixture): IO[EquivocationFixture] =
    IO.pure(
      EquivocationFixture(
        operator = operator,
        shardId = ShardId.unsafeApply(0),
        parentCheckpointHash = Hash.fromBytes("parent-shard-checkpoint".getBytes("UTF-8")),
        epoch = EtaPeriod(0L),
        shardEta = Array.fill[Byte](32)(0x24.toByte)
      )
    )

  // ===== happy path =====

  test("fully authenticated pair remains Unverifiable until signer exclusivity is specified") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Unverifiable(
              SlashingUnverifiableReason.CheckpointSignerExclusivityNotSpecified
            ) =>
          success
      }
  }

  // ===== step 1 — parent mismatch =====

  test("step 1: parent mismatch — childA.parentCheckpointHash != childB.parentCheckpointHash rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherParent = Hash.fromBytes("parent-shard-checkpoint-other".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          mutatedB = childB.copy(parentCheckpointHash = otherParent)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = mutatedB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.ParentMismatch(a, b, ev)) =>
          expect(a == f.parentCheckpointHash)
            .and(expect(b == otherParent))
            .and(expect(ev == f.parentCheckpointHash))
      }
  }

  // ===== step 2 — shard mismatch =====

  test("step 2: shard mismatch — childA.shardId != childB.shardId rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherShard = ShardId.unsafeApply(1)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          mutatedB = childB.copy(shardId = otherShard)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = mutatedB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.ShardMismatch(a, b, ev)) =>
          expect(a == f.shardId).and(expect(b == otherShard)).and(expect(ev == f.shardId))
      }
  }

  test("different shard ordinals are not two children at one height and cannot slash") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ShardOrdinal(2L), anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            childA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(
              ShardCheckpointEquivocationRejection.ShardOrdinalMismatch(ShardOrdinal(1L), ShardOrdinal(2L))
            ) =>
          success
      }
  }

  test("different execution periods are different committee draws and cannot slash") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          nextPeriodB = childB.copy(epoch = EtaPeriod(1L))
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            childA,
            nextPeriodB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(
              ShardCheckpointEquivocationRejection.ExecutionPeriodMismatch(EtaPeriod(0L), EtaPeriod(1L))
            ) =>
          success
      }
  }

  // ===== step 3 — same child =====

  test("step 3: same child — identical canonical child hashes rejects (duplicate retransmission, not equivocation)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          child <- f.buildChild(ordinalA, anchorA, kesMaker)
          // Pass the SAME child as both A and B — identical canonical hash.
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = child,
            childB = child,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
          expectedHash <- Hasher[IO].hash(child.signingPreimage)
        } yield (r, expectedHash)
      }
    } yield
      matches(result._1) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.SameChildHash(h)) =>
          expect(h == result._2)
      }
  }

  // ===== step 4 — signer not in childA =====

  test("step 4: signer not in childA — equivocatingSigner missing from childA.committeeSignatures rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChildWithoutOffender(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildA(signer)) =>
          expect(signer == f.offenderPeerId)
      }
  }

  // ===== step 4 — signer not in childB =====

  test("step 4: signer not in childB — equivocatingSigner missing from childB.committeeSignatures rejects") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChildWithoutOffender(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildB(signer)) =>
          expect(signer == f.offenderPeerId)
      }
  }

  // ===== step 5 — invalid Ed25519 signature on childA =====

  test("step 5: invalid Ed25519 — tampered ed25519Sig on childA rejects (no cryptographic proof of equivocation)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          // Tamper the Ed25519 sig bytes on childA — flip the first byte. The resulting bytes won't verify under the offender's VK.
          origSig = childA.committeeSignatures.head
          tamperedBytes = {
            val raw = origSig.ed25519Sig.toBytes
            raw(0) = (raw(0) ^ 0xff).toByte
            raw
          }
          tamperedSig = origSig.copy(ed25519Sig = Hex.fromBytes(tamperedBytes))
          tamperedChildA = childA.copy(committeeSignatures = NonEmptyList.of(tamperedSig))
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = tamperedChildA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildA) => success
      }
  }

  // ===== step 5 — invalid Ed25519 signature on childB =====

  test("step 5: invalid Ed25519 — tampered ed25519Sig on childB rejects (no cryptographic proof of equivocation)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          origSig = childB.committeeSignatures.head
          tamperedBytes = {
            val raw = origSig.ed25519Sig.toBytes
            raw(0) = (raw(0) ^ 0xff).toByte
            raw
          }
          tamperedSig = origSig.copy(ed25519Sig = Hex.fromBytes(tamperedBytes))
          tamperedChildB = childB.copy(committeeSignatures = NonEmptyList.of(tamperedSig))
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = tamperedChildB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildB) => success
      }
  }

  // ===== step 6 — invalid KES signature (additional optional coverage) =====

  test("step 6: invalid KES — tampered kesProductSig on childA rejects (KES forward-security proof fails)") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          // Tamper KES sig bytes on childA — flip the first byte. Decode may or may not succeed depending on bit-position; either way the
          // verify under master VK will fail.
          origSig = childA.committeeSignatures.head
          tamperedBytes = {
            val raw = origSig.kesProductSig.toBytes
            raw(0) = (raw(0) ^ 0xff).toByte
            raw
          }
          tamperedSig = origSig.copy(kesProductSig = Hex.fromBytes(tamperedBytes))
          tamperedChildA = childA.copy(committeeSignatures = NonEmptyList.of(tamperedSig))
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = tamperedChildA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA) => success
      }
  }

  test("wire KES step cannot select the verification key step") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          wrongStep = childA.committeeSignatures.head.copy(kesTreeStep = 1)
          mutatedA = childA.copy(committeeSignatures = NonEmptyList.of(wrongStep))
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            mutatedA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA) => success
      }
  }

  test("checkpoint VRF possession proof is verified under the resolved pair") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver())
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          original = childA.committeeSignatures.head
          garbage = Array.fill[Byte](original.vrfProof.toBytes.length)(0x6a.toByte)
          mutatedA = childA.copy(committeeSignatures = NonEmptyList.of(original.copy(vrfProof = Hex.fromBytes(garbage))))
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            mutatedA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidVrfProof.OnChildA) => success
      }
  }

  test("checkpoint VRF proof cannot be reinterpreted under a mismatched registered pair") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherVrfSk = Array.fill[Byte](32)(0x7b.toByte)
      otherVrfVk = EcVrf25519.default.getVerificationKey(otherVrfSk)
      result <- f.kesMakerResource.use { kesMaker =>
        val mismatched = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(vrfPublicKey = VrfPublicKey.fromBytes(otherVrfVk)),
          f.epoch,
          f.shardEta
        )
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver(mismatched))
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            childA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidVrfProof.OnChildA) => success
      }
  }

  test("not-yet-active checkpoint pair is Unverifiable rather than guilt") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val future = EtaPeriod(1L)
        val premature = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(
            kes = KesRegistryEntry(f.operator.resolvedPair.kes.vk, offset = future.value),
            effectiveFromPeriod = future
          ),
          f.epoch,
          f.shardEta
        )
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver(premature))
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            childA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Unverifiable(
              SlashingUnverifiableReason.ResolvedPairNotActive(EtaPeriod(1L), EtaPeriod(0L))
            ) =>
          success
      }
  }

  test("half-pair substitution cannot combine an unrelated KES key with the registered VRF key") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      otherKes <- OperationalKeyMaker
        .generateFreshKesKeyMaterial[IO](Array.fill[Byte](32)(0x71.toByte), height = (2, 2), offset = 0L)
        .map(_._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val substituted = SlashingOperatorKeyResolution.Resolved(
          f.operator.resolvedPair.copy(kes = KesRegistryEntry(otherKes, f.operator.resolvedPair.kes.offset)),
          f.epoch,
          f.shardEta
        )
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.keyResolver(substituted))
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            f.shardId,
            f.parentCheckpointHash,
            childA,
            childB,
            f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Invalid(ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA) => success
      }
  }

  // ===== missing exact historical pair (fail-closed without guilt) =====

  test("missing historical registration is Unverifiable and never a slash verdict") { res =>
    implicit val (h, sp) = res._1
    for {
      f <- freshFixture(res._2)
      result <- f.kesMakerResource.use { kesMaker =>
        val unavailable = SlashingOperatorKeyResolver.unavailable[IO](
          HistoricalStateUnavailableReason.MissingOperatorRegistration
        )
        val validator = ShardCheckpointEquivocationValidator.make[IO](unavailable)
        for {
          childA <- f.buildChild(ordinalA, anchorA, kesMaker)
          childB <- f.buildChild(ordinalB, anchorB, kesMaker)
          evidence = ShardCheckpointEquivocationEvidence(
            shardId = f.shardId,
            parentCheckpointHash = f.parentCheckpointHash,
            childA = childA,
            childB = childB,
            equivocatingSigner = f.offenderPeerId
          )
          r <- validator.validate(evidence)
        } yield r
      }
    } yield
      matches(result) {
        case SlashingValidationResult.Unverifiable(
              SlashingUnverifiableReason.HistoricalKeyStateUnavailable(
                _,
                HistoricalStateUnavailableReason.MissingOperatorRegistration
              )
            ) =>
          success
      }
  }

}
