package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, KesRegistryEntry}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.kes._
import io.constellationnetwork.security.signature.Signing
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Slice 16 — coverage for [[ShardCheckpointEquivocationValidator]] per `HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §10.1.
  *
  * 8 tests, 1:1 with the task spec:
  *   - happy path (all 6 steps pass on a real equivocation — same signer, same `(shardId, parentCheckpointHash)`, two distinct children)
  *   - 7 negative-path tests, one per [[ShardCheckpointEquivocationRejection]] case the design lists:
  *     - different parents → `ParentMismatch`
  *     - different shards → `ShardMismatch`
  *     - same child → `SameChildHash`
  *     - signer not in childA → `SignerNotPresent.OnChildA`
  *     - signer not in childB → `SignerNotPresent.OnChildB`
  *     - invalid signature on childA → `InvalidEd25519Signature.OnChildA`
  *     - invalid signature on childB → `InvalidEd25519Signature.OnChildB`
  *
  * '''Fixture strategy.''' Mirrors [[SlashableEvidenceValidatorSuite]]: real KES key (via `OperationalKeyMaker.bootstrap`) + real Ed25519
  * long-term key (via `KeyPairGenerator.makeKeyPair`). Each child checkpoint envelope is built then signed through the same producer-path
  * recipe (Hasher of `signingPreimage` → `getBytes` → real Ed25519 + real KES sigs). A faked-signature happy path would not catch
  * verification regressions in the validator.
  *
  * '''Two distinct children.''' We vary `shardOrdinal` (and through it the canonical preimage hash) while keeping `(shardId,
  * parentCheckpointHash)` identical — that's the precise equivocation algebra. The producer's actual chain-continuity rule would forbid a
  * single committee member from signing two distinct children at the same parent in honest operation; here we deliberately produce the
  * second signature to simulate adversarial behaviour.
  */
object ShardCheckpointEquivocationValidatorSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  // ===== fixture builders =====

  /** Captures everything needed to produce equivocation evidence for a single committee member: real Ed25519 + real KES key materials, the
    * static parent hash + shard id, and a `kesMakerResource` that bootstraps the KES signer for actual sig production.
    */
  private case class EquivocationFixture(
    offenderKp: KeyPair,
    offenderPeerId: PeerId,
    offenderKesVk: VerificationKeyKesProduct,
    kesMakerResource: Resource[IO, OperationalKeyMakerAlgebra[IO]],
    shardId: ShardId,
    parentCheckpointHash: Hash,
    epoch: EtaPeriod
  ) {

    def kesRegistry: KesRegistry[IO] =
      KesRegistry.make[IO](Map(offenderPeerId -> KesRegistryEntry(offenderKesVk, offset = 0L)))

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
        derivedStateDelta = ShardDerivedStateDelta.empty,
        emittedReceipts = List.empty,
        committeeSignatures = NonEmptyList.of(placeholderSig),
        epoch = epoch
      )
      for {
        preimageHash <- Hasher[IO].hash(shell.signingPreimage)
        msgBytes = preimageHash.getBytes
        edSig <- Signing.signData[IO](msgBytes)(offenderKp.getPrivate)
        kesStep <- kesMaker.currentPeriod
        kesSigEither <- kesMaker.signAt(kesStep, msgBytes)
        kesSigBytes = kesSigEither match {
          case Right(s) => OperationalKeyMaker.encodeSignature(s)
          case Left(_)  => Array.empty[Byte]
        }
        sig = CommitteeMemberSignature(
          peerId = offenderPeerId,
          vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x42.toByte)),
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

  /** Construct an equivocation fixture. All committee-VRF / KES bookkeeping is bootstrapped from deterministic seeds so the test is
    * reproducible across runs.
    */
  private def freshFixture(implicit sp: SecurityProvider[IO]): IO[EquivocationFixture] =
    for {
      offenderKp <- KeyPairGenerator.makeKeyPair[IO]
      store <- SecureStore.inMemory[IO]
      seed = Array.fill[Byte](32)(0x22.toByte)
      kesMaterial <- OperationalKeyMaker.generateFreshKesKeyMaterial[IO](seed, height = (2, 2), offset = 0L)
      (encodedSk, masterVk) = kesMaterial
      _ <- store.write("kes-sk.bin", encodedSk)
    } yield
      EquivocationFixture(
        offenderKp = offenderKp,
        offenderPeerId = PeerId.fromPublic(offenderKp.getPublic),
        offenderKesVk = masterVk,
        kesMakerResource = OperationalKeyMaker.make[IO](store, "kes-sk.bin", etaPeriodLength = 100L),
        shardId = ShardId.unsafeApply(0),
        parentCheckpointHash = Hash.fromBytes("parent-shard-checkpoint".getBytes("UTF-8")),
        epoch = EtaPeriod(0L)
      )

  private val anchorA: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L))
  private val anchorB: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(101L))
  private val ordinalA: ShardOrdinal = ShardOrdinal(1L)
  private val ordinalB: ShardOrdinal = ShardOrdinal(2L)

  // ===== happy path — all 6 steps pass =====

  test("happy: valid equivocation evidence validates (same signer, same shard+parent, distinct children)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
    } yield expect(result.isRight)
  }

  // ===== step 1 — parent mismatch =====

  test("step 1: parent mismatch — childA.parentCheckpointHash != childB.parentCheckpointHash rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherParent = Hash.fromBytes("parent-shard-checkpoint-other".getBytes("UTF-8"))
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.ParentMismatch(a, b, ev)) =>
          expect(a == f.parentCheckpointHash)
            .and(expect(b == otherParent))
            .and(expect(ev == f.parentCheckpointHash))
      }
  }

  // ===== step 2 — shard mismatch =====

  test("step 2: shard mismatch — childA.shardId != childB.shardId rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      otherShard = ShardId.unsafeApply(1)
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.ShardMismatch(a, b, ev)) =>
          expect(a == f.shardId).and(expect(b == otherShard)).and(expect(ev == f.shardId))
      }
  }

  // ===== step 3 — same child =====

  test("step 3: same child — identical canonical child hashes rejects (duplicate retransmission, not equivocation)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.SameChildHash(h)) =>
          expect(h == result._2)
      }
  }

  // ===== step 4 — signer not in childA =====

  test("step 4: signer not in childA — equivocatingSigner missing from childA.committeeSignatures rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildA(signer)) =>
          expect(signer == f.offenderPeerId)
      }
  }

  // ===== step 4 — signer not in childB =====

  test("step 4: signer not in childB — equivocatingSigner missing from childB.committeeSignatures rejects") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.SignerNotPresent.OnChildB(signer)) =>
          expect(signer == f.offenderPeerId)
      }
  }

  // ===== step 5 — invalid Ed25519 signature on childA =====

  test("step 5: invalid Ed25519 — tampered ed25519Sig on childA rejects (no cryptographic proof of equivocation)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildA) => success
      }
  }

  // ===== step 5 — invalid Ed25519 signature on childB =====

  test("step 5: invalid Ed25519 — tampered ed25519Sig on childB rejects (no cryptographic proof of equivocation)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.InvalidEd25519Signature.OnChildB) => success
      }
  }

  // ===== step 6 — invalid KES signature (additional optional coverage) =====

  test("step 6: invalid KES — tampered kesProductSig on childA rejects (KES forward-security proof fails)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        val validator = ShardCheckpointEquivocationValidator.make[IO](f.kesRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA) => success
      }
  }

  // ===== step 6 — missing KesRegistry entry (fail-closed) =====

  test("step 6: missing KesRegistry entry — signer not registered ⇒ KES verify fails closed (rejects on childA)") { res =>
    implicit val (h, sp) = res
    for {
      f <- freshFixture
      result <- f.kesMakerResource.use { kesMaker =>
        // EMPTY registry — no entry for the offender. Per slashing safety bar the validator fails closed: we can't establish the
        // cryptographic proof without the master VK, so we reject — matches SlashableEvidenceValidator's no-registry behaviour.
        val emptyRegistry = KesRegistry.empty[IO]
        val validator = ShardCheckpointEquivocationValidator.make[IO](emptyRegistry)
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
        case Left(ShardCheckpointEquivocationRejection.InvalidKesSignature.OnChildA) => success
      }
  }

}
