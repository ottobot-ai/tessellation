package io.constellationnetwork.node.shared.domain.nakamoto.slashing

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{RegisteredCheckpointSigner, TestCheckpointDutyValidator}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointGl0AcceptanceManager
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.{InvalidStateProofEvidence, InvalidStateProofRejection}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** WATCHTOWER fraud-proof — coverage for the DETERMINISTIC dispute verdict ([[InvalidStateProofValidator]]).
  *
  * The load-bearing properties under test (the safety + determinism non-negotiables):
  *   - '''A single honest node catches a quorum-signed wrong root''': when the re-derivation does NOT reproduce the committee-attested
  *     root, the verdict is UPHELD.
  *   - '''An honest committee is never slashed''': when the re-derivation REPRODUCES the attested root, the verdict is NOT upheld
  *     (`DisputeNotUpheld`) — regardless of what the challenger CLAIMS in the envelope (the verdict recomputes, never trusts).
  *   - '''Determinism''': the verdict is a pure function of the evidence + the injected re-derivation; two runs over the same inputs agree.
  *   - The header / hash-binding / challenger-signature / double-slash rejections each fire on their own malformation.
  */
object InvalidStateProofValidatorSuite extends MutableIOSuite {

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override type Res = (Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp)

  private val shardZero: ShardId = ShardId(0).get
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val anchor: SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L))
  private val mgA: Address = Address.fromBytes("mgA".getBytes("UTF-8"))

  /** Deliberately unauthenticated signer bytes used only by the explicit certificate-rejection test. Any test that can reach a slash
    * verdict uses [[authenticatedCheckpoint]] instead.
    */
  private def committeeSig(idx: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(PeerId(Hex(f"${idx}%02x" * 64)), Hex("aa" * 80), Hex("bb" * 64), Hex("cc" * 128), 0)

  private def checkpointShell(attestedRoot: Option[Hash], signerId: PeerId): ShardCheckpoint = {
    val bin: Signed[StateChannelSnapshotBinary] =
      Signed(
        StateChannelSnapshotBinary(Hash.empty, Array.emptyByteArray, SnapshotFee.MinValue),
        NonEmptySet.of(SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
      )
    val delta = ShardDerivedStateDelta.empty.copy(
      perMetagraphMptRoots = SortedMap.from(attestedRoot.toList.map(mgA -> _)),
      includedSnapshots = SortedMap(mgA -> NonEmptyList.of(bin))
    )
    val placeholder = CommitteeMemberSignature(signerId, Hex(""), Hex(""), Hex(""), 0)
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = anchor,
      slot = Slot.unsafeApply(1L),
      derivedStateDelta = delta,
      committeeSignatures = NonEmptyList.one(placeholder),
      epoch = epochZero
    )
  }

  private final case class AuthenticatedCheckpoint(
    checkpoint: ShardCheckpoint,
    checkpointSigner: RegisteredCheckpointSigner,
    acceptanceManager: ShardCheckpointGl0AcceptanceManager[IO]
  )

  /** Construct a configured one-of-one execution quorum through the production period-zero registration and certificate-verification path.
    * The registered pair is long-term signed and loader validated before its Ed25519, KES, and VRF evidence can enter the checkpoint.
    */
  private def authenticatedCheckpoint(attestedRoot: Hash)(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO]
  ): IO[AuthenticatedCheckpoint] =
    for {
      checkpointSigner <- RegisteredCheckpointSigner.make
      committeeKeyPair <- KeyPairGenerator.makeKeyPair[IO]
      committeeId = PeerId.fromPublic(committeeKeyPair.getPublic)
      _ <- checkpointSigner.preregisterGenesis(committeeKeyPair, committeeId)
      shell = checkpointShell(Some(attestedRoot), committeeId)
      signature <- checkpointSigner.sign(shell, committeeKeyPair, committeeId, checkpointSigner.defaultShardEta)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      acceptanceManager <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 1,
        etaRotationSnapshots = 1000L,
        committeeMembership = (_, _) => IO.pure(Set(committeeId)),
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        shardAssignment = ShardAssignment.make[IO](numShards = 1),
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => IO.pure(attestedRoot)
      )
      certificate <- acceptanceManager.verifyExecutionCertificate(checkpoint)
      _ <- IO.fromEither(
        certificate.leftMap(reason => new IllegalStateException(s"authenticated checkpoint fixture rejected: $reason"))
      )
    } yield AuthenticatedCheckpoint(checkpoint, checkpointSigner, acceptanceManager)

  private def unauthenticatedCheckpoint(attestedRoot: Hash, nSigners: Int): ShardCheckpoint = {
    val signers = NonEmptyList.fromListUnsafe((1 to nSigners).toList.map(committeeSig))
    checkpointShell(Some(attestedRoot), signers.head.peerId).copy(committeeSignatures = signers)
  }

  /** Build a fraud proof + matching evidence with a REAL challenger Ed25519 signature over the canonical preimage. */
  private def mkEvidence(
    cp: ShardCheckpoint,
    challengerKp: KeyPair,
    submitterId: PeerId,
    claimed: Hash,
    challengerRoot: Hash
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[InvalidStateProofEvidence] =
    h.hash(cp.signingPreimage).flatMap { cpHash =>
      val unsigned = FraudProofEnvelope(
        shardId = cp.shardId,
        disputedCheckpointHash = cpHash,
        metagraphAddress = mgA,
        gl0AnchorOrdinal = cp.gl0AnchorOrdinal,
        claimedDerivation = claimed,
        challengerDerivation = challengerRoot,
        reexecutionWitness = Hex(challengerRoot.value),
        challengerSignature = Hex(""),
        submitterId = submitterId
      )
      h.hash(unsigned.signingPreimage).flatMap { digest =>
        Signing.signData[IO](digest.getBytes)(challengerKp.getPrivate).map { sig =>
          val fp = unsigned.copy(challengerSignature = Hex.fromBytes(sig))
          InvalidStateProofEvidence(
            shardId = cp.shardId,
            disputedCheckpoint = cp,
            metagraphAddress = mgA,
            attestedRoot = cp.derivedStateDelta.perMetagraphMptRoots.getOrElse(mgA, Hash.empty),
            fraudProof = fp
          )
        }
      }
    }

  private def challengerSetup(checkpointSigner: RegisteredCheckpointSigner)(
    implicit sp: SecurityProvider[IO]
  ): IO[(KeyPair, PeerId)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      val peerId = PeerId.fromPublic(kp.getPublic)
      checkpointSigner.preregisterGenesis(kp, peerId).as((kp, peerId))
    }

  private val attested: Hash = Hash("a" * 64)
  private val honestDifferent: Hash = Hash("b" * 64) // honest re-derivation ≠ attested ⇒ UPHELD

  private def makeValidator(
    reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash],
    verifyCertificate: ShardCheckpoint => IO[Either[String, Unit]],
    slashedReader: InvalidStateProofSlashedReader[IO] = InvalidStateProofSlashedReader.neverSlashed[IO]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): InvalidStateProofValidator[IO] =
    InvalidStateProofValidator.make[IO](reDerive, slashedReader, verifyCertificate)

  test("UPHELD: a single honest re-derivation that differs from the quorum-attested root upholds the dispute") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      // reDerive returns a DIFFERENT root than attested ⇒ committee deviated ⇒ upheld.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield expect(res.isRight)
  }

  test(
    "NOT upheld (honest-committee floor): re-derivation reproducing the attested root yields DisputeNotUpheld — even if the challenger LIES"
  ) {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      // Honest re-derivation REPRODUCES the attested root ⇒ committee did NOT deviate. The challenger still CLAIMS a different
      // root (honestDifferent) in the envelope — the verdict must ignore that claim and recompute ⇒ NOT upheld.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(attested)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev <- mkEvidence(cp, kp, pid, claimed = honestDifferent, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.DisputeNotUpheld(attested, attested)))
  }

  test("determinism: the verdict is identical across repeated runs over the same evidence + pure re-derivation") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        r1 <- validator.validate(ev)
        r2 <- validator.validate(ev)
      } yield expect(r1.isRight && r2.isRight && r1.map(_ => ()) == r2.map(_ => ()))
  }

  test("reject: invalid challenger signature (tampered) ⇒ InvalidChallengerSignature, never reaching the verdict") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev0 <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        // Corrupt the signature.
        ev = ev0.copy(fraudProof = ev0.fraudProof.copy(challengerSignature = Hex("00" * 64)))
        res <- validator.validate(ev)
      } yield expect(res == Left(InvalidStateProofRejection.InvalidChallengerSignature))
  }

  test("reject: disputed-checkpoint hash mismatch between the carried checkpoint and the fraud proof") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(honestDifferent)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev0 <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        // Point the fraud proof at a different checkpoint hash (and re-sign so the sig check passes first).
        wrongHash = Hash("f" * 64)
        reUnsigned = ev0.fraudProof.copy(disputedCheckpointHash = wrongHash, challengerSignature = Hex(""))
        digest <- h.hash(reUnsigned.signingPreimage)
        sig <- Signing.signData[IO](digest.getBytes)(kp.getPrivate)
        ev = ev0.copy(fraudProof = reUnsigned.copy(challengerSignature = Hex.fromBytes(sig)))
        res <- validator.validate(ev)
      } yield expect(res match { case Left(_: InvalidStateProofRejection.CheckpointHashMismatch) => true; case _ => false })
  }

  test("FAIL-CLOSED: reDerive returning the Hash.empty cannot-re-derive sentinel ⇒ dispute NOT upheld (never a slash)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      // `Hash.empty` is the production wiring's fail-closed "cannot re-derive" sentinel (`reExecDerivationAtPinnedBase` returned
      // None — the pinned execution-base is unresolvable below this node's retention / not reached, or the derivation OMITted).
      // "This node can't check" is NOT evidence the committee deviated: upholding here would 100%-slash an honest committee
      // on a local retention miss. The verdict must fail closed — reject the dispute, never uphold.
      val reDerive: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
        (_, _, _, _) => IO.pure(Hash.empty)
      for {
        rig <- authenticatedCheckpoint(attested)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        cp = rig.checkpoint
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
      } yield
        expect(
          res == Left(InvalidStateProofRejection.CannotRederive(mgA)),
          s"a dispute this node cannot re-derive must NEVER be upheld (fail-closed CannotRederive, no slash) — got $res"
        )
  }

  test("FAIL-CLOSED: included binaries without an attested root fail certificate structure before replay") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        rig <- authenticatedCheckpoint(attested)
        cpNoRoot = rig.checkpoint.copy(
          derivedStateDelta = rig.checkpoint.derivedStateDelta.copy(perMetagraphMptRoots = SortedMap.empty)
        )
        replayCalls <- cats.effect.Ref.of[IO, Int](0)
        reDerive = (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) =>
          replayCalls.updateAndGet(_ + 1).as(Hash.empty)
        validator = makeValidator(reDerive, rig.acceptanceManager.verifyExecutionCertificate)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        ev <- mkEvidence(cpNoRoot, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        res <- validator.validate(ev)
        calls <- replayCalls.get
        invalidCertificate = res match {
          case Left(_: InvalidStateProofRejection.InvalidCheckpointCertificate) => true
          case _                                                                => false
        }
      } yield
        expect.all(
          invalidCertificate,
          calls == 0
        )
  }

  test("reject: already-slashed (double-slash guard) on (shardId, disputedCheckpointHash)") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        rig <- authenticatedCheckpoint(attested)
        cp = rig.checkpoint
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        cpHash = ev.fraudProof.disputedCheckpointHash
        reader = InvalidStateProofSlashedReader.fromSet[IO](Set((shardZero, cpHash)))
        replayCalls <- cats.effect.Ref.of[IO, Int](0)
        reDerive = (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) =>
          replayCalls.updateAndGet(_ + 1).as(honestDifferent)
        validator = makeValidator(
          reDerive,
          verifyCertificate = rig.acceptanceManager.verifyExecutionCertificate,
          slashedReader = reader
        )
        res <- validator.validate(ev)
        calls <- replayCalls.get
      } yield expect.all(res == Left(InvalidStateProofRejection.AlreadySlashed(shardZero, cpHash)), calls == 0)
  }

  test("authoritative validation uses the exact caller-supplied parent reader, not the validator's staging reader") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      for {
        rig <- authenticatedCheckpoint(attested)
        cp = rig.checkpoint
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        cpHash = ev.fraudProof.disputedCheckpointHash
        replayCalls <- cats.effect.Ref.of[IO, Int](0)
        reDerive = (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) =>
          replayCalls.updateAndGet(_ + 1).as(honestDifferent)
        // The construction-time reader models daemon staging over finalized state. The authoritative GSAM caller has a newer immutable
        // proposal-parent view in which this checkpoint is already slashed; that exact view must win.
        validator = makeValidator(
          reDerive,
          verifyCertificate = rig.acceptanceManager.verifyExecutionCertificate,
          slashedReader = InvalidStateProofSlashedReader.neverSlashed[IO]
        )
        parentReader = InvalidStateProofSlashedReader.fromSet[IO](Set((shardZero, cpHash)))
        result <- validator.validateAgainst(ev, parentReader)
        calls <- replayCalls.get
      } yield expect.all(result == Left(InvalidStateProofRejection.AlreadySlashed(shardZero, cpHash)), calls == 0)
  }

  test("FAIL-CLOSED: unavailable slash-registry state aborts validation before replay") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val unavailable = new IllegalStateException("exact slash-registry state unavailable")
      val reader = new InvalidStateProofSlashedReader[IO] {
        def wasSlashed(shardId: ShardId, disputedCheckpointHash: Hash): IO[Boolean] = IO.raiseError(unavailable)
      }
      for {
        rig <- authenticatedCheckpoint(attested)
        (kp, pid) <- challengerSetup(rig.checkpointSigner)
        ev <- mkEvidence(rig.checkpoint, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        replayCalls <- cats.effect.Ref.of[IO, Int](0)
        reDerive = (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) =>
          replayCalls.updateAndGet(_ + 1).as(honestDifferent)
        validator = makeValidator(
          reDerive,
          verifyCertificate = rig.acceptanceManager.verifyExecutionCertificate,
          slashedReader = reader
        )
        result <- validator.validate(ev).attempt
        calls <- replayCalls.get
      } yield expect.all(result == Left(unavailable), calls == 0)
  }

  test("FAIL-CLOSED: unauthenticated checkpoint signer IDs reject before replay and can never become slash targets") {
    case (h0, sp0) =>
      implicit val h: Hasher[IO] = h0
      implicit val sp: SecurityProvider[IO] = sp0
      val cp = unauthenticatedCheckpoint(attested, nSigners = 6)

      for {
        checkpointSigner <- RegisteredCheckpointSigner.make
        replayCalls <- cats.effect.Ref.of[IO, Int](0)
        reDerive = (_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal, _: SnapshotOrdinal) =>
          replayCalls.updateAndGet(_ + 1).as(honestDifferent)
        validator = makeValidator(reDerive, verifyCertificate = _ => IO.pure(Left("unregistered VRF/KES signer")))
        (kp, pid) <- challengerSetup(checkpointSigner)
        ev <- mkEvidence(cp, kp, pid, claimed = attested, challengerRoot = honestDifferent)
        result <- validator.validate(ev)
        calls <- replayCalls.get
      } yield
        expect.all(
          result == Left(InvalidStateProofRejection.InvalidCheckpointCertificate("unregistered VRF/KES signer")),
          calls == 0
        )
  }
}
