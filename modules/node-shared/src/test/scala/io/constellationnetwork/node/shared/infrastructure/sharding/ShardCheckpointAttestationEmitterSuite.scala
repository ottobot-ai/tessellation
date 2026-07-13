package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.{KeyPair, SecureRandom}

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap
import scala.util.Try

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.ShardTipTracker
import io.constellationnetwork.node.shared.domain.nakamoto.{EligibilityChecker, OperatorConsensusKeyRegistry, ShardAssignment}
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfPublicKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import io.grpc.ManagedChannel
import shapeless.test.illTyped
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointAttestationEmitter]] — the replay-capability-gated execution-signature seam.
  *
  * Coverage:
  *   1. '''Happy path''': `emit(...)` records the self-attestation into the per-shard tracker, publishes a
  *      `ShardCheckpointAttestationWire`, and the published Ed25519 signature verifies under the emitter's VK over the canonical checkpoint
  *      hash bytes (the same bytes the producer + acceptance manager use). 2. '''Skip when shard not tracked''': `emit` for a shard with no
  *      `shardEta` is a no-op — no publish, no tracker record.
  *
  * Mirrors `ShardCheckpointProducerSuite`'s fixture strategy: real `EligibilityChecker` + `Hasher` and one loader-validated Ed25519/KES/VRF
  * genesis identity, plus a recording `SidecarClient`.
  */
object ShardCheckpointAttestationEmitterSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], EligibilityChecker[IO], RegisteredCheckpointSigner)

  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8).asResource
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38).asResource
      ec = EligibilityChecker.make[IO](log1p, exp)
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
    } yield (h, sp, ec, checkpointSigner)

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val checkpointEpoch: EtaPeriod = EtaPeriod.Zero

  private val random = {
    val r = SecureRandom.getInstance("SHA1PRNG")
    r.setSeed(0x53_48_43_41_54_54L) // ASCII "SHCATT"
    r
  }

  private def randomShardEta(): Array[Byte] = {
    val eta = new Array[Byte](32)
    random.nextBytes(eta)
    eta
  }

  private def registeredSigner(
    checkpointSigner: RegisteredCheckpointSigner
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[(KeyPair, PeerId)] =
    for {
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(keyPair.getPublic)
      _ <- checkpointSigner.preregisterGenesis(keyPair, peerId)
    } yield (keyPair, peerId)

  /** Recording sidecar client: captures every published `ShardCheckpointAttestationWire`. */
  private def recordingSidecar(ref: Ref[IO, List[ShardCheckpointAttestationWire]]): SidecarClientAlgebra[IO] =
    new SidecarClientAlgebra[IO] {
      def publishSnapshot(msg: Snapshot) = IO.pure(PublishResponse(ok = true))
      def publishAttestation(msg: TipAttestation) = IO.pure(PublishResponse(ok = true))
      def publishRumor(msg: Rumor) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphBinary(msg: MetagraphBinary) = IO.pure(PublishResponse(ok = true))
      def publishMetagraphAttestation(msg: MetagraphAttestation) = IO.pure(PublishResponse(ok = true))
      def publishAllowSpendBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishDAGBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishTokenLockBlock(payload: Array[Byte]) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpoint(msg: ShardCheckpointWire) = IO.pure(PublishResponse(ok = true))
      def publishShardCheckpointAttestation(msg: ShardCheckpointAttestationWire) =
        ref.update(msg :: _).as(PublishResponse(ok = true))
      def publishFraudProof(msg: FraudProofEnvelopeWire) = IO.pure(PublishResponse(ok = true))
      def confirmFinalized(topic: String, msgIds: List[Array[Byte]]) = IO.pure(ConfirmFinalizedResponse(dropped = msgIds.size))
      def health = IO.pure(HealthResponse(healthy = true))
      def peers = IO.pure(PeerCountResponse(total = 0))
      def channel: ManagedChannel = throw new UnsupportedOperationException("stub")
    }

  private val replayedRoot = Hash("11" * 32)

  private def signedBinary: Signed[StateChannelSnapshotBinary] = {
    val proof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(
        lastSnapshotHash = Hash.empty,
        content = "replay-input".getBytes("UTF-8"),
        fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
      ),
      NonEmptySet.one(proof)
    )
  }

  private def checkpoint(
    keyPair: KeyPair,
    peerId: PeerId,
    checkpointSigner: RegisteredCheckpointSigner
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpoint] = {
    val metagraph = Address.fromBytes("capability-test-metagraph".getBytes("UTF-8"))
    val delta = ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(metagraph -> replayedRoot),
      includedSnapshots = SortedMap(metagraph -> NonEmptyList.one(signedBinary))
    )
    val placeholder = CommitteeMemberSignature(peerId, Hex(""), Hex(""), Hex(""), kesTreeStep = 0)
    val shell = ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal.Root.next,
      gl0AnchorOrdinal = SnapshotOrdinal.MinValue,
      slot = Slot.unsafeApply(5L),
      derivedStateDelta = delta,
      committeeSignatures = NonEmptyList.one(placeholder),
      epoch = checkpointEpoch
    )

    checkpointSigner
      .sign(shell, keyPair, peerId, checkpointSigner.defaultShardEta)
      .map(signature => shell.copy(committeeSignatures = NonEmptyList.one(signature)))
  }

  private def concreteAcceptanceManager(
    checkpointSigner: RegisteredCheckpointSigner,
    committeeMember: PeerId,
    executionQuorum: Int,
    reExecuteTo: Hash,
    replayCount: Ref[IO, Int]
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      executionQuorum = executionQuorum,
      etaRotationSnapshots = 1000L,
      committeeMembership = (_, _) => IO.pure(Set(committeeMember)),
      operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
      shardAssignment = ShardAssignment.make[IO](numShards = 1),
      shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
      producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
      reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1).as(reExecuteTo)
    )

  /** An arbitrary manager can claim `Accepted`, but cannot construct the private capability implementation. */
  private def acceptedVerdictStub: ShardCheckpointGl0AcceptanceManager[IO] =
    new ShardCheckpointGl0AcceptanceManager[IO] {
      def evaluate(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = IO.pure(ShardCheckpointAcceptResult.Accepted)
      def evaluateForSigning(
        checkpoint: ShardCheckpoint
      ): IO[Either[VerifiedShardCheckpointFailure, VerifiedShardCheckpoint]] =
        IO.pure(Left(VerifiedShardCheckpointFailure.Rejected("non-concrete manager cannot mint")))
      def verifyEmbedded(checkpoint: ShardCheckpoint): IO[ShardCheckpointAcceptResult] = IO.pure(ShardCheckpointAcceptResult.Accepted)
      def verifyExecutionCertificate(checkpoint: ShardCheckpoint): IO[Either[String, Unit]] =
        IO.pure(Left("non-concrete manager has no authenticated execution certificate"))
      def verifyCommitteeSignature(
        checkpoint: ShardCheckpoint,
        signature: CommitteeMemberSignature
      ): IO[Either[String, Unit]] = IO.pure(Right(()))
      def watchtowerReExec(checkpoint: ShardCheckpoint): IO[List[WatchtowerMismatch]] = IO.pure(Nil)
      def noteAdopted(shardId: ShardId, shardOrdinal: ShardOrdinal, checkpointHash: Hash): IO[Unit] = IO.unit
      def lastAdoptedOrd(shardId: ShardId): IO[Option[ShardOrdinal]] = IO.pure(None)
      def lastAdoptedAnchor(shardId: ShardId): IO[Option[Hash]] = IO.pure(None)
      def lastAdoptedCheckpoint(shardId: ShardId): IO[Option[(ShardOrdinal, Hash)]] = IO.pure(None)
    }

  test("pending after one concrete-manager replay can emit a verifiable execution signature") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    val shardEta = checkpointSigner.defaultShardEta
    for {
      (kp, selfId) <- registeredSigner(checkpointSigner)
      cp <- checkpoint(kp, selfId, checkpointSigner)
      selfVrfKeys = VrfKeyDeriver.deriveVrfKeyPair(kp)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      replayCount <- Ref.of[IO, Int](0)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = selfVrfKeys._1,
        selfVrfVk = selfVrfKeys._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = checkpointSigner.producerKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = sid => if (sid == shardZero) Some(tracker) else None,
        // Slice S4: epoch-keyed eta resolver. Fixed precomputed eta for shardZero regardless of epoch — the emitter
        // threads the resolved eta into its VRF membership proof; rotation correctness is covered in ShardSlotLeaderSuite.
        shardEtaFor = (sid, _) => IO.pure(if (sid == shardZero) Some(shardEta) else None)
      )
      verifiedEither <- manager.evaluateForSigning(cp)
      verified <- IO.fromEither(verifiedEither.leftMap(failure => new RuntimeException(failure.toString)))
      checkpointHash <- h.hash(cp.signingPreimage)
      _ <- emitter.emit(verified)
      wires <- published.get
      evaluations <- replayCount.get
      // Self-attestation recorded locally (excludeSelf=false to see it; default excludeSelf would hide it).
      selfCount <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      // Verify the published wire: shard id + checkpoint hash + the Ed25519 sig verifies over the canonical hash bytes.
      wire = wires.head
      attestation <- ShardCheckpointWireCodecs.shardCheckpointAttestationFromWire[IO](wire)
      msgBytes = checkpointHash.value.getBytes("UTF-8")
      pubKey <- selfId.value.toPublicKey[IO]
      edOk <- Signing.verifySignature[IO](msgBytes, attestation.attesterSignature.ed25519Sig.toBytes)(pubKey)
    } yield
      expect.all(
        wires.size == 1,
        attestation.shardId == shardZero,
        attestation.checkpointHash == checkpointHash,
        attestation.attesterSignature.peerId == selfId,
        attestation.attesterSignature.kesTreeStep == 0,
        edOk,
        selfCount == 1,
        verified.checkpoint == cp,
        verified.signingPreimageHash == checkpointHash,
        evaluations == 1
      )
  }

  test("reject: an unverified local execution signature is neither recorded nor published") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    val shardEta = randomShardEta()
    for {
      (kp, selfId) <- registeredSigner(checkpointSigner)
      cp <- checkpoint(kp, selfId, checkpointSigner)
      selfVrfKeys = VrfKeyDeriver.deriveVrfKeyPair(kp)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      replayCount <- Ref.of[IO, Int](0)
      verificationCalls <- Ref.of[IO, Int](0)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = selfVrfKeys._1,
        selfVrfVk = selfVrfKeys._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = checkpointSigner.producerKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = (_, _) => verificationCalls.update(_ + 1).as(Left("invalid local signature evidence")),
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Some(shardEta))
      )
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      verifiedEither <- manager.evaluateForSigning(cp)
      verified <- IO.fromEither(verifiedEither.leftMap(failure => new RuntimeException(failure.toString)))
      _ <- emitter.emit(verified)
      wires <- published.get
      records <- tracker.attestationCountFor(verified.signingPreimageHash, excludeSelf = false)
      verifications <- verificationCalls.get
      replays <- replayCount.get
    } yield expect.all(wires.isEmpty, records == 0, verifications == 1, replays == 1)
  }

  test("skip: emit for an untracked shard (no shardEta) is a no-op — no publish, no record") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    for {
      (kp, selfId) <- registeredSigner(checkpointSigner)
      cp <- checkpoint(kp, selfId, checkpointSigner)
      selfVrfKeys = VrfKeyDeriver.deriveVrfKeyPair(kp)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      replayCount <- Ref.of[IO, Int](0)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 1,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = selfVrfKeys._1,
        selfVrfVk = selfVrfKeys._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = checkpointSigner.producerKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Option.empty[Array[Byte]]) // no eta for any shard ⇒ skip
      )
      verifiedEither <- manager.evaluateForSigning(cp)
      verified <- IO.fromEither(verifiedEither.leftMap(failure => new RuntimeException(failure.toString)))
      checkpointHash = verified.signingPreimageHash
      _ <- emitter.emit(verified)
      wires <- published.get
      count <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      evaluations <- replayCount.get
    } yield expect.all(wires.isEmpty, count == 0, evaluations == 1)
  }

  test("missing or mismatched registered VRF identity has zero proof, signature, record, and publish effects") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res

    List("missing", "mismatched").traverse { mode =>
      for {
        (kp, selfId) <- registeredSigner(checkpointSigner)
        cp <- checkpoint(kp, selfId, checkpointSigner)
        selfVrfKeys = VrfKeyDeriver.deriveVrfKeyPair(kp)
        wrongVrfVk = selfVrfKeys._2.updated(0, (selfVrfKeys._2(0) ^ 0xff).toByte)
        registeredKeys <- checkpointSigner.operatorKeyRegistry
          .get(selfId)
          .flatMap(
            IO.fromOption(_)(new IllegalStateException("missing preregistered atomic test identity"))
          )
        registry =
          if (mode == "missing") OperatorConsensusKeyRegistry.empty[IO]
          else
            OperatorConsensusKeyRegistry.make[IO](
              Map(selfId -> registeredKeys.copy(vrfPublicKey = VrfPublicKey.fromBytes(wrongVrfVk)))
            )
        published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
        replayCount <- Ref.of[IO, Int](0)
        etaCalls <- Ref.of[IO, Int](0)
        signatureCalls <- Ref.of[IO, Int](0)
        tracker <- ShardTipTracker.make[IO](shardZero, selfId)
        countingKesSigner = new ShardCheckpointProducer.KesSigner[IO] {
          def sign(
            operatorKeys: OperatorConsensusKeys,
            checkpointEpoch: EtaPeriod,
            message: Array[Byte]
          ): IO[Option[ShardCheckpointProducer.KesSignature]] =
            signatureCalls
              .update(_ + 1)
              .as(Some(ShardCheckpointProducer.KesSignature(3, Array.fill[Byte](8)(0x7))))
        }
        emitter = ShardCheckpointAttestationEmitter.make[IO](
          selfPeerId = selfId,
          selfKeyPair = kp,
          selfVrfSk = selfVrfKeys._1,
          selfVrfVk = selfVrfKeys._2,
          operatorKeyRegistry = registry,
          kesSigner = countingKesSigner,
          eligibilityChecker = ec,
          verifyCommitteeSignature =
            (_, _) => IO.raiseError(new AssertionError("committee verification must not run after local VRF identity rejection")),
          sidecarClient = recordingSidecar(published),
          tipTrackerFor = _ => Some(tracker),
          shardEtaFor = (_, _) => etaCalls.update(_ + 1).as(Some(randomShardEta()))
        )
        manager <- concreteAcceptanceManager(
          checkpointSigner,
          selfId,
          executionQuorum = 1,
          reExecuteTo = replayedRoot,
          replayCount = replayCount
        )
        verifiedEither <- manager.evaluateForSigning(cp)
        verified <- IO.fromEither(verifiedEither.leftMap(failure => new RuntimeException(failure.toString)))
        _ <- emitter.emit(verified)
        wires <- published.get
        etas <- etaCalls.get
        signatures <- signatureCalls.get
        records <- tracker.attestationCountFor(verified.signingPreimageHash, excludeSelf = false)
      } yield expect.all(wires.isEmpty, etas == 0, signatures == 0, records == 0)
    }.map(_.combineAll)
  }

  test("reject: failed replay cannot mint a capability or reach the emitter") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    val shardEta = checkpointSigner.defaultShardEta
    for {
      (kp, selfId) <- registeredSigner(checkpointSigner)
      cp <- checkpoint(kp, selfId, checkpointSigner)
      selfVrfKeys = VrfKeyDeriver.deriveVrfKeyPair(kp)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      replayCount <- Ref.of[IO, Int](0)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 1,
        reExecuteTo = Hash("22" * 32),
        replayCount = replayCount
      )
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = kp,
        selfVrfSk = selfVrfKeys._1,
        selfVrfVk = selfVrfKeys._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = checkpointSigner.producerKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Some(shardEta))
      )
      verifiedEither <- manager.evaluateForSigning(cp)
      _ <- verifiedEither.toOption.traverse_(emitter.emit)
      wires <- published.get
      evaluations <- replayCount.get
    } yield expect.all(verifiedEither.isLeft, wires.isEmpty, evaluations == 1)
  }

  test("API: emitter accepts one VerifiedShardCheckpoint capability, not naked hash/routing fields") { _ =>
    illTyped("""VerifiedShardCheckpoint.evaluate[IO](null, null)""")
    illTyped("""new VerifiedShardCheckpoint { def checkpoint: ShardCheckpoint = null; def signingPreimageHash: Hash = null }""")
    val emitMethods = classOf[ShardCheckpointAttestationEmitter[IO]].getDeclaredMethods.filter(_.getName == "emit")
    val publicCapabilityCompanion = Try(
      Class.forName("io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.VerifiedShardCheckpoint$")
    ).isSuccess
    IO.pure(
      expect.all(
        emitMethods.length == 1,
        emitMethods.head.getParameterTypes.toList == List(classOf[VerifiedShardCheckpoint]),
        !publicCapabilityCompanion
      )
    )
  }

  test("API: an arbitrary Accepted-returning manager cannot mint a signing capability") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp, selfId) <- registeredSigner(checkpointSigner)
      cp <- checkpoint(kp, selfId, checkpointSigner)
      ordinaryVerdict <- acceptedVerdictStub.evaluate(cp)
      signingVerdict <- acceptedVerdictStub.evaluateForSigning(cp)
    } yield expect.all(ordinaryVerdict == ShardCheckpointAcceptResult.Accepted, signingVerdict.isLeft)
  }
}
