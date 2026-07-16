package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.{KeyPair, SecureRandom}
import java.util.concurrent.atomic.AtomicInteger

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
import io.constellationnetwork.node.shared.domain.snapshot.finality.CanonicalLineageRevision
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.SidecarClient.SidecarClientAlgebra
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.sidecar._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global._
import io.constellationnetwork.numerics.interpreters.{ExpInterpreter, Log1pInterpreter}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
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
  private val stableLineage = CanonicalLineageRevision(NonNegLong.MinValue)
  private val stableLineageRead: IO[Option[CanonicalLineageRevision]] = IO.pure(stableLineage.some)

  private def acquireStable(
    manager: ShardCheckpointGl0AcceptanceManager[IO],
    checkpoint: ShardCheckpoint
  ): IO[LineageBoundVerifiedShardCheckpoint] =
    ShardCheckpointAttestationEmitter
      .acquireLineageBound(stableLineageRead)(manager.evaluateForSigning(checkpoint))
      .flatMap {
        case LineageBoundCheckpointEvaluation.ReplayVerified(capability) => IO.pure(capability)
        case other => IO.raiseError(new RuntimeException(s"expected lineage-bound replay capability, got $other"))
      }

  private final case class EmitterEffectCounts(
    replay: Int,
    etaLookup: Int,
    possessionProof: Int,
    kesSign: Int,
    localVerification: Int,
    trackerRecord: Int,
    publish: Int
  )

  private final case class LocalIdentityCandidate(
    name: String,
    keyPair: KeyPair,
    vrfSecret: Array[Byte],
    vrfPublic: Array[Byte],
    registry: OperatorConsensusKeyRegistry[IO]
  )

  private def countingEligibilityChecker(proofCalls: AtomicInteger): IO[EligibilityChecker[IO]] =
    for {
      log1p <- Log1pInterpreter.make[IO](maxIterations = 10000, precision = 8)
      exp <- ExpInterpreter.make[IO](maxIterations = 10000, precision = 38)
    } yield
      new EligibilityChecker[IO](log1p, exp) {
        override def vrfProofForSlot(vrfSK: Array[Byte], slot: Slot, eta: Array[Byte]): Array[Byte] = {
          proofCalls.incrementAndGet()
          super.vrfProofForSlot(vrfSK, slot, eta)
        }
      }

  /** Deliberately unrooted runtime-shaped negative. Its dummy long-term proof is not loader- or cryptographically validated. */
  private def unrootedRuntimeShapedPair(genesis: OperatorConsensusKeys): OperatorConsensusKeys = {
    val cert = KesRegistrationCert(
      operatorPeerId = genesis.operatorPeerId,
      kesMasterVK = Hex.fromBytes(genesis.kes.vk.value),
      kesMasterVKStep = genesis.kes.vk.step,
      offset = 0L,
      vrfPublicKey = Hex.fromBytes(genesis.vrfPublicKey.toBytes),
      effectiveFromPeriod = EtaPeriod.Zero,
      registrationParentHash = Hash("aa" * 32),
      ordinal = KesRegistrationOrdinal(NonNegLong.unsafeFrom(1L)),
      parent = KesRegistrationReference.empty
    )
    val dummyProof = SignatureProof(Id(genesis.operatorPeerId.value), Signature(Hex("7f" * 64)))
    val record = KesRegistrationRecord(Signed(cert, NonEmptySet.one(dummyProof)), SnapshotOrdinal.unsafeApply(1L))

    genesis.copy(registration = record.some)
  }

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
      epoch = checkpointEpoch,
      executionBase = io.constellationnetwork.node.shared.ShardCheckpointTestFixtures.defaultExecutionBase
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
    replayCount: Ref[IO, Int],
    onReplay: IO[Unit] = IO.unit
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      executionQuorum = executionQuorum,
      etaRotationSnapshots = 1000L,
      committeeMembership = (_, _) => IO.pure(Set(committeeMember)),
      operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
      shardAssignment = ShardAssignment.make[IO](numShards = 1),
      shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
      producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
      reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1) >> onReplay.as(reExecuteTo)
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

  test("stable GL0 lineage plus one concrete-manager replay can emit a verifiable execution signature") { res =>
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
        shardEtaFor = (sid, _) => IO.pure(if (sid == shardZero) Some(shardEta) else None),
        localGlobalLineageRevision = stableLineageRead
      )
      verified <- acquireStable(manager, cp)
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
        shardEtaFor = (_, _) => IO.pure(Some(shardEta)),
        localGlobalLineageRevision = stableLineageRead
      )
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      verified <- acquireStable(manager, cp)
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
        shardEtaFor = (_, _) => IO.pure(Option.empty[Array[Byte]]), // no eta for any shard ⇒ skip
        localGlobalLineageRevision = stableLineageRead
      )
      verified <- acquireStable(manager, cp)
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
          shardEtaFor = (_, _) => etaCalls.update(_ + 1).as(Some(randomShardEta())),
          localGlobalLineageRevision = stableLineageRead
        )
        manager <- concreteAcceptanceManager(
          checkpointSigner,
          selfId,
          executionQuorum = 1,
          reExecuteTo = replayedRoot,
          replayCount = replayCount
        )
        verified <- acquireStable(manager, cp)
        _ <- emitter.emit(verified)
        wires <- published.get
        etas <- etaCalls.get
        signatures <- signatureCalls.get
        records <- tracker.attestationCountFor(verified.signingPreimageHash, excludeSelf = false)
      } yield expect.all(wires.isEmpty, etas == 0, signatures == 0, records == 0)
    }.map(_.combineAll)
  }

  test(
    "K7b: frozen execution-attester identity rejects before eta, proof, signing, verification, tracking, or publish after one concrete-manager re-execution-hook invocation"
  ) { res =>
    implicit val (h, sp, _, checkpointSigner) = res

    for {
      (authorityKeyPair, authorityId) <- registeredSigner(checkpointSigner)
      (controlKeyPair, controlId) <- registeredSigner(checkpointSigner)
      checkpointUnderTest <- checkpoint(authorityKeyPair, authorityId, checkpointSigner)
      authorityVrf = VrfKeyDeriver.deriveVrfKeyPair(authorityKeyPair)
      controlVrf = VrfKeyDeriver.deriveVrfKeyPair(controlKeyPair)
      authorityKeys <- checkpointSigner.operatorKeyRegistry
        .get(authorityId)
        .flatMap(IO.fromOption(_)(new IllegalStateException("missing loader-validated authority identity")))
      controlKeys <- checkpointSigner.operatorKeyRegistry
        .get(controlId)
        .flatMap(IO.fromOption(_)(new IllegalStateException("missing loader-validated control identity")))
      _ <- IO.raiseUnless(
        java.security.MessageDigest.isEqual(authorityKeys.vrfPublicKey.toBytes, authorityVrf._2) &&
          java.security.MessageDigest.isEqual(controlKeys.vrfPublicKey.toBytes, controlVrf._2) &&
          !java.security.MessageDigest.isEqual(authorityKeys.kes.vk.value, controlKeys.kes.vk.value) &&
          !java.security.MessageDigest.isEqual(authorityKeys.vrfPublicKey.toBytes, controlKeys.vrfPublicKey.toBytes)
      )(new IllegalStateException("loader-validated A/B controls are not distinct and correctly bound"))
      replayCount <- Ref.of[IO, Int](0)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        authorityId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      verified <- acquireStable(manager, checkpointUnderTest)
      runtimePair = unrootedRuntimeShapedPair(authorityKeys)
      runtimeRegistry = OperatorConsensusKeyRegistry.make[IO](Map(authorityId -> runtimePair))
      candidates = List(
        LocalIdentityCandidate(
          "missing-authority",
          authorityKeyPair,
          authorityVrf._1,
          authorityVrf._2,
          OperatorConsensusKeyRegistry.empty[IO]
        ),
        LocalIdentityCandidate(
          "loader-validated-B-vrf-for-A",
          authorityKeyPair,
          controlVrf._1,
          controlVrf._2,
          checkpointSigner.operatorKeyRegistry
        ),
        LocalIdentityCandidate(
          "loader-validated-B-long-term-for-A",
          controlKeyPair,
          authorityVrf._1,
          authorityVrf._2,
          checkpointSigner.operatorKeyRegistry
        ),
        LocalIdentityCandidate(
          "unrooted-dummy-runtime-shaped-record",
          authorityKeyPair,
          authorityVrf._1,
          authorityVrf._2,
          runtimeRegistry
        ),
        LocalIdentityCandidate(
          "malformed-31-byte-local-vrf-evidence",
          authorityKeyPair,
          authorityVrf._1,
          Array.fill[Byte](31)(0x7f.toByte),
          checkpointSigner.operatorKeyRegistry
        )
      )
      runCandidate = (candidate: LocalIdentityCandidate) =>
        for {
          etaCalls <- Ref.of[IO, Int](0)
          proofCalls = new AtomicInteger(0)
          eligibility <- countingEligibilityChecker(proofCalls)
          kesCalls <- Ref.of[IO, Int](0)
          verificationCalls <- Ref.of[IO, Int](0)
          published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
          tracker <- ShardTipTracker.make[IO](shardZero, authorityId)
          countingKesSigner = new ShardCheckpointProducer.KesSigner[IO] {
            def sign(
              operatorKeys: OperatorConsensusKeys,
              checkpointEpoch: EtaPeriod,
              message: Array[Byte]
            ): IO[Option[ShardCheckpointProducer.KesSignature]] =
              kesCalls.update(_ + 1) >> checkpointSigner.producerKesSigner.sign(operatorKeys, checkpointEpoch, message)
          }
          emitter = ShardCheckpointAttestationEmitter.make[IO](
            selfPeerId = authorityId,
            selfKeyPair = candidate.keyPair,
            selfVrfSk = candidate.vrfSecret,
            selfVrfVk = candidate.vrfPublic,
            operatorKeyRegistry = candidate.registry,
            kesSigner = countingKesSigner,
            eligibilityChecker = eligibility,
            verifyCommitteeSignature =
              (cp, signature) => verificationCalls.update(_ + 1) >> manager.verifyCommitteeSignature(cp, signature),
            sidecarClient = recordingSidecar(published),
            tipTrackerFor = _ => Some(tracker),
            shardEtaFor = (_, _) => etaCalls.update(_ + 1).as(Some(checkpointSigner.defaultShardEta)),
            localGlobalLineageRevision = stableLineageRead
          )
          _ <- emitter.emit(verified)
          replay <- replayCount.get
          eta <- etaCalls.get
          proof <- IO(proofCalls.get())
          kes <- kesCalls.get
          verification <- verificationCalls.get
          records <- tracker.attestationCountFor(verified.signingPreimageHash, excludeSelf = false)
          wires <- published.get
        } yield
          candidate.name -> EmitterEffectCounts(
            replay,
            eta,
            proof,
            kes,
            verification,
            records,
            wires.size
          )
      attackResults <- candidates.traverse(runCandidate)
      positiveResult <- runCandidate(
        LocalIdentityCandidate(
          "loader-validated-A-positive",
          authorityKeyPair,
          authorityVrf._1,
          authorityVrf._2,
          checkpointSigner.operatorKeyRegistry
        )
      )
    } yield {
      val expectedAttackNames = Set(
        "missing-authority",
        "loader-validated-B-vrf-for-A",
        "loader-validated-B-long-term-for-A",
        "unrooted-dummy-runtime-shaped-record",
        "malformed-31-byte-local-vrf-evidence"
      )
      val allEmitterEffectsZeroAfterReplay = attackResults.forall {
        case (_, counts) =>
          counts.replay == 1 &&
          counts.etaLookup == 0 &&
          counts.possessionProof == 0 &&
          counts.kesSign == 0 &&
          counts.localVerification == 0 &&
          counts.trackerRecord == 0 &&
          counts.publish == 0
      }
      val positive = positiveResult._2

      expect.all(
        attackResults.size == expectedAttackNames.size,
        attackResults.map(_._1).toSet == expectedAttackNames,
        allEmitterEffectsZeroAfterReplay,
        positive.replay == 1,
        positive.etaLookup == 1,
        positive.possessionProof == 1,
        positive.kesSign == 1,
        positive.localVerification == 1,
        positive.trackerRecord == 1,
        positive.publish == 1
      )
    }
  }

  test(
    "K7b: missing KES capability stops before Ed verification, tracking, and publish after one concrete-manager re-execution-hook invocation"
  ) { res =>
    implicit val (h, sp, _, checkpointSigner) = res

    for {
      (keyPair, selfId) <- registeredSigner(checkpointSigner)
      checkpointUnderTest <- checkpoint(keyPair, selfId, checkpointSigner)
      selfVrf = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      replayCount <- Ref.of[IO, Int](0)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      verified <- acquireStable(manager, checkpointUnderTest)
      etaCalls <- Ref.of[IO, Int](0)
      proofCalls = new AtomicInteger(0)
      eligibility <- countingEligibilityChecker(proofCalls)
      kesCalls <- Ref.of[IO, Int](0)
      verificationCalls <- Ref.of[IO, Int](0)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      noKesCapability = new ShardCheckpointProducer.KesSigner[IO] {
        def sign(
          operatorKeys: OperatorConsensusKeys,
          checkpointEpoch: EtaPeriod,
          message: Array[Byte]
        ): IO[Option[ShardCheckpointProducer.KesSignature]] = kesCalls.update(_ + 1).as(None)
      }
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = keyPair,
        selfVrfSk = selfVrf._1,
        selfVrfVk = selfVrf._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = noKesCapability,
        eligibilityChecker = eligibility,
        verifyCommitteeSignature = (_, _) => verificationCalls.update(_ + 1).as(Right(())),
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => etaCalls.update(_ + 1).as(Some(checkpointSigner.defaultShardEta)),
        localGlobalLineageRevision = stableLineageRead
      )
      _ <- emitter.emit(verified)
      replay <- replayCount.get
      eta <- etaCalls.get
      proof <- IO(proofCalls.get())
      kes <- kesCalls.get
      verification <- verificationCalls.get
      records <- tracker.attestationCountFor(verified.signingPreimageHash, excludeSelf = false)
      wires <- published.get
    } yield
      expect.all(
        replay == 1,
        eta == 1,
        proof == 1,
        kes == 1,
        verification == 0,
        records == 0,
        wires.isEmpty
      )
  }

  test("lineage unavailable before replay has zero replay, KES, tracker, and publish effects") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    for {
      (keyPair, selfId) <- registeredSigner(checkpointSigner)
      checkpointUnderTest <- checkpoint(keyPair, selfId, checkpointSigner)
      selfVrf = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      replayCount <- Ref.of[IO, Int](0)
      kesCalls <- Ref.of[IO, Int](0)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      countingKesSigner = new ShardCheckpointProducer.KesSigner[IO] {
        def sign(
          operatorKeys: OperatorConsensusKeys,
          checkpointEpoch: EtaPeriod,
          message: Array[Byte]
        ): IO[Option[ShardCheckpointProducer.KesSignature]] =
          kesCalls.update(_ + 1) >> checkpointSigner.producerKesSigner.sign(operatorKeys, checkpointEpoch, message)
      }
      lineageUnavailable = IO.pure(Option.empty[CanonicalLineageRevision])
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = keyPair,
        selfVrfSk = selfVrf._1,
        selfVrfVk = selfVrf._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = countingKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        localGlobalLineageRevision = lineageUnavailable
      )
      evaluation <- ShardCheckpointAttestationEmitter
        .acquireLineageBound(lineageUnavailable)(manager.evaluateForSigning(checkpointUnderTest))
      _ <- evaluation match {
        case LineageBoundCheckpointEvaluation.ReplayVerified(capability) => emitter.emit(capability)
        case _                                                           => IO.unit
      }
      checkpointHash <- h.hash(checkpointUnderTest.signingPreimage)
      replays <- replayCount.get
      kes <- kesCalls.get
      records <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      wires <- published.get
    } yield
      expect.all(
        evaluation == LineageBoundCheckpointEvaluation.LineageUnavailable,
        replays == 0,
        kes == 0,
        records == 0,
        wires.isEmpty
      )
  }

  test("lineage replacement during mismatching replay suppresses both signing and Rejected mismatch authority") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    val replacementLineage = CanonicalLineageRevision(NonNegLong.unsafeFrom(1L))
    for {
      (keyPair, selfId) <- registeredSigner(checkpointSigner)
      checkpointUnderTest <- checkpoint(keyPair, selfId, checkpointSigner)
      selfVrf = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      lineage <- Ref.of[IO, Option[CanonicalLineageRevision]](stableLineage.some)
      replayCount <- Ref.of[IO, Int](0)
      kesCalls <- Ref.of[IO, Int](0)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = Hash("22" * 32),
        replayCount = replayCount,
        onReplay = lineage.set(replacementLineage.some)
      )
      countingKesSigner = new ShardCheckpointProducer.KesSigner[IO] {
        def sign(
          operatorKeys: OperatorConsensusKeys,
          checkpointEpoch: EtaPeriod,
          message: Array[Byte]
        ): IO[Option[ShardCheckpointProducer.KesSignature]] =
          kesCalls.update(_ + 1) >> checkpointSigner.producerKesSigner.sign(operatorKeys, checkpointEpoch, message)
      }
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = keyPair,
        selfVrfSk = selfVrf._1,
        selfVrfVk = selfVrf._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = countingKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        localGlobalLineageRevision = lineage.get
      )
      evaluation <- ShardCheckpointAttestationEmitter
        .acquireLineageBound(lineage.get)(manager.evaluateForSigning(checkpointUnderTest))
      _ <- evaluation match {
        case LineageBoundCheckpointEvaluation.ReplayVerified(capability) => emitter.emit(capability)
        case _                                                           => IO.unit
      }
      checkpointHash <- h.hash(checkpointUnderTest.signingPreimage)
      replays <- replayCount.get
      kes <- kesCalls.get
      records <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      wires <- published.get
    } yield
      expect.all(
        evaluation == LineageBoundCheckpointEvaluation.LineageUnavailable,
        !evaluation.isInstanceOf[LineageBoundCheckpointEvaluation.Rejected],
        replays == 1,
        kes == 0,
        records == 0,
        wires.isEmpty
      )
  }

  test("lineage replacement after capability acquisition but before emit has zero KES, tracker, and publish effects") { res =>
    implicit val (h, sp, ec, checkpointSigner) = res
    val replacementLineage = CanonicalLineageRevision(NonNegLong.unsafeFrom(1L))
    for {
      (keyPair, selfId) <- registeredSigner(checkpointSigner)
      checkpointUnderTest <- checkpoint(keyPair, selfId, checkpointSigner)
      selfVrf = VrfKeyDeriver.deriveVrfKeyPair(keyPair)
      lineage <- Ref.of[IO, Option[CanonicalLineageRevision]](stableLineage.some)
      replayCount <- Ref.of[IO, Int](0)
      kesCalls <- Ref.of[IO, Int](0)
      published <- Ref.of[IO, List[ShardCheckpointAttestationWire]](Nil)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      manager <- concreteAcceptanceManager(
        checkpointSigner,
        selfId,
        executionQuorum = 2,
        reExecuteTo = replayedRoot,
        replayCount = replayCount
      )
      countingKesSigner = new ShardCheckpointProducer.KesSigner[IO] {
        def sign(
          operatorKeys: OperatorConsensusKeys,
          checkpointEpoch: EtaPeriod,
          message: Array[Byte]
        ): IO[Option[ShardCheckpointProducer.KesSignature]] =
          kesCalls.update(_ + 1) >> checkpointSigner.producerKesSigner.sign(operatorKeys, checkpointEpoch, message)
      }
      emitter = ShardCheckpointAttestationEmitter.make[IO](
        selfPeerId = selfId,
        selfKeyPair = keyPair,
        selfVrfSk = selfVrf._1,
        selfVrfVk = selfVrf._2,
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        kesSigner = countingKesSigner,
        eligibilityChecker = ec,
        verifyCommitteeSignature = manager.verifyCommitteeSignature,
        sidecarClient = recordingSidecar(published),
        tipTrackerFor = _ => Some(tracker),
        shardEtaFor = (_, _) => IO.pure(Some(checkpointSigner.defaultShardEta)),
        localGlobalLineageRevision = lineage.get
      )
      evaluation <- ShardCheckpointAttestationEmitter
        .acquireLineageBound(lineage.get)(manager.evaluateForSigning(checkpointUnderTest))
      capability <- evaluation match {
        case LineageBoundCheckpointEvaluation.ReplayVerified(value) => IO.pure(value)
        case other => IO.raiseError(new RuntimeException(s"expected lineage-bound replay capability, got $other"))
      }
      _ <- lineage.set(replacementLineage.some)
      _ <- emitter.emit(capability)
      checkpointHash <- h.hash(checkpointUnderTest.signingPreimage)
      replays <- replayCount.get
      kes <- kesCalls.get
      records <- tracker.attestationCountFor(checkpointHash, excludeSelf = false)
      wires <- published.get
    } yield expect.all(replays == 1, kes == 0, records == 0, wires.isEmpty)
  }

  test("reject: a mismatched re-execution-hook result cannot mint a capability or reach the emitter") { res =>
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
        shardEtaFor = (_, _) => IO.pure(Some(shardEta)),
        localGlobalLineageRevision = stableLineageRead
      )
      evaluation <- ShardCheckpointAttestationEmitter
        .acquireLineageBound(stableLineageRead)(manager.evaluateForSigning(cp))
      _ <- evaluation match {
        case LineageBoundCheckpointEvaluation.ReplayVerified(capability) => emitter.emit(capability)
        case _                                                           => IO.unit
      }
      wires <- published.get
      evaluations <- replayCount.get
    } yield expect.all(evaluation.isInstanceOf[LineageBoundCheckpointEvaluation.Rejected], wires.isEmpty, evaluations == 1)
  }

  test("API: emitter accepts only the nonserializable lineage-bound replay capability, never naked replay/hash fields") { _ =>
    illTyped("""VerifiedShardCheckpoint.evaluate[IO](null, null)""")
    illTyped("""new VerifiedShardCheckpoint { def checkpoint: ShardCheckpoint = null; def signingPreimageHash: Hash = null }""")
    illTyped("""(null: ShardCheckpointAttestationEmitter[IO]).emit(null: VerifiedShardCheckpoint)""")
    illTyped(
      """new LineageBoundVerifiedShardCheckpoint { def checkpoint: ShardCheckpoint = null; def signingPreimageHash: Hash = null; private[sharding] def replayCapability: VerifiedShardCheckpoint = null; private[sharding] def lineageRevision: CanonicalLineageRevision = null }"""
    )
    val emitMethods = classOf[ShardCheckpointAttestationEmitter[IO]].getDeclaredMethods.filter(_.getName == "emit")
    val publicCapabilityCompanion = Try(Class.forName(classOf[LineageBoundVerifiedShardCheckpoint].getName + "$")).isSuccess
    IO.pure(
      expect.all(
        emitMethods.length == 1,
        emitMethods.head.getParameterTypes.toList == List(classOf[LineageBoundVerifiedShardCheckpoint]),
        !publicCapabilityCompanion,
        !classOf[java.io.Serializable].isAssignableFrom(classOf[LineageBoundVerifiedShardCheckpoint])
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
