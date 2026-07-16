package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.nakamoto.kes.OperatorConsensusKeys
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.{RegisteredCheckpointSigner, TestCheckpointDutyValidator}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT, VrfPublicKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.vrf.VrfKeyDeriver
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointGl0AcceptanceManager]] — Slice 9 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §7.3.
  *
  * '''Required coverage''' (per slice spec):
  *   1. '''Pre-check fail: signer not in committee''' — checkpoint signed by a peerId not in `committeeMembership(...)` → `Rejected`
  *   1. '''Pre-check fail: bad Ed25519 sig''' — signature doesn't verify → `Rejected`
  *   1. '''Execution quorum accept''' — at least `kQuorum` distinct valid signers and matching re-execution → `Accepted`
  *   1. '''Missing execution quorum''' — fewer than `kQuorum` distinct valid signers → `Rejected` without re-execution
  *   1. '''Replay mismatch''' — intake re-execution returns a different root → `RejectedReExecutionMismatch` with every committee signer
  *
  * '''Test fixture pattern''':
  *   - Build checkpoint envelopes with real Ed25519, KES, and VRF signatures, and inject registries containing the corresponding public
  *     keys. Missing KES/VRF registrations fail closed exactly as they do in production.
  *   - The manager receives the execution quorum explicitly; shard-chain depth and node-local tracker state are not validity inputs.
  */
object ShardCheckpointGl0AcceptanceManagerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], RegisteredCheckpointSigner)

  // The tracker and acceptance-manager constructors require Metrics[F]. Metric semantics are covered by ShardMetricsSuite.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
    } yield (h, sp, j, checkpointSigner)

  // ============================================================================
  // Fixtures
  // ============================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val genesisHash: Hash = Hash("0" * 64)

  /** Real-cryptography helpers to build verifiable Ed25519 signatures. The PeerId is recovered from the KeyPair's public key so the
    * manager's `peerId.value.toPublicKey[F]` round-trip recovers the same key we signed with.
    */
  private def mkSigner(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[(KeyPair, PeerId)] =
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      peerId = PeerId.fromPublic(kp.getPublic)
      _ <- checkpointSigner.preregisterGenesis(kp, peerId)
    } yield (kp, peerId)

  /** Build a committee-member signature that passes every cryptographic pre-check for the given registered peer. */
  private def mkValidSig(
    checkpoint: ShardCheckpoint,
    kp: KeyPair,
    peerId: PeerId
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], checkpointSigner: RegisteredCheckpointSigner): IO[CommitteeMemberSignature] =
    checkpointSigner.sign(checkpoint, kp, peerId, realShardEta)

  // ── Real-VRF fixtures (committee-VRF verify tests) ─────────────────────────────────────────────────────────────────
  // A deterministic 32-byte stand-in for the per-shard leader-VRF eta (`ShardSlotLeader.computeShardEta` output). The
  // acceptance manager only needs the SAME bytes the producer used; the tests inject this both into the manager's
  // `shardEtaFor` AND into the proof-construction message, so producer + verifier agree by construction.
  private val realShardEta: Array[Byte] = Array.tabulate[Byte](32)(i => (i * 7 + 1).toByte)

  /** Build a committee-member sig whose VRF proof is a REAL `EcVrf25519` proof of `(shardEta, checkpoint.slot)` under the operator's VRF SK
    * (derived from its long-term keypair via the SAME `VrfKeyDeriver` the runtime + genesis use). The Ed25519 sig also really verifies. The
    * returned `vrfVk` is what the test registers in the `VrfRegistry` for this peer — so the manager's real verify succeeds.
    */
  private def mkRealVrfSig(
    checkpoint: ShardCheckpoint,
    kp: KeyPair,
    peerId: PeerId,
    shardEta: Array[Byte]
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[(CommitteeMemberSignature, Array[Byte])] =
    checkpointSigner.sign(checkpoint, kp, peerId, shardEta).map(_ -> VrfKeyDeriver.deriveVrfKeyPair(kp)._2)

  /** Build a committee-member sig whose Ed25519 sig is RANDOM (won't verify). Used by the "pre-check Ed25519 fail" test. */
  private def mkBadEdSig(peerId: PeerId): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerId,
      vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x44.toByte)),
      ed25519Sig = Hex.fromBytes(Array.fill[Byte](64)(0xff.toByte)), // garbage — won't verify under any VK
      kesProductSig = Hex.fromBytes(Array.fill[Byte](32)(0x45.toByte)),
      kesTreeStep = 0
    )

  /** Build a fully-signed `Signed[ShardCheckpoint]` (NOT used by the manager — the manager reads committeeSignatures field directly, but we
    * wrap in `Signed` for compatibility with the producer/store APIs that some test scenarios may transitively touch).
    */
  private def mkSignedCheckpoint(cp: ShardCheckpoint, sigs: NonEmptyList[CommitteeMemberSignature]): Signed[ShardCheckpoint] = {
    val sentinelProof: SignatureProof =
      SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(cp.copy(committeeSignatures = sigs), NonEmptySet.of(sentinelProof))
  }

  /** Build a `ShardCheckpoint` shell with a placeholder signature; the test fills in the real sigs after computing the preimage hash. */
  private def mkCheckpointShell(
    shardOrd: Long,
    gl0Anchor: Long,
    delta: ShardDerivedStateDelta,
    placeholderPeerId: PeerId
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = genesisHash,
      shardOrdinal = ShardOrdinal(shardOrd),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      slot = SlotT.unsafeApply(gl0Anchor),
      derivedStateDelta = delta,
      // Placeholder — overwritten downstream once the real signature is computed.
      committeeSignatures = NonEmptyList.of(
        CommitteeMemberSignature(
          peerId = placeholderPeerId,
          vrfProof = Hex(""),
          ed25519Sig = Hex(""),
          kesProductSig = Hex(""),
          kesTreeStep = 0
        )
      ),
      epoch = epochZero
    )

  /** Build a `ShardDerivedStateDelta` with one MG carrying `(mgAddr → mptRoot, mgAddr → headBinary)`. The included-snapshots map drives the
    * re-exec path: for each MG, the manager calls `reExecuteDerivation(mg, head)` and compares against `perMetagraphMptRoots(mg)`.
    */
  private def mkDelta(mg: Address, root: Hash, binary: Signed[StateChannelSnapshotBinary]): ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mg -> root),
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary))
    )

  /** Build a fake `Signed[StateChannelSnapshotBinary]` for the included-snapshots field. The manager doesn't validate the binary's inner
    * crypto — it only passes the head to `reExecuteDerivation`. A sentinel `Signed` wrapper is fine.
    */
  private def mkSignedBinary(content: Array[Byte]): Signed[StateChannelSnapshotBinary] = {
    val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
    Signed(
      StateChannelSnapshotBinary(
        lastSnapshotHash = Hash("0" * 64),
        content = content,
        fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
      ),
      NonEmptySet.of(sentinelProof)
    )
  }

  /** Build the manager under test with stubbed callbacks. Each callback is a parameter so individual tests can override exactly the
    * surfaces under test (e.g., wire a different `committeeMembership` to control the pre-check membership predicate).
    */
  private def mkManager(
    executionQuorum: Int = 1,
    etaRotationSnapshots: Long = 1000L,
    committeeMembership: Set[PeerId],
    operatorKeyRegistryOverride: Option[OperatorConsensusKeyRegistry[IO]] = None,
    shardEtaFor: (ShardId, EtaPeriod) => IO[Option[Array[Byte]]] = (_, _) => IO.pure(realShardEta.some),
    producerDutyValidator: ShardCheckpointProducerDutyValidator[IO] = TestCheckpointDutyValidator.allow[IO],
    reExecuteDerivation: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
      (_, _, _, _) => IO.pure(Hash("11" * 32)),
    reExecuteDerivations: Option[
      (
        SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[SortedMap[Address, Hash]]
    ] = None
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      executionQuorum = executionQuorum,
      etaRotationSnapshots = etaRotationSnapshots,
      committeeMembership = (_, _) => IO.pure(committeeMembership),
      operatorKeyRegistry = operatorKeyRegistryOverride.getOrElse(checkpointSigner.operatorKeyRegistry),
      shardAssignment = ShardAssignment.make[IO](numShards = 1),
      shardEtaFor = shardEtaFor,
      producerDutyValidator = producerDutyValidator,
      reExecuteDerivation = reExecuteDerivation,
      reExecuteDerivations = reExecuteDerivations
    )

  private def registryWithPair(
    peerId: PeerId,
    update: OperatorConsensusKeys => OperatorConsensusKeys = identity
  )(implicit checkpointSigner: RegisteredCheckpointSigner): IO[OperatorConsensusKeyRegistry[IO]] =
    checkpointSigner.operatorKeyRegistry
      .get(peerId)
      .flatMap(IO.fromOption(_)(new AssertionError(s"missing test operator-key pair for $peerId")))
      .map(keys => OperatorConsensusKeyRegistry.make[IO](Map(peerId -> update(keys))))

  // ============================================================================
  // Test 1: Pre-check fail — signer not in committee
  // ============================================================================

  test("wire epoch mismatch is rejected before committee lookup or replay on intake and embedded paths") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (_, peer) <- mkSigner
      committeeLookups <- cats.effect.Ref.of[IO, Int](0)
      replayCalls <- cats.effect.Ref.of[IO, Int](0)
      mg = Address.fromBytes("mg-wrong-epoch".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("wrong-epoch".getBytes("UTF-8")))
      checkpoint = mkCheckpointShell(1L, 199L, delta, peer).copy(epoch = EtaPeriod(7L))
      manager <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 1,
        etaRotationSnapshots = 100L,
        committeeMembership = (_, _) => committeeLookups.update(_ + 1).as(Set(peer)),
        operatorKeyRegistry = checkpointSigner.operatorKeyRegistry,
        shardAssignment = ShardAssignment.make[IO](numShards = 1),
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => replayCalls.update(_ + 1).as(root)
      )
      intake <- manager.evaluate(checkpoint)
      embedded <- manager.verifyEmbedded(checkpoint)
      lookupCount <- committeeLookups.get
      replayCount <- replayCalls.get
    } yield
      expect.all(
        intake match {
          case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("checkpoint epoch mismatch")
          case _                                            => false
        },
        embedded match {
          case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("checkpoint epoch mismatch")
          case _                                            => false
        },
        lookupCount == 0,
        replayCount == 0
      )
  }

  test("epoch derived from the signed GL0 anchor is accepted on intake and embedded paths") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (keyPair, peer) <- mkSigner
      mg = Address.fromBytes("mg-correct-epoch".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("correct-epoch".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 199L, delta, peer).copy(epoch = EtaPeriod(1L))
      signature <- mkValidSig(shell, keyPair, peer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      manager <- mkManager(
        executionQuorum = 1,
        etaRotationSnapshots = 100L,
        committeeMembership = Set(peer),
        reExecuteDerivation = (_, _, _, _) => IO.pure(root)
      )
      intake <- manager.evaluate(checkpoint)
      embedded <- manager.verifyEmbedded(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, intake) && expect.same(ShardCheckpointAcceptResult.Accepted, embedded)
  }

  test("SHARD-03 remains RED: a self-consistent producer-chosen old anchor and epoch still pass acceptance") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (keyPair, peer) <- mkSigner
      mg = Address.fromBytes("mg-old-anchor-grind".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("old-anchor".getBytes("UTF-8")))
      // Model a receiver whose proposal-parent Phase-2 anchor is already 299. That fact cannot be passed to the current manager: the wire
      // carries only the producer-chosen ordinal 99, and acceptance checks epoch=floor(99/100)=0 without proving freshness or exact hash.
      currentPhase2AnchorOutsideManager = SnapshotOrdinal(NonNegLong.unsafeFrom(299L))
      shell = mkCheckpointShell(1L, 99L, delta, peer).copy(epoch = EtaPeriod(0L))
      signature <- mkValidSig(shell, keyPair, peer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      manager <- mkManager(
        executionQuorum = 1,
        etaRotationSnapshots = 100L,
        committeeMembership = Set(peer),
        reExecuteDerivation = (_, _, _, _) => IO.pure(root)
      )
      intake <- manager.evaluate(checkpoint)
      embedded <- manager.verifyEmbedded(checkpoint)
    } yield
      expect(currentPhase2AnchorOutsideManager.value.value > checkpoint.gl0AnchorOrdinal.value.value) &&
        expect.same(ShardCheckpointAcceptResult.Accepted, intake) &&
        expect.same(ShardCheckpointAcceptResult.Accepted, embedded)
  }

  test("pre-check fail: signer peerId not in committeeMembership → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-a".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // committeeMembership is EMPTY — signer is not in committee → pre-check membership predicate fails.
      mgr <- mkManager(committeeMembership = Set.empty[PeerId])
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("not in committee"))
        case other => failure(s"Expected Rejected(not in committee), got $other")
      }
  }

  // ============================================================================
  // Test 2: Pre-check fail — bad Ed25519 sig
  // ============================================================================

  test("off-duty retained producer is rejected before replay even with a colluding execution quorum") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (kp2, p2) <- mkSigner
      mg = Address.fromBytes("mg-off-duty".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("binary-content".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      ordered <- ShardSlotLeader.dutyOrder[IO](List(p1, p2).sortBy(_.value.value), realShardEta, shell.shardOrdinal)
      duty = ShardSlotLeader.scheduledDuty(ordered, shell.slot, None, staircaseDeltaSlots = 5).toOption.get
      offDuty = Set(p1, p2).find(_ =!= duty.peerId).get
      offDutyKey = if (offDuty === p1) kp1 else kp2
      dutyKey = if (duty.peerId === p1) kp1 else kp2
      producerSig <- mkValidSig(shell, offDutyKey, offDuty)
      countersignature <- mkValidSig(shell, dutyKey, duty.peerId)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList(producerSig, List(countersignature)))
      replayCount <- cats.effect.Ref.of[IO, Int](0)
      dutyValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, _) => IO.raiseError(new AssertionError("genesis duty must not look up a parent"))
      )
      mgr <- mkManager(
        executionQuorum = 2,
        committeeMembership = Set(p1, p2),
        producerDutyValidator = dutyValidator,
        reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1).as(root)
      )
      signingResult <- mgr.evaluateForSigning(checkpoint)
      embeddedResult <- mgr.verifyEmbedded(checkpoint)
      replayed <- replayCount.get
    } yield
      signingResult match {
        case Left(VerifiedShardCheckpointFailure.Rejected(reason)) =>
          val embeddedRejected = embeddedResult match {
            case ShardCheckpointAcceptResult.Rejected(embeddedReason) => embeddedReason.contains("off-duty checkpoint producer")
            case _                                                    => false
          }
          expect(reason.contains("off-duty checkpoint producer")) && expect(embeddedRejected) && expect.same(0, replayed)
        case other => failure(s"Expected off-duty intake and embedded-artifact rejection, got $other / $embeddedResult")
      }
  }

  test("genesis scheduled producer reaches replay and passes embedded validation without parent lookup") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (kp2, p2) <- mkSigner
      mg = Address.fromBytes("mg-on-duty".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("binary-content".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      ordered <- ShardSlotLeader.dutyOrder[IO](List(p1, p2).sortBy(_.value.value), realShardEta, shell.shardOrdinal)
      duty = ShardSlotLeader.scheduledDuty(ordered, shell.slot, None, staircaseDeltaSlots = 5).toOption.get
      dutyKey = if (duty.peerId === p1) kp1 else kp2
      sig <- mkValidSig(shell, dutyKey, duty.peerId)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(sig))
      replayCount <- cats.effect.Ref.of[IO, Int](0)
      dutyValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, _) => IO.raiseError(new AssertionError("genesis duty must not look up a parent"))
      )
      mgr <- mkManager(
        committeeMembership = Set(p1, p2),
        producerDutyValidator = dutyValidator,
        reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1).as(root)
      )
      signingResult <- mgr.evaluateForSigning(checkpoint)
      embeddedResult <- mgr.verifyEmbedded(checkpoint)
      replayed <- replayCount.get
    } yield
      expect(signingResult.isRight) &&
        expect.same(ShardCheckpointAcceptResult.Accepted, embeddedResult) &&
        expect.same(2, replayed)
  }

  test("missing or non-monotone parent fails duty validation before replay") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp, peer) <- mkSigner
      mg = Address.fromBytes("mg-parent-duty".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("binary-content".getBytes("UTF-8")))
      parent = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = peer)
      parentHash <- Hasher[IO].hash(parent.signingPreimage)
      childShell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 101L, delta = delta, placeholderPeerId = peer)
        .copy(parentCheckpointHash = parentHash)
      childSig <- mkValidSig(childShell, kp, peer)
      child = childShell.copy(committeeSignatures = NonEmptyList.one(childSig))
      nonMonotoneShell = childShell.copy(slot = parent.slot)
      nonMonotoneSig <- mkValidSig(nonMonotoneShell, kp, peer)
      nonMonotone = nonMonotoneShell.copy(committeeSignatures = NonEmptyList.one(nonMonotoneSig))
      replayCount <- cats.effect.Ref.of[IO, Int](0)
      missingParentValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, _) => IO.pure(None)
      )
      presentParentValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, hash) => IO.pure(Option.when(hash === parentHash)(parent))
      )
      missingMgr <- mkManager(
        committeeMembership = Set(peer),
        producerDutyValidator = missingParentValidator,
        reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1).as(root)
      )
      nonMonotoneMgr <- mkManager(
        committeeMembership = Set(peer),
        producerDutyValidator = presentParentValidator,
        reExecuteDerivation = (_, _, _, _) => replayCount.update(_ + 1).as(root)
      )
      missingResult <- missingMgr.evaluateForSigning(child)
      nonMonotoneResult <- nonMonotoneMgr.evaluateForSigning(nonMonotone)
      replayed <- replayCount.get
    } yield {
      val missingRejected = missingResult.left.toOption.exists(_.acceptanceResult match {
        case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("producer duty parent unavailable")
        case _                                            => false
      })
      val nonMonotoneRejected = nonMonotoneResult.left.toOption.exists(_.acceptanceResult match {
        case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("not strictly greater than parent slot")
        case _                                            => false
      })
      expect(missingRejected) && expect(nonMonotoneRejected) && expect.same(0, replayed)
    }
  }

  test("SHARD-C-009 remains RED: identical embedded child diverges when local parent availability differs") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp, peer) <- mkSigner
      mg = Address.fromBytes("mg-parent-availability".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("binary-content".getBytes("UTF-8")))
      parent = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = peer)
      parentHash <- Hasher[IO].hash(parent.signingPreimage)
      childShell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 101L, delta = delta, placeholderPeerId = peer)
        .copy(parentCheckpointHash = parentHash)
      childSig <- mkValidSig(childShell, kp, peer)
      child = childShell.copy(committeeSignatures = NonEmptyList.one(childSig))
      availableValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, hash) => IO.pure(Option.when(hash === parentHash)(parent))
      )
      unavailableValidator = ShardCheckpointProducerDutyValidator.make[IO](
        staircaseDeltaSlots = 5,
        shardEtaFor = (_, _) => IO.pure(realShardEta.some),
        parentCheckpoint = (_, _) => IO.pure(None)
      )
      availableMgr <- mkManager(
        committeeMembership = Set(peer),
        producerDutyValidator = availableValidator,
        reExecuteDerivation = (_, _, _, _) => IO.pure(root)
      )
      unavailableMgr <- mkManager(
        committeeMembership = Set(peer),
        producerDutyValidator = unavailableValidator,
        reExecuteDerivation = (_, _, _, _) => IO.pure(root)
      )
      availableResult <- availableMgr.verifyEmbedded(child)
      unavailableResult <- unavailableMgr.verifyEmbedded(child)
    } yield {
      val unavailableRejected = unavailableResult match {
        case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("producer duty parent unavailable")
        case _                                            => false
      }
      expect.same(ShardCheckpointAcceptResult.Accepted, availableResult) && expect(unavailableRejected)
    }
  }

  test("pre-check fail: Ed25519 sig doesn't verify under signer's VK → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (_, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-b".getBytes("UTF-8"))
      mptRoot = Hash("22" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      // BAD Ed25519 sig — garbage bytes, won't verify under the recovered VK.
      badSig = mkBadEdSig(signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(badSig))

      // committeeMembership includes our signer so the membership pre-check passes — Ed25519 must be the one that fails.
      mgr <- mkManager(committeeMembership = Set(signerPeer))
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("Ed25519"))
        case other => failure(s"Expected Rejected(Ed25519 ...), got $other")
      }
  }

  test("pre-check fail: missing preregistered KES+VRF identity rejects a cryptographically valid signer") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-missing-kes".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("content".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 100L, delta, signerPeer)
      sig <- mkValidSig(shell, signerKp, signerPeer)
      manager <- mkManager(
        committeeMembership = Set(signerPeer),
        operatorKeyRegistryOverride = OperatorConsensusKeyRegistry.empty[IO].some
      )
      result <- manager.verifyEmbedded(shell.copy(committeeSignatures = NonEmptyList.of(sig)))
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("no preregistered KES+VRF identity"))
        case other                                        => failure(s"Expected fail-closed paired-key rejection, got $other")
      }
  }

  test("checkpoint KES step is derived from the preregistered activation offset, never selected by the wire") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (keyPair, peer) <- mkSigner
      mg = Address.fromBytes("mg-genesis-kes-offset".getBytes("UTF-8"))
      root = Hash("11" * 32)
      delta = mkDelta(mg, root, mkSignedBinary("content".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 399L, delta, peer).copy(epoch = EtaPeriod(3L))
      signature <- mkValidSig(shell, keyPair, peer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(signature))
      registeredPair <- checkpointSigner.operatorKeyRegistry
        .get(peer)
        .flatMap(IO.fromOption(_)(new AssertionError("missing test KES+VRF pair")))
      correctManager <- mkManager(
        etaRotationSnapshots = 100L,
        committeeMembership = Set(peer),
        reExecuteDerivation = (_, _, _, _) => IO.pure(root)
      )
      correct <- correctManager.verifyEmbedded(checkpoint)
      wrongStepCheckpoint = checkpoint.copy(
        committeeSignatures = NonEmptyList.one(signature.copy(kesTreeStep = Math.addExact(signature.kesTreeStep, 1)))
      )
      wrongStep <- correctManager.verifyEmbedded(wrongStepCheckpoint)
    } yield {
      val wrongStepRejected = wrongStep match {
        case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("KES sig verify failed")
        case _                                            => false
      }
      expect.all(
        signature.kesTreeStep == 3,
        registeredPair.kes.offset == 0L,
        correct == ShardCheckpointAcceptResult.Accepted,
        wrongStepRejected
      )
    }
  }

  test("pre-check fail: a runtime pair requires exact-parent historical resolution") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-missing-vrf".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("content".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 100L, delta, signerPeer)
      sig <- mkValidSig(shell, signerKp, signerPeer)
      pair <- checkpointSigner.operatorKeyRegistry
        .get(signerPeer)
        .flatMap(IO.fromOption(_)(new AssertionError("missing test KES+VRF pair")))
      effective = EtaPeriod(2L)
      cert = KesRegistrationCert(
        operatorPeerId = signerPeer,
        kesMasterVK = Hex.fromBytes(pair.kes.vk.value),
        kesMasterVKStep = 0,
        offset = effective.value,
        vrfPublicKey = Hex.fromBytes(pair.vrfPublicKey.toBytes),
        effectiveFromPeriod = effective,
        registrationParentHash = Hash("33" * 32),
        ordinal = KesRegistrationOrdinal.first
      )
      signedCert <- forAsyncHasher(cert, signerKp)
      runtimePair = pair.copy(
        kes = pair.kes.copy(offset = effective.value),
        effectiveFromPeriod = effective,
        registration = KesRegistrationRecord(signedCert, SnapshotOrdinal.MinValue).some
      )
      runtimeRegistry = OperatorConsensusKeyRegistry.make[IO](Map(signerPeer -> runtimePair))
      manager <- mkManager(
        committeeMembership = Set(signerPeer),
        operatorKeyRegistryOverride = runtimeRegistry.some
      )
      result <- manager.verifyEmbedded(shell.copy(committeeSignatures = NonEmptyList.of(sig)))
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("exact-parent historical resolution"))
        case other                                        => failure(s"Expected current-view runtime-key rejection, got $other")
      }
  }

  test("standalone attestation: Ed25519-valid outsider is rejected before tracker insertion") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (outsiderKp, outsiderPeer) <- mkSigner
      (_, selfPeer) <- mkSigner
      shell = mkCheckpointShell(
        shardOrd = 1L,
        gl0Anchor = 100L,
        delta = ShardDerivedStateDelta.empty,
        placeholderPeerId = outsiderPeer
      )
      outsiderSig <- mkValidSig(shell, outsiderKp, outsiderPeer)
      manager <- mkManager(committeeMembership = Set.empty)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      checkpointHash <- Hasher[IO].hash(shell.signingPreimage)
      validation <- manager.verifyCommitteeSignature(shell, outsiderSig)
      _ <- validation.fold(_ => IO.unit, _ => tracker.recordAttestation(checkpointHash, outsiderPeer, outsiderSig))
      retained <- tracker.signaturesFor(checkpointHash)
    } yield
      expect(validation.left.exists(_.contains("not in committee"))) &&
        expect(retained.isEmpty)
  }

  // ============================================================================
  // Test 3: execution quorum and replay accept
  // ============================================================================

  test("execution quorum with matching re-execution → Accepted") { res =>
    implicit val (h, sp, _, checkpointSigner) = res

    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-c".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      reExecCalledRef <- cats.effect.Ref.of[IO, Boolean](false)
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => reExecCalledRef.set(true).as(mptRoot)
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
      reExecCalled <- reExecCalledRef.get
    } yield
      expect.same(ShardCheckpointAcceptResult.Accepted, result) &&
        expect(reExecCalled)
  }

  // ============================================================================
  // Test 4: replay-valid intake remains pending until execution quorum
  // ============================================================================

  test("replay-valid intake below execution quorum → PendingMoreAttestations after re-execution") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-d".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      reExecCalled <- cats.effect.Ref.of[IO, Boolean](false)
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => reExecCalled.set(true).as(mptRoot)
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        executionQuorum = 2,
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
      wasReExecuted <- reExecCalled.get
    } yield expect.same(ShardCheckpointAcceptResult.PendingMoreAttestations, result) && expect(wasReExecuted)
  }

  // ============================================================================
  // Test 5: under-quorum replay mismatch — reject and retain committee signer identity for evidence construction
  // ============================================================================

  test("under-quorum re-exec mismatch → RejectedReExecutionMismatch with committee signer list") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp1, signerPeer1) <- mkSigner
      (signerKp2, signerPeer2) <- mkSigner

      mg = Address.fromBytes("mg-e".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer1)

      // TWO real signers — both remain attached to the diagnostic result for separately portable evidence construction.
      sig1 <- mkValidSig(shell, signerKp1, signerPeer1)
      sig2 <- mkValidSig(shell, signerKp2, signerPeer2)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig1, sig2))

      // Re-exec returns a WRONG hash for the MG — the manager should detect mismatch.
      wrongRoot = Hash("ff" * 32)
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => IO.pure(wrongRoot)
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        executionQuorum = 3,
        committeeMembership = Set(signerPeer1, signerPeer2),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, committeeSigners) =>
          expect(reason.contains("re-exec mismatch")) &&
          expect.same(List(signerPeer1, signerPeer2), committeeSigners)
        case other => failure(s"Expected RejectedReExecutionMismatch, got $other")
      }
  }

  // ============================================================================
  // Test 5b (execution-base-pin fail-closed): re-exec CANNOT-DERIVE sentinel — plain Rejected, NO slash
  // ============================================================================

  test("under-quorum intake CANNOT-DERIVE (Hash.empty sentinel): fail-closed plain Rejected, never sign") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-e2".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // Re-exec returns Hash.empty — the wiring's fail-closed "cannot re-derive" sentinel (`reExecDerivationAtPinnedBase` omitted: the
      // pinned execution-base is unresolvable below this node's retention / not yet reached, or the derivation deferred). That is
      // "THIS NODE can't check", NOT "the committee deviated" (the same reading `watchtowerReExec` applies when it filters
      // `Hash.empty` mismatches). The checkpoint must be REJECTED (fail-closed: never admitted unverified) but the signers must
      // NOT be affirmative mismatch evidence. A local `RejectedReExecutionMismatch` never directly reaches the slash sink; only a separately
      // carried fraud proof revalidated by every GL0 node can slash.
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => IO.pure(Hash.empty)
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        executionQuorum = 2,
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("cannot re-derive"), s"reason should name the can't-check condition, got: $reason")
        case other =>
          failure(
            s"Expected fail-closed plain Rejected (can't-check ⇒ drop, NO slash targets), got $other — " +
              s"a Hash.empty sentinel must never mark an honest committee for the 100% InvalidStateProof slash"
          )
      }
  }

  test("batch replay with a missing or extra MG result fails closed without scalar fallback or slash classification") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      scalarCalls <- cats.effect.Ref.of[IO, Int](0)
      batchCalls <- cats.effect.Ref.of[IO, Int](0)
      mgA = Address.fromBytes("mg-batch-a".getBytes("UTF-8"))
      mgB = Address.fromBytes("mg-batch-b".getBytes("UTF-8"))
      extraMg = Address.fromBytes("mg-batch-extra".getBytes("UTF-8"))
      claimedRoot = Hash("11" * 32)
      binary = mkSignedBinary("batch-binary-content".getBytes("UTF-8"))
      delta = ShardDerivedStateDelta(
        perMetagraphMptRoots = SortedMap(mgA -> claimedRoot, mgB -> claimedRoot),
        includedSnapshots = SortedMap(mgA -> NonEmptyList.one(binary), mgB -> NonEmptyList.one(binary))
      )
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.one(validSig))

      runBatch = (result: SortedMap[Address, Hash]) =>
        mkManager(
          executionQuorum = 1,
          committeeMembership = Set(signerPeer),
          reExecuteDerivation =
            (_, _, _, _) => scalarCalls.update(_ + 1) *> IO.raiseError(new AssertionError("scalar replay fallback reached")),
          reExecuteDerivations = Some((_, _, _) => batchCalls.update(_ + 1).as(result))
        ).flatMap(_.verifyEmbedded(checkpoint))

      missing <- runBatch(SortedMap(mgA -> claimedRoot))
      extra <- runBatch(SortedMap(mgA -> claimedRoot, mgB -> claimedRoot, extraMg -> claimedRoot))
      scalarCount <- scalarCalls.get
      batchCount <- batchCalls.get
    } yield {
      def isCannotDerive(result: ShardCheckpointAcceptResult): Boolean = result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => reason.contains("cannot re-derive")
        case _                                            => false
      }

      expect.all(
        isCannotDerive(missing),
        isCannotDerive(extra),
        scalarCount == 0,
        batchCount == 2
      )
    }
  }

  // ============================================================================
  // Test 5c (execution-base-pin fail-closed): affirmative mismatch on another MG remains a typed diagnostic rejection
  // ============================================================================

  test("mixed window: one MG affirmatively mismatches, another can't-derive → RejectedReExecutionMismatch (affirmative evidence wins)") {
    res =>
      implicit val (h, sp, _, checkpointSigner) = res
      for {
        (signerKp, signerPeer) <- mkSigner

        mgBad = Address.fromBytes("mg-bad".getBytes("UTF-8"))
        mgUncheckable = Address.fromBytes("mg-uncheckable".getBytes("UTF-8"))
        claimedRoot = Hash("11" * 32)
        binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
        delta = ShardDerivedStateDelta(
          perMetagraphMptRoots = SortedMap(mgBad -> claimedRoot, mgUncheckable -> claimedRoot),
          includedSnapshots = SortedMap(mgBad -> NonEmptyList.of(binary), mgUncheckable -> NonEmptyList.of(binary))
        )
        shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

        validSig <- mkValidSig(shell, signerKp, signerPeer)
        checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

        // mgBad re-derives to a REAL root ≠ claimed (affirmative deviation evidence); mgUncheckable yields the can't-check sentinel.
        // The affirmative mismatch wins the diagnostic classification regardless of the uncheckable sibling. It still cannot slash without
        // separately portable evidence.
        reExecCb = (
          (
            a: Address,
            _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
            _: SnapshotOrdinal,
            _: SnapshotOrdinal
          ) => IO.pure(if (a === mgBad) Hash("ff" * 32) else Hash.empty)
        ): (
          Address,
          NonEmptyList[Signed[StateChannelSnapshotBinary]],
          SnapshotOrdinal,
          SnapshotOrdinal
        ) => IO[Hash]

        mgr <- mkManager(
          executionQuorum = 1,
          committeeMembership = Set(signerPeer),
          reExecuteDerivation = reExecCb
        )
        result <- mgr.evaluate(checkpoint)
      } yield
        result match {
          case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, committeeSigners) =>
            expect(reason.contains("re-exec mismatch")) &&
            expect.same(List(signerPeer), committeeSigners)
          case other => failure(s"Expected RejectedReExecutionMismatch (affirmative mismatch on mgBad), got $other")
        }
  }

  // ============================================================================
  // Test 6: checkpoint depth cannot replace execution quorum
  // ============================================================================

  test("high checkpoint ordinal cannot replace execution quorum") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      calls <- cats.effect.Ref.of[IO, Int](0)

      mg = Address.fromBytes("mg-f".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 50L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      mgr <- mkManager(
        executionQuorum = 2,
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = (_, _, _, _) => calls.updateAndGet(_ + 1).as(mptRoot)
      )
      result <- mgr.verifyEmbedded(checkpoint)
      callCount <- calls.get
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("execution quorum missing")) && expect.same(0, callCount)
        case other => failure(s"Expected missing-quorum rejection, got $other")
      }
  }

  test("duplicate committee peer IDs reject before committee resolution or cryptographic registry lookup") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      committeeLookups <- cats.effect.Ref.of[IO, Int](0)
      keyLookups <- cats.effect.Ref.of[IO, Int](0)
      reExecCalls <- cats.effect.Ref.of[IO, Int](0)
      mg = Address.fromBytes("mg-pending-mismatch".getBytes("UTF-8"))
      claimedRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, claimedRoot, binary)
      shell = mkCheckpointShell(shardOrd = 50L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig, validSig))

      failOperatorRegistry = new OperatorConsensusKeyRegistry[IO] {
        private val emptyPairRegistry = OperatorConsensusKeyRegistry.empty[IO]
        def get(peerId: PeerId): IO[Option[OperatorConsensusKeys]] =
          keyLookups.update(_ + 1) *> IO.raiseError(new AssertionError(s"operator-key registry reached for $peerId"))
        def list: IO[Map[PeerId, OperatorConsensusKeys]] = IO.pure(Map.empty)
        val kesRegistry: KesRegistry[IO] = emptyPairRegistry.kesRegistry
        val vrfRegistry: VrfRegistry[IO] = emptyPairRegistry.vrfRegistry
      }

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 2,
        etaRotationSnapshots = 1000L,
        committeeMembership = (_, _) => committeeLookups.update(_ + 1) *> IO.raiseError(new AssertionError("committee resolution reached")),
        operatorKeyRegistry = failOperatorRegistry,
        shardAssignment = ShardAssignment.make[IO](numShards = 1),
        shardEtaFor = (_, _) => IO.raiseError(new AssertionError("shard eta resolution reached")),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => reExecCalls.update(_ + 1) *> IO.raiseError(new AssertionError("re-execution reached"))
      )
      result <- mgr.verifyEmbedded(checkpoint)
      committeeLookupCount <- committeeLookups.get
      keyLookupCount <- keyLookups.get
      reExecCount <- reExecCalls.get
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("duplicate committee peer IDs")) &&
          expect.same(0, committeeLookupCount) &&
          expect.same(0, keyLookupCount) &&
          expect.same(0, reExecCount)
        case other => failure(s"Expected duplicate-signer structural rejection, got $other")
      }
  }

  test("signature-list cardinality above deterministic committee size rejects before Ed25519/KES/VRF verification") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (kp2, p2) <- mkSigner
      committeeLookups <- cats.effect.Ref.of[IO, Int](0)
      keyLookups <- cats.effect.Ref.of[IO, Int](0)
      reExecCalls <- cats.effect.Ref.of[IO, Int](0)

      mg = Address.fromBytes("mg-over-cardinality".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("binary-content".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 51L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      sig1 <- mkValidSig(shell, kp1, p1)
      sig2 <- mkValidSig(shell, kp2, p2)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig1, sig2))

      failOperatorRegistry = new OperatorConsensusKeyRegistry[IO] {
        private val emptyPairRegistry = OperatorConsensusKeyRegistry.empty[IO]
        def get(peerId: PeerId): IO[Option[OperatorConsensusKeys]] =
          keyLookups.update(_ + 1) *> IO.raiseError(new AssertionError(s"operator-key registry reached for $peerId"))
        def list: IO[Map[PeerId, OperatorConsensusKeys]] = IO.pure(Map.empty)
        val kesRegistry: KesRegistry[IO] = emptyPairRegistry.kesRegistry
        val vrfRegistry: VrfRegistry[IO] = emptyPairRegistry.vrfRegistry
      }

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        executionQuorum = 2,
        etaRotationSnapshots = 1000L,
        committeeMembership = (_, _) => committeeLookups.updateAndGet(_ + 1).as(Set(p1)),
        operatorKeyRegistry = failOperatorRegistry,
        shardAssignment = ShardAssignment.make[IO](numShards = 1),
        shardEtaFor = (_, _) => IO.raiseError(new AssertionError("shard eta resolution reached")),
        producerDutyValidator = TestCheckpointDutyValidator.allow[IO],
        reExecuteDerivation = (_, _, _, _) => reExecCalls.update(_ + 1) *> IO.raiseError(new AssertionError("re-execution reached"))
      )
      result <- mgr.evaluate(checkpoint)
      committeeLookupCount <- committeeLookups.get
      keyLookupCount <- keyLookups.get
      reExecCount <- reExecCalls.get
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("signature-list cardinality=2 exceeds deterministic committee size=1")) &&
          expect.same(1, committeeLookupCount) &&
          expect.same(0, keyLookupCount) &&
          expect.same(0, reExecCount)
        case other => failure(s"Expected signature-list cardinality rejection, got $other")
      }
  }

  // ============================================================================
  // verifyEmbedded — DETERMINISTIC adopt-verifier (the split-safety contract)
  // ============================================================================

  /** The load-bearing determinism test: two managers with the same deterministic committee, quorum, and replay inputs return the same
    * verdict. There is no node-local shard-tip or finality-trigger dependency.
    */
  test("verifyEmbedded determinism: same wire and deterministic inputs produce the same result") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-det".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      mgrA <- mkManager(executionQuorum = 1, committeeMembership = Set(signerPeer))
      mgrB <- mkManager(executionQuorum = 1, committeeMembership = Set(signerPeer))
      resultA <- mgrA.verifyEmbedded(checkpoint)
      resultB <- mgrB.verifyEmbedded(checkpoint)
    } yield
      expect.same(ShardCheckpointAcceptResult.Accepted, resultA) &&
        expect.same(ShardCheckpointAcceptResult.Accepted, resultB) &&
        expect.same(resultA, resultB)
  }

  test("verifyEmbedded: multiple valid signatures still require matching re-execution") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (kp2, p2) <- mkSigner
      (kp3, p3) <- mkSigner

      mg = Address.fromBytes("mg-quorum".getBytes("UTF-8"))
      binary = mkSignedBinary("c".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      sig1 <- mkValidSig(shell, kp1, p1)
      sig2 <- mkValidSig(shell, kp2, p2)
      sig3 <- mkValidSig(shell, kp3, p3)
      // Signature count is not an economic-validity shortcut; the callback must still run and match.
      reExecCalledRef <- cats.effect.Ref.of[IO, Boolean](false)
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => reExecCalledRef.set(true).as(Hash("11" * 32))
      ): (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[
        Hash
      ]
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig1, sig2, sig3))
      mgr <- mkManager(
        executionQuorum = 3,
        committeeMembership = Set(p1, p2, p3),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.verifyEmbedded(checkpoint)
      reExecCalled <- reExecCalledRef.get
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result) && expect(reExecCalled)
  }

  test("verifyEmbedded: one valid signer plus a wrong root deterministically rejects") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner

      mg = Address.fromBytes("mg-single-signer".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("c".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      sig1 <- mkValidSig(shell, kp1, p1)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig1))

      // re-exec returns a WRONG root vs the delta's claimed root ⇒ RejectedReExecutionMismatch, deterministically.
      reExecWrong = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => IO.pure(Hash("ff" * 32))
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgrA <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(p1) ++ (1 to 3).map(i => PeerId(Hex(f"$i%02x" * 64))).toSet,
        reExecuteDerivation = reExecWrong
      )
      mgrB <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(p1) ++ (1 to 3).map(i => PeerId(Hex(f"$i%02x" * 64))).toSet,
        reExecuteDerivation = reExecWrong
      )
      resultA <- mgrA.verifyEmbedded(checkpoint)
      resultB <- mgrB.verifyEmbedded(checkpoint)
    } yield {
      val isMismatch: ShardCheckpointAcceptResult => Boolean = {
        case _: ShardCheckpointAcceptResult.RejectedReExecutionMismatch => true
        case _                                                          => false
      }
      expect(isMismatch(resultA)) && expect(isMismatch(resultB)) && expect.same(resultA, resultB)
    }
  }

  test("verifyEmbedded: pre-check fail (signer not in committee) → Rejected (deterministic)") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp, p) <- mkSigner
      mg = Address.fromBytes("mg-precheck".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("c".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p)
      sig <- mkValidSig(shell, kp, p)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      mgr <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set.empty[PeerId] // signer not in committee ⇒ deterministic pre-check rejection
      )
      result <- mgr.verifyEmbedded(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("not in committee"))
        case other                                        => failure(s"Expected Rejected(not in committee), got $other")
      }
  }

  // ============================================================================
  // REAL committee-VRF membership verify (Slice 13 — replaced verifyVrfStructural)
  // ============================================================================

  /** Build a fully-wired checkpoint + a manager whose `shardEtaFor` resolves to `realShardEta` and whose atomic operator record pairs the
    * signer with the supplied VRF VK, then run the deterministic `verifyEmbedded` adopt-verifier. The only thing that can flip
    * Accepted→Rejected is the per-signer pre-check (where the committee-VRF verify lives). This isolates the VRF verify as the decision
    * under test (a valid proof ⇒ Accepted; a forged/wrong-epoch/impersonated proof ⇒ Rejected at pre-check).
    */
  private def mkVrfScenario(
    sigForCheckpoint: ShardCheckpoint => IO[(CommitteeMemberSignature, Array[Byte])],
    registryVkFor: (PeerId, Array[Byte]) => Map[PeerId, Array[Byte]],
    managerShardEta: (ShardId, EtaPeriod) => IO[Option[Array[Byte]]],
    signerPeer: PeerId
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], checkpointSigner: RegisteredCheckpointSigner): IO[ShardCheckpointAcceptResult] = {
    val mg = Address.fromBytes("mg-vrf".getBytes("UTF-8"))
    val binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
    val delta = mkDelta(mg, Hash("11" * 32), binary)
    val shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
    for {
      (sig, vrfVk) <- sigForCheckpoint(shell)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      selectedVk <- IO.fromOption(registryVkFor(signerPeer, vrfVk).get(signerPeer))(
        new AssertionError(s"missing VRF key override for $signerPeer")
      )
      registry <- registryWithPair(
        signerPeer,
        _.copy(vrfPublicKey = VrfPublicKey.fromBytes(selectedVk))
      )
      mgr <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(signerPeer),
        operatorKeyRegistryOverride = registry.some,
        shardEtaFor = managerShardEta
      )
      result <- mgr.verifyEmbedded(checkpoint)
    } yield result
  }

  test("real VRF verify: valid committee member's proof (registered VK, correct (shardEta, slot)) → Accepted") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      result <- mkVrfScenario(
        // Real proof over the SAME `realShardEta` the manager resolves; signer's real VK registered.
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, realShardEta),
        registryVkFor = (peer, vk) => Map(peer -> vk),
        managerShardEta = (_, _) => IO.pure(realShardEta.some),
        signerPeer = signerPeer
      )
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result)
  }

  test("real VRF verify: forged (garbage) VRF proof under a registered VK → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      result <- mkVrfScenario(
        // Registered VK, but the 80-byte proof is garbage (structurally length-valid so it passes the old placeholder, but the REAL
        // EcVrf25519 verify must reject it). Use the signer's real VK so membership + the registry lookup both pass and only the
        // cryptographic verify can fail.
        sigForCheckpoint = shell =>
          for {
            validSig <- checkpointSigner.sign(shell, signerKp, signerPeer, realShardEta)
            (_, vrfVk) = VrfKeyDeriver.deriveVrfKeyPair(signerKp)
          } yield
            (
              validSig.copy(vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x42.toByte))),
              vrfVk
            ),
        registryVkFor = (peer, vk) => Map(peer -> vk),
        managerShardEta = (_, _) => IO.pure(realShardEta.some),
        signerPeer = signerPeer
      )
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("committee-VRF proof verify failed"))
        case other                                        => failure(s"Expected Rejected(committee-VRF ...), got $other")
      }
  }

  test("real VRF verify: wrong-epoch eta (proof over a DIFFERENT shardEta than the manager resolves) → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    // Producer signed under one epoch's eta; the manager resolves a DIFFERENT epoch's eta (a wrong-epoch replay). The VRF message
    // differs ⇒ the cryptographic verify fails. This is the load-bearing cross-epoch-replay guard.
    val otherEta: Array[Byte] = Array.tabulate[Byte](32)(i => (i * 13 + 5).toByte)
    for {
      (signerKp, signerPeer) <- mkSigner
      result <- mkVrfScenario(
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, otherEta), // proof over otherEta
        registryVkFor = (peer, vk) => Map(peer -> vk),
        managerShardEta = (_, _) => IO.pure(realShardEta.some), // manager resolves realShardEta ≠ otherEta
        signerPeer = signerPeer
      )
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("committee-VRF proof verify failed"))
        case other                                        => failure(s"Expected Rejected(committee-VRF ...), got $other")
      }
  }

  test("real VRF verify: impersonation — registry maps the signer to a DIFFERENT VK than signed the proof → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (otherKp, _) <- mkSigner
      // The registry binds `signerPeer` to a STRANGER's VK; the proof is a real proof under the signer's OWN VK. The verify under the
      // registered (wrong) VK fails — a peer cannot pass off another operator's committee slot.
      (otherVrfVk: Array[Byte]) = VrfKeyDeriver.deriveVrfKeyPair(otherKp)._2
      result <- mkVrfScenario(
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, realShardEta),
        registryVkFor = (peer, _) => Map(peer -> otherVrfVk), // wrong VK for this peer
        managerShardEta = (_, _) => IO.pure(realShardEta.some),
        signerPeer = signerPeer
      )
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("committee-VRF proof verify failed"))
        case other                                        => failure(s"Expected Rejected(committee-VRF ...), got $other")
      }
  }

  test("real VRF verify: determinism — two managers (same registry + eta) reach the SAME verdict for the same checkpoint") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    // The verify is a pure function of (atomic operator-key registry, shardEtaFor, proof bytes). Two managers built with byte-identical pair + eta
    // resolve identically — the #261 split-safety property for the consensus-gating pre-check.
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-vrf-det".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("c".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      (sig, vrfVk) <- mkRealVrfSig(shell, signerKp, signerPeer, realShardEta)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      registry <- registryWithPair(signerPeer, _.copy(vrfPublicKey = VrfPublicKey.fromBytes(vrfVk)))
      etaFor = (_: ShardId, _: EtaPeriod) => IO.pure(realShardEta.some)
      mgrA <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(signerPeer),
        operatorKeyRegistryOverride = registry.some,
        shardEtaFor = etaFor
      )
      mgrB <- mkManager(
        executionQuorum = 1,
        committeeMembership = Set(signerPeer),
        operatorKeyRegistryOverride = registry.some,
        shardEtaFor = etaFor
      )
      rA <- mgrA.verifyEmbedded(checkpoint)
      rB <- mgrB.verifyEmbedded(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, rA) && expect.same(rA, rB)
  }

  // ============================================================================
  // Sanity: signed-checkpoint helper round-trips (kept to confirm fixture builds work)
  // ============================================================================

  test("fixture sanity: mkSignedCheckpoint produces a Signed envelope") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-sanity".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("c".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 0L, gl0Anchor = 0L, delta = delta, placeholderPeerId = signerPeer)
      sig <- mkValidSig(shell, signerKp, signerPeer)
      signed = mkSignedCheckpoint(shell, NonEmptyList.of(sig))
    } yield expect(signed.value.committeeSignatures.head.peerId === signerPeer)
  }
}
