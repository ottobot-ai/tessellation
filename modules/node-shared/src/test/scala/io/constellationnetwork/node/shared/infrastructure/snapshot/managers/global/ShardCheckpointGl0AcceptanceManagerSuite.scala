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
import io.constellationnetwork.node.shared.domain.nakamoto.sharding._
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.RegisteredCheckpointSigner
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
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
  *   1. '''T_count path accept''' — triggers report T_count qualifies and re-exec matches → `Accepted`
  *   1. '''T_depth1-only re-exec accept''' — T_count doesn't qualify, T_depth1 does, re-exec matches → `Accepted`
  *   1. '''T_depth1-only re-exec mismatch''' — T_count doesn't qualify, T_depth1 does, re-exec returns DIFFERENT hash for one MG →
  *      `RejectedReExecutionMismatch` with slash list = checkpoint signers
  *   1. '''Neither qualifies → Pending''' — brand-new checkpoint, no attestations, depth not yet reached → `PendingMoreAttestations`
  *   1. '''Unknown shard''' — checkpoint's shardId not in `finalityTriggers` map → `Rejected`
  *
  * '''Test fixture pattern''':
  *   - Build checkpoint envelopes with real Ed25519, KES, and VRF signatures, and inject registries containing the corresponding public
  *     keys. Missing KES/VRF registrations fail closed exactly as they do in production.
  *   - `ShardFinalityTriggers` is built fresh per test off a `ShardChainStore` + `ShardTipTracker` pair so we control which trigger
  *     qualifies which ordinal exactly.
  */
object ShardCheckpointGl0AcceptanceManagerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], RegisteredCheckpointSigner)

  // Slice 19: ShardChainStore, ShardTipTracker, and ShardCheckpointGl0AcceptanceManager constructors now all require
  // Metrics[F]. No-op interpreter — slice-19 metric semantics are covered by ShardMetricsSuite.
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
  private def mkSigner(implicit sp: SecurityProvider[IO]): IO[(KeyPair, PeerId)] =
    KeyPairGenerator.makeKeyPair[IO].map { kp =>
      (kp, PeerId.fromPublic(kp.getPublic))
    }

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

  /** Dummy `CommitteeMemberSignature` used to satisfy the 3-arg `recordAttestation` signature in tests that only care about peerId-based
    * counting. The tracker stores/counts by the explicit `peerId` argument, not by the sig's internal peerId.
    */
  private val dummyCommitteeSig: CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex("00" * 64)),
      vrfProof = Hex.fromBytes(Array.emptyByteArray),
      ed25519Sig = Hex.fromBytes(Array.emptyByteArray),
      kesProductSig = Hex.fromBytes(Array.emptyByteArray),
      kesTreeStep = 0
    )

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

  /** Build a `ShardChainStore` containing a single checkpoint at the given ordinal (so `bestTip` resolves), and a `ShardFinalityTriggers`
    * instance wired with the provided `kQuorum` (the T_count_shard admit count, DIRECTLY) and `k1Shard`. The composite advances on
    * construction so the inner triggers' Refs reflect the current chain state.
    */
  private def mkFinalityTriggers(
    kQuorum: Int,
    k1Shard: Long,
    chainLength: Int,
    selfId: PeerId,
    attestations: List[(Hash, PeerId)] = List.empty
  )(implicit h: Hasher[IO]): IO[(ShardChainStore[IO], ShardFinalityTriggers[IO])] =
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, chainLength)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      _ <- attestations.traverse_ { case (hash, peer) => tracker.recordAttestation(hash, peer, dummyCommitteeSig) }
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum, k1Shard, store, tracker)
      _ <- triggers.advance
    } yield (store, triggers)

  /** Seed a linear chain of `n` checkpoints (ords 1..n) into the store. Used by the depth-finality + tip-tracker tests so the `bestTip`
    * resolves at the highest ord. Returns the canonical hash list in chain order. The `Hasher[IO]` consumed by `store.store` is captured at
    * store construction time (via `ShardChainStore.make[F: Hasher]`), so seedChain doesn't need an implicit param of its own.
    */
  private def seedChain(
    store: ShardChainStore[IO],
    n: Int
  ): IO[List[Hash]] =
    (1L to n.toLong).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash.empty)) {
        case ((acc, parent), ord) =>
          val cp = ShardCheckpoint(
            shardId = shardZero,
            parentCheckpointHash = parent,
            shardOrdinal = ShardOrdinal(ord),
            gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L + ord)),
            slot = SlotT.unsafeApply(100L + ord),
            derivedStateDelta = ShardDerivedStateDelta.empty,
            committeeSignatures = NonEmptyList.of(
              CommitteeMemberSignature(
                peerId = PeerId(Hex(f"${ord.toInt + 1}%02x" * 64)),
                vrfProof = Hex("aa" * 80),
                ed25519Sig = Hex("bb" * 64),
                kesProductSig = Hex("cc" * 128),
                kesTreeStep = 0
              )
            ),
            epoch = epochZero
          )
          val sentinelProof = SignatureProof(io.constellationnetwork.schema.ID.Id(Hex("11" * 64)), Signature(Hex("22" * 70)))
          val signed = Signed(cp, NonEmptySet.of(sentinelProof))
          store
            .store(
              signed,
              parentHash = parent,
              shardOrdinal = ShardOrdinal(ord),
              slot = 100L + ord,
              vrfOutput = Array.fill[Byte](32)(ord.toByte)
            )
            .flatMap { _ =>
              store.bestTip.map(_.get.hash).map(h => (acc :+ h, h))
            }
      }
      .map(_._1)

  /** Build the manager under test with stubbed callbacks. Each callback is a parameter so individual tests can override exactly the
    * surfaces under test (e.g., wire a different `committeeMembership` to control the pre-check membership predicate).
    */
  private def mkManager(
    finalityTriggers: Map[ShardId, ShardFinalityTriggers[IO]],
    committeeMembership: Set[PeerId],
    kesRegistryOverride: Option[KesRegistry[IO]] = None,
    vrfRegistryOverride: Option[VrfRegistry[IO]] = None,
    shardEtaFor: (ShardId, EtaPeriod) => IO[Option[Array[Byte]]] = (_, _) => IO.pure(realShardEta.some),
    reExecuteDerivation: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal, SnapshotOrdinal) => IO[Hash] =
      (_, _, _, _) => IO.pure(Hash("11" * 32))
  )(
    implicit h: Hasher[IO],
    sp: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner
  ): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      finalityTriggers = sid => IO.pure(finalityTriggers.get(sid)),
      committeeMembership = (_, _) => IO.pure(committeeMembership),
      kesRegistry = kesRegistryOverride.getOrElse(checkpointSigner.kesRegistry),
      vrfRegistry = vrfRegistryOverride.getOrElse(checkpointSigner.vrfRegistry),
      shardAssignment = ShardAssignment.make[IO](numShards = 1),
      shardEtaFor = shardEtaFor,
      reExecuteDerivation = reExecuteDerivation
    )

  // ============================================================================
  // Test 1: Pre-check fail — signer not in committee
  // ============================================================================

  test("pre-check fail: signer peerId not in committeeMembership → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-a".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // Build triggers so this is NOT the failing path — finality phase qualifies; pre-check must fire first and reject.
      (_, triggers) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 5, selfId = selfPeer)
      // committeeMembership is EMPTY — signer is not in committee → pre-check membership predicate fails.
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set.empty[PeerId]
      )
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

  test("pre-check fail: Ed25519 sig doesn't verify under signer's VK → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (_, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-b".getBytes("UTF-8"))
      mptRoot = Hash("22" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      // BAD Ed25519 sig — garbage bytes, won't verify under the recovered VK.
      badSig = mkBadEdSig(signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(badSig))

      (_, triggers) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 5, selfId = selfPeer)
      // committeeMembership includes our signer so the membership pre-check passes — Ed25519 must be the one that fails.
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer)
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("Ed25519"))
        case other => failure(s"Expected Rejected(Ed25519 ...), got $other")
      }
  }

  test("pre-check fail: missing registered KES key rejects a cryptographically valid signer") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-missing-kes".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("content".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 100L, delta, signerPeer)
      sig <- mkValidSig(shell, signerKp, signerPeer)
      manager <- mkManager(
        finalityTriggers = Map.empty,
        committeeMembership = Set(signerPeer),
        kesRegistryOverride = KesRegistry.empty[IO].some
      )
      result <- manager.verifyEmbedded(shell.copy(committeeSignatures = NonEmptyList.of(sig)))
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("KES sig verify failed"))
        case other                                        => failure(s"Expected fail-closed KES rejection, got $other")
      }
  }

  test("pre-check fail: missing registered VRF key rejects a cryptographically valid signer") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      mg = Address.fromBytes("mg-missing-vrf".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("content".getBytes("UTF-8")))
      shell = mkCheckpointShell(1L, 100L, delta, signerPeer)
      sig <- mkValidSig(shell, signerKp, signerPeer)
      manager <- mkManager(
        finalityTriggers = Map.empty,
        committeeMembership = Set(signerPeer),
        vrfRegistryOverride = VrfRegistry.empty[IO].some
      )
      result <- manager.verifyEmbedded(shell.copy(committeeSignatures = NonEmptyList.of(sig)))
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("committee-VRF proof verify failed"))
        case other                                        => failure(s"Expected fail-closed VRF rejection, got $other")
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
      manager <- mkManager(
        finalityTriggers = Map.empty,
        committeeMembership = Set.empty
      )
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
  // Test 3: T_count path accept
  // ============================================================================

  test("T_count path: triggers report T_count qualifies and re-exec matches → Accepted") { res =>
    implicit val (h, sp, _, checkpointSigner) = res

    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-c".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // Wire T_count to qualify: kQuorum=1, attestation count = 1 (the only signer attests) ⇒ trigger qualifies the
      // bestTip's ord (which we set to 5 by seeding a 6-chain). The checkpoint's ord is 1, comfortably ≤ 5 ⇒ T_count qualifies.
      // Use a fresh self for the tracker so the count includes the signer-as-attester (the self-exclusion default would otherwise
      // hide our attestation if signerPeer matched the tracker's self).
      attesterPeer = signerPeer // signer attests its own checkpoint hash
      (store, _) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 6, selfId = selfPeer)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      _ <- tracker.recordAttestation(tip.hash, attesterPeer, dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1, k1Shard = 100L, store, tracker)
      _ <- triggers.advance

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
        finalityTriggers = Map(shardZero -> triggers),
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
  // Test 4: T_depth1-only re-exec accept
  // ============================================================================

  test("T_depth1-only re-exec: T_count doesn't qualify, T_depth1 does, re-exec matches → Accepted") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-d".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // T_depth1 qualifies (chain length 10, k1=3 → qualifies ord up to 7); T_count does NOT qualify (huge kTarget=1000,
      // 0 attestations → required threshold never met).
      (_, triggers) <- mkFinalityTriggers(
        kQuorum = 1000,
        k1Shard = 3L,
        chainLength = 10,
        selfId = selfPeer
      )
      // Re-exec returns the SAME mptRoot the delta claims → all-match path → Accepted.
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal,
          _: SnapshotOrdinal
        ) => IO.pure(mptRoot)
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal,
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result)
  }

  // ============================================================================
  // Test 5: T_depth1-only re-exec mismatch — slash signers
  // ============================================================================

  test("T_depth1-only re-exec mismatch: re-exec returns DIFFERENT hash → RejectedReExecutionMismatch with slash list = signers") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp1, signerPeer1) <- mkSigner
      (signerKp2, signerPeer2) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-e".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer1)

      // TWO real signers — both go in the slash list when the re-exec mismatches.
      sig1 <- mkValidSig(shell, signerKp1, signerPeer1)
      sig2 <- mkValidSig(shell, signerKp2, signerPeer2)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig1, sig2))

      // T_depth1 qualifies; T_count doesn't.
      (_, triggers) <- mkFinalityTriggers(
        kQuorum = 1000,
        k1Shard = 3L,
        chainLength = 10,
        selfId = selfPeer
      )
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
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer1, signerPeer2),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, slashSigners) =>
          expect(reason.contains("re-exec mismatch")) &&
          expect.same(List(signerPeer1, signerPeer2), slashSigners)
        case other => failure(s"Expected RejectedReExecutionMismatch, got $other")
      }
  }

  // ============================================================================
  // Test 5b (execution-base-pin fail-closed): re-exec CANNOT-DERIVE sentinel — plain Rejected, NO slash
  // ============================================================================

  test("T_depth1-only re-exec CANNOT-DERIVE (Hash.empty sentinel): fail-closed plain Rejected — NEVER RejectedReExecutionMismatch/slash") {
    res =>
      implicit val (h, sp, _, checkpointSigner) = res
      for {
        (signerKp, signerPeer) <- mkSigner
        (_, selfPeer) <- mkSigner

        mg = Address.fromBytes("mg-e2".getBytes("UTF-8"))
        mptRoot = Hash("11" * 32)
        binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
        delta = mkDelta(mg, mptRoot, binary)
        shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

        validSig <- mkValidSig(shell, signerKp, signerPeer)
        checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

        // T_depth1 qualifies; T_count doesn't — forces the re-exec path.
        (_, triggers) <- mkFinalityTriggers(
          kQuorum = 1000,
          k1Shard = 3L,
          chainLength = 10,
          selfId = selfPeer
        )
        // Re-exec returns Hash.empty — the wiring's fail-closed "cannot re-derive" sentinel (`reExecDerivationAtPinnedBase` omitted: the
        // pinned execution-base is unresolvable below this node's retention / not yet reached, or the derivation deferred). That is
        // "THIS NODE can't check", NOT "the committee deviated" (the same reading `watchtowerReExec` applies when it filters
        // `Hash.empty` mismatches). The checkpoint must be REJECTED (fail-closed: never admitted unverified) but the signers must
        // NOT be slash targets — `RejectedReExecutionMismatch` feeds the DURABLE 100% `InvalidStateProof` slash
        // (`GlobalSnapshotAcceptanceManager.adoptShardCheckpoints` → `WatchtowerSlashRequest`), which demands an AFFIRMATIVE
        // pinned-base re-derivation mismatch as evidence.
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
          finalityTriggers = Map(shardZero -> triggers),
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

  // ============================================================================
  // Test 5c (execution-base-pin fail-closed): affirmative mismatch on ANOTHER MG still slashes
  // ============================================================================

  test("mixed window: one MG affirmatively mismatches, another can't-derive → RejectedReExecutionMismatch (affirmative evidence wins)") {
    res =>
      implicit val (h, sp, _, checkpointSigner) = res
      for {
        (signerKp, signerPeer) <- mkSigner
        (_, selfPeer) <- mkSigner

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

        (_, triggers) <- mkFinalityTriggers(
          kQuorum = 1000,
          k1Shard = 3L,
          chainLength = 10,
          selfId = selfPeer
        )
        // mgBad re-derives to a REAL root ≠ claimed (affirmative deviation evidence); mgUncheckable yields the can't-check sentinel.
        // The affirmative mismatch is cryptographic evidence the committee deviated — it must still slash, regardless of the
        // uncheckable sibling.
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
          finalityTriggers = Map(shardZero -> triggers),
          committeeMembership = Set(signerPeer),
          reExecuteDerivation = reExecCb
        )
        result <- mgr.evaluate(checkpoint)
      } yield
        result match {
          case ShardCheckpointAcceptResult.RejectedReExecutionMismatch(reason, slashSigners) =>
            expect(reason.contains("re-exec mismatch")) &&
            expect.same(List(signerPeer), slashSigners)
          case other => failure(s"Expected RejectedReExecutionMismatch (affirmative mismatch on mgBad), got $other")
        }
  }

  // ============================================================================
  // Test 6: Neither qualifies → Pending
  // ============================================================================

  test("neither trigger qualifies → PendingMoreAttestations") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-f".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      // Checkpoint ord is HIGH (50), but the chain only reached ord 3, so neither T_count nor T_depth1 can possibly qualify ord 50.
      shell = mkCheckpointShell(shardOrd = 50L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // Build triggers with a SHORT chain (length 3 → bestTip ord=2). T_count requires kTarget=100, no attestations → never
      // qualifies. T_depth1 with k1=10 vs bestOrd=2 → never qualifies. Both report MinValue → both predicates `false` for ord=50.
      (_, triggers) <- mkFinalityTriggers(
        kQuorum = 100,
        k1Shard = 10L,
        chainLength = 3,
        selfId = selfPeer
      )
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer)
      )
      result <- mgr.evaluate(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.PendingMoreAttestations, result)
  }

  test("neither trigger qualifies but re-execution mismatches → RejectedReExecutionMismatch") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner
      calls <- cats.effect.Ref.of[IO, Int](0)
      mg = Address.fromBytes("mg-pending-mismatch".getBytes("UTF-8"))
      claimedRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, claimedRoot, binary)
      shell = mkCheckpointShell(shardOrd = 50L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))
      (_, triggers) <- mkFinalityTriggers(kQuorum = 100, k1Shard = 10L, chainLength = 3, selfId = selfPeer)
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        reExecuteDerivation = (_, _, _, _) => calls.updateAndGet(_ + 1).as(Hash("22" * 32))
      )
      result <- mgr.evaluate(checkpoint)
      callCount <- calls.get
    } yield expect.all(result.isInstanceOf[ShardCheckpointAcceptResult.RejectedReExecutionMismatch], callCount == 1)
  }

  // ============================================================================
  // Test 7: Unknown shard → Rejected
  // ============================================================================

  test("unknown shard: checkpoint's shardId not in finalityTriggers map → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner

      mg = Address.fromBytes("mg-g".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // finalityTriggers map is EMPTY for shardZero — the manager's `finalityTriggers(shardZero)` callback returns None.
      mgr <- mkManager(
        finalityTriggers = Map.empty[ShardId, ShardFinalityTriggers[IO]],
        committeeMembership = Set(signerPeer)
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("unknown shard"))
        case other => failure(s"Expected Rejected(unknown shard), got $other")
      }
  }

  // ============================================================================
  // verifyEmbedded — DETERMINISTIC adopt-verifier (the split-safety contract)
  // ============================================================================

  /** The load-bearing determinism test: `verifyEmbedded` MUST return the SAME result under TWO DIFFERENT node-local `finalityTriggers`
    * states. This proves it reads NO node-local input (no triggers, no shard tip) — the property the whole symmetric-adopt design rests on.
    * Node A's triggers qualify the checkpoint's ord (T_count fires); node B's triggers never qualify (short chain, huge kTarget, zero
    * attestations). `verifyEmbedded` ignores both and decides from deterministic re-execution, so both nodes return `Accepted`.
    */
  test("verifyEmbedded determinism: SAME result under two DIFFERENT finalityTriggers states (no node-local dependency)") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeerA) <- mkSigner
      (_, selfPeerB) <- mkSigner

      mg = Address.fromBytes("mg-det".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      // The matching re-execution result is accepted regardless of trigger state.

      // Node A: triggers FULLY qualify the checkpoint ord (T_count fires, deep chain).
      (storeA, _) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 1L, chainLength = 10, selfId = selfPeerA)
      tipA <- storeA.bestTip.map(_.get)
      trackerA <- ShardTipTracker.make[IO](shardZero, selfPeerA)
      _ <- trackerA.recordAttestation(tipA.hash, signerPeer, dummyCommitteeSig)
      triggersA <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1, k1Shard = 1L, storeA, trackerA)
      _ <- triggersA.advance

      // Node B: triggers NEVER qualify (short chain, huge kQuorum, no attestations) — neither T_count nor T_depth1.
      (_, triggersB) <- mkFinalityTriggers(kQuorum = 1000, k1Shard = 1000L, chainLength = 2, selfId = selfPeerB)

      mgrA <- mkManager(
        finalityTriggers = Map(shardZero -> triggersA),
        committeeMembership = Set(signerPeer)
      )
      mgrB <- mkManager(
        finalityTriggers = Map(shardZero -> triggersB),
        committeeMembership = Set(signerPeer)
      )
      resultA <- mgrA.verifyEmbedded(checkpoint)
      resultB <- mgrB.verifyEmbedded(checkpoint)
    } yield
      expect.same(ShardCheckpointAcceptResult.Accepted, resultA) &&
        expect.same(ShardCheckpointAcceptResult.Accepted, resultB) &&
        expect.same(resultA, resultB) // the determinism assertion: byte-identical outcome on two different-trigger nodes
  }

  test("verifyEmbedded: multiple valid signatures still require matching re-execution") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (kp2, p2) <- mkSigner
      (kp3, p3) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-quorum".getBytes("UTF-8"))
      binary = mkSignedBinary("c".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p1)
      sig1 <- mkValidSig(shell, kp1, p1)
      sig2 <- mkValidSig(shell, kp2, p2)
      sig3 <- mkValidSig(shell, kp3, p3)
      // Signature count is not an economic-validity shortcut; the callback must still run and match.
      (_, anyTriggers) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 1L, chainLength = 3, selfId = selfPeer)
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
        finalityTriggers = Map(shardZero -> anyTriggers),
        committeeMembership = Set(p1, p2, p3, selfPeer),
        reExecuteDerivation = reExecCb
      )
      result <- mgr.verifyEmbedded(checkpoint)
      reExecCalled <- reExecCalledRef.get
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result) && expect(reExecCalled)
  }

  test("verifyEmbedded: one valid signer plus a wrong root deterministically rejects, trigger-independent") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (kp1, p1) <- mkSigner
      (_, selfPeerA) <- mkSigner
      (_, selfPeerB) <- mkSigner

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

      // Two different trigger states again — consensus replay must be node-local-independent.
      (_, triggersA) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 1L, chainLength = 10, selfId = selfPeerA)
      (_, triggersB) <- mkFinalityTriggers(kQuorum = 1000, k1Shard = 1000L, chainLength = 2, selfId = selfPeerB)
      mgrA <- mkManager(
        finalityTriggers = Map(shardZero -> triggersA),
        committeeMembership = Set(p1) ++ (1 to 3).map(i => PeerId(Hex(f"$i%02x" * 64))).toSet,
        reExecuteDerivation = reExecWrong
      )
      mgrB <- mkManager(
        finalityTriggers = Map(shardZero -> triggersB),
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
      (_, selfPeer) <- mkSigner
      mg = Address.fromBytes("mg-precheck".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("c".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = p)
      sig <- mkValidSig(shell, kp, p)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      (_, triggers) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 1L, chainLength = 10, selfId = selfPeer)
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
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

  /** Build a fully-wired checkpoint + a manager whose `shardEtaFor` resolves to `realShardEta` and whose `vrfRegistry` maps the signer to
    * the supplied VK, then run the DETERMINISTIC `verifyEmbedded` adopt-verifier. The only thing that can flip Accepted→Rejected is the
    * per-signer pre-check (where the committee-VRF verify lives). This isolates the VRF verify as the decision under test (a valid proof ⇒
    * Accepted; a forged/wrong-epoch/impersonated proof ⇒ Rejected at pre-check).
    */
  private def mkVrfScenario(
    sigForCheckpoint: ShardCheckpoint => IO[(CommitteeMemberSignature, Array[Byte])],
    registryVkFor: (PeerId, Array[Byte]) => Map[PeerId, Array[Byte]],
    managerShardEta: (ShardId, EtaPeriod) => IO[Option[Array[Byte]]],
    signerPeer: PeerId,
    selfPeer: PeerId
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO], checkpointSigner: RegisteredCheckpointSigner): IO[ShardCheckpointAcceptResult] = {
    val mg = Address.fromBytes("mg-vrf".getBytes("UTF-8"))
    val binary = mkSignedBinary("binary-content".getBytes("UTF-8"))
    val delta = mkDelta(mg, Hash("11" * 32), binary)
    val shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
    for {
      (sig, vrfVk) <- sigForCheckpoint(shell)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      (_, triggers) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 5, selfId = selfPeer)
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        vrfRegistryOverride = VrfRegistry.make[IO](registryVkFor(signerPeer, vrfVk)).some,
        shardEtaFor = managerShardEta
      )
      result <- mgr.verifyEmbedded(checkpoint)
    } yield result
  }

  test("real VRF verify: valid committee member's proof (registered VK, correct (shardEta, slot)) → Accepted") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner
      result <- mkVrfScenario(
        // Real proof over the SAME `realShardEta` the manager resolves; signer's real VK registered.
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, realShardEta),
        registryVkFor = (peer, vk) => Map(peer -> vk),
        managerShardEta = (_, _) => IO.pure(realShardEta.some),
        signerPeer = signerPeer,
        selfPeer = selfPeer
      )
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result)
  }

  test("real VRF verify: forged (garbage) VRF proof under a registered VK → Rejected") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner
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
        signerPeer = signerPeer,
        selfPeer = selfPeer
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
      (_, selfPeer) <- mkSigner
      result <- mkVrfScenario(
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, otherEta), // proof over otherEta
        registryVkFor = (peer, vk) => Map(peer -> vk),
        managerShardEta = (_, _) => IO.pure(realShardEta.some), // manager resolves realShardEta ≠ otherEta
        signerPeer = signerPeer,
        selfPeer = selfPeer
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
      (_, selfPeer) <- mkSigner
      // The registry binds `signerPeer` to a STRANGER's VK; the proof is a real proof under the signer's OWN VK. The verify under the
      // registered (wrong) VK fails — a peer cannot pass off another operator's committee slot.
      (otherVrfVk: Array[Byte]) = VrfKeyDeriver.deriveVrfKeyPair(otherKp)._2
      result <- mkVrfScenario(
        sigForCheckpoint = shell => mkRealVrfSig(shell, signerKp, signerPeer, realShardEta),
        registryVkFor = (peer, _) => Map(peer -> otherVrfVk), // wrong VK for this peer
        managerShardEta = (_, _) => IO.pure(realShardEta.some),
        signerPeer = signerPeer,
        selfPeer = selfPeer
      )
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) => expect(reason.contains("committee-VRF proof verify failed"))
        case other                                        => failure(s"Expected Rejected(committee-VRF ...), got $other")
      }
  }

  test("real VRF verify: determinism — two managers (same registry + eta) reach the SAME verdict for the same checkpoint") { res =>
    implicit val (h, sp, _, checkpointSigner) = res
    // The verify is a pure function of (vrfRegistry, shardEtaFor, proof bytes). Two managers built with byte-identical registry + eta
    // resolve identically — the #261 split-safety property for the consensus-gating pre-check.
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfA) <- mkSigner
      (_, selfB) <- mkSigner
      mg = Address.fromBytes("mg-vrf-det".getBytes("UTF-8"))
      delta = mkDelta(mg, Hash("11" * 32), mkSignedBinary("c".getBytes("UTF-8")))
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)
      (sig, vrfVk) <- mkRealVrfSig(shell, signerKp, signerPeer, realShardEta)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(sig))
      registry = VrfRegistry.make[IO](Map(signerPeer -> vrfVk))
      etaFor = (_: ShardId, _: EtaPeriod) => IO.pure(realShardEta.some)
      (_, triggersA) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 5, selfId = selfA)
      (_, triggersB) <- mkFinalityTriggers(kQuorum = 1, k1Shard = 100L, chainLength = 5, selfId = selfB)
      mgrA <- mkManager(
        finalityTriggers = Map(shardZero -> triggersA),
        committeeMembership = Set(signerPeer),
        vrfRegistryOverride = registry.some,
        shardEtaFor = etaFor
      )
      mgrB <- mkManager(
        finalityTriggers = Map(shardZero -> triggersB),
        committeeMembership = Set(signerPeer),
        vrfRegistryOverride = registry.some,
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
