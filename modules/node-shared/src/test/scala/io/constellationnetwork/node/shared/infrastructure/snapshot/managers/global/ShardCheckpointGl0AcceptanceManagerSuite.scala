package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardFinalityTriggers, ShardTipTracker}
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.signature.{Signed, Signing}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardCheckpointGl0AcceptanceManager]] — Slice 9 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §7.3.
  *
  * '''Required coverage''' (per slice spec):
  *   1. '''Pre-check fail: signer not in committee''' — checkpoint signed by a peerId not in `committeeMembership(...)` → `Rejected`
  *   1. '''Pre-check fail: bad Ed25519 sig''' — signature doesn't verify → `Rejected`
  *   1. '''T_count path accept''' — triggers report T_count qualifies → `Accepted` without invoking re-exec
  *   1. '''T_depth1-only re-exec accept''' — T_count doesn't qualify, T_depth1 does, re-exec matches → `Accepted`
  *   1. '''T_depth1-only re-exec mismatch''' — T_count doesn't qualify, T_depth1 does, re-exec returns DIFFERENT hash for one MG →
  *      `RejectedReExecutionMismatch` with slash list = checkpoint signers
  *   1. '''Neither qualifies → Pending''' — brand-new checkpoint, no attestations, depth not yet reached → `PendingMoreAttestations`
  *   1. '''Unknown shard''' — checkpoint's shardId not in `finalityTriggers` map → `Rejected`
  *
  * '''Test fixture pattern''':
  *   - Build the `Signed[ShardCheckpoint]` envelopes with REAL `KeyPair`s for the committee signers so the Ed25519 pre-check verifies
  *     against the recovered public key from each `PeerId`. The KES sig is structurally valid (8-byte placeholder); the manager's KES
  *     verify reads from the injected `KesRegistry` and uses the "no registry entry → accept" carve-out for tests.
  *   - VRF proof bytes use the structural minimum length (80 bytes per `EcVrf25519`); the Slice 9 manager runs a structural check only —
  *     full cryptographic VRF verify ships in Slice 13 when the VRF VK registry is wired.
  *   - `ShardFinalityTriggers` is built fresh per test off a `ShardChainStore` + `ShardTipTracker` pair so we control which trigger
  *     qualifies which ordinal exactly.
  */
object ShardCheckpointGl0AcceptanceManagerSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  // Slice 19: ShardChainStore, ShardTipTracker, and ShardCheckpointGl0AcceptanceManager constructors now all require
  // Metrics[F]. No-op interpreter — slice-19 metric semantics are covered by ShardMetricsSuite.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

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

  /** Build a committee-member sig that PASSES every pre-check predicate for the given peer:
    *   - peerId is in the test's expected committee
    *   - Ed25519 sig verifies under the peer's recovered VK
    *   - KES sig is non-empty (registry-absent carve-out kicks in)
    *   - VRF proof is 80 bytes (structural check passes)
    */
  private def mkValidSig(
    checkpoint: ShardCheckpoint,
    kp: KeyPair,
    peerId: PeerId
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[CommitteeMemberSignature] =
    for {
      preimageHash <- Hasher[IO].hash(checkpoint.signingPreimage)
      edSig <- Signing.signData[IO](preimageHash.getBytes)(kp.getPrivate)
    } yield
      CommitteeMemberSignature(
        peerId = peerId,
        vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x42.toByte)), // structural-min length passes the slice-9 VRF check
        ed25519Sig = Hex.fromBytes(edSig),
        kesProductSig = Hex.fromBytes(Array.fill[Byte](32)(0x43.toByte)), // non-empty; registry-absent carve-out → accept
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
      derivedStateDelta = delta,
      emittedReceipts = List.empty,
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
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary)),
      tokenLockBalancesDelta = SortedMap.empty,
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap.empty
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
    * instance wired with the provided `kTarget` and `k1Shard`. The composite advances on construction so the inner triggers' Refs reflect
    * the current chain state.
    */
  private def mkFinalityTriggers(
    kTarget: Int,
    k1Shard: Long,
    chainLength: Int,
    selfId: PeerId,
    attestations: List[(Hash, PeerId)] = List.empty
  )(implicit h: Hasher[IO]): IO[(ShardChainStore[IO], ShardFinalityTriggers[IO])] =
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, chainLength)
      tracker <- ShardTipTracker.make[IO](shardZero, selfId)
      _ <- attestations.traverse_ { case (hash, peer) => tracker.recordAttestation(hash, peer) }
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kTarget, k1Shard, store, tracker)
      _ <- triggers.advance
    } yield (store, triggers)

  /** Seed a linear chain of `n` checkpoints (ords 0..n-1) into the store. Used by the depth-finality + tip-tracker tests so the `bestTip`
    * resolves at the highest ord. Returns the canonical hash list in chain order. The `Hasher[IO]` consumed by `store.store` is captured at
    * store construction time (via `ShardChainStore.make[F: Hasher]`), so seedChain doesn't need an implicit param of its own.
    */
  private def seedChain(
    store: ShardChainStore[IO],
    n: Int
  ): IO[List[Hash]] =
    (0L until n.toLong).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash("0" * 64))) {
        case ((acc, parent), ord) =>
          val cp = ShardCheckpoint(
            shardId = shardZero,
            parentCheckpointHash = parent,
            shardOrdinal = ShardOrdinal(ord),
            gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L + ord)),
            derivedStateDelta = ShardDerivedStateDelta.empty,
            emittedReceipts = List.empty,
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
              slot = ord + 1L,
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
    chainStore: Map[ShardId, ShardChainStore[IO]] = Map.empty,
    committeeMembership: Set[PeerId],
    kTarget: Int = 4,
    selfId: PeerId,
    kesRegistry: KesRegistry[IO] = KesRegistry.empty[IO],
    reExecuteDerivation: (Address, NonEmptyList[Signed[StateChannelSnapshotBinary]], SnapshotOrdinal) => IO[Hash] = (_, _, _) =>
      IO.pure(Hash("0" * 64))
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[ShardCheckpointGl0AcceptanceManager[IO]] =
    ShardCheckpointGl0AcceptanceManager.make[IO](
      finalityTriggers = sid => IO.pure(finalityTriggers.get(sid)),
      chainStore = sid => IO.pure(chainStore.get(sid)),
      committeeMembership = (_, _) => IO.pure(committeeMembership),
      kTarget = kTarget,
      selfPeerId = selfId,
      kesRegistry = kesRegistry,
      reExecuteDerivation = reExecuteDerivation
    )

  // ============================================================================
  // Test 1: Pre-check fail — signer not in committee
  // ============================================================================

  test("pre-check fail: signer peerId not in committeeMembership → Rejected") { res =>
    implicit val (h, sp, _) = res
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
      (_, triggers) <- mkFinalityTriggers(kTarget = 1, k1Shard = 100L, chainLength = 5, selfId = selfPeer)
      // committeeMembership is EMPTY — signer is not in committee → pre-check membership predicate fails.
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set.empty[PeerId],
        selfId = selfPeer
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
    implicit val (h, sp, _) = res
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

      (_, triggers) <- mkFinalityTriggers(kTarget = 1, k1Shard = 100L, chainLength = 5, selfId = selfPeer)
      // committeeMembership includes our signer so the membership pre-check passes — Ed25519 must be the one that fails.
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        selfId = selfPeer
      )
      result <- mgr.evaluate(checkpoint)
    } yield
      result match {
        case ShardCheckpointAcceptResult.Rejected(reason) =>
          expect(reason.contains("Ed25519"))
        case other => failure(s"Expected Rejected(Ed25519 ...), got $other")
      }
  }

  // ============================================================================
  // Test 3: T_count path accept
  // ============================================================================

  test("T_count path: triggers report T_count qualifies → Accepted without invoking re-exec") { res =>
    implicit val (h, sp, _) = res

    // The re-exec callback is wired to ALWAYS RETURN A WRONG HASH so we can prove it was never called (if it were called, the
    // mismatch would surface as RejectedReExecutionMismatch). Sentinel-hash trick: re-exec returns 0xff but the delta carries 0x11
    // — if the test reaches the re-exec path, we'd see a Rejected; we expect plain Accepted.
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

      // Wire T_count to qualify: K_S=1, attestation count = 1 (the only signer attests). ⌈2·1/3⌉ = 1 ⇒ trigger qualifies the
      // bestTip's ord (which we set to 5 by seeding a 6-chain). The checkpoint's ord is 1, comfortably ≤ 5 ⇒ T_count qualifies.
      // Use a fresh self for the tracker so the count includes the signer-as-attester (the self-exclusion default would otherwise
      // hide our attestation if signerPeer matched the tracker's self).
      attesterPeer = signerPeer // signer attests its own checkpoint hash
      (store, _) <- mkFinalityTriggers(kTarget = 1, k1Shard = 100L, chainLength = 6, selfId = selfPeer)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      _ <- tracker.recordAttestation(tip.hash, attesterPeer)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kTarget = 1, k1Shard = 100L, store, tracker)
      _ <- triggers.advance

      // Re-exec returns a HASH THAT DOES NOT MATCH — proves the manager never enters the re-exec path on the T_count fast path.
      reExecCalledRef <- cats.effect.Ref.of[IO, Boolean](false)
      reExecCb = (
        (
          _: Address,
          _: NonEmptyList[Signed[StateChannelSnapshotBinary]],
          _: SnapshotOrdinal
        ) => reExecCalledRef.set(true).as(Hash("ff" * 32))
      ): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        selfId = selfPeer,
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
      reExecCalled <- reExecCalledRef.get
    } yield
      expect.same(ShardCheckpointAcceptResult.Accepted, result) &&
        expect(!reExecCalled) // T_count fast path must NOT invoke re-exec
  }

  // ============================================================================
  // Test 4: T_depth1-only re-exec accept
  // ============================================================================

  test("T_depth1-only re-exec: T_count doesn't qualify, T_depth1 does, re-exec matches → Accepted") { res =>
    implicit val (h, sp, _) = res
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
        kTarget = 1000,
        k1Shard = 3L,
        chainLength = 10,
        selfId = selfPeer
      )
      // Re-exec returns the SAME mptRoot the delta claims → all-match path → Accepted.
      reExecCb = ((_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal) => IO.pure(mptRoot)): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        selfId = selfPeer,
        reExecuteDerivation = reExecCb
      )
      result <- mgr.evaluate(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.Accepted, result)
  }

  // ============================================================================
  // Test 5: T_depth1-only re-exec mismatch — slash signers
  // ============================================================================

  test("T_depth1-only re-exec mismatch: re-exec returns DIFFERENT hash → RejectedReExecutionMismatch with slash list = signers") { res =>
    implicit val (h, sp, _) = res
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
        kTarget = 1000,
        k1Shard = 3L,
        chainLength = 10,
        selfId = selfPeer
      )
      // Re-exec returns a WRONG hash for the MG — the manager should detect mismatch.
      wrongRoot = Hash("ff" * 32)
      reExecCb = ((_: Address, _: NonEmptyList[Signed[StateChannelSnapshotBinary]], _: SnapshotOrdinal) => IO.pure(wrongRoot)): (
        Address,
        NonEmptyList[Signed[StateChannelSnapshotBinary]],
        SnapshotOrdinal
      ) => IO[Hash]

      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer1, signerPeer2),
        selfId = selfPeer,
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
  // Test 6: Neither qualifies → Pending
  // ============================================================================

  test("neither trigger qualifies → PendingMoreAttestations") { res =>
    implicit val (h, sp, _) = res
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
        kTarget = 100,
        k1Shard = 10L,
        chainLength = 3,
        selfId = selfPeer
      )
      mgr <- mkManager(
        finalityTriggers = Map(shardZero -> triggers),
        committeeMembership = Set(signerPeer),
        selfId = selfPeer
      )
      result <- mgr.evaluate(checkpoint)
    } yield expect.same(ShardCheckpointAcceptResult.PendingMoreAttestations, result)
  }

  // ============================================================================
  // Test 7: Unknown shard → Rejected
  // ============================================================================

  test("unknown shard: checkpoint's shardId not in finalityTriggers map → Rejected") { res =>
    implicit val (h, sp, _) = res
    for {
      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

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
        committeeMembership = Set(signerPeer),
        selfId = selfPeer
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
  // Sanity: signed-checkpoint helper round-trips (kept to confirm fixture builds work)
  // ============================================================================

  test("fixture sanity: mkSignedCheckpoint produces a Signed envelope") { res =>
    implicit val (h, sp, _) = res
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
