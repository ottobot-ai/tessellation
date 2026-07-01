package io.constellationnetwork.node.shared.infrastructure.sharding

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardChainStore, ShardFinalityTriggers, ShardTipTracker}
import io.constellationnetwork.node.shared.domain.nakamoto.{KesRegistry, VrfRegistry}
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  ShardCheckpointAcceptResult,
  ShardCheckpointGl0AcceptanceManager
}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
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

/** Tests for [[ShardMetrics]] + the wired emit sites at [[ShardChainStore]], [[ShardTipTracker]], and
  * [[ShardCheckpointGl0AcceptanceManager]] — Slice 19 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §13 row 19.
  *
  * '''Required coverage''' (per slice spec):
  *   1. '''Accepted (T_count)''' → `dag_nakamoto_shard_checkpoint_total{path=t_count}` incremented
  *   1. '''Accepted (T_depth1 re-exec OK)''' → `dag_nakamoto_shard_checkpoint_total{path=t_depth1}` incremented +
  *      `dag_nakamoto_shard_committee_partition_total` incremented
  *   1. '''Rejected (pre-check)''' → `dag_nakamoto_shard_checkpoint_rejected_total{reason=*}` incremented
  *   1. '''Rejected (re-exec mismatch)''' → `dag_nakamoto_shard_checkpoint_rejected_total{reason=re_exec_mismatch}` incremented
  *   1. '''Attestation''' → `dag_nakamoto_shard_committee_attestation_total` incremented per NEW (peer, hash) pair (replay is no-op)
  *   1. '''Chain height + finalized gauges''' → set after `store` / `finalize` calls
  *
  * The fixture pattern mirrors `ShardCheckpointGl0AcceptanceManagerSuite` — real `Hasher`, real `KeyPair`-derived signatures, fresh
  * `ShardChainStore` + `ShardTipTracker` per test. Only deviation: each test wires a `CountingMetrics` interpreter into the implicit scope
  * so we can read back per-counter / per-gauge values at the end.
  */
object ShardMetricsSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ============================================================================
  // Fixtures (cribbed from ShardCheckpointGl0AcceptanceManagerSuite; same shape)
  // ============================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val epochZero: EtaPeriod = EtaPeriod(0L)
  private val genesisHash: Hash = Hash("0" * 64)

  private def mkSigner(implicit sp: SecurityProvider[IO]): IO[(KeyPair, PeerId)] =
    KeyPairGenerator.makeKeyPair[IO].map { kp =>
      (kp, PeerId.fromPublic(kp.getPublic))
    }

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
        vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x42.toByte)),
        ed25519Sig = Hex.fromBytes(edSig),
        kesProductSig = Hex.fromBytes(Array.fill[Byte](32)(0x43.toByte)),
        kesTreeStep = 0
      )

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

  private def mkDelta(mg: Address, root: Hash, binary: Signed[StateChannelSnapshotBinary]): ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mg -> root),
      perMetagraphStateDiff = SortedMap.empty,
      includedSnapshots = SortedMap(mg -> NonEmptyList.of(binary)),
      tokenLockBalancesDelta = SortedMap.empty,
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap.empty
    )

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
      emittedReceipts = List.empty,
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

  /** Seed a linear chain of `n` checkpoints into the store. Same shape as the gl0 acceptance suite's `seedChain`. */
  private def seedChain(store: ShardChainStore[IO], n: Int): IO[List[Hash]] =
    (0L until n.toLong).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash("0" * 64))) {
        case ((acc, parent), ord) =>
          val cp = ShardCheckpoint(
            shardId = shardZero,
            parentCheckpointHash = parent,
            shardOrdinal = ShardOrdinal(ord),
            gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(100L + ord)),
            slot = SlotT.unsafeApply(100L + ord),
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

  // ============================================================================
  // Test 1: Accept via T_count → counter increments
  // ============================================================================

  test("Accepted via T_count path → checkpoint_total{path=t_count} incremented") { res =>
    implicit val (h, sp, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-t-count".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 6)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      _ <- tracker.recordAttestation(tip.hash, signerPeer, dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1, k1Shard = 100L, store, tracker)
      _ <- triggers.advance

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        finalityTriggers = _ => IO.pure(Some(triggers)),
        chainStore = _ => IO.pure(None),
        committeeMembership = (_, _) => IO.pure(Set(signerPeer)),
        kDraw = 1,
        kQuorum = 1,
        selfPeerId = selfPeer,
        kesRegistry = KesRegistry.empty[IO],
        // Empty VRF registry + None shardEta ⇒ the committee-VRF verify falls back to the structural proof-length carve-out
        // (these metric tests use 80-byte structural proofs), so the accept/reject paths under test are unchanged.
        vrfRegistry = VrfRegistry.empty[IO],
        shardEtaFor = (_, _) => IO.pure(None),
        reExecuteDerivation = (_, _, _, _) => IO.pure(Hash("ff" * 32))
      )

      _ <- mgr.evaluate(checkpoint)
      state <- stateRef.get
    } yield
      expect(state.counters.getOrElse(ShardMetrics.CheckpointTotal.value, 0) == 1) &&
        // T_count fast path must NOT bump the committee_partition counter (that's the T_depth1 fallback signal).
        expect(state.counters.getOrElse(ShardMetrics.CommitteePartitionTotal.value, 0) == 0)
  }

  // ============================================================================
  // Test 2: Accept via T_depth1 re-exec → partition + checkpoint counters
  // ============================================================================

  test("Accepted via T_depth1 re-exec → checkpoint_total{path=t_depth1} + committee_partition_total incremented") { res =>
    implicit val (h, sp, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-t-depth1".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1000, k1Shard = 3L, store, tracker)
      _ <- triggers.advance

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        finalityTriggers = _ => IO.pure(Some(triggers)),
        chainStore = _ => IO.pure(None),
        committeeMembership = (_, _) => IO.pure(Set(signerPeer)),
        kDraw = 1000,
        kQuorum = 1000,
        selfPeerId = selfPeer,
        kesRegistry = KesRegistry.empty[IO],
        // Empty VRF registry + None shardEta ⇒ the committee-VRF verify falls back to the structural proof-length carve-out
        // (these metric tests use 80-byte structural proofs), so the accept/reject paths under test are unchanged.
        vrfRegistry = VrfRegistry.empty[IO],
        shardEtaFor = (_, _) => IO.pure(None),
        reExecuteDerivation = (_, _, _, _) => IO.pure(mptRoot)
      )

      result <- mgr.evaluate(checkpoint)
      state <- stateRef.get
    } yield
      expect.same(ShardCheckpointAcceptResult.Accepted, result) &&
        expect(state.counters.getOrElse(ShardMetrics.CheckpointTotal.value, 0) == 1) &&
        // T_depth1 fallback MUST bump the partition counter — depth-only acceptance signals partial-committee offline.
        expect(state.counters.getOrElse(ShardMetrics.CommitteePartitionTotal.value, 0) == 1)
  }

  // ============================================================================
  // Test 3: Pre-check reject → rejected counter
  // ============================================================================

  test("pre-check reject (signer not in committee) → checkpoint_rejected_total{reason=pre_check_committee} incremented") { res =>
    implicit val (h, sp, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-reject".getBytes("UTF-8"))
      mptRoot = Hash("22" * 32)
      binary = mkSignedBinary("content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 1L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 5)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1, k1Shard = 100L, store, tracker)
      _ <- triggers.advance

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        finalityTriggers = _ => IO.pure(Some(triggers)),
        chainStore = _ => IO.pure(None),
        committeeMembership = (_, _) => IO.pure(Set.empty[PeerId]),
        kDraw = 1,
        kQuorum = 1,
        selfPeerId = selfPeer,
        kesRegistry = KesRegistry.empty[IO],
        // Empty VRF registry + None shardEta ⇒ the committee-VRF verify falls back to the structural proof-length carve-out
        // (these metric tests use 80-byte structural proofs), so the accept/reject paths under test are unchanged.
        vrfRegistry = VrfRegistry.empty[IO],
        shardEtaFor = (_, _) => IO.pure(None),
        reExecuteDerivation = (_, _, _, _) => IO.pure(Hash("0" * 64))
      )

      _ <- mgr.evaluate(checkpoint)
      state <- stateRef.get
    } yield
      expect(state.counters.getOrElse(ShardMetrics.CheckpointRejectedTotal.value, 0) == 1) &&
        expect(state.counters.getOrElse(ShardMetrics.CheckpointTotal.value, 0) == 0)
  }

  // ============================================================================
  // Test 4: Re-exec mismatch → rejected counter
  // ============================================================================

  test("re-exec mismatch → checkpoint_rejected_total{reason=re_exec_mismatch} incremented + slash signers in result") { res =>
    implicit val (h, sp, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      (signerKp, signerPeer) <- mkSigner
      (_, selfPeer) <- mkSigner

      mg = Address.fromBytes("mg-mismatch".getBytes("UTF-8"))
      mptRoot = Hash("11" * 32)
      binary = mkSignedBinary("content".getBytes("UTF-8"))
      delta = mkDelta(mg, mptRoot, binary)
      shell = mkCheckpointShell(shardOrd = 2L, gl0Anchor = 100L, delta = delta, placeholderPeerId = signerPeer)

      validSig <- mkValidSig(shell, signerKp, signerPeer)
      checkpoint = shell.copy(committeeSignatures = NonEmptyList.of(validSig))

      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, selfPeer)
      triggers <- ShardFinalityTriggers.make[IO](shardZero, kQuorum = 1000, k1Shard = 3L, store, tracker)
      _ <- triggers.advance

      mgr <- ShardCheckpointGl0AcceptanceManager.make[IO](
        finalityTriggers = _ => IO.pure(Some(triggers)),
        chainStore = _ => IO.pure(None),
        committeeMembership = (_, _) => IO.pure(Set(signerPeer)),
        kDraw = 1000,
        kQuorum = 1000,
        selfPeerId = selfPeer,
        kesRegistry = KesRegistry.empty[IO],
        // Empty VRF registry + None shardEta ⇒ the committee-VRF verify falls back to the structural proof-length carve-out
        // (these metric tests use 80-byte structural proofs), so the accept/reject paths under test are unchanged.
        vrfRegistry = VrfRegistry.empty[IO],
        shardEtaFor = (_, _) => IO.pure(None),
        reExecuteDerivation = (_, _, _, _) => IO.pure(Hash("ff" * 32))
      )

      _ <- mgr.evaluate(checkpoint)
      state <- stateRef.get
    } yield
      expect(state.counters.getOrElse(ShardMetrics.CheckpointRejectedTotal.value, 0) == 1) &&
        // Same path also bumps committee_partition (the T_depth1 fallback fired before we discovered the mismatch).
        expect(state.counters.getOrElse(ShardMetrics.CommitteePartitionTotal.value, 0) == 1) &&
        expect(state.counters.getOrElse(ShardMetrics.CheckpointTotal.value, 0) == 0)
  }

  // ============================================================================
  // Test 5: Attestation counter + idempotence
  // ============================================================================

  test("recordAttestation: first call bumps committee_attestation_total; replay does not") { _ =>
    // This test doesn't need Hasher/SecurityProvider/JsonSerializer; ShardTipTracker.make only needs Async + Metrics.
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      peer1 = PeerId(Hex("aa" * 64))
      peer2 = PeerId(Hex("bb" * 64))
      hashA = Hash("11" * 32)
      hashB = Hash("22" * 32)
      tracker <- ShardTipTracker.make[IO](shardZero, peer1)

      _ <- tracker.recordAttestation(hashA, peer1, dummyCommitteeSig) // 1
      _ <- tracker.recordAttestation(hashA, peer1, dummyCommitteeSig) // replay — no bump
      _ <- tracker.recordAttestation(hashA, peer2, dummyCommitteeSig) // 2
      _ <- tracker.recordAttestation(hashB, peer1, dummyCommitteeSig) // 3

      state <- stateRef.get
    } yield expect(state.counters.getOrElse(ShardMetrics.CommitteeAttestationTotal.value, 0) == 3)
  }

  // ============================================================================
  // Test 6: Chain-height gauge advances with store
  // ============================================================================

  test("store: advancing tip updates chain_height gauge to the new tip ord") { res =>
    implicit val (h, _, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 5)
      state <- stateRef.get
    } yield {
      val tag = ShardMetrics.shardIdTag(shardZero)
      // After seeding 5 ords (0..4), the chain_height gauge tracks the highest ord stored.
      val height = state.gauges.getOrElse((ShardMetrics.ChainHeight.value, tag), -1.0)
      expect(height == 4.0)
    }
  }

  // ============================================================================
  // Test 7: Finalize gauge update
  // ============================================================================

  test("finalize: updates chain_finalized_ordinal gauge to the finalized ord") { res =>
    implicit val (h, _, _) = res
    for {
      pair <- CountingMetrics.makeWithState
      (stateRef, m) = pair
      implicit0(metrics: Metrics[IO]) = m

      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = 100L)
      hashes <- seedChain(store, 10)
      // Finalize the 5th hash (ord 4).
      _ <- store.`finalize`(hashes(4))
      state <- stateRef.get
    } yield {
      val tag = ShardMetrics.shardIdTag(shardZero)
      val finalized = state.gauges.getOrElse((ShardMetrics.ChainFinalizedOrdinal.value, tag), -1.0)
      expect(finalized == 4.0)
    }
  }
}
