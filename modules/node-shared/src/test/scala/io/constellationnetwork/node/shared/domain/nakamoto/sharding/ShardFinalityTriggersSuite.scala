package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardTipTracker]] and [[ShardFinalityTriggers]] — Slice 6 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.4.
  *
  * '''Required coverage''':
  *   1. `T_count_shard` qualifies at the configured `kQuorum` count and not below it.
  *   1. tracker self-exclusion remains available for diagnostics, while the selection trigger counts all distinct embedded signers.
  *   1. `T_depth1_shard` qualifies at depth — 10 checkpoints, `k1Shard=5`; the highest qualifying ordinal is 5.
  *   1. Composite max-of — T_count fires for ord 8, T_depth1 fires for ord 3 → composite latestQualifying = 8.
  *   1. Monotone — `latestQualifying` Ref never goes backwards even if eval regresses.
  *   1. Pruning — after `pruneBelow(ord)`, attestations for checkpoints with shardOrd < ord are gone.
  */
object ShardFinalityTriggersSuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  // Slice 19: ShardChainStore + ShardTipTracker constructors now require an implicit Metrics[F]. Tests use a no-op
  // interpreter so they keep asserting on chain-store / tracker semantics rather than metric mechanics (which are
  // covered by Slice 19's own ShardMetricsSuite).
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield Hasher.forJson[IO]

  // ============================================================================
  // Fixtures — mirror the ShardChainStoreSuite shape so the store / tracker / triggers compose cleanly.
  // ============================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  /** Build a deterministic PeerId from a name string (mirrors the `pid` helper in `FinalityTriggerSuite`). */
  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x".toString).mkString.padTo(128, '0')))

  private def hex(s: String): Hex = Hex(s)

  /** Single sentinel signature — the store doesn't verify, so we don't need real key material. */
  private def sentinelProof: SignatureProof =
    SignatureProof(Id(hex("11" * 64)), Signature(hex("22" * 70)))

  private def mkSigned[A](value: A): Signed[A] =
    Signed(value, NonEmptySet.of(sentinelProof))

  /** Build a sentinel committee-member signature. Store ignores its content. */
  private def mkCommitteeSig(peerByte: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex(f"$peerByte%02x".toString * 64)),
      vrfProof = hex("aa" * 80),
      ed25519Sig = hex("bb" * 64),
      kesProductSig = hex("cc" * 128),
      kesTreeStep = 0
    )

  /** Dummy `CommitteeMemberSignature` used to satisfy the 3-arg `recordAttestation` signature in tests that only care about peerId-based
    * counting. The tracker stores/counts by the explicit `peerId` argument, not by the sig's internal peerId.
    */
  private val dummyCommitteeSig: CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = pid("self"),
      vrfProof = Hex.fromBytes(Array.emptyByteArray),
      ed25519Sig = Hex.fromBytes(Array.emptyByteArray),
      kesProductSig = Hex.fromBytes(Array.emptyByteArray),
      kesTreeStep = 0
    )

  private def mkCheckpoint(
    ord: Long,
    parent: Hash,
    peerByte: Int,
    gl0Anchor: Long
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = parent,
      shardOrdinal = ShardOrdinal(ord),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      slot = SlotT.unsafeApply(gl0Anchor),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.of(mkCommitteeSig(peerByte)),
      epoch = EtaPeriod(0L)
    )

  private def mkSignedCheckpoint(
    ord: Long,
    parent: Hash,
    peerByte: Int,
    gl0Anchor: Long
  ): Signed[ShardCheckpoint] =
    mkSigned(mkCheckpoint(ord, parent, peerByte, gl0Anchor))

  private def vrf(seed: Int): Array[Byte] = Array.fill[Byte](32)(seed.toByte)

  /** Seed a chain of `n` checkpoints (ords 1..n) into the store; return the list of canonical hashes in order. Used by the depth +
    * composite tests.
    */
  private def seedChain(
    store: ShardChainStore[IO],
    n: Int
  ): IO[List[Hash]] =
    (1L to n.toLong).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash.empty)) {
        case ((acc, parent), ord) =>
          val signed = mkSignedCheckpoint(ord, parent = parent, peerByte = ord.toInt + 1, gl0Anchor = 100L + ord)
          store
            .store(
              signed,
              parentHash = parent,
              shardOrdinal = ShardOrdinal(ord),
              slot = signed.value.slot.value.value,
              vrfOutput = vrf(ord.toInt + 1)
            )
            .flatMap { _ =>
              store.bestTip.map(_.get.hash).map(h => (acc :+ h, h))
            }
      }
      .map(_._1)

  // ============================================================================
  // Test 1: T_count_shard qualifies at threshold (kQuorum=3 → required = 3 distinct attesters, DIRECTLY)
  // ============================================================================

  test("T_count_shard: kQuorum=3, 3 non-self attesters attest bestTip → qualifies bestTip ord") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-2"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-3"), dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L, // intentionally large so T_depth1_shard contributes nothing to this test
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
      current <- triggers.currentQualifyingCheckpoint
    } yield
      expect.same(SnapshotOrdinal.unsafeApply(tip.signed.value.shardOrdinal.value), result) &&
        expect.same(tip.hash.some, current.map(_.hash))
  }

  test("T_count_shard: kQuorum=3, only 2 non-self attesters → below threshold → MinValue") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-2"), dummyCommitteeSig)
      // only 2 attesters; required = 3
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  // ============================================================================
  // Test 2: T_count_shard counts self (run-10 Gap-B alignment with verifyEmbedded's distinct-signer bar)
  // ============================================================================

  test("T_count_shard: self counted — local node + 2 others attest → count = 3 → qualifies (kQuorum=3)") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker.recordAttestation(tip.hash, self, dummyCommitteeSig) // self attests
      _ <- tracker.recordAttestation(tip.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-2"), dummyCommitteeSig)
      // The trigger counts self + attester-1 + attester-2 = 3 >= 3 → qualifies. This matches the bar the embed is held to:
      // `verifyEmbedded` counts EVERY distinct signer (producer + self + remote) against kQuorum. The old self-excluded count
      // demanded kQuorum REMOTE attestations — unanimity at N=5/kQuorum=4 — and one slow peer stalled every embed (run-10
      // Gap-B). Self alone still can't qualify anything at kQuorum >= 2.
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
      rawCount <- tracker.attestationCountFor(tip.hash, excludeSelf = false)
      excludedCount <- tracker.attestationCountFor(tip.hash, excludeSelf = true)
    } yield
      expect.same(SnapshotOrdinal.unsafeApply(tip.signed.value.shardOrdinal.value), result) &&
        expect.same(3, rawCount) &&
        expect.same(2, excludedCount)
  }

  test("T_count_shard: self + 1 other at kQuorum=3 → count = 2 → does not qualify (self grants no shortcut)") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker.recordAttestation(tip.hash, self, dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-1"), dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  // ============================================================================
  // Test 3: T_depth1_shard qualifies at depth
  // ============================================================================

  test("T_depth1_shard: 10 checkpoints (ords 1..10), k1Shard=5 → qualifies ord (10 - 5) = 5") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 1000, // intentionally large so T_count_shard contributes nothing
        k1Shard = 5L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tDepth1Shard.latestQualifyingOrdinal
      current <- triggers.currentQualifyingCheckpoint
    } yield
      // bestTip is ord=10 (10 checkpoints, 1..10), k1Shard=5 → the implementation's depth candidate is 10 - 5 = 5.
      // Per design doc §5.4 row 3 — "depth > k1Shard" (strict). At ord 5 the chain extends to ord 10, depth = 10 - 5 = 5,
      // depth > 5 is false. Match the gl0 TDepth1Trigger semantics which uses `bestOrd - k` (subtraction; result becomes the
      // qualifying ord). With bestOrd=10 and k=5 the qualifying ord is 5.
      expect.same(SnapshotOrdinal.unsafeApply(5L), result) &&
        expect.same(5L.some, current.map(_.signed.value.shardOrdinal.value))
  }

  test("T_depth1_shard: chain shorter than k1Shard → MinValue") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 3) // ords 0..2, bestOrd = 2
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 1000,
        k1Shard = 10L, // k > bestOrd
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tDepth1Shard.latestQualifyingOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  // ============================================================================
  // Test 4: composite max-of — T_count fires for ord 8, T_depth1 fires for ord 3 → max = 8
  // ============================================================================

  test("composite max-of: T_count_shard=8 + T_depth1_shard=3 → latestQualifyingOrdinal = 8") { implicit hasher =>
    val self = pid("self")
    for {
      // To make T_count qualify at ord 8 (not 10 which is the actual bestTip), use a fresh eight-checkpoint store. T_count observes
      // bestTip ord 8 while T_depth1 sees the same chain with k1Shard=5 and qualifies ord 3.
      //
      // Re-seed cleanly: build a fresh store of length 8 (ords 1..8) so bestTip = ord 8. k1Shard=5 ⇒ T_depth1 = 3. kQuorum=2 ⇒ required
      // = 2 attesters (DIRECTLY); 2 non-self attesters meet it.
      store2 <- ShardChainStore.make[IO](shardZero)
      hashes2 <- seedChain(store2, 8)
      tip2 <- store2.bestTip.map(_.get)
      tracker2 <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker2.recordAttestation(tip2.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker2.recordAttestation(tip2.hash, pid("attester-2"), dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 2,
        k1Shard = 5L,
        chainStore = store2,
        tipTracker = tracker2
      )
      _ <- triggers.advance
      countQualifying <- triggers.tCountShard.latestQualifyingOrdinal
      depthQualifying <- triggers.tDepth1Shard.latestQualifyingOrdinal
      composite <- triggers.latestQualifyingOrdinal
      current <- triggers.currentQualifyingCheckpoint
    } yield
      // ord 8 = bestTip, kQuorum=2 ⇒ required = 2 ⇒ T_count qualifies ord 8.
      expect.same(SnapshotOrdinal.unsafeApply(8L), countQualifying) &&
        // bestOrd = 8, k1Shard = 5 ⇒ T_depth1 qualifies ord 3.
        expect.same(SnapshotOrdinal.unsafeApply(3L), depthQualifying) &&
        // composite max-of ⇒ 8, and the hash-bearing selection chooses the count-qualified tip.
        expect.same(ShardOrdinal(8L), composite) &&
        expect.same(tip2.hash.some, current.map(_.hash)) &&
        expect(hashes2.nonEmpty)
  }

  // ============================================================================
  // Test 5: monotone — latestQualifyingOrdinal Ref never goes backwards
  // ============================================================================

  test("monotone: after T_count qualifies ord N, dropping the attester set does NOT roll the Ref back to MinValue") { implicit hasher =>
    // Mirrors `FinalityTriggerSuite`'s "evaluateAndAdvance is monotone: never decreases the Ref" assertion, scoped to the shard
    // composite. We record attestations to qualify, advance, then prune the tracker so the next advance would see count=0.
    // The monotone Ref must NOT roll back.
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      // Record enough to qualify (kQuorum=3 → required=3 distinct attesters).
      _ <- tracker.recordAttestation(tip.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-2"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, pid("attester-3"), dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      qualifyingAfter <- triggers.tCountShard.latestQualifyingOrdinal
      // Now wipe all attestations for the tip by pruning everything below ord 999 (which is above any ord we have).
      // Use the chain store's getByHash as the ordinal lookup so the prune lookup matches what production callers would do.
      _ <- tracker.pruneBelow(
        ShardOrdinal(999L),
        h => store.getByHash(h).map(_.map(_.signed.value.shardOrdinal))
      )
      // Confirm the tracker is wiped: count goes back to 0.
      countAfterPrune <- tracker.attestationCountFor(tip.hash, excludeSelf = true)
      _ <- triggers.advance
      qualifyingAfterPrune <- triggers.tCountShard.latestQualifyingOrdinal
    } yield
      // The first advance set the Ref to the bestTip ord.
      expect.same(SnapshotOrdinal.unsafeApply(tip.signed.value.shardOrdinal.value), qualifyingAfter) &&
        // Tracker is wiped.
        expect.same(0, countAfterPrune) &&
        // The Ref MUST NOT have rolled back even though `attestationCountFor` returned 0 → eval would return MinValue. This is
        // the monotone-Ref contract guaranteed by `FinalityTrigger.fromRef`.
        expect.same(qualifyingAfter, qualifyingAfterPrune)
  }

  test("same-ordinal reorg: branch A quorum does not qualify the canonical branch B checkpoint") { implicit hasher =>
    val self = pid("self")
    val branchA = mkSignedCheckpoint(ord = 1L, parent = Hash.empty, peerByte = 1, gl0Anchor = 20L)
    val branchB = mkSignedCheckpoint(ord = 1L, parent = Hash.empty, peerByte = 2, gl0Anchor = 10L)

    for {
      store <- ShardChainStore.make[IO](shardZero)
      storedA <- store.store(
        branchA,
        parentHash = Hash.empty,
        shardOrdinal = ShardOrdinal(1L),
        slot = branchA.value.slot.value.value,
        vrfOutput = vrf(1)
      )
      tipA <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- tracker.recordAttestation(tipA.hash, pid("attester-1"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tipA.hash, pid("attester-2"), dummyCommitteeSig)
      _ <- tracker.recordAttestation(tipA.hash, pid("attester-3"), dummyCommitteeSig)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        k1Shard = 100L,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      historicalOrdinal <- triggers.latestQualifyingOrdinal

      // Same height, lower slot: maxvalid-tk switches the canonical entry at ordinal 1 from A to B.
      storedB <- store.store(
        branchB,
        parentHash = Hash.empty,
        shardOrdinal = ShardOrdinal(1L),
        slot = branchB.value.slot.value.value,
        vrfOutput = vrf(2)
      )
      tipB <- store.bestTip.map(_.get)
      ordinalLookup <- store.getByOrdinal(historicalOrdinal)
      branchBound <- triggers.currentQualifyingCheckpoint
    } yield
      expect(storedA) &&
        expect(storedB) &&
        expect.same(ShardOrdinal(1L), historicalOrdinal) &&
        expect(tipA.hash =!= tipB.hash) &&
        // This is the pre-fix exploit primitive: resolving the monotone ordinal now returns un-attested branch B.
        expect.same(tipB.hash.some, ordinalLookup.map(_.hash)) &&
        // The embedding selector must fail closed because no hash on B's ancestry has count or depth qualification.
        expect.same(none[Hash], branchBound.map(_.hash))
  }

  // ============================================================================
  // Test 6: pruning — pruneBelow(ord) drops attestations for checkpoints with shardOrd < ord
  // ============================================================================

  test("pruning: after pruneBelow(5), attestations for ord 1..4 are gone; attestations for ord 5..10 retained") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      hashes <- seedChain(store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      // Record one attestation per checkpoint hash so we can verify the prune drops some and keeps others.
      _ <- hashes.zipWithIndex.traverse_ { case (h, i) => tracker.recordAttestation(h, pid(s"attester-$i"), dummyCommitteeSig) }
      // Count before prune: 10 distinct hashes each with 1 attester (peerId distinct per hash).
      attsBefore <- tracker.allAttestations
      _ <- tracker.pruneBelow(
        ShardOrdinal(5L),
        h => store.getByHash(h).map(_.map(_.signed.value.shardOrdinal))
      )
      attsAfter <- tracker.allAttestations
      // Per-hash post-prune counts: ords 1..4 → 0 (dropped), ords 5..10 → 1 each (retained).
      countOrd1 <- tracker.attestationCountFor(hashes(0), excludeSelf = false)
      countOrd4 <- tracker.attestationCountFor(hashes(3), excludeSelf = false)
      countOrd5 <- tracker.attestationCountFor(hashes(4), excludeSelf = false)
      countOrd10 <- tracker.attestationCountFor(hashes(9), excludeSelf = false)
    } yield
      expect.same(10, attsBefore.size) &&
        expect.same(6, attsAfter.size) && // ords 5..10 retained
        expect.same(0, countOrd1) &&
        expect.same(0, countOrd4) &&
        expect.same(1, countOrd5) &&
        expect.same(1, countOrd10)
  }

  // ============================================================================
  // Bonus: idempotent recordAttestation (idempotent at the Set level)
  // ============================================================================

  test("recordAttestation: idempotent — recording the same (hash, peerId) pair twice keeps count = 1") { implicit hasher =>
    val self = pid("self")
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      attester = pid("attester-1")
      _ <- tracker.recordAttestation(tip.hash, attester, dummyCommitteeSig)
      _ <- tracker.recordAttestation(tip.hash, attester, dummyCommitteeSig) // duplicate; Set semantics → no change
      count <- tracker.attestationCountFor(tip.hash, excludeSelf = false)
    } yield expect.same(1, count)
  }

}
