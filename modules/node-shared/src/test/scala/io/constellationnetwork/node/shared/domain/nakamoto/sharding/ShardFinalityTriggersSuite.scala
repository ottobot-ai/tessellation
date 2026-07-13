package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.security.KeyPair

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.RegisteredCheckpointSigner
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardTipTracker]] and execution-quorum-only [[ShardFinalityTriggers]].
  *
  * '''Required coverage''':
  *   1. The execution-certificate selector qualifies at configured `kQuorum` and not below it.
  *   1. tracker self-exclusion remains available for diagnostics, while the selection trigger counts all distinct embedded signers.
  *   1. Monotone — `latestQualifying` Ref never goes backwards even if eval regresses.
  *   1. Pruning — after `pruneBelow(ord)`, attestations for checkpoints with shardOrd < ord are gone.
  */
object ShardFinalityTriggersSuite extends MutableIOSuite {

  final case class RegisteredOperator(keyPair: KeyPair, peerId: PeerId)

  final case class TestContext(
    hasher: Hasher[IO],
    securityProvider: SecurityProvider[IO],
    checkpointSigner: RegisteredCheckpointSigner,
    operators: List[RegisteredOperator]
  ) {
    def operator(index: Int): RegisteredOperator = operators(Math.floorMod(index, operators.size))
  }

  override type Res = TestContext

  // Slice 19: ShardChainStore + ShardTipTracker constructors now require an implicit Metrics[F]. Tests use a no-op
  // interpreter so they keep asserting on chain-store / tracker semantics rather than metric mechanics (which are
  // covered by Slice 19's own ShardMetricsSuite).
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(securityProvider: SecurityProvider[IO]) = sp
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
      keyPairs <- Resource.eval(List.fill(12)(KeyPairGenerator.makeKeyPair[IO]).sequence)
      operators = keyPairs.map(keyPair => RegisteredOperator(keyPair, PeerId.fromPublic(keyPair.getPublic)))
      _ <- Resource.eval(operators.traverse_(operator => checkpointSigner.preregisterGenesis(operator.keyPair, operator.peerId)))
    } yield TestContext(h, securityProvider, checkpointSigner, operators)

  // ============================================================================
  // Fixtures — mirror the ShardChainStoreSuite shape so the store / tracker / triggers compose cleanly.
  // ============================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  /** Construction-only placeholder required by the non-empty wire type. It is replaced before storage or attestation. */
  private def constructionScaffold(peerId: PeerId): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerId,
      vrfProof = Hex.fromBytes(Array.emptyByteArray),
      ed25519Sig = Hex.fromBytes(Array.emptyByteArray),
      kesProductSig = Hex.fromBytes(Array.emptyByteArray),
      kesTreeStep = 0
    )

  private def checkpointTemplate(
    ord: Long,
    parent: Hash,
    producer: PeerId,
    gl0Anchor: Long
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = parent,
      shardOrdinal = ShardOrdinal(ord),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      slot = SlotT.unsafeApply(gl0Anchor),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.of(constructionScaffold(producer)),
      epoch = EtaPeriod(0L)
    )

  private def mkSignedCheckpoint(
    ctx: TestContext,
    ord: Long,
    parent: Hash,
    signerIndex: Int,
    gl0Anchor: Long
  ): IO[Signed[ShardCheckpoint]] = {
    implicit val hasher: Hasher[IO] = ctx.hasher
    implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider
    val operator = ctx.operator(signerIndex)
    val template = checkpointTemplate(ord, parent, operator.peerId, gl0Anchor)

    for {
      committeeSignature <- ctx.checkpointSigner.sign(template, operator.keyPair, operator.peerId)
      checkpoint = template.copy(committeeSignatures = NonEmptyList.one(committeeSignature))
      signed <- Signed.forAsyncHasher[IO, ShardCheckpoint](checkpoint, operator.keyPair)
    } yield signed
  }

  private def storeCheckpoint(
    ctx: TestContext,
    store: ShardChainStore[IO],
    signed: Signed[ShardCheckpoint]
  ): IO[Boolean] =
    ctx.checkpointSigner.proofDerivedVrfOutput(signed.value.producerSignature).flatMap { output =>
      store.store(
        signed,
        signed.value.parentCheckpointHash,
        signed.value.shardOrdinal,
        signed.value.slot.value.value,
        output
      )
    }

  private def recordRegisteredAttestation(
    ctx: TestContext,
    tracker: ShardTipTracker[IO],
    checkpointHash: Hash,
    checkpoint: ShardCheckpoint,
    signerIndex: Int
  ): IO[Unit] = {
    implicit val hasher: Hasher[IO] = ctx.hasher
    implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider
    val operator = ctx.operator(signerIndex)

    ctx.checkpointSigner
      .sign(checkpoint, operator.keyPair, operator.peerId)
      .flatMap(signature => tracker.recordAttestation(checkpointHash, operator.peerId, signature))
  }

  /** Seed a chain of `n` checkpoints (ords 1..n) into the store; return the canonical hashes in order. */
  private def seedChain(
    ctx: TestContext,
    store: ShardChainStore[IO],
    n: Int
  ): IO[List[Hash]] =
    (1L to n.toLong).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash.empty)) {
        case ((acc, parent), ord) =>
          mkSignedCheckpoint(ctx, ord, parent = parent, signerIndex = ord.toInt, gl0Anchor = 100L + ord).flatMap { signed =>
            storeCheckpoint(ctx, store, signed).flatMap { _ =>
              store.bestTip.map(_.get.hash).map(h => (acc :+ h, h))
            }
          }
      }
      .map(_._1)

  // ============================================================================
  // Test 1: execution-certificate selection qualifies at kQuorum=3 distinct signers.
  // ============================================================================

  test("execution certificate: registered non-self signers qualify the best-tip ordinal") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 3)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 4)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
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

  test("execution certificate: carried producer plus one registered signer stays below kQuorum=3") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      // One producer signature is carried by the stored checkpoint and one is collected locally: 2 distinct signers, required = 3.
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  // ============================================================================
  // Test 2: execution-certificate selection counts self like verifyEmbedded's distinct-signer bar.
  // ============================================================================

  test("execution certificate: registered local signer plus two others reaches kQuorum=3") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(2).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 3)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 4)
      // The trigger counts self + attester-1 + attester-2 = 3 >= 3 → qualifies. This matches the bar the embed is held to:
      // `verifyEmbedded` counts EVERY distinct signer (producer + self + remote) against kQuorum. The old self-excluded count
      // demanded kQuorum REMOTE attestations — unanimity at N=5/kQuorum=4 — and one slow peer stalled every embed (run-10
      // Gap-B). Self alone still can't qualify anything at kQuorum >= 2.
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
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

  test("execution certificate: carried producer plus registered self stays below kQuorum=3") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(2).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      result <- triggers.tCountShard.latestQualifyingOrdinal
    } yield expect.same(SnapshotOrdinal.MinValue, result)
  }

  test("execution certificate: registered carried producer counts without a local gossip echo") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 3)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        chainStore = store,
        tipTracker = tracker
      )
      current <- triggers.currentQualifyingCheckpoint
    } yield expect.same(tip.hash.some, current.map(_.hash))
  }

  // ============================================================================
  // Test 5: monotone — latestQualifyingOrdinal Ref never goes backwards
  // ============================================================================

  test("monotone: dropping registered attestations does not roll the qualifying Ref backward") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    // Mirrors `FinalityTriggerSuite`'s "evaluateAndAdvance is monotone: never decreases the Ref" assertion, scoped to the shard
    // composite. We record attestations to qualify, advance, then prune the tracker so the next advance would see count=0.
    // The monotone Ref must NOT roll back.
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      // Record enough to qualify (kQuorum=3 → required=3 distinct attesters).
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 2)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 3)
      _ <- recordRegisteredAttestation(ctx, tracker, tip.hash, tip.signed.value, 4)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
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

  test("same-ordinal reorg: branch A registered quorum does not qualify branch B") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      branchA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty, signerIndex = 1, gl0Anchor = 20L)
      branchB <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty, signerIndex = 2, gl0Anchor = 10L)
      store <- ShardChainStore.make[IO](shardZero)
      storedA <- storeCheckpoint(ctx, store, branchA)
      tipA <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- recordRegisteredAttestation(ctx, tracker, tipA.hash, tipA.signed.value, 3)
      _ <- recordRegisteredAttestation(ctx, tracker, tipA.hash, tipA.signed.value, 4)
      _ <- recordRegisteredAttestation(ctx, tracker, tipA.hash, tipA.signed.value, 5)
      triggers <- ShardFinalityTriggers.make[IO](
        shardId = shardZero,
        kQuorum = 3,
        chainStore = store,
        tipTracker = tracker
      )
      _ <- triggers.advance
      historicalOrdinal <- triggers.latestQualifyingOrdinal

      // Same height, lower slot: maxvalid-tk switches the canonical entry at ordinal 1 from A to B.
      storedB <- storeCheckpoint(ctx, store, branchB)
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
        // The embedding selector must fail closed because no hash on B's ancestry has execution quorum.
        expect.same(none[Hash], branchBound.map(_.hash))
  }

  // ============================================================================
  // Test 6: pruning — pruneBelow(ord) drops attestations for checkpoints with shardOrd < ord
  // ============================================================================

  test("pruning removes only the registered attestations below the requested ordinal") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      hashes <- seedChain(ctx, store, 10)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      _ <- hashes.zipWithIndex.traverse_ {
        case (hash, index) =>
          store
            .getByHash(hash)
            .flatMap {
              IO.fromOption(_)(new IllegalStateException("seeded checkpoint disappeared before attestation"))
            }
            .flatMap(checkpoint => recordRegisteredAttestation(ctx, tracker, hash, checkpoint.signed.value, index + 1))
      }
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

  test("recordAttestation: replaying the same registered signature keeps count = 1") { ctx =>
    implicit val hasher: Hasher[IO] = ctx.hasher
    val self = ctx.operator(11).peerId
    for {
      store <- ShardChainStore.make[IO](shardZero)
      _ <- seedChain(ctx, store, 1)
      tip <- store.bestTip.map(_.get)
      tracker <- ShardTipTracker.make[IO](shardZero, self)
      attester = ctx.operator(2)
      signature <- {
        implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider
        ctx.checkpointSigner.sign(tip.signed.value, attester.keyPair, attester.peerId)
      }
      _ <- tracker.recordAttestation(tip.hash, attester.peerId, signature)
      _ <- tracker.recordAttestation(tip.hash, attester.peerId, signature)
      count <- tracker.attestationCountFor(tip.hash, excludeSelf = false)
    } yield expect.same(1, count)
  }

}
