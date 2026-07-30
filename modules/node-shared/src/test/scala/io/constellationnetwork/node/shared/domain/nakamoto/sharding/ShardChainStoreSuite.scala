package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import java.security.KeyPair

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.infrastructure.metrics.{Metrics, NoOpMetrics}
import io.constellationnetwork.node.shared.infrastructure.sharding.RegisteredCheckpointSigner
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardChainStore]] — per-shard chain storage for the hierarchical-shard-checkpoints design
  * (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5.5).
  *
  * '''Coverage''' (per Slice 5 task spec):
  *   1. '''Single-chain insertion''' — `store(a) → store(b chained off a) → bestTip = b, walkBackTo(b, 2) = [b, a]`.
  *   1. '''Fork insertion''' — `store(a) → store(b)/store(c) both off `a` → bestTip picks one deterministically per maxvalid-tk.`
  *   1. '''Idempotent store''' — storing the same checkpoint twice returns `false` the second time; state unchanged.
  *   1. '''finalize''' — advances `lastFinalizedOrdinal`; below-keep-floor entries evicted.
  *   1. '''getByHash + getByOrdinal''' roundtrip.
  *   1. '''walkBackTo across reorg''' — bestTip change rolls walkBackTo onto the new canonical history.
  *   1. '''Bounded retention''' — many finalizes ⇒ in-memory size bounded by `keepDepthBehindFinalized`.
  *
  * '''Fixture pattern''': every checkpoint selected by fork choice has a real outer Ed25519 signature and a producer attestation containing
  * loader-validated, preregistered Ed25519, KES, and VRF evidence. The cached fork-choice output is recovered from that exact VRF proof.
  * Synthetic proofs remain only on embedded state-channel binaries because those payload signatures are not execution-committee or VRF
  * identities and are opaque inputs to the store behavior under test.
  */
object ShardChainStoreSuite extends MutableIOSuite {

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

  // Slice 19: ShardChainStore.make now requires Metrics[F]. Tests get a no-op interpreter to keep assertions focused on
  // chain-store mechanics; per-test capture of metric calls lives in Slice 19's own ShardMetricsSuite.
  implicit val metrics: Metrics[IO] = NoOpMetrics.make

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(securityProvider: SecurityProvider[IO]) = sp
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      checkpointSigner <- RegisteredCheckpointSigner.make.asResource
      keyPairs <- Resource.eval(List.fill(4)(KeyPairGenerator.makeKeyPair[IO]).sequence)
      operators = keyPairs.map(keyPair => RegisteredOperator(keyPair, PeerId.fromPublic(keyPair.getPublic)))
      _ <- Resource.eval(operators.traverse_(operator => checkpointSigner.preregisterGenesis(operator.keyPair, operator.peerId)))
    } yield TestContext(h, securityProvider, checkpointSigner, operators)

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  /** Deterministic 64-char hex string from a seed character — used for `Hash` and signature proof bytes. */
  private def hash(seed: Char): Hash = Hash(seed.toString * 64)

  private def hex(s: String): Hex = Hex(s)

  /** Sentinel metagraph-binary proof only. It is never used as a checkpoint, committee, or fork-choice identity. */
  private def sentinelProof: SignatureProof =
    SignatureProof(Id(hex("11" * 64)), Signature(hex("22" * 70)))

  private def mkSigned[A](value: A): Signed[A] =
    Signed(value, NonEmptySet.of(sentinelProof))

  /** Construction-only placeholder required by the non-empty wire type. It is replaced with a registered signature before any checkpoint is
    * returned to a test or inserted into fork choice.
    */
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
    peerId: PeerId,
    gl0Anchor: Long,
    slot: Long
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardZero,
      parentCheckpointHash = parent,
      shardOrdinal = ShardOrdinal(ord),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(gl0Anchor)),
      slot = SlotT.unsafeApply(slot),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      committeeSignatures = NonEmptyList.of(constructionScaffold(peerId)),
      epoch = EtaPeriod(0L),
      executionBase = io.constellationnetwork.node.shared.ShardCheckpointTestFixtures.defaultExecutionBase
    )

  private def mkSignedCheckpoint(
    ctx: TestContext,
    ord: Long,
    parent: Hash,
    signerIndex: Int = 0,
    gl0Anchor: Long = 100L,
    slotOverride: Option[Long] = None
  ): IO[Signed[ShardCheckpoint]] = {
    implicit val hasher: Hasher[IO] = ctx.hasher
    implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider
    val operator = ctx.operator(signerIndex)
    val template = checkpointTemplate(ord, parent, operator.peerId, gl0Anchor, slotOverride.getOrElse(ord))

    for {
      committeeSignature <- ctx.checkpointSigner.sign(template, operator.keyPair, operator.peerId)
      checkpoint = template.copy(committeeSignatures = NonEmptyList.one(committeeSignature))
      signed <- Signed.forAsyncHasher[IO, ShardCheckpoint](checkpoint, operator.keyPair)
    } yield signed
  }

  private def storeCheckpoint(
    ctx: TestContext,
    store: ShardChainStore[IO],
    signed: Signed[ShardCheckpoint],
    parentHash: Hash,
    shardOrdinal: ShardOrdinal,
    slot: Long
  ): IO[Boolean] =
    ctx.checkpointSigner
      .proofDerivedVrfOutput(signed.value.producerSignature)
      .flatMap(store.store(signed, parentHash, shardOrdinal, slot, _))

  /** Build an Address from a label — for the chain-wide-frontier tests. */
  private def mkAddr(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))

  /** Build a stub `Signed[StateChannelSnapshotBinary]` chaining off `parent`, with distinct content per `(label, idx)` so distinct
    * canonical hashes.
    */
  private def mkBinary(label: String, idx: Int, parent: Hash): Signed[StateChannelSnapshotBinary] =
    mkSigned(
      StateChannelSnapshotBinary(
        lastSnapshotHash = parent,
        content = s"$label:$idx".getBytes("UTF-8"),
        fee = SnapshotFee(NonNegLong.unsafeFrom(0L))
      )
    )

  /** A registered checkpoint carrying a non-empty per-MG `includedSnapshots` window. */
  private def mkSignedCheckpointIncl(
    ctx: TestContext,
    ord: Long,
    parent: Hash,
    signerIndex: Int,
    included: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
  ): IO[Signed[ShardCheckpoint]] = {
    implicit val hasher: Hasher[IO] = ctx.hasher
    implicit val securityProvider: SecurityProvider[IO] = ctx.securityProvider
    val operator = ctx.operator(signerIndex)
    val template = checkpointTemplate(ord, parent, operator.peerId, gl0Anchor = 100L, slot = ord)
      .copy(derivedStateDelta = ShardDerivedStateDelta.empty.copy(includedSnapshots = included))

    for {
      committeeSignature <- ctx.checkpointSigner.sign(template, operator.keyPair, operator.peerId)
      checkpoint = template.copy(committeeSignatures = NonEmptyList.one(committeeSignature))
      signed <- Signed.forAsyncHasher[IO, ShardCheckpoint](checkpoint, operator.keyPair)
    } yield signed
  }

  // ===========================================================================
  // Test 1: single-chain insertion + bestTip + walkBackTo
  // ===========================================================================

  test("single-chain insertion: store(a) → store(b off a) ⇒ bestTip = b, walkBackTo(b, 2) = [b, a]") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      // a: first checkpoint after the synthetic root (parent = Hash.empty), shardOrdinal = 1
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      storedA <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      tipAfterA <- store.bestTip
      hashA = tipAfterA.get.hash
      // b: child of a, shardOrdinal = 2, later slot
      signedB <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1)
      storedB <- storeCheckpoint(ctx, store, signedB, hashA, ShardOrdinal(2L), slot = 2L)
      tipAfterB <- store.bestTip
      walkB <- store.walkBackTo(tipAfterB.get.hash, depth = 2L)
    } yield
      expect.all(
        storedA,
        storedB,
        tipAfterB.exists(_.signed.value.shardOrdinal == ShardOrdinal(2L)),
        walkB.size == 2,
        walkB.head.signed.value.shardOrdinal == ShardOrdinal(2L),
        walkB(1).signed.value.shardOrdinal == ShardOrdinal(1L),
        walkB.head.hash =!= walkB(1).hash
      )
  }

  // ===========================================================================
  // Test 1b: lastCheckpointedPerMgTip — chain-wide, non-reverting (the ord-26 re-freeze fix)
  // ===========================================================================

  test(
    "lastCheckpointedPerMgTip walks back across a PARTIAL bestTip: an MG omitted by the latest checkpoint keeps its frontier from the " +
      "last checkpoint that included it (perMgTip reverts; lastCheckpointedPerMgTip does not)"
  ) { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    import io.constellationnetwork.security.signature.Signed.SignedOps
    val mgX = mkAddr("mg-X")
    val mgY = mkAddr("mg-Y")
    val x1 = mkBinary("X", 0, Hash.empty)
    for {
      store <- ShardChainStore.make[IO](shardZero)
      x1h <- SignedOps(x1).toHashed[IO].map(_.hash)
      x2 = mkBinary("X", 1, x1h)
      x2h <- SignedOps(x2).toHashed[IO].map(_.hash)
      y1 = mkBinary("Y", 0, Hash.empty)
      y1h <- SignedOps(y1).toHashed[IO].map(_.hash)
      // A (ord 1): includes BOTH MGs.
      aIncl = SortedMap(mgX -> NonEmptyList.of(x1), mgY -> NonEmptyList.of(y1))(Address.OrderingInstance)
      signedA <- mkSignedCheckpointIncl(ctx, 1L, Hash.empty, signerIndex = 0, included = aIncl)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      hashA <- store.bestTip.map(_.get.hash)
      // B (ord 2, child of A): includes ONLY mgX, OMITS mgY — the partial bestTip that reverts perMgTip(mgY).
      bIncl = SortedMap(mgX -> NonEmptyList.of(x2))(Address.OrderingInstance)
      signedB <- mkSignedCheckpointIncl(ctx, 2L, hashA, signerIndex = 1, included = bIncl)
      _ <- storeCheckpoint(ctx, store, signedB, hashA, ShardOrdinal(2L), slot = 2L)
      perMg <- store.perMgTip
      frontier <- store.lastCheckpointedPerMgTip
    } yield
      expect.all(
        // perMgTip reads ONLY bestTip=B: mgX present (x2), mgY ABSENT — reverted (the run-27e hazard the gate must not depend on).
        perMg.get(mgX).contains(x2h),
        !perMg.contains(mgY),
        // lastCheckpointedPerMgTip walks back: mgX from B (x2), mgY from A (y1) — NON-REVERT. This is what makes the newness gate
        // omit a stale re-include of mgY instead of minting it (the ord-26 freeze) when bestTip is a partial checkpoint.
        frontier.get(mgX).contains(x2h),
        frontier.get(mgY).contains(y1h)
      )
  }

  // ===========================================================================
  // Test 2: fork insertion + deterministic tiebreak
  // ===========================================================================

  test("fork insertion: a → b/c both children of a ⇒ bestTip picks the lower registered proof output") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      hashA <- store.bestTip.map(_.get.hash)
      // Distinct registered producers sign the same slot. The checkpoints differ by their signed GL0 anchor; fork choice receives each
      // producer's actual proof-derived output and must select the lower unsigned value.
      signedB <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1, gl0Anchor = 101L)
      signedC <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 2, gl0Anchor = 102L)
      vrfB <- ctx.checkpointSigner.proofDerivedVrfOutput(signedB.value.producerSignature)
      vrfC <- ctx.checkpointSigner.proofDerivedVrfOutput(signedC.value.producerSignature)
      _ <- store.store(signedB, parentHash = hashA, shardOrdinal = ShardOrdinal(2L), slot = 2L, vrfOutput = vrfB)
      _ <- store.store(signedC, parentHash = hashA, shardOrdinal = ShardOrdinal(2L), slot = 2L, vrfOutput = vrfC)
      tipFinal <- store.bestTip
      sizeAfter <- store.size
      expectedAnchor = if (BigInt(1, vrfB) < BigInt(1, vrfC)) 101L else 102L
    } yield
      expect.all(
        sizeAfter == 3, // a + b + c
        tipFinal.isDefined,
        !java.util.Arrays.equals(vrfB, vrfC),
        tipFinal.exists(_.signed.value.gl0AnchorOrdinal == SnapshotOrdinal(NonNegLong.unsafeFrom(expectedAnchor)))
      )
  }

  test("same-slot producer equivocations converge on the lower checkpoint hash independent of orphan arrival order") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher

    for {
      leftStore <- ShardChainStore.make[IO](shardZero)
      rightStore <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      hashA <- ctx.hasher.hash(signedA.value.signingPreimage)
      // Same registered producer, eta, and slot yield the same VRF possession proof. Distinct anchors make distinct checkpoint hashes.
      signedB <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1, gl0Anchor = 101L, slotOverride = Some(2L))
      signedC <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1, gl0Anchor = 102L, slotOverride = Some(2L))
      hashB <- ctx.hasher.hash(signedB.value.signingPreimage)
      hashC <- ctx.hasher.hash(signedC.value.signingPreimage)
      vrfB <- ctx.checkpointSigner.proofDerivedVrfOutput(signedB.value.producerSignature)
      vrfC <- ctx.checkpointSigner.proofDerivedVrfOutput(signedC.value.producerSignature)
      // Both children arrive as orphans in opposite orders. Storing A reconnects a Set containing both tied descendants.
      _ <- leftStore.store(signedB, hashA, ShardOrdinal(2L), slot = 2L, vrfOutput = vrfB)
      _ <- leftStore.store(signedC, hashA, ShardOrdinal(2L), slot = 2L, vrfOutput = vrfC)
      _ <- rightStore.store(signedC, hashA, ShardOrdinal(2L), slot = 2L, vrfOutput = vrfC)
      _ <- rightStore.store(signedB, hashA, ShardOrdinal(2L), slot = 2L, vrfOutput = vrfB)
      leftBeforeParent <- leftStore.bestTip
      rightBeforeParent <- rightStore.bestTip
      _ <- storeCheckpoint(ctx, leftStore, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      _ <- storeCheckpoint(ctx, rightStore, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      leftTip <- leftStore.bestTip
      rightTip <- rightStore.bestTip
      expected = if (hashB.value.compareTo(hashC.value) < 0) hashB else hashC
      _ <- leftStore.noteAnchor(hashA)
      _ <- rightStore.noteAnchor(hashA)
      leftAnchoredTip <- leftStore.bestTip
      rightAnchoredTip <- rightStore.bestTip
    } yield
      expect.all(
        leftBeforeParent.isEmpty,
        rightBeforeParent.isEmpty,
        java.util.Arrays.equals(vrfB, vrfC),
        hashB =!= hashC,
        leftTip.exists(_.hash === expected),
        rightTip.exists(_.hash === expected),
        leftTip.map(_.hash) === rightTip.map(_.hash),
        leftAnchoredTip.exists(_.hash === expected),
        rightAnchoredTip.exists(_.hash === expected),
        leftAnchoredTip.map(_.hash) === rightAnchoredTip.map(_.hash)
      )
  }

  // ===========================================================================
  // Test 3: idempotent store
  // ===========================================================================

  test("idempotent store: storing the same checkpoint twice returns false the second time, state unchanged") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      stored1 <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      size1 <- store.size
      tip1 <- store.bestTip.map(_.get.hash)
      // Re-store the same envelope — bytes-identical, so canonical hash collides; store must return false.
      stored2 <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      size2 <- store.size
      tip2 <- store.bestTip.map(_.get.hash)
    } yield
      expect.all(
        stored1,
        !stored2,
        size1 == 1,
        size2 == 1,
        tip1 === tip2
      )
  }

  // ===========================================================================
  // Test 4: finalize advances lastFinalizedOrdinal + evicts past the keep-window
  // ===========================================================================

  test("finalize: advances lastFinalizedOrdinal; checkpoints past the retention boundary are evicted") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    // keepDepth=3 ⇒ at finalize(ord=6), keepFloor=3. Ords 1..2 evicted; ords 3..6 retained.
    val keepDepth = 3L
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = keepDepth)
      // Seed a chain of 6 chained checkpoints (ords 1..6).
      hashesIO = (1L to 6L).toList.foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash.empty)) {
        case ((acc, parent), ord) =>
          mkSignedCheckpoint(ctx, ord, parent = parent, signerIndex = ord.toInt).flatMap { signed =>
            storeCheckpoint(ctx, store, signed, parent, ShardOrdinal(ord), slot = ord).flatMap { _ =>
              store.bestTip.map(_.get.hash).map(h => (acc :+ h, h))
            }
          }
      }
      hashesPair <- hashesIO
      (hashes, _) = hashesPair
      preFinalized <- store.lastFinalizedOrdinal
      preSize <- store.size
      // Finalize at ord=6 ⇒ keepFloor = max(0, 6-3) = 3. Ords 1,2 evicted; ords 3,4,5,6 retained (4 entries).
      _ <- store.`finalize`(hashes(5))
      postFinalized <- store.lastFinalizedOrdinal
      postSize <- store.size
      ord6Present <- store.getByHash(hashes(5)).map(_.isDefined)
      ord3Present <- store.getByHash(hashes(2)).map(_.isDefined)
      ord2Absent <- store.getByHash(hashes(1)).map(_.isEmpty)
      ord1Absent <- store.getByHash(hashes(0)).map(_.isEmpty)
    } yield
      expect.all(
        preFinalized == ShardOrdinal.Root,
        preSize == 6,
        postFinalized == ShardOrdinal(6L),
        postSize == 4,
        ord6Present,
        ord3Present,
        ord2Absent,
        ord1Absent
      )
  }

  // ===========================================================================
  // Test 5: getByHash + getByOrdinal roundtrip
  // ===========================================================================

  test("getByHash + getByOrdinal: roundtrip on stored entries") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      tip <- store.bestTip
      hashA = tip.get.hash
      byHashOut <- store.getByHash(hashA)
      byOrdinalOut <- store.getByOrdinal(ShardOrdinal(1L))
      missByHash <- store.getByHash(Hash.empty)
      missByOrdinal <- store.getByOrdinal(ShardOrdinal(99L)) // requested future
    } yield
      expect.all(
        byHashOut.isDefined,
        byHashOut.exists(_.hash === hashA),
        byOrdinalOut.isDefined,
        byOrdinalOut.exists(_.hash === hashA),
        byOrdinalOut.exists(_.signed.value.shardOrdinal == ShardOrdinal(1L)),
        missByHash.isEmpty,
        missByOrdinal.isEmpty
      )
  }

  // ===========================================================================
  // Test 6: walkBackTo across reorg — bestTip change rolls walkBackTo onto new canonical history
  // ===========================================================================

  test("walkBackTo across reorg: when bestTip changes, walkBackTo returns the new canonical's history") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      // a: shared root
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      hashA <- store.bestTip.map(_.get.hash)
      // b: child of a at ord=2, slot=10 (higher slot → loses on slot tiebreak when c comes in at same ord and lower slot). Vary
      // `gl0Anchor` to produce a distinct canonical preimage hash from c.
      signedB <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1, gl0Anchor = 101L, slotOverride = Some(10L))
      _ <- storeCheckpoint(ctx, store, signedB, hashA, ShardOrdinal(2L), slot = 10L)
      tipBeforeReorg <- store.bestTip
      hashB = tipBeforeReorg.get.hash
      walkBeforeReorg <- store.walkBackTo(hashB, depth = 2L)
      // c: alternate child of a at ord=2, slot=2 (LOWER slot → wins maxvalid-tk slot tiebreak). Distinct gl0Anchor ⇒ distinct hash.
      signedC <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 2, gl0Anchor = 102L, slotOverride = Some(2L))
      _ <- storeCheckpoint(ctx, store, signedC, hashA, ShardOrdinal(2L), slot = 2L)
      tipAfterReorg <- store.bestTip
      hashC = tipAfterReorg.get.hash
      walkAfterReorg <- store.walkBackTo(hashC, depth = 2L)
    } yield
      expect.all(
        // Before reorg: b is bestTip; walk returns [b, a].
        tipBeforeReorg.exists(_.hash === hashB),
        walkBeforeReorg.size == 2,
        walkBeforeReorg.head.hash === hashB,
        walkBeforeReorg(1).hash === hashA,
        // After reorg: c is bestTip; walkBackTo(hashC, 2) returns [c, a] — the NEW canonical history.
        tipAfterReorg.exists(_.hash === hashC),
        hashC =!= hashB, // confirm reorg actually happened
        walkAfterReorg.size == 2,
        walkAfterReorg.head.hash === hashC,
        walkAfterReorg(1).hash === hashA,
        // The losing branch (b) is STILL in the store (it lives on as an alt-branch entry until pruned by finalize).
        // walkBackTo(hashB, 2) still returns [b, a] because b's parent pointer is intact.
        walkBeforeReorg(1).hash === walkAfterReorg(1).hash
      )
  }

  // ===========================================================================
  // Test 7: bounded retention across many finalizes
  // ===========================================================================

  test("bounded retention: after many finalizes, total in-memory map size bounded by keepDepthBehindFinalized + 1") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    val keepDepth = 4L
    val totalOrds = 20L
    // After finalize(ord=N), keepFloor = N - keepDepth; entries with ord ∈ [keepFloor, N] are retained ⇒ keepDepth + 1 entries.
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = keepDepth)
      // Seed and finalize each ord one at a time — simulates production Phase 1→2 transitions.
      _ <- (1L to totalOrds).toList.foldLeftM[IO, Hash](Hash.empty) {
        case (parent, ord) =>
          mkSignedCheckpoint(ctx, ord, parent = parent, signerIndex = ord.toInt).flatMap { signed =>
            storeCheckpoint(ctx, store, signed, parent, ShardOrdinal(ord), slot = ord) >>
              store.bestTip.map(_.get.hash).flatTap(h => store.`finalize`(h))
          }
      }
      finalSize <- store.size
      finalFinalized <- store.lastFinalizedOrdinal
    } yield
      expect.all(
        // After 20 finalizes at keepDepth=4, in-memory map holds at most keepDepth+1 = 5 entries (keepFloor..tip inclusive).
        finalSize <= (keepDepth + 1L).toInt,
        finalFinalized == ShardOrdinal(totalOrds)
      )
  }

  // ===========================================================================
  // Bonus: idempotent finalize is a no-op
  // ===========================================================================

  test("finalize is monotone: re-finalizing the same hash is a no-op on lastFinalizedOrdinal") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = 8L)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      tip <- store.bestTip
      hashA = tip.get.hash
      _ <- store.`finalize`(hashA)
      finalized1 <- store.lastFinalizedOrdinal
      _ <- store.`finalize`(hashA) // second call should be no-op
      finalized2 <- store.lastFinalizedOrdinal
    } yield expect.all(finalized1 == ShardOrdinal(1L), finalized2 == ShardOrdinal(1L))
  }

  test("shardId is set at construction and exposed through the accessor") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](ShardId.unsafeApply(5))
    } yield expect(store.shardId == ShardId.unsafeApply(5))
  }

  test("empty store: bestTip = None, size = 0, lastFinalizedOrdinal = Root") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      tip <- store.bestTip
      sz <- store.size
      fo <- store.lastFinalizedOrdinal
      missByOrd <- store.getByOrdinal(ShardOrdinal(0L))
    } yield expect.all(tip.isEmpty, sz == 0, fo == ShardOrdinal.Root, missByOrd.isEmpty)
  }

  test("Root ordinal is rejected even when its evidence comes from a registered operator") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      invalidRoot <- mkSignedCheckpoint(ctx, ord = ShardOrdinal.Root.value, parent = Hash.empty)
      stored <- storeCheckpoint(ctx, store, invalidRoot, Hash.empty, ShardOrdinal.Root, slot = ShardOrdinal.Root.value)
      tip <- store.bestTip
      size <- store.size
    } yield expect.all(!stored, tip.isEmpty, size == 0)
  }

  test("walkBackTo with depth=0 returns empty list") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      hashA <- store.bestTip.map(_.get.hash)
      walk <- store.walkBackTo(hashA, depth = 0L)
    } yield expect(walk.isEmpty)
  }

  test("walkBackTo on unknown hash returns empty list (chain break)") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      walk <- store.walkBackTo(hash('z'), depth = 5L)
    } yield expect(walk.isEmpty)
  }

  test("DefaultKeepDepthBehindFinalized = 8 (matches Slice 2 HOCON default)") { _ =>
    IO.pure(expect.same(8L, ShardChainStore.DefaultKeepDepthBehindFinalized))
  }

  test("SortedMap[ShardId, _] usage compiles — ShardId Ordering is in scope") { _ =>
    val empty: SortedMap[ShardId, ShardChainStore[IO]] = SortedMap.empty
    IO.pure(expect(empty.isEmpty))
  }

  // ===========================================================================
  // Out-of-order arrival (2026-06-11, run bc5a17r12): connectivity-gated bestTip
  // ===========================================================================

  test("an unconnected registered checkpoint remains an orphan until its exact hash is anchored") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      missingParent = hash('p')
      signed <- mkSignedCheckpoint(ctx, ord = 50L, parent = missingParent)
      checkpointHash <- h.hash(signed.value.signingPreimage)
      inserted <- storeCheckpoint(ctx, store, signed, missingParent, ShardOrdinal(50L), slot = 50L)
      orphan <- store.getByHash(checkpointHash)
      tipBeforeAnchor <- store.bestTip
      _ <- store.noteAnchor(checkpointHash)
      tipAfterAnchor <- store.bestTip
    } yield
      expect.all(
        inserted,
        orphan.exists(_.hash === checkpointHash),
        tipBeforeAnchor.isEmpty,
        tipAfterAnchor.exists(_.hash === checkpointHash)
      )
  }

  test("out-of-order arrival: child before parent stays ORPHAN (no floating tip); parent arrival cascades reconnect") { ctx =>
    implicit val h: Hasher[IO] = ctx.hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA <- mkSignedCheckpoint(ctx, ord = 1L, parent = Hash.empty)
      _ <- storeCheckpoint(ctx, store, signedA, Hash.empty, ShardOrdinal(1L), slot = 1L)
      hashA <- store.bestTip.map(_.get.hash)
      signedB <- mkSignedCheckpoint(ctx, ord = 2L, parent = hashA, signerIndex = 1)
      hashB <- h.hash(signedB.value.signingPreimage)
      signedC <- mkSignedCheckpoint(ctx, ord = 3L, parent = hashB, signerIndex = 2)
      // C gossips in FIRST (its parent B is unknown). It must be stored but must NOT become a floating tip.
      storedC <- storeCheckpoint(ctx, store, signedC, hashB, ShardOrdinal(3L), slot = 3L)
      tipAfterC <- store.bestTip
      orphanHeld = tipAfterC.exists(_.hash === hashA)
      // B lands — connectivity cascades: B connects to A, C reconnects through B, the tip jumps to C.
      storedB <- storeCheckpoint(ctx, store, signedB, hashA, ShardOrdinal(2L), slot = 2L)
      tipAfterB <- store.bestTip
      walk <- store.walkBackTo(tipAfterB.get.hash, depth = 3L)
    } yield
      expect.all(
        storedC,
        storedB,
        orphanHeld,
        tipAfterB.exists(_.signed.value.shardOrdinal == ShardOrdinal(3L)),
        walk.size == 3,
        walk.map(_.signed.value.shardOrdinal.value) == List(3L, 2L, 1L)
      )
  }
}
