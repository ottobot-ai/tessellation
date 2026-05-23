package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

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
  * '''Fixture pattern''':
  *   - We synthesize `Signed[ShardCheckpoint]` instances deterministically with a single fake signature proof — the store doesn't verify
  *     signatures (verification lives in the acceptance layer slice down), so a sentinel proof is sufficient. The signing preimage hash
  *     is computed via `ShardChainStore.deriveHash`-equivalent (Hasher.forJson over the preimage), which the store does internally to
  *     key its `byHash` map. Tests retrieve the canonical hash via `chainStore.bestTip.map(_.hash)` after store to keep the fixture
  *     side-effect-free w.r.t. the deriveHash details.
  */
object ShardChainStoreSuite extends MutableIOSuite {

  override type Res = Hasher[IO]

  override def sharedResource: Resource[IO, Res] =
    for {
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    } yield Hasher.forJson[IO]

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  private val shardZero: ShardId = ShardId.unsafeApply(0)

  /** Deterministic 64-char hex string from a seed character — used for `Hash` and signature proof bytes. */
  private def hash(seed: Char): Hash = Hash(seed.toString * 64)

  private def hex(s: String): Hex = Hex(s)

  /** Single sentinel signature — the store doesn't verify, so we don't need real key material. */
  private def sentinelProof: SignatureProof =
    SignatureProof(Id(hex("11" * 64)), Signature(hex("22" * 70)))

  private def mkSigned[A](value: A): Signed[A] =
    Signed(value, NonEmptySet.of(sentinelProof))

  /** Build a sentinel committee-member signature. The store ignores the content. */
  private def mkCommitteeSig(peerByte: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex(f"$peerByte%02x" * 64)),
      vrfProof = hex("aa" * 80),
      ed25519Sig = hex("bb" * 64),
      kesProductSig = hex("cc" * 128),
      kesTreeStep = 0
    )

  /** Build a `ShardCheckpoint` with the given shard ordinal and parent hash. The `committeeSignatures` field is the only non-trivial
    * field — we keep one sentinel signature per checkpoint with varying `peerByte` so two checkpoints at the same shard ordinal but with
    * different parents (fork case) produce DIFFERENT canonical hashes (otherwise the store would idempotent-collapse them).
    */
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
      derivedStateDelta = ShardDerivedStateDelta.empty,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(mkCommitteeSig(peerByte)),
      epoch = EtaPeriod(0L)
    )

  /** Same `mkCheckpoint` but emits the `Signed` envelope ready to feed to `store`. */
  private def mkSignedCheckpoint(
    ord: Long,
    parent: Hash,
    peerByte: Int = 1,
    gl0Anchor: Long = 100L
  ): Signed[ShardCheckpoint] =
    mkSigned(mkCheckpoint(ord, parent, peerByte, gl0Anchor))

  /** Build deterministic VRF output bytes from an int seed. Used for fork tiebreaks. */
  private def vrf(seed: Int): Array[Byte] = Array.fill[Byte](32)(seed.toByte)

  // ===========================================================================
  // Test 1: single-chain insertion + bestTip + walkBackTo
  // ===========================================================================

  test("single-chain insertion: store(a) → store(b off a) ⇒ bestTip = b, walkBackTo(b, 2) = [b, a]") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      // a: genesis-like (parent = Hash.empty), shardOrdinal = 0
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      storedA <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      tipAfterA <- store.bestTip
      hashA = tipAfterA.get.hash
      // b: child of a, shardOrdinal = 1, later slot
      signedB = mkSignedCheckpoint(ord = 1L, parent = hashA, peerByte = 2)
      storedB <- store.store(signedB, parentHash = hashA, shardOrdinal = ShardOrdinal(1L), slot = 2L, vrfOutput = vrf(2))
      tipAfterB <- store.bestTip
      walkB <- store.walkBackTo(tipAfterB.get.hash, depth = 2L)
    } yield
      expect.all(
        storedA,
        storedB,
        tipAfterB.exists(_.signed.value.shardOrdinal == ShardOrdinal(1L)),
        walkB.size == 2,
        walkB.head.signed.value.shardOrdinal == ShardOrdinal(1L),
        walkB(1).signed.value.shardOrdinal == ShardOrdinal(0L),
        walkB.head.hash =!= walkB(1).hash
      )
  }

  // ===========================================================================
  // Test 2: fork insertion + deterministic tiebreak
  // ===========================================================================

  test("fork insertion: a → b/c both children of a ⇒ bestTip picks one deterministically per maxvalid-tk") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      _ <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      hashA <- store.bestTip.map(_.get.hash)
      // b and c both children of a, SAME shardOrdinal=1 and SAME slot. We vary `gl0Anchor` so the canonical preimages differ (peerByte
      // varies committeeSignatures, but that field is EXCLUDED from the signing preimage, so two checkpoints with identical preimage
      // fields hash identically — `gl0AnchorOrdinal` IS part of the preimage so varying it gives distinct hashes).
      //
      // The fork tiebreak: maxvalid-tk → equal ordinal → equal slot → lower VRF output wins. We make c's VRF the lower one, so c MUST be
      // picked.
      signedB = mkSignedCheckpoint(ord = 1L, parent = hashA, peerByte = 2, gl0Anchor = 101L)
      signedC = mkSignedCheckpoint(ord = 1L, parent = hashA, peerByte = 3, gl0Anchor = 102L)
      vrfB = vrf(0xff) // larger BigInt
      vrfC = vrf(0x10) // smaller BigInt
      _ <- store.store(signedB, parentHash = hashA, shardOrdinal = ShardOrdinal(1L), slot = 2L, vrfOutput = vrfB)
      // c arrives second
      _ <- store.store(signedC, parentHash = hashA, shardOrdinal = ShardOrdinal(1L), slot = 2L, vrfOutput = vrfC)
      tipFinal <- store.bestTip
      sizeAfter <- store.size
    } yield
      expect.all(
        sizeAfter == 3, // a + b + c
        tipFinal.isDefined,
        // c (lower VRF) must beat b under maxvalid-tk.
        // The tip's signed value carries the committee signatures we used to construct c (peerByte=3) and gl0Anchor=102,
        // so we can verify which envelope won via the gl0AnchorOrdinal — the field on the preimage that distinguishes b and c.
        tipFinal.exists(_.signed.value.gl0AnchorOrdinal == SnapshotOrdinal(NonNegLong.unsafeFrom(102L)))
      )
  }

  // ===========================================================================
  // Test 3: idempotent store
  // ===========================================================================

  test("idempotent store: storing the same checkpoint twice returns false the second time, state unchanged") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      stored1 <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      size1 <- store.size
      tip1 <- store.bestTip.map(_.get.hash)
      // Re-store the same envelope — bytes-identical, so canonical hash collides; store must return false.
      stored2 <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
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

  test("finalize: advances lastFinalizedOrdinal; checkpoints past the retention boundary are evicted") { hasher =>
    implicit val h: Hasher[IO] = hasher
    // keepDepth=3 ⇒ at finalize(ord=5), keepFloor=2. Ords 0..1 evicted; ords 2..5 retained.
    val keepDepth = 3L
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = keepDepth)
      // Seed a chain of 6 chained checkpoints (ords 0..5).
      hashesIO = (0L until 6L).toList.foldLeftM[IO, (List[Hash], Hash)]((List.empty, Hash.empty)) {
                   case ((acc, parent), ord) =>
                     val signed = mkSignedCheckpoint(ord, parent = parent, peerByte = ord.toInt + 1)
                     store
                       .store(signed, parentHash = parent, shardOrdinal = ShardOrdinal(ord), slot = ord + 1, vrfOutput = vrf(ord.toInt + 1))
                       .flatMap { _ =>
                         store.bestTip.map(_.get.hash).map(h => (acc :+ h, h))
                       }
                 }
      hashesPair <- hashesIO
      (hashes, _) = hashesPair
      preFinalized <- store.lastFinalizedOrdinal
      preSize <- store.size
      // Finalize at ord=5 ⇒ keepFloor = max(0, 5-3) = 2. Ords 0,1 evicted; ords 2,3,4,5 retained (4 entries).
      _ <- store.`finalize`(hashes(5))
      postFinalized <- store.lastFinalizedOrdinal
      postSize <- store.size
      ord5Present <- store.getByHash(hashes(5)).map(_.isDefined)
      ord2Present <- store.getByHash(hashes(2)).map(_.isDefined)
      ord1Absent <- store.getByHash(hashes(1)).map(_.isEmpty)
      ord0Absent <- store.getByHash(hashes(0)).map(_.isEmpty)
    } yield
      expect.all(
        preFinalized == ShardOrdinal.Genesis,
        preSize == 6,
        postFinalized == ShardOrdinal(5L),
        postSize == 4,
        ord5Present,
        ord2Present,
        ord1Absent,
        ord0Absent
      )
  }

  // ===========================================================================
  // Test 5: getByHash + getByOrdinal roundtrip
  // ===========================================================================

  test("getByHash + getByOrdinal: roundtrip on stored entries") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      _ <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      tip <- store.bestTip
      hashA = tip.get.hash
      byHashOut <- store.getByHash(hashA)
      byOrdinalOut <- store.getByOrdinal(ShardOrdinal(0L))
      missByHash <- store.getByHash(Hash.empty)
      missByOrdinal <- store.getByOrdinal(ShardOrdinal(99L)) // requested future
    } yield
      expect.all(
        byHashOut.isDefined,
        byHashOut.exists(_.hash === hashA),
        byOrdinalOut.isDefined,
        byOrdinalOut.exists(_.hash === hashA),
        byOrdinalOut.exists(_.signed.value.shardOrdinal == ShardOrdinal(0L)),
        missByHash.isEmpty,
        missByOrdinal.isEmpty
      )
  }

  // ===========================================================================
  // Test 6: walkBackTo across reorg — bestTip change rolls walkBackTo onto new canonical history
  // ===========================================================================

  test("walkBackTo across reorg: when bestTip changes, walkBackTo returns the new canonical's history") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      // a: shared root
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      _ <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      hashA <- store.bestTip.map(_.get.hash)
      // b: child of a at ord=1, slot=10 (higher slot → loses on slot tiebreak when c comes in at same ord and lower slot). Vary
      // `gl0Anchor` to produce a distinct canonical preimage hash from c — peerByte alone doesn't change the hash because
      // committeeSignatures are EXCLUDED from the signing preimage.
      signedB = mkSignedCheckpoint(ord = 1L, parent = hashA, peerByte = 2, gl0Anchor = 101L)
      _ <- store.store(signedB, parentHash = hashA, shardOrdinal = ShardOrdinal(1L), slot = 10L, vrfOutput = vrf(2))
      tipBeforeReorg <- store.bestTip
      hashB = tipBeforeReorg.get.hash
      walkBeforeReorg <- store.walkBackTo(hashB, depth = 2L)
      // c: alternate child of a at ord=1, slot=2 (LOWER slot → wins maxvalid-tk slot tiebreak). Distinct gl0Anchor ⇒ distinct hash.
      signedC = mkSignedCheckpoint(ord = 1L, parent = hashA, peerByte = 3, gl0Anchor = 102L)
      _ <- store.store(signedC, parentHash = hashA, shardOrdinal = ShardOrdinal(1L), slot = 2L, vrfOutput = vrf(3))
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

  test("bounded retention: after many finalizes, total in-memory map size bounded by keepDepthBehindFinalized + 1") { hasher =>
    implicit val h: Hasher[IO] = hasher
    val keepDepth = 4L
    val totalOrds = 20L
    // After finalize(ord=N), keepFloor = N - keepDepth; entries with ord ∈ [keepFloor, N] are retained ⇒ keepDepth + 1 entries.
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = keepDepth)
      // Seed and finalize each ord one at a time — simulates production Phase 1→2 transitions.
      _ <- (0L until totalOrds).toList.foldLeftM[IO, Hash](Hash.empty) {
             case (parent, ord) =>
               val signed = mkSignedCheckpoint(ord, parent = parent, peerByte = (ord.toInt % 200) + 1)
               store
                 .store(signed, parentHash = parent, shardOrdinal = ShardOrdinal(ord), slot = ord + 1, vrfOutput = vrf(ord.toInt + 1)) >>
                 store.bestTip.map(_.get.hash).flatTap(h => store.`finalize`(h))
           }
      finalSize <- store.size
      finalFinalized <- store.lastFinalizedOrdinal
    } yield
      expect.all(
        // After 20 finalizes at keepDepth=4, in-memory map holds at most keepDepth+1 = 5 entries (keepFloor..tip inclusive).
        finalSize <= (keepDepth + 1L).toInt,
        finalFinalized == ShardOrdinal(totalOrds - 1L)
      )
  }

  // ===========================================================================
  // Bonus: idempotent finalize is a no-op
  // ===========================================================================

  test("finalize is monotone: re-finalizing the same hash is a no-op on lastFinalizedOrdinal") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero, keepDepthBehindFinalized = 8L)
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      _ <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      tip <- store.bestTip
      hashA = tip.get.hash
      _ <- store.`finalize`(hashA)
      finalized1 <- store.lastFinalizedOrdinal
      _ <- store.`finalize`(hashA) // second call should be no-op
      finalized2 <- store.lastFinalizedOrdinal
    } yield expect.all(finalized1 == ShardOrdinal(0L), finalized2 == ShardOrdinal(0L))
  }

  test("shardId is set at construction and exposed through the accessor") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](ShardId.unsafeApply(5))
    } yield expect(store.shardId == ShardId.unsafeApply(5))
  }

  test("empty store: bestTip = None, size = 0, lastFinalizedOrdinal = Genesis") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      tip <- store.bestTip
      sz <- store.size
      fo <- store.lastFinalizedOrdinal
      missByOrd <- store.getByOrdinal(ShardOrdinal(0L))
    } yield expect.all(tip.isEmpty, sz == 0, fo == ShardOrdinal.Genesis, missByOrd.isEmpty)
  }

  test("walkBackTo with depth=0 returns empty list") { hasher =>
    implicit val h: Hasher[IO] = hasher
    for {
      store <- ShardChainStore.make[IO](shardZero)
      signedA = mkSignedCheckpoint(ord = 0L, parent = Hash.empty)
      _ <- store.store(signedA, parentHash = Hash.empty, shardOrdinal = ShardOrdinal(0L), slot = 1L, vrfOutput = vrf(1))
      hashA <- store.bestTip.map(_.get.hash)
      walk <- store.walkBackTo(hashA, depth = 0L)
    } yield expect(walk.isEmpty)
  }

  test("walkBackTo on unknown hash returns empty list (chain break)") { hasher =>
    implicit val h: Hasher[IO] = hasher
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
}
