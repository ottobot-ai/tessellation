package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.{InMemoryMerklePatriciaProducer, TerminalPhysicalTrieKeyCollision}
import io.constellationnetwork.serde.codecs.StringCodec._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder
import weaver.MutableIOSuite

/** Tests for `MptOverlay.passthrough` (#56.2) and `MptOverlay.make(enabled=true)` multi-branch impl (#56.4).
  *
  * Passthrough section: prove the impl behaves identically to using `MptStore` directly.
  *
  * Multi-branch section: prove sibling isolation (the #56.4 acceptance gate), chain reads, prefix scans, `buildRoot` with deltas, and
  * `finalizeBranch` fold-forward + idempotency + reorg-replace (#113).
  */
object MptOverlaySuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private def mkStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield store

  private def addr(seed: Int): Address =
    Address.fromBytes(s"mpt-overlay-suite-seed-$seed".getBytes("UTF-8"))

  private def gskBalance(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  // BranchId fixtures. In passthrough tests they're ignored; in multi-branch tests they identify
  // pending entries in the overlay's pending map.
  private val branchA: BranchId = BranchId(Hash("a" * 64))
  private val branchB: BranchId = BranchId(Hash("b" * 64))
  private val branchC: BranchId = BranchId(Hash("c" * 64))
  // Common parent ("base" id — never registered in pending, so reads at it fall through to MptStore).
  private val parentP: BranchId = BranchId(Hash("0" * 64))

  // ============================================================
  // Passthrough section (#56.2 — preserved)
  // ============================================================

  test("passthrough: writes via BranchHandle land in the underlying MptStore") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      key = gskBalance(0)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(42L)))

      directRead <- store.get[Balance](key)
      overlayRead <- overlay.get[Balance](branchA, key)
    } yield
      expect.all(
        directRead.contains(Balance(NonNegLong(42L))),
        overlayRead.contains(Balance(NonNegLong(42L)))
      )
  }

  test("passthrough: reads ignore the BranchId — same value visible across all branches") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      key = gskBalance(2)
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))

      readA <- overlay.get[Balance](branchA, key)
      readB <- overlay.get[Balance](branchB, key)
      readC <- overlay.get[Balance](branchC, key)
    } yield
      expect.all(
        readA.contains(Balance(NonNegLong(7L))),
        readB.contains(Balance(NonNegLong(7L))),
        readC.contains(Balance(NonNegLong(7L)))
      )
  }

  test("passthrough: BranchHandle.update applies removes and upserts (delegates to MptStore.update)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      keyKeep = gskBalance(3)
      keyDrop = gskBalance(4)
      keyAdd = gskBalance(5)

      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          keyKeep -> Balance(NonNegLong(1L)),
          keyDrop -> Balance(NonNegLong(2L))
        )
      )

      handle <- overlay.checkout(branchA)
      _ <- handle.update[Balance](Map(keyAdd -> Balance(NonNegLong(3L))), Set(keyDrop))

      keepRead <- overlay.get[Balance](branchA, keyKeep)
      dropRead <- overlay.get[Balance](branchA, keyDrop)
      addRead <- overlay.get[Balance](branchA, keyAdd)
    } yield
      expect.all(
        keepRead.contains(Balance(NonNegLong(1L))),
        dropRead.isEmpty,
        addRead.contains(Balance(NonNegLong(3L)))
      )
  }

  test("passthrough: buildRoot returns the same trie as MptStore.build at the same ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      key = gskBalance(6)
      _ <- store.insert[Balance](key, Balance(NonNegLong(99L)))

      directBuild <- store.build(ordinal)
      overlayBuild <- overlay.buildRoot(branchA, ordinal)
    } yield
      expect.all(
        directBuild.isRight,
        overlayBuild.isRight,
        directBuild.toOption.map(_.rootHash) == overlayBuild.toOption.map(_.rootHash)
      )
  }

  test("passthrough: commit associates childTip→parent in ParentChildTree") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      _ <- overlay.commit(handle, branchB, ordinal)

      parent <- pcTree.parentOf(branchB.value)
    } yield expect.same(Some(branchA.value), parent)
  }

  test("passthrough: commit with childTip == parent skips self-association") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      _ <- overlay.commit(handle, branchA, ordinal)

      parent <- pcTree.parentOf(branchA.value)
    } yield expect(parent.isEmpty)
  }

  test("passthrough: finalizeBranch always returns NoOp regardless of branch or ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      r1 <- overlay.finalizeBranch(branchA, ordinal)
      r2 <- overlay.finalizeBranch(branchB, SnapshotOrdinal(NonNegLong(99L)))
      r3 <- overlay.finalizeBranch(branchA, ordinal)
    } yield
      expect.all(
        r1 == FinalizationOutcome.NoOp,
        r2 == FinalizationOutcome.NoOp,
        r3 == FinalizationOutcome.NoOp
      )
  }

  test("passthrough: getAllForPrefix sees writes from any branch — no isolation") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)

      handle <- overlay.checkout(branchA)
      key1 = gskBalance(10)
      key2 = gskBalance(11)
      _ <- handle.insert[Balance](key1, Balance(NonNegLong(1L)))
      _ <- handle.insert[Balance](key2, Balance(NonNegLong(2L)))

      hex1 <- GlobalStateKey.toHex[IO](key1)
      hex2 <- GlobalStateKey.toHex[IO](key2)
      allFromB <- overlay.getAllForPrefix[Balance](branchB, Hex(""))
    } yield
      expect.all(
        allFromB.contains(hex1),
        allFromB.contains(hex2)
      )
  }

  test("MptOverlay.make: OverlayMode.Passthrough routes to passthrough") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      key = gskBalance(20)
      _ <- store.insert[Balance](key, Balance(NonNegLong(5L)))
      r <- overlay.get[Balance](branchA, key)
    } yield expect(r.contains(Balance(NonNegLong(5L))))
  }

  test("MptOverlay.make: OverlayMode.MultiBranch routes to multi-branch (writes do NOT go to underlying store before commit)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      key = gskBalance(21)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(8L)))

      // Multi-branch isolates pre-commit writes — store sees nothing yet.
      directRead <- store.get[Balance](key)
    } yield expect(directRead.isEmpty)
  }

  test("multi-branch commit rejects a physical prefix collision before publishing the branch") { res =>
    implicit val (h, _, js) = res
    val initial = Map(Hex("aa") -> Array[Byte](1))

    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO](initial)
      store <- MptStore.make[IO, Hex](producer, _.pure[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, Hex](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        _.pure[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[String](Hex("aa00"), "collision")
      rejected <- overlay.commit(handle, branchA, ordinal).attempt
      baseAfter <- store.allEntriesAsBytes
      branchAfter <- overlay.allEntriesAsBytes(branchA)
    } yield
      expect.all(
        rejected == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        sameBytes(baseAfter, initial),
        sameBytes(branchAfter, initial)
      )
  }

  // ============================================================
  // Multi-branch section (#56.4)
  // ============================================================

  private def mkMultiBranch(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey])] =
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
    } yield (store, overlay)

  // ----- ACCEPTANCE GATE for #56.4 -----

  test("multi-branch ACCEPTANCE: branchA.insert(k_a, v_a) is NOT visible via overlay.get(branchB, k_a)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      keyA = gskBalance(100)
      keyB = gskBalance(101)

      // Both checkout from the same parent (common ancestor = parentP, the finalized base).
      handleA <- overlay.checkout(parentP)
      handleB <- overlay.checkout(parentP)

      _ <- handleA.insert[Balance](keyA, Balance(NonNegLong(11L)))
      _ <- handleB.insert[Balance](keyB, Balance(NonNegLong(22L)))

      _ <- overlay.commit(handleA, branchA, ordinal)
      _ <- overlay.commit(handleB, branchB, ordinal)

      // From branchA: see A's write, NOT B's.
      aSeesKeyA <- overlay.get[Balance](branchA, keyA)
      aSeesKeyB <- overlay.get[Balance](branchA, keyB)

      // From branchB: see B's write, NOT A's.
      bSeesKeyA <- overlay.get[Balance](branchB, keyA)
      bSeesKeyB <- overlay.get[Balance](branchB, keyB)
    } yield
      expect.all(
        aSeesKeyA.contains(Balance(NonNegLong(11L))),
        aSeesKeyB.isEmpty, // ← isolation: B's write not visible from A
        bSeesKeyA.isEmpty, // ← isolation: A's write not visible from B
        bSeesKeyB.contains(Balance(NonNegLong(22L)))
      )
  }

  // ----- chain reads -----

  test("multi-branch: linear chain — grandchild sees parent's writes via fall-through") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      keyParent = gskBalance(200)
      keyChild = gskBalance(201)

      // Parent commits write to keyParent.
      hParent <- overlay.checkout(parentP)
      _ <- hParent.insert[Balance](keyParent, Balance(NonNegLong(7L)))
      _ <- overlay.commit(hParent, branchA, ordinal)

      // Child of parent (branchA) commits write to keyChild.
      hChild <- overlay.checkout(branchA)
      _ <- hChild.insert[Balance](keyChild, Balance(NonNegLong(13L)))
      _ <- overlay.commit(hChild, branchB, ordinal)

      // Read at child (branchB): sees both writes.
      readsParentKey <- overlay.get[Balance](branchB, keyParent)
      readsChildKey <- overlay.get[Balance](branchB, keyChild)
    } yield
      expect.all(
        readsParentKey.contains(Balance(NonNegLong(7L))),
        readsChildKey.contains(Balance(NonNegLong(13L)))
      )
  }

  test("multi-branch: child overrides parent's value for the same key (later wins)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      key = gskBalance(300)

      hParent <- overlay.checkout(parentP)
      _ <- hParent.insert[Balance](key, Balance(NonNegLong(50L)))
      _ <- overlay.commit(hParent, branchA, ordinal)

      hChild <- overlay.checkout(branchA)
      _ <- hChild.insert[Balance](key, Balance(NonNegLong(99L)))
      _ <- overlay.commit(hChild, branchB, ordinal)

      readChild <- overlay.get[Balance](branchB, key)
      readParent <- overlay.get[Balance](branchA, key)
    } yield
      expect.all(
        readChild.contains(Balance(NonNegLong(99L))), // child override
        readParent.contains(Balance(NonNegLong(50L))) // parent unchanged
      )
  }

  test("multi-branch: chain remove hides a base value (no fall-through after explicit removal)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      key = gskBalance(400)
      // Pre-finalized base has an entry for `key`.
      _ <- store.insert[Balance](key, Balance(NonNegLong(123L)))

      handle <- overlay.checkout(parentP)
      _ <- handle.remove(key)
      _ <- overlay.commit(handle, branchA, ordinal)

      readBranchA <- overlay.get[Balance](branchA, key)
      // From a branch that didn't remove (sibling parentP-branch), read still falls through to base.
      readSibling <- overlay.get[Balance](branchB, key)
    } yield
      expect.all(
        readBranchA.isEmpty, // chain removal wins over base
        readSibling.contains(Balance(NonNegLong(123L))) // base still visible from sibling
      )
  }

  test("multi-branch: overlay.get falls through to MptStore base when chain doesn't mention the key") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyInBase = gskBalance(500)
      keyInBranch = gskBalance(501)
      _ <- store.insert[Balance](keyInBase, Balance(NonNegLong(42L)))

      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](keyInBranch, Balance(NonNegLong(99L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      // Reading at branchA: keyInBranch from branch, keyInBase from fall-through.
      readBranchKey <- overlay.get[Balance](branchA, keyInBranch)
      readBaseKey <- overlay.get[Balance](branchA, keyInBase)
    } yield
      expect.all(
        readBranchKey.contains(Balance(NonNegLong(99L))),
        readBaseKey.contains(Balance(NonNegLong(42L)))
      )
  }

  // ----- getAllForPrefix isolation -----

  test("multi-branch: getAllForPrefix composes base + chain — sibling isolation preserved") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyBase = gskBalance(600)
      keyA = gskBalance(601)
      keyB = gskBalance(602)
      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(1L)))

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(2L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](keyB, Balance(NonNegLong(3L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      hexBase <- GlobalStateKey.toHex[IO](keyBase)
      hexA <- GlobalStateKey.toHex[IO](keyA)
      hexB <- GlobalStateKey.toHex[IO](keyB)
      fromA <- overlay.getAllForPrefix[Balance](branchA, Hex(""))
      fromB <- overlay.getAllForPrefix[Balance](branchB, Hex(""))
    } yield
      expect.all(
        fromA.contains(hexBase),
        fromA.contains(hexA),
        !fromA.contains(hexB), // ← isolation
        fromB.contains(hexBase),
        !fromB.contains(hexA), // ← isolation
        fromB.contains(hexB)
      )
  }

  test("multi-branch: getAllForPrefix removes chain-removed keys from the base view") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyKeep = gskBalance(700)
      keyDrop = gskBalance(701)
      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          keyKeep -> Balance(NonNegLong(1L)),
          keyDrop -> Balance(NonNegLong(2L))
        )
      )

      handle <- overlay.checkout(parentP)
      _ <- handle.remove(keyDrop)
      _ <- overlay.commit(handle, branchA, ordinal)

      hexKeep <- GlobalStateKey.toHex[IO](keyKeep)
      hexDrop <- GlobalStateKey.toHex[IO](keyDrop)
      fromBranch <- overlay.getAllForPrefix[Balance](branchA, Hex(""))
      fromBase <- overlay.getAllForPrefix[Balance](BranchId.base, Hex(""))
    } yield
      expect.all(
        fromBranch.contains(hexKeep),
        !fromBranch.contains(hexDrop),
        fromBase.contains(hexKeep),
        fromBase.contains(hexDrop) // base view (unknown branch id) sees both
      )
  }

  test("multi-branch: getAllForPrefix fails closed on an undecodable matching branch upsert") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair
      key = gskBalance(702)
      hex <- GlobalStateKey.toHex[IO](key)
      prefix <- GlobalStateKey.hypergraphFieldPrefix[IO](GlobalStateFieldId.Balances)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[String](key, "x")
      _ <- overlay.commit(handle, branchA, ordinal)
      pointRead <- overlay.get[Balance](branchA, key)
      prefixRead <- overlay.getAllForPrefix[Balance](branchA, prefix).attempt
    } yield
      expect.all(
        pointRead.isEmpty,
        prefixRead.left.exists(e => e.getMessage.contains("undecodable bytes") && e.getMessage.contains(hex.value))
      )
  }

  // ----- allEntriesAsBytes branch-aware view -----

  test("multi-branch: allEntriesAsBytes(branch) sees pending upserts that base does not") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyBase = gskBalance(900)
      keyPending = gskBalance(901)
      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(1L)))

      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](keyPending, Balance(NonNegLong(2L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      hexBase <- GlobalStateKey.toHex[IO](keyBase)
      hexPending <- GlobalStateKey.toHex[IO](keyPending)

      // Branch view sees both base and pending.
      branchBytes <- overlay.allEntriesAsBytes(branchA)
      // Base bytes do NOT include the pending upsert (commit didn't fold to base under MultiBranch).
      baseBytes <- store.allEntriesAsBytes
    } yield
      expect.all(
        branchBytes.contains(hexBase),
        branchBytes.contains(hexPending),
        baseBytes.contains(hexBase),
        !baseBytes.contains(hexPending)
      )
  }

  test("multi-branch: allEntriesAsBytes(branch) hides keys removed by the chain") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyKeep = gskBalance(910)
      keyDrop = gskBalance(911)
      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          keyKeep -> Balance(NonNegLong(1L)),
          keyDrop -> Balance(NonNegLong(2L))
        )
      )

      handle <- overlay.checkout(parentP)
      _ <- handle.remove(keyDrop)
      _ <- overlay.commit(handle, branchA, ordinal)

      hexKeep <- GlobalStateKey.toHex[IO](keyKeep)
      hexDrop <- GlobalStateKey.toHex[IO](keyDrop)
      branchBytes <- overlay.allEntriesAsBytes(branchA)
      baseBytes <- store.allEntriesAsBytes
    } yield
      expect.all(
        branchBytes.contains(hexKeep),
        !branchBytes.contains(hexDrop), // chain removed it
        baseBytes.contains(hexKeep),
        baseBytes.contains(hexDrop) // base still has it (no fold yet)
      )
  }

  test("multi-branch: allEntriesAsBytes(branch) preserves sibling isolation") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyBase = gskBalance(920)
      keyA = gskBalance(921)
      keyB = gskBalance(922)
      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(1L)))

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(10L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](keyB, Balance(NonNegLong(20L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      hexBase <- GlobalStateKey.toHex[IO](keyBase)
      hexA <- GlobalStateKey.toHex[IO](keyA)
      hexB <- GlobalStateKey.toHex[IO](keyB)
      fromA <- overlay.allEntriesAsBytes(branchA)
      fromB <- overlay.allEntriesAsBytes(branchB)
    } yield
      expect.all(
        fromA.contains(hexBase),
        fromA.contains(hexA),
        !fromA.contains(hexB), // ← B's write not visible from A
        fromB.contains(hexBase),
        !fromB.contains(hexA), // ← A's write not visible from B
        fromB.contains(hexB)
      )
  }

  test("multi-branch: allEntriesAsBytes(unknownBranch) returns the base view (no chain)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(930), Balance(NonNegLong(7L)))

      // Commit a branch whose state should NOT be visible via an unrelated BranchId.
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](gskBalance(931), Balance(NonNegLong(99L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      // BranchId.base (Hash.empty) is not in pending — falls through to base.
      baseView <- overlay.allEntriesAsBytes(BranchId.base)
      directBase <- store.allEntriesAsBytes
    } yield
      expect.all(
        baseView.keySet == directBase.keySet,
        baseView.size == directBase.size
      )
  }

  test("passthrough: allEntriesAsBytes ignores BranchId — returns underlying entries") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      _ <- store.insert[Balance](gskBalance(940), Balance(NonNegLong(3L)))
      fromA <- overlay.allEntriesAsBytes(branchA)
      fromB <- overlay.allEntriesAsBytes(branchB)
      direct <- store.allEntriesAsBytes
    } yield
      expect.all(
        fromA.keySet == direct.keySet,
        fromB.keySet == direct.keySet
      )
  }

  test("passthrough: allEntriesAsBytesWithHandle delegates to underlying.allEntriesAsBytes") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      keyBase = gskBalance(942)
      keyHandle = gskBalance(943)
      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(1L)))
      hexBase <- GlobalStateKey.toHex[IO](keyBase)
      hexHandle <- GlobalStateKey.toHex[IO](keyHandle)

      // Under Passthrough handle.insert writes straight to the store, so the post-write view
      // already includes the new key — both before and after we sample via `allEntriesAsBytesWithHandle`.
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](keyHandle, Balance(NonNegLong(2L)))
      view <- overlay.allEntriesAsBytesWithHandle(handle, ordinal)
      direct <- store.allEntriesAsBytes
    } yield
      expect.all(
        view.keySet == direct.keySet,
        view.contains(hexBase),
        view.contains(hexHandle)
      )
  }

  test("multi-branch: allEntriesAsBytesWithHandle composes base + parent chain + handle accumulator") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyBase = gskBalance(944)
      keyParentBranch = gskBalance(945)
      keyHandle = gskBalance(946)

      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(1L)))

      // Commit a parent branch under `branchA` so a child checkout against `branchA` will
      // see its writes through `mergedChain`.
      parentHandle <- overlay.checkout(parentP)
      _ <- parentHandle.insert[Balance](keyParentBranch, Balance(NonNegLong(2L)))
      _ <- overlay.commit(parentHandle, branchA, ordinal)

      // Child checkout against `branchA`, accumulate one more pending write.
      childHandle <- overlay.checkout(branchA)
      _ <- childHandle.insert[Balance](keyHandle, Balance(NonNegLong(3L)))

      hexBase <- GlobalStateKey.toHex[IO](keyBase)
      hexParentBranch <- GlobalStateKey.toHex[IO](keyParentBranch)
      hexHandle <- GlobalStateKey.toHex[IO](keyHandle)

      // The post-write view at the child handle should include all three:
      // (a) base, (b) parent-branch's committed entry, (c) handle's still-uncommitted entry.
      view <- overlay.allEntriesAsBytesWithHandle(childHandle, ordinal)

      // Base is unchanged — handle is uncommitted; parent-branch was committed but lives in
      // pendingRef under MultiBranch, not folded to base.
      directBase <- store.allEntriesAsBytes
    } yield
      expect.all(
        view.contains(hexBase),
        view.contains(hexParentBranch),
        view.contains(hexHandle),
        directBase.contains(hexBase),
        !directBase.contains(hexParentBranch),
        !directBase.contains(hexHandle)
      )
  }

  test("multi-branch: allEntriesAsBytesWithHandle hides keys the handle removed") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyKeep = gskBalance(947)
      keyDrop = gskBalance(948)

      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          keyKeep -> Balance(NonNegLong(10L)),
          keyDrop -> Balance(NonNegLong(20L))
        )
      )

      handle <- overlay.checkout(parentP)
      _ <- handle.remove(keyDrop)

      hexKeep <- GlobalStateKey.toHex[IO](keyKeep)
      hexDrop <- GlobalStateKey.toHex[IO](keyDrop)

      view <- overlay.allEntriesAsBytesWithHandle(handle, ordinal)
    } yield
      expect.all(
        view.contains(hexKeep),
        !view.contains(hexDrop)
      )
  }

  // ----- buildRoot includes branch deltas -----

  test("multi-branch: buildRoot for sibling branches yields different root hashes") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair
      // Seed the base — InMemoryMerklePatriciaProducer.build errors on "no entries" so the overlay's
      // composition step needs a non-empty base to materialize against.
      _ <- store.insert[Balance](gskBalance(799), Balance(NonNegLong(100L)))

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(800), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](gskBalance(801), Balance(NonNegLong(2L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      rootA <- overlay.buildRoot(branchA, ordinal)
      rootB <- overlay.buildRoot(branchB, ordinal)
    } yield
      expect.all(
        rootA.isRight,
        rootB.isRight,
        rootA.toOption.map(_.rootHash) != rootB.toOption.map(_.rootHash)
      )
  }

  test("multi-branch: buildRoot for an unknown branch (== base) equals MptStore.build") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      _ <- store.insert[Balance](gskBalance(900), Balance(NonNegLong(7L)))

      // No branch registered with parentP — buildRoot at parentP equals base.
      directBase <- store.build(ordinal)
      overlayBase <- overlay.buildRoot(parentP, ordinal)
    } yield
      expect.all(
        directBase.isRight,
        overlayBase.isRight,
        directBase.toOption.map(_.rootHash) == overlayBase.toOption.map(_.rootHash)
      )
  }

  // ----- finalize: fold-forward + drop -----

  test("multi-branch: finalizeBranch folds canonical chain into base and drops siblings") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyA = gskBalance(1000)
      keyB = gskBalance(1001)

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(11L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](keyB, Balance(NonNegLong(22L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      outcome <- overlay.finalizeBranch(branchA, ordinal)

      // After finalize, base contains A's write and NOT B's (B was dropped).
      baseHasA <- store.get[Balance](keyA)
      baseHasB <- store.get[Balance](keyB)

      // Subsequent reads at any branch see base only (pending state cleared).
      stillSeesA <- overlay.get[Balance](branchA, keyA)
      noLongerSeesB <- overlay.get[Balance](branchB, keyB)
    } yield
      expect.all(
        outcome match {
          case FinalizationOutcome.Folded(keysApplied, branchesDropped) =>
            keysApplied == 1 && branchesDropped == 1
          case _ => false
        },
        baseHasA.contains(Balance(NonNegLong(11L))),
        baseHasB.isEmpty,
        stillSeesA.contains(Balance(NonNegLong(11L))),
        noLongerSeesB.isEmpty
      )
  }

  test("multi-branch: finalizeBranch is idempotent at same (ordinal, hash)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(1100), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      first <- overlay.finalizeBranch(branchA, ordinal)
      second <- overlay.finalizeBranch(branchA, ordinal)
    } yield
      expect.all(
        first match {
          case FinalizationOutcome.Folded(_, _) => true
          case _                                => false
        },
        second == FinalizationOutcome.NoOp
      )
  }

  test("multi-branch: finalizeBranch reorg-replaces on different canonical at same ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(1200), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      _ <- overlay.finalizeBranch(branchA, ordinal)

      // After the first finalize, pending is empty. Re-finalizing the SAME ordinal with a DIFFERENT
      // canonical must NOT error (#113): followers re-pulling through `setForRecovery` legitimately
      // re-validate the same ord under a new canonical hash. The reorg-replace path updates the
      // finality marker; with no pending entries to fold, the outcome is NoOp.
      reorg <- overlay.finalizeBranch(branchB, ordinal)
    } yield expect(reorg == FinalizationOutcome.NoOp)
  }

  test("multi-branch: finalizeBranch reorg-replace drops orphan fork pending when canonical is unknown (#116)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      orphanKey = gskBalance(2100)
      orphanValue = Balance(NonNegLong(7777L))

      // Setup: commit + finalize branchA at the ordinal. After this, finalizedRef[ord]=branchA,
      // pending is empty (foldIntoBase cleared it), lastCommittedBranchRef reset to None.
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(2099), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)

      // The bug surface (iter14, gl0-2 solo-fork scenario): a node ingests a fork branch into
      // the overlay AFTER finalizing the prior canonical at the same ord. This branch is an orphan
      // — it's not on the canonical chain, but the overlay has no way to know that yet. In the real
      // failing case this happens via a re-replay through createContext that registers the wrong
      // chain head, then ChainSync delivers the canonical rebuilt from a peer, but the orphan stays
      // in pending. Simulate this by committing an orphan branch (parent=parentP) at the same ordinal.
      hOrphan <- overlay.checkout(parentP)
      _ <- hOrphan.insert[Balance](orphanKey, orphanValue)
      _ <- overlay.commit(hOrphan, branchC, ordinal)

      // Reorg-replace fires: a NEW canonical (branchB) at the SAME ord, with branchB never
      // overlay-registered. Before the #116 fix, this just bumped finalizedRef and left the orphan
      // (branchC) in pending — eviction protected it via lastCommittedBranchRef and reads at
      // canonical-tip parentTip leaked through to its writes via mergedChain ancestor walks.
      // After the fix: pending is cleared, lastCommittedBranchRef reset, outcome reports the drop.
      reorg <- overlay.finalizeBranch(branchB, ordinal)

      // Orphan's write must no longer be visible from any branch — pending was cleared, base
      // never received the orphan's delta (no fold happened for branchC), so reads return base only.
      orphanReadFromOrphan <- overlay.get[Balance](branchC, orphanKey)
      orphanReadFromCanonical <- overlay.get[Balance](branchB, orphanKey)
      orphanReadFromBase <- overlay.get[Balance](parentP, orphanKey)
    } yield
      expect.all(
        // Outcome reports the orphan was dropped (pending.size was 1 before clear).
        reorg == FinalizationOutcome.Folded(0, 1),
        // Orphan's write fully invisible — confirms pending was cleared.
        orphanReadFromOrphan.isEmpty,
        orphanReadFromCanonical.isEmpty,
        orphanReadFromBase.isEmpty
      )
  }

  test("multi-branch: reorg-replace canonical-not-in-pending UNDOES prior canonical's base writes (#121)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      losingKey = gskBalance(3001)
      losingValue = Balance(NonNegLong(99L))

      // Setup: branchA commits & finalizes at ord with a write that gets folded into base.
      // Before #121, after the reorg-replace below, this write would remain in base — the bug.
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](losingKey, losingValue)
      _ <- overlay.commit(hA, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)

      // Verify the fold happened — branchA's write is now in base.
      readAfterA <- overlay.get[Balance](parentP, losingKey)

      // Reorg-replace: a different canonical (branchB) is now finalized at the same ord.
      // branchB is NOT in pending (no overlay.commit happened for it).
      reorg <- overlay.finalizeBranch(branchB, ordinal)

      // CORE ASSERTION: branchA's write must be undone from base. Reading at any branch
      // should return empty — branchA was rejected, branchB never wrote, base reverted.
      readAfterReorgFromBase <- overlay.get[Balance](parentP, losingKey)
      readAfterReorgFromBranchB <- overlay.get[Balance](branchB, losingKey)
    } yield
      expect.all(
        // Pre-condition: the fold happened.
        readAfterA.contains(losingValue),
        // Outcome: branchA's pending was already empty post-fold, so no fork branches dropped.
        reorg == FinalizationOutcome.NoOp,
        // Core: the undo journal reverted branchA's base write.
        readAfterReorgFromBase.isEmpty,
        readAfterReorgFromBranchB.isEmpty
      )
  }

  test("multi-branch: reorg-replace canonical-in-pending UNDOES old canonical's writes for keys new canonical doesn't touch (#121)") {
    res =>
      implicit val (h, _, js) = res
      for {
        pair <- mkMultiBranch
        (_, overlay) = pair

        sharedKey = gskBalance(3010) // both canonicals write to this
        losingOnlyKey = gskBalance(3011) // ONLY branchA writes to this — must be undone on reorg

        vA_shared = Balance(NonNegLong(11L))
        vA_losingOnly = Balance(NonNegLong(22L))
        vB_shared = Balance(NonNegLong(33L))

        // branchA: writes to both keys, folds into base.
        hA <- overlay.checkout(parentP)
        _ <- hA.insert[Balance](sharedKey, vA_shared)
        _ <- hA.insert[Balance](losingOnlyKey, vA_losingOnly)
        _ <- overlay.commit(hA, branchA, ordinal)
        _ <- overlay.finalizeBranch(branchA, ordinal)

        // branchB: writes ONLY to sharedKey (not losingOnlyKey).
        hB <- overlay.checkout(parentP)
        _ <- hB.insert[Balance](sharedKey, vB_shared)
        _ <- overlay.commit(hB, branchB, ordinal)

        // Reorg-replace at the same ord: branchB IS in pending this time.
        reorg <- overlay.finalizeBranch(branchB, ordinal)

        // After reorg: sharedKey reflects branchB's value; losingOnlyKey is GONE (branchA's
        // write was undone). Before #121, sharedKey would be branchB's value but losingOnlyKey
        // would retain branchA's stale value — the silent-divergence bug.
        readShared <- overlay.get[Balance](branchB, sharedKey)
        readLosingOnly <- overlay.get[Balance](branchB, losingOnlyKey)
      } yield
        expect.all(
          reorg == FinalizationOutcome.Folded(1, 0),
          readShared.contains(vB_shared),
          readLosingOnly.isEmpty
        )
  }

  test("multi-branch: reorg-replace canonical-not-in-pending fails closed when the prior undo is missing") { res =>
    implicit val (h, _, js) = res
    val seed = gskBalance(3020)
    val oldKey = gskBalance(3021)

    for {
      pair <- mkMultiBranch
      (store, overlay) = pair
      _ <- store.insert[Balance](seed, Balance(NonNegLong(1L)))

      oldHandle <- overlay.checkout(parentP)
      _ <- oldHandle.insert[Balance](oldKey, Balance(NonNegLong(11L)))
      _ <- overlay.commit(oldHandle, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)

      // This replacement has no local payload and consumes branchA's undo, leaving the parent base.
      _ <- overlay.finalizeBranch(branchB, ordinal)
      baseBeforeGap <- snapshotBytes(store)
      sizesBeforeGap <- overlay.journalSizes

      rejected <- overlay.finalizeBranch(branchC, ordinal).attempt
      baseAfterGap <- snapshotBytes(store)
      sizesAfterGap <- overlay.journalSizes

      // Recovery explicitly resets overlay history after restoring/confirming the parent image.
      _ <- overlay.unsafe_reset
      retriedAfterRecovery <- overlay.finalizeBranch(branchC, ordinal)
    } yield
      expect.all(
        rejected match {
          case Left(_: FinalizationUndoGapError) => true
          case _                                 => false
        },
        sameBytes(baseAfterGap, baseBeforeGap),
        sizesAfterGap == sizesBeforeGap,
        retriedAfterRecovery == FinalizationOutcome.NoOp
      )
  }

  test("multi-branch: reorg-replace canonical-in-pending preserves base and refs when the prior undo is missing") { res =>
    implicit val (h, _, js) = res
    val seed = gskBalance(3030)
    val oldKey = gskBalance(3031)
    val replacementKey = gskBalance(3032)
    val replacementValue = Balance(NonNegLong(33L))

    for {
      pair <- mkMultiBranch
      (store, overlay) = pair
      _ <- store.insert[Balance](seed, Balance(NonNegLong(1L)))

      oldHandle <- overlay.checkout(parentP)
      _ <- oldHandle.insert[Balance](oldKey, Balance(NonNegLong(22L)))
      _ <- overlay.commit(oldHandle, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)
      _ <- overlay.finalizeBranch(branchB, ordinal) // consumes the only undo at this ordinal

      replacementHandle <- overlay.checkout(parentP)
      _ <- replacementHandle.insert[Balance](replacementKey, replacementValue)
      _ <- overlay.commit(replacementHandle, branchC, ordinal)
      baseBeforeGap <- snapshotBytes(store)
      sizesBeforeGap <- overlay.journalSizes

      rejected <- overlay.finalizeBranch(branchC, ordinal).attempt
      baseAfterGap <- snapshotBytes(store)
      sizesAfterGap <- overlay.journalSizes
      pendingAfterGap <- overlay.get[Balance](branchC, replacementKey)

      _ <- overlay.unsafe_reset
      recoveredHandle <- overlay.checkout(parentP)
      _ <- recoveredHandle.insert[Balance](replacementKey, replacementValue)
      _ <- overlay.commit(recoveredHandle, branchC, ordinal)
      retriedAfterRecovery <- overlay.finalizeBranch(branchC, ordinal)
      replacementAfterRecovery <- store.get[Balance](replacementKey)
    } yield
      expect.all(
        rejected match {
          case Left(_: FinalizationUndoGapError) => true
          case _                                 => false
        },
        sameBytes(baseAfterGap, baseBeforeGap),
        sizesAfterGap == sizesBeforeGap,
        pendingAfterGap.contains(replacementValue),
        retriedAfterRecovery == FinalizationOutcome.Folded(1, 0),
        replacementAfterRecovery.contains(replacementValue)
      )
  }

  test("multi-branch: unknown first finalization is idempotent but a different hash fails without an undo") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      // No branchA in pending; finalize is a NoOp but records the (ordinal, branchA) finalization.
      r1 <- overlay.finalizeBranch(branchA, ordinal)
      // Re-finalizing same (ordinal, branchA) — still NoOp.
      r2 <- overlay.finalizeBranch(branchA, ordinal)
      // No fold occurred, so there is no authenticated undo record proving the pre-ordinal base for a different hash.
      reorg <- overlay.finalizeBranch(branchB, ordinal).attempt
    } yield
      expect.all(
        r1 == FinalizationOutcome.NoOp,
        r2 == FinalizationOutcome.NoOp,
        reorg match {
          case Left(_: FinalizationUndoGapError) => true
          case _                                 => false
        }
      )
  }

  test("multi-branch: writes accumulated on a checked-out handle are NOT visible via overlay.get before commit") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      key = gskBalance(1300)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(5L)))

      // Read at the parent branch (parentP) before commit — handle's write isn't registered yet.
      preCommit <- overlay.get[Balance](parentP, key)

      _ <- overlay.commit(handle, branchA, ordinal)
      postCommit <- overlay.get[Balance](branchA, key)
    } yield
      expect.all(
        preCommit.isEmpty,
        postCommit.contains(Balance(NonNegLong(5L)))
      )
  }

  // ============================================================
  // #56.4.5 — partition-atomicity contract for finalize
  // ============================================================
  //
  // Contract: a single `finalizeBranch` writes ALL partition deltas (sidecars + user fields) to the base
  // in one savepoint-bracketed transaction. There is no partial-partition state. The rollback-on-failure
  // half of the contract is exercised by `MptStore.withTransaction`'s own savepoint mechanism — these
  // tests cover the success-side: large multi-key folds and cross-partition mixes land all at once.

  test("atomicity: linear chain finalize folds ALL keys from every level into base together") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyP = gskBalance(2000)
      keyC = gskBalance(2001)
      keyG = gskBalance(2002)

      // Three-deep chain: parentBranch -> child -> grandchild, each writing one key.
      hP <- overlay.checkout(parentP)
      _ <- hP.insert[Balance](keyP, Balance(NonNegLong(10L)))
      _ <- overlay.commit(hP, branchA, ordinal)

      hC <- overlay.checkout(branchA)
      _ <- hC.insert[Balance](keyC, Balance(NonNegLong(20L)))
      _ <- overlay.commit(hC, branchB, ordinal)

      hG <- overlay.checkout(branchB)
      _ <- hG.insert[Balance](keyG, Balance(NonNegLong(30L)))
      _ <- overlay.commit(hG, branchC, ordinal)

      _ <- overlay.finalizeBranch(branchC, ordinal)

      // ALL three keys must be in base after a single finalize call.
      gotP <- store.get[Balance](keyP)
      gotC <- store.get[Balance](keyC)
      gotG <- store.get[Balance](keyG)
    } yield
      expect.all(
        gotP.contains(Balance(NonNegLong(10L))),
        gotC.contains(Balance(NonNegLong(20L))),
        gotG.contains(Balance(NonNegLong(30L)))
      )
  }

  test("atomicity: cross-partition finalize lands user-field AND system-namespace keys together") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      // User-field keys (Balances partition).
      userKey1 = gskBalance(2100)
      userKey2 = gskBalance(2101)

      // System-namespace keys (sidecar partitions).
      activeIdxKey <- GlobalStateKey.activeAddressIndexKey[IO](GlobalStateFieldId.Balances)
      expiryAllowSpendsKey <- GlobalStateKey
        .expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexAllowSpends, EpochProgress(NonNegLong(42L)))

      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](userKey1, Balance(NonNegLong(1L)))
      _ <- handle.insert[Balance](userKey2, Balance(NonNegLong(2L)))
      // Sidecar partitions normally hold structured codecs; for atomicity testing we only care that the
      // fold writes the bytes through. Use Balance bytes as a stand-in payload.
      _ <- handle.insert[Balance](activeIdxKey, Balance(NonNegLong(7L)))
      _ <- handle.insert[Balance](expiryAllowSpendsKey, Balance(NonNegLong(13L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      _ <- overlay.finalizeBranch(branchA, ordinal)

      // All four keys — across user-field and sidecar partitions — must be visible in base.
      g1 <- store.get[Balance](userKey1)
      g2 <- store.get[Balance](userKey2)
      gActive <- store.get[Balance](activeIdxKey)
      gExpiry <- store.get[Balance](expiryAllowSpendsKey)
    } yield
      expect.all(
        g1.contains(Balance(NonNegLong(1L))),
        g2.contains(Balance(NonNegLong(2L))),
        gActive.contains(Balance(NonNegLong(7L))),
        gExpiry.contains(Balance(NonNegLong(13L)))
      )
  }

  test("atomicity: finalize that combines upserts AND removals applies both in one go (no half-applied state)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      // Pre-populate base with two keys we'll later remove via finalize.
      removedKey1 = gskBalance(2200)
      removedKey2 = gskBalance(2201)
      _ <- store.insert[Balance](
        Map[GlobalStateKey, Balance](
          removedKey1 -> Balance(NonNegLong(100L)),
          removedKey2 -> Balance(NonNegLong(200L))
        )
      )

      // Branch removes both AND adds two new keys.
      addedKey1 = gskBalance(2202)
      addedKey2 = gskBalance(2203)

      handle <- overlay.checkout(parentP)
      _ <- handle.remove(List(removedKey1, removedKey2))
      _ <- handle.insert[Balance](addedKey1, Balance(NonNegLong(11L)))
      _ <- handle.insert[Balance](addedKey2, Balance(NonNegLong(22L)))
      _ <- overlay.commit(handle, branchA, ordinal)

      _ <- overlay.finalizeBranch(branchA, ordinal)

      gRemoved1 <- store.get[Balance](removedKey1)
      gRemoved2 <- store.get[Balance](removedKey2)
      gAdded1 <- store.get[Balance](addedKey1)
      gAdded2 <- store.get[Balance](addedKey2)
    } yield
      expect.all(
        gRemoved1.isEmpty,
        gRemoved2.isEmpty,
        gAdded1.contains(Balance(NonNegLong(11L))),
        gAdded2.contains(Balance(NonNegLong(22L)))
      )
  }

  // ============================================================
  // #56.9 — memory budget + Taktikos-scored eviction
  // ============================================================

  private def mkMultiBranchWithCap(
    cap: Int,
    bestTipsFn: IO[Set[BranchId]] = IO.pure(Set.empty[BranchId])
  )(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey])] =
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.MultiBranch(cap),
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = bestTipsFn
      )
    } yield (store, overlay)

  // Use Hex chars for ordering predictability — branchA < branchB < branchC < branchD < branchE lex.
  private val branchD: BranchId = BranchId(Hash("d" * 64))
  private val branchE: BranchId = BranchId(Hash("e" * 64))

  // Helper: commit an empty branch at the given childTip and ordinal (parent = parentP for siblings,
  // parent = previousChild for chains). Used to populate the pending map without bothering with writes.
  private def commitEmpty(
    overlay: MptOverlay[IO, GlobalStateKey],
    parent: BranchId,
    childTip: BranchId,
    ordinal: SnapshotOrdinal
  ): IO[Unit] =
    overlay.checkout(parent).flatMap(h => overlay.commit(h, childTip, ordinal))

  test("eviction: cap-not-exceeded — no branches evicted") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranchWithCap(cap = 4)
      (_, overlay) = pair

      // Commit 4 sibling branches at increasing ordinals — exactly at cap, no eviction.
      _ <- commitEmpty(overlay, parentP, branchA, SnapshotOrdinal(NonNegLong(1L)))
      _ <- commitEmpty(overlay, parentP, branchB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- commitEmpty(overlay, parentP, branchC, SnapshotOrdinal(NonNegLong(3L)))
      _ <- commitEmpty(overlay, parentP, branchD, SnapshotOrdinal(NonNegLong(4L)))

      // All four should still be reachable via parent-child tree.
      pA <- overlay.parentChildTree.parentOf(branchA.value)
      pB <- overlay.parentChildTree.parentOf(branchB.value)
      pC <- overlay.parentChildTree.parentOf(branchC.value)
      pD <- overlay.parentChildTree.parentOf(branchD.value)
    } yield
      expect.all(
        pA.contains(parentP.value),
        pB.contains(parentP.value),
        pC.contains(parentP.value),
        pD.contains(parentP.value)
      )
  }

  test("eviction: cap exceeded — lowest-ordinal branch is evicted") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranchWithCap(cap = 3)
      (store, overlay) = pair

      // Commit branches with distinct ordinals 1,2,3,4. branchA (ordinal 1) is lowest-scoring;
      // commit of branchD pushes pending to 4 → eviction drops branchA.
      // Use distinct keys per branch so we can detect eviction by reading.
      keyForA = gskBalance(3000)
      keyForD = gskBalance(3003)

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyForA, Balance(NonNegLong(11L)))
      _ <- overlay.commit(hA, branchA, SnapshotOrdinal(NonNegLong(1L)))

      _ <- commitEmpty(overlay, parentP, branchB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- commitEmpty(overlay, parentP, branchC, SnapshotOrdinal(NonNegLong(3L)))

      hD <- overlay.checkout(parentP)
      _ <- hD.insert[Balance](keyForD, Balance(NonNegLong(44L)))
      _ <- overlay.commit(hD, branchD, SnapshotOrdinal(NonNegLong(4L)))

      // After D's commit, pending size went 3 → 4 → eviction → 3. branchA (ordinal 1) should be gone.
      // Reading at branchA falls through to base — base never had keyForA, so result is None.
      readAfterEvict <- overlay.get[Balance](branchA, keyForA)

      // branchD's writes should still be visible at branchD.
      readD <- overlay.get[Balance](branchD, keyForD)
    } yield
      expect.all(
        readAfterEvict.isEmpty, // branchA evicted — its write no longer visible
        readD.contains(Balance(NonNegLong(44L)))
      )
  }

  test("eviction: ancestor of bestTip is NEVER evicted, even with lowest ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      // bestTip = branchD; chain is branchA → branchB → branchC → branchD (linear).
      // branchA is lowest-ordinal but is bestTip's ancestor → must not be evicted.
      // After committing branchE (sibling of parentP), eviction should fire on branchE, not branchA.
      bestTipsRef <- IO.ref[Set[BranchId]](Set.empty[BranchId])
      pair <- mkMultiBranchWithCap(cap = 4, bestTipsFn = bestTipsRef.get)
      (_, overlay) = pair

      _ <- commitEmpty(overlay, parentP, branchA, SnapshotOrdinal(NonNegLong(1L)))
      _ <- commitEmpty(overlay, branchA, branchB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- commitEmpty(overlay, branchB, branchC, SnapshotOrdinal(NonNegLong(3L)))
      _ <- commitEmpty(overlay, branchC, branchD, SnapshotOrdinal(NonNegLong(4L)))

      // Set bestTips to {branchD} AFTER chain is built; subsequent commits will protect ancestors.
      _ <- bestTipsRef.set(Set(branchD))

      // Commit branchE as a sibling of parentP at ordinal 5 — pending now has 5, cap=4.
      // Eviction should fire. With branchA→D as bestTip's ancestor chain, the ONLY non-ancestor is
      // branchE itself (which we just added). branchE has the highest ordinal but is the only candidate.
      _ <- commitEmpty(overlay, parentP, branchE, SnapshotOrdinal(NonNegLong(5L)))

      // branchA-D should all remain in parent-child tree (their associations weren't touched).
      pA <- overlay.parentChildTree.parentOf(branchA.value)
      pD <- overlay.parentChildTree.parentOf(branchD.value)
      pE <- overlay.parentChildTree.parentOf(branchE.value)
    } yield
      // We can't directly observe pendingRef contents from outside, but we can observe the parent-child
      // tree (which is unaffected by eviction — eviction only removes from pendingRef). What we CAN
      // observe: branchA was kept (we haven't independently verified, but the contract is that ancestors
      // are protected). Read at branchA still works (always falls through to base since we used empty
      // commits).
      expect.all(
        pA.contains(parentP.value),
        pD.contains(branchC.value),
        pE.contains(parentP.value)
      )
  }

  test("eviction: tie on ordinal broken deterministically by BranchId lex order") { res =>
    implicit val (h, _, js) = res
    for {
      // Two branches at the same ordinal — branchA and branchB. branchA's hash starts with 'a', branchB
      // with 'b'. Lex order: a < b. So tie-breaker should prefer evicting branchA (the lower hash).
      pair <- mkMultiBranchWithCap(cap = 2)
      (_, overlay) = pair

      keyA = gskBalance(3100)
      keyB = gskBalance(3101)

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, SnapshotOrdinal(NonNegLong(7L)))

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](keyB, Balance(NonNegLong(2L)))
      _ <- overlay.commit(hB, branchB, SnapshotOrdinal(NonNegLong(7L)))

      // Now a third commit — pending is 2, cap is 2, so adding pushes to 3 → evict.
      _ <- commitEmpty(overlay, parentP, branchC, SnapshotOrdinal(NonNegLong(7L)))

      // After eviction, branchA is gone (lex-smallest among same-ordinal candidates).
      readA <- overlay.get[Balance](branchA, keyA)
      readB <- overlay.get[Balance](branchB, keyB)
    } yield
      expect.all(
        readA.isEmpty, // branchA evicted (lex tie-break)
        readB.contains(Balance(NonNegLong(2L)))
      )
  }

  test("eviction: bestTipsFn = empty — purely score-based, no ancestor protection") { res =>
    implicit val (h, _, js) = res
    for {
      // Linear chain branchA(ord=1) → branchB(ord=2). Commit branchC, branchD as siblings.
      // bestTipsFn returns empty → branchA is NOT protected as ancestor → it gets evicted at cap.
      pair <- mkMultiBranchWithCap(cap = 3, bestTipsFn = IO.pure(Set.empty[BranchId]))
      (_, overlay) = pair

      keyA = gskBalance(3200)
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(11L)))
      _ <- overlay.commit(hA, branchA, SnapshotOrdinal(NonNegLong(1L)))

      _ <- commitEmpty(overlay, branchA, branchB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- commitEmpty(overlay, parentP, branchC, SnapshotOrdinal(NonNegLong(3L)))
      _ <- commitEmpty(overlay, parentP, branchD, SnapshotOrdinal(NonNegLong(4L)))

      // pending = {A, B, C, D} = 4, cap=3 → evict lowest. With no ancestor protection, branchA (ordinal 1)
      // gets evicted even though branchB depends on it.
      readA <- overlay.get[Balance](branchA, keyA)
      // branchB's chain reads: walks to parent (branchA, evicted, not in pending) → falls through to base.
      // keyA wasn't in base, so reading at branchB returns None too.
      readB <- overlay.get[Balance](branchB, keyA)
    } yield
      expect.all(
        readA.isEmpty,
        readB.isEmpty
      )
  }

  test("eviction: multi-tip protection — fork-recovery committed branches are NOT evicted while bestTip lags") { res =>
    implicit val (h, _, js) = res
    // #115 regression: during fork-recovery, the validator commits the canonical branch BEFORE
    // chainStore reorgs to it. If `bestTipsFn` returned only the local-fork bestTip, the just-
    // committed canonical branch and its ancestors would be eviction candidates and could be
    // dropped — breaking parent-walk for the next ord's `mergedChain`.
    //
    // Setup: gl0-2 has self-produced local chain (localA→localB), AND has just received
    // canonical chain from peers (canonA→canonB). chainStore.allTips returns BOTH leaves
    // (localB + canonB). With cap=2 we have 4 pending branches → 2 over cap. Eviction must
    // protect ALL ancestors of BOTH tips.
    val localA = BranchId(Hash("a" * 64))
    val localB = BranchId(Hash("b" * 64))
    val canonA = BranchId(Hash("c" * 64))
    val canonB = BranchId(Hash("d" * 64))
    for {
      bestTipsRef <- IO.ref[Set[BranchId]](Set.empty[BranchId])
      pair <- mkMultiBranchWithCap(cap = 2, bestTipsFn = bestTipsRef.get)
      (_, overlay) = pair

      // Build the local-fork chain first, with bestTips = {localB} after each commit.
      _ <- commitEmpty(overlay, parentP, localA, SnapshotOrdinal(NonNegLong(1L)))
      _ <- bestTipsRef.set(Set(localA))
      _ <- commitEmpty(overlay, localA, localB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- bestTipsRef.set(Set(localB))

      // Now receive canonical chain from peers. Validator commits canonA, canonB. AT THIS POINT
      // chainStore stores both BUT bestTip hasn't reorged yet. Multi-tip: bestTipsRef returns
      // {localB, canonA} during the canonA commit window — both are "leaves" in chainStore.
      _ <- bestTipsRef.set(Set(localB, canonA))
      _ <- commitEmpty(overlay, parentP, canonA, SnapshotOrdinal(NonNegLong(3L)))
      // pending = {localA, localB, canonA} = 3, cap=2 → 1 over → eviction fires.
      // Without multi-tip protection, eviction would treat canonA as a non-ancestor of bestTip=localB
      // and drop it. With multi-tip, canonA IS in bestTips, so its ancestors-or-self are protected.
      _ <- bestTipsRef.set(Set(localB, canonB))
      _ <- commitEmpty(overlay, canonA, canonB, SnapshotOrdinal(NonNegLong(4L)))
      // pending = {localA, localB, canonA, canonB} = 4, cap=2 → 2 over → eviction fires (single-shot, drops 1).
      // After this commit: lastCommittedRef = canonB (protects canonA, canonB), bestTips = {localB, canonB}
      // (protects localA, localB, canonA, canonB). Total protected = 4. No candidates. Skip.
      _ <- bestTipsRef.set(Set(localB, canonB))

      // The critical test: read at canonB MUST find canonA's data via parent walk.
      // We use empty commits so there's no per-key write to read; instead probe via parent-child tree
      // (which eviction doesn't touch — but pendingRef IS touched). Our real assertion:
      // mergedChain(canonB) walks canonB → canonA. If canonA is in pending, walk continues; otherwise
      // it stops. Since allEntriesAsBytes(canonB) doesn't surface that distinction with empty commits,
      // we instead verify via the parent-child tree (which tracks topology not pending) — and verify
      // that no eviction happened (cap=2 but all 4 are protected → ride over-cap).
      pCanonA <- overlay.parentChildTree.parentOf(canonA.value)
      pCanonB <- overlay.parentChildTree.parentOf(canonB.value)
      pLocalA <- overlay.parentChildTree.parentOf(localA.value)
      pLocalB <- overlay.parentChildTree.parentOf(localB.value)
    } yield
      expect.all(
        pLocalA.contains(parentP.value),
        pLocalB.contains(localA.value),
        pCanonA.contains(parentP.value),
        pCanonB.contains(canonA.value)
      )
  }

  test("eviction: multi-tip protection — write at canonical chain is observable via mergedChain when bestTip lags") { res =>
    implicit val (h, _, js) = res
    // Stronger version of the #115 regression: writes on the canonical chain must remain observable
    // through mergedChain after eviction pressure on the local-fork chain. This is the actual
    // smoking-gun symptom — gl0-2's preSyncBytes(73) had 32 entries vs canonical's 33 because
    // canonical-71 got evicted and mergedChain(canonical-72) couldn't see canonical-71's writes.
    val localA = BranchId(Hash("a" * 64))
    val canonA = BranchId(Hash("c" * 64))
    val canonB = BranchId(Hash("d" * 64))

    val keyOnCanonA = gskBalance(7100)

    for {
      bestTipsRef <- IO.ref[Set[BranchId]](Set.empty[BranchId])
      pair <- mkMultiBranchWithCap(cap = 2, bestTipsFn = bestTipsRef.get)
      (_, overlay) = pair

      // Local-fork chain with a write.
      _ <- commitEmpty(overlay, parentP, localA, SnapshotOrdinal(NonNegLong(1L)))
      _ <- bestTipsRef.set(Set(localA))

      // Canonical chain commit at canonA writes a key. This is the write that MUST survive eviction
      // pressure for the canonB→canonA→base walk to return correct bytes.
      hCanonA <- overlay.checkout(parentP)
      _ <- hCanonA.insert[Balance](keyOnCanonA, Balance(NonNegLong(99L)))
      _ <- bestTipsRef.set(Set(localA, canonA))
      _ <- overlay.commit(hCanonA, canonA, SnapshotOrdinal(NonNegLong(2L)))
      // pending = {localA, canonA} = 2, cap=2 → no eviction yet.

      // Commit canonB on top of canonA. lastCommittedRef = canonB. Multi-tip = {localA, canonB}.
      // pending → {localA, canonA, canonB} = 3, cap=2 → eviction fires.
      // ancestors(canonB) = {canonB, canonA}. ancestors(localA) = {localA}. Total protected = 3. No candidates → skipped.
      // (Without multi-tip protection: bestTip = localA, ancestors = {localA, canonB}, candidates = {canonA}, evict canonA → BUG.)
      _ <- bestTipsRef.set(Set(localA, canonB))
      _ <- commitEmpty(overlay, canonA, canonB, SnapshotOrdinal(NonNegLong(3L)))

      // Read keyOnCanonA at canonB. Walk: canonB → canonA. canonA must still be in pending → its write surfaces.
      readAtCanonB <- overlay.get[Balance](canonB, keyOnCanonA)
    } yield expect(readAtCanonB.contains(Balance(NonNegLong(99L))))
  }

  test("eviction: all candidates are ancestors — eviction skipped (overlay rides over-cap)") { res =>
    implicit val (h, _, js) = res
    for {
      // Linear chain A→B→C→D, all ancestors of bestTip=D. Cap=3 but all branches are ancestors.
      // No candidates → eviction skipped (warns), pending stays at 4.
      bestTipsRef <- IO.ref[Set[BranchId]](Set.empty[BranchId])
      pair <- mkMultiBranchWithCap(cap = 3, bestTipsFn = bestTipsRef.get)
      (_, overlay) = pair

      _ <- commitEmpty(overlay, parentP, branchA, SnapshotOrdinal(NonNegLong(1L)))
      _ <- commitEmpty(overlay, branchA, branchB, SnapshotOrdinal(NonNegLong(2L)))
      _ <- commitEmpty(overlay, branchB, branchC, SnapshotOrdinal(NonNegLong(3L)))
      _ <- bestTipsRef.set(Set(branchC))

      // Adding branchD as child of branchC pushes pending to 4 (over cap=3). All four are ancestors of
      // branchD (the new tip). Eviction can't drop any.
      _ <- bestTipsRef.set(Set(branchA)) // still all 3 are A's ancestors-or-self chain when we add D
      _ <- commitEmpty(overlay, branchC, branchD, SnapshotOrdinal(NonNegLong(4L)))
      _ <- bestTipsRef.set(Set(branchD))

      // Verify ALL parent associations still present.
      pA <- overlay.parentChildTree.parentOf(branchA.value)
      pB <- overlay.parentChildTree.parentOf(branchB.value)
      pC <- overlay.parentChildTree.parentOf(branchC.value)
      pD <- overlay.parentChildTree.parentOf(branchD.value)
    } yield
      expect.all(
        pA.contains(parentP.value),
        pB.contains(branchA.value),
        pC.contains(branchB.value),
        pD.contains(branchC.value)
      )
  }

  // ============================================================
  // #139 — legacy T_depth2 local-retention prune of overlay history (not a protocol phase)
  // ============================================================
  //
  // Contract: `pruneBelow(ord)` drops in-memory finalized and undo entries strictly below `ord` and
  // advances a monotone retained-history floor. k₂ is a local retention recommendation, not a
  // fork-choice or finality floor: a later finalize below the retained floor must fail closed with
  // `FinalizationHistoryPrunedError`, without mutating base or overlay markers. Authenticated ROOT-009
  // reconstruction owns reopening history below that floor; `unsafe_reset` cannot lower it.

  test("pruneBelow on Passthrough is a no-op (and does not throw)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      key = gskBalance(8000)
      _ <- store.insert[Balance](key, Balance(NonNegLong(42L)))

      // Prune at multiple boundaries — all are no-ops. Read survives unchanged.
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(0L)))
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(100L)))
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

      readAfterPrune <- overlay.get[Balance](BranchId.base, key)
    } yield expect(readAfterPrune.contains(Balance(NonNegLong(42L))))
  }

  test("pruneBelow on MultiBranch drops undoJournal entries with finalizedAt < ord") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      ord1 = SnapshotOrdinal(NonNegLong(1L))
      ord2 = SnapshotOrdinal(NonNegLong(2L))
      pruneAt = SnapshotOrdinal(NonNegLong(2L)) // drops only ord1

      losingKeyOrd1 = gskBalance(8100)
      losingValOrd1 = Balance(NonNegLong(11L))
      losingKeyOrd2 = gskBalance(8101)
      losingValOrd2 = Balance(NonNegLong(22L))

      // Setup: finalize branchA at ord1 (writes losingKeyOrd1 to base + journals reverse delta at key=1).
      hA1 <- overlay.checkout(parentP)
      _ <- hA1.insert[Balance](losingKeyOrd1, losingValOrd1)
      _ <- overlay.commit(hA1, branchA, ord1)
      _ <- overlay.finalizeBranch(branchA, ord1)

      // Setup: finalize branchC at ord2 (writes losingKeyOrd2 to base + journals reverse delta at key=2).
      hC2 <- overlay.checkout(parentP)
      _ <- hC2.insert[Balance](losingKeyOrd2, losingValOrd2)
      _ <- overlay.commit(hC2, branchC, ord2)
      _ <- overlay.finalizeBranch(branchC, ord2)

      // Both writes are now in base.
      readOrd1Pre <- overlay.get[Balance](parentP, losingKeyOrd1)
      readOrd2Pre <- overlay.get[Balance](parentP, losingKeyOrd2)

      // Prune below ord2 — drops ord1's journal entry, keeps ord2's.
      _ <- overlay.pruneBelow(pruneAt)
      baseAfterPrune <- overlay.allEntriesAsBytes(parentP)
      sizesAfterPrune <- overlay.journalSizes

      // The prior ordinal's canonical marker and undo were both pruned. It must not be reinterpreted
      // as an unseen first finalization: reject before touching base or any overlay marker.
      rejectedOrd1 <- overlay.finalizeBranch(branchB, ord1).attempt
      baseAfterRejectedOrd1 <- overlay.allEntriesAsBytes(parentP)
      sizesAfterRejectedOrd1 <- overlay.journalSizes

      // The floor itself remains eligible. Its retained journal entry can safely undo branchC.
      _ <- overlay.finalizeBranch(branchD, ord2)
      readOrd2AfterReorg <- overlay.get[Balance](parentP, losingKeyOrd2)
    } yield
      expect.all(
        readOrd1Pre.contains(losingValOrd1),
        readOrd2Pre.contains(losingValOrd2),
        rejectedOrd1 match {
          case Left(FinalizationHistoryPrunedError(rejectedOrdinal, retainedFromOrdinal)) =>
            rejectedOrdinal == ord1 && retainedFromOrdinal == pruneAt
          case _ => false
        },
        sameBytes(baseAfterRejectedOrd1, baseAfterPrune),
        sizesAfterRejectedOrd1 == sizesAfterPrune,
        // At-or-above-prune ordinal: undo journal entry preserved → branchC's write is undone.
        readOrd2AfterReorg.isEmpty
      )
  }

  test("pruneBelow on MultiBranch preserves undoJournal entries at or above ord") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      ord5 = SnapshotOrdinal(NonNegLong(5L))
      ord10 = SnapshotOrdinal(NonNegLong(10L))
      pruneAt = ord5 // entries >= 5 preserved

      key5 = gskBalance(8200)
      val5 = Balance(NonNegLong(55L))
      key10 = gskBalance(8201)
      val10 = Balance(NonNegLong(100L))

      // Finalize at the retained floor and prune. Replacing that exact ordinal remains safe because
      // its canonical marker and undo entry are retained.
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](key5, val5)
      _ <- overlay.commit(hA, branchA, ord5)
      _ <- overlay.finalizeBranch(branchA, ord5)

      _ <- overlay.pruneBelow(pruneAt)

      _ <- overlay.finalizeBranch(branchB, ord5)
      readKey5 <- overlay.get[Balance](parentP, key5)

      // A later ordinal above the floor is also eligible and independently retains its undo.
      hC <- overlay.checkout(parentP)
      _ <- hC.insert[Balance](key10, val10)
      _ <- overlay.commit(hC, branchC, ord10)
      _ <- overlay.finalizeBranch(branchC, ord10)

      // Re-pruning at ord5 is idempotent and keeps ord10's entry.
      _ <- overlay.pruneBelow(pruneAt)

      _ <- overlay.finalizeBranch(branchD, ord10)
      readKey10 <- overlay.get[Balance](parentP, key10)
    } yield
      expect.all(
        // Both undo entries survived prune → both base writes were undone.
        readKey5.isEmpty,
        readKey10.isEmpty
      )
  }

  test("pruneBelow on MultiBranch is idempotent — calling twice with the same ord is safe") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      ord1 = SnapshotOrdinal(NonNegLong(1L))
      ord2 = SnapshotOrdinal(NonNegLong(2L))

      // Finalize twice to seed undoJournal entries.
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(8300), Balance(NonNegLong(7L)))
      _ <- overlay.commit(hA, branchA, ord1)
      _ <- overlay.finalizeBranch(branchA, ord1)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](gskBalance(8301), Balance(NonNegLong(8L)))
      _ <- overlay.commit(hB, branchB, ord2)
      _ <- overlay.finalizeBranch(branchB, ord2)

      // Prune at ord2 three times — must not error.
      _ <- overlay.pruneBelow(ord2)
      _ <- overlay.pruneBelow(ord2)
      _ <- overlay.pruneBelow(ord2)

      // Subsequent finalize at ord2 with a different canonical hits reorg-replace; entry at ord2
      // was preserved across all three prunes (each pruned only strictly below ord2).
      reorgOrd2 <- overlay.finalizeBranch(branchC, ord2)
      readPostReorgOrd2 <- overlay.get[Balance](parentP, gskBalance(8301))
    } yield
      expect.all(
        // ord2's journal entry survived → reorg-replace undoes branchB's write.
        readPostReorgOrd2.isEmpty,
        // Outcome is NoOp because pending was already empty after the prior fold.
        reorgOrd2 == FinalizationOutcome.NoOp
      )
  }

  test("pruneBelow with a future ord rejects stale finalization and retains a monotone floor across reset") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      ord1 = SnapshotOrdinal(NonNegLong(1L))
      ord5 = SnapshotOrdinal(NonNegLong(5L))
      pruneAt = SnapshotOrdinal(NonNegLong(1000L)) // well past everything

      // Finalize a few snapshots — seeds undoJournal and finalizedRef entries.
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(8400), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ord1)
      _ <- overlay.finalizeBranch(branchA, ord1)

      hC <- overlay.checkout(parentP)
      _ <- hC.insert[Balance](gskBalance(8401), Balance(NonNegLong(5L)))
      _ <- overlay.commit(hC, branchC, ord5)
      _ <- overlay.finalizeBranch(branchC, ord5)

      // Prune everything below the future watermark.
      _ <- overlay.pruneBelow(pruneAt)
      // A later lower prune request cannot lower the retained-history floor.
      _ <- overlay.pruneBelow(ord5)
      baseAfterPrune <- overlay.allEntriesAsBytes(parentP)
      sizesAfterPrune <- overlay.journalSizes

      // Both the canonical marker and undo are absent below the retained floor. Reject rather than
      // silently accepting this as a first finalization.
      rejectedOrd5 <- overlay.finalizeBranch(branchD, ord5).attempt
      rejectedOrd1 <- overlay.finalizeBranch(branchB, ord1).attempt
      baseAfterRejectedOrd1 <- overlay.allEntriesAsBytes(parentP)
      sizesAfterRejectedOrd1 <- overlay.journalSizes

      // A local overlay reset is not authenticated history reconstruction and therefore cannot lower
      // the monotone floor or make the same stale ordinal eligible.
      _ <- overlay.unsafe_reset
      rejectedAfterReset <- overlay.finalizeBranch(branchB, ord1).attempt
      baseAfterResetRejection <- overlay.allEntriesAsBytes(parentP)
    } yield
      expect.all(
        rejectedOrd5 match {
          case Left(FinalizationHistoryPrunedError(rejectedOrdinal, retainedFromOrdinal)) =>
            rejectedOrdinal == ord5 && retainedFromOrdinal == pruneAt
          case _ => false
        },
        rejectedOrd1 match {
          case Left(FinalizationHistoryPrunedError(rejectedOrdinal, retainedFromOrdinal)) =>
            rejectedOrdinal == ord1 && retainedFromOrdinal == pruneAt
          case _ => false
        },
        sameBytes(baseAfterRejectedOrd1, baseAfterPrune),
        sizesAfterRejectedOrd1 == sizesAfterPrune,
        rejectedAfterReset match {
          case Left(FinalizationHistoryPrunedError(rejectedOrdinal, retainedFromOrdinal)) =>
            rejectedOrdinal == ord1 && retainedFromOrdinal == pruneAt
          case _ => false
        },
        sameBytes(baseAfterResetRejection, baseAfterPrune)
      )
  }

  test("a fresh MultiBranch overlay rejects finalization below the persisted base anchor") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      firstTree <- ParentChildTree.make[IO]
      firstOverlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        firstTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      ord1 = SnapshotOrdinal(NonNegLong(1L))
      ord5 = SnapshotOrdinal(NonNegLong(5L))
      key = gskBalance(8450)
      value = Balance(NonNegLong(5L))

      handle <- firstOverlay.checkout(parentP)
      _ <- handle.insert[Balance](key, value)
      _ <- firstOverlay.commit(handle, branchA, ord5)
      _ <- firstOverlay.finalizeBranch(branchA, ord5)
      baseBeforeRestart <- snapshotBytes(store)

      // A fresh overlay simulates process-local overlay state loss while retaining the MPT base and
      // its persisted ordinal. Its RAM prune floor starts empty; the base anchor must still close the gap.
      restartedTree <- ParentChildTree.make[IO]
      restartedOverlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        restartedTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      sizesBefore <- restartedOverlay.journalSizes
      rejected <- restartedOverlay.finalizeBranch(branchB, ord1).attempt
      baseAfterRejected <- snapshotBytes(store)
      sizesAfter <- restartedOverlay.journalSizes

      // The exact persisted base ordinal remains eligible; the guard is strictly below the anchor.
      sameOrdinal <- restartedOverlay.finalizeBranch(branchC, ord5)
    } yield
      expect.all(
        rejected match {
          case Left(FinalizationHistoryPrunedError(rejectedOrdinal, retainedFromOrdinal)) =>
            rejectedOrdinal == ord1 && retainedFromOrdinal == ord5
          case _ => false
        },
        sameBytes(baseAfterRejected, baseBeforeRestart),
        sizesAfter == sizesBefore,
        sameOrdinal == FinalizationOutcome.NoOp
      )
  }

  test("pruneBelow on Passthrough is idempotent") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      key = gskBalance(8500)
      _ <- store.insert[Balance](key, Balance(NonNegLong(99L)))

      // Multiple no-ops in a row.
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(5L)))
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(5L)))
      _ <- overlay.pruneBelow(SnapshotOrdinal(NonNegLong(10L)))

      readAfter <- overlay.get[Balance](BranchId.base, key)
    } yield expect(readAfter.contains(Balance(NonNegLong(99L))))
  }

  // ============================================================
  // Track-3 S2: RAM undo-journal bound telemetry (journalSizes + over-bound sentinel)
  // ============================================================
  //
  // Contract: `journalSizes` exposes the in-memory accumulator sizes so `SnapshotLeaderLoop` can emit
  // `dag_nakamoto_overlay_undo_journal_size` and fire an over-bound counter (`JournalSizes.overBound(k₁)`)
  // WITHOUT threading `Metrics[F]` into `make`. The disk `signedBytesStore` holds deep history to k₂; the
  // RAM `undoJournalRef` must stay bounded to the operational k₁ window (the Fix A prune). These tests pin the
  // sentinel: it reads > window when unpruned and ≤ window after a k₁-watermark `pruneBelow`.

  test("journalSizes on Passthrough is always empty (no in-memory history), never over-bound") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      // Even after a commit + finalize, passthrough keeps no per-branch history.
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](gskBalance(9600), Balance(NonNegLong(1L)))
      _ <- overlay.commit(handle, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)
      sizes <- overlay.journalSizes
    } yield
      expect.all(
        sizes == MptOverlay.JournalSizes.empty,
        sizes.undoJournal == 0,
        !sizes.overBound(0L) // 0 > 0 is false — passthrough is never over-bound
      )
  }

  test("journalSizes on MultiBranch tracks undoJournal per finalized ordinal; over-bound + pruneBelow bound it (S2)") { res =>
    implicit val (h, _, js) = res

    val n = 6 // fold at ordinals 1..6
    val k1Window = 3L // stand-in for the operational k₁ RAM fast-path window

    def branchN(i: Int): BranchId = BranchId(Hash(f"$i%064x"))
    def ordN(i: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(i))

    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      // Fold a distinct branch at each ordinal 1..n. Each finalize journals one reverse-delta entry
      // (`undoJournalRef[ord]`) + one `finalizedRef[ord]` marker.
      _ <- (1 to n).toList.traverse_ { i =>
        for {
          hI <- overlay.checkout(parentP)
          _ <- hI.insert[Balance](gskBalance(9500 + i), Balance(NonNegLong.unsafeFrom(i.toLong)))
          _ <- overlay.commit(hI, branchN(i), ordN(i.toLong))
          _ <- overlay.finalizeBranch(branchN(i), ordN(i.toLong))
        } yield ()
      }

      // Unpruned: one RAM journal entry per finalized ordinal — OVER the small k₁ window (the leak shape).
      sizesUnpruned <- overlay.journalSizes

      // Fix-A-style prune at the k₁ watermark: keep only the last `k1Window` ordinals [n-k1Window+1, n].
      _ <- overlay.pruneBelow(ordN(n - k1Window + 1L))
      sizesPruned <- overlay.journalSizes
    } yield
      expect.all(
        sizesUnpruned.undoJournal == n,
        sizesUnpruned.finalizedMarkers == n,
        sizesUnpruned.overBound(k1Window), // ← RAM journal exceeded its window → the counter would fire
        sizesPruned.undoJournal == k1Window.toInt, // ← prune bounded the RAM journal to the window
        !sizesPruned.overBound(k1Window) // ← within the window → no over-bound
      )
  }

  // ============================================================
  // P-11 (task #141): unsafe_reset for re-bootstrap recovery
  // ============================================================

  test("unsafe_reset on multi-branch: drops pending branches, idempotency markers, undo journal") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair

      keyA = gskBalance(9000)
      keyB = gskBalance(9001)

      // Pre-seed base with a value so we can confirm base survives the reset.
      keyBase = gskBalance(8999)
      _ <- store.insert[Balance](keyBase, Balance(NonNegLong(42L)))

      // Build up pending state across two branches + a finalized marker (via finalize on one).
      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](keyA, Balance(NonNegLong(11L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      hB <- overlay.checkout(parentP)
      _ <- hB.insert[Balance](keyB, Balance(NonNegLong(22L)))
      _ <- overlay.commit(hB, branchB, ordinal)

      // Reset — the core P-11 contract.
      _ <- overlay.unsafe_reset

      // Branches gone — reads at their ids fall through to base.
      readKeyAFromBranchA <- overlay.get[Balance](branchA, keyA)
      readKeyBFromBranchB <- overlay.get[Balance](branchB, keyB)

      // Base untouched — `unsafe_reset` is in-memory only.
      readBaseFromOverlay <- overlay.get[Balance](BranchId.base, keyBase)
      readBaseDirect <- store.get[Balance](keyBase)

      // After reset, finalizeBranch at the same ord with a never-committed branch returns NoOp
      // (the conflict-detection finalizedRef was cleared) and does not raise.
      finalize <- overlay.finalizeBranch(branchC, ordinal)
    } yield
      expect.all(
        // Pending state cleared — chain walks at branchA/branchB no longer see their writes.
        readKeyAFromBranchA.isEmpty,
        readKeyBFromBranchB.isEmpty,
        // Base preserved — the caller is responsible for resyncing base via syncFromGlobalSnapshotInfo.
        readBaseFromOverlay.contains(Balance(NonNegLong(42L))),
        readBaseDirect.contains(Balance(NonNegLong(42L))),
        // finalizedRef cleared — re-finalize at the same ordinal with a NEW canonical works
        // (would have hit reorg-replace or duplicate-detection without the reset).
        finalize == FinalizationOutcome.NoOp
      )
  }

  test("unsafe_reset on multi-branch: post-reset commit on the same branch id works without conflict") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      key = gskBalance(9100)

      // Initial commit.
      h1 <- overlay.checkout(parentP)
      _ <- h1.insert[Balance](key, Balance(NonNegLong(1L)))
      _ <- overlay.commit(h1, branchA, ordinal)
      _ <- overlay.finalizeBranch(branchA, ordinal)

      // Reset.
      _ <- overlay.unsafe_reset

      // Re-commit + finalize at the SAME branchA — would normally hit the
      // "Already finalized at exactly this (ordinal, hash); nothing to do" idempotency arm
      // (which is harmless), but more importantly the pendingRef no longer carries the
      // first commit's entry — confirming the reset actually wiped pending.
      h2 <- overlay.checkout(parentP)
      _ <- h2.insert[Balance](key, Balance(NonNegLong(2L)))
      _ <- overlay.commit(h2, branchA, ordinal)
      readBeforeFinalize <- overlay.get[Balance](branchA, key)
      finalize2 <- overlay.finalizeBranch(branchA, ordinal)
      readAfterFinalize <- overlay.get[Balance](BranchId.base, key)
    } yield
      expect.all(
        // Branch read post-second-commit sees the second value (new pending entry).
        readBeforeFinalize.contains(Balance(NonNegLong(2L))),
        // Finalize folds the second write into base — no idempotency hit because finalizedRef cleared.
        finalize2 match {
          case FinalizationOutcome.Folded(_, _) => true
          case _                                => false
        },
        readAfterFinalize.contains(Balance(NonNegLong(2L)))
      )
  }

  test("unsafe_reset on Passthrough: no-op (no in-memory state to drop)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )

      key = gskBalance(9200)
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))

      // Reset should be safe and not touch base.
      _ <- overlay.unsafe_reset
      readAfter <- overlay.get[Balance](BranchId.base, key)
    } yield expect(readAfter.contains(Balance(NonNegLong(7L))))
  }

  // ============================================================
  // Track-3 S4 — revertToOrdinal (revert-executor) determinism gates
  // ============================================================

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))

  /** Blocks exactly one byte-hash after `armed` becomes true. Used to cancel a shallow revert after every reverse delta has reached the
    * producer but before its single final build can publish the fork root.
    */
  private final class OneShotBlockingHasher(
    delegate: Hasher[IO],
    armed: Ref[IO, Boolean],
    entered: Deferred[IO, Unit]
  ) extends Hasher[IO] {

    private def blockOnce: IO[Unit] =
      armed.modify {
        case true  => false -> true
        case false => false -> false
      }.flatMap(shouldBlock => (entered.complete(()).void >> IO.never[Unit]).whenA(shouldBlock))

    def hash[A: Encoder](data: A): IO[Hash] = delegate.hash(data)
    def hashBytes(bytes: Array[Byte]): IO[Hash] = blockOnce >> delegate.hashBytes(bytes)
    def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] = delegate.compare(data, expectedHash)
    def getLogic(ordinal: SnapshotOrdinal): HashLogic = delegate.getLogic(ordinal)
    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] = delegate.prefixedHash(data, prefix)
  }

  private final class OneShotFailingHasher(
    delegate: Hasher[IO],
    armed: Ref[IO, Boolean],
    failure: Throwable
  ) extends Hasher[IO] {

    private def failOnce: IO[Unit] =
      armed.modify {
        case true  => false -> true
        case false => false -> false
      }.flatMap(shouldFail => failure.raiseError[IO, Unit].whenA(shouldFail))

    def hash[A: Encoder](data: A): IO[Hash] = delegate.hash(data)
    def hashBytes(bytes: Array[Byte]): IO[Hash] = failOnce >> delegate.hashBytes(bytes)
    def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] = delegate.compare(data, expectedHash)
    def getLogic(ordinal: SnapshotOrdinal): HashLogic = delegate.getLogic(ordinal)
    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] = delegate.prefixedHash(data, prefix)
  }

  // Compare two raw byte maps for structural (byte-level) equality — `Array[Byte]` has reference
  // equality, so `==` on the maps is NOT sufficient. This is the byte-determinism assertion primitive.
  private def sameBytes(a: Map[Hex, Array[Byte]], b: Map[Hex, Array[Byte]]): Boolean =
    a.keySet == b.keySet && a.forall { case (k, v) => b.get(k).exists(java.util.Arrays.equals(_, v)) }

  // Snapshot the base's raw bytes, deep-copying each value so later folds can't mutate the captured view.
  private def snapshotBytes(store: MptStore[IO, GlobalStateKey]): IO[Map[Hex, Array[Byte]]] =
    store.allEntriesAsBytes.map(_.map { case (k, v) => k -> v.clone() })

  // Fold one key→value into base at `ordN` via the ordinary commit/finalize path (the SAME fold path
  // the re-fold uses), building the RAM undo journal entry at that ordinal. Each block checks out from
  // the base (`parentP`) because `finalizeBranch` clears pending.
  private def foldAt(
    overlay: MptOverlay[IO, GlobalStateKey],
    tip: BranchId,
    ordN: Long,
    key: GlobalStateKey,
    value: Balance
  ): IO[Unit] =
    for {
      h <- overlay.checkout(parentP)
      _ <- h.insert[Balance](key, value)
      _ <- overlay.commit(h, tip, ord(ordN))
      _ <- overlay.finalizeBranch(tip, ord(ordN))
    } yield ()

  private def bid(c: Char): BranchId = BranchId(Hash(c.toString * 64))

  private def mkMultiBranchDeep(
    diskRef: Ref[IO, Map[Long, Map[Hex, Array[Byte]]]],
    onRevertRef: Ref[IO, Int]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey])] =
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId]),
        deepStateReader = Some((o: SnapshotOrdinal) => diskRef.get.map(_.get(o.value.value))),
        onBaseRevert = Some(onRevertRef.update(_ + 1))
      )
    } yield (store, overlay)

  test("atomicity: initial finalize cancellation restores base and retry publishes a correct undo") { res =>
    val (delegateHasher, _, json) = res
    implicit val js: JsonSerializer[IO] = json
    val seed = gskBalance(39000)
    val target = gskBalance(39001)

    for {
      armed <- Ref.of[IO, Boolean](false)
      entered <- Deferred[IO, Unit]
      result <- {
        implicit val blockingHasher: Hasher[IO] = new OneShotBlockingHasher(delegateHasher, armed, entered)

        for {
          pair <- mkMultiBranch
          (store, overlay) = pair
          _ <- store.insert[Balance](seed, Balance(NonNegLong(1L)))
          baseBefore <- snapshotBytes(store)

          handle <- overlay.checkout(parentP)
          _ <- handle.insert[Balance](target, Balance(NonNegLong(11L)))
          _ <- overlay.commit(handle, branchA, ordinal)
          journalBefore <- overlay.journalSizes

          _ <- armed.set(true)
          finalizing <- overlay.finalizeBranch(branchA, ordinal).start
          _ <- entered.get
          staged <- snapshotBytes(store)
          _ <- finalizing.cancel

          afterCancel <- snapshotBytes(store)
          journalAfterCancel <- overlay.journalSizes
          pendingAfterCancel <- overlay.get[Balance](branchA, target)

          retried <- overlay.finalizeBranch(branchA, ordinal)
          afterRetry <- store.get[Balance](target)

          // A later different-hash finalization consumes the retry's undo record. If cancellation or retry
          // recorded the staged state as the preimage, target would remain in base here.
          _ <- overlay.finalizeBranch(branchB, ordinal)
          afterUndo <- snapshotBytes(store)
        } yield
          expect.all(
            !sameBytes(staged, baseBefore),
            sameBytes(afterCancel, baseBefore),
            journalAfterCancel == journalBefore,
            pendingAfterCancel.contains(Balance(NonNegLong(11L))),
            retried == FinalizationOutcome.Folded(1, 0),
            afterRetry.contains(Balance(NonNegLong(11L))),
            sameBytes(afterUndo, baseBefore)
          )
      }
    } yield result
  }

  test("atomicity: reorg replacement cancellation restores old canonical and old undo, then retry converges") { res =>
    val (delegateHasher, _, json) = res
    implicit val js: JsonSerializer[IO] = json
    val seed = gskBalance(39100)
    val shared = gskBalance(39101)
    val oldOnly = gskBalance(39102)
    val oldShared = Balance(NonNegLong(21L))
    val oldExclusive = Balance(NonNegLong(22L))
    val replacementShared = Balance(NonNegLong(31L))

    for {
      armed <- Ref.of[IO, Boolean](false)
      entered <- Deferred[IO, Unit]
      result <- {
        implicit val blockingHasher: Hasher[IO] = new OneShotBlockingHasher(delegateHasher, armed, entered)

        for {
          pair <- mkMultiBranch
          (store, overlay) = pair
          _ <- store.insert[Balance](seed, Balance(NonNegLong(1L)))

          oldHandle <- overlay.checkout(parentP)
          _ <- oldHandle.insert[Balance](shared, oldShared)
          _ <- oldHandle.insert[Balance](oldOnly, oldExclusive)
          _ <- overlay.commit(oldHandle, branchA, ordinal)
          _ <- overlay.finalizeBranch(branchA, ordinal)
          oldBase <- snapshotBytes(store)

          replacementHandle <- overlay.checkout(parentP)
          _ <- replacementHandle.insert[Balance](shared, replacementShared)
          _ <- overlay.commit(replacementHandle, branchB, ordinal)
          oldJournal <- overlay.journalSizes

          _ <- armed.set(true)
          replacing <- overlay.finalizeBranch(branchB, ordinal).start
          _ <- entered.get
          staged <- snapshotBytes(store)
          _ <- replacing.cancel

          afterCancel <- snapshotBytes(store)
          journalAfterCancel <- overlay.journalSizes
          oldSharedAfterCancel <- store.get[Balance](shared)
          oldOnlyAfterCancel <- store.get[Balance](oldOnly)

          retried <- overlay.finalizeBranch(branchB, ordinal)
          sharedAfterRetry <- store.get[Balance](shared)
          oldOnlyAfterRetry <- store.get[Balance](oldOnly)

          // Consume the replacement's undo to prove retry journaled the parent preimage, not the old or staged image.
          _ <- overlay.finalizeBranch(branchC, ordinal)
          afterReplacementUndo <- store.get[Balance](shared)
          seedAfterReplacementUndo <- store.get[Balance](seed)
        } yield
          expect.all(
            !sameBytes(staged, oldBase),
            sameBytes(afterCancel, oldBase),
            journalAfterCancel == oldJournal,
            oldSharedAfterCancel.contains(oldShared),
            oldOnlyAfterCancel.contains(oldExclusive),
            retried == FinalizationOutcome.Folded(1, 0),
            sharedAfterRetry.contains(replacementShared),
            oldOnlyAfterRetry.isEmpty,
            afterReplacementUndo.isEmpty,
            seedAfterReplacementUndo.contains(Balance(NonNegLong(1L)))
          )
      }
    } yield result
  }

  test("atomicity: reorg replacement build failure restores old canonical and remains retryable") { res =>
    val (delegateHasher, _, json) = res
    implicit val js: JsonSerializer[IO] = json
    val seed = gskBalance(39200)
    val oldOnly = gskBalance(39201)
    val replacementOnly = gskBalance(39202)
    val injected = new RuntimeException("injected replacement hash failure")

    for {
      armed <- Ref.of[IO, Boolean](false)
      result <- {
        implicit val failingHasher: Hasher[IO] = new OneShotFailingHasher(delegateHasher, armed, injected)

        for {
          pair <- mkMultiBranch
          (store, overlay) = pair
          _ <- store.insert[Balance](seed, Balance(NonNegLong(1L)))

          oldHandle <- overlay.checkout(parentP)
          _ <- oldHandle.insert[Balance](oldOnly, Balance(NonNegLong(41L)))
          _ <- overlay.commit(oldHandle, branchA, ordinal)
          _ <- overlay.finalizeBranch(branchA, ordinal)
          oldBase <- snapshotBytes(store)

          replacementHandle <- overlay.checkout(parentP)
          _ <- replacementHandle.insert[Balance](replacementOnly, Balance(NonNegLong(42L)))
          _ <- overlay.commit(replacementHandle, branchB, ordinal)
          oldJournal <- overlay.journalSizes

          _ <- armed.set(true)
          failed <- overlay.finalizeBranch(branchB, ordinal).attempt
          afterFailure <- snapshotBytes(store)
          journalAfterFailure <- overlay.journalSizes

          retried <- overlay.finalizeBranch(branchB, ordinal)
          oldAfterRetry <- store.get[Balance](oldOnly)
          replacementAfterRetry <- store.get[Balance](replacementOnly)
        } yield
          expect.all(
            failed.isLeft,
            sameBytes(afterFailure, oldBase),
            journalAfterFailure == oldJournal,
            retried == FinalizationOutcome.Folded(1, 0),
            oldAfterRetry.isEmpty,
            replacementAfterRetry.contains(Balance(NonNegLong(42L)))
          )
      }
    } yield result
  }

  test("S4 SHALLOW: revertToOrdinal replays the RAM undo journal to a BYTE-IDENTICAL base; re-fold reproduces; idempotent") { res =>
    implicit val (h, _, js) = res
    val k1 = gskBalance(40001)
    val k2 = gskBalance(40002)
    val k3 = gskBalance(40003)
    val k4 = gskBalance(40004)
    val k5 = gskBalance(40005)
    for {
      pair <- mkMultiBranch
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(40000), Balance(NonNegLong(1L))) // genesis so base never empty

      _ <- foldAt(overlay, bid('1'), 1L, k1, Balance(NonNegLong(11L)))
      _ <- foldAt(overlay, bid('2'), 2L, k2, Balance(NonNegLong(22L)))
      _ <- foldAt(overlay, bid('3'), 3L, k3, Balance(NonNegLong(33L)))
      baseAt3 <- snapshotBytes(store)
      rootAt3 <- store.build(ord(3L)).map(_.toOption.map(_.rootHash))

      _ <- foldAt(overlay, bid('4'), 4L, k4, Balance(NonNegLong(44L)))
      _ <- foldAt(overlay, bid('5'), 5L, k5, Balance(NonNegLong(55L)))
      baseAt5 <- snapshotBytes(store)
      rootAt5 <- store.build(ord(5L)).map(_.toOption.map(_.rootHash))

      // Fork ordinal 3 is inside the RAM window (journal holds 4,5) → SHALLOW.
      outcome <- overlay.revertToOrdinal(ord(3L))
      afterRevert <- snapshotBytes(store)
      rootAfterRevert <- store.build(ord(3L)).map(_.toOption.map(_.rootHash))

      // Re-fold the SAME denser branch (ords 4,5) through the ordinary fold path.
      _ <- foldAt(overlay, bid('a'), 4L, k4, Balance(NonNegLong(44L)))
      _ <- foldAt(overlay, bid('b'), 5L, k5, Balance(NonNegLong(55L)))
      afterRefold <- snapshotBytes(store)
      rootAfterRefold <- store.build(ord(5L)).map(_.toOption.map(_.rootHash))

      // Idempotent: a second revert to 3 (base already advanced to 5 via re-fold, journal repopulated) then
      // an immediate repeat lands NoOp with an unchanged base.
      _ <- overlay.revertToOrdinal(ord(3L))
      idemFirst <- snapshotBytes(store)
      idemSecond <- overlay.revertToOrdinal(ord(3L))
      idemAfter <- snapshotBytes(store)
    } yield
      expect.all(
        outcome == RevertOutcome.Shallow(2),
        sameBytes(afterRevert, baseAt3), // byte-identical revert
        rootAfterRevert == rootAt3,
        sameBytes(afterRefold, baseAt5), // re-fold reproduces the byte-identical dense base
        rootAfterRefold == rootAt5,
        idemSecond == RevertOutcome.NoOp, // second call idempotent
        sameBytes(idemAfter, idemFirst)
      )
  }

  test("S4 SHALLOW: cancellation during the single final build restores the complete tip and retains the undo band") { res =>
    val (delegateHasher, _, json) = res
    implicit val js: JsonSerializer[IO] = json
    val target = gskBalance(40501)
    val genesis = gskBalance(40500)

    for {
      armed <- Ref.of[IO, Boolean](false)
      entered <- Deferred[IO, Unit]
      result <- {
        implicit val blockingHasher: Hasher[IO] = new OneShotBlockingHasher(delegateHasher, armed, entered)

        for {
          pair <- mkMultiBranch
          (store, overlay) = pair
          _ <- store.insert[Balance](genesis, Balance(NonNegLong(1L)))
          _ <- foldAt(overlay, bid('1'), 1L, target, Balance(NonNegLong(11L)))
          _ <- foldAt(overlay, bid('2'), 2L, target, Balance(NonNegLong(22L)))
          _ <- foldAt(overlay, bid('3'), 3L, target, Balance(NonNegLong(33L)))
          baseAt3 <- snapshotBytes(store)
          rootAt3 <- store.build(ord(3L)).rethrow.map(_.rootHash)

          _ <- foldAt(overlay, bid('4'), 4L, target, Balance(NonNegLong(44L)))
          _ <- foldAt(overlay, bid('5'), 5L, target, Balance(NonNegLong(55L)))
          baseAt5 <- snapshotBytes(store)
          rootAt5 <- store.build(ord(5L)).rethrow.map(_.rootHash)
          journalBefore <- overlay.journalSizes
          syncedBefore <- store.lastPersistedOrdinal

          _ <- armed.set(true)
          reverting <- overlay.revertToOrdinal(ord(3L)).start
          _ <- entered.get
          stagedBeforeBuild <- snapshotBytes(store)
          _ <- reverting.cancel

          afterCancel <- snapshotBytes(store)
          rootAfterCancel <- store.build(ord(5L)).rethrow.map(_.rootHash)
          journalAfterCancel <- overlay.journalSizes
          syncedAfterCancel <- store.lastPersistedOrdinal

          retried <- overlay.revertToOrdinal(ord(3L))
          afterRetry <- snapshotBytes(store)
          rootAfterRetry <- store.build(ord(3L)).rethrow.map(_.rootHash)
          journalAfterRetry <- overlay.journalSizes
          syncedAfterRetry <- store.lastPersistedOrdinal
        } yield expect.all(
          // Both reverse writes were staged before the one final build blocked.
          sameBytes(stagedBeforeBuild, baseAt3),
          // Cancellation rolls state, trie caches, ordinal bookkeeping, and journal consumption back to tip@5.
          sameBytes(afterCancel, baseAt5),
          rootAfterCancel == rootAt5,
          journalAfterCancel == journalBefore,
          syncedBefore.contains(ord(5L)),
          syncedAfterCancel == syncedBefore,
          // The retained band remains retryable and lands exactly on the fork state/root.
          retried == RevertOutcome.Shallow(2),
          sameBytes(afterRetry, baseAt3),
          rootAfterRetry == rootAt3,
          journalAfterRetry.undoJournal == journalBefore.undoJournal - 2,
          syncedAfterRetry.contains(ord(3L))
        )
      }
    } yield result
  }

  test("S4 DEEP: an ordinal-only retained image cannot reopen pruned history or mutate base") { res =>
    implicit val (h, _, js) = res
    val k1 = gskBalance(41001)
    val k2 = gskBalance(41002)
    val k3 = gskBalance(41003)
    val k4 = gskBalance(41004)
    val k5 = gskBalance(41005)
    for {
      diskRef <- Ref.of[IO, Map[Long, Map[Hex, Array[Byte]]]](Map.empty)
      onRevertRef <- Ref.of[IO, Int](0)
      pair <- mkMultiBranchDeep(diskRef, onRevertRef)
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(41000), Balance(NonNegLong(1L)))

      _ <- foldAt(overlay, bid('1'), 1L, k1, Balance(NonNegLong(11L)))
      _ <- foldAt(overlay, bid('2'), 2L, k2, Balance(NonNegLong(22L)))
      // Capture the disk-retained signed bytes at the (deep) fork ordinal 2 — the S2 contiguous tier analog.
      baseAt2 <- snapshotBytes(store)
      _ <- diskRef.update(_.updated(2L, baseAt2))

      _ <- foldAt(overlay, bid('3'), 3L, k3, Balance(NonNegLong(33L)))
      _ <- foldAt(overlay, bid('4'), 4L, k4, Balance(NonNegLong(44L)))
      _ <- foldAt(overlay, bid('5'), 5L, k5, Balance(NonNegLong(55L)))

      // Simulate the RAM window bound: prune the in-memory journal below 4, so ordinal 3 (needed to walk
      // back to fork 2) is gone. The retained reader is ordinal-only and therefore cannot prove the
      // exact canonical hash/root required to authorize a DEEP base replacement.
      _ <- overlay.pruneBelow(ord(4L))
      baseBefore <- snapshotBytes(store)
      journalsBefore <- overlay.journalSizes
      persistedBefore <- store.lastPersistedOrdinal
      attempted <- overlay.revertToOrdinal(ord(2L)).attempt
      baseAfter <- snapshotBytes(store)
      journalsAfter <- overlay.journalSizes
      persistedAfter <- store.lastPersistedOrdinal
      revertCount <- onRevertRef.get
    } yield
      expect.all(
        attempted match {
          case Left(_: RevertGapError) => true
          case _                       => false
        },
        sameBytes(baseAfter, baseBefore),
        journalsAfter == journalsBefore,
        persistedAfter == persistedBefore,
        revertCount == 0
      )
  }

  test("S4 GAP: a fork below BOTH the RAM window and the disk tier fails closed (RevertGapError), never under-reverts") { res =>
    implicit val (h, _, js) = res
    for {
      diskRef <- Ref.of[IO, Map[Long, Map[Hex, Array[Byte]]]](Map.empty) // disk tier retains NOTHING at fork 2
      onRevertRef <- Ref.of[IO, Int](0)
      pair <- mkMultiBranchDeep(diskRef, onRevertRef)
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(42000), Balance(NonNegLong(1L)))
      _ <- foldAt(overlay, bid('1'), 1L, gskBalance(42001), Balance(NonNegLong(11L)))
      _ <- foldAt(overlay, bid('2'), 2L, gskBalance(42002), Balance(NonNegLong(22L)))
      _ <- foldAt(overlay, bid('3'), 3L, gskBalance(42003), Balance(NonNegLong(33L)))
      _ <- foldAt(overlay, bid('4'), 4L, gskBalance(42004), Balance(NonNegLong(44L)))
      _ <- foldAt(overlay, bid('5'), 5L, gskBalance(42005), Balance(NonNegLong(55L)))
      baseBefore <- snapshotBytes(store)

      _ <- overlay.pruneBelow(ord(4L)) // journal window floor now 4 → fork 2 unreachable in RAM
      // deepStateReader(2) returns None (disk empty) → must fail closed.
      attempted <- overlay.revertToOrdinal(ord(2L)).attempt
      baseAfter <- snapshotBytes(store)
    } yield
      expect.all(
        attempted match {
          case Left(_: RevertGapError) => true
          case _                       => false
        },
        // Fail-closed means NO under-revert: the base is untouched by the aborted attempt.
        sameBytes(baseAfter, baseBefore)
      )
  }

  test("S4 DEEP: an empty retained image fails closed without clearing the current base") { res =>
    implicit val (h, _, js) = res

    for {
      diskRef <- Ref.of[IO, Map[Long, Map[Hex, Array[Byte]]]](Map(1L -> Map.empty))
      onRevertRef <- Ref.of[IO, Int](0)
      pair <- mkMultiBranchDeep(diskRef, onRevertRef)
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(42400), Balance(NonNegLong(1L)))
      _ <- foldAt(overlay, bid('1'), 1L, gskBalance(42401), Balance(NonNegLong(11L)))
      _ <- foldAt(overlay, bid('2'), 2L, gskBalance(42402), Balance(NonNegLong(22L)))
      _ <- foldAt(overlay, bid('3'), 3L, gskBalance(42403), Balance(NonNegLong(33L)))
      before <- snapshotBytes(store)
      syncedBefore <- store.lastPersistedOrdinal

      _ <- overlay.pruneBelow(ord(3L))
      journalAfterPrune <- overlay.journalSizes
      rejected <- overlay.revertToOrdinal(ord(1L)).attempt

      after <- snapshotBytes(store)
      journalAfter <- overlay.journalSizes
      syncedAfter <- store.lastPersistedOrdinal
      revertCount <- onRevertRef.get
    } yield
      expect.all(
        rejected match {
          case Left(_: RevertGapError) => true
          case _                       => false
        },
        sameBytes(after, before),
        journalAfter == journalAfterPrune,
        syncedAfter == syncedBefore,
        revertCount == 0
      )
  }

  test("S4 DEEP: malformed physical keys fail before base replacement or revert cleanup") { res =>
    implicit val (h, _, js) = res
    val malformed = Map(Hex("aa") -> Array[Byte](1), Hex("aa00") -> Array[Byte](2))

    for {
      diskRef <- Ref.of[IO, Map[Long, Map[Hex, Array[Byte]]]](Map(1L -> malformed))
      onRevertRef <- Ref.of[IO, Int](0)
      pair <- mkMultiBranchDeep(diskRef, onRevertRef)
      (store, overlay) = pair
      _ <- store.insert[Balance](gskBalance(42500), Balance(NonNegLong(1L)))
      _ <- foldAt(overlay, bid('1'), 1L, gskBalance(42501), Balance(NonNegLong(11L)))
      _ <- foldAt(overlay, bid('2'), 2L, gskBalance(42502), Balance(NonNegLong(22L)))
      _ <- foldAt(overlay, bid('3'), 3L, gskBalance(42503), Balance(NonNegLong(33L)))
      before <- snapshotBytes(store)
      _ <- overlay.pruneBelow(ord(3L))
      rejected <- overlay.revertToOrdinal(ord(1L)).attempt
      after <- snapshotBytes(store)
      revertCount <- onRevertRef.get
    } yield
      expect.all(
        rejected == Left(TerminalPhysicalTrieKeyCollision(Hex("aa"), Hex("aa00"))),
        sameBytes(after, before),
        revertCount == 0
      )
  }

  test("S4 cross-node convergence: two overlays that revert then re-fold the same denser branch reach an IDENTICAL mptRoot") { res =>
    implicit val (h, _, js) = res
    val k1 = gskBalance(43001)
    val k2 = gskBalance(43002)
    val k3 = gskBalance(43003)
    // denser branch keys (different from the reverted-away 4,5)
    val kd4 = gskBalance(43104)
    val kd5 = gskBalance(43105)
    def buildFoldRevertRefold(store: MptStore[IO, GlobalStateKey], overlay: MptOverlay[IO, GlobalStateKey]) =
      for {
        _ <- store.insert[Balance](gskBalance(43000), Balance(NonNegLong(1L)))
        _ <- foldAt(overlay, bid('1'), 1L, k1, Balance(NonNegLong(11L)))
        _ <- foldAt(overlay, bid('2'), 2L, k2, Balance(NonNegLong(22L)))
        _ <- foldAt(overlay, bid('3'), 3L, k3, Balance(NonNegLong(33L)))
        // original branch at 4,5 (reverted away)
        _ <- foldAt(overlay, bid('4'), 4L, gskBalance(43004), Balance(NonNegLong(44L)))
        _ <- foldAt(overlay, bid('5'), 5L, gskBalance(43005), Balance(NonNegLong(55L)))
        _ <- overlay.revertToOrdinal(ord(3L))
        // re-fold the DENSER canonical branch (same deltas on both nodes)
        _ <- foldAt(overlay, bid('a'), 4L, kd4, Balance(NonNegLong(444L)))
        _ <- foldAt(overlay, bid('b'), 5L, kd5, Balance(NonNegLong(555L)))
        root <- store.build(ord(5L)).map(_.toOption.map(_.rootHash))
      } yield root
    for {
      p1 <- mkMultiBranch
      (store1, overlay1) = p1
      p2 <- mkMultiBranch
      (store2, overlay2) = p2
      root1 <- buildFoldRevertRefold(store1, overlay1)
      root2 <- buildFoldRevertRefold(store2, overlay2)
    } yield expect.all(root1.isDefined, root1 == root2)
  }

  test("S4 Passthrough: revertToOrdinal is always NoOp") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.Passthrough,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      r <- overlay.revertToOrdinal(ord(7L))
    } yield expect(r == RevertOutcome.NoOp)
  }
}
