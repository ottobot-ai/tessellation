package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.effect.{IO, Resource}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for `MptOverlay.passthrough` (#56.2) and `MptOverlay.make(enabled=true)` multi-branch impl (#56.4).
  *
  * Passthrough section: prove the impl behaves identically to using `MptStore` directly.
  *
  * Multi-branch section: prove sibling isolation (the #56.4 acceptance gate), chain reads, prefix scans, `buildRoot` with deltas, and
  * `finalizeBranch` fold-forward + idempotency + conflict detection.
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

  test("MptOverlay.make: enabled=false routes to passthrough") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](enabled = false, store, pcTree, GlobalStateKey.toHex[IO])

      key = gskBalance(20)
      _ <- store.insert[Balance](key, Balance(NonNegLong(5L)))
      r <- overlay.get[Balance](branchA, key)
    } yield expect(r.contains(Balance(NonNegLong(5L))))
  }

  test("MptOverlay.make: enabled=true routes to multi-branch (writes do NOT go to underlying store before commit)") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](enabled = true, store, pcTree, GlobalStateKey.toHex[IO])

      key = gskBalance(21)
      handle <- overlay.checkout(parentP)
      _ <- handle.insert[Balance](key, Balance(NonNegLong(8L)))

      // Multi-branch isolates pre-commit writes — store sees nothing yet.
      directRead <- store.get[Balance](key)
    } yield expect(directRead.isEmpty)
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
      overlay <- MptOverlay.make[IO, GlobalStateKey](enabled = true, store, pcTree, GlobalStateKey.toHex[IO])
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

  test("multi-branch: finalizeBranch raises on conflict — different canonical at same ordinal") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      hA <- overlay.checkout(parentP)
      _ <- hA.insert[Balance](gskBalance(1200), Balance(NonNegLong(1L)))
      _ <- overlay.commit(hA, branchA, ordinal)

      _ <- overlay.finalizeBranch(branchA, ordinal)

      // After finalize, branchB is no longer in pending (cleared by previous finalize). But the conflict
      // contract is at the (ordinal, canonical) level: re-finalizing the SAME ordinal with a DIFFERENT
      // canonical must error, even if that other canonical isn't in pending.
      conflict <- overlay.finalizeBranch(branchB, ordinal).attempt
    } yield expect(conflict.isLeft)
  }

  test("multi-branch: finalizeBranch on an unknown branch is a NoOp (idempotency entry recorded)") { res =>
    implicit val (h, _, js) = res
    for {
      pair <- mkMultiBranch
      (_, overlay) = pair

      // No branchA in pending; finalize is a NoOp but records the (ordinal, branchA) finalization.
      r1 <- overlay.finalizeBranch(branchA, ordinal)
      // Re-finalizing same (ordinal, branchA) — still NoOp.
      r2 <- overlay.finalizeBranch(branchA, ordinal)
      // Re-finalizing same ordinal with DIFFERENT canonical now conflicts.
      conflict <- overlay.finalizeBranch(branchB, ordinal).attempt
    } yield
      expect.all(
        r1 == FinalizationOutcome.NoOp,
        r2 == FinalizationOutcome.NoOp,
        conflict.isLeft
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
}
