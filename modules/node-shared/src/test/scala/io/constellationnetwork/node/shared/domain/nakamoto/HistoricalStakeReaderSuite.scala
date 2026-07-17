package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.MptOverlay.OverlayMode
import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.{historicalImmutableCodec => historicalStakeSnapshotImmutable}

import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder
import weaver.MutableIOSuite

/** Spec assertions for the §G3 MPT-primary historical-stake-snapshots reader.
  *
  * The reader is a thin point-read against `GlobalStateKey.historicalStakeSnapshotsKey[F](period)`. The boundary writer (`GSAM.accept()`
  * via `AcceptanceMptStateChanges.applyStateChanges`) lands one MPT entry per stored period from the GSI's `historicalStakeSnapshots` map.
  * The MPT projection in `GlobalStateConverter.toAllStateKeyValueBytes` produces byte-identical entries (verified by
  * `GsamWritePathParitySuite`); this suite asserts that the reader observes those entries.
  *
  * Covers the three properties the migration depends on:
  *
  *   1. Lookup of a non-existent period → `None` (warmup / pre-boundary semantics). 2. Lookup of an existing period after a boundary write
  *      → `Some(distribution)` byte-equal to the GSI snapshot that landed on the writer side. 3. Byte-determinism: two independent MPT
  *      builds with the same input agree on every lookup — the core property §G3 buys over GSI iteration (closes the cross-node drift class
  *      on the historical-distribution read path).
  */
object HistoricalStakeReaderSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ---- helpers --------------------------------------------------------------

  private def pid(label: String): PeerId =
    PeerId(Hex(label.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def dist(stakes: (PeerId, BigInt)*): StakeDistribution =
    StakeDistribution(SortedMap(stakes: _*))

  /** Path 1 (heap-leak workstream): the per-period entry now bundles `(stakes, eta)`. Tests that drop a `StakeDistribution` into the
    * partition implicitly pair it with a sentinel eta to keep the schema-required field populated; suites that need specific eta assertions
    * construct `HistoricalStakeSnapshot` directly.
    */
  private def entry(stakes: StakeDistribution, eta: Hash = Hash.empty): HistoricalStakeSnapshot =
    HistoricalStakeSnapshot(stakes, eta)

  private def taggedHasher(delegate: Hasher[IO], tag: Byte): Hasher[IO] =
    new Hasher[IO] {
      def hash[A: Encoder](data: A): IO[Hash] =
        delegate.prefixedHash(data, Array(tag))
      def hashBytes(bytes: Array[Byte]): IO[Hash] =
        delegate.hashBytes(Array(tag) ++ bytes)
      def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] =
        hash(data).map(_ == expectedHash)
      def getLogic(ordinal: SnapshotOrdinal): HashLogic =
        delegate.getLogic(ordinal)
      def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] =
        delegate.prefixedHash(data, Array(tag) ++ prefix)
    }

  /** Build an MPT store seeded with `historical` entries by writing each `(period, snapshot)` directly via the same key derivation the GSAM
    * boundary writer uses (`AcceptanceMptStateChanges.applyStateChanges` → `historicalStakeSnapshotsKey[F](period)` →
    * `mpt.insert[HistoricalStakeSnapshot]`). The codec is the canonical `StakeDistributionCodec.historicalImmutableCodec` shared by writer
    * and reader.
    *
    * Why direct writes, not `syncFromGlobalSnapshotInfo`. The bootstrap-projector at `GlobalStateConverter.toAllStateKeyValueBytes`
    * includes the historical-stake-snapshots field, but the in-place writer `syncFromGlobalSnapshotInfo` does not — that path is intended
    * to seed/reset the live-state partitions and the historical-snapshots partition is incrementally written by GSAM at every boundary
    * ordinal. Writing here directly through `store.insert[HistoricalStakeSnapshot]` mirrors the boundary-time path exercised in production
    * by `AcceptanceMptStateChanges`.
    */
  private def mkStore(
    historical: SortedMap[EtaPeriod, HistoricalStakeSnapshot]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] = {
    val _ = historicalStakeSnapshotImmutable
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      entries <- historical.toList.traverse {
        case (period, snap) => GlobalStateKey.historicalStakeSnapshotsKey[IO](period).map(_ -> snap)
      }.map(_.toMap)
      _ <- store.insert[HistoricalStakeSnapshot](entries)
      _ <- store.build(SnapshotOrdinal(NonNegLong(1L))).void
    } yield store
  }

  // ---- core spec assertions ------------------------------------------------

  test("lookup non-existent period → None") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore(SortedMap.empty)
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(EtaPeriod(0L))
    } yield expect(out.isEmpty)
  }

  test("lookup existing period after boundary write → Some(snapshot) byte-equal to GSI") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val period = EtaPeriod(5L)
    val etaHex = Hash("01" * 32)
    val expected = HistoricalStakeSnapshot(dist(nA -> BigInt(1000), nB -> BigInt(500)), etaHex)
    for {
      store <- mkStore(SortedMap(period -> expected))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(period)
    } yield expect.same(Some(expected), out)
  }

  test("lookup wrong period returns None when other periods are present") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val present = EtaPeriod(3L)
    val absent = EtaPeriod(4L)
    val presentEntry = entry(dist(nA -> BigInt(100)))
    for {
      store <- mkStore(SortedMap(present -> presentEntry))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      hit <- hist.lookup(present)
      miss <- hist.lookup(absent)
    } yield
      expect.same(Some(presentEntry), hit) &&
        expect(miss.isEmpty)
  }

  test("multiple periods retained — each lookup hits the correct one") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    // Mirrors the production retention (last 4 periods). Each period's value differs in both peer
    // membership and amounts so a stray lookup that returned a different period's bytes would fail
    // the equality assertion below.
    val p1 = EtaPeriod(7L) -> entry(dist(nA -> BigInt(100)))
    val p2 = EtaPeriod(8L) -> entry(dist(nA -> BigInt(150), nB -> BigInt(200)))
    val p3 = EtaPeriod(9L) -> entry(dist(nB -> BigInt(300), nC -> BigInt(50)))
    val p4 = EtaPeriod(10L) -> entry(dist(nA -> BigInt(500), nB -> BigInt(500), nC -> BigInt(500)))
    val all = SortedMap(p1, p2, p3, p4)
    for {
      store <- mkStore(all)
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      r1 <- hist.lookup(p1._1)
      r2 <- hist.lookup(p2._1)
      r3 <- hist.lookup(p3._1)
      r4 <- hist.lookup(p4._1)
    } yield
      expect.same(Some(p1._2), r1) &&
        expect.same(Some(p2._2), r2) &&
        expect.same(Some(p3._2), r3) &&
        expect.same(Some(p4._2), r4)
  }

  test("byte-determinism — two independent MPT builds agree on the same lookup") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    val period = EtaPeriod(11L)
    // Determinism is the core property §G3 buys: under GSI iteration, two nodes' in-memory map
    // walks could produce divergent intermediate state on identical inputs; under MPT point read
    // the key is `Hash(period.value.toString)` and the value is scodec-encoded `HistoricalStakeSnapshot`
    // (stakes + eta), both byte-identical across independent builds.
    val expected = HistoricalStakeSnapshot(
      dist(nA -> BigInt(100), nB -> BigInt(200), nC -> BigInt(50)),
      Hash("02" * 32)
    )
    val historical = SortedMap(period -> expected)
    for {
      store1 <- mkStore(historical)
      store2 <- mkStore(historical)
      r1 <- HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store1)).lookup(period)
      r2 <- HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store2)).lookup(period)
    } yield expect.same(r1, r2) && expect.same(Some(expected), r1)
  }

  test("byte-determinism across periods — independent builds agree on every period") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    // Cross-node determinism over a multi-period set; mirrors what `StakeRegistry.stakeWeightedMpt`
    // would observe across the live retention window.
    val historical = SortedMap(
      EtaPeriod(0L) -> entry(dist(nA -> BigInt(50))),
      EtaPeriod(1L) -> entry(dist(nA -> BigInt(100), nB -> BigInt(100))),
      EtaPeriod(2L) -> entry(dist(nB -> BigInt(250)))
    )
    for {
      store1 <- mkStore(historical)
      store2 <- mkStore(historical)
      h1 = HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store1))
      h2 = HistoricalStakeReader.make[IO](GlobalStateReader.fromMptStore[IO](store2))
      r1 <- historical.keys.toList.traverse(p => h1.lookup(p).map(p -> _))
      r2 <- historical.keys.toList.traverse(p => h2.lookup(p).map(p -> _))
    } yield expect.same(r1, r2)
  }

  test("negative period lookup → None (warmup-bootstrap window edge case)") { res =>
    implicit val (h, _, js) = res
    // `EtaPeriod` permits negatives during `currentEtaPeriod - 2` queries before period 2. The reader
    // must return None for those (the registry falls through to the live aggregate). Verified here so
    // the migration doesn't accidentally introduce a synthetic key for negative periods.
    for {
      store <- mkStore(SortedMap(EtaPeriod(0L) -> entry(dist(pid("n") -> BigInt(1)))))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(EtaPeriod(-1L))
    } yield expect(out.isEmpty)
  }

  test("eta round-trip: write HistoricalStakeSnapshot with eta, read back the eta") { res =>
    implicit val (h, _, js) = res
    // Path 1 acceptance test: the per-period entry's eta must survive an MPT round trip with the
    // exact 32-byte content (this is the load-bearing property — `EtaStateManager.getEta` reads
    // this back and feeds it into VRF eligibility / KES verification).
    val period = EtaPeriod(12L)
    val nA = pid("node-A")
    val etaBytes = Array.fill[Byte](32)(0x42.toByte)
    val eta = Hash(etaBytes.map(b => f"$b%02x").mkString)
    val expected = HistoricalStakeSnapshot(dist(nA -> BigInt(777)), eta)
    for {
      store <- mkStore(SortedMap(period -> expected))
      reader = GlobalStateReader.fromMptStore[IO](store)
      hist = HistoricalStakeReader.make[IO](reader)
      out <- hist.lookup(period)
    } yield
      expect.same(Some(expected), out) &&
        expect.same(Some(eta), out.map(_.eta)) &&
        expect.same(Some(eta.value), out.map(_.eta.value))
  }

  test("staged exact reader rejects a mismatched finalized-base root") { res =>
    implicit val (h, _, js) = res
    implicit val selector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent[IO](h)
    val period = EtaPeriod(20L)
    val expected = entry(dist(pid("node-A") -> BigInt(100)))

    for {
      store <- mkStore(SortedMap(period -> expected))
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      bytes <- store.allEntriesAsBytes
      root <- GlobalSnapshotInfo.consensusMptRoot[IO](bytes)
      base = GlobalSnapshotStateRef(
        SnapshotOrdinal.MinValue,
        Hash("a" * 64),
        Hash.empty,
        MptRoot(root)
      )
      mismatched = base.copy(mptRoot = MptRoot(Hash("f" * 64)))
      result <- HistoricalStakeReader.exact[IO](overlay, etaRotationSnapshots = 1L).lookupExact(mismatched, mismatched, period)
    } yield expect(result.swap.exists(_.isInstanceOf[ParentStateError.ParentStateRootMismatch]))
  }

  test("staged exact reader selects each state-reference ordinal and rejects a mismatched parent root") { res =>
    implicit val (h, _, js) = res
    val selectedOrdinals = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    implicit val selector: HasherSelector[IO] = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = h
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] = {
        selectedOrdinals.add(ordinal.value.value)
        h
      }
    }
    val period = EtaPeriod.Zero
    val expected = entry(dist(pid("node-A") -> BigInt(200)))
    val baseHash = Hash("b" * 64)
    val childHash = Hash("c" * 64)
    val ord1 = SnapshotOrdinal(NonNegLong(1L))
    val ord2 = SnapshotOrdinal(NonNegLong(2L))

    for {
      store <- mkStore(SortedMap.empty)
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      handle <- overlay.checkout(BranchId(baseHash))
      key <- GlobalStateKey.historicalStakeSnapshotsKey[IO](period)
      _ <- {
        val _ = historicalStakeSnapshotImmutable
        handle.insert[HistoricalStakeSnapshot](key, expected)
      }
      _ <- overlay.commit(handle, BranchId(childHash), ord2)
      baseBytes <- store.allEntriesAsBytes
      parentBytes <- overlay.allEntriesAsBytes(BranchId(childHash))
      baseRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](baseBytes)
      parentRoot <- GlobalSnapshotInfo.consensusMptRoot[IO](parentBytes)
      base = GlobalSnapshotStateRef(ord1, baseHash, Hash("d" * 64), MptRoot(baseRoot))
      parent = GlobalSnapshotStateRef(ord2, childHash, baseHash, MptRoot(parentRoot))
      reader = HistoricalStakeReader.exact[IO](overlay, etaRotationSnapshots = 2L)
      valid <- reader.lookupExact(base, parent, period)
      mismatched <- reader.lookupExact(base, parent.copy(mptRoot = MptRoot(Hash("e" * 64))), period)
      selected = {
        import scala.jdk.CollectionConverters._
        selectedOrdinals.iterator().asScala.toSet
      }
    } yield
      expect.all(
        valid.contains(HistoricalStakeReader.ParentStakeView.Historical(expected)),
        mismatched.swap.exists(_.isInstanceOf[ParentStateError.ParentStateRootMismatch]),
        selected.contains(ord1.value.value),
        selected.contains(ord2.value.value)
      )
  }

  test("staged exact reader derives a historical key with the boundary-write hasher, not the later parent hasher") { res =>
    val (currentHasher, _, jsonSerializer) = res
    val writerHasher = taggedHasher(currentHasher, 0x11.toByte)
    val parentHasher = taggedHasher(currentHasher, 0x22.toByte)
    implicit val js: JsonSerializer[IO] = jsonSerializer
    val selectedOrdinals = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    val period = EtaPeriod(1L)
    val etaRotationSnapshots = 10L
    val writeOrdinal = SnapshotOrdinal(NonNegLong(19L))
    val parentOrdinal = SnapshotOrdinal(NonNegLong(25L))
    val expected = entry(dist(pid("node-migration") -> BigInt(300)))
    implicit val selector: HasherSelector[IO] = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = parentHasher
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] = {
        selectedOrdinals.add(ordinal.value.value)
        if (ordinal == writeOrdinal) writerHasher else parentHasher
      }
    }

    for {
      producer <- {
        implicit val producerHasher: Hasher[IO] = writerHasher
        InMemoryMerklePatriciaProducer.make[IO]()
      }
      store <- {
        implicit val writeHasher: Hasher[IO] = writerHasher
        MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      }
      key <- {
        implicit val writeHasher: Hasher[IO] = writerHasher
        GlobalStateKey.historicalStakeSnapshotsKey[IO](period)
      }
      _ <- {
        val _ = historicalStakeSnapshotImmutable
        store.insert[HistoricalStakeSnapshot](key, expected)
      }
      bytes <- store.allEntriesAsBytes
      root <- {
        implicit val rootHasher: Hasher[IO] = parentHasher
        GlobalSnapshotInfo.consensusMptRoot[IO](bytes)
      }
      state = GlobalSnapshotStateRef(parentOrdinal, Hash("a" * 64), Hash("b" * 64), MptRoot(root))
      pcTree <- ParentChildTree.make[IO]
      overlay <- {
        implicit val overlayHasher: Hasher[IO] = parentHasher
        MptOverlay.make[IO, GlobalStateKey](
          mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
          underlying = store,
          pcTree = pcTree,
          toHex = GlobalStateKey.toHex[IO],
          bestTipsFn = IO.pure(Set.empty[BranchId])
        )
      }
      result <- HistoricalStakeReader
        .exact[IO](overlay, etaRotationSnapshots)
        .lookupExact(state, state, period)
      selected = {
        import scala.jdk.CollectionConverters._
        selectedOrdinals.iterator().asScala.toList
      }
    } yield
      expect.all(
        result.contains(HistoricalStakeReader.ParentStakeView.Historical(expected)),
        selected.contains(writeOrdinal.value.value),
        selected.contains(parentOrdinal.value.value)
      )
  }

  test("staged exact reader allows warmup only for negative lookbacks and fails closed on absent mature history") { res =>
    implicit val (h, _, js) = res
    implicit val selector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent[IO](h)
    val parentOrdinal = SnapshotOrdinal(NonNegLong(5L))

    for {
      store <- mkStore(SortedMap.empty)
      bytes <- store.allEntriesAsBytes
      root <- GlobalSnapshotInfo.consensusMptRoot[IO](bytes)
      state = GlobalSnapshotStateRef(parentOrdinal, Hash("c" * 64), Hash("d" * 64), MptRoot(root))
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      reader = HistoricalStakeReader.exact[IO](overlay, etaRotationSnapshots = 2L)
      warmup <- reader.lookupExact(state, state, EtaPeriod(-1L))
      mature <- reader.lookupExact(state, state, EtaPeriod.Zero)
    } yield
      expect.all(
        warmup.contains(HistoricalStakeReader.ParentStakeView.GenesisWarmup(Map.empty)),
        mature.left.toOption.contains(ParentStateError.HistoricalStakeUnavailable(state, EtaPeriod.Zero))
      )
  }

  test("staged exact reader rejects invalid rotation length and write-ordinal overflow") { res =>
    implicit val (h, _, js) = res
    implicit val selector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent[IO](h)
    val parentOrdinal = SnapshotOrdinal(NonNegLong(Long.MaxValue))

    for {
      store <- mkStore(SortedMap.empty)
      bytes <- store.allEntriesAsBytes
      root <- GlobalSnapshotInfo.consensusMptRoot[IO](bytes)
      state = GlobalSnapshotStateRef(parentOrdinal, Hash("e" * 64), Hash("f" * 64), MptRoot(root))
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      invalid <- HistoricalStakeReader.exact[IO](overlay, etaRotationSnapshots = 0L).lookupExact(state, state, EtaPeriod.Zero)
      overflow <- HistoricalStakeReader
        .exact[IO](overlay, etaRotationSnapshots = 2L)
        .lookupExact(state, state, EtaPeriod(Long.MaxValue))
    } yield
      expect.all(
        invalid.left.toOption.contains(ParentStateError.InvalidEtaRotationSnapshots(0L)),
        overflow.left.toOption.contains(ParentStateError.HistoricalStakeWriteOrdinalOverflow(EtaPeriod(Long.MaxValue), 2L))
      )
  }

  test(
    "MULTI-BRANCH REORG: HistoricalStakeSnapshot written on branch A is invisible to branch B, " +
      "and visible from base only after A is finalized"
  ) { res =>
    implicit val (h, _, js) = res
    // Path 1 reorg-safety check: the per-period entry rides through the existing MultiBranch
    // overlay; non-canonical branch writes are dropped on chain selection rollback, canonical
    // branch writes fold into the base on finalize. Mirrors `MutableKesRegistrySuite`'s
    // "MULTI-BRANCH REORG" pattern.
    val period = EtaPeriod(99L)
    val nA = pid("node-A")
    val etaA = Hash("ab" * 32)
    val entryA = HistoricalStakeSnapshot(dist(nA -> BigInt(999)), etaA)
    val tipA: BranchId = BranchId(Hash("a" * 64))
    val tipB: BranchId = BranchId(Hash("b" * 64))
    val finalOrdinal = SnapshotOrdinal(NonNegLong(1L))
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      // Step 1: branch A writes the period entry, commits to tipA. Overlay-pending; not in store.
      handleA <- overlay.checkout(BranchId.base)
      keyA <- GlobalStateKey.historicalStakeSnapshotsKey[IO](period)
      _ <- {
        val _ = historicalStakeSnapshotImmutable
        handleA.insert[HistoricalStakeSnapshot](keyA, entryA)
      }
      _ <- overlay.commit(handleA, tipA, finalOrdinal)
      // Step 2: reader bound to branch A → sees `entryA`.
      readerA = GlobalStateReader.fromOverlay[IO](overlay, tipA)
      onA <- HistoricalStakeReader.make[IO](readerA).lookup(period)
      // Step 3: sibling branch B writes nothing; reader bound to tipB → sees only base.
      handleB <- overlay.checkout(BranchId.base)
      _ <- overlay.commit(handleB, tipB, finalOrdinal)
      readerB = GlobalStateReader.fromOverlay[IO](overlay, tipB)
      onB <- HistoricalStakeReader.make[IO](readerB).lookup(period)
      // Step 4: finalize branch A — the entry folds into the base store; a base-bound reader now
      // sees `entryA`.
      _ <- overlay.finalizeBranch(tipA, finalOrdinal)
      readerBase = GlobalStateReader.fromMptStore[IO](store)
      onBase <- HistoricalStakeReader.make[IO](readerBase).lookup(period)
    } yield
      expect.all(
        // Branch A's view contains the boundary write.
        onA.contains(entryA),
        // Branch B's view has no entry for `period` (writes never landed on B).
        onB.isEmpty,
        // Post-finalize: the entry has folded into the base store.
        onBase.contains(entryA)
      )
  }
}
