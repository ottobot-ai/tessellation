package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.node.shared.infrastructure.metrics.{CountingMetrics, Metrics}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.EpochStakeSnapshotter
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Spec assertions for the §G1 MPT-primary stake aggregator.
  *
  * Covers the four properties the StakeRegistry.stakeWeightedMpt path depends on:
  *
  *   1. Empty MPT → empty Map (boot fallback). 2. Multiple records pointing at the same nodeId on different source addresses sum into a
  *      single aggregate per node. 3. Mixed delegated-stake + node-collateral for the same nodeId sum together (two-tier stake). 4.
  *      Byte-equivalent across independent MPT builds with the same input (core determinism — the §G1 motivation: closing #218-style
  *      cross-node drift on stake reads).
  *
  * The exact-branch cached variant ([[NodeStakeAggregator.cached]]) is exercised in separate tests below: hit on the same branch, miss on
  * same-height branch replacement, and `None` defeats cache.
  */
object NodeStakeAggregatorSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimitCtx: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  // Test-scope Metrics — no-op for non-counter calls, so the latency-distribution recording in
  // `NodeStakeAggregator.fromMptStore` becomes a no-op. The test assertions check aggregate output, not
  // the metric side-effect. Made `implicit` here so all `NodeStakeAggregator.fromMptStore[IO](...)`
  // invocations below pick it up without each test having to rewire it.
  implicit val testMetrics: Metrics[IO] =
    CountingMetrics.instance(Ref.unsafe[IO, Map[String, Int]](Map.empty))

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ---- helpers --------------------------------------------------------------

  private val testSignature = signature.Signature(Hex(""))
  private val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)
  private val testProofs = NonEmptySet.one(testSignatureProof)

  private def pid(label: String): PeerId =
    PeerId(Hex(label.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  private def branch(hexDigit: String): BranchId = BranchId(Hash(hexDigit * 64))

  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  // `SortedSet[DelegatedStakeRecord]` orders by `(createdAt, rewards, event)`. Two records with the
  // same createdAt+rewards collapse under the SortedSet builder if their derived `Ordering[Signed]`
  // can't tell them apart. We make each record's `createdAt` distinct (the `ord` arg) so the test
  // exercises a real multi-record set without dedup; mirrors production where every distinct stake
  // event lands at a distinct snapshot ordinal.
  private def mkDelegated(nodeId: PeerId, amt: Long, source: Address, ord: Long): DelegatedStakeRecord = {
    val create = UpdateDelegatedStake.Create(
      source = source,
      nodeId = nodeId,
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amt)),
      fee = DelegatedStakeFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = DelegatedStakeReference.empty
    )
    DelegatedStakeRecord(
      event = Signed(create, testProofs),
      createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)),
      rewards = Amount(NonNegLong(0L))
    )
  }

  private def mkCollateral(nodeId: PeerId, amt: Long, source: Address, ord: Long): NodeCollateralRecord = {
    val create = UpdateNodeCollateral.Create(
      source = source,
      nodeId = nodeId,
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amt)),
      fee = NodeCollateralFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = NodeCollateralReference.empty
    )
    NodeCollateralRecord(event = Signed(create, testProofs), createdAt = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)))
  }

  private def mkStore(
    delegated: SortedMap[Address, SortedSet[DelegatedStakeRecord]],
    collateral: SortedMap[Address, SortedSet[NodeCollateralRecord]]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
      info = GlobalSnapshotInfo.empty.copy(
        activeDelegatedStakes = if (delegated.isEmpty) None else Some(delegated),
        activeNodeCollaterals = if (collateral.isEmpty) None else Some(collateral)
      )
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield store

  private def captureStakePrefixes(
    store: MptStore[IO, GlobalStateKey]
  )(implicit h: Hasher[IO]): IO[Map[Hex, List[StrictMptRawEntry]]] =
    for {
      delegatedPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[IO](GlobalStateFieldId.ActiveDelegatedStakes)
      collateralPrefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[IO](GlobalStateFieldId.ActiveNodeCollaterals)
      captured <- store.withExclusiveLock {
        List(delegatedPrefix, collateralPrefix)
          .traverse(prefix => store.rawEntriesForPrefixStrict(prefix).map(prefix -> _))
          .map(_.toMap)
      }
    } yield captured

  // ---- core spec assertions ------------------------------------------------

  test("empty MPT → empty Map") { res =>
    implicit val (h, _, js) = res
    for {
      store <- mkStore(SortedMap.empty, SortedMap.empty)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      out <- aggregator.aggregateFromMpt
    } yield expect(out.isEmpty)
  }

  test("two records for same nodeId on different source addresses → sum") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src1 = addr("source-1")
    val src2 = addr("source-2")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src1 -> SortedSet(mkDelegated(n, 300L, src1, ord = 1L)),
      src2 -> SortedSet(mkDelegated(n, 700L, src2, ord = 1L))
    )
    for {
      store <- mkStore(delegated, SortedMap.empty)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      out <- aggregator.aggregateFromMpt
    } yield expect.same(Map(n -> BigInt(1000)), out)
  }

  test("mixed delegated + collateral for same nodeId → sum (two-tier)") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("source-1")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 400L, src, ord = 1L))
    )
    val collateral = SortedMap[Address, SortedSet[NodeCollateralRecord]](
      src -> SortedSet(mkCollateral(n, 600L, src, ord = 1L))
    )
    for {
      store <- mkStore(delegated, collateral)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      out <- aggregator.aggregateFromMpt
    } yield expect.same(Map(n -> BigInt(1000)), out)
  }

  test("byte-determinism — two independent MPT builds with same input agree") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    val s1 = addr("source-1")
    val s2 = addr("source-2")
    val s3 = addr("source-3")

    // Two independently built MPT stores with the same logical input must produce identical
    // aggregates. This is the core property §G1 buys over GSI iteration — under the GSI-primary
    // path, cross-node map iteration order could produce divergent intermediate state; under MPT
    // prefix-scan the order is determined by hex(serialize(addr)) and byte-identical across nodes.
    //
    // Each record at a distinct `createdAt` ordinal so the SortedSet builder doesn't collapse
    // them under `Ordering[DelegatedStakeRecord] = Ordering.by((createdAt, rewards, event))`.
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      s1 -> SortedSet(mkDelegated(nA, 100L, s1, ord = 1L), mkDelegated(nB, 200L, s1, ord = 2L)),
      s2 -> SortedSet(mkDelegated(nA, 50L, s2, ord = 3L), mkDelegated(nC, 300L, s2, ord = 4L))
    )
    val collateral = SortedMap[Address, SortedSet[NodeCollateralRecord]](
      s3 -> SortedSet(mkCollateral(nB, 150L, s3, ord = 5L))
    )

    for {
      store1 <- mkStore(delegated, collateral)
      store2 <- mkStore(delegated, collateral)
      agg1 <- NodeStakeAggregator.fromMptStore[IO](store1).aggregateFromMpt
      agg2 <- NodeStakeAggregator.fromMptStore[IO](store2).aggregateFromMpt
    } yield
      // Expected: A = 100 + 50 = 150, B = 200 + 150 = 350, C = 300.
      expect.same(agg1, agg2) &&
        expect.same(Map(nA -> BigInt(150), nB -> BigInt(350), nC -> BigInt(300)), agg1)
  }

  test("off-seedlist nodeIds appear in the aggregate; seedlist filtering is the caller's job") { res =>
    implicit val (h, _, js) = res
    // The aggregator is a pure MPT read; it doesn't know about the seedlist. Confirms that
    // `StakeRegistry.stakeWeightedMpt` is the layer that drops off-seedlist nodes from numerator
    // AND denominator (matching the legacy `stakeWeighted` behaviour).
    val seed = pid("in-seedlist")
    val ghost = pid("ghost")
    val src = addr("s")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(seed, 100L, src, ord = 1L), mkDelegated(ghost, 900L, src, ord = 2L))
    )
    for {
      store <- mkStore(delegated, SortedMap.empty)
      out <- NodeStakeAggregator.fromMptStore[IO](store).aggregateFromMpt
    } yield expect.same(Map(seed -> BigInt(100), ghost -> BigInt(900)), out)
  }

  test("one aggregate captures delegated stake and collateral in a single multi-prefix request") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("source-1")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 400L, src, ord = 1L))
    )
    val collateral = SortedMap[Address, SortedSet[NodeCollateralRecord]](
      src -> SortedSet(mkCollateral(n, 600L, src, ord = 2L))
    )

    for {
      store <- mkStore(delegated, collateral)
      snapshot <- captureStakePrefixes(store)
      requestsR <- Ref.of[IO, List[List[Hex]]](List.empty)
      aggregator = NodeStakeAggregator.fromRawPrefixSnapshot[IO] { prefixes =>
        requestsR.update(_ :+ prefixes) >> IO.pure(snapshot)
      }
      out <- aggregator.aggregateFromMpt
      requests <- requestsR.get
    } yield
      expect.same(Map(n -> BigInt(1000)), out) &&
        expect.same(1, requests.size) &&
        expect.same(snapshot.keySet, requests.headOption.fold(Set.empty[Hex])(_.toSet))
  }

  // ---- cached wrapper -------------------------------------------------------

  test("cached: same exact branch serves from cache (underlying called once)") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("s")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 500L, src, ord = 1L))
    )
    for {
      store <- mkStore(delegated, SortedMap.empty)
      callsR <- Ref.of[IO, Int](0)
      // Counting wrapper around the real aggregator — every aggregateFromMpt call bumps `callsR`.
      countingAggregator = new NodeStakeAggregator[IO] {
        private val underlying = NodeStakeAggregator.fromMptStore[IO](store)
        def aggregateFromMpt(implicit hasher: Hasher[IO]): IO[Map[PeerId, BigInt]] =
          callsR.update(_ + 1) >> underlying.aggregateFromMpt(hasher)
      }
      cached <- NodeStakeAggregator.cached[IO](_ => countingAggregator, IO.pure(Some(branch("1"))))
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      calls <- callsR.get
    } yield expect.same(1, calls)
  }

  test("cached: same-height branch replacement invalidates cache and ABA cannot reuse the sibling result") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("s")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 500L, src, ord = 1L))
    )
    for {
      store <- mkStore(delegated, SortedMap.empty)
      callsR <- Ref.of[IO, Int](0)
      countingAggregator = new NodeStakeAggregator[IO] {
        private val underlying = NodeStakeAggregator.fromMptStore[IO](store)
        def aggregateFromMpt(implicit hasher: Hasher[IO]): IO[Map[PeerId, BigInt]] =
          callsR.update(_ + 1) >> underlying.aggregateFromMpt(hasher)
      }
      branchA = branch("1")
      branchB = branch("2")
      currentBranchR <- Ref.of[IO, BranchId](branchA)
      cached <- NodeStakeAggregator.cached[IO](_ => countingAggregator, currentBranchR.get.map(_.some))
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      _ <- currentBranchR.set(branchB)
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      _ <- currentBranchR.set(branchA)
      _ <- cached.aggregateFromMpt
      calls <- callsR.get
    } yield expect.same(3, calls)
  }

  test("cached: no selected branch defeats cache (every call re-reads base)") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("s")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 500L, src, ord = 1L))
    )
    for {
      store <- mkStore(delegated, SortedMap.empty)
      callsR <- Ref.of[IO, Int](0)
      countingAggregator = new NodeStakeAggregator[IO] {
        private val underlying = NodeStakeAggregator.fromMptStore[IO](store)
        def aggregateFromMpt(implicit hasher: Hasher[IO]): IO[Map[PeerId, BigInt]] =
          callsR.update(_ + 1) >> underlying.aggregateFromMpt(hasher)
      }
      cached <- NodeStakeAggregator.cached[IO](_ => countingAggregator, IO.pure(Option.empty[BranchId]))
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      _ <- cached.aggregateFromMpt
      calls <- callsR.get
    } yield expect.same(3, calls)
  }

  test("cached: a selected-tip change during a miss retries and never returns or caches the stale branch") { res =>
    implicit val (h, _, js) = res
    val n = pid("node-A")
    val src = addr("source-1")
    val branchA = branch("1")
    val branchB = branch("2")
    val delegatedA = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 100L, src, ord = 1L))
    )
    val delegatedB = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      src -> SortedSet(mkDelegated(n, 900L, src, ord = 2L))
    )

    for {
      storeA <- mkStore(delegatedA, SortedMap.empty)
      storeB <- mkStore(delegatedB, SortedMap.empty)
      snapshotA <- captureStakePrefixes(storeA)
      snapshotB <- captureStakePrefixes(storeB)
      currentBranchR <- Ref.of[IO, BranchId](branchA)
      callsR <- Ref.of[IO, Int](0)
      underlyingAt = (requested: BranchId) =>
        NodeStakeAggregator.fromRawPrefixSnapshot[IO] { _ =>
          callsR.update(_ + 1) >>
            (if (requested == branchA) currentBranchR.set(branchB).as(snapshotA) else IO.pure(snapshotB))
        }
      cached <- NodeStakeAggregator.cached[IO](underlyingAt, currentBranchR.get.map(_.some))
      first <- cached.aggregateFromMpt
      second <- cached.aggregateFromMpt
      calls <- callsR.get
    } yield
      expect.same(Map(n -> BigInt(900)), first) &&
        expect.same(first, second) &&
        expect.same(2, calls)
  }

  // ---- §G2 byte-equivalence parity ------------------------------------------
  //
  // The G2 migration replaces `EpochStakeSnapshotter.snapshot(info)` (walks the in-memory
  // `activeDelegatedStakes + activeNodeCollaterals` GSI maps) with
  // `NodeStakeAggregator.snapshotFromMpt(aggregator)` (prefix-scans the same data out of the MPT).
  // The two MUST produce byte-identical `StakeDistribution` when MPT and GSI describe the same
  // committed state. The closing-ordinal history writer separately uses its exact post-transition
  // GSI because the current accumulator has not yet landed in the parent MPT.
  //
  // The MPT path returns a flat `Map[PeerId, BigInt]` aggregate (sum-as-we-fold), the GSI path
  // computes separate delegated + collateral maps then merges. Both funnel through
  // `EpochStakeSnapshotter.fromCombined`, which is the single point owning the `SortedMap`
  // materialization — so the resulting scodec bytes are identical regardless of source.
  //
  // Asserting both `.stakes` (logical equality) and `StakeDistributionCodec` bytes (binary
  // equality) covers the MPT/Brotli state-proof rebuild contract end-to-end.

  test("snapshotFromMpt parity — single-tier delegated stakes byte-equal to GSI-primary snapshot") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val s1 = addr("s1")
    val s2 = addr("s2")
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      s1 -> SortedSet(mkDelegated(nA, 100L, s1, ord = 1L)),
      s2 -> SortedSet(mkDelegated(nB, 250L, s2, ord = 2L))
    )
    val info = GlobalSnapshotInfo.empty.copy(activeDelegatedStakes = Some(delegated))
    val gsiPrimary = EpochStakeSnapshotter.snapshot(info)

    for {
      store <- mkStore(delegated, SortedMap.empty)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      mptPrimary <- NodeStakeAggregator.snapshotFromMpt[IO](aggregator)
      gsiBytes = StakeDistributionCodec.codec.encode(gsiPrimary).require.toByteArray
      mptBytes = StakeDistributionCodec.codec.encode(mptPrimary).require.toByteArray
    } yield
      expect.same(gsiPrimary.stakes, mptPrimary.stakes) &&
        expect(java.util.Arrays.equals(gsiBytes, mptBytes))
  }

  test("snapshotFromMpt parity — delegated + collateral two-tier sums byte-equal to GSI-primary snapshot") { res =>
    implicit val (h, _, js) = res
    val nA = pid("node-A")
    val nB = pid("node-B")
    val nC = pid("node-C")
    val s1 = addr("s1")
    val s2 = addr("s2")
    val s3 = addr("s3")
    // Mixed shape exercising the same nodeId from two delegated sources AND a collateral entry,
    // plus a third-party collateral-only node. Mirrors the byte-determinism test above so the same
    // input shape feeds both the GSI walk and the MPT prefix-scan and we can compare bytes head-to-
    // head.
    val delegated = SortedMap[Address, SortedSet[DelegatedStakeRecord]](
      s1 -> SortedSet(mkDelegated(nA, 100L, s1, ord = 1L), mkDelegated(nB, 200L, s1, ord = 2L)),
      s2 -> SortedSet(mkDelegated(nA, 50L, s2, ord = 3L))
    )
    val collateral = SortedMap[Address, SortedSet[NodeCollateralRecord]](
      s2 -> SortedSet(mkCollateral(nB, 75L, s2, ord = 4L)),
      s3 -> SortedSet(mkCollateral(nC, 300L, s3, ord = 5L))
    )
    val info = GlobalSnapshotInfo.empty.copy(
      activeDelegatedStakes = Some(delegated),
      activeNodeCollaterals = Some(collateral)
    )
    val gsiPrimary = EpochStakeSnapshotter.snapshot(info)

    for {
      store <- mkStore(delegated, collateral)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      mptPrimary <- NodeStakeAggregator.snapshotFromMpt[IO](aggregator)
      gsiBytes = StakeDistributionCodec.codec.encode(gsiPrimary).require.toByteArray
      mptBytes = StakeDistributionCodec.codec.encode(mptPrimary).require.toByteArray
    } yield
      // Logical equality first (fast-fails on Map content) then byte equality (the load-bearing
      // assertion for the MPT/Brotli state-proof rebuild contract).
      expect.same(gsiPrimary.stakes, mptPrimary.stakes) &&
        expect(java.util.Arrays.equals(gsiBytes, mptBytes)) &&
        // Sanity-check the expected aggregate so a future StakeDistribution semantic change can't
        // silently pass parity with two co-broken paths.
        expect.same(BigInt(150), gsiPrimary.stakeOf(nA)) &&
        expect.same(BigInt(275), gsiPrimary.stakeOf(nB)) &&
        expect.same(BigInt(300), gsiPrimary.stakeOf(nC))
  }

  test("snapshotFromMpt parity — empty MPT/GSI both yield empty distribution (boot path)") { res =>
    implicit val (h, _, js) = res
    val info = GlobalSnapshotInfo.empty
    val gsiPrimary = EpochStakeSnapshotter.snapshot(info)
    for {
      store <- mkStore(SortedMap.empty, SortedMap.empty)
      aggregator = NodeStakeAggregator.fromMptStore[IO](store)
      mptPrimary <- NodeStakeAggregator.snapshotFromMpt[IO](aggregator)
      gsiBytes = StakeDistributionCodec.codec.encode(gsiPrimary).require.toByteArray
      mptBytes = StakeDistributionCodec.codec.encode(mptPrimary).require.toByteArray
    } yield
      // Both are `StakeDistribution.Empty` in shape; assert the bytes also agree so the boot path
      // doesn't slip past the byte-equivalence contract on a technicality (e.g. a SortedMap with a
      // different ordering instance would compare equal in `.stakes` but encode differently).
      expect.same(gsiPrimary.stakes, mptPrimary.stakes) &&
        expect(java.util.Arrays.equals(gsiBytes, mptBytes)) &&
        expect(mptPrimary.stakes.isEmpty)
  }
}
