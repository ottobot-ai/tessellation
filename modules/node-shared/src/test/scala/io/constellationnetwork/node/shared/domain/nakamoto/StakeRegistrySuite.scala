package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.IO

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.numerics.implicits._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Amount
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object StakeRegistrySuite extends SimpleIOSuite with Checkers {

  // ---- helpers --------------------------------------------------------------

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Deterministic Address from a tag (only used as a SortedMap key in the GSI — the actual
  // value is irrelevant to stake aggregation, which keys off `record.event.value.nodeId`).
  private def addr(tag: String): Address =
    Address.fromBytes(tag.getBytes("UTF-8"))

  private val anyProofs: NonEmptySet[SignatureProof] =
    NonEmptySet.one[SignatureProof](
      SignatureProof(Id(Hex("00")), Signature(Hex(Hash.empty.value)))
    )

  // Synthetic delegated-stake record pointing at `nodeId` with amount `amt` (units).
  // The signing identity is irrelevant for stake aggregation, so we attach a placeholder proof.
  private def mkDelegated(nodeId: PeerId, amt: Long, source: Address): DelegatedStakeRecord = {
    val create = UpdateDelegatedStake.Create(
      source = source,
      nodeId = nodeId,
      amount = DelegatedStakeAmount(NonNegLong.unsafeFrom(amt)),
      fee = DelegatedStakeFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = DelegatedStakeReference.empty
    )
    DelegatedStakeRecord(
      event = Signed(create, anyProofs),
      createdAt = SnapshotOrdinal.MinValue,
      rewards = Amount(NonNegLong(0L)),
      currentTokenLockRef = None,
      currentAmount = None
    )
  }

  private def mkCollateral(nodeId: PeerId, amt: Long, source: Address): NodeCollateralRecord = {
    val create = UpdateNodeCollateral.Create(
      source = source,
      nodeId = nodeId,
      amount = NodeCollateralAmount(NonNegLong.unsafeFrom(amt)),
      fee = NodeCollateralFee(NonNegLong(0L)),
      tokenLockRef = Hash.empty,
      parent = NodeCollateralReference.empty
    )
    NodeCollateralRecord(event = Signed(create, anyProofs), createdAt = SnapshotOrdinal.MinValue)
  }

  // Build a minimal GlobalSnapshotInfo with only the two stake fields populated.
  // Each `(nodeId -> amount)` is bucketed under a synthetic source-address so multiple records
  // with the same nodeId are kept in their respective `SortedSet` partitions — which is exactly
  // the on-chain shape the stake-weighted registry consumes.
  private def mkSnapshotInfo(
    delegated: Map[PeerId, Long],
    collateral: Map[PeerId, Long]
  ): GlobalSnapshotInfo = {
    val delegatedMap: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
      delegated.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"delegated-source-$idx")
          acc.updated(a, SortedSet(mkDelegated(nodeId, amt, a)))
      }

    val collateralMap: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
      collateral.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[NodeCollateralRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"collateral-source-$idx")
          acc.updated(a, SortedSet(mkCollateral(nodeId, amt, a)))
      }

    GlobalSnapshotInfo.empty.copy(
      activeDelegatedStakes = Some(delegatedMap),
      activeNodeCollaterals = Some(collateralMap)
    )
  }

  // Like `mkSnapshotInfo` but accepts a list so multiple records per peer can coexist.
  private def mkSnapshotInfoMulti(
    delegated: List[(PeerId, Long)],
    collateral: List[(PeerId, Long)]
  ): GlobalSnapshotInfo = {
    val delegatedMap: SortedMap[Address, SortedSet[DelegatedStakeRecord]] =
      delegated.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"delegated-source-$idx")
          acc.updated(a, SortedSet(mkDelegated(nodeId, amt, a)))
      }
    val collateralMap: SortedMap[Address, SortedSet[NodeCollateralRecord]] =
      collateral.zipWithIndex.foldLeft(SortedMap.empty[Address, SortedSet[NodeCollateralRecord]]) {
        case (acc, ((nodeId, amt), idx)) =>
          val a = addr(s"collateral-source-$idx")
          acc.updated(a, SortedSet(mkCollateral(nodeId, amt, a)))
      }
    GlobalSnapshotInfo.empty.copy(
      activeDelegatedStakes = Some(delegatedMap),
      activeNodeCollaterals = Some(collateralMap)
    )
  }

  // ============ existing equalWeight tests (preserved) ==========================

  test("empty registry returns 0 stake for any peer") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      stake <- registry.relativeStake(pid("unknown"))
    } yield expect.same(Ratio.Zero, stake)
  }

  test("empty registry has validator count of 0") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      count <- registry.validatorCount
    } yield expect.same(0, count)
  }

  test("empty registry returns empty allStakes") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      stakes <- registry.allStakes
    } yield expect.same(Map.empty[PeerId, Ratio], stakes)
  }

  test("single validator gets relativeStake of 1.0") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      _ <- registry.updateValidators(Set(peer1))
      stake <- registry.relativeStake(peer1)
    } yield expect.same(Ratio.One, stake)
  }

  test("two validators each get relativeStake of 0.5") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      peer2 = pid("peer2")
      _ <- registry.updateValidators(Set(peer1, peer2))
      stake1 <- registry.relativeStake(peer1)
      stake2 <- registry.relativeStake(peer2)
    } yield expect.same(Ratio(1, 2), stake1) && expect.same(Ratio(1, 2), stake2)
  }

  test("N validators each get 1/N stake") {
    val n = 10
    val peers = (1 to n).map(i => pid(s"peer$i")).toSet

    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(peers)
      stakes <- registry.allStakes
    } yield {
      val expectedStake = Ratio(1, n)
      expect(stakes.size == n) &&
      expect(stakes.values.forall(_ == expectedStake))
    }
  }

  test("unknown peer gets 0 stake when validators exist") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      peer2 = pid("peer2")
      unknownPeer = pid("unknown")
      _ <- registry.updateValidators(Set(peer1, peer2))
      stake <- registry.relativeStake(unknownPeer)
    } yield expect.same(Ratio.Zero, stake)
  }

  test("update replaces entire validator set") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      peer2 = pid("peer2")
      peer3 = pid("peer3")
      _ <- registry.updateValidators(Set(peer1, peer2))
      stakePeer1Before <- registry.relativeStake(peer1)
      _ <- registry.updateValidators(Set(peer3))
      stakePeer1After <- registry.relativeStake(peer1)
      stakePeer3After <- registry.relativeStake(peer3)
    } yield
      expect.same(Ratio(1, 2), stakePeer1Before) &&
        expect.same(Ratio.Zero, stakePeer1After) &&
        expect.same(Ratio.One, stakePeer3After)
  }

  test("allStakes sums to exactly 1.0") {
    val n = 7
    val peers = (1 to n).map(i => pid(s"peer$i")).toSet

    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(peers)
      stakes <- registry.allStakes
    } yield {
      val total = stakes.values.foldLeft(Ratio.Zero)(_ + _)
      expect.same(Ratio.One, total)
    }
  }

  test("validatorCount returns correct count after update") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      _ <- registry.updateValidators(Set(pid("p1"), pid("p2"), pid("p3")))
      count <- registry.validatorCount
    } yield expect.same(3, count)
  }

  test("clearing validators returns registry to empty state") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      _ <- registry.updateValidators(Set(peer1))
      stakeBefore <- registry.relativeStake(peer1)
      _ <- registry.updateValidators(Set.empty)
      stakeAfter <- registry.relativeStake(peer1)
      count <- registry.validatorCount
    } yield
      expect.same(Ratio.One, stakeBefore) &&
        expect.same(Ratio.Zero, stakeAfter) &&
        expect.same(0, count)
  }

  // ============ stakeWeighted unit tests =======================================

  test("stakeWeighted: empty snapshot returns Ratio.Zero for any seedlist peer") {
    val info = mkSnapshotInfo(Map.empty, Map.empty)
    val p1 = pid("p1")
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(p1, pid("p2"), pid("p3")))
      s <- registry.relativeStake(p1)
    } yield expect.same(Ratio.Zero, s)
  }

  test("stakeWeighted: None snapshot info falls back to 1/N (boot path)") {
    val peers = (1 to 4).map(i => pid(s"p$i")).toSet
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(None), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(peers)
      s1 <- registry.relativeStake(pid("p1"))
      s2 <- registry.relativeStake(pid("p2"))
      sUnknown <- registry.relativeStake(pid("unknown"))
    } yield
      expect.same(Ratio(1, 4), s1) &&
        expect.same(Ratio(1, 4), s2) &&
        expect.same(Ratio.Zero, sUnknown)
  }

  test("stakeWeighted: single-tier delegated [100,200,700] → [1/10, 2/10, 7/10]") {
    val a = pid("a")
    val b = pid("b")
    val c = pid("c")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L, b -> 200L, c -> 700L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c))
      sA <- registry.relativeStake(a)
      sB <- registry.relativeStake(b)
      sC <- registry.relativeStake(c)
    } yield
      expect.same(Ratio(1, 10), sA) &&
        expect.same(Ratio(2, 10), sB) &&
        expect.same(Ratio(7, 10), sC)
  }

  test("stakeWeighted: single-tier collateral only — symmetric to delegated") {
    val a = pid("a")
    val b = pid("b")
    val c = pid("c")
    val info = mkSnapshotInfo(delegated = Map.empty, collateral = Map(a -> 100L, b -> 200L, c -> 700L))
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c))
      sA <- registry.relativeStake(a)
      sB <- registry.relativeStake(b)
      sC <- registry.relativeStake(c)
    } yield
      expect.same(Ratio(1, 10), sA) &&
        expect.same(Ratio(2, 10), sB) &&
        expect.same(Ratio(7, 10), sC)
  }

  test("stakeWeighted: two-tier sum — A(delegated=100, collateral=100) B(delegated=200) C(collateral=700)") {
    val a = pid("a")
    val b = pid("b")
    val c = pid("c")
    // A and B both have delegated stake → both must live under distinct source addresses (the
    // SortedMap key); the mkSnapshotInfoMulti helper handles that.
    val info = mkSnapshotInfoMulti(
      delegated = List(a -> 100L, b -> 200L),
      collateral = List(a -> 100L, c -> 700L)
    )
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c))
      sA <- registry.relativeStake(a)
      sB <- registry.relativeStake(b)
      sC <- registry.relativeStake(c)
    } yield
      // total = 100+100+200+700 = 1100; A=200/1100=2/11, B=200/1100=2/11, C=700/1100=7/11
      expect.same(Ratio(2, 11), sA) &&
        expect.same(Ratio(2, 11), sB) &&
        expect.same(Ratio(7, 11), sC)
  }

  test("stakeWeighted: off-seedlist nodeId in stake records is excluded from numerator AND denominator") {
    val a = pid("in-seedlist-A")
    val b = pid("in-seedlist-B")
    val ghost = pid("ghost-not-in-seedlist")
    val info = mkSnapshotInfo(
      delegated = Map(a -> 100L, b -> 100L, ghost -> 800L),
      collateral = Map.empty
    )
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b))
      sA <- registry.relativeStake(a)
      sB <- registry.relativeStake(b)
      sGhost <- registry.relativeStake(ghost)
      all <- registry.allStakes
    } yield
      // total over seedlist only = 200; ghost's 800 is dropped from both numerator and denominator.
      expect.same(Ratio(1, 2), sA) &&
        expect.same(Ratio(1, 2), sB) &&
        expect.same(Ratio.Zero, sGhost) &&
        expect.same(Set(a, b), all.keySet)
  }

  test("stakeWeighted: zero-stake seedlist peer → Ratio.Zero") {
    val a = pid("a")
    val b = pid("b") // no stake records
    val info = mkSnapshotInfo(delegated = Map(a -> 100L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b))
      sA <- registry.relativeStake(a)
      sB <- registry.relativeStake(b)
    } yield
      expect.same(Ratio.One, sA) &&
        expect.same(Ratio.Zero, sB)
  }

  test("stakeWeighted: allStakes.values.sum == Ratio.One for non-empty distribution") {
    val a = pid("a")
    val b = pid("b")
    val c = pid("c")
    val info = mkSnapshotInfo(delegated = Map(a -> 100L, b -> 250L, c -> 650L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c))
      stakes <- registry.allStakes
    } yield {
      val total = stakes.values.foldLeft(Ratio.Zero)(_ + _)
      expect.same(Ratio.One, total)
    }
  }

  test("stakeWeighted: relativeStake(p) == allStakes(p) for every seedlist peer (cross-API consistency)") {
    val peers = (1 to 5).map(i => pid(s"p$i"))
    val amounts = List(10L, 20L, 30L, 40L, 50L)
    val info = mkSnapshotInfo(delegated = peers.zip(amounts).toMap, collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(peers.toSet)
      stakes <- registry.allStakes
      perPeer <- peers.toList.traverseListIO(registry.relativeStake)
    } yield {
      val ok = peers.zip(perPeer).forall { case (p, s) => stakes(p) == s }
      expect(ok)
    }
  }

  test("stakeWeighted: 8-node fixture [5,5,5,5,15,15,25,25] returns the e2e harness ratios") {
    val peers = (1 to 8).map(i => pid(s"n$i"))
    val amounts = List(5L, 5L, 5L, 5L, 15L, 15L, 25L, 25L)
    // total = 5*4 + 15*2 + 25*2 = 20 + 30 + 50 = 100
    val info = mkSnapshotInfo(delegated = peers.zip(amounts).toMap, collateral = Map.empty)
    val expected = amounts.map(a => Ratio(BigInt(a), BigInt(100)))
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(peers.toSet)
      observed <- peers.toList.traverseListIO(registry.relativeStake)
    } yield expect(observed == expected)
  }

  // ---- optimisticRelativeStake ---------------------------------------------

  test("stakeWeighted: optimisticRelativeStake meets-quorum — relative-to-active-only") {
    val a = pid("a") // 10 — inactive
    val b = pid("b") // 20 — active
    val c = pid("c") // 30 — active
    val d = pid("d") // 40 — active
    val info = mkSnapshotInfo(delegated = Map(a -> 10L, b -> 20L, c -> 30L, d -> 40L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c, d))
      _ <- registry.markActive(b)
      _ <- registry.markActive(c)
      _ <- registry.markActive(d)
      // activeStakeFraction = 90/100 >= 1/2 → relative-to-active-only
      sB <- registry.optimisticRelativeStake(b)
      sC <- registry.optimisticRelativeStake(c)
      sD <- registry.optimisticRelativeStake(d)
    } yield
      expect.same(Ratio(20, 90), sB) &&
        expect.same(Ratio(30, 90), sC) &&
        expect.same(Ratio(40, 90), sD)
  }

  test("stakeWeighted: optimisticRelativeStake below-quorum → full-seedlist fallback") {
    val a = pid("a") // 10
    val b = pid("b") // 20
    val c = pid("c") // 30 — only active peer (30/100 < 1/2)
    val d = pid("d") // 40
    val info = mkSnapshotInfo(delegated = Map(a -> 10L, b -> 20L, c -> 30L, d -> 40L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c, d))
      _ <- registry.markActive(c)
      sC <- registry.optimisticRelativeStake(c)
    } yield expect.same(Ratio(30, 100), sC)
  }

  test("stakeWeighted: optimisticRelativeStake peer not in active set → full-seedlist fallback") {
    val a = pid("a") // 10
    val b = pid("b") // 20 — active
    val c = pid("c") // 30 — active
    val d = pid("d") // 40 — active
    val info = mkSnapshotInfo(delegated = Map(a -> 10L, b -> 20L, c -> 30L, d -> 40L), collateral = Map.empty)
    for {
      registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
      _ <- registry.updateValidators(Set(a, b, c, d))
      _ <- registry.markActive(b)
      _ <- registry.markActive(c)
      _ <- registry.markActive(d)
      // Quorum met (90/100 >= 1/2) but A is NOT in the observed-active set → fall back to 10/100
      sA <- registry.optimisticRelativeStake(a)
    } yield expect.same(Ratio(10, 100), sA)
  }

  // ============ stakeWeighted property tests ====================================

  test("stakeWeightedProperty: allStakes.values.sum == Ratio.One for any non-empty stake distribution") {
    import org.scalacheck.Gen
    // Force a non-zero total by drawing each amount from [1, 1_000_000]. The constructive
    // approach avoids `suchThat` discard pressure when scalacheck happens to roll many zeros.
    val genCase: Gen[(Set[PeerId], Map[PeerId, Long])] =
      for {
        n <- Gen.chooseNum(1, 12)
        peers = (1 to n).map(i => pid(s"prop-peer-$i")).toSet
        amounts <- Gen.listOfN(n, Gen.chooseNum(1L, 1_000_000L))
      } yield (peers, peers.zip(amounts).toMap)

    forall(genCase) {
      case (peers, weights) =>
        val info = mkSnapshotInfo(delegated = weights, collateral = Map.empty)
        for {
          registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
          _ <- registry.updateValidators(peers)
          stakes <- registry.allStakes
        } yield {
          val total = stakes.values.foldLeft(Ratio.Zero)(_ + _)
          expect.same(Ratio.One, total)
        }
    }
  }

  test("stakeWeightedProperty: relativeStake(p) is in [Ratio.Zero, Ratio.One] for any peer") {
    import org.scalacheck.Gen
    val genCase: Gen[(Set[PeerId], Map[PeerId, Long], PeerId)] =
      for {
        n <- Gen.chooseNum(1, 12)
        peers = (1 to n).map(i => pid(s"prop-peer-$i")).toSet
        amounts <- Gen.listOfN(n, Gen.chooseNum(0L, 1_000_000L))
        // Sample peer can be in or out of the seedlist (covers both branches).
        sampleIdx <- Gen.chooseNum(0, n + 2)
        sample = if (sampleIdx < n) peers.toList(sampleIdx) else pid(s"outsider-$sampleIdx")
      } yield (peers, peers.zip(amounts).toMap, sample)

    forall(genCase) {
      case (peers, weights, sample) =>
        val info = mkSnapshotInfo(delegated = weights, collateral = Map.empty)
        for {
          registry <- StakeRegistry.stakeWeighted[IO](IO.pure(Some(info)), _ => IO.pure(Option.empty[StakeDistribution]))
          _ <- registry.updateValidators(peers)
          s <- registry.relativeStake(sample)
        } yield expect(s >= Ratio.Zero) && expect(s <= Ratio.One)
    }
  }

  // ---- small helper: foldM-style traverse for List in IO (cats stdlib alternative) -----
  private implicit class TraverseListIO[A](xs: Seq[A]) {
    def traverseListIO[B](f: A => IO[B]): IO[List[B]] =
      xs.foldLeft(IO.pure(List.empty[B])) { (accIO, a) =>
        for {
          acc <- accIO
          b <- f(a)
        } yield acc :+ b
      }
  }
}
