package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

object StakeRegistrySuite extends SimpleIOSuite {

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  test("empty registry returns 0 stake for any peer") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      stake    <- registry.relativeStake(pid("unknown"))
    } yield expect.same(0.0, stake)
  }

  test("empty registry has validator count of 0") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      count    <- registry.validatorCount
    } yield expect.same(0, count)
  }

  test("empty registry returns empty allStakes") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      stakes   <- registry.allStakes
    } yield expect.same(Map.empty[PeerId, Double], stakes)
  }

  test("single validator gets relativeStake of 1.0") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      _     <- registry.updateValidators(Set(peer1))
      stake <- registry.relativeStake(peer1)
    } yield expect.same(1.0, stake)
  }

  test("two validators each get relativeStake of 0.5") {
    for {
      registry <- StakeRegistry.equalWeight[IO]
      peer1 = pid("peer1")
      peer2 = pid("peer2")
      _ <- registry.updateValidators(Set(peer1, peer2))
      stake1 <- registry.relativeStake(peer1)
      stake2 <- registry.relativeStake(peer2)
    } yield expect.same(0.5, stake1) && expect.same(0.5, stake2)
  }

  test("N validators each get 1/N stake") {
    val n = 10
    val peers = (1 to n).map(i => pid(s"peer$i")).toSet

    for {
      registry <- StakeRegistry.equalWeight[IO]
      _        <- registry.updateValidators(peers)
      stakes   <- registry.allStakes
    } yield {
      val expectedStake = 1.0 / n.toDouble
      expect(stakes.size == n) &&
        expect(stakes.values.forall(s => math.abs(s - expectedStake) < 1e-10))
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
    } yield expect.same(0.0, stake)
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
    } yield {
      expect.same(0.5, stakePeer1Before) &&
        expect.same(0.0, stakePeer1After) &&
        expect.same(1.0, stakePeer3After)
    }
  }

  test("allStakes sums to ~1.0 within floating point tolerance") {
    val n = 7
    val peers = (1 to n).map(i => pid(s"peer$i")).toSet

    for {
      registry <- StakeRegistry.equalWeight[IO]
      _        <- registry.updateValidators(peers)
      stakes   <- registry.allStakes
    } yield {
      val total = stakes.values.sum
      expect(math.abs(total - 1.0) < 1e-10)
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
    } yield {
      expect.same(1.0, stakeBefore) &&
        expect.same(0.0, stakeAfter) &&
        expect.same(0, count)
    }
  }
}
