package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.IO

import io.constellationnetwork.schema.nakamoto.slot.Slot

import weaver.SimpleIOSuite

object EpochStateSuite extends SimpleIOSuite {

  private val genesisEta: Array[Byte] = Array.fill(32)(0x42.toByte)
  private val vrfOutput1: Array[Byte] = Array.fill(64)(0x01.toByte)
  private val vrfOutput2: Array[Byte] = Array.fill(64)(0x02.toByte)
  private val vrfOutput3: Array[Byte] = Array.fill(64)(0x03.toByte)

  test("genesis eta is returned initially") {
    for {
      state <- EpochState.make[IO](genesisEta)
      eta <- state.currentEta
    } yield expect(eta.sameElements(genesisEta))
  }

  test("lastProducedSlot returns MinValue initially") {
    for {
      state <- EpochState.make[IO](genesisEta)
      slot <- state.lastProducedSlot
    } yield expect.same(Slot.MinValue, slot)
  }

  test("recording production updates lastProducedSlot") {
    for {
      state <- EpochState.make[IO](genesisEta)
      slot5 = Slot.unsafeApply(5)
      _ <- state.recordProduction(slot5, vrfOutput1)
      lastSlot <- state.lastProducedSlot
    } yield expect.same(slot5, lastSlot)
  }

  test("recording production updates lastProducedSlot to most recent") {
    for {
      state <- EpochState.make[IO](genesisEta)
      slot5 = Slot.unsafeApply(5)
      slot10 = Slot.unsafeApply(10)
      _ <- state.recordProduction(slot5, vrfOutput1)
      _ <- state.recordProduction(slot10, vrfOutput2)
      slot <- state.lastProducedSlot
    } yield expect.same(slot10, slot)
  }

  test("multiple productions accumulate VRF outputs") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      _ <- state.recordProduction(Slot.unsafeApply(2), vrfOutput2)
      _ <- state.recordProduction(Slot.unsafeApply(3), vrfOutput3)
      count <- state.accumulatedCount
    } yield expect.same(3, count)
  }

  test("epoch rotation produces new eta different from previous") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      _ <- state.recordProduction(Slot.unsafeApply(2), vrfOutput2)
      etaBefore <- state.currentEta
      _ <- state.rotateEpoch(1L)
      etaAfter <- state.currentEta
    } yield expect(!etaBefore.sameElements(etaAfter))
  }

  test("epoch rotation clears VRF accumulator") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      _ <- state.recordProduction(Slot.unsafeApply(2), vrfOutput2)
      countBefore <- state.accumulatedCount
      _ <- state.rotateEpoch(1L)
      countAfter <- state.accumulatedCount
    } yield expect.same(2, countBefore) && expect.same(0, countAfter)
  }

  test("epoch rotation returns the new eta") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      newEta <- state.rotateEpoch(1L)
      current <- state.currentEta
    } yield expect(newEta.sameElements(current))
  }

  test("consecutive epoch rotations produce different etas") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      eta1 <- state.rotateEpoch(1L)
      _ <- state.recordProduction(Slot.unsafeApply(2), vrfOutput2)
      eta2 <- state.rotateEpoch(2L)
    } yield expect(!eta1.sameElements(eta2))
  }

  test("eta has correct length of 32 bytes (SHA-256)") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(1), vrfOutput1)
      newEta <- state.rotateEpoch(1L)
    } yield expect.same(32, newEta.length)
  }

  test("slot gap is computable from lastProducedSlot") {
    for {
      state <- EpochState.make[IO](genesisEta)
      _ <- state.recordProduction(Slot.unsafeApply(5), vrfOutput1)
      lastSlot <- state.lastProducedSlot
      currentSlot = Slot.unsafeApply(15)
      gap = currentSlot.value.value - lastSlot.value.value
    } yield expect.same(10L, gap)
  }

  test("empty epoch still rotates using at least 1 output") {
    for {
      state <- EpochState.make[IO](genesisEta)
      // No productions recorded
      etaBefore <- state.currentEta
      newEta <- state.rotateEpoch(1L)
      etaAfter <- state.currentEta
    } yield
      // Even with no outputs, eta changes due to epoch number contribution
      expect(!etaBefore.sameElements(etaAfter)) &&
        expect(newEta.sameElements(etaAfter))
  }

  test("computeNextEta is deterministic") {
    IO {
      val eta1 = EpochState.computeNextEta(genesisEta, 1L, List(vrfOutput1, vrfOutput2))
      val eta2 = EpochState.computeNextEta(genesisEta, 1L, List(vrfOutput1, vrfOutput2))

      expect(eta1.sameElements(eta2))
    }
  }

  test("computeNextEta with different epoch numbers produces different results") {
    IO {
      val eta1 = EpochState.computeNextEta(genesisEta, 1L, List(vrfOutput1))
      val eta2 = EpochState.computeNextEta(genesisEta, 2L, List(vrfOutput1))

      expect(!eta1.sameElements(eta2))
    }
  }

  test("computeNextEta with different inputs produces different results") {
    IO {
      val eta1 = EpochState.computeNextEta(genesisEta, 1L, List(vrfOutput1))
      val eta2 = EpochState.computeNextEta(genesisEta, 1L, List(vrfOutput2))

      expect(!eta1.sameElements(eta2))
    }
  }
}
