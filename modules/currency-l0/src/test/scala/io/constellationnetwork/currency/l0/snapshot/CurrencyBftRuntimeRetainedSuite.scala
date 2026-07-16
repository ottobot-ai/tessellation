package io.constellationnetwork.currency.l0.snapshot

import cats.effect.IO

import weaver.SimpleIOSuite

object CurrencyBftRuntimeRetainedSuite extends SimpleIOSuite {

  test("compiled CurrencyL0 services retain the generic BFT consensus API and state machine") {
    IO.blocking {
      val serviceMethods = Class
        .forName("io.constellationnetwork.currency.l0.modules.Services")
        .getMethods
        .iterator
        .map(_.getName)
        .toSet

      expect.all(
        serviceMethods.contains("consensus"),
        Class.forName("io.constellationnetwork.currency.l0.snapshot.CurrencySnapshotConsensusStateCreator") != null,
        Class.forName("io.constellationnetwork.currency.l0.snapshot.CurrencySnapshotConsensusStateAdvancer") != null,
        Class.forName("io.constellationnetwork.currency.l0.snapshot.CurrencySnapshotConsensusStateRemover$") != null
      )
    }
  }
}
