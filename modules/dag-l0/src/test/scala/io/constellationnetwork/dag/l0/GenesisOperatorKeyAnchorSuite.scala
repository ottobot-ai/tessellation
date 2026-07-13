package io.constellationnetwork.dag.l0

import fs2.io.file.Path
import weaver.SimpleIOSuite

object GenesisOperatorKeyAnchorSuite extends SimpleIOSuite {

  pureTest("Nakamoto GL0 requires a key anchor even for cold restart and rollback") {
    val missing = GenesisOperatorKeyAnchor.requireJson(None)

    expect(missing.swap.exists(_.getMessage.contains("including cold restart and rollback")))
  }

  pureTest("Nakamoto GL0 rejects CSV as an operator-key anchor") {
    val csv = GenesisOperatorKeyAnchor.requireJson(Some(Path("/tmp/genesis.csv")))

    expect(csv.swap.exists(_.getMessage.contains("JSON genesis argument")))
  }

  pureTest("Nakamoto GL0 accepts a canonical JSON operator-key anchor path") {
    val json = Path("/tmp/l0-genesis.json")

    expect.same(Right(json), GenesisOperatorKeyAnchor.requireJson(Some(json)))
  }
}
