package io.constellationnetwork.node.shared.config

import io.constellationnetwork.env.AppEnvironment._
import io.constellationnetwork.node.shared.config.types._
import io.constellationnetwork.node.shared.ext.pureconfig._

import eu.timepit.refined.pureconfig._
import pureconfig.ConfigSource
import pureconfig.generic.auto._
import weaver.SimpleIOSuite

/** Parse + invariant tests for the per-environment `nakamoto.confirmation-depth-k` (k₁) block.
  *
  * k₁ moved from a single scalar to a `{ mainnet, testnet, integrationnet, dev }` block typed
  * `Map[AppEnvironment, PosLong]`, resolved once at the use site (`confirmationDepthK(env)`). R and k₂
  * DERIVE from the resolved k₁. This decode goes through two non-trivial pureconfig pieces compile can't
  * check — the `environmentToPosLongMapReader` and the field-rename `ProductHint` (`confirmationDepthKByEnv`
  * ← `confirmation-depth-k`) — so a regression here would only surface at node boot. Values are powers of
  * two by design (no off-by-one).
  */
object NakamotoConfigSuite extends SimpleIOSuite {

  private lazy val nakamoto: NakamotoConfig =
    ConfigSource.default.at("nakamoto").loadOrThrow[NakamotoConfig]

  pureTest("confirmation-depth-k parses the per-environment block (1024 / 256 / 256 / 32)") {
    expect.all(
      nakamoto.confirmationDepthK(Mainnet).value == 1024L,
      nakamoto.confirmationDepthK(Testnet).value == 256L,
      nakamoto.confirmationDepthK(Integrationnet).value == 256L,
      nakamoto.confirmationDepthK(Dev).value == 32L
    )
  }

  pureTest("R = round(3.1·k₁) and k₂ = 100·k₁ derive from the resolved per-env k₁") {
    expect.all(
      // dev k₁=32 → R=round(99.2)=99, k₂=3200
      nakamoto.etaRotationSnapshots(Dev).value == 99L,
      nakamoto.keepDepthBehindFinalized(Dev).value == 3200L,
      // mainnet k₁=1024 → R=round(3174.4)=3174, k₂=102400
      nakamoto.etaRotationSnapshots(Mainnet).value == 3174L,
      nakamoto.keepDepthBehindFinalized(Mainnet).value == 102400L
    )
  }
}
