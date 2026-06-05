package io.constellationnetwork.node.shared.infrastructure.snapshot.services

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import cats.effect.IO
import cats.syntax.either._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.snapshot.services.BalanceProof
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.verifier.MerklePatriciaInclusionVerifier

import eu.timepit.refined.auto._
import io.circe.Json
import io.circe.syntax.EncoderOps
import weaver.SimpleIOSuite

/** Cross-language KAT emitter for the roots-only-sharding balance light client.
  *
  * Builds the per-address balance MPT through the SAME production path the `/currency/{address}/balance/proof` route uses
  * ([[BalanceMpt.buildProof]] → [[Hasher.forCanonicalJson]] → tessellation's `StatelessMerklePatriciaProducer` + inclusion prover), then:
  *   1. self-verifies the proof in Scala against the emitted root with [[MerklePatriciaInclusionVerifier]] (the Scala analog of the TS
  *      verifier — recomputes every node digest as `SHA-256(prefix ++ canonicalJSON(commitment))`), and
  *   2. writes the proof (the real [[BalanceProof]] wire shape, plus a `targetKey` echo) to `mpt-crosslang-balance/fixtures/proof.json`.
  *
  * The companion `node mpt-crosslang-balance/verify.ts` step then feeds that fixture to the unmodified `mptVerifier.ts` lifted from the
  * `digital-evidence-app` Faraday backup — proving ml0's balance proof verifies in TypeScript byte-for-byte (no Brotli). Run the pair:
  * {{{
  *   sbt 'nodeShared/testOnly *BalanceProofCrossLangSuite'
  *   node mpt-crosslang-balance/verify.ts
  * }}}
  */
object BalanceProofCrossLangSuite extends SimpleIOSuite {

  // A handful of deterministic DAG addresses (derived from tags, always valid) with distinct balances — the metagraph's committed
  // `balances` map. `Address.fromBytes` yields a checksum-valid DAG address without needing literal-refinement.
  private def addr(tag: String): Address = Address.fromBytes(tag.getBytes(StandardCharsets.UTF_8))

  private val balances: SortedMap[Address, Balance] = SortedMap(
    addr("alice") -> Balance(100L),
    addr("bob") -> Balance(250L),
    addr("carol") -> Balance(99998L),
    addr("dave") -> Balance(42L),
    addr("erin") -> Balance(7L)
  )

  // The address we attest inclusion for (any present key works).
  private val target: Address = addr("carol")

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal.unsafeApply(123L)

  // Resolve `<repoRoot>/mpt-crosslang-balance/fixtures/proof.json`. sbt sets `user.dir` to the module base (`modules/node-shared`), so walk
  // up until we find a directory containing `mpt-crosslang-balance` (the cross-lang test dir, which also holds `verify.ts`), else fall back
  // to `user.dir`. This keeps the emitted fixture next to the TS runner regardless of which dir sbt is launched from.
  private val fixturePath = {
    val start = Paths.get(sys.props("user.dir")).toAbsolutePath
    val root = Iterator
      .iterate(start)(_.getParent)
      .takeWhile(_ != null)
      .find(p => Files.isDirectory(p.resolve("mpt-crosslang-balance")))
      .getOrElse(start)
    root.resolve(Paths.get("mpt-crosslang-balance", "fixtures", "proof.json"))
  }

  test("balance proof builds via production path, self-verifies in Scala, and is written for the TS verifier") {
    // The brotli-free, RFC8785-canonical hasher — the SAME one BalanceMpt.buildProof uses internally; needed here for the Scala self-verify.
    implicit val canonicalHasher: Hasher[IO] = Hasher.forCanonicalJson[IO]

    for {
      maybeProof <- BalanceMpt.buildProof[IO](balances, target, ordinal)
      bp <- IO.fromOption(maybeProof)(new RuntimeException("expected an inclusion proof for a present address"))
      // Scala-side self-verification: recompute the root from the witness; must equal the emitted root.
      verifier = MerklePatriciaInclusionVerifier.make[IO](bp.root)
      confirmE <- verifier.confirm(bp.proof)
      // Persist the fixture (the real BalanceProof wire shape + a targetKey echo for the runner's display).
      out = bp.asJson.deepMerge(
        Json.obj(
          "targetKey" -> bp.proof.path.asJson,
          "targetAddress" -> target.value.value.asJson
        )
      )
      _ <- IO.blocking {
        Files.createDirectories(fixturePath.getParent)
        Files.write(fixturePath, out.spaces2.getBytes(StandardCharsets.UTF_8))
      }
      _ <- IO.println(
        s"[BalanceProofCrossLangSuite] wrote $fixturePath\n" +
          s"  root=${bp.root.value}\n  path=${bp.proof.path.value}\n  balance=${bp.balance.value.value}\n  witnessNodes=${bp.proof.witness.size}"
      )
    } yield expect(confirmE.isRight) and expect(bp.balance == balances(target))
  }
}
