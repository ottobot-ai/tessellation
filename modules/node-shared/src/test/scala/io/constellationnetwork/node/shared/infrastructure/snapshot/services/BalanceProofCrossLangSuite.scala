package io.constellationnetwork.node.shared.infrastructure.snapshot.services

import java.nio.charset.StandardCharsets

import cats.effect.IO

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.mpt.verifier.MerklePatriciaInclusionVerifier

import eu.timepit.refined.auto._
import weaver.SimpleIOSuite

/** Cross-language-compatible KAT for the canonical balance MPT.
  *
  * Builds the per-address balance MPT through the SAME production path the `/currency/{address}/balance/proof` route uses
  * ([[BalanceMpt.buildProof]] → [[Hasher.forCanonicalJson]] → tessellation's `StatelessMerklePatriciaProducer` + inclusion prover), then:
  * self-verifies the proof against the emitted root with [[MerklePatriciaInclusionVerifier]]. The verifier recomputes every node digest as
  * `SHA-256(prefix ++ canonicalJSON(commitment))`, which is the byte contract consumed by non-Scala light clients.
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

  test("balance proof builds via production path and self-verifies") {
    // The brotli-free, RFC8785-canonical hasher — the SAME one BalanceMpt.buildProof uses internally; needed here for the Scala self-verify.
    implicit val canonicalHasher: Hasher[IO] = Hasher.forCanonicalJson[IO]

    for {
      maybeProof <- BalanceMpt.buildProof[IO](balances, target, ordinal)
      bp <- IO.fromOption(maybeProof)(new RuntimeException("expected an inclusion proof for a present address"))
      // Scala-side self-verification: recompute the root from the witness; must equal the emitted root.
      verifier = MerklePatriciaInclusionVerifier.make[IO](bp.root)
      confirmE <- verifier.confirm(bp.proof)
    } yield expect(confirmE.isRight).and(expect(bp.balance == balances(target)))
  }
}
