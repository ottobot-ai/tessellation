package io.constellationnetwork.serde

import cats.data.NonEmptyList

import io.constellationnetwork.merkletree.{MerkleTree, Proof, ProofEntry}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.codecs.instances.MerkleTreeCodecs.{
  merkleTreeImmutableCodec,
  proofEntryImmutableCodec,
  proofImmutableCodec
}
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegInt
import weaver.FunSuite

/** Round-trip suite for Proof / ProofEntry / MerkleTree. First use of `EitherCodec` in an instance. */
object MerkleTreeCodecsSuite extends FunSuite {

  private def h(prefix: String) = Hash(prefix * 32)

  test("ProofEntry round-trips with Left sibling") {
    val entry = ProofEntry(h("11"), Left(h("22")))
    val bytes = proofEntryImmutableCodec.immutableBytes(entry)
    expect(bytes.fromImmutableBytes[ProofEntry](proofEntryImmutableCodec) == Right(entry))
  }

  test("ProofEntry round-trips with Right sibling") {
    val entry = ProofEntry(h("11"), Right(h("33")))
    val bytes = proofEntryImmutableCodec.immutableBytes(entry)
    expect(bytes.fromImmutableBytes[ProofEntry](proofEntryImmutableCodec) == Right(entry))
  }

  test("ProofEntry Left vs Right with same inner hashes produce different bytes") {
    val l = ProofEntry(h("00"), Left(h("ab")))
    val r = ProofEntry(h("00"), Right(h("ab")))
    expect(proofEntryImmutableCodec.immutableBytes(l) != proofEntryImmutableCodec.immutableBytes(r))
  }

  test("Proof round-trips with multiple entries") {
    val p = Proof(
      NonEmptyList.of(
        ProofEntry(h("01"), Left(h("02"))),
        ProofEntry(h("03"), Right(h("04"))),
        ProofEntry(h("05"), Left(h("06")))
      )
    )
    val bytes = proofImmutableCodec.immutableBytes(p)
    expect(bytes.fromImmutableBytes[Proof](proofImmutableCodec) == Right(p))
  }

  test("MerkleTree round-trips") {
    val tree = MerkleTree(
      leafCount = NonNegInt.unsafeFrom(3),
      nodes = NonEmptyList.of(h("aa"), h("bb"), h("cc"))
    )
    val bytes = merkleTreeImmutableCodec.immutableBytes(tree)
    expect(bytes.fromImmutableBytes[MerkleTree](merkleTreeImmutableCodec) == Right(tree))
  }
}
