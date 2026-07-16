package io.constellationnetwork.serde

import scala.collection.immutable.ListMap

import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MerklePatriciaCommitment.{Branch, Extension, Leaf}
import io.constellationnetwork.security.mpt.{MerklePatriciaCommitment, Nibble}
import io.constellationnetwork.serde.codecs.instances.MerklePatriciaCommitmentScodecV1Codec

import scodec.Attempt
import scodec.bits.ByteVector
import weaver.FunSuite

/** Byte-contract tests only. These vectors do not assert equivalence with, or activate, the live JSON-based MPT root. */
object MerklePatriciaCommitmentScodecV1CodecSuite extends FunSuite {

  private val codec = MerklePatriciaCommitmentScodecV1Codec.codec
  private val immutableCodec = MerklePatriciaCommitmentScodecV1Codec.immutableCodec

  private val ascendingDigest = Hash("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f")
  private val fullDigest = Hash("ff" * 32)
  private val repeatedDigest = Hash("aa" * 32)
  private val zeroDigest = Hash.empty

  private def nibble(value: Int): Nibble = Nibble.unsafe(value.toByte)
  private def nibbles(values: Int*): Seq[Nibble] = values.map(nibble)

  private val leaf = Leaf(nibbles(10, 11, 12), ascendingDigest)
  private val branch = Branch(ListMap(nibble(15) -> fullDigest, nibble(0) -> ascendingDigest))
  private val extension = Extension(nibbles(1, 2, 3, 4), repeatedDigest)

  private val leafGolden = GoldenVectors.load("MerklePatriciaCommitment-Leaf-scodec-v1")
  private val branchGolden = GoldenVectors.load("MerklePatriciaCommitment-Branch-scodec-v1")
  private val extensionGolden = GoldenVectors.load("MerklePatriciaCommitment-Extension-scodec-v1")

  private def encode(value: MerklePatriciaCommitment): ByteVector = immutableCodec.immutableBytes(value)
  private def decode(bytes: ByteVector): Either[SerdeError, MerklePatriciaCommitment] = immutableCodec.fromImmutableBytes(bytes)
  private def encodeFails(value: MerklePatriciaCommitment): Boolean =
    codec.encode(value) match {
      case Attempt.Failure(_)    => true
      case Attempt.Successful(_) => false
    }

  private def branchWire(entries: List[(Int, Hash)]): ByteVector =
    ByteVector(0x01, entries.size) ++ entries.foldLeft(ByteVector.empty) {
      case (bytes, (key, digest)) => bytes ++ ByteVector(key) ++ ByteVector.fromValidHex(digest.value)
    }

  test("leaf has the frozen odd-path packed encoding") {
    expect(encode(leaf) == leafGolden).and(expect(decode(leafGolden) == Right(leaf)))
  }

  test("branch has the frozen ascending-entry encoding independent of Map insertion order") {
    val reverseInsertion = Branch(ListMap(nibble(0) -> ascendingDigest, nibble(15) -> fullDigest))

    expect(encode(branch) == branchGolden) &&
    expect(encode(reverseInsertion) == branchGolden) &&
    expect(decode(branchGolden) == Right(branch))
  }

  test("extension has the frozen even-path packed encoding") {
    expect(encode(extension) == extensionGolden).and(expect(decode(extensionGolden) == Right(extension)))
  }

  test("decode then encode reproduces every golden byte-for-byte") {
    val canonical = List(leafGolden, branchGolden, extensionGolden).forall { golden =>
      decode(golden).exists(value => encode(value) == golden)
    }

    expect(canonical)
  }

  test("empty and maximum-length paths round-trip at the uint16 boundary") {
    val empty = Leaf(Seq.empty, zeroDigest)
    val maximum = Leaf(Vector.fill(MerklePatriciaCommitmentScodecV1Codec.MaxPathNibbles)(nibble(15)), fullDigest)

    expect(decode(encode(empty)) == Right(empty)) &&
    expect(decode(encode(maximum)) == Right(maximum))
  }

  test("empty and 16-child branches round-trip at the uint8 semantic boundary") {
    val empty = Branch(Map.empty)
    val maximum = Branch((0 until MerklePatriciaCommitmentScodecV1Codec.MaxBranchChildren).map(value => nibble(value) -> zeroDigest).toMap)

    expect(decode(encode(empty)) == Right(empty)) &&
    expect(decode(encode(maximum)) == Right(maximum))
  }

  test("empty input, every unknown tag, and trailing bytes reject") {
    val leafBody = leafGolden.drop(1L)
    val unknownTagsReject = (3 to 255).forall(tag => decode(ByteVector(tag) ++ leafBody).isLeft)

    expect(decode(ByteVector.empty).isLeft) &&
    expect(unknownTagsReject) &&
    expect(decode(leafGolden ++ ByteVector(0x00)).isLeft)
  }

  test("truncated commitment variants reject") {
    val truncatedReject = List(leafGolden, branchGolden, extensionGolden).forall(bytes => decode(bytes.dropRight(1L)).isLeft)

    expect(truncatedReject)
  }

  test("odd paths reject a non-zero low-nibble pad") {
    val nonCanonical = ByteVector.fromValidHex("000001af" + zeroDigest.value)

    expect(decode(nonCanonical).isLeft)
  }

  test("paths reject invalid in-memory nibbles and values above the uint16 bound") {
    val invalidNibble = Leaf(Seq(nibble(16)), zeroDigest)
    val tooLong = Leaf(Vector.fill(MerklePatriciaCommitmentScodecV1Codec.MaxPathNibbles + 1)(nibble(0)), zeroDigest)

    expect(encodeFails(invalidNibble)) &&
    expect(encodeFails(tooLong))
  }

  test("branch decoding rejects counts above 16 before consuming entries") {
    decode(ByteVector(0x01, 0x11)) match {
      case Left(SerdeError.ScodecFailure(message)) => expect(message.contains("exceeds maximum 16"))
      case other                                   => failure(s"expected bounded-count failure, got $other")
    }
  }

  test("branch encoding rejects more than 16 children and invalid in-memory nibbles") {
    val tooMany = Branch((0 to 16).map(value => nibble(value) -> zeroDigest).toMap)
    val invalidNibble = Branch(Map(nibble(16) -> zeroDigest))

    expect(encodeFails(tooMany)) &&
    expect(encodeFails(invalidNibble))
  }

  test("branch decoding rejects wire nibbles outside [0,15]") {
    expect(decode(branchWire(List(16 -> zeroDigest))).isLeft)
  }

  test("branch decoding rejects duplicate and descending keys") {
    val duplicate = branchWire(List(1 -> zeroDigest, 1 -> fullDigest))
    val descending = branchWire(List(2 -> zeroDigest, 1 -> fullDigest))

    expect(decode(duplicate).isLeft) &&
    expect(decode(descending).isLeft)
  }

  test("commitment encoding rejects malformed and non-lowercase hashes") {
    val malformed = Leaf(Seq.empty, Hash("not-a-32-byte-digest"))
    val uppercase = Leaf(Seq.empty, Hash("AA" * 32))

    expect(encodeFails(malformed)) &&
    expect(encodeFails(uppercase))
  }
}
