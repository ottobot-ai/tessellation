package io.constellationnetwork.serde

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.util.Try

import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.NonEmptySetCodec.nonEmptySetCanonical
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMapCanonical
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSetCanonical
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec, proofsCodec => proofsHashCodec}
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignatureCodecs.idCodec
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}

import scodec.codecs.uint8
import scodec.{Attempt, Codec}
import weaver.FunSuite

object WireCanonicalOrderSuite extends FunSuite {

  private val upperFirstHex = "B0"
  private val lowerFirstHex = "a0"
  private val upperFirstHash = upperFirstHex + "00" * 31
  private val lowerFirstHash = lowerFirstHex + "00" * 31

  private def roundTrip[A](codec: Codec[A], value: A): Either[String, A] =
    codec
      .encode(value)
      .flatMap(codec.complete.decodeValue)
      .toEither
      .left
      .map(_.messageWithContext)

  test("SortedMap canonicalizes mixed-case PeerId keys before strict decode") {
    val codec = sortedMapCanonical(peerIdCodec, uint8)
    val source = SortedMap(PeerId(Hex(upperFirstHex)) -> 1, PeerId(Hex(lowerFirstHex)) -> 2)

    expect(
      roundTrip(codec, source).map(_.keysIterator.map(_.value.value).toList) == Right(List(lowerFirstHex, upperFirstHex.toLowerCase))
    )
  }

  test("SortedMap canonicalizes mixed-case Id keys before strict decode") {
    val codec = sortedMapCanonical(idCodec, uint8)
    val source = SortedMap(Id(Hex(upperFirstHex)) -> 1, Id(Hex(lowerFirstHex)) -> 2)

    expect(
      roundTrip(codec, source).map(_.keysIterator.map(_.hex.value).toList) == Right(List(lowerFirstHex, upperFirstHex.toLowerCase))
    )
  }

  test("SortedSet canonicalizes mixed-case Hash and ProofsHash elements before strict decode") {
    val hashesCodec = sortedSetCanonical(hashCodec)
    val proofsHashesCodec = sortedSetCanonical(proofsHashCodec)
    val hashes = SortedSet(Hash(upperFirstHash), Hash(lowerFirstHash))
    val proofsHashes = SortedSet(ProofsHash(upperFirstHash), ProofsHash(lowerFirstHash))
    val expected = List(lowerFirstHash, upperFirstHash.toLowerCase)

    expect(roundTrip(hashesCodec, hashes).map(_.iterator.map(_.value).toList) == Right(expected))
      .and(expect(roundTrip(proofsHashesCodec, proofsHashes).map(_.iterator.map(_.value).toList) == Right(expected)))
  }

  test("SortedSet canonicalizes outer Signed values before strict decode") {
    val signedHashCodec = signedCodecFor(hashCodec)
    val codec = sortedSetCanonical(signedHashCodec)
    val source = SortedSet(
      Signed(Hash(upperFirstHash), NonEmptySet.of(proof(upperFirstHex, "11"))),
      Signed(Hash(lowerFirstHash), NonEmptySet.of(proof(lowerFirstHex, "22")))
    )

    expect(roundTrip(codec, source).map(_.size) == Right(2))
  }

  test("NonEmptySet canonicalizes outer Signed proofs before strict decode") {
    val signedHashCodec = signedCodecFor(hashCodec)
    val codec = nonEmptySetCanonical(signedHashCodec)
    val value = Hash("00" * 32)
    val source = NonEmptySet.of(
      Signed(value, NonEmptySet.of(proof(upperFirstHex, "11"))),
      Signed(value, NonEmptySet.of(proof(lowerFirstHex, "22")))
    )

    expect(roundTrip(codec, source).map(_.toSortedSet.size) == Right(2))
  }

  test("canonical collection encode returns Attempt.Failure instead of throwing for malformed Hex") {
    val codec = sortedMapCanonical(peerIdCodec, uint8)
    val source = SortedMap(PeerId(Hex("not-hex")) -> 1)

    expect(Try(codec.encode(source)).toOption.exists {
      case Attempt.Failure(_) => true
      case _                  => false
    })
  }

  test("canonical collection encode rejects source keys that collapse to one wire key") {
    val codec = sortedMapCanonical(peerIdCodec, uint8)
    val source = SortedMap(PeerId(Hex("B0")) -> 1, PeerId(Hex("b0")) -> 2)

    expect(codec.encode(source) match {
      case Attempt.Failure(_) => true
      case _                  => false
    })
  }

  private def proof(id: String, signature: String): SignatureProof =
    SignatureProof(Id(Hex(id)), Signature(Hex(signature)))
}
