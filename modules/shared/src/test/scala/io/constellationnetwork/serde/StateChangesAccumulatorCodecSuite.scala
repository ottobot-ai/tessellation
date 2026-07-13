package io.constellationnetwork.serde

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.{SystemIndexDelta, TokenLockExpiryKey}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.AddressCodec
import io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite for the per-ordinal `StateChangesAccumulator` wire codec (currency-l0 adopt path, task #14). */
object StateChangesAccumulatorCodecSuite extends FunSuite {

  test("empty accumulator round-trips (all partitions empty)") {
    val empty = StateChangesAccumulator()
    expect(empty.immutableBytes.fromImmutableBytes[StateChangesAccumulator] == Right(empty))
  }

  test("accumulator with a populated expiry-index delta round-trips") {
    val addr = AddressCodec.unsafeFromLiteral("DAG6kfTqFxLLPLopHqR43CeQrcvJ5k3eXgYSeELt")
    val hash: Hash = Hash("0000000000000000000000000000000000000000000000000000000000000001")
    val bucket = SystemIndexDelta.EpochBucket[TokenLockExpiryKey](
      adds = SortedMap(EpochProgress(10L) -> Set(TokenLockExpiryKey(addr, hash))),
      removes = SortedMap.empty
    )
    val sample = StateChangesAccumulator(
      tokenLockExpiryIndex = bucket,
      removedTokenLockKeys = Set(addr),
      removedHistoricalStakeSnapshotKeys = Set.empty
    )
    expect(sample.immutableBytes.fromImmutableBytes[StateChangesAccumulator] == Right(sample))
  }

  test("accumulator with unified KES+VRF history and latest pointer round-trips") {
    val peerId = PeerId(Hex("11" * 64))
    val cert = KesRegistrationCert(
      peerId,
      Hex("22" * 32),
      0,
      2L,
      Hex("33" * 32),
      EtaPeriod(2L),
      Hash("44" * 32),
      KesRegistrationOrdinal.first
    )
    val signed = Signed(cert, NonEmptySet.one(SignatureProof(peerId.toId, Signature(Hex("55" * 64)))))
    val acceptedAt = SnapshotOrdinal(NonNegLong(7L))
    val ref = KesRegistrationReference(cert.ordinal, Hash("66" * 32))
    val sample = StateChangesAccumulator(
      kesRegistrationCerts = SortedMap(peerId -> SortedSet(KesRegistrationRecord(signed, acceptedAt))),
      lastKesRegistrationRefs = SortedMap(peerId -> ref)
    )
    expect(sample.immutableBytes.fromImmutableBytes[StateChangesAccumulator] == Right(sample))
  }
}
