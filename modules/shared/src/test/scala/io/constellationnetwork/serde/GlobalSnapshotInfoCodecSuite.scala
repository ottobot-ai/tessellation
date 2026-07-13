package io.constellationnetwork.serde

import cats.data.NonEmptySet

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.slot.VrfPublicKey
import io.constellationnetwork.schema.nakamoto.{EtaPeriod, GenesisOperatorConsensusKey}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotInfoCodec._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Round-trip suite for the 21-field `GlobalSnapshotInfo` capstone codec. */
object GlobalSnapshotInfoCodecSuite extends FunSuite {

  private def empty = GlobalSnapshotInfo(
    lastStateChannelSnapshotHashes = SortedMap.empty,
    lastTxRefs = SortedMap.empty,
    balances = SortedMap.empty,
    lastCurrencySnapshots = SortedMap.empty,
    lastCurrencySnapshotsProofs = SortedMap.empty,
    activeAllowSpends = None,
    activeTokenLocks = None,
    tokenLockBalances = None,
    lastAllowSpendRefs = None,
    lastTokenLockRefs = None,
    updateNodeParameters = None,
    activeDelegatedStakes = None,
    delegatedStakesWithdrawals = None,
    activeNodeCollaterals = None,
    nodeCollateralWithdrawals = None,
    priceState = None,
    metagraphSyncData = None,
    historicalStakeSnapshots = SortedMap.empty
  )

  test("Empty GlobalSnapshotInfo round-trips (all options absent, all maps empty)") {
    expect(empty.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(empty))
  }

  test("Empty GlobalSnapshotInfo has a minimal encoded size") {
    // 3 required maps (6 uint16 zero prefixes, 2 bytes each = 6 bytes for 3 count-only maps = 6 bytes,
    // but actually 2 more required maps = 5 × 2 = 10 bytes) + 12 absent-option discriminators (12 bytes)
    //   required-map count prefixes: 5 × 2 = 10 bytes + 1 for historicalStakeSnapshots = 12 bytes
    //   option discriminators:        12 × 1 = 12 bytes
    //   unified KES+VRF history/ref maps: 2 × 2 = 4 bytes
    //   immutable genesis operator-key map: 2 bytes
    //   total:                        30 bytes
    expect(empty.immutableBytes.length == 30L)
  }

  test("GlobalSnapshotInfo with Some(empty) options round-trips") {
    val sample = empty.copy(
      activeAllowSpends = Some(SortedMap.empty),
      activeTokenLocks = Some(SortedMap.empty),
      tokenLockBalances = Some(SortedMap.empty),
      lastAllowSpendRefs = Some(SortedMap.empty),
      lastTokenLockRefs = Some(SortedMap.empty),
      updateNodeParameters = Some(SortedMap.empty),
      activeDelegatedStakes = Some(SortedMap.empty),
      delegatedStakesWithdrawals = Some(SortedMap.empty),
      activeNodeCollaterals = Some(SortedMap.empty),
      nodeCollateralWithdrawals = Some(SortedMap.empty),
      priceState = Some(SortedMap.empty),
      metagraphSyncData = Some(SortedMap.empty)
    )
    expect(sample.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(sample))
  }

  test("GlobalSnapshotInfo with unified operator-key history and latest pointer round-trips") {
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
    val ref = KesRegistrationReference(cert.ordinal, Hash("66" * 32))
    val sample = empty.copy(
      kesRegistrationCerts = SortedMap(
        peerId -> SortedSet(KesRegistrationRecord(signed, SnapshotOrdinal(NonNegLong(7L))))
      ),
      lastKesRegistrationRefs = SortedMap(peerId -> ref)
    )
    expect(sample.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(sample))
  }

  test("GlobalSnapshotInfo with immutable rooted genesis operator identity round-trips") {
    val peerId = PeerId(Hex("11" * 64))
    val record = GenesisOperatorConsensusKey(
      networkMagic = "test",
      activationOrdinal = 0L,
      startingEpochProgress = 0L,
      operatorPeerId = peerId,
      operatorAddress = Address.fromBytes("operator".getBytes("UTF-8")),
      kesMasterVerificationKey = Hex("22" * 32),
      kesMasterVerificationKeyStep = 0,
      kesPeriodOffset = 0L,
      vrfPublicKey = VrfPublicKey(Hex("33" * 32)),
      longTermSignature = Signature(Hex("44" * 64))
    )
    val sample = empty.copy(genesisOperatorKeys = SortedMap(peerId -> record))

    expect(sample.immutableBytes.fromImmutableBytes[GlobalSnapshotInfo] == Right(sample))
  }
}
