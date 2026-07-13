package io.constellationnetwork.serde

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.KesRegistrationOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotCodecs._
import io.constellationnetwork.serde.implicits._

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.FunSuite

/** Capstone round-trip suite for the top-level snapshot codecs. */
object GlobalSnapshotCodecsSuite extends FunSuite {

  private def v0 = SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))
  private def peer = PeerId(Hex("deadbeef"))

  private def baseTips = SnapshotTips(SortedSet.empty, SortedSet.empty)
  private def emptyV1Info = GlobalSnapshotInfoV1(SortedMap.empty, SortedMap.empty, SortedMap.empty)

  test("GlobalSnapshot (full) round-trips with minimal state") {
    val sample = GlobalSnapshot(
      ordinal = SnapshotOrdinal.MinValue,
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash("0" * 64),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      rewards = SortedSet.empty,
      epochProgress = EpochProgress(NonNegLong.unsafeFrom(0L)),
      nextFacilitators = NonEmptyList.of(peer),
      info = emptyV1Info,
      tips = baseTips
    )
    expect(sample.immutableBytes.fromImmutableBytes[GlobalSnapshot] == Right(sample))
  }

  test("GlobalIncrementalSnapshotV1 round-trips with minimal state") {
    val sample = GlobalIncrementalSnapshotV1(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(1L)),
      height = Height(NonNegLong.unsafeFrom(0L)),
      subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
      lastSnapshotHash = Hash("0" * 64),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      rewards = SortedSet.empty,
      epochProgress = EpochProgress(NonNegLong.unsafeFrom(0L)),
      nextFacilitators = NonEmptyList.of(peer),
      tips = baseTips,
      stateProof = GlobalSnapshotStateProofV1(Hash("a" * 64), Hash("b" * 64), Hash("c" * 64), None),
      version = v0
    )
    expect(sample.immutableBytes.fromImmutableBytes[GlobalIncrementalSnapshotV1] == Right(sample))
  }

  test("GlobalIncrementalSnapshot (current, 27 fields) round-trips with empty and populated operator-key registrations") {
    val stateProof = GlobalSnapshotStateProof(
      Hash("a" * 64),
      Hash("b" * 64),
      Hash("c" * 64),
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None,
      None
    )
    val sample = GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(10L)),
      height = Height(NonNegLong.unsafeFrom(0L)),
      subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
      lastSnapshotHash = Hash("0" * 64),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress(NonNegLong.unsafeFrom(0L)),
      nextFacilitators = NonEmptyList.of(peer),
      tips = baseTips,
      stateProof = stateProof,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = None,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      version = v0,
      slotCertificate = None,
      eta = None,
      fraudProofs = SortedSet.empty
    )
    val registration = Signed(
      KesRegistrationCert(
        operatorPeerId = peer,
        kesMasterVK = Hex("11" * 32),
        kesMasterVKStep = 0,
        offset = 2L,
        vrfPublicKey = Hex("22" * 32),
        effectiveFromPeriod = EtaPeriod(2L),
        registrationParentHash = Hash("3" * 64),
        ordinal = KesRegistrationOrdinal.first
      ),
      NonEmptySet.one(SignatureProof(peer.toId, Signature(Hex("44" * 64))))
    )
    val populated = sample.copy(operatorKeyRegistrations = SortedSet(registration))
    expect.all(
      sample.immutableBytes.fromImmutableBytes[GlobalIncrementalSnapshot] == Right(sample),
      populated.immutableBytes.fromImmutableBytes[GlobalIncrementalSnapshot] == Right(populated)
    )
  }
}
