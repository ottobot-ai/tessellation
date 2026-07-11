package io.constellationnetwork.serde

import cats.Eq
import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.Slot
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.slashing.InvalidStateProofEvidence
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.GlobalSnapshotCodecs._
import io.constellationnetwork.serde.codecs.instances.ShardingScodecCodecs._
import io.constellationnetwork.serde.implicits._
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.NonNegLong
import scodec.codecs.int32
import weaver.FunSuite

object ShardingScodecCodecsSuite extends FunSuite {

  private def hash(nibble: Char): Hash = Hash(nibble.toString * 64)
  private def address(label: String): Address = Address.fromBytes(label.getBytes("UTF-8"))
  private def peer(byte: String): PeerId = PeerId(Hex(byte * 64))

  private val shardId = ShardId.unsafeApply(2)
  private val metagraph = address("scodec-sharding-metagraph")
  private val submitter = peer("01")

  private val signature = CommitteeMemberSignature(
    peerId = submitter,
    vrfProof = Hex("aa" * 80),
    ed25519Sig = Hex("bb" * 64),
    kesProductSig = Hex("cc" * 128),
    kesTreeStep = 17
  )

  private val signedBinary = Signed(
    StateChannelSnapshotBinary(
      lastSnapshotHash = hash('1'),
      content = Array[Byte](1, 2, 3, 4),
      fee = SnapshotFee(NonNegLong.unsafeFrom(9L))
    ),
    NonEmptySet.of(SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
  )

  private val delta = ShardDerivedStateDelta(
    perMetagraphMptRoots = SortedMap(metagraph -> hash('a')),
    includedSnapshots = SortedMap(metagraph -> NonEmptyList.of(signedBinary))
  )

  private val checkpoint = ShardCheckpoint(
    shardId = shardId,
    parentCheckpointHash = hash('b'),
    shardOrdinal = ShardOrdinal(11L),
    gl0AnchorOrdinal = SnapshotOrdinal.unsafeApply(90L),
    slot = Slot.unsafeApply(91L),
    derivedStateDelta = delta,
    committeeSignatures = NonEmptyList.of(signature),
    epoch = EtaPeriod(3L),
    executionBaseOrdinal = SnapshotOrdinal.unsafeApply(89L)
  )

  private val fraudProof = FraudProofEnvelope(
    shardId = shardId,
    disputedCheckpointHash = hash('c'),
    metagraphAddress = metagraph,
    gl0AnchorOrdinal = checkpoint.gl0AnchorOrdinal,
    claimedDerivation = hash('d'),
    challengerDerivation = hash('e'),
    reexecutionWitness = Hex("de" * 32),
    challengerSignature = Hex("ad" * 64),
    submitterId = submitter
  )

  private val evidence = InvalidStateProofEvidence(
    shardId = shardId,
    disputedCheckpoint = checkpoint,
    metagraphAddress = metagraph,
    attestedRoot = hash('f'),
    fraudProof = fraudProof
  )

  private def roundTrip[A: Eq: ImmutableCodec](value: A): weaver.Expectations =
    value.immutableBytes.fromImmutableBytes[A] match {
      case Right(decoded) => expect(Eq[A].eqv(decoded, value))
      case Left(error)    => failure(s"decode failed: $error")
    }

  test("ShardId rejects a negative int32 at the binary boundary") {
    val negative = int32.encode(-1).require
    expect(shardIdCodec.decodeValue(negative).toEither.isLeft)
  }

  test("current sharding schema round-trips every binary record") {
    roundTrip(shardId) and
      roundTrip(ShardOrdinal(11L)) and
      roundTrip(signature) and
      roundTrip(delta) and
      roundTrip(checkpoint) and
      roundTrip(fraudProof) and
      roundTrip(evidence)
  }

  test("ShardCheckpoint binary encoding commits to executionBaseOrdinal") {
    val changed = checkpoint.copy(executionBaseOrdinal = SnapshotOrdinal.unsafeApply(88L))
    expect(checkpoint.immutableBytes != changed.immutableBytes)
  }

  private val stateProof = GlobalSnapshotStateProof(
    lastStateChannelSnapshotHashesProof = hash('1'),
    lastTxRefsProof = hash('2'),
    balancesProof = hash('3'),
    lastCurrencySnapshotsProof = None,
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
    lastGlobalSnapshotsWithCurrency = None,
    mptRoot = None,
    historicalStakeSnapshots = None,
    smtRoot = None
  )

  private val version =
    SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))

  private val snapshot = GlobalIncrementalSnapshot(
    ordinal = SnapshotOrdinal.unsafeApply(100L),
    height = Height(NonNegLong.unsafeFrom(1L)),
    subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
    lastSnapshotHash = hash('4'),
    blocks = SortedSet.empty,
    stateChannelSnapshots = SortedMap.empty,
    shardCheckpoints = SortedMap(shardId -> checkpoint),
    rewards = SortedSet.empty,
    delegateRewards = None,
    epochProgress = EpochProgress(NonNegLong.unsafeFrom(1L)),
    nextFacilitators = NonEmptyList.of(submitter),
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
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
    version = version,
    slotCertificate = None,
    eta = Some(hash('5')),
    fraudProofs = SortedSet(evidence)
  )

  test("GlobalIncrementalSnapshot binary round-trip preserves non-empty checkpoints and fraud proofs") {
    snapshot.immutableBytes.fromImmutableBytes[GlobalIncrementalSnapshot] match {
      case Right(decoded) =>
        expect.all(
          Eq[GlobalIncrementalSnapshot].eqv(decoded, snapshot),
          decoded.shardCheckpoints.nonEmpty,
          decoded.fraudProofs.nonEmpty
        )
      case Left(error) => failure(s"decode failed: $error")
    }
  }

  test("shardCheckpoints participates in canonical snapshot bytes") {
    val withoutCheckpoints = snapshot.copy(shardCheckpoints = SortedMap.empty)
    expect(snapshot.immutableBytes != withoutCheckpoints.immutableBytes)
  }

  test("fraudProofs participates in canonical snapshot bytes") {
    val withoutFraudProofs = snapshot.copy(fraudProofs = SortedSet.empty)
    expect(snapshot.immutableBytes != withoutFraudProofs.immutableBytes)
  }
}
