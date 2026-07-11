package io.constellationnetwork.schema

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.parser.{decode, parse}
import io.circe.syntax._
import weaver.FunSuite

/** Circe round-trip suite for the `GlobalIncrementalSnapshot.shardCheckpoints` field landed by Slice 4 of
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §3.4.
  *
  * '''What this suite proves.''' Three contracts on the field:
  *
  *   1. '''Round-trip with non-empty content.''' A snapshot constructed with N `ShardId`-keyed `ShardCheckpoint` entries encodes via Circe
  *      and decodes back equal (including ordered iteration: `SortedMap` keys come out in `Ordering[ShardId]` order; per-checkpoint
  *      `NonEmptyList` element order preserved).
  *   1. '''Required wire shape.''' Omitting `shardCheckpoints` or `fraudProofs` is rejected; this greenfield fork has no deployed
  *      predecessor shape to support.
  *   1. '''Mixed with existing fields.''' Encoding/decoding does not regress any of the existing field round-trips — the encoder still
  *      emits a `shardCheckpoints` key, the decoder still requires the other 24 fields, and equality holds.
  *
  * '''What this suite does NOT exercise.''' State-application semantics (acceptance into MPT and fork choice) are covered elsewhere.
  *
  * '''Why `FunSuite`, not `MutableIOSuite`.''' Circe codecs are pure functions; no `IO` is needed. Matches `ShardingCodecsSuite` (Slice 1)
  * and `GlobalSnapshotCodecsSuite` (the existing top-level snapshot round-trip suite).
  */
object GlobalIncrementalSnapshotShardCheckpointsSuite extends FunSuite {

  // ===========================================================================
  // Shared fixtures — deterministic, mirror the style used in `ShardingCodecsSuite` so equivalent constructs stay byte-identical.
  // ===========================================================================

  private def hex(s: String): Hex = Hex(s)
  private def hash(seed: Char): Hash = Hash(seed.toString * 64)
  private def addr(tag: String): Address = Address.fromBytes(tag.getBytes("UTF-8"))

  private val mgAddrA: Address = addr("mg-aaa")
  private val mgAddrB: Address = addr("mg-bbb")

  private def peerIdN(n: Int): PeerId =
    PeerId(Hex(n.toHexString.padTo(2, '0') * 64))

  private val peerOne: PeerId = peerIdN(1)

  private val v0: SnapshotVersion =
    SnapshotVersion(Refined.unsafeApply[String, MatchesRegex["^(0\\.0\\.1|1\\.0\\.0)$"]]("0.0.1"))

  private val shardZero: ShardId = ShardId.unsafeApply(0)
  private val shardOne: ShardId = ShardId.unsafeApply(1)
  private val shardTwo: ShardId = ShardId.unsafeApply(2)

  // ---- Signed builder used for both the checkpoint envelope's inner pieces and the outer Signed[Snapshot] wrapper -----

  private def mkProof(id: String, sigHex: String): SignatureProof =
    SignatureProof(Id(hex(id)), Signature(hex(sigHex)))

  // ---- Build a non-empty `ShardCheckpoint` --------------------------------

  private def mkCommitteeSig(peerN: Int, step: Int): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = peerIdN(peerN),
      vrfProof = hex("aa" * 80),
      ed25519Sig = hex("bb" * 64),
      kesProductSig = hex("cc" * 128),
      kesTreeStep = step
    )

  private def mkSignedBinary(seed: Char): Signed[StateChannelSnapshotBinary] =
    Signed(
      StateChannelSnapshotBinary(
        lastSnapshotHash = hash(seed),
        content = Array.fill(8)(0x01.toByte),
        fee = SnapshotFee(NonNegLong.unsafeFrom(10L))
      ),
      NonEmptySet.of(mkProof("11" * 64, "22" * 70))
    )

  private def mkDelta(mgAddr: Address): ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mgAddr -> hash('a')),
      includedSnapshots = SortedMap(mgAddr -> NonEmptyList.of(mkSignedBinary('s')))
    )

  private def mkCheckpoint(shard: ShardId, mgAddr: Address): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shard,
      parentCheckpointHash = hash('p'),
      shardOrdinal = ShardOrdinal(42L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(99L)),
      slot = SlotT.unsafeApply(99L),
      derivedStateDelta = mkDelta(mgAddr),
      committeeSignatures = NonEmptyList.of(mkCommitteeSig(1, 7), mkCommitteeSig(2, 8)),
      epoch = EtaPeriod(5L)
    )

  // ---- Build a `GlobalIncrementalSnapshot` ---------------------------------

  private def baseStateProof: GlobalSnapshotStateProof =
    GlobalSnapshotStateProof(
      lastStateChannelSnapshotHashesProof = Hash("a" * 64),
      lastTxRefsProof = Hash("b" * 64),
      balancesProof = Hash("c" * 64),
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

  private def mkSnapshot(shardCps: SortedMap[ShardId, ShardCheckpoint]): GlobalIncrementalSnapshot =
    GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(10L)),
      height = Height(NonNegLong.unsafeFrom(0L)),
      subHeight = SubHeight(NonNegLong.unsafeFrom(0L)),
      lastSnapshotHash = Hash("0" * 64),
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = shardCps,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress(NonNegLong.unsafeFrom(0L)),
      nextFacilitators = NonEmptyList.of(peerOne),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = baseStateProof,
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
      eta = None
    )

  // ---- Helper: equality on `GlobalIncrementalSnapshot` via cats Eq ---------

  private def decodeSnapshot(jsonStr: String): GlobalIncrementalSnapshot =
    decode[GlobalIncrementalSnapshot](jsonStr)
      .fold(err => throw new AssertionError(s"decode failed: ${err.getMessage}; json=$jsonStr"), identity)

  // ===========================================================================
  // Test 1 — non-empty round-trip
  // ===========================================================================

  test("GlobalIncrementalSnapshot.shardCheckpoints — non-empty SortedMap round-trips through Circe (3 shards)") {
    val checkpoints = SortedMap(
      shardZero -> mkCheckpoint(shardZero, mgAddrA),
      shardOne -> mkCheckpoint(shardOne, mgAddrB),
      shardTwo -> mkCheckpoint(shardTwo, mgAddrA)
    )
    val snapshot = mkSnapshot(checkpoints)
    val jsonStr = snapshot.asJson.noSpaces

    val decoded = decodeSnapshot(jsonStr)
    // Use cats `===` instead of Scala `==`: `ShardDerivedStateDelta.includedSnapshots` transitively carries `Array[Byte]`
    // (`StateChannelSnapshotBinary.content`) whose default equality is reference-identity. Same rationale as `ShardingCodecsSuite.roundtrip`.
    expect.all(
      cats.Eq[GlobalIncrementalSnapshot].eqv(decoded, snapshot),
      // Spot-check the map structure: same keys, same iteration order, same first-shard's content
      decoded.shardCheckpoints.keys.toList == checkpoints.keys.toList,
      decoded.shardCheckpoints.size == 3
    )
  }

  // ===========================================================================
  // Test 2 — default-empty: explicit empty map round-trips
  // ===========================================================================

  test("GlobalIncrementalSnapshot.shardCheckpoints — explicit SortedMap.empty round-trips through Circe") {
    val snapshot = mkSnapshot(SortedMap.empty[ShardId, ShardCheckpoint])
    val jsonStr = snapshot.asJson.noSpaces

    val decoded = decodeSnapshot(jsonStr)
    expect.all(
      cats.Eq[GlobalIncrementalSnapshot].eqv(decoded, snapshot),
      decoded.shardCheckpoints.isEmpty
    )
  }

  // ===========================================================================
  // Current greenfield schema rejects a missing checkpoint field.
  //
  // Fork-only consensus fields have no compatibility default.
  // ===========================================================================

  test("GlobalIncrementalSnapshot.shardCheckpoints — missing key is rejected") {
    val snapshotWithEmpty = mkSnapshot(SortedMap.empty[ShardId, ShardCheckpoint])
    val jsonStr = snapshotWithEmpty.asJson.noSpaces
    val parsedJson = parse(jsonStr).fold(err => throw new AssertionError(s"parse failed: ${err.getMessage}"), identity)
    val strippedJson = parsedJson.hcursor
      .downField("shardCheckpoints")
      .delete
      .top
      .getOrElse(throw new AssertionError("could not strip shardCheckpoints key from JSON"))

    val strippedStr = strippedJson.noSpaces
    expect.all(
      !strippedStr.contains("shardCheckpoints"), // guard: confirm we actually stripped the key
      decode[GlobalIncrementalSnapshot](strippedStr).isLeft
    )
  }

  // ===========================================================================
  // Test 4 — encoder always emits the key (so post-Slice-4 round-trips include it)
  // ===========================================================================

  test("GlobalIncrementalSnapshot encoder always emits a `shardCheckpoints` JSON key (even when empty)") {
    val snapshot = mkSnapshot(SortedMap.empty[ShardId, ShardCheckpoint])
    val jsonStr = snapshot.asJson.noSpaces
    expect(jsonStr.contains("\"shardCheckpoints\""))
  }

  // ===========================================================================
  // Test 5 — SortedMap key ordering preserved across encode/decode
  //
  // The map is keyed by `ShardId` (refined `NonNegInt` ordering); insertion order in this test deliberately reverses canonical order
  // to confirm `SortedMap` normalizes by key ordering and the JSON encoding preserves that — important because per-snapshot encoded
  // bytes affect any downstream hashing.
  // ===========================================================================

  test("GlobalIncrementalSnapshot.shardCheckpoints — SortedMap key order preserved across encode/decode regardless of insertion order") {
    val reverseInsertion: SortedMap[ShardId, ShardCheckpoint] = SortedMap(
      shardTwo -> mkCheckpoint(shardTwo, mgAddrA),
      shardZero -> mkCheckpoint(shardZero, mgAddrA),
      shardOne -> mkCheckpoint(shardOne, mgAddrA)
    )
    val snapshot = mkSnapshot(reverseInsertion)
    val jsonStr = snapshot.asJson.noSpaces
    val decoded = decodeSnapshot(jsonStr)
    expect.same(
      List(shardZero, shardOne, shardTwo),
      decoded.shardCheckpoints.keys.toList
    )
  }

  // ===========================================================================
  // W3a — WATCHTOWER fraud-proof field round-trip: the `fraudProofs` consensus artifact follows the EXACT `shardCheckpoints` contract
  //   (a dedicated, defaulted, forgiving-decoded field), so the same three properties hold: non-empty round-trip, missing-key → empty
  //   (pre-watchtower wire compat), and the encoder always emits the key.
  // ===========================================================================

  private def mkFraudProofEnvelope(shard: ShardId, cpHash: Hash, mgAddr: Address): FraudProofEnvelope =
    FraudProofEnvelope(
      shardId = shard,
      disputedCheckpointHash = cpHash,
      metagraphAddress = mgAddr,
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(99L)),
      claimedDerivation = hash('a'),
      challengerDerivation = hash('b'),
      reexecutionWitness = hex("dd" * 32),
      challengerSignature = hex("ee" * 64),
      submitterId = peerOne
    )

  private def mkEvidence(
    shard: ShardId,
    mgAddr: Address,
    cpHashSeed: Char
  ): io.constellationnetwork.schema.slashing.InvalidStateProofEvidence = {
    val cp = mkCheckpoint(shard, mgAddr)
    io.constellationnetwork.schema.slashing.InvalidStateProofEvidence(
      shardId = shard,
      disputedCheckpoint = cp,
      metagraphAddress = mgAddr,
      attestedRoot = hash('a'),
      fraudProof = mkFraudProofEnvelope(shard, hash(cpHashSeed), mgAddr)
    )
  }

  test("GlobalIncrementalSnapshot.fraudProofs — non-empty SortedSet round-trips through Circe") {
    val proofs = SortedSet(mkEvidence(shardZero, mgAddrA, 'x'), mkEvidence(shardOne, mgAddrB, 'y'))
    val snapshot = mkSnapshot(SortedMap.empty).copy(fraudProofs = proofs)
    val jsonStr = snapshot.asJson.noSpaces
    val decoded = decodeSnapshot(jsonStr)
    expect.all(
      cats.Eq[GlobalIncrementalSnapshot].eqv(decoded, snapshot),
      decoded.fraudProofs.size == 2,
      decoded.fraudProofs.map(_.shardId).toList == List(shardZero, shardOne)
    )
  }

  test("GlobalIncrementalSnapshot.fraudProofs — missing key is rejected") {
    val snapshot = mkSnapshot(SortedMap.empty) // fraudProofs defaults to empty
    val jsonStr = snapshot.asJson.noSpaces
    val parsedJson = parse(jsonStr).fold(err => throw new AssertionError(s"parse failed: ${err.getMessage}"), identity)
    val strippedJson = parsedJson.hcursor
      .downField("fraudProofs")
      .delete
      .top
      .getOrElse(throw new AssertionError("could not strip fraudProofs key from JSON"))
    val strippedStr = strippedJson.noSpaces
    expect.all(
      !strippedStr.contains("fraudProofs"),
      decode[GlobalIncrementalSnapshot](strippedStr).isLeft
    )
  }

  test("GlobalIncrementalSnapshot encoder always emits a `fraudProofs` JSON key (even when empty)") {
    val snapshot = mkSnapshot(SortedMap.empty)
    val jsonStr = snapshot.asJson.noSpaces
    expect(jsonStr.contains("\"fraudProofs\""))
  }
}
