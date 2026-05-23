package io.constellationnetwork.schema

import cats.data.{NonEmptyList, NonEmptySet}

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.SnapshotFee
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
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
  * '''What this suite proves.''' Three contracts on the new field:
  *
  *   1. '''Round-trip with non-empty content.''' A snapshot constructed with N `ShardId`-keyed `ShardCheckpoint` entries encodes via Circe
  *      and decodes back equal (including ordered iteration: `SortedMap` keys come out in `Ordering[ShardId]` order; per-checkpoint
  *      `NonEmptyList` element order preserved).
  *   1. '''Default-empty path.''' Since the case-class default for the field is `SortedMap.empty`, a snapshot whose JSON omits the
  *      `shardCheckpoints` key (the pre-Slice-4 wire shape) still decodes — because the project keeps `SlotCertificate`-style "forgiving
  *      decoder" precedent (`schema.nakamoto.slot.SlotCertificate.decoder`) and `GlobalIncrementalSnapshot.decoder` mirrors it for this
  *      field.
  *   1. '''Mixed with existing fields.''' Encoding/decoding does not regress any of the existing field round-trips — the encoder still
  *      emits a `shardCheckpoints` key, the decoder still requires the other 24 fields, and equality holds.
  *
  * '''What this suite does NOT exercise.''' No state-application semantics (acceptance into MPT, fork-choice, etc.) — those land in later
  * slices (9, 13). No scodec wire-format coverage — `JsonScodecParitySuite` proves scodec parity on pre-Slice-4 fixtures continues to
  * work, and a dedicated `ShardingScodecCodecs` package (for non-empty `ShardCheckpoint` scodec encoding) is deferred to a follow-up slice
  * (see scaladoc in `GlobalSnapshotCodecs.globalIncrementalSnapshotCodec`).
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
  private val holderA: Address = addr("holder-1")
  private val holderB: Address = addr("holder-2")

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

  private def mkDelta(mgAddr: Address, lockHolderAmount: Long): ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap(mgAddr -> hash('a')),
      includedSnapshots = SortedMap(mgAddr -> NonEmptyList.of(mkSignedBinary('s'))),
      tokenLockBalancesDelta = SortedMap(
        mgAddr -> SortedMap(
          holderA -> io.constellationnetwork.schema.balance.Balance(NonNegLong.unsafeFrom(lockHolderAmount)),
          holderB -> io.constellationnetwork.schema.balance.Balance(NonNegLong.unsafeFrom(lockHolderAmount * 2L))
        )
      ),
      perMetagraphArtifacts = SortedMap.empty,
      perMetagraphSyncDataDelta = SortedMap(
        mgAddr -> MetagraphSyncDataInfo(
          globalOrdinalLastAcceptedOn = SnapshotOrdinal(NonNegLong.unsafeFrom(7L)),
          globalEpochProgressLastAcceptedOn = EpochProgress(NonNegLong.unsafeFrom(3L)),
          unappliedGlobalChangeOrdinals = SortedSet.empty
        )
      )
    )

  private def mkCheckpoint(shard: ShardId, mgAddr: Address, lockHolderAmount: Long): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shard,
      parentCheckpointHash = hash('p'),
      shardOrdinal = ShardOrdinal(42L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(99L)),
      derivedStateDelta = mkDelta(mgAddr, lockHolderAmount),
      emittedReceipts = List.empty,
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
      historicalStakeSnapshots = None
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
      shardZero -> mkCheckpoint(shardZero, mgAddrA, 100L),
      shardOne -> mkCheckpoint(shardOne, mgAddrB, 200L),
      shardTwo -> mkCheckpoint(shardTwo, mgAddrA, 300L)
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
  // Test 3 — forgiving decoder: missing `shardCheckpoints` key in JSON (pre-Slice-4 fixtures) decodes to empty map
  //
  // This is the load-bearing test for the `SlotCertificate`-style forgiving decoder in `GlobalIncrementalSnapshot.decoder`. Pre-Slice-4
  // brotli fixtures in `JsonScodecParitySuite` (e.g. `incremental_snapshot_ordinal_700.brotli`) lack the field; the suite-wide contract
  // is "missing key → default empty map" via the project precedent in `schema.nakamoto.slot.SlotCertificate.decoder`.
  // ===========================================================================

  test("GlobalIncrementalSnapshot.shardCheckpoints — missing key in JSON decodes to SortedMap.empty (pre-Slice-4 wire compat)") {
    // Build a snapshot and serialize, then strip out the `shardCheckpoints` field from the JSON object — emulating a pre-Slice-4 fixture.
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
      // The forgiving decoder fills in `SortedMap.empty` for the missing field.
      decode[GlobalIncrementalSnapshot](strippedStr).map(_.shardCheckpoints.isEmpty) == Right(true)
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
      shardTwo -> mkCheckpoint(shardTwo, mgAddrA, 1L),
      shardZero -> mkCheckpoint(shardZero, mgAddrA, 2L),
      shardOne -> mkCheckpoint(shardOne, mgAddrA, 3L)
    )
    val snapshot = mkSnapshot(reverseInsertion)
    val jsonStr = snapshot.asJson.noSpaces
    val decoded = decodeSnapshot(jsonStr)
    expect.same(
      List(shardZero, shardOne, shardTwo),
      decoded.shardCheckpoints.keys.toList
    )
  }
}
