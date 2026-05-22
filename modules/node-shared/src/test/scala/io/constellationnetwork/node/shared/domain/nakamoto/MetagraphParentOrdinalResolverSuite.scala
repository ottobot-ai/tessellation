package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.{IO, Ref, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.MetagraphParentOrdinalResolver.HistoryLookup
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.schema.transaction.RewardTransaction
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.SelfAwareStructuredLogger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import weaver.MutableIOSuite

/** Spec assertions for [[MetagraphParentOrdinalResolver]] with the v2 history-walk fallback.
  *
  * Coverage maps to the five paths the resolver must navigate at 4-metagraph stress:
  *
  *   1. ''Tip-match fast path'' — gl0 GSI tip equals the binary's `parentHash`, resolver returns the ordinal from
  *      `LastIncrementalCurrencySnapshots`.
  *   1. ''Historical match'' — gl0 GSI tip is stale (an ancestor of the cluster-canonical view); the binary's parent matches a known past
  *      ordinal via the history walk.
  *   1. ''True unknown parent'' — neither tip nor history matches; resolver returns `None` (fail-closed).
  *   1. ''Eviction survival'' — even when the in-memory tip cache (the GSI's `LastStateChannelSnapshotHashes`) reports `None` for the
  *      metagraph (e.g. recovery reset wiped pending state), the history walk recovers the ordinal.
  *   1. ''Multi-metagraph isolation'' — a binary from metagraph X whose parent happens to match a metagraph Y binary hash does NOT
  *      cross-resolve (history walk keyed on `(mg, hash)`).
  *
  * The resolver's fast path uses three reader accessors (`getLastStateChannelSnapshotHash`, `getLastIncrementalCurrencySnapshot`,
  * `getLastCurrencySnapshot`) — all reduce to `reader.get[V](GlobalStateKey)`. The tests use a `StubReader` that holds a `Map[GlobalStateKey,
  * Any]` and returns matches against pure type-tags carried by the test fixtures. This avoids depending on the full `MptStore` codec
  * machinery while exercising the same surface the production code reads.
  */
object MetagraphParentOrdinalResolverSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private implicit val logger: SelfAwareStructuredLogger[IO] =
    Slf4jLogger.getLoggerFromName[IO]("MetagraphParentOrdinalResolverSuite")

  // -------------- test fixtures --------------

  private val mgA: Address = Address("DAG0y4eLqhhXUafeE3mgBstezPTnr8L3tZjAtMWB")
  private val mgB: Address = Address("DAG07tqNLYW8jHU9emXcRTT3CfgCUoumwcLghopd")

  private def mkHash(seed: String): Hash =
    Hash(seed.padTo(64, '0').take(64))

  private val testSignatureProof = SignatureProof(Id(Hex("")), Signature(Hex("")))
  private val testProofs: NonEmptySet[SignatureProof] = NonEmptySet.one(testSignatureProof)

  /** Build a minimal `Signed[CurrencyIncrementalSnapshot]` with only the ordinal field load-bearing — the resolver consumes only
    * `.value.ordinal.value.value`, so the rest can stay at sensible defaults.
    */
  private def mkSignedIncrementalSnapshot(ord: Long): Signed[CurrencyIncrementalSnapshot] = {
    val inc = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty[RewardTransaction],
      tips = io.constellationnetwork.schema.SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      epochProgress = EpochProgress.MinValue,
      dataApplication = None,
      messages = None,
      globalSnapshotSyncs = None,
      feeTransactions = None,
      artifacts = None,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      globalSyncView = None,
      version = SnapshotVersion("0.0.1")
    )
    Signed(inc, testProofs)
  }

  /** Build a minimal `Signed[CurrencySnapshot]` (the Left side, genesis-only window). Resolver consumes only `.value.ordinal.value.value`. */
  private def mkSignedGenesisSnapshot(ord: Long): Signed[CurrencySnapshot] = {
    val genesis = CurrencySnapshot(
      ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(ord)),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty[RewardTransaction],
      tips = io.constellationnetwork.schema.SnapshotTips(SortedSet.empty, SortedSet.empty),
      info = CurrencySnapshotInfoV1(SortedMap.empty, SortedMap.empty),
      epochProgress = EpochProgress.MinValue
    )
    Signed(genesis, testProofs)
  }

  /** Stub reader that pattern-matches on `GlobalStateKey` and returns canned values from a per-test fixture map.
    *
    * Why a stub rather than a real `MptStore.fromMptStore`. The resolver consumes `reader.get[Hash]`, `reader.get[Signed[CurrencyIncrementalSnapshot]]`,
    * and `reader.get[Signed[CurrencySnapshot]]`. Round-tripping the last two through an MPT codec requires the full ImmutableCodec instance
    * for `Signed[...]` over the snapshot types — heavy lifting irrelevant to the resolver's logic. The stub interprets the
    * `(metagraphAddress, GlobalStateFieldId)` tuple directly, returning whatever the test fixture stored under it. The price: tests can't
    * assert codec parity with production (that's covered elsewhere — `GsamWritePathParitySuite`), but the dispatch + None-vs-Some + history
    * lookup are all faithfully exercised.
    */
  private final class StubReader(
    entries: Map[(Address, GlobalStateFieldId), Any]
  ) extends GlobalStateReader[IO] {
    def get[V: ImmutableCodec](key: GlobalStateKey): IO[Option[V]] =
      IO.pure(decodeKey(key).flatMap(entries.get).map(_.asInstanceOf[V]))

    def getMany[V: ImmutableCodec](keys: List[GlobalStateKey]): IO[Map[GlobalStateKey, V]] =
      keys.traverse(k => get[V](k).map(_.map(k -> _))).map(_.flatten.toMap)

    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): IO[Map[Hex, V]] =
      IO.pure(Map.empty)

    /** Recover the `(Address, FieldId)` tuple from a `GlobalStateKey.metagraph(...)`. The key's binary shape is opaque here — we mirror the
      * production key constructor and recover the original tuple by passing the test fixture's `mkKey` factory through the same path.
      */
    private def decodeKey(key: GlobalStateKey): Option[(Address, GlobalStateFieldId)] =
      // Production key construction is `GlobalStateKey.metagraph(addr, fieldId)`. We compare on the rendered hex to recover the tuple, since
      // GlobalStateKey is opaque at this layer. The fixture pre-renders every (mg, fid) pair the test uses; missing entries return None.
      keyToTuple.get(key)

    val keyToTuple: Map[GlobalStateKey, (Address, GlobalStateFieldId)] = entries.keys.map { case (addr, fid) =>
      GlobalStateKey.metagraph(addr, fid) -> (addr, fid)
    }.toMap
  }

  private object StubReader {
    def make(entries: ((Address, GlobalStateFieldId), Any)*): StubReader =
      new StubReader(entries.toMap)
  }

  /** History stub that returns canned per-`(mg, hash)` ordinals. Tracks every lookup invocation in a Ref so tests can assert ''how'' the
    * resolver consulted history (e.g. that it queried with the correct metagraph isolation).
    */
  private final class StubHistory(
    entries: Map[(Address, Hash), Long],
    invocations: Ref[IO, List[(Address, Hash)]]
  ) extends HistoryLookup[IO] {
    def lookup(metagraphAddress: Address, scBinaryHash: Hash): IO[Option[Long]] =
      invocations.update((metagraphAddress, scBinaryHash) :: _) *>
        IO.pure(entries.get((metagraphAddress, scBinaryHash)))
  }

  private object StubHistory {
    def make(entries: ((Address, Hash), Long)*): IO[(StubHistory, Ref[IO, List[(Address, Hash)]])] =
      Ref.of[IO, List[(Address, Hash)]](Nil).map(ref => (new StubHistory(entries.toMap, ref), ref))
  }

  // -------------- tests --------------

  test("(1) tip-match fast path: parentHash equals gl0-recorded tip → returns ordinal from LastIncrementalCurrencySnapshots") { _ =>
    val tipHash = mkHash("tip-")
    val incrementalOrdinal = 42L
    val reader = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> tipHash,
      (mgA, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> mkSignedIncrementalSnapshot(incrementalOrdinal)
    )
    for {
      (history, invocations) <- StubHistory.make()
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, tipHash, history)
      historyCalls <- invocations.get
    } yield
      expect.eql(Some(incrementalOrdinal), resolved) and
        // Fast path must NOT consult history when tip matches — guards against perf regression.
        expect.eql(0, historyCalls.length)
  }

  test("(2) historical match: parentHash mismatches GSI tip but history walk recovers the ordinal") { _ =>
    val gl0Tip = mkHash("tip-")
    val pastTip = mkHash("past")
    val pastOrdinal = 17L
    val currentOrdinal = 42L
    val reader = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> gl0Tip,
      (mgA, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> mkSignedIncrementalSnapshot(currentOrdinal)
    )
    for {
      (history, invocations) <- StubHistory.make((mgA, pastTip) -> pastOrdinal)
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, pastTip, history)
      historyCalls <- invocations.get
    } yield
      expect.eql(Some(pastOrdinal), resolved) and
        expect.eql(1, historyCalls.length) and
        // Verify the resolver asked history about the RIGHT (mg, hash) tuple — no cross-metagraph leak.
        expect.eql((mgA, pastTip), historyCalls.head)
  }

  test("(3) true unknown parent: tip mismatch AND history walk miss → None (fail-closed)") { _ =>
    val gl0Tip = mkHash("tip-")
    val unknownHash = mkHash("unkn")
    val currentOrdinal = 42L
    val reader = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> gl0Tip,
      (mgA, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> mkSignedIncrementalSnapshot(currentOrdinal)
    )
    for {
      (history, invocations) <- StubHistory.make() // empty history — every lookup misses.
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, unknownHash, history)
      historyCalls <- invocations.get
    } yield
      expect.eql(None, resolved) and
        // The resolver must HAVE called history (not just fail-closed without trying).
        expect.eql(1, historyCalls.length)
  }

  test("(4) eviction survival: gl0 GSI has no entry for the metagraph at all → history walk recovers the ordinal") { _ =>
    // Simulates the GSAM "Recovery reset at ordinal=N" path that wipes pending state. The resolver still consults history.
    val parentHash = mkHash("parn")
    val recoveredOrdinal = 5L
    val reader = StubReader.make() // no GSI entries at all — getLastStateChannelSnapshotHash returns None.
    for {
      (history, invocations) <- StubHistory.make((mgA, parentHash) -> recoveredOrdinal)
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, parentHash, history)
      historyCalls <- invocations.get
    } yield
      expect.eql(Some(recoveredOrdinal), resolved) and
        expect.eql(1, historyCalls.length)
  }

  test("(5) multi-metagraph isolation: mgA binary whose parentHash equals an mgB binary hash → None (no cross-resolve)") { _ =>
    val gl0TipA = mkHash("tipA")
    val mgBBinaryHash = mkHash("mgBb")
    val mgBOrdinal = 7L
    val readerWithMgA = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> gl0TipA,
      (mgA, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> mkSignedIncrementalSnapshot(42L)
    )
    // History knows about mgB's binary at ordinal 7 — but the query is for mgA. The history stub keys on (mg, hash); a query for
    // (mgA, mgBBinaryHash) must miss even though (mgB, mgBBinaryHash) -> 7 is in the table.
    for {
      (history, invocations) <- StubHistory.make((mgB, mgBBinaryHash) -> mgBOrdinal)
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](readerWithMgA, mgA, mgBBinaryHash, history)
      historyCalls <- invocations.get
    } yield
      expect.eql(None, resolved) and
        expect.eql(1, historyCalls.length) and
        // The resolver MUST have queried with mgA (the caller's metagraph), not mgB.
        expect.eql((mgA, mgBBinaryHash), historyCalls.head)
  }

  // -------------- existing-behaviour preservation --------------

  test("tip-match + LastIncrementalCurrencySnapshots empty: falls back to LastCurrencySnapshots (genesis-only window)") { _ =>
    val tipHash = mkHash("tipG")
    val genesisOrdinal = 0L
    val reader = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> tipHash,
      (mgA, GlobalStateFieldId.LastCurrencySnapshots) -> mkSignedGenesisSnapshot(genesisOrdinal)
    )
    for {
      (history, _) <- StubHistory.make()
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, tipHash, history)
    } yield expect.eql(Some(genesisOrdinal), resolved)
  }

  test("HistoryLookup.noop overload: legacy callers that didn't pass historyLookup observe pre-v2 fail-closed semantics") { _ =>
    val gl0Tip = mkHash("tip-")
    val unknownParent = mkHash("unkn")
    val reader = StubReader.make(
      (mgA, GlobalStateFieldId.LastStateChannelSnapshotHashes) -> gl0Tip,
      (mgA, GlobalStateFieldId.LastIncrementalCurrencySnapshots) -> mkSignedIncrementalSnapshot(42L)
    )
    for {
      resolved <- MetagraphParentOrdinalResolver.resolve[IO](reader, mgA, unknownParent)
    } yield expect.eql(None, resolved)
  }
}
