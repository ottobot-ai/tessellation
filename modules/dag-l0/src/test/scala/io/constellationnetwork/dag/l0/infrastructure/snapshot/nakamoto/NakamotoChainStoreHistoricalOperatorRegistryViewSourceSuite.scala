package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object NakamotoChainStoreHistoricalOperatorRegistryViewSourceSuite extends SimpleIOSuite {

  private val signer = PeerId(Hex("0d" * 64))
  private val proof = SignatureProof(signer.toId, Signature(Hex("0e" * 64)))

  private def emptyInfo: GlobalSnapshotInfo =
    GlobalSnapshotInfoV1(SortedMap.empty, SortedMap.empty, SortedMap.empty).toGlobalSnapshotInfo

  private def signedSnapshot(ordinal: Long, parentHash: Hash): Signed[GlobalIncrementalSnapshot] = {
    val snapshot = GlobalIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(ordinal),
      height = Height(NonNegLong.MinValue),
      subHeight = SubHeight(NonNegLong.MinValue),
      lastSnapshotHash = parentHash,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress.MinValue,
      nextFacilitators = NonEmptyList.one(signer),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = GlobalSnapshotStateProof(
        lastStateChannelSnapshotHashesProof = Hash.empty,
        lastTxRefsProof = Hash.empty,
        balancesProof = Hash.empty,
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
      ),
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = None,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      version = SnapshotVersion("0.0.1")
    )

    Signed(snapshot, NonEmptySet.one(proof))
  }

  private def stored(
    hash: Hash,
    ordinal: Long,
    signedOrdinal: Long,
    parentHash: Hash,
    signedParentHash: Hash,
    context: GlobalSnapshotInfo
  ): NakamotoChainStore.StoredSnapshot =
    NakamotoChainStore.StoredSnapshot(
      signedSnapshot = signedSnapshot(signedOrdinal, signedParentHash),
      context = context,
      ordinal = ordinal,
      slot = 0L,
      parentHash = parentHash,
      hash = hash
    )

  private def source(
    lookup: Hash => Option[NakamotoChainStore.StoredSnapshot]
  ) =
    NakamotoChainStoreHistoricalOperatorRegistryViewSource.make[IO](new ExactGetOnlyChainStore(lookup))

  test("resolves same-ordinal sibling snapshots by exact hash and returns each locally reproduced context") {
    val hashA = Hash("branch-a")
    val hashB = Hash("branch-b")
    val parentA = Hash("parent-a")
    val parentB = Hash("parent-b")
    val contextA = emptyInfo
    val contextB = emptyInfo
    val storedA = stored(hashA, 17L, 17L, parentA, parentA, contextA)
    val storedB = stored(hashB, 17L, 17L, parentB, parentB, contextB)
    val views = source(Map(hashA -> storedA, hashB -> storedB).get)

    for {
      viewA <- views.get(hashA)
      viewB <- views.get(hashB)
    } yield expect(viewA.exists(_.parentHash == hashA)) &&
      expect(viewB.exists(_.parentHash == hashB)) &&
      expect(viewA.exists(_.parentOrdinal == SnapshotOrdinal.unsafeApply(17L))) &&
      expect(viewB.exists(_.parentOrdinal == SnapshotOrdinal.unsafeApply(17L))) &&
      expect(viewA.exists(_.info eq contextA)) &&
      expect(viewB.exists(_.info eq contextB))
  }

  test("rejects a chain-store result whose stored hash differs from the requested hash") {
    val requested = Hash("requested")
    val returned = Hash("returned")
    val parent = Hash("parent")
    val wrong = stored(returned, 7L, 7L, parent, parent, emptyInfo)

    source(_ => Some(wrong)).get(requested).map(view => expect.same(None, view))
  }

  test("returns None when the exact hot-chain hash is missing") {
    source(_ => None).get(Hash("missing")).map(view => expect.same(None, view))
  }

  test("rejects a negative stored ordinal") {
    val hash = Hash("negative-ordinal")
    val parent = Hash("parent")
    val malformed = stored(hash, -1L, 7L, parent, parent, emptyInfo)

    source(_ => Some(malformed)).get(hash).map(view => expect.same(None, view))
  }

  test("rejects stored ordinal metadata that differs from the signed snapshot") {
    val hash = Hash("ordinal-mismatch")
    val parent = Hash("parent")
    val malformed = stored(hash, 8L, 7L, parent, parent, emptyInfo)

    source(_ => Some(malformed)).get(hash).map(view => expect.same(None, view))
  }

  test("rejects stored parent metadata that differs from the signed snapshot") {
    val hash = Hash("parent-mismatch")
    val malformed = stored(hash, 7L, 7L, Hash("stored-parent"), Hash("signed-parent"), emptyInfo)

    source(_ => Some(malformed)).get(hash).map(view => expect.same(None, view))
  }

  /** Every non-`get` method fails the test if the adapter reaches it. This makes best-tip, ordinal, disk-fallback, and other chain-store
    * authority substitutions observable without broad mocking dependencies.
    */
  private final class ExactGetOnlyChainStore(
    lookup: Hash => Option[NakamotoChainStore.StoredSnapshot]
  ) extends NakamotoChainStore.NakamotoChainStoreAlgebra[IO] {

    private def unexpected[A](method: String): IO[A] =
      IO.raiseError(new AssertionError(s"historical registry view source called forbidden chain-store method: $method"))

    def get(hash: Hash): IO[Option[NakamotoChainStore.StoredSnapshot]] = IO.pure(lookup(hash))

    def store(
      signedSnapshot: Signed[GlobalIncrementalSnapshot],
      context: GlobalSnapshotInfo,
      ordinal: Long,
      slot: Long,
      parentHash: Hash,
      vrfOutput: Array[Byte]
    ): IO[Boolean] = unexpected("store")
    def bestTip: IO[Option[NakamotoChainStore.StoredSnapshot]] = unexpected("bestTip")
    def bestTipSlot: IO[Option[Long]] = unexpected("bestTipSlot")
    def bestTipOrdinal: IO[Option[Long]] = unexpected("bestTipOrdinal")
    def chainLength: IO[Int] = unexpected("chainLength")
    def forkCount: IO[Int] = unexpected("forkCount")
    def allTips: IO[Set[Hash]] = unexpected("allTips")
    def lastFinalizedOrdinal: IO[Long] = unexpected("lastFinalizedOrdinal")
    def getWithOrdinalFallback(hash: Hash, expectedOrdinal: Long): IO[Option[NakamotoChainStore.StoredSnapshot]] =
      unexpected("getWithOrdinalFallback")
    def chainFromTip: IO[List[NakamotoChainStore.StoredSnapshot]] = unexpected("chainFromTip")
    def vrfOutputRangeForPeriodFrom(
      period: Long,
      etaRotationSnapshots: Long,
      fromHash: Hash
    ): IO[NakamotoChainStore.VrfOutputRange] = unexpected("vrfOutputRangeForPeriodFrom")
    def finalize(hash: Hash, ordinal: Long): IO[Unit] = unexpected("finalize")
    def walkBackTo(startHash: Hash, targetOrdinal: Long): IO[Option[Hash]] = unexpected("walkBackTo")
    def getByOrdinal(ordinal: Long): IO[Option[NakamotoChainStore.StoredSnapshot]] = unexpected("getByOrdinal")
    def size: IO[Int] = unexpected("size")
    def tree: ParentChildTree[IO] = throw new AssertionError("historical registry view source called forbidden chain-store method: tree")
    def tipFor(hash: Hash): IO[Option[ChainTip]] = unexpected("tipFor")
    def divergentRefuseCount: IO[Long] = unexpected("divergentRefuseCount")
    def divergentRefuseSample: IO[Option[(Long, Hash)]] = unexpected("divergentRefuseSample")
    def unsafe_clearFinality: IO[Boolean] = unexpected("unsafe_clearFinality")
  }
}
