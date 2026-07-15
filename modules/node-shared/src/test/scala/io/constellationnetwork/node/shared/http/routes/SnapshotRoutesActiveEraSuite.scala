package io.constellationnetwork.node.shared.http.routes

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO

import scala.collection.immutable.{SortedMap, SortedSet}
import scala.concurrent.duration._

import io.constellationnetwork.node.shared.config.types.SnapshotTimeoutsConfig
import io.constellationnetwork.node.shared.domain.snapshot.finality.{FinalityGate, FinalizedSnapshotReader}
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.node.NodeStorage
import io.constellationnetwork.node.shared.infrastructure.snapshot.storage.LastCheckpointInfo
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.node.NodeState
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hashed, Hasher, HasherSelector}

import eu.timepit.refined.auto._
import org.http4s.Method.GET
import org.http4s._
import org.http4s.headers.Accept
import suite.HttpSuite

object SnapshotRoutesActiveEraSuite extends HttpSuite {

  override type Res = Unit
  override def sharedResource: cats.effect.Resource[IO, Unit] = cats.effect.Resource.unit[IO]

  private[node] val ordinal = SnapshotOrdinal.unsafeApply(12L)
  private val forbiddenRoot = Hash("ab" * 32)
  private val lookupHash = Hash("cd" * 32)

  private[node] val proof = GlobalSnapshotStateProof(
    lastStateChannelSnapshotHashesProof = Hash("11" * 32),
    lastTxRefsProof = Hash("22" * 32),
    balancesProof = Hash("33" * 32),
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
    mptRoot = Some(Hash("44" * 32)),
    historicalStakeSnapshots = None,
    smtRoot = None
  )

  private[node] val snapshot = GlobalIncrementalSnapshot(
    ordinal = ordinal,
    height = Height(0L),
    subHeight = SubHeight.MinValue,
    lastSnapshotHash = Hash.empty,
    blocks = SortedSet.empty,
    stateChannelSnapshots = SortedMap.empty,
    shardCheckpoints = SortedMap.empty,
    rewards = SortedSet.empty,
    delegateRewards = Some(SortedMap.empty),
    epochProgress = EpochProgress.MinValue,
    nextFacilitators = NonEmptyList.one(PeerId(Hex("aa" * 64))),
    tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
    stateProof = proof,
    allowSpendBlocks = Some(SortedSet.empty),
    tokenLockBlocks = Some(SortedSet.empty),
    spendActions = Some(SortedMap.empty),
    updateNodeParameters = Some(SortedMap.empty),
    artifacts = Some(SortedSet.empty),
    activeDelegatedStakes = Some(SortedMap.empty),
    delegatedStakesWithdrawals = Some(SortedMap.empty),
    activeNodeCollaterals = Some(SortedMap.empty),
    nodeCollateralWithdrawals = Some(SortedMap.empty),
    version = SnapshotVersion("0.0.1"),
    slotCertificate = None,
    eta = None
  )

  private[node] def signed(value: GlobalIncrementalSnapshot): Signed[GlobalIncrementalSnapshot] =
    Signed(
      value,
      NonEmptySet.one(SignatureProof(ID.Id(Hex("55" * 64)), Signature(Hex("66" * 70))))
    )

  private def storage(value: Signed[GlobalIncrementalSnapshot]): SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(implicit hasher: Hasher[IO]): IO[Boolean] =
        IO.pure(false)
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] =
        IO.pure(Some((value, GlobalSnapshotInfo.empty)))
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(Some(value))
      def get(requested: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        IO.pure(Option.when(requested == ordinal)(value))
      def getHashed(requested: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
        IO.pure(None)
      def get(requested: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(Some(value))
      def getHash(requested: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] = IO.pure(None)
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(implicit hasher: Hasher[IO]): IO[Unit] =
        IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
    }

  private val reader = new FinalizedSnapshotReader[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
    def latestCombinedResponse: IO[Option[Response[IO]]] = IO.pure(None)
    def latestMptEntriesResponse: IO[Option[Response[IO]]] = IO.pure(None)
    def mptEntriesAt(ordinal: SnapshotOrdinal): IO[Option[Response[IO]]] = IO.pure(None)
    def latestCheckpointInfo: IO[Option[LastCheckpointInfo]] = IO.pure(None)
    def combinedCheckpointAt(ordinal: SnapshotOrdinal): IO[Option[Response[IO]]] = IO.pure(None)
  }

  private def routes(value: Signed[GlobalIncrementalSnapshot]): IO[HttpRoutes[IO]] = {
    implicit val finalityGate: FinalityGate[IO] = FinalityGate.passThrough(IO.pure[Option[SnapshotOrdinal]](Some(ordinal)))
    val hasherSelector = HasherSelector.forSyncAlwaysCurrent(Hasher.forCanonicalJson[IO])

    for {
      nodeStorage <- NodeStorage.make[IO]
      _ <- nodeStorage.setNodeState(NodeState.Ready)
      snapshotRoutes <- SnapshotRoutes.make[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo](
        storage(value),
        None,
        "/global-snapshots",
        nodeStorage,
        hasherSelector,
        SnapshotTimeoutsConfig(5.seconds, 5.seconds),
        reader
      )
    } yield snapshotRoutes.publicRoutes
  }

  private def request(path: String): Request[IO] =
    Request[IO](GET, Uri.unsafeFromString(path)).putHeaders(Accept(MediaType.application.json))

  test("serves a direct active-era snapshot with smtRoot absent") {
    routes(signed(snapshot)).flatMap(expectHttpStatus(_, request("/global-snapshots/latest"))(Status.Ok))
  }

  List(
    "/global-snapshots/latest",
    "/global-snapshots/latest/ordinal",
    "/global-snapshots/latest/finalized-ordinal",
    "/global-snapshots/latest/metadata",
    "/global-snapshots/latest/info",
    s"/global-snapshots/${ordinal.value.value}",
    s"/global-snapshots/${ordinal.value.value}?full",
    s"/global-snapshots/${ordinal.value.value}/hash",
    s"/global-snapshots/${lookupHash.value}"
  ).foreach { path =>
    test(s"does not serve forbidden smtRoot at $path") {
      val invalid = signed(snapshot.copy(stateProof = proof.copy(smtRoot = Some(forbiddenRoot))))
      routes(invalid).flatMap(expectHttpStatus(_, request(path))(Status.ServiceUnavailable))
    }
  }
}
