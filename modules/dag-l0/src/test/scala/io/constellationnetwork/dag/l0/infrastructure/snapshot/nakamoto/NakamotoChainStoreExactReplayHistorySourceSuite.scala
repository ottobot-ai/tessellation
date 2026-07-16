package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.{ChainTip, GlobalSnapshotStateRef}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.semver.SnapshotVersion
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.Encoder
import weaver.SimpleIOSuite

object NakamotoChainStoreExactReplayHistorySourceSuite extends SimpleIOSuite {

  private implicit val hasher: Hasher[IO] = new Hasher[IO] {
    def hash[A: Encoder](data: A): IO[Hash] =
      hashBytes(Encoder[A].apply(data).noSpaces.getBytes(StandardCharsets.UTF_8))

    def hashBytes(bytes: Array[Byte]): IO[Hash] =
      IO {
        val digest = MessageDigest.getInstance("SHA-256").digest(bytes)
        Hash(digest.iterator.map(byte => f"${byte & 0xff}%02x").mkString)
      }

    def compare[A: Encoder](data: A, expectedHash: Hash): IO[Boolean] = hash(data).map(_ == expectedHash)

    def getLogic(ordinal: SnapshotOrdinal): HashLogic = JsonHash

    def prefixedHash[A: Encoder](data: A, prefix: Array[Byte]): IO[Hash] =
      hashBytes(prefix ++ Encoder[A].apply(data).noSpaces.getBytes(StandardCharsets.UTF_8))
  }
  private implicit val hasherSelector: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(hasher)

  private val signer = PeerId(Hex("0d" * 64))
  private val proof = SignatureProof(signer.toId, Signature(Hex("0e" * 64)))
  private val parentA = Hash("11" * 32)
  private val parentB = Hash("22" * 32)
  private val rootA = Hash("33" * 32)

  private def emptyInfo: GlobalSnapshotInfo =
    GlobalSnapshotInfoV1(SortedMap.empty, SortedMap.empty, SortedMap.empty).toGlobalSnapshotInfo

  private def signedSnapshot(
    ordinal: Long,
    parentHash: Hash,
    mptRoot: Option[Hash]
  ): Signed[GlobalIncrementalSnapshot] = {
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
        mptRoot = mptRoot,
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
    signed: Signed[GlobalIncrementalSnapshot],
    hash: Hash,
    parentHash: Hash
  ): NakamotoChainStore.StoredSnapshot =
    NakamotoChainStore.StoredSnapshot(
      signedSnapshot = signed,
      context = emptyInfo,
      ordinal = signed.value.ordinal.value.value,
      slot = 0L,
      parentHash = parentHash,
      hash = hash
    )

  test("returns only a second-read content-rehashed artifact after a bounded exact one-link walk") {
    for {
      signed <- IO.pure(signedSnapshot(7L, parentA, Some(rootA)))
      hashed <- signed.toHashed[IO]
      position = ExactReplayHistoryPosition(hashed.hash, signed.value.ordinal)
      expectedWalk = NakamotoChainStore.ExactWalkPosition(position.hash, position.ordinal)
      chainStore = new ScriptedChainStore(
        walk = (start, target, maxSteps) =>
          IO {
            assert(start == expectedWalk)
            assert(target == position.ordinal)
            assert(maxSteps == 1)
            Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
          },
        read = (_, _) => IO.pure(Some(stored(signed, hashed.hash, parentA)))
      )
      result <- NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position)
    } yield
      result match {
        case Right(actual) => expect.same(hashed.hash, actual.hash) && expect.same(signed, actual.signed)
        case Left(error)   => failure(s"expected exact rehashed artifact, got $error")
      }
  }

  test("session opens only when ordinal, hash, parent, and signed MPT root reproduce the anchor") {
    for {
      signed <- IO.pure(signedSnapshot(7L, parentA, Some(rootA)))
      hashed <- signed.toHashed[IO]
      exact = GlobalSnapshotStateRef(signed.value.ordinal, hashed.hash, parentA, MptRoot(rootA))
      source = NakamotoChainStoreExactReplayHistorySource.make[IO](
        new ScriptedChainStore(
          walk = (start, _, _) =>
            IO.pure(
              Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
            ),
          read = (_, _) => IO.pure(Some(stored(signed, hashed.hash, parentA)))
        )
      )
      opened <- ExactReplayHistorySession.open[IO](exact, ExactReplayHistoryBounds(1, 1), source)
      wrongParent <- ExactReplayHistorySession.open[IO](
        exact.copy(parentHash = parentB),
        ExactReplayHistoryBounds(1, 1),
        source
      )
      wrongRoot <- ExactReplayHistorySession.open[IO](
        exact.copy(mptRoot = MptRoot(Hash("44" * 32))),
        ExactReplayHistoryBounds(1, 1),
        source
      )
    } yield
      expect(opened.isRight) &&
        expect(wrongParent.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.AnchorMismatch])) &&
        expect(wrongRoot.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.AnchorMismatch]))
  }

  test("session classifies an absent signed MPT root as unavailable, not corrupt") {
    for {
      signed <- IO.pure(signedSnapshot(7L, parentA, None))
      hashed <- signed.toHashed[IO]
      claimed = GlobalSnapshotStateRef(signed.value.ordinal, hashed.hash, parentA, MptRoot(rootA))
      source = NakamotoChainStoreExactReplayHistorySource.make[IO](
        new ScriptedChainStore(
          walk = (start, _, _) =>
            IO.pure(
              Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
            ),
          read = (_, _) => IO.pure(Some(stored(signed, hashed.hash, parentA)))
        )
      )
      result <- ExactReplayHistorySession.open[IO](claimed, ExactReplayHistoryBounds(1, 1), source)
    } yield
      expect(result.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.MissingCommittedRoot])) &&
        expect(result.left.toOption.exists(_.isInstanceOf[io.constellationnetwork.node.shared.domain.nakamoto.overlay.ExactReplayHistoryUnavailable]))
  }

  test("same-ordinal fallback sibling remains typed unavailable and never reaches the artifact read") {
    val requested = ExactReplayHistoryPosition(Hash("44" * 32), SnapshotOrdinal.unsafeApply(8L))
    val sibling = Hash("55" * 32)
    val chainStore = new ScriptedChainStore(
      walk = (start, _, _) =>
        IO.pure(
          Right(
            NakamotoChainStore.ExactWalkResult.Incomplete(
              Vector.empty,
              start,
              NakamotoChainStore.ExactWalkIncompleteReason.SiblingAtOrdinal(sibling)
            )
          )
        ),
      read = (_, _) => IO.raiseError(new AssertionError("sibling result must not read or return ordinal bytes"))
    )

    NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(requested).map { result =>
      expect.same(Left(ExactReplayHistoryFailure.SameOrdinalSibling(requested, sibling)), result)
    }
  }

  test("second read cannot change the exact walked parent") {
    for {
      signed <- IO.pure(signedSnapshot(7L, parentB, Some(rootA)))
      hashed <- signed.toHashed[IO]
      position = ExactReplayHistoryPosition(hashed.hash, signed.value.ordinal)
      chainStore = new ScriptedChainStore(
        walk = (start, _, _) =>
          IO.pure(
            Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
          ),
        read = (_, _) => IO.pure(Some(stored(signed, hashed.hash, parentB)))
      )
      result <- NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position)
    } yield expect(result.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceCorrupt]))
  }

  test("exact bytes disappearing after the walk remain typed unavailable") {
    val position = ExactReplayHistoryPosition(Hash("88" * 32), SnapshotOrdinal.unsafeApply(7L))
    val chainStore = new ScriptedChainStore(
      walk = (start, _, _) =>
        IO.pure(
          Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
        ),
      read = (_, _) => IO.pure(None)
    )

    NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position).map { result =>
      expect.same(Left(ExactReplayHistoryFailure.ArtifactUnavailable(position)), result)
    }
  }

  test("second-read body hash mismatch is demonstrated corruption") {
    for {
      signed <- IO.pure(signedSnapshot(7L, parentA, Some(rootA)))
      actual <- signed.toHashed[IO]
      claimedHash = Hash("99" * 32)
      _ = assert(actual.hash != claimedHash)
      position = ExactReplayHistoryPosition(claimedHash, signed.value.ordinal)
      chainStore = new ScriptedChainStore(
        walk = (start, _, _) =>
          IO.pure(
            Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
          ),
        read = (_, _) => IO.pure(Some(stored(signed, claimedHash, parentA)))
      )
      result <- NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position)
    } yield expect(result.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceCorrupt]))
  }

  test("second-read throwable remains a read failure rather than corruption") {
    val position = ExactReplayHistoryPosition(Hash("aa" * 32), SnapshotOrdinal.unsafeApply(7L))
    val chainStore = new ScriptedChainStore(
      walk = (start, _, _) =>
        IO.pure(
          Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
        ),
      read = (_, _) => IO.raiseError(new IllegalStateException("disk read failed"))
    )

    NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position).map { result =>
      expect(result.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceReadFailed]))
    }
  }

  test("missing signed MPT root reaches session classification while a reserved present root is corrupt") {
    def run(root: Option[Hash]): IO[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]] =
      for {
        signed <- IO.pure(signedSnapshot(7L, parentA, root))
        hashed <- signed.toHashed[IO]
        position = ExactReplayHistoryPosition(hashed.hash, signed.value.ordinal)
        chainStore = new ScriptedChainStore(
          walk = (start, _, _) =>
            IO.pure(
              Right(NakamotoChainStore.ExactWalkResult.Complete(Vector(NakamotoChainStore.ExactWalkLink(start, parentA))))
            ),
          read = (_, _) => IO.pure(Some(stored(signed, hashed.hash, parentA)))
        )
        result <- NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position)
      } yield result

    (run(None), run(Some(Hash.empty))).mapN { (absent, reserved) =>
      expect(absent.isRight) &&
      expect(reserved.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceCorrupt]))
    }
  }

  test("hash-era absence and storage failure remain distinct from corrupt source bytes") {
    val position = ExactReplayHistoryPosition(Hash("66" * 32), SnapshotOrdinal.unsafeApply(9L))
    val eraStore = new ScriptedChainStore(
      walk = (_, _, _) =>
        IO.pure(Left(NakamotoChainStore.ExactWalkError.HashEraUnavailable(
          NakamotoChainStore.ExactWalkPosition(position.hash, position.ordinal),
          JsonHash,
          KryoHash
        ))),
      read = (_, _) => IO.raiseError(new AssertionError("hash-era failure must stop before read"))
    )
    val storageStore = new ScriptedChainStore(
      walk = (_, _, _) =>
        IO.pure(Left(NakamotoChainStore.ExactWalkError.StorageReadFailed(
          NakamotoChainStore.ExactWalkPosition(position.hash, position.ordinal),
          "hash",
          "disk unavailable"
        ))),
      read = (_, _) => IO.raiseError(new AssertionError("storage failure must stop before second read"))
    )

    (
      NakamotoChainStoreExactReplayHistorySource.make[IO](eraStore).fetchExact(position),
      NakamotoChainStoreExactReplayHistorySource.make[IO](storageStore).fetchExact(position)
    ).mapN { (eraResult, storageResult) =>
      expect(eraResult.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceHashEraUnavailable])) &&
      expect(storageResult.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceReadFailed]))
    }
  }

  test("content hashing inability is verification-unavailable and cannot accuse source corruption") {
    val position = ExactReplayHistoryPosition(Hash("77" * 32), SnapshotOrdinal.unsafeApply(10L))
    val chainStore = new ScriptedChainStore(
      walk = (_, _, _) =>
        IO.pure(Left(NakamotoChainStore.ExactWalkError.ContentHashFailed(
          NakamotoChainStore.ExactWalkPosition(position.hash, position.ordinal),
          "codec unavailable"
        ))),
      read = (_, _) => IO.raiseError(new AssertionError("verification failure must stop before read"))
    )

    NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position).map { result =>
      expect(result.left.toOption.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceVerificationUnavailable]))
    }
  }

  test("only demonstrated stored or signed mismatches receive source-corrupt classification") {
    val position = ExactReplayHistoryPosition(Hash("78" * 32), SnapshotOrdinal.unsafeApply(10L))
    val walkPosition = NakamotoChainStore.ExactWalkPosition(position.hash, position.ordinal)

    def run(error: NakamotoChainStore.ExactWalkError): IO[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]] = {
      val chainStore = new ScriptedChainStore(
        walk = (_, _, _) => IO.pure(Left(error)),
        read = (_, _) => IO.raiseError(new AssertionError("failed exact walk must stop before artifact read"))
      )

      NakamotoChainStoreExactReplayHistorySource.make[IO](chainStore).fetchExact(position)
    }

    val malformedCompleteStore = new ScriptedChainStore(
      walk = (_, _, _) => IO.pure(Right(NakamotoChainStore.ExactWalkResult.Complete(Vector.empty))),
      read = (_, _) => IO.raiseError(new AssertionError("malformed exact walk must stop before artifact read"))
    )

    (
      run(NakamotoChainStore.ExactWalkError.InvalidMaxSteps(0)),
      run(
        NakamotoChainStore.ExactWalkError.NonCanonicalSnapshotHash(
          walkPosition,
          NakamotoChainStore.ExactWalkHashRole.Rehashed,
          Hash.empty
        )
      ),
      run(
        NakamotoChainStore.ExactWalkError.NonCanonicalSnapshotHash(
          walkPosition,
          NakamotoChainStore.ExactWalkHashRole.Stored,
          Hash.empty
        )
      ),
      NakamotoChainStoreExactReplayHistorySource.make[IO](malformedCompleteStore).fetchExact(position)
    ).mapN { (contractFailure, localHashFailure, storedFailure, malformedComplete) =>
      expect(contractFailure.left.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceVerificationUnavailable])) &&
      expect(localHashFailure.left.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceVerificationUnavailable])) &&
      expect(storedFailure.left.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceCorrupt])) &&
      expect(malformedComplete.left.exists(_.isInstanceOf[ExactReplayHistoryFailure.SourceVerificationUnavailable]))
    }
  }

  /** All unlisted calls fail so a best tip, ordinal scan, or canonical-head fallback is observable. */
  private final class ScriptedChainStore(
    walk: (NakamotoChainStore.ExactWalkPosition, SnapshotOrdinal, Int) =>
      IO[Either[NakamotoChainStore.ExactWalkError, NakamotoChainStore.ExactWalkResult]],
    read: (Hash, Long) => IO[Option[NakamotoChainStore.StoredSnapshot]]
  ) extends NakamotoChainStore.NakamotoChainStoreAlgebra[IO] {

    private def unexpected[A](method: String): IO[A] =
      IO.raiseError(new AssertionError(s"exact replay history adapter called forbidden chain-store method: $method"))

    def walkBackExact(
      start: NakamotoChainStore.ExactWalkPosition,
      targetOrdinal: SnapshotOrdinal,
      maxSteps: Int
    ): IO[Either[NakamotoChainStore.ExactWalkError, NakamotoChainStore.ExactWalkResult]] =
      walk(start, targetOrdinal, maxSteps)

    def getWithOrdinalFallback(hash: Hash, expectedOrdinal: Long): IO[Option[NakamotoChainStore.StoredSnapshot]] =
      read(hash, expectedOrdinal)

    def store(
      signedSnapshot: Signed[GlobalIncrementalSnapshot],
      context: GlobalSnapshotInfo,
      ordinal: Long,
      slot: Long,
      parentHash: Hash,
      vrfOutput: Array[Byte]
    ): IO[NakamotoChainStore.StoreOutcome] = unexpected("store")
    def selectedTip: IO[Option[NakamotoChainStore.SelectedTip]] = unexpected("selectedTip")
    private[nakamoto] def runCanonicalEffectsIfCurrent[A](
      expected: NakamotoChainStore.SelectedTip
    )(
      effects: NakamotoChainStore.StoredSnapshot => IO[A]
    ): IO[NakamotoChainStore.CanonicalEffectsOutcome[A]] = unexpected("runCanonicalEffectsIfCurrent")
    def bestTip: IO[Option[NakamotoChainStore.StoredSnapshot]] = unexpected("bestTip")
    def bestTipSlot: IO[Option[Long]] = unexpected("bestTipSlot")
    def bestTipOrdinal: IO[Option[Long]] = unexpected("bestTipOrdinal")
    def chainLength: IO[Int] = unexpected("chainLength")
    def forkCount: IO[Int] = unexpected("forkCount")
    def allTips: IO[Set[Hash]] = unexpected("allTips")
    def lastFinalizedOrdinal: IO[Long] = unexpected("lastFinalizedOrdinal")
    def get(hash: Hash): IO[Option[NakamotoChainStore.StoredSnapshot]] = unexpected("get")
    def chainFromTip: IO[List[NakamotoChainStore.StoredSnapshot]] = unexpected("chainFromTip")
    def vrfOutputRangeForPeriodFrom(
      period: Long,
      etaRotationSnapshots: Long,
      fromHash: Hash
    ): IO[NakamotoChainStore.VrfOutputRange] = unexpected("vrfOutputRangeForPeriodFrom")
    def finalizeSelectedAt(
      expected: NakamotoChainStore.SelectedTip,
      targetOrdinal: Long
    ): IO[NakamotoChainStore.FinalizeOutcome] = unexpected("finalizeSelectedAt")
    def walkBackTo(startHash: Hash, targetOrdinal: Long): IO[Option[Hash]] = unexpected("walkBackTo")
    def getByOrdinal(ordinal: Long): IO[Option[NakamotoChainStore.StoredSnapshot]] = unexpected("getByOrdinal")
    def size: IO[Int] = unexpected("size")
    def tipFor(hash: Hash): IO[Option[ChainTip]] = unexpected("tipFor")
    def divergentRefuseCount: IO[Long] = unexpected("divergentRefuseCount")
    def divergentRefuseSample: IO[Option[(Long, Hash)]] = unexpected("divergentRefuseSample")
    def unsafe_clearFinality: IO[Boolean] = unexpected("unsafe_clearFinality")
  }
}
