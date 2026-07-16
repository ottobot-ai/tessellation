package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import java.io.{ByteArrayOutputStream, NotSerializableException, ObjectOutputStream}

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.ExactReplayHistoryFailure._
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import weaver.SimpleIOSuite

object ExactReplayHistorySessionSuite extends SimpleIOSuite {

  private val generousBounds = ExactReplayHistoryBounds(maxSteps = 32, maxUniqueTargets = 16)
  private val proof = SignatureProof(Id(Hex("01" * 32)), Signature(Hex("02" * 64)))

  private def ordinal(value: Long): SnapshotOrdinal = SnapshotOrdinal.unsafeApply(value)
  private def hash(id: Int): Hash = Hash(f"$id%064x")
  private def root(id: Int): Hash = Hash(f"${10000 + id}%064x")
  private def position(reference: GlobalSnapshotStateRef): ExactReplayHistoryPosition =
    ExactReplayHistoryPosition(reference.hash, reference.ordinal)

  private def reference(ordinalValue: Long, id: Int, parentId: Int, rootId: Int = -1): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      ordinal(ordinalValue),
      hash(id),
      hash(parentId),
      MptRoot(root(if (rootId < 0) id else rootId))
    )

  private def genesisReference(id: Int, rootId: Int = -1): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal.MinValue,
      hash(id),
      Hash.empty,
      MptRoot(root(if (rootId < 0) id else rootId))
    )

  private def snapshot(
    ordinalValue: Long,
    snapshotHash: Hash,
    parentHash: Hash,
    committedRoot: Option[Hash]
  ): Hashed[GlobalIncrementalSnapshot] = {
    val value = GlobalIncrementalSnapshot(
      ordinal = ordinal(ordinalValue),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = parentHash,
      blocks = SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress.MinValue,
      nextFacilitators = NonEmptyList.one(PeerId(Hex(""))),
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = GlobalSnapshotStateProof(
        Hash.empty,
        Hash.empty,
        Hash.empty,
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
        committedRoot,
        None,
        None
      ),
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = None,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None
    )

    Hashed(Signed(value, NonEmptySet.one(proof)), snapshotHash, ProofsHash(snapshotHash.value))
  }

  private def snapshot(reference: GlobalSnapshotStateRef): Hashed[GlobalIncrementalSnapshot] =
    snapshot(reference.ordinal.value.value, reference.hash, reference.parentHash, reference.mptRoot.value.some)

  private def countingSource(
    counts: Ref[IO, Map[ExactReplayHistoryPosition, Int]]
  )(
    fetch: ExactReplayHistoryPosition => IO[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]]
  ): ExactReplayHistorySource[IO] =
    new ExactReplayHistorySource[IO] {
      def fetchExact(
        requested: ExactReplayHistoryPosition
      ): IO[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]] =
        counts.update(current => current.updated(requested, current.getOrElse(requested, 0) + 1)) >> fetch(requested)
    }

  private def mapSource(
    counts: Ref[IO, Map[ExactReplayHistoryPosition, Int]],
    artifacts: Map[ExactReplayHistoryPosition, Hashed[GlobalIncrementalSnapshot]]
  ): ExactReplayHistorySource[IO] =
    countingSource(counts)(requested => IO.pure(artifacts.get(requested).toRight(ArtifactUnavailable(requested))))

  private def open(
    anchor: GlobalSnapshotStateRef,
    source: ExactReplayHistorySource[IO],
    bounds: ExactReplayHistoryBounds = generousBounds
  ): IO[ExactReplayHistorySession[IO]] =
    ExactReplayHistorySession.open[IO](anchor, bounds, source).flatMap(_.fold(IO.raiseError, IO.pure))

  private def serializationAttempt(value: AnyRef): IO[Either[Throwable, Unit]] =
    IO.blocking {
      val bytes = new ByteArrayOutputStream()
      val out = new ObjectOutputStream(bytes)
      try out.writeObject(value)
      finally out.close()
    }.attempt

  test("open validates all four anchor fields and seeds the anchor observation exactly once") {
    val anchor = reference(3L, 303, 302)
    val wrongParent = anchor.copy(parentHash = hash(999))
    val wrongRoot = anchor.copy(mptRoot = MptRoot(root(999)))

    for {
      parentCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      rootCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      goodCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      parentResult <- ExactReplayHistorySession.open[IO](
        anchor,
        generousBounds,
        mapSource(parentCounts, Map(position(anchor) -> snapshot(wrongParent)))
      )
      rootResult <- ExactReplayHistorySession.open[IO](
        anchor,
        generousBounds,
        mapSource(rootCounts, Map(position(anchor) -> snapshot(wrongRoot)))
      )
      session <- open(anchor, mapSource(goodCounts, Map(position(anchor) -> snapshot(anchor))))
      resolved <- session.resolveExact(NonEmptyList.one(anchor))
      counts <- goodCounts.get
    } yield
      expect(parentResult.left.toOption.contains(AnchorMismatch(anchor, wrongParent))) &&
        expect(rootResult.left.toOption.contains(AnchorMismatch(anchor, wrongRoot))) &&
        expect(parentResult.left.exists(_.isInstanceOf[ExactReplayHistoryInvalid])) &&
        expect(!parentResult.left.exists(_.isInstanceOf[ExactReplayHistoryCorrupt])) &&
        expect(resolved.exists(_.artifactsOldestFirst.map(_.reference) == Vector(anchor))) &&
        expect.eql(Map(position(anchor) -> 1), counts)
  }

  test("exact reference assertions isolate a same-ordinal sibling and detect a returned sibling hash") {
    val a1 = reference(1L, 401, 400)
    val a2 = reference(2L, 402, 401)
    val a3 = reference(3L, 403, 402)
    val sibling2 = reference(2L, 499, 401)
    val returnedSibling = snapshot(2L, sibling2.hash, a1.hash, a2.mptRoot.value.some)
    val returnedWrongOrdinal = snapshot(1L, a2.hash, a1.parentHash, a2.mptRoot.value.some)

    for {
      exactCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      exactSession <- open(
        a3,
        mapSource(exactCounts, Vector(a1, a2, a3).map(ref => position(ref) -> snapshot(ref)).toMap)
      )
      exactResult <- exactSession.resolveExact(NonEmptyList.one(sibling2))
      mismatchCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      mismatchSession <- open(
        a3,
        mapSource(
          mismatchCounts,
          Map(position(a3) -> snapshot(a3), position(a2) -> returnedSibling)
        )
      )
      mismatchResult <- mismatchSession.resolveOrdinals(NonEmptyList.one(a2.ordinal))
      ordinalCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      ordinalSession <- open(
        a3,
        mapSource(
          ordinalCounts,
          Map(position(a3) -> snapshot(a3), position(a2) -> returnedWrongOrdinal)
        )
      )
      ordinalResult <- ordinalSession.resolveOrdinals(NonEmptyList.one(a2.ordinal))
    } yield
      expect(
        exactResult.left.exists {
          case RequestedReferenceNotAncestor(`sibling2`, observed) => observed == a2
          case _                                                   => false
        }
      ) &&
        expect(
          mismatchResult.left.toOption.contains(ReturnedArtifactMismatch(position(a2), sibling2.hash, a2.ordinal))
        ) &&
        expect(
          ordinalResult.left.toOption.contains(ReturnedArtifactMismatch(position(a2), a2.hash, a1.ordinal))
        )
  }

  test("ordinal batches deduplicate targets, traverse once, and return requested artifacts oldest first") {
    val a1 = reference(1L, 501, 500)
    val a2 = reference(2L, 502, 501)
    val a3 = reference(3L, 503, 502)
    val a4 = reference(4L, 504, 503)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(
        a4,
        mapSource(counts, Vector(a1, a2, a3, a4).map(ref => position(ref) -> snapshot(ref)).toMap)
      )
      result <- session.resolveOrdinals(NonEmptyList.of(a4.ordinal, a1.ordinal, a3.ordinal, a1.ordinal))
      repeated <- session.resolveOrdinals(NonEmptyList.of(a3.ordinal, a1.ordinal))
      observed <- counts.get
    } yield
      expect(result.exists(_.artifactsOldestFirst.map(_.reference) == Vector(a1, a3, a4))) &&
        expect(repeated.exists(_.artifactsOldestFirst.map(_.reference) == Vector(a1, a3))) &&
        expect.eql(
          Vector(a1, a2, a3, a4).map(ref => position(ref) -> 1).toMap,
          observed
        )
  }

  test("concurrent requests single-flight every exact position") {
    val a1 = reference(1L, 601, 600)
    val a2 = reference(2L, 602, 601)
    val a3 = reference(3L, 603, 602)
    val artifacts = Vector(a1, a2, a3).map(ref => position(ref) -> snapshot(ref)).toMap

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(a3, countingSource(counts)(requested => IO.cede.as(artifacts.get(requested).toRight(ArtifactUnavailable(requested)))))
      results <- NonEmptyList
        .of(a1.ordinal, a2.ordinal)
        .pure[IO]
        .flatMap(targets => List.fill(16)(session.resolveOrdinals(targets)).parSequence)
      observed <- counts.get
    } yield
      expect(results.forall(_.isRight)) &&
        expect.eql(Vector(a1, a2, a3).map(ref => position(ref) -> 1).toMap, observed)
  }

  test("typed unavailable and corrupt source failures are cached per exact position") {
    val a1 = reference(1L, 701, 700)
    val a2 = reference(2L, 702, 701)
    val a3 = reference(3L, 703, 702)
    val unavailable = ArtifactUnavailable(position(a2))
    val corrupt = SourceCorrupt(position(a2), "demonstrated metadata mismatch")

    def run(failure: ExactReplayHistoryFailure): IO[(List[Either[ExactReplayHistoryFailure, ExactReplayHistoryBatch]], Int)] =
      for {
        counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
        source = countingSource(counts) { requested =>
          if (requested == position(a3)) IO.pure(Right(snapshot(a3)))
          else if (requested == position(a2)) IO.pure(Left(failure))
          else IO.pure(Left(ArtifactUnavailable(requested)))
        }
        session <- open(a3, source)
        outcomes <- List.fill(2)(session.resolveOrdinals(NonEmptyList.one(a1.ordinal))).sequence
        observed <- counts.get
      } yield (outcomes, observed.getOrElse(position(a2), 0))

    for {
      unavailableResult <- run(unavailable)
      corruptResult <- run(corrupt)
    } yield
      expect(unavailableResult._1.forall(_.left.toOption.contains(unavailable))) &&
        expect.eql(1, unavailableResult._2) &&
        expect(corruptResult._1.forall(_.left.toOption.contains(corrupt))) &&
        expect.eql(1, corruptResult._2)
  }

  test("raised source failures become typed and are cached") {
    val a1 = reference(1L, 801, 800)
    val a2 = reference(2L, 802, 801)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      source = countingSource(counts) { requested =>
        if (requested == position(a2)) IO.pure(Right(snapshot(a2)))
        else IO.raiseError(new IllegalStateException("disk-read-failed"))
      }
      session <- open(a2, source)
      first <- session.resolveOrdinals(NonEmptyList.one(a1.ordinal))
      second <- session.resolveOrdinals(NonEmptyList.one(a1.ordinal))
      observed <- counts.get
    } yield
      expect(first.left.exists {
        case SourceReadFailed(requested, detail) => requested == position(a1) && detail == "disk-read-failed"
        case _                                   => false
      }) &&
        expect(first.left.toOption == second.left.toOption) &&
        expect.eql(1, observed.getOrElse(position(a1), 0))
  }

  test("cancelling a lookup caches SourceReadCancelled and leaves later callers unstranded") {
    val a1 = reference(1L, 901, 900)
    val a2 = reference(2L, 902, 901)

    for {
      started <- Deferred[IO, Unit]
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      source = countingSource(counts) { requested =>
        if (requested == position(a2)) IO.pure(Right(snapshot(a2)))
        else started.complete(()).void >> IO.never
      }
      session <- open(a2, source)
      fiber <- session.resolveOrdinals(NonEmptyList.one(a1.ordinal)).start
      _ <- started.get
      _ <- fiber.cancel
      later <- session.resolveOrdinals(NonEmptyList.one(a1.ordinal))
      observed <- counts.get
    } yield
      expect(later.left.toOption.contains(SourceReadCancelled(position(a1)))) &&
        expect.eql(1, observed.getOrElse(position(a1), 0))
  }

  test("missing roots, malformed roots and non-genesis empty parents fail closed; genesis empty parent is valid") {
    val anchor = reference(2L, 1002, 1001)
    val missingRoot = snapshot(2L, anchor.hash, anchor.parentHash, None)
    val emptyRoot = snapshot(2L, anchor.hash, anchor.parentHash, Hash.empty.some)
    val emptyParent = snapshot(2L, anchor.hash, Hash.empty, anchor.mptRoot.value.some)
    val genesis = genesisReference(1000)

    def openWith(returned: Hashed[GlobalIncrementalSnapshot]): IO[Either[ExactReplayHistoryFailure, ExactReplayHistorySession[IO]]] =
      for {
        counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
        result <- ExactReplayHistorySession.open[IO](
          anchor,
          generousBounds,
          mapSource(counts, Map(position(anchor) -> returned))
        )
      } yield result

    for {
      missing <- openWith(missingRoot)
      invalidRoot <- openWith(emptyRoot)
      invalidParent <- openWith(emptyParent)
      genesisCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      genesisSession <- open(genesis, mapSource(genesisCounts, Map(position(genesis) -> snapshot(genesis))))
      genesisResult <- genesisSession.resolveOrdinals(NonEmptyList.one(SnapshotOrdinal.MinValue))
    } yield
      expect(missing.left.toOption.contains(MissingCommittedRoot(position(anchor)))) &&
        expect(invalidRoot.left.toOption.contains(InvalidCommittedRoot(position(anchor), Hash.empty))) &&
        expect(invalidParent.left.toOption.contains(InvalidArtifactParent(position(anchor), Hash.empty))) &&
        expect(genesisResult.exists(_.artifactsOldestFirst.map(_.reference) == Vector(genesis)))
  }

  test("request and traversal bounds reject before historical source reads") {
    val anchor = reference(5L, 1105, 1104)
    val above = ordinal(6L)
    val hugeAnchor = GlobalSnapshotStateRef(
      ordinal(Long.MaxValue),
      hash(1199),
      hash(1198),
      MptRoot(root(1199))
    )

    for {
      invalidCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      invalid <- ExactReplayHistorySession.open[IO](
        anchor,
        ExactReplayHistoryBounds(maxSteps = 0, maxUniqueTargets = 1),
        mapSource(invalidCounts, Map(position(anchor) -> snapshot(anchor)))
      )
      invalidObserved <- invalidCounts.get
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(anchor, mapSource(counts, Map(position(anchor) -> snapshot(anchor))))
      aboveResult <- session.resolveOrdinals(NonEmptyList.one(above))
      afterAbove <- counts.get
      hugeCounts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      hugeSession <- open(
        hugeAnchor,
        mapSource(hugeCounts, Map(position(hugeAnchor) -> snapshot(hugeAnchor))),
        ExactReplayHistoryBounds(maxSteps = Int.MaxValue, maxUniqueTargets = 1)
      )
      hugeResult <- hugeSession.resolveOrdinals(NonEmptyList.one(SnapshotOrdinal.MinValue))
      hugeObserved <- hugeCounts.get
    } yield
      expect(invalid.left.exists(_.isInstanceOf[InvalidBounds])) &&
        expect(invalidObserved.isEmpty) &&
        expect(aboveResult.left.toOption.contains(TargetAboveAnchor(anchor, above))) &&
        expect.eql(Map(position(anchor) -> 1), afterAbove) &&
        expect(hugeResult.left.exists {
          case TraversalLimitExceeded(`hugeAnchor`, SnapshotOrdinal.MinValue, required, Int.MaxValue) =>
            required == BigInt(Long.MaxValue) + 1
          case _ => false
        }) &&
        expect.eql(Map(position(hugeAnchor) -> 1), hugeObserved)
  }

  test("maxUniqueTargets rejects a deduplicated oversized batch before ancestry reads") {
    val a2 = reference(2L, 1152, 1151)
    val a3 = reference(3L, 1153, 1152)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(
        a3,
        mapSource(counts, Map(position(a3) -> snapshot(a3))),
        ExactReplayHistoryBounds(maxSteps = 8, maxUniqueTargets = 1)
      )
      result <- session.resolveOrdinals(NonEmptyList.of(a2.ordinal, a3.ordinal, a2.ordinal))
      observed <- counts.get
    } yield
      expect(result.left.toOption.contains(TooManyUniqueTargets(actual = 2, maximum = 1))) &&
        expect.eql(Map(position(a3) -> 1), observed)
  }

  test("malformed anchors reject before the source is observed") {
    val valid = reference(2L, 1172, 1171)
    val badHash = valid.copy(hash = Hash("ABC"))
    val badRoot = valid.copy(mptRoot = MptRoot(Hash.empty))
    val emptyParent = valid.copy(parentHash = Hash.empty)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      source = mapSource(counts, Map.empty)
      hashResult <- ExactReplayHistorySession.open[IO](badHash, generousBounds, source)
      rootResult <- ExactReplayHistorySession.open[IO](badRoot, generousBounds, source)
      parentResult <- ExactReplayHistorySession.open[IO](emptyParent, generousBounds, source)
      observed <- counts.get
    } yield
      expect(hashResult.left.exists {
        case InvalidReference(`badHash`, "snapshot hash", _) => true
        case _                                               => false
      }) &&
        expect(rootResult.left.toOption.contains(InvalidReference(badRoot, "MPT root", Hash.empty))) &&
        expect(parentResult.left.toOption.contains(InvalidReference(emptyParent, "parent hash", Hash.empty))) &&
        expect(observed.isEmpty)
  }

  test("a self-parenting anchor is rejected as an ancestry cycle without a second source observation") {
    val anchor = reference(2L, 1182, 1182)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(anchor, mapSource(counts, Map(position(anchor) -> snapshot(anchor))))
      result <- session.resolveOrdinals(NonEmptyList.one(ordinal(1L)))
      observed <- counts.get
    } yield
      expect(result.left.toOption.contains(AncestryCycle(anchor, ExactReplayHistoryPosition(anchor.hash, ordinal(1L))))) &&
        expect.eql(Map(position(anchor) -> 1), observed)
  }

  test("conflicting exact targets at one ordinal fail without another source observation") {
    val a2 = reference(2L, 1202, 1201)
    val a3 = reference(3L, 1203, 1202)
    val sibling2 = a2.copy(mptRoot = MptRoot(root(1299)))

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(a3, mapSource(counts, Map(position(a3) -> snapshot(a3))))
      result <- session.resolveExact(NonEmptyList.of(a2, sibling2, a2))
      observed <- counts.get
    } yield
      expect(result.left.exists {
        case ConflictingTargetsAtOrdinal(ordinal, first, second) =>
          ordinal == a2.ordinal && first == a2 && second == sibling2
        case _ => false
      }) &&
        expect.eql(Map(position(a3) -> 1), observed)
  }

  test("the session capability is neither Product nor Java-serializable") {
    val anchor = reference(2L, 1302, 1301)

    for {
      counts <- Ref.of[IO, Map[ExactReplayHistoryPosition, Int]](Map.empty)
      session <- open(anchor, mapSource(counts, Map(position(anchor) -> snapshot(anchor))))
      batch <- session.resolveOrdinals(NonEmptyList.one(anchor.ordinal)).flatMap(_.fold(IO.raiseError, IO.pure))
      artifact = batch.artifactsOldestFirst.head
      capability = session.asInstanceOf[AnyRef]
      structuralBatch = batch.asInstanceOf[AnyRef]
      structuralArtifact = artifact.asInstanceOf[AnyRef]
      serializedSession <- serializationAttempt(capability)
      serializedBatch <- serializationAttempt(structuralBatch)
      serializedArtifact <- serializationAttempt(structuralArtifact)
    } yield
      expect(!capability.isInstanceOf[Product]) &&
        expect(!capability.isInstanceOf[java.io.Serializable]) &&
        expect(!structuralBatch.isInstanceOf[Product]) &&
        expect(!structuralBatch.isInstanceOf[java.io.Serializable]) &&
        expect(!structuralArtifact.isInstanceOf[Product]) &&
        expect(!structuralArtifact.isInstanceOf[java.io.Serializable]) &&
        expect(serializedSession.left.exists(_.isInstanceOf[NotSerializableException])) &&
        expect(serializedBatch.left.exists(_.isInstanceOf[NotSerializableException])) &&
        expect(serializedArtifact.left.exists(_.isInstanceOf[NotSerializableException]))
  }
}
