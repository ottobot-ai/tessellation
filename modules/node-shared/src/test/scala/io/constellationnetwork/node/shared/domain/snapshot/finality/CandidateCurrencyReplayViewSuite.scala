package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.IO
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.overlay._
import io.constellationnetwork.node.shared.domain.snapshot.finality.CandidateCurrencyReplayViewFailure.Component._
import io.constellationnetwork.node.shared.domain.snapshot.finality.CandidateCurrencyReplayViewFailure._
import io.constellationnetwork.node.shared.domain.snapshot.finality.Field32ReplayWitnessState._
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2ReferencePolicy.ExactCanonicalAncestor
import io.constellationnetwork.node.shared.domain.snapshot.finality.Phase2UseScope.{BinaryAdmission, CurrencySnapshotReplay}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap.SwapAmount
import io.constellationnetwork.security.Hashed
import io.constellationnetwork.security.hash.{Hash, ProofsHash}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptImageDigest
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.SimpleIOSuite

object CandidateCurrencyReplayViewSuite extends SimpleIOSuite {
  private val target = FinalityCodecFixtures.targetState
  private val released = FinalityCodecFixtures.releasedCore
  private val image = FinalityCodecFixtures.imageReceipt
  private val metagraphId = Address.fromBytes("candidate-replay-mg".getBytes("UTF-8"))
  private val source = Address.fromBytes("candidate-replay-source".getBytes("UTF-8"))
  private val destination = Address.fromBytes("candidate-replay-destination".getBytes("UTF-8"))
  private val scope = CurrencySnapshotReplay(metagraphId, hash(700))
  private val semanticReceipt = released.payload.receipt.semanticReceipt
  private val proof = SignatureProof(Id(Hex("01" * 32)), Signature(Hex("02" * 64)))
  private val spendAction = SpendAction(
    NonEmptyList.one(SpendTransaction(None, None, SwapAmount(PosLong.unsafeFrom(1L)), source, destination))
  )

  private def hash(seed: Int): Hash = Hash(f"$seed%064x")

  private def snapshot(
    reference: GlobalSnapshotStateRef,
    spendActions: Option[SortedMap[Address, List[SpendAction]]]
  ): Hashed[GlobalIncrementalSnapshot] = {
    val value = GlobalIncrementalSnapshot(
      ordinal = reference.ordinal,
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = reference.parentHash,
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
        Some(reference.mptRoot.value),
        None,
        None
      ),
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = spendActions,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None
    )

    Hashed(Signed(value, NonEmptySet.one(proof)), reference.hash, ProofsHash(reference.hash.value))
  }

  private def historyAt(
    reference: GlobalSnapshotStateRef,
    spendActions: Option[SortedMap[Address, List[SpendAction]]] = None
  ): IO[ExactReplayHistorySession[IO]] = {
    val artifact = snapshot(reference, spendActions)
    val expected = ExactReplayHistoryPosition(reference.hash, reference.ordinal)
    val source = new ExactReplayHistorySource[IO] {
      def fetchExact(
        position: ExactReplayHistoryPosition
      ): IO[Either[ExactReplayHistoryFailure, Hashed[GlobalIncrementalSnapshot]]] =
        IO.pure(Either.cond(position == expected, artifact, ExactReplayHistoryFailure.ArtifactUnavailable(position)))
    }

    ExactReplayHistorySession
      .open[IO](reference, ExactReplayHistoryBounds(maxSteps = 8, maxUniqueTargets = 8), source)
      .flatMap(_.fold(IO.raiseError, IO.pure))
  }

  private def lease(useScope: Phase2UseScope = scope): CanonicalPhase2Lease =
    new CanonicalPhase2Lease(
      useScope,
      ExactCanonicalAncestor,
      target,
      released,
      FinalityCodecFixtures.selection,
      FinalityCodecFixtures.selection.branchRevision,
      CanonicalLineageRevision(NonNegLong.MinValue),
      Phase2ConsumerSinkRevision(NonNegLong.MinValue)
    )

  private def exactImage(
    useScope: CurrencySnapshotReplay = scope,
    receipt: io.constellationnetwork.security.mpt.MptImageReceipt = image
  ): CandidateExactImageReceipt = new CandidateExactImageReceipt(useScope, receipt)

  private def semantic(
    useScope: CurrencySnapshotReplay = scope,
    exactTarget: GlobalSnapshotStateRef = target,
    digest: MptImageDigest = image.digest,
    generation: Long = image.generation,
    receipt: ScopedArtifactRef = semanticReceipt
  ): CandidateWholeImageSemanticReceipt =
    new CandidateWholeImageSemanticReceipt(useScope, exactTarget, digest, generation, receipt)

  private def field32(
    useScope: CurrencySnapshotReplay = scope,
    exactTarget: GlobalSnapshotStateRef = target,
    digest: MptImageDigest = image.digest,
    generation: Long = image.generation,
    receipt: ScopedArtifactRef = semanticReceipt,
    state: Field32ReplayWitnessState = new PresentEmpty,
    preimageDigest: Hash = hash(710),
    populationDigest: Hash = hash(711)
  ): CandidateField32ReplayWitnessReceipt =
    new CandidateField32ReplayWitnessReceipt(
      useScope,
      exactTarget,
      digest,
      generation,
      receipt,
      state,
      preimageDigest,
      populationDigest
    )

  private def values(
    useScope: CurrencySnapshotReplay = scope,
    exactTarget: GlobalSnapshotStateRef = target,
    digest: MptImageDigest = image.digest,
    generation: Long = image.generation,
    receipt: ScopedArtifactRef = semanticReceipt
  ): CandidateRootedCurrencyValues =
    new CandidateRootedCurrencyValues(
      useScope,
      exactTarget,
      digest,
      generation,
      receipt,
      SortedMap(source -> Balance(NonNegLong.unsafeFrom(9L))),
      SortedMap.empty,
      None
    )

  private def compose(
    history: ExactReplayHistorySession[IO],
    currentLease: CanonicalPhase2Lease = lease(),
    currentImage: CandidateExactImageReceipt = exactImage(),
    currentSemantic: CandidateWholeImageSemanticReceipt = semantic(),
    currentField32: CandidateField32ReplayWitnessReceipt = field32(),
    currentValues: CandidateRootedCurrencyValues = values()
  ) =
    CandidateCurrencyReplayView.composeIdentityModel(
      currentLease,
      history,
      currentImage,
      currentSemantic,
      currentField32,
      currentValues
    )

  test("the dark model composes a Phase-2 lease and one exact identity closure; SpendActions come from exact history") {
    for {
      history <- historyAt(target, Some(SortedMap(source -> List(spendAction))))
      view = compose(history).fold(throw _, identity)
      resolved <- view.resolveOrdinals(NonEmptyList.one(target.ordinal))
      artifacts = resolved.fold(throw _, identity)
    } yield
      expect.same(target, view.target) &&
        expect.same(scope, view.scope) &&
        expect.same(Balance(NonNegLong.unsafeFrom(9L)).some, view.balances.get(source)) &&
        expect.same(
          Vector(target -> Some(SortedMap(source -> List(spendAction)))),
          artifacts.spendActionsOldestFirst
        )
  }

  test("structural history cannot substitute for a purpose-specific Phase-2 lease or the exact lease target") {
    for {
      history <- historyAt(target)
      wrongHistory <- historyAt(FinalityCodecFixtures.priorState)
      wrongPurpose = compose(
        history,
        currentLease = lease(BinaryAdmission(metagraphId, hash(720), hash(721)))
      )
      wrongTarget = compose(wrongHistory)
    } yield
      expect(wrongPurpose.left.exists(_.isInstanceOf[WrongLeaseScope])) &&
        expect(wrongTarget.left.toOption.contains(TargetMismatch(History, target, FinalityCodecFixtures.priorState)))
  }

  test("image, semantic, field-32, and rooted-value receipts must share one exact identity") {
    val otherScope = CurrencySnapshotReplay(metagraphId, hash(730))
    val otherTarget = target.copy(hash = hash(731))
    val otherDigest = MptImageDigest(hash(732))
    val otherSemantic = ScopedArtifactRef(
      FinalityCodecFixtures.intentId,
      FinalityCodecFixtures.artifact(FinalityArtifactKind.AppliedSemanticStateReceipt, 733)
    )

    val checks = historyAt(target).map { history =>
      List(
        compose(history, currentImage = exactImage(otherScope)).left.exists(_.isInstanceOf[ScopeMismatch]),
        compose(history, currentImage = exactImage(receipt = image.copy(anchor = otherTarget))).left
          .exists(_.isInstanceOf[TargetMismatch]),
        compose(history, currentImage = exactImage(receipt = image.copy(digest = otherDigest))).left
          .exists(_.isInstanceOf[ImageDigestMismatch]),
        compose(history, currentSemantic = semantic(exactTarget = otherTarget)).left.exists(_.isInstanceOf[TargetMismatch]),
        compose(history, currentSemantic = semantic(receipt = otherSemantic)).left
          .exists(_.isInstanceOf[SemanticReceiptMismatch]),
        compose(history, currentField32 = field32(generation = image.generation + 1L)).left
          .exists(_.isInstanceOf[ImageGenerationMismatch]),
        compose(history, currentValues = values(digest = otherDigest)).left.exists(_.isInstanceOf[ImageDigestMismatch])
      )
    }

    checks.map(results => expect(results.forall(identity)))
  }

  test("field-32 absence, present-empty, and present-nonempty remain distinct and malformed witness states reject") {
    historyAt(target).map { history =>
      val acceptedStates = List(new Absent, new PresentEmpty, new PresentNonEmpty(1)).map(state =>
        compose(history, currentField32 = field32(state = state)).isRight
      )
      val zeroCount = compose(history, currentField32 = field32(state = new PresentNonEmpty(0)))
      val emptyDigest = compose(history, currentField32 = field32(preimageDigest = Hash.empty))

      expect(acceptedStates.forall(identity)) &&
      expect(zeroCount.left.exists(_.isInstanceOf[InvalidField32State])) &&
      expect(emptyDigest.left.exists(_.isInstanceOf[InvalidDigest]))
    }
  }

  test("the composed replay view and every package-owned input capability are non-serializable") {
    historyAt(target).map { history =>
      val imageCapability = exactImage()
      val semanticCapability = semantic()
      val field32Capability = field32()
      val valuesCapability = values()
      val view = compose(
        history,
        currentImage = imageCapability,
        currentSemantic = semanticCapability,
        currentField32 = field32Capability,
        currentValues = valuesCapability
      ).fold(throw _, identity)

      expect(
        List[AnyRef](view, imageCapability, semanticCapability, field32Capability, valuesCapability).forall(value =>
          !classOf[java.io.Serializable].isAssignableFrom(value.getClass)
        )
      )
    }
  }
}
