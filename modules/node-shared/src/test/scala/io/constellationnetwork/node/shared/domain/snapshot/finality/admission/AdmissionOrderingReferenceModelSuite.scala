package io.constellationnetwork.node.shared.domain.snapshot.finality.admission

import io.constellationnetwork.node.shared.domain.snapshot.finality._
import io.constellationnetwork.node.shared.domain.snapshot.finality.admission.AdmissionCleanupProgress._
import io.constellationnetwork.node.shared.domain.snapshot.finality.admission.AdmissionIdempotencyObservation._
import io.constellationnetwork.node.shared.domain.snapshot.finality.admission.AdmissionOrderingOutcome._
import io.constellationnetwork.node.shared.domain.snapshot.finality.admission.AdmissionOrderingStage._
import io.constellationnetwork.node.shared.domain.snapshot.finality.admission.AdmissionOrderingTransition._
import io.constellationnetwork.security.hash.Hash

import eu.timepit.refined.types.numeric.NonNegLong
import org.scalacheck.Gen
import weaver.SimpleIOSuite
import weaver.scalacheck.Checkers

object AdmissionOrderingReferenceModelSuite extends SimpleIOSuite with Checkers {

  private def hash(seed: Int): Hash = Hash(f"$seed%064x")
  private def nonNeg(value: Long): NonNegLong = NonNegLong.unsafeFrom(value)

  private def context(
    release: Long,
    lineage: Long,
    branch: Long
  ): AdmissionOrderingContext =
    AdmissionOrderingContext(
      ReleaseGeneration(nonNeg(release)),
      CanonicalLineageRevision(nonNeg(lineage)),
      CanonicalBranchRevision(nonNeg(branch))
    )

  private def key(seed: Int): EffectIdempotencyKey =
    EffectIdempotencyKey(hash(100000 + seed))

  private val initialContext = context(7L, 11L, 13L)
  private val initialScope = AdmissionOrderingScope(ArtifactId(hash(1)), initialContext)

  private def initial(revision: Long = 0L): AdmissionOrderingState =
    AdmissionOrderingReferenceModel.initialize(initialScope, AdmissionOrderingRevision(nonNeg(revision)))

  private def replacementContext(
    current: AdmissionOrderingContext,
    delta: Long = 1L
  ): AdmissionOrderingContext =
    context(
      current.releaseGeneration.value.value,
      current.lineageRevision.value.value + delta,
      current.branchRevision.value.value + delta
    )

  private def replacementScope(
    current: AdmissionOrderingScope,
    artifactSeed: Int,
    delta: Long = 1L
  ): AdmissionOrderingScope =
    AdmissionOrderingScope(ArtifactId(hash(artifactSeed)), replacementContext(current.context, delta))

  private def command(
    state: AdmissionOrderingState,
    transition: AdmissionOrderingTransition,
    keySeed: Int,
    scopeOverride: Option[AdmissionOrderingScope] = None,
    contextOverride: Option[AdmissionOrderingContext] = None,
    revisionOverride: Option[AdmissionOrderingRevision] = None
  ): AdmissionOrderingCommand = {
    val scope = scopeOverride.getOrElse(state.scope)
    AdmissionOrderingCommand(
      scope,
      contextOverride.getOrElse(scope.context),
      revisionOverride.getOrElse(state.revision),
      key(keySeed),
      transition
    )
  }

  private def invoke(
    state: AdmissionOrderingState,
    command: AdmissionOrderingCommand,
    idempotency: AdmissionIdempotencyObservation = AuthoritativelyAbsent
  ): AdmissionOrderingOutcome =
    AdmissionOrderingReferenceModel.applyCommand(state, command, idempotency)

  private def applyFreshWithRecord(
    state: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): (AdmissionOrderingState, AdmissionIdempotencyRecord) =
    invoke(state, command) match {
      case Applied(next, record) => (next, record)
      case other                 => throw new IllegalStateException(s"Expected fresh application, got $other")
    }

  private def applyFresh(
    state: AdmissionOrderingState,
    command: AdmissionOrderingCommand
  ): AdmissionOrderingState =
    applyFreshWithRecord(state, command)._1

  private def advanceForward(count: Int): AdmissionOrderingState =
    (0 until count).foldLeft(initial()) {
      case (state, index) => applyFresh(state, command(state, ApplyForwardMutation, 10 + index))
    }

  private def invalidatedStage(state: AdmissionOrderingState): Invalidated =
    state.stage match {
      case value: Invalidated => value
      case other              => throw new IllegalStateException(s"Expected invalidated stage, got $other")
    }

  pureTest("generic forward mutations are repeatable under the current scope") {
    val states = (0 until 20).scanLeft(initial()) {
      case (state, index) => applyFresh(state, command(state, ApplyForwardMutation, 20 + index))
    }

    expect(states.forall(_.stage == Current)) &&
    expect.same(20L, states.last.revision.value.value)
  }

  pureTest("inverse and requeue are invalid before replacement") {
    val before = initial()

    val inverse = invoke(before, command(before, OrderInverse, 100)) match {
      case Retry(state, AdmissionRetryReason.UnexpectedStage(Current, OrderInverse)) => expect.same(before, state)
      case other => failure(s"Expected inverse ordering to retry, got $other")
    }
    val requeue = invoke(before, command(before, OrderRequeue, 101)) match {
      case Retry(state, AdmissionRetryReason.UnexpectedStage(Current, OrderRequeue)) => expect.same(before, state)
      case other => failure(s"Expected requeue ordering to retry, got $other")
    }

    inverse && requeue
  }

  test("PROPERTY: identical forward-command retries are current only at the exact committed state") {
    forall(Gen.chooseNum(0, 20)) { prefix =>
      val before = advanceForward(prefix)
      val nextCommand = command(before, ApplyForwardMutation, 200 + prefix)
      val (after, record) = applyFreshWithRecord(before, nextCommand)

      invoke(after, nextCommand, Recorded(record)) match {
        case AlreadyAppliedCurrent(replayedState, replayedRecord) =>
          expect.same(after, replayedState) &&
          expect.same(record, replayedRecord) &&
          expect.same(before, record.before) &&
          expect.same(after, record.after)
        case other => failure(s"Expected current idempotent replay, got $other")
      }
    }
  }

  pureTest("invalidation, inverse, and requeue records replay only at their exact committed states") {
    val start = advanceForward(2)
    val scopeB = replacementScope(start.scope, artifactSeed = 2)

    val invalidateCommand = command(start, Invalidate(scopeB), 250)
    val (invalidated, invalidateRecord) = applyFreshWithRecord(start, invalidateCommand)
    val invalidateReplay = invoke(invalidated, invalidateCommand, Recorded(invalidateRecord)) match {
      case AlreadyAppliedCurrent(state, record) => expect.same(invalidated, state) && expect.same(invalidateRecord, record)
      case other                                => failure(s"Expected current invalidation replay, got $other")
    }

    val inverseCommand = command(invalidated, OrderInverse, 251)
    val (inverseOrdered, inverseRecord) = applyFreshWithRecord(invalidated, inverseCommand)
    val inverseReplay = invoke(inverseOrdered, inverseCommand, Recorded(inverseRecord)) match {
      case AlreadyAppliedCurrent(state, record) => expect.same(inverseOrdered, state) && expect.same(inverseRecord, record)
      case other                                => failure(s"Expected current inverse replay, got $other")
    }

    val requeueCommand = command(inverseOrdered, OrderRequeue, 252)
    val (requeueOrdered, requeueRecord) = applyFreshWithRecord(inverseOrdered, requeueCommand)
    val requeueReplay = invoke(requeueOrdered, requeueCommand, Recorded(requeueRecord)) match {
      case AlreadyAppliedCurrent(state, record) => expect.same(requeueOrdered, state) && expect.same(requeueRecord, record)
      case other                                => failure(s"Expected current requeue replay, got $other")
    }

    invalidateReplay && inverseReplay && requeueReplay
  }

  pureTest("cleanup records become historical after a later cleanup state") {
    val start = initial()
    val scopeB = replacementScope(start.scope, artifactSeed = 20)
    val invalidateCommand = command(start, Invalidate(scopeB), 260)
    val (invalidated, invalidateRecord) = applyFreshWithRecord(start, invalidateCommand)
    val inverseCommand = command(invalidated, OrderInverse, 261)
    val (inverseOrdered, inverseRecord) = applyFreshWithRecord(invalidated, inverseCommand)
    val requeueCommand = command(inverseOrdered, OrderRequeue, 262)
    val requeueOrdered = applyFresh(inverseOrdered, requeueCommand)

    val invalidationHistory = invoke(requeueOrdered, invalidateCommand, Recorded(invalidateRecord)) match {
      case HistoricallyRecorded(state, record) =>
        expect.same(requeueOrdered, state) && expect.same(invalidateRecord, record)
      case other => failure(s"Expected invalidation record to be historical, got $other")
    }
    val inverseHistory = invoke(requeueOrdered, inverseCommand, Recorded(inverseRecord)) match {
      case HistoricallyRecorded(state, record) =>
        expect.same(requeueOrdered, state) && expect.same(inverseRecord, record)
      case other => failure(s"Expected inverse record to be historical, got $other")
    }

    invalidationHistory && inverseHistory
  }

  pureTest("reusing an idempotency key for a different command requires recovery") {
    val before = initial()
    val first = command(before, ApplyForwardMutation, 300)
    val (after, firstRecord) = applyFreshWithRecord(before, first)
    val conflicting = command(after, ApplyForwardMutation, 300)

    invoke(after, conflicting, Recorded(firstRecord)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.IdempotencyConflict(conflictKey, recorded, supplied)) =>
        expect.same(after, state) &&
        expect.same(first.idempotencyKey, conflictKey) &&
        expect.same(first, recorded) &&
        expect.same(conflicting, supplied)
      case other => failure(s"Expected idempotency-conflict recovery, got $other")
    }
  }

  pureTest("recorded transition with before-state, conflicting state, or malformed after-state requires recovery") {
    val before = initial()
    val forward = command(before, ApplyForwardMutation, 301)
    val (after, record) = applyFreshWithRecord(before, forward)
    val sameRevisionConflict = after.copy(scope = after.scope.copy(opaqueArtifactId = ArtifactId(hash(99))))
    val forgedAfter = after.copy(revision = AdmissionOrderingRevision(nonNeg(2L)))
    val forgedRecord = record.copy(after = forgedAfter)

    val stateNotAdvanced = invoke(before, forward, Recorded(record)) match {
      case RecoveryRequired(state, _: AdmissionRecoveryReason.IdempotencyStateConflict) => expect.same(before, state)
      case other => failure(s"Expected non-advanced state to require recovery, got $other")
    }
    val conflictingState = invoke(sameRevisionConflict, forward, Recorded(record)) match {
      case RecoveryRequired(state, _: AdmissionRecoveryReason.IdempotencyStateConflict) =>
        expect.same(sameRevisionConflict, state)
      case other => failure(s"Expected same-revision state conflict to require recovery, got $other")
    }
    val malformedRecord = invoke(forgedAfter, forward, Recorded(forgedRecord)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.InvalidIdempotencyRecord(observed)) =>
        expect.same(forgedAfter, state) && expect.same(forgedRecord, observed)
      case other => failure(s"Expected malformed record to require recovery, got $other")
    }

    stateNotAdvanced && conflictingState && malformedRecord
  }

  pureTest("idempotency lookup mismatch and unavailable compacted history fail closed") {
    val before = initial()
    val requested = command(before, ApplyForwardMutation, 310)
    val unrelatedCommand = command(before, ApplyForwardMutation, 311)
    val (_, unrelatedRecord) = applyFreshWithRecord(before, unrelatedCommand)

    val mismatch = invoke(before, requested, Recorded(unrelatedRecord)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.IdempotencyLookupMismatch(expected, observed)) =>
        expect.same(before, state) &&
        expect.same(requested.idempotencyKey, expected) &&
        expect.same(unrelatedRecord.key, observed)
      case other => failure(s"Expected idempotency lookup mismatch, got $other")
    }

    val unavailable = invoke(before, requested, HistoryUnavailable) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.IdempotencyHistoryUnavailable(observedKey)) =>
        expect.same(before, state) && expect.same(requested.idempotencyKey, observedKey)
      case other => failure(s"Expected unavailable-history recovery, got $other")
    }

    mismatch && unavailable
  }

  pureTest("a stale ordering revision is retryable without mutation") {
    val before = initial()
    val staleRevision = AdmissionOrderingRevision(nonNeg(9L))
    val staleCommand = command(before, ApplyForwardMutation, 320, revisionOverride = Some(staleRevision))

    invoke(before, staleCommand) match {
      case Retry(state, AdmissionRetryReason.RevisionChanged(`staleRevision`, actual)) =>
        expect.same(before, state) && expect.same(before.revision, actual)
      case other => failure(s"Expected ordering-revision retry, got $other")
    }
  }

  pureTest("opaque artifact scope and observed lineage are exact-match guards") {
    val before = initial()
    val wrongScope = initialScope.copy(opaqueArtifactId = ArtifactId(hash(2)))
    val wrongScopeCommand = command(before, ApplyForwardMutation, 330, scopeOverride = Some(wrongScope))
    val staleContext = initialContext.copy(branchRevision = CanonicalBranchRevision(nonNeg(99L)))
    val staleContextCommand = command(before, ApplyForwardMutation, 331, contextOverride = Some(staleContext))

    val scopeCheck = invoke(before, wrongScopeCommand) match {
      case Stale(state, _: AdmissionStaleReason.ScopeMismatch) => expect.same(before, state)
      case other                                               => failure(s"Expected scope mismatch, got $other")
    }
    val contextCheck = invoke(before, staleContextCommand) match {
      case Stale(state, _: AdmissionStaleReason.ContextChanged) => expect.same(before, state)
      case other                                                => failure(s"Expected context change, got $other")
    }

    scopeCheck && contextCheck
  }

  test("PROPERTY: replacement after any forward prefix makes a fresh old-scope command stale") {
    forall(Gen.chooseNum(0, 20)) { prefix =>
      val before = advanceForward(prefix)
      val oldCommand = command(before, ApplyForwardMutation, 400 + prefix)
      val scopeB = replacementScope(before.scope, artifactSeed = 1000 + prefix)
      val invalidated = applyFresh(before, command(before, Invalidate(scopeB), 500 + prefix))

      invoke(invalidated, oldCommand) match {
        case Stale(state, _: AdmissionStaleReason.ScopeMismatch) =>
          expect.same(invalidated, state) && expect(state.stage.isInstanceOf[Invalidated])
        case other => failure(s"Expected old scope to be stale, got $other")
      }
    }
  }

  pureTest("a recorded old-scope transition is historical and non-authorizing after replacement") {
    val start = initial()
    val forward = command(start, ApplyForwardMutation, 580)
    val (advanced, forwardRecord) = applyFreshWithRecord(start, forward)
    val scopeB = replacementScope(advanced.scope, artifactSeed = 2)
    val invalidated = applyFresh(advanced, command(advanced, Invalidate(scopeB), 581))

    invoke(invalidated, forward, Recorded(forwardRecord)) match {
      case HistoricallyRecorded(state, record) =>
        expect.same(invalidated, state) &&
        expect.same(forwardRecord, record) &&
        expect(state.stage.isInstanceOf[Invalidated]) &&
        expect.same(scopeB, state.scope)
      case other => failure(s"Expected non-authorizing historical result, got $other")
    }
  }

  pureTest("replacement cleanup orders inverse before requeue") {
    val before = advanceForward(6)
    val scopeB = replacementScope(before.scope, artifactSeed = 2)
    val invalidated = applyFresh(before, command(before, Invalidate(scopeB), 600))

    val earlyRequeue = invoke(invalidated, command(invalidated, OrderRequeue, 601)) match {
      case Retry(state, _: AdmissionRetryReason.UnexpectedStage) => expect.same(invalidated, state)
      case other                                                 => failure(s"Expected early requeue to retry, got $other")
    }

    val inverseOrdered = applyFresh(invalidated, command(invalidated, OrderInverse, 602))
    val repeatedInverse = invoke(inverseOrdered, command(inverseOrdered, OrderInverse, 603)) match {
      case Retry(state, _: AdmissionRetryReason.UnexpectedStage) => expect.same(inverseOrdered, state)
      case other                                                 => failure(s"Expected repeated inverse to retry, got $other")
    }
    val requeueOrdered = applyFresh(inverseOrdered, command(inverseOrdered, OrderRequeue, 604))

    earlyRequeue &&
    repeatedInverse &&
    expect.same(RequeueOrdered, invalidatedStage(requeueOrdered).cleanup) &&
    expect.same(initialScope, invalidatedStage(requeueOrdered).displacedScope) &&
    expect.same(before.revision.value.value + 3L, requeueOrdered.revision.value.value)
  }

  pureTest("a second replacement requires external reconciliation and leaves state unchanged") {
    val before = advanceForward(4)
    val scopeB = replacementScope(before.scope, artifactSeed = 2)
    val firstInvalidation = applyFresh(before, command(before, Invalidate(scopeB), 610))
    val inverseOrdered = applyFresh(firstInvalidation, command(firstInvalidation, OrderInverse, 611))
    val scopeC = replacementScope(scopeB, artifactSeed = 3)
    val secondReplacement = command(inverseOrdered, Invalidate(scopeC), 612)

    invoke(inverseOrdered, secondReplacement) match {
      case RecoveryRequired(
            state,
            AdmissionRecoveryReason.ReplacementReconciliationRequired(current, requested)
          ) =>
        expect.same(inverseOrdered, state) &&
        expect.same(scopeB, current) &&
        expect.same(scopeC, requested) &&
        expect.same(InverseOrdered, invalidatedStage(state).cleanup)
      case other => failure(s"Expected replacement reconciliation recovery, got $other")
    }
  }

  pureTest("second replacement requires reconciliation before and after cleanup completion") {
    val start = initial()
    val scopeB = replacementScope(start.scope, artifactSeed = 30)
    val replacementOrdered = applyFresh(start, command(start, Invalidate(scopeB), 615))
    val inverseOrdered = applyFresh(replacementOrdered, command(replacementOrdered, OrderInverse, 616))
    val requeueOrdered = applyFresh(inverseOrdered, command(inverseOrdered, OrderRequeue, 617))
    val scopeC = replacementScope(scopeB, artifactSeed = 31)

    val beforeCleanup = invoke(
      replacementOrdered,
      command(replacementOrdered, Invalidate(scopeC), 618)
    ) match {
      case RecoveryRequired(state, _: AdmissionRecoveryReason.ReplacementReconciliationRequired) =>
        expect.same(replacementOrdered, state) &&
        expect.same(ReplacementOrdered, invalidatedStage(state).cleanup)
      case other => failure(s"Expected pre-cleanup replacement reconciliation, got $other")
    }
    val afterCleanup = invoke(
      requeueOrdered,
      command(requeueOrdered, Invalidate(scopeC), 619)
    ) match {
      case RecoveryRequired(state, _: AdmissionRecoveryReason.ReplacementReconciliationRequired) =>
        expect.same(requeueOrdered, state) &&
        expect.same(RequeueOrdered, invalidatedStage(state).cleanup)
      case other => failure(s"Expected post-cleanup replacement reconciliation, got $other")
    }

    beforeCleanup && afterCleanup
  }

  pureTest("complete scopes prevent A-to-B-to-A reuse even with higher replacement revisions") {
    val stateA0 = initial()
    val oldACommand = command(stateA0, ApplyForwardMutation, 620)
    val scopeB = replacementScope(stateA0.scope, artifactSeed = 2)
    val invalidatedA = applyFresh(stateA0, command(stateA0, Invalidate(scopeB), 621))

    // Reconciliation is outside this model. A new sequential epoch represents its completed result without specifying how it occurred.
    val reconciledB = AdmissionOrderingReferenceModel.initialize(scopeB, invalidatedA.revision)
    val scopeA2 = AdmissionOrderingScope(initialScope.opaqueArtifactId, replacementContext(scopeB.context))
    val invalidatedB = applyFresh(reconciledB, command(reconciledB, Invalidate(scopeA2), 622))

    val staleOldA = invoke(invalidatedB, oldACommand) match {
      case Stale(state, _: AdmissionStaleReason.ScopeMismatch) => expect.same(invalidatedB, state)
      case other                                               => failure(s"Expected old A scope to remain stale, got $other")
    }

    staleOldA &&
    expect.same(initialScope.opaqueArtifactId, scopeA2.opaqueArtifactId) &&
    expect(scopeA2.context.lineageRevision.value.value > initialScope.context.lineageRevision.value.value) &&
    expect(scopeA2.context.branchRevision.value.value > initialScope.context.branchRevision.value.value)
  }

  pureTest("replacement rejects release decrease and equal lineage or branch revisions") {
    val start = initial()
    val releaseDecreased = AdmissionOrderingScope(ArtifactId(hash(40)), context(6L, 12L, 14L))
    val equalLineage = AdmissionOrderingScope(ArtifactId(hash(41)), context(7L, 11L, 14L))
    val equalBranch = AdmissionOrderingScope(ArtifactId(hash(42)), context(7L, 12L, 13L))

    def rejects(candidate: AdmissionOrderingScope, seed: Int) =
      invoke(start, command(start, Invalidate(candidate), seed)) match {
        case Stale(state, AdmissionStaleReason.ReplacementNotNewer(current, replacement)) =>
          expect.same(start, state) &&
          expect.same(initialContext, current) &&
          expect.same(candidate.context, replacement)
        case other => failure(s"Expected non-monotone replacement rejection, got $other")
      }

    rejects(releaseDecreased, 623) &&
    rejects(equalLineage, 624) &&
    rejects(equalBranch, 625)
  }

  pureTest("lineage and branch replacement revision exhaustion require recovery") {
    val lineageMaxScope = AdmissionOrderingScope(ArtifactId(hash(10)), context(7L, Long.MaxValue, 13L))
    val lineageMaxState =
      AdmissionOrderingReferenceModel.initialize(lineageMaxScope, AdmissionOrderingRevision(nonNeg(0L)))
    val lineageCandidate = AdmissionOrderingScope(ArtifactId(hash(11)), context(7L, Long.MaxValue, 14L))

    val branchMaxScope = AdmissionOrderingScope(ArtifactId(hash(12)), context(7L, 11L, Long.MaxValue))
    val branchMaxState =
      AdmissionOrderingReferenceModel.initialize(branchMaxScope, AdmissionOrderingRevision(nonNeg(0L)))
    val branchCandidate = AdmissionOrderingScope(ArtifactId(hash(13)), context(7L, 12L, Long.MaxValue))

    val lineage = invoke(lineageMaxState, command(lineageMaxState, Invalidate(lineageCandidate), 630)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.ReplacementRevisionExhausted(current)) =>
        expect.same(lineageMaxState, state) && expect.same(lineageMaxScope.context, current)
      case other => failure(s"Expected lineage exhaustion recovery, got $other")
    }
    val branch = invoke(branchMaxState, command(branchMaxState, Invalidate(branchCandidate), 631)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.ReplacementRevisionExhausted(current)) =>
        expect.same(branchMaxState, state) && expect.same(branchMaxScope.context, current)
      case other => failure(s"Expected branch exhaustion recovery, got $other")
    }

    lineage && branch
  }

  pureTest("sequential forward-first and replacement-first orderings enforce the same stale-scope rule") {
    val start = initial()
    val forward = command(start, ApplyForwardMutation, 640)
    val scopeB = replacementScope(start.scope, artifactSeed = 2)
    val sameRevisionInvalidation = command(start, Invalidate(scopeB), 641)

    val forwardFirst = applyFresh(start, forward)
    val staleRevision = invoke(forwardFirst, sameRevisionInvalidation)
    val invalidatedAfterRetry = applyFresh(forwardFirst, command(forwardFirst, Invalidate(scopeB), 642))

    val invalidationFirst = applyFresh(start, sameRevisionInvalidation)
    val staleForward = invoke(invalidationFirst, forward)

    val retryCheck = staleRevision match {
      case Retry(state, _: AdmissionRetryReason.RevisionChanged) => expect.same(forwardFirst, state)
      case other                                                 => failure(s"Expected superseded invalidation revision, got $other")
    }
    val staleCheck = staleForward match {
      case Stale(state, _: AdmissionStaleReason.ScopeMismatch) => expect.same(invalidationFirst, state)
      case other                                               => failure(s"Expected old forward scope to become stale, got $other")
    }

    retryCheck &&
    staleCheck &&
    expect(invalidatedAfterRetry.stage.isInstanceOf[Invalidated]) &&
    expect(invalidationFirst.stage.isInstanceOf[Invalidated])
  }

  pureTest("one hundred sequential identical retries do not mutate current state") {
    val start = initial()
    val firstCommand = command(start, ApplyForwardMutation, 650)
    val (afterFirst, record) = applyFreshWithRecord(start, firstCommand)
    val retries = List
      .fill(100)(firstCommand)
      .scanLeft[AdmissionOrderingOutcome](AlreadyAppliedCurrent(afterFirst, record)) {
        case (previous, retry) => invoke(previous.state, retry, Recorded(record))
      }
      .tail

    expect(retries.forall {
      case AlreadyAppliedCurrent(state, replayedRecord) => state == afterFirst && replayedRecord == record
      case _                                            => false
    }) &&
    expect.same(1L, afterFirst.revision.value.value)
  }

  pureTest("a command using a superseded ordering revision retries without mutation") {
    val start = initial()
    val first = command(start, ApplyForwardMutation, 660)
    val superseded = command(start, ApplyForwardMutation, 661)
    val afterFirst = applyFresh(start, first)

    invoke(afterFirst, superseded) match {
      case Retry(state, _: AdmissionRetryReason.RevisionChanged) => expect.same(afterFirst, state)
      case other                                                 => failure(s"Expected superseded revision to retry, got $other")
    }
  }

  pureTest("ordering revision exhaustion fails closed into recovery") {
    val exhausted = initial(Long.MaxValue)

    invoke(exhausted, command(exhausted, ApplyForwardMutation, 670)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.RevisionExhausted(revision)) =>
        expect.same(exhausted, state) && expect.same(exhausted.revision, revision)
      case other => failure(s"Expected ordering-revision exhaustion recovery, got $other")
    }
  }

  pureTest("ordering revision exhaustion fails closed during invalidation, inverse, and requeue") {
    val scopeB = replacementScope(initialScope, artifactSeed = 50)

    val invalidateExhausted = initial(Long.MaxValue)
    val invalidateCheck = invoke(
      invalidateExhausted,
      command(invalidateExhausted, Invalidate(scopeB), 680)
    ) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.RevisionExhausted(revision)) =>
        expect.same(invalidateExhausted, state) && expect.same(invalidateExhausted.revision, revision)
      case other => failure(s"Expected invalidation revision exhaustion, got $other")
    }

    val beforeInverse = initial(Long.MaxValue - 1L)
    val inverseExhausted = applyFresh(beforeInverse, command(beforeInverse, Invalidate(scopeB), 681))
    val inverseCheck = invoke(inverseExhausted, command(inverseExhausted, OrderInverse, 682)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.RevisionExhausted(revision)) =>
        expect.same(inverseExhausted, state) && expect.same(inverseExhausted.revision, revision)
      case other => failure(s"Expected inverse revision exhaustion, got $other")
    }

    val beforeRequeue = initial(Long.MaxValue - 2L)
    val replacementOrdered = applyFresh(beforeRequeue, command(beforeRequeue, Invalidate(scopeB), 683))
    val requeueExhausted = applyFresh(replacementOrdered, command(replacementOrdered, OrderInverse, 684))
    val requeueCheck = invoke(requeueExhausted, command(requeueExhausted, OrderRequeue, 685)) match {
      case RecoveryRequired(state, AdmissionRecoveryReason.RevisionExhausted(revision)) =>
        expect.same(requeueExhausted, state) && expect.same(requeueExhausted.revision, revision)
      case other => failure(s"Expected requeue revision exhaustion, got $other")
    }

    invalidateCheck && inverseCheck && requeueCheck
  }

  test("PROPERTY: no forward mutation can apply after invalidation") {
    forall(Gen.chooseNum(0, 20)) { prefix =>
      val before = advanceForward(prefix)
      val scopeB = replacementScope(before.scope, artifactSeed = 2000 + prefix)
      val invalidated = applyFresh(before, command(before, Invalidate(scopeB), 700 + prefix))
      val currentScopeForward = command(invalidated, ApplyForwardMutation, 800 + prefix)

      invoke(invalidated, currentScopeForward) match {
        case Stale(state, _: AdmissionStaleReason.ScopeInvalidated) => expect.same(invalidated, state)
        case other => failure(s"Expected invalidated scope to reject forward work, got $other")
      }
    }
  }
}
