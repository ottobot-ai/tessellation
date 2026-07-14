package io.constellationnetwork.node.shared.domain.snapshot.finality

import cats.data.ValidatedNec
import cats.syntax.all._

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityBaseCodecs.{
  pathChunkPayloadCodec,
  pathManifestPayloadCodec
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.releasedCoreRecordPayloadCodec
import io.constellationnetwork.node.shared.domain.snapshot.finality.CoordinatorMode.{RecoveryRequired, Running}
import io.constellationnetwork.node.shared.domain.snapshot.finality.CoreTransition.{
  Advance,
  DensityReplacement,
  DensityRollbackToOperationalMrca
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.EffectKind._
import io.constellationnetwork.node.shared.domain.snapshot.finality.OperationalQualification.{
  CanonicalDepthK1,
  DecidedAttestationTWeight
}
import io.constellationnetwork.node.shared.domain.snapshot.finality.PathRole._
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.{DurableMptImageStore, MptActivePublication, MptImageReceipt}

/** Pure cross-field validation for the dark greenfield ScodecV1 finality artifacts.
  *
  * This validator does not load content-addressed artifacts or choose a branch. Callers must supply decoded payloads loaded from the claimed
  * immutable locations. Every identity which can be derived from those payloads is recomputed here with the canonical active-era codecs;
  * caller-supplied "derived" identities are never authority.
  */
object FinalityIntentValidator {

  final case class Violation(path: String, invariant: String)

  type ValidationResult[A] = ValidatedNec[Violation, A]

  final case class PreviousEffectManifest(pointer: EffectManifestPointer, manifest: FinalityEffectManifest)

  final case class CoreValidationContext(
    priorReleased: Option[ReleasedCore],
    previousEffects: Option[PreviousEffectManifest]
  )

  /** One decoded chunk paired with the exact immutable pointer by which it was loaded. */
  final case class ResolvedPathChunk(artifact: ImmutableArtifactPointer, value: PathChunk)

  /** Fully resolved manifest input. Artifact pointers and the entries root are derived from these decoded values by the validator. */
  final case class ResolvedPathManifest(
    intentId: IntentId,
    commitment: PathCommitment,
    payload: PathManifestPayload,
    chunks: List[ResolvedPathChunk]
  )

  /** Exact immutable manifest plus all 18 terminal receipts needed to advance the rebuildable outbox cursor. */
  final case class CompletedEffectManifest(
    pointer: EffectManifestPointer,
    manifest: FinalityEffectManifest,
    receipts: List[TerminalEffectReceipt]
  )

  def validateCoreBatch(
    batch: FinalityCoreBatch,
    effectManifest: FinalityEffectManifest,
    resolvedPaths: List[ResolvedPathManifest],
    context: CoreValidationContext
  ): ValidationResult[FinalityCoreBatch] =
    validateValue(
      batch,
      List(
        checkDerived(FinalityIdentity.intentId(batch.scope), batch.intentId, "batch.intentId", "must equal IntentId(scope)"),
        validateHash(batch.scope.domain.networkId, "batch.scope.domain.networkId", rejectZero = true),
        validateHash(batch.scope.domain.genesisHash, "batch.scope.domain.genesisHash", rejectZero = true),
        validateHash(batch.scope.domain.protocolEra, "batch.scope.domain.protocolEra", rejectZero = true),
        check(isExpectedGeneration(batch.scope.generation, context.priorReleased), "batch.scope.generation", "must be prior generation + 1, or zero initially"),
        check(batch.scope.expectedPrior == context.priorReleased.map(_.pointer), "batch.scope.expectedPrior", "must name the exact authenticated prior release"),
        check(batch.scope.target == batch.prepared.target, "batch.scope.target", "must equal the prepared target"),
        check(batch.scope.selection.operationalTarget == batch.scope.target, "batch.scope.selection.operationalTarget", "must equal the release target"),
        check(batch.scope.transition == batch.transition.shape, "batch.scope.transition", "must equal the decoded transition commitment"),
        check(batch.scope.qualification == batch.qualification.scope, "batch.scope.qualification", "must equal the decoded qualification commitment"),
        check(batch.scope.prepared == batch.prepared.commitment, "batch.scope.prepared", "must equal every decoded prepared-core commitment"),
        check(batch.scope.effects == effectManifest.commitment, "batch.scope.effects", "must equal the complete decoded 18-sink effect plan"),
        validateScoped(
          batch.selectionEvidence,
          batch.intentId,
          FinalityArtifactKind.CanonicalSelectionEvidence,
          "batch.selectionEvidence"
        ),
        check(batch.selectionEvidence.artifact == batch.scope.selection.decision, "batch.selectionEvidence", "must be the exact selection-decision pointer committed by the scope"),
        validateSelection(batch.scope.selection, "batch.scope.selection"),
        validateResolvedCorePaths(batch, resolvedPaths),
        validateTransition(batch.intentId, batch.scope.target, batch.transition, context.priorReleased),
        validateQualification(batch.intentId, batch.scope.target, batch.qualification, batch.transition, context.priorReleased),
        validatePrepared(batch.intentId, batch.prepared, context.priorReleased),
        validateEffectManifest(batch, effectManifest, context)
      )
    )

  /** Test-visible partial check; production admission must call `validateCoreBatch`, which always invokes this check. */
  private[finality] def validateResolvedCorePaths(
    batch: FinalityCoreBatch,
    resolvedPaths: List[ResolvedPathManifest]
  ): ValidationResult[Unit] = {
    val transitionCommitments = batch.transition match {
      case Advance(adopted)                                 => List(adopted.commitment)
      case DensityReplacement(_, orphaned, adopted, _)      => List(orphaned.commitment, adopted.commitment)
      case DensityRollbackToOperationalMrca(_, orphaned, _) => List(orphaned.commitment)
    }
    val expectedCommitments = batch.scope.selection.lineage :: transitionCommitments ::: batch.qualification.ancestorClosure.toList.map(_.commitment)
    val actualCommitments = resolvedPaths.map(_.commitment)
    val expectedMultiplicity = expectedCommitments.groupBy(identity).map { case (commitment, values) => commitment -> values.size }
    val actualMultiplicity = actualCommitments.groupBy(identity).map { case (commitment, values) => commitment -> values.size }

    val resolvedChecks = resolvedPaths.zipWithIndex.flatMap {
      case (resolved, index) =>
        List(
          check(resolved.intentId == batch.intentId, s"batch.resolvedPaths[$index].intentId", "must equal the batch intent"),
          validateResolvedPath(resolved).void
        )
    }

    def resolved(commitment: PathCommitment): Option[ResolvedPathManifest] =
      resolvedPaths.find(_.commitment == commitment)

    def entries(path: ResolvedPathManifest): List[GlobalSnapshotStateRef] =
      path.chunks.flatMap(_.value.entriesOldestFirst.toList)

    val qualificationLineage = batch.qualification.ancestorClosure match {
      case None => validUnit
      case Some(closureRef) =>
        (resolved(closureRef.commitment), resolved(batch.scope.selection.lineage)) match {
          case (Some(closure), Some(lineage)) =>
            batch.transition match {
              case rollback: DensityRollbackToOperationalMrca =>
                resolved(rollback.orphaned.commitment) match {
                  case None => invalid("batch.resolvedPaths", "must resolve the rollback orphaned path")
                  case Some(orphaned) =>
                    check(
                      entries(closure).drop(1).startsWith(entries(orphaned)),
                      "batch.qualification.ancestorClosure",
                      "inherited rollback closure must continue through the exact orphaned old-canonical suffix"
                    )
                }
              case _ =>
                check(
                  entries(lineage).startsWith(entries(closure)),
                  "batch.qualification.ancestorClosure",
                  "qualification closure must be an exact prefix of the selected canonical lineage"
                )
            }
          case _ => invalid("batch.resolvedPaths", "must resolve both qualification closure and canonical lineage")
        }
    }

    val densityPathSemantics = batch.transition match {
      case _: Advance => validUnit
      case replacement: DensityReplacement =>
        (resolved(replacement.orphaned.commitment), resolved(replacement.adopted.commitment)) match {
          case (Some(orphaned), Some(adopted)) =>
            val orphanedEntries = entries(orphaned)
            val adoptedEntries = entries(adopted)
            val orphanedFirst = orphanedEntries.headOption
            val adoptedFirst = adoptedEntries.headOption

            combine(
              List(
                check(
                  orphanedFirst.exists(isDirectChild(replacement.commonAncestor, _)),
                  "batch.transition.orphaned",
                  "must be a nonempty child suffix immediately after the claimed common ancestor"
                ),
                check(
                  adoptedFirst.exists(isDirectChild(replacement.commonAncestor, _)),
                  "batch.transition.adopted",
                  "must be a nonempty child suffix immediately after the claimed common ancestor"
                ),
                check(
                  (orphanedFirst, adoptedFirst) match {
                    case (Some(oldChild), Some(newChild)) => oldChild.hash != newChild.hash
                    case _                                => false
                  },
                  "batch.transition.commonAncestor",
                  "must be the exact divergence point: orphaned and adopted paths require different first-child hashes"
                ),
                check(
                  (orphanedEntries.lastOption, adoptedEntries.lastOption) match {
                    case (Some(oldTarget), Some(newTarget)) => oldTarget.hash != newTarget.hash
                    case _                                  => false
                  },
                  "batch.transition.adopted.newest",
                  "density replacement must select a different target hash, not re-release the orphaned target"
                )
              )
            )
          case _ => invalid("batch.resolvedPaths", "must resolve both density-replacement suffixes")
        }
      case rollback: DensityRollbackToOperationalMrca =>
        (resolved(rollback.orphaned.commitment), resolved(batch.scope.selection.lineage)) match {
          case (Some(orphaned), Some(lineage)) =>
            val orphanedFirst = entries(orphaned).headOption
            val lineageEntries = entries(lineage)
            val selectedSuccessor = lineageEntries.drop(1).headOption

            combine(
              List(
                check(
                  orphanedFirst.exists(isDirectChild(rollback.operationalMrca, _)),
                  "batch.transition.orphaned",
                  "must be a nonempty child suffix immediately after the operational MRCA"
                ),
                check(
                  lineageEntries.headOption.contains(rollback.operationalMrca),
                  "batch.scope.selection.lineage",
                  "must start at the exact operational MRCA"
                ),
                check(
                  selectedSuccessor.isEmpty || orphanedFirst.exists(oldChild => selectedSuccessor.exists(_.hash != oldChild.hash)),
                  "batch.scope.selection.lineage",
                  "may stop at the MRCA or continue only through a different first-child hash than the orphaned suffix"
                )
              )
            )
          case _ => invalid("batch.resolvedPaths", "must resolve both rollback orphaned suffix and selected canonical lineage")
        }
    }

    combine(
      List(
        check(
          actualMultiplicity == expectedMultiplicity,
          "batch.resolvedPaths",
          "must contain exactly the complete multiset of canonical lineage, transition paths, and optional qualification closure"
        ),
        qualificationLineage,
        densityPathSemantics
      ) ++ resolvedChecks
    )
  }

  def validateResolvedPath(resolved: ResolvedPathManifest): ValidationResult[ResolvedPathManifest] = {
    val chunks = resolved.chunks
    val entries = chunks.flatMap(_.value.entriesOldestFirst.toList)
    val derivedEntriesRoot = FinalityIdentity.pathEntriesRoot(entries)
    val derivedManifest = FinalityIdentity.artifactPointer(
      FinalityArtifactKind.PathManifest,
      pathManifestPayloadCodec,
      resolved.payload
    )

    val chunkChecks = chunks.zipWithIndex.flatMap {
      case (resolvedChunk, index) =>
        val chunk = resolvedChunk.value
        val nextExpected = chunks.lift(index + 1)
        val expectedNext = nextExpected.map { next =>
          PathChunkPointer(resolved.intentId, resolved.commitment.manifest.id, next.value.chunkIndex, next.artifact)
        }

        List(
          validateArtifact(resolvedChunk.artifact, FinalityArtifactKind.PathChunk, s"path.chunks[$index].artifact"),
          checkDerived(
            FinalityIdentity.artifactPointer(FinalityArtifactKind.PathChunk, pathChunkPayloadCodec, chunk),
            resolvedChunk.artifact,
            s"path.chunks[$index].artifact",
            "must equal the canonical pointer derived from the complete chunk payload"
          ),
          check(chunk.intentId == resolved.intentId, s"path.chunks[$index].intentId", "must equal the manifest intent"),
          check(chunk.manifestId == resolved.commitment.manifest.id, s"path.chunks[$index].manifestId", "must equal the manifest artifact id"),
          check(chunk.chunkIndex.value == index.toLong, s"path.chunks[$index].chunkIndex", "must be contiguous from zero"),
          check(chunk.entriesOldestFirst.size <= PathChunk.MaxEntries, s"path.chunks[$index].entries", s"must contain at most ${PathChunk.MaxEntries} entries"),
          check(
            nextExpected.isEmpty || chunk.entriesOldestFirst.size == PathChunk.MaxEntries,
            s"path.chunks[$index].entries",
            s"every nonterminal chunk must use the canonical full framing of ${PathChunk.MaxEntries} entries"
          ),
          check(chunk.next == expectedNext, s"path.chunks[$index].next", "must point exactly to the next resolved chunk, and the final chunk must terminate")
        )
    }

    val adjacencyChecks = entries.sliding(2).zipWithIndex.toList.flatMap {
      case (List(before, after), index) => validateAdjacent(before, after, s"path.entries[$index]") :: Nil
      case _                            => Nil
    }
    val entryChecks = entries.zipWithIndex.map {
      case (entry, index) => validateStateRef(entry, s"path.entries[$index]")
    }

    validateValue(
      resolved,
      List(
        validatePathCommitment(resolved.commitment, resolved.commitment.summary.role, "path.commitment"),
        validateHash(resolved.intentId.value, "path.intentId", rejectZero = true),
        check(resolved.payload == resolved.commitment.payload, "path.payload", "must equal the exact scope-independent manifest payload"),
        checkDerived(
          derivedManifest,
          resolved.commitment.manifest,
          "path.commitment.manifest",
          "must equal the canonical pointer derived from PathManifestPayload"
        ),
        check(chunks.nonEmpty, "path.chunks", "must resolve chunk zero and at least one chunk"),
        check(BigInt(entries.size) == BigInt(resolved.commitment.summary.entryCount.value), "path.entries", "must contain exactly entryCount state refs"),
        check(entries.headOption.contains(resolved.commitment.summary.oldest), "path.entries.head", "must equal summary.oldest"),
        check(entries.lastOption.contains(resolved.commitment.summary.newest), "path.entries.last", "must equal summary.newest"),
        checkDerived(
          derivedEntriesRoot,
          resolved.commitment.entriesRoot,
          "path.entriesRoot",
          "must equal the canonical framing-independent root of the oldest-first state refs"
        )
      ) ++ chunkChecks ++ entryChecks ++ adjacencyChecks
    )
  }

  def validateReleasedCore(
    batch: FinalityCoreBatch,
    released: ReleasedCore
  ): ValidationResult[ReleasedCore] = {
    val receipt = released.payload.receipt
    val derivedRecordArtifact = FinalityIdentity.artifactPointer(
      FinalityArtifactKind.ReleasedCoreRecord,
      releasedCoreRecordPayloadCodec,
      released.payload
    )

    validateValue(
      released,
      List(
        checkDerived(
          derivedRecordArtifact,
          released.pointer.record,
          "released.pointer.record",
          "must equal the canonical pointer derived from ReleasedCoreRecordPayload"
        ),
        validateScoped(released.record, batch.intentId, FinalityArtifactKind.ReleasedCoreRecord, "released.record"),
        check(released.record.artifact == released.pointer.record, "released.record", "must wrap the exact derived record pointer"),
        check(released.pointer.intentId == batch.intentId, "released.pointer.intentId", "must equal the batch intent"),
        check(released.pointer.generation == batch.scope.generation, "released.pointer.generation", "must equal the batch generation"),
        check(released.pointer.target == batch.scope.target, "released.pointer.target", "must equal the batch target"),
        check(released.payload.qualification == batch.qualification, "released.payload.qualification", "must equal the validated batch qualification"),
        check(released.payload.effectManifest == batch.effectManifest, "released.payload.effectManifest", "must equal the pre-persisted effect manifest"),
        check(receipt.intentId == batch.intentId, "released.payload.receipt.intentId", "must equal the batch intent"),
        check(receipt.generation == batch.scope.generation, "released.payload.receipt.generation", "must equal the batch generation"),
        check(receipt.target == batch.scope.target, "released.payload.receipt.target", "must equal the exact batch target"),
        check(receipt.beforePublication == batch.prepared.expectedBefore, "released.payload.receipt.beforePublication", "must read back the exact prepared CAS prior"),
        check(receipt.activePublication == batch.prepared.targetPublication, "released.payload.receipt.activePublication", "must read back the exact prepared target publication"),
        validateScoped(receipt.semanticReceipt, batch.intentId, FinalityArtifactKind.AppliedSemanticStateReceipt, "released.payload.receipt.semanticReceipt"),
        validateScoped(receipt.authenticatedAnchorReceipt, batch.intentId, FinalityArtifactKind.AuthenticatedAnchorReceipt, "released.payload.receipt.authenticatedAnchorReceipt")
      )
    )
  }

  def validateCoordinatorHead(head: CoordinatorHead): ValidationResult[CoordinatorHead] = {
    val releasedPointer = head.released.map(_.pointer)
    val expectedEffects = head.released.map(released => EffectsIndex(released.payload.effectManifest.some, released.pointer.generation.some)).getOrElse(EffectsIndex(None, None))

    validateValue(
      head,
      List(
        validateEffectsIndex(head.effects, "head.effects"),
        check(head.effects == expectedEffects, "head.effects", "must exactly index the latest released core, or be empty before the first release"),
        check(head.released.isEmpty || head.lastAttempt.nonEmpty, "head.lastAttempt", "must be present after any release"),
        head.active.fold(validUnit)(validateActiveIntent),
        head.active.fold(validUnit) { active =>
          combine(
            List(
              check(active.scope.expectedPrior == releasedPointer, "head.active.scope.expectedPrior", "must equal the current released core"),
              check(isExpectedGeneration(active.scope.generation, head.released), "head.active.scope.generation", "must be current release generation + 1"),
              check(head.lastAttempt.contains(active.scope.attempt), "head.lastAttempt", "must equal the active intent attempt")
            )
          )
        }
      )
    )
  }

  /** Validate a mutable-head replacement and the independently addressed audit record which explains it. */
  def validateCoordinatorTransition(
    before: Option[CoordinatorHead],
    after: CoordinatorHead,
    audit: CoordinatorAuditRecord,
    recoveryRecord: Option[RecoveryRecord] = None
  ): ValidationResult[CoordinatorHead] = {
    val common = List(
      validateCoordinatorHead(after).void,
      check(audit.before == before.map(_.commitment), "audit.before", "must equal the replaced head commitment"),
      check(audit.after == after.commitment, "audit.after", "must equal the new head commitment"),
      check(audit.priorAudit == before.flatMap(_.auditTail), "audit.priorAudit", "must equal the replaced head audit tail"),
      checkDerived(
        FinalityIdentity.auditPointer(audit).map(_.some),
        after.auditTail,
        "head.auditTail",
        "must contain the canonical pointer derived from the audit record"
      )
    )

    val transitionChecks = before match {
      case None =>
        combine(
          List(
            validateInitialization(after, audit),
            check(recoveryRecord.isEmpty, "recoveryRecord", "initialization cannot install a recovery record")
          )
        )
      case Some(prior) =>
        combine(
          List(
            validateCoordinatorHead(prior).void,
            check(next(prior.revision.value.value, after.revision.value.value), "head.revision", "must advance by exactly one"),
            validateModeTransition(prior, after, audit, recoveryRecord),
            validateMutation(prior, after, audit.mutation)
          )
        )
    }

    validateValue(after, common :+ transitionChecks)
  }

  def validateOutbox(head: CoordinatorHead, outbox: FinalityEffectOutboxHead): ValidationResult[FinalityEffectOutboxHead] = {
    val cursor = outbox.cursor
    val paired = cursor.completedThrough.isDefined == cursor.completedManifest.isDefined
    val withinHigh = (cursor.completedThrough, head.effects.highGeneration) match {
      case (None, _)                    => true
      case (Some(_), None)              => false
      case (Some(completed), Some(high)) => completed.value.value <= high.value.value
    }
    val exactTailAtHigh = (cursor.completedThrough, cursor.completedManifest, head.effects.highGeneration, head.effects.tail) match {
      case (Some(completed), Some(manifest), Some(high), Some(tail)) if completed == high => manifest == tail
      case _                                                                               => true
    }

    validateValue(
      outbox,
      List(
        validateEffectsIndex(head.effects, "head.effects"),
        check(paired, "outbox.cursor", "completedThrough and completedManifest must both be present or both be absent"),
        cursor.completedManifest.fold(validUnit)(pointer => validateEffectManifestPointer(pointer, "outbox.cursor.completedManifest")),
        check(cursor.completedManifest.forall(pointer => cursor.completedThrough.contains(pointer.generation)), "outbox.cursor.completedManifest", "generation must equal completedThrough"),
        check(withinHigh, "outbox.cursor.completedThrough", "cannot exceed the coordinator effect high generation or exist without effects"),
        check(exactTailAtHigh, "outbox.cursor.completedManifest", "must equal the coordinator tail when completed through the high generation")
      )
    )
  }

  def validateOutboxTransition(
    before: FinalityEffectOutboxHead,
    after: FinalityEffectOutboxHead,
    coordinator: CoordinatorHead,
    completions: List[CompletedEffectManifest]
  ): ValidationResult[FinalityEffectOutboxHead] = {
    val nondecreasing = (before.cursor.completedThrough, after.cursor.completedThrough) match {
      case (None, _)                    => true
      case (Some(_), None)              => false
      case (Some(old), Some(current)) => old.value.value <= current.value.value
    }
    val sameGenerationSameManifest =
      if (before.cursor.completedThrough == after.cursor.completedThrough)
        before.cursor.completedManifest == after.cursor.completedManifest
      else true

    val cursorAdvanced = before.cursor != after.cursor
    val completionChecks = validateEffectCompletions(before.cursor, after.cursor, completions)

    validateValue(
      after,
      List(
        validateOutbox(coordinator, before).void,
        validateOutbox(coordinator, after).void,
        check(next(before.revision.value.value, after.revision.value.value), "outbox.revision", "must advance by exactly one"),
        check(nondecreasing, "outbox.cursor.completedThrough", "cannot regress"),
        check(sameGenerationSameManifest, "outbox.cursor.completedManifest", "cannot replace the manifest at an already completed generation"),
        check(cursorAdvanced, "outbox.cursor", "an outbox CAS must advance the completed cursor; no-op revisions are forbidden"),
        check(completions.nonEmpty, "outbox.completions", "cursor advancement requires exact completion evidence"),
        completionChecks
      )
    )
  }

  /** Validate a terminal receipt after the sink-specific operation and readback have completed. */
  def validateEffectReceipt(
    command: ScopedEffectCommand,
    manifest: EffectManifestPointer,
    receipt: TerminalEffectReceipt
  ): ValidationResult[TerminalEffectReceipt] = {
    val common = List(
      validateEffectCommand(command, "effectReceipt.command"),
      validateEffectManifestPointer(manifest, "effectReceipt.manifest"),
      check(receipt.manifest == manifest, "effectReceipt.manifest", "must equal the command manifest"),
      check(receipt.effectId == command.effectId, "effectReceipt.effectId", "must equal the command id"),
      validateHash(receipt.effectId.value, "effectReceipt.effectId", rejectZero = true),
      validateHash(receipt.observedState.value, "effectReceipt.observedState", rejectZero = true)
    )

    val specific = receipt match {
      case applied: AppliedEffectReceipt =>
        combine(
          List(
            check(
              command.expectedBefore != command.desiredAfter,
              "effectReceipt",
              "an unchanged command has no mutation to apply and requires an independently read-back AlreadyApplied receipt"
            ),
            validateHash(applied.observedBefore.value, "effectReceipt.observedBefore", rejectZero = true),
            check(applied.observedBefore == command.expectedBefore, "effectReceipt.observedBefore", "must equal expectedBefore"),
            check(applied.observedBeforeRevision == command.expectedRevision, "effectReceipt.observedBeforeRevision", "must equal expectedRevision"),
            check(applied.observedState == command.desiredAfter, "effectReceipt.observedState", "must equal desiredAfter"),
            check(applied.sinkRevision == command.desiredRevision, "effectReceipt.sinkRevision", "must equal desiredRevision")
          )
        )
      case already: AlreadyAppliedEffectReceipt =>
        combine(
          List(
            check(already.observedState == command.desiredAfter, "effectReceipt.observedState", "must independently read back desiredAfter"),
            check(already.sinkRevision == command.desiredRevision, "effectReceipt.sinkRevision", "must independently read back desiredRevision")
          )
        )
    }

    validateValue(receipt, common :+ specific)
  }

  private def validateEffectCompletions(
    before: EffectOutboxCursor,
    after: EffectOutboxCursor,
    completions: List[CompletedEffectManifest]
  ): ValidationResult[Unit] = {
    val beforeGeneration = before.completedThrough.fold(BigInt(-1))(value => BigInt(value.value.value))
    val afterGeneration = after.completedThrough.fold(BigInt(-1))(value => BigInt(value.value.value))
    val expectedCount = afterGeneration - beforeGeneration

    val perManifest = completions.zipWithIndex.flatMap {
      case (completed, index) =>
        val expectedGeneration = beforeGeneration + BigInt(index) + 1
        val expectedPrevious = if (index == 0) before.completedManifest else completions.lift(index - 1).map(_.pointer)
        val commands = effectCommands(completed.manifest).map(_._3)
        val receiptIds = completed.receipts.map(_.effectId)
        val commandIds = commands.map(_.effectId)
        val receiptChecks = commands.zipWithIndex.map {
          case (command, commandIndex) =>
            completed.receipts.find(_.effectId == command.effectId) match {
              case Some(receipt) => validateEffectReceipt(command, completed.pointer, receipt).void
              case None          => invalid(s"outbox.completions[$index].receipts[$commandIndex]", "every one of the 18 commands requires an exact terminal receipt")
            }
        }

        List(
          validateCompletedManifestShape(completed.manifest, s"outbox.completions[$index].manifest"),
          validateEffectManifestPointer(completed.pointer, s"outbox.completions[$index].pointer"),
          checkDerived(
            FinalityIdentity.effectManifestPointer(completed.manifest),
            completed.pointer,
            s"outbox.completions[$index].pointer",
            "must equal the canonical pointer derived from the exact manifest"
          ),
          check(BigInt(completed.pointer.generation.value.value) == expectedGeneration, s"outbox.completions[$index].pointer.generation", "must be the next contiguous release generation"),
          check(completed.manifest.scope.generation == completed.pointer.generation, s"outbox.completions[$index].manifest.scope.generation", "must equal the manifest pointer generation"),
          check(completed.manifest.previous == expectedPrevious, s"outbox.completions[$index].manifest.previous", "must continue the exact immutable manifest chain"),
          check(completed.receipts.size == commands.size, s"outbox.completions[$index].receipts", "must contain exactly 18 receipts"),
          check(receiptIds.distinct.size == receiptIds.size, s"outbox.completions[$index].receipts", "receipt effect ids must be unique"),
          check(receiptIds.toSet == commandIds.toSet, s"outbox.completions[$index].receipts", "receipt ids must equal the complete 18-command id set")
        ) ++ receiptChecks
    }

    combine(
      List(
        check(expectedCount >= 0, "outbox.completions", "completed generation cannot regress"),
        check(BigInt(completions.size) == expectedCount, "outbox.completions", "must prove every contiguous generation crossed by the cursor"),
        check(expectedCount == 0 || completions.lastOption.map(_.pointer) == after.completedManifest, "outbox.cursor.completedManifest", "must equal the final completely receipted manifest")
      ) ++ perManifest
    )
  }

  private def validateCompletedManifestShape(manifest: FinalityEffectManifest, path: String): ValidationResult[Unit] = {
    val commands = effectCommands(manifest)
    val commandChecks = commands.map {
      case (name, kind, command) =>
        combine(
          List(
            validateEffectCommand(command, s"$path.$name"),
            check(command.scope == manifest.scope, s"$path.$name.scope", "must equal the manifest scope"),
            check(command.kind == kind, s"$path.$name.kind", s"must be $kind"),
            validateScoped(command.payload, manifest.scope.intent, FinalityArtifactKind.EffectPayload, s"$path.$name.payload")
          )
        )
    }

    combine(
      List(
        validateHash(manifest.scope.intent.value, s"$path.scope.intent", rejectZero = true),
        manifest.scope.priorState.fold(validUnit)(validateStateRef(_, s"$path.scope.priorState")),
        validateHash(manifest.scope.domain.networkId, s"$path.scope.domain.networkId", rejectZero = true),
        validateHash(manifest.scope.domain.genesisHash, s"$path.scope.domain.genesisHash", rejectZero = true),
        validateHash(manifest.scope.domain.protocolEra, s"$path.scope.domain.protocolEra", rejectZero = true),
        validateHash(manifest.scope.transitionDigest, s"$path.scope.transitionDigest", rejectZero = true),
        validateStateRef(manifest.scope.target, s"$path.scope.target"),
        manifest.previous.fold(validUnit)(validateEffectManifestPointer(_, s"$path.previous")),
        check(commands.map(_._3.effectId).distinct.size == commands.size, s"$path.effectIds", "all 18 canonically derived effect ids must be unique")
      ) ++ commandChecks
    )
  }

  private def validateSelection(selection: CanonicalSelectionToken, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateStateRef(selection.selectedTip, s"$path.selectedTip"),
        validateStateRef(selection.operationalTarget, s"$path.operationalTarget"),
        validateArtifact(selection.decision, FinalityArtifactKind.CanonicalSelectionEvidence, s"$path.decision"),
        validatePathCommitment(selection.lineage, CanonicalLineage, s"$path.lineage"),
        check(selection.lineage.summary.oldest == selection.operationalTarget, s"$path.lineage.oldest", "must equal operationalTarget"),
        check(selection.lineage.summary.newest == selection.selectedTip, s"$path.lineage.newest", "must equal selectedTip")
      )
    )

  private def validateTransition(
    intentId: IntentId,
    target: GlobalSnapshotStateRef,
    transition: CoreTransition,
    prior: Option[ReleasedCore]
  ): ValidationResult[Unit] =
    transition match {
      case Advance(adopted) =>
        combine(
          List(
            validatePathManifest(adopted, intentId, Adopted, "batch.transition.adopted"),
            check(adopted.summary.newest == target, "batch.transition.adopted.newest", "must equal the release target"),
            prior.fold(validUnit)(released => validateSuffixAfter(released.pointer.target, adopted.summary, "batch.transition.adopted"))
          )
        )
      case DensityReplacement(commonAncestor, orphaned, adopted, densityDecision) =>
        combine(
          List(
            requirePrior(prior, "batch.transition", "density replacement requires an exact prior release"),
            validateStateRef(commonAncestor, "batch.transition.commonAncestor"),
            validatePathManifest(orphaned, intentId, Orphaned, "batch.transition.orphaned"),
            validatePathManifest(adopted, intentId, Adopted, "batch.transition.adopted"),
            validateScoped(densityDecision, intentId, FinalityArtifactKind.DensityDecisionEvidence, "batch.transition.densityDecision"),
            validateSuffixAfter(commonAncestor, orphaned.summary, "batch.transition.orphaned"),
            validateSuffixAfter(commonAncestor, adopted.summary, "batch.transition.adopted"),
            check(prior.exists(_.pointer.target == orphaned.summary.newest), "batch.transition.orphaned.newest", "must equal the prior released target"),
            check(adopted.summary.newest == target, "batch.transition.adopted.newest", "must equal the replacement target")
          )
        )
      case DensityRollbackToOperationalMrca(operationalMrca, orphaned, densityDecision) =>
        combine(
          List(
            requirePrior(prior, "batch.transition", "density rollback requires an exact prior release"),
            validateStateRef(operationalMrca, "batch.transition.operationalMrca"),
            validatePathManifest(orphaned, intentId, Orphaned, "batch.transition.orphaned"),
            validateScoped(densityDecision, intentId, FinalityArtifactKind.DensityDecisionEvidence, "batch.transition.densityDecision"),
            validateSuffixAfter(operationalMrca, orphaned.summary, "batch.transition.orphaned"),
            check(operationalMrca == target, "batch.transition.operationalMrca", "must equal the rollback release target"),
            check(prior.exists(_.pointer.target == orphaned.summary.newest), "batch.transition.orphaned.newest", "must equal the prior released target")
          )
        )
    }

  private def validateQualification(
    intentId: IntentId,
    target: GlobalSnapshotStateRef,
    qualification: OperationalQualification,
    transition: CoreTransition,
    prior: Option[ReleasedCore]
  ): ValidationResult[Unit] = {
    val evidenceKind = qualification match {
      case _: DecidedAttestationTWeight => FinalityArtifactKind.DecidedAttestationEvidence
      case _: CanonicalDepthK1           => FinalityArtifactKind.DepthK1Evidence
    }
    val closureChecks = qualification.ancestorClosure match {
      case None =>
        check(qualification.operationalTarget == qualification.qualifyingDescendant, "batch.qualification.qualifyingDescendant", "direct qualification requires target == qualifying descendant")
      case Some(closure) =>
        combine(
          List(
            validatePathManifest(closure, intentId, OperationalAncestorClosure, "batch.qualification.ancestorClosure"),
            check(closure.summary.oldest == qualification.operationalTarget, "batch.qualification.ancestorClosure.oldest", "must equal the operational target"),
            check(closure.summary.newest == qualification.qualifyingDescendant, "batch.qualification.ancestorClosure.newest", "must equal the qualifying descendant"),
            check(qualification.operationalTarget != qualification.qualifyingDescendant, "batch.qualification.ancestorClosure", "must not redundantly encode a direct qualification")
          )
        )
    }
    val inheritedRollback = transition match {
      case _: DensityRollbackToOperationalMrca if qualification.ancestorClosure.nonEmpty =>
        prior match {
          case None => invalid("batch.qualification", "inherited rollback qualification requires the prior released qualification")
          case Some(released) =>
            val previous = released.payload.qualification
            combine(
              List(
                check(previous.scope.rail == qualification.scope.rail, "batch.qualification.rail", "inherited rollback must retain the original T_weight or k1 rail"),
                check(previous.qualifyingDescendant == qualification.qualifyingDescendant, "batch.qualification.qualifyingDescendant", "inherited rollback must retain the original qualifying descendant"),
                check(previous.evidence.artifact == qualification.evidence.artifact, "batch.qualification.evidence", "inherited rollback must retain the original rail evidence")
              )
            )
        }
      case _ => validUnit
    }

    combine(
      List(
        check(qualification.operationalTarget == target, "batch.qualification.operationalTarget", "must equal the release target"),
        validateScoped(qualification.evidence, intentId, evidenceKind, "batch.qualification.evidence"),
        closureChecks,
        inheritedRollback
      )
    )
  }

  private def validatePrepared(
    intentId: IntentId,
    prepared: PreparedCoreTarget,
    prior: Option[ReleasedCore]
  ): ValidationResult[Unit] = {
    val expectedPriorPublication = prior.map(_.payload.receipt.activePublication)

    combine(
      List(
        validateStateRef(prepared.target, "batch.prepared.target"),
        validateImageReceipt(prepared.preparedImage, prepared.target, "batch.prepared.preparedImage"),
        validatePublication(prepared.expectedBefore, "batch.prepared.expectedBefore"),
        validatePublication(prepared.targetPublication, "batch.prepared.targetPublication"),
        check(next(prepared.expectedBefore.revision.value, prepared.targetPublication.revision.value), "batch.prepared.targetPublication.revision", "must be expectedBefore revision + 1"),
        check(prepared.targetPublication.image.contains(prepared.preparedImage), "batch.prepared.targetPublication.image", "must select the exact prepared image"),
        check(expectedPriorPublication.forall(_ == prepared.expectedBefore), "batch.prepared.expectedBefore", "must equal the prior released active publication"),
        validateScoped(prepared.semanticState, intentId, FinalityArtifactKind.PreparedSemanticState, "batch.prepared.semanticState"),
        validateScoped(
          prepared.authenticatedAnchor,
          intentId,
          FinalityArtifactKind.AuthenticatedTargetAnchor,
          "batch.prepared.authenticatedAnchor"
        )
      )
    )
  }

  private def validateEffectManifest(
    batch: FinalityCoreBatch,
    manifest: FinalityEffectManifest,
    context: CoreValidationContext
  ): ValidationResult[Unit] = {
    val commands = effectCommands(manifest)
    val previous = context.previousEffects
    val previousCommands = previous.map(value => effectCommands(value.manifest).map { case (_, _, command) => command })
    val laneChecks = commands.zipWithIndex.flatMap {
      case ((name, kind, command), index) =>
        val priorCommand = previousCommands.flatMap(_.lift(index))
        List(
          validateEffectCommand(command, s"effectManifest.$name"),
          check(command.scope == manifest.scope, s"effectManifest.$name.scope", "must equal the manifest scope"),
          check(command.kind == kind, s"effectManifest.$name.kind", s"must be $kind"),
          validateScoped(command.payload, batch.intentId, FinalityArtifactKind.EffectPayload, s"effectManifest.$name.payload"),
          check(command.predecessor == priorCommand.map(_.effectId), s"effectManifest.$name.predecessor", "must name the immediately preceding command in this sink lane"),
          priorCommand.fold(validUnit) { prior =>
            combine(
              List(
                check(command.expectedBefore == prior.desiredAfter, s"effectManifest.$name.expectedBefore", "must equal the preceding command desired state"),
                check(command.expectedRevision == prior.desiredRevision, s"effectManifest.$name.expectedRevision", "must equal the preceding command desired revision"),
                check(command.effectId != prior.effectId, s"effectManifest.$name.effectId", "canonical identity must not equal its predecessor")
              )
            )
          }
        )
    }
    val priorPointer = context.priorReleased.map(_.payload.effectManifest)
    val priorManifestChecks = (context.priorReleased, previous) match {
      case (None, None) => validUnit
      case (Some(released), Some(resolved)) =>
        combine(
          List(
            check(resolved.pointer == released.payload.effectManifest, "effectManifest.previous", "resolved prior manifest must equal the prior released pointer"),
            checkVerified(
              FinalityIdentity.verifyEffectManifestPointer(resolved.pointer, resolved.manifest),
              "effectManifest.previous",
              "resolved prior manifest pointer must be canonically derived from its exact payload"
            ),
            validateCompletedManifestShape(resolved.manifest, "effectManifest.previous"),
            check(resolved.manifest.scope.domain == batch.scope.domain, "effectManifest.previous.scope.domain", "must equal the current finality domain"),
            check(resolved.manifest.scope.intent == released.pointer.intentId, "effectManifest.previous.scope.intent", "must equal the authenticated prior release intent"),
            check(resolved.manifest.scope.target == released.pointer.target, "effectManifest.previous.scope.target", "must equal the authenticated prior release target"),
            check(resolved.manifest.scope.generation == released.pointer.generation, "effectManifest.previous.scope.generation", "must equal the authenticated prior release generation"),
            check(resolved.manifest.scope.generation == resolved.pointer.generation, "effectManifest.previous.generation", "resolved prior manifest generation must equal its pointer")
          )
        )
      case _ => invalid("effectManifest.previous", "prior release and resolved previous manifest must either both exist or both be absent")
    }

    combine(
      List(
        validateEffectManifestPointer(batch.effectManifest, "batch.effectManifest"),
        checkDerived(
          FinalityIdentity.effectManifestPointer(manifest),
          batch.effectManifest,
          "batch.effectManifest",
          "must equal the canonical pointer derived from the exact 18-command manifest"
        ),
        check(batch.effectManifest.generation == batch.scope.generation, "batch.effectManifest.generation", "must equal the release generation"),
        check(manifest.scope.intent == batch.intentId, "effectManifest.scope.intent", "must equal the batch intent"),
        check(manifest.scope.domain == batch.scope.domain, "effectManifest.scope.domain", "must equal the batch finality domain"),
        check(manifest.scope.generation == batch.scope.generation, "effectManifest.scope.generation", "must equal the release generation"),
        check(manifest.scope.priorState == context.priorReleased.map(_.pointer.target), "effectManifest.scope.priorState", "must equal the exact prior released target"),
        checkDerived(
          FinalityIdentity.transitionDigest(batch.transition.shape),
          manifest.scope.transitionDigest,
          "effectManifest.scope.transitionDigest",
          "must equal transitionDigest(batch.transition.shape)"
        ),
        check(manifest.scope.target == batch.scope.target, "effectManifest.scope.target", "must equal the release target"),
        check(manifest.previous == priorPointer, "effectManifest.previous", "must equal the prior released effect manifest"),
        check(manifest.commitment == batch.scope.effects, "effectManifest", "must equal the exact plan committed by IntentScope"),
        check(commands.map(_._3.effectId).distinct.size == commands.size, "effectManifest.effectIds", "all 18 canonically derived effect ids must be unique"),
        priorManifestChecks
      ) ++ laneChecks
    )
  }

  private def validateEffectCommand(command: ScopedEffectCommand, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(command.effectId.value, s"$path.effectId", rejectZero = true),
        checkDerived(
          FinalityIdentity.effectId(command.identityPreimage),
          command.effectId,
          s"$path.effectId",
          "must equal the canonical domain-separated identity of the complete command preimage"
        ),
        validateHash(command.expectedBefore.value, s"$path.expectedBefore", rejectZero = true),
        validateHash(command.desiredAfter.value, s"$path.desiredAfter", rejectZero = true),
        check(
          if (command.expectedBefore == command.desiredAfter)
            command.desiredRevision == command.expectedRevision
          else
            BigInt(command.desiredRevision.value.value) == BigInt(command.expectedRevision.value.value) + 1,
          s"$path.desiredRevision",
          "unchanged state must retain the revision; changed state must advance it by exactly one without overflow"
        ),
        command.predecessor.fold(validUnit)(value => validateHash(value.value, s"$path.predecessor", rejectZero = true))
      )
    )

  private def validateActiveIntent(active: ActiveCoreIntent): ValidationResult[Unit] = {
    val scope = active.scope
    val common = List(
      validateArtifact(active.batch.artifact, FinalityArtifactKind.CoreBatch, "head.active.batch.artifact"),
      validateHash(active.batch.intentId.value, "head.active.batch.intentId", rejectZero = true),
      checkDerived(FinalityIdentity.intentId(scope), active.batch.intentId, "head.active.batch.intentId", "must equal IntentId(active.scope)"),
      check(active.batch.generation == scope.generation, "head.active.batch.generation", "must equal scope.generation"),
      check(active.batch.attempt == scope.attempt, "head.active.batch.attempt", "must equal scope.attempt")
    )

    val stage = active.stage match {
      case CoreStage.Prepared => validUnit
      case CoreStage.CoreApplied(receipt) =>
        validateStageReceipt(active.batch.intentId, scope, receipt, "head.active.stage.receipt")
      case CoreStage.RestoringPrior(cause) => validateOrphanCause(active.batch.intentId, scope.selection, cause)
      case CoreStage.RestoredAbandoned(cause, receipt) =>
        combine(
          List(
            validateOrphanCause(active.batch.intentId, scope.selection, cause),
            check(receipt.intentId == active.batch.intentId, "head.active.stage.restoration.intentId", "must equal the active intent"),
            check(
              receipt.beforeRestoration == scope.prepared.targetPublication || receipt.beforeRestoration == scope.prepared.expectedBefore,
              "head.active.stage.restoration.beforeRestoration",
              "must read either the applied target or the already-restored expected prior after crash uncertainty"
            ),
            check(receipt.restoredPublication == scope.prepared.expectedBefore, "head.active.stage.restoration.restoredPublication", "must equal the exact prior publication"),
            validateScoped(receipt.semanticReceipt, active.batch.intentId, FinalityArtifactKind.PriorSemanticStateReceipt, "head.active.stage.restoration.semanticReceipt"),
            validateScoped(receipt.authenticatedAnchorReceipt, active.batch.intentId, FinalityArtifactKind.PriorAnchorReceipt, "head.active.stage.restoration.authenticatedAnchorReceipt")
          )
        )
    }

    combine(common :+ stage)
  }

  private def validateStageReceipt(
    intentId: IntentId,
    scope: IntentScope,
    receipt: ReleasedCoreReceipt,
    path: String
  ): ValidationResult[Unit] =
    combine(
      List(
        check(receipt.intentId == intentId, s"$path.intentId", "must equal the active intent"),
        check(receipt.generation == scope.generation, s"$path.generation", "must equal scope.generation"),
        check(receipt.target == scope.target, s"$path.target", "must equal scope.target"),
        check(receipt.beforePublication == scope.prepared.expectedBefore, s"$path.beforePublication", "must equal the prepared CAS prior"),
        check(receipt.activePublication == scope.prepared.targetPublication, s"$path.activePublication", "must equal the prepared publication target"),
        validateScoped(receipt.semanticReceipt, intentId, FinalityArtifactKind.AppliedSemanticStateReceipt, s"$path.semanticReceipt"),
        validateScoped(receipt.authenticatedAnchorReceipt, intentId, FinalityArtifactKind.AuthenticatedAnchorReceipt, s"$path.authenticatedAnchorReceipt")
      )
    )

  private def validateOrphanCause(
    intentId: IntentId,
    selection: CanonicalSelectionToken,
    cause: ObjectiveOrphanCause
  ): ValidationResult[Unit] =
    combine(
      List(
        check(cause.intentId == intentId, "head.active.stage.cause.intentId", "must equal the active intent"),
        check(cause.supersededSelection == selection, "head.active.stage.cause.supersededSelection", "must equal the intent's captured selection"),
        validateSelection(cause.supersededSelection, "head.active.stage.cause.supersededSelection"),
        validateSelection(cause.replacementSelection, "head.active.stage.cause.replacementSelection"),
        validateStateRef(cause.commonAncestor, "head.active.stage.cause.commonAncestor"),
        validateScopedOneOf(
          cause.evidence,
          intentId,
          Set(FinalityArtifactKind.CanonicalSelectionEvidence, FinalityArtifactKind.DensityDecisionEvidence),
          "head.active.stage.cause.evidence"
        )
      )
    )

  private def validateInitialization(after: CoordinatorHead, audit: CoordinatorAuditRecord): ValidationResult[Unit] =
    combine(
      List(
        check(audit.mutation == CoordinatorMutationKind.Initialized, "audit.mutation", "must be Initialized when no prior head exists"),
        check(after.revision.value.value == 0L, "head.revision", "initial revision must be zero"),
        check(after.lastAttempt.isEmpty, "head.lastAttempt", "must be empty initially"),
        check(after.mode == Running, "head.mode", "must start Running"),
        check(after.released.isEmpty, "head.released", "must be empty initially"),
        check(after.active.isEmpty, "head.active", "must be empty initially"),
        check(after.effects == EffectsIndex(None, None), "head.effects", "must be empty initially"),
        check(audit.priorAudit.isEmpty, "audit.priorAudit", "must be empty initially")
      )
    )

  private def validateModeTransition(
    before: CoordinatorHead,
    after: CoordinatorHead,
    audit: CoordinatorAuditRecord,
    recoveryRecord: Option[RecoveryRecord]
  ): ValidationResult[Unit] =
    before.mode match {
      case Running =>
        after.mode match {
          case Running =>
            combine(
              List(
                check(audit.mutation != CoordinatorMutationKind.RecoveryEntered, "audit.mutation", "must describe an ordinary running transition"),
                check(recoveryRecord.isEmpty, "recoveryRecord", "an ordinary running transition cannot append a recovery record")
              )
            )
          case required: RecoveryRequired =>
            combine(
              List(
                check(audit.mutation == CoordinatorMutationKind.RecoveryEntered, "audit.mutation", "entering RecoveryRequired must use RecoveryEntered"),
                check(sameCoreState(before, after), "head.mode", "entering recovery must freeze lastAttempt, released, active, and effects"),
                recoveryRecord.fold[ValidationResult[Unit]](invalid("recoveryRecord", "entering RecoveryRequired requires the exact immutable recovery record"))(
                  validateRecoveryRecord(before, after, required, _)
                )
              )
            )
        }
      case _: RecoveryRequired =>
        invalid(
          "head.mode",
          "RecoveryRequired is absorbing: exit requires a future specialized authenticated reconstruction verifier, never this ordinary transition API"
        )
    }

  private def validateRecoveryRecord(
    before: CoordinatorHead,
    after: CoordinatorHead,
    required: RecoveryRequired,
    record: RecoveryRecord
  ): ValidationResult[Unit] =
    combine(
      List(
        checkDerived(
          FinalityIdentity.recoveryPointer(record),
          required.record,
          "head.mode.record",
          "must equal the canonical pointer derived from the exact recovery record"
        ),
        check(record.enteredAt == after.revision, "recoveryRecord.enteredAt", "must equal the RecoveryRequired head revision"),
        check(record.lastAttempt == before.lastAttempt, "recoveryRecord.lastAttempt", "must preserve the frozen prior head value"),
        check(record.released == before.released, "recoveryRecord.released", "must preserve the frozen prior head value"),
        check(record.active == before.active, "recoveryRecord.active", "must preserve the frozen prior head value"),
        check(record.effects == before.effects, "recoveryRecord.effects", "must preserve the frozen prior head value"),
        check(record.priorAudit == before.auditTail, "recoveryRecord.priorAudit", "must equal the prior head audit tail"),
        validateRecoveryReason(record.reason)
      )
    )

  private def validateRecoveryReason(reason: RecoveryReason): ValidationResult[Unit] =
    reason match {
      case RecoveryReason.UnknownCanonicality(target, missingHash) =>
        combine(
          List(
            validateStateRef(target, "recoveryRecord.reason.target"),
            validateHash(missingHash, "recoveryRecord.reason.missingHash", rejectZero = true)
          )
        )
      case RecoveryReason.MissingCoreBatch(batch) => validateCoreBatchPointer(batch, "recoveryRecord.reason.batch")
      case RecoveryReason.CorruptCoreBatch(batch, observedDigest) =>
        combine(
          List(
            validateCoreBatchPointer(batch, "recoveryRecord.reason.batch"),
            validateHash(observedDigest, "recoveryRecord.reason.observedDigest", rejectZero = true),
            check(observedDigest != batch.artifact.digest.value, "recoveryRecord.reason.observedDigest", "corrupt evidence must differ from the committed digest")
          )
        )
      case RecoveryReason.CoreConflict(expected, actual) =>
        combine(
          List(
            check(expected != actual, "recoveryRecord.reason", "a core conflict must name unequal expected and actual states"),
            expected.fold(validUnit)(validateReleasedPointer(_, "recoveryRecord.reason.expected")),
            actual.fold(validUnit)(validateReleasedPointer(_, "recoveryRecord.reason.actual"))
          )
        )
      case RecoveryReason.UnknownCore(reasonDigest) =>
        validateHash(reasonDigest, "recoveryRecord.reason.reasonDigest", rejectZero = true)
      case RecoveryReason.MissingEffectManifest(manifest) =>
        validateEffectManifestPointer(manifest, "recoveryRecord.reason.manifest")
      case RecoveryReason.CorruptEffectManifest(manifest, observedDigest) =>
        combine(
          List(
            validateEffectManifestPointer(manifest, "recoveryRecord.reason.manifest"),
            validateHash(observedDigest, "recoveryRecord.reason.observedDigest", rejectZero = true),
            check(observedDigest != manifest.digest.value, "recoveryRecord.reason.observedDigest", "corrupt evidence must differ from the committed digest")
          )
        )
      case RecoveryReason.EffectConflict(_, expected, actual) =>
        combine(
          List(
            validateHash(expected.value, "recoveryRecord.reason.expected", rejectZero = true),
            validateHash(actual.value, "recoveryRecord.reason.actual", rejectZero = true),
            check(expected != actual, "recoveryRecord.reason", "an effect conflict must name unequal expected and actual states")
          )
        )
      case RecoveryReason.UnknownEffect(_, reasonDigest) =>
        validateHash(reasonDigest, "recoveryRecord.reason.reasonDigest", rejectZero = true)
      case RecoveryReason.JournalCorruption(observedDigest) =>
        validateHash(observedDigest, "recoveryRecord.reason.observedDigest", rejectZero = true)
      case RecoveryReason.StartupDependencyFailure(reasonDigest) =>
        validateHash(reasonDigest, "recoveryRecord.reason.reasonDigest", rejectZero = true)
    }

  private def validateCoreBatchPointer(pointer: FinalityCoreBatchPointer, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(pointer.intentId.value, s"$path.intentId", rejectZero = true),
        validateArtifact(pointer.artifact, FinalityArtifactKind.CoreBatch, s"$path.artifact")
      )
    )

  private def validateReleasedPointer(pointer: ReleasedCorePointer, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(pointer.intentId.value, s"$path.intentId", rejectZero = true),
        validateStateRef(pointer.target, s"$path.target"),
        validateArtifact(pointer.record, FinalityArtifactKind.ReleasedCoreRecord, s"$path.record")
      )
    )

  private def validateMutation(before: CoordinatorHead, after: CoordinatorHead, mutation: CoordinatorMutationKind): ValidationResult[Unit] =
    mutation match {
      case CoordinatorMutationKind.Prepared =>
        (before.active, after.active) match {
          case (None, Some(active)) =>
            combine(
              List(
                check(active.stage == CoreStage.Prepared, "head.active.stage", "a newly prepared intent must start at Prepared"),
                check(after.released == before.released, "head.released", "preparation cannot change released core"),
                check(after.effects == before.effects, "head.effects", "preparation cannot change effects index"),
                check(after.lastAttempt.contains(active.scope.attempt), "head.lastAttempt", "must equal the new attempt"),
                check(before.lastAttempt.forall(old => old.value.value < active.scope.attempt.value.value), "head.lastAttempt", "attempts must increase monotonically")
              )
            )
          case _ => invalid("head.active", "Prepared must install exactly one new active intent")
        }
      case CoordinatorMutationKind.CoreApplied | CoordinatorMutationKind.RestorationStarted |
          CoordinatorMutationKind.RestoredAbandoned =>
        (before.active, after.active) match {
          case (Some(old), Some(current)) =>
            val expectedKind = (old.stage, current.stage) match {
              case (_, _: CoreStage.CoreApplied)       => CoordinatorMutationKind.CoreApplied
              case (_, _: CoreStage.RestoringPrior)    => CoordinatorMutationKind.RestorationStarted
              case (_, _: CoreStage.RestoredAbandoned) => CoordinatorMutationKind.RestoredAbandoned
              case _                                             => mutation
            }
            combine(
              List(
                check(old.batch == current.batch && old.scope == current.scope, "head.active", "stage advancement cannot replace the active intent"),
                check(CoreStage.canAdvance(old.stage, current.stage), "head.active.stage", "must follow the legal core-stage graph"),
                check(mutation == expectedKind, "audit.mutation", "must name the exact stage transition"),
                check(after.released == before.released, "head.released", "stage advancement cannot release state"),
                check(after.effects == before.effects, "head.effects", "stage advancement cannot enqueue effects"),
                check(after.lastAttempt == before.lastAttempt, "head.lastAttempt", "stage advancement cannot change attempt")
              )
            )
          case _ => invalid("head.active", "stage advancement requires the same active intent before and after")
        }
      case CoordinatorMutationKind.AbandonedRetired =>
        combine(
          List(
            check(before.active.exists(_.stage.isInstanceOf[CoreStage.RestoredAbandoned]), "head.active", "only RestoredAbandoned may be retired"),
            check(after.active.isEmpty, "head.active", "retirement must clear the abandoned intent"),
            check(after.released == before.released, "head.released", "retirement cannot change released state"),
            check(after.effects == before.effects, "head.effects", "retirement cannot change effects"),
            check(after.lastAttempt == before.lastAttempt, "head.lastAttempt", "retirement cannot change attempt")
          )
        )
      case CoordinatorMutationKind.Released => validateReleaseMutation(before, after)
      case CoordinatorMutationKind.RecoveryEntered => validUnit
      case CoordinatorMutationKind.Initialized => invalid("audit.mutation", "Initialized is valid only without a prior head")
    }

  private def validateReleaseMutation(before: CoordinatorHead, after: CoordinatorHead): ValidationResult[Unit] =
    (before.active, after.released) match {
      case (Some(active), Some(released)) =>
        val stageReceipt = active.stage match {
          case CoreStage.CoreApplied(receipt) => Some(receipt)
          case _                    => None
        }
        combine(
          List(
            check(stageReceipt.nonEmpty, "head.active.stage", "release is legal only from CoreApplied"),
            check(after.active.isEmpty, "head.active", "release must clear the active intent"),
            check(released.pointer.intentId == active.batch.intentId, "head.released.pointer.intentId", "must equal the active intent"),
            check(released.pointer.generation == active.scope.generation, "head.released.pointer.generation", "must equal the active generation"),
            check(released.pointer.target == active.scope.target, "head.released.pointer.target", "must equal the active target"),
            validateScoped(released.record, active.batch.intentId, FinalityArtifactKind.ReleasedCoreRecord, "head.released.record"),
            check(released.record.artifact == released.pointer.record, "head.released.record", "must wrap the released record pointer"),
            check(released.payload.qualification.scope == active.scope.qualification, "head.released.payload.qualification", "must equal the active qualification commitment"),
            check(released.payload.qualification.operationalTarget == active.scope.target, "head.released.payload.qualification.operationalTarget", "must equal the active target"),
            check(stageReceipt.contains(released.payload.receipt), "head.released.payload.receipt", "must equal the CoreApplied receipt"),
            check(
              released.payload.effectManifest.generation == active.scope.generation,
              "head.released.payload.effectManifest.generation",
              "must equal the active generation"
            ),
            check(after.effects == EffectsIndex(released.payload.effectManifest.some, active.scope.generation.some), "head.effects", "must atomically enqueue the exact released effect manifest"),
            check(after.lastAttempt == before.lastAttempt, "head.lastAttempt", "release cannot change attempt"),
            check(active.scope.expectedPrior == before.released.map(_.pointer), "head.active.scope.expectedPrior", "must CAS the exact prior release")
          )
        )
      case _ => invalid("head.released", "Released must replace CoreApplied with one exact released core")
    }

  private def validateEffectsIndex(index: EffectsIndex, path: String): ValidationResult[Unit] =
    combine(
      List(
        check(index.tail.isDefined == index.highGeneration.isDefined, path, "tail and highGeneration must both be present or both be absent"),
        check(index.tail.forall(tail => index.highGeneration.contains(tail.generation)), s"$path.tail", "tail generation must equal highGeneration"),
        index.tail.fold(validUnit)(pointer => validateEffectManifestPointer(pointer, s"$path.tail"))
      )
    )

  private def validatePathManifest(
    ref: PathManifestRef,
    intentId: IntentId,
    role: PathRole,
    path: String
  ): ValidationResult[Unit] =
    combine(
      List(
        validatePathCommitment(ref.commitment, role, s"$path.commitment"),
        validateScoped(ref.manifest, intentId, FinalityArtifactKind.PathManifest, s"$path.manifest"),
        check(ref.manifest.artifact == ref.commitment.manifest, s"$path.manifest", "must wrap the exact committed manifest pointer")
      )
    )

  private def validatePathCommitment(commitment: PathCommitment, role: PathRole, path: String): ValidationResult[Unit] = {
    val summary = commitment.summary
    val count = BigInt(summary.entryCount.value)
    val expectedCount = BigInt(summary.newest.ordinal.value.value) - BigInt(summary.oldest.ordinal.value.value) + 1

    combine(
      List(
        check(summary.role == role, s"$path.summary.role", s"must be $role"),
        validateStateRef(summary.oldest, s"$path.summary.oldest"),
        validateStateRef(summary.newest, s"$path.summary.newest"),
        check(expectedCount > 0, s"$path.summary", "oldest ordinal must not exceed newest ordinal"),
        check(count == expectedCount, s"$path.summary.entryCount", "must equal newest.ordinal - oldest.ordinal + 1"),
        validateArtifact(commitment.manifest, FinalityArtifactKind.PathManifest, s"$path.manifest"),
        validateHash(commitment.entriesRoot, s"$path.entriesRoot", rejectZero = true)
      )
    )
  }

  private def validateSuffixAfter(
    ancestor: GlobalSnapshotStateRef,
    summary: PathSummary,
    path: String
  ): ValidationResult[Unit] =
    combine(
      List(
        check(next(ancestor.ordinal.value.value, summary.oldest.ordinal.value.value), s"$path.oldest.ordinal", "must immediately follow the common/prior ancestor"),
        check(summary.oldest.parentHash == ancestor.hash, s"$path.oldest.parentHash", "must equal the common/prior ancestor hash")
      )
    )

  private def isDirectChild(parent: GlobalSnapshotStateRef, child: GlobalSnapshotStateRef): Boolean =
    next(parent.ordinal.value.value, child.ordinal.value.value) && child.parentHash == parent.hash

  private def validateAdjacent(before: GlobalSnapshotStateRef, after: GlobalSnapshotStateRef, path: String): ValidationResult[Unit] =
    combine(
      List(
        check(next(before.ordinal.value.value, after.ordinal.value.value), s"$path.ordinal", "entries must be ordinal-contiguous"),
        check(after.parentHash == before.hash, s"$path.parentHash", "must equal the preceding entry hash")
      )
    )

  private def validateImageReceipt(receipt: MptImageReceipt, target: GlobalSnapshotStateRef, path: String): ValidationResult[Unit] =
    combine(
      List(
        check(receipt.formatVersion == DurableMptImageStore.CurrentFormatVersion, s"$path.formatVersion", "must use the active durable image format"),
        check(receipt.generation >= 0L, s"$path.generation", "must be non-negative"),
        check(receipt.entryCount >= 0, s"$path.entryCount", "must be non-negative"),
        check(receipt.anchor == target, s"$path.anchor", "must equal the exact release target"),
        validateHash(receipt.imageId.value, s"$path.imageId", rejectZero = true),
        validateHash(receipt.digest.value, s"$path.digest", rejectZero = true),
        validateHash(receipt.codecEra.value, s"$path.codecEra", rejectZero = true),
        validateHash(receipt.rootEra.value, s"$path.rootEra", rejectZero = true)
      )
    )

  private def validatePublication(publication: MptActivePublication, path: String): ValidationResult[Unit] =
    combine(
      List(
        check(publication.revision.value >= 0L, s"$path.revision", "must be non-negative"),
        publication.image.fold(validUnit)(image => validateImageReceipt(image, image.anchor, s"$path.image"))
      )
    )

  private def validateStateRef(ref: GlobalSnapshotStateRef, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(ref.hash, s"$path.hash", rejectZero = true),
        validateHash(ref.parentHash, s"$path.parentHash", rejectZero = ref.ordinal.value.value > 0L),
        validateHash(ref.mptRoot.value, s"$path.mptRoot", rejectZero = true)
      )
    )

  private def validateScoped(
    ref: ScopedArtifactRef,
    intentId: IntentId,
    kind: FinalityArtifactKind,
    path: String
  ): ValidationResult[Unit] =
    combine(
      List(
        check(ref.intentId == intentId, s"$path.intentId", "must equal the containing intent"),
        validateArtifact(ref.artifact, kind, s"$path.artifact")
      )
    )

  private def validateScopedOneOf(
    ref: ScopedArtifactRef,
    intentId: IntentId,
    kinds: Set[FinalityArtifactKind],
    path: String
  ): ValidationResult[Unit] =
    combine(
      List(
        check(ref.intentId == intentId, s"$path.intentId", "must equal the containing intent"),
        check(kinds.contains(ref.artifact.kind), s"$path.artifact.kind", s"must be one of ${kinds.mkString(", ")}"),
        validateArtifactShape(ref.artifact, s"$path.artifact")
      )
    )

  private def validateArtifact(pointer: ImmutableArtifactPointer, kind: FinalityArtifactKind, path: String): ValidationResult[Unit] =
    combine(
      List(
        check(pointer.kind == kind, s"$path.kind", s"must be $kind"),
        validateArtifactShape(pointer, path)
      )
    )

  private def validateArtifactShape(pointer: ImmutableArtifactPointer, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(pointer.encoding.value, s"$path.encoding", rejectZero = true),
        validateHash(pointer.id.value, s"$path.id", rejectZero = true),
        validateHash(pointer.digest.value, s"$path.digest", rejectZero = true),
        check(pointer.byteLength.value > 0L, s"$path.byteLength", "must be positive")
      )
    )

  private def validateEffectManifestPointer(pointer: EffectManifestPointer, path: String): ValidationResult[Unit] =
    combine(
      List(
        validateHash(pointer.id.value, s"$path.id", rejectZero = true),
        validateHash(pointer.digest.value, s"$path.digest", rejectZero = true)
      )
    )

  private def validateHash(hash: Hash, path: String, rejectZero: Boolean): ValidationResult[Unit] = {
    val canonical = hash.value.length == 64 && hash.value.forall(character => character >= '0' && character <= '9' || character >= 'a' && character <= 'f')
    combine(
      List(
        check(canonical, path, "must be canonical 32-byte lowercase hexadecimal"),
        check(!rejectZero || hash != Hash.empty, path, "must not be the all-zero reserved hash")
      )
    )
  }

  private def effectCommands(manifest: FinalityEffectManifest): List[(String, EffectKind, ScopedEffectCommand)] =
    List(
      ("chainStoreProjection", ChainStoreProjection, manifest.chainStoreProjection),
      ("snapshotStorageProjection", SnapshotStorageProjection, manifest.snapshotStorageProjection),
      ("tipTrackerProjection", TipTrackerProjection, manifest.tipTrackerProjection),
      ("overlayCacheProjection", OverlayCacheProjection, manifest.overlayCacheProjection),
      (
        "serviceAvailabilityWatermarkProjection",
        ServiceAvailabilityWatermarkProjection,
        manifest.serviceAvailabilityWatermarkProjection
      ),
      ("latestSliceProjection", LatestSliceProjection, manifest.latestSliceProjection),
      ("followProjectionRing", FollowProjectionRing, manifest.followProjectionRing),
      ("accumulatorChangesetPromotion", AccumulatorChangesetPromotion, manifest.accumulatorChangesetPromotion),
      ("signedBytesPromotion", SignedBytesPromotion, manifest.signedBytesPromotion),
      ("sidecarOutboxReconciliation", SidecarOutboxReconciliation, manifest.sidecarOutboxReconciliation),
      ("shardAnchorAndWatermarkReconciliation", ShardAnchorAndWatermarkReconciliation, manifest.shardAnchorAndWatermarkReconciliation),
      ("binaryConfirmationAndRequeue", BinaryConfirmationAndRequeue, manifest.binaryConfirmationAndRequeue),
      ("committeeAdmissionMaintenance", CommitteeAdmissionMaintenance, manifest.committeeAdmissionMaintenance),
      ("etaCommitteeAnchorReconciliation", EtaCommitteeAnchorReconciliation, manifest.etaCommitteeAnchorReconciliation),
      ("mempoolReconciliation", MempoolReconciliation, manifest.mempoolReconciliation),
      ("towerIndexReconciliation", TowerIndexReconciliation, manifest.towerIndexReconciliation),
      ("downstreamFollowerEventEnqueue", DownstreamFollowerEventEnqueue, manifest.downstreamFollowerEventEnqueue),
      ("snapshotRetentionPruning", SnapshotRetentionPruning, manifest.snapshotRetentionPruning)
    )

  private def requirePrior(prior: Option[ReleasedCore], path: String, message: String): ValidationResult[Unit] =
    check(prior.nonEmpty, path, message)

  private def isExpectedGeneration(actual: ReleaseGeneration, prior: Option[ReleasedCore]): Boolean = {
    val expected = prior.fold(BigInt(0))(released => BigInt(released.pointer.generation.value.value) + 1)
    BigInt(actual.value.value) == expected
  }

  private def sameCoreState(left: CoordinatorHead, right: CoordinatorHead): Boolean =
    left.lastAttempt == right.lastAttempt && left.released == right.released && left.active == right.active && left.effects == right.effects

  private def next(before: Long, after: Long): Boolean =
    BigInt(after) == BigInt(before) + 1

  private def validateValue[A](value: A, checks: List[ValidationResult[Unit]]): ValidationResult[A] =
    combine(checks).as(value)

  private def combine(checks: List[ValidationResult[Unit]]): ValidationResult[Unit] =
    checks.foldLeft(validUnit)(_.productR(_))

  private val validUnit: ValidationResult[Unit] = ().validNec

  private def check(condition: Boolean, path: String, invariant: String): ValidationResult[Unit] =
    if (condition) validUnit else Violation(path, invariant).invalidNec

  private def invalid(path: String, invariant: String): ValidationResult[Unit] =
    Violation(path, invariant).invalidNec

  private def checkDerived[A](
    derived: Either[FinalityIdentityError, A],
    actual: A,
    path: String,
    invariant: String
  ): ValidationResult[Unit] =
    derived.fold(
      error => invalid(path, s"canonical derivation failed: ${error.getMessage}"),
      expected => check(actual == expected, path, invariant)
    )

  private def checkVerified(
    verified: Either[FinalityIdentityError, Unit],
    path: String,
    invariant: String
  ): ValidationResult[Unit] =
    verified.fold(
      error => invalid(path, s"$invariant: ${error.getMessage}"),
      _ => validUnit
    )
}
