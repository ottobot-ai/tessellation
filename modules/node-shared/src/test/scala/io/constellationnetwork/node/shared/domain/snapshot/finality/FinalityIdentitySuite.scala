package io.constellationnetwork.node.shared.domain.snapshot.finality

import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCodecFixtures._
import io.constellationnetwork.node.shared.domain.snapshot.finality.FinalityCoreCodecs.releasedCoreRecordPayloadCodec
import io.constellationnetwork.security.hash.Hash

import scodec.bits.ByteVector
import weaver.SimpleIOSuite

object FinalityIdentitySuite extends SimpleIOSuite {

  pureTest("effect identity commits every state-bearing command input but excludes retry scope") {
    val command = effectCommands.head
    val original = FinalityIdentity.effectId(command.identityPreimage)
    val changedInputs = List(
      command.copy(scope = command.scope.copy(domain = command.scope.domain.copy(networkId = hash(9400)))),
      command.copy(scope = command.scope.copy(generation = ReleaseGeneration(nonNeg(6L)))),
      command.copy(scope = command.scope.copy(priorState = None)),
      command.copy(scope = command.scope.copy(transitionDigest = hash(9401))),
      command.copy(scope = command.scope.copy(target = state(42L, 42, 41))),
      command.copy(kind = EffectKind.SnapshotStorageProjection),
      command.copy(expectedBefore = EffectStateDigest(hash(9402))),
      command.copy(expectedRevision = EffectSinkRevision(nonNeg(6L))),
      command.copy(desiredAfter = EffectStateDigest(hash(9403))),
      command.copy(desiredRevision = EffectSinkRevision(nonNeg(9L))),
      command.copy(payload = command.payload.copy(artifact = artifact(FinalityArtifactKind.EffectPayload, 9404))),
      command.copy(predecessor = Some(EffectId(hash(9405))))
    )
    val changedIdentities = changedInputs.map(value => FinalityIdentity.effectId(value.identityPreimage))
    val retryScoped = command.copy(scope = command.scope.copy(intent = IntentId(hash(9406))))
    val idempotency = original.flatMap(FinalityIdentity.effectIdempotencyKey)

    expect.all(
      original.isRight,
      changedIdentities.forall(result => result.isRight && result != original),
      FinalityIdentity.effectId(retryScoped.identityPreimage) == original,
      idempotency == original.flatMap(FinalityIdentity.effectIdempotencyKey),
      idempotency.exists(key => original.exists(_.value != key.value))
    )
  }

  pureTest("intent identity commits every scope field") {
    val original = FinalityIdentity.intentId(intentScope)
    val changedAttempt = FinalityIdentity.intentId(
      intentScope.copy(attempt = IntentAttempt(nonNeg(intentScope.attempt.value.value + 1L)))
    )
    val changedTarget = FinalityIdentity.intentId(intentScope.copy(target = state(42L, 42, 41)))
    val changedEffect = FinalityIdentity.intentId(
      intentScope.copy(
        effects = intentScope.effects.copy(
          towerIndexReconciliation = intentScope.effects.towerIndexReconciliation.copy(
            desiredAfter = EffectStateDigest(hash(9000))
          )
        )
      )
    )

    expect.all(
      original.isRight,
      original == FinalityIdentity.intentId(intentScope),
      changedAttempt.isRight && changedAttempt != original,
      changedTarget.isRight && changedTarget != original,
      changedEffect.isRight && changedEffect != original
    )
  }

  pureTest("artifact pointer commits kind, encoding, bytes, and byte length") {
    val bytes = ByteVector.encodeAscii("exact finality payload").toOption.get
    val alternate = ByteVector.encodeAscii("exact finality payload!").toOption.get
    val externalEncoding = ArtifactEncoding(hash(9100))

    val canonical = FinalityIdentity.artifactPointerFromBytes(FinalityArtifactKind.EffectPayload, bytes)
    val external = FinalityIdentity.artifactPointerFromBytes(
      FinalityArtifactKind.EffectPayload,
      externalEncoding,
      bytes
    )
    val changedKind = FinalityIdentity.artifactPointerFromBytes(FinalityArtifactKind.CoreBatch, bytes)
    val changedBytes = FinalityIdentity.artifactPointerFromBytes(FinalityArtifactKind.EffectPayload, alternate)

    expect.all(
      canonical.exists(_.byteLength.value == bytes.length),
      canonical.exists(_.encoding == FinalityIdentity.ScodecV1Encoding),
      external.exists(_.encoding == externalEncoding),
      external != canonical,
      changedKind != canonical,
      changedBytes != canonical
    )
  }

  pureTest("artifact identity rejects an empty payload instead of emitting an invalid pointer") {
    val result = FinalityIdentity.artifactPointerFromBytes(
      FinalityArtifactKind.EffectPayload,
      ByteVector.empty
    )

    expect(result == Left(FinalityIdentityError.EmptyArtifact(FinalityArtifactKind.EffectPayload)))
  }

  pureTest("typed artifact verification rejects a substituted released-record payload") {
    val pointer = FinalityIdentity.artifactPointer(
      FinalityArtifactKind.ReleasedCoreRecord,
      releasedCoreRecordPayloadCodec,
      releasedRecordPayload
    )
    val substituted = releasedRecordPayload.copy(effectManifest = previousEffectManifest)

    val accepted = pointer.flatMap(
      FinalityIdentity.verifyArtifactPointer(
        _,
        FinalityArtifactKind.ReleasedCoreRecord,
        releasedCoreRecordPayloadCodec,
        releasedRecordPayload
      )
    )
    val rejected = pointer.flatMap(
      FinalityIdentity.verifyArtifactPointer(
        _,
        FinalityArtifactKind.ReleasedCoreRecord,
        releasedCoreRecordPayloadCodec,
        substituted
      )
    )

    expect.all(accepted == Right(()), rejected.isLeft)
  }

  pureTest("effect manifest pointer changes with any command and verifies exact bytes") {
    val pointer = FinalityIdentity.effectManifestPointer(effectManifest)
    val changed = effectManifest.copy(
      mempoolReconciliation = effectManifest.mempoolReconciliation.copy(
        desiredAfter = EffectStateDigest(hash(9200))
      )
    )

    expect.all(
      pointer.isRight,
      pointer == FinalityIdentity.effectManifestPointer(effectManifest),
      FinalityIdentity.effectManifestPointer(changed) != pointer,
      pointer.flatMap(FinalityIdentity.verifyEffectManifestPointer(_, effectManifest)) == Right(()),
      pointer.flatMap(FinalityIdentity.verifyEffectManifestPointer(_, changed)).isLeft
    )
  }

  pureTest("audit and recovery pointers commit their complete payloads") {
    val audit = FinalityIdentity.auditPointer(auditRecord)
    val changedAudit = auditRecord.copy(mutation = CoordinatorMutationKind.Released)
    val recovery = FinalityIdentity.recoveryPointer(recoveryRecord)
    val changedRecovery = recoveryRecord.copy(reason = RecoveryReason.UnknownCore(hash(9300)))

    expect.all(
      audit.isRight,
      audit != FinalityIdentity.auditPointer(changedAudit),
      audit.flatMap(FinalityIdentity.verifyAuditPointer(_, auditRecord)) == Right(()),
      audit.flatMap(FinalityIdentity.verifyAuditPointer(_, changedAudit)).isLeft,
      recovery.isRight,
      recovery != FinalityIdentity.recoveryPointer(changedRecovery),
      recovery.flatMap(FinalityIdentity.verifyRecoveryPointer(_, recoveryRecord)) == Right(()),
      recovery.flatMap(FinalityIdentity.verifyRecoveryPointer(_, changedRecovery)).isLeft
    )
  }

  pureTest("path root commits ordered entries but not chunk framing") {
    val entries = List(state(10L, 10, 9), state(11L, 11, 10), state(12L, 12, 11))
    val oneShot = FinalityIdentity.pathEntriesRoot(entries)
    val framedOneByOne = entries
      .foldLeft[Either[FinalityIdentityError, FinalityIdentity.PathEntriesAccumulator]](
        Right(FinalityIdentity.PathEntriesAccumulator.empty)
      )((acc, entry) => acc.flatMap(_.append(entry)))
      .flatMap(_.root)
    val framedTwoPlusOne = for {
      first <- FinalityIdentity.PathEntriesAccumulator.empty.append(entries.head).flatMap(_.append(entries(1)))
      second <- first.append(entries(2))
      root <- second.root
    } yield root
    val reordered = FinalityIdentity.pathEntriesRoot(entries.reverse)
    val duplicated = FinalityIdentity.pathEntriesRoot(entries :+ entries.last)

    expect.all(
      oneShot.isRight,
      framedOneByOne == oneShot,
      framedTwoPlusOne == oneShot,
      reordered != oneShot,
      duplicated != oneShot
    )
  }

  pureTest("external encoding identifiers must be canonical 32-byte hashes") {
    val malformed = ArtifactEncoding(Hash("not-a-hash"))
    val uppercase = ArtifactEncoding(Hash("AB" * 32))
    val empty = ArtifactEncoding(Hash.empty)
    val malformedResult = FinalityIdentity.artifactPointerFromBytes(
      FinalityArtifactKind.EffectPayload,
      malformed,
      ByteVector(1, 2, 3)
    )
    val uppercaseResult = FinalityIdentity.artifactPointerFromBytes(
      FinalityArtifactKind.EffectPayload,
      uppercase,
      ByteVector(1, 2, 3)
    )
    val emptyResult = FinalityIdentity.artifactPointerFromBytes(
      FinalityArtifactKind.EffectPayload,
      empty,
      ByteVector(1, 2, 3)
    )

    expect.all(malformedResult.isLeft, uppercaseResult.isLeft, emptyResult.isLeft)
  }
}
