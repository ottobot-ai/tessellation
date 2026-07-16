package io.constellationnetwork.node.shared.domain.snapshot.finality.model

import io.constellationnetwork.node.shared.domain.snapshot.finality.model.Phase2ConsumerLeaseReferenceModel.CommitResult.{AlreadyCommitted, Committed, Stale}
import io.constellationnetwork.node.shared.domain.snapshot.finality.model.Phase2ConsumerLeaseReferenceModel.CoordinatorMode.Running
import io.constellationnetwork.node.shared.domain.snapshot.finality.model.Phase2ConsumerLeaseReferenceModel.Failure._
import io.constellationnetwork.node.shared.domain.snapshot.finality.model.Phase2ConsumerLeaseReferenceModel.ReferenceUsePolicy._
import io.constellationnetwork.node.shared.domain.snapshot.finality.model.Phase2ConsumerLeaseReferenceModel._
import io.constellationnetwork.node.shared.domain.snapshot.finality.{CanonicalBranchRevision, CanonicalLineageRevision, ReleaseGeneration}
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.SimpleIOSuite

object Phase2ConsumerLeaseReferenceModelSuite extends SimpleIOSuite {

  private def nonNeg(value: Long): NonNegLong = NonNegLong.unsafeFrom(value)
  private def hash(value: Int): Hash = Hash(f"$value%064x")

  private def ref(ordinal: Long, hashValue: Int, parentHash: Hash, rootValue: Int): GlobalSnapshotStateRef =
    GlobalSnapshotStateRef(
      SnapshotOrdinal(nonNeg(ordinal)),
      hash(hashValue),
      parentHash,
      MptRoot(hash(rootValue))
    )

  private val genesis = ref(0L, 1, hash(0), 101)
  private val a = ref(1L, 2, genesis.hash, 102)
  private val b = ref(1L, 3, genesis.hash, 103)
  private val a2 = ref(2L, 4, a.hash, 104)

  private def state(
    canonical: Vector[GlobalSnapshotStateRef] = Vector(genesis, a),
    operational: Map[GlobalSnapshotStateRef, ReleaseGeneration] = Map(a -> ReleaseGeneration(nonNeg(1L))),
    branchRevision: Long = 1L,
    lineageRevision: Long = 0L,
    sink: SinkState = SinkState.empty,
    mode: CoordinatorMode = Running
  ): State =
    State(
      mode,
      CanonicalBranchRevision(nonNeg(branchRevision)),
      CanonicalLineageRevision(nonNeg(lineageRevision)),
      canonical,
      operational,
      sink
    )

  private def leaseOf(base: State, target: GlobalSnapshotStateRef, policy: ReferenceUsePolicy): Lease =
    Phase2ConsumerLeaseReferenceModel
      .capture(base, target, policy)
      .flatMap(descriptor =>
        Phase2ConsumerLeaseReferenceModel.acquireIfCurrent(base, Phase2ConsumerLeaseReferenceModel.verify(descriptor))
      )
      .fold(error => throw new AssertionError(s"expected lease, got $error"), lease => lease)

  pureTest("[FOLLOW-008A] same-ordinal replacement cannot inherit the old hash's Phase-2 release") {
    val before = state()
    val descriptor = capture(before, a, ExactCanonicalAncestor).toOption.get
    val replacement = state(
      canonical = Vector(genesis, b),
      operational = Map.empty,
      branchRevision = 2L,
      lineageRevision = 1L
    )

    expect(capture(replacement, b, ExactCanonicalAncestor) == Left(NotOperational(b))) &&
    expect(acquireIfCurrent(replacement, Phase2ConsumerLeaseReferenceModel.verify(descriptor)) == Left(AcquisitionChanged))
  }

  pureTest("[FOLLOW-001] every GlobalSnapshotStateRef field participates in exact identity") {
    val before = state()
    val variants = Vector(
      a.copy(ordinal = SnapshotOrdinal(NonNegLong.unsafeFrom(2L))),
      a.copy(hash = hash(22)),
      a.copy(parentHash = hash(23)),
      a.copy(mptRoot = MptRoot(hash(24)))
    )

    expect(variants.forall(target => capture(before, target, ExactCanonicalAncestor) == Left(NotCanonical(target))))
  }

  pureTest("[FOLLOW-008C] replacement during unlocked acquisition work mints no lease") {
    val before = state()
    val descriptor = capture(before, a, ExactCanonicalAncestor).toOption.get
    val verifiedOutsideLock = Phase2ConsumerLeaseReferenceModel.verify(descriptor)
    val replacement = state(
      canonical = Vector(genesis, b),
      operational = Map(b -> ReleaseGeneration(nonNeg(2L))),
      branchRevision = 2L,
      lineageRevision = 1L
    )

    expect(acquireIfCurrent(replacement, verifiedOutsideLock) == Left(AcquisitionChanged))
  }

  pureTest("[FOLLOW-008D] replacement after acquisition makes the closed sink command stale") {
    val before = state()
    val lease = leaseOf(before, a, ExactCanonicalAncestor)
    val replacement = state(
      canonical = Vector(genesis, b),
      operational = Map(b -> ReleaseGeneration(nonNeg(2L))),
      branchRevision = 2L,
      lineageRevision = 1L
    )
    val command = ClosedCommand("signature-a", "candidate-signature-bytes")

    expect(commitIfCurrent(replacement, lease, command) == Stale(replacement, LeaseLineageChanged)) &&
    expect(replacement.sink == SinkState.empty)
  }

  pureTest("[FOLLOW-008I] descendant extension preserves ancestor work but invalidates latest-head work") {
    val before = state()
    val ancestorLease = leaseOf(before, a, ExactCanonicalAncestor)
    val latestLease = leaseOf(before, a, LatestOperationalHead)
    val extension = state(
      canonical = Vector(genesis, a, a2),
      operational = Map(a -> ReleaseGeneration(nonNeg(1L)), a2 -> ReleaseGeneration(nonNeg(2L))),
      branchRevision = 2L,
      lineageRevision = 0L
    )
    val ancestorCommit = commitIfCurrent(extension, ancestorLease, ClosedCommand("ancestor-read", "result"))
    val latestCommit = commitIfCurrent(extension, latestLease, ClosedCommand("latest-read", "result"))

    expect(ancestorCommit.isInstanceOf[Committed]) &&
    expect(latestCommit == Stale(extension, NotLatestOperational(a, Some(a2))))
  }

  pureTest("[FOLLOW-008J] replacement above a surviving target still requires lease reacquisition") {
    val before = state(
      canonical = Vector(genesis, a, a2),
      operational = Map(a -> ReleaseGeneration(nonNeg(1L)), a2 -> ReleaseGeneration(nonNeg(2L)))
    )
    val oldLease = leaseOf(before, a, ExactCanonicalAncestor)
    val b2 = ref(2L, 5, a.hash, 105)
    val replacement = state(
      canonical = Vector(genesis, a, b2),
      operational = Map(a -> ReleaseGeneration(nonNeg(1L)), b2 -> ReleaseGeneration(nonNeg(3L))),
      branchRevision = 2L,
      lineageRevision = 1L
    )
    val oldCommit = commitIfCurrent(replacement, oldLease, ClosedCommand("old", "result"))
    val reacquired = leaseOf(replacement, a, ExactCanonicalAncestor)
    val newCommit = commitIfCurrent(replacement, reacquired, ClosedCommand("new", "result"))

    expect(oldCommit == Stale(replacement, LeaseLineageChanged)) &&
    expect(newCommit.isInstanceOf[Committed])
  }

  pureTest("[FOLLOW-008K] A to B to A cannot revive a lease from A's prior lineage") {
    val firstA = state()
    val oldLease = leaseOf(firstA, a, ExactCanonicalAncestor)
    val returnedA = state(
      canonical = Vector(genesis, a),
      operational = Map(a -> ReleaseGeneration(nonNeg(3L))),
      branchRevision = 3L,
      lineageRevision = 2L
    )

    expect(commitIfCurrent(returnedA, oldLease, ClosedCommand("aba", "result")) == Stale(returnedA, LeaseLineageChanged))
  }

  pureTest("[FOLLOW-008P] identical current commands are idempotent and an ID collision enters recovery") {
    val before = state()
    val lease = leaseOf(before, a, ExactCanonicalAncestor)
    val command = ClosedCommand("command-1", "payload")
    val first = commitIfCurrent(before, lease, command).asInstanceOf[Committed]
    val retry = commitIfCurrent(first.state, lease, command)
    val collision = commitIfCurrent(first.state, lease, command.copy(payload = "different"))

    expect(retry == AlreadyCommitted(first.state, first.command)) &&
    expect(
      collision == CommitResult.RecoveryRequired(
        first.state,
        CommandIdCollision(first.command, first.command.copy(payload = "different"))
      )
    ) &&
    expect(first.state.sink.revision == SinkRevision(1L))
  }

  pureTest("RecoveryRequired is fail-closed at capture and commit") {
    val running = state()
    val lease = leaseOf(running, a, ExactCanonicalAncestor)
    val recovery = running.copy(mode = CoordinatorMode.RecoveryRequired)

    expect(capture(recovery, a, ExactCanonicalAncestor) == Left(Failure.RecoveryRequired)) &&
    expect(
      commitIfCurrent(recovery, lease, ClosedCommand("blocked", "payload")) ==
        CommitResult.RecoveryRequired(recovery, Failure.RecoveryRequired)
    )
  }
}
