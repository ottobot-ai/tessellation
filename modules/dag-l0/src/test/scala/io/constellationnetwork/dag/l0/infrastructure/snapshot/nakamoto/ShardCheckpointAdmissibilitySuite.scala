package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Ref}

import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.{
  ShardCheckpointAcceptResult,
  VerifiedShardCheckpointFailure
}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import weaver.SimpleIOSuite

object ShardCheckpointAdmissibilitySuite extends SimpleIOSuite {

  test("only non-rejecting replay verdicts may proceed to store or attestation") {
    val signers = List(PeerId(Hex("aa" * 64)))
    IO.pure(
      expect.all(
        NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.Accepted),
        NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.PendingMoreAttestations),
        !NakamotoSyncDaemon.shardCheckpointAdmissible(ShardCheckpointAcceptResult.Rejected("bad pre-check")),
        !NakamotoSyncDaemon.shardCheckpointAdmissible(
          ShardCheckpointAcceptResult.RejectedReExecutionMismatch("mismatch", signers)
        )
      )
    )
  }

  test("only an affirmative replay mismatch may trigger portable fraud-evidence construction") {
    val signers = List(PeerId(Hex("aa" * 64)))
    IO.pure(
      expect.all(
        NakamotoSyncDaemon.shardCheckpointFraudEvidenceEligible(
          VerifiedShardCheckpointFailure.ReExecutionMismatch("wrong root", signers)
        ),
        !NakamotoSyncDaemon.shardCheckpointFraudEvidenceEligible(
          VerifiedShardCheckpointFailure.Rejected("pinned replay base unavailable")
        ),
        !NakamotoSyncDaemon.shardCheckpointFraudEvidenceEligible(
          VerifiedShardCheckpointFailure.Rejected("execution certificate malformed")
        )
      )
    )
  }

  test("failed producer pre-authorization cannot fetch, replay, or store a missing-parent checkpoint") {
    for {
      fetches <- Ref.of[IO, Int](0)
      replays <- Ref.of[IO, Int](0)
      stores <- Ref.of[IO, Int](0)
      buffers <- Ref.of[IO, Int](0)
      result <- NakamotoSyncDaemon.afterCheckpointProducerPreAuthorization[IO, Unit](
        IO.pure(Left("producer is not in the expected committee"))
      ) {
        fetches.update(_ + 1) >> replays.update(_ + 1) >> stores.update(_ + 1) >> buffers.update(_ + 1)
      }
      fetchCount <- fetches.get
      replayCount <- replays.get
      storeCount <- stores.get
      bufferCount <- buffers.get
    } yield
      expect.all(
        result == Left("producer is not in the expected committee"),
        fetchCount == 0,
        replayCount == 0,
        storeCount == 0,
        bufferCount == 0
      )
  }

  test("parent-first recursive recovery re-enters replay only after the full ancestry is stored") {
    final case class Checkpoint(id: String, parent: Option[String])

    val grandparent = Checkpoint("grandparent", None)
    val parent = Checkpoint("parent", Some(grandparent.id))
    val child = Checkpoint("child", Some(parent.id))
    val byId = List(grandparent, parent, child).map(cp => cp.id -> cp).toMap

    for {
      stored <- Ref.of[IO, Set[String]](Set.empty)
      events <- Ref.of[IO, List[String]](List.empty)
      deferred <- Ref.of[IO, List[String]](List.empty)

      process = (checkpoint: Checkpoint, depth: Int) => {
        def loop(current: Checkpoint, remaining: Int): IO[Unit] = {
          val parentReady = current.parent match {
            case None => IO.pure(true)
            case Some(parentId) =>
              stored.get.flatMap { alreadyStored =>
                if (alreadyStored.contains(parentId)) IO.pure(true)
                else if (remaining <= 0) IO.pure(false)
                else loop(byId(parentId), remaining - 1) >> stored.get.map(_.contains(parentId))
              }
          }

          NakamotoSyncDaemon.afterCheckpointParentRecovery(parentReady)(
            defer = deferred.update(_ :+ current.id),
            validateAndReplay = events.update(_ :+ s"replay-${current.id}") >>
              stored.update(_ + current.id) >>
              events.update(_ :+ s"store-${current.id}")
          )
        }

        loop(checkpoint, depth)
      }
      _ <- process(child, 2)
      storedIds <- stored.get
      observed <- events.get
      deferredIds <- deferred.get
    } yield
      expect.all(
        storedIds == Set(grandparent.id, parent.id, child.id),
        observed == List(
          "replay-grandparent",
          "store-grandparent",
          "replay-parent",
          "store-parent",
          "replay-child",
          "store-child"
        ),
        deferredIds.isEmpty
      )
  }

  test("an authenticated missing-parent checkpoint is drained exactly once after its exact parent is stored") {
    val parent = Hash("11" * 32)
    val otherParent = Hash("22" * 32)
    val childId = Hash("33" * 32)

    for {
      pending <- Ref.of[IO, NakamotoSyncDaemon.PendingCheckpointChildren[String]](
        NakamotoSyncDaemon.PendingCheckpointChildren.empty
      )
      buffered <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        childId,
        "authenticated-child",
        retainedBytes = 64L,
        maxCount = 4,
        maxBytes = 1024L,
        pendingRef = pending
      )
      unrelated <- NakamotoSyncDaemon.drainBoundedPendingCheckpointChildren(otherParent, pending)
      ready <- NakamotoSyncDaemon.drainBoundedPendingCheckpointChildren(parent, pending)
      duplicateDrain <- NakamotoSyncDaemon.drainBoundedPendingCheckpointChildren(parent, pending)
    } yield
      expect.all(
        buffered.buffered,
        !buffered.duplicate,
        unrelated.isEmpty,
        ready == List("authenticated-child"),
        duplicateDrain.isEmpty
      )
  }

  test("the pending checkpoint recovery buffer deduplicates and enforces count and byte bounds") {
    val parent = Hash("44" * 32)
    val child1 = Hash("55" * 32)
    val child2 = Hash("66" * 32)
    val child3 = Hash("77" * 32)
    val child4 = Hash("99" * 32)
    val oversized = Hash("88" * 32)

    for {
      pending <- Ref.of[IO, NakamotoSyncDaemon.PendingCheckpointChildren[String]](
        NakamotoSyncDaemon.PendingCheckpointChildren.empty
      )
      first <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        child1,
        "first",
        retainedBytes = 6L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      second <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        child2,
        "second",
        retainedBytes = 6L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      afterByteEviction <- pending.get
      duplicate <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        child2,
        "duplicate-second",
        retainedBytes = 6L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      refused <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        oversized,
        "oversized",
        retainedBytes = 11L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      third <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        child3,
        "third",
        retainedBytes = 4L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      fourth <- NakamotoSyncDaemon.bufferBoundedPendingCheckpointChild(
        parent,
        child4,
        "fourth",
        retainedBytes = 0L,
        maxCount = 2,
        maxBytes = 10L,
        pendingRef = pending
      )
      state <- pending.get
      ready <- NakamotoSyncDaemon.drainBoundedPendingCheckpointChildren(parent, pending)
      afterDrain <- pending.get
    } yield
      expect.all(
        first.buffered,
        second.buffered && second.evicted == 1,
        afterByteEviction.entries.map(_.childId) == Vector(child2),
        afterByteEviction.retainedBytes == 6L,
        duplicate.duplicate && !duplicate.buffered,
        !refused.buffered && !refused.duplicate,
        third.buffered,
        fourth.buffered && fourth.evicted == 1,
        state.entries.map(_.childId) == Vector(child3, child4),
        state.retainedBytes == 4L,
        ready == List("third", "fourth"),
        afterDrain.entries.isEmpty && afterDrain.retainedBytes == 0L
      )
  }
}
