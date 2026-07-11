package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO

import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ShardCheckpointAcceptResult
import io.constellationnetwork.schema.peer.PeerId
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
}
