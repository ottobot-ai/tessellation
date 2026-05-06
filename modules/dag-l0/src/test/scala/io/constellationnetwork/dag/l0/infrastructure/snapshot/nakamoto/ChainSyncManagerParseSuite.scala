package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import io.constellationnetwork.node.shared.domain.nakamoto.chainsync.ChainSyncStateResponse
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.node.shared.infrastructure.consensus.nakamoto.proto.{sidecar => pb}
import io.constellationnetwork.security.hash.Hash

import com.google.protobuf.ByteString
import weaver.SimpleIOSuite

/** Wire-parse tests for `ChainSyncManager.parsePbSnapshot` (#56.8). Exercises the three disposition cases plus the legacy-peer fallback
  * when `branch_id` is empty.
  */
object ChainSyncManagerParseSuite extends SimpleIOSuite {

  private def mkPb(
    hashStr: String,
    finalized: Boolean,
    branchIdStr: String = ""
  ): pb.Snapshot = pb.Snapshot(
    hash = ByteString.copyFromUtf8(hashStr),
    slot = 1L,
    ordinal = 1L,
    parentHash = ByteString.copyFromUtf8("p" * 64),
    payload = ByteString.EMPTY,
    finalized = finalized,
    branchId = if (branchIdStr.isEmpty) ByteString.EMPTY else ByteString.copyFromUtf8(branchIdStr)
  )

  pureTest("finalized=true ⇒ Finalized variant") {
    val pbSnap = mkPb("a" * 64, finalized = true)
    val resp = ChainSyncManager.parsePbSnapshot(pbSnap)
    expect(
      resp match {
        case ChainSyncStateResponse.Finalized(s) => s eq pbSnap
        case _                                   => false
      }
    )
  }

  pureTest("finalized=false with branch_id populated ⇒ Provisional with that branch") {
    val branchHash = "b" * 64
    val pbSnap = mkPb("a" * 64, finalized = false, branchIdStr = branchHash)
    val resp = ChainSyncManager.parsePbSnapshot(pbSnap)
    expect(
      resp match {
        case ChainSyncStateResponse.Provisional(_, branch) => branch == BranchId(Hash(branchHash))
        case _                                             => false
      }
    )
  }

  pureTest("finalized=false with branch_id empty ⇒ Provisional fallback to snapshot.hash") {
    val snapHash = "a" * 64
    val pbSnap = mkPb(snapHash, finalized = false, branchIdStr = "")
    val resp = ChainSyncManager.parsePbSnapshot(pbSnap)
    expect(
      resp match {
        case ChainSyncStateResponse.Provisional(_, branch) => branch == BranchId(Hash(snapHash))
        case _                                             => false
      }
    )
  }
}
