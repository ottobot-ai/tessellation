package io.constellationnetwork.dag.l0.infrastructure.snapshot

import cats.data.NonEmptyList

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex

import eu.timepit.refined.types.all.NonNegLong
import weaver.SimpleIOSuite

/** Slice 14 — unit tests for [[GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures]]: the pure helper the gl0 consensus LEADER uses
  * to enrich a candidate checkpoint's `committeeSignatures` with the committee attestations collected in the per-shard `ShardTipTracker`,
  * so `ShardCheckpointGl0AcceptanceManager.verifyEmbedded` counts `>= kQuorum` distinct signers and adopts via the FAST verify path instead
  * of the slow re-exec failover.
  *
  * Determinism is the load-bearing property: every follower threads the leader's enriched set unchanged and re-verifies it, so the splice
  * must (a) leave the canonical signing-preimage UNTOUCHED (`committeeSignatures` is excluded from `ShardCheckpointSigPreimage`), (b) dedup
  * by `peerId` so the producer's own embedded signature is never double-counted, and (c) order the appended signers canonically (sorted by
  * `peerId` hex) so the leader re-validating its own artifact yields byte-identical bytes regardless of `Map` iteration order.
  */
object SpliceCommitteeSignaturesSuite extends SimpleIOSuite {

  private def peer(b: String): PeerId = PeerId(Hex(b * 64))

  private def sig(p: PeerId): CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = p,
      vrfProof = Hex.fromBytes(Array.fill[Byte](80)(0x42.toByte)),
      ed25519Sig = Hex.fromBytes(Array.fill[Byte](64)(0x11.toByte)),
      kesProductSig = Hex.fromBytes(Array.fill[Byte](32)(0x43.toByte)),
      kesTreeStep = 0
    )

  // peerIds with KNOWN hex ordering: aa < bb < cc < dd. Producer = pB (the MIDDLE) so the sorted appended set
  // (pA, then pC, pD) straddles it — proving the producer head is PRESERVED, not re-sorted into position.
  private val pA = peer("aa")
  private val pB = peer("bb")
  private val pC = peer("cc")
  private val pD = peer("dd")

  private def baseCheckpoint(producer: PeerId): ShardCheckpoint =
    ShardCheckpoint(
      shardId = ShardId.unsafeApply(0),
      parentCheckpointHash = Hash("0" * 64),
      shardOrdinal = ShardOrdinal(1L),
      gl0AnchorOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(5L)),
      slot = SlotT.unsafeApply(5L),
      derivedStateDelta = ShardDerivedStateDelta.empty,
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(sig(producer)),
      epoch = EtaPeriod(0L)
    )

  pureTest("enriches with collected sigs, dedups the producer by peerId, sorts the appended signers by peerId hex") {
    val cp = baseCheckpoint(pB)
    // collected includes the producer (pB) plus three others, in NON-sorted insertion order
    val collected: Map[PeerId, CommitteeMemberSignature] =
      Map(pD -> sig(pD), pB -> sig(pB), pA -> sig(pA), pC -> sig(pC))
    val out = GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(cp, collected)
    val peers = out.committeeSignatures.toList.map(_.peerId)
    expect(out.committeeSignatures.size == 4)
      .and( // pB + {pA, pC, pD}; pB NOT doubled
        expect(peers.toSet == Set(pA, pB, pC, pD))
      )
      .and(expect(peers.head == pB))
      .and( // producer head preserved (not re-sorted)
        expect(peers.tail == List(pA, pC, pD))
      ) // appended signers sorted by peerId hex
  }

  pureTest("leaves the signing preimage (canonical checkpoint-hash bytes) UNCHANGED") {
    val cp = baseCheckpoint(pB)
    val out = GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(cp, Map(pA -> sig(pA), pC -> sig(pC)))
    // committeeSignatures is excluded from ShardCheckpointSigPreimage, so splicing extra signers must not change identity.
    expect(out.signingPreimage == cp.signingPreimage)
  }

  pureTest("no-op when collected is empty") {
    val cp = baseCheckpoint(pB)
    expect(GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(cp, Map.empty) == cp)
  }

  pureTest("no-op when the only collected signer is the already-embedded producer (dedup)") {
    val cp = baseCheckpoint(pB)
    expect(GlobalSnapshotConsensusFunctions.spliceCommitteeSignatures(cp, Map(pB -> sig(pB))) == cp)
  }
}
