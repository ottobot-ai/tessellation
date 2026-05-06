package io.constellationnetwork.node.shared.domain.nakamoto.chainsync

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.BranchId
import io.constellationnetwork.security.hash.Hash

/** Sealed-trait response for ChainSync state queries (#56.8).
  *
  * Replaces the unstructured raw-snapshot return path of `ChainSyncServer.ServeSnapshots`. The disposition variant lets consumers react
  * correctly to whether a fetched snapshot is durable or provisional:
  *
  *   - [[ChainSyncStateResponse.Finalized]] — the snapshot is on the canonical chain at an ordinal ≤ chainStore's last finalized ordinal.
  *     Durable. Consumers may cache aggressively and treat as authoritative.
  *   - [[ChainSyncStateResponse.Provisional]] — the snapshot is on a pending branch above the finalized point. May be reorged out.
  *     Consumers should NOT treat as authoritative; the carried `branch` lets them correlate provisional fetches across requests.
  *   - [[ChainSyncStateResponse.NotFound]] — the responding peer does not have the requested hash. Stream-omission was the legacy encoding;
  *     the explicit ADT case lets JVM-side consumers express the absence without conflating it with empty streams.
  *
  * '''Wire encoding''' (added in #56.8): `pb.Snapshot` carries `finalized: bool` + `branch_id: bytes` fields. `Finalized` ⇒
  * `finalized=true`, `branch_id` empty. `Provisional` ⇒ `finalized=false`, `branch_id` populated. `NotFound` is currently encoded as stream
  * omission — there's no `pb.Snapshot` emitted for missing hashes. Future work could add an explicit "not found" wire variant if consumers
  * want to distinguish "asked for, peer doesn't have it" from "asked for, peer has it but stream cut off mid-flight".
  *
  * '''Green-field deployment note''': no peer-version compat required; the disposition fields are non-default in proto3 so existing Go
  * sidecars that haven't been regenerated still relay bytes correctly (proto3 unknown-field passthrough).
  */
sealed trait ChainSyncStateResponse[+A] extends Product with Serializable {
  def fold[B](onFinalized: A => B, onProvisional: (A, BranchId) => B, onNotFound: Hash => B): B = this match {
    case ChainSyncStateResponse.Finalized(state)             => onFinalized(state)
    case ChainSyncStateResponse.Provisional(state, branchId) => onProvisional(state, branchId)
    case ChainSyncStateResponse.NotFound(hash)               => onNotFound(hash)
  }
}

object ChainSyncStateResponse {

  /** Snapshot at a finalized ordinal — durable, immutable. Served from the on-disk base trie / chain store's canonical view. */
  final case class Finalized[A](state: A) extends ChainSyncStateResponse[A]

  /** Snapshot on a pending branch, identified by `branch`. May be discarded if the branch is reorged out. The branch id is the snapshot's
    * own hash (since branch identity = snapshot tip hash in the multi-branch overlay).
    */
  final case class Provisional[A](state: A, branch: BranchId) extends ChainSyncStateResponse[A]

  /** The responding peer does not have the requested hash. Carries the requested hash so consumers can correlate the absence with their
    * pending fetch list (especially useful when batching multiple hashes into one request).
    */
  final case class NotFound(hash: Hash) extends ChainSyncStateResponse[Nothing]
}
