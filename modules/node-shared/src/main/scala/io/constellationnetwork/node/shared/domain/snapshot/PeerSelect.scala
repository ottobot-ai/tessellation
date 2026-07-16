package io.constellationnetwork.node.shared.domain.snapshot

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.peer.L0Peer
import io.constellationnetwork.security.hash.Hash

final case class PeerSelection(peer: L0Peer, ordinal: SnapshotOrdinal, hash: Hash)

trait PeerSelect[F[_]] {
  def select: F[PeerSelection]
}
