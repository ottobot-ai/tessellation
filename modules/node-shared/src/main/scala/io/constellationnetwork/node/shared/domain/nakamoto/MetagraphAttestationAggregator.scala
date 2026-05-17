package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Tally of per-metagraph committee attestations (Slice S2.5).
  *
  * Inbound `MetagraphAttestation` messages from gl0 peers land here keyed by
  * `(metagraphAddress, snapshotOrd, binaryHash)`. Per key, the aggregator holds the set of distinct
  * peers that have attested. Slice S3 reads `thresholdReached` to gate inclusion of the binary
  * into the next global snapshot; for now (S2.5) the aggregator is observability-only — the gate
  * call is added without re-shaping this interface.
  *
  * '''Verification responsibility is upstream of `record`.''' Callers MUST verify the attestation's
  * committee VRF proof (via `CommitteeSortition.verifyMembership`) AND its KES signature (Slice 9
  * path) BEFORE invoking `record`. This trait does not re-verify — it counts. Bug-by-construction
  * note: if callers skip verification, attackers could inflate counts.
  *
  * Eviction strategy: caller invokes `pruneBelow(metagraphAddress, keepFromOrd)` when a metagraph
  * snapshot ordinal has been included in a finalized gl0 snapshot (i.e., the attestation is no
  * longer load-bearing for any decision). Without pruning the map grows monotonically.
  *
  * Concurrency: single `Ref` holds all per-binary tally state. Reads are lock-free; writes use
  * `modify`/`update`. At thousands of metagraphs × ~10 in-flight binaries per metagraph × ~K
  * peers per binary, the map stays well under 1M entries — fine in a single Ref. If we hit
  * contention later, sharding by metagraph hash is a drop-in change.
  */
trait MetagraphAttestationAggregator[F[_]] {

  /** Record a verified inbound attestation. Returns the post-record count for the binary —
    * callers can compare against threshold without a second read. Idempotent: re-recording the
    * same `(metagraphAddress, snapshotOrd, binaryHash, peerId)` returns the same count.
    */
  def record(
    metagraphAddress: Address,
    snapshotOrd: Long,
    binaryHash: Hash,
    peerId: PeerId
  ): F[Int]

  /** Current count of distinct committee members that have attested this binary. */
  def countFor(
    metagraphAddress: Address,
    snapshotOrd: Long,
    binaryHash: Hash
  ): F[Int]

  /** Check whether the binary has reached `⌈2 K_target / 3⌉` attestations. Threshold matches
    * the `TipTracker.FinalityThreshold` semantics so committee finality parallels chain finality.
    * `kTarget = 0` is rejected (would make the threshold meaningless).
    */
  def thresholdReached(
    metagraphAddress: Address,
    snapshotOrd: Long,
    binaryHash: Hash,
    kTarget: Int
  ): F[Boolean]

  /** Drop tally state for `metagraphAddress` snapshots strictly below `keepFromOrd`. Called when
    * the corresponding binaries are finalized — no further decisions are pending against them.
    */
  def pruneBelow(metagraphAddress: Address, keepFromOrd: Long): F[Unit]

  /** Diagnostic: number of distinct `(metagraph, ord, hash)` keys currently tracked. */
  def size: F[Int]
}

object MetagraphAttestationAggregator {

  /** Quorum fraction parallel to `TipTracker.FinalityThreshold` (2/3 by default, env override).
    * Same env knob to keep one finality surface: equivocation in the committee threshold against
    * chain finality would be a footgun if the two ever drifted out of sync.
    */
  val FinalityThreshold: Ratio = TipTracker.FinalityThreshold

  /** Compute the count required to reach `FinalityThreshold` of `kTarget`. `ceil(threshold · K)`
    * computed in exact-Ratio so we stay byte-identical across observers. */
  def requiredCount(kTarget: Int): Int = {
    require(kTarget > 0, s"kTarget must be positive, got $kTarget")
    val num = FinalityThreshold.numerator
    val den = FinalityThreshold.denominator
    val product = num * BigInt(kTarget)
    val (quot, rem) = (product / den, product % den)
    val ceil = if (rem == 0) quot else quot + 1
    ceil.toInt
  }

  /** Composite key for the per-binary tally state. Keeping `Address` separate (not folded into
    * the hash) gives `pruneBelow(metagraphAddress, ord)` a cheap O(per-metagraph) prune via the
    * outer-map filter rather than walking every entry.
    */
  private final case class BinaryKey(snapshotOrd: Long, binaryHash: Hash)

  def make[F[_]: Sync]: F[MetagraphAttestationAggregator[F]] =
    Ref.of[F, Map[Address, Map[BinaryKey, Set[PeerId]]]](Map.empty).map { tallyRef =>
      new MetagraphAttestationAggregator[F] {

        def record(
          metagraphAddress: Address,
          snapshotOrd: Long,
          binaryHash: Hash,
          peerId: PeerId
        ): F[Int] = {
          val key = BinaryKey(snapshotOrd, binaryHash)
          tallyRef.modify { outer =>
            val inner = outer.getOrElse(metagraphAddress, Map.empty)
            val priorPeers = inner.getOrElse(key, Set.empty)
            val newPeers = priorPeers + peerId
            val updatedInner = inner.updated(key, newPeers)
            val updatedOuter = outer.updated(metagraphAddress, updatedInner)
            (updatedOuter, newPeers.size)
          }
        }

        def countFor(
          metagraphAddress: Address,
          snapshotOrd: Long,
          binaryHash: Hash
        ): F[Int] = {
          val key = BinaryKey(snapshotOrd, binaryHash)
          tallyRef.get.map { outer =>
            outer.get(metagraphAddress).flatMap(_.get(key)).fold(0)(_.size)
          }
        }

        def thresholdReached(
          metagraphAddress: Address,
          snapshotOrd: Long,
          binaryHash: Hash,
          kTarget: Int
        ): F[Boolean] = {
          val required = requiredCount(kTarget)
          countFor(metagraphAddress, snapshotOrd, binaryHash).map(_ >= required)
        }

        def pruneBelow(metagraphAddress: Address, keepFromOrd: Long): F[Unit] =
          tallyRef.update { outer =>
            outer.get(metagraphAddress) match {
              case None        => outer
              case Some(inner) =>
                val kept = inner.filter { case (k, _) => k.snapshotOrd >= keepFromOrd }
                if (kept.isEmpty) outer - metagraphAddress
                else outer.updated(metagraphAddress, kept)
            }
          }

        def size: F[Int] =
          tallyRef.get.map(_.valuesIterator.map(_.size).sum)
      }
    }
}
