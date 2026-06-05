package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{Ref, Sync}
import cats.syntax.all._

import io.constellationnetwork.numerics.Ratio
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash

/** Tally of per-metagraph committee attestations (Slice S2.5).
  *
  * Inbound `MetagraphAttestation` messages from gl0 peers land here keyed by `(metagraphAddress, parentHash, binaryHash)`. Per key, the
  * aggregator holds the set of distinct peers that have attested. Slice S3 reads `thresholdReached` to gate inclusion of the binary into
  * the next global snapshot; for now (S2.5) the aggregator is observability-only — the gate call is added without re-shaping this
  * interface.
  *
  * '''Why `parentHash` not `snapshotOrd`.''' Two competing metagraph binaries on the same parent share the same VRF input (see
  * [[CommitteeSortition]]). Tallying by `(metagraph, parent, binary)` lets each binary accrue its own peer set independently; a committee
  * member that signs both is detectably equivocating (slashable, S4). Keying by `snapshotOrd` would lose that property — peers attesting
  * different binaries at the same ord could mask the equivocation as "different attestation streams".
  *
  * '''Verification responsibility is upstream of `record`.''' Callers MUST verify the attestation's committee VRF proof (via
  * `CommitteeSortition.verifyMembership`) AND its KES signature (Slice 9 path) BEFORE invoking `record`. This trait does not re-verify — it
  * counts. Bug-by-construction note: if callers skip verification, attackers could inflate counts.
  *
  * Eviction strategy: caller invokes `pruneParents(metagraphAddress, parents)` when a set of parent hashes have moved past the live
  * decision window (their children have either been finalized into gl0 or have rolled out of the in-flight horizon). Without pruning the
  * map grows monotonically.
  *
  * Concurrency: single `Ref` holds all per-binary tally state. Reads are lock-free; writes use `modify`/`update`. At thousands of
  * metagraphs × ~10 in-flight binaries per metagraph × ~K peers per binary, the map stays well under 1M entries — fine in a single Ref. If
  * we hit contention later, sharding by metagraph hash is a drop-in change.
  */
trait MetagraphAttestationAggregator[F[_]] {

  /** Record a verified inbound attestation. Returns the post-record count for the binary — callers can compare against threshold without a
    * second read. Idempotent: re-recording the same `(metagraphAddress, parentHash, binaryHash, peerId)` returns the same count.
    */
  def record(
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash,
    peerId: PeerId
  ): F[Int]

  /** Has this EXACT `(metagraphAddress, parentHash, binaryHash, peerId)` already been recorded? Lets the committee gate SKIP the expensive
    * Ed25519+KES+VRF re-verification of a gossip RE-DELIVERY of an already-verified attestation — the redundant verify was CPU-saturating
    * gl0 under multi-metagraph×sharding (load 156, finality grinding to >70s/snapshot, step-like progression). Safe: a NEW or forged
    * attestation for an un-recorded key is still fully verified (a forged sig fails verification; equivocation uses a different binaryHash
    * → different key → not deduped). Only a re-delivery of the SAME already-verified sender's attestation is short-circuited.
    */
  def alreadyRecorded(
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash,
    peerId: PeerId
  ): F[Boolean]

  /** Current count of distinct committee members that have attested this binary. */
  def countFor(
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash
  ): F[Int]

  /** Check whether the binary has reached `requiredQuorum` DISTINCT committee attestations. `requiredQuorum` is the admit-quorum count
    * DIRECTLY (the cluster-uniform `nakamoto.committee.kQuorum`), decoupled from the committee DRAW target (`kDraw`) — see the
    * draw/quorum-decouple note on [[CommitteeSortition]]. NOT a fraction of a target: the gate passes the exact count it waits for.
    * `requiredQuorum = 0` is rejected (would admit on zero attestations).
    */
  def thresholdReached(
    metagraphAddress: Address,
    parentHash: Hash,
    binaryHash: Hash,
    requiredQuorum: Int
  ): F[Boolean]

  /** Drop tally state for `metagraphAddress` entries whose parent hash is in `parents`. Called when those parents have moved past the live
    * decision window — no further attestations are load-bearing against them.
    */
  def pruneParents(metagraphAddress: Address, parents: Set[Hash]): F[Unit]

  /** Diagnostic: number of distinct `(metagraph, parent, binary)` keys currently tracked. */
  def size: F[Int]
}

object MetagraphAttestationAggregator {

  /** Quorum fraction parallel to `TipTracker.FinalityThreshold` (2/3 by default, env override). Same env knob to keep one finality surface:
    * equivocation in the committee threshold against chain finality would be a footgun if the two ever drifted out of sync.
    */
  val FinalityThreshold: Ratio = TipTracker.FinalityThreshold

  /** Compute the count required to reach `FinalityThreshold` of `kTarget`. `ceil(threshold · K)` computed in exact-Ratio so we stay
    * byte-identical across observers.
    *
    * '''Standalone helper after the draw/quorum decouple.''' [[thresholdReached]] no longer routes through this — the gate now passes the
    * cluster-uniform `nakamoto.committee.kQuorum` count DIRECTLY. This 2/3-of-K conversion remains exported for the parallel finality
    * semantics + its own tests; production no longer derives the metagraph admit quorum from it.
    */
  def requiredCount(kTarget: Int): Int = {
    require(kTarget > 0, s"kTarget must be positive, got $kTarget")
    val num = FinalityThreshold.numerator
    val den = FinalityThreshold.denominator
    val product = num * BigInt(kTarget)
    val (quot, rem) = (product / den, product % den)
    val ceil = if (rem == 0) quot else quot + 1
    ceil.toInt
  }

  /** Composite key for the per-binary tally state. Keeping `Address` separate (not folded into the hash) gives
    * `pruneParents(metagraphAddress, parents)` a cheap O(per-metagraph) prune via the outer-map filter rather than walking every entry.
    */
  private final case class BinaryKey(parentHash: Hash, binaryHash: Hash)

  def make[F[_]: Sync]: F[MetagraphAttestationAggregator[F]] =
    Ref.of[F, Map[Address, Map[BinaryKey, Set[PeerId]]]](Map.empty).map { tallyRef =>
      new MetagraphAttestationAggregator[F] {

        def record(
          metagraphAddress: Address,
          parentHash: Hash,
          binaryHash: Hash,
          peerId: PeerId
        ): F[Int] = {
          val key = BinaryKey(parentHash, binaryHash)
          tallyRef.modify { outer =>
            val inner = outer.getOrElse(metagraphAddress, Map.empty)
            val priorPeers = inner.getOrElse(key, Set.empty)
            val newPeers = priorPeers + peerId
            val updatedInner = inner.updated(key, newPeers)
            val updatedOuter = outer.updated(metagraphAddress, updatedInner)
            (updatedOuter, newPeers.size)
          }
        }

        // Local read-only contains: is this (mg, parent, binary, peer) already in the tally?
        // O(1) hash lookups; never serialized/hashed — pure local short-circuit for the gate.
        def alreadyRecorded(
          metagraphAddress: Address,
          parentHash: Hash,
          binaryHash: Hash,
          peerId: PeerId
        ): F[Boolean] =
          tallyRef.get.map { outer =>
            outer.get(metagraphAddress).flatMap(_.get(BinaryKey(parentHash, binaryHash))).exists(_.contains(peerId))
          }

        def countFor(
          metagraphAddress: Address,
          parentHash: Hash,
          binaryHash: Hash
        ): F[Int] = {
          val key = BinaryKey(parentHash, binaryHash)
          tallyRef.get.map { outer =>
            outer.get(metagraphAddress).flatMap(_.get(key)).fold(0)(_.size)
          }
        }

        def thresholdReached(
          metagraphAddress: Address,
          parentHash: Hash,
          binaryHash: Hash,
          requiredQuorum: Int
        ): F[Boolean] = {
          require(requiredQuorum > 0, s"requiredQuorum must be positive, got $requiredQuorum")
          countFor(metagraphAddress, parentHash, binaryHash).map(_ >= requiredQuorum)
        }

        def pruneParents(metagraphAddress: Address, parents: Set[Hash]): F[Unit] =
          if (parents.isEmpty) Sync[F].unit
          else
            tallyRef.update { outer =>
              outer.get(metagraphAddress) match {
                case None => outer
                case Some(inner) =>
                  val kept = inner.filterNot { case (k, _) => parents.contains(k.parentHash) }
                  if (kept.isEmpty) outer - metagraphAddress
                  else outer.updated(metagraphAddress, kept)
              }
            }

        def size: F[Int] =
          tallyRef.get.map(_.valuesIterator.map(_.size).sum)
      }
    }
}
