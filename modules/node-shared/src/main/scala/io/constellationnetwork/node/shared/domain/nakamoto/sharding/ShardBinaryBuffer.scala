package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.infrastructure.metrics.Metrics
import io.constellationnetwork.node.shared.infrastructure.sharding.ShardMetrics
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Per-shard raw-binary accumulator — the intake side of the execution-sharding inversion
  * (`docs/nakamoto/EXECUTION-SHARDING-COMMITTEE-VERIFY-DESIGN.md` §2/§6.1, R-1).
  *
  * '''Why this exists (the inversion).''' Before R-1 the shard checkpoint producer read its input from gl0's own post-chain-link committed
  * `signed.value.stateChannelSnapshots` map. With the `CHANGE-3` Axis-1a filter that map is empty at `numShards > 1`, so the producer
  * starved and no checkpoint could ever be produced (circular starvation), AND it inherited gl0's #259 chain-link freeze. The fix is to
  * invert the producer's input: the shard committee buffers the RAW metagraph binaries for ITS shard here, and the producer
  * ([[ShardCheckpointProducer]]) reads from this buffer + chain-links them off the shard's own prior-checkpoint tip
  * ([[ShardChainStore.perMgTip]]) — fully decoupled from gl0's chain-link admission.
  *
  * '''Determinism model — leader-proposes / members-attest (Polkadot backing-group).''' This buffer is intentionally NODE-LOCAL and is NOT
  * required to converge across committee members. Only the shard's SLOT LEADER builds a checkpoint from ITS OWN buffer; the other committee
  * members do NOT re-select from their own buffers — they receive the leader's gossiped `Signed[ShardCheckpoint]` and attest it (the
  * existing slice-14 path). So there is no cross-node buffer-convergence requirement and no firstSeen / pull-delay multi-proposer machinery
  * here. The only determinism requirement is downstream: the leader's `includedSnapshots` must be a correctly chain-ordered per-MG list
  * (the producer's `chainLinkOrder`), which gl0's deterministic `verifyEmbedded` then accepts.
  *
  * '''Non-destructive read.''' [[snapshotPending]] is a read, not a take — it does not clear the buffer. Re-buffering the same binary is an
  * idempotent no-op (dedup by binary hash), and `ShardChainStore.store` is idempotent by checkpoint hash, so re-reading a still-pending
  * window across successive gl0 ords does not double-include.
  *
  * '''Finalize-keyed pruning (S3, `docs/nakamoto/SHARDED-CURRENCY-MIRROR-ENDGAME-PLAN.md` §S3).''' [[pruneFinalized]] drops the STRICT
  * ANCESTORS of each MG's per-MG finalize floor. The floor MUST be the gl0 DEPTH-K-FINALIZED per-MG SC tip (the finalized global snapshot's
  * `lastStateChannelSnapshotHashes`, read at the gl0 `onFinalize` / on-disk-base-advance seam) — NEVER the adopted-but-unfinalized tip.
  * (`lastAdoptedAnchor` is the node-local ADOPTED watermark set on the accept/proposal path — reorg-able; sound as a fork-choice BIAS but
  * NOT as an irreversible prune floor, since a reorg can un-adopt it.) The floor binary and every binary AT or ABOVE it are retained: those
  * are S2's base->latest re-inclusion set (adopted-but-not-yet-finalized binaries an optimistic reorg may still need to rebuild the window)
  * plus any fork siblings. Pruning strictly below an UN-finalized tip would be unsafe — a reorg could drop the adopted binary, and if it
  * had been pruned it could not be re-included. The caller passes `MIN(finalizedTip, bestTip)` per MG (the §5 reorg-safety guard) so a
  * backward `noteAnchor` reorg that pushes bestTip below the finalized tip never drops a binary the post-reorg window needs. Idempotent.
  *
  * '''Live drive deferred to S2.''' This slice ships the prune CONTRACT only (this API + at-cap reclaim + the occupancy gauge + the
  * `ShardBinaryBufferSuite` cases). The live call is wired in S2, keyed on the SAME depth-k finalized-base per-MG tip S2 establishes as the
  * producer window anchor — one source of truth for both the window and the prune. Until then `pruneFinalized` is exercised only by the
  * suite; `retainedFloor` stays empty in production, so the at-cap path behaves exactly as the prior reject-new (plus the additive overflow
  * counter). An earlier S3 draft drove it off `lastAdoptedAnchor` (the adopted watermark) — reverted as unsafe per the §S3 correction.
  *
  * '''Bounded (HOCON, not sys.env)''' per `[[feedback-prefer-hocon-over-sysenv]]`: the per-shard cap comes from
  * `cfg.nakamoto.sharding.checkpoint.binaryBufferCap` (typed [[io.constellationnetwork.node.shared.config.types.ShardCheckpointConfig]]),
  * threaded by the wiring. At cap, [[bufferBinary]] FIRST reclaims by pruning strict ancestors of the retained per-MG finalize floor (the
  * same walk as [[pruneFinalized]]) — only finalized binaries are ever evicted, so a chain-link parent the producer still needs is never
  * dropped. If no finalized binary exists to evict (the buffer is full of UN-finalized binaries = genuine overload), the new binary is
  * rejected AND `dag_nakamoto_shard_buffer_overflow_total{shard_id}` is incremented (a loud counter, not just a warn).
  *
  * '''Occupancy gauge.''' `dag_nakamoto_shard_buffer_size{shard_id}` is emitted from `BufferState` on every successful [[bufferBinary]] and
  * every [[pruneFinalized]], so the S2/S3 e2e can read the slow-freeze signal (the gauge plateaus once pruning bounds growth).
  *
  * '''Use Hasher rule''' (`[[feedback-use-hasher-no-manual-serialize]]`): dedup keys are `Hasher[F]` over the `Signed[…]` envelope (its
  * canonical `toHashed.hash`), the same binary hash the chain-link admission compares against — never a hand-rolled byte digest.
  */
trait ShardBinaryBuffer[F[_]] {

  /** Which shard this buffer is scoped to. Set at construction; immutable. */
  def shardId: ShardId

  /** Buffer a received raw metagraph binary for the in-shard metagraph `mgAddr`.
    *
    * Idempotent: a binary whose `Hasher[F]` hash is already buffered (for any MG in this shard) is dropped silently. At the per-shard cap,
    * the buffer FIRST reclaims by pruning strict ancestors of the retained per-MG finalize floor; if that frees a slot the new binary is
    * admitted, otherwise (buffer full of un-finalized binaries) the new binary is rejected and the overflow counter fires — see the
    * overflow-policy note on the object scaladoc.
    */
  def bufferBinary(mgAddr: Address, b: Signed[StateChannelSnapshotBinary]): F[Unit]

  /** Finalize-keyed prune (S3). For each `(mg, floorHash)`, drop the STRICT ANCESTORS of `floorHash` from `mg`'s buffered window (walking
    * down the `lastSnapshotHash` parent chain through the buffered binaries), keeping `floorHash`'s binary and every non-ancestor (the
    * descendants = S2's adopted-but-unfinalized re-inclusion set, plus fork siblings).
    *
    * The floor MUST be the gl0 DEPTH-K-FINALIZED per-MG SC tip (the finalized snapshot's `lastStateChannelSnapshotHashes`; the caller
    * passes `MIN(finalizedTip, bestTip)` per the §5 reorg-safety guard) — never the adopted (`lastAdoptedAnchor`) watermark, which is
    * reorg-able. If `floorHash` is not a buffered binary for `mg`, this is a no-op for that MG (conservative; never over-drops).
    * Idempotent: re-running with the same floor drops nothing new. The most-recent floor is retained in `BufferState` and reused by the
    * at-cap reclaim in [[bufferBinary]]. Emits the occupancy gauge after the mutation.
    */
  def pruneFinalized(perMgPruneFloor: SortedMap[Address, Hash]): F[Unit]

  /** Non-destructive snapshot of the currently-buffered binaries, grouped per metagraph in insertion order. MGs with no buffered binary are
    * absent. The per-MG list is "as received" — the producer chain-link-orders it (R-2) before deriving state.
    */
  def snapshotPending: F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]]
}

object ShardBinaryBuffer {

  /** One buffered binary: its canonical hash (dedup key) + the envelope. Insertion order is preserved by the surrounding `Vector`. */
  private final case class Entry(hash: Hash, binary: Signed[StateChannelSnapshotBinary])

  /** In-memory state: per-MG insertion-ordered entries, a flat set of all buffered hashes for O(1) dedup across the shard, and the
    * most-recent per-MG finalize floor (`MIN(finalizedTip, bestTip)`) seen by [[pruneFinalized]] — reused by the at-cap reclaim so a cap
    * hit can evict strict ancestors of the retained floor without the caller re-supplying it.
    */
  private final case class BufferState(
    perMg: SortedMap[Address, Vector[Entry]],
    knownHashes: Set[Hash],
    retainedFloor: SortedMap[Address, Hash]
  ) {
    def total: Int = knownHashes.size
  }

  private object BufferState {
    def empty: BufferState =
      BufferState(
        SortedMap.empty[Address, Vector[Entry]](Address.OrderingInstance),
        Set.empty,
        SortedMap.empty[Address, Hash](Address.OrderingInstance)
      )
  }

  /** Pure prune: drop, for each `(mg, floorHash)` in `floor`, the STRICT ANCESTORS of `floorHash` from `mg`'s buffered window.
    *
    * For each MG: build `byHash` over its entries; if `floorHash` is not buffered ⇒ leave that MG untouched (conservative). Else walk DOWN
    * the `lastSnapshotHash` parent chain from `floorHash`, collecting every parent that is itself buffered (strict ancestors only — the
    * floor binary is kept). Drop those hashes from the MG's vector and from `knownHashes`; drop the MG key entirely if its vector empties.
    * MGs absent from `floor` are untouched. Non-ancestors (descendants of the floor, fork siblings) are always retained. Idempotent: once
    * the ancestors are gone, the walk finds nothing buffered below the floor.
    */
  private def pruneAncestorsBelow(state: BufferState, floor: SortedMap[Address, Hash]): BufferState = {
    val (prunedPerMg, allDropped) =
      state.perMg.foldLeft((SortedMap.empty[Address, Vector[Entry]](Address.OrderingInstance), Set.empty[Hash])) {
        case ((accPerMg, accDropped), (mg, entries)) =>
          floor.get(mg) match {
            case Some(floorHash) if entries.exists(_.hash === floorHash) =>
              val byHash = entries.iterator.map(e => e.hash -> e).toMap
              val toDrop = scala.collection.mutable.Set.empty[Hash]
              var parent = byHash(floorHash).binary.value.lastSnapshotHash
              while (byHash.contains(parent)) {
                toDrop += parent
                parent = byHash(parent).binary.value.lastSnapshotHash
              }
              val kept = entries.filterNot(e => toDrop.contains(e.hash))
              val nextPerMg = if (kept.isEmpty) accPerMg else accPerMg.updated(mg, kept)
              (nextPerMg, accDropped ++ toDrop.toSet)
            case _ =>
              // floorHash not buffered for this MG (or MG absent from floor) ⇒ retain the whole window untouched.
              (accPerMg.updated(mg, entries), accDropped)
          }
      }
    state.copy(perMg = prunedPerMg, knownHashes = state.knownHashes -- allDropped)
  }

  /** Construct a per-shard binary buffer.
    *
    * @param shardId
    *   which shard this buffer is scoped to. Immutable; one instance per shard the operator tracks (held in
    *   [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.ShardRegistryEntry]] so intake + producer share
    *   the SAME instance).
    * @param cap
    *   max number of distinct binaries buffered for this shard at once (from `cfg.nakamoto.sharding.checkpoint.binaryBufferCap`). At cap,
    *   the buffer reclaims by pruning strict ancestors of the retained per-MG finalize floor (evict-finalized-first); only when the buffer
    *   is full of UN-finalized binaries is a new binary rejected (loud overflow counter).
    */
  def make[F[_]: Async: Hasher: Metrics](shardId: ShardId, cap: Int): F[ShardBinaryBuffer[F]] = {
    val outerShardId = shardId
    val logger = Slf4jLogger.getLoggerFromName[F](s"ShardBinaryBuffer[$shardId]")

    Ref.of[F, BufferState](BufferState.empty).map { stateRef =>
      new ShardBinaryBuffer[F] {

        val shardId: ShardId = outerShardId

        def bufferBinary(mgAddr: Address, b: Signed[StateChannelSnapshotBinary]): F[Unit] =
          b.toHashed[F].flatMap { hashed =>
            val h = hashed.hash
            stateRef.modify { state =>
              if (state.knownHashes.contains(h)) {
                // Idempotent: already buffered (possibly under the same MG). No-op.
                (state, logger.debug(s"bufferBinary: duplicate hash=${h.value.take(12)} mg=${mgAddr.show}; ignoring"))
              } else {
                // At cap, FIRST reclaim by pruning strict ancestors of the RETAINED per-MG finalize floor (evict-finalized-first).
                // Only finalized binaries are ever evicted, so a chain-link parent the producer still needs is never dropped.
                val reclaimed = if (state.total >= cap) pruneAncestorsBelow(state, state.retainedFloor) else state
                if (reclaimed.total >= cap) {
                  // No finalized binary to evict ⇒ the buffer is full of UN-finalized binaries (genuine overload). Reject the
                  // new binary and fire the LOUD counter (prom-metrics-not-logs SOP), not just a warn.
                  (
                    reclaimed,
                    ShardMetrics.incrementBufferOverflow[F](outerShardId) >>
                      logger.warn(
                        s"bufferBinary: shard buffer at cap=$cap with no finalized binary to evict (have=${reclaimed.total}); " +
                          s"rejecting new binary hash=${h.value.take(12)} mg=${mgAddr.show} (overflow counter incremented)"
                      )
                  )
                } else {
                  val existing = reclaimed.perMg.getOrElse(mgAddr, Vector.empty)
                  val updatedPerMg = reclaimed.perMg.updated(mgAddr, existing :+ Entry(h, b))
                  val newState = reclaimed.copy(perMg = updatedPerMg, knownHashes = reclaimed.knownHashes + h)
                  (
                    newState,
                    ShardMetrics.setBufferSize[F](outerShardId, newState.total) >>
                      logger.debug(
                        s"bufferBinary: buffered hash=${h.value.take(12)} mg=${mgAddr.show} " +
                          s"(mgCount=${existing.size + 1} shardTotal=${newState.total})"
                      )
                  )
                }
              }
            }.flatten
          }

        def pruneFinalized(perMgPruneFloor: SortedMap[Address, Hash]): F[Unit] =
          stateRef.modify { state =>
            val pruned = pruneAncestorsBelow(state, perMgPruneFloor)
            val dropped = state.total - pruned.total
            // Persist the most-recent floor so the at-cap reclaim in `bufferBinary` can evict against it without the caller re-supplying.
            val nextState = pruned.copy(retainedFloor = perMgPruneFloor)
            (
              nextState,
              ShardMetrics.setBufferSize[F](outerShardId, nextState.total) >>
                Async[F].whenA(dropped > 0)(
                  logger.debug(
                    s"pruneFinalized: dropped=$dropped strict-ancestor binaries below the per-MG finalize floor " +
                      s"(floorMgs=${perMgPruneFloor.size} shardTotal=${nextState.total})"
                  )
                )
            )
          }.flatten

        def snapshotPending: F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]] =
          stateRef.get.map { state =>
            val pairs = state.perMg.toList.flatMap {
              case (mg, entries) =>
                NonEmptyList.fromList(entries.map(_.binary).toList).map(mg -> _)
            }
            SortedMap.from(pairs)(Address.OrderingInstance)
          }
      }
    }
  }
}
