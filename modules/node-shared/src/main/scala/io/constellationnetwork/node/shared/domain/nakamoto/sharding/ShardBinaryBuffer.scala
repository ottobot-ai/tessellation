package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.NonEmptyList
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

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
  * window across successive gl0 ords does not double-include. Pruning of finalized binaries is a later slice (on finalize); v1 relies on the
  * cap below to bound growth.
  *
  * '''Bounded (HOCON, not sys.env)''' per `[[feedback-prefer-hocon-over-sysenv]]`: the per-shard cap comes from
  * `cfg.nakamoto.sharding.checkpoint.binaryBufferCap` (typed [[io.constellationnetwork.node.shared.config.types.ShardCheckpointConfig]]),
  * threaded by the wiring. At cap, NEW distinct binaries are rejected (logged + dropped) rather than evicting older ones — evicting an
  * older binary could drop a chain-link parent the producer still needs, so reject-new is the chain-safe overflow policy.
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
    * a NEW distinct binary is rejected (logged + dropped) — see the overflow-policy note on the object scaladoc.
    */
  def bufferBinary(mgAddr: Address, b: Signed[StateChannelSnapshotBinary]): F[Unit]

  /** Non-destructive snapshot of the currently-buffered binaries, grouped per metagraph in insertion order. MGs with no buffered binary are
    * absent. The per-MG list is "as received" — the producer chain-link-orders it (R-2) before deriving state.
    */
  def snapshotPending: F[SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]]
}

object ShardBinaryBuffer {

  /** One buffered binary: its canonical hash (dedup key) + the envelope. Insertion order is preserved by the surrounding `Vector`. */
  private final case class Entry(hash: Hash, binary: Signed[StateChannelSnapshotBinary])

  /** In-memory state: per-MG insertion-ordered entries, plus a flat set of all buffered hashes for O(1) dedup across the shard. */
  private final case class BufferState(
    perMg: SortedMap[Address, Vector[Entry]],
    knownHashes: Set[Hash]
  ) {
    def total: Int = knownHashes.size
  }

  private object BufferState {
    def empty: BufferState =
      BufferState(SortedMap.empty[Address, Vector[Entry]](Address.OrderingInstance), Set.empty)
  }

  /** Construct a per-shard binary buffer.
    *
    * @param shardId
    *   which shard this buffer is scoped to. Immutable; one instance per shard the operator tracks (held in
    *   [[io.constellationnetwork.node.shared.infrastructure.sharding.ShardCheckpointWiring.ShardRegistryEntry]] so intake + producer share
    *   the SAME instance).
    * @param cap
    *   max number of distinct binaries buffered for this shard at once (from
    *   `cfg.nakamoto.sharding.checkpoint.binaryBufferCap`). At cap, new distinct binaries are rejected (chain-safe overflow).
    */
  def make[F[_]: Async: Hasher](shardId: ShardId, cap: Int): F[ShardBinaryBuffer[F]] = {
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
              } else if (state.total >= cap) {
                // Chain-safe overflow: reject the NEW binary rather than evicting an older one (which could drop a
                // chain-link parent the producer still needs). Surfaced as a warn so a wedged shard is visible.
                (
                  state,
                  logger.warn(
                    s"bufferBinary: shard buffer at cap=$cap (have=${state.total}); rejecting new binary " +
                      s"hash=${h.value.take(12)} mg=${mgAddr.show}"
                  )
                )
              } else {
                val existing = state.perMg.getOrElse(mgAddr, Vector.empty)
                val updatedPerMg = state.perMg.updated(mgAddr, existing :+ Entry(h, b))
                val newState = BufferState(updatedPerMg, state.knownHashes + h)
                (
                  newState,
                  logger.debug(
                    s"bufferBinary: buffered hash=${h.value.take(12)} mg=${mgAddr.show} " +
                      s"(mgCount=${existing.size + 1} shardTotal=${newState.total})"
                  )
                )
              }
            }.flatten
          }

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
