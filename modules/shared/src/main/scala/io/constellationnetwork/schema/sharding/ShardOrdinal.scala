package io.constellationnetwork.schema.sharding

import io.constellationnetwork.ext.derevo.ordering

import derevo.cats.{order, show}
import derevo.derive
import io.circe._

/** Monotonic per-shard sequence number for the shard's mini-Taktikos chain.
  *
  * Each shard runs its own micro-chain producing one `ShardCheckpoint` per shard-ordinal
  * (`docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §5). The shard committee chain-links checkpoints via `parentCheckpointHash`;
  * the `shardOrdinal` is the chain height within the shard.
  *
  * '''Backing type: raw `Long`.''' The design doc (§3.1) specifies `value: Long`. Negative shard-ordinals aren't a domain concept, but the
  * arithmetic at the producer site (e.g. lookback into prior shard heights for fork-choice) can go negative transiently during
  * initialization, mirroring the rationale on [[io.constellationnetwork.schema.nakamoto.EtaPeriod]]. Modelling that as `NonNegLong` would
  * force partial semantics through every call site for the bootstrap edge case; keeping `Long` matches the existing convention.
  *
  * '''Why a newtype rather than a bare `Long`.''' Type safety against accidental cross-use with `SnapshotOrdinal` (gl0 height) or
  * `ChainTip.ordinal` (per-chain height) — different domains, different scopes, do not pun arithmetic across them.
  */
@derive(order, ordering, show)
case class ShardOrdinal(value: Long) {

  /** Increment by one. Used at the shard committee's chain-extension site (each new accepted checkpoint advances the shard ordinal by 1).
    */
  def next: ShardOrdinal = ShardOrdinal(value + 1L)
}

object ShardOrdinal {

  /** Genesis ordinal — the very first checkpoint a shard emits. Mirrors `SnapshotOrdinal.MinValue` for the gl0 chain. */
  val Genesis: ShardOrdinal = ShardOrdinal(0L)

  // ---- JSON value codecs ---------------------------------------------------
  // Manual rather than `@derive(encoder, decoder)` for the same rationale as `ShardId`: keep the JSON shape a plain number.

  implicit val encoder: Encoder[ShardOrdinal] = Encoder[Long].contramap(_.value)
  implicit val decoder: Decoder[ShardOrdinal] = Decoder[Long].map(ShardOrdinal(_))

  // ---- JSON key codecs -----------------------------------------------------
  // Not strictly needed by `ShardCheckpoint` (it carries `shardOrdinal` as a value, not a key), but if the shard chain ever materialises
  // as `SortedMap[ShardOrdinal, ShardCheckpoint]` for a per-shard read path, the key codec is already in place — costs ~6 lines and
  // matches the pattern set by `EpochProgress` / `EtaPeriod`.

  implicit val keyEncoder: KeyEncoder[ShardOrdinal] = KeyEncoder.instance(_.value.toString)
  implicit val keyDecoder: KeyDecoder[ShardOrdinal] =
    KeyDecoder.instance(s => scala.util.Try(s.toLong).toOption.map(ShardOrdinal(_)))

  // `Order`, `Show`, `scala.math.Ordering`, and `Eq` (from Order) all come from derevo's `order`/`ordering`/`show` derivations on the
  // case class itself. No additional declarations required.
}
