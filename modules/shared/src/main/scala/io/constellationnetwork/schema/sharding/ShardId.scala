package io.constellationnetwork.schema.sharding

import io.constellationnetwork.ext.derevo.ordering
import io.constellationnetwork.schema._

import derevo.cats.{order, show}
import derevo.derive
import eu.timepit.refined.api.Refined
import eu.timepit.refined.cats._
import eu.timepit.refined.numeric.NonNegative
import eu.timepit.refined.types.numeric.NonNegInt
import io.circe._

/** Identifier for a static shard.
  *
  * Range: `[0, M)` where `M = numShards` is the cluster-wide constant from `nakamoto.sharding.num-shards` (see
  * `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §4.2).
  *
  * Backing type is `NonNegInt` (refined): the design-doc bound is non-negative, and Int is sufficient because cluster-wide `M` is expected
  * to be in the 4..1000 range (test/laptop = 4; production target ≈ 100; see §4.1). Using `Int` rather than `Long` keeps the wire bytes
  * short and matches the `mod numShards` arithmetic at the assignment site.
  *
  * '''Why a refined newtype, not a raw Int.''' Matches the prevailing pattern in this package (e.g., [[io.constellationnetwork.schema.SnapshotOrdinal]]
  * wraps a `NonNegLong` for the same reason): a stray negative literal becomes a compile-time refinement failure rather than a runtime
  * surprise inside `SortedMap[ShardId, _]` ordering, MPT-key construction, or sortition arithmetic. The cost is one allocation per
  * construction; the benefit is that every downstream call site can assume non-negativity without re-checking.
  */
@derive(order, ordering, show)
case class ShardId(value: NonNegInt)

object ShardId {

  /** Convenience constructor that performs the refinement check. Returns `None` for negative inputs. */
  def apply(value: Int): Option[ShardId] =
    NonNegInt.from(value).toOption.map(new ShardId(_))

  /** Unchecked constructor for known-valid call sites (e.g., test fixtures, sortition results already gated through `mod`). */
  def unsafeApply(value: Int): ShardId =
    ShardId(Refined.unsafeApply(value))

  // ---- JSON value codecs ---------------------------------------------------
  // Implemented manually rather than via `@derive(decoder, encoder)` to bypass the magnolia derivation; the canonical wire shape is the
  // underlying Int (NonNegInt at the boundary, but the JSON is just a number). This keeps a `ShardId` JSON value indistinguishable from
  // an `Int` for human inspection — matches the convention used by other numeric-newtype IDs in this package
  // (e.g. `SnapshotOrdinal` decoded straight from `NonNegLong`).

  implicit val encoder: Encoder[ShardId] = Encoder[NonNegInt].contramap(_.value)
  implicit val decoder: Decoder[ShardId] = Decoder[NonNegInt].map(ShardId(_))

  // ---- JSON key codecs -----------------------------------------------------
  // SortedMap[ShardId, Signed[ShardCheckpoint]] is the headline use site (per `GlobalIncrementalSnapshot.shardCheckpoints` evolution
  // in §3.4). Circe needs a KeyEncoder/KeyDecoder for map-key serialization — we render the underlying Int as a plain decimal string so
  // the JSON shape is `{"0": ..., "1": ...}` not `{"ShardId(0)": ...}`.

  implicit val keyEncoder: KeyEncoder[ShardId] = KeyEncoder.instance(id => id.value.value.toString)
  implicit val keyDecoder: KeyDecoder[ShardId] =
    KeyDecoder.instance { s =>
      for {
        i <- scala.util.Try(s.toInt).toOption
        n <- NonNegInt.from(i).toOption
      } yield ShardId(n)
    }

  // ---- Cats instances ------------------------------------------------------
  // `Eq` comes from `@derive(order)` (Order extends Eq). `Show` comes from `@derive(show)`. `scala.math.Ordering` comes from
  // derevo's `ordering` annotation (see `io.constellationnetwork.ext.derevo.ordering`) — needed to instantiate
  // `SortedMap[ShardId, _]` at call sites. No additional declarations required.

  // ---- Refinement aliases --------------------------------------------------
  // `Refined[Int, NonNegative]` is the actual underlying type — alias here for code that needs to talk about it without dragging in the
  // full refined-types vocabulary at the call site.
  type Refinement = Refined[Int, NonNegative]
}
