package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.Order
import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.AcceptanceMpt
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.SpendAction
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.codecs.instances.CompatCodecs.byteArrayImmutableCodec

/** Local `scala.math.Ordering[GlobalStateKey]` (derived from its `cats.Order`) so the per-type nullifier markers can be carried in a
  * deterministic `SortedMap[GlobalStateKey, Array[Byte]]` (the write set fed to `mpt.insert`). `GlobalStateKey` derives `cats.Order` but no
  * `scala.math.Ordering`; `Order#toOrdering` bridges them without touching the schema.
  */
private[global] object CrossShardMessageOrderings {
  implicit val globalStateKeyOrdering: Ordering[GlobalStateKey] = Order[GlobalStateKey].toOrdering
}
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.CrossShardMessageOrderings._

/** The write side of one accepted cross-shard message's nullifier (spent-set) lifecycle for this ordinal — the output of
  * [[CrossShardMessageHandler.settle]].
  *
  * @param markers
  *   the new nullifier entries to write this ordinal, keyed by their FULL `GlobalStateKey` (the engine writes them through the same
  *   branch-aware overlay `mpt.insert` path the watchtower `Slashings` partition uses), value = the type's canonical immutable bytes
  *   (pre-encoded by the handler via its own codec, so the engine is value-type-agnostic — it stores the bytes verbatim via the
  *   `Array[Byte]` passthrough codec). One marker per ADMITTED cross-shard instance.
  * @param rejected
  *   the single-use identities (content hashes) REJECTED this ordinal — double-consume / replay (already in the nullifier set) or failed
  *   the type's include-check. Surfaced for logging / downstream effect handlers.
  *
  * '''Why key-typed (`GlobalStateKey`) rather than raw `Hex`.''' The branch-aware acceptance overlay exposes only a `GlobalStateKey`-keyed
  * `insert` (writing by raw `Hex` would bypass the overlay's branch handle and its MultiBranch pending-write semantics). The engine writes
  * the pre-encoded bytes through the `Array[Byte]` passthrough `ImmutableCodec` (`byteArrayImmutableCodec`, `ByteVector.view`), so the
  * stored bytes are byte-identical to a typed `insert[T]` with the type's real codec — uniform for every message type, with no per-type
  * codec in the engine. The engine derives each marker's `Hex` via `GlobalStateKey.toHex` for the #107 verify-replay fold.
  */
final case class CrossShardSettlementWrite(
  markers: SortedMap[GlobalStateKey, Array[Byte]],
  rejected: scala.collection.immutable.SortedSet[Hash]
)

object CrossShardSettlementWrite {
  val empty: CrossShardSettlementWrite =
    CrossShardSettlementWrite(SortedMap.empty[GlobalStateKey, Array[Byte]], scala.collection.immutable.SortedSet.empty[Hash])
}

/** A thin GENERIC seam for cross-shard message settlement at gl0 (`docs/nakamoto/ECONOMIC-TRUST-SHARDED-SECURITY-ARCHITECTURE.md` §5,
  * I-ONCE). gl0 is the shared sequencer: it sees every shard checkpoint in one total-ordered global snapshot, so it settles a cross-shard
  * message ATOMICALLY at its fold — verify the message's single-use identity is ABSENT from this type's nullifier set, write the nullifier,
  * and (type-specifically, OUTSIDE this seam) apply the message's effect.
  *
  * This trait owns ONLY the NULLIFIER lifecycle (the part that is uniform across every message type: classify cross-shard, reject
  * double-spend/replay, emit markers). Instance 1 is [[AllowSpendConsumeHandler]] over the `ConsumedAllowSpends` partition (fieldId 33). A
  * future type (cross-shard token-lock consume, cross-shard transfer, data-app message) adds a NEW handler over its OWN distinct
  * `nullifierFieldId` partition (never colliding) and registers it with [[CrossShardMessageEngine]]; the generic engine then writes every
  * registered type's markers through one uniform `mpt.insert` path and folds them all into the #107 verify-replay. The message's STATE
  * EFFECT stays type-specific and lives at that type's read/validation sites (for allow-spend: the read-side effective-CURRENCY-balance
  * overlay at the `SpendActionValidator`, NOT here) — this keeps the safety-critical inflation correction explicit and uncoupled from the
  * nullifier bookkeeping.
  *
  * '''Determinism / gating.''' `settle` iterates sorted collections and routes hashing through `Hasher[F]` (via `ShardAssignment`); the
  * engine is gated `numShards > 1` (at `numShards = 1` no instance is ever cross-shard, so every handler returns empty and no partition is
  * touched ⇒ the `mptRoot` is byte-identical to the pre-change path).
  */
trait CrossShardMessageHandler[F[_]] {

  /** This message type's own nullifier (spent-set) partition fieldId — distinct per type so two types' markers never collide. Allow-spend =
    * [[GlobalStateFieldId.ConsumedAllowSpends]] (33).
    */
  def nullifierFieldId: GlobalStateFieldId

  /** Self-contained nullifier lifecycle for this ordinal: materialize this type's prior nullifier set, classify the cross-shard instances
    * out of `acceptedSpendActions` (owner-shard ≠ producing-shard via `ShardAssignment`), REJECT any whose single-use identity is already
    * in the nullifier set OR fails the type's include-check, and return the markers to write (`GlobalStateKey` → canonical bytes) + the
    * rejected identities. Deterministic same-snapshot collision order (sorted by single-use identity ⇒ first admits, rest reject).
    */
  def settle(
    acceptedSpendActions: SortedMap[Address, List[SpendAction]],
    shardAssignment: ShardAssignment[F],
    ordinal: SnapshotOrdinal
  )(implicit hasher: Hasher[F]): F[CrossShardSettlementWrite]
}

/** Generic write engine over the registered [[CrossShardMessageHandler]]s. Runs each handler's `settle`, UNIONS their markers, writes them
  * through the ONE branch-aware `mpt.insert` path (the same path the watchtower `Slashings` partition uses), and returns the hex-keyed byte
  * view of every written marker so the GSAM accept path folds them into the #107 verify-replay `expectedBytes` (exactly as the directly
  * written `Slashings` + `ConsumedAllowSpends` entries are folded today). One uniform write path for every cross-shard message type.
  *
  * Caller gates this on `numShards > 1`; at `numShards = 1` it is never invoked (or every handler returns empty), so no nullifier partition
  * is written and the `mptRoot` stays byte-identical.
  */
object CrossShardMessageEngine {

  /** Output of [[settle]]: the unioned markers to write (across all registered handlers) and the union of rejected identities. Held by the
    * GSAM accept path between the settle point (after artifact validation) and the marker WRITE point (after `applyStateChanges`, alongside
    * the `Slashings` write).
    */
  final case class EngineResult(
    markers: SortedMap[GlobalStateKey, Array[Byte]],
    rejected: scala.collection.immutable.SortedSet[Hash]
  )

  object EngineResult {
    val empty: EngineResult =
      EngineResult(SortedMap.empty[GlobalStateKey, Array[Byte]], scala.collection.immutable.SortedSet.empty[Hash])
  }

  /** Settle every registered handler and UNION their markers + rejected sets. No write — the GSAM accept path writes the markers later
    * (after `applyStateChanges`) so they land in the same `postBytes` the proof's `mptRoot` is computed over, exactly like the watchtower
    * `Slashings` partition.
    */
  def settle[F[_]: Async](
    handlers: List[CrossShardMessageHandler[F]],
    acceptedSpendActions: SortedMap[Address, List[SpendAction]],
    shardAssignment: ShardAssignment[F],
    ordinal: SnapshotOrdinal
  )(implicit hasher: Hasher[F]): F[EngineResult] =
    handlers
      .traverse(_.settle(acceptedSpendActions, shardAssignment, ordinal))
      .map { writes =>
        // Partitions are distinct per type (distinct fieldId), so marker keys never collide across handlers; a deterministic fold is safe.
        val allMarkers: SortedMap[GlobalStateKey, Array[Byte]] =
          writes.foldLeft(SortedMap.empty[GlobalStateKey, Array[Byte]])((acc, w) => acc ++ w.markers)
        val allRejected: scala.collection.immutable.SortedSet[Hash] =
          writes.foldLeft(scala.collection.immutable.SortedSet.empty[Hash])((acc, w) => acc ++ w.rejected)
        EngineResult(allMarkers, allRejected)
      }

  /** Write the unioned markers through the ONE branch-aware `mpt.insert` path (the SAME path the watchtower `Slashings` partition uses),
    * and return the hex-keyed byte view of every written marker for the #107 verify-replay fold. Skips the write when there are no markers
    * (the common path / always at `numShards = 1`) ⇒ no partition touched ⇒ `mptRoot` byte-identical.
    */
  def write[F[_]: Async](
    markers: SortedMap[GlobalStateKey, Array[Byte]],
    mpt: AcceptanceMpt[F]
  )(implicit hasher: Hasher[F]): F[SortedMap[Hex, Array[Byte]]] =
    if (markers.isEmpty) SortedMap.empty[Hex, Array[Byte]].pure[F]
    else
      for {
        // Store the pre-encoded bytes through the `Array[Byte]` passthrough codec (byte-identical to a typed `insert[T]`).
        _ <- mpt.insert[Array[Byte]](markers.toMap)(byteArrayImmutableCodec)
        // Hex-keyed byte view for the #107 verify-replay fold (SAME `toHex` the writer used ⇒ identical bytes to `postBytes`).
        replay <- markers.toList.traverse {
          case (key, bytes) => GlobalStateKey.toHex[F](key).map(hex => hex -> bytes)
        }
      } yield SortedMap.from(replay)
}
