package io.constellationnetwork.schema.sharding

import cats.Show
import cats.data.NonEmptyList

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.statechannel.StateChannelSnapshotBinary

import derevo.cats.eqv
import derevo.circe.magnolia.{decoder, encoder}
import derevo.derive

/** The per-shard checkpoint payload for one cycle. [[includedSnapshots]] is the only economic transition input: GL0 authenticates and
  * re-executes every included CL1 snapshot with the global currency transition function before adoption. The committee-provided root is an
  * equality claim checked against that local execution. No committee-produced diff, artifact list, balance delta, sync delta, or receipt
  * can authorize economic state.
  *
  * @param perMetagraphMptRoots
  *   committee claim for the per-MG MPT subtree root. GL0 recreates the included snapshots and accepts only an exact local-root match.
  * @param includedSnapshots
  *   per-MG signed state-channel binary chain. This is the input GL0 authenticates and re-executes before adoption.
  */
@derive(encoder, decoder, eqv)
final case class ShardDerivedStateDelta(
  perMetagraphMptRoots: SortedMap[Address, Hash],
  includedSnapshots: SortedMap[Address, NonEmptyList[Signed[StateChannelSnapshotBinary]]]
)

object ShardDerivedStateDelta {

  /** Empty value for construction and codec tests. A live checkpoint carrying it is rejected. */
  val empty: ShardDerivedStateDelta =
    ShardDerivedStateDelta(
      perMetagraphMptRoots = SortedMap.empty,
      includedSnapshots = SortedMap.empty
    )

  /** Manual `Show` instance to dodge the cats / `OrphanInstances.showSortedMapAsList` ambiguity that bites `@derive(show)` over any
    * `SortedMap[K, V]` inside the `io.constellationnetwork.schema` package object (see `StakeDistribution.show` for the same workaround).
    * `Show.fromToString` is fine — this is only used for diagnostic output, not consensus bytes.
    */
  implicit val show: Show[ShardDerivedStateDelta] = Show.fromToString
}
