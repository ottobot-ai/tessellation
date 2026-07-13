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

/** Transitional per-shard checkpoint payload for one cycle. [[includedSnapshots]] is the only encoded economic transition input. The
  * current implementation makes every GL0 adopter authenticate and replay each included CL1 snapshot, then compare the claimed root. That
  * universal replay is transitional behavior, not the target execution-sharding contract.
  *
  * In the target contract the producer and every execution signer replay the complete ordered input; deterministic noncommittee watchtowers
  * provide positive replay coverage before GL0 inclusion. An ordinary noncommittee GL0 node verifies those identities and signatures,
  * applies the canonical namespace-confined byte diff to the exact Phase-2 base, and recomputes the root without replaying the CL1
  * transition. This schema does not yet carry that diff or complete exact-hash base and therefore cannot implement the target adoption
  * path. A root, artifact list, balance delta, sync delta, receipt, or signature count alone never authorizes economic state.
  *
  * @param perMetagraphMptRoots
  *   committee claim for each per-MG MPT subtree root. Under the current transitional path GL0 recreates the included snapshots and accepts
  *   only an exact local-root match; target adopters root-check the certified canonical diff
  * @param includedSnapshots
  *   complete ordered per-MG signed state-channel binary chain. Producer, execution signers, and watchtowers replay this input. Ordinary
  *   GL0 universal replay remains current transitional behavior until the canonical diff schema and adoption verifier land
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
