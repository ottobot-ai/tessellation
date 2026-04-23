package io.constellationnetwork.schema.mpt

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.epoch.EpochProgress

/** Delta applied to a system-namespaced MPT partition during `syncFromStateChanges`. The ADT leaves room for other system-index shapes
  * (counters, reverse-lookups, ...) without disturbing existing variants.
  */
sealed trait SystemIndexDelta[K]

object SystemIndexDelta {

  /** Adds and removes applied to epoch-bucketed `Set[K]` values. Empty buckets after the adds/removes are garbage-collected by the sync
    * helper.
    */
  case class EpochBucket[K](
    adds: SortedMap[EpochProgress, Set[K]] = SortedMap.empty[EpochProgress, Set[K]],
    removes: SortedMap[EpochProgress, Set[K]] = SortedMap.empty[EpochProgress, Set[K]]
  ) extends SystemIndexDelta[K] {

    /** Union of `adds` and `removes`, used by the sync helper to know which epoch buckets must be touched. */
    def touchedEpochs: Set[EpochProgress] = adds.keySet ++ removes.keySet

    /** Has no effect if both maps are empty. */
    def isEmpty: Boolean = adds.isEmpty && removes.isEmpty
  }

  def empty[K]: SystemIndexDelta[K] = EpochBucket[K]()
}
