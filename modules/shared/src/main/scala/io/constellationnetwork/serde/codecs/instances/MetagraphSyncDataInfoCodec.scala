package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `MetagraphSyncDataInfo` — `(globalOrdinalLastAcceptedOn, globalEpochProgressLastAcceptedOn,
  * unappliedGlobalChangeOrdinals)`.
  *
  * Wire layout:
  *   - ordinal-last-accepted : SnapshotOrdinal (8 bytes)
  *   - epoch-last-accepted : EpochProgress (8 bytes)
  *   - unapplied-ordinals : SortedSet[SnapshotOrdinal] (uint16 count + 8 bytes each)
  */
object MetagraphSyncDataInfoCodec {

  private val ordinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]
  private val unappliedCodec: Codec[SortedSet[SnapshotOrdinal]] = sortedSet(ordinalCodec)

  implicit val codec: Codec[MetagraphSyncDataInfo] =
    (ordinalCodec :: epochCodec :: unappliedCodec)
      .xmap[MetagraphSyncDataInfo](
        { case o :: e :: u :: HNil => MetagraphSyncDataInfo(o, e, u) },
        i => i.globalOrdinalLastAcceptedOn :: i.globalEpochProgressLastAcceptedOn :: i.unappliedGlobalChangeOrdinals :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[MetagraphSyncDataInfo] = ImmutableCodec.fromScodecCodec(codec)
}
