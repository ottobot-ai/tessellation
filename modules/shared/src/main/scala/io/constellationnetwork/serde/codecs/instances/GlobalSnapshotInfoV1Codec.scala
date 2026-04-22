package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.GlobalSnapshotInfoV1
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{codec => transactionReferenceCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for `GlobalSnapshotInfoV1` — the legacy 3-field info record.
  *
  * Wire layout (three sorted maps in declaration order, each prefixed with a uint16 entry count):
  *   - lastStateChannelSnapshotHashes : SortedMap[Address, Hash]
  *   - lastTxRefs                      : SortedMap[Address, TransactionReference]
  *   - balances                        : SortedMap[Address, Balance]
  *
  * V1 is frozen — never mutated. Historical V1 bytes on disk decode through this codec. The
  * current `GlobalSnapshotInfo` (17 fields) is a separate codec in a follow-up commit.
  */
object GlobalSnapshotInfoV1Codec {

  private val stateChannelHashesCodec: Codec[SortedMap[Address, Hash]] =
    sortedMap(addressCodec, hashCodec)

  private val lastTxRefsCodec: Codec[SortedMap[Address, TransactionReference]] =
    sortedMap(addressCodec, transactionReferenceCodec)

  private val balancesCodec: Codec[SortedMap[Address, Balance]] =
    sortedMap(addressCodec, Codec[Balance])

  implicit val codec: Codec[GlobalSnapshotInfoV1] =
    (stateChannelHashesCodec :: lastTxRefsCodec :: balancesCodec)
      .xmap[GlobalSnapshotInfoV1](
        { case sch :: tx :: bal :: HNil => GlobalSnapshotInfoV1(sch, tx, bal) },
        i => i.lastStateChannelSnapshotHashes :: i.lastTxRefs :: i.balances :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[GlobalSnapshotInfoV1] =
    ImmutableCodec.fromScodecCodec(codec)
}
