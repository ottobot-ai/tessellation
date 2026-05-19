package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.nakamoto.{EtaPeriod, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMap
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}

import scodec.codecs._
import scodec.{Attempt, Codec}

/** Canonical scodec codecs for §3 NIPoPoW stake-distribution types.
  *
  *   - `EtaPeriod` — int64. `EtaPeriod.value` can go negative during eligibility-query fall-through; the codec preserves sign.
  *   - `BigInt` — two's-complement variable-length bytes, length-prefixed with uint32. Matches `java.math.BigInteger.toByteArray` / `new
  *     BigInteger(bytes)` round-trip — the canonical wire form for arbitrary-precision integers. Stake sums in practice fit comfortably in
  *     16 bytes but the codec accepts anything we'd ever produce.
  *   - `StakeDistribution` — sortedMap(PeerId, BigInt). Determinism comes from the `SortedMap` insertion order plus PeerId's canonical
  *     `Order` instance.
  *
  * Used both by the GSI capstone codec ([[GlobalSnapshotInfoCodec]]) and by the MPT projection that authenticates the per-period partition
  * under [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.HistoricalStakeSnapshots]].
  */
object StakeDistributionCodec {

  implicit val etaPeriodCodec: Codec[EtaPeriod] =
    int64.xmap[EtaPeriod](EtaPeriod(_), _.value)

  implicit val bigIntCodec: Codec[BigInt] =
    variableSizeBytes(uint32.xmap[Int](_.toInt, _.toLong), bytes).exmap[BigInt](
      bv => Attempt.successful(BigInt(bv.toArray)),
      (bi: BigInt) => Attempt.successful(scodec.bits.ByteVector.view(bi.toByteArray))
    )

  implicit val stakesMapCodec: Codec[SortedMap[PeerId, BigInt]] =
    sortedMap(peerIdCodec, bigIntCodec)

  implicit val codec: Codec[StakeDistribution] =
    stakesMapCodec.xmap[StakeDistribution](StakeDistribution(_), _.stakes)

  implicit val immutableCodec: ImmutableCodec[StakeDistribution] = ImmutableCodec.fromScodecCodec(codec)
}
