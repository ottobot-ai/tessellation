package io.constellationnetwork.serde.codecs.instances

import scala.collection.immutable.SortedMap

import io.constellationnetwork.schema.nakamoto.{EtaPeriod, HistoricalStakeSnapshot, StakeDistribution}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.SortedMapCodec.sortedMapCanonical
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}

import scodec.codecs._
import scodec.{Attempt, Codec}

/** Canonical scodec codecs for §3 NIPoPoW stake-distribution types.
  *
  *   - `EtaPeriod` — int64. `EtaPeriod.value` can go negative during eligibility-query fall-through; the codec preserves sign.
  *   - `BigInt` — two's-complement variable-length bytes, length-prefixed with uint32. Matches `java.math.BigInteger.toByteArray` / `new
  *     BigInteger(bytes)` round-trip — the canonical wire form for arbitrary-precision integers. Stake sums in practice fit comfortably in
  *     16 bytes but the codec accepts anything we'd ever produce.
  *   - `StakeDistribution` — sortedMap(PeerId, BigInt). The encode path canonicalizes each PeerId through its wire codec once before
  *     sorting, so mixed-case source hex cannot disagree with lowercase decoded order.
  *   - `HistoricalStakeSnapshot` — pair-codec `(StakeDistribution, Hash)`. The stake half rides the same `stakeDistributionCodec`; the eta
  *     half rides `HashCodec` (fixed-width 32 bytes). Order: stakes first, then eta — chosen to keep the GSI / MPT byte-prefix of an
  *     extended entry stable with the legacy stake-only encoding for the bytes that overlap.
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
    sortedMapCanonical(peerIdCodec, bigIntCodec)

  implicit val codec: Codec[StakeDistribution] =
    stakesMapCodec.xmap[StakeDistribution](StakeDistribution(_), _.stakes)

  implicit val immutableCodec: ImmutableCodec[StakeDistribution] = ImmutableCodec.fromScodecCodec(codec)

  /** Pair codec for the combined stake + eta record stored under
    * [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.HistoricalStakeSnapshots]]. Bound here (next to the `StakeDistribution` codec
    * it composes with) so the per-period partition writer and the GSI capstone share a single source.
    */
  implicit val historicalCodec: Codec[HistoricalStakeSnapshot] = {
    val _ = (codec, hashCodec) // bind context-bound implicits for the tuple below
    (codec :: hashCodec)
      .as[HistoricalStakeSnapshot]
  }

  implicit val historicalImmutableCodec: ImmutableCodec[HistoricalStakeSnapshot] =
    ImmutableCodec.fromScodecCodec(historicalCodec)

  /** Codec for the eta scalar by itself. The boundary writer in GSAM also lands a per-period eta-only entry under
    * [[io.constellationnetwork.schema.mpt.GlobalStateFieldId.HistoricalStakeSnapshots]] when the stake side is unchanged from the previous
    * period — not used today but kept exported so other call sites can decode the eta half independently if they already hold `Hash` in
    * their consumer schema.
    */
  implicit val historicalEtaCodec: Codec[Hash] = hashCodec

  /** Bind so `_ = stakeDistributionImmutable` style imports keep resolving even when callers only need the pair codec. */
  val _stakeDistributionImmutable: ImmutableCodec[StakeDistribution] = immutableCodec
  locally { val _ = _stakeDistributionImmutable }
}
