package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.node._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.StringCodec.{codec => stringCodec}
import io.constellationnetwork.serde.codecs.instances.AddressCodec.{codec => addressCodec}
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.codecs.int32
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Canonical scodec codecs for the `UpdateNodeParameters` family:
  *   - `RewardFraction`                 — Int Refined Closed[0, 100_000_000], 4 bytes with range validation.
  *   - `UpdateNodeParametersReference`  — 40 bytes (ordinal + hash), same layout as other *Reference types.
  *   - `DelegatedStakeRewardParameters` — single-field wrapper.
  *   - `NodeMetadataParameters`         — `(name: String, description: String)`.
  *   - `UpdateNodeParameters`           — 4-field record.
  *
  * `RewardFraction` is the first consensus-field refined type with a bounded-range predicate (not
  * just non-negative / positive). Decode validates the `[0, 100_000_000]` range — a byte stream
  * with a value outside that range fails at the codec layer, not at a later consistency check.
  */
object UpdateNodeParametersCodec {

  // ---- RewardFraction ------------------------------------------------------

  implicit val rewardFractionCodec: Codec[RewardFraction] =
    int32.exmap(
      i =>
        RewardFraction
          .from(i)
          .fold(err => Attempt.failure(Err(s"RewardFraction decode: $err")), a => Attempt.successful(a)),
      (rf: RewardFraction) => Attempt.successful(rf.value)
    )

  implicit val rewardFractionImmutableCodec: ImmutableCodec[RewardFraction] =
    ImmutableCodec.fromScodecCodec(rewardFractionCodec)

  // ---- UpdateNodeParametersReference --------------------------------------

  private val ordinalCodec: Codec[UpdateNodeParametersOrdinal] = Codec[UpdateNodeParametersOrdinal]

  implicit val updateNodeParametersReferenceCodec: Codec[UpdateNodeParametersReference] =
    (ordinalCodec :: hashCodec)
      .xmap[UpdateNodeParametersReference](
        { case o :: h :: HNil => UpdateNodeParametersReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val updateNodeParametersReferenceImmutableCodec: ImmutableCodec[UpdateNodeParametersReference] =
    ImmutableCodec.fromScodecCodec(updateNodeParametersReferenceCodec)

  // ---- DelegatedStakeRewardParameters -------------------------------------

  implicit val delegatedStakeRewardParametersCodec: Codec[DelegatedStakeRewardParameters] =
    rewardFractionCodec.xmap[DelegatedStakeRewardParameters](
      DelegatedStakeRewardParameters(_),
      _.rewardFraction
    )

  implicit val delegatedStakeRewardParametersImmutableCodec: ImmutableCodec[DelegatedStakeRewardParameters] =
    ImmutableCodec.fromScodecCodec(delegatedStakeRewardParametersCodec)

  // ---- NodeMetadataParameters ---------------------------------------------

  implicit val nodeMetadataParametersCodec: Codec[NodeMetadataParameters] =
    (stringCodec :: stringCodec)
      .xmap[NodeMetadataParameters](
        { case n :: d :: HNil => NodeMetadataParameters(n, d) },
        m => m.name :: m.description :: HNil
      )

  implicit val nodeMetadataParametersImmutableCodec: ImmutableCodec[NodeMetadataParameters] =
    ImmutableCodec.fromScodecCodec(nodeMetadataParametersCodec)

  // ---- UpdateNodeParameters -----------------------------------------------

  implicit val updateNodeParametersCodec: Codec[UpdateNodeParameters] =
    (addressCodec ::
      delegatedStakeRewardParametersCodec ::
      nodeMetadataParametersCodec ::
      updateNodeParametersReferenceCodec)
      .xmap[UpdateNodeParameters](
        { case src :: rewardParams :: metaParams :: parent :: HNil =>
          UpdateNodeParameters(src, rewardParams, metaParams, parent)
        },
        u => u.source ::
          u.delegatedStakeRewardParameters ::
          u.nodeMetadataParameters ::
          u.parent ::
          HNil
      )

  implicit val updateNodeParametersImmutableCodec: ImmutableCodec[UpdateNodeParameters] =
    ImmutableCodec.fromScodecCodec(updateNodeParametersCodec)
}
