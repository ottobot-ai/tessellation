package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.nakamoto.follow.GlobalChangeSetResponse
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.ListCodec.list
import io.constellationnetwork.serde.codecs.OptionCodec.option
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.StateChangesAccumulatorCodec.{codec => stateChangesAccumulatorCodec}

import scodec.Codec
import shapeless.{::, HNil}

/** Canonical scodec codec for [[GlobalChangeSetResponse]] — the wire form of the gl0 → currency-l0 (ml0) changeset transport (task #12,
  * slice 3; see `project_ml0_diff_adopt_design`).
  *
  * Explicit, hand-written (NO auto-derivation): the wire layout follows the case-class field order exactly —
  *   1. `latestOrdinal` — a [[SnapshotOrdinal]] (NonNegLong newtype; codec via [[NewtypeLongShapes]]);
  *   2. `baseOrdinal` — `Option[SnapshotOrdinal]` (1-byte present/absent flag + body, see [[io.constellationnetwork.serde.codecs.OptionCodec]]);
  *   3. `deltas` — `List[(SnapshotOrdinal, StateChangesAccumulator)]` (2-byte count + insertion-order tuples, see
   *     [[io.constellationnetwork.serde.codecs.ListCodec]]). The per-ordinal accumulator REUSES the canonical
  *     [[StateChangesAccumulatorCodec]] — the accumulator binary is scodec, never Circe/auto-derived.
  *
  * This is a TRANSPORT codec, not a signing/hashing preimage: the cryptographic anchor of each delta is the matching signed snapshot's
  * `mptRoot`, recomputed independently by applying the delta. The encoding is nonetheless frozen byte-for-byte like every other consensus
  * codec — the per-ordinal accumulator's bytes are byte-identical to those `StateChangesAccumulatorCodec` produces standalone, so a follower
  * can decode the embedded delta with the same codec it uses elsewhere.
  */
object GlobalChangeSetResponseCodec {

  /** `(SnapshotOrdinal, StateChangesAccumulator)` tuple — ordinal first, then the accumulator binary, mirroring the case-class field order
    * in [[GlobalChangeSetResponse.deltas]].
    */
  private val deltaEntryCodec: Codec[(SnapshotOrdinal, StateChangesAccumulator)] =
    (Codec[SnapshotOrdinal] :: stateChangesAccumulatorCodec).xmap(
      { case o :: acc :: HNil => (o, acc) },
      t => t._1 :: t._2 :: HNil
    )

  implicit val codec: Codec[GlobalChangeSetResponse] =
    (Codec[SnapshotOrdinal] :: option(Codec[SnapshotOrdinal]) :: list(deltaEntryCodec))
      .xmap[GlobalChangeSetResponse](
        {
          case latestOrdinal :: baseOrdinal :: deltas :: HNil =>
            GlobalChangeSetResponse(latestOrdinal, baseOrdinal, deltas)
        },
        r => r.latestOrdinal :: r.baseOrdinal :: r.deltas :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[GlobalChangeSetResponse] = ImmutableCodec.fromScodecCodec(codec)
}
