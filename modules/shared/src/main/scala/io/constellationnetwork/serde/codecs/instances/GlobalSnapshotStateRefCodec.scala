package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.nakamoto.GlobalSnapshotStateRef
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Canonical ScodecV1 shape of an exact global snapshot state identity. In addition to the fixed-width representation, this boundary
  * rejects empty authority sentinels outside the genesis parent exception. Semantic consumers must still authenticate exact branch
  * membership and Phase-2 status. The finality domain owns a separate codec and semantic validator for its evidence types.
  */
object GlobalSnapshotStateRefCodec {
  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val mptRootCodec: Codec[MptRoot] = hashCodec.xmap(MptRoot(_), _.value)

  private def isCanonicalHash(hash: Hash): Boolean =
    hash.value.length == HashCodec.HashByteLength * 2 && hash.value.forall { char =>
      (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')
    }

  private val structuralCodec: Codec[GlobalSnapshotStateRef] =
    (snapshotOrdinalCodec :: hashCodec :: hashCodec :: mptRootCodec).xmap[GlobalSnapshotStateRef](
      { case ordinal :: hash :: parentHash :: mptRoot :: HNil => GlobalSnapshotStateRef(ordinal, hash, parentHash, mptRoot) },
      ref => ref.ordinal :: ref.hash :: ref.parentHash :: ref.mptRoot :: HNil
    )

  private def validate(ref: GlobalSnapshotStateRef): Attempt[GlobalSnapshotStateRef] =
    if (!List(ref.hash, ref.parentHash, ref.mptRoot.value).forall(isCanonicalHash))
      Attempt.failure(Err("GlobalSnapshotStateRef hashes must be 64-character lowercase hexadecimal"))
    else if (ref.hash == Hash.empty) Attempt.failure(Err("GlobalSnapshotStateRef.hash must not be Hash.empty"))
    else if (ref.mptRoot.value == Hash.empty) Attempt.failure(Err("GlobalSnapshotStateRef.mptRoot must not be Hash.empty"))
    else if (ref.ordinal != SnapshotOrdinal.MinValue && ref.parentHash == Hash.empty)
      Attempt.failure(Err("GlobalSnapshotStateRef.parentHash may be Hash.empty only at ordinal zero"))
    else Attempt.successful(ref)

  implicit val codec: Codec[GlobalSnapshotStateRef] = structuralCodec.exmap(validate, validate)

  implicit val immutableCodec: ImmutableCodec[GlobalSnapshotStateRef] = ImmutableCodec.fromScodecCodec(codec)
}
