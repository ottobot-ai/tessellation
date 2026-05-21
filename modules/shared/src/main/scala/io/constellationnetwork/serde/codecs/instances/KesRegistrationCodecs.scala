package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}

import scodec.Codec
import scodec.codecs.int32
import shapeless.{::, HNil}

/** Canonical scodec codecs for the §1.2 Slice 10 KES runtime-registration family (#179):
  *   - `KesRegistrationOrdinal` — NonNegLong newtype, derived via [[NewtypeLongShapes]].
  *   - `KesRegistrationReference` — `(ordinal, hash)`, 40-byte layout matching the other *Reference types.
  *   - `KesRegistrationCert` — operator's runtime cert body.
  *   - `Signed[KesRegistrationCert]` — wire envelope around the body.
  *   - `KesRegistrationRecord` — persisted record carrying the signed cert + `acceptedAt: SnapshotOrdinal`.
  *
  * Mirrors `NodeCollateralCodecs` 1:1 in structure. Used by the MPT-backed `KesRegistrationStateManager` to encode/decode the
  * `KesRegistrationCerts` and `LastKesRegistrationRefs` partitions, and by genesis/snapshot conversion paths to sync into MPT.
  *
  * Consensus contract: FROZEN once the first Slice 10 cert lands on chain. Adding a field to `KesRegistrationCert` would require a new
  * codec variant + migration.
  */
object KesRegistrationCodecs {

  private val ordinalCodec: Codec[KesRegistrationOrdinal] = Codec[KesRegistrationOrdinal]
  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val epochCodec: Codec[EpochProgress] = Codec[EpochProgress]

  implicit val kesRegistrationReferenceCodec: Codec[KesRegistrationReference] =
    (ordinalCodec :: hashCodec)
      .xmap[KesRegistrationReference](
        { case o :: h :: HNil => KesRegistrationReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val kesRegistrationReferenceImmutableCodec: ImmutableCodec[KesRegistrationReference] =
    ImmutableCodec.fromScodecCodec(kesRegistrationReferenceCodec)

  implicit val kesRegistrationCertCodec: Codec[KesRegistrationCert] =
    (peerIdCodec :: hexCodec :: int32 :: scodec.codecs.int64 :: epochCodec :: ordinalCodec :: kesRegistrationReferenceCodec)
      .xmap[KesRegistrationCert](
        {
          case op :: vk :: vkStep :: offset :: epoch :: ord :: parent :: HNil =>
            KesRegistrationCert(op, vk, vkStep, offset, epoch, ord, parent)
        },
        c =>
          c.operatorPeerId ::
            c.kesMasterVK ::
            c.kesMasterVKStep ::
            c.offset ::
            c.effectiveFromEpoch ::
            c.ordinal ::
            c.parent ::
            HNil
      )

  implicit val kesRegistrationCertImmutableCodec: ImmutableCodec[KesRegistrationCert] =
    ImmutableCodec.fromScodecCodec(kesRegistrationCertCodec)

  private val signedKesRegistrationCertCodec: Codec[Signed[KesRegistrationCert]] = signedCodecFor(kesRegistrationCertCodec)

  implicit val signedKesRegistrationCertImmutableCodec: ImmutableCodec[Signed[KesRegistrationCert]] =
    ImmutableCodec.fromScodecCodec(signedKesRegistrationCertCodec)

  implicit val kesRegistrationRecordCodec: Codec[KesRegistrationRecord] =
    (signedKesRegistrationCertCodec :: snapshotOrdinalCodec)
      .xmap[KesRegistrationRecord](
        { case evt :: accepted :: HNil => KesRegistrationRecord(evt, accepted) },
        r => r.event :: r.acceptedAt :: HNil
      )

  implicit val kesRegistrationRecordImmutableCodec: ImmutableCodec[KesRegistrationRecord] =
    ImmutableCodec.fromScodecCodec(kesRegistrationRecordCodec)
}
