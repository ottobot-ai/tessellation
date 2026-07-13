package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.kes.KesRegistrationCert
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationOrdinal, KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{codec => hashCodec}
import io.constellationnetwork.serde.codecs.instances.HexContentCodec.{codec => hexCodec}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PeerIdCodec.{codec => peerIdCodec}
import io.constellationnetwork.serde.codecs.instances.SignedCodec.{codecFor => signedCodecFor}
import io.constellationnetwork.serde.codecs.instances.StakeDistributionCodec.etaPeriodCodec

import scodec.Codec
import scodec.codecs.int32
import shapeless.{::, HNil}

/** Canonical scodec codecs for the current unified KES+VRF operator-key registration candidate:
  *   - `KesRegistrationOrdinal` — NonNegLong newtype, derived via [[NewtypeLongShapes]].
  *   - `KesRegistrationReference` — `(ordinal, hash)`, 40-byte layout matching the other *Reference types.
  *   - `KesRegistrationCert` — operator's signed KES+VRF registration body.
  *   - `Signed[KesRegistrationCert]` — wire envelope around the body.
  *   - `KesRegistrationRecord` — persisted record carrying the signed cert + `acceptedAt: SnapshotOrdinal`.
  *
  * Mirrors `NodeCollateralCodecs` 1:1 in structure. Used by the MPT-backed `KesRegistrationStateManager` to encode/decode the
  * `KesRegistrationCerts` and `LastKesRegistrationRefs` partitions, and by genesis/snapshot conversion paths to sync into MPT.
  *
  * This fork is greenfield: this is the only fork wire shape and there is no compatibility codec for the abandoned KES-only candidate.
  */
object KesRegistrationCodecs {

  private val ordinalCodec: Codec[KesRegistrationOrdinal] = Codec[KesRegistrationOrdinal]
  private val snapshotOrdinalCodec: Codec[SnapshotOrdinal] = Codec[SnapshotOrdinal]
  private val effectivePeriodCodec: Codec[EtaPeriod] = etaPeriodCodec

  implicit val kesRegistrationReferenceCodec: Codec[KesRegistrationReference] =
    (ordinalCodec :: hashCodec)
      .xmap[KesRegistrationReference](
        { case o :: h :: HNil => KesRegistrationReference(o, h) },
        r => r.ordinal :: r.hash :: HNil
      )

  implicit val kesRegistrationReferenceImmutableCodec: ImmutableCodec[KesRegistrationReference] =
    ImmutableCodec.fromScodecCodec(kesRegistrationReferenceCodec)

  implicit val kesRegistrationCertCodec: Codec[KesRegistrationCert] =
    (peerIdCodec :: hexCodec :: int32 :: scodec.codecs.int64 :: hexCodec :: effectivePeriodCodec :: hashCodec :: ordinalCodec :: kesRegistrationReferenceCodec)
      .xmap[KesRegistrationCert](
        {
          case op :: kesVk :: kesVkStep :: offset :: vrfVk :: period :: registrationParent :: ord :: parent :: HNil =>
            KesRegistrationCert(op, kesVk, kesVkStep, offset, vrfVk, period, registrationParent, ord, parent)
        },
        c =>
          c.operatorPeerId ::
            c.kesMasterVK ::
            c.kesMasterVKStep ::
            c.offset ::
            c.vrfPublicKey ::
            c.effectiveFromPeriod ::
            c.registrationParentHash ::
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
