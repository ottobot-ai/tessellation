package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.PartitionNamespace._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, PartitionNamespace}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.serde.ImmutableCodec

import scodec.codecs.{discriminated, provide, uint8}
import scodec.{Attempt, Codec, Err}
import shapeless.{::, HNil}

/** Canonical scodec codec for the MPT-as-primary key types:
  *   - `PartitionNamespace` — sealed ADT (5 variants) via 1-byte discriminator.
  *   - `GlobalStateFieldId`  — sealed ADT (19 case objects) via 1-byte id.
  *   - `GlobalStateKey`      — 4-field compound.
  *
  * PartitionNamespace discriminator layout:
  *   - 0x00: HypergraphNamespace   (no body)
  *   - 0x01: EmptyNamespace        (no body)
  *   - 0x02: MetagraphNamespace    (+ Address)
  *   - 0x03: AddressNamespace      (+ Address)
  *   - 0x04: HashNamespace         (+ 32-byte Hash)
  *
  * These are NEW discriminator bytes, unrelated to the existing
  * `PartitionKeyType.toByte` (which is used by the hex-derivation path in
  * `GlobalStateKey.toHex`). Keeping them separate means the two semantic
  * concerns — "what shape of key does this namespace produce in the MPT"
  * (PartitionKeyType) vs. "which ADT variant is this on the wire"
  * (discriminator) — don't coincidentally collide. The two tag families
  * must be allowed to evolve independently.
  *
  * GlobalStateFieldId: 1-byte id via `fromInt`. 19 variants today (0..18);
  * 1-byte headroom to 255 is ample. When the registry ever grows past 255,
  * introduce `GlobalStateKeyCodecV2` with uint16.
  *
  * Consensus contract: FROZEN. Discriminator bytes, field-id width, and
  * field-order in `GlobalStateKey` are all persistent — every MPT key
  * written by the scodec era depends on them being stable.
  *
  * First sum-type codec: unlocks MPT-as-primary write path.
  */
object GlobalStateKeyCodec {

  private val addressCodec: Codec[Address] = AddressCodec.codec
  private val hashCodec: Codec[Hash] = HashCodec.codec

  // The discriminated-codec pattern: scodec's `discriminated[..].by(uint8)`
  // walks the tag, picks the right body codec, and re-emits the tag on encode.
  // Each `typecase` must take a `Codec[Subtype]` (NOT a widened `Codec[PartitionNamespace]`)
  // because scodec's encode-time dispatch uses `ClassTag[Subtype]` to pick the branch
  // via a runtime `v.getClass == subtypeClass` check. Widening the codec would point
  // the ClassTag at the base type and match every variant on the first branch.
  private val metagraphNsCodec: Codec[MetagraphNamespace] =
    addressCodec.xmap(MetagraphNamespace(_), _.address)

  private val addressNsCodec: Codec[AddressNamespace] =
    addressCodec.xmap(AddressNamespace(_), _.address)

  private val hashNsCodec: Codec[HashNamespace] =
    hashCodec.xmap(HashNamespace(_), _.hash)

  implicit val partitionNamespaceCodec: Codec[PartitionNamespace] =
    discriminated[PartitionNamespace]
      .by(uint8)
      .typecase(0, provide(HypergraphNamespace))
      .typecase(1, provide(EmptyNamespace))
      .typecase(2, metagraphNsCodec)
      .typecase(3, addressNsCodec)
      .typecase(4, hashNsCodec)

  implicit val partitionNamespaceImmutableCodec: ImmutableCodec[PartitionNamespace] =
    ImmutableCodec.fromScodecCodec(partitionNamespaceCodec)

  implicit val globalStateFieldIdCodec: Codec[GlobalStateFieldId] =
    uint8.exmap(
      i =>
        GlobalStateFieldId
          .fromInt(i)
          .fold[Attempt[GlobalStateFieldId]](
            Attempt.failure(Err(s"GlobalStateFieldId decode: unknown id $i"))
          )(Attempt.successful),
      f => Attempt.successful(f.toInt)
    )

  implicit val globalStateFieldIdImmutableCodec: ImmutableCodec[GlobalStateFieldId] =
    ImmutableCodec.fromScodecCodec(globalStateFieldIdCodec)

  implicit val codec: Codec[GlobalStateKey] =
    (partitionNamespaceCodec ::
      globalStateFieldIdCodec ::
      partitionNamespaceCodec ::
      partitionNamespaceCodec)
      .xmap[GlobalStateKey](
        { case net :: fid :: contract :: user :: HNil =>
          GlobalStateKey(net, fid, contract, user)
        },
        k => k.networkNamespace :: k.fieldId :: k.contractNamespace :: k.userNamespace :: HNil
      )

  implicit val immutableCodec: ImmutableCodec[GlobalStateKey] = ImmutableCodec.fromScodecCodec(codec)
}
