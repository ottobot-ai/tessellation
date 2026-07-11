package io.constellationnetwork.serde.codecs.instances

import cats.data.NonEmptyList

import scala.collection.immutable.SortedSet

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.artifact._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.NonEmptyListCodec.nonEmptyList
import io.constellationnetwork.serde.codecs.SortedSetCodec.sortedSet
import io.constellationnetwork.serde.codecs.instances.CurrencyAtomCodecs._
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.PriceOracleCodecs.{pricingUpdateCodec => priceUpdateCodec}

import scodec.Codec
import scodec.codecs.{discriminated, uint8}

/** Canonical scodec codec for the `SharedArtifact` sealed ADT.
  *
  * 1-byte discriminator; all variants live in a single file so the mapping is auditable at a glance. FROZEN discriminator bytes — do NOT
  * renumber:
  *   - 0x00: SpendAction
  *   - 0x01: TokenUnlock
  *   - 0x02: AllowSpendExpiration
  *   - 0x03: PricingUpdate
  *   - 0x04: unused
  *   - 0x05: GlobalSnapshotsProcessed
  *
  * PricingUpdate / TokenUnlock / AllowSpendExpiration already have their own codecs (from PriceOracleCodecs and CurrencyAtomCodecs); this
  * file just wires them into the ADT.
  */
object SharedArtifactCodec {

  // ---- SpendAction --------------------------------------------------------
  private val spendTransactionsCodec: Codec[NonEmptyList[SpendTransaction]] =
    nonEmptyList(spendTransactionCodec)

  implicit val spendActionCodec: Codec[SpendAction] =
    spendTransactionsCodec.xmap[SpendAction](SpendAction(_), _.spendTransactions)

  implicit val spendActionImmutableCodec: ImmutableCodec[SpendAction] =
    ImmutableCodec.fromScodecCodec(spendActionCodec)

  // ---- GlobalSnapshotsProcessed ------------------------------------------
  private val sortedOrdinalSetCodec: Codec[SortedSet[SnapshotOrdinal]] =
    sortedSet(Codec[SnapshotOrdinal])

  implicit val globalSnapshotsProcessedCodec: Codec[GlobalSnapshotsProcessed] =
    sortedOrdinalSetCodec.xmap[GlobalSnapshotsProcessed](GlobalSnapshotsProcessed(_), _.ordinals)

  implicit val globalSnapshotsProcessedImmutableCodec: ImmutableCodec[GlobalSnapshotsProcessed] =
    ImmutableCodec.fromScodecCodec(globalSnapshotsProcessedCodec)

  // ---- SharedArtifact sum-type discriminator ------------------------------
  implicit val sharedArtifactCodec: Codec[SharedArtifact] =
    discriminated[SharedArtifact]
      .by(uint8)
      .typecase(0, spendActionCodec)
      .typecase(1, tokenUnlockCodec)
      .typecase(2, allowSpendExpirationCodec)
      .typecase(3, priceUpdateCodec)
      .typecase(5, globalSnapshotsProcessedCodec)

  implicit val sharedArtifactImmutableCodec: ImmutableCodec[SharedArtifact] =
    ImmutableCodec.fromScodecCodec(sharedArtifactCodec)
}
