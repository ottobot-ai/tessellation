package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake.DelegatedStakeAmount
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.node.UpdateNodeParametersOrdinal
import io.constellationnetwork.schema.swap.{AllowSpendFee, AllowSpendOrdinal, SwapAmount}
import io.constellationnetwork.schema.tokenLock.{TokenLockAmount, TokenLockFee, TokenLockOrdinal}
import io.constellationnetwork.schema.transaction._
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.{LongNewtype, NonNegLongNewtype, PosLongNewtype}

import scodec.Codec

/** Shape registrations for every consensus type that is a newtype-over-NonNegLong.
  *
  * One line per type. The `NonNegLongNewtype` witness plus the derived `Codec[T]` and `ImmutableCodec[T]` do the rest. If any of these
  * types ever gains a second field, the `T(_)` constructor application fails to compile here — refactor-silent-byte-drift is not possible.
  *
  * Downstream usage: a single `import NewtypeLongShapes._` brings both the shape witnesses AND the derivation in scope, so `Codec[Balance]`
  * / `ImmutableCodec[Balance]` resolve without needing to also `import NonNegLongNewtype._`. The re-exports below bridge the two
  * implicit-scope searches that Scala performs (target-type companion and in-scope imports).
  *
  * Consensus contract: adding a new entry is FROZEN behaviour. Removing or reordering entries here doesn't change byte layout (the
  * derivation is keyed on `T`, not on position), but the existing golden files `<T>-scodec-v1.hex` must continue to match on every build.
  */
object NewtypeLongShapes {

  // Re-export the derivation implicits so a single import of this object brings
  // the full chain (shape witness + Codec[T] + ImmutableCodec[T]) into scope.
  // Uniquely named (not plain `derivedCodec`) so multiple shape re-exports can
  // coexist in the same import scope without implicit resolution ambiguity —
  // e.g. a file that imports NewtypeLongShapes._ AND SignedCodec._ can't have
  // both re-exports named `derivedCodec`.
  implicit def nonNegLongShapeCodec[T](implicit ev: NonNegLongNewtype[T]): Codec[T] =
    NonNegLongNewtype.derivedCodec[T]
  implicit def nonNegLongShapeImmutableCodec[T](implicit ev: NonNegLongNewtype[T]): ImmutableCodec[T] =
    NonNegLongNewtype.derivedImmutableCodec[T]

  implicit def posLongShapeCodec[T](implicit ev: PosLongNewtype[T]): Codec[T] =
    PosLongNewtype.derivedCodec[T]
  implicit def posLongShapeImmutableCodec[T](implicit ev: PosLongNewtype[T]): ImmutableCodec[T] =
    PosLongNewtype.derivedImmutableCodec[T]

  implicit def longShapeCodec[T](implicit ev: LongNewtype[T]): Codec[T] =
    LongNewtype.derivedCodec[T]
  implicit def longShapeImmutableCodec[T](implicit ev: LongNewtype[T]): ImmutableCodec[T] =
    LongNewtype.derivedImmutableCodec[T]

  // --- NonNegLong shapes --------------------------------------------------

  implicit val balanceShape: NonNegLongNewtype[Balance] =
    NonNegLongNewtype.instance(Balance(_), _.value)

  implicit val amountShape: NonNegLongNewtype[Amount] =
    NonNegLongNewtype.instance(Amount(_), _.value)

  implicit val snapshotOrdinalShape: NonNegLongNewtype[SnapshotOrdinal] =
    NonNegLongNewtype.instance(SnapshotOrdinal(_), _.value)

  implicit val epochProgressShape: NonNegLongNewtype[EpochProgress] =
    NonNegLongNewtype.instance(EpochProgress(_), _.value)

  implicit val transactionOrdinalShape: NonNegLongNewtype[TransactionOrdinal] =
    NonNegLongNewtype.instance(TransactionOrdinal(_), _.value)

  implicit val transactionFeeShape: NonNegLongNewtype[TransactionFee] =
    NonNegLongNewtype.instance(TransactionFee(_), _.value)

  implicit val heightShape: NonNegLongNewtype[Height] =
    NonNegLongNewtype.instance(Height(_), _.value)

  implicit val subHeightShape: NonNegLongNewtype[SubHeight] =
    NonNegLongNewtype.instance(SubHeight(_), _.value)

  implicit val allowSpendOrdinalShape: NonNegLongNewtype[AllowSpendOrdinal] =
    NonNegLongNewtype.instance(AllowSpendOrdinal(_), _.value)

  implicit val allowSpendFeeShape: NonNegLongNewtype[AllowSpendFee] =
    NonNegLongNewtype.instance(AllowSpendFee(_), _.value)

  implicit val tokenLockOrdinalShape: NonNegLongNewtype[TokenLockOrdinal] =
    NonNegLongNewtype.instance(TokenLockOrdinal(_), _.value)

  implicit val tokenLockFeeShape: NonNegLongNewtype[TokenLockFee] =
    NonNegLongNewtype.instance(TokenLockFee(_), _.value)

  implicit val delegatedStakeAmountShape: NonNegLongNewtype[DelegatedStakeAmount] =
    NonNegLongNewtype.instance(DelegatedStakeAmount(_), _.value)

  implicit val updateNodeParametersOrdinalShape: NonNegLongNewtype[UpdateNodeParametersOrdinal] =
    NonNegLongNewtype.instance(UpdateNodeParametersOrdinal(_), _.value)

  // --- PosLong shapes -----------------------------------------------------

  implicit val transactionAmountShape: PosLongNewtype[TransactionAmount] =
    PosLongNewtype.instance(TransactionAmount(_), _.value)

  implicit val tokenLockAmountShape: PosLongNewtype[TokenLockAmount] =
    PosLongNewtype.instance(TokenLockAmount(_), _.value)

  implicit val swapAmountShape: PosLongNewtype[SwapAmount] =
    PosLongNewtype.instance(SwapAmount(_), _.value)

  // --- Plain Long shapes --------------------------------------------------

  implicit val transactionSaltShape: LongNewtype[TransactionSalt] =
    LongNewtype.instance(TransactionSalt(_), _.value)
}
