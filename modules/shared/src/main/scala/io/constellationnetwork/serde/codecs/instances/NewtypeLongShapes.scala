package io.constellationnetwork.serde.codecs.instances

import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.NonNegLongNewtype

import scodec.Codec

/** Shape registrations for every consensus type that is a newtype-over-NonNegLong.
  *
  * One line per type. The `NonNegLongNewtype` witness plus the derived `Codec[T]`
  * and `ImmutableCodec[T]` do the rest. If any of these types ever gains a
  * second field, the `T(_)` constructor application fails to compile here —
  * refactor-silent-byte-drift is not possible.
  *
  * Downstream usage: a single `import NewtypeLongShapes._` brings both the shape
  * witnesses AND the derivation in scope, so `Codec[Balance]` /
  * `ImmutableCodec[Balance]` resolve without needing to also `import
  * NonNegLongNewtype._`. The re-exports below bridge the two implicit-scope
  * searches that Scala performs (target-type companion and in-scope imports).
  *
  * Consensus contract: adding a new entry is FROZEN behaviour. Removing or
  * reordering entries here doesn't change byte layout (the derivation is keyed
  * on `T`, not on position), but the existing golden files `<T>-scodec-v1.hex`
  * must continue to match on every build.
  */
object NewtypeLongShapes {

  // Re-export the derivation implicits so a single import of this object brings
  // the full chain (shape witness + Codec[T] + ImmutableCodec[T]) into scope.
  implicit def derivedCodec[T](implicit ev: NonNegLongNewtype[T]): Codec[T] =
    NonNegLongNewtype.derivedCodec[T]
  implicit def derivedImmutableCodec[T](implicit ev: NonNegLongNewtype[T]): ImmutableCodec[T] =
    NonNegLongNewtype.derivedImmutableCodec[T]

  implicit val balanceShape: NonNegLongNewtype[Balance] =
    NonNegLongNewtype.instance(Balance(_), _.value)

  implicit val amountShape: NonNegLongNewtype[Amount] =
    NonNegLongNewtype.instance(Amount(_), _.value)

  implicit val snapshotOrdinalShape: NonNegLongNewtype[SnapshotOrdinal] =
    NonNegLongNewtype.instance(SnapshotOrdinal(_), _.value)

  implicit val epochProgressShape: NonNegLongNewtype[EpochProgress] =
    NonNegLongNewtype.instance(EpochProgress(_), _.value)
}
