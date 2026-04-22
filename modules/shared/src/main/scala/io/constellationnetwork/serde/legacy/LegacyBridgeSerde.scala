package io.constellationnetwork.serde.legacy

import io.constellationnetwork.serde.SerdeError
import io.constellationnetwork.serde.era.SerdeEra

import scodec.bits.ByteVector

/** Read-only decoder for historical bytes encoded under a past era (JSON / Kryo).
  *
  * This typeclass deliberately has NO encode method. The write path is locked to
  * the current era (`SerdeEra.Scodec`); historical eras exist to sync from chain
  * tail. There is no implicit conversion from `LegacyBridgeSerde[T]` to
  * `Signable[T]` / `ImmutableCodec[T]` / `Persistable[T]` / `Transmittable[T]` —
  * the type system refuses to route legacy bytes back into the write path.
  *
  * Concrete implementations for JSON / Kryo are built as thin adapters over the
  * existing `JsonSerializer` / `KryoSerializer`. As consensus types migrate to
  * scodec (era boundary moves forward), their legacy bridge instances REMAIN
  * registered so re-sync from historical ordinals still works.
  */
trait LegacyBridgeSerde[T] {

  /** Which era these bytes were written under (for error messages and telemetry). */
  def era: SerdeEra

  def fromLegacyBytes(bytes: ByteVector): Either[SerdeError, T]
}

object LegacyBridgeSerde {
  def apply[T](implicit ev: LegacyBridgeSerde[T]): LegacyBridgeSerde[T] = ev
}
