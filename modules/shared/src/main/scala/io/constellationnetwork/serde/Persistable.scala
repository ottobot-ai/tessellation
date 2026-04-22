package io.constellationnetwork.serde

import scodec.bits.ByteVector

/** Byte representation for persisting `T` to disk / MPT leaves. May evolve across releases via one-shot reindex — disk format is less rigid
  * than signing bytes.
  *
  * Laws:
  *   - `fromPersistedBytes(persistedBytes(a)) == Right(a)` (round-trip).
  *   - Stable within a release. Migrations are explicit: change the era config and run the reindex.
  *   - The bytes MAY be compressed (brotli by default; see `BrotliPersistable`). Importantly, the HASH of a leaf is NOT of these bytes —
  *     the content-address is `Hash.fromBytes(ImmutableCodec[T].immutableBytes(t).toArray)`. Compression is a storage optimisation, never
  *     consensus-critical.
  *
  * Convention: in-package defaults produce `ImmutableCodec` bytes (no compression). For brotli-wrapped variants, see `BrotliPersistable`.
  *
  * @tparam T
  *   the value type to persist
  */
trait Persistable[T] {
  def persistedBytes(value: T): ByteVector
  def fromPersistedBytes(bytes: ByteVector): Either[SerdeError, T]
}

object Persistable {
  def apply[T](implicit ev: Persistable[T]): Persistable[T] = ev

  /** Default: delegate to `ImmutableCodec`. Uncompressed. Use `BrotliPersistable` when disk-cost matters. Call sites that want a
    * brotli-compressed instance must reach for `BrotliPersistable.fromImmutableCodec` explicitly — compression is never implicit.
    */
  implicit def fromImmutableCodec[T](implicit ev: ImmutableCodec[T]): Persistable[T] = new Persistable[T] {
    def persistedBytes(value: T): ByteVector = ev.immutableBytes(value)
    def fromPersistedBytes(bytes: ByteVector): Either[SerdeError, T] = ev.fromImmutableBytes(bytes)
  }

  /** Syntax. */
  trait PersistableSyntax {
    implicit class PersistableOps[T](private val self: T) {
      def persistedBytes(implicit ev: Persistable[T]): ByteVector = ev.persistedBytes(self)
    }
    implicit class PersistableBytesOps(private val self: ByteVector) {
      def fromPersistedBytes[T](implicit ev: Persistable[T]): Either[SerdeError, T] =
        ev.fromPersistedBytes(self)
    }
  }
}
