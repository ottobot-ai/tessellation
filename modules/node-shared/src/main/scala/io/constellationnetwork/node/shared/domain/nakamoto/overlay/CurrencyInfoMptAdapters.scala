package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import io.constellationnetwork.schema.mpt.GlobalStateConverter.{CurrencyInfoMpt, CurrencyInfoReader}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, StrictMptEntry}
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.serde.ImmutableCodec

/** Overlay-side adapters from the branch-scoped read/write algebras (`GlobalStateReader` / `AcceptanceMpt`) onto the shared
  * `CurrencyInfoReader` / `CurrencyInfoMpt` capabilities `GlobalStateConverter.reconstructCurrencyInfoFrom` / `writeCurrencyInfo` consume.
  *
  * This keeps the unrolled per-metagraph `CurrencySnapshotInfo` reconstruct + write + removal logic in ONE place (shared
  * `GlobalStateConverter`, see docs/nakamoto/UNROLL-CURRENCY-SNAPSHOT-INFO-DESIGN.md §6) while the overlay path supplies its branch-aware
  * `getAllForPrefix` / `insert(Map)` / `remove(List)`. The MPT-store path uses `GlobalStateConverter.CurrencyInfoMpt.fromMptStore` instead.
  */
object CurrencyInfoMptAdapters {

  /** Read-only: a `GlobalStateReader[F]` exposes `getAllForPrefix`, which is exactly the `CurrencyInfoReader` capability
    * `reconstructCurrencyInfoFrom` needs. Use for follower / read-path reconstruction where no writer is in scope.
    */
  def readerFor[F[_]](reader: GlobalStateReader[F]): CurrencyInfoReader[F] = new CurrencyInfoReader[F] {
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] = reader.getAllForPrefix[V](prefix)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      reader.getAllForPrefixStrict[V](prefix)
  }

  /** Read+write: an `AcceptanceMpt[F]` is `GlobalStateReader` (prefix scan) + `GlobalStateWriter` (`insert(Map)` / `remove(List)`), so it
    * satisfies the full `CurrencyInfoMpt` capability `writeCurrencyInfo` needs. Use inside the acceptance pipeline (`applyStateChanges`).
    */
  def mptFor[F[_]](acceptanceMpt: AcceptanceMpt[F]): CurrencyInfoMpt[F] = new CurrencyInfoMpt[F] {
    def getAllForPrefix[V: ImmutableCodec](prefix: Hex): F[Map[Hex, V]] = acceptanceMpt.getAllForPrefix[V](prefix)
    def getAllForPrefixStrict[V: ImmutableCodec](prefix: Hex): F[List[StrictMptEntry[V]]] =
      acceptanceMpt.getAllForPrefixStrict[V](prefix)
    def insert[V: ImmutableCodec](entries: Map[GlobalStateKey, V]): F[Unit] = acceptanceMpt.insert[V](entries)
    def remove(keys: List[GlobalStateKey]): F[Unit] = acceptanceMpt.remove(keys)
  }
}
