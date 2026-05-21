package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.node.UpdateNodeParameters
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.unpRecordImmutableCodec

/** §G5 — MPT-backed reader for the `UpdateNodeParameters` partition.
  *
  * The `UpdateNodeParameters` partition is keyed by a hash of `Id` (not by `Address`), so the MPT key alone cannot reconstruct the `Id`.
  * Recovery uses the signed value's `proofs.head.id` — by GSAM convention the signer's `Id` equals the map's keying `Id`, so a prefix-scan
  * + value-decode is sufficient.
  *
  * The reader returns the same `SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]` shape that `info.updateNodeParameters`
  * carries on the GSI side. Byte-equivalent to the GSI map when MPT and GSI are in sync: each entry decodes via `unpRecordImmutableCodec`
  * (same codec used by `MptStore.syncFromGlobalSnapshotInfo`) and is keyed by the `Id` extracted from the signed record's first proof.
  *
  * Used by:
  *   - `RewardsInfoCalculator` to look up commission rate per peer-node when totaling reward pools
  *   - `GlobalDelegatedRewardsDistributor.calculateDelegatorRewards` / `calculateNodeOperatorRewards` to look up
  *     `delegatedStakeRewardParameters` per node
  *
  * Empty result is returned via `SortedMap.empty` when the prefix scan returns no entries.
  */
trait UpdateNodeParametersStateReader[F[_]] {
  def materializeUpdateNodeParametersFromMpt(
    implicit hasher: Hasher[F]
  ): F[SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]]
}

object UpdateNodeParametersStateReader {

  def make[F[_]: Async](reader: GlobalStateReader[F]): UpdateNodeParametersStateReader[F] =
    new UpdateNodeParametersStateReader[F] {
      def materializeUpdateNodeParametersFromMpt(
        implicit hasher: Hasher[F]
      ): F[SortedMap[Id, (Signed[UpdateNodeParameters], SnapshotOrdinal)]] =
        for {
          prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.UpdateNodeParameters)
          entries <- reader.getAllForPrefix[(Signed[UpdateNodeParameters], SnapshotOrdinal)](prefix)
        } yield SortedMap.from(entries.values.map { case (signed, ord) => signed.proofs.head.id -> (signed, ord) })
    }
}
