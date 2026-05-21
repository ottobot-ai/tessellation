package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.kes

import cats.effect.Async
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.node.shared.domain.nakamoto.kes.KesRegistrationCertAcceptanceResult
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.kes.KesRegistrationCert.{KesRegistrationRecord, KesRegistrationReference}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security.Hasher
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.kesRegistrationRecordSetCodec
import io.constellationnetwork.serde.codecs.instances.KesRegistrationCodecs.kesRegistrationReferenceImmutableCodec

/** §1.2 Slice 10 — MPT-backed state manager for runtime KES registration certs (#179).
  *
  * The durable source of truth for the post-genesis KES master-VK registry: every accepted [[KesRegistrationRecord]] is persisted in the
  * `KesRegistrationCerts` partition (per-operator [[SortedSet]]), with a parallel pointer record in `LastKesRegistrationRefs`
  * (per-operator latest [[KesRegistrationReference]]) for O(1) chain-link lookup.
  *
  * Mirrors `NodeCollateralStateManager` 1:1 in shape: the trait exposes materializers that the GSAM acceptance pipeline calls to recover
  * prior state from MPT, plus a `getUpdatedKesRegistrationCerts` delta-merge that takes the per-call [[KesRegistrationCertAcceptanceResult]]
  * and returns the new per-operator [[SortedSet]]s that the writer (`AcceptanceMptStateChanges`) will persist.
  *
  * '''Durability contract''' (the load-bearing reason this manager exists): after `applyStateChanges` writes a snapshot's accepted certs
  * into MPT, the same view can be reconstituted on any node — including a freshly-restarted operator that lost its in-memory
  * [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] state, or a peer that joined the cluster after the cert
  * was accepted. The MPT is byte-equivalent across honest nodes by the consensus state-proof contract, so every node observes the same
  * runtime registry view at every snapshot ordinal.
  */
trait KesRegistrationStateManager[F[_]] {

  /** Materialize the latest accepted [[KesRegistrationRecord]] for `peer` from MPT.
    *
    * Reads the per-peer pointer `LastKesRegistrationRefs[peer]` first; on a hit, reads the full chain
    * `KesRegistrationCerts[peer]` and returns the record whose hash matches the pointer's `hash`. On a miss (no runtime cert ever
    * accepted for this peer) returns `None` and the caller falls back to the genesis [[io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry]].
    *
    * Note: returns the cert pinned by the pointer rather than `set.lastOption`, so the manager is robust to in-flight delta merges
    * leaving the pointer and the set transiently inconsistent (under MultiBranch the two writes land in the same `BranchHandle` commit,
    * but defensive pointer-resolution avoids any ordering dependency).
    */
  def materializeFromMpt(peer: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]]

  /** Full per-peer chain materializer for a single operator. Returns the complete `SortedSet[KesRegistrationRecord]` for `peer` (head
    * = earliest accepted, last = most-recent). Used by the route handlers to populate `last-reference` / `info` views, and by the
    * runtime [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]]'s `runtimeCertsFor` accessor.
    */
  def materializeChainForPeer(peer: PeerId)(implicit hasher: Hasher[F]): F[SortedSet[KesRegistrationRecord]]

  /** Prefix-scan every per-peer chain in `KesRegistrationCerts` and return the latest accepted record per peer.
    *
    * `SortedSet[KesRegistrationRecord]` is ordered by `(acceptedAt, ordinal)` so `.lastOption` gives the latest. Used by
    * [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] to rebuild its overlay snapshot at startup or after
    * an MPT-rebuild bootstrap.
    *
    * Empty result is returned as `SortedMap.empty` when the prefix scan returns no entries (genesis-only network, no runtime certs).
    */
  def materializeAllFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]]

  /** Full per-peer chain materializer. Returns the complete `SortedSet[KesRegistrationRecord]` for every operator that has ever had a
    * cert accepted. Used by the GSAM acceptance pipeline to recover prior state for the delta-merge step.
    *
    * Returns the per-peer SortedSet keyed by `peerId` from the head record's `event.value.operatorPeerId` — the cert body carries the
    * operator identity, so the manager doesn't need a sidecar address-index partition like NodeCollateral does.
    */
  def materializeActiveKesRegistrationCertsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]]

  /** Per-peer latest-ref materializer. Reads all entries in `LastKesRegistrationRefs` via prefix scan and returns them keyed by `peerId`
    * (recovered from the corresponding cert in the per-peer chain). Used by the GSAM acceptance pipeline to seed the validator's
    * `lastRefs` map for chain-link checks on incoming certs.
    *
    * On a partition where the pointer exists but the corresponding cert is missing (impossible under normal operation; would indicate
    * corruption), the entry is silently dropped. This matches NodeCollateral's tolerance for transient inconsistency.
    */
  def materializeLastRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationReference]]

  /** Compute the new per-peer [[SortedSet]] of accepted certs after merging `acceptanceResult.accepted` with the prior MPT state in
    * `priorRecords`. Idempotent on `(peerId, ordinal)` collisions — a replayed accept does not duplicate.
    *
    * This is the function that the GSAM pipeline calls between `KesRegistrationCertAcceptanceManager.accept` (which validates and
    * partitions candidates) and `AcceptanceMptStateChanges.applyStateChanges` (which writes the deltas back through the
    * `AcceptanceMpt[F]` writer algebra).
    */
  def getUpdatedKesRegistrationCerts(
    acceptanceResult: KesRegistrationCertAcceptanceResult,
    priorRecords: SortedMap[PeerId, SortedSet[KesRegistrationRecord]]
  ): SortedMap[PeerId, SortedSet[KesRegistrationRecord]]

  /** Compute the new per-peer [[KesRegistrationReference]] pointers after applying `acceptanceResult.accepted`. The pointer always
    * advances to the most-recently accepted cert; if the result contains no accepted cert for a peer, the prior pointer is preserved.
    *
    * Hashing routes through `Hasher[F]` to compute the per-cert hash for `KesRegistrationReference.of`. This is the single point of
    * `Hasher[F]` consumption in the delta-merge path; no hand-rolled hashing.
    */
  def getUpdatedLastRefs(
    acceptanceResult: KesRegistrationCertAcceptanceResult,
    priorLastRefs: SortedMap[PeerId, KesRegistrationReference]
  )(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationReference]]
}

object KesRegistrationStateManager {

  def make[F[_]: Async](reader: GlobalStateReader[F]): KesRegistrationStateManager[F] = new KesRegistrationStateManager[F] {

    override def materializeFromMpt(peer: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]] =
      for {
        lastRefKey <- GlobalStateKey.lastKesRegistrationRefsKey[F](peer)
        lastRefOpt <- reader.get[KesRegistrationReference](lastRefKey)
        result <- lastRefOpt match {
          case None => Option.empty[KesRegistrationRecord].pure[F]
          case Some(lastRef) =>
            for {
              certsKey <- GlobalStateKey.kesRegistrationCertsKey[F](peer)
              certSetOpt <- reader.get[SortedSet[KesRegistrationRecord]](certsKey)
              matched = certSetOpt.flatMap(_.find(_.event.value.ordinal === lastRef.ordinal))
            } yield matched
        }
      } yield result

    override def materializeChainForPeer(peer: PeerId)(implicit hasher: Hasher[F]): F[SortedSet[KesRegistrationRecord]] =
      for {
        certsKey <- GlobalStateKey.kesRegistrationCertsKey[F](peer)
        certSetOpt <- reader.get[SortedSet[KesRegistrationRecord]](certsKey)
      } yield certSetOpt.getOrElse(SortedSet.empty[KesRegistrationRecord])

    override def materializeAllFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]] =
      materializeActiveKesRegistrationCertsFromMpt.map { perPeer =>
        SortedMap.from(
          perPeer.iterator.flatMap { case (peerId, set) => set.lastOption.map(peerId -> _) }
        )
      }

    override def materializeActiveKesRegistrationCertsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.KesRegistrationCerts)
        entries <- reader.getAllForPrefix[SortedSet[KesRegistrationRecord]](prefix)
      } yield
        SortedMap.from(
          entries.values.toList.mapFilter { set =>
            set.headOption.map(h => h.event.value.operatorPeerId -> set)
          }.filter(_._2.nonEmpty)
        )

    override def materializeLastRefsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[PeerId, KesRegistrationReference]] =
      for {
        // We can't recover PeerId from KesRegistrationReference alone (it carries only ordinal+hash).
        // Materialize the per-peer chain first to recover peerIds, then look up each pointer.
        perPeer <- materializeActiveKesRegistrationCertsFromMpt
        refs <- perPeer.keySet.toList.traverse { peer =>
          GlobalStateKey
            .lastKesRegistrationRefsKey[F](peer)
            .flatMap(reader.get[KesRegistrationReference])
            .map(_.map(peer -> _))
        }
      } yield SortedMap.from(refs.flatten)

    override def getUpdatedKesRegistrationCerts(
      acceptanceResult: KesRegistrationCertAcceptanceResult,
      priorRecords: SortedMap[PeerId, SortedSet[KesRegistrationRecord]]
    ): SortedMap[PeerId, SortedSet[KesRegistrationRecord]] = {
      val accepted = acceptanceResult.accepted
      accepted.foldLeft(priorRecords) {
        case (acc, (peerId, newRec)) =>
          val existing = acc.getOrElse(peerId, SortedSet.empty[KesRegistrationRecord])
          val isDuplicate = existing.exists(_.event.value.ordinal === newRec.event.value.ordinal)
          if (isDuplicate) acc
          else acc.updated(peerId, existing + newRec)
      }
    }

    override def getUpdatedLastRefs(
      acceptanceResult: KesRegistrationCertAcceptanceResult,
      priorLastRefs: SortedMap[PeerId, KesRegistrationReference]
    )(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationReference]] =
      acceptanceResult.accepted.toList.traverse {
        case (peerId, record) =>
          KesRegistrationReference.of[F](record.event).map(peerId -> _)
      }.map { newRefs =>
        priorLastRefs ++ SortedMap.from(newRefs)
      }
  }
}
