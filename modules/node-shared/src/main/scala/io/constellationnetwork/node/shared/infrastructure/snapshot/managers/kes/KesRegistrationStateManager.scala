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
  * Reader/delta primitives for the target post-genesis unified KES+VRF registry. Records use the `KesRegistrationCerts` partition
  * (per-operator [[SortedSet]]) with a parallel `LastKesRegistrationRefs` pointer (per-operator latest [[KesRegistrationReference]]) for
  * exact chain-tip lookup.
  *
  * Mirrors `NodeCollateralStateManager` 1:1 in shape: the trait exposes materializers that the GSAM acceptance pipeline calls to recover
  * prior state from MPT, plus a `getUpdatedKesRegistrationCerts` delta-merge that takes the per-call
  * [[KesRegistrationCertAcceptanceResult]] and returns the new per-operator [[SortedSet]]s that the writer (`AcceptanceMptStateChanges`)
  * will persist.
  *
  * GSAM selects and validates registration events against the exact candidate parent, then atomically writes the resulting record set and
  * pointer through the same branch handle as every other rooted state change. Runtime eligibility still requires consumers to resolve this
  * branch-historical registry rather than a live or wire-carried key.
  */
trait KesRegistrationStateManager[F[_]] {

  /** Materialize the latest accepted [[KesRegistrationRecord]] for `peer` from MPT.
    *
    * Reads the per-peer pointer `LastKesRegistrationRefs[peer]` first; on a hit, reads the full chain `KesRegistrationCerts[peer]` and
    * returns the record whose hash matches the pointer's `hash`. On a miss (no runtime cert ever accepted for this peer) returns `None` and
    * the caller falls back to the genesis [[io.constellationnetwork.node.shared.domain.nakamoto.KesRegistry]].
    *
    * Note: returns the cert pinned by the pointer rather than `set.lastOption`, so the manager is robust to in-flight delta merges leaving
    * the pointer and the set transiently inconsistent (under MultiBranch the two writes land in the same `BranchHandle` commit, but
    * defensive pointer-resolution avoids any ordering dependency).
    */
  def materializeFromMpt(peer: PeerId)(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]]

  /** Full per-peer chain materializer for a single operator. Returns the complete `SortedSet[KesRegistrationRecord]` for `peer` (head \=
    * earliest accepted, last = most-recent). Used by the route handlers to populate `last-reference` / `info` views, and by the runtime
    * [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]]'s `runtimeCertsFor` accessor.
    */
  def materializeChainForPeer(peer: PeerId)(implicit hasher: Hasher[F]): F[SortedSet[KesRegistrationRecord]]

  /** Prefix-scan every per-peer chain in `KesRegistrationCerts` and resolve each exact latest-reference pointer.
    *
    * Both pointer ordinal and pointer hash must match exactly; zero or multiple matches fail closed for that peer. Used by
    * [[io.constellationnetwork.node.shared.domain.nakamoto.kes.MutableKesRegistry]] to rebuild its overlay snapshot at startup or after an
    * MPT-rebuild bootstrap.
    *
    * Empty result is returned as `SortedMap.empty` when the prefix scan returns no entries (genesis-only network, no runtime certs).
    */
  def materializeAllFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]]

  /** Full per-peer chain materializer. Returns the complete `SortedSet[KesRegistrationRecord]` for every operator that has ever had a cert
    * accepted. Used by the GSAM acceptance pipeline to recover prior state for the delta-merge step.
    *
    * A partition entry is usable only when every record names the same operator, the actual MPT key equals that operator's canonical
    * derived key, and exactly one entry in the prefix scan claims that operator. Mixed sets, empty sets, misplaced entries, and duplicate
    * homogeneous claims raise deterministic corruption; they are never silently omitted, because omission could erase permanent ownership
    * and permit a duplicate key registration.
    */
  def materializeActiveKesRegistrationCertsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]]

  /** Per-peer latest-ref materializer. Reads all entries in `LastKesRegistrationRefs` via prefix scan and returns them keyed by `peerId`
    * (recovered from the corresponding cert in the per-peer chain). Used by the GSAM acceptance pipeline to seed the validator's `lastRefs`
    * map for chain-link checks on incoming certs.
    *
    * On a partition where the pointer has zero or multiple exact record matches, the entry is dropped fail-closed as corrupted/ambiguous.
    */
  def materializeLastRefsFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationReference]]

  /** Compute the new per-peer [[SortedSet]] of accepted certs after merging `acceptanceResult.accepted` with the prior MPT state in
    * `priorRecords`. Idempotent on `(peerId, ordinal)` collisions — a replayed accept does not duplicate.
    *
    * This is the function that the GSAM pipeline calls between `KesRegistrationCertAcceptanceManager.accept` (which validates and
    * partitions candidates) and `AcceptanceMptStateChanges.applyStateChanges` (which writes the deltas back through the `AcceptanceMpt[F]`
    * writer algebra).
    */
  def getUpdatedKesRegistrationCerts(
    acceptanceResult: KesRegistrationCertAcceptanceResult,
    priorRecords: SortedMap[PeerId, SortedSet[KesRegistrationRecord]]
  ): SortedMap[PeerId, SortedSet[KesRegistrationRecord]]

  /** Compute the new per-peer [[KesRegistrationReference]] pointers after applying `acceptanceResult.accepted`. The pointer always advances
    * to the most-recently accepted cert; if the result contains no accepted cert for a peer, the prior pointer is preserved.
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

    private def resolveExactReference(
      ref: KesRegistrationReference,
      records: SortedSet[KesRegistrationRecord]
    )(implicit hasher: Hasher[F]): F[Option[KesRegistrationRecord]] =
      records.toList.traverse { record =>
        KesRegistrationReference.of[F](record.event).map(_ -> record)
      }.map { referenced =>
        referenced.collect { case (candidateRef, record) if candidateRef === ref => record } match {
          case record :: Nil => record.some
          case _             => none[KesRegistrationRecord]
        }
      }

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
              matched <- certSetOpt.fold(none[KesRegistrationRecord].pure[F])(resolveExactReference(lastRef, _))
            } yield matched
        }
      } yield result

    override def materializeChainForPeer(peer: PeerId)(implicit hasher: Hasher[F]): F[SortedSet[KesRegistrationRecord]] =
      for {
        certsKey <- GlobalStateKey.kesRegistrationCertsKey[F](peer)
        certSetOpt <- reader.get[SortedSet[KesRegistrationRecord]](certsKey)
      } yield certSetOpt.getOrElse(SortedSet.empty[KesRegistrationRecord])

    /** Pointer-canonical materializer (aligned with `materializeFromMpt`). For each peer that has a `LastKesRegistrationRefs` entry,
      * resolves the unique cert whose computed `(ordinal, hash)` exactly equals the pointer. Missing or ambiguous matches fail closed for
      * that peer.
      *
      * Why the alignment matters: under MultiBranch a delta merge can transiently leave the pointer and the set out of sync (different
      * branch handles, different write timings). The single-peer `materializeFromMpt` resolves this defensively via the pointer; the
      * pre-fix version of `materializeAllFromMpt` used `set.lastOption` and could disagree. Aligning the two means observers get the same
      * canonical "current cert" view regardless of which materializer they use.
      */
    override def materializeAllFromMpt(implicit hasher: Hasher[F]): F[SortedMap[PeerId, KesRegistrationRecord]] =
      for {
        perPeer <- materializeActiveKesRegistrationCertsFromMpt
        resolved <- perPeer.toList.traverse {
          case (peer, records) =>
            for {
              refKey <- GlobalStateKey.lastKesRegistrationRefsKey[F](peer)
              refOpt <- reader.get[KesRegistrationReference](refKey)
              matched <- refOpt.fold(none[KesRegistrationRecord].pure[F])(resolveExactReference(_, records))
            } yield matched.map(peer -> _)
        }
      } yield SortedMap.from(resolved.flatten)

    override def materializeActiveKesRegistrationCertsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]] =
      for {
        prefix <- GlobalStateKey.hypergraphFieldPrefixAcrossContracts[F](GlobalStateFieldId.KesRegistrationCerts)
        entries <- reader.getAllForPrefix[SortedSet[KesRegistrationRecord]](prefix)
        classified <- entries.toList.sortBy(_._1.value).traverse {
          case (actualKey, records) =>
            val peers = records.toList.map(_.event.value.operatorPeerId).toSet
            peers.toList match {
              case peer :: Nil =>
                GlobalStateKey
                  .kesRegistrationCertsKey[F](peer)
                  .flatMap(GlobalStateKey.toHex[F])
                  .map(expectedKey => (actualKey, records, peers, (peer -> expectedKey).some))
              case _ =>
                (actualKey, records, peers, Option.empty[(PeerId, io.constellationnetwork.security.hex.Hex)]).pure[F]
            }
        }
        result <- {
          val homogeneousByPeer = classified.collect {
            case (actualKey, records, _, Some((peer, expectedKey))) if actualKey === expectedKey => peer -> records
          }.groupBy(_._1)
          val malformedEntryCount = classified.count { case (_, _, peers, _) => peers.sizeCompare(1) != 0 }
          val misplacedOperators = classified.collect {
            case (actualKey, _, _, Some((peer, expectedKey))) if actualKey =!= expectedKey => peer
          }.distinct.sorted
          val duplicateOperators = homogeneousByPeer.collect {
            case (peer, claims) if claims.sizeCompare(1) > 0 => peer
          }.toList.sorted

          if (malformedEntryCount > 0 || misplacedOperators.nonEmpty || duplicateOperators.nonEmpty)
            Async[F].raiseError[SortedMap[PeerId, SortedSet[KesRegistrationRecord]]](
              new IllegalStateException(
                s"Corrupt KES+VRF registration partition: malformedEntries=$malformedEntryCount " +
                  s"misplacedOperators=${misplacedOperators.mkString(",")} " +
                  s"duplicateOperators=${duplicateOperators.mkString(",")}"
              )
            )
          else
            SortedMap
              .from(homogeneousByPeer.toList.collect { case (peer, (_, records) :: Nil) => peer -> records })
              .pure[F]
        }
      } yield result

    override def materializeLastRefsFromMpt(
      implicit hasher: Hasher[F]
    ): F[SortedMap[PeerId, KesRegistrationReference]] =
      for {
        // We can't recover PeerId from KesRegistrationReference alone (it carries only ordinal+hash).
        // Materialize the per-peer chain first to recover peerIds, then look up each pointer.
        perPeer <- materializeActiveKesRegistrationCertsFromMpt
        refs <- perPeer.toList.traverse {
          case (peer, records) =>
            GlobalStateKey
              .lastKesRegistrationRefsKey[F](peer)
              .flatMap(reader.get[KesRegistrationReference])
              .flatMap {
                case None => none[(PeerId, KesRegistrationReference)].pure[F]
                case Some(ref) =>
                  resolveExactReference(ref, records).flatMap {
                    case Some(_) => (peer -> ref).some.pure[F]
                    case None    => none[(PeerId, KesRegistrationReference)].pure[F]
                  }
              }
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
        newRefs.foldLeft(priorLastRefs) {
          case (refs, (peerId, ref)) => refs.updated(peerId, ref)
        }
      }
  }
}
