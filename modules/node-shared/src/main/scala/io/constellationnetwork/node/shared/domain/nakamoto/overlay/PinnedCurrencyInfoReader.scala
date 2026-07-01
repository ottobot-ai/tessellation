package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Parallel
import cats.effect.Async
import cats.syntax.all._

import io.constellationnetwork.currency.schema.currency.CurrencySnapshotInfo
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReaderOps.GlobalStateReaderTypedOps
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.schema.{GlobalIncrementalSnapshot, GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.storages.MptStateStorage
import io.constellationnetwork.security.{Hashed, Hasher}

import org.typelevel.log4cats.slf4j.Slf4jLogger

/** Track-1 blocker-2a — a version-retained, BY-ORDINAL, per-metagraph `CurrencySnapshotInfo` reader.
  *
  * Reads a metagraph's global-committed currency state AT A PINNED ANCHOR `(ordinal, expectedGlobalSnapshotHash)`, i.e. the exact global
  * snapshot recorded in a currency snapshot's `globalSyncView`. It replaces the HEAD read (`lastGlobalSnapshotStorage.getCombined`) that
  * today's per-MG derivation performs — HEAD is non-deterministic across nodes and can run ahead of the pinned view, so a re-executing
  * committee/watchtower (Track-1 re-exec primary) and a byteDiff-adopting follower would read a DIFFERENT prior than the metagraph's own
  * `accept()` saw. Pinning to the anchor makes the prior deterministic and Byzantine-checkable.
  *
  * '''HARD-REJECT contract (returns `None`, NEVER a HEAD fallback):'''
  *   1. no global snapshot resolvable at `ordinal`;
  *   1. the resolved snapshot's hash ≠ `expectedGlobalSnapshotHash` (we're on a fork, or it's not the pinned snapshot);
  *   1. the pinned snapshot carries no committed `stateProof.mptRoot` (BFT / pre-MPT — nothing to byte-verify against);
  *   1. no version-retained state bytes at `ordinal` (evicted by the backing store's retention — see below);
  *   1. the retained bytes' consensus `sidecarFreeMptRoot` ≠ the pinned snapshot's `stateProof.mptRoot` (bytes do not reproduce the pinned
  *      committed root).
  *
  * A `None` from a clean verify (the metagraph simply had no reconstructible state at the verified anchor) is indistinguishable from a
  * reject by design: the future I-PIN consumer treats every `None` as "no pinned prior — hard reject", never as "read HEAD instead".
  *
  * '''Retention (version depth this reader can serve depends ENTIRELY on the injected `byteStore`):'''
  *   - gl0 produce/validate rail: `byteStore` = the `signedBytesStore` (`<mptSnapshotInfoPath>_signed`,
  *     `ContiguousOrdinalCutoff(keepDepthBehindFinalized)`) — a CONTIGUOUS window of k₂ = 100·k₁ finalized ordinals after Track-3 S2
  *     (raised from the old, stale 512), whose bytes reproduce the signed `mptRoot` by construction. Serves any anchor within k₂ of the
  *     finalized tip; only anchors deeper than the k₂ absolute floor hard-reject. (S2 raised the DISK depth; the deep-revert EXECUTOR that
  *     consumes it is S4.)
  *   - follower `createContext` rail (cl0/dl1): `byteStore` = a read-only view over the producer's `mpt_snapshot_info` store, which prunes
  *     with `LogarithmicOrdinalCutoff` — a SPARSE, gappy retention below the head. By-ordinal reads at an arbitrary past ordinal MISS
  *     unless that ordinal happens to sit on the logarithmic ladder ⇒ this rail hard-rejects most anchors. '''Track-3 S2 decision: the
  *     follower contiguous/disk-backed store is DEFERRED''' — followers do not yet consume deep anchors (that need lands with
  *     byteDiff-adopt / diff-base-pin), so building it now is scaffolding ahead of the blocker. It is NOT a silent shallow ship: this
  *     reader HARD-REJECTS a deep follower anchor (returns `None`, never a HEAD fallback), so a follower that cannot serve the anchor
  *     defers/re-pulls rather than adopting wrong bytes. Surfaced here + at the `SharedServices` `pinnedByteStore` wiring, not worked
  *     around.
  *
  * The reader is stateless: each `readAt` loads the retained byte map into a throwaway in-memory `MptStore` and reconstructs ONLY the
  * requested metagraph's `CurrencySnapshotInfo` via the exact same unrolled-partition inverse
  * (`GlobalStateReaderOps.getCurrencySnapshotInfo`) every other read path uses — so the result is byte-identical to a live finalized-reader
  * read of the same committed bytes. No trie is built (reads scan the raw byte map), but the whole global byte map is materialized per
  * call; a consumer that reads the same anchor repeatedly (e.g. once per `accept()`) should cache the result.
  */
trait PinnedCurrencyInfoReader[F[_]] {

  /** Read `metagraphId`'s `CurrencySnapshotInfo` at the pinned global anchor. See the class scaladoc for the hard-reject contract. */
  def readAt(
    ordinal: SnapshotOrdinal,
    expectedGlobalSnapshotHash: Hash,
    metagraphId: Address
  ): F[Option[CurrencySnapshotInfo]]

  /** Read `metagraphId`'s gl0-maintained cross-shard `MetagraphSyncDataInfo` (the fieldId-18 hypergraph partition — the set of unapplied
    * global-change ordinals for this metagraph) at the pinned global anchor.
    *
    * This is the SIBLING of [[readAt]] used by Track-1 I-PIN: `CurrencySnapshotAcceptanceManager.accept` must gate which cross-shard
    * `SpendAction`s it folds off the state committed at the RECORDED `globalSyncView`, not off the node-local head. `MetagraphSyncDataInfo`
    * is NOT part of `CurrencySnapshotInfo` (it lives in `GlobalSnapshotInfo.metagraphSyncData`, MPT fieldId 18, keyed per metagraph
    * address), so it needs its own accessor rather than a field of [[readAt]]'s result. Same pin+verify+hard-reject contract as [[readAt]]
    * (returns `None` on any miss — no head fallback); a clean `None` means the metagraph has no unapplied cross-shard state at the verified
    * anchor.
    */
  def readMetagraphSyncDataAt(
    ordinal: SnapshotOrdinal,
    expectedGlobalSnapshotHash: Hash,
    metagraphId: Address
  ): F[Option[MetagraphSyncDataInfo]]
}

object PinnedCurrencyInfoReader {

  /** @param byteStore
    *   the version-retained, per-ordinal state-bytes store this reader reads at the anchor. Its retention (contiguous depth vs logarithmic
    *   gaps) is exactly the version depth `readAt` can serve — see the class scaladoc's retention section.
    * @param getGlobalSnapshotByOrdinal
    *   resolves the FINALIZED global snapshot at an ordinal (carries `hash` for the pin + `stateProof.mptRoot` for the byte-verify). gl0
    *   wires its finalized-chain lookup (`getGlobalSnapshotByOrdinalWithFallback`); followers wire their
    *   `lastNGlobalSnapshot.getByOrdinal`.
    */
  def make[F[_]: Async: Parallel: Hasher: JsonSerializer](
    byteStore: MptStateStorage[F],
    getGlobalSnapshotByOrdinal: SnapshotOrdinal => F[Option[Hashed[GlobalIncrementalSnapshot]]]
  ): PinnedCurrencyInfoReader[F] = new PinnedCurrencyInfoReader[F] {

    private val logger = Slf4jLogger.getLoggerFromName[F]("PinnedCurrencyInfoReader")

    def readAt(
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      metagraphId: Address
    ): F[Option[CurrencySnapshotInfo]] =
      withVerifiedAnchorBytes(ordinal, expectedGlobalSnapshotHash, metagraphId, "CurrencySnapshotInfo")(
        reconstructPerMgInfo(_, metagraphId)
      )

    def readMetagraphSyncDataAt(
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      metagraphId: Address
    ): F[Option[MetagraphSyncDataInfo]] =
      withVerifiedAnchorBytes(ordinal, expectedGlobalSnapshotHash, metagraphId, "MetagraphSyncDataInfo")(
        reconstructMetagraphSyncData(_, metagraphId)
      )

    /** Pin to the EXACT canonical snapshot at `ordinal`, verify the retained state bytes reproduce its committed `mptRoot`, and hand them
      * to `reconstruct`. Returns `None` (NEVER a HEAD fallback) on any miss along the way — no resolvable snapshot at `ordinal`, hash ≠ the
      * pinned hash (a fork's snapshot, or one this node can't resolve), no committed `mptRoot` (BFT/pre-MPT), retained bytes
      * evicted/absent, or retained bytes that do not recompute the pinned root. Shared by [[readAt]] and [[readMetagraphSyncDataAt]] so
      * both honor one pin+verify+hard-reject contract; they differ only in which partition they reconstruct from the verified bytes.
      */
    private def withVerifiedAnchorBytes[A](
      ordinal: SnapshotOrdinal,
      expectedGlobalSnapshotHash: Hash,
      metagraphId: Address,
      what: String
    )(reconstruct: Map[Hex, Array[Byte]] => F[Option[A]]): F[Option[A]] =
      getGlobalSnapshotByOrdinal(ordinal).flatMap {
        case Some(snap) if snap.hash === expectedGlobalSnapshotHash =>
          snap.signed.value.stateProof.mptRoot match {
            case None =>
              logger.debug(
                s"[2a] pinned snapshot ord=${ordinal.show} carries no committed mptRoot (BFT/pre-MPT) — hard-reject $what for mg=${metagraphId.show}"
              ) >> none[A].pure[F]
            case Some(expectedMptRoot) =>
              byteStore.readState(ordinal).flatMap {
                case None =>
                  // Retained state bytes evicted/absent at the anchor (store retention doesn't reach this depth) — hard-reject, no fallback.
                  logger.debug(
                    s"[2a] no retained state bytes at pinned ord=${ordinal.show} (evicted/absent) — hard-reject $what for mg=${metagraphId.show}"
                  ) >> none[A].pure[F]
                case Some(bytes) =>
                  GlobalSnapshotInfo.sidecarFreeMptRoot[F](bytes).flatMap { computedRoot =>
                    if (computedRoot =!= expectedMptRoot)
                      // Retained bytes do NOT reproduce the pinned snapshot's committed root (wrong branch / corrupt) — hard-reject.
                      logger.debug(
                        s"[2a] retained bytes at pinned ord=${ordinal.show} recompute mptRoot=${computedRoot.show} ≠ pinned " +
                          s"stateProof.mptRoot=${expectedMptRoot.show} — hard-reject $what for mg=${metagraphId.show}"
                      ) >> none[A].pure[F]
                    else
                      reconstruct(bytes)
                  }
              }
          }
        case _ =>
          logger.debug(
            s"[2a] no canonical snapshot at pinned ord=${ordinal.show} matching hash=${expectedGlobalSnapshotHash.show} — hard-reject $what for mg=${metagraphId.show}"
          ) >> none[A].pure[F]
      }

    /** Load the verified byte map into a throwaway in-memory store and reconstruct ONLY `metagraphId`'s `CurrencySnapshotInfo`. Reuses the
      * exact unrolled-partition inverse every finalized-reader path uses, so the result is byte-identical to a live read of these bytes.
      */
    private def reconstructPerMgInfo(
      bytes: Map[Hex, Array[Byte]],
      metagraphId: Address
    ): F[Option[CurrencySnapshotInfo]] =
      for {
        producer <- InMemoryMerklePatriciaProducer.make[F](bytes)
        store <- MptStore.make[F, GlobalStateKey](producer, GlobalStateKey.toHex[F])
        info <- GlobalStateReader.fromMptStore[F](store).getCurrencySnapshotInfo(metagraphId)
      } yield info

    /** Load the verified byte map into a throwaway in-memory store and read ONLY `metagraphId`'s fieldId-18 `MetagraphSyncDataInfo`
      * (`GlobalStateConverter.syntax`'s `getMetagraphSyncData` — the exact hypergraph-partition accessor every finalized reader uses), so
      * the result is byte-identical to a live read of these bytes.
      */
    private def reconstructMetagraphSyncData(
      bytes: Map[Hex, Array[Byte]],
      metagraphId: Address
    ): F[Option[MetagraphSyncDataInfo]] =
      for {
        producer <- InMemoryMerklePatriciaProducer.make[F](bytes)
        store <- MptStore.make[F, GlobalStateKey](producer, GlobalStateKey.toHex[F])
        info <- store.getMetagraphSyncData(metagraphId)
      } yield info
  }
}
