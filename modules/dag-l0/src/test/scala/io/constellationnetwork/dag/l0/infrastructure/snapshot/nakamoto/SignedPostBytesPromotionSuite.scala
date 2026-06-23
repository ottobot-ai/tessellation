package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.generators.addressGen
import io.constellationnetwork.schema.mpt.{GlobalStateKey, WithdrawalTimeLimit}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.storages.MptStateStorage

import eu.timepit.refined.types.numeric.NonNegLong
import fs2.io.file.Files
import weaver.MutableIOSuite

/** Unit gate for the 3c-A seal-time signed-postBytes enabler (`docs/serde/FINISH-3C-EXECUTION-PLAN.md` §3c-A / §6).
  *
  * The enabler stages the EXACT signed `postBytes` at the `overlay.commit` site keyed by snapshot hash, then `SnapshotLeaderLoop` promotes
  * ONLY the FINALIZED hash's bytes into the served signed-bytes store (`mpt_snapshot_info_signed`), watermark-pruning every other staged
  * entry at-or-below the finalized tip. This suite drives the EXACT production helpers (`SnapshotLeaderLoop.rekeyStagedPostBytes` /
  * `pruneStagedPostBytesAtOrBelow`) — the same functions `onSlotWon` / the validator / `recordFinalizedAccumulator` now call — plus the
  * `MptStateStorage` round-trip, to pin two properties compilation can't:
  *   1. '''reorg-replace''' — only the finalized branch's bytes survive a finalize tick; same-ordinal forks + below-tip dead forks are
  *      dropped. 2. '''persisted bytes reproduce the signed `mptRoot`''' — `sidecarFreeMptRoot(readBack) === mptStateProofFromBytes(info,
  *      bytes).mptRoot`, so a follower's verify gate passes BY CONSTRUCTION on the served signed bytes.
  *
  * The capture-equals-signed-postBytes property (the overlay handle view at the stage site == GSAM's signed bytes) is the same-handle
  * invariant verified at the seam + proven by the sharded e2e; it requires the full MultiBranch overlay + accept pipeline and is out of
  * unit scope (same rationale as `ChangeSetRingPromotionSuite`).
  */
object SignedPostBytesPromotionSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO], MptStateStorage[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
      dir <- Files[IO].tempDirectory
      store <- MptStateStorage.make[IO](dir / "mpt_signed_snapshot_info").toResource
    } yield (h, sp, j, store)

  private def ord(n: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(n))
  private def bytesFor(seed: Int): Map[Hex, Array[Byte]] = Map(Hex(f"$seed%02x" * 4) -> Array[Byte](seed.toByte, 1, 2, 3))

  // Distinct raw-artifact vs with-cert canonical hashes (the difference is the whole rekey bug class).
  private val rawHash: Hash = Hash("11" * 32)
  private val withCertHash: Hash = Hash("22" * 32)

  /** A small but non-trivial GSI → its exact hex-keyed user-field byte map (the shape the producer signs `mptRoot` over). */
  private def userBytes(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[(GlobalSnapshotInfo, Map[Hex, Array[Byte]])] = {
    import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
    val addrs = (0 until 5).map(_ => addressGen.sample.get).distinct.toList
    val gsi = GlobalSnapshotInfo.empty.copy(
      balances = SortedMap.from(addrs.zipWithIndex.map { case (a, i) => a -> Balance(NonNegLong.unsafeFrom((i + 1) * 1000L)) })
    )
    gsi.allStateEntriesAsBytes.flatMap { typed =>
      typed.toList.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_ -> v) }.map(pairs => (gsi, pairs.toMap))
    }
  }

  test("produce-path rekey: signed bytes staged under the RAW hash surface under the WITH-CERT hash after rekey") { _ =>
    val staged: Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])] = Map(rawHash -> ((ord(5L), bytesFor(5))))
    val rekeyed = SnapshotLeaderLoop.rekeyStagedPostBytes(staged, rawHash, withCertHash)
    IO.pure(
      expect.same(rekeyed.get(withCertHash).map(_._2.keySet), Some(bytesFor(5).keySet)) && // promote lookup (with-cert) now hits
        expect.same(rekeyed.get(rawHash), None) && // raw key removed — no leak
        // guard: WITHOUT the rekey the with-cert promote lookup misses → bytes never promote (the inert-without-rekey bug)
        expect.same(staged.get(withCertHash), None)
    )
  }

  test("reorg-replace: a finalize tick keeps only strictly-above entries; same-ordinal forks + below-tip dead forks drop") { _ =>
    val hPromoted: Hash = Hash("33" * 32) // the snapshot finalized at the tip
    val hForkAtTip: Hash = Hash("44" * 32) // a LOSING fork at the finalized ordinal — must drop
    val hBelow: Hash = Hash("55" * 32) // a dead fork below the tip — must drop
    val hAbove: Hash = Hash("66" * 32) // still in flight — must survive

    val staged: Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])] = Map(
      hPromoted -> ((ord(10L), bytesFor(1))),
      hForkAtTip -> ((ord(10L), bytesFor(2))),
      hBelow -> ((ord(7L), bytesFor(3))),
      hAbove -> ((ord(11L), bytesFor(4)))
    )
    // Mirrors `recordFinalizedAccumulator`'s atomic modify: pull the finalized hash's bytes, prune the rest.
    val promoted = staged.get(hPromoted).map(_._2)
    val pruned = SnapshotLeaderLoop.pruneStagedPostBytesAtOrBelow(staged, ord(10L))
    IO.pure(
      expect.same(promoted.map(_.keySet), Some(bytesFor(1).keySet)) && // only the finalized branch's bytes promote
        expect.same(pruned.keySet, Set(hAbove)) && // strictly-above survives
        expect(!pruned.contains(hForkAtTip)) && // reorg loser at the same ordinal is dropped (never served)
        expect(!pruned.contains(hBelow)) // below-tip dead fork dropped
    )
  }

  test("persisted bytes reproduce the signed mptRoot: sidecarFreeMptRoot(readBack) === mptStateProofFromBytes(info, bytes).mptRoot") {
    res =>
      implicit val (h, _, j, store) = res
      for {
        gsiAndBytes <- userBytes
        (gsi, signedBytes) = gsiAndBytes
        // The producer's signed global root (the trust anchor the follower verifies against).
        signedRoot <- GlobalSnapshotInfo.mptStateProofFromBytes[IO](gsi, signedBytes).map(_.mptRoot)
        // The enabler writes the SIGNED bytes verbatim to the served store at the finalized ordinal; the follower reads them back.
        _ <- store.writeState(ord(42L), signedBytes)
        readBack <- store.readState(ord(42L))
        recomputed <- readBack.traverse(GlobalSnapshotInfo.sidecarFreeMptRoot[IO])
      } yield
        expect(readBack.isDefined) &&
          expect.same(readBack.map(_.keySet), Some(signedBytes.keySet)) && // store round-trips the byte set losslessly
          expect(signedRoot.isDefined) &&
          // the served store's recompute equals the producer's SIGNED mptRoot — the follower verify gate passes BY CONSTRUCTION
          expect.same(recomputed, signedRoot)
  }
}
