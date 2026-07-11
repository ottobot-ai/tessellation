package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.cutoff.ContiguousOrdinalCutoff
import io.constellationnetwork.dag.l0.infrastructure.snapshot.GlobalSnapshotConsensus
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

  // Track-3 S2 (disk-backed k₂ retention). Two properties compilation can't pin:
  //   1. the signed-bytes store retains a CONTIGUOUS window to its configured depth — every ordinal in
  //      [current-depth+1, current] survives `applyCutoff`, with NO logarithmic gaps (a gap would 404 the
  //      3c-A serve route + hard-reject a 2a pinned anchor);
  //   2. `signedBytesRetentionDepth` is now the k₂ depth (`keepDepthBehindFinalized` = 100·k₁), not the old
  //      stale 512 — so the DISK tier reaches k₂ for the later S4 deep revert.
  // The depth here is kept small (5) purely so the on-disk write/cutoff round-trip is fast; the derivation
  // assertions cover the real k₂ magnitudes.
  // Signed-byte history can contain a hole when the canonical proposal's produce/validate bytes were not staged under its final hash. The
  // retained regression below proves `pinnedReaderAt` fails closed at that ordinal while neighboring retained states remain readable.

  /** A distinct GSI (one balance keyed off `seed`) + the EXACT hex byte map `syncFromGlobalSnapshotInfoVerifiedBytes` verifies/returns
    * (`toAllStateKeyValueBytes`), + its sidecar-free consensus root (what the snapshot at that ordinal commits as `stateProof.mptRoot`).
    */
  private def gsiFixture(
    seed: Long
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[(GlobalSnapshotInfo, Map[Hex, Array[Byte]], Hash)] = {
    import io.constellationnetwork.schema.mpt.GlobalStateConverter
    val acct = io.constellationnetwork.schema.address.Address.fromBytes(s"fidelity-acct-$seed".getBytes("UTF-8"))
    val gsi = GlobalSnapshotInfo.empty.copy(balances = SortedMap(acct -> Balance(NonNegLong.unsafeFrom(1000L + seed))))
    for {
      typed <- GlobalStateConverter.toAllStateKeyValueBytes[IO](gsi)
      bytes <- typed.toList.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_ -> v) }.map(_.toMap)
      root <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](bytes)
    } yield (gsi, bytes, root)
  }

  /** A minimal finalized `Hashed[GlobalIncrementalSnapshot]` at `ordinal` committing `mptRoot` — the resolver-side pin for `pinnedReaderAt`
    * (mirrors `PinnedCurrencyInfoReaderSuite.mkHashed`).
    */
  private def mkFinalizedSnapshot(ordinal: Long, mptRoot: Hash)(implicit h: Hasher[IO]): IO[Hashed[GlobalIncrementalSnapshot]] = {
    import cats.data.{NonEmptyList, NonEmptySet}
    import io.constellationnetwork.schema.epoch.EpochProgress
    import io.constellationnetwork.schema.height.{Height, SubHeight}
    import io.constellationnetwork.schema.peer.PeerId
    import io.constellationnetwork.schema.semver.SnapshotVersion

    import eu.timepit.refined.auto._
    import io.constellationnetwork.security.signature.Signed
    import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
    val unsigned = GlobalIncrementalSnapshot(
      ordinal = ord(ordinal),
      height = Height(NonNegLong(0L)),
      subHeight = SubHeight(NonNegLong(0L)),
      lastSnapshotHash = Hash.empty,
      blocks = scala.collection.immutable.SortedSet.empty,
      stateChannelSnapshots = SortedMap.empty,
      shardCheckpoints = SortedMap.empty,
      rewards = scala.collection.immutable.SortedSet.empty,
      delegateRewards = None,
      epochProgress = EpochProgress(NonNegLong(0L)),
      nextFacilitators = NonEmptyList.of(PeerId(Hex("0d" * 64))),
      tips = SnapshotTips(scala.collection.immutable.SortedSet.empty, scala.collection.immutable.SortedSet.empty),
      stateProof = GlobalSnapshotStateProof(
        Hash.empty,
        Hash.empty,
        Hash.empty,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        Some(mptRoot),
        None,
        None
      ),
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      spendActions = None,
      updateNodeParameters = None,
      artifacts = None,
      activeDelegatedStakes = None,
      delegatedStakesWithdrawals = None,
      activeNodeCollaterals = None,
      nodeCollateralWithdrawals = None,
      version = SnapshotVersion("0.0.1"),
      slotCertificate = None,
      eta = None
    )
    Signed(unsigned, NonEmptySet.of(SignatureProof(PeerId(Hex("0d" * 64)).toId, Signature(Hex("0e" * 64))))).toHashed[IO]
  }

  /** The finalize sink's postBytes leg VERBATIM (`SnapshotLeaderLoop.recordFinalizedAccumulator` L679-696): atomically pull the finalized
    * hash's staged bytes + watermark-prune, then write the store on a hit and SKIP on a miss (the skip is the hole mechanism).
    */
  private def finalizeSinkPostBytes(
    stagedRef: cats.effect.Ref[IO, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]],
    store: MptStateStorage[IO],
    ordinal: SnapshotOrdinal,
    finalizedHash: Hash
  ): IO[Unit] =
    stagedRef.modify { staged =>
      val promoted = staged.get(finalizedHash).map { case (_, bytes) => bytes }
      (SnapshotLeaderLoop.pruneStagedPostBytesAtOrBelow(staged, ordinal), promoted)
    }.flatMap {
      case Some(bytes) => store.writeState(ordinal, bytes) >> store.applyCutoff(ordinal)
      case None        => IO.unit
    }

  test(
    "FIDELITY repro: an ADOPTED (never-staged) ordinal is a permanent hole — pinnedReaderAt fail-closes at a retained ordinal whose neighbors read fine"
  ) { res =>
    implicit val (h, _, j, _) = res
    import io.constellationnetwork.node.shared.domain.nakamoto.overlay.PinnedCurrencyInfoReader
    Files[IO].tempDirectory.use { dir =>
      for {
        store <- MptStateStorage.make[IO](dir / "signed", ContiguousOrdinalCutoff.make(100))
        f31 <- gsiFixture(31L)
        f32 <- gsiFixture(32L)
        f33 <- gsiFixture(33L)
        (_, bytes31, root31) = f31
        (_, _, root32) = f32
        (_, bytes33, root33) = f33
        snap31 <- mkFinalizedSnapshot(31L, root31)
        snap32 <- mkFinalizedSnapshot(32L, root32) // the fork WINNER this node ADOPTED (reorg) — canonical at 32
        snap33 <- mkFinalizedSnapshot(33L, root33)
        loserHash = Hash("aa" * 32) // this node's OWN produced 32-candidate that LOST the proposal race
        stagedRef <- cats.effect.Ref.of[IO, Map[Hash, (SnapshotOrdinal, Map[Hex, Array[Byte]])]](Map.empty)
        // Produce/validate staging for 31 + 33 and for the LOSING own-candidate at 32; the ADOPTED winner stages NOTHING (pre-fix shape).
        _ <- stagedRef.update(_ + (snap31.hash -> ((ord(31L), bytes31))) + (loserHash -> ((ord(32L), bytesFor(9)))))
        _ <- stagedRef.update(_ + (snap33.hash -> ((ord(33L), bytes33))))
        // Finality passes 31 → 32 → 33 (ascending, exactly `recordFinalizedRange`).
        _ <- finalizeSinkPostBytes(stagedRef, store, ord(31L), snap31.hash)
        _ <- finalizeSinkPostBytes(stagedRef, store, ord(32L), snap32.hash) // staged.get misses ⇒ SKIP ⇒ hole; prune drops the loser
        _ <- finalizeSinkPostBytes(stagedRef, store, ord(33L), snap33.hash)
        stored <- store.listStoredOrdinals.map(_.map(_.value.value).toSet)
        resolver = (o: SnapshotOrdinal) =>
          (o.value.value match {
            case 31L => snap31.some
            case 32L => snap32.some
            case 33L => snap33.some
            case _   => none[Hashed[GlobalIncrementalSnapshot]]
          }).pure[IO]
        reader = PinnedCurrencyInfoReader.make[IO](store, resolver)
        at31 <- reader.pinnedReaderAt(ord(31L))
        at32 <- reader.pinnedReaderAt(ord(32L))
        at33 <- reader.pinnedReaderAt(ord(33L))
      } yield
        expect.same(stored, Set(31L, 33L)) && // the hole: 32 was finalized but never written (adopt skipped staging)
          expect(at31.isDefined) && expect(at33.isDefined) && // neighbors read fine — 32 is WITHIN retention, not evicted
          // the e2e failure verbatim: `pinned ANCHOR at executionBaseOrdinal=32 unreadable (evicted/fork) — FAIL-CLOSED DROP`
          expect(at32.isEmpty)
    }
  }

  test("S2: signed-bytes store retains a contiguous window to the configured depth; depth derives from k₂ (not 512)") { res =>
    implicit val (_, _, j, _) = res

    val depth = 5 // stand-in for k₂, small for a fast on-disk test
    val highest = 12L // write ords 0..12 (> 2×depth) so the retained window sits well inside the range

    Files[IO].tempDirectory.use { dir =>
      for {
        store <- MptStateStorage.make[IO](
          dir / "mpt_snapshot_info_signed",
          ContiguousOrdinalCutoff.make(depth)
        )
        // One file per finalized ordinal — exactly how the leader's promote sink writes `signedBytesStore`.
        _ <- (0L to highest).toList.traverse_(o => store.writeState(ord(o), bytesFor(o.toInt)))
        _ <- store.applyCutoff(ord(highest))
        kept <- store.listStoredOrdinals.map(_.map(_.value.value).toSet)
      } yield {
        val expectedWindow = ((highest - depth + 1L) to highest).toSet
        expect.all(
          // exactly the last `depth` ordinals survive, contiguously — no logarithmic gaps
          kept == expectedWindow,
          kept.size == depth,
          kept == (kept.min to kept.max).toSet,
          // the raise: derivation returns k₂ verbatim. mainnet k₁=1024 ⇒ k₂=102400; dev k₁=32 ⇒ k₂=3200; both ≫ the stale 512.
          GlobalSnapshotConsensus.signedBytesRetentionDepth(100L * 1024L) == 102400,
          GlobalSnapshotConsensus.signedBytesRetentionDepth(100L * 32L) == 3200,
          GlobalSnapshotConsensus.signedBytesRetentionDepth(100L * 1024L) > 512
        )
      }
    }
  }
}
