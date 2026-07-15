package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.effect.IO
import cats.effect.kernel.{Deferred, Ref}
import cats.effect.std.Supervisor
import cats.syntax.all._

import scala.concurrent.duration._

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.ext.crypto._
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto._
import io.constellationnetwork.node.shared.domain.snapshot.storage.SnapshotStorage
import io.constellationnetwork.node.shared.infrastructure.metrics.NoOpMetrics
import io.constellationnetwork.node.shared.nodeSharedKryoRegistrar
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.nakamoto.ChainTip
import io.constellationnetwork.schema.nakamoto.slot.{Slot, VrfOutput}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for `NakamotoChainStore` divergent-self-finalize signal + `unsafe_clearFinality` (P-11, task #141).
  *
  * The store also has rich logic for fork-aware chain storage (ChainSelection-driven reorgs, finalize pruning, etc.) but those paths are
  * exercised through `NakamotoSyncDaemon` integration sites; this suite focuses on the new P-11 surface.
  */
object NakamotoChainStoreSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  // Required implicit for `GlobalIncrementalSnapshot.fromGlobalSnapshot`.
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  // Minimal Metrics for the chain store + (transitively, downstream) the orchestrator. Tests
  // don't assert counters; behavior tests live below.
  implicit val metrics: io.constellationnetwork.node.shared.infrastructure.metrics.Metrics[IO] = NoOpMetrics.make

  type Res = (Supervisor[IO], KryoSerializer[IO], JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: cats.effect.Resource[IO, Res] = for {
    supervisor <- Supervisor[IO]
    implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](nodeSharedKryoRegistrar)
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (supervisor, ks, j, h, sp)

  // Minimal in-memory SnapshotStorage stub. Candidate retention/selection must never mutate this
  // revision-unaware canonical projection before the caller's exact-selection CAS, so both write
  // methods are fail-fast tripwires. Historical fallback reads return None.
  private def stubStorage: SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Boolean] = IO.raiseError(new AssertionError("chain store must not prepend canonical projection state"))
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = IO.pure(None)
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHashed(ordinal: SnapshotOrdinal)(
        implicit hasher: Hasher[IO]
      ): IO[Option[Hashed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] = IO.pure(None)
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.raiseError(new AssertionError("chain store must not set canonical projection head"))
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
    }

  /** In-memory disk-simulator: the `NakamotoChainStore.getWithOrdinalFallback` path consults `SnapshotStorage.get(ordinal)` when the
    * in-memory `byHash` map misses. Tests that need to verify the fallback engage need an `SnapshotStorage` whose `get(ordinal)` returns a
    * real snapshot — `stubStorage` always returns None which would mask the disk path.
    *
    * This wraps a mutable `Ref[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]]`; tests pre-populate it with the snapshots they
    * want "on disk" (typically the ones below the eviction floor). Reads return the stored entry; writes are no-ops (tests drive disk
    * content directly).
    */
  private def diskBackedStorage(
    disk: Ref[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]],
    diskByHash: Ref[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]]
  ): SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] =
    new SnapshotStorage[IO, GlobalIncrementalSnapshot, GlobalSnapshotInfo] {
      def prepend(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Boolean] = IO.pure(true)
      def head: IO[Option[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)]] = IO.pure(None)
      def headSnapshot: IO[Option[Signed[GlobalIncrementalSnapshot]]] = IO.pure(None)
      def get(ordinal: SnapshotOrdinal): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        disk.get.map(_.get(ordinal))
      def getHashed(ordinal: SnapshotOrdinal)(
        implicit hasher: Hasher[IO]
      ): IO[Option[Hashed[GlobalIncrementalSnapshot]]] =
        disk.get.flatMap(_.get(ordinal).traverse(_.toHashed[IO]))
      def get(hash: Hash): IO[Option[Signed[GlobalIncrementalSnapshot]]] =
        diskByHash.get.map(_.get(hash))
      def getHash(ordinal: SnapshotOrdinal)(implicit hasher: Hasher[IO]): IO[Option[Hash]] =
        disk.get.flatMap(_.get(ordinal).traverse(_.toHashed[IO].map(_.hash)))
      def setHeadForRecovery(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def setTentativeHead(snapshot: Signed[GlobalIncrementalSnapshot], state: GlobalSnapshotInfo)(
        implicit hasher: Hasher[IO]
      ): IO[Unit] = IO.unit
      def confirmHead(hash: Hash): IO[Unit] = IO.unit
      def pruneTentative(finalizedOrdinal: SnapshotOrdinal): IO[Unit] = IO.unit
    }

  // Test fixture: NakamotoChainStore + the finalized-ordinal Ref it gates against.
  private def mkChainStore(
    keepDepthBehindFinalized: Long = NakamotoChainStore.DefaultKeepDepthBehindFinalized
  )(
    implicit hs: HasherSelector[IO]
  ): IO[
    (NakamotoChainStore.NakamotoChainStoreAlgebra[IO], Ref[IO, SnapshotOrdinal], TipTracker[IO], StakeRegistry[IO])
  ] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      // Track-3 S1.5 "marker split": the DISTINCT k₂ settled ref. These tests don't drive the settled marker, so it's allocated
      // internally at MinValue and not exposed; `mkChainStoreWithSettled` returns it for the S1.5 gate tests.
      settledRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      // ChainSelection wants a fetchParent — for our P-11 surface tests we never trigger fork
      // selection, so a None-returning stub suffices.
      chainSelection = ChainSelection.make[IO](tipTracker, _ => IO.pure(None))
      chainStore <- NakamotoChainStore.make[IO](stubStorage, chainSelection, finalizedRef, settledRef, keepDepthBehindFinalized)
    } yield (chainStore, finalizedRef, tipTracker, stakeRegistry)

  /** Track-3 S1.5 "marker split": exposes BOTH the k₁ finalized ref AND the DISTINCT k₂ settled ref so tests can drive them independently
    * and assert (a) they advance independently, (b) the store's finality-safety gate keys off finalized (k₁) — NOT the deeper settled (k₂)
    * marker — and (c) `unsafe_clearFinality` resets BOTH.
    */
  private def mkChainStoreWithSettled(
    keepDepthBehindFinalized: Long = NakamotoChainStore.DefaultKeepDepthBehindFinalized,
    // Track-3 S3: when true, BOTH the store-gate and ChainSelection.shouldSwitch key off the k₂ settled
    // ref (band-density reorg). Defaults false (legacy k₁-freeze) so the S1.5 tests keep their behavior.
    bandDensityReorgEnabled: Boolean = false
  )(
    implicit hs: HasherSelector[IO]
  ): IO[
    (NakamotoChainStore.NakamotoChainStoreAlgebra[IO], Ref[IO, SnapshotOrdinal], Ref[IO, SnapshotOrdinal])
  ] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      settledRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      chainSelection = ChainSelection.make[IO](
        tipTracker,
        _ => IO.pure(None),
        settledOrdinalReader = Some(settledRef.get.map(_.value.value)),
        bandDensityReorgEnabled = bandDensityReorgEnabled
      )
      chainStore <- NakamotoChainStore.make[IO](
        stubStorage,
        chainSelection,
        finalizedRef,
        settledRef,
        keepDepthBehindFinalized,
        bandDensityReorgEnabled = bandDensityReorgEnabled
      )
    } yield (chainStore, finalizedRef, settledRef)

  /** Same as `mkChainStore` but with a `diskBackedStorage` plumbed in. Returns the disk Refs so tests can simulate "this snapshot is on
    * disk but evicted from memory".
    */
  private def mkChainStoreWithDisk(
    keepDepthBehindFinalized: Long = NakamotoChainStore.DefaultKeepDepthBehindFinalized
  )(
    implicit hs: HasherSelector[IO]
  ): IO[
    (
      NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
      Ref[IO, SnapshotOrdinal],
      Ref[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]],
      Ref[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]]
    )
  ] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      // Track-3 S1.5: distinct k₂ settled ref (unused by the disk-fallback tests, allocated internally).
      settledRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      diskRef <- Ref.of[IO, Map[SnapshotOrdinal, Signed[GlobalIncrementalSnapshot]]](Map.empty)
      diskByHashRef <- Ref.of[IO, Map[Hash, Signed[GlobalIncrementalSnapshot]]](Map.empty)
      storage = diskBackedStorage(diskRef, diskByHashRef)
      chainSelection = ChainSelection.make[IO](tipTracker, _ => IO.pure(None))
      chainStore <- NakamotoChainStore.make[IO](storage, chainSelection, finalizedRef, settledRef, keepDepthBehindFinalized)
    } yield (chainStore, finalizedRef, diskRef, diskByHashRef)

  private def pid(name: String): PeerId =
    PeerId(Hex(name.getBytes("UTF-8").map(b => f"$b%02x").mkString))

  // Real signed snapshots are expensive to build (need genesis + key pairs + hash). For
  // the divergent-refuse-counter surface we just need to call `chainStore.store` with
  // sentinel parameters that drive the safety-gate path. The gate inspects `ordinal` and
  // `parentHash`; the signed-snapshot body is hashed lazily inside `store` and never
  // re-read by the gate logic. We construct minimal real snapshots via the standard
  // genesis fixture from SnapshotStorageSuite.
  private def mkGenesis(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { keyPair =>
      Signed
        .forAsyncHasher[IO, GlobalSnapshot](
          GlobalSnapshot.mkGenesis(Map.empty, EpochProgress.MinValue),
          keyPair
        )
        .flatMap { genesis =>
          GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](genesis).flatMap { incremental =>
            Signed
              .forAsyncHasher[IO, GlobalIncrementalSnapshot](incremental, keyPair)
              .map((_, genesis.info.toGlobalSnapshotInfo))
          }
        }
    }

  // Build a SECOND signed snapshot at the same ordinal=1 but with different content — this
  // simulates the "canonical chain has a different hash at the ordinal we already finalized"
  // scenario. We change a non-essential field (epoch progress) so the hash differs.
  private def mkAltSnapshot(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    mkSnapshotWithEpoch(epoch = 1L)

  /** Build a signed snapshot whose content hash varies with `epoch`. Lets a single test build a sequence of `N` distinct-hash snapshots
    * without colliding on bytes. Heap-leak Fix B tests use this to seed `byHash` with N entries and then assert eviction below the
    * keep-window.
    */
  private def mkSnapshotWithEpoch(epoch: Long)(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[(Signed[GlobalIncrementalSnapshot], GlobalSnapshotInfo)] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { keyPair =>
      Signed
        .forAsyncHasher[IO, GlobalSnapshot](
          GlobalSnapshot.mkGenesis(Map.empty, EpochProgress(NonNegLong.unsafeFrom(epoch))),
          keyPair
        )
        .flatMap { genesis =>
          GlobalIncrementalSnapshot.fromGlobalSnapshot[IO](genesis).flatMap { incremental =>
            Signed
              .forAsyncHasher[IO, GlobalIncrementalSnapshot](incremental, keyPair)
              .map((_, genesis.info.toGlobalSnapshotInfo))
          }
        }
    }

  private final case class LinkedSnapshot(
    signed: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    hash: Hash
  )

  private final case class StrictCycleNode(
    name: String,
    ordinal: Long,
    slot: Long,
    parent: Option[String],
    vrf: Byte,
    uniqueEpoch: Long
  )

  private final case class SignedStrictCycleNode(
    signed: Signed[GlobalIncrementalSnapshot],
    context: GlobalSnapshotInfo,
    tip: ChainTip
  )

  private val strictCycleNodes = List(
    StrictCycleNode("g", 0L, 0L, None, 0x40.toByte, 1L),
    StrictCycleNode("x1", 1L, 1L, Some("g"), 0x10.toByte, 2L),
    StrictCycleNode("x2", 2L, 2L, Some("x1"), 0x11.toByte, 3L),
    StrictCycleNode("a1", 3L, 20L, Some("x2"), 0x20.toByte, 4L),
    StrictCycleNode("a2", 4L, 30L, Some("a1"), 0x21.toByte, 5L),
    StrictCycleNode("a", 5L, 40L, Some("a2"), 0x22.toByte, 6L),
    StrictCycleNode("b1", 3L, 3L, Some("x2"), 0x30.toByte, 7L),
    StrictCycleNode("b", 4L, 4L, Some("b1"), 0x31.toByte, 8L),
    StrictCycleNode("c1", 1L, 1L, Some("g"), 0x50.toByte, 9L),
    StrictCycleNode("c2", 2L, 5L, Some("c1"), 0x51.toByte, 10L),
    StrictCycleNode("c3", 3L, 9L, Some("c2"), 0x52.toByte, 11L),
    StrictCycleNode("c", 4L, 20L, Some("c3"), 0x53.toByte, 12L)
  )

  private def mkSignedStrictCycle(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[Map[String, SignedStrictCycleNode]] =
    for {
      templatePair <- mkGenesis
      (template, context) = templatePair
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      built <- strictCycleNodes.foldLeftM[IO, Map[String, SignedStrictCycleNode]](Map.empty) { (acc, node) =>
        val parentHash = node.parent.flatMap(acc.get).fold(Hash.empty)(_.tip.hash)
        val value = template.value.copy(
          ordinal = SnapshotOrdinal.unsafeApply(node.ordinal),
          lastSnapshotHash = parentHash,
          epochProgress = EpochProgress(NonNegLong.unsafeFrom(node.uniqueEpoch))
        )

        Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](value, keyPair).flatMap { signed =>
          signed.toHashed[IO].map { hashed =>
            val tip = ChainTip(
              hashed.hash,
              Slot(NonNegLong.unsafeFrom(node.slot)),
              node.ordinal,
              parentHash,
              VrfOutput(Hex(List.fill(64)(f"${node.vrf & 0xff}%02x").mkString))
            )
            acc.updated(node.name, SignedStrictCycleNode(signed, context, tip))
          }
        }
      }
    } yield built

  private def mkStrictCycleChainStore(
    implicit hs: HasherSelector[IO]
  ): IO[NakamotoChainStore.NakamotoChainStoreAlgebra[IO]] =
    for {
      stakeRegistry <- StakeRegistry.equalWeight[IO]
      _ <- stakeRegistry.updateValidators(Set(pid("self")))
      tipTracker <- TipTracker.make[IO](stakeRegistry)
      finalizedRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      settledRef <- Ref.of[IO, SnapshotOrdinal](SnapshotOrdinal.MinValue)
      chainStoreRef <- Ref.of[IO, Option[NakamotoChainStore.NakamotoChainStoreAlgebra[IO]]](None)
      chainSelection = ChainSelection.make[IO](
        tipTracker,
        tip => chainStoreRef.get.flatMap(_.traverse(_.tipFor(tip.parentHash)).map(_.flatten)),
        kLookback = 3L,
        sWindow = 10L,
        maxAncestorDepth = 100L,
        settledOrdinalReader = Some(settledRef.get.map(_.value.value)),
        bandDensityReorgEnabled = true
      )
      chainStore <- NakamotoChainStore.make[IO](
        stubStorage,
        chainSelection,
        finalizedRef,
        settledRef,
        NakamotoChainStore.DefaultKeepDepthBehindFinalized,
        bandDensityReorgEnabled = true
      )
      _ <- chainStoreRef.set(Some(chainStore))
    } yield chainStore

  /** Build snapshots whose signed ordinal and signed parent hash match the chain-store metadata. The older `seedChainOfLength` fixture is
    * intentionally metadata-only and must not be used to prove exact signed ancestry.
    */
  private def mkSignedLinkedChain(length: Int)(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[List[LinkedSnapshot]] =
    for {
      templatePair <- mkGenesis
      (template, context) = templatePair
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      result <- (1 to length).toList.foldLeftM[IO, (List[LinkedSnapshot], Hash)]((Nil, Hash.empty)) {
        case ((acc, parentHash), ordinal) =>
          val value = template.value.copy(
            ordinal = SnapshotOrdinal.unsafeApply(ordinal.toLong),
            lastSnapshotHash = parentHash,
            epochProgress = EpochProgress(NonNegLong.unsafeFrom(ordinal.toLong))
          )
          Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](value, keyPair).flatMap { signed =>
            signed.toHashed[IO].map { hashed =>
              (acc :+ LinkedSnapshot(signed, context, hashed.hash), hashed.hash)
            }
          }
      }
    } yield result._1

  /** Seed a chain of `n` distinct-hash snapshots at ordinals 1..n into `chainStore`, parent-linked left-to-right. Returns the hash at each
    * ordinal. The actual chain-linking via `parentHash` isn't tied to any production semantics here — we just need `byHash` populated;
    * `chainStore`'s canonical-walk in `finalize` follows the parent chain so we link each new store to the prior.
    */
  private def seedChainOfLength(
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
    n: Int
  )(
    implicit H: Hasher[IO],
    S: SecurityProvider[IO],
    j: JsonSerializer[IO]
  ): IO[List[Hash]] =
    (1 to n).toList
      .foldLeftM[IO, (List[Hash], Hash)]((List.empty[Hash], Hash.empty)) {
        case ((acc, parent), ord) =>
          mkSnapshotWithEpoch(epoch = ord.toLong).flatMap {
            case (signed, ctx) =>
              signed.toHashed[IO].flatMap { hashed =>
                chainStore
                  .store(signed, ctx, ordinal = ord.toLong, slot = ord.toLong, parentHash = parent, vrfOutput = Array.empty)
                  .as((acc :+ hashed.hash, hashed.hash))
              }
          }
      }
      .map(_._1)

  private def currentSelection(
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[IO]
  ): IO[NakamotoChainStore.SelectedTip] =
    chainStore.selectedTip.flatMap(_.liftTo[IO](new IllegalStateException("missing selected chain tip")))

  private def becameSelected(outcome: NakamotoChainStore.StoreOutcome): IO[NakamotoChainStore.SelectedTip] =
    outcome match {
      case NakamotoChainStore.StoreOutcome.BecameSelected(selected, _) => IO.pure(selected)
      case other => IO.raiseError(new IllegalStateException(s"expected selected store outcome, got $other"))
    }

  private def finalizeCurrentSelection(
    chainStore: NakamotoChainStore.NakamotoChainStoreAlgebra[IO],
    targetOrdinal: Long
  ): IO[NakamotoChainStore.FinalizeOutcome] =
    currentSelection(chainStore).flatMap(chainStore.finalizeSelectedAt(_, targetOrdinal))

  private def finalizedExactly(
    outcome: NakamotoChainStore.FinalizeOutcome,
    targetHash: Hash,
    targetOrdinal: Long,
    previousOrdinal: Long
  ): Boolean =
    outcome match {
      case NakamotoChainStore.FinalizeOutcome.Finalized(target, selected, previous, _) =>
        target.hash == targetHash &&
        target.ordinal == targetOrdinal &&
        selected.snapshot.ordinal >= targetOrdinal &&
        previous == previousOrdinal
      case _ => false
    }

  test("RTA-RED-019: canonical k1 depth finalizes the exact tip-minus-k1 hash without attestations") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val k1 = 2L

    for {
      tuple <- mkChainStore()
      (chainStore, _, tipTracker, _) = tuple
      chain <- mkSignedLinkedChain(length = 5)
      _ <- chain.traverse_ { linked =>
        val value = linked.signed.value
        chainStore.store(
          linked.signed,
          linked.context,
          value.ordinal.value.value,
          value.ordinal.value.value,
          value.lastSnapshotHash,
          Array.emptyByteArray
        )
      }
      selected <- currentSelection(chainStore)
      best = selected.snapshot
      trigger <- TDepth1Trigger.make[IO](k1)
      qualifying <- trigger.evaluate(
        FinalityTrigger.ConsensusState[IO](
          selfId = pid("self"),
          bestTipOrdinal = SnapshotOrdinal.unsafeApply(best.ordinal),
          bestTipHash = best.hash,
          canonicalHashAt = ordinal => chainStore.walkBackTo(best.hash, ordinal)
        )
      )
      exactHash <- chainStore
        .walkBackTo(best.hash, qualifying.value.value)
        .flatMap(_.liftTo[IO](new IllegalStateException("missing exact depth target")))
      finalizedOutcome <- chainStore.finalizeSelectedAt(selected, qualifying.value.value)
      finalized <- finalizedOutcome match {
        case result: NakamotoChainStore.FinalizeOutcome.Finalized => IO.pure(result)
        case other => IO.raiseError(new IllegalStateException(s"depth finalization failed: $other"))
      }
      _ <- tipTracker.markFinalized(finalized.target.hash, Slot(NonNegLong.unsafeFrom(finalized.target.slot)))
      finalizedOrdinal <- chainStore.lastFinalizedOrdinal
      attestations <- tipTracker.allAttestations
    } yield
      expect.all(
        best.ordinal == 5L,
        qualifying == SnapshotOrdinal.unsafeApply(3L),
        exactHash == chain(2).hash,
        finalized.target.hash == exactHash,
        finalized.target.ordinal == qualifying.value.value,
        finalized.selected == selected,
        finalized.previousOrdinal == 0L,
        finalizedOrdinal == 3L,
        attestations.isEmpty
      )
  }

  test("typed store outcomes preserve exact selection revisions across duplicate and extension") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      second = chain.last
      initialized <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      duplicate <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      extended <- chainStore.store(second.signed, second.context, 2L, 2L, first.hash, Array.emptyByteArray)
      current <- currentSelection(chainStore)
    } yield {
      val exactInitialized = initialized match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(selected, NakamotoChainStore.SelectionChange.Initialized) =>
          selected.snapshot.hash == first.hash &&
          selected.branchRevision.value.value == 1L &&
          selected.lineageRevision.value.value == 0L
        case _ => false
      }
      val exactDuplicate = duplicate match {
        case NakamotoChainStore.StoreOutcome.Duplicate(existing, Some(selected)) =>
          existing.hash == first.hash &&
          selected.snapshot.hash == first.hash &&
          selected.branchRevision.value.value == 1L &&
          selected.lineageRevision.value.value == 0L
        case _ => false
      }
      val exactExtension = extended match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(selected, NakamotoChainStore.SelectionChange.Extended) =>
          selected.snapshot.hash == second.hash &&
          selected.branchRevision.value.value == 2L &&
          selected.lineageRevision.value.value == 0L &&
          selected == current
        case _ => false
      }

      expect.all(exactInitialized, exactDuplicate, exactExtension)
    }
  }

  test("stored VRF output is immutable after crossing the chain-store boundary") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    val callerOwned = Array.fill[Byte](64)(0x11.toByte)
    val expected = callerOwned.clone()

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      pair <- mkGenesis
      (signed, context) = pair
      outcome <- chainStore.store(signed, context, 1L, 1L, Hash.empty, callerOwned)
      selectedHash <- outcome match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(selected, _) => selected.snapshot.hash.pure[IO]
        case other => IO.raiseError[Hash](new AssertionError(s"expected selected snapshot, got $other"))
      }
      _ <- IO.delay(callerOwned(0) = 0x22.toByte)
      first <- chainStore.bestTip.flatMap(_.liftTo[IO](new AssertionError("missing selected tip")))
      exposed = first.vrfOutput.toBytes
      _ <- IO.delay(exposed(1) = 0x33.toByte)
      second <- chainStore.bestTip.flatMap(_.liftTo[IO](new AssertionError("missing selected tip")))
      tip <- chainStore.tipFor(selectedHash).flatMap(_.liftTo[IO](new AssertionError("missing selected ChainTip")))
    } yield
      expect.all(
        first.vrfOutput.toBytes.sameElements(expected),
        second.vrfOutput.toBytes.sameElements(expected),
        tip.vrfOutput.toBytes.sameElements(expected)
      )
  }

  test("canonical effects run against the store-owned snapshot only while the exact selection is current") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      first <- mkSignedLinkedChain(length = 1).map(_.head)
      stored <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      selected <- becameSelected(stored)
      observed <- Ref.of[IO, Option[NakamotoChainStore.StoredSnapshot]](None)
      outcome <- chainStore.runCanonicalEffectsIfCurrent(selected) { canonical =>
        observed.set(canonical.some).as(canonical.hash)
      }
      exactObserved <- observed.get
    } yield {
      val exactApplied = outcome match {
        case NakamotoChainStore.CanonicalEffectsOutcome.Applied(current, value) =>
          current == selected && value == first.hash
        case _ => false
      }

      expect.all(
        exactApplied,
        exactObserved.exists(snapshot => snapshot.hash == first.hash && snapshot.context == first.context)
      )
    }
  }

  test("candidate selection never writes the revision-unaware SnapshotStorage projection") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      first <- mkSignedLinkedChain(length = 1).map(_.head)
      outcome <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
    } yield expect(outcome.isInstanceOf[NakamotoChainStore.StoreOutcome.BecameSelected])
  }

  test("stale selected token performs zero canonical effects and retains pending events") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      second = chain.last
      initial <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      observed <- becameSelected(initial)
      _ <- chainStore.store(second.signed, second.context, 2L, 2L, first.hash, Array.emptyByteArray)
      pendingEvents <- Ref.of[IO, Set[String]](Set("pending-native-event"))
      published <- Ref.of[IO, Boolean](false)
      outcome <- chainStore.runCanonicalEffectsIfCurrent(observed) { _ =>
        pendingEvents.set(Set.empty) >> published.set(true)
      }
      eventsAfter <- pendingEvents.get
      publishedAfter <- published.get
    } yield {
      val rejectedExactStale = outcome match {
        case NakamotoChainStore.CanonicalEffectsOutcome.StaleSelection(Some(current)) =>
          current.snapshot.hash == second.hash &&
          current.branchRevision.value.value > observed.branchRevision.value.value
        case _ => false
      }

      expect.all(rejectedExactStale, eventsAfter == Set("pending-native-event"), !publishedAfter)
    }
  }

  test("selected token rejects same-hash ABA reconstruction after reset") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      first <- mkSignedLinkedChain(length = 1).map(_.head)
      initial <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      beforeReset <- becameSelected(initial)
      _ <- chainStore.unsafe_clearFinality
      reconstructed <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      afterReset <- becameSelected(reconstructed)
      effectRan <- Ref.of[IO, Boolean](false)
      outcome <- chainStore.runCanonicalEffectsIfCurrent(beforeReset)(_ => effectRan.set(true))
      didRun <- effectRan.get
    } yield {
      val rejectedAba = outcome match {
        case NakamotoChainStore.CanonicalEffectsOutcome.StaleSelection(Some(current)) =>
          current.snapshot.hash == beforeReset.snapshot.hash &&
          current == afterReset &&
          current.branchRevision.value.value > beforeReset.branchRevision.value.value &&
          current.lineageRevision.value.value > beforeReset.lineageRevision.value.value
        case _ => false
      }

      expect.all(rejectedAba, !didRun)
    }
  }

  test("selected token cannot authorize canonical effects in another chain-store instance") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      firstStoreTuple <- mkChainStore()
      secondStoreTuple <- mkChainStore()
      firstStore = firstStoreTuple._1
      secondStore = secondStoreTuple._1
      snapshot <- mkSignedLinkedChain(length = 1).map(_.head)
      firstOutcome <- firstStore.store(snapshot.signed, snapshot.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      foreignSelection <- becameSelected(firstOutcome)
      _ <- secondStore.store(snapshot.signed, snapshot.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      effectRan <- Ref.of[IO, Boolean](false)
      outcome <- secondStore.runCanonicalEffectsIfCurrent(foreignSelection)(_ => effectRan.set(true))
      didRun <- effectRan.get
    } yield {
      val rejectedForeignToken = outcome match {
        case NakamotoChainStore.CanonicalEffectsOutcome.StaleSelection(Some(current)) =>
          current.snapshot.hash == foreignSelection.snapshot.hash && current != foreignSelection
        case _ => false
      }

      expect.all(rejectedForeignToken, !didRun)
    }
  }

  test("canonical effects exclude concurrent store mutation until the callback completes") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      second = chain.last
      initial <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      selected <- becameSelected(initial)
      callbackEntered <- Deferred[IO, Unit]
      releaseCallback <- Deferred[IO, Unit]
      storeStarted <- Deferred[IO, Unit]
      storeFinished <- Deferred[IO, Unit]
      callbackFiber <- chainStore
        .runCanonicalEffectsIfCurrent(selected) { _ =>
          callbackEntered.complete(()).void >> releaseCallback.get
        }
        .start
      _ <- callbackEntered.get
      storeFiber <- (storeStarted.complete(()).void >>
        chainStore
          .store(second.signed, second.context, 2L, 2L, first.hash, Array.emptyByteArray)
          .flatTap(_ => storeFinished.complete(()).void)).start
      _ <- storeStarted.get >> IO.cede
      completedWhileCallbackHeldLock <- storeFinished.tryGet
      _ <- releaseCallback.complete(())
      callbackOutcome <- callbackFiber.joinWithNever
      storeOutcome <- storeFiber.joinWithNever
      current <- currentSelection(chainStore)
    } yield {
      val callbackApplied = callbackOutcome match {
        case NakamotoChainStore.CanonicalEffectsOutcome.Applied(exact, _) => exact == selected
        case _                                                            => false
      }
      val extensionApplied = storeOutcome match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(exact, NakamotoChainStore.SelectionChange.Extended) =>
          exact.snapshot.hash == second.hash && exact == current
        case _ => false
      }

      expect.all(completedWhileCallbackHeldLock.isEmpty, callbackApplied, extensionApplied)
    }
  }

  test("raising canonical callback releases the mutation permit for a subsequent extension") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val projectionFailure = new RuntimeException("projection failed")

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      second = chain.last
      initial <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      selected <- becameSelected(initial)
      failed <- chainStore.runCanonicalEffectsIfCurrent(selected)(_ => IO.raiseError[Unit](projectionFailure)).attempt
      extension <- chainStore
        .store(second.signed, second.context, 2L, 2L, first.hash, Array.emptyByteArray)
        .timeout(2.seconds)
      current <- currentSelection(chainStore)
    } yield {
      val exactFailure = failed match {
        case Left(error) => error eq projectionFailure
        case _           => false
      }
      val extensionApplied = extension match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(exact, NakamotoChainStore.SelectionChange.Extended) =>
          exact.snapshot.hash == second.hash && exact == current
        case _ => false
      }

      expect.all(exactFailure, extensionApplied)
    }
  }

  test("finalizeSelectedAt rejects an observed selection after the selected branch advances") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      second = chain.last
      _ <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      observed <- currentSelection(chainStore)
      _ <- chainStore.store(second.signed, second.context, 2L, 2L, first.hash, Array.emptyByteArray)
      outcome <- chainStore.finalizeSelectedAt(observed, targetOrdinal = 1L)
      finalizedOrdinal <- chainStore.lastFinalizedOrdinal
    } yield {
      val exactStaleSelection = outcome match {
        case NakamotoChainStore.FinalizeOutcome.StaleSelection(Some(current)) =>
          current.snapshot.hash == second.hash &&
          current.branchRevision.value.value == observed.branchRevision.value.value + 1L &&
          current.lineageRevision == observed.lineageRevision
        case _ => false
      }

      expect.all(exactStaleSelection, finalizedOrdinal == 0L)
    }
  }

  test("finalizeSelectedAt derives the exact ancestor and repeated finalization is an exact no-op") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 3)
      _ <- chain.traverse_ { item =>
        val ordinal = item.signed.value.ordinal.value.value
        chainStore.store(item.signed, item.context, ordinal, ordinal, item.signed.value.lastSnapshotHash, Array.emptyByteArray)
      }
      selected <- currentSelection(chainStore)
      first <- chainStore.finalizeSelectedAt(selected, targetOrdinal = 2L)
      repeated <- chainStore.finalizeSelectedAt(selected, targetOrdinal = 2L)
      finalizedOrdinal <- chainStore.lastFinalizedOrdinal
    } yield {
      val exactFinalized = first match {
        case NakamotoChainStore.FinalizeOutcome.Finalized(target, current, previousOrdinal, prunedHashes) =>
          target.hash == chain(1).hash &&
          target.ordinal == 2L &&
          current == selected &&
          previousOrdinal == 0L &&
          prunedHashes.isEmpty
        case _ => false
      }
      val exactRepeatedNoOp = repeated match {
        case NakamotoChainStore.FinalizeOutcome.AlreadyFinalized(lastOrdinal, Some(lastHash)) =>
          lastOrdinal == 2L && lastHash == chain(1).hash
        case _ => false
      }

      expect.all(exactFinalized, exactRepeatedNoOp, finalizedOrdinal == 2L)
    }
  }

  test("internal finality CAS keeps the store floor closed while the public watermark lags") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, publishedFinalityRef, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 3)
      _ <- chain.traverse_ { item =>
        val ordinal = item.signed.value.ordinal.value.value
        chainStore.store(item.signed, item.context, ordinal, ordinal, item.signed.value.lastSnapshotHash, Array.emptyByteArray)
      }
      selected <- currentSelection(chainStore)
      finalized <- chainStore.finalizeSelectedAt(selected, targetOrdinal = 2L)
      publishedBeforeEffects <- publishedFinalityRef.get
      alternateKey <- KeyPairGenerator.makeKeyPair[IO]
      alternate <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        chain(1).signed.value.copy(epochProgress = EpochProgress(NonNegLong.unsafeFrom(99L))),
        alternateKey
      )
      rejected <- chainStore.store(alternate, chain(1).context, 2L, 2L, chain.head.hash, Array.emptyByteArray)
    } yield {
      val finalizedExactHash = finalized match {
        case NakamotoChainStore.FinalizeOutcome.Finalized(target, _, _, _) => target.hash == chain(1).hash
        case _                                                             => false
      }
      val refusedOnInternalFloor = rejected match {
        case NakamotoChainStore.StoreOutcome.Rejected(
              NakamotoChainStore.StoreRejection.FinalityConflict(_, existingHash, _, floor, _),
              _
            ) =>
          existingHash == chain(1).hash && floor.value.value == 2L
        case _ => false
      }

      expect.all(
        finalizedExactHash,
        publishedBeforeEffects == SnapshotOrdinal.MinValue,
        refusedOnInternalFloor
      )
    }
  }

  test("finalizeSelectedAt cannot advance across a missing selected-lineage ordinal") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      parent <- mkSignedLinkedChain(length = 1).map(_.head)
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      childSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        parent.signed.value.copy(
          ordinal = SnapshotOrdinal.unsafeApply(3L),
          lastSnapshotHash = parent.hash,
          epochProgress = EpochProgress(NonNegLong.unsafeFrom(3L))
        ),
        keyPair
      )
      _ <- chainStore.store(parent.signed, parent.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      _ <- chainStore.store(childSigned, parent.context, 3L, 3L, parent.hash, Array.emptyByteArray)
      selected <- currentSelection(chainStore)
      outcome <- chainStore.finalizeSelectedAt(selected, targetOrdinal = 2L)
      finalizedOrdinal <- chainStore.lastFinalizedOrdinal
    } yield
      expect.all(
        outcome == NakamotoChainStore.FinalizeOutcome.TargetUnavailable(2L),
        selected.snapshot.ordinal == 3L,
        finalizedOrdinal == 0L
      )
  }

  test("unsafe_clearFinality advances revision identities before reconstructed selection") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    for {
      tuple <- mkChainStore()
      (chainStore, _, _, _) = tuple
      chain <- mkSignedLinkedChain(length = 2)
      first = chain.head
      replacement = chain.last
      _ <- chainStore.store(first.signed, first.context, 1L, 1L, Hash.empty, Array.emptyByteArray)
      beforeClear <- currentSelection(chainStore)
      cleared <- chainStore.unsafe_clearFinality
      emptyAfterClear <- chainStore.selectedTip
      reconstructed <- chainStore.store(
        replacement.signed,
        replacement.context,
        2L,
        2L,
        replacement.signed.value.lastSnapshotHash,
        Array.emptyByteArray
      )
    } yield {
      val exactReconstruction = reconstructed match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(after, NakamotoChainStore.SelectionChange.Reconstructed) =>
          after.snapshot.hash == replacement.hash &&
          after.branchRevision.value.value > beforeClear.branchRevision.value.value &&
          after.lineageRevision.value.value > beforeClear.lineageRevision.value.value
        case _ => false
      }

      expect.all(cleared, emptyAfterClear.isEmpty, exactReconstruction)
    }
  }

  /** Store/control-flow witness under a synthetic enabled k/s configuration only: the bodies bind signed ordinal/parent ancestry and every
    * schedule is parent-first, but this deliberately bypasses NakamotoSnapshotValidator and therefore does not prove VRF/KES/eta/era-valid
    * network admission or divergence under a shipped environment configuration.
    */
  test("activation blocker: parent-first signed-ancestry store schedules over one strict frontier leave three different best tips") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)

    val schedules = List(
      List("g", "x1", "x2", "a1", "a2", "a", "b1", "b", "c1", "c2", "c3", "c"),
      List("g", "x1", "x2", "a1", "a2", "a", "b1", "c1", "c2", "c3", "b", "c"),
      List("g", "x1", "x2", "a1", "a2", "b1", "b", "c1", "c2", "c3", "c", "a")
    )

    def run(
      nodes: Map[String, SignedStrictCycleNode],
      schedule: List[String]
    ): IO[Hash] =
      for {
        chainStore <- mkStrictCycleChainStore
        stored <- schedule.traverse { name =>
          val node = nodes(name)
          chainStore
            .store(
              node.signed,
              node.context,
              node.tip.ordinal,
              node.tip.slot.value.value,
              node.tip.parentHash,
              node.tip.vrfOutput.toBytes
            )
        }
        _ <- IO.raiseUnless(
          stored.forall {
            case NakamotoChainStore.StoreOutcome.BecameSelected(_, _)  => true
            case NakamotoChainStore.StoreOutcome.StoredAlternate(_, _) => true
            case _                                                     => false
          }
        )(new IllegalStateException(s"strict-cycle schedule was not fully stored: $stored"))
        best <- chainStore.bestTip.flatMap(_.liftTo[IO](new IllegalStateException("strict-cycle store has no best tip")))
      } yield best.hash

    for {
      nodes <- mkSignedStrictCycle
      winners <- schedules.traverse(run(nodes, _))
    } yield expect.same(List(nodes("c").tip.hash, nodes("b").tip.hash, nodes("a").tip.hash), winners)
  }

  test("divergentRefuseCount starts at 0 and divergentRefuseSample is None") { res =>
    implicit val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      count <- chainStore.divergentRefuseCount
      sample <- chainStore.divergentRefuseSample
    } yield expect.same(0L, count) && expect.same(None, sample)
  }

  test("divergentRefuseCount increments when store refuses a different-hash at-or-below-finalized write") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      // Store it at ordinal=1 — this seeds byHash[h1.hash] -> ord=1.
      stored1 <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      // Simulate finalization: bump the finalized-ordinal Ref so the safety gate engages.
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Now try to store a DIFFERENT signed snapshot at the same ordinal — this is the
      // canonical chain trying to overwrite our local-finalized hash. The gate must refuse.
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      stored2 <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      h2 <- s2.toHashed[IO]
      count <- chainStore.divergentRefuseCount
      sample <- chainStore.divergentRefuseSample
      firstHash <- s1.toHashed[IO].map(_.hash)
    } yield {
      val firstSelected = stored1 match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(selected, NakamotoChainStore.SelectionChange.Initialized) =>
          selected.snapshot.hash == firstHash
        case _ => false
      }
      val secondRejected = stored2 match {
        case NakamotoChainStore.StoreOutcome.Rejected(
              NakamotoChainStore.StoreRejection.FinalityConflict(1L, existingHash, candidateHash, floor, false),
              Some(selected)
            ) =>
          existingHash == firstHash && candidateHash == h2.hash && floor == SnapshotOrdinal.unsafeApply(1L) &&
          selected.snapshot.hash == firstHash
        case _ => false
      }

      expect.all(
        firstSelected,
        secondRejected,
        count == 1L, // counter incremented
        sample.contains((1L, h2.hash))
      )
    }
  }

  test("divergentRefuseCount: same-hash re-delivery at-or-below-finalized does NOT increment") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair <- mkGenesis
      (s1, ctx1) = pair
      first <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Re-deliver the SAME snapshot — should fall through to the tryStore path and be a duplicate.
      duplicate <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      count <- chainStore.divergentRefuseCount
    } yield {
      val exactDuplicate = (first, duplicate) match {
        case (
              NakamotoChainStore.StoreOutcome.BecameSelected(initial, NakamotoChainStore.SelectionChange.Initialized),
              NakamotoChainStore.StoreOutcome.Duplicate(existing, Some(selected))
            ) =>
          existing.hash == initial.snapshot.hash && selected == initial
        case _ => false
      }

      expect.all(exactDuplicate, count == 0L)
    }
  }

  test("unsafe_clearFinality: clears byHash, bestTip, lastFinalizedOrdinal, refuse counter, refuse sample") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      _ <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      // Pre-reset: state has refuse count 1, sample set, finalized ord = 1, bestTip = h1.
      preCount <- chainStore.divergentRefuseCount
      preSample <- chainStore.divergentRefuseSample
      preBestTip <- chainStore.bestTip
      preChainSize <- chainStore.size
      preFinalizedOrd <- chainStore.lastFinalizedOrdinal
      // Reset.
      cleared <- chainStore.unsafe_clearFinality
      // Post-reset assertions.
      postCount <- chainStore.divergentRefuseCount
      postSample <- chainStore.divergentRefuseSample
      postBestTip <- chainStore.bestTip
      postChainSize <- chainStore.size
      postFinalizedOrd <- chainStore.lastFinalizedOrdinal
      postFinalizedRef <- finalizedRef.get
    } yield
      expect.all(
        // Pre-state sanity.
        preCount == 1L,
        preSample.isDefined,
        preBestTip.isDefined,
        preChainSize == 1,
        preFinalizedOrd == 0L, // finalize was never called — it's the finalizedRef that's bumped
        // Reset returned true since state was cleared.
        cleared,
        // Post-reset: all state zero / empty / None.
        postCount == 0L,
        postSample.isEmpty,
        postBestTip.isEmpty,
        postChainSize == 0,
        postFinalizedOrd == 0L,
        postFinalizedRef == SnapshotOrdinal.MinValue
      )
  }

  test("unsafe_clearFinality on empty store: returns false (no-op)") { res =>
    implicit val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      cleared <- chainStore.unsafe_clearFinality
    } yield expect.same(false, cleared)
  }

  test("unsafe_clearFinality: post-reset, a previously-refused canonical hash CAN now be stored") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, finalizedRef, _, _) = r
      // Seed local-finalized hash h1 at ord=1.
      pair1 <- mkGenesis
      (s1, ctx1) = pair1
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong(1L)))
      // Canonical h2 attempts to store — refused.
      pair2 <- mkAltSnapshot
      (s2, ctx2) = pair2
      preRefused <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      preCount <- chainStore.divergentRefuseCount
      // Re-bootstrap fires — call unsafe_clearFinality.
      _ <- chainStore.unsafe_clearFinality
      // Now retry storing h2 — should succeed because finalized is reset to MinValue.
      postStored <- chainStore.store(s2, ctx2, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      h2 <- s2.toHashed[IO]
      postBestTip <- chainStore.bestTip.map(_.map(_.hash))
    } yield {
      val refusedByFinality = preRefused match {
        case NakamotoChainStore.StoreOutcome.Rejected(_: NakamotoChainStore.StoreRejection.FinalityConflict, _) => true
        case _                                                                                                  => false
      }
      val reconstructed = postStored match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(selected, NakamotoChainStore.SelectionChange.Reconstructed) =>
          selected.snapshot.hash == h2.hash
        case _ => false
      }

      expect.all(
        // Pre-reset: refused.
        refusedByFinality,
        preCount == 1L,
        // Post-reset: accepted.
        reconstructed,
        // The previously-refused canonical is now the bestTip (the core P-11 contract — the
        // reset unblocks the chain).
        postBestTip.contains(h2.hash)
      )
    }
  }

  // ============================================================
  // Track-3 S1.5 "marker split": the settled (k₂) ordinal is a source DISTINCT from the finalized
  // (k₁) ordinal. These gate the three S1.5 contracts — independent advancement, the store-gate
  // stays k₁ (NOT the deeper settled marker), and lock-step reset — WITHOUT re-keying the k₁
  // store-gate or the SnapshotLeaderLoop production floor (those move in S3).
  // ============================================================

  test("S1.5: settled (k₂) and finalized (k₁) advance independently; invariant settled ≤ finalized holds") { res =>
    implicit val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithSettled()
      (_, finalizedRef, settledRef) = r
      // The settled marker is advanced ONLY through the tracker (the leader loop's T_depth2 sink), which shares this exact ref.
      settledTracker = SettledOrdinalTracker.makeFromRef[IO](settledRef)
      // Advance finalized (k₁) to 100 — the settled (k₂) marker must NOT move.
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(100L)))
      settledAfterFinalizedBump <- settledRef.get
      // Advance settled (k₂) to 40 (deeper, smaller ordinal ≤ finalized) — finalized must NOT move.
      _ <- settledTracker.markSettled(SnapshotOrdinal(NonNegLong.unsafeFrom(40L)))
      finalizedAfterSettledBump <- finalizedRef.get
      settledFinal <- settledRef.get
    } yield
      expect.all(
        settledAfterFinalizedBump == SnapshotOrdinal.MinValue, // finalized advance left settled untouched
        finalizedAfterSettledBump == SnapshotOrdinal(NonNegLong.unsafeFrom(100L)), // settled advance left finalized untouched
        settledFinal == SnapshotOrdinal(NonNegLong.unsafeFrom(40L)),
        settledFinal.value.value <= finalizedAfterSettledBump.value.value // settled ≤ finalized invariant
      )
  }

  test("S1.5/S3 (flag OFF/default): store finality-gate keys off finalized (k₁), NOT the deeper settled (k₂) marker") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // Default band-density flag = OFF ⇒ legacy k₁ store-gate (the 2026-06-27 storm backstop preserved).
      r <- mkChainStoreWithSettled()
      (chainStore, finalizedRef, settledRef) = r
      // finalized (k₁) = 5, settled (k₂) = 1 — settled is the DEEPER (smaller) marker.
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(5L)))
      _ <- settledRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
      // Seed hash A at ordinal 3 — in the discriminating window settled(1) < 3 ≤ finalized(5).
      pairA <- mkGenesis
      (sA, ctxA) = pairA
      storedA <- chainStore.store(sA, ctxA, ordinal = 3L, slot = 3L, parentHash = Hash.empty, vrfOutput = Array.empty)
      // A DIFFERENT-hash write at ordinal 3 must be REFUSED — 3 ≤ finalized(5) with an existing divergent hash. With the S3
      // flag ON the gate would instead move to the settled marker (3 > settled(1) ⇒ ALLOW) — that is the next test.
      pairB <- mkAltSnapshot
      (sB, ctxB) = pairB
      storedB <- chainStore.store(sB, ctxB, ordinal = 3L, slot = 3L, parentHash = Hash.empty, vrfOutput = Array.empty)
      refuseCount <- chainStore.divergentRefuseCount
    } yield {
      val initialized = storedA match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(_, NakamotoChainStore.SelectionChange.Initialized) => true
        case _                                                                                                 => false
      }
      val rejectedAtK1 = storedB match {
        case NakamotoChainStore.StoreOutcome.Rejected(
              NakamotoChainStore.StoreRejection.FinalityConflict(3L, _, _, floor, false),
              Some(_)
            ) =>
          floor == SnapshotOrdinal.unsafeApply(5L)
        case _ => false
      }

      expect.all(
        initialized,
        rejectedAtK1,
        refuseCount == 1L
      )
    }
  }

  test("S3 (flag ON): store-gate keyed on settled (k₂) — divergent write in (settled, finalized] is routed to compare, NOT auto-refused") {
    res =>
      val (_, _, j, h, sp) = res
      implicit val jSer: JsonSerializer[IO] = j
      implicit val hh: Hasher[IO] = h
      implicit val spp: SecurityProvider[IO] = sp
      implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
      for {
        // Band-density flag ON ⇒ the store-gate keys off the k₂ settled marker (S3 floor-move).
        r <- mkChainStoreWithSettled(bandDensityReorgEnabled = true)
        (chainStore, finalizedRef, settledRef) = r
        // finalized (k₁) = 5, settled (k₂) = 1 — the (settled, finalized] band is ordinals 2..5.
        _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(5L)))
        _ <- settledRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))
        // Seed hash A at ordinal 3 — in the band settled(1) < 3 ≤ finalized(5).
        pairA <- mkGenesis
        (sA, ctxA) = pairA
        storedA <- chainStore.store(sA, ctxA, ordinal = 3L, slot = 3L, parentHash = Hash.empty, vrfOutput = Array.empty)
        // A DIFFERENT-hash write at ordinal 3: because 3 > settled(1), the S3 store-gate does NOT auto-refuse it — it
        // routes through to `tryStore` → `ChainSelection.shouldSwitch` (density-revertable band). It is stored (here as
        // an alternate branch, since with the stub fetchParent the density comparison can't find the MRCA to switch), and
        // crucially the P-11 divergent-refuse counter does NOT trip (proving the gate moved to settled, not finalized).
        pairB <- mkAltSnapshot
        (sB, ctxB) = pairB
        storedB <- chainStore.store(sB, ctxB, ordinal = 3L, slot = 3L, parentHash = Hash.empty, vrfOutput = Array.empty)
        refuseCount <- chainStore.divergentRefuseCount
        sample <- chainStore.divergentRefuseSample
      } yield {
        val initialized = storedA match {
          case NakamotoChainStore.StoreOutcome.BecameSelected(_, NakamotoChainStore.SelectionChange.Initialized) => true
          case _                                                                                                 => false
        }
        val alternate = storedB match {
          case NakamotoChainStore.StoreOutcome.StoredAlternate(stored, selected) =>
            stored.ordinal == 3L && selected.snapshot.hash =!= stored.hash
          case _ => false
        }

        expect.all(
          initialized,
          alternate,
          refuseCount == 0L, // the finality-safety gate did NOT trip (it keys off settled now, not finalized)
          sample.isEmpty
        )
      }
  }

  test("S1.5: unsafe_clearFinality resets BOTH finalized (k₁) and settled (k₂) to MinValue") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithSettled()
      (chainStore, finalizedRef, settledRef) = r
      // Seed chain state (so the reset reports work done) and drive both markers high (settled ≤ finalized).
      pair <- mkGenesis
      (s1, ctx1) = pair
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      _ <- finalizedRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(100L)))
      _ <- settledRef.set(SnapshotOrdinal(NonNegLong.unsafeFrom(40L)))
      cleared <- chainStore.unsafe_clearFinality
      postFinalized <- finalizedRef.get
      postSettled <- settledRef.get
    } yield
      expect.all(
        cleared, // state was cleared
        postFinalized == SnapshotOrdinal.MinValue, // k₁ reset
        postSettled == SnapshotOrdinal.MinValue // k₂ reset in lock-step (S1.5)
      )
  }

  // ============================================================
  // Decision logic tests for the orchestrator's pure decide() function.
  // No I/O — exercises the threshold + cooldown logic directly.
  // ============================================================

  pureTest("RebootstrapOrchestrator.decide: below threshold returns Quiet") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 2L,
      threshold = 3L,
      lastResetAtMs = None,
      nowMs = 1000L,
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Quiet, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold with no prior reset returns Trigger") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 3L,
      threshold = 3L,
      lastResetAtMs = None,
      nowMs = 1000L,
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Trigger, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold within cooldown returns Cooldown") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 10L,
      threshold = 3L,
      lastResetAtMs = Some(900L),
      nowMs = 1000L, // only 100ms ago
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Cooldown, d)
  }

  pureTest("RebootstrapOrchestrator.decide: at threshold past cooldown returns Trigger") {
    val d = RebootstrapOrchestrator.decide(
      refuseCount = 10L,
      threshold = 3L,
      lastResetAtMs = Some(900L),
      nowMs = 10000L, // 9.1s ago, past 5s cooldown
      cooldownMs = 5000L
    )
    expect.same(RebootstrapOrchestrator.Decision.Trigger, d)
  }

  // ============================================================
  // Heap-leak Fix B: byHash retention bounded by keepDepthBehindFinalized.
  // ============================================================

  test("Fix B: byHash retains all entries when chain length below keepDepthBehindFinalized") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // keepDepth=10, chain length=5 (well below). No eviction should fire.
      r <- mkChainStore(keepDepthBehindFinalized = 10L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 5)
      preSize <- chainStore.size
      // Finalize the tip — at ord=5, keepFloor = max(0, 5-10) = 0, so all canonical entries stay.
      finalizedOutcome <- finalizeCurrentSelection(chainStore, targetOrdinal = 5L)
      postSize <- chainStore.size
      tipPresent <- chainStore.get(hashes.last).map(_.isDefined)
      genesisPresent <- chainStore.get(hashes.head).map(_.isDefined)
    } yield
      expect.all(
        preSize == 5,
        finalizedExactly(finalizedOutcome, hashes.last, targetOrdinal = 5L, previousOrdinal = 0L),
        postSize == 5, // nothing evicted
        tipPresent,
        genesisPresent // genesis still in byHash because keepFloor=0
      )
  }

  test("Fix B: byHash evicts canonical entries below finalizedOrd - keepDepthBehindFinalized on finalize") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // keepDepth=3, chain length=10. Finalize at ord=10 ⇒ keepFloor=7. Ords 1..6 evicted.
      r <- mkChainStore(keepDepthBehindFinalized = 3L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 10)
      preSize <- chainStore.size
      finalizedOutcome <- finalizeCurrentSelection(chainStore, targetOrdinal = 10L)
      postSize <- chainStore.size
      // Entries at ords 7..10 (the keep-window) should remain. 4 entries: 10, 9, 8, 7.
      tipPresent <- chainStore.get(hashes(9)).map(_.isDefined) // ord 10
      keepFloorPresent <- chainStore.get(hashes(6)).map(_.isDefined) // ord 7
      belowKeepFloorAbsent <- chainStore.get(hashes(5)).map(_.isEmpty) // ord 6
      genesisAbsent <- chainStore.get(hashes.head).map(_.isEmpty) // ord 1
    } yield
      expect.all(
        preSize == 10,
        finalizedExactly(finalizedOutcome, hashes.last, targetOrdinal = 10L, previousOrdinal = 0L),
        postSize == 4, // 10, 9, 8, 7 retained
        tipPresent,
        keepFloorPresent,
        belowKeepFloorAbsent,
        genesisAbsent
      )
  }

  test("Fix B: subsequent finalize advances eviction floor (sliding window)") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore(keepDepthBehindFinalized = 3L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 15)
      // First finalize at ord=10 ⇒ keepFloor=7; entries 7..15 retained (9 entries).
      firstFinalized <- finalizeCurrentSelection(chainStore, targetOrdinal = 10L)
      sizeAfterFirst <- chainStore.size
      // Second finalize at ord=15 ⇒ keepFloor=12; entries 12..15 retained (4 entries).
      secondFinalized <- finalizeCurrentSelection(chainStore, targetOrdinal = 15L)
      sizeAfterSecond <- chainStore.size
      tipPresent <- chainStore.get(hashes(14)).map(_.isDefined) // ord 15
      newFloorPresent <- chainStore.get(hashes(11)).map(_.isDefined) // ord 12
      belowNewFloorAbsent <- chainStore.get(hashes(10)).map(_.isEmpty) // ord 11
      priorFloorAbsent <- chainStore.get(hashes(6)).map(_.isEmpty) // ord 7
    } yield
      expect.all(
        sizeAfterFirst == 9, // 7..15
        sizeAfterSecond == 4, // 12..15
        finalizedExactly(firstFinalized, hashes(9), targetOrdinal = 10L, previousOrdinal = 0L),
        finalizedExactly(secondFinalized, hashes(14), targetOrdinal = 15L, previousOrdinal = 10L),
        tipPresent,
        newFloorPresent,
        belowNewFloorAbsent,
        priorFloorAbsent
      )
  }

  test("Fix B: keepDepthBehindFinalized > 0 with finalizedOrd <= keepDepth is a no-op (genesis bootstrap)") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      // Production-ish: keepDepth=255. Chain shorter than that — keepFloor stays at 0.
      r <- mkChainStore(keepDepthBehindFinalized = 255L)
      (chainStore, _, _, _) = r
      hashes <- seedChainOfLength(chainStore, n = 20)
      finalizedOutcome <- finalizeCurrentSelection(chainStore, targetOrdinal = 20L)
      postSize <- chainStore.size
      genesisPresent <- chainStore.get(hashes.head).map(_.isDefined)
    } yield
      expect.all(
        finalizedExactly(finalizedOutcome, hashes.last, targetOrdinal = 20L, previousOrdinal = 0L),
        postSize == 20,
        genesisPresent
      )
  }

  pureTest("Fix B: DefaultKeepDepthBehindFinalized is 255 (matches operational confirmation depth k₁)") {
    expect.same(255L, NakamotoChainStore.DefaultKeepDepthBehindFinalized)
  }

  // ============================================================
  // Path 1 (heap-leak workstream): `getWithOrdinalFallback` — chain walks remain correct
  // across the Fix B eviction boundary by falling through to disk-backed `SnapshotStorage`.
  // ============================================================

  test("getWithOrdinalFallback: in-memory hit returns the in-memory entry without touching disk") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, _) = r
      pair <- mkGenesis
      (s1, ctx1) = pair
      _ <- chainStore.store(s1, ctx1, ordinal = 1L, slot = 1L, parentHash = Hash.empty, vrfOutput = Array.empty)
      h1 <- s1.toHashed[IO]
      out <- chainStore.getWithOrdinalFallback(h1.hash, expectedOrdinal = 1L)
    } yield expect.all(out.isDefined, out.exists(_.hash === h1.hash))
  }

  test("getWithOrdinalFallback: in-memory miss falls through to disk on matching ordinal+hash") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, diskRef, diskByHashRef) = r
      item <- mkSignedLinkedChain(7).map(_.last)
      // DON'T store in chainStore (so byHash misses). DO put on disk so the fallback engages.
      _ <- diskRef.update(_.updated(item.signed.value.ordinal, item.signed))
      _ <- diskByHashRef.update(_.updated(item.hash, item.signed))
      preInMem <- chainStore.get(item.hash)
      out <- chainStore.getWithOrdinalFallback(item.hash, expectedOrdinal = 7L)
    } yield
      expect.all(
        preInMem.isEmpty, // byHash miss confirmed
        out.isDefined, // disk fallback succeeded
        out.exists(_.hash === item.hash) // returned entry's hash matches request
      )
  }

  test("getWithOrdinalFallback: hash mismatch (fork case) on disk returns None") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, diskRef, _) = r
      itemA <- mkSignedLinkedChain(5).map(_.last)
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      signedB <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        itemA.signed.value.copy(epochProgress = EpochProgress(NonNegLong.unsafeFrom(999L))),
        keyPair
      )
      hashedB <- signedB.toHashed[IO]
      _ <- diskRef.update(_.updated(itemA.signed.value.ordinal, itemA.signed))
      out <- chainStore.getWithOrdinalFallback(hashedB.hash, expectedOrdinal = 5L)
    } yield expect.same(None, out) // hash-verify rejects the disk's different-chain snapshot
  }

  test("getWithOrdinalFallback: wrong embedded ordinal is rejected before hashing") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, diskRef, _) = r
      item <- mkSignedLinkedChain(1).map(_.head)
      _ <- diskRef.update(_.updated(SnapshotOrdinal.unsafeApply(5L), item.signed))
      out <- chainStore.getWithOrdinalFallback(item.hash, expectedOrdinal = 5L)
    } yield expect.same(None, out)
  }

  test("getWithOrdinalFallback: in-memory miss AND disk miss returns None") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, _) = r
      pair <- mkGenesis
      (s1, _) = pair
      h1 <- s1.toHashed[IO]
      out <- chainStore.getWithOrdinalFallback(h1.hash, expectedOrdinal = 99L)
    } yield expect.same(None, out)
  }

  test("getWithOrdinalFallback: negative expectedOrdinal returns None without disk read") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, _) = r
      out <- chainStore.getWithOrdinalFallback(Hash.empty, expectedOrdinal = -1L)
    } yield expect.same(None, out)
  }

  test("walkBackExact: proves a complete signed, contiguous in-memory ancestry path") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      chain <- mkSignedLinkedChain(3)
      _ <- chain.traverse_ { item =>
        chainStore
          .store(
            item.signed,
            item.context,
            item.signed.value.ordinal.value.value,
            item.signed.value.ordinal.value.value,
            item.signed.value.lastSnapshotHash,
            Array.empty
          )
          .void
      }
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(chain.last.hash, chain.last.signed.value.ordinal),
        chain.head.signed.value.ordinal,
        maxSteps = 3
      )
    } yield
      result match {
        case Right(NakamotoChainStore.ExactWalkResult.Complete(path)) =>
          expect.same(chain.reverse.map(_.hash).toVector, path.map(_.position.hash))
        case other => failure(s"expected complete exact ancestry, got $other")
      }
  }

  test("walkBackExact: cross-era reconstruction rejects until canonical identity migrates end to end") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    val historicalHash = Hash("66" * 32)
    val historicalHasher = new Hasher[IO] {
      def hash[A: io.circe.Encoder](data: A): IO[Hash] = IO.pure(historicalHash)
      def hashBytes(bytes: Array[Byte]): IO[Hash] = IO.pure(historicalHash)
      def compare[A: io.circe.Encoder](data: A, expectedHash: Hash): IO[Boolean] = IO.pure(expectedHash === historicalHash)
      def getLogic(ordinal: SnapshotOrdinal): HashLogic = KryoHash
      def prefixedHash[A: io.circe.Encoder](data: A, prefix: Array[Byte]): IO[Hash] = IO.pure(historicalHash)
    }
    implicit val hs: HasherSelector[IO] = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = h
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] =
        if (ordinal == SnapshotOrdinal.unsafeApply(1L)) historicalHasher else h
    }

    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      item <- mkSignedLinkedChain(1).map(_.head)
      stored <- chainStore.store(
        item.signed,
        item.context,
        ordinal = 1L,
        slot = 1L,
        parentHash = Hash.empty,
        vrfOutput = Array.empty
      )
      storedByHistoricalHash <- chainStore.get(historicalHash)
      storedByCurrentHash <- chainStore.get(item.hash)
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(item.hash, SnapshotOrdinal.unsafeApply(1L)),
        SnapshotOrdinal.unsafeApply(1L),
        maxSteps = 1
      )
    } yield {
      val initialized = stored match {
        case NakamotoChainStore.StoreOutcome.BecameSelected(_, NakamotoChainStore.SelectionChange.Initialized) => true
        case _                                                                                                 => false
      }

      expect(initialized) &&
      expect(storedByHistoricalHash.isEmpty) &&
      expect(storedByCurrentHash.nonEmpty) &&
      expect.same(
        Left(
          NakamotoChainStore.ExactWalkError.HashEraUnavailable(
            NakamotoChainStore.ExactWalkPosition(item.hash, SnapshotOrdinal.unsafeApply(1L)),
            JsonHash,
            KryoHash
          )
        ),
        result
      )
    }
  }

  test("walkBackExact: content hashing failures remain inside the typed result") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    val failure = new IllegalStateException("historical hash codec failed")
    val failingHasher = new Hasher[IO] {
      def hash[A: io.circe.Encoder](data: A): IO[Hash] = IO.raiseError(failure)
      def hashBytes(bytes: Array[Byte]): IO[Hash] = IO.raiseError(failure)
      def compare[A: io.circe.Encoder](data: A, expectedHash: Hash): IO[Boolean] = IO.raiseError(failure)
      def getLogic(ordinal: SnapshotOrdinal): HashLogic = JsonHash
      def prefixedHash[A: io.circe.Encoder](data: A, prefix: Array[Byte]): IO[Hash] = IO.raiseError(failure)
    }
    implicit val hs: HasherSelector[IO] = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = failingHasher
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] = failingHasher
    }

    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, diskByHashRef) = r
      item <- mkSignedLinkedChain(1).map(_.head)
      _ <- diskByHashRef.set(Map(item.hash -> item.signed))
      position = NakamotoChainStore.ExactWalkPosition(item.hash, item.signed.value.ordinal)
      result <- chainStore.walkBackExact(position, position.ordinal, maxSteps = 1)
    } yield
      expect.same(
        Left(NakamotoChainStore.ExactWalkError.ContentHashFailed(position, failure.getMessage)),
        result
      )
  }

  test("walkBackExact: hash selector failures remain inside the typed result") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    val failure = new IllegalStateException("hash era selector failed")
    implicit val hs: HasherSelector[IO] = new HasherSelector[IO] {
      def getCurrent: Hasher[IO] = throw failure
      def getForOrdinal(ordinal: SnapshotOrdinal): Hasher[IO] = h
    }

    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, diskByHashRef) = r
      item <- mkSignedLinkedChain(1).map(_.head)
      _ <- diskByHashRef.set(Map(item.hash -> item.signed))
      position = NakamotoChainStore.ExactWalkPosition(item.hash, item.signed.value.ordinal)
      result <- chainStore.walkBackExact(position, position.ordinal, maxSteps = 1)
    } yield
      expect.same(
        Left(NakamotoChainStore.ExactWalkError.ContentHashFailed(position, failure.getMessage)),
        result
      )
  }

  test("walkBackExact: exact-hash disk record wins over a same-ordinal sibling") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, diskRef, diskByHashRef) = r
      chain <- mkSignedLinkedChain(1)
      original = chain.head
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      siblingSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        original.signed.value.copy(epochProgress = EpochProgress(NonNegLong.unsafeFrom(999L))),
        keyPair
      )
      siblingHashed <- siblingSigned.toHashed[IO]
      ordinal = original.signed.value.ordinal
      _ <- diskRef.set(Map(ordinal -> siblingSigned))
      _ <- diskByHashRef.set(Map(original.hash -> original.signed))
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(original.hash, ordinal),
        ordinal,
        maxSteps = 1
      )
    } yield
      result match {
        case Right(NakamotoChainStore.ExactWalkResult.Complete(path)) =>
          expect.same(Vector(original.hash), path.map(_.position.hash)) &&
          expect(original.hash =!= siblingHashed.hash)
        case other => failure(s"expected exact hash-addressed record, got $other")
      }
  }

  test("walkBackExact: ordinal fallback reports a sibling as incomplete, never complete") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, diskRef, _) = r
      chain <- mkSignedLinkedChain(1)
      requested = chain.head
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      siblingSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        requested.signed.value.copy(epochProgress = EpochProgress(NonNegLong.unsafeFrom(999L))),
        keyPair
      )
      siblingHashed <- siblingSigned.toHashed[IO]
      ordinal = requested.signed.value.ordinal
      _ <- diskRef.set(Map(ordinal -> siblingSigned))
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(requested.hash, ordinal),
        ordinal,
        maxSteps = 1
      )
    } yield
      expect.same(
        Right(
          NakamotoChainStore.ExactWalkResult.Incomplete(
            Vector.empty,
            NakamotoChainStore.ExactWalkPosition(requested.hash, ordinal),
            NakamotoChainStore.ExactWalkIncompleteReason.SiblingAtOrdinal(siblingHashed.hash)
          )
        ),
        result
      )
  }

  test("walkBackExact: unknown start is incomplete and cannot substitute the ordinal record") { res =>
    val (_, _, _, h, _) = res
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val position = NakamotoChainStore.ExactWalkPosition(Hash("22" * 32), SnapshotOrdinal.unsafeApply(7L))
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, _) = r
      result <- chainStore.walkBackExact(position, position.ordinal, maxSteps = 1)
    } yield
      expect.same(
        Right(
          NakamotoChainStore.ExactWalkResult.Incomplete(
            Vector.empty,
            position,
            NakamotoChainStore.ExactWalkIncompleteReason.NotFound
          )
        ),
        result
      )
  }

  test("walkBackExact: a missing middle ancestor returns the exact missing hash and ordinal") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      chain <- mkSignedLinkedChain(3)
      child = chain.last
      _ <- chainStore.store(
        child.signed,
        child.context,
        child.signed.value.ordinal.value.value,
        child.signed.value.ordinal.value.value,
        child.signed.value.lastSnapshotHash,
        Array.empty
      )
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(child.hash, child.signed.value.ordinal),
        chain.head.signed.value.ordinal,
        maxSteps = 3
      )
    } yield
      result match {
        case Right(NakamotoChainStore.ExactWalkResult.Incomplete(path, missing, reason)) =>
          expect.same(Vector(child.hash), path.map(_.position.hash)) &&
          expect.same(chain(1).hash, missing.hash) &&
          expect.same(SnapshotOrdinal.unsafeApply(2L), missing.ordinal) &&
          expect.same(NakamotoChainStore.ExactWalkIncompleteReason.NotFound, reason)
        case other => failure(s"expected incomplete exact ancestry, got $other")
      }
  }

  test("walkBackExact: rejects caller metadata that disagrees with signed ordinal or parent") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val wrongParent = Hash("33" * 32)
    for {
      ordinalStoreFixture <- mkChainStore()
      (ordinalStore, _, _, _) = ordinalStoreFixture
      parentStoreFixture <- mkChainStore()
      (parentStore, _, _, _) = parentStoreFixture
      chain <- mkSignedLinkedChain(1)
      item = chain.head
      _ <- ordinalStore.store(item.signed, item.context, 2L, 2L, item.signed.value.lastSnapshotHash, Array.empty)
      ordinalResult <- ordinalStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(item.hash, SnapshotOrdinal.unsafeApply(2L)),
        SnapshotOrdinal.unsafeApply(2L),
        maxSteps = 1
      )
      _ <- parentStore.store(item.signed, item.context, 1L, 1L, wrongParent, Array.empty)
      parentResult <- parentStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(item.hash, SnapshotOrdinal.unsafeApply(1L)),
        SnapshotOrdinal.unsafeApply(1L),
        maxSteps = 1
      )
    } yield
      expect(
        ordinalResult.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.SignedOrdinalMismatch])
      ) &&
        expect(parentResult.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.StoredParentMismatch]))
  }

  test("walkBackExact: rejects wrong bytes returned by the exact hash index") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val requestedHash = Hash("44" * 32)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, diskByHashRef) = r
      chain <- mkSignedLinkedChain(1)
      item = chain.head
      _ <- diskByHashRef.set(Map(requestedHash -> item.signed))
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(requestedHash, item.signed.value.ordinal),
        item.signed.value.ordinal,
        maxSteps = 1
      )
    } yield expect(result.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.ExactHashContentMismatch]))
  }

  test("walkBackExact: rejects non-canonical requested hashes and signed parents") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    val ordinal = SnapshotOrdinal.unsafeApply(1L)
    val nonCanonical = Hash("ABC")
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, diskByHashRef) = r
      invalidRequested <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(nonCanonical, ordinal),
        ordinal,
        maxSteps = 1
      )
      chain <- mkSignedLinkedChain(1)
      template = chain.head
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      invalidParentSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        template.signed.value.copy(lastSnapshotHash = nonCanonical),
        keyPair
      )
      invalidParentHashed <- invalidParentSigned.toHashed[IO]
      _ <- diskByHashRef.set(Map(invalidParentHashed.hash -> invalidParentSigned))
      invalidParent <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(invalidParentHashed.hash, ordinal),
        ordinal,
        maxSteps = 1
      )
    } yield
      expect.same(
        Left(
          NakamotoChainStore.ExactWalkError.NonCanonicalSnapshotHash(
            NakamotoChainStore.ExactWalkPosition(nonCanonical, ordinal),
            NakamotoChainStore.ExactWalkHashRole.Requested,
            nonCanonical
          )
        ),
        invalidRequested
      ) &&
        expect(
          invalidParent.left.toOption.contains(
            NakamotoChainStore.ExactWalkError.NonCanonicalSnapshotHash(
              NakamotoChainStore.ExactWalkPosition(invalidParentHashed.hash, ordinal),
              NakamotoChainStore.ExactWalkHashRole.SignedParent,
              nonCanonical
            )
          )
        )
  }

  test("walkBackExact: rejects ordinal discontinuity and over-budget requests") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    for {
      r <- mkChainStore()
      (chainStore, _, _, _) = r
      chain <- mkSignedLinkedChain(1)
      parent = chain.head
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      childSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        parent.signed.value.copy(
          ordinal = SnapshotOrdinal.unsafeApply(3L),
          lastSnapshotHash = parent.hash,
          epochProgress = EpochProgress(NonNegLong.unsafeFrom(3L))
        ),
        keyPair
      )
      childHashed <- childSigned.toHashed[IO]
      _ <- chainStore.store(parent.signed, parent.context, 1L, 1L, Hash.empty, Array.empty)
      _ <- chainStore.store(childSigned, parent.context, 3L, 3L, parent.hash, Array.empty)
      start = NakamotoChainStore.ExactWalkPosition(childHashed.hash, SnapshotOrdinal.unsafeApply(3L))
      discontinuity <- chainStore.walkBackExact(start, SnapshotOrdinal.unsafeApply(1L), maxSteps = 3)
      overBudget <- chainStore.walkBackExact(start, SnapshotOrdinal.unsafeApply(1L), maxSteps = 2)
      aboveStart <- chainStore.walkBackExact(start, SnapshotOrdinal.unsafeApply(4L), maxSteps = 1)
      invalidBudget <- chainStore.walkBackExact(start, SnapshotOrdinal.unsafeApply(3L), maxSteps = 0)
    } yield
      expect(discontinuity.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.StoredOrdinalMismatch])) &&
        expect(overBudget.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.RequiredStepsExceedLimit])) &&
        expect(aboveStart.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.TargetAboveStart])) &&
        expect.same(Left(NakamotoChainStore.ExactWalkError.InvalidMaxSteps(0)), invalidBudget)
  }

  test("walkBackExact: detects a repeated content-addressed parent before a second read") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    val repeatedHash = Hash("55" * 32)
    val constantHasher = new Hasher[IO] {
      def hash[A: io.circe.Encoder](data: A): IO[Hash] = IO.pure(repeatedHash)
      def hashBytes(bytes: Array[Byte]): IO[Hash] = IO.pure(repeatedHash)
      def compare[A: io.circe.Encoder](data: A, expectedHash: Hash): IO[Boolean] = IO.pure(expectedHash === repeatedHash)
      def getLogic(ordinal: SnapshotOrdinal): HashLogic = JsonHash
      def prefixedHash[A: io.circe.Encoder](data: A, prefix: Array[Byte]): IO[Hash] = IO.pure(repeatedHash)
    }
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(constantHasher)
    for {
      r <- mkChainStoreWithDisk()
      (chainStore, _, _, diskByHashRef) = r
      template <- mkSignedLinkedChain(1).map(_.head)
      keyPair <- KeyPairGenerator.makeKeyPair[IO]
      cyclicSigned <- Signed.forAsyncHasher[IO, GlobalIncrementalSnapshot](
        template.signed.value.copy(
          ordinal = SnapshotOrdinal.unsafeApply(2L),
          lastSnapshotHash = repeatedHash,
          epochProgress = EpochProgress(NonNegLong.unsafeFrom(2L))
        ),
        keyPair
      )
      _ <- diskByHashRef.set(Map(repeatedHash -> cyclicSigned))
      result <- chainStore.walkBackExact(
        NakamotoChainStore.ExactWalkPosition(repeatedHash, SnapshotOrdinal.unsafeApply(2L)),
        SnapshotOrdinal.unsafeApply(1L),
        maxSteps = 2
      )
    } yield expect(result.left.toOption.exists(_.isInstanceOf[NakamotoChainStore.ExactWalkError.CycleDetected]))
  }

  test("vrfOutputsForPeriod: walks across the Fix B eviction boundary via disk fallback") { res =>
    val (_, _, j, h, sp) = res
    implicit val jSer: JsonSerializer[IO] = j
    implicit val hh: Hasher[IO] = h
    implicit val spp: SecurityProvider[IO] = sp
    implicit val hs: HasherSelector[IO] = HasherSelector.forSyncAlwaysCurrent(h)
    // Cluster topology: keepDepth=3, etaRotation=6 (so a "period" spans 6 ords; cutoff at ords ≤
    // 2/3·6 = 4 within the source period). We seed 10 snapshots, finalize at ord=10 ⇒ keepFloor=7.
    // For period=1 (starting at ord=6, with cutoff at ord=6+4=10), VRF outputs are taken from ords
    // 6..9 ∩ `vrfOutput.nonEmpty`. After eviction, ord 6 is below floor and absent in-memory;
    // disk-fallback must pick it up. Period 0 (ord<6) is fully evicted but we don't query it here
    // since `walkBackTo periodStart` stops at 6.
    //
    // Note on the VRF-output check: the genesis fixtures used here don't carry a `SlotCertificate`
    // (slotCertificate=None), so the disk-recovered `StoredSnapshot.vrfOutput` has an empty immutable value
    // and the walk's `vrfOutput.nonEmpty` filter drops it. The walk itself reaches ord=6 — the
    // assertion below verifies the walk-reachability (parent-resolution across the boundary) via
    // the fact that ords 7..9 are returned, which requires the walk to have visited ord=7 via the
    // in-memory path and ord=8, 9 from there. The cross-boundary case is exercised in the
    // production code path where `SlotCertificate` IS present (the on-disk `Signed`'s
    // `slotCertificate.vrfOutput` is non-empty).
    val keepDepth = 3L
    val etaRotation = 6L
    for {
      r <- mkChainStoreWithDisk(keepDepthBehindFinalized = keepDepth)
      (chainStore, _, diskRef, diskByHashRef) = r
      // Seed 10 stores with non-empty vrfOutput so collectVrfOutputsForPeriod can pick them up.
      chain <- mkSignedLinkedChain(10)
      hashes <- chain.traverse { item =>
        val ordinal = item.signed.value.ordinal.value.value
        val vrf = Array.fill[Byte](32)(ordinal.toByte)
        chainStore
          .store(
            item.signed,
            item.context,
            ordinal = ordinal,
            slot = ordinal,
            parentHash = item.signed.value.lastSnapshotHash,
            vrfOutput = vrf
          ) >>
          diskRef.update(_.updated(item.signed.value.ordinal, item.signed)) >>
          diskByHashRef.update(_.updated(item.hash, item.signed)).as(item.hash)
      }
      completeBeforeEviction <- chainStore.vrfOutputRangeForPeriodFrom(
        period = 1L,
        etaRotationSnapshots = etaRotation,
        fromHash = hashes.last
      )
      prematurePrefix <- chainStore.vrfOutputRangeForPeriodFrom(
        period = 1L,
        etaRotationSnapshots = etaRotation,
        fromHash = hashes(8) // ordinal 9 is below the exclusive cutoff at ordinal 10
      )
      // Finalize at ord=10 ⇒ keepFloor = max(0, 10-3) = 7. Ords 1..6 evicted from byHash.
      finalizedOutcome <- finalizeCurrentSelection(chainStore, targetOrdinal = 10L)
      postSize <- chainStore.size
      lowestPresent <- chainStore.get(hashes(6)).map(_.isDefined) // ord 7
      ord6Absent <- chainStore.get(hashes(5)).map(_.isEmpty) // ord 6 (below floor)
      // The walk: bestTip (ord 10) → walks back through in-memory entries down to ord 7 (kept).
      // Then ord 7's signed parent resolves through the exact linked disk record at ord 6.
      // That proves the eviction-boundary lookup itself worked. The disk fixture has no slot
      // certificate, so ord 6 lacks mandatory rho evidence and the range correctly remains
      // incomplete rather than silently treating ancestry alone as eta completeness.
      incompleteAfterEviction <- chainStore.vrfOutputRangeForPeriodFrom(
        period = 1L,
        etaRotationSnapshots = etaRotation,
        fromHash = hashes.last
      )
      ordsRecovered = incompleteAfterEviction.outputs.map(_._1).toSet

      // Cross-check: the walk DID visit ord 6 (the disk-fallback engaged); we verify this via
      // `getWithOrdinalFallback` directly to demonstrate the disk path returns a value, and we
      // verify in-memory ords are present.
      ord6Resolved <- chainStore.getWithOrdinalFallback(hashes(5), expectedOrdinal = 6L)
    } yield
      expect.all(
        postSize == 4, // 7..10 retained in-memory
        finalizedExactly(finalizedOutcome, hashes.last, targetOrdinal = 10L, previousOrdinal = 0L),
        lowestPresent,
        ord6Absent,
        // Direct fallback test: ord 6 IS reachable via disk fallback.
        ord6Resolved.isDefined,
        ord6Resolved.exists(_.hash === hashes(5)),
        completeBeforeEviction.isInstanceOf[NakamotoChainStore.VrfOutputRange.Complete],
        prematurePrefix.isInstanceOf[NakamotoChainStore.VrfOutputRange.Incomplete],
        incompleteAfterEviction.isInstanceOf[NakamotoChainStore.VrfOutputRange.Incomplete],
        // In-memory ords contribute their VRF outputs to the walk (production fixture would also
        // include the disk-recovered ord 6 since its SlotCertificate.vrfOutput would be set).
        ordsRecovered.contains(7L),
        ordsRecovered.contains(8L),
        ordsRecovered.contains(9L),
        !ordsRecovered.contains(10L) // cutoff exclusive
      )
  }
}
