package io.constellationnetwork.dag.l0.infrastructure.snapshot.nakamoto

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.{StateChangesAccumulator, applyAccumulatorToGSI}
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.Signed.forAsyncHasher

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import org.typelevel.log4cats.noop.NoOpLogger
import weaver.MutableIOSuite

/** Pins the byte-faithful catch-up MPT seeder ([[NakamotoSyncDaemon.seedMptByteFaithful]]) — the gl0 analog of ml0's `resyncToCanonical`
  * verify gate, and the root fix for the catch-up wedge. The contract:
  *
  *   - '''Some(signed bytes) matching the signed mptRoot → adopt VERBATIM, return Some(bytes)''' — the MPT ends up with exactly the served
  *     bytes (sidecar-free root === signed), and the RETURNED map recomputes to the signed root too (signed-byte-store FIDELITY: it is what
  *     the caller persists into `mpt_snapshot_info_signed` at the adopted finalized ordinal). This is the path that, unlike the old GSI
  *     rebuild, reproduces the producer's per-MG currency root.
  *   - '''Some(bytes) whose root ≠ signed mptRoot → return None AND leave the MPT untouched''' — the recompute happens BEFORE any write, so
  *     a corrupt/truncated transfer never clobbers the live store.
  *   - '''None (byte route 404) + GSI that rebuilds to the signed root → return Some(verified rebuild bytes)''' (legacy fallback gate
  *     passes; the returned candidate map recomputes to the signed root).
  *   - '''None + signed mptRoot the GSI rebuild can't reproduce → return None''' (fallback gate rejects rather than adopt-divergent).
  */
object SeedMptByteFaithfulSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] = for {
    implicit0(sp: SecurityProvider[IO]) <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  private val ord = SnapshotOrdinal(NonNegLong(7L))
  private val addrA: Address = Address("DAG2FGeUYivtEo9EjvpELY4ZS7zDQWvJzQYVzXkX")

  private def freshStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  /** A non-trivial post-state: one balance entry. The exact contents are irrelevant — only that the producer (`syncFromStateChanges`) and
    * the rebuild (`syncFromGlobalSnapshotInfo`) land the same sidecar-free root (the #107 byte-equivalence contract the parity suite
    * proves).
    */
  private val acc: StateChangesAccumulator =
    StateChangesAccumulator(balances = SortedMap(addrA -> Balance(NonNegLong(100L))))

  /** Build a Hashed gl0 snapshot whose committed `stateProof.mptRoot` is exactly `signedRoot` (every other proof slot is irrelevant to the
    * seeder's gate, so left empty/None). Signed so `.toHashed` yields a real `Hashed`.
    */
  private def mkSnapshot(
    signedRoot: Option[Hash]
  )(implicit h: Hasher[IO], j: JsonSerializer[IO], sp: SecurityProvider[IO]): IO[Hashed[GlobalIncrementalSnapshot]] = {
    val proof = GlobalSnapshotStateProof(
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
      signedRoot, // mptRoot (field 17)
      None, // historicalStakeSnapshots
      None // smtRoot
    )
    val snapshot = GlobalIncrementalSnapshot(
      ord,
      Height.MinValue,
      SubHeight.MinValue,
      Hash.empty,
      SortedSet.empty,
      SortedMap.empty,
      SortedMap.empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint],
      SortedSet.empty,
      None,
      EpochProgress.MinValue,
      NonEmptyList.of(PeerId(io.constellationnetwork.security.hex.Hex(""))),
      SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = proof,
      Some(SortedSet.empty),
      Some(SortedSet.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedSet.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty)
    )
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      signed <- forAsyncHasher(snapshot, kp)
      hashed <- signed.toHashed
    } yield hashed
  }

  /** Producer-truth: the signed byte map + its sidecar-free root, derived by running `syncFromStateChanges` into a reference store. */
  private def producerTruth(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[(Map[Hex, Array[Byte]], Hash)] =
    for {
      ref <- freshStore
      _ <- ref.syncFromStateChanges(acc, ord)
      entries <- ref.allEntriesAsBytes
      root <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](entries)
    } yield (entries, root)

  test("Some(signed bytes) matching signed mptRoot → adopts verbatim, returns the verified bytes, store root === signed") { res =>
    implicit val (j, h, sp) = res
    val logger = NoOpLogger[IO]
    for {
      (entries, root) <- producerTruth
      snapshot <- mkSnapshot(root.some)
      store <- freshStore
      adopted <- NakamotoSyncDaemon.seedMptByteFaithful[IO](snapshot, GlobalSnapshotInfo.empty, entries.some, store, logger)
      after <- store.allEntriesAsBytes
      afterRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](after)
      // Signed-byte-store FIDELITY: the returned map is what the caller persists into `mpt_snapshot_info_signed` at the pulled
      // FINALIZED ordinal — it MUST itself recompute to the signed root (here it is the served bytes verbatim).
      adoptedRoot <- adopted.traverse(GlobalSnapshotInfo.sidecarFreeMptRoot[IO])
    } yield
      expect(adopted.isDefined)
        .and(expect(afterRoot === root))
        .and(expect(after.nonEmpty))
        .and(expect.same(adoptedRoot, root.some))
  }

  test("Some(bytes) whose root ≠ signed mptRoot → returns None AND does NOT clobber the MPT (verify-before-write)") { res =>
    implicit val (j, h, sp) = res
    val logger = NoOpLogger[IO]
    for {
      (entries, _) <- producerTruth
      // Snapshot commits to a DIFFERENT mptRoot than the served bytes recompute to ⇒ a corrupt/wrong transfer.
      snapshot <- mkSnapshot(Hash("ff".padTo(64, 'a').take(64)).some)
      store <- freshStore
      adopted <- NakamotoSyncDaemon.seedMptByteFaithful[IO](snapshot, GlobalSnapshotInfo.empty, entries.some, store, logger)
      after <- store.allEntriesAsBytes
    } yield expect(adopted.isEmpty).and(expect(after.isEmpty)) // never written — the gate failed before loadBytes
  }

  test("None (byte route 404) + GSI rebuilding to the signed root → legacy fallback gate passes, returns the verified rebuild bytes") {
    res =>
      implicit val (j, h, sp) = res
      val logger = NoOpLogger[IO]
      val gsi = applyAccumulatorToGSI(GlobalSnapshotInfo.empty, acc)
      for {
        (_, root) <- producerTruth
        snapshot <- mkSnapshot(root.some)
        store <- freshStore
        adopted <- NakamotoSyncDaemon.seedMptByteFaithful[IO](snapshot, gsi, none, store, logger)
        // Signed-byte-store FIDELITY: the fallback's returned map (the verified GSI-rebuild candidate) must ALSO recompute to the
        // signed root — it is what the catch-up persists at the adopted ordinal.
        adoptedRoot <- adopted.traverse(GlobalSnapshotInfo.sidecarFreeMptRoot[IO])
      } yield expect(adopted.isDefined).and(expect.same(adoptedRoot, root.some))
  }

  test("None + signed mptRoot the GSI rebuild can't reproduce → fallback gate rejects, returns None") { res =>
    implicit val (j, h, sp) = res
    val logger = NoOpLogger[IO]
    val gsi = applyAccumulatorToGSI(GlobalSnapshotInfo.empty, acc)
    for {
      snapshot <- mkSnapshot(Hash("ab".padTo(64, 'c').take(64)).some)
      store <- freshStore
      adopted <- NakamotoSyncDaemon.seedMptByteFaithful[IO](snapshot, gsi, none, store, logger)
    } yield expect(adopted.isEmpty)
  }
}
