package io.constellationnetwork.node.shared.domain.nakamoto.overlay

import cats.Show
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ParentChildTree
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.generators._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.addressSetImmutableCodec
import io.constellationnetwork.serde.codecs.instances.HashCodec.{immutableCodec => hashImmutable}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.types.numeric.NonNegLong
import org.scalacheck.Gen
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

/** #56.10 Phase D acceptance gate.
  *
  * Asserts byte-parity between the two GSAM accept() write paths, fed the same `StateChangesAccumulator` slice:
  *   - '''Path A (legacy):''' `mptStore.syncFromStateChanges(acc, ordinal)` — the production write path.
  *   - '''Path F (algebra over passthrough):''' writer-algebra (`AcceptanceMpt[F]`) over `MptOverlay.passthrough` — `handle.update[V]` per
  *     field, sidecar read-modify-write through `overlay.get` + `handle.insert`, `overlay.commit`, `overlay.buildRoot`.
  *   - '''Path G (algebra over multi-branch, optional):''' same writer-algebra over `MptOverlay.MultiBranch`. After `commit` the writes live
  *     in the per-branch `ChangeSet`; `finalizeBranch` folds them into the base. Post-finalize bytes must match path A. Exercises the
  *     `ChangeSet`-accumulation path Phase D will adopt in production.
  *
  * Failure on root or per-key bytes blocks Phase D — silent divergence here would be a consensus-correctness regression. We compare both
  * dimensions (root + entries) because root agreement can mask per-key drift on a serialiser that happens to commute under `withChanges`.
  *
  * Coverage: balances, lastTxRefs, lastStateChannelSnapshotHashes, plus their `ActiveAddressIndex` sidecars. Three typed `Map[Address,
  * V]`-shape fields (Hash, ref, NonNegLong-newtype) is the right gate per the handoff — Phase D's full integration exercises the rest.
  */
object GsamWritePathParitySuite extends MutableIOSuite with Checkers {

  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  implicit val stateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (j, h, sp)

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private val parentBranch: BranchId = BranchId(Hash("0" * 64))
  private val childBranch: BranchId = BranchId(Hash("1" * 64))

  /** Random-but-bounded GSI subset — same shape as `MptCrossPathDeterminismSuite.GsiSlice`. Three fields cover Hash/ref/NonNegLong-newtype
    * codecs plus their sidecars; Phase D's full integration exercises the remaining 24 fields end-to-end.
    */
  final case class GsiSlice(
    balances: SortedMap[Address, Balance],
    lastTxRefs: SortedMap[Address, TransactionReference],
    stateChanHashes: SortedMap[Address, Hash]
  ) {
    def toAccumulator: StateChangesAccumulator =
      StateChangesAccumulator(
        balances = balances,
        lastTxRefs = lastTxRefs,
        lastStateChannelSnapshotHashes = stateChanHashes
      )
  }

  implicit val showGsiSlice: Show[GsiSlice] = Show.show { s =>
    s"GsiSlice(balances=${s.balances.size}, lastTxRefs=${s.lastTxRefs.size}, stateChanHashes=${s.stateChanHashes.size})"
  }

  private val gsiSliceGen: Gen[GsiSlice] = for {
    n <- Gen.chooseNum(0, 8)
    addrs <- Gen.listOfN(n, addressGen)
    bals <- Gen.listOfN(n, balanceGen)
    refs <- Gen.listOfN(n, transactionReferenceGen)
    hashLabels <- Gen.listOfN(n, Gen.alphaNumStr.suchThat(_.nonEmpty))
  } yield {
    val pairs = addrs.distinct
    val take = pairs.size.min(n)
    GsiSlice(
      balances = SortedMap(pairs.zip(bals).take(take): _*),
      lastTxRefs = SortedMap(pairs.zip(refs).take(take): _*),
      stateChanHashes = SortedMap(pairs.zip(hashLabels.map(testHash)).take(take): _*)
    )
  }

  private def freshStore(implicit hasher: Hasher[IO], j: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](producer, GlobalStateKey.toHex[IO])
    } yield store

  private def bytesEntriesEq(a: Map[Hex, Array[Byte]], b: Map[Hex, Array[Byte]]): Boolean =
    a.view.mapValues(_.toVector).toMap == b.view.mapValues(_.toVector).toMap

  /** Apply a slice through the writer-algebra. Mirrors the per-field insert order and the `applyActiveAddressIndexDelta` sidecar maintenance
    * of `GlobalStateConverter.syncFromStateChanges` (lines ~1600–1660), but expressed against `AcceptanceMpt[F]` instead of `MptStore[F, K]`
    * directly. Produces the same final base bytes when the algebra is correct — that is the parity contract.
    *
    * Sidecar: only the `ActiveAddressIndex` partition is exercised here (the three fields are address-keyed). Read-modify-write via
    * `acceptanceMpt.get[SortedSet[Address]]` — when the slice's added-set is empty the sidecar is not touched at all, matching the legacy
    * helper's `if (added.isEmpty && removed.isEmpty) ...` short-circuit.
    */
  private def applySliceViaWriter(acceptanceMpt: AcceptanceMpt[IO], slice: GsiSlice)(
    implicit hasher: Hasher[IO]
  ): IO[Unit] = {
    import GlobalStateFieldId._

    val stateChanHashes: Map[GlobalStateKey, Hash] = slice.stateChanHashes.iterator.map {
      case (addr, h) => GlobalStateKey.metagraph(addr, LastStateChannelSnapshotHashes) -> h
    }.toMap
    val txRefs: Map[GlobalStateKey, TransactionReference] = slice.lastTxRefs.iterator.map {
      case (addr, r) => GlobalStateKey.hypergraph(LastTxRefs, addr) -> r
    }.toMap
    val balanceEntries: Map[GlobalStateKey, Balance] = slice.balances.iterator.map {
      case (addr, b) => GlobalStateKey.hypergraph(Balances, addr) -> b
    }.toMap

    def applySidecar(fieldId: GlobalStateFieldId, added: Set[Address], removed: Set[Address]): IO[Unit] =
      if (added.isEmpty && removed.isEmpty) IO.unit
      else
        GlobalStateKey.activeAddressIndexKey[IO](fieldId).flatMap { key =>
          for {
            existing <- acceptanceMpt.get[SortedSet[Address]](key).map(_.getOrElse(SortedSet.empty[Address]))
            merged = (existing ++ added) -- removed
            _ <-
              if (merged.isEmpty && existing.nonEmpty) acceptanceMpt.remove(key)
              else if (merged.nonEmpty && merged != existing) acceptanceMpt.insert[SortedSet[Address]](key, merged)
              else IO.unit
          } yield ()
        }

    for {
      _ <- acceptanceMpt.insert[Hash](stateChanHashes)
      _ <- acceptanceMpt.insert[TransactionReference](txRefs)
      _ <- acceptanceMpt.insert[Balance](balanceEntries)
      // Sidecar maintenance — same call list as the corresponding `applyActiveAddressIndexDelta` invocations in
      // `syncFromStateChanges` for these three fields (LastTxRefs, Balances, LastStateChannelSnapshotHashes).
      _ <- applySidecar(LastTxRefs, slice.lastTxRefs.keySet.toSet, Set.empty)
      _ <- applySidecar(Balances, slice.balances.keySet.toSet, Set.empty)
      _ <- applySidecar(LastStateChannelSnapshotHashes, slice.stateChanHashes.keySet.toSet, Set.empty)
    } yield ()
  }

  /** PATH A — legacy direct write via `syncFromStateChanges`. */
  private def buildViaLegacy(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      _ <- store.syncFromStateChanges(slice.toAccumulator, ordinal)
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash), bytes)

  /** PATH F — writer-algebra over `MptOverlay.passthrough`. Writes go directly to the underlying store as the handle accumulates them, so
    * `store.allEntriesAsBytes` reflects everything immediately after commit; `overlay.buildRoot` delegates to `store.build(ordinal)`.
    */
  private def buildViaAlgebraPassthrough(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      pcTree <- ParentChildTree.make[IO]
      overlay = MptOverlay.passthrough[IO, GlobalStateKey](store, pcTree)
      handle <- overlay.checkout(parentBranch)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentBranch, handle)
      _ <- applySliceViaWriter(mpt, slice)
      _ <- overlay.commit(handle, childBranch, ordinal)
      rootRes <- overlay.buildRoot(childBranch, ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (rootRes.toOption.map(_.rootHash), bytes)

  /** PATH G — writer-algebra over `MptOverlay.MultiBranch`. Writes accumulate in the per-branch `ChangeSet` until `finalizeBranch` folds
    * them into the underlying store. Compared to path A post-finalize, the base bytes must be identical — divergence here points to a bug in
    * either `MultiBranchHandle`'s encoding or `foldIntoBase`'s producer-level apply order.
    *
    * Note: for an empty slice the merged ChangeSet is empty and `foldIntoBase` is a no-op — identical to path A on empty.
    */
  private def buildViaAlgebraMultiBranch(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.MultiBranch(MptOverlay.DefaultMaxPendingBranches),
        underlying = store,
        pcTree = pcTree,
        toHex = GlobalStateKey.toHex[IO],
        bestTipFn = IO.pure(none[BranchId])
      )
      handle <- overlay.checkout(parentBranch)
      mpt = AcceptanceMpt.fromOverlay[IO](overlay, parentBranch, handle)
      _ <- applySliceViaWriter(mpt, slice)
      _ <- overlay.commit(handle, childBranch, ordinal)
      _ <- overlay.finalizeBranch(childBranch, ordinal)
      // Post-finalize: ChangeSet folded into `store`; `store.build(ordinal)` is the direct comparison point.
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash), bytes)

  // ============================================================
  // Property tests — paths must agree on root + per-key bytes
  // ============================================================

  test("PATH F (algebra/passthrough) == PATH A (legacy syncFromStateChanges) on random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaLegacy(slice, ordinal)
        f <- buildViaAlgebraPassthrough(slice, ordinal)
      } yield
        expect.all(
          a._1 == f._1,
          bytesEntriesEq(a._2, f._2)
        )
    }
  }

  test("PATH G (algebra/multi-branch + finalize) == PATH A (legacy syncFromStateChanges) on random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaLegacy(slice, ordinal)
        g <- buildViaAlgebraMultiBranch(slice, ordinal)
      } yield
        expect.all(
          a._1 == g._1,
          bytesEntriesEq(a._2, g._2)
        )
    }
  }

  test("all three paths agree on root + per-key bytes for random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaLegacy(slice, ordinal)
        f <- buildViaAlgebraPassthrough(slice, ordinal)
        g <- buildViaAlgebraMultiBranch(slice, ordinal)
      } yield
        expect.all(
          a._1 == f._1,
          a._1 == g._1,
          bytesEntriesEq(a._2, f._2),
          bytesEntriesEq(a._2, g._2)
        )
    }
  }

  // ============================================================
  // Concrete sanity + empty edge case
  // ============================================================

  /** Non-trivial root + non-empty entries on all three paths. Pinned because property tests can pass vacuously when the generator yields
    * empty slices (all-`None` agreement is consistent but uninteresting); this guarantees at least one execution exercises the
    * insert+sidecar machinery on each path.
    */
  test("concrete sanity: 1-balance / 1-txRef / 1-stateChanHash slice produces a non-trivial root on all paths") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(100L))
    val addr = addressGen.sample.get
    val ref = transactionReferenceGen.sample.get
    val slice = GsiSlice(
      balances = SortedMap(addr -> balanceGen.sample.get),
      lastTxRefs = SortedMap(addr -> ref),
      stateChanHashes = SortedMap(addr -> testHash("metagraph"))
    )
    for {
      a <- buildViaLegacy(slice, ordinal)
      f <- buildViaAlgebraPassthrough(slice, ordinal)
      g <- buildViaAlgebraMultiBranch(slice, ordinal)
    } yield
      expect.all(
        a._1.isDefined,
        // 3 user entries (one per field) + 3 sidecar entries (one per non-empty field). Same expectation
        // as `MptCrossPathDeterminismSuite`'s sanity case — locks the entry-count contract.
        a._2.size == 6,
        a._1 == f._1,
        a._1 == g._1,
        bytesEntriesEq(a._2, f._2),
        bytesEntriesEq(a._2, g._2)
      )
  }

  test("empty slice: all paths return None root and empty bytes") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1L))
    val empty = GsiSlice(SortedMap.empty, SortedMap.empty, SortedMap.empty)
    for {
      a <- buildViaLegacy(empty, ordinal)
      f <- buildViaAlgebraPassthrough(empty, ordinal)
      g <- buildViaAlgebraMultiBranch(empty, ordinal)
    } yield
      expect.all(
        a._1.isEmpty,
        f._1.isEmpty,
        g._1.isEmpty,
        a._2.isEmpty,
        f._2.isEmpty,
        g._2.isEmpty
      )
  }
}
