package io.constellationnetwork.security.mpt

import cats.Show
import cats.effect.IO
import cats.effect.kernel.Resource
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.generators._
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.transaction.TransactionReference
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MptRoot
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.{Hasher, SecurityProvider}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import org.scalacheck.Gen
import weaver.MutableIOSuite
import weaver.scalacheck.Checkers

/** Track-0diag cross-path determinism property test.
  *
  * Builds the same logical state via multiple trie-mutation paths and asserts every path produces an identical mptRoot AND identical
  * per-key bytes. Divergence on any pair of paths is the signal Track 0 was created to surface. The validator currently tolerates mptRoot
  * mismatches for "non-determinism being fixed by undo journal" (`NakamotoSnapshotValidator.scala:177`); this suite is the gate that lets
  * us remove that tolerance.
  *
  * Paths covered (per the proposal at `~/.claude/plans/meticulous-shedding-patricia.md`, "Track 0"):
  *   - **A: delta-apply** — `MptStore.syncFromStateChanges(accumulator)` (the accept() path).
  *   - **B: full-sync from GSI** — `MptStore.syncFromGlobalSnapshotInfo` (catch-up, L1 init, peer download). Uses its own `clear → typed
  *     insert per field → build`, NOT `MptStore.syncFull`.
  *   - **C: per-field syncFull** — `MptStore.syncFull[V]` for one field type at a time (cross-checks single-field semantics; not a direct
  *     GSI replacement since it clears on every call).
  *   - **D: build vs commit** — `build(ordinal)` and `commit(ordinal)` after the same delta-apply must produce the same root.
  *   - **E: verify-replay** — `toAccumulatorHexDelta` produces (upserts, removes); applying that to a fresh store and building must produce
  *     the same root as path A.
  *
  * The acceptance criterion is byte-equal entries plus byte-equal root, not just root agreement — root accidents can mask per-key bytes
  * drift on uncommonly-serialised fields.
  */
object MptCrossPathDeterminismSuite extends MutableIOSuite with Checkers {

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

  /** Random-but-bounded GSI subset: balances, lastTxRefs, lastStateChannelSnapshotHashes. Three is enough to detect cross-path drift;
    * adding more field types is a strict extension and lives in follow-up coverage tests.
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

    def toGSI: GlobalSnapshotInfo =
      GlobalSnapshotInfo.empty.copy(
        balances = balances,
        lastTxRefs = lastTxRefs,
        lastStateChannelSnapshotHashes = stateChanHashes
      )
  }

  // Weaver's `forall` requires a `Show` for the generated type — when a property fails it prints the offending sample.
  // For our purposes a one-line summary is enough; the full slice is reconstructable from the seed.
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

  /** Run path A: incremental delta-apply via `syncFromStateChanges`. */
  private def buildViaDeltaApply(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      _ <- store.syncFromStateChanges(slice.toAccumulator, ordinal)
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash), bytes)

  /** Run path B: full-sync from a `GlobalSnapshotInfo`. */
  private def buildViaSyncFromGsi(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      _ <- store.syncFromGlobalSnapshotInfo(slice.toGSI, ordinal)
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash), bytes)

  /** Run path D: same as A but use `commit(ordinal)` instead of `build(ordinal)` for the final root step. The two must produce the same
    * trie root since `commit` is `persistAsync + build + bookkeeping`.
    */
  private def buildViaDeltaApplyThenCommit(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      store <- freshStore
      _ <- store.syncFromStateChanges(slice.toAccumulator, ordinal)
      _ <- store.commit(ordinal)
      // commit doesn't return the trie; build it again — it should be cheap and deterministic.
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash), bytes)

  /** Run path E: produce a hex-keyed delta via `toAccumulatorHexDelta`, apply it to a fresh producer's bytes via direct insert/remove,
    * build the trie. This is the verify-path's reconstruction; if it disagrees with path A the verifier will reject snapshots that the
    * writer accepted.
    */
  private def buildViaVerifyReplay(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[(Option[MptRoot], Map[Hex, Array[Byte]])] =
    for {
      // Empty pre-sync bytes: this is "build from genesis with delta only".
      replay <- GlobalStateConverter.toAccumulatorHexDelta[IO](slice.toAccumulator, Map.empty[Hex, Array[Byte]])
      (upserts, removes) = replay
      producer <- InMemoryMerklePatriciaProducer.make[IO]()
      _ <- producer.insertBytes(upserts).void
      _ <- producer.remove(removes.toList)
      trie <- producer.buildForOrdinal(ordinal)
      bytes <- producer.entries
    } yield (trie.toOption.map(_.rootHash), bytes)

  /** Compare two `Map[Hex, Array[Byte]]` for byte-equal entry agreement. Since `Array[Byte]` does not have a structural equality, we
    * normalise to `Map[Hex, Vector[Byte]]` before comparing.
    */
  private def bytesEntriesEq(a: Map[Hex, Array[Byte]], b: Map[Hex, Array[Byte]]): Boolean =
    a.view.mapValues(_.toVector).toMap == b.view.mapValues(_.toVector).toMap

  /** Filter to only "user" entries, dropping the `SystemNamespace` sidecar (ActiveAddressIndex, expiry indices, …). `GlobalStateKey.toHex`
    * encodes the namespace's `keyType` byte as the first 2 hex chars (see `PartitionKeyType`): `00` Hypergraph, `01` Metagraph/Address,
    * `02` Hash, `03` System. Path C (`syncFull[V]`) does not maintain sidecars, so direct byte comparison against path A
    * (`syncFromStateChanges`, which does) requires filtering the sidecar (`03…`) entries out of A first.
    */
  private def userEntriesOnly(bytes: Map[Hex, Array[Byte]]): Map[Hex, Array[Byte]] =
    bytes.filter { case (k, _) => !k.value.startsWith("03") }

  /** Lift a property over a `GsiSlice` into a Weaver assertion that the four cross-comparable paths (A/B/D/E) agree on root and entry
    * bytes. "Agree" includes the empty case — when the slice is empty, all paths return `None` (build errors with "no entries"), which is
    * consistent and therefore acceptable. Path C is exercised separately because it operates on a single field type at a time.
    */
  private def assertCrossPathAgreement(slice: GsiSlice, ordinal: SnapshotOrdinal)(
    implicit hasher: Hasher[IO],
    j: JsonSerializer[IO]
  ): IO[weaver.Expectations] =
    for {
      a <- buildViaDeltaApply(slice, ordinal)
      b <- buildViaSyncFromGsi(slice, ordinal)
      d <- buildViaDeltaApplyThenCommit(slice, ordinal)
      e <- buildViaVerifyReplay(slice, ordinal)
      (rootA, bytesA) = a
      (rootB, bytesB) = b
      (rootD, bytesD) = d
      (rootE, bytesE) = e
    } yield
      expect.all(
        rootA == rootB,
        rootA == rootD,
        rootA == rootE,
        bytesEntriesEq(bytesA, bytesB),
        bytesEntriesEq(bytesA, bytesD),
        bytesEntriesEq(bytesA, bytesE)
      )

  test("path A == path B (delta-apply == syncFromGlobalSnapshotInfo) on random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaDeltaApply(slice, ordinal)
        b <- buildViaSyncFromGsi(slice, ordinal)
      } yield
        expect.all(
          a._1 == b._1,
          bytesEntriesEq(a._2, b._2)
        )
    }
  }

  test("path A == path D (build == commit) on random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaDeltaApply(slice, ordinal)
        d <- buildViaDeltaApplyThenCommit(slice, ordinal)
      } yield
        expect.all(
          a._1 == d._1,
          bytesEntriesEq(a._2, d._2)
        )
    }
  }

  test("path A == path E (delta-apply == verify-replay) on random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      for {
        a <- buildViaDeltaApply(slice, ordinal)
        e <- buildViaVerifyReplay(slice, ordinal)
      } yield
        expect.all(
          a._1 == e._1,
          bytesEntriesEq(a._2, e._2)
        )
    }
  }

  test("all four cross-comparable paths agree on root and per-key bytes for random slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      assertCrossPathAgreement(slice, ordinal)
    }
  }

  /** Path C: per-field `syncFull[Balance]`. The store is cleared on every call, so we exercise `syncFull` only as a single-field-type
    * primitive — not as a GSI rebuild path. The check is: `syncFull[Balance](balances)` produces the same set of *user* entries as
    * `syncFromStateChanges` for a balances-only slice. `syncFromStateChanges` additionally maintains the `ActiveAddressIndex` sidecar
    * (added in `40d6f957`), which `syncFull` does not write — those sidecar entries are filtered before comparing. Roots therefore can
    * differ: path A's root covers user-entries-plus-sidecar; path C's covers user-entries-only. The test asserts user-byte equality, which
    * is the semantic invariant `syncFull` is meant to honour.
    */
  test("path C (syncFull[Balance]) matches delta-apply user entries for the balances-only slice") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1000L))
    forall(gsiSliceGen) { slice =>
      val balsOnly = slice.copy(
        lastTxRefs = SortedMap.empty[Address, TransactionReference],
        stateChanHashes = SortedMap.empty[Address, Hash]
      )
      for {
        a <- buildViaDeltaApply(balsOnly, ordinal)

        keyedBalances = balsOnly.balances.iterator.map {
          case (addr, b) =>
            GlobalStateKey.hypergraph(io.constellationnetwork.schema.mpt.GlobalStateFieldId.Balances, addr) -> b
        }.toMap
        cStore <- freshStore
        _ <- cStore.syncFull[Balance](keyedBalances, ordinal)
        _ <- cStore.build(ordinal)
        cBytes <- cStore.allEntriesAsBytes
      } yield expect(bytesEntriesEq(userEntriesOnly(a._2), cBytes))
    }
  }

  /** Sanity: empty slice produces consistent results across all paths (all paths return None — `build` errors on empty with
    * `OperationError("Cannot build trie with no entries")`, which is consistent across paths).
    */
  test("all paths agree on the empty slice (all return None)") { res =>
    implicit val (j, h, _) = res
    val ordinal = SnapshotOrdinal(NonNegLong(1L))
    val empty = GsiSlice(SortedMap.empty, SortedMap.empty, SortedMap.empty)
    for {
      a <- buildViaDeltaApply(empty, ordinal)
      b <- buildViaSyncFromGsi(empty, ordinal)
      d <- buildViaDeltaApplyThenCommit(empty, ordinal)
      e <- buildViaVerifyReplay(empty, ordinal)
    } yield
      expect.all(
        a._1.isEmpty,
        b._1.isEmpty,
        d._1.isEmpty,
        e._1.isEmpty,
        a._2.isEmpty,
        b._2.isEmpty,
        d._2.isEmpty,
        e._2.isEmpty
      )
  }

  /** Single concrete sanity case: 1 balance, 1 lastTxRef, 1 stateChanHash. Asserts the root is non-empty (excluded from the property-based
    * tests because empty slices fall through to all-`None` agreement). Pinning a concrete case here means the property tests can't pass
    * vacuously — at least one execution exercises a non-trivial trie.
    *
    * Entry count: 3 user entries (one per field) plus 3 ActiveAddressIndex sidecar entries (one per non-empty field), for 6 total.
    * `applyActiveAddressIndexDelta` short-circuits when both `added` and `removed` are empty, so empty fields don't contribute a sidecar.
    */
  test("concrete sanity: single non-empty slice produces a non-trivial root") { res =>
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
      a <- buildViaDeltaApply(slice, ordinal)
      b <- buildViaSyncFromGsi(slice, ordinal)
      d <- buildViaDeltaApplyThenCommit(slice, ordinal)
      e <- buildViaVerifyReplay(slice, ordinal)
    } yield
      expect.all(
        a._1.isDefined,
        userEntriesOnly(a._2).size == 3,
        a._2.size == 6,
        a._1 == b._1,
        a._1 == d._1,
        a._1 == e._1,
        bytesEntriesEq(a._2, b._2),
        bytesEntriesEq(a._2, d._2),
        bytesEntriesEq(a._2, e._2)
      )
  }
}
