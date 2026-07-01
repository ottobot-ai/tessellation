package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency.{CurrencyIncrementalSnapshot, CurrencySnapshotStateProof}
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.config.types.LastGlobalSnapshotsSyncConfig
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{GlobalSnapshotsProcessed, SharedArtifact}
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.snapshot.MetagraphSyncDataInfo
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.{NonNegLong, PosInt}
import fs2.concurrent.SignallingRef
import weaver.MutableIOSuite

/** Blocker-1a gate — the in-band set-valued processed-set `P`.
  *
  * These tests pin the applied-set semantics `A = { o ∈ U : o ≤ view ∧ o ∉ P }` at the exact site where the former node-local
  * `globalSnapshotsAlreadyProcessed` cache was read (`GlobalSnapshotOpsManager.getLastGlobalSnapshotsSpendActions`), now that `P` is a pure
  * input (reconstructed in-band from the retained CL0 chain by the caller). Observable = the returned "processed" set (the second element of
  * the result), which is exactly the `GlobalSnapshotsProcessed(A)` the acceptance manager emits into the snapshot.
  *
  * Cases: (a) gaps in `U`; (b) same-ordinal retry determinism; (c) below-view injection (the ordinal-below-an-advanced-view case a scalar
  * interval `(prior_view, view]` drops permanently — the set-valued `P` applies it); and (d) node-local-ness (Step-5b): two sequential calls
  * on ONE manager vs a fresh manager yield the same applied-set (impossible with the removed mutable cache). A final pure test pins the
  * reconstruction `P = ⋃ GlobalSnapshotsProcessed.ordinals`.
  */
object CurrencySnapshotProcessedSetSuite extends MutableIOSuite {

  // Always-legacy selector so the benign GlobalSnapshotInfo.stateProof needs no MptStore.
  implicit val globalStateProofSelector: GlobalStateProofSelector =
    GlobalStateProofSelector(SnapshotOrdinal(NonNegLong.unsafeFrom(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

  override type Res = (Hasher[IO], JsonSerializer[IO], SecurityProvider[IO])

  override def sharedResource: Resource[IO, Res] = for {
    sp <- SecurityProvider.forAsync[IO]
    implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
    h = Hasher.forJson[IO]
  } yield (h, j, sp)

  private def ord(v: Long): SnapshotOrdinal = SnapshotOrdinal(NonNegLong.unsafeFrom(v))
  private def oset(vs: Long*): SortedSet[SnapshotOrdinal] = SortedSet.from(vs.iterator.map(ord))

  private val syncConfig = LastGlobalSnapshotsSyncConfig(NonNegLong(2L), PosInt(10))

  private def mkGsom: IO[GlobalSnapshotOpsManager[IO]] =
    SignallingRef
      .of[IO, Map[SnapshotOrdinal, Hashed[GlobalIncrementalSnapshot]]](Map.empty)
      .map(cache => GlobalSnapshotOpsManager.make[IO](syncConfig, cache))

  private def mkGlobalInfoEmpty: GlobalSnapshotInfo =
    GlobalSnapshotInfo(
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      SortedMap.empty,
      None,
      None,
      None,
      None,
      None,
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      Some(SortedMap.empty),
      None,
      SortedMap.empty
    )

  /** A benign `Hashed[GlobalIncrementalSnapshot]` at `ordinal` with NO spendActions — enough for `processUnappliedOrdinals` to resolve every
    * applied ordinal from the `lastGlobalSnapshots` cache (so no network fetch / retry), while the applied-set under test is driven purely by
    * `U`, `view`, and `P`.
    */
  private def mkGlobalSnapshotAt(ordinal: SnapshotOrdinal)(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[Hashed[GlobalIncrementalSnapshot]] =
    mkGlobalInfoEmpty.stateProof[IO](ordinal).flatMap { sp =>
      Signed(
        GlobalIncrementalSnapshot(
          ordinal,
          Height.MinValue,
          SubHeight.MinValue,
          Hash.empty,
          SortedSet.empty,
          SortedMap.empty,
          SortedMap.empty[io.constellationnetwork.schema.sharding.ShardId, io.constellationnetwork.schema.sharding.ShardCheckpoint],
          SortedSet.empty,
          None,
          EpochProgress.MinValue,
          NonEmptyList.of(PeerId(Hex(""))),
          SnapshotTips(SortedSet.empty, SortedSet.empty),
          stateProof = sp,
          Some(SortedSet.empty),
          Some(SortedSet.empty),
          None,
          Some(SortedMap.empty),
          Some(SortedSet.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty),
          Some(SortedMap.empty)
        ),
        NonEmptySet.fromSetUnsafe(SortedSet(SignatureProof(ID.Id(Hex("")), Signature(Hex("")))))
      ).toHashed[IO]
    }

  /** Invoke the exact 1a call: `A = { o ∈ U : o ≤ view ∧ o ∉ P }`; returns the applied/processed ordinal set. All `U` ordinals are seeded
    * into `lastGlobalSnapshots` so any applied ordinal resolves from cache (no fetch).
    */
  private def appliedSet(
    gsom: GlobalSnapshotOpsManager[IO],
    metagraphId: Address,
    view: SnapshotOrdinal,
    u: SortedSet[SnapshotOrdinal],
    p: SortedSet[SnapshotOrdinal]
  )(implicit h: Hasher[IO], j: JsonSerializer[IO]): IO[SortedSet[SnapshotOrdinal]] =
    u.toList.traverse(mkGlobalSnapshotAt).flatMap { seeded =>
      gsom
        .getLastGlobalSnapshotsSpendActions(
          globalSnapshotViewOrdinal = view,
          lastGlobalSnapshots = seeded,
          getGlobalSnapshotByOrdinal = _ => IO.pure(none[Hashed[GlobalIncrementalSnapshot]]),
          currencyId = metagraphId,
          metagraphSyncData = Some(SortedMap(metagraphId -> MetagraphSyncDataInfo(ord(0L), EpochProgress.MinValue, u))),
          alreadyProcessedGlobalOrdinals = p,
          lastUnsyncGlobalSnapshotOrdinal = view,
          updatedLastSyncGlobalFromPeersInConsensus = view
        )
        .map { case (_, processed) => processed }
    }

  test("(a) gaps in U: applied-set = U ∩ (≤ view) \\ P, preserving non-contiguous gaps") { res =>
    implicit val (h, j, sp) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      mgId = PublicKeyOps(kp.getPublic).toAddress
      gsom <- mkGsom
      // U is non-contiguous; 102 already processed (in P); 108 is ABOVE the view and must be excluded.
      a <- appliedSet(gsom, mgId, view = ord(105L), u = oset(100L, 102L, 105L, 108L), p = oset(102L))
    } yield expect(a == oset(100L, 105L))
  }

  test("(b) same-ordinal retry: two invocations with identical (U, view, P) return the identical applied-set (no per-ordinal cache state)") {
    res =>
      implicit val (h, j, sp) = res
      for {
        kp <- KeyPairGenerator.makeKeyPair[IO]
        mgId = PublicKeyOps(kp.getPublic).toAddress
        gsom <- mkGsom
        first <- appliedSet(gsom, mgId, view = ord(110L), u = oset(101L, 103L), p = oset(103L))
        second <- appliedSet(gsom, mgId, view = ord(110L), u = oset(101L, 103L), p = oset(103L))
      } yield expect(first == oset(101L)) && expect(first == second)
  }

  test(
    "(c) below-view injection: an ordinal entering U BELOW an already-advanced view (with a HIGHER ordinal already in P) IS applied — the " +
      "exact case a scalar interval (prior_view, view] drops permanently"
  ) { res =>
    implicit val (h, j, sp) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      mgId = PublicKeyOps(kp.getPublic).toAddress
      gsom <- mkGsom
      // view advanced to 110; 105 already processed (P). 101 is injected below the view. A scalar interval (105, 110] would MISS 101.
      a <- appliedSet(gsom, mgId, view = ord(110L), u = oset(101L, 105L), p = oset(105L))
    } yield expect(a == oset(101L))
  }

  test(
    "(d) node-local-ness (Step-5b): two sequential calls on ONE manager == a call on a FRESH manager, given the same recorded view/U/P"
  ) { res =>
    implicit val (h, j, sp) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      mgId = PublicKeyOps(kp.getPublic).toAddress
      shared <- mkGsom
      seq1 <- appliedSet(shared, mgId, view = ord(200L), u = oset(150L, 175L), p = oset(150L))
      seq2 <- appliedSet(shared, mgId, view = ord(200L), u = oset(150L, 175L), p = oset(150L))
      fresh <- mkGsom
      freshA <- appliedSet(fresh, mgId, view = ord(200L), u = oset(150L, 175L), p = oset(150L))
    } yield expect(seq1 == oset(175L)) && expect(seq1 == seq2) && expect(seq2 == freshA)
  }

  private def mkCurrencySnapshotWithProcessed(
    ordinal: Long,
    processed: SortedSet[SnapshotOrdinal]
  ): CurrencyIncrementalSnapshot =
    CurrencyIncrementalSnapshot(
      ord(ordinal),
      Height.MinValue,
      SubHeight.MinValue,
      Hash.empty,
      SortedSet.empty,
      SortedSet.empty,
      SnapshotTips(SortedSet.empty, SortedSet.empty),
      CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      EpochProgress.MinValue,
      None,
      None,
      None,
      None,
      Some(SortedSet[SharedArtifact](GlobalSnapshotsProcessed(processed))),
      None,
      None,
      None
    )

  pureTest("reconstructProcessedGlobalOrdinals: unions GlobalSnapshotsProcessed.ordinals across the retained CL0 window (with dedup)") {
    val snapshots = List(
      mkCurrencySnapshotWithProcessed(5L, oset(100L, 101L)),
      mkCurrencySnapshotWithProcessed(4L, oset(101L, 102L)), // 101 overlaps -> dedup
      mkCurrencySnapshotWithProcessed(3L, oset()), // empty processed set (a snapshot that applied nothing)
      mkCurrencySnapshotWithProcessed(2L, oset(90L))
    )
    val p = GlobalSnapshotOpsManager.reconstructProcessedGlobalOrdinals(snapshots)
    expect(p == oset(90L, 100L, 101L, 102L))
  }

  pureTest("reconstructProcessedGlobalOrdinals: empty window -> empty P") {
    expect(GlobalSnapshotOpsManager.reconstructProcessedGlobalOrdinals(Nil) == SortedSet.empty[SnapshotOrdinal])
  }
}
