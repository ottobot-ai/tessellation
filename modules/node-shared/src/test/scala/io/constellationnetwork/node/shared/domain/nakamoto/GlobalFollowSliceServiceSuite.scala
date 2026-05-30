package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.schema.{GlobalSnapshotInfo, SnapshotOrdinal}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** The gl0-side SLICE PRODUCER ([[GlobalFollowSliceService]]) — `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`.
  *
  * The producer is sourced from gl0's latest-finalized `GlobalSnapshotInfo` (Address-keyed): it projects the five consumed fields
  * (`gsi.balances`, `gsi.lastTxRefs`, `gsi.lastAllowSpendRefs`, `gsi.lastTokenLockRefs`) into a [[ConsumedFieldDelta]] of all-upserts that
  * a follower applies + recompute-matches via [[GlobalFollowMirrorVerifier.verifyByFieldRoot]].
  *
  * Coverage:
  *   1. projection — `sliceFromGsi` carries exactly the five GSI consumed fields, Address-keyed, no removals; ignores the other ~12 GSI
  *      fields.
  *   1. cold start — `latestSlice` over a `None` finalized source → `None`.
  *   1. END-TO-END round-trip (THE IMPORTANT ONE) — produce the slice from a GSI, hand it to `verifyByFieldRoot` with `signedFieldRoots`
  *      derived in-test from a REFERENCE MPT built the way gl0 builds it (`MptStore.insert[V]` → `toHex(hypergraph(field, addr))` keys +
  *      scodec `immutableBytes` values → `GlobalStateConverter.fieldRootFromBytes`, NEVER hardcoded), assert `Verified` whose
  *      `ConsumedFieldState` (Address-keyed) == the GSI's five maps. THIS PROVES the Address-keyed forward-hash reproduces gl0's signed
  *      roots byte-identically.
  */
object GlobalFollowSliceServiceSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))

  private def addr(seed: Int): Address =
    Address.fromBytes(s"global-follow-slice-suite-seed-$seed".getBytes("UTF-8"))

  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))
  private def txRef(n: Long): TransactionReference = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def allowSpendRef(n: Long): AllowSpendReference =
    AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def tokenLockRef(n: Long): TokenLockReference = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))

  /** A dummy `Signed[TokenLock]` — the signature isn't verified on this path (the recompute hashes encoded bytes); a fixed placeholder
    * proof keeps the encoding deterministic so the reference-MPT root and the verifier's recompute are byte-identical.
    */
  private def signedTokenLock(seed: Int, amount: Long): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = addr(seed),
        amount = TokenLockAmount(PosLong.unsafeFrom(amount)),
        fee = TokenLockFee(NonNegLong(1L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(1L)), Hash(f"$seed%064d")),
        currencyId = none,
        unlockEpoch = EpochProgress(NonNegLong(1000L)).some,
        replaceTokenLockRef = none
      ),
      NonEmptySet.one(SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
    )

  // The five consumed fields, Address-keyed — the slice the producer must serve.
  private val balances: SortedMap[Address, Balance] = SortedMap(addr(1) -> bal(1000), addr(2) -> bal(2000), addr(4) -> bal(4000))
  private val lastTxRefs: SortedMap[Address, TransactionReference] = SortedMap(addr(1) -> txRef(1), addr(2) -> txRef(5))
  private val lastAllowSpendRefs: SortedMap[Address, AllowSpendReference] =
    SortedMap(addr(1) -> allowSpendRef(10), addr(2) -> allowSpendRef(20))
  private val lastTokenLockRefs: SortedMap[Address, TokenLockReference] = SortedMap(addr(1) -> tokenLockRef(101))
  private val activeTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = SortedMap(addr(1) -> SortedSet(signedTokenLock(1, 600)))

  /** A finalized GSI carrying the five consumed fields (incl. `activeTokenLocks`) plus a NON-consumed field
    * (`lastStateChannelSnapshotHashes`) so the projection's "ignore other fields" property is still exercised. Built off
    * `GlobalSnapshotInfo.empty`.
    */
  private val gsi: GlobalSnapshotInfo =
    GlobalSnapshotInfo.empty.copy(
      lastStateChannelSnapshotHashes = SortedMap(addr(9) -> Hash("ff" * 32)), // a non-consumed field — must be ignored
      lastTxRefs = lastTxRefs,
      balances = balances,
      lastAllowSpendRefs = lastAllowSpendRefs.some,
      lastTokenLockRefs = lastTokenLockRefs.some,
      activeTokenLocks = activeTokenLocks.some
    )

  private def service(latest: Option[(SnapshotOrdinal, GlobalSnapshotInfo)]): GlobalFollowSliceService[IO] =
    GlobalFollowSliceService.make[IO](IO.pure(latest), IO.pure(SortedMap.empty))

  // #287: a service over an explicit recent-projection ring (the bootstrap thunk is `None`, exercising only `sliceSince`).
  private def sliceSinceService(ring: SortedMap[SnapshotOrdinal, ConsumedFieldDelta]): GlobalFollowSliceService[IO] =
    GlobalFollowSliceService.make[IO](IO.pure(none), IO.pure(ring))

  /** gl0's exact signed per-field roots for the five consumed fields — built the PRODUCTION way: insert each field's entries into a real
    * MPT store via `MptStore.insert[V]` (which keys by `toHex(hypergraph(field, addr))` and encodes values via scodec `immutableBytes`),
    * read the bytes back, group by the consumed field's prefix, and feed each group to `GlobalStateConverter.fieldRootFromBytes` — the SAME
    * callable gl0's stateProof builder routes through. NOT hardcoded; this is the determinism cross-check.
    */
  private def signedRootsFromReferenceMpt(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[SortedMap[GlobalStateFieldId, Hash]] =
    for {
      mptProducer <- io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- store.insert[Balance](balances.toList.map {
        case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, a) -> v
      }.toMap)
      _ <- store.insert[TransactionReference](
        lastTxRefs.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, a) -> v }.toMap
      )
      _ <- store.insert[AllowSpendReference](
        lastAllowSpendRefs.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, a) -> v }.toMap
      )
      _ <- store.insert[TokenLockReference](
        lastTokenLockRefs.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, a) -> v }.toMap
      )
      _ <- store.insert[SortedSet[Signed[TokenLock]]](
        activeTokenLocks.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, a) -> v }.toMap
      )
      _ <- store.commit(ordinal)
      entries <- store.allEntriesAsBytes
      roots <- FollowVerifyCore.consumedFields.traverse { field =>
        GlobalStateKey.hypergraphFieldPrefix[IO](field).flatMap { prefix =>
          GlobalStateConverter
            .fieldRootFromBytes[IO](entries.filter { case (hex, _) => hex.value.startsWith(prefix.value) })
            .map(field -> _)
        }
      }
    } yield SortedMap.from(roots)

  test("sliceFromGsi: carries exactly the five consumed fields Address-keyed, no removals; ignores other GSI fields") { _ =>
    val slice = GlobalFollowSliceService.sliceFromGsi(gsi)
    IO.pure(
      expect.all(
        slice.balances == balances,
        slice.lastTxRefs == lastTxRefs,
        slice.lastAllowSpendRefs == lastAllowSpendRefs,
        slice.lastTokenLockRefs == lastTokenLockRefs,
        slice.activeTokenLocks == activeTokenLocks,
        slice.removals.isEmpty
      )
    )
  }

  test("latestSlice: None finalized source → None (cold start)") { _ =>
    service(none).latestSlice.map(o => expect(o.isEmpty))
  }

  test("latestSlice: serves the latest-finalized (ordinal, slice) projected from the GSI") { _ =>
    service((ordinal -> gsi).some).latestSlice.map {
      case Some((o, slice)) =>
        expect.all(o == ordinal, slice == GlobalFollowSliceService.sliceFromGsi(gsi))
      case None => failure("expected Some((ordinal, slice))")
    }
  }

  // ── #287 sliceSince — the incremental transport ──────────────────────────────────────────────
  // Two projection-ring fixtures: a SUBSET projection at the prior ordinal and the FULL projection at the latest, so
  // `diff(subset → full)` is a real upsert (addr(4)'s balance) — the exact change-set the producer must serve.
  private val priorOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val subsetProjection: ConsumedFieldDelta =
    GlobalFollowSliceService.sliceFromGsi(GlobalSnapshotInfo.empty.copy(balances = SortedMap(addr(1) -> bal(1000), addr(2) -> bal(2000))))
  private val fullProjection: ConsumedFieldDelta = GlobalFollowSliceService.sliceFromGsi(gsi)

  test("sliceSince: empty ring (cold start) → None") {
    sliceSinceService(SortedMap.empty).sliceSince(priorOrdinal).map(o => expect(o.isEmpty))
  }

  test("sliceSince: since == latest → NO-CHANGE (empty delta), baseOrdinal=Some(since), ordinal=latest") {
    val ring = SortedMap(ordinal -> fullProjection)
    sliceSinceService(ring).sliceSince(ordinal).map {
      case Some(r) =>
        expect.all(
          r.ordinal == ordinal,
          r.baseOrdinal == ordinal.some,
          r.slice == ConsumedFieldDelta.empty
        )
      case None => failure("expected Some(no-change response)")
    }
  }

  test("sliceSince: since in ring → diff(proj@since, proj@latest), baseOrdinal=Some(since), ordinal=latest") {
    val ring = SortedMap(priorOrdinal -> subsetProjection, ordinal -> fullProjection)
    sliceSinceService(ring).sliceSince(priorOrdinal).map {
      case Some(r) =>
        expect.all(
          r.ordinal == ordinal,
          r.baseOrdinal == priorOrdinal.some,
          r.slice == ConsumedFieldDelta.diff(subsetProjection, fullProjection)
        )
      case None => failure("expected Some(diff response)")
    }
  }

  test("sliceSince: since NOT in ring (evicted) → full proj@latest with baseOrdinal=None (full fallback)") {
    val ring = SortedMap(ordinal -> fullProjection) // priorOrdinal evicted
    sliceSinceService(ring).sliceSince(priorOrdinal).map {
      case Some(r) =>
        expect.all(
          r.ordinal == ordinal,
          r.baseOrdinal.isEmpty,
          r.slice == fullProjection
        )
      case None => failure("expected Some(full-fallback response)")
    }
  }

  // THE IMPORTANT ONE — produce (from GSI) → verify (forward-hash) → Verified == GSI's slice, with gl0-style signed roots.
  test("end-to-end round-trip: latestSlice(GSI) → verifyByFieldRoot(empty mirror) → Verified == GSI's five Address-keyed maps") { res =>
    implicit val (h, _, js) = res
    val verifier = GlobalFollowMirrorVerifier.make[IO]
    for {
      // Producer side: the Address-keyed slice projected from the finalized GSI.
      latest <- service((ordinal -> gsi).some).latestSlice
      (servedOrdinal, slice) <- IO.fromOption(latest)(new RuntimeException("expected a served slice"))

      // gl0's exact signed per-field roots (NOT hardcoded — from a reference MPT built the gl0 way).
      signedRoots <- signedRootsFromReferenceMpt

      // Verifier side: a FRESH (empty) follower mirror. Applying the slice to empty + forward-hashing must reconstruct gl0's roots.
      verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, servedOrdinal, slice, signedRoots)
    } yield
      verifiedE match {
        case Right(verified: Verified[ConsumedFieldState]) =>
          val st = verified.value
          expect.all(
            servedOrdinal == ordinal,
            st.balances == balances,
            st.lastTxRefs == lastTxRefs,
            st.lastAllowSpendRefs == lastAllowSpendRefs,
            st.lastTokenLockRefs == lastTokenLockRefs,
            st.activeTokenLocks == activeTokenLocks
          )
        case Left(err) => failure(s"expected Right(Verified) from the round-trip, got Left($err)")
      }
  }

  // #287 "send diffs" — the core safety property on a NON-EMPTY prior: apply diff(prev, curr) on the state a follower
  // holds at `prev` (verified there) and recompute-match against `curr`'s gl0 roots ⇒ the verifier reconstructs EXACTLY
  // `curr`'s five maps. This is what `DAGSnapshotProcessor` does on the incremental path (held prior + matching base).
  test("#287 round-trip on a non-empty prior: verifyFieldRoots(prevState, diff(prev,curr), currRoots) == curr") { res =>
    implicit val (h, _, js) = res
    val verifier = GlobalFollowMirrorVerifier.make[IO]
    // prev = the reference state MINUS addr(4)'s balance; curr = the full reference state. diff = upsert addr(4).
    val prevGsi = gsi.copy(balances = SortedMap(addr(1) -> bal(1000), addr(2) -> bal(2000)))
    val prevProjection = GlobalFollowSliceService.sliceFromGsi(prevGsi)
    val currProjection = GlobalFollowSliceService.sliceFromGsi(gsi)
    val theDiff = ConsumedFieldDelta.diff(prevProjection, currProjection)
    for {
      // gl0's signed roots at `prev` and at `curr`, both from reference MPTs built the gl0 way.
      prevRoots <- rootsForGsi(prevGsi)
      currRoots <- signedRootsFromReferenceMpt
      // 1) verify the follower reaches `prev`'s state from empty (the prior it holds).
      prevVerifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, priorOrdinal, prevProjection, prevRoots)
      prevState <- IO.fromEither(prevVerifiedE.leftMap(e => new RuntimeException(s"prev verify failed: $e")))
      // 2) apply the DIFF on the held prior and recompute-match `curr`'s roots.
      currVerifiedE <- verifier.verifyByFieldRoot(prevState.value, ordinal, theDiff, currRoots)
    } yield
      currVerifiedE match {
        case Right(v) =>
          val st = v.value
          expect.all(
            // the diff is genuinely incremental (only addr(4)'s balance upserted, nothing removed)
            theDiff.balances == SortedMap(addr(4) -> bal(4000)),
            theDiff.removals.isEmpty,
            // applying it on the held prior reconstructs the FULL reference state
            st.balances == balances,
            st.lastTxRefs == lastTxRefs,
            st.lastAllowSpendRefs == lastAllowSpendRefs,
            st.lastTokenLockRefs == lastTokenLockRefs,
            st.activeTokenLocks == activeTokenLocks
          )
        case Left(err) => failure(s"expected Right(Verified) from the diff-on-prior round-trip, got Left($err)")
      }
  }

  // gl0's signed roots for an arbitrary consumed-field GSI, built the gl0 way (same as signedRootsFromReferenceMpt
  // but parameterised) — lets the #287 prior round-trip anchor a verify at the `prev` state.
  private def rootsForGsi(g: GlobalSnapshotInfo)(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[SortedMap[GlobalStateFieldId, Hash]] =
    for {
      mptProducer <- io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- store.insert[Balance](g.balances.toList.map {
        case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, a) -> v
      }.toMap)
      _ <- store.insert[TransactionReference](
        g.lastTxRefs.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, a) -> v }.toMap
      )
      _ <- store.insert[AllowSpendReference](
        g.lastAllowSpendRefs
          .getOrElse(SortedMap.empty[Address, AllowSpendReference])
          .toList
          .map {
            case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, a) -> v
          }
          .toMap
      )
      _ <- store.insert[TokenLockReference](
        g.lastTokenLockRefs
          .getOrElse(SortedMap.empty[Address, TokenLockReference])
          .toList
          .map {
            case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, a) -> v
          }
          .toMap
      )
      _ <- store.insert[SortedSet[Signed[TokenLock]]](
        g.getActiveTokenLocks.toList.map { case (a, v) => GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, a) -> v }.toMap
      )
      _ <- store.commit(ordinal)
      entries <- store.allEntriesAsBytes
      roots <- FollowVerifyCore.consumedFields.traverse { field =>
        GlobalStateKey.hypergraphFieldPrefix[IO](field).flatMap { prefix =>
          GlobalStateConverter
            .fieldRootFromBytes[IO](entries.filter { case (hex, _) => hex.value.startsWith(prefix.value) })
            .map(field -> _)
        }
      }
    } yield SortedMap.from(roots)

}
