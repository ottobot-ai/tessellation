package io.constellationnetwork.node.shared.domain.nakamoto

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt.{GlobalStateConverter, GlobalStateFieldId}
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** The cl1/dl1 6th consumed field — `lastCurrencySnapshots` — recompute-and-match verify path
  * (`docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`; the cl1/dl1 extension of the gl1 own-slice follow).
  *
  * Unlike the five uniform-`Hash` hypergraph fields, `lastCurrencySnapshots` is SHAPE-DIFFERENT: per `Address` the value is the metagraph's
  * latest currency snapshot (`Left` genesis full / `Right((incremental, info))`), and in gl0's MPT it SPLITS into two `metagraph`-namespaced
  * sub-keys (`LastIncrementalCurrencySnapshots` fieldId 5 + `LastCurrencySnapshotInfo` fieldId 6). In the signed `GlobalSnapshotStateProof`
  * it IS carried in the field-4 slot `lastCurrencySnapshotsProof` (the two partition roots). It is verified by a
  * recompute-vs-SIGNED match: `FollowVerifyCore.currencySnapshotsCheck` recomputes both subtree roots of the applied post-state via gl0's
  * EXACT `GlobalStateConverter.currencySnapshotFieldRoots` (which reuses gl0's exact `currencySnapshotEntryBytes` + `fieldRootFromBytes`) and
  * asserts they equal the SIGNED roots — a TRUE Byzantine anchor symmetric with the five uniform-`Hash` fields.
  *
  * Coverage:
  *   1. correct currency map matching the SIGNED roots → `Verified` carrying the currency map through `ConsumedFieldState.lastCurrencySnapshots`,
  *      AND the recomputed roots equal gl0's exact `currencySnapshotFieldRoots` (determinism cross-check, NOT hardcoded).
  *   1. an INCREMENTAL slice whose applied post-state diverges from the SIGNED roots (a dropped/changed entry) → `FieldRootMismatch` on
  *      fieldId 5 or 6 (the omission/tamper the structure must catch).
  *   1. the SIGNED-anchor property: a correct applied map but WRONG signed roots → `FieldRootMismatch` (proves it binds to the SIGNED value,
  *      not the served map).
  *   1. the empty-map case: empty applied map + `signedRoots = None` ⇒ both expected roots `Hash.empty` ⇒ verify SUCCEEDS.
  *   1. the 5-field gl1 path with `currencySnapshotsCheck = None` IGNORES `lastCurrencySnapshots` entirely (carried through, not checked) —
  *      proves the additive 6th field never touches gl1's behavior.
  */
object CurrencySnapshotsFollowFieldSuite extends MutableIOSuite {

  override type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private implicit val selector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal.unsafeApply(Long.MaxValue))

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  private def mgAddr(seed: Int): Address = Address.fromBytes(s"currency-follow-field-suite-mg-$seed".getBytes("UTF-8"))

  private def currencyInfo(balance: Long): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap(mgAddr(99) -> TransactionReference(TransactionOrdinal(NonNegLong(1L)), Hash(f"$balance%064d"))),
      balances = SortedMap(mgAddr(99) -> Balance(NonNegLong.unsafeFrom(balance))),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  /** A signed `CurrencyIncrementalSnapshot` at `ordinal` — the `Right` arm of `lastCurrencySnapshots`, the steady-state shape gl0's
    * finalized GSI holds. Field shape mirrors `MetagraphParentOrdinalResolverSuite`.
    */
  private def signedIncremental(
    snapOrdinal: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Signed[CurrencyIncrementalSnapshot]] = {
    val snapshot = CurrencyIncrementalSnapshot(
      SnapshotOrdinal.unsafeApply(snapOrdinal),
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
      None,
      None,
      None,
      None
    )
    KeyPairGenerator.makeKeyPair[IO].flatMap(kp => Signed.forAsyncHasher[IO, CurrencyIncrementalSnapshot](snapshot, kp))
  }

  private def currencyEntry(
    snapOrdinal: Long,
    balance: Long
  )(implicit sp: SecurityProvider[IO], h: Hasher[IO]): IO[Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
    signedIncremental(snapOrdinal).map(s => (s, currencyInfo(balance)).asRight)

  private def verifier(implicit h: Hasher[IO], js: JsonSerializer[IO]): GlobalFollowMirrorVerifier[IO] = GlobalFollowMirrorVerifier.make[IO]

  // The five hypergraph fields are empty in these tests, so an empty `signedFieldRoots` matches
  // (`fieldRootFromBytes(empty) == Hash.empty == getOrElse(_, Hash.empty)`).
  private val emptyHashRoots: SortedMap[GlobalStateFieldId, Hash] = SortedMap.empty

  /** The SIGNED currency-snapshots roots for a map — gl0's exact `currencySnapshotFieldRoots`, packaged as the field-4
    * `lastCurrencySnapshotsProof` value the producer would sign for that GSI currency map.
    */
  private def signedRootsFor(
    map: ConsumedFieldState.LastCurrencySnapshots
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[CurrencySnapshotMptRoots] =
    GlobalStateConverter.currencySnapshotFieldRoots[IO](map).map { case (inc, info) => CurrencySnapshotMptRoots(inc, info) }

  test("correct currency map matching the SIGNED roots: Verified carries lastCurrencySnapshots through AND roots equal gl0's exact roots") {
    res =>
      implicit val (h, sp, js) = res
      for {
        e0 <- currencyEntry(3L, 1000L)
        e1 <- currencyEntry(7L, 2000L)
        currencyMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> e0, mgAddr(1) -> e1)

        // Full-from-empty slice: prior empty, no removals, applied post-state == the map the SIGNED roots cover.
        delta = ConsumedFieldDelta.empty.copy(lastCurrencySnapshots = currencyMap)
        signed <- signedRootsFor(currencyMap)
        check = FollowVerifyCore.currencySnapshotsCheck[IO](signed.some)

        verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, check.some)

        // Determinism cross-check: gl0's exact per-field roots for the same map, NOT hardcoded.
        gl0Roots <- GlobalStateConverter.currencySnapshotFieldRoots[IO](currencyMap)
      } yield
        verifiedE match {
          case Right(verified) =>
            expect.all(
              verified.value.lastCurrencySnapshots == currencyMap,
              // both subtree roots are non-empty (the map is non-empty) and equal the signed value
              gl0Roots._1 == signed.incrementalRoot,
              gl0Roots._2 == signed.infoRoot,
              gl0Roots._1 =!= Hash.empty,
              gl0Roots._2 =!= Hash.empty
            )
          case Left(err) => failure(s"expected Right(Verified), got Left($err)")
        }
  }

  test("diverging applied post-state (a dropped entry vs the SIGNED roots) → FieldRootMismatch on a currency sub-field") { res =>
    implicit val (h, sp, js) = res
    for {
      e0 <- currencyEntry(3L, 1000L)
      e1 <- currencyEntry(7L, 2000L)
      signedMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> e0, mgAddr(1) -> e1)

      // The SIGNED roots cover both entries, but the delta only upserts ONE — so the applied post-state (one entry) does NOT
      // reproduce the signed two-entry roots. The recompute must catch it.
      delta = ConsumedFieldDelta.empty.copy(lastCurrencySnapshots = SortedMap(mgAddr(0) -> e0))
      signed <- signedRootsFor(signedMap)
      check = FollowVerifyCore.currencySnapshotsCheck[IO](signed.some)

      verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, check.some)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(field, _, _)) =>
          expect(
            field == GlobalStateFieldId.LastIncrementalCurrencySnapshots || field == GlobalStateFieldId.LastCurrencySnapshotInfo
          )
        case other => failure(s"expected Left(FieldRootMismatch(currency sub-field, ...)), got $other")
      }
  }

  test("a tampered currency value (same key, different snapshot vs the SIGNED roots) → FieldRootMismatch") { res =>
    implicit val (h, sp, js) = res
    for {
      eGood <- currencyEntry(3L, 1000L)
      eBad <- currencyEntry(3L, 9999L) // same address key, different info balance ⇒ different subtree root
      signedMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> eGood)

      delta = ConsumedFieldDelta.empty.copy(lastCurrencySnapshots = SortedMap(mgAddr(0) -> eBad))
      signed <- signedRootsFor(signedMap)
      check = FollowVerifyCore.currencySnapshotsCheck[IO](signed.some)

      verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, check.some)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(field, _, _)) =>
          expect(
            field == GlobalStateFieldId.LastIncrementalCurrencySnapshots || field == GlobalStateFieldId.LastCurrencySnapshotInfo
          )
        case other => failure(s"expected Left(FieldRootMismatch(currency sub-field, ...)), got $other")
      }
  }

  test("SIGNED-anchor property: a CORRECT applied map but WRONG signed roots → FieldRootMismatch (binds to signed value, not served map)") {
    res =>
      implicit val (h, sp, js) = res
      for {
        e0 <- currencyEntry(3L, 1000L)
        eOther <- currencyEntry(3L, 5555L)
        appliedMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> e0)
        // signed roots cover a DIFFERENT map than the one actually applied — the recompute of the applied map won't match.
        wrongSignedMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> eOther)

        delta = ConsumedFieldDelta.empty.copy(lastCurrencySnapshots = appliedMap)
        wrongSigned <- signedRootsFor(wrongSignedMap)
        check = FollowVerifyCore.currencySnapshotsCheck[IO](wrongSigned.some)

        verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, check.some)
      } yield
        verifiedE match {
          case Left(FollowVerificationError.FieldRootMismatch(field, _, _)) =>
            expect(
              field == GlobalStateFieldId.LastIncrementalCurrencySnapshots || field == GlobalStateFieldId.LastCurrencySnapshotInfo
            )
          case other => failure(s"expected Left(FieldRootMismatch) when applied map disagrees with signed roots, got $other")
        }
  }

  test("empty currency map + signedRoots = None ⇒ both expected roots Hash.empty ⇒ verify SUCCEEDS") { res =>
    implicit val (h, sp, js) = res
    val delta = ConsumedFieldDelta.empty // empty currency map
    val check = FollowVerifyCore.currencySnapshotsCheck[IO](None)
    verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, check.some).map {
      case Right(verified) => expect(verified.value.lastCurrencySnapshots.isEmpty)
      case Left(err)       => failure(s"expected Right(Verified) for empty map + None signed roots, got Left($err)")
    }
  }

  test("gl1 path (currencySnapshotsCheck = None): a non-empty lastCurrencySnapshots is carried through but NOT verified") { res =>
    implicit val (h, sp, js) = res
    for {
      e0 <- currencyEntry(3L, 1000L)
      currencyMap: ConsumedFieldState.LastCurrencySnapshots = SortedMap(mgAddr(0) -> e0)

      delta = ConsumedFieldDelta.empty.copy(lastCurrencySnapshots = currencyMap)
      // No check (gl1). The 5 hash-field roots are empty and match. The currency map must NOT be checked, so even though
      // there is no signed root for it, the verify SUCCEEDS and carries the map through.
      verifiedE <- verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, emptyHashRoots, None)
    } yield
      verifiedE match {
        case Right(verified) => expect(verified.value.lastCurrencySnapshots == currencyMap)
        case Left(err)       => failure(s"expected Right(Verified) on the gl1 (no-check) path, got Left($err)")
      }
  }
}
