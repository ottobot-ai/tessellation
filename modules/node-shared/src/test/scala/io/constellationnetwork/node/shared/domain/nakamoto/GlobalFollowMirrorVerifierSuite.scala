package io.constellationnetwork.node.shared.domain.nakamoto

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** The FIELD-ROOT-MATCH verify path for an own-slice mirror follower (gl1) — `docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`.
  *
  * This is the correct-by-design proof for a holder of the full content of the five consumed fields. The mechanism: apply a producer's
  * claimed Address-keyed slice on top of the follower's prior state, forward-hash each `(Address, value)` to its MPT leaf the way gl0's
  * writer does (`toHex(hypergraph(field, addr))` + `immutableBytes(value)`), recompute each field's subtree root via gl0's EXACT
  * `GlobalStateConverter.fieldRootFromBytes`, and assert it equals the signed `stateProof.<field>Proof`. A wrong / missing / extra entry
  * yields a different subtree root ⇒ [[FollowVerificationError.FieldRootMismatch]].
  *
  * '''Determinism cross-check (byte-identity by construction).''' The `signedFieldRoots` the verifier matches against are NEVER hardcoded —
  * they are derived in-test from a REFERENCE post-state MPT built the way gl0 builds it (via `MptStore.insert[V]` →
  * `toHex(hypergraph(field, addr))` keys + scodec `immutableBytes` values), read back as `(leaf-path Hex → bytes)` and fed to the SAME
  * `GlobalStateConverter.fieldRootFromBytes` callable gl0's `stateProofBuilder` / `mptStateProofFromBytes` route through. So a passing
  * match proves the Address-keyed forward-hash reproduces gl0's signed root byte-identically. See `expectedRootsFromReference`.
  *
  * Coverage:
  *   1. correct delta → all five field roots match → `Verified(ConsumedFieldState)` equal to the reference post-state (Address-keyed).
  *   1. wrong value (tamper one upsert value) → `FieldRootMismatch` for that field.
  *   1. OMITTED entry (drop a genuinely-changed key from the delta) → `FieldRootMismatch`. The whole point: the omission the per-key range
  *      verifier could NOT catch IS caught here by root mismatch.
  *   1. extra spurious entry (a key gl0 did not change) → `FieldRootMismatch`.
  *   1. removal handled — delete a key gl0 removed → field root matches gl0's post-removal root → `Verified`.
  */
object GlobalFollowMirrorVerifierSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  private def addr(seed: Int): Address =
    Address.fromBytes(s"global-follow-mirror-suite-seed-$seed".getBytes("UTF-8"))

  private def balanceKey(seed: Int): GlobalStateKey = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))
  private def txRefKey(seed: Int): GlobalStateKey = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr(seed))
  private def allowSpendRefKey(seed: Int): GlobalStateKey = GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, addr(seed))
  private def tokenLockRefKey(seed: Int): GlobalStateKey = GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, addr(seed))
  private def activeTokenLockKey(seed: Int): GlobalStateKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr(seed))

  private def bal(n: Long): Balance = Balance(NonNegLong.unsafeFrom(n))
  private def txRef(n: Long): TransactionReference = TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def allowSpendRef(n: Long): AllowSpendReference =
    AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def tokenLockRef(n: Long): TokenLockReference = TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))

  /** A dummy `Signed[TokenLock]` keyed by `(holder seed, amount)` — `amount` lets a test bump the value to force a different field root.
    * The signature is never verified on this path (the recompute hashes the encoded bytes); a fixed placeholder proof keeps the encoding
    * deterministic so the reference root and the verifier's recompute are byte-identical.
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

  private def tokenLockSet(seed: Int, amount: Long): SortedSet[Signed[TokenLock]] = SortedSet(signedTokenLock(seed, amount))

  /** A freshly-built store+overlay backed by an in-memory producer — used to build a "reference post-state" store whose per-field roots are
    * gl0's signed roots (built the production way via `MptStore.insert[V]`). Each call is independent.
    */
  private def mkStore(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey])] =
    for {
      mptProducer <- io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
    } yield (store, overlay)

  /** The PRIOR consumed-field state — what the follower's mirror holds before this ordinal's writes (Address-keyed). Three balances, two
    * txRefs, one allowSpendRef, one tokenLockRef. addr(3) balance is included so a later test can prove a REMOVAL. Built via the verifier
    * itself (verify a delta-from-empty against its own recomputed roots) so it is a real `Verified` value with no public-constructor
    * access.
    */
  private def mkPriorState(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[ConsumedFieldState] =
    verifyToState(
      ConsumedFieldDelta(
        balances = SortedMap(addr(1) -> bal(1000), addr(2) -> bal(2000), addr(3) -> bal(3000)),
        lastTxRefs = SortedMap(addr(1) -> txRef(1), addr(2) -> txRef(2)),
        lastAllowSpendRefs = SortedMap(addr(1) -> allowSpendRef(10)),
        lastTokenLockRefs = SortedMap(addr(1) -> tokenLockRef(100)),
        activeTokenLocks = SortedMap(addr(1) -> tokenLockSet(1, 500)),
        removals = SortedMap.empty
      )
    )

  /** Run the verifier over an all-upserts delta-from-empty using its OWN self-derived roots, returning the resulting `ConsumedFieldState`.
    * Lets the suite materialize a real prior state (private constructor) the same way gl1 builds its mirror — the self-derived roots are by
    * construction the roots of exactly this state, so it always verifies.
    */
  private def verifyToState(delta: ConsumedFieldDelta)(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[ConsumedFieldState] =
    selfRoots(delta).flatMap { roots =>
      verifier.verifyByFieldRoot(ConsumedFieldState.empty, ordinal, delta, roots).flatMap {
        case Right(v)  => IO.pure(v.value)
        case Left(err) => IO.raiseError(new RuntimeException(s"verifyToState failed: $err"))
      }
    }

  /** The per-field roots of the post-state that applying `delta` to an empty mirror produces — derived by forward-hashing the delta's
    * Address-keyed upserts exactly the way the verifier does (`toHex(hypergraph(field, addr))` + `immutableBytes`), then
    * `fieldRootFromBytes`. Used only to self-seed `verifyToState`.
    */
  private def selfRoots(
    delta: ConsumedFieldDelta
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[SortedMap[GlobalStateFieldId, Hash]] =
    for {
      bal <- rootOf(GlobalStateFieldId.Balances, delta.balances)
      tx <- rootOf(GlobalStateFieldId.LastTxRefs, delta.lastTxRefs)
      as <- rootOf(GlobalStateFieldId.LastAllowSpendRefs, delta.lastAllowSpendRefs)
      tl <- rootOf(GlobalStateFieldId.LastTokenLockRefs, delta.lastTokenLockRefs)
      atl <- rootOf(GlobalStateFieldId.ActiveTokenLocks, delta.activeTokenLocks)
    } yield
      SortedMap(
        GlobalStateFieldId.Balances -> bal,
        GlobalStateFieldId.LastTxRefs -> tx,
        GlobalStateFieldId.LastAllowSpendRefs -> as,
        GlobalStateFieldId.LastTokenLockRefs -> tl,
        GlobalStateFieldId.ActiveTokenLocks -> atl
      )

  private def rootOf[V: ImmutableCodec](
    field: GlobalStateFieldId,
    m: SortedMap[Address, V]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[Hash] =
    m.toList.traverse {
      case (a, v) =>
        GlobalStateKey
          .toHex[IO](GlobalStateKey.hypergraph(field, a))
          .map(_ -> ImmutableCodec[V].immutableBytes(v).toArray)
    }
      .map(_.toMap)
      .flatMap(GlobalStateConverter.fieldRootFromBytes[IO])

  /** The REFERENCE post-state — prior with the delta applied: addr(1) balance 1000→1500, addr(4) added, addr(3) removed; addr(2) txRef
    * bumped; addr(2) allowSpendRef added; addr(1) tokenLockRef bumped; addr(1) activeTokenLocks 500→600. Its per-field roots are gl0's
    * signed roots — `activeTokenLocks` inserted via `store.insert[SortedSet[Signed[TokenLock]]]` (the EXACT gl0 writer codec) so the
    * recompute is byte-identical.
    */
  private def seedReference(store: MptStore[IO, GlobalStateKey]): IO[Unit] =
    for {
      _ <- store.insert[Balance](Map(balanceKey(1) -> bal(1500), balanceKey(2) -> bal(2000), balanceKey(4) -> bal(4000)))
      _ <- store.insert[TransactionReference](Map(txRefKey(1) -> txRef(1), txRefKey(2) -> txRef(5)))
      _ <- store.insert[AllowSpendReference](Map(allowSpendRefKey(1) -> allowSpendRef(10), allowSpendRefKey(2) -> allowSpendRef(20)))
      _ <- store.insert[TokenLockReference](Map(tokenLockRefKey(1) -> tokenLockRef(101)))
      _ <- store.insert[SortedSet[Signed[TokenLock]]](Map(activeTokenLockKey(1) -> tokenLockSet(1, 600)))
      _ <- store.commit(ordinal)
    } yield ()

  /** gl0's exact signed per-field roots for the reference store: read the reference store's bytes the way the MPT writer wrote them, group
    * by the consumed field's prefix, and feed each group to `GlobalStateConverter.fieldRootFromBytes` — the SAME callable gl0's stateProof
    * builder routes through. NOT hardcoded; this is the determinism cross-check.
    */
  private def expectedRootsFromReference(
    refStore: MptStore[IO, GlobalStateKey]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[SortedMap[GlobalStateFieldId, Hash]] =
    refStore.allEntriesAsBytes.flatMap { entries =>
      FollowVerifyCore.consumedFields.traverse { field =>
        GlobalStateKey.hypergraphFieldPrefix[IO](field).flatMap { prefix =>
          val fieldEntries = entries.filter { case (hex, _) => hex.value.startsWith(prefix.value) }
          GlobalStateConverter.fieldRootFromBytes[IO](fieldEntries).map(field -> _)
        }
      }
        .map(SortedMap.from(_))
    }

  /** The correct delta from prior → reference (Address-keyed): upserts (changed/added entries) + removals (addr(3) in Balances). */
  private val correctDelta: ConsumedFieldDelta =
    ConsumedFieldDelta(
      balances = SortedMap(addr(1) -> bal(1500), addr(4) -> bal(4000)),
      lastTxRefs = SortedMap(addr(2) -> txRef(5)),
      lastAllowSpendRefs = SortedMap(addr(2) -> allowSpendRef(20)),
      lastTokenLockRefs = SortedMap(addr(1) -> tokenLockRef(101)),
      activeTokenLocks = SortedMap(addr(1) -> tokenLockSet(1, 600)),
      removals = SortedMap(GlobalStateFieldId.Balances -> Set(addr(3)))
    )

  /** The expected Address-keyed post-state maps == the reference (prior + correct delta applied). Plain `SortedMap`s — `ConsumedFieldState`
    * has a private constructor, so the verified value's accessor maps are compared against these directly.
    */
  private val expectedBalances: SortedMap[Address, Balance] = SortedMap(addr(1) -> bal(1500), addr(2) -> bal(2000), addr(4) -> bal(4000))
  private val expectedTxRefs: SortedMap[Address, TransactionReference] = SortedMap(addr(1) -> txRef(1), addr(2) -> txRef(5))
  private val expectedAllowSpendRefs: SortedMap[Address, AllowSpendReference] =
    SortedMap(addr(1) -> allowSpendRef(10), addr(2) -> allowSpendRef(20))
  private val expectedTokenLockRefs: SortedMap[Address, TokenLockReference] = SortedMap(addr(1) -> tokenLockRef(101))
  private val expectedActiveTokenLocks: SortedMap[Address, SortedSet[Signed[TokenLock]]] = SortedMap(addr(1) -> tokenLockSet(1, 600))

  private def verifier(implicit h: Hasher[IO], js: JsonSerializer[IO]): GlobalFollowMirrorVerifier[IO] = GlobalFollowMirrorVerifier.make[IO]

  test("correct delta: all five field roots match → Verified equals the reference post-state") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, correctDelta, signedRoots)
    } yield
      verifiedE match {
        case Right(verified: Verified[ConsumedFieldState]) =>
          val st = verified.value
          expect.all(
            st.balances == expectedBalances,
            st.lastTxRefs == expectedTxRefs,
            st.lastAllowSpendRefs == expectedAllowSpendRefs,
            st.lastTokenLockRefs == expectedTokenLockRefs,
            st.activeTokenLocks == expectedActiveTokenLocks
          )
        case Left(err) => failure(s"expected Right(Verified), got Left($err)")
      }
  }

  test("wrong value: tampering one upsert value → FieldRootMismatch for that field") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      // Corrupt the upserted Balances value for addr(1): 1500 → 9999. Same key, different bytes ⇒ different
      // subtree root than gl0's signed Balances root.
      tamperedDelta = correctDelta.copy(balances = correctDelta.balances.updated(addr(1), bal(9999)))

      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, tamperedDelta, signedRoots)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(GlobalStateFieldId.Balances, expected, got)) =>
          expect.all(expected == signedRoots(GlobalStateFieldId.Balances), got != expected)
        case other => failure(s"expected Left(FieldRootMismatch(Balances, ...)), got $other")
      }
  }

  // The 5th consumed field specifically: tampering the activeTokenLocks value (different TokenLock amount → different
  // encoded bytes via `signedTokenLockSetCodec`) must mismatch gl0's signed ActiveTokenLocks root. This is the local
  // byte-identity proof for the field the gl1 token-lock-replacement validator reads.
  test("wrong value (activeTokenLocks): tampering the token-lock set → FieldRootMismatch for ActiveTokenLocks") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      // Reference activeTokenLocks for addr(1) is the set with amount 600; bump the delta's value to 9999.
      tamperedDelta = correctDelta.copy(activeTokenLocks = correctDelta.activeTokenLocks.updated(addr(1), tokenLockSet(1, 9999)))

      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, tamperedDelta, signedRoots)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(GlobalStateFieldId.ActiveTokenLocks, expected, got)) =>
          expect.all(expected == signedRoots(GlobalStateFieldId.ActiveTokenLocks), got != expected)
        case other => failure(s"expected Left(FieldRootMismatch(ActiveTokenLocks, ...)), got $other")
      }
  }

  // THE WHOLE POINT: an OMITTED genuinely-changed entry — invisible to the per-key range verifier when dropped
  // with its inclusion proof — IS caught here because the field subtree root no longer matches gl0's signed root.
  test("OMITTED entry (the point): dropping a genuinely-changed key from the delta → FieldRootMismatch") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      // Drop the addr(2) txRef bump from the delta entirely. The follower's txRefs subtree then keeps the OLD
      // value (txRef(2)) → its recomputed root differs from gl0's signed LastTxRefs root (which reflects txRef(5)).
      omittedDelta = correctDelta.copy(lastTxRefs = correctDelta.lastTxRefs - addr(2))

      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, omittedDelta, signedRoots)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(GlobalStateFieldId.LastTxRefs, expected, got)) =>
          expect.all(expected == signedRoots(GlobalStateFieldId.LastTxRefs), got != expected)
        case other => failure(s"expected Left(FieldRootMismatch(LastTxRefs, ...)) for the omitted entry, got $other")
      }
  }

  test("extra spurious entry: a key gl0 did not change → FieldRootMismatch") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      // Inject an upsert for addr(7), which gl0 never wrote. The follower's Balances subtree gains a leaf the
      // reference post-state does not have ⇒ recomputed root differs from gl0's signed Balances root.
      spuriousDelta = correctDelta.copy(balances = correctDelta.balances.updated(addr(7), bal(7777)))

      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, spuriousDelta, signedRoots)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.FieldRootMismatch(GlobalStateFieldId.Balances, expected, got)) =>
          expect.all(expected == signedRoots(GlobalStateFieldId.Balances), got != expected)
        case other => failure(s"expected Left(FieldRootMismatch(Balances, ...)) for the spurious entry, got $other")
      }
  }

  test("removal handled: deleting a key gl0 removed → Balances root matches the post-removal root → Verified") { res =>
    implicit val (h, _, js) = res
    for {
      refSetup <- mkStore
      (refStore, _) = refSetup
      _ <- seedReference(refStore)
      signedRoots <- expectedRootsFromReference(refStore)

      // `correctDelta` already removes addr(3) and the reference omits it. A successful verify therefore proves
      // the removal is applied before the recompute and the post-removal Balances subtree root matches.
      removalPresent = correctDelta.removals.getOrElse(GlobalStateFieldId.Balances, Set.empty).contains(addr(3))
      prior <- mkPriorState
      verifiedE <- verifier.verifyByFieldRoot(prior, ordinal, correctDelta, signedRoots)

      // Control: dropping the removal leaves addr(3) in the subtree ⇒ mismatch. Confirms the removal is genuinely
      // load-bearing for the match (not vacuously satisfied).
      noRemovalDelta = correctDelta.copy(removals = SortedMap.empty[GlobalStateFieldId, Set[Address]])
      noRemovalE <- verifier.verifyByFieldRoot(prior, ordinal, noRemovalDelta, signedRoots)
    } yield
      expect.all(
        removalPresent,
        verifiedE.isRight,
        noRemovalE match {
          case Left(FollowVerificationError.FieldRootMismatch(GlobalStateFieldId.Balances, _, _)) => true
          case _                                                                                  => false
        }
      )
  }

}
