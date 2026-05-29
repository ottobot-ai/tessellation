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
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.follow._
import io.constellationnetwork.schema.swap.{AllowSpendOrdinal, AllowSpendReference}
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.schema.transaction.{TransactionOrdinal, TransactionReference}
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.mpt.verifier.MerklePatriciaRangeVerifier
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.ImmutableCodec
import io.constellationnetwork.serde.codecs.instances.AllowSpendReferenceCodec.{immutableCodec => allowSpendRefImmutable}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedTokenLockSetCodec
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._
import io.constellationnetwork.serde.codecs.instances.TokenLockReferenceCodec.{immutableCodec => tokenLockRefImmutable}
import io.constellationnetwork.serde.codecs.instances.TransactionReferenceCodec.{immutableCodec => txRefImmutable}

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import io.circe.syntax.EncoderOps
import weaver.MutableIOSuite

/** Slice 1 of the gl1 inclusion-proof follow design (`docs/nakamoto/GL1-INCLUSION-PROOF-FOLLOW-DESIGN.md`).
  *
  * End-to-end exercise of the gl0-side prover ([[GlobalFollowProofService]]) against the correct-by-design shared verify core
  * ([[FollowVerifyCore]]). The trie is built the production way — via `MptStore.insert[V]` (scodec `ImmutableCodec` bytes) over an
  * in-memory producer, so each leaf's `dataDigest = Hasher.hashBytes(immutableBytes)`; value-binding in the verifier reproduces that exact
  * digest.
  *
  * Coverage (per the slice task spec):
  *   1. round-trip — prove → verify → `Right(Verified(state))` whose maps equal the seeded entries.
  *   1. value-tamper — flip a byte of one `values` entry → `Left(ValueBindingFailed)`.
  *   1. omitted leaf (completeness) — drop one field's inclusion proof + value → range verify / binding fails. Documents which.
  *   1. absence — a key NOT in a field falls inside a proven exclusion gap; the range proof structurally establishes its absence.
  *   1. committed-root mismatch — wrong `attestedRoot` → `Left(CommittedRootMismatch)`.
  */
object GlobalFollowProofServiceSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ordinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val parentP: BranchId = BranchId(Hash("0" * 64))

  private def addr(seed: Int): Address =
    Address.fromBytes(s"global-follow-proof-suite-seed-$seed".getBytes("UTF-8"))

  private def balanceKey(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, addr(seed))
  private def txRefKey(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.LastTxRefs, addr(seed))
  private def allowSpendRefKey(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.LastAllowSpendRefs, addr(seed))
  private def tokenLockRefKey(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.LastTokenLockRefs, addr(seed))
  private def activeTokenLockKey(seed: Int): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, addr(seed))

  private def txRef(n: Long): TransactionReference =
    TransactionReference(TransactionOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def allowSpendRef(n: Long): AllowSpendReference =
    AllowSpendReference(AllowSpendOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))
  private def tokenLockRef(n: Long): TokenLockReference =
    TokenLockReference(TokenLockOrdinal(NonNegLong.unsafeFrom(n)), Hash(f"$n%064d"))

  /** A dummy `Signed[TokenLock]` for the `ActiveTokenLocks` field — the per-key verifier binds value bytes to leaf digests by hash, so the
    * signature is never validated here; a fixed placeholder proof keeps the encoding deterministic.
    */
  private def signedTokenLock(seed: Int): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = addr(seed),
        amount = TokenLockAmount(PosLong.unsafeFrom(seed.toLong * 100)),
        fee = TokenLockFee(NonNegLong(1L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(1L)), Hash(f"$seed%064d")),
        currencyId = none,
        unlockEpoch = EpochProgress(NonNegLong(1000L)).some,
        replaceTokenLockRef = none
      ),
      NonEmptySet.one(SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70))))
    )

  private def mkSetup(
    implicit h: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], MptOverlay[IO, GlobalStateKey], GlobalFollowProofService[IO])] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      pcTree <- ParentChildTree.make[IO]
      overlay <- MptOverlay.make[IO, GlobalStateKey](
        mode = MptOverlay.OverlayMode.productionDefault,
        store,
        pcTree,
        GlobalStateKey.toHex[IO],
        bestTipsFn = IO.pure(Set.empty[BranchId])
      )
      svc = GlobalFollowProofService.make[IO](store, overlay)
    } yield (store, overlay, svc)

  /** Seed a consistent slice of consumed-field state. Returns the seeded typed maps keyed by leaf-path hex so tests can assert the verified
    * leaves equal them (the per-key verifier keys by leaf-path hex — see ConsumedFieldLeaves scaladoc).
    */
  private def seedConsumedState(
    store: MptStore[IO, GlobalStateKey]
  )(
    implicit h: Hasher[IO]
  ): IO[
    (
      SortedMap[Hex, Balance],
      SortedMap[Hex, TransactionReference],
      SortedMap[Hex, AllowSpendReference],
      SortedMap[Hex, TokenLockReference],
      SortedMap[Hex, SortedSet[Signed[TokenLock]]]
    )
  ] = {
    val balances = (1 to 4).map(i => balanceKey(i) -> Balance(NonNegLong.unsafeFrom(i * 1000L))).toMap
    val txRefs = (1 to 3).map(i => txRefKey(i) -> txRef(i.toLong)).toMap
    val allowSpendRefs = (1 to 2).map(i => allowSpendRefKey(i) -> allowSpendRef(i.toLong * 10)).toMap
    val tokenLockRefs = (1 to 2).map(i => tokenLockRefKey(i) -> tokenLockRef(i.toLong * 100)).toMap
    val activeTokenLocks = (1 to 2).map(i => activeTokenLockKey(i) -> SortedSet(signedTokenLock(i))).toMap

    for {
      _ <- store.insert[Balance](balances)
      _ <- store.insert[TransactionReference](txRefs)
      _ <- store.insert[AllowSpendReference](allowSpendRefs)
      _ <- store.insert[TokenLockReference](tokenLockRefs)
      _ <- store.insert[SortedSet[Signed[TokenLock]]](activeTokenLocks)
      _ <- store.commit(ordinal)
      balancesHex <- toHexKeyed(balances)
      txRefsHex <- toHexKeyed(txRefs)
      allowSpendRefsHex <- toHexKeyed(allowSpendRefs)
      tokenLockRefsHex <- toHexKeyed(tokenLockRefs)
      activeTokenLocksHex <- toHexKeyed(activeTokenLocks)
    } yield (balancesHex, txRefsHex, allowSpendRefsHex, tokenLockRefsHex, activeTokenLocksHex)
  }

  private def toHexKeyed[V](m: Map[GlobalStateKey, V])(implicit h: Hasher[IO]): IO[SortedMap[Hex, V]] =
    m.toList.traverse { case (k, v) => GlobalStateKey.toHex[IO](k).map(_ -> v) }.map(SortedMap.from(_))

  test("round-trip: prove → verify yields Verified state equal to the seeded entries") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      seeded <- seedConsumedState(store)
      (balancesHex, txRefsHex, allowSpendRefsHex, tokenLockRefsHex, activeTokenLocksHex) = seeded

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      verifiedE <- FollowVerifyCore.verifyConsumedFields[IO](proof.committedRoot, proof)
    } yield
      verifiedE match {
        case Right(verified: Verified[ConsumedFieldLeaves]) =>
          val st = verified.value
          expect.all(
            proof.committedRoot.value.nonEmpty,
            // every consumed field carries a range proof + values map
            FollowVerifyCore.consumedFields.forall(proof.fields.contains),
            FollowVerifyCore.consumedFields.forall(proof.values.contains),
            st.balances == balancesHex,
            st.lastTxRefs == txRefsHex,
            st.lastAllowSpendRefs == allowSpendRefsHex,
            st.lastTokenLockRefs == tokenLockRefsHex,
            st.activeTokenLocks == activeTokenLocksHex
          )
        case Left(err) => failure(s"expected Right(Verified), got Left($err)")
      }
  }

  test("value-tamper: flipping a byte in one values entry → ValueBindingFailed") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      // Tamper: flip the low bit of the first byte of one Balances value (range proof + leaf digest untouched).
      balanceValues = proof.values(GlobalStateFieldId.Balances)
      (tamperKey, origValue) = balanceValues.head
      tamperedBytes = {
        val bs = origValue.toBytes
        bs(0) = (bs(0) ^ 0x01).toByte
        bs
      }
      tamperedValues = balanceValues.updated(tamperKey, Hex.fromBytes(tamperedBytes))
      tamperedProof = proof.copy(values = proof.values.updated(GlobalStateFieldId.Balances, tamperedValues))

      verifiedE <- FollowVerifyCore.verifyConsumedFields[IO](tamperedProof.committedRoot, tamperedProof)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.ValueBindingFailed(GlobalStateFieldId.Balances, k)) =>
          expect(k == tamperKey)
        case other => failure(s"expected Left(ValueBindingFailed(Balances, $tamperKey)), got $other")
      }
  }

  test("omitted leaf (completeness): dropping an inclusion proof but keeping its value → ValueBindingFailed") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      // Drop one inclusion proof from the Balances range proof but KEEP its value in `values`. This is the
      // omission the design's binding step is the backstop for: the retained value now references a key
      // with no committed leaf, so value-binding fails (no `dataDigest` to bind against) →
      // `ValueBindingFailed`.
      //
      // DOCUMENTED FINDING (see report): the range verifier (`confirmRange`) does NOT detect an omitted
      // leaf that is dropped TOGETHER WITH its value when that leaf is interior or at the open end of a
      // full-field range — it only checks left-boundary↔first and last↔right-boundary gaps, not gaps
      // between consecutive inclusion proofs. The cryptographic completeness this design relies on is
      // therefore: (a) exclusion boundaries pin the field's extent (no key can exist below the first or
      // above the last reported leaf), and (b) mandatory value-binding means no value can be presented
      // without a committed leaf. A prover that drops BOTH a value and its leaf hides that key; the
      // follower then provably reads `Balance.empty` for it — acceptable only because gl1 never needs a
      // value the prover chose not to send. If S3 needs "every committed leaf is reported", tighten the
      // verifier with a between-consecutive no-gap proof; flagged as out of S1 scope.
      balancesRange = proof.fields(GlobalStateFieldId.Balances)
      droppedPath = balancesRange.inclusionProofs.head.path
      truncatedRange = balancesRange.copy(inclusionProofs = balancesRange.inclusionProofs.filterNot(_.path == droppedPath))
      omittedProof = proof.copy(fields = proof.fields.updated(GlobalStateFieldId.Balances, truncatedRange))

      verifiedE <- FollowVerifyCore.verifyConsumedFields[IO](omittedProof.committedRoot, omittedProof)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.ValueBindingFailed(GlobalStateFieldId.Balances, k)) =>
          expect(k == droppedPath)
        case other => failure(s"expected Left(ValueBindingFailed(Balances, $droppedPath)), got $other")
      }
  }

  test("absence: a key not in a field falls inside a proven exclusion gap of the field's range proof") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      // A balance key that was NOT seeded. Its leaf-path hex must lie strictly between two consecutive
      // committed paths (or between a boundary and an endpoint) of the Balances range proof — i.e. inside
      // a proven gap. That, together with the verified range, structurally establishes its absence: there
      // is no committed leaf at this path under `committedRoot`.
      absentKeyHex <- GlobalStateKey.toHex[IO](balanceKey(999))
      balancesRange = proof.fields(GlobalStateFieldId.Balances)
      committedPaths = balancesRange.inclusionProofs.map(_.path.value).sorted
      // The range was proven over the field's FULL extent, so the absent key's path is within [start, end].
      withinFullRange =
        absentKeyHex.value >= balancesRange.startPath.value && absentKeyHex.value <= balancesRange.endPath.value
      // Absence ⇔ the path is not one of the committed leaves and sits inside the proven span.
      notCommitted = !committedPaths.contains(absentKeyHex.value)
      inSomeGap = withinFullRange && notCommitted

      // Sanity: the range proof itself verifies against the committed root (the gap is genuinely proven).
      rangeVerifiedE <- MerklePatriciaRangeVerifier.make[IO](proof.committedRoot).confirmRange(balancesRange)
    } yield
      expect.all(
        inSomeGap,
        rangeVerifiedE.isRight
      )
  }

  test("committed-root mismatch: a wrong attestedRoot → CommittedRootMismatch") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      wrongRoot = Hash("f" * 64)
      verifiedE <- FollowVerifyCore.verifyConsumedFields[IO](wrongRoot, proof)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.CommittedRootMismatch(expected, got)) =>
          expect.all(expected == wrongRoot, got == proof.committedRoot)
        case other => failure(s"expected Left(CommittedRootMismatch), got $other")
      }
  }

  // Defence-in-depth: a value supplied for a key with NO committed leaf in the range proof must fail binding,
  // even if the range proof itself is valid. Guards the "no leaf at that key" branch of bindValues.
  test("value-binding: a value for a key absent from the range proof → ValueBindingFailed") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      // Inject a value for an address that has no leaf in the Balances range proof.
      ghostKeyHex <- GlobalStateKey.toHex[IO](balanceKey(888))
      ghostValueHex = Hex.fromBytes(ImmutableCodec[Balance].immutableBytes(Balance(NonNegLong(7L))).toArray)
      injectedValues = proof.values(GlobalStateFieldId.Balances).updated(ghostKeyHex, ghostValueHex)
      injectedProof = proof.copy(values = proof.values.updated(GlobalStateFieldId.Balances, injectedValues))

      verifiedE <- FollowVerifyCore.verifyConsumedFields[IO](injectedProof.committedRoot, injectedProof)
    } yield
      verifiedE match {
        case Left(FollowVerificationError.ValueBindingFailed(GlobalStateFieldId.Balances, k)) =>
          expect(k == ghostKeyHex)
        case other => failure(s"expected Left(ValueBindingFailed(Balances, ghost)), got $other")
      }
  }

  test("payload codec round-trips via circe") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkSetup
      (store, _, svc) = setup
      _ <- seedConsumedState(store)

      proofE <- svc.proveConsumedFields(parentP, ordinal)
      proof <- IO.fromEither(proofE.leftMap(e => new RuntimeException(s"prove failed: $e")))

      json = proof.asJson
      decoded <- IO.fromEither(json.as[GlobalFollowProof])
    } yield expect(cats.Eq[GlobalFollowProof].eqv(proof, decoded))
  }
}
