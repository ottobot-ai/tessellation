package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator.NotEnoughCurrencyIdBalance
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Acceptance-bar suite for ATOMIC CROSS-SHARD ALLOW-SPEND SETTLEMENT (I-ONCE, Option 2 — read-side effective-balance overlay).
  *
  * THE invariant under test: a cross-shard allow-spend consume can never INFLATE the money supply. An allow-spend `AS` on currency `M`
  * reserves the source `−amount−fee` at creation; `M` autonomously refunds `+amount` on expiry (fee permanently taken); gl0 ADOPTS `M`'s
  * balances into the per-MG `MgBalances` mirror — so after a cross-shard consume `M` never witnessed expires, `M` REFUNDS the source a
  * PHANTOM `+amount`. The settlement closes this two ways:
  *   - '''W3d nullifier''' (`AllowSpendConsumeHandler` / `ConsumedAllowSpendStateManager.settle`): the consume writes a
  *     `ConsumedAllowSpends` marker; a replay (the same `hash(AS)`) is REJECTED by the absence check.
  *   - '''W3e read-side effective-balance overlay''' (`effectiveCurrencyBalances`): at the `SpendActionValidator` read site, `M`'s ATTESTED
  *     balances are overlaid with the committed spent-set so the source's EFFECTIVE balance stays debited — a no-`allowSpendRef` self-spend
  *     of the phantom-refunded amount is REJECTED for insufficient balance.
  *
  * Tests:
  *   1. NO-INFLATION (effective-balance arithmetic) — source effective stays `B−X` across the expiry boundary; destination credited once.
  *   1. FORCING FUNCTION through the real `SpendActionValidator` — after expiry, a no-`allowSpendRef` self-spend of `X` by the source is
  *      REJECTED (`NotEnoughCurrencyIdBalance`) under the EFFECTIVE balance, but ACCEPTED under the (phantom-refunded) ATTESTED balance.
  *   1. numShards=1 IDENTITY — empty spent-set ⇒ effective == attested ⇒ validator behaviour byte-identical across ordinals.
  *   1. F1 CLAMP — attested < debit ⇒ effective clamps to `Balance.empty`, no exception.
  *   1. W3d DOUBLE-CONSUME / REPLAY / NO-RESERVATION — the nullifier rejects double/replay/absent consumes.
  */
object ConsumedAllowSpendSettlementSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val ord1: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  // Refined helpers — refined macros require literals, so build from `Long` test vars via `unsafeFrom`.
  private def swap(v: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(v))
  private def feeOf(v: Long): AllowSpendFee = AllowSpendFee(NonNegLong.unsafeFrom(v))
  private def epoch(v: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(v))
  private def bal(v: Long): Balance = Balance(NonNegLong.unsafeFrom(v))

  /** A `ConsumedAllowSpendStateManager` backed by a fresh in-memory MPT store (empty spent-set unless seeded). */
  private def mkManager(
    seedSpentSet: SortedMap[Hash, ConsumedAllowSpend] = SortedMap.empty
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[ConsumedAllowSpendStateManager[IO]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- seedSpentSet.toList.traverse_ {
        case (hash, marker) =>
          store.insert[ConsumedAllowSpend](GlobalStateKey.consumedAllowSpendKey(hash), marker)(
            io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec.immutableCodec
          )
      }
    } yield ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(store))

  /** Scan `numShards ∈ [2,64]` for a value placing the two metagraph addresses on DIFFERENT shards. */
  private def findNumShardsSplitting(m: Address, mPrime: Address)(implicit hasher: Hasher[IO]): IO[Int] =
    (2 to 64).toList.findM { n =>
      val a = ShardAssignment.make[IO](n)
      (a.shardIdFor(m), a.shardIdFor(mPrime)).mapN(_ =!= _)
    }
      .map(_.getOrElse(throw new AssertionError("could not split the two metagraphs across any numShards in [2,64]")))

  private def mkAllowSpend(
    source: Address,
    destination: Address,
    currencyId: Option[CurrencyId],
    amount: Long,
    feeAmount: Long,
    expireAt: EpochProgress
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      Signed.forAsyncHasher(
        AllowSpend(
          source = source,
          destination = destination,
          currencyId = currencyId,
          amount = swap(amount),
          fee = feeOf(feeAmount),
          parent = AllowSpendReference.empty,
          lastValidEpochProgress = expireAt,
          approvers = List(destination)
        ),
        kp
      )
    }

  private def marker(
    asHash: Hash,
    source: Address,
    destination: Address,
    currency: Address,
    amount: Long,
    expiry: Long
  ): ConsumedAllowSpend =
    ConsumedAllowSpend(
      allowSpendHash = asHash,
      source = source,
      destination = destination,
      currencyId = CurrencyId(currency).some,
      amount = swap(amount),
      lastValidEpochProgress = epoch(expiry),
      consumedAtOrdinal = ord1,
      consumingSpendRef = asHash
    )

  // ───────────────────────────────────────────────────────────────────────────
  // Test 1 — NO-INFLATION: effective-balance arithmetic across the expiry boundary
  // ───────────────────────────────────────────────────────────────────────────

  test("NO-INFLATION: effective source balance stays debited across expiry; destination credited once") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress
      mgr <- mkManager()

      // AS on currency M: X=100, expiry E=200. One spent-set marker (the cross-shard consume already settled).
      x = 100L
      asHash = Hash("aa".padTo(64, '0').take(64))
      spentSet = SortedMap(asHash -> marker(asHash, source, dest, m, x, 200L))

      // M's attested balances: BEFORE expiry source already −X (reservation in M's ledger) = B−X; dest D0.
      bigB = 1000L
      d0 = 10L
      attestedPre = SortedMap(source -> bal(bigB - x), dest -> bal(d0)): SortedMap[Address, Balance]
      effPre = mgr.effectiveCurrencyBalances(attestedPre, m.some, spentSet, Map(m -> epoch(150L)), epoch(150L))

      // AFTER expiry M REFUNDS source +X ⇒ attested source = B (phantom). The overlay must re-debit.
      attestedPost = SortedMap(source -> bal(bigB), dest -> bal(d0)): SortedMap[Address, Balance]
      effPost = mgr.effectiveCurrencyBalances(attestedPost, m.some, spentSet, Map(m -> epoch(250L)), epoch(250L))
    } yield
      expect.all(
        // BEFORE expiry: source still −X (attested already debited; no correction), dest credited +X.
        effPre.getOrElse(source, Balance.empty).value.value == bigB - x,
        effPre.getOrElse(dest, Balance.empty).value.value == d0 + x,
        // AFTER expiry: source STILL B−X (NOT the phantom B), dest still credited once.
        effPost.getOrElse(source, Balance.empty).value.value == bigB - x,
        effPost.getOrElse(dest, Balance.empty).value.value == d0 + x
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 2 — FORCING FUNCTION through the real SpendActionValidator
  // ───────────────────────────────────────────────────────────────────────────

  test("FORCING FUNCTION: after expiry a phantom-refunded self-spend is REJECTED under effective balance, ACCEPTED under attested") { res =>
    implicit val (h, sp, js) = res
    // numShards=1 validator (the read site is a same-shard balance check on the source's own currency-M balance; the cross-shard
    // classification that produced the spent-set is upstream — here we exercise the EFFECT: a no-allowSpendRef self-spend of the phantom X).
    val validator = SpendActionValidator.make[IO]
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      dest = destKp.getPublic.toAddress
      mgr <- mkManager()

      // The source IS the metagraph M (a no-allowSpendRef self-spend is by the MG address against its own currency balance).
      // Earlier, M created AS(source=M, X) consumed cross-shard; spent-set holds the marker; AS has expired (epoch 250 > E=200).
      x = 100L
      asHash = Hash("bb".padTo(64, '0').take(64))
      spentSet = SortedMap(asHash -> marker(asHash, m, dest, m, x, 200L))

      // M's ATTESTED currency balance AFTER refund = B (phantom: the X came back). Real spendable = B − X.
      bigB = 100L // exactly X, so the phantom refund is the ONLY thing that could fund the self-spend
      attested = SortedMap(m -> bal(bigB)): SortedMap[Address, Balance]
      effective = mgr.effectiveCurrencyBalances(attested, m.some, spentSet, Map(m -> epoch(250L)), epoch(250L))

      // A no-allowSpendRef self-spend by M of X against currency M.
      selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swap(x), m, dest)
      action = SpendAction(NonEmptyList.of(selfSpend))
      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

      // Under the EFFECTIVE balance (m = B − X = 0) ⇒ REJECTED for insufficient balance.
      resEffective <- validator.validate(action, activeAllowSpends, Map(m.some -> effective), m)
      // Under the ATTESTED (phantom) balance (m = B = X) ⇒ ACCEPTED — proving the overlay is what closes the hole.
      resAttested <- validator.validate(action, activeAllowSpends, Map(m.some -> attested), m)
    } yield
      expect.all(
        // effective m-balance is the real (debited) value.
        effective.getOrElse(m, Balance.empty).value.value == bigB - x,
        // The self-spend is REJECTED under effective with NotEnoughCurrencyIdBalance (the double-spend is closed).
        resEffective.isInvalid,
        resEffective.fold(_.exists { case _: NotEnoughCurrencyIdBalance => true; case _ => false }, _ => false),
        // …but ACCEPTED under the raw attested (phantom-refunded) balance — confirming the overlay is load-bearing.
        resAttested.isValid
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 3 — numShards=1 multi-ordinal IDENTITY
  // ───────────────────────────────────────────────────────────────────────────

  test("numShards=1 IDENTITY: empty spent-set ⇒ effective == attested ⇒ validator behaviour byte-identical") { res =>
    implicit val (h, sp, js) = res
    val validator = SpendActionValidator.make[IO]
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      dest = destKp.getPublic.toAddress
      mgr <- mkManager()

      x = 100L
      // Two "ordinals" worth of attested balances; spent-set is EMPTY (numShards=1 ⇒ no cross-shard consume ever).
      emptySpentSet = SortedMap.empty[Hash, ConsumedAllowSpend]
      attestedN = SortedMap(m -> bal(1000L)): SortedMap[Address, Balance]
      attestedNk = SortedMap(m -> bal(950L)): SortedMap[Address, Balance]
      effN = mgr.effectiveCurrencyBalances(attestedN, m.some, emptySpentSet, Map.empty, epoch(100L))
      effNk = mgr.effectiveCurrencyBalances(attestedNk, m.some, emptySpentSet, Map.empty, epoch(250L))

      selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swap(x), m, dest)
      action = SpendAction(NonEmptyList.of(selfSpend))
      noAllow = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      resEff <- validator.validate(action, noAllow, Map(m.some -> effN), m)
      resAtt <- validator.validate(action, noAllow, Map(m.some -> attestedN), m)
    } yield
      expect.all(
        // Identity: effective IS the attested map (the SAME reference is returned when no marker applies).
        effN == attestedN,
        effNk == attestedNk,
        // Validator behaviour is identical between effective and attested input.
        resEff.isValid == resAtt.isValid
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 4 — F1 CLAMP: attested < debit ⇒ Balance.empty, no exception
  // ───────────────────────────────────────────────────────────────────────────

  test("F1 CLAMP: attested source balance below the consumed-and-expired amount ⇒ effective clamps to Balance.empty (no raise)") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress
      mgr <- mkManager()

      // Consumed-and-expired amount X=100, but the source's attested balance is only 30 (e.g. the metagraph let the source spend the
      // phantom-refund elsewhere first). The debit (100) exceeds 30 ⇒ MUST clamp to empty, NOT raise / underflow.
      x = 100L
      asHash = Hash("cc".padTo(64, '0').take(64))
      spentSet = SortedMap(asHash -> marker(asHash, source, dest, m, x, 200L))
      attested = SortedMap(source -> bal(30L)): SortedMap[Address, Balance]
      effective = mgr.effectiveCurrencyBalances(attested, m.some, spentSet, Map(m -> epoch(250L)), epoch(250L))
    } yield
      expect.all(
        // Saturating subtraction: 30 + 0(credit to source) − 100(debit) clamps to 0, no exception thrown.
        effective.getOrElse(source, Balance.empty).value.value == 0L,
        // destination still credited the +X (separate address).
        effective.getOrElse(dest, Balance.empty).value.value == x
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 5 — W3d DOUBLE-CONSUME (two cross-shard spends, one AS, same ordinal)
  // ───────────────────────────────────────────────────────────────────────────

  test("W3d DOUBLE-CONSUME: two cross-shard spends consume the same AS ⇒ first settles, second rejected") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeAKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeBKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      mPrimeA = mPrimeAKp.getPublic.toAddress
      mPrimeB = mPrimeBKp.getPublic.toAddress
      numShards <- (2 to 128).toList.findM { n =>
        val a = ShardAssignment.make[IO](n)
        (a.shardIdFor(m), a.shardIdFor(mPrimeA), a.shardIdFor(mPrimeB)).mapN {
          case (sM, sA, sB) => sM =!= sA && sM =!= sB
        }
      }
        .map(_.getOrElse(throw new AssertionError("could not split both producers from M")))
      assignment = ShardAssignment.make[IO](numShards)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress

      x = 100L
      as <- mkAllowSpend(source, dest, CurrencyId(m).some, x, 0L, epoch(200L))
      asHashed <- as.toHashed
      asHash = asHashed.hash
      lastActiveAllowSpends = SortedMap(
        m.some -> SortedMap(source -> SortedSet(as))
      ): SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

      spendTxA = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      spendTxB = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      acceptedSpendActions = SortedMap(
        mPrimeA -> List(SpendAction(NonEmptyList.of(spendTxA))),
        mPrimeB -> List(SpendAction(NonEmptyList.of(spendTxB)))
      )

      mgr <- mkManager()
      candidates <- mgr.classifyCrossShardConsumes(acceptedSpendActions, assignment)
      settlement <- mgr.settleCrossShardConsumes(candidates, SortedMap.empty, lastActiveAllowSpends, ord1)
    } yield
      expect.all(
        candidates.size == 2,
        settlement.newMarkers.size == 1,
        settlement.newMarkers.contains(asHash),
        settlement.rejected.contains(asHash)
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 6 — W3d CROSS-SNAPSHOT REPLAY (already in on-disk spent-set ⇒ rejected)
  // ───────────────────────────────────────────────────────────────────────────

  test("W3d CROSS-SNAPSHOT REPLAY: a consume whose AS is already in the on-disk spent-set is rejected") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      mPrime = mPrimeKp.getPublic.toAddress
      numShards <- findNumShardsSplitting(m, mPrime)
      assignment = ShardAssignment.make[IO](numShards)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress

      x = 100L
      as <- mkAllowSpend(source, dest, CurrencyId(m).some, x, 0L, epoch(200L))
      asHashed <- as.toHashed
      asHash = asHashed.hash
      lastActiveAllowSpends = SortedMap(
        m.some -> SortedMap(source -> SortedSet(as))
      ): SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

      priorMarker = marker(asHash, source, dest, m, x, 200L)
      mgr <- mkManager(SortedMap(asHash -> priorMarker))
      existing <- mgr.materializeConsumedAllowSpendsFromMpt

      spendTx = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      acceptedSpendActions = SortedMap(mPrime -> List(SpendAction(NonEmptyList.of(spendTx))))
      candidates <- mgr.classifyCrossShardConsumes(acceptedSpendActions, assignment)
      settlement <- mgr.settleCrossShardConsumes(candidates, existing, lastActiveAllowSpends, ord1)
    } yield
      expect.all(
        existing.size == 1,
        existing.get(asHash).contains(priorMarker),
        candidates.size == 1,
        settlement.newMarkers.isEmpty,
        settlement.rejected.contains(asHash)
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 7 — W3d NO-SUCH-RESERVATION (AS absent from M's mirror ⇒ rejected)
  // ───────────────────────────────────────────────────────────────────────────

  test("W3d NO-SUCH-RESERVATION: a cross-shard consume of an AS absent from M's mirror is rejected") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      mPrime = mPrimeKp.getPublic.toAddress
      numShards <- findNumShardsSplitting(m, mPrime)
      assignment = ShardAssignment.make[IO](numShards)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress

      x = 100L
      as <- mkAllowSpend(source, dest, CurrencyId(m).some, x, 0L, epoch(200L))
      asHashed <- as.toHashed
      asHash = asHashed.hash
      emptyMirror = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]

      mgr <- mkManager()
      spendTx = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      acceptedSpendActions = SortedMap(mPrime -> List(SpendAction(NonEmptyList.of(spendTx))))
      candidates <- mgr.classifyCrossShardConsumes(acceptedSpendActions, assignment)
      settlement <- mgr.settleCrossShardConsumes(candidates, SortedMap.empty, emptyMirror, ord1)
    } yield
      expect.all(
        candidates.size == 1,
        settlement.newMarkers.isEmpty,
        settlement.rejected.contains(asHash)
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 8 — AllowSpendConsumeHandler (the generic seam instance) end-to-end markers
  // ───────────────────────────────────────────────────────────────────────────

  test("AllowSpendConsumeHandler.settle produces the ConsumedAllowSpends marker keyed by consumedAllowSpendKey") { res =>
    implicit val (h, sp, js) = res
    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      mPrime = mPrimeKp.getPublic.toAddress
      numShards <- findNumShardsSplitting(m, mPrime)
      assignment = ShardAssignment.make[IO](numShards)

      sourceKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      source = sourceKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress

      x = 100L
      // Seed M's ActiveAllowSpends mirror in the store (the handler materializes it for the include-check).
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      as <- mkAllowSpend(source, dest, CurrencyId(m).some, x, 0L, epoch(200L))
      asHashed <- as.toHashed
      asHash = asHashed.hash
      _ <- store.insert[SortedSet[Signed[AllowSpend]]](
        GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, m.some, source),
        SortedSet(as)
      )(io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.signedAllowSpendSetCodec)
      mgr = ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(store))
      handler = AllowSpendConsumeHandler[IO](mgr)

      spendTx = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      acceptedSpendActions = SortedMap(mPrime -> List(SpendAction(NonEmptyList.of(spendTx))))
      writeOut <- handler.settle(acceptedSpendActions, assignment, ord1)
      expectedKey = GlobalStateKey.consumedAllowSpendKey(asHash)
    } yield
      expect.all(
        handler.nullifierFieldId == GlobalStateFieldId.ConsumedAllowSpends,
        writeOut.markers.size == 1,
        writeOut.markers.contains(expectedKey),
        writeOut.rejected.isEmpty
      )
  }
}
