package io.constellationnetwork.node.shared.domain.swap

import cats.data.NonEmptyList
import cats.effect.kernel.Ref
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.kryo.KryoSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.node.shared.domain.nakamoto.sharding.{ShardSubtreeProof, ShardSubtreeProofClient}
import io.constellationnetwork.node.shared.domain.swap.SpendActionValidator._
import io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global.ConsumedAllowSpendStateManager
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey}
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.prover.attestation.MerklePatriciaInclusionProof
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.{Hasher, KeyPairGenerator, SecurityProvider}
import io.constellationnetwork.shared.sharedKryoRegistrar

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import io.circe.syntax._
import weaver.MutableIOSuite

/** Slice 11 cross-shard coverage for [[SpendActionValidator]] — exercises the `ShardSubtreeProofClient`-aware constructor introduced by
  * `feat(shard): slice 11`.
  *
  * '''Coverage''' (per slice 11 task spec):
  *   1. '''Same-shard SpendAction''' — target MG hashes to the current shard → uses in-process state; proofClient MUST NOT be invoked.
  *   1. '''Cross-shard happy path''' — target MG hashes to a different shard → proofClient is invoked, returns proven `AllowSpend` bytes,
  *      validator accepts.
  *   1. '''Cross-shard proof fails''' — proofClient returns `None` → SpendAction rejected with [[CrossShardProofUnavailable]] (validator
  *      retries next round).
  *   1. '''Cross-shard tampered value''' — proofClient returns a proof whose value bytes don't decode as `SortedSet[Signed[AllowSpend]]` →
  *      SpendAction rejected with [[CrossShardProofTampered]] (defence-in-depth at the validator).
  *   1. '''Cross-shard balance proof fails''' — for the no-`allowSpendRef` branch, proofClient `None` rejects with
  *      [[CrossShardProofUnavailable]].
  *   1. '''Local fallback (single-shard cluster)''' — `numShards = 1` collapses every MG to shard 0; proofClient never invoked even for
  *      SpendActions referencing other MGs.
  *
  * '''Fixture strategy''':
  *   - Real [[ShardAssignment]] via `ShardAssignment.make`; tests scan `numShards` to ensure two test MG addresses split across different
  *     shards (same trick the Slice 10 suite uses via `findNumShardsSplitting`).
  *   - Mock [[ShardSubtreeProofClient]] backed by a `Ref` so each test can (a) seed a response and (b) assert on the invocation count after
  *     the fact. The mock doesn't run any verification — the production `ShardSubtreeProofClient.http` would, but the validator's contract
  *     here is to trust the client's return value and decode the bytes; tampering is exercised via "value bytes that decode to the wrong
  *     type", which the validator catches structurally.
  *   - Real Circe encoding for the proof's value bytes: `SortedSet[Signed[AllowSpend]].asJson` serialized as UTF-8 — the same shape the
  *     validator decodes via `io.circe.parser`. This gives end-to-end byte-fidelity coverage of the cross-shard wire path without dragging
  *     in the full MPT-prover infrastructure (which is Slice 10's concern, separately covered).
  */
object SpendActionValidatorCrossShardSuite extends MutableIOSuite {

  type Res = (JsonSerializer[IO], Hasher[IO], SecurityProvider[IO])

  def sharedResource: Resource[IO, Res] =
    for {
      implicit0(ks: KryoSerializer[IO]) <- KryoSerializer.forAsync[IO](sharedKryoRegistrar)
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (j, h, sp)

  // ===========================================================================
  // Mock proof client (Ref-backed — captures invocations + returns canned responses)
  // ===========================================================================

  /** Snapshot of one invocation of [[ShardSubtreeProofClient.fetchAndVerify]] — surfaces the args so tests can assert on which (shard, MG,
    * key) tuples the validator queried. The test for "same-shard MUST NOT invoke proofClient" asserts on this list being empty.
    */
  private final case class FetchCall(
    targetShardId: ShardId,
    metagraphAddress: Address,
    key: GlobalStateKey
  )

  /** Construct a Ref-backed mock client. The `responder` callback maps each call to a response; the call is also recorded in `callsRef`.
    * The default responder returns `None` — the test overrides per-scenario.
    */
  private def mkMockClient(
    callsRef: Ref[IO, List[FetchCall]],
    responder: FetchCall => IO[Option[(Option[Array[Byte]], ShardSubtreeProof)]]
  ): ShardSubtreeProofClient[IO] = new ShardSubtreeProofClient[IO] {
    def fetchAndVerify(
      targetShardId: ShardId,
      metagraphAddress: Address,
      key: GlobalStateKey
    ): IO[Option[(Option[Array[Byte]], ShardSubtreeProof)]] = {
      val call = FetchCall(targetShardId, metagraphAddress, key)
      callsRef.update(call :: _) >> responder(call)
    }
  }

  /** A sentinel [[ShardSubtreeProof]] — value-bearing field is only the `value` (the validator uses it to extract bytes); the rest are
    * placeholders. The validator does NOT re-run MPT verification against the proof envelope; that's the client implementation's
    * responsibility (the production `ShardSubtreeProofClient.http` would verify before surfacing). The mock simulates a verified proof by
    * always returning Some(_).
    */
  private def sentinelProof(value: Option[Hex]): ShardSubtreeProof =
    ShardSubtreeProof(
      shardCheckpointHash = Hash.empty,
      metagraphAddress = Address.fromBytes("sentinel".getBytes),
      perMgMptRoot = Hash.empty,
      key = GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, Address.fromBytes("sentinel".getBytes)),
      value = value,
      mptProof = MerklePatriciaInclusionProof(path = Hex(""), witness = List.empty)
    )

  // ===========================================================================
  // Helpers for finding a shard split that makes two addresses cross-shard
  // ===========================================================================

  /** Scan `numShards` in [2, 64] for a value that places `addrA` and `addrB` in DIFFERENT shards. Same trick as
    * `ShardSubtreeProofServiceSuite.findNumShardsSplitting` — keeps the test independent of which specific shard each addr maps to.
    */
  private def findNumShardsSplitting(addrA: Address, addrB: Address)(implicit hasher: Hasher[IO]): IO[Int] = {
    def tryN(n: Int): IO[Boolean] = {
      val a = ShardAssignment.make[IO](n)
      for {
        sA <- a.shardIdFor(addrA)
        sB <- a.shardIdFor(addrB)
      } yield sA =!= sB
    }
    (2 to 64).toList
      .findM(tryN)
      .map(_.getOrElse(throw new AssertionError(s"Could not split $addrA and $addrB across any numShards in [2,64]")))
  }

  // ===========================================================================
  // Test 1: same-shard MUST NOT invoke proofClient
  // ===========================================================================

  test("same-shard SpendAction: proofClient never invoked — uses in-process state") { res =>
    implicit val (_, hs, sp) = res

    // numShards = 1 collapses every MG to shard 0 — the same-shard fast path covers every read
    // regardless of which MGs the SpendTransaction references. The proofClient should never be
    // called.
    for {
      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(callsRef, _ => IO.pure(None))
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPair2 <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      ammAddress = keyPair2.getPublic.toAddress

      allowSpend = AllowSpend(
        address,
        ammAddress,
        None,
        SwapAmount(1L),
        AllowSpendFee(1L),
        AllowSpendReference.empty,
        EpochProgress(20L),
        List(ammAddress)
      )
      signedAllowSpend <- Signed.forAsyncHasher(allowSpend, keyPair1)
      hashedAllowSpend <- signedAllowSpend.toHashed

      activeAllowSpends = SortedMap(none[Address] -> SortedMap(address -> SortedSet(signedAllowSpend)))
      userSpendTx = SpendTransaction(hashedAllowSpend.hash.some, None, SwapAmount(1L), address, ammAddress)
      metagraphSpendTx = SpendTransaction(none, None, SwapAmount(2L), ammAddress, ammAddress)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx, metagraphSpendTx))
      balances = Map(none[Address] -> SortedMap(ammAddress -> Balance(NonNegLong(1000L))))

      result <- validator.validate(spendAction, activeAllowSpends, balances, ammAddress)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isValid,
        // Critical assertion: same-shard reads MUST NOT consult the proof client. The validator's
        // same-shard fast path is the load-bearing optimisation — every call site that's not
        // cross-shard should be free of the network round-trip.
        calls.isEmpty
      )
  }

  // ===========================================================================
  // Test 2: cross-shard happy path — proofClient invoked, validator accepts
  // ===========================================================================

  test("cross-shard SpendAction: proofClient invoked, valid proof returned, validator accepts") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress

      // Pick numShards so that currentMg and targetMg hash to DIFFERENT shards. This forces the
      // cross-shard path in the validator.
      numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
      shardAssignment = ShardAssignment.make[IO](numShards)
      targetShardId <- shardAssignment.shardIdFor(targetMgAddr)

      // Build the proven SortedSet[Signed[AllowSpend]] that the proofClient will return as the
      // proof's value bytes. The validator decodes via Circe and scans for the matching hash.
      // The AllowSpend.currencyId must reference targetMg so the SpendTransaction's currencyId
      // check passes (`allowSpend.currencyId =!= spendTransaction.currencyId` else InvalidCurrency).
      targetCurrencyId = CurrencyId(targetMgAddr)
      allowSpend = AllowSpend(
        address,
        currentMgAddr,
        targetCurrencyId.some,
        SwapAmount(1L),
        AllowSpendFee(1L),
        AllowSpendReference.empty,
        EpochProgress(20L),
        List(currentMgAddr)
      )
      signedAllowSpend <- Signed.forAsyncHasher(allowSpend, keyPair1)
      hashedAllowSpend <- signedAllowSpend.toHashed
      provenValue = SortedSet(signedAllowSpend).asJson.noSpaces.getBytes("UTF-8")

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(
        callsRef,
        _ => IO.pure(Some((Some(provenValue), sentinelProof(Hex.fromBytes(provenValue).some))))
      )
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      // SpendTransaction.currencyId = Some(targetMg), source = address (the AllowSpend's source),
      // destination = currentMg (the AllowSpend's destination). The validator's currentMg
      // (parameter `currencyId`) is currentMgAddr — when validator sees
      // spendTransaction.currencyId = Some(targetMg) ≠ Some(currentMg), the classifyReference
      // path lands on Cross(targetMg, targetShard).
      userSpendTx =
        SpendTransaction(hashedAllowSpend.hash.some, targetCurrencyId.some, SwapAmount(1L), address, currentMgAddr)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx))

      // The in-process maps for currentMgAddr's shard do NOT carry the targetMg's slice — the
      // cross-shard fetch is what supplies the AllowSpend.
      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isValid,
        calls.length === 1,
        calls.head.targetShardId === targetShardId,
        calls.head.metagraphAddress === targetMgAddr,
        // Validator queries the (MG, source) slot in the ActiveAllowSpends partition.
        calls.head.key === GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveAllowSpends, targetMgAddr, address)
      )
  }

  // ===========================================================================
  // Test 3: cross-shard proof unavailable — validator rejects for retry
  // ===========================================================================

  test("cross-shard SpendAction: proofClient returns None, validator rejects with CrossShardProofUnavailable") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress

      numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
      shardAssignment = ShardAssignment.make[IO](numShards)

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      // proofClient returns None — simulates target shard offline / network failure.
      proofClient = mkMockClient(callsRef, _ => IO.pure(None))
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      targetCurrencyId = CurrencyId(targetMgAddr)
      userSpendTx =
        SpendTransaction(Hash("aa" * 32).some, targetCurrencyId.some, SwapAmount(1L), address, currentMgAddr)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx))

      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isInvalid,
        result.toEither.left.exists(_.exists {
          case CrossShardProofUnavailable(_) => true
          case _                             => false
        }),
        calls.length === 1
      )
  }

  // ===========================================================================
  // Test 4: cross-shard tampered value — validator rejects with CrossShardProofTampered
  // ===========================================================================

  test("cross-shard SpendAction: proofClient returns proof with undecodable value, validator rejects with CrossShardProofTampered") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress

      numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
      shardAssignment = ShardAssignment.make[IO](numShards)

      // Garbage bytes — definitely not a valid JSON-encoded SortedSet[Signed[AllowSpend]]. The
      // validator's Circe decode step rejects and surfaces CrossShardProofTampered. Stands in
      // for a malicious peer that returns structurally-valid proof bytes but the wrong type
      // (or just random bytes). A peer that returned bytes for, e.g., a `Balance` instead of
      // an `AllowSpends` set would land in the same rejection: Circe decode fails because the
      // expected shape is a JSON array of signed AllowSpends.
      garbageBytes = "this is not valid AllowSpend JSON".getBytes("UTF-8")

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(
        callsRef,
        _ => IO.pure(Some((Some(garbageBytes), sentinelProof(Hex.fromBytes(garbageBytes).some))))
      )
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      targetCurrencyId = CurrencyId(targetMgAddr)
      userSpendTx =
        SpendTransaction(Hash("bb" * 32).some, targetCurrencyId.some, SwapAmount(1L), address, currentMgAddr)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx))

      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isInvalid,
        result.toEither.left.exists(_.exists {
          case CrossShardProofTampered(_) => true
          case _                          => false
        }),
        calls.length === 1
      )
  }

  // ===========================================================================
  // Test 5: cross-shard proof returns non-membership — AllowSpendNotFound
  // ===========================================================================

  test("cross-shard SpendAction: proofClient returns Some((None, _)), validator rejects with AllowSpendNotFound") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress

      numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
      shardAssignment = ShardAssignment.make[IO](numShards)

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      // Some((None, proof)) — proof of non-membership. Validator surfaces this as
      // AllowSpendNotFound (the AllowSpend isn't present at the anchor checkpoint).
      proofClient = mkMockClient(callsRef, _ => IO.pure(Some((none[Array[Byte]], sentinelProof(none[Hex])))))
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      targetCurrencyId = CurrencyId(targetMgAddr)
      userSpendTx =
        SpendTransaction(Hash("cc" * 32).some, targetCurrencyId.some, SwapAmount(1L), address, currentMgAddr)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx))

      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isInvalid,
        result.toEither.left.exists(_.exists {
          case AllowSpendNotFound(_) => true
          case _                     => false
        }),
        calls.length === 1
      )
  }

  // ===========================================================================
  // Test 6: cross-shard balance proof unavailable (no-allowSpendRef branch)
  // ===========================================================================

  test("cross-shard balance lookup (no allowSpendRef): proofClient None ⇒ CrossShardProofUnavailable") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairTargetMg <- KeyPairGenerator.makeKeyPair[IO]
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      targetMgAddr = keyPairTargetMg.getPublic.toAddress

      numShards <- findNumShardsSplitting(currentMgAddr, targetMgAddr)
      shardAssignment = ShardAssignment.make[IO](numShards)
      targetShardId <- shardAssignment.shardIdFor(targetMgAddr)

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(callsRef, _ => IO.pure(None))
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      targetCurrencyId = CurrencyId(targetMgAddr)
      // No allowSpendRef ⇒ balance-check branch. SpendTransaction is metagraph-emitted, so source
      // = currentMgAddr (the validator's `currencyId`); destination is the recipient.
      metagraphSpendTx =
        SpendTransaction(none, targetCurrencyId.some, SwapAmount(5L), currentMgAddr, targetMgAddr)
      spendAction = SpendAction(NonEmptyList.of(metagraphSpendTx))

      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isInvalid,
        result.toEither.left.exists(_.exists {
          case CrossShardProofUnavailable(_) => true
          case _                             => false
        }),
        calls.length === 1,
        calls.head.targetShardId === targetShardId,
        calls.head.metagraphAddress === targetMgAddr,
        // The balance-check branch queries the (MG, currencyId) slot in the Balances partition,
        // where `currencyId` is the validator's current MG.
        calls.head.key === GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, targetMgAddr, currentMgAddr)
      )
  }

  // ===========================================================================
  // Test 7: local fallback — numShards=1 collapses everything to shard 0; proofClient never invoked
  //          even for SpendActions referencing "other" MGs (because all MGs are now same-shard).
  // ===========================================================================

  test("local fallback (numShards=1): proofClient never invoked even for cross-MG SpendAction references") { res =>
    implicit val (_, hs, sp) = res

    for {
      keyPair1 <- KeyPairGenerator.makeKeyPair[IO]
      keyPairCurrentMg <- KeyPairGenerator.makeKeyPair[IO]
      keyPairOtherMg <- KeyPairGenerator.makeKeyPair[IO]
      address = keyPair1.getPublic.toAddress
      currentMgAddr = keyPairCurrentMg.getPublic.toAddress
      otherMgAddr = keyPairOtherMg.getPublic.toAddress

      // numShards = 1: all MGs hash to shard 0. The validator's same-shard fast path covers
      // every read — proofClient must NEVER be invoked.
      shardAssignment = ShardAssignment.make[IO](numShards = 1)

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(callsRef, _ => IO.pure(None))
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      // Build an AllowSpend keyed under "otherMgAddr" — different MG than the validator's
      // currentMgAddr, but in numShards=1 they're both shard 0 so the in-process map carries
      // the otherMgAddr slice and no cross-shard fetch is needed.
      otherCurrencyId = CurrencyId(otherMgAddr)
      allowSpend = AllowSpend(
        address,
        currentMgAddr,
        otherCurrencyId.some,
        SwapAmount(1L),
        AllowSpendFee(1L),
        AllowSpendReference.empty,
        EpochProgress(20L),
        List(currentMgAddr)
      )
      signedAllowSpend <- Signed.forAsyncHasher(allowSpend, keyPair1)
      hashedAllowSpend <- signedAllowSpend.toHashed

      activeAllowSpends = SortedMap(otherMgAddr.some -> SortedMap(address -> SortedSet(signedAllowSpend)))
      userSpendTx =
        SpendTransaction(hashedAllowSpend.hash.some, otherCurrencyId.some, SwapAmount(1L), address, currentMgAddr)
      spendAction = SpendAction(NonEmptyList.of(userSpendTx))
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      result <- validator.validate(spendAction, activeAllowSpends, balances, currentMgAddr)
      calls <- callsRef.get
    } yield
      expect.all(
        result.isValid,
        // numShards=1 ⇒ every MG is same-shard ⇒ proofClient is unreachable.
        calls.isEmpty
      )
  }

  // ===========================================================================
  // Test 8 (W3c FORCING FUNCTION): cross-shard balance-spend of a PHANTOM expiry-refund is
  //   REJECTED under the EFFECTIVE-balance overlay but ACCEPTED under the raw attested balance —
  //   proving the overlay is load-bearing on the CROSS-SHARD path (mirrors the same-shard forcing
  //   function in ConsumedAllowSpendSettlementSuite, routed through validateBalanceCrossShard).
  // ===========================================================================

  /** Refined helper — refined macros need literals, so build from `Long`. */
  private def swapAmt(v: Long): SwapAmount = SwapAmount(eu.timepit.refined.types.numeric.PosLong.unsafeFrom(v))

  test(
    "cross-shard balance-spend of a phantom expiry-refund: REJECTED under effective overlay, ACCEPTED under raw attested (W3c forcing function)"
  ) { res =>
    implicit val (_, hs, sp) = res

    for {
      // M = owner metagraph (holds the reserved funds + autonomously refunds on expiry); the cross-shard
      // balance proof is fetched from M's shard. M′ = currentMg = the spender AND the source of the
      // consumed-and-expired allow-spend (so its `Balances[M][M′]` entry is the one the refund inflated).
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      mPrimeKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress // owner metagraph M (= targetMg of the cross-shard fetch)
      mPrime = mPrimeKp.getPublic.toAddress // spender M′ (= validator's currentMg, also the marker source)
      dest = destKp.getPublic.toAddress

      // Split M and M′ across different shards ⇒ the no-allowSpendRef balance check routes through
      // validateBalanceCrossShard (the proof path) rather than the in-process same-shard map.
      numShards <- findNumShardsSplitting(m, mPrime)
      shardAssignment = ShardAssignment.make[IO](numShards)
      targetShardId <- shardAssignment.shardIdFor(m)

      // The gl0-finalized ConsumedAllowSpends spent-set: M′'s earlier cross-shard consume of an
      // allow-spend on M (X=100, expiry E=200), now EXPIRED (pinned/live epoch 250 > 200) so M has
      // refunded M′ a phantom +X. Marker.currencyId = Some(M) (owner scope), marker.source = M′.
      x = 100L
      expiry = 200L
      asHash = Hash("ab".padTo(64, '0').take(64))
      spentSet: SortedMap[Hash, ConsumedAllowSpend] = SortedMap(
        asHash -> ConsumedAllowSpend(
          allowSpendHash = asHash,
          source = mPrime,
          destination = dest,
          currencyId = CurrencyId(m).some,
          amount = swapAmt(x),
          lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(expiry)),
          consumedAtOrdinal = SnapshotOrdinal(NonNegLong(1L)),
          consumingSpendRef = asHash
        )
      )

      // M's ATTESTED per-MG balance for M′ AFTER the refund = exactly X (phantom): the only thing that
      // could fund the self-spend is the refund. Real spendable = X − X = 0.
      provenAttestedBalance = Balance(NonNegLong.unsafeFrom(x))
      provenValue = provenAttestedBalance.asJson.noSpaces.getBytes("UTF-8")

      // The overlay closure REUSES ConsumedAllowSpendStateManager.effectiveCurrencyBalances verbatim,
      // closing over the finalized spent-set + the (post-expiry) epoch. Empty pinned map ⇒ epochFor(M)
      // = liveEpoch = 250 > 200 ⇒ the marker's source-debit fires ⇒ M′'s effective balance = 0.
      mgr = ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.empty[IO])
      effectiveOverlay: SpendActionValidator.CrossShardEffectiveBalanceOverlay =
        (attested, scope) =>
          mgr.effectiveCurrencyBalances(
            attested,
            scope,
            spentSet,
            Map(m -> SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))),
            Map.empty[Address, EpochProgress],
            EpochProgress(NonNegLong.unsafeFrom(250L))
          )

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(
        callsRef,
        _ => IO.pure(Some((Some(provenValue), sentinelProof(Hex.fromBytes(provenValue).some))))
      )
      // Two validators: one with the effective overlay (W3c), one with the IDENTITY default (the raw path).
      effectiveValidator = SpendActionValidator.make[IO](proofClient, shardAssignment, effectiveOverlay)
      rawValidator = SpendActionValidator.make[IO](proofClient, shardAssignment)

      // A no-allowSpendRef self-spend by M′ of X against currency M: source = M′ (= validator's
      // currencyId/currentMg), currencyId = Some(M) ⇒ Cross(M, mShard) ⇒ balance fetched from M's shard.
      selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swapAmt(x), mPrime, dest)
      spendAction = SpendAction(NonEmptyList.of(selfSpend))
      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      balances = Map.empty[Option[Address], SortedMap[Address, Balance]]

      resEffective <- effectiveValidator.validate(spendAction, activeAllowSpends, balances, mPrime)
      effectiveCalls <- callsRef.get
      _ <- callsRef.set(List.empty)
      resRaw <- rawValidator.validate(spendAction, activeAllowSpends, balances, mPrime)
    } yield
      expect.all(
        // The cross-shard fetch DID happen, against M's shard + the (M, M′) Balances key.
        effectiveCalls.length === 1,
        effectiveCalls.head.targetShardId === targetShardId,
        effectiveCalls.head.metagraphAddress === m,
        effectiveCalls.head.key === GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, m, mPrime),
        // Under the EFFECTIVE overlay (M′ effective balance = X − X = 0) ⇒ REJECTED for insufficient balance.
        resEffective.isInvalid,
        resEffective.toEither.left.exists(_.exists {
          case NotEnoughCurrencyIdBalance(_) => true
          case _                             => false
        }),
        // …but ACCEPTED under the raw attested (phantom-refunded) balance ⇒ the overlay is load-bearing
        // on the cross-shard path. (Same forcing-function shape as the same-shard ConsumedAllowSpendSettlementSuite.)
        resRaw.isValid
      )
  }

  // ===========================================================================
  // Test 9 (W3c numShards=1 IDENTITY): the cross-shard balance overlay seam is unreachable at
  //   numShards=1 — proofClient is never invoked and the result matches the same spend validated
  //   against the in-process attested balance, even when an overlay is wired. Byte-identical.
  // ===========================================================================

  test("numShards=1: cross-shard balance overlay is unreachable ⇒ behaviour byte-identical to the in-process attested path") { res =>
    implicit val (_, hs, sp) = res

    for {
      mKp <- KeyPairGenerator.makeKeyPair[IO]
      destKp <- KeyPairGenerator.makeKeyPair[IO]
      m = mKp.getPublic.toAddress
      dest = destKp.getPublic.toAddress

      // numShards=1 ⇒ every MG hashes to shard 0 ⇒ the no-allowSpendRef balance check uses the SAME-SHARD
      // in-process map and the cross-shard overlay seam is never consulted.
      shardAssignment = ShardAssignment.make[IO](numShards = 1)

      x = 100L
      // An overlay that WOULD debit if reached (marker source = m), proving it is NOT reached at numShards=1.
      asHash = Hash("cd".padTo(64, '0').take(64))
      spentSet: SortedMap[Hash, ConsumedAllowSpend] = SortedMap(
        asHash -> ConsumedAllowSpend(
          allowSpendHash = asHash,
          source = m,
          destination = dest,
          currencyId = CurrencyId(m).some,
          amount = swapAmt(x),
          lastValidEpochProgress = EpochProgress(NonNegLong.unsafeFrom(200L)),
          consumedAtOrdinal = SnapshotOrdinal(NonNegLong(1L)),
          consumingSpendRef = asHash
        )
      )
      mgr = ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.empty[IO])
      neverReachedOverlay: SpendActionValidator.CrossShardEffectiveBalanceOverlay =
        (attested, scope) =>
          mgr.effectiveCurrencyBalances(
            attested,
            scope,
            spentSet,
            Map(m -> SortedSet(SnapshotOrdinal(NonNegLong.unsafeFrom(1L)))),
            Map.empty[Address, EpochProgress],
            EpochProgress(NonNegLong.unsafeFrom(250L))
          )

      callsRef <- Ref.of[IO, List[FetchCall]](List.empty)
      proofClient = mkMockClient(callsRef, _ => IO.pure(None))
      validator = SpendActionValidator.make[IO](proofClient, shardAssignment, neverReachedOverlay)

      // Source = m (the validator's currencyId), currencyId = Some(m) (same MG) ⇒ Same ⇒ in-process map.
      // Attested in-process balance = X ⇒ the self-spend of X is ACCEPTED (the overlay, if it had been
      // reached, would have debited it to 0 and rejected — but it is NOT reached at numShards=1).
      selfSpend = SpendTransaction(none[Hash], CurrencyId(m).some, swapAmt(x), m, dest)
      spendAction = SpendAction(NonEmptyList.of(selfSpend))
      activeAllowSpends = SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      inProcessBalances = Map(m.some -> (SortedMap(m -> Balance(NonNegLong.unsafeFrom(x))): SortedMap[Address, Balance]))

      result <- validator.validate(spendAction, activeAllowSpends, inProcessBalances, m)
      calls <- callsRef.get
    } yield
      expect.all(
        // proofClient never consulted ⇒ the cross-shard overlay seam is dead at numShards=1.
        calls.isEmpty,
        // Accepted off the in-process attested balance, unaffected by the (unreached) overlay.
        result.isValid
      )
  }
}
