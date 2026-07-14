package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.currency.schema.currency._
import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.height.{Height, SubHeight}
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.MerklePatriciaCommitment
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardSubtreeProofService]] — Slice 10 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.5, updated for the
  * PIN-1 COMPONENT-ADDRESSABLE per-MG root.
  *
  * '''PIN-1 change.''' `perMetagraphMptRoots(mg)` is now the `rootHash` of the per-MG SUB-TRIE built from
  * `GlobalStateConverter.currencySnapshotMgEntries(mg -> state)` (fieldId-5 incremental + the `infoSubFields` `Mg*` entries, one leaf per
  * account) — a real MPT root, NOT the old flat `hash((incrementalRoot, infoRoot))`. The service builds the inclusion proof over the SAME
  * sub-trie (via the `perMgEntriesFor` callback), so the proof's witness chain terminates at the committed root and `verifyProof` confirms
  * a SINGLE `(field, account)` leaf against it. These tests drive that end-to-end with a real `CurrencySnapshotInfo` carrying balance
  * entries and prove a per-account `MgBalances` leaf.
  *
  * '''Coverage''':
  *   1. '''Round-trip happy''': generate proof for a single account's `MgBalances` leaf (out of several); verifyProof returns true (the
  *      component-addressable witness terminates at the committed PIN-1 root).
  *   1. '''Verify rejects wrong checkpoint hash''': tamper the proof's `shardCheckpointHash`; verify returns false.
  *   1. '''Verify rejects wrong root''': tamper the proof's `perMgMptRoot`; verify returns false.
  *   1. '''Verify binds returned bytes''': substituted or missing value bytes reject even when the root witness itself is unchanged.
  *   1. '''Verify binds structured key to path''': substituting either the structured key or the proof path rejects.
  *   1. '''Verify requires one terminal leaf''': missing and duplicate leaf commitments reject.
  *   1. '''Non-membership''': generate proof for an account NOT in the MG state; generate returns None (v1 surfaces absence as None).
  *   1. '''Shard ownership mismatch''': request proof from shard 0 for an MG owned by a different shard; generate returns None.
  *
  * '''Fixture strategy''':
  *   - Real `currencySnapshotMgEntries` / `currencySnapshotMgRoot` — the producer/follower byte path is exercised end-to-end; the proof is
  *     generated over a trie built from those EXACT bytes, so any drift would fail the round-trip (not a hand-rolled root).
  *   - Real [[ShardAssignment]] over a configurable `numShards` (ownership tests split MG addresses across shards).
  *   - `perMgEntriesFor` supplied directly from `currencySnapshotMgEntries` (production wires this off a `GlobalStateReader`); decoupled so
  *     the suite needs no MPT store.
  */
object ShardSubtreeProofServiceSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // The state-proof selector must be active (low ordinal) so currencySnapshotMgEntries includes the unrolled `Mg*` fields.
  implicit val stateProofSelector: StateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(0L)))

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  /** Deterministic Address derived from a label — same shape as `ShardChainStoreSuite` / `HistoricalMptProofServiceSuite` use, so the
    * per-MG addresses are stable across test runs.
    */
  private def addr(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  /** The MPT key for a single account's `MgBalances` entry under metagraph `mg` — exactly how `GlobalStateConverter.infoEntryBytes` keys a
    * balance, so this key resolves to a leaf in the per-MG sub-trie that `perMetagraphMptRoots(mg)` commits to.
    */
  private def mgBalanceKey(mg: Address, account: Address): GlobalStateKey =
    GlobalStateKey.metagraphEntry(mg, GlobalStateFieldId.MgBalances, account)

  /** Standard test ordinal. */
  private val testOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  /** Single sentinel signature — the verify path doesn't check outer-envelope signatures; that's the gossip layer's job. The test only
    * exercises the MPT proof + per-MG-root cross-check.
    */
  private def sentinelProof: SignatureProof =
    SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  /** Build a sentinel [[CommitteeMemberSignature]] — verify path doesn't inspect its contents either. */
  private def sentinelCommitteeSig: CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex("aa" * 64)),
      vrfProof = Hex("bb" * 80),
      ed25519Sig = Hex("cc" * 64),
      kesProductSig = Hex("dd" * 128),
      kesTreeStep = 0
    )

  /** A `Signed[CurrencyIncrementalSnapshot]` (sentinel proof — only its bytes feed the fieldId-5 incremental leaf; signature validity is
    * irrelevant to the proof/verify path).
    */
  private def mkSignedIncremental(snapOrdinal: Long): Signed[CurrencyIncrementalSnapshot] = {
    val proof = SignatureProof(Id(Hex("33" * 64)), Signature(Hex("44" * 70)))
    val snap = CurrencyIncrementalSnapshot(
      ordinal = SnapshotOrdinal.unsafeApply(snapOrdinal),
      height = Height.MinValue,
      subHeight = SubHeight.MinValue,
      lastSnapshotHash = Hash.empty,
      blocks = SortedSet.empty,
      rewards = SortedSet.empty,
      tips = SnapshotTips(SortedSet.empty, SortedSet.empty),
      stateProof = CurrencySnapshotStateProof(Hash.empty, Hash.empty, None, None, None, None, None, None, None),
      epochProgress = EpochProgress.MinValue,
      dataApplication = None,
      messages = None,
      globalSnapshotSyncs = None,
      feeTransactions = None,
      artifacts = None,
      allowSpendBlocks = None,
      tokenLockBlocks = None,
      globalSyncView = None
    )
    Signed(snap, NonEmptySet.of(proof))
  }

  /** A `CurrencySnapshotInfo` carrying exactly the given balance entries (other sub-maps `.some`-empty, mirroring
    * `reconstructCurrencyInfoFrom`'s always-`.some` shape).
    */
  private def infoWithBalances(balances: (Address, Long)*): CurrencySnapshotInfo =
    CurrencySnapshotInfo(
      lastTxRefs = SortedMap.empty,
      balances = SortedMap.from(balances.map { case (a, v) => a -> Balance(NonNegLong.unsafeFrom(v)) }),
      lastMessages = None,
      lastFeeTxRefs = None,
      lastAllowSpendRefs = None,
      activeAllowSpends = None,
      globalSnapshotSyncView = None,
      lastTokenLockRefs = None,
      activeTokenLocks = None
    )

  /** Construct a [[ShardCheckpoint]] with a single MG in `perMetagraphMptRoots`. */
  private def mkCheckpoint(
    shardId: ShardId,
    perMgRoots: SortedMap[Address, Hash]
  ): ShardCheckpoint =
    ShardCheckpoint(
      shardId = shardId,
      parentCheckpointHash = Hash.empty,
      shardOrdinal = ShardOrdinal(0L),
      gl0AnchorOrdinal = testOrdinal,
      slot = SlotT.unsafeApply(testOrdinal.value.value),
      derivedStateDelta = ShardDerivedStateDelta(
        perMetagraphMptRoots = perMgRoots,
        includedSnapshots = SortedMap.empty
      ),
      committeeSignatures = NonEmptyList.of(sentinelCommitteeSig),
      epoch = EtaPeriod(0L)
    )

  /** Wrap a value in a [[Signed]] envelope with the sentinel proof. */
  private def mkSigned[A](value: A): Signed[A] = Signed(value, NonEmptySet.of(sentinelProof))

  /** Compute the committed PIN-1 per-MG root + the proof sub-trie entries for `mg` over `info`, both via the SAME `GlobalStateConverter`
    * helpers production uses — byte-identity by construction.
    */
  private def rootAndEntries(
    mg: Address,
    inc: Signed[CurrencyIncrementalSnapshot],
    info: CurrencySnapshotInfo
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[(Hash, Map[Hex, Array[Byte]])] = {
    val state: SortedMap[Address, Either[Signed[CurrencySnapshot], (Signed[CurrencyIncrementalSnapshot], CurrencySnapshotInfo)]] =
      SortedMap(mg -> Right((inc, info)))
    (
      GlobalStateConverter.currencySnapshotMgRoot[IO](state),
      GlobalStateConverter.currencySnapshotMgEntries[IO](state)
    ).tupled
  }

  /** Construct a [[ShardSubtreeProofService.ShardCheckpointLookup]] that always returns the same pre-built checkpoint for the matching
    * shard. Returns `None` for any other shard.
    */
  private def fixedLookup(
    expectedShard: ShardId,
    checkpoint: Signed[ShardCheckpoint]
  ): ShardSubtreeProofService.ShardCheckpointLookup[IO] =
    (s: ShardId) => if (s === expectedShard) checkpoint.some.pure[IO] else (none[Signed[ShardCheckpoint]]).pure[IO]

  /** A `perMgEntriesFor` that returns the supplied sub-trie entries for `mg`, `None` for any other MG. */
  private def fixedEntries(mg: Address, entries: Map[Hex, Array[Byte]]): ShardSubtreeProofService.PerMgEntriesLookup[IO] =
    (a: Address) => if (a === mg) entries.some.pure[IO] else none[Map[Hex, Array[Byte]]].pure[IO]

  /** Find a numShards value such that two given addresses map to DIFFERENT shards. */
  private def findNumShardsSplitting(
    addrA: Address,
    addrB: Address
  )(implicit hasher: Hasher[IO]): IO[Int] = {
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
  // Test 1: round-trip happy
  // ===========================================================================

  test("round-trip: generate proof for a single MgBalances account leaf ⇒ verifyProof returns true (component-addressable PIN-1 root)") {
    res =>
      implicit val (h, _, js) = res
      // numShards=1 → every MG maps to shard 0, no ownership concern in the happy path.
      val mg = addr("mg-happy-path")
      val account = addr("user-happy-path-1")
      val inc = mkSignedIncremental(5L)
      // Two balance accounts so the proof witnesses ONE leaf out of several — proves single-account addressability.
      val info = infoWithBalances(account -> 100L, addr("user-happy-path-2") -> 200L)
      for {
        re <- rootAndEntries(mg, inc, info)
        (perMgRoot, entries) = re

        shardId = ShardId.unsafeApply(0)
        shardAssignment = ShardAssignment.make[IO](numShards = 1)
        checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
        svc = ShardSubtreeProofService.make[IO](
          shardAssignment,
          fixedLookup(shardId, checkpoint),
          fixedEntries(mg, entries)
        )

        key = mgBalanceKey(mg, account)
        keyHex <- GlobalStateKey.toHex[IO](key)
        proofOpt <- svc.generateProofForMetagraph(shardId, mg, key)
        verifyResult <- proofOpt match {
          case Some(p) => svc.verifyProof(checkpoint, p)
          case None    => IO.pure(false)
        }
      } yield
        expect.all(
          proofOpt.isDefined,
          proofOpt.flatMap(_.value).exists(_.toBytes.sameElements(entries(keyHex))),
          verifyResult
        )
  }

  // ===========================================================================
  // Test 2: verify rejects tampered checkpoint hash
  // ===========================================================================

  test("verify rejects tampered shardCheckpointHash") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-tamper-ckhash")
    val account = addr("user-tamper-ckhash")
    val inc = mkSignedIncremental(2L)
    val info = infoWithBalances(account -> 42L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      proofOpt <- svc.generateProofForMetagraph(shardId, mg, mgBalanceKey(mg, account))
      tampered = proofOpt.get.copy(shardCheckpointHash = Hash("ff" * 32))
      verifyResult <- svc.verifyProof(checkpoint, tampered)
    } yield expect.all(proofOpt.isDefined, !verifyResult)
  }

  // ===========================================================================
  // Test 3: verify rejects tampered per-MG MPT root
  // ===========================================================================

  test("verify rejects tampered perMgMptRoot") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-tamper-root")
    val account = addr("user-tamper-root")
    val inc = mkSignedIncremental(3L)
    val info = infoWithBalances(account -> 7L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      proofOpt <- svc.generateProofForMetagraph(shardId, mg, mgBalanceKey(mg, account))
      tampered = proofOpt.get.copy(perMgMptRoot = Hash("00" * 32))
      verifyResult <- svc.verifyProof(checkpoint, tampered)
    } yield expect.all(proofOpt.isDefined, !verifyResult)
  }

  // ===========================================================================
  // Test 4: verify binds the returned value bytes to the terminal leaf digest
  // ===========================================================================

  test("verify rejects substituted value bytes while the authenticated witness is unchanged") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-tamper-value")
    val account = addr("user-tamper-value")
    val inc = mkSignedIncremental(4L)
    val info = infoWithBalances(account -> 123L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      proofOpt <- svc.generateProofForMetagraph(shardId, mg, mgBalanceKey(mg, account))
      tampered = proofOpt.get.copy(value = Hex.fromBytes("substituted-value".getBytes("UTF-8")).some)
      verifyResult <- svc.verifyProof(checkpoint, tampered)
    } yield expect.all(proofOpt.isDefined, !verifyResult)
  }

  // ===========================================================================
  // Test 5: verify binds the structured key to the canonical proof path
  // ===========================================================================

  test("verify rejects structured-key and proof-path substitution") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-tamper-key-path")
    val account = addr("user-tamper-key-path")
    val substitutedAccount = addr("user-substituted-key-path")
    val inc = mkSignedIncremental(5L)
    val info = infoWithBalances(account -> 123L, substitutedAccount -> 456L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      requestedKey = mgBalanceKey(mg, account)
      substitutedKey = mgBalanceKey(mg, substitutedAccount)
      substitutedPath <- GlobalStateKey.toHex[IO](substitutedKey)
      proofOpt <- svc.generateProofForMetagraph(shardId, mg, requestedKey)
      proof = proofOpt.get
      keySubstitutionResult <- svc.verifyProof(checkpoint, proof.copy(key = substitutedKey))
      pathSubstitutionResult <- svc.verifyProof(checkpoint, proof.copy(mptProof = proof.mptProof.copy(path = substitutedPath)))
    } yield
      expect.all(
        proofOpt.isDefined,
        !keySubstitutionResult,
        !pathSubstitutionResult
      )
  }

  // ===========================================================================
  // Test 6: membership proofs require value bytes
  // ===========================================================================

  test("verify rejects a membership proof with no value bytes") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-missing-value")
    val account = addr("user-missing-value")
    val inc = mkSignedIncremental(6L)
    val info = infoWithBalances(account -> 123L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      proofOpt <- svc.generateProofForMetagraph(shardId, mg, mgBalanceKey(mg, account))
      verifyResult <- svc.verifyProof(checkpoint, proofOpt.get.copy(value = None))
    } yield expect.all(proofOpt.isDefined, !verifyResult)
  }

  // ===========================================================================
  // Test 7: membership proofs require exactly one terminal leaf
  // ===========================================================================

  test("verify rejects missing and duplicate terminal leaf commitments") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-invalid-leaf-witness")
    val account = addr("user-invalid-leaf-witness")
    val inc = mkSignedIncremental(7L)
    val info = infoWithBalances(account -> 123L, addr("user-invalid-leaf-witness-2") -> 456L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      proofOpt <- svc.generateProofForMetagraph(shardId, mg, mgBalanceKey(mg, account))
      proof = proofOpt.get
      leaves = proof.mptProof.witness.collect { case leaf: MerklePatriciaCommitment.Leaf => leaf }
      missingLeaf = proof.copy(
        mptProof = proof.mptProof.copy(witness = proof.mptProof.witness.filterNot(_.isInstanceOf[MerklePatriciaCommitment.Leaf]))
      )
      duplicateLeaf = proof.copy(mptProof = proof.mptProof.copy(witness = leaves.head :: proof.mptProof.witness))
      missingLeafResult <- svc.verifyProof(checkpoint, missingLeaf)
      duplicateLeafResult <- svc.verifyProof(checkpoint, duplicateLeaf)
    } yield
      expect.all(
        proofOpt.isDefined,
        leaves.size === 1,
        !missingLeafResult,
        !duplicateLeafResult
      )
  }

  // ===========================================================================
  // Test 8: non-membership — generate returns None for an account not in the MG state
  // ===========================================================================

  test("non-membership: generate returns None for an account NOT in the MG state (v1 surfaces absence as None)") { res =>
    implicit val (h, _, js) = res
    val mg = addr("mg-non-member")
    val presentAccount = addr("user-present")
    val inc = mkSignedIncremental(6L)
    val info = infoWithBalances(presentAccount -> 1L)
    for {
      re <- rootAndEntries(mg, inc, info)
      (perMgRoot, entries) = re

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      svc = ShardSubtreeProofService.make[IO](shardAssignment, fixedLookup(shardId, checkpoint), fixedEntries(mg, entries))

      // Ask for a DIFFERENT account that is not in the MG's balances — the prover returns PathNotFound ⇒ service returns None.
      absentKey = mgBalanceKey(mg, addr("user-not-in-state"))
      proofOpt <- svc.generateProofForMetagraph(shardId, mg, absentKey)
    } yield expect(proofOpt.isEmpty)
  }

  // ===========================================================================
  // Test 9: shard ownership mismatch
  // ===========================================================================

  test("shard ownership mismatch: request proof from shard 0 for an MG that hashes to a different shard ⇒ None") { res =>
    implicit val (h, _, js) = res
    val mgA = addr("mg-owner-shard-0-anchor")
    val mgB = addr("mg-owner-not-shard-0-anchor")
    for {
      numShards <- findNumShardsSplitting(mgA, mgB)
      shardAssignment = ShardAssignment.make[IO](numShards)
      shardOfA <- shardAssignment.shardIdFor(mgA)
      shardOfB <- shardAssignment.shardIdFor(mgB)

      shardZero = ShardId.unsafeApply(0)
      foreignMg = if (shardOfA === shardZero) mgB else mgA
      foreignShard = if (shardOfA === shardZero) shardOfB else shardOfA
      key = mgBalanceKey(foreignMg, addr("user-foreign-mg"))

      // The foreign MG doesn't belong to shard 0, so the service short-circuits before consulting the lookup/entries.
      checkpoint = mkSigned(mkCheckpoint(shardZero, SortedMap.empty))
      svc = ShardSubtreeProofService.make[IO](
        shardAssignment,
        fixedLookup(shardZero, checkpoint),
        (_: Address) => none[Map[Hex, Array[Byte]]].pure[IO]
      )

      proofOpt <- svc.generateProofForMetagraph(shardZero, foreignMg, key)
    } yield
      expect.all(
        foreignShard =!= shardZero,
        proofOpt.isEmpty
      )
  }
}
