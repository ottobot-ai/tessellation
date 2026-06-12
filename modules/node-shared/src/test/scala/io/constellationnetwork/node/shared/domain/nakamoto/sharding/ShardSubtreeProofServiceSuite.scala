package io.constellationnetwork.node.shared.domain.nakamoto.sharding

import cats.data.{NonEmptyList, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.SortedMap

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.{BranchId, MptOverlay}
import io.constellationnetwork.node.shared.domain.nakamoto.{HistoricalMptProofService, ParentChildTree, ShardAssignment}
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.SnapshotOrdinal
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.mpt.{GlobalStateFieldId, GlobalStateKey, MptStore}
import io.constellationnetwork.schema.nakamoto.EtaPeriod
import io.constellationnetwork.schema.nakamoto.slot.{Slot => SlotT}
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed
import io.constellationnetwork.security.signature.signature.{Signature, SignatureProof}
import io.constellationnetwork.serde.codecs.instances.NewtypeLongShapes._

import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Tests for [[ShardSubtreeProofService]] — Slice 10 of `docs/nakamoto/HIERARCHICAL-SHARD-CHECKPOINTS-DESIGN.md` §8.5.
  *
  * '''Coverage''' (per slice 10 task spec):
  *   1. '''Round-trip happy''': generate proof for (shardId, mgAddr, key); verifyProof returns true.
  *   1. '''Verify rejects wrong checkpoint hash''': tamper with the proof's checkpointHash; verify returns false.
  *   1. '''Verify rejects wrong root''': tamper with the proof's perMgMptRoot; verify returns false.
  *   1. '''Verify rejects mismatched value''': tamper with the value bytes via the embedded MPT proof leaf commitment; verify returns
  *      false.
  *   1. '''Non-membership''': generate proof for a key NOT in the MPT; generate returns None. The v1 underlying
  *      [[io.constellationnetwork.security.mpt.verifier.MerklePatriciaInclusionVerifier]] has no absence-witness support, so v1 only ships
  *      membership proofs and surfaces absence by [[ShardSubtreeProofService.generateProofForMetagraph]] returning None (per the service's
  *      scaladoc).
  *   1. '''Shard ownership mismatch''': request proof from shard 0 for an MG that hashes to a different shard; `generateProofForMetagraph`
  *      returns None.
  *
  * '''Fixture strategy''' (per slice 10 task spec):
  *   - Real [[HistoricalMptProofService]] over an in-memory MPT producer — the proof generation path is exercised end-to-end. No mocking on
  *     the prover/verifier side; tampering tests use real cryptographic primitives.
  *   - Real [[ShardAssignment]] over a configurable `numShards`. Shard-ownership tests pick a `numShards` that splits the test MG addresses
  *     across multiple shards.
  *   - Stubbed `lookupShardCheckpoint`: a `Ref`-backed test fixture that returns a pre-constructed [[ShardCheckpoint]] for the matching
  *     shard. Decoupled from [[ShardChainStore]] so this slice doesn't pull that dependency into its tests.
  */
object ShardSubtreeProofServiceSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // ===========================================================================
  // Fixtures
  // ===========================================================================

  /** Deterministic Address derived from a label — same shape as `ShardChainStoreSuite` / `HistoricalMptProofServiceSuite` use, so the
    * per-MG addresses are stable across test runs.
    */
  private def addr(label: String): Address =
    Address.fromBytes(label.getBytes("UTF-8"))

  /** Key under the user-keyed `Balances` partition for a given holder address. Picked because `Balances` is one of the simpler partitions
    * to seed and the existing `HistoricalMptProofServiceSuite` uses the same shape — keeps the test scope tight.
    */
  private def gskBalance(user: Address): GlobalStateKey =
    GlobalStateKey.hypergraph(GlobalStateFieldId.Balances, user)

  /** Standard test ordinal — fixed because the proof service's branch+ordinal parameters are pre-computed at construction time in
    * production wiring.
    */
  private val testOrdinal: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))

  /** Use `BranchId.base` (Hash.empty) so the overlay falls through to the base trie — there's no branch checkout in these tests, only base
    * inserts.
    */
  private val testBranch: BranchId = BranchId.base

  /** Single sentinel signature — the verify path doesn't check outer-envelope signatures; that's the gossip layer's job. The test only
    * exercises the MPT proof + per-MG-root cross-check.
    */
  private def sentinelProof: SignatureProof =
    SignatureProof(Id(Hex("11" * 64)), Signature(Hex("22" * 70)))

  /** Build a sentinel [[CommitteeMemberSignature]] — verify path doesn't inspect its contents either.
    */
  private def sentinelCommitteeSig: CommitteeMemberSignature =
    CommitteeMemberSignature(
      peerId = PeerId(Hex("aa" * 64)),
      vrfProof = Hex("bb" * 80),
      ed25519Sig = Hex("cc" * 64),
      kesProductSig = Hex("dd" * 128),
      kesTreeStep = 0
    )

  /** Construct a [[ShardCheckpoint]] with a single MG in `perMetagraphMptRoots`. Used to drive the service's verify path — the per-MG root
    * is what `verifyProof` cross-checks against, so the checkpoint fixture only needs to populate that one field.
    */
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
        includedSnapshots = SortedMap.empty,
        tokenLockBalancesDelta = SortedMap.empty,
        perMetagraphArtifacts = SortedMap.empty,
        perMetagraphSyncDataDelta = SortedMap.empty
      ),
      emittedReceipts = List.empty,
      committeeSignatures = NonEmptyList.of(sentinelCommitteeSig),
      epoch = EtaPeriod(0L)
    )

  /** Wrap a value in a [[Signed]] envelope with the sentinel proof. */
  private def mkSigned[A](value: A): Signed[A] = Signed(value, NonEmptySet.of(sentinelProof))

  /** Find a numShards value such that two given addresses map to DIFFERENT shards. Used by the shard-ownership-mismatch test — we need
    * addrA in shard 0 and addrB in shard ≠ 0 so the mismatch path is exercised. We scan [2, 16] which is fast and always finds a split for
    * any two distinct addresses (with overwhelming probability under SHA-256 uniformity).
    */
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

  /** Build the in-memory MPT proof service plus a way to (a) seed the MPT (b) compute the real trie root (which production wiring would
    * carry on the checkpoint's `perMetagraphMptRoots`).
    *
    * We return:
    *   - `store`: the MPT store, so the test can `insert[V]` keys directly
    *   - `proofSvc`: the proof service wired over `store + overlay`
    *   - `currentTrieRoot`: helper that builds the trie at the test ordinal and returns its root hash — used by the test to construct a
    *     [[ShardCheckpoint]] whose `perMetagraphMptRoots[mgAddress]` matches the actual trie root the proof was generated against (so the
    *     round-trip verify succeeds).
    */
  private def mkProofSetup(
    implicit hasher: Hasher[IO],
    js: JsonSerializer[IO]
  ): IO[(MptStore[IO, GlobalStateKey], HistoricalMptProofService[IO], IO[Hash])] =
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
      proofSvc = HistoricalMptProofService.make[IO](store, overlay)
      currentTrieRoot = overlay
        .buildRoot(testBranch, testOrdinal)
        .map(_.toOption.get.rootHash.value)
    } yield (store, proofSvc, currentTrieRoot)

  /** Construct a [[ShardSubtreeProofService.ShardCheckpointLookup]] that always returns the same pre-built checkpoint for the matching
    * shard. Returns `None` for any other shard.
    */
  private def fixedLookup(
    expectedShard: ShardId,
    checkpoint: Signed[ShardCheckpoint]
  ): ShardSubtreeProofService.ShardCheckpointLookup[IO] =
    (s: ShardId) => if (s === expectedShard) checkpoint.some.pure[IO] else (none[Signed[ShardCheckpoint]]).pure[IO]

  // ===========================================================================
  // Test 1: round-trip happy
  // ===========================================================================

  test("round-trip: generate proof for (shardId, mgAddr, key) ⇒ verifyProof returns true") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (store, proofSvc, currentRoot) = setup

      // Pick an MG and a key under it. With numShards=1 every MG maps to shard 0 so we don't have
      // to worry about ownership mismatch in the happy-path test.
      mg = addr("mg-happy-path")
      user = addr("user-happy-path-1")
      key = gskBalance(user)

      // Seed the trie with the key + value we want to prove.
      _ <- store.insert[Balance](key, Balance(NonNegLong(100L)))

      // The shard checkpoint must claim the actual trie root. In production this is derived by
      // GSAM during checkpoint construction; in tests we read it directly via overlay.buildRoot.
      perMgRoot <- currentRoot

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardId, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardId, mg, key)
      verifyResult <- proofOpt match {
        case Some(p) => proofSvcWrapped.verifyProof(checkpoint, p)
        case None    => IO.pure(false)
      }
    } yield
      expect.all(
        proofOpt.isDefined,
        verifyResult
      )
  }

  // ===========================================================================
  // Test 2: verify rejects tampered checkpoint hash
  // ===========================================================================

  test("verify rejects tampered shardCheckpointHash") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (store, proofSvc, currentRoot) = setup
      mg = addr("mg-tamper-ckhash")
      user = addr("user-tamper-ckhash")
      key = gskBalance(user)
      _ <- store.insert[Balance](key, Balance(NonNegLong(42L)))
      perMgRoot <- currentRoot

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardId, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardId, mg, key)
      // Tamper with the proof's `shardCheckpointHash` — set to a different (but well-formed) hash.
      // The verifier MUST reject because the embedded hash doesn't match what the checkpoint
      // actually hashes to. This catches a misbehaving prover that proxies a proof for a different
      // checkpoint.
      tampered = proofOpt.get.copy(shardCheckpointHash = Hash("ff" * 32))
      verifyResult <- proofSvcWrapped.verifyProof(checkpoint, tampered)
    } yield expect(!verifyResult)
  }

  // ===========================================================================
  // Test 3: verify rejects tampered per-MG MPT root
  // ===========================================================================

  test("verify rejects tampered perMgMptRoot") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (store, proofSvc, currentRoot) = setup
      mg = addr("mg-tamper-root")
      user = addr("user-tamper-root")
      key = gskBalance(user)
      _ <- store.insert[Balance](key, Balance(NonNegLong(7L)))
      perMgRoot <- currentRoot

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardId, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardId, mg, key)
      // Tamper with the proof's `perMgMptRoot` — set to a different hash than what the checkpoint
      // signed. verify must reject because the cross-check between proof.perMgMptRoot and
      // checkpoint.derivedStateDelta.perMetagraphMptRoots[mg] fails.
      tampered = proofOpt.get.copy(perMgMptRoot = Hash("00" * 32))
      verifyResult <- proofSvcWrapped.verifyProof(checkpoint, tampered)
    } yield expect(!verifyResult)
  }

  // ===========================================================================
  // Test 4: verify rejects tampered value (via tampered leaf commitment)
  // ===========================================================================

  test("verify rejects when the proof's underlying MPT inclusion proof is tampered") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (store, proofSvc, currentRoot) = setup
      mg = addr("mg-tamper-value")
      user = addr("user-tamper-value")
      key = gskBalance(user)
      _ <- store.insert[Balance](key, Balance(NonNegLong(123L)))
      perMgRoot <- currentRoot

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardId, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardId, mg, key)
      // Tamper with the underlying MPT inclusion proof: clear its witness list. The verifier MUST
      // reject because the witness chain no longer connects to the per-MG root. This stands in for
      // "tampered value bytes" — the value bytes are committed to via the leaf's dataDigest, which
      // is hashed into the witness chain; tampering with the value (without re-deriving dataDigest)
      // would break the leaf commitment exactly the same way. Clearing the witness is the simplest
      // structural tamper that reaches the same rejection code path.
      tampered = proofOpt.get.copy(
        mptProof = proofOpt.get.mptProof.copy(witness = List.empty)
      )
      verifyResult <- proofSvcWrapped.verifyProof(checkpoint, tampered)
    } yield expect(!verifyResult)
  }

  // ===========================================================================
  // Test 5: non-membership — generate returns None for a key not in the MPT
  // ===========================================================================

  test("non-membership: generate returns None for a key NOT in the MPT (v1 surfaces absence as None)") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (store, proofSvc, currentRoot) = setup

      // Seed the trie with SOMETHING (so `currentRoot` can build — `InMemoryMerklePatriciaProducer.build`
      // errors on an empty trie per `HistoricalMptProofServiceSuite`'s "TrieBuildFailed" case).
      seedMg = addr("mg-non-member-seed")
      seedUser = addr("user-non-member-seed")
      seedKey = gskBalance(seedUser)
      _ <- store.insert[Balance](seedKey, Balance(NonNegLong(1L)))
      perMgRoot <- currentRoot

      // Now ask for a proof for a DIFFERENT key that we never inserted — the underlying prover
      // will return `PathNotFound`, which the service surfaces as `None` (v1 doesn't ship
      // proof-of-absence).
      mg = addr("mg-non-member-target")
      absentUser = addr("user-not-in-trie")
      absentKey = gskBalance(absentUser)

      shardId = ShardId.unsafeApply(0)
      shardAssignment = ShardAssignment.make[IO](numShards = 1)
      checkpoint = mkSigned(mkCheckpoint(shardId, SortedMap(mg -> perMgRoot)))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardId, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardId, mg, absentKey)
    } yield expect(proofOpt.isEmpty)
  }

  // ===========================================================================
  // Test 6: shard ownership mismatch
  // ===========================================================================

  test("shard ownership mismatch: request proof from shard 0 for an MG that hashes to a different shard ⇒ None") { res =>
    implicit val (h, _, js) = res
    for {
      setup <- mkProofSetup
      (_, proofSvc, _) = setup

      // Pick two MG addresses, then pick a `numShards` that splits them across two shards. We then
      // request a proof from shard 0 for an MG that hashes to a non-zero shard ⇒ the
      // ownership-mismatch path fires and the service returns None.
      mgA = addr("mg-owner-shard-0-anchor")
      mgB = addr("mg-owner-not-shard-0-anchor")
      numShards <- findNumShardsSplitting(mgA, mgB)
      shardAssignment = ShardAssignment.make[IO](numShards)
      shardOfA <- shardAssignment.shardIdFor(mgA)
      shardOfB <- shardAssignment.shardIdFor(mgB)

      // Pick the MG that does NOT belong to shard 0 (if A is in shard 0, ask for B's proof from
      // shard 0; otherwise ask for A's proof from shard 0).
      shardZero = ShardId.unsafeApply(0)
      foreignMg = if (shardOfA === shardZero) mgB else mgA
      foreignShard = if (shardOfA === shardZero) shardOfB else shardOfA
      key = gskBalance(addr("user-foreign-mg"))

      // The lookup callback returns a checkpoint for shard 0 — but the foreign MG doesn't belong
      // to shard 0, so the service short-circuits and returns None before even consulting the
      // lookup. Provide a valid checkpoint anyway to make sure the rejection isn't due to a
      // missing checkpoint.
      checkpoint = mkSigned(mkCheckpoint(shardZero, SortedMap.empty))
      proofSvcWrapped = ShardSubtreeProofService.make[IO](
        proofSvc,
        shardAssignment,
        fixedLookup(shardZero, checkpoint),
        testBranch,
        testOrdinal
      )

      proofOpt <- proofSvcWrapped.generateProofForMetagraph(shardZero, foreignMg, key)
    } yield
      expect.all(
        // Pre-condition: the foreign MG MUST NOT hash to shard 0 (the fixture relies on
        // `findNumShardsSplitting` having found a split).
        foreignShard =!= shardZero,
        // Service must reject the cross-shard request with None — this is the ownership guard.
        proofOpt.isEmpty
      )
  }
}
