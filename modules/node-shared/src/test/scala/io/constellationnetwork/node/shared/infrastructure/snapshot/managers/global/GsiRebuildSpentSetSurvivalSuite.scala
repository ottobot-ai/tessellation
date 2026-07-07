package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.ShardAssignment
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.{SpendAction, SpendTransaction}
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.sharding.ShardId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.Signed

import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** FINDING-S01 regression — the GSI-rebuild paths silently WIPE the MPT-native consensus partitions.
  *
  * `ConsumedAllowSpends` (fieldId 33, the cross-shard single-use spent-set / nullifier) and `Slashings` (fieldId 34) are IN the signed
  * consensus root (`GlobalStateKey.consensusRootEntries` keeps them; `GlobalSnapshotInfo.sidecarFreeMptRoot` folds them), but
  * `GlobalSnapshotInfo` has NO case-class field for either partition. So every `syncFromGlobalSnapshotInfo` rebuild (reorg self-heal
  * `NakamotoSyncDaemon.storeForkBranch`, reward-realign, gossip catch-up fallback, dag-l0 `Main` cold-restart / peer-join, `Download`)
  * clears the store and repopulates ONLY the GSI-native partitions — dropping 33/34:
  *
  *   1. the spent-set marker written at a FINALIZED ordinal vanishes ⇒ the SAME cross-shard allow-spend passes the W3d absence check again
  *      (a flag-independent cross-shard DOUBLE-SPEND), and
  *   1. the rebuilt store's sidecar-free root no longer equals the signed `stateProof.mptRoot` ⇒ the node self-forks on its next stateProof
  *      comparison.
  *
  * Scope is `numShards > 1` (both partitions are structurally empty at `numShards = 1`, which the identity test pins).
  *
  * The fix under test: `syncFromGlobalSnapshotInfo` PRESERVES the `GlobalStateFieldId.mptNativeConsensusFields` entries verbatim across the
  * rebuild, and the root-verified adopt variant (`syncFromGlobalSnapshotInfoVerified`) reconciles {with-preserve, without-preserve} against
  * the SIGNED `mptRoot` BEFORE writing, failing CLOSED (no write, `false`) when neither candidate reproduces the signed root.
  */
object GsiRebuildSpentSetSurvivalSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  // Same implicit pattern as IncrementalVsRebuildEarlyRemovalParitySuite — MPT stateProof format for all ordinals under test.
  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: WithdrawalTimeLimit = WithdrawalTimeLimit.none

  private val ord1: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(1L))
  private val ord2: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(2L))
  private val ord3: SnapshotOrdinal = SnapshotOrdinal(NonNegLong(3L))

  private def swap(v: Long): SwapAmount = SwapAmount(PosLong.unsafeFrom(v))
  private def feeOf(v: Long): AllowSpendFee = AllowSpendFee(NonNegLong.unsafeFrom(v))
  private def epoch(v: Long): EpochProgress = EpochProgress(NonNegLong.unsafeFrom(v))
  private def bal(v: Long): Balance = Balance(NonNegLong.unsafeFrom(v))

  /** Scan `numShards ∈ [2,64]` for a value placing the two metagraph addresses on DIFFERENT shards (same helper as
    * `ConsumedAllowSpendSettlementSuite`).
    */
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
    expireAt: EpochProgress
  )(implicit h: Hasher[IO], sp: SecurityProvider[IO]): IO[Signed[AllowSpend]] =
    KeyPairGenerator.makeKeyPair[IO].flatMap { kp =>
      Signed.forAsyncHasher(
        AllowSpend(
          source = source,
          destination = destination,
          currencyId = currencyId,
          amount = swap(amount),
          fee = feeOf(0L),
          parent = AllowSpendReference.empty,
          lastValidEpochProgress = expireAt,
          approvers = List(destination)
        ),
        kp
      )
    }

  /** The full cross-shard fixture: two metagraphs on different shards, one allow-spend on M consumed by a spend produced on M′, the
    * GSI-native state (M's active-allow-spend mirror + a balance), and a store seeded from that GSI.
    */
  private case class Fixture(
    gsi: GlobalSnapshotInfo,
    store: MptStore[IO, GlobalStateKey],
    mgr: ConsumedAllowSpendStateManager[IO],
    assignment: ShardAssignment[IO],
    acceptedSpendActions: SortedMap[Address, List[SpendAction]],
    asHash: Hash,
    slashingsHex: Hex
  )

  private def mkFixture(implicit h: Hasher[IO], sp: SecurityProvider[IO], js: JsonSerializer[IO]): IO[Fixture] =
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
      as <- mkAllowSpend(source, dest, CurrencyId(m).some, x, epoch(200L))
      asHashed <- as.toHashed
      asHash = asHashed.hash

      // GSI-native state at the finalized base: M's ActiveAllowSpends mirror holds AS (the owner metagraph NEVER witnesses a
      // cross-shard consume, so its mirror keeps AS until expiry — that is exactly why the fieldId-33 nullifier is load-bearing),
      // plus a plain balance entry.
      gsi = GlobalSnapshotInfo.empty.copy(
        balances = SortedMap(source -> bal(1000L)),
        activeAllowSpends = (SortedMap(
          (m.some: Option[Address]) -> SortedMap(source -> SortedSet(as))
        ): SortedMap[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]]).some
      )

      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      _ <- store.syncFromGlobalSnapshotInfo(gsi, ord1)
      mgr = ConsumedAllowSpendStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      spendTx = SpendTransaction(asHash.some, CurrencyId(m).some, swap(x), source, dest)
      acceptedSpendActions = SortedMap(mPrime -> List(SpendAction(NonEmptyList.of(spendTx))))

      slashKey <- GlobalStateKey.slashingsKey[IO](PeerId(Hex("ab" * 64)), ShardId.unsafeApply(0), asHash)
      slashingsHex <- GlobalStateKey.toHex[IO](slashKey)
    } yield Fixture(gsi, store, mgr, assignment, acceptedSpendActions, asHash, slashingsHex)

  /** Settle + write the FIRST cross-shard consume at the finalized ordinal (the GSAM fold write: marker inserted with the canonical
    * `ConsumedAllowSpend` codec at `consumedAllowSpendKey`, plus a raw fieldId-34 `Slashings` entry through the same store), and return the
    * sidecar-free consensus root the producer would SIGN over that state.
    */
  private def consumeAndFinalize(f: Fixture)(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[Hash] =
    for {
      candidates <- f.mgr.classifyCrossShardConsumes(f.acceptedSpendActions, f.assignment)
      spent0 <- f.mgr.materializeConsumedAllowSpendsFromMpt
      mirror <- f.mgr.materializeActiveAllowSpendsFromMpt
      settled <- f.mgr.settleCrossShardConsumes(candidates, spent0, mirror, ord2)
      _ <- IO.raiseError(new AssertionError(s"first consume did not settle: $settled")).unlessA(settled.newMarkers.contains(f.asHash))
      _ <- f.store.insert[ConsumedAllowSpend](GlobalStateKey.consumedAllowSpendKey(f.asHash), settled.newMarkers(f.asHash))(
        io.constellationnetwork.serde.codecs.instances.ConsumedAllowSpendCodec.immutableCodec
      )
      // A fieldId-34 Slashings ledger entry at the same finalized ordinal (raw bytes — preservation is a bytes-level contract).
      _ <- f.store.underlying.insertBytes(Map(f.slashingsHex -> Array[Byte](1, 2, 3, 4))).flatMap(_.liftTo[IO])
      _ <- f.store.commit(ord2)
      signedRoot <- f.store.allEntriesAsBytes.flatMap(GlobalSnapshotInfo.sidecarFreeMptRoot[IO](_))
    } yield signedRoot

  // ───────────────────────────────────────────────────────────────────────────
  // Test 1 — THE S01 regression (RED until the preservation fix lands)
  // ───────────────────────────────────────────────────────────────────────────

  test(
    "S01: fieldId-33/34 markers written at a finalized ordinal SURVIVE a GSI rebuild; the rebuilt root stays the signed root; a replay of the SAME allow-spend is rejected"
  ) { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture
      signedRoot <- consumeAndFinalize(f)
      spentBefore <- f.mgr.materializeConsumedAllowSpendsFromMpt

      // ===== THE S01 EVENT: any GSI-rebuild self-heal (reorg storeForkBranch:2144 / cold restart Main:430 / catch-up :3499)
      // rebuilds the base from the carried GlobalSnapshotInfo — which structurally has NO field for partitions 33/34.
      _ <- f.store.syncFromGlobalSnapshotInfo(f.gsi, ord2)

      spentAfter <- f.mgr.materializeConsumedAllowSpendsFromMpt
      entriesAfter <- f.store.allEntriesAsBytes
      rebuiltRoot <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](entriesAfter)

      // The SECOND consume of the SAME allow-spend, post-rebuild (the cross-shard double-spend replay). The owner mirror still
      // holds AS (it is GSI-native and legitimately survives the rebuild), so ONLY the spent-set stands between this replay
      // and acceptance.
      candidates2 <- f.mgr.classifyCrossShardConsumes(f.acceptedSpendActions, f.assignment)
      mirror2 <- f.mgr.materializeActiveAllowSpendsFromMpt
      replay <- f.mgr.settleCrossShardConsumes(candidates2, spentAfter, mirror2, ord3)
    } yield
      expect.all(
        // sanity: the first consume settled and was committed at the finalized ordinal
        spentBefore.contains(f.asHash),
        // (1) the spent-set marker SURVIVES the rebuild — RED today: the rebuild wipes fieldId 33
        spentAfter.contains(f.asHash),
        // (2) the Slashings (fieldId 34) entry SURVIVES the rebuild — RED today
        entriesAfter.contains(f.slashingsHex),
        // (3) the rebuilt sidecar-free root still equals the SIGNED consensus root — RED today (self-fork on next stateProof compare)
        rebuiltRoot === signedRoot,
        // (4) the replay of the SAME allow-spend is REJECTED — RED today: with the spent-set wiped the absence check passes
        //     and the double-spend is (wrongly) accepted
        replay.newMarkers.isEmpty,
        replay.rejected.contains(f.asHash)
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Test 2 — numShards=1 byte-identity (GREEN before AND after the fix)
  // ───────────────────────────────────────────────────────────────────────────

  test("numShards=1 identity: with EMPTY 33/34 partitions the rebuild reproduces the exact same root (preservation is a no-op)") { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture // fixture writes nothing to 33/34 until consumeAndFinalize — spent-set empty here, as at numShards=1
      rootBefore <- f.store.allEntriesAsBytes.flatMap(GlobalSnapshotInfo.sidecarFreeMptRoot[IO](_))
      _ <- f.store.syncFromGlobalSnapshotInfo(f.gsi, ord2)
      rootAfter <- f.store.allEntriesAsBytes.flatMap(GlobalSnapshotInfo.sidecarFreeMptRoot[IO](_))
      spent <- f.mgr.materializeConsumedAllowSpendsFromMpt
    } yield
      expect.all(
        rootBefore === rootAfter,
        spent.isEmpty
      )
  }

  // ───────────────────────────────────────────────────────────────────────────
  // Tests 3-6 — the ROOT-VERIFIED adopt variant (syncFromGlobalSnapshotInfoVerified)
  // ───────────────────────────────────────────────────────────────────────────

  test("verified adopt: WITH-preserve reconcile — signed root carries 33/34 ⇒ adopt=true, markers survive, root == signed") { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture
      signedRoot <- consumeAndFinalize(f)
      adopted <- f.store.syncFromGlobalSnapshotInfoVerified(f.gsi, ord2, signedRoot.some)
      entriesAfter <- f.store.allEntriesAsBytes
      rootAfter <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](entriesAfter)
      spentAfter <- f.mgr.materializeConsumedAllowSpendsFromMpt
    } yield
      expect.all(
        adopted,
        spentAfter.contains(f.asHash),
        entriesAfter.contains(f.slashingsHex),
        rootAfter === signedRoot
      )
  }

  test(
    "verified adopt: WITHOUT-preserve reconcile — target root has EMPTY 33/34 while OUR store holds stale markers ⇒ adopt=true, stale markers dropped, root == target"
  ) { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture
      // The target chain never consumed anything: its signed root is the GSI-only root (fresh store seeded from the same GSI).
      freshProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      freshStore <- MptStore.make[IO, GlobalStateKey](freshProducer, GlobalStateKey.toHex[IO])
      _ <- freshStore.syncFromGlobalSnapshotInfo(f.gsi, ord2)
      targetRoot <- freshStore.allEntriesAsBytes.flatMap(GlobalSnapshotInfo.sidecarFreeMptRoot[IO](_))
      // OUR store DID consume (stale local markers that must NOT be carried into the adopted state).
      _ <- consumeAndFinalize(f)
      adopted <- f.store.syncFromGlobalSnapshotInfoVerified(f.gsi, ord2, targetRoot.some)
      entriesAfter <- f.store.allEntriesAsBytes
      rootAfter <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](entriesAfter)
      spentAfter <- f.mgr.materializeConsumedAllowSpendsFromMpt
    } yield
      expect.all(
        adopted,
        spentAfter.isEmpty,
        !entriesAfter.contains(f.slashingsHex),
        rootAfter === targetRoot
      )
  }

  test("verified adopt: FAIL-CLOSED — neither candidate reproduces the signed root ⇒ adopt=false and the store is UNTOUCHED") { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture
      signedRoot <- consumeAndFinalize(f)
      bogus = Hash("ff".padTo(64, 'f').take(64))
      adopted <- f.store.syncFromGlobalSnapshotInfoVerified(f.gsi, ord2, bogus.some)
      entriesAfter <- f.store.allEntriesAsBytes
      rootAfter <- GlobalSnapshotInfo.sidecarFreeMptRoot[IO](entriesAfter)
      spentAfter <- f.mgr.materializeConsumedAllowSpendsFromMpt
    } yield
      expect.all(
        !adopted,
        // nothing was written: the pre-rebuild state (markers included) is intact
        spentAfter.contains(f.asHash),
        entriesAfter.contains(f.slashingsHex),
        rootAfter === signedRoot
      )
  }

  test("verified adopt: signedMptRoot=None (pre-MPT legacy snapshot) ⇒ adopt=false, store untouched") { res =>
    implicit val (h, sp, js) = res
    for {
      f <- mkFixture
      signedRoot <- consumeAndFinalize(f)
      adopted <- f.store.syncFromGlobalSnapshotInfoVerified(f.gsi, ord2, none[Hash])
      rootAfter <- f.store.allEntriesAsBytes.flatMap(GlobalSnapshotInfo.sidecarFreeMptRoot[IO](_))
    } yield
      expect.all(
        !adopted,
        rootAfter === signedRoot
      )
  }
}
