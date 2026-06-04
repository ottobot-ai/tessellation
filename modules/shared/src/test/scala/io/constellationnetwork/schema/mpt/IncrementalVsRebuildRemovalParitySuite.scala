package io.constellationnetwork.schema.mpt

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.balance.{Amount, Balance}
import io.constellationnetwork.schema.delegatedStake._
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.StateChangesAccumulator
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.schema.peer.PeerId
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Consensus success-criterion gate for the ml0 adopt path: for ALL logical states reachable by add-then-EARLY-remove of every
  * removal-bearing field, the cumulative incremental writer (`syncFromStateChanges`, the producer's signed root) must produce the SAME MPT
  * root + byte-identical entries as the from-active-records rebuild (`syncFromGlobalSnapshotInfo`, ml0's bootstrap/recovery path).
  *
  * If the two diverge for any field, ml0's rebuilt base no longer matches the producer's incremental base; adopt then recomputes a root ≠
  * the signed `mptRoot`, falls back to full re-execution, and the resulting StateProofMismatch cascades.
  *
  * Unlike `IncrementalVsRebuildEarlyRemovalParitySuite` (which drives the real AllowSpend/TokenLock managers), this suite feeds the
  * converter directly with hand-built accumulators — the removal-set fields + system-index deltas shaped exactly as `accept()` emits them —
  * so it can cover EVERY field in one place: allow-spends (global + metagraph scope), token-locks, tokenLockBalances pairs, delegated
  * stakes, node-collaterals, node-collateral-withdrawals (with their expiry index), all in their add-then-remove transition.
  */
object IncrementalVsRebuildRemovalParitySuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  // Node-collateral-withdrawal expiry index requires a withdrawalTimeLimit on BOTH paths; pick a fixed value so add+remove are indexed.
  private val ncwLimit: EpochProgress = EpochProgress(NonNegLong(100L))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit(ncwLimit.some)

  type Res = (Hasher[IO], SecurityProvider[IO], JsonSerializer[IO])

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      implicit0(h: Hasher[IO]) = Hasher.forJson[IO]
    } yield (h, sp, j)

  private val testSignature = signature.Signature(Hex(""))
  private val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)
  private val testProofs = NonEmptySet.one(testSignatureProof)

  private def testHash(label: String): Hash =
    Hash(label.getBytes("UTF-8").map("%02x".format(_)).mkString.padTo(64, '0').take(64))

  private val ord1 = SnapshotOrdinal(NonNegLong(1L))
  private val ord2 = SnapshotOrdinal(NonNegLong(2L))

  private def mkAllowSpend(source: Address, dest: Address, expireAt: EpochProgress, label: String): Signed[AllowSpend] =
    Signed(
      AllowSpend(
        source = source,
        destination = dest,
        currencyId = None,
        amount = SwapAmount(PosLong(100L)),
        fee = AllowSpendFee(NonNegLong(0L)),
        parent = AllowSpendReference(AllowSpendOrdinal(NonNegLong(0L)), testHash(s"as-parent-$label")),
        lastValidEpochProgress = expireAt,
        approvers = List.empty
      ),
      testProofs
    )

  private def mkTokenLock(source: Address, unlockAt: Option[EpochProgress], label: String): Signed[TokenLock] =
    Signed(
      TokenLock(
        source,
        TokenLockAmount(PosLong(200L)),
        TokenLockFee(NonNegLong(0L)),
        TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        None,
        unlockAt,
        None
      ),
      testProofs
    )

  private def mkStake(source: Address, nodeId: PeerId, amount: Long, label: String): DelegatedStakeRecord =
    DelegatedStakeRecord(
      Signed(
        UpdateDelegatedStake
          .Create(source, nodeId, DelegatedStakeAmount(NonNegLong.unsafeFrom(amount)), DelegatedStakeFee(0L), testHash(s"ds-tlr-$label")),
        NonEmptySet.one[signature.SignatureProof](signature.SignatureProof(nodeId.toId, signature.Signature(Hex(Hash.empty.value))))
      ),
      ord1,
      Amount(NonNegLong(0L)),
      None,
      None
    )

  private def mkCollateral(source: Address, nodeId: PeerId, label: String): NodeCollateralRecord =
    NodeCollateralRecord(
      Signed(
        UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong(1_000_000L)),
          NodeCollateralFee(NonNegLong(0L)),
          testHash(s"nc-tlr-$label")
        ),
        testProofs
      ),
      ord1
    )

  private def mkNcWithdrawal(source: Address, nodeId: PeerId, createdAt: EpochProgress, label: String): PendingNodeCollateralWithdrawal =
    PendingNodeCollateralWithdrawal(
      Signed(
        UpdateNodeCollateral.Create(
          source,
          nodeId,
          NodeCollateralAmount(NonNegLong(1_000_000L)),
          NodeCollateralFee(NonNegLong(0L)),
          testHash(s"ncw-tlr-$label")
        ),
        testProofs
      ),
      ord1,
      createdAt
    )

  private def freshStore(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
    } yield store

  private def rootAndBytes(store: MptStore[IO, GlobalStateKey], ordinal: SnapshotOrdinal): IO[(Option[Hash], Map[Hex, Vector[Byte]])] =
    for {
      trie <- store.build(ordinal)
      bytes <- store.allEntriesAsBytes
    } yield (trie.toOption.map(_.rootHash.value), bytes.view.mapValues(_.toVector).toMap)

  // A stable anchor address whose balance persists across ord1→ord2 on BOTH paths. Without it, a post-state that empties out entirely would
  // hit the trie's `Some(empty-root)` (incremental, emptied) vs `None` (rebuild, never-populated) edge — an unrelated quirk that isn't the
  // index-orphan question under test. With the anchor present on both sides, the trie is non-empty on both paths and root equality isolates
  // whether the removal left an orphan index/active-set entry the rebuild doesn't have.
  private val anchorAddr: Address = Address("DAG0KpQNqMsED4FC5grhFCBWG8iwU8Gm6aLhB9w5")
  private val anchorBalance: (Address, Balance) = anchorAddr -> Balance(NonNegLong(42L))

  /** Merge the disjoint anchor balance into a balances map. `updated` (not `++`) keeps the NoMapConcat linter happy and documents that the
    * anchor key is disjoint from any field-under-test address by construction (a fixed DAG literal vs freshly-generated keypairs).
    */
  private def withAnchor(balances: SortedMap[Address, Balance]): SortedMap[Address, Balance] =
    balances.updated(anchorBalance._1, anchorBalance._2)

  /** Apply `acc1` then `acc2` incrementally; rebuild `postInfo` fresh; assert equal roots + byte-identical entries at ord2. A stable anchor
    * balance is injected into `acc1` and `postInfo` so the trie is non-empty on both paths (see `anchorAddr`).
    */
  private def assertParity(
    acc1: StateChangesAccumulator,
    acc2: StateChangesAccumulator,
    postInfo: GlobalSnapshotInfo
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[weaver.Expectations] =
    for {
      prod <- freshStore
      _ <- prod.syncFromStateChanges(acc1.copy(balances = withAnchor(acc1.balances)), ord1)
      _ <- prod.syncFromStateChanges(acc2, ord2)
      (incRoot, incBytes) <- rootAndBytes(prod, ord2)

      rebuild <- freshStore
      _ <- rebuild.syncFromGlobalSnapshotInfo(postInfo.copy(balances = withAnchor(postInfo.balances)), ord2)
      (rebuildRoot, rebuildBytes) <- rootAndBytes(rebuild, ord2)
    } yield {
      val onlyInInc = incBytes.keySet -- rebuildBytes.keySet
      val onlyInRebuild = rebuildBytes.keySet -- incBytes.keySet
      expect.all(
        clue(incRoot) === clue(rebuildRoot),
        clue(incBytes) == clue(rebuildBytes),
        clue(onlyInInc).isEmpty,
        clue(onlyInRebuild).isEmpty
      )
    }

  test("allow-spend (global scope) add@ord1 then early-remove@ord2: incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      dst <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      as = mkAllowSpend(src, dst, EpochProgress(NonNegLong(500L)), "g")
      asHashed <- as.toHashed
      expiryKey = AllowSpendExpiryKey(None, src, asHashed.hash)

      acc1 = StateChangesAccumulator(
        activeAllowSpends = SortedMap(Option.empty[Address] -> SortedMap(src -> SortedSet(as))),
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      // Early remove: drop the active-set key + remove the FUTURE-epoch bucket entry (what the manager's before/after diff emits).
      acc2 = StateChangesAccumulator(
        removedAllowSpendKeys = Set((Option.empty[Address], src)),
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(removes = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      postInfo = GlobalSnapshotInfo.empty.copy(activeAllowSpends =
        SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]].some
      )
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("allow-spend (metagraph scope) add@ord1 then early-remove@ord2: incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      mid <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      dst <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      as = mkAllowSpend(src, dst, EpochProgress(NonNegLong(500L)), "m")
      asHashed <- as.toHashed
      expiryKey = AllowSpendExpiryKey(mid.some, src, asHashed.hash)

      acc1 = StateChangesAccumulator(
        activeAllowSpends = SortedMap(mid.some -> SortedMap(src -> SortedSet(as))),
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      acc2 = StateChangesAccumulator(
        removedAllowSpendKeys = Set((mid.some, src)),
        allowSpendExpiryIndex = SystemIndexDelta.EpochBucket(removes = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      postInfo = GlobalSnapshotInfo.empty.copy(activeAllowSpends =
        SortedMap.empty[Option[Address], SortedMap[Address, SortedSet[Signed[AllowSpend]]]].some
      )
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("token-lock add@ord1 then early-remove@ord2 (future unlock bucket removed): incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      tl = mkTokenLock(src, EpochProgress(NonNegLong(500L)).some, "x")
      tlHashed <- tl.toHashed
      expiryKey = TokenLockExpiryKey(src, tlHashed.hash)

      acc1 = StateChangesAccumulator(
        activeTokenLocks = SortedMap(src -> SortedSet(tl)),
        tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      acc2 = StateChangesAccumulator(
        removedTokenLockKeys = Set(src),
        tokenLockExpiryIndex = SystemIndexDelta.EpochBucket(removes = SortedMap(EpochProgress(NonNegLong(500L)) -> Set(expiryKey)))
      )
      postInfo = GlobalSnapshotInfo.empty.copy(activeTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]].some)
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("tokenLockBalances pair add@ord1 then remove@ord2 (pair-index pruned): incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      mid <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      holder <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)

      acc1 = StateChangesAccumulator(
        tokenLockBalances = SortedMap(mid -> SortedMap(holder -> Balance(NonNegLong(777L))))
      )
      acc2 = StateChangesAccumulator(
        removedTokenLockBalanceKeys = Set((mid, holder))
      )
      postInfo = GlobalSnapshotInfo.empty.copy(tokenLockBalances = SortedMap.empty[Address, SortedMap[Address, Balance]].some)
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("delegated stake add@ord1 then remove@ord2: incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      nodeId = Id(Hex("1234567890abcdef" * 8)).toPeerId
      stake = mkStake(src, nodeId, 1000L, "x")

      acc1 = StateChangesAccumulator(activeDelegatedStakes = SortedMap(src -> SortedSet(stake)))
      acc2 = StateChangesAccumulator(removedDelegatedStakeKeys = Set(src))
      postInfo = GlobalSnapshotInfo.empty.copy(activeDelegatedStakes = SortedMap.empty[Address, SortedSet[DelegatedStakeRecord]].some)
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("node-collateral add@ord1 then remove@ord2: incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      nodeId = Id(Hex("abcdef1234567890" * 8)).toPeerId
      coll = mkCollateral(src, nodeId, "x")

      acc1 = StateChangesAccumulator(activeNodeCollaterals = SortedMap(src -> SortedSet(coll)))
      acc2 = StateChangesAccumulator(removedNodeCollateralKeys = Set(src))
      postInfo = GlobalSnapshotInfo.empty.copy(activeNodeCollaterals = SortedMap.empty[Address, SortedSet[NodeCollateralRecord]].some)
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }

  test("node-collateral WITHDRAWAL add@ord1 then early-remove@ord2 (future expiry bucket removed): incremental === rebuild") { res =>
    implicit val (h, sp, js) = res
    for {
      src <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      nodeId = Id(Hex("0fedcba987654321" * 8)).toPeerId
      // createdAt 200; expiry bucket = createdAt |+| limit(100) = 300. We "remove early" at ord2 (before 300).
      createdAt = EpochProgress(NonNegLong(200L))
      ncw = mkNcWithdrawal(src, nodeId, createdAt, "x")
      ncwHashed <- ncw.event.toHashed
      expiryEpoch = createdAt |+| ncwLimit
      expiryKey = NodeCollateralWithdrawalExpiryKey(src, ncwHashed.hash)

      acc1 = StateChangesAccumulator(
        nodeCollateralWithdrawals = SortedMap(src -> SortedSet(ncw)),
        nodeCollateralWithdrawalExpiryIndex = SystemIndexDelta.EpochBucket(adds = SortedMap(expiryEpoch -> Set(expiryKey)))
      )
      acc2 = StateChangesAccumulator(
        removedNodeCollateralWithdrawalKeys = Set(src),
        nodeCollateralWithdrawalExpiryIndex = SystemIndexDelta.EpochBucket(removes = SortedMap(expiryEpoch -> Set(expiryKey)))
      )
      postInfo = GlobalSnapshotInfo.empty.copy(
        nodeCollateralWithdrawals = SortedMap.empty[Address, SortedSet[PendingNodeCollateralWithdrawal]].some
      )
      r <- assertParity(acc1, acc2, postInfo)
    } yield r
  }
}
