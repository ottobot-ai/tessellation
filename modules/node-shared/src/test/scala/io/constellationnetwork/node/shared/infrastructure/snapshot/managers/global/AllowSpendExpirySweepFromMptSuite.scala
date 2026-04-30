package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.swap._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Spec assertions for the surviving MPT-backed allow-spend expiry sweep.
  *
  * After the legacy in-memory `findExpiredGlobalAllowSpendsViaIndex(map)` was deleted, this suite asserts
  * `findExpiredGlobalAllowSpendsViaIndexFromMpt(prev, curr)` directly. Window semantics: AllowSpend uses `lastValidEpochProgress <
  * epochProgress` so the sweep range is `[prev .. curr - 1]`. Only global-partition (metagraphId = None) records are returned.
  */
object AllowSpendExpirySweepFromMptSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  implicit val withdrawalTimeLimit: io.constellationnetwork.schema.mpt.WithdrawalTimeLimit =
    io.constellationnetwork.schema.mpt.WithdrawalTimeLimit.none

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

  /** Seed an MPT store from a `lastActive` map by going through `syncFromGlobalSnapshotInfo` — same writer the sweep reads, so the seed
    * mirrors the phase-2a invariant.
    */
  private def mkSeededMptStore(
    lastActive: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      info = GlobalSnapshotInfo.empty.copy(
        activeAllowSpends = SortedMap(Option.empty[Address] -> lastActive).some
      )
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield store

  private def toAddressHashPairs(
    m: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit h: Hasher[IO]): IO[Set[(Address, Hash)]] =
    m.toList.flatTraverse {
      case (addr, set) =>
        set.toList.traverse(s => s.toHashed.map(hashed => (addr, hashed.hash)))
    }.map(_.toSet)

  test("FromMpt sweep returns exactly the expiring records — mixed expired and not") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      // addr1 has two allow spends: one expired (100), one not yet (500)
      // addr2 has one allow spend still valid (800)
      asExpiredA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(100L)), "expiredA")
      asValidA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(500L)), "validA")
      asValidB = mkAllowSpend(addr2, dest, EpochProgress(NonNegLong(800L)), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(asExpiredA, asValidA),
        addr2 -> SortedSet(asValidB)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      // Current epoch 300: asExpiredA (100) is expired, others not yet.
      // previousEpochProgress = MinValue: sweep all buckets up to 299.
      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedHashed <- asExpiredA.toHashed
    } yield expect(indexPairs == Set((addr1, expectedHashed.hash)))
  }

  test("FromMpt sweep returns empty when nothing is expired") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr = kp.getPublic.toAddress

      asValid = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(1000L)), "valid")
      lastActive = SortedMap(addr -> SortedSet(asValid))

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("FromMpt sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr = kp.getPublic.toAddress

      // Even though this record WOULD be expired by a free filter, the sweep window is empty.
      asExpired = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(50L)), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(asExpired))

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      sameEpoch = EpochProgress(NonNegLong(200L))
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndexFromMpt(sameEpoch, sameEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("FromMpt sweep returns all expiring records when window covers multiple epochs") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      asAt101 = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(101L)), "at101")
      asAt105 = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(105L)), "at105")
      asAt110 = mkAllowSpend(addr2, dest, EpochProgress(NonNegLong(110L)), "at110")

      lastActive = SortedMap(
        addr1 -> SortedSet(asAt101, asAt105),
        addr2 -> SortedSet(asAt110)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      // Sweep window [101 .. 114] captures all three records (predicate is `< curr`).
      prevEpoch = EpochProgress(NonNegLong(101L))
      currentEpoch = EpochProgress(NonNegLong(115L))

      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)

      hashedAt101 <- asAt101.toHashed
      hashedAt105 <- asAt105.toHashed
      hashedAt110 <- asAt110.toHashed
    } yield
      expect.all(
        indexPairs.size == 3,
        indexPairs == Set(
          (addr1, hashedAt101.hash),
          (addr1, hashedAt105.hash),
          (addr2, hashedAt110.hash)
        )
      )
  }

  test("end-to-end acceptAllowSpends evicts the expired record from fullState") { res =>
    implicit val (h, sp, js) = res
    // Exercises the full `acceptAllowSpends` method to guard against shape-only differences (e.g. the sweep
    // emitting only addresses with expiring records vs. downstream reconciliation needing all addresses).
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      asExpiredA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(100L)), "expiredA")
      asValidA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(500L)), "validA")
      asValidB = mkAllowSpend(addr2, dest, EpochProgress(NonNegLong(800L)), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(asExpiredA, asValidA),
        addr2 -> SortedSet(asValidB)
      )
      lastActiveOuter = SortedMap(Option.empty[Address] -> lastActive)

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      emptyCurrencySnapshots = SortedMap.empty[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      emptyGlobalAllowSpends = SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]]
      emptySpendTxns = List.empty[io.constellationnetwork.schema.artifact.SpendTransaction]

      result <- mgr.acceptAllowSpends(
        currentEpoch,
        prevEpoch,
        emptyCurrencySnapshots,
        emptyGlobalAllowSpends,
        lastActiveOuter,
        emptySpendTxns
      )
      // Expected post-state: addr1 keeps only asValidA (asExpiredA is swept), addr2 keeps asValidB.
      expectedGlobal = SortedMap(
        addr1 -> SortedSet(asValidA),
        addr2 -> SortedSet(asValidB)
      )
    } yield
      expect.all(
        result.fullState.get(None) == Some(expectedGlobal),
        // No address became fully empty.
        result.removedKeys.isEmpty
      )
  }

  test("acceptAllowSpends: yields correct expiryIndexDelta — expired record contributes a remove") { res =>
    implicit val (h, sp, js) = res
    // Direct correctness check on the index-delta shape: the expired allow-spend should appear in the
    // `removes` map keyed by its lastValidEpochProgress; `adds` should be empty (no incoming records).
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      asExpiredA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(100L)), "expiredA")
      asValidA = mkAllowSpend(addr1, dest, EpochProgress(NonNegLong(500L)), "validA")
      asValidB = mkAllowSpend(addr2, dest, EpochProgress(NonNegLong(800L)), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(asExpiredA, asValidA),
        addr2 -> SortedSet(asValidB)
      )
      lastActiveOuter = SortedMap(Option.empty[Address] -> lastActive)

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      emptyCurrencySnapshots = SortedMap.empty[Address, SortedMap[Address, SortedSet[Signed[AllowSpend]]]]
      emptyGlobalAllowSpends = SortedMap.empty[Address, SortedSet[Signed[AllowSpend]]]
      emptySpendTxns = List.empty[io.constellationnetwork.schema.artifact.SpendTransaction]

      result <- mgr.acceptAllowSpends(
        currentEpoch,
        prevEpoch,
        emptyCurrencySnapshots,
        emptyGlobalAllowSpends,
        lastActiveOuter,
        emptySpendTxns
      )
      expectedHashed <- asExpiredA.toHashed
      expectedKey = AllowSpendExpiryKey(None, addr1, expectedHashed.hash)
      bucket = result.expiryIndexDelta match {
        case eb: SystemIndexDelta.EpochBucket[AllowSpendExpiryKey] => eb
      }
      allRemovedKeys = bucket.removes.values.flatten.toSet
      allAddedKeys = bucket.adds.values.flatten.toSet
    } yield
      expect.all(
        // The expired record should appear in the `removes` set.
        clue(allRemovedKeys).contains(expectedKey),
        // No incoming allow-spends → the `adds` side is empty.
        clue(allAddedKeys).isEmpty
      )
  }
}
