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
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore}
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

/** Equivalence harness for phase 2b index-driven expiry sweep.
  *
  * Phase 2a maintains the allow-spend expiry index on every accept(). Phase 2b replaces the `O(N)` full-map filter with an index-driven
  * sweep of buckets `[previousEpochProgress .. epochProgress - 1]`. For the flip to be safe at #88/#91, the two paths must return the same
  * set of `(address, hash)` pairs for the global (`metagraphId = None`) partition under the phase-2a invariant.
  *
  * These tests seed matched state on both paths — identical `lastActiveGlobalAllowSpends` map plus an index rebuilt from that map — and
  * assert
  * `findExpiredGlobalAllowSpendsViaIndex` returns the same set as `filterExpiredAllowSpends` (with empty-set entries elided, since the
  * index path cannot emit addresses with zero expiring records).
  */
object AllowSpendExpirySweepEquivalenceSuite extends MutableIOSuite {

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

  /** Build an MPT store seeded with the same state that the GSI rebuild path would produce from `lastActive`. Uses
    * `syncFromGlobalSnapshotInfo` so the expiry index is materialized from the same records the in-memory map contains — the exact
    * phase-2a invariant.
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

  /** Helper: extract `(address, hash)` pairs from a nested SortedMap[Address, SortedSet[Signed[AllowSpend]]], filtering out empty entries.
    * Used to compare index-sweep output against legacy-filter output without caring about empty-set key placeholders.
    */
  private def toAddressHashPairs(
    m: SortedMap[Address, SortedSet[Signed[AllowSpend]]]
  )(implicit h: Hasher[IO]): IO[Set[(Address, Hash)]] =
    m.toList.flatTraverse {
      case (addr, set) =>
        set.toList.traverse(s => s.toHashed.map(hashed => (addr, hashed.hash)))
    }.map(_.toSet)

  test("index sweep and legacy filter return the same (addr, hash) set — mixed expired and not") { res =>
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
      lastActiveOuter = SortedMap(Option.empty[Address] -> lastActive)

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](Some(store))

      // Current epoch 300: asExpiredA (100) is expired, others not yet.
      // previousEpochProgress = MinValue: sweep all buckets up to 299.
      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      legacyExpired = mgr.filterExpiredAllowSpends(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedHashed <- asExpiredA.toHashed
    } yield
      expect.all(
        legacyPairs == indexPairs,
        indexPairs == Set((addr1, expectedHashed.hash))
      )
  }

  test("index sweep and legacy filter agree when nothing is expired") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr = kp.getPublic.toAddress

      asValid = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(1000L)), "valid")
      lastActive = SortedMap(addr -> SortedSet(asValid))

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](Some(store))

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      legacyExpired = mgr.filterExpiredAllowSpends(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect.all(legacyPairs.isEmpty, indexPairs.isEmpty, legacyPairs == indexPairs)
  }

  test("index sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr = kp.getPublic.toAddress

      // Even though this record WOULD be expired by the legacy filter, the sweep window is empty.
      asExpired = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(50L)), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(asExpired))

      store <- mkSeededMptStore(lastActive)
      mgr = AllowSpendStateManager.make[IO](Some(store))

      sameEpoch = EpochProgress(NonNegLong(200L))
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndex(sameEpoch, sameEpoch, lastActive)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("index sweep matches legacy filter when sweep window covers multiple epochs") { res =>
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
      mgr = AllowSpendStateManager.make[IO](Some(store))

      // Sweep window covers 101..114 — captures all three records.
      prevEpoch = EpochProgress(NonNegLong(101L))
      currentEpoch = EpochProgress(NonNegLong(115L))

      legacyExpired = mgr.filterExpiredAllowSpends(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect.all(legacyPairs == indexPairs, legacyPairs.size == 3)
  }

  test("no-mptStore fallback: findExpiredGlobalAllowSpendsViaIndex with None mptStore returns legacy filter output") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      dest <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      addr = kp.getPublic.toAddress

      asExpired = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(100L)), "expired")
      asValid = mkAllowSpend(addr, dest, EpochProgress(NonNegLong(500L)), "valid")

      lastActive = SortedMap(addr -> SortedSet(asExpired, asValid))

      mgr = AllowSpendStateManager.make[IO](None)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress(NonNegLong(50L))

      legacyExpired = mgr.filterExpiredAllowSpends(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalAllowSpendsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(legacyPairs == indexPairs)
  }
}
