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
import io.constellationnetwork.schema.mpt.{GlobalStateKey, MptStore, WithdrawalTimeLimit}
import io.constellationnetwork.schema.nodeCollateral._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.NonNegLong
import weaver.MutableIOSuite

/** Equivalence harness for phase 2b index-driven NC-withdrawal expiry sweep.
  *
  * NC differs from AllowSpend/TokenLock in two ways:
  *   1. Expiry predicate is `<=` instead of `<`, so the sweep window is `(prevEpoch, curEpoch]` — fromL = prevEpoch + 1, toL = curEpoch. 2.
  *      The expiry epoch is derived (`createdAt + withdrawalTimeLimit`) rather than stored in the record itself. The index bucket keys
  *      already encode the derived epoch, so `withdrawalTimeLimit` is not a sweep parameter.
  *
  * Test suite uses an explicit `WithdrawalTimeLimit` implicit with a concrete value so the seeding `syncFromGlobalSnapshotInfo` populates
  * the index. Compares legacy filter vs index sweep via `(address, event-hash)` pairs.
  */
object NodeCollateralExpirySweepEquivalenceSuite extends MutableIOSuite {

  implicit val globalStateProofSelector: GlobalStateProofSelector = GlobalStateProofSelector(SnapshotOrdinal(NonNegLong(Long.MaxValue)))
  // Non-none so the NC expiry index is populated on seed.
  private val withdrawalTimeLimit: EpochProgress = EpochProgress(NonNegLong(100L))
  implicit val withdrawalTimeLimitCtx: WithdrawalTimeLimit = WithdrawalTimeLimit.some(withdrawalTimeLimit)

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

  private def mkWithdrawal(
    source: Address,
    nodeId: io.constellationnetwork.schema.peer.PeerId,
    createdAt: EpochProgress,
    label: String
  ): PendingNodeCollateralWithdrawal = {
    val createEvent = UpdateNodeCollateral.Create(
      source = source,
      nodeId = nodeId,
      amount = NodeCollateralAmount(NonNegLong(1_000_000L)),
      tokenLockRef = testHash(s"tl-ref-$label")
    )
    PendingNodeCollateralWithdrawal(
      event = Signed(createEvent, testProofs),
      acceptedOrdinal = SnapshotOrdinal(NonNegLong(1L)),
      createdAt = createdAt
    )
  }

  private def mkSeededMptStore(
    lastActive: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      info = GlobalSnapshotInfo.empty.copy(nodeCollateralWithdrawals = lastActive.some)
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield store

  private def legacyExpired(
    lastActive: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]],
    epochProgress: EpochProgress
  ): SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]] = {
    def isExpired(createdAt: EpochProgress): Boolean = (createdAt |+| withdrawalTimeLimit) <= epochProgress
    lastActive.map {
      case (addr, ws) =>
        addr -> ws.filter {
          case PendingNodeCollateralWithdrawal(_, _, createdAt) => isExpired(createdAt)
        }
    }.filter(_._2.nonEmpty)
  }

  private def toAddressHashPairs(
    m: SortedMap[Address, SortedSet[PendingNodeCollateralWithdrawal]]
  )(implicit h: Hasher[IO]): IO[Set[(Address, Hash)]] =
    m.toList.flatTraverse {
      case (addr, set) =>
        set.toList.traverse(w => w.event.toHashed.map(hashed => (addr, hashed.hash)))
    }.map(_.toSet)

  test("index sweep and legacy filter return the same (addr, hash) set — mixed expired and not") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      // withdrawalTimeLimit = 100, curEpoch = 300
      // addr1 withdrawal createdAt=50 → expiry=150 <= 300 → expired
      // addr1 withdrawal createdAt=250 → expiry=350 > 300 → not expired
      // addr2 withdrawal createdAt=300 → expiry=400 > 300 → not expired
      wExpired = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(50L)), "expired")
      wValidA = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(250L)), "validA")
      wValidB = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(300L)), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(wExpired, wValidA),
        addr2 -> SortedSet(wValidB)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      legacy = legacyExpired(lastActive, currentEpoch)
      index <- mgr.findExpiredWithdrawalsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacy)
      indexPairs <- toAddressHashPairs(index)

      expectedHashed <- wExpired.event.toHashed
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
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      // createdAt=500, WTL=100 → expiry=600 > 500 → not expired yet
      wFresh = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(500L)), "fresh")
      lastActive = SortedMap(addr -> SortedSet(wFresh))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      legacy = legacyExpired(lastActive, currentEpoch)
      index <- mgr.findExpiredWithdrawalsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacy)
      indexPairs <- toAddressHashPairs(index)
    } yield expect.all(legacyPairs.isEmpty, indexPairs.isEmpty, legacyPairs == indexPairs)
  }

  test("index sweep captures expiry exactly at currentEpoch (inclusive upper bound)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      // createdAt=200, WTL=100 → expiry=300; currentEpoch=300 → expired (<=)
      wBoundary = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(200L)), "boundary")
      lastActive = SortedMap(addr -> SortedSet(wBoundary))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      legacy = legacyExpired(lastActive, currentEpoch)
      index <- mgr.findExpiredWithdrawalsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacy)
      indexPairs <- toAddressHashPairs(index)
    } yield expect.all(legacyPairs == indexPairs, indexPairs.size == 1)
  }

  test("index sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress
      peer = kp.getPublic.toId.toPeerId

      wExpired = mkWithdrawal(addr, peer, EpochProgress(NonNegLong(50L)), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(wExpired))

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](store)

      sameEpoch = EpochProgress(NonNegLong(200L))
      index <- mgr.findExpiredWithdrawalsViaIndex(sameEpoch, sameEpoch, lastActive)
      indexPairs <- toAddressHashPairs(index)
    } yield expect(indexPairs.isEmpty)
  }

  test("end-to-end acceptNodeCollaterals parity: flag on vs flag off yield identical tuples") { res =>
    implicit val (h, sp, js) = res
    // Lesson from TokenLock: index-only iteration can silently skip downstream work that the legacy full-map
    // filter would have visited. Exercise the full method, not just the helper, so shape-only differences
    // (e.g. index emits fewer addresses than legacy) can't produce divergent downstream state.
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      // addr1: one expired (createdAt=50 → expiry=150 <= 300), one not yet (createdAt=250 → expiry=350)
      // addr2: one not yet (createdAt=300 → expiry=400)
      wExpired = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(50L)), "expired")
      wValidA = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(250L)), "validA")
      wValidB = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(300L)), "validB")

      existingWithdrawals = SortedMap(
        addr1 -> SortedSet(wExpired, wValidA),
        addr2 -> SortedSet(wValidB)
      )

      info = GlobalSnapshotInfo.empty.copy(nodeCollateralWithdrawals = existingWithdrawals.some)

      storeOn <- mkSeededMptStore(existingWithdrawals)
      storeOff <- mkSeededMptStore(existingWithdrawals)
      mgrOn = NodeCollateralStateManager.make[IO](storeOn, shouldUseMptStore = true)
      mgrOff = NodeCollateralStateManager.make[IO](storeOff, shouldUseMptStore = false)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      resOn <- mgrOn.acceptNodeCollaterals(info, currentEpoch, prevEpoch, withdrawalTimeLimit)
      resOff <- mgrOff.acceptNodeCollaterals(info, currentEpoch, prevEpoch, withdrawalTimeLimit)

      (_, unexpOn, expOn) = resOn
      (_, unexpOff, expOff) = resOff
    } yield
      expect.all(
        unexpOn == unexpOff,
        expOn == expOff,
        // addr1 should have one expired, one unexpired; addr2 should have one unexpired.
        expOn.get(addr1).map(_.size) == Some(1),
        unexpOn.get(addr1).map(_.size) == Some(1),
        unexpOn.get(addr2).map(_.size) == Some(1)
      )
  }

  test("index sweep matches legacy filter when sweep window covers multiple epochs") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress
      peer1 = kp1.getPublic.toId.toPeerId
      peer2 = kp2.getPublic.toId.toPeerId

      // WTL=100. Expiry epochs: 201, 205, 210 (from createdAt 101, 105, 110)
      w201 = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(101L)), "at201")
      w205 = mkWithdrawal(addr1, peer1, EpochProgress(NonNegLong(105L)), "at205")
      w210 = mkWithdrawal(addr2, peer2, EpochProgress(NonNegLong(110L)), "at210")

      lastActive = SortedMap(
        addr1 -> SortedSet(w201, w205),
        addr2 -> SortedSet(w210)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = NodeCollateralStateManager.make[IO](store)

      prevEpoch = EpochProgress(NonNegLong(200L))
      currentEpoch = EpochProgress(NonNegLong(215L))

      legacy = legacyExpired(lastActive, currentEpoch)
      index <- mgr.findExpiredWithdrawalsViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacy)
      indexPairs <- toAddressHashPairs(index)
    } yield expect.all(legacyPairs == indexPairs, legacyPairs.size == 3)
  }
}
