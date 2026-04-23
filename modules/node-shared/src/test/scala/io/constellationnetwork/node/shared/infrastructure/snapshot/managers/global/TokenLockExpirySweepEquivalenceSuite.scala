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

/** Equivalence harness for phase 2b index-driven token-lock expiry sweep.
  *
  * Same shape as `AllowSpendExpirySweepEquivalenceSuite` but for TokenLocks. TokenLock has no `metagraphId` dimension; records with
  * `unlockEpoch = None` never expire and are not indexed. Tests assert `findExpiredGlobalTokenLocksViaIndex` and `filterExpiredTokenLocks`
  * return the same `(address, hash)` set under the phase-2a invariant.
  */
object TokenLockExpirySweepEquivalenceSuite extends MutableIOSuite {

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

  private def mkTokenLock(source: Address, unlockAt: Option[EpochProgress], label: String): Signed[TokenLock] =
    Signed(
      TokenLock(
        source = source,
        amount = TokenLockAmount(PosLong(200L)),
        fee = TokenLockFee(NonNegLong(0L)),
        parent = TokenLockReference(TokenLockOrdinal(NonNegLong(0L)), testHash(s"tl-parent-$label")),
        currencyId = None,
        unlockEpoch = unlockAt,
        replaceTokenLockRef = None
      ),
      testProofs
    )

  private def mkSeededMptStore(
    lastActive: SortedMap[Address, SortedSet[Signed[TokenLock]]]
  )(implicit h: Hasher[IO], js: JsonSerializer[IO]): IO[MptStore[IO, GlobalStateKey]] =
    for {
      mptProducer <- InMemoryMerklePatriciaProducer.make[IO]()
      store <- MptStore.make[IO, GlobalStateKey](mptProducer, GlobalStateKey.toHex[IO])
      info = GlobalSnapshotInfo.empty.copy(activeTokenLocks = lastActive.some)
      _ <- store.syncFromGlobalSnapshotInfo(info, SnapshotOrdinal(NonNegLong(1L)))
    } yield store

  private def toAddressHashPairs(
    m: SortedMap[Address, SortedSet[Signed[TokenLock]]]
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
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      // addr1: one expired (100), one not yet (500)
      // addr2: one still valid (800)
      tlExpiredA = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(100L))), "expiredA")
      tlValidA = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(500L))), "validA")
      tlValidB = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(800L))), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(tlExpiredA, tlValidA),
        addr2 -> SortedSet(tlValidB)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      legacyExpired = mgr.filterExpiredTokenLocks(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedHashed <- tlExpiredA.toHashed
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

      tlValid = mkTokenLock(addr, Some(EpochProgress(NonNegLong(1000L))), "valid")
      lastActive = SortedMap(addr -> SortedSet(tlValid))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      legacyExpired = mgr.filterExpiredTokenLocks(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect.all(legacyPairs.isEmpty, indexPairs.isEmpty, legacyPairs == indexPairs)
  }

  test("records with unlockEpoch = None are never expired (not indexed, not in legacy either)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress

      // No unlockEpoch = permanent lock; never appears in either path's expired output.
      tlPermanent = mkTokenLock(addr, None, "permanent")
      tlExpires = mkTokenLock(addr, Some(EpochProgress(NonNegLong(100L))), "expires")

      lastActive = SortedMap(addr -> SortedSet(tlPermanent, tlExpires))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](store)

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress.MinValue

      legacyExpired = mgr.filterExpiredTokenLocks(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedExpires <- tlExpires.toHashed
    } yield
      expect.all(
        legacyPairs == indexPairs,
        // The permanent lock is NOT expired; only the one with an unlockEpoch in the past is.
        indexPairs == Set((addr, expectedExpires.hash))
      )
  }

  test("index sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress

      tlExpires = mkTokenLock(addr, Some(EpochProgress(NonNegLong(50L))), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(tlExpires))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](store)

      sameEpoch = EpochProgress(NonNegLong(200L))
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndex(sameEpoch, sameEpoch, lastActive)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("end-to-end acceptTokenLocks parity: flag on vs flag off yield identical result") { res =>
    implicit val (h, sp, js) = res
    // Guards against bugs where the index path skips addresses that the legacy path's empty-set entries would have
    // caused the downstream fold to visit (e.g. addresses with `generatedTokenUnlocks` but no expiring locks).
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      // addr1: a non-expiring lock (unlockEpoch = None) that will be removed by an incoming TokenUnlock.
      tlPermanent = mkTokenLock(addr1, None, "permanent1")
      // addr2: an expiring lock (unlockEpoch in the past) that the filter should find.
      tlExpires = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(100L))), "expires2")

      lastActive = SortedMap(addr1 -> SortedSet(tlPermanent), addr2 -> SortedSet(tlExpires))

      hashedPermanent <- tlPermanent.toHashed
      // TokenUnlock targeting addr1 — only `acceptTokenLocks` with the right iteration set will apply it.
      tokenUnlock = io.constellationnetwork.schema.artifact.TokenUnlock(
        tokenLockRef = hashedPermanent.hash,
        amount = io.constellationnetwork.schema.tokenLock.TokenLockAmount(PosLong(100L)),
        source = addr1,
        currencyId = None
      )
      generatedUnlocks = Map(addr1 -> List(tokenUnlock))

      storeOn <- mkSeededMptStore(lastActive)
      storeOff <- mkSeededMptStore(lastActive)
      mgrOn = TokenLockStateManager.make[IO](storeOn, shouldUseMptStore = true)
      mgrOff = TokenLockStateManager.make[IO](storeOff, shouldUseMptStore = false)

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      acceptedGlobal = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]

      resultOn <- mgrOn.acceptTokenLocks(currentEpoch, prevEpoch, acceptedGlobal, lastActive, generatedUnlocks)
      resultOff <- mgrOff.acceptTokenLocks(currentEpoch, prevEpoch, acceptedGlobal, lastActive, generatedUnlocks)
    } yield
      expect.all(
        resultOn.fullState == resultOff.fullState,
        resultOn.removedKeys == resultOff.removedKeys,
        // Both paths must remove addr1's lock via the TokenUnlock AND addr2's lock via expiry.
        !resultOn.fullState.contains(addr1),
        !resultOn.fullState.contains(addr2)
      )
  }

  test("index sweep matches legacy filter when sweep window covers multiple epochs") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      tlAt101 = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(101L))), "at101")
      tlAt105 = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(105L))), "at105")
      tlAt110 = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(110L))), "at110")

      lastActive = SortedMap(
        addr1 -> SortedSet(tlAt101, tlAt105),
        addr2 -> SortedSet(tlAt110)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](store)

      prevEpoch = EpochProgress(NonNegLong(101L))
      currentEpoch = EpochProgress(NonNegLong(115L))

      legacyExpired = mgr.filterExpiredTokenLocks(lastActive, currentEpoch)
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndex(prevEpoch, currentEpoch, lastActive)

      legacyPairs <- toAddressHashPairs(legacyExpired)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect.all(legacyPairs == indexPairs, legacyPairs.size == 3)
  }
}
