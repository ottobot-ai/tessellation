package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.global

import cats.data.NonEmptySet
import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.node.shared.domain.nakamoto.overlay.GlobalStateReader
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema._
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.mpt.GlobalStateConverter.syntax._
import io.constellationnetwork.schema.mpt._
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.mpt.producer.InMemoryMerklePatriciaProducer
import io.constellationnetwork.security.signature.{Signed, signature}
import io.constellationnetwork.serde.codecs.instances.GlobalStateMptCodecs.tokenLockExpiryKeySetImmutableCodec

import eu.timepit.refined.auto._
import eu.timepit.refined.types.numeric.{NonNegLong, PosLong}
import weaver.MutableIOSuite

/** Spec assertions for the surviving MPT-backed token-lock expiry sweep.
  *
  * After the legacy in-memory `findExpiredGlobalTokenLocksViaIndex(map)` was deleted, this suite asserts
  * `findExpiredGlobalTokenLocksViaIndexFromMpt(prev, curr)` directly. Window semantics: TokenLock uses `unlockEpoch < epochProgress` so the
  * sweep range is `[prev .. curr - 1]`. Records with `unlockEpoch = None` never expire and are not indexed.
  */
object TokenLockExpirySweepFromMptSuite extends MutableIOSuite {

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

  private def sameBytes(left: Map[Hex, Array[Byte]], right: Map[Hex, Array[Byte]]): Boolean =
    left.keySet == right.keySet && left.forall { case (key, bytes) => right.get(key).exists(_.sameElements(bytes)) }

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

  /** Seed an MPT store from a `lastActive` map by going through `syncFromGlobalSnapshotInfo` — the same code path that populates the expiry
    * index in production, so the seed exercises the same writer the sweep reads.
    */
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

  test("FromMpt sweep returns exactly the expiring records — mixed expired and not") { res =>
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
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue

      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedHashed <- tlExpiredA.toHashed
    } yield expect(indexPairs == Set((addr1, expectedHashed.hash)))
  }

  test("FromMpt sweep returns empty when nothing is expired") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress

      tlValid = mkTokenLock(addr, Some(EpochProgress(NonNegLong(1000L))), "valid")
      lastActive = SortedMap(addr -> SortedSet(tlValid))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress(NonNegLong(100L))

      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("records with unlockEpoch = None are never returned (not indexed)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress

      tlPermanent = mkTokenLock(addr, None, "permanent")
      tlExpires = mkTokenLock(addr, Some(EpochProgress(NonNegLong(100L))), "expires")

      lastActive = SortedMap(addr -> SortedSet(tlPermanent, tlExpires))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(500L))
      prevEpoch = EpochProgress.MinValue

      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)

      expectedExpires <- tlExpires.toHashed
    } yield expect(indexPairs == Set((addr, expectedExpires.hash)))
  }

  test("FromMpt sweep returns empty when previousEpochProgress >= epochProgress (same-epoch ordinals)") { res =>
    implicit val (h, sp, js) = res
    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      addr = kp.getPublic.toAddress

      tlExpires = mkTokenLock(addr, Some(EpochProgress(NonNegLong(50L))), "wouldExpire")
      lastActive = SortedMap(addr -> SortedSet(tlExpires))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      sameEpoch = EpochProgress(NonNegLong(200L))
      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndexFromMpt(sameEpoch, sameEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)
    } yield expect(indexPairs.isEmpty)
  }

  test("end-to-end acceptTokenLocks removes both expired locks and TokenUnlock-targeted permanent locks") { res =>
    implicit val (h, sp, js) = res
    // Guards the iteration shape: the sweep emits only addresses with expiring records, but downstream
    // accept must still process addresses with TokenUnlocks (no expiring locks), e.g. a permanent lock
    // (unlockEpoch = None) being removed via a generated TokenUnlock.
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      // addr1: a non-expiring lock (unlockEpoch = None) that will be removed by an incoming TokenUnlock.
      tlPermanent = mkTokenLock(addr1, None, "permanent1")
      // addr2: an expiring lock (unlockEpoch in the past) that the sweep should find.
      tlExpires = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(100L))), "expires2")

      lastActive = SortedMap(addr1 -> SortedSet(tlPermanent), addr2 -> SortedSet(tlExpires))

      hashedPermanent <- tlPermanent.toHashed
      tokenUnlock = io.constellationnetwork.schema.artifact.TokenUnlock(
        tokenLockRef = hashedPermanent.hash,
        amount = io.constellationnetwork.schema.tokenLock.TokenLockAmount(PosLong(100L)),
        source = addr1,
        currencyId = None
      )
      generatedUnlocks = Map(addr1 -> List(tokenUnlock))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      acceptedGlobal = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]

      result <- mgr.acceptTokenLocks(currentEpoch, prevEpoch, acceptedGlobal, lastActive, generatedUnlocks)
    } yield
      expect.all(
        // Both addresses' locks should be gone: addr1 via TokenUnlock, addr2 via expiry.
        !result.fullState.contains(addr1),
        !result.fullState.contains(addr2),
        result.removedKeys.contains(addr1),
        result.removedKeys.contains(addr2)
      )
  }

  test("FromMpt sweep returns all expiring records when window covers multiple epochs") { res =>
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
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      // Window [101 .. 114] covers all three records (TokenLock predicate is `unlockEpoch < curr`).
      prevEpoch = EpochProgress(NonNegLong(101L))
      currentEpoch = EpochProgress(NonNegLong(115L))

      indexExpired <- mgr.findExpiredGlobalTokenLocksViaIndexFromMpt(prevEpoch, currentEpoch)
      indexPairs <- toAddressHashPairs(indexExpired)

      hashedAt101 <- tlAt101.toHashed
      hashedAt105 <- tlAt105.toHashed
      hashedAt110 <- tlAt110.toHashed
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

  test("acceptTokenLocks: mixed expired/not yields the expected post-state and removedKeys") { res =>
    implicit val (h, sp, js) = res
    // End-to-end shape on the acceptTokenLocks API: same scenario as the legacy `#85 parity` test, but
    // asserting absolute correctness against an explicit expected fullState rather than two paths agreeing.
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      tlExpiredA = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(100L))), "expiredA")
      tlValidA = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(500L))), "validA")
      tlValidB = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(800L))), "validB")

      lastActive = SortedMap(
        addr1 -> SortedSet(tlExpiredA, tlValidA),
        addr2 -> SortedSet(tlValidB)
      )

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      accepted = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      unlocks = Map.empty[Address, List[io.constellationnetwork.schema.artifact.TokenUnlock]]

      result <- mgr.acceptTokenLocks(currentEpoch, prevEpoch, accepted, lastActive, unlocks)
    } yield
      expect.all(
        // addr1 only retains tlValidA (tlExpiredA is swept).
        result.fullState.get(addr1) == Some(SortedSet(tlValidA)),
        // addr2 unchanged (no expiring records).
        result.fullState.get(addr2) == Some(SortedSet(tlValidB)),
        // No address became fully empty.
        result.removedKeys.isEmpty,
        // The delta only mentions addr1 (its set shape changed).
        result.deltas.keySet == Set(addr1),
        result.deltas.get(addr1) == Some(SortedSet(tlValidA))
      )
  }

  test("updateGlobalBalancesByTokenLocks: expired locks credit the source's balance") { res =>
    implicit val (h, sp, js) = res
    for {
      kp1 <- KeyPairGenerator.makeKeyPair[IO]
      kp2 <- KeyPairGenerator.makeKeyPair[IO]
      addr1 = kp1.getPublic.toAddress
      addr2 = kp2.getPublic.toAddress

      tlExpired = mkTokenLock(addr1, Some(EpochProgress(NonNegLong(100L))), "expired")
      tlValid = mkTokenLock(addr2, Some(EpochProgress(NonNegLong(800L))), "valid")
      lastActive = SortedMap(addr1 -> SortedSet(tlExpired), addr2 -> SortedSet(tlValid))

      store <- mkSeededMptStore(lastActive)
      mgr = TokenLockStateManager.make[IO](GlobalStateReader.fromMptStore(store))

      currentBalances = SortedMap.empty[Address, io.constellationnetwork.schema.balance.Balance]
      currentEpoch = EpochProgress(NonNegLong(300L))
      prevEpoch = EpochProgress.MinValue
      accepted = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      unlocks = Map.empty[Address, List[io.constellationnetwork.schema.artifact.TokenUnlock]]

      resultE <- mgr.updateGlobalBalancesByTokenLocks(currentEpoch, prevEpoch, currentBalances, accepted, unlocks)
    } yield
      expect.all(
        // The Right side carries (full balances, deltas). For an empty starting balance and an expiring
        // 200-amount lock, addr1 should end at 200 (refunded) and addr2 unchanged at 0 (not touched
        // because tlValid is still active, not in the expiredGlobalTokenLocks fold input).
        resultE.isRight,
        resultE.exists {
          case (_, deltas) => deltas.get(addr1).map(_.value.value) == Some(200L)
        }
      )
  }

  test("FromMpt sweep rejects an absent active target at its exact physical key without mutation") { res =>
    implicit val (h, sp, js) = res
    val expiry = EpochProgress(NonNegLong(100L))

    for {
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      tokenLock = mkTokenLock(source, Some(expiry), "absent-target")
      store <- mkSeededMptStore(SortedMap(source -> SortedSet(tokenLock)))
      targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      targetHex <- GlobalStateKey.toHex[IO](targetKey)
      _ <- store.remove(targetKey)
      before <- store.allEntriesAsBytes
      result <- TokenLockStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .findExpiredGlobalTokenLocksViaIndexFromMpt(expiry, EpochProgress(NonNegLong(101L)))
        .attempt
      after <- store.allEntriesAsBytes
    } yield
      expect.all(
        result match {
          case Left(error: StrictMptRead.MissingConsensusMptValue) => error.physicalKey == targetHex
          case _                                                   => false
        },
        sameBytes(before, after)
      )
  }

  test("FromMpt sweep rejects a forged earlier bucket for an active token lock without mutation") { res =>
    implicit val (h, sp, js) = res
    val forgedExpiry = EpochProgress(NonNegLong(100L))
    val actualExpiry = EpochProgress(NonNegLong(101L))

    for {
      source <- KeyPairGenerator.makeKeyPair[IO].map(_.getPublic.toAddress)
      tokenLock = mkTokenLock(source, Some(actualExpiry), "forged-earlier-bucket")
      hashed <- tokenLock.toHashed
      store <- mkSeededMptStore(SortedMap(source -> SortedSet(tokenLock)))
      forgedBucketKey <- GlobalStateKey.expiryIndexKey[IO](SystemNamespaceLabel.ExpiryIndexTokenLocks, forgedExpiry)
      forgedIndexKey = TokenLockExpiryKey(source, hashed.hash)
      _ <- store.insert[SortedSet[TokenLockExpiryKey]](forgedBucketKey, SortedSet(forgedIndexKey))
      targetKey = GlobalStateKey.hypergraph(GlobalStateFieldId.ActiveTokenLocks, source)
      targetHex <- GlobalStateKey.toHex[IO](targetKey)
      before <- store.allEntriesAsBytes
      result <- TokenLockStateManager
        .make[IO](GlobalStateReader.fromMptStore(store))
        .findExpiredGlobalTokenLocksViaIndexFromMpt(forgedExpiry, actualExpiry)
        .attempt
      after <- store.allEntriesAsBytes
    } yield
      expect.all(
        result match {
          case Left(error: StrictMptRead.InconsistentConsensusMptIndex) => error.physicalKey == targetHex
          case _                                                        => false
        },
        sameBytes(before, after)
      )
  }
}
