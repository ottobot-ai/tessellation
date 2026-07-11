package io.constellationnetwork.node.shared.infrastructure.snapshot.managers.currency

import cats.effect.{IO, Resource}
import cats.syntax.all._

import scala.collection.immutable.{SortedMap, SortedSet}

import io.constellationnetwork.ext.cats.effect.ResourceIO
import io.constellationnetwork.json.JsonSerializer
import io.constellationnetwork.schema.ID.Id
import io.constellationnetwork.schema.address.Address
import io.constellationnetwork.schema.artifact.TokenUnlock
import io.constellationnetwork.schema.balance.Balance
import io.constellationnetwork.schema.epoch.EpochProgress
import io.constellationnetwork.schema.swap.CurrencyId
import io.constellationnetwork.schema.tokenLock._
import io.constellationnetwork.security._
import io.constellationnetwork.security.hash.Hash
import io.constellationnetwork.security.hex.Hex
import io.constellationnetwork.security.key.ops.PublicKeyOps
import io.constellationnetwork.security.signature.{Signed, signature}

import eu.timepit.refined.auto._
import weaver.MutableIOSuite

object TokenLockOpsManagerSuite extends MutableIOSuite {

  type Res = (Hasher[IO], SecurityProvider[IO])

  // Test data
  val testSignature = signature.Signature(Hex(""))
  val testSignatureProof = signature.SignatureProof(Id(Hex("")), testSignature)

  private def activeTokenLock(
    source: Address,
    amount: TokenLockAmount,
    currencyId: Option[CurrencyId] = None
  ): TokenLock =
    TokenLock(
      source,
      amount,
      TokenLockFee(0L),
      TokenLockReference(TokenLockOrdinal(1L), Hash("parent")),
      currencyId,
      EpochProgress(2000L).some,
      none
    )

  override def sharedResource: Resource[IO, Res] =
    for {
      sp <- SecurityProvider.forAsync[IO]
      implicit0(j: JsonSerializer[IO]) <- JsonSerializer.forAsync[IO].asResource
      h = Hasher.forJson[IO]
    } yield (h, sp)

  test("acceptTokenUnlocks - should filter out token unlocks with non-existent token lock refs") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    val expiredTokenLockHashes = List(Hash("expired1"), Hash("expired2"))
    val source1 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebU")
    val source2 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebT")
    val source3 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebV")
    val activeTokenLocksByRef = Map(
      Hash("active1") -> activeTokenLock(source1, TokenLockAmount(100L)),
      Hash("active2") -> activeTokenLock(source2, TokenLockAmount(200L)),
      Hash("expired1") -> activeTokenLock(source3, TokenLockAmount(300L))
    )

    val incomingTokenUnlocks = SortedSet(
      TokenUnlock(Hash("active1"), TokenLockAmount(100L), none, source1),
      TokenUnlock(Hash("nonExistent"), TokenLockAmount(200L), none, source2),
      TokenUnlock(Hash("expired1"), TokenLockAmount(300L), none, source3)
    )

    val result = manager.acceptTokenUnlocks(expiredTokenLockHashes, incomingTokenUnlocks, activeTokenLocksByRef)

    (expect(result.size == 1) &&
      expect(result.head.tokenLockRef == Hash("active1"))).pure[IO]
  }

  test("acceptTokenUnlocks - should filter out token unlocks for expired token locks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    val expiredTokenLockHashes = List(Hash("expired1"), Hash("expired2"))
    val source1 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebU")
    val source2 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebT")
    val source3 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebV")
    val activeTokenLocksByRef = Map(
      Hash("active1") -> activeTokenLock(source1, TokenLockAmount(100L)),
      Hash("expired1") -> activeTokenLock(source2, TokenLockAmount(200L)),
      Hash("expired2") -> activeTokenLock(source3, TokenLockAmount(300L))
    )

    val incomingTokenUnlocks = SortedSet(
      TokenUnlock(Hash("active1"), TokenLockAmount(100L), none, source1),
      TokenUnlock(Hash("expired1"), TokenLockAmount(200L), none, source2),
      TokenUnlock(Hash("expired2"), TokenLockAmount(300L), none, source3)
    )

    val result = manager.acceptTokenUnlocks(expiredTokenLockHashes, incomingTokenUnlocks, activeTokenLocksByRef)

    (expect(result.size == 1) &&
      expect(result.head.tokenLockRef == Hash("active1"))).pure[IO]
  }

  test("acceptTokenUnlocks - should accept all valid token unlocks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    val expiredTokenLockHashes = List.empty[Hash]
    val source1 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebU")
    val source2 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebT")
    val source3 = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebV")
    val activeTokenLocksByRef = Map(
      Hash("active1") -> activeTokenLock(source1, TokenLockAmount(100L)),
      Hash("active2") -> activeTokenLock(source2, TokenLockAmount(200L)),
      Hash("active3") -> activeTokenLock(source3, TokenLockAmount(300L))
    )

    val incomingTokenUnlocks = SortedSet(
      TokenUnlock(Hash("active1"), TokenLockAmount(100L), none, source1),
      TokenUnlock(Hash("active2"), TokenLockAmount(200L), none, source2),
      TokenUnlock(Hash("active3"), TokenLockAmount(300L), none, source3)
    )

    val result = manager.acceptTokenUnlocks(expiredTokenLockHashes, incomingTokenUnlocks, activeTokenLocksByRef)

    (expect(result.size == 3) &&
      expect(result.map(_.tokenLockRef).toSet == Set(Hash("active1"), Hash("active2"), Hash("active3")))).pure[IO]
  }

  test("acceptTokenUnlocks - should authenticate amount, currency, and source against the referenced lock") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    val source = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebU")
    val attacker = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebT")
    val otherCurrency = CurrencyId(Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebV"))
    val tokenLockRef = Hash("active1")
    val tokenLock = activeTokenLock(source, TokenLockAmount(100L))
    val validUnlock = TokenUnlock(tokenLockRef, tokenLock.amount, tokenLock.currencyId, tokenLock.source)
    val incomingTokenUnlocks = SortedSet(
      validUnlock,
      validUnlock.copy(amount = TokenLockAmount(200L)),
      validUnlock.copy(currencyId = otherCurrency.some),
      validUnlock.copy(source = attacker)
    )

    val result = manager.acceptTokenUnlocks(List.empty, incomingTokenUnlocks, Map(tokenLockRef -> tokenLock))

    expect(result == SortedSet(validUnlock)).pure[IO]
  }

  test("acceptTokenUnlocks - forged fields cannot refund or remove the referenced lock") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      source <- kp.getPublic.toId.toAddress
      attacker = Address("DAG53ho9ssY8KYQdjxsWPYgNbDJ1YqM2RaPDZebT")
      signedTokenLock <- Signed.forAsyncHasher(activeTokenLock(source, TokenLockAmount(100L)), kp)
      hashedTokenLock <- signedTokenLock.toHashed
      activeTokenLocks = SortedMap(source -> SortedSet(signedTokenLock))
      currentBalances = SortedMap(source -> Balance.empty, attacker -> Balance.empty)
      forgedUnlock = TokenUnlock(hashedTokenLock.hash, TokenLockAmount(200L), none, attacker)
      acceptedUnlocks = manager.acceptTokenUnlocks(
        List.empty,
        SortedSet(forgedUnlock),
        Map(hashedTokenLock.hash -> signedTokenLock.value)
      )
      (updatedActiveTokenLocks, _) <- manager.acceptTokenLocks(
        EpochProgress(1000L),
        SortedMap.empty,
        activeTokenLocks,
        acceptedUnlocks
      )
      updatedBalances = manager.updateBalancesByTokenLocks(
        EpochProgress(1000L),
        currentBalances,
        SortedMap.empty,
        activeTokenLocks,
        acceptedUnlocks
      )
    } yield
      expect(acceptedUnlocks.isEmpty) &&
        expect(updatedActiveTokenLocks == activeTokenLocks) &&
        expect(updatedBalances == Right(currentBalances))
  }

  test("filterExpiredTokenLocks - should filter out token locks with past unlock epoch") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      currentEpoch = EpochProgress(1000L)
      pastEpoch = EpochProgress(999L)
      futureEpoch = EpochProgress(1001L)

      pastTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        pastEpoch.some,
        none
      )

      currentTokenLock = TokenLock(
        address,
        TokenLockAmount(200L),
        TokenLockFee(20L),
        TokenLockReference(TokenLockOrdinal(2L), Hash("ref2")),
        none,
        currentEpoch.some,
        none
      )

      futureTokenLock = TokenLock(
        address,
        TokenLockAmount(300L),
        TokenLockFee(30L),
        TokenLockReference(TokenLockOrdinal(3L), Hash("ref3")),
        none,
        futureEpoch.some,
        none
      )

      signedPastTokenLock <- Signed.forAsyncHasher(pastTokenLock, kp)
      signedCurrentTokenLock <- Signed.forAsyncHasher(currentTokenLock, kp)
      signedFutureTokenLock <- Signed.forAsyncHasher(futureTokenLock, kp)

      tokenLocks = SortedMap(
        address -> SortedSet(signedPastTokenLock, signedCurrentTokenLock, signedFutureTokenLock)
      )

      result = manager.filterExpiredTokenLocks(tokenLocks, currentEpoch)
    } yield
      expect(result(address).size == 1) &&
        expect(result(address).head == signedPastTokenLock)
  }

  test("filterExpiredTokenLocks - should handle token locks without unlock epoch") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      currentEpoch = EpochProgress(1000L)

      tokenLockWithoutEpoch = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        none, // No unlock epoch
        none
      )

      signedTokenLock <- Signed.forAsyncHasher(tokenLockWithoutEpoch, kp)

      tokenLocks = SortedMap(address -> SortedSet(signedTokenLock))

      result = manager.filterExpiredTokenLocks(tokenLocks, currentEpoch)
    } yield expect(!result.contains(address))
  }

  test("updateBalancesByTokenLocks - should handle empty inputs") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    val epochProgress = EpochProgress(1000L)
    val currentBalances = SortedMap.empty[Address, Balance]
    val acceptedTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
    val lastActiveTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
    val acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

    val result = manager.updateBalancesByTokenLocks(
      epochProgress,
      currentBalances,
      acceptedTokenLocks,
      lastActiveTokenLocks,
      acceptedTokenUnlocks
    )

    (expect(result.isRight) &&
      expect(result.toOption.get.isEmpty)).pure[IO]
  }

  test("updateBalancesByTokenLocks - should deduct amounts for new token locks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)
      initialBalance = Balance(1000L)
      currentBalances = SortedMap(address -> initialBalance)

      tokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(2000L).some,
        none
      )

      signedTokenLock <- Signed.forAsyncHasher(tokenLock, kp)
      acceptedTokenLocks = SortedMap(address -> SortedSet(signedTokenLock))
      lastActiveTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

      result = manager.updateBalancesByTokenLocks(
        epochProgress,
        currentBalances,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )

      expectedBalance = Balance(890L) // 1000 - 100 - 10
    } yield
      expect(result.isRight) &&
        expect(result.toOption.get(address) == expectedBalance)
  }

  test("updateBalancesByTokenLocks - should add back amounts for expired token locks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)
      initialBalance = Balance(500L)
      currentBalances = SortedMap(address -> initialBalance)

      expiredTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(900L).some, // Past epoch
        none
      )

      signedExpiredTokenLock <- Signed.forAsyncHasher(expiredTokenLock, kp)
      acceptedTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      lastActiveTokenLocks = SortedMap(address -> SortedSet(signedExpiredTokenLock))
      acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

      result = manager.updateBalancesByTokenLocks(
        epochProgress,
        currentBalances,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )

      expectedBalance = Balance(600L) // 500 + 100 (only amount, not fee)
    } yield
      expect(result.isRight) &&
        expect(result.toOption.get(address) == expectedBalance)
  }

  test("updateBalancesByTokenLocks - should add amounts for accepted token unlocks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)
      initialBalance = Balance(500L)
      currentBalances = SortedMap(address -> initialBalance)

      acceptedTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(0L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(1900L).some, // Future epoch
        none
      )
      signedAcceptedTokenLock <- Signed.forAsyncHasher(acceptedTokenLock, kp)

      acceptedTokenLocks = SortedMap(signedAcceptedTokenLock.source -> SortedSet(signedAcceptedTokenLock))
      lastActiveTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]

      tokenUnlock = TokenUnlock(Hash("ref1"), TokenLockAmount(100L), none, address)
      acceptedTokenUnlocks = SortedSet(tokenUnlock)

      result = manager.updateBalancesByTokenLocks(
        epochProgress,
        currentBalances,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )

      expectedBalance = Balance(500L) // 500 - 100 + 100
    } yield
      expect(result.isRight) &&
        expect(result.toOption.get(address) == expectedBalance)
  }

  test("updateBalancesByTokenLocks - should handle balance arithmetic errors") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)
      initialBalance = Balance(50L) // Small balance
      currentBalances = SortedMap(address -> initialBalance)

      tokenLock = TokenLock(
        address,
        TokenLockAmount(100L), // Larger than balance
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(2000L).some,
        none
      )

      signedTokenLock <- Signed.forAsyncHasher(tokenLock, kp)
      acceptedTokenLocks = SortedMap(address -> SortedSet(signedTokenLock))
      lastActiveTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

      result = manager.updateBalancesByTokenLocks(
        epochProgress,
        currentBalances,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )
    } yield expect(result.isLeft) // Should fail due to insufficient balance
  }

  test("acceptTokenLocks - should accept new token locks and filter expired ones") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)

      // New token lock
      newTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(2000L).some,
        none
      )

      // Expired token lock
      expiredTokenLock = TokenLock(
        address,
        TokenLockAmount(200L),
        TokenLockFee(20L),
        TokenLockReference(TokenLockOrdinal(2L), Hash("ref2")),
        none,
        EpochProgress(999L).some,
        none
      )

      signedNewTokenLock <- Signed.forAsyncHasher(newTokenLock, kp)
      signedExpiredTokenLock <- Signed.forAsyncHasher(expiredTokenLock, kp)

      acceptedTokenLocks = SortedMap(address -> SortedSet(signedNewTokenLock))
      lastActiveTokenLocks = SortedMap(address -> SortedSet(signedExpiredTokenLock))
      acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

      result <- manager.acceptTokenLocks(
        epochProgress,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )

      (updatedTokenLocks, expiredTokenLocks) = result
    } yield
      expect(updatedTokenLocks(address).size == 1) &&
        expect(updatedTokenLocks(address).head == signedNewTokenLock) &&
        expect(expiredTokenLocks(address).size == 1) &&
        expect(expiredTokenLocks(address).head == signedExpiredTokenLock)
  }

  test("acceptTokenLocks - should remove token locks that are in accepted unlocks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      epochProgress = EpochProgress(1000L)

      tokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(2000L).some,
        none
      )

      signedTokenLock <- Signed.forAsyncHasher(tokenLock, kp)
      tokenLockHash <- signedTokenLock.toHashed

      acceptedTokenLocks = SortedMap.empty[Address, SortedSet[Signed[TokenLock]]]
      lastActiveTokenLocks = SortedMap(address -> SortedSet(signedTokenLock))

      tokenUnlock = TokenUnlock(tokenLockHash.hash, TokenLockAmount(100L), none, address)
      acceptedTokenUnlocks = SortedSet(tokenUnlock)

      result <- manager.acceptTokenLocks(
        epochProgress,
        acceptedTokenLocks,
        lastActiveTokenLocks,
        acceptedTokenUnlocks
      )

      (updatedTokenLocks, expiredTokenLocks) = result
    } yield
      expect(!updatedTokenLocks.contains(address)) &&
        expect(!expiredTokenLocks.contains(address))
  }

  test("emitTokenUnlocks - should emit new unlocks for expired token locks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      expiredTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(999L).some,
        none
      )

      signedExpiredTokenLock <- Signed.forAsyncHasher(expiredTokenLock, kp)
      expiredTokenLocks = SortedMap(address -> SortedSet(signedExpiredTokenLock))
      acceptedTokenUnlocks = SortedSet.empty[TokenUnlock]

      result <- manager.emitTokenUnlocks(acceptedTokenUnlocks, expiredTokenLocks)
    } yield
      expect(result.size == 1) &&
        expect(result.head.isInstanceOf[TokenUnlock])
  }

  test("emitTokenUnlocks - should not emit duplicates for already accepted unlocks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      expiredTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(999L).some,
        none
      )

      signedExpiredTokenLock <- Signed.forAsyncHasher(expiredTokenLock, kp)
      tokenLockHash <- signedExpiredTokenLock.toHashed

      expiredTokenLocks = SortedMap(address -> SortedSet(signedExpiredTokenLock))

      acceptedTokenUnlock = TokenUnlock(tokenLockHash.hash, TokenLockAmount(100L), none, address)
      acceptedTokenUnlocks = SortedSet(acceptedTokenUnlock)

      result <- manager.emitTokenUnlocks(acceptedTokenUnlocks, expiredTokenLocks)
    } yield
      expect(result.size == 1) &&
        expect(result.head == acceptedTokenUnlock)
  }

  test("emitTokenUnlocks - should combine new and accepted unlocks") { res =>
    implicit val (hasher, sp) = res
    val manager = TokenLockOpsManager.make[IO]

    for {
      kp <- KeyPairGenerator.makeKeyPair[IO]
      address <- kp.getPublic.toId.toAddress

      expiredTokenLock = TokenLock(
        address,
        TokenLockAmount(100L),
        TokenLockFee(10L),
        TokenLockReference(TokenLockOrdinal(1L), Hash("ref1")),
        none,
        EpochProgress(999L).some,
        none
      )

      signedExpiredTokenLock <- Signed.forAsyncHasher(expiredTokenLock, kp)
      expiredTokenLocks = SortedMap(address -> SortedSet(signedExpiredTokenLock))

      acceptedTokenUnlock = TokenUnlock(Hash("different"), TokenLockAmount(200L), none, address)
      acceptedTokenUnlocks = SortedSet(acceptedTokenUnlock)

      result <- manager.emitTokenUnlocks(acceptedTokenUnlocks, expiredTokenLocks)
    } yield expect(result.size == 2) // One new unlock + one accepted unlock
  }

}
